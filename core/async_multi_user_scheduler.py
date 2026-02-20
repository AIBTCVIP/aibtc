# async_multi_user_scheduler.py
"""
纯 Asyncio 多用户调度器

核心设计：
1. 使用 TaskGroup 实现用户隔离（一个用户错误不影响其他用户）
2. 每个用户有独立的 AsyncUserContext（不共享全局 batch_cache）
3. 纯 asyncio 实现，无 threading 依赖

与 multi_user_scheduler.py 的区别：
- 使用 AsyncUserContext 而不是 UserContext
- 使用 TaskGroup 而不是 asyncio.gather
- 每个用户的 batch_cache 独立（数据隔离）
- 更细粒度的错误处理和恢复
"""

import asyncio
import threading
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Any, TYPE_CHECKING

if TYPE_CHECKING:
    from core.user_context import UserConfig
from dataclasses import dataclass
from enum import Enum
import logging

from core.async_user_context import (
    AsyncUserContext, 
    async_context_manager, 
    get_async_user_context,
    UserIsolatedExecutor,
)
from core.user_db import config_loader

logger = logging.getLogger(__name__)


class UserTier(Enum):
    """用户等级"""
    VIP = 4
    PRO = 3
    BASIC = 2
    FREE = 1


@dataclass
class AsyncScheduleTask:
    """异步调度任务"""
    uid: str
    tier: UserTier
    last_run_at: float = 0
    error_count: int = 0
    consecutive_errors: int = 0
    is_running: bool = False
    last_error: Optional[str] = None


# 可执行的动作类型
EXECUTABLE_ACTIONS = {
    "open_long", "open_short",
    "open_long_market", "open_short_market",
    "open_long_limit", "open_short_limit",
    "close_long", "close_short",
    "reverse", "stop_orders",
    "increase_position", "decrease_position",
    "update_stop_loss", "update_take_profit",
    "cancel",
}

# 需要校验的开仓动作
OPEN_ACTIONS = {
    "open_long", "open_short",
    "open_long_market", "open_short_market",
    "open_long_limit", "open_short_limit",
    "increase_position",  # 加仓也需要校验金额
}

# 限价单动作
LIMIT_ACTIONS = {
    "open_long_limit", "open_short_limit",
}


def validate_signal(
    sig: Dict[str, Any],
    config: Any,  # UserConfig
    account_balance: float,
    current_price: Optional[float]
) -> tuple[bool, Optional[str]]:
    """
    校验信号是否满足用户配置的约束条件
    
    Args:
        sig: 信号字典
        config: 用户配置
        account_balance: 账户余额 (USDT)
        current_price: 当前价格
        
    Returns:
        (is_valid, rejection_reason)
        - (True, None) 表示校验通过
        - (False, "原因") 表示校验失败
    """
    action = sig.get("action", "")
    symbol = sig.get("symbol", "")
    
    # 只校验开仓动作
    if action not in OPEN_ACTIONS:
        return True, None
    
    # 判断多空方向 (从 action 或 side 字段)
    action_lower = action.lower()
    side = str(sig.get("side", "")).upper()
    is_long = "long" in action_lower or side == "LONG" or side == "BUY"
    is_short = "short" in action_lower or side == "SHORT" or side == "SELL"
    
    # 1. 开仓金额校验 (position_size_pct 作为倍数上限: 3.0 = 单仓 ≤ 3× balance)
    position_size = float(sig.get("position_size") or sig.get("order_value") or sig.get("amount") or 0)
    if position_size > 0 and account_balance > 0:
        max_position_size = account_balance * config.position_size_pct
        if position_size > max_position_size * 1.001:  # 0.1% 容差，避免浮点精度误拒
            return False, (
                f"开仓金额 {position_size:.2f} USDT 超过限制 "
                f"(上限 {config.position_size_pct}× × {account_balance:.2f} = {max_position_size:.2f} USDT)"
            )

    # 2. 限价单距离校验
    if action in LIMIT_ACTIONS and current_price and current_price > 0:
        entry_price = float(sig.get("entry") or sig.get("entry_price") or 0)
        if entry_price > 0:
            distance_pct = abs(entry_price - current_price) / current_price * 100
            if distance_pct < config.limit_order_min_distance_pct:
                return False, (
                    f"限价单距离 {distance_pct:.2f}% 低于最小要求 {config.limit_order_min_distance_pct}% "
                    f"(entry={entry_price:.4f}, current={current_price:.4f})"
                )
    
    return True, None


class AsyncMultiUserScheduler:
    """
    纯 Asyncio 多用户调度器
    
    调度策略：
    1. 每个 15m 周期开始时，收集所有需要执行的用户
    2. 按优先级排序（VIP 优先）
    3. 使用 TaskGroup 并发执行（用户隔离）
    4. 每个用户有独立的 batch_cache
    """
    
    def __init__(
        self,
        batch_size: int = 500,           # 每批用户数
        batch_interval: float = 1.0,     # 批次间隔（秒）
        user_timeout: float = 300.0,     # 单用户超时（秒）
        max_consecutive_errors: int = 3, # 最大连续错误次数
        error_cooldown: float = 900.0,   # 错误冷却时间（秒）
    ):
        self.batch_size = batch_size
        self.batch_interval = batch_interval
        self.user_timeout = user_timeout
        self.max_consecutive_errors = max_consecutive_errors
        self.error_cooldown = error_cooldown
        
        self._tasks: Dict[str, AsyncScheduleTask] = {}
        self._is_running = False
        # P3 Fix: 延迟初始化 asyncio.Lock，避免在模块导入时创建
        self._lock: Optional[asyncio.Lock] = None
        self._lock_init = threading.Lock()
        
        # 用户隔离执行器
        self._executor = UserIsolatedExecutor(
            timeout=user_timeout,
            max_concurrent=batch_size,
        )
        
        # 全局数据缓存（只读，用于复制到用户独立缓存）
        self._global_batch_cache: Dict[str, Dict] = {}
        # P3 Fix: 延迟初始化 asyncio.Lock
        self._cache_lock: Optional[asyncio.Lock] = None
        self._cache_lock_init = threading.Lock()
        
        # 统计
        self._stats = {
            "total_runs": 0,
            "success_runs": 0,
            "failed_runs": 0,
            "last_cycle_at": None,
            "last_cycle_duration": 0,
        }
    
    def _get_lock(self) -> asyncio.Lock:
        """获取调度锁（延迟初始化）"""
        if self._lock is None:
            with self._lock_init:
                if self._lock is None:
                    self._lock = asyncio.Lock()
        return self._lock
    
    def _get_cache_lock(self) -> asyncio.Lock:
        """获取缓存锁（延迟初始化）"""
        if self._cache_lock is None:
            with self._cache_lock_init:
                if self._cache_lock is None:
                    self._cache_lock = asyncio.Lock()
        return self._cache_lock
    
    def register_user(self, uid: str, tier: str = "free"):
        """注册用户到调度器"""
        tier_enum = UserTier[tier.upper()] if tier.upper() in UserTier.__members__ else UserTier.FREE
        self._tasks[uid] = AsyncScheduleTask(uid=uid, tier=tier_enum)
        logger.info(f"[调度器] 用户 {uid} 已注册 (tier={tier})")
    
    def unregister_user(self, uid: str):
        """从调度器移除用户"""
        if uid in self._tasks:
            del self._tasks[uid]
            logger.info(f"[调度器] 用户 {uid} 已移除")
    
    def _get_sorted_tasks(self) -> List[AsyncScheduleTask]:
        """获取排序后的任务列表"""
        now = time.time()
        
        # 过滤掉错误冷却中的用户
        active_tasks = [
            task for task in self._tasks.values()
            if task.consecutive_errors < self.max_consecutive_errors or
               (now - task.last_run_at) > self.error_cooldown
        ]
        
        # 按优先级排序
        return sorted(active_tasks, key=lambda t: (-t.tier.value, t.last_run_at))
    
    async def _run_single_user_isolated(self, ctx: AsyncUserContext) -> bool:
        """
        在隔离环境中执行单个用户的交易逻辑
        
        这个函数会被 TaskGroup 调用，异常不会影响其他用户
        """
        uid = ctx.uid
        task = self._tasks.get(uid)
        if task:
            task.is_running = True
            task.last_run_at = time.time()
        
        try:
            success = await self._execute_user_cycle(ctx)
            
            if success:
                if task:
                    task.consecutive_errors = 0
                    task.last_error = None
                return True
            else:
                if task:
                    task.consecutive_errors += 1
                    task.error_count += 1
                return False
                
        except asyncio.CancelledError:
            logger.info(f"[{uid}] 任务被取消")
            raise
            
        except Exception as e:
            logger.error(f"[{uid}] 执行异常: {e}")
            if task:
                task.consecutive_errors += 1
                task.error_count += 1
                task.last_error = str(e)
            ctx.record_error(e)
            return False
            
        finally:
            if task:
                task.is_running = False
    
    async def _execute_user_cycle(self, ctx: AsyncUserContext) -> bool:
        """
        执行用户的单轮交易逻辑
        
        流程：
        1. 从全局缓存复制数据到用户独立缓存
        2. 刷新账户状态
        3. 按策略投喂 AI
        4. 执行交易
        """
        uid = ctx.uid
        
        # 检查同步上下文
        if not ctx.sync_ctx:
            logger.warning(f"[{uid}] 同步上下文不可用")
            return False
        
        # 检查是否有启用的交易所
        if not ctx.sync_ctx.has_enabled_exchanges():
            logger.info(f"[{uid}] 未配置任何交易所，跳过执行")
            return True
        
        try:
            # 1. 刷新账户状态
            await ctx.refresh_account_snapshot()
            await asyncio.sleep(0)  # 让出事件循环给前端API
            
            # 2. 获取用户所有监控的币种
            all_symbols = await ctx.get_monitor_symbols()
            if not all_symbols:
                logger.warning(f"[{uid}] 未配置监控币种，跳过")
                return True
            
            # 3. 从全局缓存复制数据到用户独立缓存
            # 注意：这里只读取全局缓存，不需要全局锁
            # 用户独立缓存由 ctx._batch_cache_lock 保护
            await ctx.copy_from_global_batch_cache(self._global_batch_cache, all_symbols)
            await asyncio.sleep(0)  # 让出事件循环给前端API
            
            # 4. 按策略分组交易所
            strategy_groups = await self._group_exchanges_by_strategy(ctx)
            
            if not strategy_groups:
                # 检查全局 AI 是否启用
                if ctx.config.ai_enabled:
                    running_exchanges = config_loader.get_running_exchanges(uid)
                    if running_exchanges:
                        for exchange in running_exchanges:
                            strategy_groups[f"{exchange}__global__"] = {
                                "exchanges": [exchange],
                                "strategy_info": None
                            }
                    else:
                        logger.info(f"[{uid}] 全局 AI 启用但没有运行中的交易所，跳过投喂")
                        return True
                else:
                    logger.info(f"[{uid}] AI 未启用，跳过投喂")
                    return True
            
            # 5. 预初始化交易所连接
            multi_trader = await ctx.get_multi_trader()
            trader_initialized = await multi_trader.initialize()
            
            if not trader_initialized:
                logger.warning(f"[{uid}] 没有可用的交易所，仅执行投喂（不交易）")
            
            # 6. 按策略并行投喂 AI 并执行
            all_executed_signals = []
            executed_signals_lock = asyncio.Lock()
            
            async def feed_and_execute(strategy_id: str, group_info: dict):
                """单个策略的投喂+执行"""
                target_exchanges = group_info["exchanges"]
                strategy_info = group_info["strategy_info"]
                strategy_name = strategy_info.get("name", "全局配置") if strategy_info else "全局配置"
                
                # 收集监控币种
                group_symbols = await self._collect_symbols_for_exchanges(ctx, target_exchanges)
                symbol_to_exchanges = await self._build_symbol_exchange_mapping(ctx, target_exchanges)
                
                if not group_symbols:
                    return
                
                # 提取实际的策略ID
                actual_strategy_id = None
                if strategy_id and "__global__" not in strategy_id:
                    parts = strategy_id.split("_strategy_")
                    if len(parts) == 2:
                        actual_strategy_id = parts[1]
                    else:
                        actual_strategy_id = strategy_id
                
                # 调用 AI（使用用户独立的 batch_cache）
                # 在 LLM 调用前让出事件循环，确保前端API能响应
                await asyncio.sleep(0)
                
                try:
                    from llm.llm_api import push_batch_to_ai_for_user_async
                    signals, decision_id = await push_batch_to_ai_for_user_async(
                        ctx,
                        strategy_id=actual_strategy_id,
                        symbols=group_symbols,
                        target_exchanges=target_exchanges,
                        multi_trader=multi_trader
                    )
                except Exception as e:
                    logger.error(f"[{uid}] 策略 {strategy_name} 投喂失败: {e}")
                    await self._notify_user_error(ctx, f"AI 投喂失败\n策略: {strategy_name}\n错误: {str(e)[:200]}")
                    return
                
                # LLM 调用后让出事件循环
                await asyncio.sleep(0)
                
                if not signals:
                    logger.info(f"[{uid}] [{strategy_name}] AI 未返回信号")
                    return
                
                # 过滤可执行信号
                executable_signals = [
                    sig for sig in signals
                    if sig.get("action") in EXECUTABLE_ACTIONS
                ]
                
                if not executable_signals or not trader_initialized:
                    return
                
                # ========== 信号校验层 ==========
                # 获取账户余额用于校验
                account_balance = 0.0
                try:
                    from core.async_redis import AsyncPFCompatibilityLayer
                    account_data = await AsyncPFCompatibilityLayer.get_pf_account_async(uid)
                    account_balance = float(account_data.get("walletBalance") or 0)
                except Exception as e:
                    logger.warning(f"[{uid}] 获取账户余额失败，跳过金额校验: {e}")
                
                # 获取 batch_cache 用于获取当前价格
                batch_cache = await ctx.get_batch_cache()
                
                # 校验每个信号
                validated_signals = []
                for sig in executable_signals:
                    sig_symbol = sig.get("symbol", "")
                    
                    # 从 batch_cache 获取当前价格
                    current_price = None
                    if sig_symbol in batch_cache:
                        ind_15m = batch_cache[sig_symbol].get("15m", {}).get("indicators", {})
                        current_price = ind_15m.get("close")
                    
                    # 执行校验
                    is_valid, rejection_reason = validate_signal(
                        sig, ctx.config, account_balance, current_price
                    )
                    
                    if is_valid:
                        validated_signals.append(sig)
                    else:
                        reason = rejection_reason or "未知原因"
                        logger.warning(f"[{uid}] [{strategy_name}] 信号被拒绝: {sig_symbol} - {reason}")
                        # 通知用户信号被拒绝
                        await self._notify_signal_rejected(ctx, sig, reason, strategy_name)
                
                if not validated_signals:
                    logger.info(f"[{uid}] [{strategy_name}] 所有信号校验未通过，跳过执行")
                    return
                
                executable_signals = validated_signals
                # ========== 信号校验层结束 ==========
                
                # 执行交易
                logger.info(f"[{uid}] [{strategy_name}] 执行 {len(executable_signals)} 个信号")
                
                for sig in executable_signals:
                    try:
                        sig_symbol = sig.get("symbol")
                        sig_target_exchanges = symbol_to_exchanges.get(sig_symbol, target_exchanges)
                        
                        if not sig_target_exchanges:
                            continue
                        
                        result = await multi_trader.execute_trade(
                            symbol=sig_symbol,
                            action=sig.get("action"),
                            stop_loss=sig.get("stop_loss"),
                            take_profit=sig.get("take_profit"),
                            position_size=(
                                sig.get("position_size")
                                or sig.get("order_value")
                                or sig.get("amount")
                            ),
                            quantity=sig.get("quantity"),
                            leverage=ctx.config.default_leverage,
                            entry_price=sig.get("entry") or sig.get("entry_price"),
                            target_exchanges=sig_target_exchanges,
                            ai_decision_id=decision_id,  # 传递 AI 决策 ID
                        )
                        
                        if result.success_count > 0:
                            sig_with_meta = sig.copy()
                            sig_with_meta["_strategy_name"] = strategy_name
                            sig_with_meta["_model"] = strategy_info.get("llm_model") if strategy_info else None
                            sig_with_meta["_provider"] = strategy_info.get("llm_provider") if strategy_info else None
                            sig_with_meta["_decision_id"] = decision_id  # AI 决策记录 ID
                            success_exchanges = [ex for ex, ex_res in result.results.items() if ex_res.success]
                            sig_with_meta["_exchanges"] = success_exchanges
                            
                            async with executed_signals_lock:
                                all_executed_signals.append(sig_with_meta)
                        
                        # 通知失败情况
                        if result.failure_count > 0:
                            if result.success_count > 0:
                                # 部分失败
                                await self._notify_partial_failure(ctx, sig, result)
                            else:
                                # 全部失败
                                await self._notify_trade_failure(ctx, sig, result, strategy_name)
                            
                    except Exception as trade_err:
                        logger.error(f"[{uid}] [{strategy_name}] 交易执行失败: {trade_err}")
                        await self._notify_user_error(ctx, f"交易执行异常\n策略: {strategy_name}\n币种: {sig.get('symbol')}\n错误: {str(trade_err)[:200]}")
            
            # 使用 asyncio.gather 并行执行所有策略（策略级隔离）
            # return_exceptions=True 确保一个策略失败不会影响其他策略
            results = await asyncio.gather(
                *[feed_and_execute(strategy_id, group_info) 
                  for strategy_id, group_info in strategy_groups.items()],
                return_exceptions=True
            )
            for result in results:
                if isinstance(result, Exception):
                    logger.error(f"[{uid}] 策略执行异常: {result}")
            
            # 清空用户的 batch_cache
            await ctx.clear_batch_cache()
            
            # 7. 推送通知
            if all_executed_signals and ctx.config.telegram_enabled:
                await self._send_trade_notification(ctx, all_executed_signals)
            
            if all_executed_signals:
                logger.info(f"[{uid}] 本轮执行完成: {len(all_executed_signals)} 个信号")
            
            return True
            
        except Exception as e:
            logger.error(f"[{uid}] 执行异常: {e}", exc_info=True)
            return False
    
    async def _group_exchanges_by_strategy(self, ctx: AsyncUserContext) -> Dict[str, Dict]:
        """按交易所分组"""
        from core.async_helpers import run_sync
        from core.strategy_cache import get_strategy_cache
        
        groups = {}
        running_exchanges = await run_sync(lambda: config_loader.get_running_exchanges(ctx.uid))
        
        if not running_exchanges:
            return groups
        
        cache = get_strategy_cache()
        for exchange in running_exchanges:
            strategy_id = await run_sync(
                lambda ex=exchange: config_loader.get_exchange_ai_strategy_id(ctx.uid, ex)
            )
            
            if strategy_id:
                group_key = f"{exchange}_strategy_{strategy_id}"
                strategy_info = cache.get_strategy(ctx.uid, strategy_id)
                groups[group_key] = {
                    "exchanges": [exchange],
                    "strategy_info": strategy_info
                }
        
        return groups
    
    async def _collect_symbols_for_exchanges(self, ctx: AsyncUserContext, exchanges: List[str]) -> List[str]:
        """收集指定交易所的监控币种"""
        from core.symbol_availability import filter_symbols_for_exchange
        
        symbols = set()
        for exchange in exchanges:
            exchange_symbols = await ctx.get_monitor_symbols(exchange=exchange)
            available_symbols = filter_symbols_for_exchange(exchange, list(exchange_symbols))
            symbols.update(available_symbols)
        
        return list(symbols)
    
    async def _build_symbol_exchange_mapping(self, ctx: AsyncUserContext, exchanges: List[str]) -> Dict[str, List[str]]:
        """构建 symbol -> exchanges 映射"""
        from core.symbol_availability import filter_symbols_for_exchange
        
        mapping: Dict[str, List[str]] = {}
        for exchange in exchanges:
            exchange_symbols = await ctx.get_monitor_symbols(exchange=exchange)
            available_symbols = filter_symbols_for_exchange(exchange, list(exchange_symbols))
            
            for symbol in available_symbols:
                if symbol not in mapping:
                    mapping[symbol] = []
                if exchange not in mapping[symbol]:
                    mapping[symbol].append(exchange)
        
        return mapping
    
    async def _notify_user_error(self, ctx: AsyncUserContext, message: str):
        """通知用户错误"""
        if not ctx.config.telegram_enabled or not ctx.config.telegram_chat_id:
            logger.debug(f"[{ctx.uid}] Telegram 未配置，跳过通知")
            return
        
        try:
            from notifications.notifier import get_async_queue
            from core.config import SYSTEM_TELEGRAM_BOT_TOKEN
            
            if not SYSTEM_TELEGRAM_BOT_TOKEN:
                logger.warning(f"[{ctx.uid}] 系统未配置 SYSTEM_TELEGRAM_BOT_TOKEN")
                return
            
            queue = get_async_queue()
            await queue.put({
                "text": f"[Warning] {message}",
                "uid": ctx.uid,
                "bot_token": SYSTEM_TELEGRAM_BOT_TOKEN,
                "chat_id": ctx.config.telegram_chat_id,
                "topic_id": getattr(ctx.config, 'telegram_topic_id', None),
            })
            logger.debug(f"[{ctx.uid}] 错误通知已入队: {message[:50]}...")
        except Exception as e:
            logger.warning(f"[{ctx.uid}] 发送错误通知失败: {e}")
    
    async def _notify_partial_failure(self, ctx: AsyncUserContext, sig: Dict, result):
        """通知部分交易失败"""
        if not ctx.config.telegram_enabled or not ctx.config.telegram_chat_id:
            logger.debug(f"[{ctx.uid}] Telegram 未配置，跳过部分失败通知")
            return
        
        try:
            from notifications.notifier import get_async_queue
            from core.config import SYSTEM_TELEGRAM_BOT_TOKEN
            
            if not SYSTEM_TELEGRAM_BOT_TOKEN:
                logger.warning(f"[{ctx.uid}] 系统未配置 SYSTEM_TELEGRAM_BOT_TOKEN")
                return
            
            failed_details = []
            for ex_name, ex_result in result.results.items():
                if not ex_result.success:
                    failed_details.append(f"  - {ex_name}: {ex_result.error or '未知错误'}")
            
            msg = (
                f"[Warning] 下单部分失败\n"
                f"币种: {sig.get('symbol')}\n"
                f"操作: {sig.get('action')}\n"
                f"成功: {result.success_count} | 失败: {result.failure_count}\n"
                f"失败详情:\n" + "\n".join(failed_details[:5])
            )
            
            queue = get_async_queue()
            await queue.put({
                "text": msg,
                "uid": ctx.uid,
                "bot_token": SYSTEM_TELEGRAM_BOT_TOKEN,
                "chat_id": ctx.config.telegram_chat_id,
                "topic_id": getattr(ctx.config, 'telegram_topic_id', None),
            })
            logger.info(f"[{ctx.uid}] 部分失败通知已入队: {sig.get('symbol')}")
        except Exception as e:
            logger.warning(f"[{ctx.uid}] 发送部分失败通知失败: {e}")
    
    async def _notify_trade_failure(self, ctx: AsyncUserContext, sig: Dict, result, strategy_name: str):
        """通知交易全部失败"""
        if not ctx.config.telegram_enabled or not ctx.config.telegram_chat_id:
            logger.debug(f"[{ctx.uid}] Telegram 未配置，跳过下单失败通知")
            return
        
        try:
            from notifications.notifier import get_async_queue
            from core.config import SYSTEM_TELEGRAM_BOT_TOKEN
            
            if not SYSTEM_TELEGRAM_BOT_TOKEN:
                logger.warning(f"[{ctx.uid}] 系统未配置 SYSTEM_TELEGRAM_BOT_TOKEN")
                return
            
            failed_details = []
            for ex_name, ex_result in result.results.items():
                if not ex_result.success:
                    failed_details.append(f"  - {ex_name}: {ex_result.error or '未知错误'}")
            
            msg = (
                f"[Error] 下单失败\n"
                f"策略: {strategy_name}\n"
                f"币种: {sig.get('symbol')}\n"
                f"操作: {sig.get('action')}\n"
                f"失败详情:\n" + "\n".join(failed_details[:5])
            )
            
            queue = get_async_queue()
            await queue.put({
                "text": msg,
                "uid": ctx.uid,
                "bot_token": SYSTEM_TELEGRAM_BOT_TOKEN,
                "chat_id": ctx.config.telegram_chat_id,
                "topic_id": getattr(ctx.config, 'telegram_topic_id', None),
            })
            logger.info(f"[{ctx.uid}] 下单失败通知已入队: {sig.get('symbol')}")
        except Exception as e:
            logger.warning(f"[{ctx.uid}] 发送下单失败通知失败: {e}")
    
    async def _notify_signal_rejected(self, ctx: AsyncUserContext, sig: Dict, reason: str, strategy_name: str):
        """通知信号被拒绝（校验未通过）"""
        if not ctx.config.telegram_enabled or not ctx.config.telegram_chat_id:
            logger.debug(f"[{ctx.uid}] Telegram 未配置，跳过信号拒绝通知")
            return
        
        try:
            from notifications.notifier import get_async_queue
            from core.config import SYSTEM_TELEGRAM_BOT_TOKEN
            
            if not SYSTEM_TELEGRAM_BOT_TOKEN:
                logger.warning(f"[{ctx.uid}] 系统未配置 SYSTEM_TELEGRAM_BOT_TOKEN")
                return
            
            msg = (
                f"[Rejected] 信号校验未通过\n"
                f"策略: {strategy_name}\n"
                f"币种: {sig.get('symbol')}\n"
                f"操作: {sig.get('action')}\n"
                f"原因: {reason}"
            )
            
            queue = get_async_queue()
            await queue.put({
                "text": msg,
                "uid": ctx.uid,
                "bot_token": SYSTEM_TELEGRAM_BOT_TOKEN,
                "chat_id": ctx.config.telegram_chat_id,
                "topic_id": getattr(ctx.config, 'telegram_topic_id', None),
            })
            logger.info(f"[{ctx.uid}] 信号拒绝通知已入队: {sig.get('symbol')}")
        except Exception as e:
            logger.warning(f"[{ctx.uid}] 发送信号拒绝通知失败: {e}")
    
    async def _send_trade_notification(self, ctx: AsyncUserContext, signals: List[Dict]):
        """发送交易通知"""
        if not ctx.config.telegram_enabled or not ctx.config.telegram_chat_id:
            logger.debug(f"[{ctx.uid}] Telegram 未配置，跳过交易通知")
            return
        
        try:
            from notifications.notifier import get_async_queue
            from notifications.trade_notifier import _format_signal_message
            from core.config import SYSTEM_TELEGRAM_BOT_TOKEN
            
            if not SYSTEM_TELEGRAM_BOT_TOKEN:
                logger.warning(f"[{ctx.uid}] 系统未配置 SYSTEM_TELEGRAM_BOT_TOKEN")
                return
            
            if not signals:
                return
            
            queue = get_async_queue()
            count = 0
            for sig in signals:
                msg = _format_signal_message(sig)
                if msg:
                    await queue.put({
                        "text": msg,
                        "uid": ctx.uid,
                        "bot_token": SYSTEM_TELEGRAM_BOT_TOKEN,
                        "chat_id": ctx.config.telegram_chat_id,
                        "topic_id": getattr(ctx.config, 'telegram_topic_id', None),
                    })
                    count += 1
            
            if count > 0:
                logger.info(f"[{ctx.uid}] 交易通知已入队: {count} 条")
        except Exception as e:
            logger.warning(f"[{ctx.uid}] Telegram 推送失败: {e}")
    
    async def run_cycle(self):
        """执行一轮调度（所有用户）- 使用 TaskGroup 隔离"""
        async with self._get_lock():
            cycle_start = time.time()
            self._stats["last_cycle_at"] = cycle_start
            
            tasks = self._get_sorted_tasks()
            total = len(tasks)
            
            if total == 0:
                logger.info("[调度器] 无待执行用户")
                return
            
            # 1. 清空全局缓存
            async with self._get_cache_lock():
                self._global_batch_cache.clear()
            
            # 2. 批量预处理数据（填充全局缓存）
            await self._prepare_cycle_data(tasks)
            
            logger.info(f"[调度器] 开始调度 {total} 个用户 (TaskGroup 隔离模式)")
            
            success_count = 0
            fail_count = 0
            
            # 3. 分批执行（使用 TaskGroup 实现用户隔离）
            for i in range(0, total, self.batch_size):
                batch = tasks[i:i + self.batch_size]
                batch_num = i // self.batch_size + 1
                batch_uids = [t.uid for t in batch]
                
                logger.info(f"[调度器] 执行第 {batch_num} 批 ({len(batch)} 用户)")
                
                # 使用 TaskGroup 隔离执行
                batch_results: Dict[str, bool] = {}
                
                async def run_user_task(task: AsyncScheduleTask):
                    """包装单用户执行"""
                    uid = task.uid
                    try:
                        ctx = await get_async_user_context(uid)
                        if not ctx:
                            logger.warning(f"[{uid}] 无法获取异步上下文")
                            batch_results[uid] = False
                            return
                        
                        result = await asyncio.wait_for(
                            self._run_single_user_isolated(ctx),
                            timeout=self.user_timeout
                        )
                        batch_results[uid] = result
                        
                    except asyncio.TimeoutError:
                        logger.error(f"[{uid}] 执行超时")
                        batch_results[uid] = False
                        if task:
                            task.consecutive_errors += 1
                            task.error_count += 1
                            task.last_error = "执行超时"
                            
                    except Exception as e:
                        logger.error(f"[{uid}] 执行异常: {e}")
                        batch_results[uid] = False
                        if task:
                            task.consecutive_errors += 1
                            task.error_count += 1
                            task.last_error = str(e)
                
                # 使用 asyncio.gather 确保一个用户失败不影响其他用户
                # return_exceptions=True 捕获异常但不中断
                results = await asyncio.gather(
                    *[run_user_task(task) for task in batch],
                    return_exceptions=True
                )
                for result in results:
                    if isinstance(result, Exception):
                        logger.error(f"[调度器] 用户执行异常: {result}")
                
                # 统计结果
                for uid, result in batch_results.items():
                    if result:
                        success_count += 1
                    else:
                        fail_count += 1
                
                # 批次间隔
                if i + self.batch_size < total:
                    await asyncio.sleep(self.batch_interval)
            
            duration = time.time() - cycle_start
            self._stats["total_runs"] += total
            self._stats["success_runs"] += success_count
            self._stats["failed_runs"] += fail_count
            self._stats["last_cycle_duration"] = duration
            
            logger.info(
                f"[调度器] 调度完成: {success_count}/{total} 成功, "
                f"耗时 {duration:.2f}s"
            )
    
    async def _prepare_cycle_data(self, tasks: List[AsyncScheduleTask]):
        """批量预处理数据"""
        try:
            # 1. 刷新交易所符号可用性
            try:
                from core.symbol_availability import refresh_symbol_availability
                await refresh_symbol_availability()
            except Exception as e:
                logger.warning(f"[调度器] 刷新符号可用性失败: {e}")
            
            # 2. 收集所有币种
            # 使用全局市场指标币种作为系统基础币种（用于 market_regime 计算）
            from core.config import GLOBAL_MARKET_SYMBOLS
            system_base_symbols = set(GLOBAL_MARKET_SYMBOLS)
            user_symbols = set()
            
            for task in tasks:
                uid = task.uid
                try:
                    ctx = await get_async_user_context(uid)
                    if ctx:
                        running_exchanges = config_loader.get_running_exchanges(uid)
                        for exchange in running_exchanges:
                            exchange_symbols = await ctx.get_monitor_symbols(exchange=exchange)
                            if exchange_symbols:
                                user_symbols.update(exchange_symbols)
                except Exception as e:
                    logger.warning(f"获取用户 {uid} 监控币种失败: {e}")
            
            all_symbols = system_base_symbols.union(user_symbols)
            
            if not all_symbols:
                logger.warning("[调度器] 没有找到任何需要下载的币种")
                return
            
            logger.info(f"[调度器] 准备数据: {len(all_symbols)} 个币种")
            
            # 3. 批量下载 K 线
            await self._batch_download_klines(list(all_symbols))
            await asyncio.sleep(0)  # 让出事件循环给前端API
            
            # 4. 批量计算指标
            await self._batch_calculate_indicators(list(all_symbols))
            await asyncio.sleep(0)  # 让出事件循环给前端API
            
            # 5. 预热市场数据
            await self._preheat_market_data_cache(list(all_symbols))
            await asyncio.sleep(0)  # 让出事件循环给前端API
            
            # 6. 将数据存入全局缓存（供用户复制）
            from llm.llm_api import batch_cache
            async with self._get_cache_lock():
                self._global_batch_cache.update(batch_cache)
            
        except Exception as e:
            logger.error(f"[调度器] 批量数据准备失败: {e}")
    
    async def _batch_download_klines(self, symbols: List[str]):
        """批量下载 K 线"""
        from analysis.data.kline_fetcher import batch_download_klines
        try:
            await batch_download_klines(symbols)
        except Exception as e:
            logger.error(f"[调度器] K线批量下载失败: {e}")
    
    async def _batch_calculate_indicators(self, symbols: List[str]):
        """批量计算指标"""
        from analysis.indicators import batch_calculate_indicators
        try:
            await batch_calculate_indicators(symbols)
        except Exception as e:
            logger.error(f"[调度器] 指标批量计算失败: {e}")
    
    async def _preheat_market_data_cache(self, symbols: List[str]):
        """预热市场数据"""
        from analysis.data.volume_stats import batch_fetch_async
        try:
            start_time = time.time()
            result = await batch_fetch_async(symbols)
            duration = time.time() - start_time
            logger.info(f"[调度器] 市场数据预热完成: {len(symbols)} 币种, 耗时 {duration:.2f}s")
        except Exception as e:
            logger.error(f"[调度器] 市场数据预热失败: {e}")
    
    async def start(self, run_immediately: Optional[bool] = None):
        """启动调度器"""
        self._is_running = True
        logger.info("[调度器] 启动 (AsyncMultiUserScheduler)")
        
        # 注意：符号可用性已在 lifecycle.on_startup 中刷新，这里不再重复
        # 如需强制刷新可取消注释下面代码
        # try:
        #     from core.symbol_availability import get_symbol_manager
        #     manager = get_symbol_manager()
        #     await manager.refresh_all(force=True)
        # except Exception as e:
        #     logger.error(f"[调度器] 刷新符号可用性失败: {e}")
        
        # 加载用户
        await self._load_users_from_db()
        
        # 判断是否立即执行
        if run_immediately is None:
            from core.config import SCHEDULER_RUN_IMMEDIATELY
            run_immediately = SCHEDULER_RUN_IMMEDIATELY
        
        if run_immediately and self._tasks:
            logger.info("[调度器] 首次启动，立即执行一轮")
            try:
                await self.run_cycle()
            except Exception as e:
                logger.error(f"[调度器] 首次执行异常: {e}")
        
        while self._is_running:
            try:
                now = datetime.now(timezone.utc)
                wait_seconds = self._seconds_to_next_15m(now)
                
                logger.info(f"[调度器] 距下次执行 {int(wait_seconds)} 秒")
                await asyncio.sleep(wait_seconds)
                
                if not self._is_running:
                    break
                
                await self.run_cycle()
                
            except asyncio.CancelledError:
                logger.info("[调度器] 收到取消信号")
                break
            except Exception as e:
                if not self._is_running:
                    break
                logger.error(f"[调度器] 主循环异常: {e}")
                try:
                    await asyncio.sleep(60)
                except asyncio.CancelledError:
                    break
    
    def stop(self):
        """停止调度器"""
        self._is_running = False
        logger.info("[调度器] 已停止")
    
    async def _load_users_from_db(self):
        """
        从数据库加载用户（并行初始化）
        
        使用 asyncio 并行初始化所有用户，大幅加速启动时间：
        - 100 用户顺序初始化：约 7 分钟
        - 100 用户并行初始化：约 10-20 秒
        
        1000+ 用户优化：
        - 使用 load_batch() 批量加载用户配置
        - 使用 get_running_exchanges_batch() 批量获取运行中交易所
        - 原实现：每用户 3 次查询 = 3000+ 查询
        - 优化后：3 次批量查询
        """
        from core.user_context import context_manager
        from core.async_helpers import run_sync
        
        uids = await run_sync(config_loader.get_all_active_uids)
        
        if not uids:
            logger.info("[调度器] 没有需要恢复的用户")
            return
        
        logger.info(f"[调度器] 发现 {len(uids)} 个需要恢复的用户，开始批量加载...")
        
        # 批量加载用户配置（优化 N+1 查询）
        user_configs = await run_sync(lambda: config_loader.load_batch(uids))
        logger.info(f"[调度器] 用户配置加载完成: {len(user_configs)}/{len(uids)}")
        
        # 批量获取 running_exchanges（优化 N+1 查询）
        running_exchanges_map = await run_sync(lambda: config_loader.get_running_exchanges_batch(uids))
        logger.info(f"[调度器] 运行中交易所加载完成")
        
        # 并行初始化结果统计
        results = {"restored": 0, "failed": 0}
        results_lock = asyncio.Lock()
        
        async def init_single_user(uid: str):
            """初始化单个用户（异步）"""
            try:
                # 从预加载的配置中获取（避免重复查询）
                config = user_configs.get(uid)
                if not config:
                    async with results_lock:
                        results["failed"] += 1
                    return
                
                # 从预加载的 running_exchanges 中获取
                running_exchanges = running_exchanges_map.get(uid, [])
                
                if not running_exchanges:
                    return
                
                # 创建同步上下文
                sync_ctx = context_manager.get_context(uid, auto_start=False, init_exchanges=False)
                if not sync_ctx:
                    async with results_lock:
                        results["failed"] += 1
                    return
                
                # 启动交易所（在线程池中执行，避免阻塞事件循环）
                started_count = 0
                for exchange in running_exchanges:
                    try:
                        # add_single_exchange 和 start_exchange 可能有阻塞操作
                        added = await run_sync(lambda ex=exchange: sync_ctx.add_single_exchange(ex))
                        if added:
                            started = await run_sync(lambda ex=exchange: sync_ctx.start_exchange(ex))
                            if started:
                                started_count += 1
                                logger.info(f"[调度器] 用户 {uid} 交易所 {exchange} 启动成功")
                            else:
                                logger.warning(f"[调度器] 用户 {uid} 交易所 {exchange} 启动失败")
                        else:
                            logger.warning(f"[调度器] 用户 {uid} 交易所 {exchange} 添加失败")
                    except Exception as e:
                        logger.error(f"[调度器] 用户 {uid} 交易所 {exchange} 异常: {e}")
                
                if started_count == 0:
                    return
                
                # 注册到调度器
                self.register_user(uid, config.tier)
                async with results_lock:
                    results["restored"] += 1
                
            except Exception as e:
                logger.error(f"[调度器] 恢复用户 {uid} 失败: {e}")
                async with results_lock:
                    results["failed"] += 1
        
        # 并行初始化所有用户（使用 semaphore 限制并发数，避免过载）
        from core.config import SCHEDULER_INIT_CONCURRENT
        max_concurrent = SCHEDULER_INIT_CONCURRENT  # 默认 30（避免触发交易所限流）
        semaphore = asyncio.Semaphore(max_concurrent)
        
        async def init_with_semaphore(uid: str):
            """初始化单个用户（带 semaphore 限制）"""
            async with semaphore:
                await init_single_user(uid)
        
        # 分批启动，避免瞬间创建过多协程
        batch_size = 50  # 每批 50 个用户
        for i in range(0, len(uids), batch_size):
            batch = uids[i:i + batch_size]
            await asyncio.gather(*[init_with_semaphore(uid) for uid in batch], return_exceptions=True)
            
            # 批次间添加小延迟，让限速器有时间恢复
            if i + batch_size < len(uids):
                await asyncio.sleep(0.5)
        
        logger.info(f"[调度器] 用户恢复完成: 成功 {results['restored']}, 失败 {results['failed']}")
    
    @staticmethod
    def _seconds_to_next_15m(now: datetime) -> float:
        """计算距下一个 15m 整点的秒数"""
        minute = ((now.minute // 15) + 1) * 15
        next_run = now.replace(second=0, microsecond=0)
        
        if minute >= 60:
            next_run = next_run.replace(minute=0) + timedelta(hours=1)
        else:
            next_run = next_run.replace(minute=minute)
        
        return max(1.0, (next_run - now).total_seconds())
    
    def get_stats(self) -> Dict:
        """获取调度器统计"""
        return {
            **self._stats,
            "registered_users": len(self._tasks),
            "is_running": self._is_running,
            "mode": "TaskGroup_isolated",
        }


# 全局调度器实例（使用配置文件参数）
from core.config import SCHEDULER_BATCH_SIZE, SCHEDULER_USER_TIMEOUT
async_scheduler = AsyncMultiUserScheduler(
    batch_size=SCHEDULER_BATCH_SIZE,        # 默认 800
    user_timeout=SCHEDULER_USER_TIMEOUT,    # 默认 300 秒
)


# ==================== 统一调度器访问函数 ====================
# 这些函数同时操作 async_scheduler 和旧的 multi_user_scheduler
# 确保在迁移期间两个调度器保持同步

def register_user_to_schedulers(uid: str, tier: str = "free"):
    """
    注册用户到所有调度器（兼容函数）
    
    同时注册到：
    - AsyncMultiUserScheduler (新版 TaskGroup 隔离)
    - MultiUserScheduler (旧版 asyncio.gather)
    """
    # 注册到新调度器
    async_scheduler.register_user(uid, tier)
    
    # 同时注册到旧调度器（兼容）
    try:
        from core.multi_user_scheduler import scheduler as old_scheduler
        old_scheduler.register_user(uid, tier)
    except Exception as e:
        # P5 Fix: 记录旧调度器注册失败
        logger.debug(f"[调度器] 旧调度器注册失败 (可忽略): {e}")
    
    logger.info(f"[调度器] 用户 {uid} 已注册到所有调度器 (tier={tier})")


def unregister_user_from_schedulers(uid: str):
    """
    从所有调度器移除用户（兼容函数）
    """
    # 从新调度器移除
    async_scheduler.unregister_user(uid)
    
    # 从旧调度器移除（兼容）
    try:
        from core.multi_user_scheduler import scheduler as old_scheduler
        old_scheduler.unregister_user(uid)
    except Exception as e:
        # P5 Fix: 记录旧调度器注销失败
        logger.debug(f"[调度器] 旧调度器注销失败 (可忽略): {e}")
    
    logger.info(f"[调度器] 用户 {uid} 已从所有调度器移除")


def get_scheduler_stats() -> dict:
    """
    获取调度器统计（优先返回新调度器）
    """
    return async_scheduler.get_stats()


def is_user_registered(uid: str) -> bool:
    """
    检查用户是否已注册到调度器
    """
    return uid in async_scheduler._tasks


def update_user_tier_in_schedulers(uid: str, tier: str):
    """
    更新用户等级（兼容函数）
    """
    # 更新新调度器
    if uid in async_scheduler._tasks:
        async_scheduler._tasks[uid].tier = UserTier[tier.upper()]
    
    # 更新旧调度器
    try:
        from core.multi_user_scheduler import scheduler as old_scheduler
        from core.multi_user_scheduler import UserTier as OldUserTier
        if uid in old_scheduler._tasks:
            old_scheduler._tasks[uid].tier = OldUserTier[tier.upper()]
    except Exception:
        pass