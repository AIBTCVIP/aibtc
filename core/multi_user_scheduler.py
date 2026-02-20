# multi_user_scheduler.py
"""
多用户调度器

核心功能：
1. 时间片轮转：避免所有用户同时投喂 AI
2. 优先级调度：VIP 用户优先
3. 限流熔断：保护 AI API 和交易所 API
4. 错误隔离：单用户错误不影响其他用户
"""

import asyncio
import threading
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Callable, Any
from dataclasses import dataclass
from enum import Enum
import logging

from core.user_context import UserContext, context_manager, get_user_context
from core.user_db import config_loader
from notifications.notifier import queue_message_for_user

logger = logging.getLogger(__name__)


class UserTier(Enum):
    """用户等级（影响调度优先级）"""
    VIP = 4
    PRO = 3
    BASIC = 2
    FREE = 1


@dataclass
class ScheduleTask:
    """调度任务"""
    uid: str
    tier: UserTier
    last_run_at: float = 0
    error_count: int = 0
    is_running: bool = False


class MultiUserScheduler:
    """
    多用户调度器
    
    调度策略：
    1. 每个 15m 周期开始时，收集所有需要执行的用户
    2. 按优先级排序（VIP > PRO > BASIC > FREE）- VIP 用户优先被处理
    3. 批量并发执行：
       - 每个用户有独立的 AI API key，可以同时调用不同的 AI 服务
       - 同一批次内的用户并发执行（asyncio.gather）
       - 批次大小默认 50，可根据服务器性能调整
    4. 错误隔离：单用户执行超时或错误不影响其他用户
    
    注意：
    - 优先级只影响执行顺序，不影响并发
    - VIP 用户在排序后会先进入第一批，因此优先获得执行机会
    - 所有用户的 AI 请求是独立的，互不影响
    """
    
    def __init__(
        self,
        batch_size: int = 500,           # 每批用户数（每个用户有独立的 AI key，可以大批量并发）
        batch_interval: float = 1.0,    # 批次间隔（秒）
        user_timeout: float = 300.0,    # 单用户超时（秒）- AI 响应可能较慢
        max_errors: int = 3,            # 最大连续错误次数（暂停）
        error_cooldown: float = 900.0,  # 错误冷却时间（秒）
    ):
        self.batch_size = batch_size
        self.batch_interval = batch_interval
        self.user_timeout = user_timeout
        self.max_errors = max_errors
        self.error_cooldown = error_cooldown
        
        self._tasks: Dict[str, ScheduleTask] = {}
        self._is_running = False
        # P3 Fix: 延迟初始化 asyncio.Lock，避免在模块导入时创建
        self._lock: Optional[asyncio.Lock] = None
        self._lock_init = threading.Lock()
        
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
    
    def register_user(self, uid: str, tier: str = "free"):
        """注册用户到调度器"""
        tier_enum = UserTier[tier.upper()] if tier.upper() in UserTier.__members__ else UserTier.FREE
        self._tasks[uid] = ScheduleTask(uid=uid, tier=tier_enum)
        logger.info(f"[调度器] 用户 {uid} 已注册 (tier={tier})")
    
    def unregister_user(self, uid: str):
        """从调度器移除用户"""
        if uid in self._tasks:
            del self._tasks[uid]
            logger.info(f"[调度器] 用户 {uid} 已移除")
    
    def _get_sorted_tasks(self) -> List[ScheduleTask]:
        """获取排序后的任务列表（优先级高的在前）"""
        now = time.time()

        # 过滤掉错误冷却中的用户
        active_tasks = [
            task for task in self._tasks.values()
            if task.error_count < self.max_errors or
               (now - task.last_run_at) > self.error_cooldown
        ]

        # 按优先级排序
        sorted_tasks = sorted(active_tasks, key=lambda t: (-t.tier.value, t.last_run_at))

        return sorted_tasks
    
    async def _run_single_user(self, task: ScheduleTask) -> bool:
        """
        执行单个用户的交易逻辑
        
        Returns:
            True: 成功
            False: 失败
        """
        uid = task.uid
        task.is_running = True
        task.last_run_at = time.time()
        
        try:
            # 获取用户上下文
            ctx = get_user_context(uid)
            if not ctx:
                logger.warning(f"[{uid}] 无法获取用户上下文，跳过")
                return False
            
            # 执行用户的交易逻辑
            success = await self._execute_user_cycle(ctx)
            
            if success:
                task.error_count = 0
                return True
            else:
                task.error_count += 1
                return False
                
        except asyncio.TimeoutError:
            logger.error(f"[{uid}] 执行超时")
            task.error_count += 1
            return False
            
        except Exception as e:
            logger.error(f"[{uid}] 执行异常: {e}")
            task.error_count += 1
            return False
            
        finally:
            task.is_running = False
    
    async def _execute_user_cycle(self, ctx: UserContext) -> bool:
        """
        执行用户的单轮交易逻辑
        
        流程：
        1. 刷新账户状态
        2. 拉取 K 线（共享公共数据）
        3. 计算指标并添加到 batch
        4. 按策略分组投喂 AI（同一策略合并，不同策略分开）
        5. 执行交易
        """
        uid = ctx.uid
        
        # 检查是否有启用的交易所
        if not ctx.has_enabled_exchanges():
            logger.info(f"[{uid}] 未配置任何交易所，跳过执行")
            return True
        
        try:
            # 1. 刷新账户状态
            from trading.account_positions import get_account_status_for_user
            snapshot = get_account_status_for_user(ctx)
            if snapshot:
                ctx.account_snapshot = snapshot
            
            # 2. 获取用户所有监控的币种（全局）
            all_symbols = ctx.get_monitor_symbols()
            if not all_symbols:
                logger.warning(f"[{uid}] 未配置监控币种，跳过")
                return True
            
            # 3. 数据已预处理完成，直接从全局batch_cache复制到用户batch_cache
            from llm.llm_api import batch_cache as global_batch_cache

            for sym in all_symbols:
                # 从全局batch_cache复制到用户的batch_cache
                if sym in global_batch_cache:
                    ctx.batch_cache[sym] = global_batch_cache[sym].copy()
            
            # 4. 按策略分组交易所
            # {strategy_id: {"exchanges": [ex1, ex2], "strategy_info": {...}}}
            strategy_groups = self._group_exchanges_by_strategy(ctx)
            
            # 如果没有任何交易所配置了策略，检查全局 AI 是否启用
            if not strategy_groups:
                if ctx.config.ai_enabled:
                    # 使用全局 AI 配置，但每个交易所独立投喂
                    running_exchanges = config_loader.get_running_exchanges(uid)
                    if running_exchanges:
                        # 每个交易所独立分组
                        for exchange in running_exchanges:
                            strategy_groups[f"{exchange}__global__"] = {
                                "exchanges": [exchange],  # 每个交易所独立
                                "strategy_info": None  # 使用全局配置
                            }
                    else:
                        logger.info(f"[{uid}] 全局 AI 启用但没有运行中的交易所，跳过投喂")
                        return True
                else:
                    logger.info(f"[{uid}] AI 未启用，跳过投喂")
                    return True
            
            # 5. 预初始化交易所连接（所有策略共享）
            multi_trader = ctx.multi_trader
            trader_initialized = await multi_trader.initialize()
            
            if not trader_initialized:
                logger.warning(f"[{uid}] 没有可用的交易所，仅执行投喂（不交易）")
            
            # 6. 按策略并行投喂 AI 并立即执行
            from llm.llm_api import push_batch_to_ai_for_user
            
            # 用于收集所有执行的信号（用于最后的通知）
            all_executed_signals = []
            executed_signals_lock = asyncio.Lock()
            
            async def feed_and_execute(strategy_id: str, group_info: dict):
                """单个策略的投喂+执行任务（返回后立即执行，不等待其他策略）"""
                target_exchanges = group_info["exchanges"]
                strategy_info = group_info["strategy_info"]
                strategy_name = strategy_info.get("name", "全局配置") if strategy_info else "全局配置"
                
                # 收集这些交易所的监控币种，并建立 symbol -> exchanges 映射
                group_symbols = self._collect_symbols_for_exchanges(ctx, target_exchanges)
                symbol_to_exchanges = self._build_symbol_exchange_mapping(ctx, target_exchanges)
                
                if not group_symbols:
                    logger.debug(f"[{uid}] 策略 {strategy_id} 无监控币种，跳过")
                    return
                
                # 调用 AI
                logger.info(f"[{uid}] 投喂策略: {strategy_name} | 交易所: {target_exchanges} | 币种: {len(group_symbols)}个")
                
                # 提取实际的策略ID
                # group_key 格式: "{exchange}_strategy_{strategy_id}" 或 "{exchange}__global__"
                actual_strategy_id = None
                if strategy_id and "__global__" not in strategy_id:
                    # 从 "binance_strategy_96fa9e2609c9ec14" 中提取 "96fa9e2609c9ec14"
                    parts = strategy_id.split("_strategy_")
                    if len(parts) == 2:
                        actual_strategy_id = parts[1]
                    else:
                        actual_strategy_id = strategy_id
                    logger.debug(f"[{uid}] 策略ID转换: {strategy_id} -> {actual_strategy_id}")
                
                try:
                    signals = await push_batch_to_ai_for_user(
                        ctx,
                        strategy_id=actual_strategy_id,
                        symbols=group_symbols,
                        target_exchanges=target_exchanges,
                        multi_trader=multi_trader  # 传入用于获取挂单
                    )
                except Exception as e:
                    logger.error(f"[{uid}] 策略 {strategy_name} 投喂失败: {e}")
                    # 通知用户投喂失败
                    if ctx.config.telegram_enabled and ctx.config.telegram_chat_id:
                        queue_message_for_user(
                            ctx,
                            f"⚠️ AI 投喂失败\n策略: {strategy_name}\n错误: {str(e)[:200]}"
                        )
                    return
                
                if not signals:
                    logger.info(f"[{uid}] [{strategy_name}] AI 未返回信号")
                    return
                
                # 过滤可执行信号
                executable_signals = [
                    sig for sig in signals
                    if sig.get("action") in EXECUTABLE_ACTIONS
                ]
                
                if not executable_signals:
                    logger.info(f"[{uid}] [{strategy_name}] 无可执行信号")
                    return
                
                # 立即执行交易
                if not trader_initialized:
                    logger.warning(f"[{uid}] [{strategy_name}] 交易所未初始化，跳过执行")
                    return
                
                logger.info(f"[{uid}] [{strategy_name}] 立即执行 {len(executable_signals)} 个信号")
                
                for sig in executable_signals:
                    try:
                        sig_symbol = sig.get("symbol")
                        
                        # 根据 symbol 找到配置了该币种的交易所
                        # 只在配置了该币种的交易所执行，而不是所有交易所
                        sig_target_exchanges = symbol_to_exchanges.get(sig_symbol, target_exchanges)
                        
                        if not sig_target_exchanges:
                            logger.warning(f"[{uid}] [{strategy_name}] {sig_symbol} 没有找到对应的交易所配置，跳过")
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
                            entry_price=sig.get("entry") or sig.get("entry_price"),  # 限价单入场价格
                            target_exchanges=sig_target_exchanges  # 只在配置了该币种的交易所执行
                        )
                        
                        if result.success_count > 0:
                            logger.info(f"[{uid}] [{strategy_name}] {sig_symbol} {sig.get('action')} 成功: {result.success_count}/{result.success_count + result.failure_count} 交易所 ({sig_target_exchanges})")
                        
                        if result.failure_count > 0:
                            logger.warning(f"[{uid}] [{strategy_name}] {sig.get('symbol')} {sig.get('action')} 失败: {result.failure_count} 交易所")
                            # 通知用户部分交易所执行失败
                            if ctx.config.telegram_enabled and ctx.config.telegram_chat_id:
                                # 收集失败的交易所和原因
                                failed_details = []
                                for ex_name, ex_result in result.results.items():
                                    if not ex_result.success:
                                        failed_details.append(f"  • {ex_name}: {ex_result.error or '未知错误'}")
                                
                                queue_message_for_user(
                                    ctx,
                                    f"⚠️ 下单部分失败\n"
                                    f"币种: {sig_symbol}\n"
                                    f"操作: {sig.get('action')}\n"
                                    f"成功: {result.success_count} | 失败: {result.failure_count}\n"
                                    f"失败详情:\n" + "\n".join(failed_details[:5])  # 最多显示5个
                                )
                        
                        # 记录已执行的信号（只要有成功就记录）
                        if result.success_count > 0:
                            # 附加策略和交易所信息，便于通知展示
                            sig_with_meta = sig.copy()
                            sig_with_meta["_strategy_name"] = strategy_name
                            sig_with_meta["_model"] = strategy_info.get("llm_model") if strategy_info else None
                            sig_with_meta["_provider"] = strategy_info.get("llm_provider") if strategy_info else None
                            # 记录成功执行的交易所
                            success_exchanges = [ex for ex, ex_res in result.results.items() if ex_res.success]
                            sig_with_meta["_exchanges"] = success_exchanges
                            
                            async with executed_signals_lock:
                                all_executed_signals.append(sig_with_meta)
                            
                    except Exception as trade_err:
                        logger.error(f"[{uid}] [{strategy_name}] 交易执行失败 {sig.get('symbol')}: {trade_err}")
                        # 通知用户交易执行异常
                        if ctx.config.telegram_enabled and ctx.config.telegram_chat_id:
                            queue_message_for_user(
                                ctx,
                                f"❌ 交易执行异常\n"
                                f"币种: {sig.get('symbol')}\n"
                                f"操作: {sig.get('action')}\n"
                                f"错误: {str(trade_err)[:200]}"
                            )
            
            # 并行执行所有策略（每个策略独立投喂+执行）
            feed_tasks = [
                feed_and_execute(strategy_id, group_info)
                for strategy_id, group_info in strategy_groups.items()
            ]
            
            if feed_tasks:
                await asyncio.gather(*feed_tasks, return_exceptions=True)
            
            # 所有策略完成后清空 batch_cache
            ctx.batch_cache.clear()
            
            # 7. 推送通知（汇总所有已执行的信号）
            if all_executed_signals and ctx.config.telegram_enabled and ctx.config.telegram_chat_id:
                try:
                    from notifications.trade_notifier import send_tg_trade_signal_for_user
                    # 使用多用户版本，传入 ctx 而不是 chat_id
                    await asyncio.to_thread(
                        send_tg_trade_signal_for_user,
                        ctx,
                        all_executed_signals,
                    )
                except Exception as tg_err:
                    logger.warning(f"[{uid}] Telegram 推送失败: {tg_err}")
            
            if all_executed_signals:
                logger.info(f"[{uid}] 本轮执行完成: {len(all_executed_signals)} 个信号")
            else:
                logger.info(f"[{uid}] 本轮无可执行信号")
            return True
            
        except Exception as e:
            logger.error(f"[{uid}] 执行异常: {e}", exc_info=True)
            return False
    
    def _group_exchanges_by_strategy(self, ctx: UserContext) -> Dict[str, Dict]:
        """
        按交易所分组（每个交易所独立投喂，即使使用相同策略）
        
        设计原因：
        - 每个交易所的资产是独立的
        - AI 需要基于单个交易所的资产做决策
        - 避免合并资产导致的仓位计算错误
        
        Returns:
            {
                "binance_strategy_abc123": {
                    "exchanges": ["binance"],  # 始终只有一个交易所
                    "strategy_info": {"id": "abc123", "name": "保守型", ...}
                },
                "okx_strategy_abc123": {
                    "exchanges": ["okx"],
                    "strategy_info": {"id": "abc123", "name": "保守型", ...}
                },
                "bitget_strategy_def456": {
                    "exchanges": ["bitget"],
                    "strategy_info": {"id": "def456", "name": "激进型", ...}
                }
            }
        """
        groups = {}
        
        # 获取正在运行的交易所（而不是所有启用的交易所）
        running_exchanges = config_loader.get_running_exchanges(ctx.uid)
        
        if not running_exchanges:
            logger.debug(f"[{ctx.uid}] 没有正在运行的交易所")
            return groups
        
        from core.strategy_cache import get_strategy_cache
        cache = get_strategy_cache()
        
        for exchange in running_exchanges:
            strategy_id = config_loader.get_exchange_ai_strategy_id(ctx.uid, exchange)
            
            if strategy_id:
                # 每个交易所独立分组，即使使用相同策略
                # 这样 AI 投喂时只看到该交易所的资产
                group_key = f"{exchange}_strategy_{strategy_id}"
                
                # 获取策略信息（使用缓存）
                strategy_info = cache.get_strategy(ctx.uid, strategy_id)
                groups[group_key] = {
                    "exchanges": [exchange],  # 始终只有一个交易所
                    "strategy_info": strategy_info
                }
        
        return groups
    
    def _collect_symbols_for_exchanges(self, ctx: UserContext, exchanges: List[str]) -> List[str]:
        """
        收集指定交易所的监控币种（去重），并过滤掉交易所不支持的符号
        
        Args:
            ctx: 用户上下文
            exchanges: 交易所列表
        
        Returns:
            交易所实际支持的币种列表
        """
        from core.symbol_availability import filter_symbols_for_exchange
        
        symbols = set()
        
        for exchange in exchanges:
            # 获取交易所级别的监控币种
            exchange_symbols = ctx.get_monitor_symbols(exchange=exchange)
            
            # 过滤掉该交易所不支持的符号
            available_symbols = filter_symbols_for_exchange(exchange, list(exchange_symbols))
            symbols.update(available_symbols)
        
        return list(symbols)
    
    def _build_symbol_exchange_mapping(self, ctx: UserContext, exchanges: List[str]) -> Dict[str, List[str]]:
        """
        构建 symbol -> exchanges 的映射
        
        用于确定每个信号应该在哪些交易所执行
        只包含交易所实际支持的符号
        
        Args:
            ctx: 用户上下文
            exchanges: 交易所列表
        
        Returns:
            {"BTCUSDT": ["binance", "okx"], "ETHUSDT": ["bitget"], ...}
        """
        from core.symbol_availability import filter_symbols_for_exchange
        
        mapping: Dict[str, List[str]] = {}
        
        for exchange in exchanges:
            # 获取该交易所配置的监控币种
            exchange_symbols = ctx.get_monitor_symbols(exchange=exchange)
            
            # 过滤掉该交易所不支持的符号
            available_symbols = filter_symbols_for_exchange(exchange, list(exchange_symbols))
            
            for symbol in available_symbols:
                if symbol not in mapping:
                    mapping[symbol] = []
                if exchange not in mapping[symbol]:
                    mapping[symbol].append(exchange)
        
        return mapping
    
    async def run_cycle(self):
        """执行一轮调度（所有用户）- 优化版：批量预处理数据"""
        async with self._get_lock():
            cycle_start = time.time()
            self._stats["last_cycle_at"] = cycle_start

            tasks = self._get_sorted_tasks()
            total = len(tasks)

            if total == 0:
                logger.info("[调度器] 无待执行用户")
                return

            # 清空全局 batch_cache（避免旧数据残留）
            from llm.llm_api import batch_cache
            batch_cache.clear()

            # 批量预处理数据
            await self._prepare_cycle_data(tasks)

            logger.info(f"[调度器] 开始调度 {total} 个用户")

            success_count = 0
            fail_count = 0

            # 分批执行
            for i in range(0, total, self.batch_size):
                batch = tasks[i:i + self.batch_size]
                batch_num = i // self.batch_size + 1

                logger.info(f"[调度器] 执行第 {batch_num} 批 ({len(batch)} 用户并发)")

                # 并发执行本批用户（每个用户独立的 AI key，互不影响）
                results = await asyncio.gather(
                    *[
                        asyncio.wait_for(
                            self._run_single_user(task),
                            timeout=self.user_timeout
                        )
                        for task in batch
                    ],
                    return_exceptions=True
                )

                for result in results:
                    if result is True:
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

    async def _prepare_cycle_data(self, tasks: List[ScheduleTask]):
        """
        批量预处理数据：收集用户币种 + 系统基准币种，预先下载K线和计算指标

        系统基准币种（BTC、ETH）总是被下载，用于构建全局市场上下文
        用户监控币种根据个人设置下载，用于AI分析

        Args:
            tasks: 调度任务列表
        """
        try:
            # 0. 刷新交易所符号可用性列表（每个周期刷新一次）
            try:
                from core.symbol_availability import refresh_symbol_availability
                await refresh_symbol_availability()
            except Exception as e:
                logger.warning(f"[调度器] 刷新符号可用性失败（使用缓存）: {e}")
            
            # 1. 系统基准币种（必须总是下载）
            # 使用全局市场指标币种（用于 market_regime 计算）
            from core.config import GLOBAL_MARKET_SYMBOLS
            system_base_symbols = set(GLOBAL_MARKET_SYMBOLS)

            # 2. 收集所有用户的监控币种（只收集正在运行的交易所的监控币种）
            user_symbols = set()

            for task in tasks:
                uid = task.uid
                try:
                    ctx = get_user_context(uid)
                    if ctx:
                        # 只收集正在运行的交易所的监控币种
                        running_exchanges = config_loader.get_running_exchanges(uid)
                        for exchange in running_exchanges:
                            exchange_symbols = ctx.get_monitor_symbols(exchange=exchange)
                            if exchange_symbols:
                                user_symbols.update(exchange_symbols)
                except Exception as e:
                    logger.warning(f"获取用户 {uid} 监控币种失败: {e}")
                    continue

            # 3. 合并所有需要下载的币种
            all_symbols = system_base_symbols.union(user_symbols)

            if not all_symbols:
                logger.warning("[调度器] 没有找到任何需要下载的币种")
                return

            logger.info(f"[调度器] 准备数据: {len(all_symbols)} 个币种 (用户:{len(user_symbols)}, 基准:{len(system_base_symbols)})")

            # 4. 批量下载K线数据
            await self._batch_download_klines(list(all_symbols))

            # 5. 批量计算指标
            await self._batch_calculate_indicators(list(all_symbols))
            
            # 6. 预热市场数据缓存（funding rate, OI, 24h change）
            await self._preheat_market_data_cache(list(all_symbols))

        except Exception as e:
            logger.error(f"[调度器] 批量数据准备失败: {e}")

    async def _batch_download_klines(self, symbols: List[str]):
        """
        批量下载K线数据

        Args:
            symbols: 需要下载的币种列表
        """
        from analysis.data.kline_fetcher import batch_download_klines

        try:
            await batch_download_klines(symbols)
        except Exception as e:
            logger.error(f"[调度器] K线批量下载失败: {e}")

    async def _batch_calculate_indicators(self, symbols: List[str]):
        """
        批量计算指标

        Args:
            symbols: 需要计算指标的币种列表
        """
        from analysis.indicators import batch_calculate_indicators

        try:
            await batch_calculate_indicators(symbols)
        except Exception as e:
            logger.error(f"[调度器] 指标批量计算失败: {e}")
    
    async def _preheat_market_data_cache(self, symbols: List[str]):
        """
        预热市场数据缓存（funding rate, OI, 24h change）
        
        在所有用户执行前统一请求，避免重复 API 调用。
        所有用户共享同一份缓存数据。
        
        Args:
            symbols: 需要预热的币种列表
        """
        from analysis.data.volume_stats import batch_fetch_async
        
        try:
            start_time = time.time()
            result = await batch_fetch_async(symbols)
            
            # 统计结果
            funding_count = len([v for v in result.get("funding", {}).values() if v is not None])
            oi_count = len([v for v in result.get("oi", {}).values() if v is not None])
            p24_count = len([v for v in result.get("p24", {}).values() if v is not None])
            
            duration = time.time() - start_time
            logger.info(
                f"[调度器] 市场数据预热完成: {len(symbols)} 币种, "
                f"funding:{funding_count}, oi:{oi_count}, 24h:{p24_count}, "
                f"耗时 {duration:.2f}s"
            )
        except Exception as e:
            logger.error(f"[调度器] 市场数据预热失败: {e}")
    
    async def start(self, run_immediately: bool = None):
        """
        启动调度器
        
        Args:
            run_immediately: 是否立即执行一轮
                - None: 从 config.py 读取 SCHEDULER_RUN_IMMEDIATELY 配置
                - True: 立即执行
                - False: 等待下一个15分钟周期
        """
        self._is_running = True
        logger.info("[调度器] 启动")

        # 刷新交易所符号可用性列表（启动时强制刷新）
        try:
            from core.symbol_availability import get_symbol_manager
            manager = get_symbol_manager()
            await manager.refresh_all(force=True)
        except Exception as e:
            logger.error(f"[调度器] 刷新符号可用性失败: {e}")

        # 从数据库加载所有活跃用户
        await self._load_users_from_db()

        # 判断是否立即执行
        if run_immediately is None:
            from core.config import SCHEDULER_RUN_IMMEDIATELY
            run_immediately = SCHEDULER_RUN_IMMEDIATELY

        # 首次启动立即执行一次
        if run_immediately and self._tasks:
            logger.info("[调度器] 首次启动，立即执行一轮")
            try:
                await self.run_cycle()
            except Exception as e:
                logger.error(f"[调度器] 首次执行异常: {e}")
        else:
            logger.info("[调度器] 首次启动，等待下一个15分钟周期执行")
        
        while self._is_running:
            try:
                # 等待到下一个 15m 整点
                now = datetime.now(timezone.utc)
                wait_seconds = self._seconds_to_next_15m(now)
                
                logger.info(f"[调度器] 距下次执行 {int(wait_seconds)} 秒")
                await asyncio.sleep(wait_seconds)
                
                if not self._is_running:
                    break
                
                # 执行调度
                await self.run_cycle()
            
            except asyncio.CancelledError:
                logger.info("[调度器] 收到取消信号，退出主循环")
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
        从数据库加载活跃用户并恢复运行状态
        
        系统重启后，自动恢复之前启用交易的用户：
        1. 从数据库读取 trading_enabled=1 的用户
        2. 创建 UserContext 并启动 WebSocket
        3. 注册到调度器
        """
        uids = config_loader.get_all_active_uids()
        
        if not uids:
            logger.info("[调度器] 没有需要恢复的用户")
            return

        logger.info(f"[调度器] 发现 {len(uids)} 个需要恢复的用户")
        
        restored_count = 0
        failed_count = 0

        for uid in uids:
            try:
                logger.info(f"[调度器] 正在恢复用户 {uid}...")
                
                config = config_loader.load(uid)
                if not config:
                    logger.warning(f"[调度器] 用户 {uid} 配置加载失败")
                    failed_count += 1
                    continue
                
                # 获取之前运行的交易所列表（而不是所有启用的交易所）
                running_exchanges = config_loader.get_running_exchanges(uid)
                
                logger.debug(f"[调度器] 用户 {uid} 配置: enabled_exchanges={config.enabled_exchanges}, running_exchanges={running_exchanges}")
                
                # 只恢复之前运行的交易所
                if not running_exchanges:
                    logger.info(f"[调度器] 用户 {uid} 没有正在运行的交易所，跳过恢复")
                    continue
                
                # 创建用户上下文但不自动初始化所有交易所
                ctx = context_manager.get_context(uid, auto_start=False, init_exchanges=False)
                if not ctx:
                    logger.warning(f"[调度器] 用户 {uid} 创建上下文失败")
                    failed_count += 1
                    continue
                
                # 只启动之前运行的交易所
                started_count = 0
                for exchange in running_exchanges:
                    try:
                        # 添加交易所上下文
                        if not ctx.add_single_exchange(exchange):
                            logger.warning(f"[调度器] 用户 {uid} 添加交易所 {exchange} 失败")
                            continue
                        
                        # 启动交易所
                        if ctx.start_exchange(exchange):
                            started_count += 1
                            logger.info(f"[调度器] 用户 {uid} 交易所 {exchange} 已恢复启动")
                        else:
                            logger.warning(f"[调度器] 用户 {uid} 启动交易所 {exchange} 失败")
                    except Exception as ex_err:
                        logger.error(f"[调度器] 用户 {uid} 恢复交易所 {exchange} 异常: {ex_err}")
                
                if started_count == 0:
                    logger.warning(f"[调度器] 用户 {uid} 没有成功恢复任何交易所")
                    continue
                
                # 注册到调度器
                self.register_user(uid, config.tier)
                restored_count += 1
                logger.info(f"[调度器] 用户 {uid} 恢复成功，启动了 {started_count} 个交易所")
                
            except Exception as e:
                logger.error(f"[调度器] 恢复用户 {uid} 失败: {e}", exc_info=True)
                failed_count += 1

        logger.info(f"[调度器] 用户恢复完成: 成功 {restored_count}, 失败 {failed_count}")
    
    @staticmethod
    def _seconds_to_next_15m(now: datetime) -> float:
        """计算距下一个 15m 整点的秒数（00, 15, 30, 45分）"""
        # 计算下一个15分钟整点
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
        }


# 需要从 scheduler.py 导入的常量
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


# 全局调度器实例
scheduler = MultiUserScheduler()
