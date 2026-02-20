# trading/global_stop_loss_processor.py
"""
全局止损处理器 - 1000+ 用户规模优化版

核心优化：
1. 单例模式：全局一个处理器，而非每用户一个
2. 批量 Redis 读取：使用 Pipeline 一次读取所有用户数据
3. 本地内存缓存：500ms TTL，减少 Redis 调用 90%+
4. 批量计算：一次 tick 处理所有用户
5. 队列化下单：限速控制，避免交易所 API 限制

性能对比：
- 优化前：1000用户 × 6次Redis/tick = 6000 QPS
- 优化后：1次批量读取 + 本地缓存 = ~50 QPS
"""

from __future__ import annotations

import asyncio
import logging
import threading
import time
import json
from collections import defaultdict
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from typing import Dict, List, Optional, Set, Tuple, Any, Callable
from concurrent.futures import ThreadPoolExecutor

from core.utils import D, now_ms, d_to_str
from core.user_db import config_loader

logger = logging.getLogger(__name__)


# =============================================================================
# 数据结构
# =============================================================================

@dataclass
class PositionCache:
    """持仓缓存数据"""
    uid: str
    exchange: str
    symbol: str
    side: str  # LONG / SHORT
    qty: Decimal
    entry_price: Decimal
    current_sl: Optional[Decimal]
    current_tp: Optional[Decimal]
    trail_stage: int
    trail_pending_stage: int
    updated_at: float  # 缓存更新时间


@dataclass
class TrailingTrigger:
    """移动止损触发事件"""
    uid: str
    exchange: str
    symbol: str
    side: str
    target_stage: int
    new_sl: Decimal
    entry_price: Decimal
    mark_price: Decimal
    profit_pct: Decimal
    timestamp: int


@dataclass
class OrderTask:
    """下单任务"""
    uid: str
    exchange: str
    symbol: str
    action: str
    stop_loss: Optional[str] = None
    take_profit: Optional[str] = None
    priority: int = 0  # 0=普通, 1=高优先级
    created_at: float = field(default_factory=time.time)
    retry_count: int = 0


# =============================================================================
# 全局止损处理器
# =============================================================================

class GlobalStopLossProcessor:
    """
    全局止损处理器 - 单例模式
    
    处理所有用户的移动止损逻辑，优化 Redis 访问
    """
    
    _instance: Optional["GlobalStopLossProcessor"] = None
    _lock = threading.Lock()
    
    # 移动止损阶段配置
    TRAIL_STAGES = [
        (Decimal("2.2"), Decimal("0.01")),   # >=2.2% -> 锁盈1%
        (Decimal("3.5"), Decimal("0.02")),   # >=3.5% -> 锁盈2%
        (Decimal("5.5"), Decimal("0.04")),   # +5.5% → 锁4%
        (Decimal("7.5"), Decimal("0.055")),  # +7.5% → 锁5.5%
        (Decimal("10.0"), Decimal("0.08")),  # +10.0% → 锁8.0%
    ]
    
    # 缓存配置
    CACHE_TTL_MS = 500  # 本地缓存 TTL
    BATCH_INTERVAL_MS = 200  # 批量处理间隔
    DEBOUNCE_SECONDS = 10  # 防抖时间
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
        
        # Redis 连接
        from core.database import redis_client
        self.rds = redis_client
        
        # 本地缓存: {uid:exchange:symbol:side -> PositionCache}
        self._position_cache: Dict[str, PositionCache] = {}
        self._cache_lock = threading.RLock()
        self._last_cache_refresh = 0.0
        
        # 活跃用户集合: {uid -> set(exchange)}
        self._active_users: Dict[str, Set[str]] = defaultdict(set)
        
        # 防抖记录: {uid:exchange:symbol:side:stage -> timestamp}
        self._debounce_map: Dict[str, float] = {}
        self._debounce_lock = threading.Lock()  # 防抖记录专用锁
        
        # 下单队列: {exchange -> List[OrderTask]}
        self._order_queues: Dict[str, List[OrderTask]] = defaultdict(list)
        self._queue_lock = threading.Lock()
        
        # 后台任务
        self._executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="slp_")
        self._running = False
        self._order_tasks: List[asyncio.Task] = []  # 保存任务引用，用于优雅关闭
        
        # 交易所连接池（可选，用于直接执行止损更新）
        self._exchange_pool = None
        self._use_pool = False  # 是否使用连接池直接执行（默认仍使用 multi_exchange_trader）
        
        # 统计
        self._stats = {
            "ticks_processed": 0,
            "cache_hits": 0,
            "cache_misses": 0,
            "triggers_fired": 0,
            "orders_queued": 0,
        }
        
        # 防抖清理计数器
        self._debounce_cleanup_counter = 0
        
        # Lua 脚本：原子更新止损数据（避免读写竞争）
        self._update_trigger_script = self.rds.register_script("""
            local user_key = KEYS[1]
            local cycle_field = ARGV[1]
            local pos_field = ARGV[2]
            local field = ARGV[3]
            local target_stage = ARGV[4]
            local new_sl = ARGV[5]
            local timestamp = ARGV[6]
            local mark_price = ARGV[7]
            local profit_pct = ARGV[8]
            
            -- 原子读取并更新 cycle 数据
            local cycle_raw = redis.call('HGET', user_key, cycle_field)
            local cycle_data = {}
            if cycle_raw then
                cycle_data = cjson.decode(cycle_raw)
            end
            
            -- 检查 cycle 是否存在，如果不存在则跳过（仓位可能已平仓）
            local cyc = cycle_data[field]
            if not cyc then
                return 0  -- cycle 不存在，跳过更新
            end
            
            cyc['slTrailPendingStage'] = target_stage
            cyc['slTrailPendingStopLoss'] = new_sl
            cyc['slTrailPendingTs'] = timestamp
            cyc['slTrailPendingMark'] = mark_price
            cyc['slTrailPendingPct'] = profit_pct
            cycle_data[field] = cyc
            
            redis.call('HSET', user_key, cycle_field, cjson.encode(cycle_data))
            
            -- 原子读取并更新 pos 数据
            local pos_raw = redis.call('HGET', user_key, pos_field)
            if pos_raw then
                local pos_data = cjson.decode(pos_raw)
                local pos = pos_data[field]
                if pos then
                    pos['slTrailPendingStage'] = target_stage
                    pos['slTrailPendingStopLoss'] = new_sl
                    pos['slTrailPendingTs'] = timestamp
                    pos['updatedAt'] = timestamp
                    pos_data[field] = pos
                    redis.call('HSET', user_key, pos_field, cjson.encode(pos_data))
                end
            end
            
            return 1
        """)
        
        self._initialized = True
        logger.info("[GlobalSLP] 全局止损处理器已初始化")
    
    # =========================================================================
    # 用户管理
    # =========================================================================
    
    def register_user(self, uid: str, exchange: str):
        """注册活跃用户"""
        self._active_users[uid].add(exchange)
        logger.debug(f"[GlobalSLP] 注册用户: {uid}/{exchange}")
    
    def unregister_user(self, uid: str, exchange: str = None):
        """注销用户"""
        if exchange:
            self._active_users[uid].discard(exchange)
            if not self._active_users[uid]:
                del self._active_users[uid]
        else:
            self._active_users.pop(uid, None)
        
        # 清理缓存
        self._clear_user_cache(uid, exchange)
    
    def _clear_user_cache(self, uid: str, exchange: str = None):
        """清理用户缓存"""
        with self._cache_lock:
            keys_to_remove = []
            prefix = f"{uid}:{exchange}:" if exchange else f"{uid}:"
            for key in self._position_cache:
                if key.startswith(prefix):
                    keys_to_remove.append(key)
            for key in keys_to_remove:
                del self._position_cache[key]
    
    # =========================================================================
    # 缓存管理
    # =========================================================================
    
    def _refresh_cache_batch(self, force: bool = False) -> int:
        """
        批量刷新缓存
        
        优化：使用 Pipeline 一次读取所有用户数据
        
        Returns:
            刷新的用户数
        """
        now = time.time() * 1000
        
        # 检查是否需要刷新
        if not force and (now - self._last_cache_refresh) < self.CACHE_TTL_MS:
            self._stats["cache_hits"] += 1
            return 0
        
        if not self._active_users:
            return 0
        
        try:
            from core.database import RedisKeys
            
            # 构建批量读取的 keys
            # 格式: user:{uid} -> HMGET {exchange}:positions, {exchange}:cycles
            pipe = self.rds.pipeline()
            
            user_exchange_list = []  # [(uid, exchange), ...]
            
            for uid, exchanges in self._active_users.items():
                user_key = RedisKeys.user(uid)
                for exchange in exchanges:
                    pos_field = RedisKeys.exchange_positions(exchange)
                    cycle_field = RedisKeys.exchange_cycles(exchange)
                    pipe.hmget(user_key, pos_field, cycle_field)
                    user_exchange_list.append((uid, exchange))
            
            if not user_exchange_list:
                return 0
            
            # 执行批量读取
            results = pipe.execute()
            
            # 解析结果并更新缓存
            refreshed = 0
            with self._cache_lock:
                for i, (uid, exchange) in enumerate(user_exchange_list):
                    pos_data_raw, cycle_data_raw = results[i]
                    
                    pos_data = json.loads(pos_data_raw) if pos_data_raw else {}
                    cycle_data = json.loads(cycle_data_raw) if cycle_data_raw else {}
                    
                    # 更新缓存
                    self._update_position_cache(uid, exchange, pos_data, cycle_data, now)
                    refreshed += 1
            
            self._last_cache_refresh = now
            self._stats["cache_misses"] += 1
            
            return refreshed
            
        except Exception as e:
            error_msg = str(e).lower()
            if any(kw in error_msg for kw in ["timeout", "connect", "network", "redis"]):
                logger.warning(f"[GlobalSLP] 批量刷新缓存网络错误: {e}")
            else:
                logger.error(f"[GlobalSLP] 批量刷新缓存失败: {e}")
            return 0
    
    def _update_position_cache(
        self,
        uid: str,
        exchange: str,
        pos_data: Dict,
        cycle_data: Dict,
        timestamp: float
    ):
        """更新单个用户的持仓缓存"""
        for key, pos in pos_data.items():
            if not isinstance(pos, dict):
                continue
            
            # 解析 key: "BTCUSDT:LONG"
            parts = key.split(":")
            if len(parts) != 2:
                continue
            
            symbol, side = parts
            side = side.upper()
            
            if side not in ("LONG", "SHORT"):
                continue
            
            qty = D(pos.get("qty", "0"))
            if qty <= 0:
                continue
            
            # 获取 cycle 数据
            cyc = cycle_data.get(key, {})
            
            entry = D(pos.get("entryPrice", "0"))
            if entry <= 0:
                entry = D(cyc.get("avgOpenPrice", "0"))
            
            if entry <= 0:
                continue
            
            cache_key = f"{uid}:{exchange}:{symbol}:{side}"
            
            self._position_cache[cache_key] = PositionCache(
                uid=uid,
                exchange=exchange,
                symbol=symbol,
                side=side,
                qty=qty,
                entry_price=entry,
                current_sl=self._parse_decimal(pos.get("stopLossPrice")),
                current_tp=self._parse_decimal(pos.get("takeProfitPrice")),
                trail_stage=int(cyc.get("slTrailStage", "0") or "0"),
                trail_pending_stage=int(cyc.get("slTrailPendingStage", "0") or "0"),
                updated_at=timestamp
            )
    
    def _parse_decimal(self, value) -> Optional[Decimal]:
        """安全解析 Decimal"""
        if not value or value == "0" or value == "":
            return None
        try:
            return D(str(value))
        except (ValueError, TypeError, InvalidOperation):
            return None
    
    # =========================================================================
    # 止损计算
    # =========================================================================
    
    def _profit_pct(self, side: str, entry: Decimal, mark: Decimal) -> Decimal:
        """计算盈利百分比"""
        if entry <= 0 or mark <= 0:
            return D("0")
        if side == "LONG":
            return (mark - entry) / entry * Decimal("100")
        return (entry - mark) / entry * Decimal("100")
    
    def _trail_stage_for_pct(self, pct: Decimal) -> int:
        """根据盈利百分比确定移动止损阶段"""
        for i in range(len(self.TRAIL_STAGES) - 1, -1, -1):
            trigger_pct = self.TRAIL_STAGES[i][0]
            if pct >= trigger_pct:
                return i + 1
        return 0
    
    def _trail_stop_price(self, side: str, entry: Decimal, stage: int) -> Decimal:
        """计算移动止损价格"""
        if entry <= 0 or stage <= 0:
            return D("0")
        lock = self.TRAIL_STAGES[stage - 1][1]
        if side == "LONG":
            return entry * (Decimal("1") + lock)
        return entry * (Decimal("1") - lock)
    
    # =========================================================================
    # 核心处理逻辑
    # =========================================================================
    
    def on_mark_tick_batch(
        self,
        prices: Dict[str, Decimal],  # {symbol -> mark_price}
        timestamp: int
    ) -> List[TrailingTrigger]:
        """
        批量处理 Mark 价格更新
        
        优化：一次处理所有币种的所有用户
        
        Args:
            prices: 币种价格字典
            timestamp: 时间戳
            
        Returns:
            触发的移动止损列表
        """
        self._stats["ticks_processed"] += 1
        
        # 刷新缓存（如果需要）
        self._refresh_cache_batch()
        
        triggers: List[TrailingTrigger] = []
        now = time.time()
        
        with self._cache_lock:
            for cache_key, pos_cache in self._position_cache.items():
                symbol = pos_cache.symbol
                
                # 检查是否有该币种的价格
                mark = prices.get(symbol)
                if mark is None:
                    continue
                
                # 计算盈利和目标阶段
                pct = self._profit_pct(pos_cache.side, pos_cache.entry_price, mark)
                target_stage = self._trail_stage_for_pct(pct)
                
                if target_stage <= 0:
                    continue
                
                # 检查是否需要升级
                effective_stage = max(pos_cache.trail_stage, pos_cache.trail_pending_stage)
                if target_stage <= effective_stage:
                    continue
                
                # 计算新止损价
                new_sl = self._trail_stop_price(pos_cache.side, pos_cache.entry_price, target_stage)
                if new_sl <= 0:
                    continue
                
                # 防止立刻触发
                eps = pos_cache.entry_price * Decimal("0.00005")
                if pos_cache.side == "LONG" and new_sl >= (mark - eps):
                    continue
                if pos_cache.side == "SHORT" and new_sl <= (mark + eps):
                    continue
                
                # 检查是否比当前止损更优
                if pos_cache.current_sl is not None and pos_cache.current_sl > 0:
                    if pos_cache.side == "LONG" and new_sl <= pos_cache.current_sl:
                        continue
                    if pos_cache.side == "SHORT" and new_sl >= pos_cache.current_sl:
                        continue
                
                # 防抖检查
                debounce_key = f"{cache_key}:{target_stage}"
                with self._debounce_lock:
                    last_trigger = self._debounce_map.get(debounce_key, 0)
                    if now - last_trigger < self.DEBOUNCE_SECONDS:
                        continue
                    # 记录触发
                    self._debounce_map[debounce_key] = now
                
                triggers.append(TrailingTrigger(
                    uid=pos_cache.uid,
                    exchange=pos_cache.exchange,
                    symbol=symbol,
                    side=pos_cache.side,
                    target_stage=target_stage,
                    new_sl=new_sl,
                    entry_price=pos_cache.entry_price,
                    mark_price=mark,
                    profit_pct=pct,
                    timestamp=timestamp
                ))
                
                # 更新本地缓存的 pending stage
                pos_cache.trail_pending_stage = target_stage
        
        self._stats["triggers_fired"] += len(triggers)
        
        # 处理触发的止损
        if triggers:
            self._process_triggers(triggers, timestamp)
        
        return triggers
    
    def on_mark_tick(
        self,
        symbol: str,
        mark: Decimal,
        timestamp: int,
        exchange: str = None
    ):
        """
        单币种 tick 处理（兼容旧接口）
        
        内部转换为批量处理
        """
        self.on_mark_tick_batch({symbol: mark}, timestamp)
    
    def _process_triggers(self, triggers: List[TrailingTrigger], timestamp: int):
        """处理触发的移动止损"""
        for trigger in triggers:
            try:
                # 更新 Redis（批量）
                self._update_redis_for_trigger(trigger, timestamp)
                
                # 加入下单队列
                self._enqueue_order(OrderTask(
                    uid=trigger.uid,
                    exchange=trigger.exchange,
                    symbol=trigger.symbol,
                    action="update_stop_loss",
                    stop_loss=d_to_str(trigger.new_sl),
                    priority=0
                ))
                
                logger.info(
                    f"[GlobalSLP] 触发移动止损: {trigger.uid}/{trigger.exchange} "
                    f"{trigger.symbol} {trigger.side} stage={trigger.target_stage} "
                    f"entry={d_to_str(trigger.entry_price)} mark={d_to_str(trigger.mark_price)} "
                    f"new_sl={d_to_str(trigger.new_sl)}"
                )
                
            except Exception as e:
                error_msg = str(e).lower()
                if any(kw in error_msg for kw in ["timeout", "connect", "network"]):
                    logger.debug(f"[GlobalSLP] 处理触发网络错误: {trigger} - {e}")
                else:
                    logger.error(f"[GlobalSLP] 处理触发失败: {trigger} - {e}")
    
    def _update_redis_for_trigger(self, trigger: TrailingTrigger, timestamp: int):
        """
        更新 Redis 中的止损数据
        
        使用 Lua 脚本实现原子性读-改-写，避免并发竞争
        """
        from core.database import RedisKeys
        
        user_key = RedisKeys.user(trigger.uid)
        field = f"{trigger.symbol}:{trigger.side}"
        cycle_field = RedisKeys.exchange_cycles(trigger.exchange)
        pos_field = RedisKeys.exchange_positions(trigger.exchange)
        
        # 使用 Lua 脚本原子更新
        self._update_trigger_script(
            keys=[user_key],
            args=[
                cycle_field,
                pos_field,
                field,
                str(trigger.target_stage),
                d_to_str(trigger.new_sl),
                str(timestamp),
                d_to_str(trigger.mark_price),
                d_to_str(trigger.profit_pct),
            ]
        )
    
    # =========================================================================
    # 下单队列
    # =========================================================================
    
    def _enqueue_order(self, task: OrderTask):
        """将订单加入队列"""
        with self._queue_lock:
            self._order_queues[task.exchange].append(task)
            self._stats["orders_queued"] += 1
    
    def _cleanup_stale_cache(self, uid: str, exchange: str, symbol: str):
        """
        清理指定用户/交易所/币种的本地缓存
        
        当交易所确认没有持仓时调用，避免反复触发无效止损更新
        """
        removed = []
        with self._cache_lock:
            for side in ("LONG", "SHORT"):
                cache_key = f"{uid}:{exchange}:{symbol}:{side}"
                if cache_key in self._position_cache:
                    del self._position_cache[cache_key]
                    removed.append(cache_key)
        
        if removed:
            logger.info(f"[GlobalSLP] 已清理无效缓存: {uid}/{exchange} {symbol} ({len(removed)} 条)")
    
    def _cleanup_stale_cache_from_result(self, task: OrderTask, result):
        """
        根据订单执行结果清理无效缓存
        
        当返回"没有持仓"错误时，说明 Redis 持仓数据与交易所不同步，
        需要清理本地缓存避免反复触发。
        """
        for ex_name, order_result in result.results.items():
            if order_result.error and "没有持仓" in order_result.error:
                self._cleanup_stale_cache(task.uid, ex_name, task.symbol)
    
    async def _process_order_queue(self, exchange: str):
        """
        处理指定交易所的订单队列
        
        限速控制：
        - Binance: 10 orders/s
        - OKX: 20 orders/s
        - Bitget: 10 orders/s
        """
        rate_limits = {
            "binance": 10,
            "okx": 20,
            "bitget": 10,
            "hyperliquid": 10,
        }
        
        rate_limit = rate_limits.get(exchange, 10)
        interval = 1.0 / rate_limit
        
        while self._running:
            task = None
            
            with self._queue_lock:
                if self._order_queues[exchange]:
                    task = self._order_queues[exchange].pop(0)
            
            if task:
                try:
                    await self._execute_order(task)
                except Exception as e:
                    logger.error(f"[GlobalSLP] 执行订单失败: {task} - {e}")
                    
                    # 重试逻辑
                    if task.retry_count < 3:
                        task.retry_count += 1
                        with self._queue_lock:
                            self._order_queues[exchange].append(task)
                
                await asyncio.sleep(interval)
            else:
                await asyncio.sleep(0.1)
    
    async def _execute_order(self, task: OrderTask):
        """
        执行单个订单
        
        支持两种模式：
        1. 使用 multi_exchange_trader（默认，兼容性好）
        2. 使用 exchange_pool 直接执行（可选，性能更好）
        """
        if self._use_pool and self._exchange_pool:
            await self._execute_order_with_pool(task)
        else:
            await self._execute_order_with_trader(task)
    
    async def _execute_order_with_trader(self, task: OrderTask):
        """使用 multi_exchange_trader 执行订单（默认方式）"""
        from trading.multi_exchange_trader import execute_multi_exchange_trade
        
        result = await execute_multi_exchange_trade(
            uid=task.uid,
            symbol=task.symbol,
            action=task.action,
            stop_loss=task.stop_loss,
            take_profit=task.take_profit,
            exchange=task.exchange
        )
        
        if result.failure_count > 0:
            logger.warning(f"[GlobalSLP] 订单执行部分失败: {result.to_dict()}")
            
            # 如果是"没有持仓"，清理本地缓存，避免反复触发无效止损更新
            self._cleanup_stale_cache_from_result(task, result)
    
    async def _execute_order_with_pool(self, task: OrderTask):
        """
        使用 exchange_pool 直接执行订单
        
        优点：
        - 连接复用，减少连接创建开销
        - 统一限速控制
        - 更好的资源管理
        """
        try:
            # 获取用户的交易所配置
            config = config_loader.get_user_exchange_config(task.uid, task.exchange)
            if not config or not config.get("api_key"):
                logger.warning(f"[GlobalSLP] 用户 {task.uid} 没有 {task.exchange} 配置")
                return
            
            # 使用连接池获取连接
            async with self._exchange_pool.connection_context(
                exchange_name=task.exchange,
                api_key=config["api_key"],
                api_secret=config.get("api_secret", ""),
                passphrase=config.get("passphrase"),
                is_testnet=config.get("is_testnet", False),
                wallet_address=config.get("wallet_address"),
            ) as exchange:
                # 执行止损更新
                if task.action == "update_stop_loss" and task.stop_loss:
                    # 获取持仓
                    positions = await exchange.get_positions()
                    pos = None
                    for p in positions:
                        if p.symbol.upper() == task.symbol.upper():
                            pos = p
                            break
                    
                    if not pos:
                        logger.warning(f"[GlobalSLP] {task.uid}/{task.exchange} 没有 {task.symbol} 持仓")
                        self._cleanup_stale_cache(task.uid, task.exchange, task.symbol)
                        return
                    
                    # 设置止损
                    result = await exchange.set_stop_loss(
                        task.symbol, pos.side, pos.qty, float(task.stop_loss)
                    )
                    
                    if result.success:
                        # 同步到 Redis
                        from trading.utils import sync_sl_tp_to_position
                        sync_sl_tp_to_position(
                            symbol=task.symbol,
                            position_side=pos.side,
                            sl=float(task.stop_loss),
                            tp=None,
                            uid=task.uid,
                            exchange=task.exchange
                        )
                        logger.info(f"[GlobalSLP] 止损更新成功: {task.uid}/{task.exchange} {task.symbol} SL={task.stop_loss}")
                    else:
                        logger.warning(f"[GlobalSLP] 止损更新失败: {task.uid}/{task.exchange} {task.symbol} - {result.error}")
                else:
                    # 其他动作仍使用 trader
                    await self._execute_order_with_trader(task)
                    
        except Exception as e:
            logger.error(f"[GlobalSLP] 使用连接池执行订单失败: {task} - {e}")
            # 回退到 trader 方式
            await self._execute_order_with_trader(task)
    
    # =========================================================================
    # 生命周期
    # =========================================================================
    
    async def start(self, use_pool: bool = False):
        """
        启动处理器
        
        Args:
            use_pool: 是否使用交易所连接池直接执行订单（默认 False，使用 multi_exchange_trader）
        """
        if self._running:
            return
        
        self._running = True
        self._use_pool = use_pool
        
        # 如果启用连接池，初始化它
        if use_pool:
            try:
                from trading.exchange_pool import get_exchange_pool
                self._exchange_pool = get_exchange_pool()
                await self._exchange_pool.start()
                logger.info("[GlobalSLP] 交易所连接池已启动")
            except Exception as e:
                logger.warning(f"[GlobalSLP] 连接池启动失败，回退到 trader 模式: {e}")
                self._use_pool = False
                self._exchange_pool = None
        
        # 启动各交易所的订单处理协程，保存任务引用
        exchanges = ["binance", "okx", "bitget", "hyperliquid"]
        self._order_tasks = []
        for exchange in exchanges:
            task = asyncio.create_task(self._process_order_queue(exchange))
            self._order_tasks.append(task)
        
        # 启动防抖清理任务
        self._cleanup_task = asyncio.create_task(self._cleanup_debounce_map())
        
        logger.info(f"[GlobalSLP] 全局止损处理器已启动 (use_pool={self._use_pool})")
    
    async def _cleanup_debounce_map(self):
        """定期清理过期的防抖记录，防止内存泄漏"""
        while self._running:
            try:
                await asyncio.sleep(60)  # 每分钟清理一次
                
                now = time.time()
                expired_keys = []
                
                with self._debounce_lock:
                    for key, ts in self._debounce_map.items():
                        if now - ts > self.DEBOUNCE_SECONDS * 2:  # 过期时间的2倍
                            expired_keys.append(key)
                    
                    for key in expired_keys:
                        del self._debounce_map[key]
                
                if expired_keys:
                    logger.debug(f"[GlobalSLP] 清理 {len(expired_keys)} 条过期防抖记录")
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[GlobalSLP] 防抖清理失败: {e}")
    
    async def stop(self):
        """停止处理器"""
        self._running = False
        
        # 取消所有订单处理任务
        for task in self._order_tasks:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        self._order_tasks.clear()
        
        # 取消清理任务
        if hasattr(self, '_cleanup_task') and self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass
        
        # 等待队列清空
        for _ in range(50):  # 最多等待 5 秒
            total_pending = sum(len(q) for q in self._order_queues.values())
            if total_pending == 0:
                break
            await asyncio.sleep(0.1)
        
        # 停止连接池
        if self._exchange_pool:
            try:
                await self._exchange_pool.stop()
                logger.info("[GlobalSLP] 交易所连接池已停止")
            except Exception as e:
                logger.warning(f"[GlobalSLP] 连接池停止失败: {e}")
            self._exchange_pool = None
        
        # 关闭线程池
        if self._executor:
            self._executor.shutdown(wait=False)
            self._executor = None
            logger.info("[GlobalSLP] 线程池已关闭")
        
        logger.info(f"[GlobalSLP] 全局止损处理器已停止，统计: {self._stats}")
    
    def get_stats(self) -> Dict:
        """获取统计信息"""
        stats = {
            **self._stats,
            "active_users": len(self._active_users),
            "cached_positions": len(self._position_cache),
            "pending_orders": sum(len(q) for q in self._order_queues.values()),
            "use_pool": self._use_pool,
        }
        
        # 添加连接池统计
        if self._exchange_pool:
            stats["pool_stats"] = self._exchange_pool.get_stats()
        
        return stats


# =============================================================================
# 全局实例
# =============================================================================

def get_global_stop_loss_processor() -> GlobalStopLossProcessor:
    """获取全局止损处理器实例"""
    return GlobalStopLossProcessor()
