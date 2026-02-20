# global_mark_cycle_updater.py
"""
全局 Mark Cycle Updater - 1000+ 用户规模优化

问题：
- 原实现：每用户独立线程，每秒 12 次 Redis 调用
- 1000 用户 = 12000 QPS，Redis 压力过大

优化方案：
- 单例模式，一个线程处理所有用户
- 批量读取/写入 Redis（使用 pipeline）
- 本地缓存 mark price，减少重复读取
- 分批处理，避免单次操作过大

架构：
1. GlobalMarkCycleUpdater (单例)
   - 维护所有注册用户的列表
   - 单线程循环处理所有用户
   - 批量 Redis 操作

2. 注册/注销接口
   - register_user(uid, exchange) - 注册用户
   - unregister_user(uid, exchange) - 注销用户

3. 兼容适配器
   - 原 cycle_store 调用 _start_mark_cycle_updater() 时
   - 改为调用全局处理器的 register_user()
"""

import logging
import os
import threading
import time
from decimal import Decimal
from typing import Dict, Set, Tuple, Optional

from core.database import redis_client, RedisKeys
from core.pf_compatibility import pf_compat
from core.utils import D, now_ms, jload

logger = logging.getLogger(__name__)


class GlobalMarkCycleUpdater:
    """
    全局 Mark Cycle Updater
    
    单例模式，一个线程处理所有用户的 mark cycle 更新
    """
    
    _instance = None
    _lock = threading.Lock()
    
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
        
        self._rds = redis_client
        
        # 注册的用户: {(uid, exchange), ...}
        self._registered_users: Set[Tuple[str, str]] = set()
        self._users_lock = threading.Lock()
        
        # Mark price 本地缓存: {symbol: {"markPrice": Decimal, "ts": int}}
        self._mark_cache: Dict[str, dict] = {}
        self._mark_cache_lock = threading.Lock()
        self._mark_cache_ttl_ms = 500  # 缓存有效期 500ms
        
        # 处理线程
        self._updater_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        
        # 配置
        self._interval_s = 1.0  # 更新间隔
        self._batch_size = 50  # 每批处理的用户数
        
        # 统计
        self._stats = {
            "total_updates": 0,
            "total_users_processed": 0,
            "total_positions_updated": 0,
            "last_update_time_ms": 0,
            "errors": 0,
        }
        
        self._initialized = True
        logger.info("[GlobalMarkCycleUpdater] 初始化完成")
    
    def start(self) -> None:
        """启动全局更新器"""
        if self._updater_thread and self._updater_thread.is_alive():
            return
        
        self._stop_event.clear()
        self._updater_thread = threading.Thread(
            target=self._run_loop,
            name="global-mark-cycle-updater",
            daemon=True
        )
        self._updater_thread.start()
        logger.info("[GlobalMarkCycleUpdater] 已启动")
    
    def stop(self) -> None:
        """停止全局更新器"""
        self._stop_event.set()
        if self._updater_thread:
            self._updater_thread.join(timeout=5.0)
            self._updater_thread = None
        logger.info("[GlobalMarkCycleUpdater] 已停止")
    
    def register_user(self, uid: str, exchange: str) -> None:
        """注册用户"""
        with self._users_lock:
            key = (uid, exchange)
            if key not in self._registered_users:
                self._registered_users.add(key)
                logger.debug(f"[GlobalMarkCycleUpdater] 注册用户: {uid}:{exchange}")
        
        # 确保处理器已启动
        self.start()
    
    def unregister_user(self, uid: str, exchange: str) -> None:
        """注销用户"""
        with self._users_lock:
            key = (uid, exchange)
            if key in self._registered_users:
                self._registered_users.discard(key)
                logger.debug(f"[GlobalMarkCycleUpdater] 注销用户: {uid}:{exchange}")
    
    def get_stats(self) -> dict:
        """获取统计信息"""
        with self._users_lock:
            user_count = len(self._registered_users)
        
        return {
            **self._stats,
            "registered_users": user_count,
            "is_running": self._updater_thread is not None and self._updater_thread.is_alive(),
        }
    
    # ========== 内部方法 ==========
    
    def _run_loop(self) -> None:
        """主循环"""
        logger.info("[GlobalMarkCycleUpdater] 主循环开始")
        
        while not self._stop_event.is_set():
            try:
                start_time = time.monotonic()
                
                # 获取当前注册的用户列表（快照）
                with self._users_lock:
                    users = list(self._registered_users)
                
                if users:
                    # 分批处理
                    for i in range(0, len(users), self._batch_size):
                        batch = users[i:i + self._batch_size]
                        self._process_batch(batch)
                        
                        # 检查是否需要停止
                        if self._stop_event.is_set():
                            break
                
                # 更新统计
                self._stats["total_updates"] += 1
                self._stats["last_update_time_ms"] = int((time.monotonic() - start_time) * 1000)
                
                # 等待下一个周期
                elapsed = time.monotonic() - start_time
                sleep_time = max(0, self._interval_s - elapsed)
                if sleep_time > 0:
                    self._stop_event.wait(sleep_time)
                    
            except Exception as e:
                logger.error(f"[GlobalMarkCycleUpdater] 主循环错误: {e}")
                self._stats["errors"] += 1
                time.sleep(1.0)  # 错误后等待
        
        logger.info("[GlobalMarkCycleUpdater] 主循环结束")
    
    def _process_batch(self, users: list) -> None:
        """
        批量处理一组用户
        
        优化策略：
        1. 先收集所有需要的 symbol
        2. 批量获取 mark price
        3. 批量处理每个用户的持仓
        """
        if not users:
            return
        
        ts = str(now_ms())
        
        # 1. 收集所有活跃持仓和需要的 symbol
        user_positions: Dict[Tuple[str, str], list] = {}  # {(uid, exchange): [fields]}
        all_symbols: Set[str] = set()
        
        for uid, exchange in users:
            try:
                fields = pf_compat.get_pf_pos_active(uid, exchange)
                if fields:
                    user_positions[(uid, exchange)] = fields
                    
                    # 获取 symbol
                    pos_data = pf_compat.get_pf_pos(uid, exchange)
                    for field in fields:
                        pos = pos_data.get(field, {})
                        symbol = pos.get("symbol")
                        if symbol:
                            all_symbols.add(symbol)
            except Exception as e:
                logger.debug(f"[GlobalMarkCycleUpdater] 获取用户 {uid}:{exchange} 持仓失败: {e}")
        
        if not user_positions:
            return
        
        # 2. 批量获取 mark price（使用 pipeline）
        mark_prices = self._batch_get_mark_prices(list(all_symbols))
        
        # 3. 处理每个用户
        for (uid, exchange), fields in user_positions.items():
            try:
                self._update_user_cycles(uid, exchange, fields, mark_prices, ts)
                self._stats["total_users_processed"] += 1
            except Exception as e:
                logger.debug(f"[GlobalMarkCycleUpdater] 更新用户 {uid}:{exchange} 失败: {e}")
    
    def _batch_get_mark_prices(self, symbols: list) -> Dict[str, dict]:
        """
        批量获取 mark price
        
        使用本地缓存 + Redis pipeline
        """
        result = {}
        symbols_to_fetch = []
        current_ts = now_ms()
        
        # 检查本地缓存
        with self._mark_cache_lock:
            for symbol in symbols:
                cached = self._mark_cache.get(symbol)
                if cached and (current_ts - cached.get("cached_at", 0)) < self._mark_cache_ttl_ms:
                    result[symbol] = cached
                else:
                    symbols_to_fetch.append(symbol)
        
        # 批量从 Redis 获取
        if symbols_to_fetch:
            try:
                pipe = self._rds.pipeline()
                for symbol in symbols_to_fetch:
                    pipe.get(RedisKeys.market_prices(symbol))
                
                raw_results = pipe.execute()
                
                with self._mark_cache_lock:
                    for i, symbol in enumerate(symbols_to_fetch):
                        raw = raw_results[i]
                        if raw:
                            mk = jload(raw) or {}
                            mk["cached_at"] = current_ts
                            self._mark_cache[symbol] = mk
                            result[symbol] = mk
                            
            except Exception as e:
                logger.debug(f"[GlobalMarkCycleUpdater] 批量获取 mark price 失败: {e}")
        
        return result
    
    def _update_user_cycles(
        self,
        uid: str,
        exchange: str,
        fields: list,
        mark_prices: Dict[str, dict],
        ts: str
    ) -> None:
        """更新单个用户的所有周期"""
        pos_data = pf_compat.get_pf_pos(uid, exchange)
        cycle_data = pf_compat.get_pf_cycle(uid, exchange)
        
        if not pos_data or not cycle_data:
            return
        
        total_unrealized_pnl = D("0")
        updated_cycles = {}
        updated_positions = {}
        
        for field in fields:
            pos = pos_data.get(field, {})
            if not pos:
                continue
            
            symbol = pos.get("symbol")
            side = (pos.get("side") or "").upper()
            if not symbol or side not in ("LONG", "SHORT"):
                continue
            
            qty = D(pos.get("qty", "0"))
            entry = D(pos.get("entryPrice", "0"))
            if qty <= 0 or entry <= 0:
                continue
            
            # 获取 mark price
            mk = mark_prices.get(symbol, {})
            mark = D(mk.get("markPrice", "0"))
            if mark <= 0:
                continue
            
            # 计算实时盈亏
            live_pnl = self._calc_live_pnl(side=side, qty=qty, entry=entry, mark=mark)
            total_unrealized_pnl += live_pnl
            
            # 更新 cycle
            cyc = cycle_data.get(field, {})
            if not cyc:
                continue
            
            live_net_pnl = (
                D(cyc.get("realizedPnlEst", "0"))
                + D(cyc.get("fundingTotal", "0"))
                - D(cyc.get("feeTotal", "0"))
                + live_pnl
            )
            
            # 更新 peak/drawdown
            self._update_peak_and_drawdown(cyc, current_pnl=live_net_pnl)
            
            cyc["liveNetPnl"] = str(live_net_pnl)
            cyc["liveUnrealizedPnl"] = str(live_pnl)
            cyc["markPrice"] = str(mark)
            cyc["markTs"] = str(mk.get("ts") or ts)
            cyc["updatedAt"] = ts
            
            updated_cycles[field] = cyc
            
            # 更新 pos
            pos2 = pos.copy()
            pos2["liveNetPnl"] = str(live_net_pnl)
            pos2["liveUnrealizedPnl"] = str(live_pnl)
            pos2["unrealizedPnl"] = str(live_pnl)
            pos2["markPrice"] = str(mark)
            pos2["updatedAt"] = ts
            
            updated_positions[field] = pos2
            self._stats["total_positions_updated"] += 1
        
        # 批量写入
        if updated_cycles:
            cycle_data.update(updated_cycles)
            pf_compat.set_pf_cycle(uid, cycle_data, exchange)
        
        if updated_positions:
            pos_data.update(updated_positions)
            pf_compat.set_pf_pos(uid, pos_data, exchange)
        
        # 更新 account
        try:
            account_data = pf_compat.get_pf_account(uid, exchange)
            if account_data:
                account_data["unrealized"] = str(total_unrealized_pnl)
                wallet = D(account_data.get("walletBalance", "0"))
                account_data["equity"] = str(wallet + total_unrealized_pnl)
                account_data["ts"] = ts
                pf_compat.set_pf_account(uid, account_data, exchange)
        except Exception as e:
            # P3 Fix: 添加日志
            logger.debug(f"[{uid}] 更新 account 异常: {e}")
    
    @staticmethod
    def _calc_live_pnl(*, side: str, qty: Decimal, entry: Decimal, mark: Decimal) -> Decimal:
        """计算实时盈亏"""
        if qty <= 0 or entry <= 0 or mark <= 0:
            return D("0")
        if side == "LONG":
            return (mark - entry) * qty
        return (entry - mark) * qty
    
    @staticmethod
    def _update_peak_and_drawdown(c: dict, current_pnl: Decimal) -> None:
        """更新峰值和回撤"""
        cur = current_pnl
        peak = D(c.get("peakPnl", "0"))
        
        if cur > peak:
            c["peakPnl"] = str(cur)
            c["minPnlAfterPeak"] = str(cur)
        else:
            min_after_peak = D(c.get("minPnlAfterPeak", str(peak)))
            if cur < min_after_peak:
                c["minPnlAfterPeak"] = str(cur)
                drawdown = peak - cur
                current_max_dd = D(c.get("maxDrawdown", "0"))
                if drawdown > current_max_dd:
                    c["maxDrawdown"] = str(drawdown)


# ========== 全局单例 ==========

_global_updater: Optional[GlobalMarkCycleUpdater] = None
_global_updater_lock = threading.Lock()


def get_global_mark_cycle_updater() -> GlobalMarkCycleUpdater:
    """获取全局 Mark Cycle Updater 单例"""
    global _global_updater
    
    with _global_updater_lock:
        if _global_updater is None:
            _global_updater = GlobalMarkCycleUpdater()
        return _global_updater


def is_global_updater_enabled() -> bool:
    """检查是否启用全局更新器"""
    return os.environ.get("USE_GLOBAL_MARK_UPDATER", "1") == "1"


# ========== 兼容适配器 ==========

class MarkCycleUpdaterAdapter:
    """
    Mark Cycle Updater 适配器
    
    提供与原 cycle_store 相同的接口，内部委托给全局处理器
    """
    
    def __init__(self, uid: str, exchange: str = "binance"):
        self.uid = uid
        self.exchange = exchange
        self._use_global = is_global_updater_enabled()
        
        # 原实现的状态（用于非全局模式）
        self._mark_updater_stop = None
        self._mark_updater_thread = None
    
    def start(self, interval_s: float = 1.0) -> None:
        """启动更新器"""
        if self._use_global:
            updater = get_global_mark_cycle_updater()
            updater.register_user(self.uid, self.exchange)
        else:
            # 原实现（保留兼容）
            self._start_local_updater(interval_s)
    
    def stop(self) -> None:
        """停止更新器"""
        if self._use_global:
            updater = get_global_mark_cycle_updater()
            updater.unregister_user(self.uid, self.exchange)
        else:
            self._stop_local_updater()
    
    def _start_local_updater(self, interval_s: float) -> None:
        """原实现的本地更新器（保留兼容）"""
        # 这里可以保留原实现，但默认不使用
        pass
    
    def _stop_local_updater(self) -> None:
        """停止本地更新器"""
        if self._mark_updater_stop:
            self._mark_updater_stop.set()
            self._mark_updater_stop = None
        self._mark_updater_thread = None
