# cycle_store.py - WebSocket 周期数据存储
"""
WS-only 仓位/周期数据存储

职责:
1. 接收 User Data Stream (ACCOUNT_UPDATE / ORDER_TRADE_UPDATE)
2. 维护 Redis 中的仓位 (pf:pos) 和周期 (pf:cycle) 数据
3. 接收 Mark Price Stream，更新展示数据

止盈止损逻辑已分离到 stop_loss_manager.py
"""

from __future__ import annotations

import json
import logging
import threading
import time
import uuid
from decimal import Decimal
from typing import Callable, Optional
from websocket.user_stream import FuturesUserWS
from websocket.price_stream import MarkPriceWS
# 使用适配器，支持全局处理器优化（1000+用户规模）
from trading.stop_loss_adapter import StopLossManagerAdapter as StopLossManager
from core.pf_compatibility import pf_compat
from core.utils import D, now_ms, jload, pos_field

logger = logging.getLogger(__name__)


class BinanceUMHedgeWSOnlyStore:
    """
    WS-only: 用 User Data Stream 驱动仓位/成交/周期聚合写入 Redis

    事件处理:
    - ACCOUNT_UPDATE -> pf:pos + cycle open/close
    - ORDER_TRADE_UPDATE(x=TRADE) -> cycle qty/quote/fee/realized
    - MarkPriceWS -> pf:mark:{symbol} + 展示数据更新

    止损管理:
    - 委托给 StopLossManager 处理
    
    多交易所支持:
    - exchange 参数指定交易所名称（默认 "binance"）
    - 数据写入交易所特定的 Redis 字段
    """

    # 交易所标识
    EXCHANGE_NAME = "binance"

    def __init__(
        self,
        client_sync,
        redis_conn,
        uid: str,
        *,
        closed_maxlen: int = 20000,
        stop_manager: Optional[StopLossManager] = None,
        user_context: Optional['UserContext'] = None,
        exchange: str = "binance",
        on_auth_failed: Optional[Callable[[str], None]] = None,
    ):
        self.client = client_sync
        self.rds = redis_conn
        self.uid = uid
        self.closed_maxlen = closed_maxlen
        self._user_context = user_context
        self.exchange = exchange  # 交易所标识
        
        # 认证失败回调（用于自动停止交易所）
        self._on_auth_failed = on_auth_failed

        # 止损管理器（可外部注入，也可自动创建）
        if stop_manager is not None:
            self.stop_manager = stop_manager
        else:
            # StopLossManager 现在会根据是否有 user_context 自动选择交易函数
            self.stop_manager = StopLossManager(
                redis_conn=redis_conn,
                uid=uid,
                execute_trade_func=None,  # 多用户模式下不需要
                user_context=user_context,
            )
        
        # 设置交易所（用于全局处理器注册）
        self.stop_manager.set_exchange(self.exchange)

        self._ws: Optional[FuturesUserWS] = None
        self._mp: Optional[MarkPriceWS] = None

        self._mark_updater_stop = None
        self._mark_updater_thread = None
        
        # 连接状态跟踪
        self._is_connected = False
        
        # H4: backfill 锁 — 防止并发 backfill 写入同一周期
        self._backfill_lock = threading.Lock()

    # ========== 工具方法 ==========

    def _d_to_str(self, x: Decimal) -> str:
        s = format(x, "f")
        if "." in s:
            s = s.rstrip("0").rstrip(".")
        return s or "0"

    def _calc_live_pnl(self, *, side: str, qty: Decimal, entry: Decimal, mark: Decimal) -> Decimal:
        if qty <= 0 or entry <= 0 or mark <= 0:
            return D("0")
        if (side or "").upper() == "LONG":
            return (mark - entry) * qty
        return (entry - mark) * qty

    @staticmethod
    def _is_open_trade(position_side: str, trade_side: str) -> bool:
        if position_side == "LONG":
            return trade_side == "BUY"
        if position_side == "SHORT":
            return trade_side == "SELL"
        return False

    def _new_cycle_id(self, symbol: str, side: str) -> str:
        return f"{symbol}:{side}:{int(time.time())}:{uuid.uuid4().hex[:8]}"

    def _update_peak_and_drawdown(self, c: dict, current_pnl=None):
        """
        更新峰值收益和最大回撤
        
        - peakPnl: 持仓期间的最高净收益
        - minPnlAfterPeak: 峰值后的最低净收益（用于计算回撤）
        - maxDrawdown: 从峰值回落的最大幅度
        """
        cur = current_pnl if current_pnl is not None else D(c.get("netPnl", "0"))
        peak = D(c.get("peakPnl", "0"))
        
        # 更新峰值
        if cur > peak:
            c["peakPnl"] = str(cur)
            # 新峰值时，重置峰值后最低点
            c["minPnlAfterPeak"] = str(cur)
        else:
            # 非峰值时，更新峰值后的最低点
            min_after_peak = D(c.get("minPnlAfterPeak", str(peak)))
            if cur < min_after_peak:
                c["minPnlAfterPeak"] = str(cur)
                # 更新最大回撤 = 峰值 - 峰值后最低点
                drawdown = peak - cur
                current_max_dd = D(c.get("maxDrawdown", "0"))
                if drawdown > current_max_dd:
                    c["maxDrawdown"] = str(drawdown)

    # ========== Mark Cycle Updater (展示层) ==========

    def _start_mark_cycle_updater(self, interval_s: float = 1.0) -> None:
        """
        仅用于展示/统计更新（不做 trailing 下单）：
        - 用 pf:mark:{symbol} 更新 cycle 的 liveNetPnl / peakPnl / markPrice / markTs 等
        - 同步一部分字段到 pos（供前端展示）
        
        1000+ 用户优化：
        - 默认使用全局 GlobalMarkCycleUpdater（单线程批量处理所有用户）
        - 设置 USE_GLOBAL_MARK_UPDATER=0 可回退到原实现
        """
        import os
        
        # 检查是否使用全局更新器（默认启用）
        if os.environ.get("USE_GLOBAL_MARK_UPDATER", "1") == "1":
            try:
                from trading.global_mark_cycle_updater import get_global_mark_cycle_updater
                updater = get_global_mark_cycle_updater()
                updater.register_user(self.uid, self.exchange)
                logger.debug(f"[{self.uid}][{self.exchange}] 已注册到全局 MarkCycleUpdater")
                return
            except Exception as e:
                logger.warning(f"[{self.uid}] 全局 MarkCycleUpdater 注册失败，回退到本地模式: {e}")
        
        # 原实现（本地线程模式）
        self._start_mark_cycle_updater_local(interval_s)

    def _start_mark_cycle_updater_local(self, interval_s: float = 1.0) -> None:
        """原实现：本地线程模式（保留兼容）"""
        if self._mark_updater_thread and self._mark_updater_thread.is_alive():
            return
        self._mark_updater_stop = threading.Event()

        def _run():
            stop_event = self._mark_updater_stop
            while stop_event and not stop_event.is_set():
                try:
                    fields = pf_compat.get_pf_pos_active(self.uid, self.exchange)
                    if not fields:
                        time.sleep(interval_s)
                        continue

                    pos_data = pf_compat.get_pf_pos(self.uid, self.exchange)
                    cycle_data = pf_compat.get_pf_cycle(self.uid, self.exchange)
                    pipe = self.rds.pipeline()
                    ts = str(now_ms())
                    total_unrealized_pnl = D("0")

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

                        from core.database import RedisKeys
                        mk_raw = self.rds.get(RedisKeys.market_prices(symbol))
                        if not mk_raw:
                            continue
                        mk = jload(mk_raw) or {}
                        mark = D(mk.get("markPrice", "0"))
                        if mark <= 0:
                            continue

                        live_pnl = self._calc_live_pnl(side=side, qty=qty, entry=entry, mark=mark)
                        total_unrealized_pnl += live_pnl

                        lock_key = f"pf:lock:cycle:{self.uid}:{field}"
                        if not self.rds.set(lock_key, "1", nx=True, px=250):
                            continue

                        try:
                            cycle_data = pf_compat.get_pf_cycle(self.uid, self.exchange)
                            cyc = cycle_data.get(field, {}) if cycle_data else {}
                            if not cyc:
                                continue

                            live_net_pnl = (
                                D(cyc.get("realizedPnlEst", "0"))
                                + D(cyc.get("fundingTotal", "0"))
                                - D(cyc.get("feeTotal", "0"))
                                + live_pnl
                            )

                            self._update_peak_and_drawdown(cyc, current_pnl=live_net_pnl)
                            cyc["liveNetPnl"] = str(live_net_pnl)
                            cyc["liveUnrealizedPnl"] = str(live_pnl)
                            cyc["markPrice"] = str(mark)
                            cyc["markTs"] = str(mk.get("ts") or ts)
                            cyc["updatedAt"] = ts

                            if cycle_data:
                                cycle_data[field] = cyc
                                pf_compat.set_pf_cycle(self.uid, cycle_data, self.exchange)

                            pos_data = pf_compat.get_pf_pos(self.uid, self.exchange)
                            if field in pos_data:
                                pos2 = pos_data[field].copy()
                                pos2["liveNetPnl"] = str(live_net_pnl)
                                pos2["liveUnrealizedPnl"] = str(live_pnl)
                                pos2["unrealizedPnl"] = str(live_pnl)
                                pos2["markPrice"] = str(mark)
                                pos2["updatedAt"] = ts
                                pos_data[field] = pos2
                                pf_compat.set_pf_pos(self.uid, pos_data, self.exchange)
                        finally:
                            try:
                                self.rds.delete(lock_key)
                            except Exception as e:
                                logger.debug(f"[{self.uid}] 删除 lock_key 失败 (非关键): {e}")

                    try:
                        account_data = pf_compat.get_pf_account(self.uid, self.exchange)
                        if account_data:
                            account_data["unrealized"] = str(total_unrealized_pnl)
                            wallet = D(account_data.get("walletBalance", "0"))
                            account_data["equity"] = str(wallet + total_unrealized_pnl)
                            account_data["ts"] = ts
                            pf_compat.set_pf_account(self.uid, account_data, self.exchange)
                    except Exception as e:
                        logger.debug(f"[{self.uid}] 更新账户数据失败 (非关键): {e}")

                    pipe.execute()
                except Exception as e:
                    logger.debug(f"[{self.uid}] mark cycle updater 循环异常: {e}")
                time.sleep(interval_s)

        self._mark_updater_thread = threading.Thread(target=_run, name="mark-cycle-updater", daemon=True)
        self._mark_updater_thread.start()

    def _stop_mark_cycle_updater(self) -> None:
        """停止 Mark Cycle Updater"""
        import os
        
        # 如果使用全局更新器，注销用户
        if os.environ.get("USE_GLOBAL_MARK_UPDATER", "1") == "1":
            try:
                from trading.global_mark_cycle_updater import get_global_mark_cycle_updater
                updater = get_global_mark_cycle_updater()
                updater.unregister_user(self.uid, self.exchange)
                logger.debug(f"[{self.uid}][{self.exchange}] 已从全局 MarkCycleUpdater 注销")
            except Exception as e:
                logger.debug(f"[{self.uid}] 从全局 MarkCycleUpdater 注销失败 (非关键): {e}")
        
        # 停止本地线程（如果有）
        if self._mark_updater_stop:
            self._mark_updater_stop.set()
            self._mark_updater_stop = None
        self._mark_updater_thread = None

    # ========== 连接状态 ==========
    
    @property
    def is_connected(self) -> bool:
        """当前是否已连接"""
        return self._is_connected
    
    def _on_ws_connect(self):
        """WebSocket 连接成功回调"""
        self._is_connected = True
        self._update_ws_status("connected")
        logger.info(f"[{self.uid}][binance] WebSocket connected")
    
    def _on_ws_disconnect(self, reason: str):
        """WebSocket 断开连接回调"""
        self._is_connected = False
        self._update_ws_status("disconnected", reason)
        logger.info(f"[{self.uid}][binance] WebSocket disconnected: {reason}")
    
    def _handle_auth_failed(self, error_msg: str):
        """
        认证失败回调
        - 更新状态到 Redis
        - 触发外部回调（自动停止交易所）
        """
        logger.error(f"[{self.uid}][binance] Authentication failed: {error_msg}")
        self._update_ws_status("auth_failed", error_msg)
        
        # 触发外部回调
        if self._on_auth_failed:
            try:
                self._on_auth_failed(error_msg)
            except Exception as e:
                logger.error(f"[{self.uid}][binance] on_auth_failed callback error: {e}")
    
    def _update_ws_status(self, state: str, error: str = None):
        """更新 WebSocket 状态到 Redis"""
        try:
            status_key = f"pf:{self.uid}:{self.exchange}:ws_status"
            status_data = {
                "state": state,
                "error": error,
                "ts": now_ms(),
            }
            self.rds.set(status_key, json.dumps(status_data))
            self.rds.expire(status_key, 300)  # 5分钟过期
        except Exception as e:
            logger.debug(f"[{self.uid}][binance] Failed to update ws status: {e}")
    
    def _init_account_snapshot(self):
        """
        初始化账户快照
        
        WebSocket 连接后不会推送初始账户状态，需要调用一次 REST API 获取。
        这个方法只在启动时调用一次，后续通过 WebSocket 实时更新。
        
        初始化内容：
        1. 账户余额 (pf_account)
        2. 初始权益 (pf_equity_init) - 只写一次
        3. 挂单缓存 (open_orders)
        
        注意：持仓数据 (pf_pos)、活跃列表 (pf_pos_active)、交易周期 (pf_cycle) 
        由 PositionAuditor 负责同步，这里不再初始化，避免覆盖已有的 TP/SL 状态
        （如 openStopordersFired、slTrailStage 等）导致重复下止盈止损单。
        """
        try:
            # 使用 API Key 级别限速器
            if self.exchange == "binance":
                from core.rate_limiter import get_binance_rate_limiter
                api_key = getattr(self.client, 'API_KEY', None)
                rate_limiter = get_binance_rate_limiter(api_key)
                rate_limiter.acquire(endpoint="futures_account", timeout=30.0)
            else:
                from core.rate_limiter import get_rate_limiter
                rate_limiter = get_rate_limiter(self.exchange)
                rate_limiter.acquire(timeout=30.0)
            
            # 获取账户信息
            account_data = self.client.futures_account()
            
            if not account_data:
                logger.warning(f"[{self.uid}][{self.exchange}] 初始化账户快照失败: 无数据")
                return
            
            ts = now_ms()
            
            # ===== 1. 提取并保存余额信息 =====
            wallet = D(account_data.get("totalWalletBalance", "0"))
            available = D(account_data.get("availableBalance", "0"))
            unrealized = D(account_data.get("totalUnrealizedProfit", "0"))
            equity = wallet + unrealized
            
            account_obj = {
                "uid": self.uid,
                "ts": str(ts),
                "walletBalance": self._d_to_str(wallet),
                "availableBalance": self._d_to_str(available),
                "equity": self._d_to_str(equity),
                "unrealized": self._d_to_str(unrealized),
                "source": "REST_INIT",
                "exchange": self.exchange,
            }
            pf_compat.set_pf_account(self.uid, account_obj, self.exchange)
            
            # ===== 2. 写初始权益快照（只写一次） =====
            existing_equity = pf_compat.get_pf_equity_init(self.uid, self.exchange)
            if equity > 0 and not existing_equity:
                equity_obj = {
                    "uid": self.uid,
                    "ts": str(ts),
                    "walletBalance": self._d_to_str(equity),
                    "source": "REST_INIT",
                    "exchange": self.exchange,
                }
                pf_compat.set_pf_equity_init(self.uid, equity_obj, self.exchange)
            
            # ===== 3. 初始化挂单缓存 =====
            open_orders_count = self._init_open_orders_cache()
            
            logger.info(
                f"[{self.uid}][{self.exchange}] 账户快照初始化完成: "
                f"wallet={wallet}, available={available}, unrealized={unrealized}, "
                f"open_orders={open_orders_count}"
            )
            
        except Exception as e:
            error_msg = str(e).lower()
            if any(kw in error_msg for kw in ["auth", "key", "permission", "signature"]):
                logger.error(f"[{self.uid}][{self.exchange}] 初始化账户快照认证失败: {e}")
            elif any(kw in error_msg for kw in ["timeout", "connect", "network"]):
                logger.warning(f"[{self.uid}][{self.exchange}] 初始化账户快照网络错误: {e}")
            else:
                logger.warning(f"[{self.uid}][{self.exchange}] 初始化账户快照失败: {e}")
            logger.exception("初始化账户快照失败")
    
    def _init_open_orders_cache(self) -> int:
        """
        初始化挂单缓存
        
        从 REST API 获取当前所有挂单，缓存到 Redis。
        后续通过 WebSocket ORDER_TRADE_UPDATE 事件实时更新。
        
        Returns:
            挂单数量
        """
        try:
            # 使用 API Key 级别限速器
            if self.exchange == "binance":
                from core.rate_limiter import get_binance_rate_limiter
                api_key = getattr(self.client, 'API_KEY', None)
                rate_limiter = get_binance_rate_limiter(api_key)
                rate_limiter.acquire(endpoint="futures_get_open_orders", timeout=30.0)
            
            # 获取所有挂单
            raw_orders = self.client.futures_get_open_orders()
            
            if not raw_orders:
                pf_compat.set_pf_open_orders(self.uid, {}, self.exchange)
                return 0
            
            ts = now_ms()
            open_orders = {}
            
            for o in raw_orders:
                # 只缓存 LIMIT 入场单（非 reduceOnly, 非 closePosition）
                order_type = (o.get("type") or "").upper()
                if order_type != "LIMIT":
                    continue
                if o.get("reduceOnly") is True:
                    continue
                if o.get("closePosition") is True:
                    continue
                
                order_id = str(o.get("orderId"))
                open_orders[order_id] = {
                    "orderId": order_id,
                    "symbol": o.get("symbol"),
                    "side": o.get("side"),
                    "positionSide": o.get("positionSide"),
                    "price": str(o.get("price", "0")),
                    "origQty": str(o.get("origQty", "0")),
                    "executedQty": str(o.get("executedQty", "0")),
                    "status": o.get("status"),
                    "time": o.get("time"),
                    "updateTime": o.get("updateTime"),
                    "cachedAt": ts,
                }
            
            pf_compat.set_pf_open_orders(self.uid, open_orders, self.exchange)
            return len(open_orders)
            
        except Exception as e:
            logger.warning(f"[{self.uid}][{self.exchange}] 初始化挂单缓存失败: {e}")
            return 0
    
    def _update_open_orders_cache(self, o: dict) -> None:
        """
        根据 ORDER_TRADE_UPDATE 事件更新挂单缓存
        
        WebSocket ORDER_TRADE_UPDATE 字段说明:
        - i: orderId
        - s: symbol
        - S: side (BUY/SELL)
        - ps: positionSide (LONG/SHORT)
        - o: orderType (LIMIT, MARKET, STOP_MARKET, etc.)
        - p: price
        - q: origQty
        - z: executedQty (累计成交数量)
        - X: orderStatus (NEW, PARTIALLY_FILLED, FILLED, CANCELED, EXPIRED, etc.)
        - x: executionType (NEW, CANCELED, CALCULATED, EXPIRED, TRADE)
        - R: reduceOnly
        - cp: closePosition
        - T: transactTime
        """
        try:
            order_id = str(o.get("i") or "")
            if not order_id:
                return
            
            order_type = (o.get("o") or "").upper()  # LIMIT, MARKET, etc.
            order_status = (o.get("X") or "").upper()  # NEW, FILLED, CANCELED, etc.
            reduce_only = o.get("R", False)
            close_position = o.get("cp", False)
            
            # 只关注 LIMIT 入场单
            if order_type != "LIMIT":
                return
            if reduce_only or close_position:
                return
            
            # 获取当前缓存
            open_orders = pf_compat.get_pf_open_orders(self.uid, self.exchange) or {}
            
            if order_status == "NEW":
                # 新挂单 - 添加到缓存
                open_orders[order_id] = {
                    "orderId": order_id,
                    "symbol": o.get("s"),
                    "side": o.get("S"),
                    "positionSide": o.get("ps"),
                    "price": str(o.get("p", "0")),
                    "origQty": str(o.get("q", "0")),
                    "executedQty": str(o.get("z", "0")),
                    "status": order_status,
                    "time": o.get("T"),
                    "cachedAt": now_ms(),
                }
                logger.debug(f"[{self.uid}][{self.exchange}] 挂单缓存添加: {order_id} {o.get('s')} {o.get('S')}")
                
            elif order_status in ("FILLED", "CANCELED", "EXPIRED", "REJECTED"):
                # 订单结束 - 从缓存移除
                if order_id in open_orders:
                    del open_orders[order_id]
                    logger.debug(f"[{self.uid}][{self.exchange}] 挂单缓存移除: {order_id} status={order_status}")
                
                # 如果是撤单/过期/拒绝，清理 ai_decision_id temp key
                if order_status in ("CANCELED", "EXPIRED", "REJECTED"):
                    from core.pf_compatibility import cleanup_ai_decision_id_for_order
                    cleanup_ai_decision_id_for_order(self.uid, self.exchange, str(order_id))
                    
            elif order_status == "PARTIALLY_FILLED":
                # 部分成交 - 更新已成交数量
                if order_id in open_orders:
                    open_orders[order_id]["executedQty"] = str(o.get("z", "0"))
                    open_orders[order_id]["status"] = order_status
                    open_orders[order_id]["cachedAt"] = now_ms()
            
            # 保存更新后的缓存
            pf_compat.set_pf_open_orders(self.uid, open_orders, self.exchange)
            
        except Exception as e:
            logger.debug(f"[{self.uid}][{self.exchange}] 更新挂单缓存失败: {e}")
    
    # ========== 生命周期 ==========

    def start(self) -> None:
        if self._ws:
            return
        
        def _on_msg(msg: dict):
            self._handle_ws_message(msg)
        
        # ① User Data Stream
        # 从 client 获取用户的 API 密钥（多用户支持）
        api_key = getattr(self.client, 'API_KEY', None)
        is_testnet = getattr(self.client, 'testnet', False)
        
        if not api_key:
            logger.error(f"[{self.uid}] API key 为空，无法启动 User Data Stream")
            return
        
        # P3 Fix: 不在日志中打印 API Key，使用哈希标识符
        import hashlib
        api_key_id = hashlib.sha256(api_key.encode()).hexdigest()[:8]
        logger.info(f"[{self.uid}] 启动 User Data Stream (testnet={is_testnet}, key_id={api_key_id})")
        
        # ⓪ 初始化账户状态（WebSocket 连接后不会推送初始状态，需要 REST 获取一次）
        self._init_account_snapshot()
        
        self._ws = FuturesUserWS(
            _on_msg,
            api_key=api_key,
            is_testnet=is_testnet,
            uid=self.uid,
            on_connect=self._on_ws_connect,
            on_disconnect=self._on_ws_disconnect,
            on_auth_failed=self._handle_auth_failed,
        )
        self._ws.start()
        
        # ② Mark Price Stream（on_tick 委托给 stop_manager，包装以传递 exchange 参数）
        self._mp = MarkPriceWS(self.rds, self.uid, on_tick=self._on_mark_tick_wrapper)
        self._mp.start()
        
        # ③ 展示层 updater
        self._start_mark_cycle_updater(interval_s=1.0)

    def stop(self) -> None:
        if self._ws:
            self._ws.stop()
            self._ws = None
        if self._mp:
            self._mp.stop()
            self._mp = None
        self._stop_mark_cycle_updater()
        
        # 清理止损管理器资源
        if hasattr(self.stop_manager, 'cleanup'):
            self.stop_manager.cleanup()

    def _on_mark_tick_wrapper(self, symbol: str, mark: Decimal, ts: int) -> None:
        """包装 stop_manager.on_mark_tick，添加 exchange 参数"""
        self.stop_manager.on_mark_tick(symbol, mark, ts, exchange="binance")

    # ========== WS 消息分发 ==========

    def _handle_ws_message(self, msg: dict) -> None:
        et = msg.get("e")
        if et == "ACCOUNT_UPDATE":
            self._on_account_update(msg)
        elif et == "ORDER_TRADE_UPDATE":
            self._on_order_trade_update(msg)

    # ========== ACCOUNT_UPDATE 处理 ==========

    def _on_account_update(self, msg: dict) -> None:
        ts = int(msg.get("T", 0) or 0)
        a = msg.get("a") or {}
        
        # 检查事件原因类型
        reason = (a.get("m") or "").upper()
        
        # 处理资金费事件
        if reason == "FUNDING_FEE":
            self._on_funding_fee(msg, ts, a)
            # 资金费事件只更新余额，不更新持仓，可以提前返回
            # 但仍然更新账户余额快照
        
        # 余额/权益快照
        wallet = D("0")
        for b in (a.get("B") or []):
            if b.get("a") == "USDT":
                wallet = D(b.get("wb", "0"))
                break

        unrealized_all = D("0")
        for pp in (a.get("P") or []):
            unrealized_all += D(pp.get("up", "0"))

        equity = wallet + unrealized_all

        # 写初始权益快照（只写一次，指定交易所）
        existing_equity = pf_compat.get_pf_equity_init(self.uid, self.exchange)
        if equity > 0 and not existing_equity:
            obj = {
                "uid": self.uid,
                "ts": str(ts),
                "walletBalance": self._d_to_str(equity),
                "source": "ACCOUNT_UPDATE.wallet+unrealized",
                "exchange": self.exchange,
            }
            pf_compat.set_pf_equity_init(self.uid, obj, self.exchange)

        # 写"当前余额/权益"（指定交易所）
        try:
            account_obj = {
                "uid": self.uid,
                "ts": str(ts),
                "walletBalance": self._d_to_str(wallet),
                "equity": self._d_to_str(equity),
                "unrealized": self._d_to_str(unrealized_all),
                "source": "ACCOUNT_UPDATE",
                "exchange": self.exchange,
            }
            pf_compat.set_pf_account(self.uid, account_obj, self.exchange)
        except Exception as e:
            logger.debug(f"[{self.uid}] 更新账户数据失败 (非关键): {e}")

        # positions 可能为空（简略事件）
        positions = a.get("P") or []
        if not positions:
            return

        pipe = self.rds.pipeline()

        for p in positions:
            symbol = p.get("s")
            side = (p.get("ps") or "").upper()
            if not symbol or side not in ("LONG", "SHORT"):
                continue

            pa = D(p.get("pa", "0"))
            qty_abs = abs(pa)
            field = pos_field(symbol, side)

            # 使用兼容层获取数据（指定交易所）
            pos_data = pf_compat.get_pf_pos(self.uid, self.exchange)
            cycle_data = pf_compat.get_pf_cycle(self.uid, self.exchange)
            active_positions = pf_compat.get_pf_pos_active(self.uid, self.exchange)

            old = pos_data.get(field, {})
            old_qty = D(old.get("qty", "0")) if old else D("0")

            unrealized_pnl = D(p.get("up", "0"))

            # qty=0 -> delete + close cycle
            if qty_abs == 0:
                # ⭐ 在删除前保存持仓快照，用于 _close_cycle 回补数据
                pos_snapshot = {field: pos_data[field].copy()} if field in pos_data else {}
                
                # 从兼容层删除数据（指定交易所）
                if field in pos_data:
                    del pos_data[field]
                    pf_compat.set_pf_pos(self.uid, pos_data, self.exchange)

                if field in active_positions:
                    active_positions.remove(field)
                    pf_compat.set_pf_pos_active(self.uid, active_positions, self.exchange)

                if old_qty != 0 and field in cycle_data:
                    self._close_cycle(pipe, field, close_time_ms=ts, pos_data_snapshot=pos_snapshot)
                continue

            # read cycle info for openTime + open order meta
            cycle_info = cycle_data.get(field, {})
            cycle_open_time = cycle_info.get("openTimeMs")
            cycle_open_order_type = cycle_info.get("openOrderType", "")
            cycle_open_tif = cycle_info.get("openTimeInForce", "")

            pos_obj = {
                "symbol": symbol,
                "side": side,
                "qty": str(qty_abs),
                "entryPrice": str(D(p.get("ep", "0"))),
                "breakEvenPrice": str(D(p.get("bep", "0"))),
                "unrealizedPnl": str(unrealized_pnl),
                "marginType": (p.get("mt") or "cross").lower(),
                "isolatedMargin": str(D(p.get("iw", "0"))),
                "openTimeMs": cycle_open_time or str(ts),
                "updatedAt": str(ts),
                "openOrderType": cycle_open_order_type,
                "openTimeInForce": cycle_open_tif,
                "exchange": self.exchange,
            }
            
            # 保留之前设置的止盈止损价格（不被覆盖）
            if old:
                if old.get("stopLossPrice"):
                    pos_obj["stopLossPrice"] = old["stopLossPrice"]
                if old.get("takeProfitPrice"):
                    pos_obj["takeProfitPrice"] = old["takeProfitPrice"]

            # 更新到兼容层（指定交易所）
            pos_data[field] = pos_obj
            pf_compat.set_pf_pos(self.uid, pos_data, self.exchange)

            if field not in active_positions:
                active_positions.append(field)
                pf_compat.set_pf_pos_active(self.uid, active_positions, self.exchange)

            # 0->non0: create cycle if missing
            if old_qty == 0 and field not in cycle_data:
                cycle_obj = self._new_cycle_dict(symbol, side, ts, qty_abs, field)
                # 更新到兼容层（指定交易所）
                cycle_data[field] = cycle_obj
                pf_compat.set_pf_cycle(self.uid, cycle_data, self.exchange)

        pipe.execute()

    def _new_cycle_dict(self, symbol: str, side: str, ts: int, qty_abs: Decimal, field: str) -> dict:
        """创建新周期的初始字典"""
        return {
            "cycleId": self._new_cycle_id(symbol, side),
            "uid": self.uid,
            "symbol": symbol,
            "side": side,
            "exchange": self.exchange,
            "openTimeMs": str(ts),
            "closeTimeMs": "0",
            "durationMs": "0",
            "openQty": "0",
            "openQuote": "0",
            "avgOpenPrice": "0",
            "closeQty": "0",
            "closeQuote": "0",
            "avgClosePrice": "0",
            "feeTotal": "0",
            "fundingTotal": "0",
            "realizedPnlEst": "0",
            "netPnl": "0",
            "peakPnl": "0",
            "minPnlAfterPeak": "0",
            "maxDrawdown": "0",
            "maxAbsQty": str(qty_abs),
            "updatedAt": str(ts),
            "closeTradeCount": "0",
            "closeOrderIds": "[]",
            "openStopordersFired": "0",
            "field": field,
            "drawdownToClose": "0",
            # trailing confirmed
            "slTrailStage": "0",
            "slTrailStopLoss": "0",
            "slTrailLastTs": "0",
            # pending (for confirm)
            "slTrailPendingStage": "0",
            "slTrailPendingStopLoss": "0",
            "slTrailPendingTs": "0",
            "slTrailPendingMark": "0",
            "slTrailPendingPct": "0",
        }

    # ========== 止损止盈订单处理 ==========
    
    def _update_tp_sl_from_order(self, o: dict, exec_type: str, order_type: str) -> None:
        """
        从止损止盈订单事件中更新持仓的 TP/SL 价格
        
        Args:
            o: 订单数据
            exec_type: 执行类型 (NEW, CANCELED, EXPIRED, TRADE, etc.)
            order_type: 订单类型 (STOP_MARKET, TAKE_PROFIT_MARKET, etc.)
        """
        symbol = o.get("s")
        ps = (o.get("ps") or "").upper()  # positionSide: LONG/SHORT
        
        if not symbol or ps not in ("LONG", "SHORT"):
            return
        
        field = pos_field(symbol, ps)
        stop_price = o.get("sp") or o.get("stopPrice") or "0"  # 触发价格
        
        # 获取当前持仓数据
        pos_data = pf_compat.get_pf_pos(self.uid, self.exchange)
        if field not in pos_data:
            return
        
        pos = pos_data[field]
        
        # 判断是止损还是止盈
        is_stop_loss = order_type in ("STOP_MARKET", "STOP")
        is_take_profit = order_type in ("TAKE_PROFIT_MARKET", "TAKE_PROFIT")
        
        if exec_type == "NEW":
            # 订单创建 - 记录价格
            if is_stop_loss:
                pos["stopLossPrice"] = str(stop_price)
                logger.debug(f"[{self.uid}] {symbol} {ps} 止损订单创建: {stop_price}")
            elif is_take_profit:
                pos["takeProfitPrice"] = str(stop_price)
                logger.debug(f"[{self.uid}] {symbol} {ps} 止盈订单创建: {stop_price}")
        
        elif exec_type in ("CANCELED", "EXPIRED"):
            # 订单取消/过期 - 清除价格
            if is_stop_loss:
                pos["stopLossPrice"] = None
                logger.debug(f"[{self.uid}] {symbol} {ps} 止损订单取消")
            elif is_take_profit:
                pos["takeProfitPrice"] = None
                logger.debug(f"[{self.uid}] {symbol} {ps} 止盈订单取消")
        
        elif exec_type == "TRADE":
            # 订单触发成交 - 清除价格（仓位可能已平）
            if is_stop_loss:
                pos["stopLossPrice"] = None
            elif is_take_profit:
                pos["takeProfitPrice"] = None
        
        # 保存更新
        pos_data[field] = pos
        pf_compat.set_pf_pos(self.uid, pos_data, self.exchange)
        
        # 同时更新 cycle 数据
        cycle_data = pf_compat.get_pf_cycle(self.uid, self.exchange)
        if field in cycle_data:
            cyc = cycle_data[field]
            if is_stop_loss:
                cyc["stopLossPrice"] = pos.get("stopLossPrice")
            elif is_take_profit:
                cyc["takeProfitPrice"] = pos.get("takeProfitPrice")
            cycle_data[field] = cyc
            pf_compat.set_pf_cycle(self.uid, cycle_data, self.exchange)

    # ========== FUNDING_FEE 处理 ==========
    
    def _on_funding_fee(self, msg: dict, ts: int, a: dict) -> None:
        """
        处理资金费事件
        
        Binance WebSocket ACCOUNT_UPDATE 资金费格式:
        {
            "e": "ACCOUNT_UPDATE",
            "T": 1234567890000,
            "a": {
                "m": "FUNDING_FEE",
                "B": [
                    {"a": "USDT", "wb": "10000.00", "cw": "9900.00", "bc": "1.23"}
                ],
                "P": [...]  // 可能为空或包含 isolated 持仓
            }
        }
        
        字段说明:
        - m: 原因类型 (FUNDING_FEE)
        - B: 余额数组
        - B[].a: 资产 (USDT)
        - B[].wb: wallet balance (钱包余额)
        - B[].cw: cross wallet balance (全仓钱包余额)
        - B[].bc: balance change (余额变化，即资金费金额)
        - P: 持仓数组（isolated 持仓时会包含）
        """
        try:
            # 提取资金费金额
            funding_amount = D("0")
            for b in (a.get("B") or []):
                if b.get("a") == "USDT":
                    # bc = balance change，即资金费金额（正数收入，负数支出）
                    funding_amount = D(b.get("bc", "0") or "0")
                    break
            
            if funding_amount == 0:
                return
            
            # 获取持仓信息（确定是哪个持仓的资金费）
            positions = a.get("P") or []
            
            if positions:
                # isolated 模式：资金费事件包含持仓信息
                for p in positions:
                    symbol = p.get("s")
                    side = (p.get("ps") or "").upper()
                    if not symbol or side not in ("LONG", "SHORT"):
                        continue
                    
                    field = pos_field(symbol, side)
                    self._apply_funding_to_cycle(field, funding_amount, ts)
                    logger.debug(
                        f"[{self.uid}][{self.exchange}] 资金费(isolated): {field} "
                        f"amount={funding_amount}"
                    )
            else:
                # cross 模式：资金费事件不包含持仓信息
                # 需要将资金费分摊到所有活跃持仓（按持仓价值比例）
                # 简化处理：记录到一个通用的资金费账户，或者按持仓数量平均分摊
                self._apply_funding_to_all_active_cycles(funding_amount, ts)
                logger.debug(
                    f"[{self.uid}][{self.exchange}] 资金费(cross): amount={funding_amount}"
                )
                
        except Exception as e:
            error_msg = str(e).lower()
            if any(kw in error_msg for kw in ["timeout", "connect", "network"]):
                logger.debug(f"[{self.uid}][{self.exchange}] 资金费处理网络错误: {e}")
            else:
                logger.warning(f"[{self.uid}][{self.exchange}] 资金费处理失败: {e}")
            logger.exception("资金费处理失败")
    
    def _apply_funding_to_cycle(self, field: str, amount: Decimal, ts: int) -> None:
        """将资金费应用到指定周期"""
        cycle_data = pf_compat.get_pf_cycle(self.uid, self.exchange)
        
        if field not in cycle_data:
            logger.debug(f"[{self.uid}][{self.exchange}] 资金费跳过: {field} 无活跃周期")
            return
        
        c = cycle_data[field]
        
        # 更新 fundingTotal
        old_funding = D(c.get("fundingTotal", "0"))
        new_funding = old_funding + amount
        c["fundingTotal"] = str(new_funding)
        
        # 重新计算 netPnl
        net = (
            D(c.get("realizedPnlEst", "0"))
            + new_funding
            - D(c.get("feeTotal", "0"))
        )
        c["netPnl"] = str(net)
        c["updatedAt"] = str(ts)
        
        # 更新 peak/drawdown
        peak = D(c.get("peakPnl", "0"))
        if net > peak:
            c["peakPnl"] = str(net)
            peak = net
        
        min_after_peak = D(c.get("minPnlAfterPeak", "0"))
        if net < min_after_peak or min_after_peak == 0:
            c["minPnlAfterPeak"] = str(net)
            min_after_peak = net
        
        drawdown = peak - min_after_peak
        if drawdown > D(c.get("maxDrawdown", "0")):
            c["maxDrawdown"] = str(drawdown)
        
        # 保存
        cycle_data[field] = c
        pf_compat.set_pf_cycle(self.uid, cycle_data, self.exchange)
        
        logger.debug(
            f"[{self.uid}][{self.exchange}] 资金费更新: {field} "
            f"fundingTotal={new_funding} netPnl={net}"
        )
    
    def _apply_funding_to_all_active_cycles(self, total_amount: Decimal, ts: int) -> None:
        """
        将资金费分摊到所有活跃周期（cross 模式）
        
        简化策略：按持仓价值比例分摊
        """
        cycle_data = pf_compat.get_pf_cycle(self.uid, self.exchange)
        pos_data = pf_compat.get_pf_pos(self.uid, self.exchange)
        
        if not cycle_data:
            logger.debug(f"[{self.uid}][{self.exchange}] 资金费跳过: 无活跃周期")
            return
        
        # 计算每个持仓的价值
        position_values = {}
        total_value = D("0")
        
        for field, pos in pos_data.items():
            if field not in cycle_data:
                continue
            
            c = cycle_data[field]
            # 跳过已关闭的周期
            if int(c.get("closeTimeMs", "0") or "0") != 0:
                continue
            
            qty = D(pos.get("qty", "0"))
            entry_price = D(pos.get("entryPrice", "0"))
            value = qty * entry_price
            
            if value > 0:
                position_values[field] = value
                total_value += value
        
        if total_value <= 0 or not position_values:
            # 没有有效持仓，但仍有资金费
            # 可能是刚平仓，资金费延迟到达，记录日志
            logger.warning(
                f"[{self.uid}][{self.exchange}] 资金费无法分摊: "
                f"amount={total_amount} 无活跃持仓"
            )
            return
        
        # 按比例分摊
        for field, value in position_values.items():
            ratio = value / total_value
            amount = total_amount * ratio
            self._apply_funding_to_cycle(field, amount, ts)

    # ========== ORDER_TRADE_UPDATE 处理 ==========

    def _on_order_trade_update(self, msg: dict) -> None:
        o = msg.get("o") or {}

        # 更新挂单缓存（实时同步）
        self._update_open_orders_cache(o)

        # 先尝试确认 trailing pending（委托给 stop_manager，传递 exchange）
        self.stop_manager.confirm_trailing_from_order(o, exchange="binance")

        exec_type = (o.get("x") or "").upper()
        order_type = (o.get("ot") or "").upper()  # 订单类型
        
        # 处理止损止盈订单的创建/取消/触发 - 更新持仓的 TP/SL 价格
        if order_type in ("STOP_MARKET", "STOP", "TAKE_PROFIT_MARKET", "TAKE_PROFIT"):
            self._update_tp_sl_from_order(o, exec_type, order_type)
        
        if exec_type != "TRADE":
            return

        t_ms = int(o.get("T", 0) or msg.get("T", 0) or 0)

        symbol = o.get("s")
        ps = (o.get("ps") or "").upper()
        trade_side = (o.get("S") or "").upper()
        if not symbol or ps not in ("LONG", "SHORT") or trade_side not in ("BUY", "SELL"):
            return

        qty = D(o.get("l", "0"))
        price = D(o.get("L", "0"))
        if qty <= 0 or price <= 0:
            return

        fee = D(o.get("n", "0"))
        realized = D(o.get("rp", "0"))

        trade_id = int(o.get("t", 0) or 0)
        order_id = int(o.get("i", 0) or 0)

        is_open_trade = self._is_open_trade(ps, trade_side)

        # dedupe
        seen_trades = pf_compat.get_pf_seen_trades(self.uid)
        if trade_id > 0:
            seen_member = f"{symbol}:{ps}:{trade_id}"
        else:
            seen_member = f"{symbol}:{ps}:oid:{order_id}:T:{t_ms}:l:{qty}:L:{price}"

        if seen_member in seen_trades:
            return

        field = pos_field(symbol, ps)
        cycle_data = pf_compat.get_pf_cycle(self.uid, self.exchange)

        # no active cycle
        if field not in cycle_data:
            if not is_open_trade:
                ok = self._backfill_close_trade_to_recent_closed(
                    field=field,
                    t_ms=t_ms,
                    qty=qty,
                    price=price,
                    fee=fee,
                    realized=realized,
                    order_id=order_id,
                )
                pf_compat.add_pf_seen_trades(self.uid, seen_member)
                return

            # open trade comes first -> create cycle
            c = self._new_cycle_dict(symbol, ps, t_ms, D("0"), field)
        else:
            c = cycle_data[field]

        # accounting
        if is_open_trade:
            # 检测是否为加仓（已有开仓数量 > 0）
            existing_open_qty = D(c.get("openQty", "0"))
            if existing_open_qty > 0:
                self.stop_manager.reset_trailing_on_add_position(symbol, ps, exchange="binance")

            c["openQty"] = str(D(c.get("openQty", "0")) + qty)
            c["openQuote"] = str(D(c.get("openQuote", "0")) + qty * price)
            c["feeTotal"] = str(D(c.get("feeTotal", "0")) + fee)
            c["realizedPnlEst"] = str(D(c.get("realizedPnlEst", "0")) + realized)

            # open order meta (once)
            if not c.get("openOrderType"):
                order_type = (o.get("o") or o.get("ot") or "").upper()
                c["openOrderType"] = order_type
                c["openTimeInForce"] = (o.get("f") or "").upper()
                c["openOrderId"] = str(o.get("i") or "")
                c["openClientOrderId"] = str(o.get("c") or "")
                logger.debug(f"[WS][ORDER_META] symbol={symbol} side={ps} order_type={order_type}")
            
            # 检查是否有关联的 AI 决策 ID（每次开仓成交都尝试）
            if not c.get("aiDecisionId"):
                from core.pf_compatibility import consume_ai_decision_id_for_order, consume_ai_decision_id_for_market
                
                # 先尝试限价单 key（使用 order_id）
                ai_decision_id = consume_ai_decision_id_for_order(
                    self.uid, self.exchange, str(o.get("i") or "")
                )
                
                # 如果没有，尝试市价单 key（使用 symbol:side）
                if not ai_decision_id:
                    ai_decision_id = consume_ai_decision_id_for_market(
                        self.uid, self.exchange, symbol, ps
                    )
                
                if ai_decision_id:
                    c["aiDecisionId"] = str(ai_decision_id)
                    logger.debug(f"[WS][AI_DECISION] {symbol}:{ps} aiDecisionId={ai_decision_id}")

            if "openStopordersFired" not in c:
                c["openStopordersFired"] = "0"

            # 设置初始止盈止损（委托给 stop_manager）
            if c.get("openStopordersFired", "0") != "1":
                oq = D(c.get("openQty", "0"))
                entry = (D(c.get("openQuote", "0")) / oq) if oq else D("0")

                # ✅ 获取订单类型(已在前面记录到 cycle 中)
                order_type = c.get("openOrderType") or ""

                # ✅ 传入 order_type 和 exchange 参数
                self.stop_manager.set_initial_stops(
                    symbol=symbol,
                    side=ps,
                    entry=entry,
                    order_type=order_type,
                    exchange=self.exchange  # 指定交易所
                )
                c["openStopordersFired"] = "1"
        else:
            c = self._add_close_fill_to_cycle_dict(
                c,
                qty=qty,
                price=price,
                fee=fee,
                realized=realized,
                order_id=order_id,
            )

        # net pnl
        net = D(c.get("realizedPnlEst", "0")) + D(c.get("fundingTotal", "0")) - D(c.get("feeTotal", "0"))
        c["netPnl"] = str(net)

        # avg prices
        oq = D(c.get("openQty", "0"))
        cq = D(c.get("closeQty", "0"))
        c["avgOpenPrice"] = str(D(c.get("openQuote", "0")) / oq) if oq else "0"
        c["avgClosePrice"] = str(D(c.get("closeQuote", "0")) / cq) if cq else "0"
        c["updatedAt"] = str(t_ms)

        # write back with lock
        lock_key = f"pf:lock:cycle:{self.uid}:{field}"
        got = self.rds.set(lock_key, "1", nx=True, px=800)
        if not got:
            time.sleep(0.05)
            got = self.rds.set(lock_key, "1", nx=True, px=800)

        try:
            pipe = self.rds.pipeline()
            # 更新到兼容层（指定交易所）
            cycle_data[field] = c
            pf_compat.set_pf_cycle(self.uid, cycle_data, self.exchange)

            # patch pos openOrderType if missing
            pos_data = pf_compat.get_pf_pos(self.uid, self.exchange)
            if field in pos_data:
                pos_obj = pos_data[field].copy()
                if (not pos_obj.get("openOrderType")) and c.get("openOrderType"):
                    pos_obj["openOrderType"] = c.get("openOrderType")
                    if c.get("openTimeInForce"):
                        pos_obj["openTimeInForce"] = c.get("openTimeInForce")
                    pos_obj["updatedAt"] = str(t_ms)
                    # 更新到兼容层（指定交易所）
                    pos_data[field] = pos_obj
                    pf_compat.set_pf_pos(self.uid, pos_data, self.exchange)

            # 添加到已见交易
            pf_compat.add_pf_seen_trades(self.uid, seen_member)
            pipe.execute()
        finally:
            if got:
                try:
                    self.rds.delete(lock_key)
                except Exception as e:
                    logger.debug(f"[{self.uid}] 删除 lock_key 失败 (非关键): {e}")

    # ========== Cycle 辅助方法 ==========

    def _add_close_fill_to_cycle_dict(
        self,
        c: dict,
        *,
        qty: Decimal,
        price: Decimal,
        fee: Decimal,
        realized: Decimal,
        order_id: int,
    ) -> dict:
        c["closeQty"] = str(D(c.get("closeQty", "0")) + qty)
        c["closeQuote"] = str(D(c.get("closeQuote", "0")) + qty * price)

        if "closeOrderIds" not in c:
            c["closeOrderIds"] = "[]"
        if "closeTradeCount" not in c:
            c["closeTradeCount"] = "0"

        if order_id > 0:
            try:
                ids = json.loads(c.get("closeOrderIds") or "[]")
                s = set()
                for x in ids:
                    try:
                        s.add(int(x))
                    except Exception as e:
                        logger.debug(f"[{self.uid}][{self.exchange}] 解析 closeOrderId 失败: {x}, error: {e}")
            except Exception as e:
                logger.debug(f"[{self.uid}][{self.exchange}] 解析 closeOrderIds JSON 失败: {e}")
                s = set()
            s.add(order_id)
            c["closeOrderIds"] = json.dumps(sorted(s), separators=(",", ":"))
            c["closeTradeCount"] = str(len(s))

        c["feeTotal"] = str(D(c.get("feeTotal", "0")) + fee)
        c["realizedPnlEst"] = str(D(c.get("realizedPnlEst", "0")) + realized)

        cq = D(c.get("closeQty", "0"))
        c["avgClosePrice"] = str(D(c.get("closeQuote", "0")) / cq) if cq else "0"

        net = D(c.get("realizedPnlEst", "0")) + D(c.get("fundingTotal", "0")) - D(c.get("feeTotal", "0"))
        c["netPnl"] = str(net)

        peak = D(c.get("peakPnl", "0"))
        dd_close = peak - net
        if dd_close < 0:
            dd_close = D("0")
        c["drawdownToClose"] = str(dd_close)

        return c

    def _backfill_close_trade_to_recent_closed(
        self,
        *,
        field: str,
        t_ms: int,
        qty: Decimal,
        price: Decimal,
        fee: Decimal,
        realized: Decimal,
        order_id: int,
        lookback_ms: int = 15_000,
        forward_ms: int = 2_000,
        scan_n: int = 30,
    ) -> bool:
        """
        回补平仓交易到最近的已关闭周期
        用于处理不在活跃周期中的平仓交易（比如回补操作）
        """
        # H4: 加锁防止并发 backfill 写入同一周期
        with self._backfill_lock:
            return self._backfill_close_trade_to_recent_closed_locked(
                field=field, t_ms=t_ms, qty=qty, price=price,
                fee=fee, realized=realized, order_id=order_id,
                lookback_ms=lookback_ms, forward_ms=forward_ms, scan_n=scan_n,
            )

    def _backfill_close_trade_to_recent_closed_locked(
        self,
        *,
        field: str,
        t_ms: int,
        qty: Decimal,
        price: Decimal,
        fee: Decimal,
        realized: Decimal,
        order_id: int,
        lookback_ms: int = 15_000,
        forward_ms: int = 2_000,
        scan_n: int = 30,
    ) -> bool:
        """H4: 实际的 backfill 逻辑（在锁内执行）"""
        try:
            # 获取已关闭交易数据（指定交易所）
            closed_h = pf_compat.get_pf_closed_h(self.uid, self.exchange)
            if not closed_h:
                return False

            # 计算时间窗口
            win_start = t_ms - lookback_ms
            win_end = t_ms + forward_ms

            # 按关闭时间倒序查找最近的交易
            candidates = []
            for cycle_id, c in closed_h.items():
                try:
                    close_time_ms = int(c.get("closeTimeMs", "0") or "0")
                    if close_time_ms >= win_start and close_time_ms <= win_end:
                        if c.get("field") == field:
                            candidates.append((cycle_id, c, close_time_ms))
                except (ValueError, TypeError) as e:
                    logger.debug(f"[{self.uid}][{self.exchange}] 解析 closeTimeMs 失败: {e}")
                    continue

            if not candidates:
                return False

            # 选择最近的一个（按关闭时间倒序）
            candidates.sort(key=lambda x: x[2], reverse=True)
            cycle_id, c, close_time_ms = candidates[0]

            # ⭐ 记录旧的 netPnl，用于后续增量修正排行榜
            old_net_pnl = D(c.get("netPnl", "0") or "0")

            # 更新周期数据
            c["updatedAt"] = str(max(int(c.get("updatedAt", "0") or "0"), t_ms))

            # 添加平仓填充
            c = self._add_close_fill_to_cycle_dict(
                c,
                qty=qty,
                price=price,
                fee=fee,
                realized=realized,
                order_id=order_id,
            )

            # 保存更新后的数据（指定交易所）
            closed_h[cycle_id] = c
            pf_compat.set_pf_closed_h(self.uid, cycle_id, c, self.exchange)

            new_net_pnl = D(c.get("netPnl", "0") or "0")
            pnl_delta = new_net_pnl - old_net_pnl

            logger.info(f"[BACKFILL] 更新已关闭周期 {cycle_id} - {field} qty:{qty} "
                        f"pnl:{c.get('netPnl', '0')} (old={old_net_pnl}, delta={pnl_delta})")
            
            # ⭐ 方案C核心：如果 netPnl 发生变化，触发排行榜对账修正
            # 这解决了 ACCOUNT_UPDATE 先于 ORDER_TRADE_UPDATE 到达导致的竞态条件：
            # _close_cycle 用不完整的 realizedPnlEst 更新了排行榜，
            # 现在 backfill 拿到了完整的 realized PnL，需要修正排行榜
            if abs(pnl_delta) > D("0.001"):
                try:
                    from core.referral_db import ReferralDB
                    rdb = ReferralDB()
                    rdb.reconcile_user_profit_stats(self.uid)
                    logger.info(f"[BACKFILL] 排行榜已对账修正: uid={self.uid} delta={pnl_delta}")
                except Exception as e:
                    logger.warning(f"[BACKFILL] 排行榜对账修正失败: {e}")
            
            return True

        except Exception as e:
            logger.error(f"[BACKFILL] 更新已关闭周期失败: {e}", exc_info=True)
            # H5: 不再 fallthrough 到创建新记录 — 如果更新失败，
            # 可能已经写入了部分数据，创建新记录会导致重复
            return False

        # 如果没有找到合适的已关闭周期，创建一个新的交易记录
        logger.info(f"[BACKFILL] 未找到合适周期，为 {field} 创建新的已关闭交易记录")

        try:
            # 解析field
            symbol, side = field.split(':')

            # 获取已关闭交易数据（指定交易所）
            closed_h = pf_compat.get_pf_closed_h(self.uid, self.exchange) or {}

            # 创建新的已关闭交易记录
            new_cycle_id = f"{symbol}:{side}:{t_ms}"
            net_pnl = realized - fee
            new_cycle = {
                "cycleId": new_cycle_id,
                "uid": self.uid,
                "symbol": symbol,
                "side": side,
                "exchange": self.exchange,
                "openTimeMs": str(t_ms - 1000),  # 假设开仓时间是1秒前
                "closeTimeMs": str(t_ms),
                "durationMs": "1000",
                "openQty": str(abs(qty)),  # 使用平仓数量作为开仓数量
                "closeQty": str(abs(qty)),
                "avgOpenPrice": str(price),  # 使用当前价格作为开仓价格
                "avgClosePrice": str(price),
                "feeTotal": str(fee),
                "fundingTotal": "0",
                "realizedPnlEst": str(realized),
                "netPnl": str(net_pnl),
                "peakPnl": str(max(net_pnl, D("0"))),  # 峰值收益（backfill 时取 netPnl 或 0）
                "drawdownToClose": "0",  # 回撤（backfill 无法计算，默认0）
                "maxDrawdown": "0",
                "maxAbsQty": str(abs(qty)),
                "closeTradeCount": "1",
                "openOrderType": "UNKNOWN",
                "field": field,
                "closeSource": "backfill_new_record"
            }

            # 保存新记录（指定交易所）
            closed_h[new_cycle_id] = new_cycle
            pf_compat.set_pf_closed_h(self.uid, new_cycle_id, new_cycle, self.exchange)

            logger.info(f"[BACKFILL] 创建新的已关闭交易记录 {new_cycle_id} - {field} qty:{qty} pnl:{new_cycle['netPnl']}")
            
            # ⭐ 触发排行榜更新（backfill 创建新记录时也需要更新排行榜）
            try:
                from core.commission_service import trigger_commission_on_trade_close
                trigger_commission_on_trade_close(
                    uid=self.uid,
                    symbol=symbol,
                    side=side,
                    net_pnl=float(net_pnl),
                    realized_pnl=float(realized),
                    fee_total=float(fee),
                    trade_id=new_cycle_id
                )
                logger.info(f"[BACKFILL] 排行榜已更新: {new_cycle_id} pnl={net_pnl}")
            except Exception as e:
                logger.warning(f"[BACKFILL] 排行榜更新失败: {e}")
            
            return True

        except Exception as e:
            logger.error(f"[BACKFILL] 创建新交易记录失败: {e}")
            return False

    def _mark_seen_trade(self, seen_key: str, seen_member: str) -> None:
        self.rds.sadd(seen_key, seen_member)
        self.rds.expire(seen_key, 2 * 24 * 3600)

    def _close_cycle(self, pipe, field: str, close_time_ms: int, pos_data_snapshot: dict = None) -> None:
        # 使用兼容层获取和更新数据（指定交易所）
        cycle_data = pf_compat.get_pf_cycle(self.uid, self.exchange)
        closed_h = pf_compat.get_pf_closed_h(self.uid, self.exchange)

        c = cycle_data.get(field)
        if not c:
            return

        # 确保 symbol 和 side 存在（从 field 解析）
        if not c.get("symbol") or not c.get("side"):
            try:
                parts = field.split(":")
                if len(parts) >= 2:
                    c["symbol"] = c.get("symbol") or parts[0]
                    c["side"] = c.get("side") or parts[1]
                    logger.info(f"[{self.uid}][{self.exchange}] Parsed symbol/side from field: {field}")
            except Exception as e:
                logger.warning(f"[{self.uid}][{self.exchange}] Failed to parse field {field}: {e}")

        # ⭐ 检查关键字段是否缺失，如果缺失则尝试从持仓快照或交易所 API 回补
        open_time_ms = int(c.get("openTimeMs", "0") or "0")
        open_qty = D(c.get("openQty", "0") or "0")
        avg_open_price = D(c.get("avgOpenPrice", "0") or "0")
        max_abs_qty = D(c.get("maxAbsQty", "0") or "0")
        
        # 检测数据是否不完整（openTimeMs=0 或 openQty=0 表示数据丢失）
        data_incomplete = (open_time_ms == 0 or open_qty == 0 or avg_open_price == 0)
        
        if data_incomplete:
            logger.warning(
                f"[{self.uid}][{self.exchange}] Cycle data incomplete for {field}: "
                f"openTimeMs={open_time_ms}, openQty={open_qty}, avgOpenPrice={avg_open_price}"
            )
            
            # 尝试从持仓快照回补数据
            if pos_data_snapshot and field in pos_data_snapshot:
                pos = pos_data_snapshot[field]
                if open_time_ms == 0:
                    pos_open_time = int(pos.get("openTimeMs", "0") or "0")
                    if pos_open_time > 0:
                        c["openTimeMs"] = str(pos_open_time)
                        open_time_ms = pos_open_time
                        logger.info(f"[{self.uid}][{self.exchange}] Recovered openTimeMs from pos snapshot: {open_time_ms}")
                
                if open_qty == 0:
                    pos_qty = D(pos.get("qty", "0") or "0")
                    if pos_qty > 0:
                        c["openQty"] = str(pos_qty)
                        c["closeQty"] = str(pos_qty)  # 全部平仓
                        c["maxAbsQty"] = str(pos_qty)
                        open_qty = pos_qty
                        max_abs_qty = pos_qty
                        logger.info(f"[{self.uid}][{self.exchange}] Recovered qty from pos snapshot: {open_qty}")
                
                if avg_open_price == 0:
                    pos_entry = D(pos.get("entryPrice", "0") or "0")
                    if pos_entry > 0:
                        c["avgOpenPrice"] = str(pos_entry)
                        c["openQuote"] = str(open_qty * pos_entry) if open_qty > 0 else "0"
                        avg_open_price = pos_entry
                        logger.info(f"[{self.uid}][{self.exchange}] Recovered entryPrice from pos snapshot: {avg_open_price}")
            
            # 如果仍然不完整，尝试从交易所 API 回补
            if open_time_ms == 0 or open_qty == 0 or avg_open_price == 0:
                try:
                    self._backfill_cycle_from_exchange(c, field, close_time_ms)
                except Exception as e:
                    logger.warning(f"[{self.uid}][{self.exchange}] Failed to backfill from exchange: {e}")
        
        # ⭐ 修复 maxAbsQty 为 0 的问题（即使其他字段正常）
        max_abs_qty = D(c.get("maxAbsQty", "0") or "0")
        if max_abs_qty == 0:
            # 使用 openQty 或 closeQty 中较大的值
            open_qty_final = D(c.get("openQty", "0") or "0")
            close_qty_final = D(c.get("closeQty", "0") or "0")
            max_abs_qty = max(open_qty_final, close_qty_final)
            if max_abs_qty > 0:
                c["maxAbsQty"] = str(max_abs_qty)
                logger.info(f"[{self.uid}][{self.exchange}] Fixed maxAbsQty from qty: {max_abs_qty}")

        c["closeTimeMs"] = str(close_time_ms)
        open_t = int(c.get("openTimeMs", "0") or "0")
        c["durationMs"] = str(max(0, close_time_ms - open_t))
        c["updatedAt"] = str(close_time_ms)
        c["field"] = field

        net = D(c.get("realizedPnlEst", "0")) + D(c.get("fundingTotal", "0")) - D(c.get("feeTotal", "0"))
        c["netPnl"] = str(net)

        peak = D(c.get("peakPnl", "0"))
        # 如果净收益超过峰值（瞬间插针止盈），更新峰值
        if net > peak:
            peak = net
            c["peakPnl"] = str(peak)
        dd_close = peak - net
        if dd_close < 0:
            dd_close = D("0")
        c["drawdownToClose"] = str(dd_close)

        cycle_id = c.get("cycleId") or self._new_cycle_id(c.get("symbol", "UNK"), c.get("side", "UNK"))
        c["cycleId"] = cycle_id

        # 确保所有ClosedTrade模型需要的字段都存在
        required_fields = {
            "openTimeMs": c.get("openTimeMs", "0"),
            "avgOpenPrice": c.get("avgOpenPrice", "0"),
            "avgClosePrice": c.get("avgClosePrice", "0"),
            "openQty": c.get("openQty", "0"),
            "closeQty": c.get("closeQty", "0"),
            "feeTotal": c.get("feeTotal", "0"),
            "fundingTotal": c.get("fundingTotal", "0"),
            "realizedPnlEst": c.get("realizedPnlEst", "0"),
            "maxAbsQty": c.get("maxAbsQty", "0"),
            "peakPnl": c.get("peakPnl", "0"),
            "drawdownToClose": c.get("drawdownToClose", "0"),
            "maxDrawdown": c.get("maxDrawdown", "0"),
            "closeTradeCount": c.get("closeTradeCount", "0"),
            "openOrderType": c.get("openOrderType", ""),
            "exchange": c.get("exchange", self.exchange),
        }

        # 合并到cycle数据中
        c.update(required_fields)

        # 更新到兼容层（指定交易所）
        closed_h[cycle_id] = c
        pf_compat.set_pf_closed_h(self.uid, cycle_id, c, self.exchange)

        # 从cycle中删除（指定交易所）
        if field in cycle_data:
            del cycle_data[field]
            pf_compat.set_pf_cycle(self.uid, cycle_data, self.exchange)
        
        # 触发返佣发放和统计更新
        try:
            from core.commission_service import trigger_commission_on_trade_close
            trigger_commission_on_trade_close(
                uid=self.uid,
                symbol=c.get("symbol", ""),
                side=c.get("side", ""),
                net_pnl=float(net),
                realized_pnl=float(c.get("realizedPnlEst", "0")),
                fee_total=float(c.get("feeTotal", "0")),
                trade_id=cycle_id
            )
        except Exception as e:
            logger.warning(f"[{self.uid}] 返佣触发失败: {e}")

    def _backfill_cycle_from_exchange(self, c: dict, field: str, close_time_ms: int) -> None:
        """
        从交易所 API 回补 cycle 数据
        
        当 cycle 数据不完整时（如 WebSocket 断开重连后数据丢失），
        尝试从交易所 API 获取交易记录来回补数据。
        """
        symbol = c.get("symbol")
        side = c.get("side")
        
        if not symbol or not side:
            return
        
        try:
            # 使用 API Key 级别限速器
            from core.rate_limiter import get_binance_rate_limiter
            api_key = getattr(self.client, 'API_KEY', None)
            rate_limiter = get_binance_rate_limiter(api_key or "")
            rate_limiter.acquire(endpoint="futures_account_trades", timeout=30.0)
            
            # 查询最近的交易记录（最近 24 小时）
            start_time = close_time_ms - 24 * 60 * 60 * 1000
            end_time = close_time_ms + 5 * 60 * 1000
            
            trades = self.client.futures_account_trades(
                symbol=symbol,
                startTime=start_time,
                endTime=end_time,
                limit=1000,
            )
            
            if not trades:
                logger.warning(f"[{self.uid}][{self.exchange}] No trades found for backfill: {field}")
                return
            
            # 筛选该持仓方向的交易
            open_side = "BUY" if side.upper() == "LONG" else "SELL"
            close_side = "SELL" if side.upper() == "LONG" else "BUY"
            
            open_fills = []
            close_fills = []
            
            for t in trades:
                trade_side = (t.get("side") or "").upper()
                position_side = (t.get("positionSide") or "").upper()
                
                if position_side != side.upper():
                    continue
                
                trade_time = int(t.get("time", 0) or 0)
                qty = D(t.get("qty", "0"))
                price = D(t.get("price", "0"))
                fee = abs(D(t.get("commission", "0")))
                realized = D(t.get("realizedPnl", "0"))
                
                fill = {
                    "time": trade_time,
                    "qty": qty,
                    "price": price,
                    "fee": fee,
                    "realized": realized,
                }
                
                if trade_side == open_side:
                    open_fills.append(fill)
                elif trade_side == close_side:
                    close_fills.append(fill)
            
            logger.info(
                f"[{self.uid}][{self.exchange}] Backfill found {len(open_fills)} open + {len(close_fills)} close trades for {field}"
            )
            
            # 回补开仓数据
            if open_fills:
                total_open_qty = sum(f["qty"] for f in open_fills)
                total_open_quote = sum(f["qty"] * f["price"] for f in open_fills)
                total_open_fee = sum(f["fee"] for f in open_fills)
                earliest_open_time = min(f["time"] for f in open_fills)
                
                if D(c.get("openQty", "0")) == 0:
                    c["openQty"] = str(total_open_qty)
                    c["openQuote"] = str(total_open_quote)
                    c["avgOpenPrice"] = str(total_open_quote / total_open_qty) if total_open_qty > 0 else "0"
                    c["maxAbsQty"] = str(total_open_qty)
                    logger.info(f"[{self.uid}][{self.exchange}] Backfilled openQty={total_open_qty}, avgOpenPrice={c['avgOpenPrice']}")
                
                if int(c.get("openTimeMs", "0") or "0") == 0:
                    c["openTimeMs"] = str(earliest_open_time)
                    logger.info(f"[{self.uid}][{self.exchange}] Backfilled openTimeMs={earliest_open_time}")
                
                # 累加手续费
                c["feeTotal"] = str(D(c.get("feeTotal", "0")) + total_open_fee)
            
            # 回补平仓数据
            if close_fills:
                total_close_qty = sum(f["qty"] for f in close_fills)
                total_close_quote = sum(f["qty"] * f["price"] for f in close_fills)
                total_close_fee = sum(f["fee"] for f in close_fills)
                total_realized = sum(f["realized"] for f in close_fills)
                
                if D(c.get("closeQty", "0")) == 0:
                    c["closeQty"] = str(total_close_qty)
                    c["closeQuote"] = str(total_close_quote)
                    c["avgClosePrice"] = str(total_close_quote / total_close_qty) if total_close_qty > 0 else "0"
                    logger.info(f"[{self.uid}][{self.exchange}] Backfilled closeQty={total_close_qty}, avgClosePrice={c['avgClosePrice']}")
                
                # 累加手续费和已实现盈亏
                c["feeTotal"] = str(D(c.get("feeTotal", "0")) + total_close_fee)
                c["realizedPnlEst"] = str(D(c.get("realizedPnlEst", "0")) + total_realized)
            
            # 重新计算 netPnl
            net = D(c.get("realizedPnlEst", "0")) + D(c.get("fundingTotal", "0")) - D(c.get("feeTotal", "0"))
            c["netPnl"] = str(net)
            
        except Exception as e:
            logger.error(f"[{self.uid}][{self.exchange}] Backfill from exchange failed: {e}")

