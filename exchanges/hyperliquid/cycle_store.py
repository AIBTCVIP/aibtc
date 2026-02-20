# exchanges/hyperliquid/cycle_store.py
"""
Hyperliquid WebSocket CycleStore - 基于 WebSocket 的周期跟踪

改造自 REST 轮询方式，现使用 WebSocket 实时推送：
1. 订阅 userEvents 接收成交 (fills)、资金费率 (funding)
2. 订阅 allMids 接收标记价格
3. 通过 REST API 轮询获取持仓变化（Hyperliquid userEvents 不推送持仓变化）

WebSocket 订阅:
- userEvents: fills, funding, liquidation
- allMids: 所有币种中间价 (mark price)

轮询补充 (因为 userEvents 不推送持仓变化):
- 定期轮询持仓 API，检测开平仓
"""

from __future__ import annotations

import asyncio
import json
import logging
import threading
import time
import uuid
from decimal import Decimal
from typing import Callable, Dict, Optional, List, TYPE_CHECKING

from exchanges.hyperliquid.websocket import (
    HyperliquidUserStream,
    ConnectionState,
)
# 使用共享 MarkPrice 流适配器（多用户共享1个WebSocket连接）
from websocket.mark_price_adapters import HyperliquidMarkPriceStreamAdapter as HyperliquidMarkPriceStream
# 使用适配器，支持全局处理器优化（1000+用户规模）
from trading.stop_loss_adapter import StopLossManagerAdapter as StopLossManager
from core.pf_compatibility import pf_compat
from core.utils import D, now_ms, pos_field

if TYPE_CHECKING:
    from core.user_context import UserContext
    from exchanges.hyperliquid_exchange import HyperliquidExchange

logger = logging.getLogger(__name__)


class HyperliquidCycleStore:
    """
    Hyperliquid WebSocket CycleStore
    
    事件处理:
    - userEvents (fills) -> cycle qty/quote/fee/realized
    - userEvents (funding) -> cycle fundingTotal
    - allMids -> 标记价格 -> 移动止损
    - REST 轮询 -> 持仓变化检测 -> cycle open/close
    """
    
    EXCHANGE_NAME = "hyperliquid"
    POLL_INTERVAL_S = 2.0  # 持仓轮询间隔（WebSocket 不推送持仓变化）
    
    def __init__(
        self,
        exchange_client: 'HyperliquidExchange',
        redis_conn,
        uid: str,
        *,
        closed_maxlen: int = 20000,
        user_context: Optional['UserContext'] = None,
        stop_manager: Optional[StopLossManager] = None,
        on_auth_failed: Optional[Callable[[str], None]] = None,
    ):
        self.client = exchange_client
        self.rds = redis_conn
        self.uid = uid
        self.closed_maxlen = closed_maxlen
        self._user_context = user_context
        self.exchange = self.EXCHANGE_NAME
        
        # 认证失败回调
        self._on_auth_failed = on_auth_failed
        
        # 连续失败计数
        self._consecutive_failures = 0
        self._max_consecutive_failures = 10
        
        # 止损管理器
        if stop_manager is not None:
            self.stop_manager = stop_manager
        else:
            self.stop_manager = StopLossManager(
                redis_conn=redis_conn,
                uid=uid,
                execute_trade_func=None,
                user_context=user_context,
            )
        
        # 设置交易所（用于全局处理器注册）
        self.stop_manager.set_exchange(self.exchange)
        
        # WebSocket 流
        self._user_stream: Optional[HyperliquidUserStream] = None
        self._mark_stream: Optional[HyperliquidMarkPriceStream] = None
        
        # 持仓轮询（补充 WebSocket）
        self._stop_event = threading.Event()
        self._poll_thread: Optional[threading.Thread] = None
        
        # Mark Cycle Updater
        self._mark_updater_stop = None
        self._mark_updater_thread = None
        
        # 上一次的持仓快照
        self._last_positions: Dict[str, dict] = {}
        
        # 连接状态
        self._connection_state = ConnectionState.DISCONNECTED
    
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
    
    def _new_cycle_id(self, symbol: str, side: str) -> str:
        return f"{symbol}:{side}:{int(time.time())}:{uuid.uuid4().hex[:8]}"
    
    def _update_peak_and_drawdown(self, c: dict, current_pnl=None):
        """更新峰值收益和最大回撤"""
        cur = current_pnl if current_pnl is not None else D(c.get("netPnl", "0"))
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
    
    @staticmethod
    def _is_open_trade(position_side: str, trade_side: str) -> bool:
        """判断是否为开仓交易"""
        ps = position_side.upper()
        ts = trade_side.lower()
        if ps == "LONG":
            return ts == "buy"
        if ps == "SHORT":
            return ts == "sell"
        return False
    
    def _convert_hl_symbol(self, hl_symbol: str) -> str:
        """转换 Hyperliquid symbol 到标准格式: BTC -> BTCUSDT"""
        if not hl_symbol.endswith('USDT') and not hl_symbol.endswith('USDC'):
            return f"{hl_symbol}USDT"
        return hl_symbol
    
    def _convert_to_hl_symbol(self, symbol: str) -> str:
        """转换标准 symbol 到 Hyperliquid 格式: BTCUSDT -> BTC"""
        if symbol.endswith('USDT'):
            return symbol[:-4]
        if symbol.endswith('USDC'):
            return symbol[:-4]
        return symbol
    
    # ========== Mark Cycle Updater ==========
    
    def _start_mark_cycle_updater(self, interval_s: float = 1.0) -> None:
        """
        用于展示/统计更新（参考 Binance/Bitget 实现）
        
        功能:
        - 用 pf:mark:{symbol} 更新 cycle 的 liveNetPnl / peakPnl / markPrice / markTs 等
        - 同步字段到 pos（供前端展示）
        - 更新 account 的 unrealized 字段
        """
        if self._mark_updater_thread and self._mark_updater_thread.is_alive():
            return
        self._mark_updater_stop = threading.Event()
        
        def _run():
            stop_event = self._mark_updater_stop
            logger.debug(f"[{self.uid}][hyperliquid] Mark updater thread started")
            update_count = 0
            
            while stop_event and not stop_event.is_set():
                try:
                    fields = pf_compat.get_pf_pos_active(self.uid, self.exchange)
                    if not fields:
                        time.sleep(interval_s)
                        continue
                    
                    pos_data = pf_compat.get_pf_pos(self.uid, self.exchange)
                    cycle_data = pf_compat.get_pf_cycle(self.uid, self.exchange)
                    
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
                        
                        # 获取标记价格
                        from core.database import RedisKeys
                        from core.utils import jload
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
                            
                            update_count += 1
                            if update_count % 30 == 0:
                                logger.debug(f"[{self.uid}][hyperliquid] Mark updater: {field} mark={mark} liveNetPnl={live_net_pnl:.4f}")
                            
                            if cycle_data:
                                cycle_data[field] = cyc
                                pf_compat.set_pf_cycle(self.uid, cycle_data, self.exchange)
                            
                            # 同步到 pos
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
                            except Exception:
                                pass
                    
                    # 更新 account
                    try:
                        account_data = pf_compat.get_pf_account(self.uid, self.exchange)
                        if account_data:
                            account_data["unrealized"] = str(total_unrealized_pnl)
                            wallet = D(account_data.get("walletBalance", "0"))
                            account_data["equity"] = str(wallet + total_unrealized_pnl)
                            account_data["ts"] = ts
                            pf_compat.set_pf_account(self.uid, account_data, self.exchange)
                    except Exception:
                        pass
                    
                except Exception as e:
                    logger.warning(f"[{self.uid}][hyperliquid] Mark updater error: {e}")
                
                time.sleep(interval_s)
        
        self._mark_updater_thread = threading.Thread(
            target=_run, name=f"hyperliquid-mark-updater-{self.uid}", daemon=True
        )
        self._mark_updater_thread.start()
    
    def _stop_mark_cycle_updater(self) -> None:
        if self._mark_updater_stop:
            self._mark_updater_stop.set()
            self._mark_updater_stop = None
        self._mark_updater_thread = None
    
    # ========== 连接状态 ==========
    
    @property
    def connection_state(self) -> ConnectionState:
        return self._connection_state
    
    def _on_user_state_change(self, state: ConnectionState, error: Optional[str] = None):
        """UserStream 状态变化回调"""
        self._connection_state = state
        try:
            status_key = f"pf:{self.uid}:{self.exchange}:ws_status"
            status_data = {
                "state": state.value,
                "error": error,
                "ts": now_ms(),
            }
            self.rds.set(status_key, json.dumps(status_data))
            self.rds.expire(status_key, 300)
        except Exception as e:
            logger.debug(f"[{self.uid}][hyperliquid] Failed to update ws status: {e}")
        
        if state == ConnectionState.AUTH_FAILED and self._on_auth_failed:
            logger.warning(f"[{self.uid}][hyperliquid] Auth failed, triggering auto-stop callback")
            try:
                self._on_auth_failed(error or "Authentication failed")
            except Exception as e:
                logger.error(f"[{self.uid}][hyperliquid] on_auth_failed callback error: {e}")
    
    def _on_mark_state_change(self, state: ConnectionState, error: Optional[str] = None):
        """Mark price WebSocket 状态变化回调"""
        try:
            status_key = f"pf:{self.uid}:{self.exchange}:mark_ws_status"
            status_data = {
                "state": state.value,
                "error": error,
                "ts": now_ms(),
            }
            self.rds.set(status_key, json.dumps(status_data))
            self.rds.expire(status_key, 300)
            
            if state in (ConnectionState.CONNECTED, ConnectionState.ERROR, ConnectionState.AUTH_FAILED):
                logger.info(f"[{self.uid}][hyperliquid] Mark price WS state: {state.value}")
        except Exception as e:
            logger.debug(f"[{self.uid}][hyperliquid] Failed to update mark ws status: {e}")
    
    # ========== 生命周期 ==========
    
    def start(self) -> None:
        if self._user_stream:
            return
        
        logger.info(f"[{self.uid}][hyperliquid] 启动 CycleStore (WebSocket + REST 混合模式)")
        
        # 初始化现有仓位
        self._init_existing_positions()
        
        # 初始化挂单缓存
        open_orders_count = self._init_open_orders_cache()
        logger.info(f"[{self.uid}][hyperliquid] 挂单缓存初始化完成: {open_orders_count} 个")
        
        # 用户数据流 WebSocket
        wallet_address = self.client.wallet_address
        if wallet_address:
            self._user_stream = HyperliquidUserStream(
                wallet_address=wallet_address,
                is_testnet=self.client.is_testnet,
                uid=self.uid,
                on_fill=self._on_fill,
                on_funding=self._on_funding,
                on_liquidation=self._on_liquidation,
                on_order_update=self._on_order_update,
                on_state_change=self._on_user_state_change,
            )
            self._user_stream.start()
        else:
            logger.warning(f"[{self.uid}][hyperliquid] 无钱包地址，跳过 UserStream")
        
        # 标记价格流 WebSocket
        self._mark_stream = HyperliquidMarkPriceStream(
            redis_conn=self.rds,
            uid=self.uid,
            is_testnet=self.client.is_testnet,
            on_tick=self._on_mark_tick,
            on_state_change=self._on_mark_state_change,
        )
        self._mark_stream.start()
        
        # 持仓轮询线程（因为 Hyperliquid WebSocket 不推送持仓变化）
        self._stop_event.clear()
        self._poll_thread = threading.Thread(
            target=self._run_poll_loop,
            name=f"hyperliquid-position-poll-{self.uid}",
            daemon=True
        )
        self._poll_thread.start()
        
        # Mark Cycle Updater
        self._start_mark_cycle_updater(interval_s=1.0)
        
        logger.info(f"[{self.uid}][hyperliquid] CycleStore 已启动")
    
    def stop(self) -> None:
        self._stop_event.set()
        
        if self._user_stream:
            self._user_stream.stop()
            self._user_stream = None
        
        if self._mark_stream:
            self._mark_stream.stop()
            self._mark_stream = None
        
        if self._poll_thread:
            self._poll_thread.join(timeout=5.0)
            self._poll_thread = None
        
        self._stop_mark_cycle_updater()
        
        logger.info(f"[{self.uid}][hyperliquid] CycleStore 已停止")
    
    # ========== 持仓轮询 ==========
    
    def _run_poll_loop(self) -> None:
        """持仓轮询循环（补充 WebSocket，因为 Hyperliquid 不推送持仓变化）"""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        try:
            while not self._stop_event.is_set():
                try:
                    loop.run_until_complete(self._poll_positions())
                except Exception as e:
                    logger.warning(f"[{self.uid}][hyperliquid] 持仓轮询错误: {e}")
                
                self._stop_event.wait(self.POLL_INTERVAL_S)
        finally:
            loop.close()
    
    async def _poll_positions(self) -> None:
        """轮询持仓变化"""
        try:
            account = await self.client.get_account()
            ts = now_ms()
            
            self._consecutive_failures = 0
            
            # 更新账户数据
            self._update_account(account, ts)
            
            # 更新持仓数据
            await self._update_positions(account.positions, ts)
            
            # 更新止盈止损订单
            await self._update_tp_sl_orders()
            
        except Exception as e:
            self._consecutive_failures += 1
            error_msg = str(e)
            logger.debug(f"[{self.uid}][hyperliquid] 轮询失败 ({self._consecutive_failures}/{self._max_consecutive_failures}): {e}")
            
            is_auth_error = any(kw in error_msg.lower() for kw in ["unauthorized", "invalid key", "permission", "forbidden"])
            
            if is_auth_error or self._consecutive_failures >= self._max_consecutive_failures:
                if self._on_auth_failed:
                    logger.warning(f"[{self.uid}][hyperliquid] Auth failed or too many failures, triggering auto-stop")
                    try:
                        self._on_auth_failed(error_msg)
                    except Exception as cb_err:
                        logger.error(f"[{self.uid}][hyperliquid] on_auth_failed callback error: {cb_err}")
    
    def _init_existing_positions(self) -> None:
        """初始化现有仓位数据"""
        try:
            self._patch_missing_open_order_type()
            logger.info(f"[{self.uid}][hyperliquid] 初始化现有仓位数据完成")
        except Exception as e:
            logger.warning(f"[{self.uid}][hyperliquid] 初始化现有仓位数据失败: {e}")
    
    def _patch_missing_open_order_type(self) -> None:
        """为缺少 openOrderType 的仓位设置默认值"""
        try:
            pos_data = pf_compat.get_pf_pos(self.uid, self.exchange)
            cycle_data = pf_compat.get_pf_cycle(self.uid, self.exchange)
            
            updated = False
            for field, pos in pos_data.items():
                current_type = pos.get("openOrderType")
                if not current_type:
                    pos["openOrderType"] = "UNKNOWN"
                    pos_data[field] = pos
                    updated = True
                    logger.info(f"[{self.uid}][hyperliquid] {field} openOrderType 设置为 UNKNOWN")
                    
                    if field in cycle_data:
                        cycle_data[field]["openOrderType"] = "UNKNOWN"
            
            if updated:
                pf_compat.set_pf_pos(self.uid, pos_data, self.exchange)
                pf_compat.set_pf_cycle(self.uid, cycle_data, self.exchange)
                
        except Exception as e:
            logger.warning(f"[{self.uid}][hyperliquid] 设置 openOrderType 失败: {e}")
    
    def _init_open_orders_cache(self) -> int:
        """
        初始化挂单缓存（参考 Binance 实现）
        
        从 REST API 获取当前所有挂单，缓存到 Redis。
        后续通过 WebSocket orderUpdates 实时更新。
        
        Returns:
            挂单数量
        """
        try:
            address = self.client.wallet_address
            if not address:
                return 0
            
            # 使用同步方式获取挂单（frontend_open_orders 包含 orderType）
            loop = asyncio.new_event_loop()
            try:
                orders_data = loop.run_until_complete(
                    self.client._run_sync(self.client._info.frontend_open_orders, address)
                )
            finally:
                loop.close()
            
            if not orders_data:
                pf_compat.set_pf_open_orders(self.uid, {}, self.exchange)
                return 0
            
            ts = now_ms()
            open_orders = {}
            
            for o in orders_data:
                order_type = o.get('orderType', '')
                
                # 只缓存 LIMIT 入场单（非止损止盈、非 reduceOnly）
                # 与 Binance 逻辑一致
                if 'Stop' in order_type or 'Take Profit' in order_type:
                    continue
                if o.get('reduceOnly', False):
                    continue
                if order_type != 'Limit':
                    continue
                
                oid = str(o.get('oid', ''))
                if not oid:
                    continue
                    
                hl_symbol = o.get('coin', '')
                symbol = self._convert_hl_symbol(hl_symbol)
                
                side_char = o.get('side', '')
                side = 'BUY' if side_char == 'B' else 'SELL'
                position_side = 'LONG' if side == 'BUY' else 'SHORT'
                
                orig_sz = float(o.get('origSz', '0') or '0')
                current_sz = float(o.get('sz', '0') or '0')
                executed_qty = orig_sz - current_sz
                
                open_orders[oid] = {
                    "orderId": oid,
                    "symbol": symbol,
                    "side": side,
                    "positionSide": position_side,
                    "price": str(o.get('limitPx', '0')),
                    "origQty": str(orig_sz),
                    "executedQty": str(executed_qty),
                    "status": "NEW",
                    "time": o.get('timestamp'),
                    "cachedAt": ts,
                }
            
            pf_compat.set_pf_open_orders(self.uid, open_orders, self.exchange)
            return len(open_orders)
            
        except Exception as e:
            logger.warning(f"[{self.uid}][hyperliquid] 初始化挂单缓存失败: {e}")
            logger.exception("初始化挂单缓存失败")
            return 0
    
    def _on_order_update(self, data: dict) -> None:
        """
        处理 orderUpdates WebSocket 事件（参考 Binance _update_open_orders_cache）
        
        与 Binance 的区别：
        - Binance WebSocket 推送包含 orderType 字段
        - Hyperliquid WebSocket 推送不包含 orderType，需要调用 REST API 查询
        
        data 格式 (WsOrder):
        {
            "order": {
                "coin": "BTC",
                "side": "B",
                "limitPx": "50000.0",
                "sz": "0.01",
                "oid": 123456,
                "timestamp": 1234567890000,
                "origSz": "0.01",
                "cloid": null
            },
            "status": "open" | "filled" | "canceled" | ...,
            "statusTimestamp": 1234567890000
        }
        """
        try:
            order_info = data.get("order", {})
            status = (data.get("status") or "").lower()
            
            oid = str(order_info.get("oid", ""))
            if not oid:
                return
            
            hl_symbol = order_info.get("coin", "")
            symbol = self._convert_hl_symbol(hl_symbol)
            
            # 获取当前缓存
            open_orders = pf_compat.get_pf_open_orders(self.uid, self.exchange) or {}
            
            if status == "open":
                # 新挂单 - 需要查询 REST API 获取 orderType
                # 与 Binance 一致：只缓存 LIMIT 入场单
                order_detail = self._query_order_detail(oid)
                if order_detail is None:
                    return
                
                order_type = order_detail.get('orderType', '')
                
                # 只缓存 LIMIT 入场单（非止损止盈、非 reduceOnly）
                if 'Stop' in order_type or 'Take Profit' in order_type:
                    return
                if order_detail.get('reduceOnly', False):
                    return
                if order_type != 'Limit':
                    return
                
                side_char = order_info.get("side", "")
                side = "BUY" if side_char == "B" else "SELL"
                position_side = "LONG" if side == "BUY" else "SHORT"
                
                orig_sz = float(order_info.get("origSz", "0") or "0")
                current_sz = float(order_info.get("sz", "0") or "0")
                executed_qty = orig_sz - current_sz
                
                open_orders[oid] = {
                    "orderId": oid,
                    "symbol": symbol,
                    "side": side,
                    "positionSide": position_side,
                    "price": str(order_info.get("limitPx", "0")),
                    "origQty": str(orig_sz),
                    "executedQty": str(executed_qty),
                    "status": "NEW",
                    "time": order_info.get("timestamp"),
                    "cachedAt": now_ms(),
                }
                logger.debug(f"[{self.uid}][hyperliquid] 挂单缓存添加: {oid} {symbol} {side}")
                
            elif status in ("filled", "canceled", "triggered", "rejected", 
                            "margincanceled", "selftradecanceled", "reduceonlycanceled",
                            "liquidatedcanceled", "scheduledcancel", "vaultwithdrawalcanceled",
                            "openinterestcapcanceled", "delistedcanceled"):
                # 订单结束 - 从缓存移除（与 Binance 一致）
                if oid in open_orders:
                    del open_orders[oid]
                    logger.debug(f"[{self.uid}][hyperliquid] 挂单缓存移除: {oid} status={status}")
                
                # 如果是撤单相关状态，清理 ai_decision_id temp key
                if status in ("canceled", "rejected", "margincanceled", "selftradecanceled", 
                              "reduceonlycanceled", "liquidatedcanceled", "scheduledcancel",
                              "vaultwithdrawalcanceled", "openinterestcapcanceled", "delistedcanceled"):
                    from core.pf_compatibility import cleanup_ai_decision_id_for_order
                    cleanup_ai_decision_id_for_order(self.uid, self.exchange, str(oid))
                    
            elif status == "partially_filled":
                # 部分成交 - 更新已成交数量（与 Binance 一致）
                if oid in open_orders:
                    orig_sz = float(order_info.get("origSz", "0") or "0")
                    current_sz = float(order_info.get("sz", "0") or "0")
                    executed_qty = orig_sz - current_sz
                    
                    open_orders[oid]["executedQty"] = str(executed_qty)
                    open_orders[oid]["status"] = "PARTIALLY_FILLED"
                    open_orders[oid]["cachedAt"] = now_ms()
            
            # 保存更新后的缓存
            pf_compat.set_pf_open_orders(self.uid, open_orders, self.exchange)
            
        except Exception as e:
            logger.debug(f"[{self.uid}][hyperliquid] 更新挂单缓存失败: {e}")
    
    def _query_order_detail(self, oid: str) -> Optional[dict]:
        """
        查询订单详情（获取 orderType）
        
        使用 query_order_by_oid API 获取完整订单信息
        
        Returns:
            订单详情字典，包含 orderType 等字段；失败返回 None
        """
        try:
            address = self.client.wallet_address
            if not address:
                return None
            
            # 使用同步方式查询订单状态
            loop = asyncio.new_event_loop()
            try:
                result = loop.run_until_complete(
                    self.client._run_sync(
                        self.client._info.query_order_by_oid,
                        address,
                        int(oid)
                    )
                )
            finally:
                loop.close()
            
            if not result:
                return None
            
            # query_order_by_oid API 返回格式:
            # {"status": "order", "order": {"order": {...}, "status": "...", "statusTimestamp": ...}}
            if result.get("status") == "order":
                order_data = result.get("order", {})
                return order_data.get("order", {})
            
            return None
            
        except Exception as e:
            logger.debug(f"[{self.uid}][hyperliquid] 查询订单详情失败 {oid}: {e}")
            return None

    def _update_account(self, account, ts: int) -> None:
        """更新账户数据"""
        try:
            wallet = D(str(account.total_balance))
            unrealized = D(str(account.unrealized_pnl))
            equity = wallet
            
            existing_equity = pf_compat.get_pf_equity_init(self.uid, self.exchange)
            if equity > 0 and not existing_equity:
                obj = {
                    "uid": self.uid,
                    "ts": str(ts),
                    "walletBalance": self._d_to_str(equity),
                    "source": "HYPERLIQUID_REST",
                    "exchange": self.exchange,
                }
                pf_compat.set_pf_equity_init(self.uid, obj, self.exchange)
            
            account_obj = {
                "uid": self.uid,
                "ts": str(ts),
                "walletBalance": self._d_to_str(wallet),
                "equity": self._d_to_str(equity),
                "unrealized": self._d_to_str(unrealized),
                "source": "HYPERLIQUID_REST",
                "exchange": self.exchange,
            }
            pf_compat.set_pf_account(self.uid, account_obj, self.exchange)
            
        except Exception as e:
            logger.warning(f"[{self.uid}][hyperliquid] 账户更新失败: {e}")
    
    async def _update_tp_sl_orders(self) -> None:
        """从挂单中提取止损止盈价格并更新到持仓数据"""
        try:
            address = self.client.wallet_address
            if not address:
                return
            
            orders_data = await self.client._run_sync(self.client._info.frontend_open_orders, address)
            if not orders_data:
                return
            
            pos_data = pf_compat.get_pf_pos(self.uid, self.exchange)
            cycle_data = pf_compat.get_pf_cycle(self.uid, self.exchange)
            
            # 重置所有持仓的 TP/SL
            for field in pos_data:
                pos_data[field]["stopLossPrice"] = None
                pos_data[field]["takeProfitPrice"] = None
            
            for o in orders_data:
                hl_symbol = o.get('coin', '')
                symbol = self._convert_hl_symbol(hl_symbol)
                order_type = o.get('orderType', '')
                trigger_px = o.get('triggerPx') or o.get('limitPx') or "0"
                
                is_sl = 'Stop' in order_type and 'Take' not in order_type
                is_tp = 'Take Profit' in order_type
                
                if not (is_sl or is_tp):
                    continue
                
                side = o.get('side', '').upper()
                if side == 'B':
                    ps = 'SHORT'
                elif side == 'A':
                    ps = 'LONG'
                else:
                    continue
                
                field = f"{symbol}:{ps}"
                
                if field not in pos_data:
                    continue
                
                if is_sl and trigger_px:
                    pos_data[field]["stopLossPrice"] = str(trigger_px)
                elif is_tp and trigger_px:
                    pos_data[field]["takeProfitPrice"] = str(trigger_px)
            
            pf_compat.set_pf_pos(self.uid, pos_data, self.exchange)
            
            for field in cycle_data:
                if field in pos_data:
                    cycle_data[field]["stopLossPrice"] = pos_data[field].get("stopLossPrice")
                    cycle_data[field]["takeProfitPrice"] = pos_data[field].get("takeProfitPrice")
            pf_compat.set_pf_cycle(self.uid, cycle_data, self.exchange)
            
        except Exception as e:
            logger.debug(f"[{self.uid}][hyperliquid] 更新止损止盈订单失败: {e}")
    
    async def _update_positions(self, positions: List, ts: int) -> None:
        """更新持仓数据，检测变化"""
        try:
            current_positions: Dict[str, dict] = {}
            for pos in positions:
                hl_symbol = pos.symbol
                symbol = self._convert_hl_symbol(hl_symbol)
                side = pos.side.upper()
                field = pos_field(symbol, side)
                
                current_positions[field] = {
                    "symbol": symbol,
                    "hl_symbol": hl_symbol,
                    "side": side,
                    "qty": float(pos.qty),
                    "entryPrice": float(pos.entry_price),
                    "unrealizedPnl": float(pos.unrealized_pnl),
                    "leverage": pos.leverage,
                    "marginType": pos.margin_type,
                }
            
            pos_data = pf_compat.get_pf_pos(self.uid, self.exchange)
            cycle_data = pf_compat.get_pf_cycle(self.uid, self.exchange)
            active_positions = pf_compat.get_pf_pos_active(self.uid, self.exchange)
            
            # 检测新开仓位
            for field, pos_info in current_positions.items():
                old_pos = self._last_positions.get(field)
                symbol = pos_info["symbol"]
                hl_symbol = pos_info["hl_symbol"]
                side = pos_info["side"]
                qty = D(str(pos_info["qty"]))
                entry = D(str(pos_info["entryPrice"]))
                
                pos_obj = {
                    "symbol": symbol,
                    "side": side,
                    "qty": str(qty),
                    "entryPrice": str(entry),
                    "unrealizedPnl": str(pos_info["unrealizedPnl"]),
                    "marginType": pos_info["marginType"],
                    "leverage": str(pos_info["leverage"]),
                    "openTimeMs": str(ts),
                    "updatedAt": str(ts),
                    "exchange": self.exchange,
                }
                
                old_redis_pos = pos_data.get(field, {})
                if old_redis_pos:
                    if old_redis_pos.get("stopLossPrice"):
                        pos_obj["stopLossPrice"] = old_redis_pos["stopLossPrice"]
                    if old_redis_pos.get("takeProfitPrice"):
                        pos_obj["takeProfitPrice"] = old_redis_pos["takeProfitPrice"]
                    if old_redis_pos.get("openTimeMs"):
                        pos_obj["openTimeMs"] = old_redis_pos["openTimeMs"]
                    if old_redis_pos.get("openOrderType"):
                        pos_obj["openOrderType"] = old_redis_pos["openOrderType"]
                    if old_redis_pos.get("openTimeInForce"):
                        pos_obj["openTimeInForce"] = old_redis_pos["openTimeInForce"]
                
                pos_data[field] = pos_obj
                
                if field not in active_positions:
                    active_positions.append(field)
                
                if old_pos is None:
                    # 新开仓
                    if field not in cycle_data:
                        cycle_obj = self._new_cycle_dict(symbol, side, ts, qty, field)
                        cycle_obj["openQty"] = str(qty)
                        cycle_obj["openQuote"] = str(qty * entry)
                        cycle_obj["avgOpenPrice"] = str(entry)
                        cycle_data[field] = cycle_obj
                        logger.info(f"[{self.uid}][hyperliquid] 新周期: {field}")
                        
                        self.stop_manager.set_initial_stops(
                            symbol=symbol,
                            side=side,
                            entry=entry,
                            order_type="UNKNOWN",
                            exchange=self.exchange
                        )
                        cycle_data[field]["openStopordersFired"] = "1"
                        
                        # 动态添加标记价格订阅
                        if self._mark_stream and hl_symbol:
                            self._mark_stream.add_symbol(hl_symbol)
                else:
                    old_qty = D(str(old_pos.get("qty", 0)))
                    if qty > old_qty:
                        # 加仓（传递 exchange）
                        self.stop_manager.reset_trailing_on_add_position(symbol, side, exchange=self.exchange)
                        
                        if field in cycle_data:
                            c = cycle_data[field]
                            added_qty = qty - old_qty
                            c["openQty"] = str(D(c.get("openQty", "0")) + added_qty)
                            c["openQuote"] = str(D(c.get("openQuote", "0")) + added_qty * entry)
                            oq = D(c.get("openQty", "0"))
                            c["avgOpenPrice"] = str(D(c.get("openQuote", "0")) / oq) if oq else "0"
                            c["updatedAt"] = str(ts)
                            cycle_data[field] = c
                
                # 触发移动止损检查
                mark_price = entry + D(str(pos_info["unrealizedPnl"])) / qty if qty > 0 else entry
                self.stop_manager.on_mark_tick(symbol, mark_price, ts, exchange=self.exchange)
            
            # 检测已平仓位
            for field in list(self._last_positions.keys()):
                if field not in current_positions:
                    # ⭐ 在删除前保存持仓快照
                    pos_snapshot = {field: pos_data[field].copy()} if field in pos_data else {}
                    
                    if field in pos_data:
                        del pos_data[field]
                    if field in active_positions:
                        active_positions.remove(field)
                    if field in cycle_data:
                        self._close_cycle(field, ts, cycle_data, pos_data_snapshot=pos_snapshot)
            
            pf_compat.set_pf_pos(self.uid, pos_data, self.exchange)
            pf_compat.set_pf_cycle(self.uid, cycle_data, self.exchange)
            pf_compat.set_pf_pos_active(self.uid, active_positions, self.exchange)
            
            self._last_positions = current_positions
            
        except Exception as e:
            logger.warning(f"[{self.uid}][hyperliquid] 持仓更新失败: {e}")
            logger.exception("持仓更新失败")
    
    # ========== WebSocket 事件处理 ==========
    
    def _on_mark_tick(self, symbol: str, mark_price: float, timestamp: int):
        """
        处理标记价格更新
        
        1. 写入 Redis（供 UI / Mark Updater 读取）
        2. 设置 TTL（15秒，与 Binance 一致）
        3. 回调 stop_manager 处理移动止损
        """
        try:
            from core.database import RedisKeys
            
            price_key = RedisKeys.market_prices(symbol)
            price_data = {
                "markPrice": str(mark_price),
                "ts": str(timestamp),
            }
            self.rds.set(price_key, json.dumps(price_data, separators=(",", ":")))
            self.rds.expire(price_key, 15)  # 15秒 TTL
            
            # DEBUG: 每100次打印一次（降级为 DEBUG）
            if not hasattr(self, '_mark_tick_count'):
                self._mark_tick_count = {}
            self._mark_tick_count[symbol] = self._mark_tick_count.get(symbol, 0) + 1
            if self._mark_tick_count[symbol] % 100 == 1:
                logger.debug(f"[{self.uid}][hyperliquid] Mark tick: {symbol} = {mark_price}")
            
            # 委托给 stop_manager 处理移动止损
            self.stop_manager.on_mark_tick(symbol, D(str(mark_price)), timestamp, exchange=self.exchange)
        except Exception as e:
            logger.warning(f"[{self.uid}][hyperliquid] Mark tick error: {e}")
    
    def _on_fill(self, data: dict) -> None:
        """
        处理 Hyperliquid 成交明细 (fills)
        
        Hyperliquid fill 格式:
        {
            "coin": "BTC",
            "px": "50000.5",
            "sz": "0.01",
            "side": "B",           # B=Buy, A=Ask(Sell)
            "time": 1234567890000,
            "startPosition": "0.05",
            "dir": "Open Long",    # Open Long, Close Long, Open Short, Close Short
            "closedPnl": "10.5",
            "hash": "0x...",
            "oid": 123456,
            "crossed": true,
            "fee": "0.05",
            "tid": 789,
            "feeToken": "USDC"
        }
        """
        try:
            logger.info(f"[{self.uid}][hyperliquid] fill 推送: {data}")
            
            hl_symbol = data.get("coin", "")
            symbol = self._convert_hl_symbol(hl_symbol)
            
            dir_str = data.get("dir", "")
            side_char = data.get("side", "")
            
            # 确定持仓方向和是否平仓
            is_close_trade = "Close" in dir_str
            if "Long" in dir_str:
                ps = "LONG"
            elif "Short" in dir_str:
                ps = "SHORT"
            else:
                # 从 side 推断
                if side_char == "B":
                    ps = "LONG" if not is_close_trade else "SHORT"
                else:
                    ps = "SHORT" if not is_close_trade else "LONG"
            
            fill_qty = D(data.get("sz", "0") or "0")
            fill_price = D(data.get("px", "0") or "0")
            closed_pnl = D(data.get("closedPnl", "0") or "0")
            fee = abs(D(data.get("fee", "0") or "0"))
            
            t_ms = int(data.get("time") or now_ms())
            order_id = str(data.get("oid", ""))
            trade_id = str(data.get("tid", ""))
            
            field = f"{symbol}:{ps}"
            
            logger.info(f"[{self.uid}][hyperliquid] fill: {field} dir={dir_str} qty={fill_qty} price={fill_price} pnl={closed_pnl} fee={fee}")
            
            # 去重
            seen_trades = pf_compat.get_pf_seen_trades(self.uid)
            seen_member = f"{symbol}:{ps}:fill:{trade_id or order_id}:T:{t_ms}:qty:{fill_qty}"
            
            if seen_member in seen_trades:
                return
            
            cycle_data = pf_compat.get_pf_cycle(self.uid, self.exchange)
            
            if field not in cycle_data:
                if is_close_trade:
                    # 平仓但没有活跃周期
                    self._backfill_close_trade(
                        field=field,
                        t_ms=t_ms,
                        qty=fill_qty,
                        price=fill_price,
                        fee=fee,
                        realized=closed_pnl,
                        order_id=order_id,
                    )
                    pf_compat.add_pf_seen_trades(self.uid, seen_member)
                    return
                # 开仓但没有周期（可能是 REST 还没同步）
                # 创建新周期
                c = self._new_cycle_dict(symbol, ps, t_ms, D("0"), field)
            else:
                c = cycle_data[field]
            
            is_open_trade = not is_close_trade
            
            # 尝试确认 trailing pending（委托给 stop_manager，传递 exchange）
            is_taker = data.get("crossed", True)
            self.stop_manager.confirm_trailing_from_order({
                "x": "TRADE",  # 模拟 Binance 格式
                "s": symbol,
                "ps": ps,
                "o": "MARKET" if is_taker else "LIMIT",
            }, exchange=self.exchange)
            
            if is_open_trade:
                # 开仓成交
                existing_open_qty = D(c.get("openQty", "0"))
                if existing_open_qty > 0:
                    self.stop_manager.reset_trailing_on_add_position(symbol, ps, exchange=self.exchange)
                
                c["openQty"] = str(D(c.get("openQty", "0")) + fill_qty)
                c["openQuote"] = str(D(c.get("openQuote", "0")) + fill_qty * fill_price)
                c["feeTotal"] = str(D(c.get("feeTotal", "0")) + fee)
                
                # 记录开仓订单类型
                if not c.get("openOrderType"):
                    # Hyperliquid fill 没有订单类型，从 crossed 字段推断
                    # crossed=true 表示 taker (市价成交)
                    # crossed=false 表示 maker (限价成交)
                    is_taker = data.get("crossed", True)
                    c["openOrderType"] = "MARKET" if is_taker else "LIMIT"
                    c["openTimeInForce"] = "IOC" if is_taker else "GTC"
                    logger.info(f"[{self.uid}][hyperliquid][ORDER_META] {symbol} {ps} type={'MARKET' if is_taker else 'LIMIT'}")
                
                # 检查是否有关联的 AI 决策 ID (moved outside openOrderType check to handle race conditions)
                if not c.get("aiDecisionId"):
                    from core.pf_compatibility import consume_ai_decision_id_for_order, consume_ai_decision_id_for_market
                    
                    # 先尝试限价单 key
                    if order_id:
                        ai_decision_id = consume_ai_decision_id_for_order(
                            self.uid, self.exchange, str(order_id)
                        )
                    else:
                        ai_decision_id = None
                    
                    # 如果没有，尝试市价单 key
                    if not ai_decision_id:
                        ai_decision_id = consume_ai_decision_id_for_market(
                            self.uid, self.exchange, symbol, ps
                        )
                    
                    if ai_decision_id:
                        c["aiDecisionId"] = str(ai_decision_id)
                        logger.info(f"[{self.uid}][hyperliquid][AI_DECISION] {symbol} {ps} linked to ai_decision_id={ai_decision_id}")
                
                if c.get("openStopordersFired", "0") != "1":
                    oq = D(c.get("openQty", "0"))
                    entry = (D(c.get("openQuote", "0")) / oq) if oq else D("0")
                    order_type = c.get("openOrderType") or "UNKNOWN"
                    
                    self.stop_manager.set_initial_stops(
                        symbol=symbol,
                        side=ps,
                        entry=entry,
                        order_type=order_type,
                        exchange=self.exchange
                    )
                    c["openStopordersFired"] = "1"
            else:
                # 平仓成交
                c = self._add_close_fill_to_cycle_dict(
                    c,
                    qty=fill_qty,
                    price=fill_price,
                    fee=fee,
                    realized=closed_pnl,
                    order_id=order_id,
                )
            
            # 计算净收益
            net = D(c.get("realizedPnlEst", "0")) + D(c.get("fundingTotal", "0")) - D(c.get("feeTotal", "0"))
            c["netPnl"] = str(net)
            
            # 计算均价
            oq = D(c.get("openQty", "0"))
            cq = D(c.get("closeQty", "0"))
            c["avgOpenPrice"] = str(D(c.get("openQuote", "0")) / oq) if oq else "0"
            c["avgClosePrice"] = str(D(c.get("closeQuote", "0")) / cq) if cq else "0"
            c["updatedAt"] = str(t_ms)
            
            # 写入 Redis（带锁，与 Bitget 一致）
            lock_key = f"pf:lock:cycle:{self.uid}:{field}"
            got = self.rds.set(lock_key, "1", nx=True, px=800)
            if not got:
                time.sleep(0.05)
                got = self.rds.set(lock_key, "1", nx=True, px=800)
            
            try:
                cycle_data[field] = c
                pf_compat.set_pf_cycle(self.uid, cycle_data, self.exchange)
                
                # 更新 pos 的 openOrderType 和 openTimeInForce
                pos_data = pf_compat.get_pf_pos(self.uid, self.exchange)
                if field in pos_data:
                    pos_obj = pos_data[field].copy()
                    updated = False
                    if (not pos_obj.get("openOrderType")) and c.get("openOrderType"):
                        pos_obj["openOrderType"] = c.get("openOrderType")
                        updated = True
                    if (not pos_obj.get("openTimeInForce")) and c.get("openTimeInForce"):
                        pos_obj["openTimeInForce"] = c.get("openTimeInForce")
                        updated = True
                    if updated:
                        pos_obj["updatedAt"] = str(t_ms)
                        pos_data[field] = pos_obj
                        pf_compat.set_pf_pos(self.uid, pos_data, self.exchange)
                
                pf_compat.add_pf_seen_trades(self.uid, seen_member)
            finally:
                if got:
                    try:
                        self.rds.delete(lock_key)
                    except Exception:
                        pass
            
            logger.debug(f"[{self.uid}][hyperliquid] 成交: {field} {'开仓' if is_open_trade else '平仓'} qty={fill_qty}")
            
        except Exception as e:
            logger.warning(f"[{self.uid}][hyperliquid] fill 处理失败: {e}")
            logger.exception("fill 处理失败")
    
    def _on_funding(self, data: dict) -> None:
        """
        处理资金费率
        
        Hyperliquid funding 格式:
        {
            "time": 1234567890000,
            "coin": "BTC",
            "usdc": "1.23",
            "szi": "0.05",
            "fundingRate": "0.0001"
        }
        """
        try:
            logger.info(f"[{self.uid}][hyperliquid] funding 推送: {data}")
            
            hl_symbol = data.get("coin", "")
            symbol = self._convert_hl_symbol(hl_symbol)
            funding_amount = D(data.get("usdc", "0") or "0")
            szi = D(data.get("szi", "0") or "0")
            
            # 确定持仓方向
            ps = "LONG" if szi > 0 else "SHORT"
            field = f"{symbol}:{ps}"
            
            t_ms = int(data.get("time") or now_ms())
            
            # 更新 cycle 的 fundingTotal
            cycle_data = pf_compat.get_pf_cycle(self.uid, self.exchange)
            if field in cycle_data:
                c = cycle_data[field]
                c["fundingTotal"] = str(D(c.get("fundingTotal", "0")) + funding_amount)
                
                net = D(c.get("realizedPnlEst", "0")) + D(c.get("fundingTotal", "0")) - D(c.get("feeTotal", "0"))
                c["netPnl"] = str(net)
                c["updatedAt"] = str(t_ms)
                
                cycle_data[field] = c
                pf_compat.set_pf_cycle(self.uid, cycle_data, self.exchange)
                
                logger.info(f"[{self.uid}][hyperliquid] funding: {field} amount={funding_amount} total={c['fundingTotal']}")
            
        except Exception as e:
            logger.warning(f"[{self.uid}][hyperliquid] funding 处理失败: {e}")
    
    def _on_liquidation(self, data: dict) -> None:
        """处理清算事件"""
        try:
            logger.warning(f"[{self.uid}][hyperliquid] 清算事件: {data}")
            # 清算会导致持仓归零，等待下次轮询处理
        except Exception as e:
            logger.warning(f"[{self.uid}][hyperliquid] 清算处理失败: {e}")
    
    # ========== 周期辅助方法 ==========
    
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
            # trailing 相关字段
            "slTrailStage": "0",
            "slTrailStopLoss": "0",
            "slTrailLastTs": "0",
            "slTrailPendingStage": "0",
            "slTrailPendingStopLoss": "0",
            "slTrailPendingTs": "0",
            "slTrailPendingMark": "0",
            "slTrailPendingPct": "0",
        }
    
    def _add_close_fill_to_cycle_dict(
        self,
        c: dict,
        *,
        qty: Decimal,
        price: Decimal,
        fee: Decimal,
        realized: Decimal,
        order_id: str,
    ) -> dict:
        """添加平仓成交到周期（与 Bitget 一致）"""
        c["closeQty"] = str(D(c.get("closeQty", "0")) + qty)
        c["closeQuote"] = str(D(c.get("closeQuote", "0")) + qty * price)
        
        if "closeOrderIds" not in c:
            c["closeOrderIds"] = "[]"
        if "closeTradeCount" not in c:
            c["closeTradeCount"] = "0"
        
        if order_id:
            try:
                ids = json.loads(c.get("closeOrderIds") or "[]")
            except Exception:
                ids = []
            if str(order_id) not in [str(x) for x in ids]:
                ids.append(str(order_id))
                c["closeOrderIds"] = json.dumps(sorted(set(str(x) for x in ids)), separators=(",", ":"))
                c["closeTradeCount"] = str(len(ids))
        
        c["feeTotal"] = str(D(c.get("feeTotal", "0")) + fee)
        c["realizedPnlEst"] = str(D(c.get("realizedPnlEst", "0")) + realized)
        
        # 计算均价
        cq = D(c.get("closeQty", "0"))
        c["avgClosePrice"] = str(D(c.get("closeQuote", "0")) / cq) if cq else "0"
        
        # 计算净盈亏
        net = D(c.get("realizedPnlEst", "0")) + D(c.get("fundingTotal", "0")) - D(c.get("feeTotal", "0"))
        c["netPnl"] = str(net)
        
        # 计算 drawdownToClose
        peak = D(c.get("peakPnl", "0"))
        dd_close = peak - net
        if dd_close < 0:
            dd_close = D("0")
        c["drawdownToClose"] = str(dd_close)
        
        return c
    
    def _backfill_close_trade(
        self,
        field: str,
        t_ms: int,
        qty: Decimal,
        price: Decimal,
        fee: Decimal,
        realized: Decimal,
        order_id: str,
    ) -> bool:
        """
        回补平仓交易到最近的已关闭周期（与 Bitget 一致）
        
        如果找不到匹配的周期，创建新的回补记录
        """
        try:
            closed_h = pf_compat.get_pf_closed_h(self.uid, self.exchange)
            if not closed_h:
                return self._create_backfill_record(field, t_ms, qty, price, fee, realized, order_id)
            
            # 查找 15 秒内关闭的同一仓位的周期
            win_start = t_ms - 15000
            win_end = t_ms + 2000
            
            candidates = []
            for cycle_id, c in closed_h.items():
                try:
                    close_time_ms = int(c.get("closeTimeMs", "0") or "0")
                    if win_start <= close_time_ms <= win_end and c.get("field") == field:
                        candidates.append((cycle_id, c, close_time_ms))
                except (ValueError, TypeError):
                    continue
            
            if not candidates:
                return self._create_backfill_record(field, t_ms, qty, price, fee, realized, order_id)
            
            # 取最近的一个
            candidates.sort(key=lambda x: x[2], reverse=True)
            cycle_id, c, _ = candidates[0]
            
            c["updatedAt"] = str(max(int(c.get("updatedAt", "0") or "0"), t_ms))
            c = self._add_close_fill_to_cycle_dict(c, qty=qty, price=price, fee=fee, realized=realized, order_id=order_id)
            
            closed_h[cycle_id] = c
            pf_compat.set_pf_closed_h(self.uid, cycle_id, c, self.exchange)
            
            logger.info(f"[{self.uid}][hyperliquid][BACKFILL] 更新已关闭周期 {cycle_id}")
            
            # ⚠️ 注意：更新已有周期时，不触发排行榜更新
            # 因为这个周期之前通过 _close_cycle 关闭时已经触发过排行榜更新了
            # 如果再次触发会导致重复计算（total_trades 和 net_profit 会被重复累加）
            # 增量的 pnl 变化会在下次对账时自动修正
            
            return True
            
        except Exception as e:
            logger.warning(f"[{self.uid}][hyperliquid][BACKFILL] 查找失败: {e}")
            return self._create_backfill_record(field, t_ms, qty, price, fee, realized, order_id)
    
    def _create_backfill_record(
        self,
        field: str,
        t_ms: int,
        qty: Decimal,
        price: Decimal,
        fee: Decimal,
        realized: Decimal,
        order_id: str,
    ) -> bool:
        """创建新的回补记录"""
        try:
            symbol, side = field.split(':')
            
            new_cycle_id = f"{symbol}:{side}:{t_ms}"
            net_pnl = realized - fee
            
            new_cycle = {
                "cycleId": new_cycle_id,
                "uid": self.uid,
                "symbol": symbol,
                "side": side,
                "exchange": self.exchange,
                "openTimeMs": str(t_ms - 1000),
                "closeTimeMs": str(t_ms),
                "durationMs": "1000",
                "openQty": str(abs(qty)),
                "closeQty": str(abs(qty)),
                "avgOpenPrice": str(price),
                "avgClosePrice": str(price),
                "feeTotal": str(fee),
                "fundingTotal": "0",
                "realizedPnlEst": str(realized),
                "netPnl": str(net_pnl),
                "peakPnl": str(max(net_pnl, D("0"))),
                "drawdownToClose": "0",
                "maxDrawdown": "0",
                "maxAbsQty": str(abs(qty)),
                "closeTradeCount": "1",
                "openOrderType": "UNKNOWN",
                "field": field,
                "closeSource": "backfill_new_record",
            }
            
            pf_compat.set_pf_closed_h(self.uid, new_cycle_id, new_cycle, self.exchange)
            logger.info(f"[{self.uid}][hyperliquid][BACKFILL] 创建新记录 {new_cycle_id}")
            
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
                logger.info(f"[{self.uid}][hyperliquid][BACKFILL] 排行榜已更新: {new_cycle_id} pnl={net_pnl}")
            except Exception as e:
                logger.warning(f"[{self.uid}][hyperliquid][BACKFILL] 排行榜更新失败: {e}")
            
            return True
            
        except Exception as e:
            logger.error(f"[{self.uid}][hyperliquid][BACKFILL] 创建失败: {e}")
            return False
    
    def _close_cycle(self, field: str, close_time_ms: int, cycle_data: dict = None, pos_data_snapshot: dict = None) -> None:
        """关闭周期（与 Bitget 一致）"""
        logger.info(f"[{self.uid}][hyperliquid] _close_cycle 开始: {field}")
        
        if cycle_data is None:
            cycle_data = pf_compat.get_pf_cycle(self.uid, self.exchange)
        
        c = cycle_data.get(field)
        if not c:
            logger.warning(f"[{self.uid}][hyperliquid] _close_cycle: {field} 不在 cycle_data 中")
            return
        
        # 确保 symbol 和 side 存在（从 field 解析）
        if not c.get("symbol") or not c.get("side"):
            try:
                parts = field.split(":")
                if len(parts) >= 2:
                    c["symbol"] = c.get("symbol") or parts[0]
                    c["side"] = c.get("side") or parts[1]
                    logger.info(f"[{self.uid}][hyperliquid] Parsed symbol/side from field: {field}")
            except Exception as e:
                logger.warning(f"[{self.uid}][hyperliquid] Failed to parse field {field}: {e}")
        
        # ⭐ 检查关键字段是否缺失，如果缺失则尝试从持仓快照回补
        open_time_ms = int(c.get("openTimeMs", "0") or "0")
        open_qty = D(c.get("openQty", "0") or "0")
        avg_open_price = D(c.get("avgOpenPrice", "0") or "0")
        
        data_incomplete = (open_time_ms == 0 or open_qty == 0 or avg_open_price == 0)
        
        if data_incomplete:
            logger.warning(
                f"[{self.uid}][hyperliquid] Cycle data incomplete for {field}: "
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
                        logger.info(f"[{self.uid}][hyperliquid] Recovered openTimeMs from pos snapshot: {open_time_ms}")
                
                if open_qty == 0:
                    pos_qty = D(pos.get("qty", "0") or "0")
                    if pos_qty > 0:
                        c["openQty"] = str(pos_qty)
                        c["closeQty"] = str(pos_qty)
                        c["maxAbsQty"] = str(pos_qty)
                        open_qty = pos_qty
                        logger.info(f"[{self.uid}][hyperliquid] Recovered qty from pos snapshot: {open_qty}")
                
                if avg_open_price == 0:
                    pos_entry = D(pos.get("entryPrice", "0") or "0")
                    if pos_entry > 0:
                        c["avgOpenPrice"] = str(pos_entry)
                        c["openQuote"] = str(open_qty * pos_entry) if open_qty > 0 else "0"
                        avg_open_price = pos_entry
                        logger.info(f"[{self.uid}][hyperliquid] Recovered entryPrice from pos snapshot: {avg_open_price}")
        
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
        
        # 确保所有必要字段存在（与 Bitget 一致）
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
        c.update(required_fields)
        
        # 保存到已关闭交易
        pf_compat.set_pf_closed_h(self.uid, cycle_id, c, self.exchange)
        
        # 从活跃周期删除
        if field in cycle_data:
            del cycle_data[field]
            pf_compat.set_pf_cycle(self.uid, cycle_data, self.exchange)
        
        logger.info(f"[{self.uid}][hyperliquid] 周期关闭: {field} netPnl={net}")
        
        # 触发返佣
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
            logger.warning(f"[{self.uid}][hyperliquid] 返佣触发失败: {e}")
