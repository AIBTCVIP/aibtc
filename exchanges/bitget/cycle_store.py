# exchanges/bitget/cycle_store.py
"""
Bitget WebSocket CycleStore - 管理 Bitget 交易周期数据

基于 Binance CycleStore 模式，适配 Bitget WebSocket 消息格式。

职责:
1. 接收 Bitget WebSocket 消息 (account/positions/orders)
2. 维护 Redis 中的仓位 (pf:pos) 和周期 (pf:cycle) 数据
3. 接收标记价格流，更新展示数据
"""

from __future__ import annotations

import json
import logging
import threading
import time
import uuid
from decimal import Decimal
from typing import Callable, Dict, Optional, TYPE_CHECKING

from exchanges.bitget.websocket import BitgetUserStream, ConnectionState
# 使用共享 MarkPrice 流适配器（多用户共享1个WebSocket连接）
from websocket.mark_price_adapters import BitgetMarkPriceStreamAdapter as BitgetMarkPriceStream
# 使用适配器，支持全局处理器优化（1000+用户规模）
from trading.stop_loss_adapter import StopLossManagerAdapter as StopLossManager
from core.pf_compatibility import pf_compat
from core.utils import D, now_ms, pos_field

if TYPE_CHECKING:
    from core.user_context import UserContext

logger = logging.getLogger(__name__)


class BitgetCycleStore:
    """
    Bitget WebSocket CycleStore
    
    事件处理:
    - account -> 账户余额更新
    - positions -> 持仓更新 + cycle open/close
    - orders (FILLED) -> cycle qty/quote/fee/realized
    - MarkPrice (via ticker) -> 展示数据更新
    """
    
    EXCHANGE_NAME = "bitget"
    PRODUCT_TYPE = "USDT-FUTURES"  # Bitget 产品类型
    
    def __init__(
        self,
        api_key: str,
        api_secret: str,
        passphrase: str,
        redis_conn,
        uid: str,
        *,
        is_testnet: bool = False,
        closed_maxlen: int = 20000,
        user_context: Optional['UserContext'] = None,
        stop_manager: Optional[StopLossManager] = None,
        on_auth_failed: Optional[Callable[[str], None]] = None,
    ):
        self.api_key = api_key
        self.api_secret = api_secret
        self.passphrase = passphrase
        self.is_testnet = is_testnet
        self.rds = redis_conn
        self.uid = uid
        self.closed_maxlen = closed_maxlen
        self._user_context = user_context
        self.exchange = self.EXCHANGE_NAME
        
        # 认证失败回调（用于自动停止交易所）
        self._on_auth_failed = on_auth_failed
        
        # 止损管理器（可外部注入，也可自动创建）
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
        
        self._user_stream: Optional[BitgetUserStream] = None
        self._mark_stream: Optional[BitgetMarkPriceStream] = None
        
        self._mark_updater_stop = None
        self._mark_updater_thread = None
        
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
    
    # ========== Mark Cycle Updater ==========
    
    def _start_mark_cycle_updater(self, interval_s: float = 1.0) -> None:
        """
        用于展示/统计更新（参考 Binance 实现）
        
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
            logger.debug(f"[{self.uid}][bitget] Mark updater thread started")
            update_count = 0
            
            while stop_event and not stop_event.is_set():
                try:
                    # 每次循环重新获取活跃持仓列表
                    fields = pf_compat.get_pf_pos_active(self.uid, self.exchange)
                    if not fields:
                        time.sleep(interval_s)
                        continue
                    
                    pos_data = pf_compat.get_pf_pos(self.uid, self.exchange)
                    cycle_data = pf_compat.get_pf_cycle(self.uid, self.exchange)
                    
                    ts = str(now_ms())
                    
                    # 累计所有持仓的未实现盈亏，用于更新 account
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
                        total_unrealized_pnl += live_pnl  # 累加
                        
                        lock_key = f"pf:lock:cycle:{self.uid}:{field}"
                        if not self.rds.set(lock_key, "1", nx=True, px=250):
                            continue
                        
                        try:
                            # 重新获取最新数据
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
                            
                            # DEBUG: 每30次更新报告一次（降级为 DEBUG）
                            update_count += 1
                            if update_count % 30 == 0:
                                logger.debug(f"[{self.uid}][bitget] Mark updater: {field} mark={mark} liveNetPnl={live_net_pnl:.4f}")
                            
                            if cycle_data:
                                cycle_data[field] = cyc
                                pf_compat.set_pf_cycle(self.uid, cycle_data, self.exchange)
                            
                            # 同步到 pos（与 Binance 一致）
                            pos_data = pf_compat.get_pf_pos(self.uid, self.exchange)
                            if field in pos_data:
                                pos2 = pos_data[field].copy()
                                pos2["liveNetPnl"] = str(live_net_pnl)
                                pos2["liveUnrealizedPnl"] = str(live_pnl)  # 实时未实现盈亏
                                pos2["unrealizedPnl"] = str(live_pnl)      # 同步更新展示字段
                                pos2["markPrice"] = str(mark)              # 当前标记价格
                                pos2["updatedAt"] = ts
                                pos_data[field] = pos2
                                pf_compat.set_pf_pos(self.uid, pos_data, self.exchange)
                        finally:
                            try:
                                self.rds.delete(lock_key)
                            except Exception:
                                pass
                    
                    # 更新 account 的 unrealized 字段（同步实时盈亏到账户卡片）
                    try:
                        account_data = pf_compat.get_pf_account(self.uid, self.exchange)
                        if account_data:
                            account_data["unrealized"] = str(total_unrealized_pnl)
                            # 同步更新 equity = walletBalance + unrealized
                            wallet = D(account_data.get("walletBalance", "0"))
                            account_data["equity"] = str(wallet + total_unrealized_pnl)
                            account_data["ts"] = ts
                            pf_compat.set_pf_account(self.uid, account_data, self.exchange)
                    except Exception:
                        pass
                    
                except Exception as e:
                    logger.warning(f"[{self.uid}][bitget] Mark updater error: {e}")
                
                time.sleep(interval_s)
        
        self._mark_updater_thread = threading.Thread(
            target=_run, name=f"bitget-mark-updater-{self.uid}", daemon=True
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
        """当前连接状态"""
        return self._connection_state
    
    def _on_state_change(self, state: ConnectionState, error: Optional[str] = None):
        """
        WebSocket 状态变化回调
        - 更新内部状态
        - 写入 Redis 供 ExchangeMonitor 读取
        - 认证失败时触发自动停止
        """
        self._connection_state = state
        try:
            # 写入 Redis
            status_key = f"pf:{self.uid}:{self.exchange}:ws_status"
            status_data = {
                "state": state.value,
                "error": error,
                "ts": now_ms(),
            }
            self.rds.set(status_key, json.dumps(status_data))
            self.rds.expire(status_key, 300)  # 5分钟过期
        except Exception as e:
            logger.debug(f"[{self.uid}][bitget] Failed to update ws status: {e}")
        
        # 认证失败时触发回调（自动停止交易所）
        if state == ConnectionState.AUTH_FAILED and self._on_auth_failed:
            logger.warning(f"[{self.uid}][bitget] Auth failed, triggering auto-stop callback")
            try:
                self._on_auth_failed(error or "Authentication failed")
            except Exception as e:
                logger.error(f"[{self.uid}][bitget] on_auth_failed callback error: {e}")
    
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
            
            # 记录状态变化日志
            if state in (ConnectionState.CONNECTED, ConnectionState.ERROR, ConnectionState.AUTH_FAILED):
                logger.info(f"[{self.uid}][bitget] Mark price WS state: {state.value}")
        except Exception as e:
            logger.debug(f"[{self.uid}][bitget] Failed to update mark ws status: {e}")
    
    # ========== 初始化方法 ==========
    
    def _init_existing_positions(self) -> None:
        """
        初始化现有仓位数据
        
        在启动时调用:
        1. 为缺少 openOrderType 的仓位设置默认值
        2. 从仓位 API 获取 TPSL 和盈亏平衡价
        """
        try:
            # 1. 检查并设置缺少 openOrderType 的仓位
            self._patch_missing_open_order_type()
            
            # 2. 从仓位 API 获取止盈止损和盈亏平衡价
            # 仓位 API 直接返回 takeProfit, stopLoss, breakEvenPrice
            self._fetch_and_update_tpsl_from_positions()
            
            logger.info(f"[{self.uid}][bitget] 初始化现有仓位数据完成")
            
        except Exception as e:
            logger.warning(f"[{self.uid}][bitget] 初始化现有仓位数据失败: {e}")
    
    def _patch_missing_open_order_type(self) -> None:
        """
        为缺少 openOrderType 的仓位设置默认值
        
        对于系统启动前已存在的仓位，无法获取开仓订单类型，
        设置为 "UNKNOWN" 以区分于新开仓位
        """
        try:
            pos_data = pf_compat.get_pf_pos(self.uid, self.exchange)
            cycle_data = pf_compat.get_pf_cycle(self.uid, self.exchange)
            
            logger.info(f"[{self.uid}][bitget] 检查 openOrderType: 共 {len(pos_data)} 个仓位")
            
            updated = False
            for field, pos in pos_data.items():
                current_type = pos.get("openOrderType")
                logger.info(f"[{self.uid}][bitget] 仓位 {field}: openOrderType={current_type}")
                
                if not current_type:
                    pos["openOrderType"] = "UNKNOWN"
                    pos_data[field] = pos
                    updated = True
                    logger.info(f"[{self.uid}][bitget] {field} openOrderType 设置为 UNKNOWN")
                    
                    # 同步到 cycle
                    if field in cycle_data:
                        cycle_data[field]["openOrderType"] = "UNKNOWN"
            
            if updated:
                pf_compat.set_pf_pos(self.uid, pos_data, self.exchange)
                pf_compat.set_pf_cycle(self.uid, cycle_data, self.exchange)
                logger.info(f"[{self.uid}][bitget] openOrderType 更新已保存")
                
        except Exception as e:
            logger.warning(f"[{self.uid}][bitget] 设置 openOrderType 失败: {e}")
    
    def _fetch_and_update_tpsl_from_positions(self) -> None:
        """
        从仓位 API 获取并更新止盈止损价格
        
        使用 /api/v2/mix/position/all-position 获取仓位信息
        该 API 直接返回 takeProfit, stopLoss, breakEvenPrice 字段
        """
        import requests
        import hmac
        import base64
        from hashlib import sha256
        from core.rate_limiter import get_bitget_rate_limiter
        
        try:
            # 使用 API Key 级别限速
            rate_limiter = get_bitget_rate_limiter(self.api_key)
            if not rate_limiter.acquire(endpoint="/api/v2/mix/position/all-position", timeout=30.0):
                logger.warning(f"[{self.uid}][bitget] 获取仓位 TPSL 限速超时")
                return
            
            timestamp = str(int(time.time() * 1000))
            method = "GET"
            base_url = "https://api.bitget.com"
            
            # 使用仓位 API 获取止盈止损信息
            request_path = f"/api/v2/mix/position/all-position?productType={self.PRODUCT_TYPE}"
            
            full_url = f"{base_url}{request_path}"
            logger.info(f"[{self.uid}][bitget] 获取仓位 TPSL: GET {full_url}")
            
            message = timestamp + method + request_path
            signature = hmac.new(
                self.api_secret.encode('utf-8'),
                message.encode('utf-8'),
                sha256
            ).digest()
            sign = base64.b64encode(signature).decode('utf-8')
            
            headers = {
                'ACCESS-KEY': self.api_key,
                'ACCESS-SIGN': sign,
                'ACCESS-TIMESTAMP': timestamp,
                'ACCESS-PASSPHRASE': self.passphrase,
                'Content-Type': 'application/json',
                'locale': 'en-US',
            }
            
            resp = requests.get(full_url, headers=headers, timeout=10)
            
            logger.info(f"[{self.uid}][bitget] 仓位 API 响应: HTTP {resp.status_code}")
            
            if resp.status_code != 200:
                try:
                    resp_text = resp.text[:500]
                except Exception:
                    resp_text = "(无法读取响应)"
                logger.warning(f"[{self.uid}][bitget] 获取仓位失败: HTTP {resp.status_code}, 响应: {resp_text}")
                return
            
            data = resp.json()
            
            if data.get('code') != '00000':
                logger.warning(f"[{self.uid}][bitget] 仓位 API 返回: code={data.get('code')} msg={data.get('msg')}")
                return
            
            positions = data.get('data', [])
            if not positions:
                logger.info(f"[{self.uid}][bitget] 无仓位数据")
                return
            
            # logger.info(f"[{self.uid}][bitget] 获取到 {len(positions)} 个仓位")
            
            # 更新 Redis 中的仓位数据
            pos_data = pf_compat.get_pf_pos(self.uid, self.exchange)
            cycle_data = pf_compat.get_pf_cycle(self.uid, self.exchange)
            updated = False
            
            for pos in positions:
                symbol = pos.get("symbol", "")
                hold_side = (pos.get("holdSide") or "").upper()
                
                if not symbol or hold_side not in ("LONG", "SHORT"):
                    continue
                
                total = float(pos.get("total", "0") or "0")
                if total <= 0:
                    continue
                
                field = f"{symbol}:{hold_side}"
                
                # 从仓位 API 获取的数据
                take_profit = pos.get("takeProfit") or ""
                stop_loss = pos.get("stopLoss") or ""
                break_even_price = pos.get("breakEvenPrice") or ""
                
                logger.info(f"[{self.uid}][bitget] 仓位 {field}: TP={take_profit} SL={stop_loss} BEP={break_even_price}")
                
                if field in pos_data:
                    # 更新止盈止损价格
                    if take_profit and take_profit != "null":
                        pos_data[field]["takeProfitPrice"] = str(take_profit)
                        updated = True
                    if stop_loss and stop_loss != "null":
                        pos_data[field]["stopLossPrice"] = str(stop_loss)
                        updated = True
                    if break_even_price and break_even_price != "null":
                        pos_data[field]["breakEvenPrice"] = str(break_even_price)
                        updated = True
                    
                    # 同步到 cycle
                    if field in cycle_data:
                        if take_profit and take_profit != "null":
                            cycle_data[field]["takeProfitPrice"] = str(take_profit)
                        if stop_loss and stop_loss != "null":
                            cycle_data[field]["stopLossPrice"] = str(stop_loss)
            
            if updated:
                pf_compat.set_pf_pos(self.uid, pos_data, self.exchange)
                pf_compat.set_pf_cycle(self.uid, cycle_data, self.exchange)
                logger.info(f"[{self.uid}][bitget] TPSL 数据已从仓位 API 更新")
            else:
                logger.info(f"[{self.uid}][bitget] 无 TPSL 数据需要更新")
                
        except Exception as e:
            import traceback
            logger.warning(f"[{self.uid}][bitget] 获取仓位 TPSL 失败: {e}")
            logger.exception("获取仓位 TPSL 失败")
    
    def _init_open_orders_cache(self) -> int:
        """
        初始化挂单缓存
        
        从 REST API 获取当前所有挂单，缓存到 Redis。
        后续通过 WebSocket orders 事件实时更新。
        
        Returns:
            挂单数量
        """
        import requests
        import hmac
        import base64
        from hashlib import sha256
        from core.rate_limiter import get_bitget_rate_limiter
        
        try:
            # 使用 API Key 级别限速
            rate_limiter = get_bitget_rate_limiter(self.api_key)
            if not rate_limiter.acquire(endpoint="/api/v2/mix/order/orders-pending", timeout=30.0):
                logger.warning(f"[{self.uid}][bitget] 获取挂单限速超时")
                return 0
            
            timestamp = str(int(time.time() * 1000))
            method = "GET"
            base_url = "https://api.bitget.com"
            request_path = f"/api/v2/mix/order/orders-pending?productType={self.PRODUCT_TYPE}"
            
            message = timestamp + method + request_path
            signature = hmac.new(
                self.api_secret.encode('utf-8'),
                message.encode('utf-8'),
                sha256
            ).digest()
            sign = base64.b64encode(signature).decode('utf-8')
            
            headers = {
                'ACCESS-KEY': self.api_key,
                'ACCESS-SIGN': sign,
                'ACCESS-TIMESTAMP': timestamp,
                'ACCESS-PASSPHRASE': self.passphrase,
                'Content-Type': 'application/json',
                'locale': 'en-US',
            }
            
            resp = requests.get(f"{base_url}{request_path}", headers=headers, timeout=10)
            
            if resp.status_code != 200:
                logger.warning(f"[{self.uid}][bitget] 获取挂单失败: HTTP {resp.status_code}")
                pf_compat.set_pf_open_orders(self.uid, {}, self.exchange)
                return 0
            
            data = resp.json()
            if data.get('code') != '00000':
                logger.warning(f"[{self.uid}][bitget] 获取挂单失败: {data.get('msg')}")
                pf_compat.set_pf_open_orders(self.uid, {}, self.exchange)
                return 0
            
            raw_orders = data.get('data', {}).get('entrustedList', [])
            if not raw_orders:
                pf_compat.set_pf_open_orders(self.uid, {}, self.exchange)
                return 0
            
            ts = now_ms()
            open_orders = {}
            
            for o in raw_orders:
                # 只缓存 LIMIT 入场单
                order_type = (o.get("orderType") or "").lower()
                if order_type != "limit":
                    continue
                
                # 跳过平仓单（tradeSide == "close"）
                trade_side = (o.get("tradeSide") or "").lower()
                if trade_side == "close":
                    continue
                
                order_id = str(o.get("orderId"))
                symbol = o.get("symbol", "")
                
                # 转换 symbol 格式
                # BTCUSDT_UMCBL -> BTCUSDT 或直接使用 instId
                normalized_symbol = symbol.replace("_UMCBL", "")
                
                # 转换 side 和 positionSide
                side = (o.get("side") or "").upper()
                pos_side = (o.get("posSide") or "long").lower()
                if pos_side == "net":
                    position_side = "LONG" if side == "BUY" else "SHORT"
                else:
                    position_side = pos_side.upper()
                
                open_orders[order_id] = {
                    "orderId": order_id,
                    "symbol": normalized_symbol,
                    "side": side,
                    "positionSide": position_side,
                    "price": str(o.get("price", "0")),
                    "origQty": str(o.get("size", "0")),
                    "executedQty": str(o.get("baseVolume", "0")),
                    "status": (o.get("status") or "").upper(),
                    "time": o.get("cTime"),
                    "updateTime": o.get("uTime"),
                    "cachedAt": ts,
                }
            
            pf_compat.set_pf_open_orders(self.uid, open_orders, self.exchange)
            logger.info(f"[{self.uid}][bitget] 挂单缓存初始化完成: {len(open_orders)} 个")
            return len(open_orders)
            
        except Exception as e:
            logger.warning(f"[{self.uid}][bitget] 初始化挂单缓存失败: {e}")
            logger.exception("初始化挂单缓存失败")
            return 0
    
    def _update_open_orders_cache(self, data: dict) -> None:
        """
        根据 orders WebSocket 事件更新挂单缓存
        
        Bitget orders 频道字段说明:
        - orderId: 订单 ID
        - instId: 合约 ID (BTCUSDT)
        - side: buy/sell
        - posSide: long/short/net
        - orderType: limit/market
        - price: 委托价格
        - size: 委托数量
        - accBaseVolume: 累计成交数量
        - status: live/partially_filled/filled/canceled
        - tradeSide: open/close (开平仓方向)
        """
        try:
            order_type = (data.get("orderType") or "").lower()
            status = (data.get("status") or "").lower()
            trade_side = (data.get("tradeSide") or "").lower()
            
            # 只处理限价入场单
            if order_type != "limit":
                return
            
            # 跳过平仓单
            if trade_side == "close":
                return
            
            order_id = str(data.get("orderId"))
            symbol = data.get("instId", "")
            
            open_orders = pf_compat.get_pf_open_orders(self.uid, self.exchange) or {}
            
            if status == "live":
                # 新订单或更新
                side = (data.get("side") or "").upper()
                pos_side = (data.get("posSide") or "long").lower()
                if pos_side == "net":
                    position_side = "LONG" if side == "BUY" else "SHORT"
                else:
                    position_side = pos_side.upper()
                
                open_orders[order_id] = {
                    "orderId": order_id,
                    "symbol": symbol,
                    "side": side,
                    "positionSide": position_side,
                    "price": str(data.get("price", "0")),
                    "origQty": str(data.get("size", "0")),
                    "executedQty": str(data.get("accBaseVolume", "0")),
                    "status": status.upper(),
                    "time": data.get("cTime"),
                    "updateTime": data.get("uTime"),
                    "cachedAt": now_ms(),
                }
                logger.debug(f"[{self.uid}][bitget] 挂单缓存更新: {symbol} {order_id}")
                
            elif status in ("canceled", "filled"):
                # 订单完成或取消，从缓存移除
                if order_id in open_orders:
                    del open_orders[order_id]
                    logger.debug(f"[{self.uid}][bitget] 挂单缓存移除: {symbol} {order_id} ({status})")
                
                # 如果是撤单，清理 ai_decision_id temp key
                if status == "canceled":
                    from core.pf_compatibility import cleanup_ai_decision_id_for_order
                    cleanup_ai_decision_id_for_order(self.uid, self.exchange, str(order_id))
                    
            elif status == "partially_filled":
                # 部分成交，更新成交数量
                if order_id in open_orders:
                    open_orders[order_id]["executedQty"] = str(data.get("accBaseVolume", "0"))
                    open_orders[order_id]["status"] = status.upper()
                    open_orders[order_id]["cachedAt"] = now_ms()
            
            pf_compat.set_pf_open_orders(self.uid, open_orders, self.exchange)
            
        except Exception as e:
            logger.warning(f"[{self.uid}][bitget] 更新挂单缓存失败: {e}")
    
    # ========== 生命周期 ==========
    
    def start(self) -> None:
        if self._user_stream:
            return
        
        logger.info(f"[{self.uid}][bitget] 启动 CycleStore (testnet={self.is_testnet})")
        
        # 初始化现有仓位的 TP/SL 数据
        self._init_existing_positions()
        
        # 初始化挂单缓存
        open_orders_count = self._init_open_orders_cache()
        
        # 用户数据流
        self._user_stream = BitgetUserStream(
            api_key=self.api_key,
            api_secret=self.api_secret,
            passphrase=self.passphrase,
            is_testnet=self.is_testnet,
            uid=self.uid,
            on_account=self._on_account,
            on_position=self._on_position,
            on_order=self._on_order,
            on_order_algo=self._on_order_algo,  # 计划委托回调（止盈止损）
            on_fill=self._on_fill,  # 成交明细回调 - 用于检测平仓
            on_state_change=self._on_state_change,  # 状态回调
        )
        self._user_stream.start()
        
        # 标记价格流
        # 现在 BitgetMarkPriceStream 会自动从 Redis 读取活跃持仓的 symbols
        # 没有持仓时不连接，symbols 变化时自动重连
        self._mark_stream = BitgetMarkPriceStream(
            redis_conn=self.rds,
            uid=self.uid,
            is_testnet=self.is_testnet,
            on_tick=self._on_mark_tick,
            on_state_change=self._on_mark_state_change,
        )
        self._mark_stream.start()
        
        # 展示层 updater
        self._start_mark_cycle_updater(interval_s=1.0)
        
        logger.info(f"[{self.uid}][bitget] CycleStore 已启动")
    
    def stop(self) -> None:
        if self._user_stream:
            self._user_stream.stop()
            self._user_stream = None
        if self._mark_stream:
            self._mark_stream.stop()
            self._mark_stream = None
        self._stop_mark_cycle_updater()
        
        # 清理止损管理器资源
        if hasattr(self.stop_manager, 'cleanup'):
            self.stop_manager.cleanup()
        
        logger.info(f"[{self.uid}][bitget] CycleStore 已停止")
    
    # ========== 标记价格处理 ==========
    
    def _on_mark_tick(self, symbol: str, mark_price: float, timestamp: int):
        """
        处理标记价格更新
        
        参考 Binance MarkPriceWS:
        1. 写入 Redis（供 UI / 兜底轮询读取）
        2. 设置 TTL（防止断流时前端用到陈旧 mark）
        3. 回调 stop_manager 处理移动止损
        """
        try:
            from core.database import RedisKeys
            import json
            
            price_key = RedisKeys.market_prices(symbol)
            price_data = {
                "markPrice": str(mark_price),
                "ts": str(timestamp),
            }
            self.rds.set(price_key, json.dumps(price_data, separators=(",", ":")))
            # 设置 TTL（15秒），与 Binance 一致
            self.rds.expire(price_key, 15)
            
            # DEBUG: 每100次打印一次，避免日志过多
            # Mark tick 日志改为 debug 级别，避免生产环境日志过多
            logger.debug(f"[{self.uid}][bitget] Mark tick: {symbol} = {mark_price}")
            
            # 委托给 stop_manager 处理移动止损
            self.stop_manager.on_mark_tick(symbol, D(str(mark_price)), timestamp, exchange=self.exchange)
        except Exception as e:
            logger.warning(f"[{self.uid}][bitget] Mark tick error: {e}")
    
    # ========== 账户更新处理 ==========
    
    def _on_account(self, data: dict) -> None:
        """
        处理 Bitget 账户更新
        
        Bitget 格式: {
            "marginCoin": "USDT",
            "locked": "0",
            "available": "1000",
            "crossedMaxAvailable": "1000",
            "fixedMaxAvailable": "1000",
            "maxTransferOut": "1000",
            "equity": "1000",
            "usdtEquity": "1000",
            "btcEquity": "0.02",
            "unrealizedPL": "0",
            ...
        }
        """
        try:
            ts = now_ms()
            margin_coin = (data.get("marginCoin") or "").upper()
            
            if margin_coin != "USDT":
                return
            
            wallet = D(data.get("usdtEquity", "0") or data.get("equity", "0") or "0")
            unrealized = D(data.get("unrealizedPL", "0") or "0")
            equity = wallet
            
            # 写初始权益快照
            existing_equity = pf_compat.get_pf_equity_init(self.uid, self.exchange)
            if equity > 0 and not existing_equity:
                obj = {
                    "uid": self.uid,
                    "ts": str(ts),
                    "walletBalance": self._d_to_str(equity),
                    "source": "BITGET_WS_ACCOUNT",
                    "exchange": self.exchange,
                }
                pf_compat.set_pf_equity_init(self.uid, obj, self.exchange)
            
            # 写当前账户数据
            account_obj = {
                "uid": self.uid,
                "ts": str(ts),
                "walletBalance": self._d_to_str(wallet),
                "equity": self._d_to_str(equity),
                "unrealized": self._d_to_str(unrealized),
                "source": "BITGET_WS",
                "exchange": self.exchange,
            }
            pf_compat.set_pf_account(self.uid, account_obj, self.exchange)
            
        except Exception as e:
            logger.warning(f"[{self.uid}][bitget] 账户更新处理失败: {e}")
    
    # ========== 持仓更新处理 ==========
    
    def _on_position(self, data: dict) -> None:
        """
        处理 Bitget 持仓更新
        
        Bitget 格式: {
            "instId": "BTCUSDT",        # 或 "symbol": "BTCUSDT"
            "posId": "123456789",
            "holdSide": "long",          # long/short
            "openPriceAvg": "50000",     # 平均开仓价
            "marginCoin": "USDT",
            "total": "1",                # 总持仓
            "available": "1",            # 可平仓
            "frozen": "0",               # 冻结
            "unrealizedPL": "100",       # 未实现盈亏
            "marginMode": "crossed",     # crossed/fixed
            "cTime": "1234567890000",
            ...
        }
        """
        try:
            # logger.info(f"[{self.uid}][bitget] _on_position 收到数据: {list(data.keys())}")
            # 使用消息中的时间戳，而不是当前时间（与 Binance 一致）
            ts = int(data.get("cTime") or data.get("uTime") or now_ms())
            
            # Bitget symbol 可能在 instId 或 symbol 字段
            symbol = data.get("instId") or data.get("symbol", "")
            
            total = float(data.get("total", "0") or "0")
            qty_abs = abs(total)
            
            # DEBUG: 记录所有持仓推送
            hold_side_raw = data.get("holdSide", "")
            # logger.info(f"[{self.uid}][bitget] 持仓推送: {symbol} {hold_side_raw} total={total} qty_abs={qty_abs}")
            
            # 确定方向
            hold_side = (data.get("holdSide") or "long").lower()
            side = hold_side.upper()
            
            field = pos_field(symbol, side)
            
            # 获取当前数据
            pos_data = pf_compat.get_pf_pos(self.uid, self.exchange)
            cycle_data = pf_compat.get_pf_cycle(self.uid, self.exchange)
            active_positions = pf_compat.get_pf_pos_active(self.uid, self.exchange)
            
            old = pos_data.get(field, {})
            old_qty = D(old.get("qty", "0")) if old else D("0")
            
            unrealized_pnl = D(data.get("unrealizedPL", "0") or "0")
            
            # 持仓为 0 -> 删除 + 关闭周期
            if qty_abs == 0:
                logger.info(f"[{self.uid}][bitget] 持仓平仓事件: {field} (old_qty={old_qty})")
                
                # ⭐ 在删除前保存持仓快照
                pos_snapshot = {field: pos_data[field].copy()} if field in pos_data else {}
                
                if field in pos_data:
                    del pos_data[field]
                    pf_compat.set_pf_pos(self.uid, pos_data, self.exchange)
                    logger.info(f"[{self.uid}][bitget] 已从 pos_data 删除: {field}")
                
                if field in active_positions:
                    active_positions.remove(field)
                    pf_compat.set_pf_pos_active(self.uid, active_positions, self.exchange)
                    logger.info(f"[{self.uid}][bitget] 已从 active_positions 删除: {field}")
                
                if old_qty != 0 and field in cycle_data:
                    logger.info(f"[{self.uid}][bitget] 调用 _close_cycle: {field}")
                    self._close_cycle(field, close_time_ms=ts, pos_data_snapshot=pos_snapshot)
                else:
                    logger.info(f"[{self.uid}][bitget] 跳过 _close_cycle: old_qty={old_qty}, in_cycle={field in cycle_data}")
                return
            
            # 读取 cycle 信息
            cycle_info = cycle_data.get(field, {})
            cycle_open_time = cycle_info.get("openTimeMs")
            cycle_open_order_type = cycle_info.get("openOrderType", "")
            cycle_open_tif = cycle_info.get("openTimeInForce", "")  # 与 Binance 一致
            
            # 盈亏平衡价 - Bitget WebSocket 可能推送 breakEvenPrice 或 achievedProfits
            # 如果没有，使用开仓均价作为近似值
            entry_price = D(data.get("openPriceAvg", "0") or "0")
            be_px = data.get("breakEvenPrice") or data.get("achievedProfits") or "0"
            if not be_px or be_px == "0":
                be_px = str(entry_price)
            
            pos_obj = {
                "symbol": symbol,
                "side": side,
                "qty": str(qty_abs),
                "entryPrice": str(entry_price),
                "breakEvenPrice": str(D(be_px or "0")),  # 盈亏平衡价
                "unrealizedPnl": str(unrealized_pnl),
                "marginType": "cross" if data.get("marginMode") == "crossed" else "isolated",
                "isolatedMargin": str(D(data.get("margin", "0") or "0")),
                "openTimeMs": cycle_open_time or str(data.get("cTime") or ts),
                "updatedAt": str(ts),
                "openOrderType": cycle_open_order_type,
                "openTimeInForce": cycle_open_tif,  # 与 Binance 一致
                "exchange": self.exchange,
                "leverage": str(data.get("leverage", "1")),
            }
            
            # 保留止盈止损价格
            if old:
                if old.get("stopLossPrice"):
                    pos_obj["stopLossPrice"] = old["stopLossPrice"]
                if old.get("takeProfitPrice"):
                    pos_obj["takeProfitPrice"] = old["takeProfitPrice"]
            
            # 更新持仓
            pos_data[field] = pos_obj
            pf_compat.set_pf_pos(self.uid, pos_data, self.exchange)
            
            if field not in active_positions:
                active_positions.append(field)
                pf_compat.set_pf_pos_active(self.uid, active_positions, self.exchange)
            
            # 0 -> non-0: 创建新周期
            if old_qty == 0 and field not in cycle_data:
                cycle_obj = self._new_cycle_dict(symbol, side, ts, D(str(qty_abs)), field)
                cycle_data[field] = cycle_obj
                pf_compat.set_pf_cycle(self.uid, cycle_data, self.exchange)
                logger.info(f"[{self.uid}][bitget] 新周期: {field}")
                
                # 动态添加标记价格订阅（如果尚未订阅）
                if self._mark_stream and symbol:
                    self._mark_stream.add_symbol(symbol)
            
        except Exception as e:
            logger.warning(f"[{self.uid}][bitget] 持仓更新处理失败: {e}")
            logger.exception("持仓更新处理失败")
    
    # ========== 止损止盈订单处理 ==========
    
    def _update_tp_sl_from_order(self, data: dict, status: str, order_type: str) -> None:
        """
        从止损止盈订单事件中更新持仓的 TP/SL 价格
        
        Bitget 止损止盈订单类型:
        - profit_loss: 止盈止损订单
        - pos_profit: 仓位止盈
        - pos_loss: 仓位止损
        - trigger: 计划委托/触发订单
        - oco: OCO订单
        """
        try:
            symbol = data.get("instId") or data.get("symbol", "")
            pos_side = (data.get("posSide") or data.get("holdSide") or "long").lower()
            
            if not symbol or pos_side not in ("long", "short"):
                return
            
            ps = pos_side.upper()
            field = f"{symbol}:{ps}"
            
            # 触发价格
            trigger_price = data.get("triggerPrice") or data.get("planPrice") or data.get("stopSurplusTriggerPrice") or data.get("stopLossTriggerPrice") or "0"
            
            # 获取当前持仓数据
            pos_data = pf_compat.get_pf_pos(self.uid, self.exchange)
            if field not in pos_data:
                return
            
            pos = pos_data[field]
            
            # 判断是止损还是止盈
            # Bitget: pos_loss / stopLossTriggerPrice = 止损, pos_profit / stopSurplusTriggerPrice = 止盈
            is_stop_loss = order_type in ("pos_loss",) or data.get("stopLossTriggerPrice")
            is_take_profit = order_type in ("pos_profit",) or data.get("stopSurplusTriggerPrice")
            
            # 对于 profit_loss 类型，需要根据价格和方向判断
            if order_type == "profit_loss" and not is_stop_loss and not is_take_profit:
                # 根据 planType 或其他字段判断
                plan_type = (data.get("planType") or "").lower()
                if "loss" in plan_type or "sl" in plan_type:
                    is_stop_loss = True
                elif "profit" in plan_type or "tp" in plan_type:
                    is_take_profit = True
            
            if status in ("live", "new", "not_trigger"):
                # 订单创建/未触发 - 记录价格
                if is_stop_loss and trigger_price:
                    pos["stopLossPrice"] = str(trigger_price)
                    logger.debug(f"[{self.uid}][bitget] {symbol} {ps} 止损订单创建: {trigger_price}")
                elif is_take_profit and trigger_price:
                    pos["takeProfitPrice"] = str(trigger_price)
                    logger.debug(f"[{self.uid}][bitget] {symbol} {ps} 止盈订单创建: {trigger_price}")
            
            elif status in ("canceled", "cancelled", "expired", "fail"):
                # 订单取消/过期 - 清除价格
                if is_stop_loss:
                    pos["stopLossPrice"] = None
                    logger.debug(f"[{self.uid}][bitget] {symbol} {ps} 止损订单取消")
                elif is_take_profit:
                    pos["takeProfitPrice"] = None
                    logger.debug(f"[{self.uid}][bitget] {symbol} {ps} 止盈订单取消")
            
            elif status in ("filled", "triggered"):
                # 订单触发成交 - 清除价格
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
                
        except Exception as e:
            logger.warning(f"[{self.uid}][bitget] 更新止损止盈失败: {e}")
    
    def _on_order_algo(self, data: dict) -> None:
        """
        处理 Bitget 计划委托更新 (orders-algo 频道)
        
        用于实时更新止盈止损价格
        
        planType:
        - ptp: 仓位止盈
        - psl: 仓位止损
        - pl: 计划委托
        - tp/sl: 部分止盈/止损
        
        status:
        - live: 创建订单
        - executed: 已执行
        - fail_execute: 执行失败
        - cancelled: 取消
        - executing: 执行中
        """
        try:
            # logger.info(f"[{self.uid}][bitget] orders-algo 推送: {data}")
            
            symbol = data.get("instId") or data.get("symbol", "")
            plan_type = (data.get("planType") or "").lower()
            status = (data.get("status") or "").lower()
            pos_side = (data.get("posSide") or "").lower()
            
            if not symbol or pos_side not in ("long", "short"):
                logger.debug(f"[{self.uid}][bitget] orders-algo 跳过: symbol={symbol} posSide={pos_side}")
                return
            
            ps = pos_side.upper()
            field = f"{symbol}:{ps}"
            
            # 获取触发价格
            # ptp/psl 使用 stopSurplusTriggerPrice / stopLossTriggerPrice
            trigger_price = "0"
            if plan_type == "ptp":
                trigger_price = data.get("stopSurplusTriggerPrice") or data.get("triggerPrice") or "0"
            elif plan_type == "psl":
                trigger_price = data.get("stopLossTriggerPrice") or data.get("triggerPrice") or "0"
            else:
                trigger_price = data.get("triggerPrice") or "0"
            
            logger.info(f"[{self.uid}][bitget] orders-algo: {field} planType={plan_type} status={status} triggerPrice={trigger_price}")
            
            # 获取当前持仓数据
            pos_data = pf_compat.get_pf_pos(self.uid, self.exchange)
            if field not in pos_data:
                logger.debug(f"[{self.uid}][bitget] orders-algo: {field} 不在持仓中")
                return
            
            pos = pos_data[field]
            cycle_data = pf_compat.get_pf_cycle(self.uid, self.exchange)
            
            # 根据状态和类型更新
            should_check_position = False
            
            if plan_type == "psl":
                # 仓位止损
                if status == "live":
                    if trigger_price and float(trigger_price) > 0:
                        pos["stopLossPrice"] = str(trigger_price)
                        logger.info(f"[{self.uid}][bitget] {field} 止损价 (WS): {trigger_price}")
                elif status in ("cancelled", "executed", "fail_execute"):
                    pos["stopLossPrice"] = None
                    logger.info(f"[{self.uid}][bitget] {field} 止损订单 {status}")
                    # 止损执行或取消可能意味着仓位已平，需要检查
                    if status == "executed":
                        should_check_position = True
                    
            elif plan_type == "ptp":
                # 仓位止盈
                if status == "live":
                    if trigger_price and float(trigger_price) > 0:
                        pos["takeProfitPrice"] = str(trigger_price)
                        logger.info(f"[{self.uid}][bitget] {field} 止盈价 (WS): {trigger_price}")
                elif status in ("cancelled", "executed", "fail_execute"):
                    pos["takeProfitPrice"] = None
                    logger.info(f"[{self.uid}][bitget] {field} 止盈订单 {status}")
                    # 止盈执行或取消可能意味着仓位已平，需要检查
                    if status == "executed":
                        should_check_position = True
            
            # 保存更新
            pos_data[field] = pos
            pf_compat.set_pf_pos(self.uid, pos_data, self.exchange)
            
            # 同步到 cycle
            if field in cycle_data:
                if plan_type == "psl":
                    cycle_data[field]["stopLossPrice"] = pos.get("stopLossPrice")
                elif plan_type == "ptp":
                    cycle_data[field]["takeProfitPrice"] = pos.get("takeProfitPrice")
                pf_compat.set_pf_cycle(self.uid, cycle_data, self.exchange)
            
            # 如果止盈/止损被执行，触发仓位检查
            # Bitget 有时不会推送 position 更新，需要主动查询
            if should_check_position:
                logger.info(f"[{self.uid}][bitget] TPSL 执行，触发仓位检查: {field}")
                self._check_position_after_tpsl(symbol, ps)
                
        except Exception as e:
            logger.warning(f"[{self.uid}][bitget] 处理 orders-algo 失败: {e}")
            logger.exception("处理 orders-algo 失败")
    
    def _on_fill(self, data: dict) -> None:
        """
        处理 Bitget 成交明细更新 (fill 频道)
        
        用于检测平仓成交，比 orders-algo 更可靠
        
        Bitget fill 数据结构:
        {
            "symbol": "BTCUSDT",
            "side": "buy",              # buy/sell
            "tradeSide": "close",       # open/close/reduce_close_long/burst_close_long 等
            "price": "51000.5",
            "baseVolume": "0.01",       # 成交数量
            "profit": "10.5",           # 收益
            "feeDetail": [{"fee": "-0.18", "feeCoin": "USDT"}],
            "posMode": "hedge_mode",    # 仓位模式
            "posSide": "long",          # 仓位方向
            "cTime": "1234567890000",
            "tradeId": "xxx",
            "orderId": "xxx"
        }
        
        tradeSide 平仓类型:
        - close: 普通平仓
        - reduce_close_long/short: 强制减仓
        - burst_close_long/short: 爆仓
        - offset_close_long/short: 抵消平仓
        - delivery_close_long/short: 交割平仓
        """
        try:
            logger.info(f"[{self.uid}][bitget] fill 推送: {data}")
            
            symbol = data.get("symbol") or data.get("instId", "")
            trade_side = (data.get("tradeSide") or "").lower()
            pos_side = (data.get("posSide") or "").lower()
            
            if not symbol:
                return
            
            # 判断仓位方向
            # 如果没有 posSide，根据 side 和 tradeSide 推断
            if not pos_side or pos_side not in ("long", "short"):
                side = (data.get("side") or "").lower()
                # 开多: buy+open, 平多: sell+close
                # 开空: sell+open, 平空: buy+close
                if "close" in trade_side:
                    if side == "sell":
                        pos_side = "long"
                    elif side == "buy":
                        pos_side = "short"
                elif "open" in trade_side:
                    if side == "buy":
                        pos_side = "long"
                    elif side == "sell":
                        pos_side = "short"
            
            if pos_side not in ("long", "short"):
                logger.debug(f"[{self.uid}][bitget] fill 无法确定仓位方向: {data}")
                return
            
            ps = pos_side.upper()
            field = f"{symbol}:{ps}"
            
            # 检查是否为平仓成交
            is_close_trade = "close" in trade_side
            
            fill_qty = D(data.get("baseVolume", "0") or data.get("size", "0") or "0")
            fill_price = D(data.get("price", "0") or "0")
            profit = D(data.get("profit", "0") or "0")
            
            # 解析手续费
            fee = D("0")
            fee_detail = data.get("feeDetail") or []
            for fd in fee_detail:
                fee += abs(D(fd.get("fee", "0") or "0"))
            
            trade_id = data.get("tradeId", "")
            order_id = data.get("orderId", "")
            t_ms = int(data.get("cTime") or now_ms())
            
            logger.info(f"[{self.uid}][bitget] fill: {field} tradeSide={trade_side} qty={fill_qty} price={fill_price} profit={profit} fee={fee}")
            
            if is_close_trade and fill_qty > 0:
                # 平仓成交 - 检查是否完全平仓
                logger.info(f"[{self.uid}][bitget] fill 检测到平仓成交: {field}")
                
                # 更新 cycle 的平仓数据
                cycle_data = pf_compat.get_pf_cycle(self.uid, self.exchange)
                if field in cycle_data:
                    c = cycle_data[field]
                    
                    # 去重检查 - 使用统一的 key 格式，避免与 _on_orders 重复处理
                    # 注意：_on_orders 使用 "oid:" 前缀，这里也使用相同格式
                    seen_trades = pf_compat.get_pf_seen_trades(self.uid)
                    # 使用 order_id 作为主键（与 _on_orders 一致），trade_id 作为备用
                    primary_id = order_id or trade_id
                    seen_member = f"{symbol}:{ps}:oid:{primary_id}:T:{t_ms}:qty:{fill_qty}"
                    
                    if seen_member not in seen_trades:
                        # 添加平仓成交
                        c = self._add_close_fill_to_cycle_dict(
                            c,
                            qty=fill_qty,
                            price=fill_price,
                            fee=fee,
                            realized=profit,
                            order_id=order_id,
                        )
                        c["updatedAt"] = str(t_ms)
                        
                        cycle_data[field] = c
                        pf_compat.set_pf_cycle(self.uid, cycle_data, self.exchange)
                        pf_compat.add_pf_seen_trades(self.uid, seen_member)
                        
                        logger.info(f"[{self.uid}][bitget] fill 已更新 cycle: {field} closeQty={c.get('closeQty')}")
                
                # 延迟检查仓位是否完全平仓
                # 使用线程避免阻塞 WebSocket
                def _delayed_check():
                    time.sleep(0.3)  # 等待可能的后续 fill
                    self._check_position_after_tpsl(symbol, ps)
                
                import threading
                t = threading.Thread(target=_delayed_check, daemon=True)
                t.start()
                
        except Exception as e:
            logger.warning(f"[{self.uid}][bitget] 处理 fill 失败: {e}")
            logger.exception("处理 fill 失败")
    
    def _check_position_after_tpsl(self, symbol: str, side: str) -> None:
        """
        TPSL 执行后检查仓位是否已平
        
        Bitget 有时不会推送 position WebSocket 更新，
        所以需要通过 REST API 主动查询仓位状态
        """
        import requests
        import hmac
        import base64
        from hashlib import sha256
        from core.rate_limiter import get_bitget_rate_limiter
        
        try:
            field = f"{symbol}:{side}"
            logger.info(f"[{self.uid}][bitget] 检查仓位状态: {field}")
            
            # 稍微延迟，等待交易所处理完成
            time.sleep(0.5)
            
            # 使用 API Key 级别限速
            rate_limiter = get_bitget_rate_limiter(self.api_key)
            if not rate_limiter.acquire(endpoint="/api/v2/mix/position/all-position", timeout=30.0):
                logger.warning(f"[{self.uid}][bitget] 检查仓位限速超时")
                return
            
            timestamp = str(int(time.time() * 1000))
            method = "GET"
            base_url = "https://api.bitget.com"
            request_path = f"/api/v2/mix/position/all-position?productType={self.PRODUCT_TYPE}"
            
            message = timestamp + method + request_path
            signature = hmac.new(
                self.api_secret.encode('utf-8'),
                message.encode('utf-8'),
                sha256
            ).digest()
            sign = base64.b64encode(signature).decode('utf-8')
            
            headers = {
                'ACCESS-KEY': self.api_key,
                'ACCESS-SIGN': sign,
                'ACCESS-TIMESTAMP': timestamp,
                'ACCESS-PASSPHRASE': self.passphrase,
                'Content-Type': 'application/json',
                'locale': 'en-US',
            }
            
            resp = requests.get(f"{base_url}{request_path}", headers=headers, timeout=10)
            
            if resp.status_code != 200:
                logger.warning(f"[{self.uid}][bitget] 仓位检查 API 失败: HTTP {resp.status_code}")
                return
            
            data = resp.json()
            if data.get('code') != '00000':
                logger.warning(f"[{self.uid}][bitget] 仓位检查 API 返回错误: {data.get('msg')}")
                return
            
            positions = data.get('data', [])
            
            # 检查目标仓位是否还存在
            position_exists = False
            for pos in positions:
                pos_symbol = pos.get("symbol", "")
                hold_side = (pos.get("holdSide") or "").upper()
                total = float(pos.get("total", "0") or "0")
                
                if pos_symbol == symbol and hold_side == side and total > 0:
                    position_exists = True
                    logger.info(f"[{self.uid}][bitget] 仓位仍存在: {field} total={total}")
                    break
            
            if not position_exists:
                logger.info(f"[{self.uid}][bitget] 仓位已不存在，执行平仓处理: {field}")
                
                # 获取当前 Redis 数据
                pos_data = pf_compat.get_pf_pos(self.uid, self.exchange)
                cycle_data = pf_compat.get_pf_cycle(self.uid, self.exchange)
                active_positions = pf_compat.get_pf_pos_active(self.uid, self.exchange)
                
                old_qty = D(pos_data.get(field, {}).get("qty", "0"))
                ts = now_ms()
                
                # ⭐ 在删除前保存持仓快照
                pos_snapshot = {field: pos_data[field].copy()} if field in pos_data else {}
                
                # 删除仓位
                if field in pos_data:
                    del pos_data[field]
                    pf_compat.set_pf_pos(self.uid, pos_data, self.exchange)
                    logger.info(f"[{self.uid}][bitget] 已从 pos_data 删除: {field}")
                
                # 从活跃列表删除
                if field in active_positions:
                    active_positions.remove(field)
                    pf_compat.set_pf_pos_active(self.uid, active_positions, self.exchange)
                    logger.info(f"[{self.uid}][bitget] 已从 active_positions 删除: {field}")
                
                # 关闭周期
                if old_qty != 0 and field in cycle_data:
                    logger.info(f"[{self.uid}][bitget] 关闭周期: {field}")
                    self._close_cycle(field, close_time_ms=ts, pos_data_snapshot=pos_snapshot)
                    
        except Exception as e:
            logger.warning(f"[{self.uid}][bitget] 检查仓位状态失败: {e}")
            logger.exception("检查仓位状态失败")
    
    # ========== 订单更新处理 ==========
    
    def _on_order(self, data: dict) -> None:
        """
        处理 Bitget 订单更新
        
        Bitget V2 WebSocket orders 频道格式:
        {
            "instId": "BTCUSDT",
            "orderId": "123456789",
            "clientOid": "client_order_id",
            "side": "buy",              # buy/sell (订单方向)
            "posSide": "long",          # long/short/net (持仓方向)
            "tradeSide": "open",        # open/close (开平仓方向) - 关键字段!
            "orderType": "limit",       # limit/market
            "status": "filled",         # live/partially_filled/filled/canceled
            "price": "50000",           # 委托价格
            "size": "1",                # 委托数量
            "accBaseVolume": "1",       # 累计成交数量
            "baseVolume": "0.1",        # 最新成交数量
            "priceAvg": "50000",        # 成交均价
            "pnl": "0",                 # 收益
            "feeDetail": [{"feeCoin": "USDT", "fee": "-0.05"}],  # 手续费数组
            "fillTime": "1234567890000",
            "uTime": "1234567890000",
            ...
        }
        """
        try:
            # DEBUG: 打印完整的订单数据
            # logger.info(f"[{self.uid}][bitget] orders 推送: {data}")
            
            status = (data.get("status") or "").lower()
            order_type = (data.get("orderType") or "").lower()
            
            # 更新挂单缓存（实时同步）
            self._update_open_orders_cache(data)
            
            # 处理止损止盈订单的创建/取消（这些类型不会出现在普通 orders 频道）
            if order_type in ("profit_loss", "pos_profit", "pos_loss", "trigger", "oco"):
                self._update_tp_sl_from_order(data, status, order_type)
            
            # 只处理已成交的订单
            if status not in ("filled", "partially_filled"):
                return
            
            # 成交数量: 优先用 accBaseVolume（累计），其次 baseVolume（最新）
            fill_qty = D(data.get("accBaseVolume", "0") or "0")
            if fill_qty <= 0:
                fill_qty = D(data.get("baseVolume", "0") or "0")
            if fill_qty <= 0:
                return
            
            t_ms = int(data.get("fillTime") or data.get("uTime") or now_ms())
            
            symbol = data.get("instId", "")
            if not symbol:
                logger.warning(f"[{self.uid}][bitget] orders 推送缺少 instId: {data}")
                return
            
            # 持仓方向
            pos_side = (data.get("posSide") or "long").lower()
            if pos_side == "net":
                # 单向持仓模式，根据 side 推断
                side = (data.get("side") or "").lower()
                pos_side = "long" if side == "buy" else "short"
            
            ps = pos_side.upper()
            if ps not in ("LONG", "SHORT"):
                logger.warning(f"[{self.uid}][bitget] 无效的持仓方向: {pos_side}")
                return
            
            # 成交价格: 优先用 priceAvg，其次 fillPrice，最后 price
            fill_px = D(data.get("priceAvg", "0") or "0")
            if fill_px <= 0:
                fill_px = D(data.get("fillPrice", "0") or data.get("price", "0") or "0")
            
            # 手续费: 从 feeDetail 数组中累加
            fee = D("0")
            fee_detail = data.get("feeDetail") or []
            for fd in fee_detail:
                fee += abs(D(fd.get("fee", "0") or "0"))
            
            realized = D(data.get("pnl", "0") or "0")
            
            order_id = data.get("orderId", "")
            
            # 判断开平仓: 直接使用 tradeSide 字段（比推断更可靠）
            trade_side = (data.get("tradeSide") or "").lower()
            is_open_trade = trade_side == "open" or trade_side.startswith("buy_single") or trade_side.startswith("sell_single")
            # 如果 tradeSide 包含 "close" 或其他平仓类型，则为平仓
            if "close" in trade_side:
                is_open_trade = False
            
            logger.info(f"[{self.uid}][bitget] 订单成交: {symbol}:{ps} tradeSide={trade_side} is_open={is_open_trade} qty={fill_qty} price={fill_px}")
            
            # 去重
            seen_trades = pf_compat.get_pf_seen_trades(self.uid)
            seen_member = f"{symbol}:{ps}:oid:{order_id}:T:{t_ms}:qty:{fill_qty}"
            
            if seen_member in seen_trades:
                return
            
            field = pos_field(symbol, ps)
            cycle_data = pf_compat.get_pf_cycle(self.uid, self.exchange)
            
            # 没有活跃周期
            if field not in cycle_data:
                if not is_open_trade:
                    self._backfill_close_trade(
                        field=field,
                        t_ms=t_ms,
                        qty=fill_qty,
                        price=fill_px,
                        fee=fee,
                        realized=realized,
                        order_id=order_id,
                    )
                    pf_compat.add_pf_seen_trades(self.uid, seen_member)
                    return
                
                c = self._new_cycle_dict(symbol, ps, t_ms, D("0"), field)
            else:
                c = cycle_data[field]
            
            # 先尝试确认 trailing pending（委托给 stop_manager，传递 exchange）
            self.stop_manager.confirm_trailing_from_order({
                "x": "TRADE",  # 模拟 Binance 格式
                "s": symbol,
                "ps": ps,
                "o": data.get("orderType", ""),
            }, exchange=self.exchange)
            
            # 记账
            if is_open_trade:
                # 检测是否为加仓（已有开仓数量 > 0）
                existing_open_qty = D(c.get("openQty", "0"))
                if existing_open_qty > 0:
                    self.stop_manager.reset_trailing_on_add_position(symbol, ps, exchange=self.exchange)
                
                c["openQty"] = str(D(c.get("openQty", "0")) + fill_qty)
                c["openQuote"] = str(D(c.get("openQuote", "0")) + fill_qty * fill_px)
                c["feeTotal"] = str(D(c.get("feeTotal", "0")) + fee)
                c["realizedPnlEst"] = str(D(c.get("realizedPnlEst", "0")) + realized)
                
                # 记录开仓订单类型
                # Bitget 使用 orderType 字段，值为 limit/market（小写）
                # force 字段: gtc/fok/ioc 等（与 Binance 的 timeInForce 对应）
                if not c.get("openOrderType"):
                    order_type_raw = data.get("orderType", "")
                    order_type = order_type_raw.upper() if order_type_raw else ""
                    c["openOrderType"] = order_type
                    # Bitget 的 force 字段对应 Binance 的 timeInForce
                    c["openTimeInForce"] = (data.get("force") or "").upper()
                    c["openOrderId"] = str(order_id)
                    c["openClientOid"] = str(data.get("clientOid", ""))
                    logger.info(f"[{self.uid}][bitget][ORDER_META] {symbol} {ps} orderType={order_type} force={c.get('openTimeInForce')}")
                
                # 检查是否有关联的 AI 决策 ID (moved outside openOrderType check to handle race conditions)
                if not c.get("aiDecisionId"):
                    from core.pf_compatibility import consume_ai_decision_id_for_order, consume_ai_decision_id_for_market
                    
                    # 先尝试限价单 key
                    ai_decision_id = consume_ai_decision_id_for_order(
                        self.uid, self.exchange, str(order_id)
                    )
                    
                    # 如果没有，尝试市价单 key
                    if not ai_decision_id:
                        ai_decision_id = consume_ai_decision_id_for_market(
                            self.uid, self.exchange, symbol, ps
                        )
                    
                    if ai_decision_id:
                        c["aiDecisionId"] = str(ai_decision_id)
                        logger.info(f"[{self.uid}][bitget][AI_DECISION] {symbol} {ps} linked to ai_decision_id={ai_decision_id}")
                
                # 初始化 openStopordersFired 标志
                if "openStopordersFired" not in c:
                    c["openStopordersFired"] = "0"
                
                # 设置初始止盈止损（委托给 stop_manager）
                if c.get("openStopordersFired", "0") != "1":
                    oq = D(c.get("openQty", "0"))
                    entry = (D(c.get("openQuote", "0")) / oq) if oq else D("0")
                    
                    # 获取订单类型
                    order_type = c.get("openOrderType") or ""
                    
                    # 传入 order_type 和 exchange 参数
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
                    qty=fill_qty,
                    price=fill_px,
                    fee=fee,
                    realized=realized,
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
            
            # 写入 Redis（带锁）
            lock_key = f"pf:lock:cycle:{self.uid}:{field}"
            got = self.rds.set(lock_key, "1", nx=True, px=800)
            if not got:
                time.sleep(0.05)
                got = self.rds.set(lock_key, "1", nx=True, px=800)
            
            try:
                cycle_data[field] = c
                pf_compat.set_pf_cycle(self.uid, cycle_data, self.exchange)
                
                # 更新 pos 的 openOrderType 和 openTimeInForce（与 Binance 一致）
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
            
            logger.debug(f"[{self.uid}][bitget] 订单成交: {field} {'开仓' if is_open_trade else '平仓'} qty={fill_qty}")
            
        except Exception as e:
            logger.warning(f"[{self.uid}][bitget] 订单更新处理失败: {e}")
            logger.exception("订单更新处理失败")
    
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
        """添加平仓成交到周期"""
        c["closeQty"] = str(D(c.get("closeQty", "0")) + qty)
        c["closeQuote"] = str(D(c.get("closeQuote", "0")) + qty * price)
        
        if "closeOrderIds" not in c:
            c["closeOrderIds"] = "[]"
        if "closeTradeCount" not in c:
            c["closeTradeCount"] = "0"
        
        if order_id:
            try:
                ids = json.loads(c.get("closeOrderIds") or "[]")
                s = set(ids)
            except Exception:
                s = set()
            s.add(str(order_id))
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
    
    def _backfill_close_trade(
        self,
        *,
        field: str,
        t_ms: int,
        qty: Decimal,
        price: Decimal,
        fee: Decimal,
        realized: Decimal,
        order_id: str,
    ) -> bool:
        """回补平仓交易到最近的已关闭周期"""
        try:
            closed_h = pf_compat.get_pf_closed_h(self.uid, self.exchange)
            if not closed_h:
                return self._create_backfill_record(field, t_ms, qty, price, fee, realized, order_id)
            
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
            
            candidates.sort(key=lambda x: x[2], reverse=True)
            cycle_id, c, _ = candidates[0]
            
            c["updatedAt"] = str(max(int(c.get("updatedAt", "0") or "0"), t_ms))
            c = self._add_close_fill_to_cycle_dict(c, qty=qty, price=price, fee=fee, realized=realized, order_id=order_id)
            
            closed_h[cycle_id] = c
            pf_compat.set_pf_closed_h(self.uid, cycle_id, c, self.exchange)
            
            logger.info(f"[{self.uid}][bitget][BACKFILL] 更新已关闭周期 {cycle_id}")
            
            # ⚠️ 注意：更新已有周期时，不触发排行榜更新
            # 因为这个周期之前通过 _close_cycle 关闭时已经触发过排行榜更新了
            # 如果再次触发会导致重复计算（total_trades 和 net_profit 会被重复累加）
            # 增量的 pnl 变化会在下次对账时自动修正
            
            return True
            
        except Exception as e:
            logger.warning(f"[{self.uid}][bitget][BACKFILL] 查找失败: {e}")
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
            logger.info(f"[{self.uid}][bitget][BACKFILL] 创建新记录 {new_cycle_id}")
            
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
                logger.info(f"[{self.uid}][bitget][BACKFILL] 排行榜已更新: {new_cycle_id} pnl={net_pnl}")
            except Exception as e:
                logger.warning(f"[{self.uid}][bitget][BACKFILL] 排行榜更新失败: {e}")
            
            return True
            
        except Exception as e:
            logger.error(f"[{self.uid}][bitget][BACKFILL] 创建失败: {e}")
            return False
    
    def _close_cycle(self, field: str, close_time_ms: int, pos_data_snapshot: dict = None) -> None:
        """关闭周期"""
        logger.info(f"[{self.uid}][bitget] _close_cycle 开始: {field}")
        cycle_data = pf_compat.get_pf_cycle(self.uid, self.exchange)
        
        c = cycle_data.get(field)
        if not c:
            logger.warning(f"[{self.uid}][bitget] _close_cycle: {field} 不在 cycle_data 中")
            return
        
        # 确保 symbol 和 side 存在（从 field 解析）
        if not c.get("symbol") or not c.get("side"):
            try:
                parts = field.split(":")
                if len(parts) >= 2:
                    c["symbol"] = c.get("symbol") or parts[0]
                    c["side"] = c.get("side") or parts[1]
                    logger.info(f"[{self.uid}][bitget] Parsed symbol/side from field: {field}")
            except Exception as e:
                logger.warning(f"[{self.uid}][bitget] Failed to parse field {field}: {e}")
        
        # ⭐ 检查关键字段是否缺失，如果缺失则尝试从持仓快照回补
        open_time_ms = int(c.get("openTimeMs", "0") or "0")
        open_qty = D(c.get("openQty", "0") or "0")
        avg_open_price = D(c.get("avgOpenPrice", "0") or "0")
        
        data_incomplete = (open_time_ms == 0 or open_qty == 0 or avg_open_price == 0)
        
        if data_incomplete:
            logger.warning(
                f"[{self.uid}][bitget] Cycle data incomplete for {field}: "
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
                        logger.info(f"[{self.uid}][bitget] Recovered openTimeMs from pos snapshot: {open_time_ms}")
                
                if open_qty == 0:
                    pos_qty = D(pos.get("qty", "0") or "0")
                    if pos_qty > 0:
                        c["openQty"] = str(pos_qty)
                        c["closeQty"] = str(pos_qty)
                        c["maxAbsQty"] = str(pos_qty)
                        open_qty = pos_qty
                        logger.info(f"[{self.uid}][bitget] Recovered qty from pos snapshot: {open_qty}")
                
                if avg_open_price == 0:
                    pos_entry = D(pos.get("entryPrice", "0") or "0")
                    if pos_entry > 0:
                        c["avgOpenPrice"] = str(pos_entry)
                        c["openQuote"] = str(open_qty * pos_entry) if open_qty > 0 else "0"
                        avg_open_price = pos_entry
                        logger.info(f"[{self.uid}][bitget] Recovered entryPrice from pos snapshot: {avg_open_price}")
        
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
        
        # 确保所有必要字段存在
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
        
        logger.info(f"[{self.uid}][bitget] 周期关闭: {field} netPnl={net}")
        
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
            logger.warning(f"[{self.uid}][bitget] 返佣触发失败: {e}")
