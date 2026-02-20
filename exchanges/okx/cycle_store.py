# exchanges/okx/cycle_store.py
"""
OKX WebSocket CycleStore - 管理 OKX 交易周期数据

基于 Binance CycleStore 模式，适配 OKX WebSocket 消息格式。

职责:
1. 接收 OKX WebSocket 消息 (account/positions/orders)
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

from exchanges.okx.websocket import OKXUserStream, ConnectionState
# 使用共享 MarkPrice 流适配器（多用户共享1个WebSocket连接）
from websocket.mark_price_adapters import OKXMarkPriceStreamAdapter as OKXMarkPriceStream
# 使用适配器，支持全局处理器优化（1000+用户规模）
from trading.stop_loss_adapter import StopLossManagerAdapter as StopLossManager
from core.pf_compatibility import pf_compat
from core.utils import D, now_ms, pos_field

if TYPE_CHECKING:
    from core.user_context import UserContext

logger = logging.getLogger(__name__)


class OKXCycleStore:
    """
    OKX WebSocket CycleStore
    
    事件处理:
    - account -> 账户余额更新
    - positions -> 持仓更新 + cycle open/close
    - orders (FILLED) -> cycle qty/quote/fee/realized
    - MarkPrice -> 展示数据更新
    
    止损管理:
    - 开仓后自动设置初始止盈止损
    - 移动止损 (Trailing Stop)
    """
    
    EXCHANGE_NAME = "okx"
    
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
        
        self._user_stream: Optional[OKXUserStream] = None
        self._mark_stream: Optional[OKXMarkPriceStream] = None
        
        self._mark_updater_stop = None
        self._mark_updater_thread = None
        
        # 缓存最近的订单信息用于匹配成交
        self._recent_orders: Dict[str, dict] = {}
        self._orders_lock = threading.Lock()
        
        # 缓存订单的累计值（用于计算增量）
        # key: orderId, value: {"accFillSz": Decimal, "accFee": Decimal, "accPnl": Decimal}
        self._order_acc_cache: Dict[str, dict] = {}
        
        # 连接状态
        self._connection_state = ConnectionState.DISCONNECTED
        
        # 缓存合约乘数 (instId -> ctVal)
        # 用于将张数转换为币数量
        self._ct_val_cache: Dict[str, float] = {}
    
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
    
    def _convert_inst_id_to_symbol(self, inst_id: str) -> str:
        """
        将 OKX instId 转换为标准 symbol
        例如: BTC-USDT-SWAP -> BTCUSDT
        """
        parts = inst_id.split("-")
        if len(parts) >= 2:
            return f"{parts[0]}{parts[1]}"
        return inst_id
    
    def _get_ct_val(self, inst_id: str) -> float:
        """
        获取合约乘数 (ctVal)
        
        OKX 持仓返回的 pos 是张数，需要乘以 ctVal 得到币数量
        例如：
        - BTC-USDT-SWAP: ctVal = 0.01，10 张 = 0.1 BTC
        - ETH-USDT-SWAP: ctVal = 0.1，10 张 = 1 ETH
        
        使用全局公共缓存，避免每个用户重复请求
        """
        if inst_id in self._ct_val_cache:
            return self._ct_val_cache[inst_id]
        
        # 使用全局公共缓存获取
        ct_val = 1.0
        try:
            from core.okx_public_cache import get_okx_public_cache
            cache = get_okx_public_cache()
            ct_val = cache.get_ct_val(inst_id)
            if ct_val != 1.0:
                logger.debug(f"[{self.uid}][okx] {inst_id} ctVal = {ct_val} (from cache)")
        except Exception as e:
            logger.warning(f"[{self.uid}][okx] 获取 {inst_id} ctVal 失败: {e}, 使用默认值 1.0")
        
        self._ct_val_cache[inst_id] = ct_val
        return ct_val
    
    def _contracts_to_coins(self, inst_id: str, contracts: float) -> float:
        """
        将张数转换为币数量
        """
        ct_val = self._get_ct_val(inst_id)
        coins = contracts * ct_val
        return coins
    
    def _update_peak_and_drawdown(self, c: dict, current_pnl=None):
        """
        更新峰值收益和最大回撤
        """
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
            logger.info(f"[{self.uid}][okx] Mark updater thread started")
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
                                logger.debug(f"[{self.uid}][okx] Mark updater: {field} mark={mark} liveNetPnl={live_net_pnl:.4f}")
                            
                            if cycle_data:
                                cycle_data[field] = cyc
                                pf_compat.set_pf_cycle(self.uid, cycle_data, self.exchange)
                            
                            # 同步到 pos（与 Binance/Bitget 一致）
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
                    logger.warning(f"[{self.uid}][okx] Mark updater error: {e}")
                
                time.sleep(interval_s)
        
        self._mark_updater_thread = threading.Thread(
            target=_run, name=f"okx-mark-updater-{self.uid}", daemon=True
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
            logger.debug(f"[{self.uid}][okx] Failed to update ws status: {e}")
        
        # 认证失败时触发回调（自动停止交易所）
        if state == ConnectionState.AUTH_FAILED and self._on_auth_failed:
            logger.warning(f"[{self.uid}][okx] Auth failed, triggering auto-stop callback")
            try:
                self._on_auth_failed(error or "Authentication failed")
            except Exception as e:
                logger.error(f"[{self.uid}][okx] on_auth_failed callback error: {e}")
    
    # ========== 初始化方法 ==========
    
    def _init_existing_positions(self) -> None:
        """
        初始化现有仓位的盈亏平衡价
        
        在启动时调用，通过 REST API 获取完整的仓位数据（包含 bePx）
        
        注意：止盈止损价格由 stop_loss_manager 管理，不在这里处理
        """
        try:
            # 根据 testnet 选择正确的 URL
            base_url = "https://www.okx.com"  # OKX 测试网也用主网 URL，通过 header 区分
            
            # 获取现有持仓（包含 bePx 盈亏平衡价）
            self._fetch_and_update_positions(base_url)
            
            logger.info(f"[{self.uid}][okx] 初始化现有仓位数据完成")
            
        except Exception as e:
            logger.warning(f"[{self.uid}][okx] 初始化现有仓位数据失败: {e}")
    
    def _fetch_and_update_positions(self, base_url: str) -> None:
        """获取并更新仓位数据（包含盈亏平衡价）"""
        import requests
        import hmac
        import base64
        from hashlib import sha256
        from datetime import datetime, timezone
        from core.rate_limiter import get_okx_rate_limiter
        
        try:
            # 使用 API Key 级别限速
            rate_limiter = get_okx_rate_limiter(self.api_key)
            if not rate_limiter.acquire(endpoint="/api/v5/account/positions", timeout=30.0):
                logger.warning(f"[{self.uid}][okx] 获取仓位限速超时")
                return
            
            # 构建签名
            timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
            method = "GET"
            request_path = "/api/v5/account/positions?instType=SWAP"
            
            message = timestamp + method + request_path
            signature = hmac.new(
                self.api_secret.encode('utf-8'),
                message.encode('utf-8'),
                sha256
            ).digest()
            sign = base64.b64encode(signature).decode('utf-8')
            
            headers = {
                'OK-ACCESS-KEY': self.api_key,
                'OK-ACCESS-SIGN': sign,
                'OK-ACCESS-TIMESTAMP': timestamp,
                'OK-ACCESS-PASSPHRASE': self.passphrase,
                'Content-Type': 'application/json',
            }
            
            if self.is_testnet:
                headers['x-simulated-trading'] = '1'
            
            resp = requests.get(f"{base_url}{request_path}", headers=headers, timeout=10)
            
            if resp.status_code != 200:
                logger.warning(f"[{self.uid}][okx] 获取仓位失败: HTTP {resp.status_code}")
                return
            
            data = resp.json()
            if data.get('code') != '0':
                logger.warning(f"[{self.uid}][okx] 获取仓位失败: {data.get('msg')}")
                return
            
            positions = data.get('data', [])
            if not positions:
                return
            
            # 更新仓位数据
            pos_data = pf_compat.get_pf_pos(self.uid, self.exchange)
            updated = False
            
            for p in positions:
                pos_contracts = float(p.get("pos", 0))
                if pos_contracts == 0:
                    continue
                
                inst_id = p.get("instId", "")
                symbol = self._convert_inst_id_to_symbol(inst_id)
                
                pos_side = (p.get("posSide") or "net").lower()
                if pos_side == "net":
                    side = "LONG" if pos_contracts > 0 else "SHORT"
                else:
                    side = pos_side.upper()
                
                field = f"{symbol}:{side}"
                
                if field in pos_data:
                    # 更新盈亏平衡价
                    be_px = p.get("bePx") or "0"
                    if be_px and be_px != "0":
                        pos_data[field]["breakEvenPrice"] = str(be_px)
                        updated = True
                        logger.debug(f"[{self.uid}][okx] {field} 盈亏平衡价: {be_px}")
            
            if updated:
                pf_compat.set_pf_pos(self.uid, pos_data, self.exchange)
                
        except Exception as e:
            logger.warning(f"[{self.uid}][okx] 获取仓位数据失败: {e}")
    
    def _init_existing_algo_orders(self) -> None:
        """
        初始化已有的止盈止损订单
        
        在启动时调用，通过 REST API 获取现有的策略订单（止盈止损）
        并更新到持仓数据中
        
        OKX API: GET /api/v5/trade/orders-algo-pending
        参数: ordType=conditional (条件单/止盈止损)
        """
        try:
            import requests
            import hmac
            import base64
            from hashlib import sha256
            from datetime import datetime, timezone
            from core.rate_limiter import get_okx_rate_limiter
            
            # 使用 API Key 级别限速
            rate_limiter = get_okx_rate_limiter(self.api_key)
            if not rate_limiter.acquire(endpoint="/api/v5/trade/orders-algo-pending", timeout=30.0):
                logger.warning(f"[{self.uid}][okx] 获取策略订单限速超时")
                return
            
            base_url = "https://www.okx.com"
            
            # 获取策略订单
            timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
            method = "GET"
            # 获取所有待执行的策略订单（条件单类型）
            # ordType: conditional(条件单), oco(OCO), trigger(计划委托), move_order_stop(移动止损)
            request_path = "/api/v5/trade/orders-algo-pending?ordType=conditional"
            
            message = timestamp + method + request_path
            signature = hmac.new(
                self.api_secret.encode('utf-8'),
                message.encode('utf-8'),
                sha256
            ).digest()
            sign = base64.b64encode(signature).decode('utf-8')
            
            headers = {
                'OK-ACCESS-KEY': self.api_key,
                'OK-ACCESS-SIGN': sign,
                'OK-ACCESS-TIMESTAMP': timestamp,
                'OK-ACCESS-PASSPHRASE': self.passphrase,
                'Content-Type': 'application/json',
            }
            
            if self.is_testnet:
                headers['x-simulated-trading'] = '1'
            
            resp = requests.get(f"{base_url}{request_path}", headers=headers, timeout=10)
            
            if resp.status_code != 200:
                logger.warning(f"[{self.uid}][okx] 获取策略订单失败: HTTP {resp.status_code} - {resp.text[:200]}")
                return
            
            data = resp.json()
            if data.get('code') != '0':
                logger.warning(f"[{self.uid}][okx] 获取策略订单失败: {data.get('msg')}")
                return
            
            orders = data.get('data', [])
            if not orders:
                logger.info(f"[{self.uid}][okx] 无待执行的策略订单")
                return
            
            # 更新持仓的止盈止损价格
            pos_data = pf_compat.get_pf_pos(self.uid, self.exchange)
            cycle_data = pf_compat.get_pf_cycle(self.uid, self.exchange)
            updated = False
            
            for o in orders:
                inst_id = o.get("instId", "")
                symbol = self._convert_inst_id_to_symbol(inst_id)
                pos_side = (o.get("posSide") or "net").lower()
                
                if pos_side not in ("long", "short"):
                    continue
                
                ps = pos_side.upper()
                field = f"{symbol}:{ps}"
                
                if field not in pos_data:
                    continue
                
                # 获取止损止盈价格
                sl_trigger_px = o.get("slTriggerPx")
                tp_trigger_px = o.get("tpTriggerPx")
                
                if sl_trigger_px:
                    pos_data[field]["stopLossPrice"] = str(sl_trigger_px)
                    if field in cycle_data:
                        cycle_data[field]["stopLossPrice"] = str(sl_trigger_px)
                    updated = True
                    logger.info(f"[{self.uid}][okx] {field} 止损价: {sl_trigger_px}")
                
                if tp_trigger_px:
                    pos_data[field]["takeProfitPrice"] = str(tp_trigger_px)
                    if field in cycle_data:
                        cycle_data[field]["takeProfitPrice"] = str(tp_trigger_px)
                    updated = True
                    logger.info(f"[{self.uid}][okx] {field} 止盈价: {tp_trigger_px}")
            
            if updated:
                pf_compat.set_pf_pos(self.uid, pos_data, self.exchange)
                pf_compat.set_pf_cycle(self.uid, cycle_data, self.exchange)
                logger.info(f"[{self.uid}][okx] 止盈止损订单初始化完成")
                
        except Exception as e:
            logger.warning(f"[{self.uid}][okx] 获取策略订单失败: {e}")
    
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
        from datetime import datetime, timezone
        from core.rate_limiter import get_okx_rate_limiter
        from core.okx_public_cache import get_okx_public_cache
        
        try:
            # 使用 API Key 级别限速
            rate_limiter = get_okx_rate_limiter(self.api_key)
            if not rate_limiter.acquire(endpoint="/api/v5/trade/orders-pending", timeout=30.0):
                logger.warning(f"[{self.uid}][okx] 获取挂单限速超时")
                return 0
            
            base_url = "https://www.okx.com"
            
            # 构建签名
            timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
            method = "GET"
            request_path = "/api/v5/trade/orders-pending?instType=SWAP"
            
            message = timestamp + method + request_path
            signature = hmac.new(
                self.api_secret.encode('utf-8'),
                message.encode('utf-8'),
                sha256
            ).digest()
            sign = base64.b64encode(signature).decode('utf-8')
            
            headers = {
                'OK-ACCESS-KEY': self.api_key,
                'OK-ACCESS-SIGN': sign,
                'OK-ACCESS-TIMESTAMP': timestamp,
                'OK-ACCESS-PASSPHRASE': self.passphrase,
                'Content-Type': 'application/json',
            }
            
            if self.is_testnet:
                headers['x-simulated-trading'] = '1'
            
            resp = requests.get(f"{base_url}{request_path}", headers=headers, timeout=10)
            
            if resp.status_code != 200:
                logger.warning(f"[{self.uid}][okx] 获取挂单失败: HTTP {resp.status_code}")
                pf_compat.set_pf_open_orders(self.uid, {}, self.exchange)
                return 0
            
            data = resp.json()
            if data.get('code') != '0':
                logger.warning(f"[{self.uid}][okx] 获取挂单失败: {data.get('msg')}")
                pf_compat.set_pf_open_orders(self.uid, {}, self.exchange)
                return 0
            
            raw_orders = data.get('data', [])
            if not raw_orders:
                pf_compat.set_pf_open_orders(self.uid, {}, self.exchange)
                return 0
            
            ts = now_ms()
            open_orders = {}
            okx_cache = get_okx_public_cache()
            
            for o in raw_orders:
                # 只缓存 LIMIT 入场单
                ord_type = (o.get("ordType") or "").lower()
                if ord_type not in ("limit", "post_only"):
                    continue
                
                # 跳过 reduceOnly 订单（平仓单）
                reduce_only = o.get("reduceOnly") == "true"
                if reduce_only:
                    continue
                
                order_id = str(o.get("ordId"))
                inst_id = o.get("instId", "")
                symbol = self._convert_inst_id_to_symbol(inst_id)
                
                # 获取合约乘数，将张数转为币数量
                ct_val = okx_cache.get_ct_val(inst_id)
                sz_contracts = float(o.get("sz") or 0)
                filled_contracts = float(o.get("accFillSz") or 0)
                sz_coins = sz_contracts * ct_val
                filled_coins = filled_contracts * ct_val
                
                # 转换 side 和 positionSide
                side = "BUY" if o.get("side") == "buy" else "SELL"
                pos_side = (o.get("posSide") or "net").lower()
                if pos_side == "net":
                    position_side = "LONG" if side == "BUY" else "SHORT"
                else:
                    position_side = pos_side.upper()
                
                open_orders[order_id] = {
                    "orderId": order_id,
                    "symbol": symbol,
                    "instId": inst_id,
                    "side": side,
                    "positionSide": position_side,
                    "price": str(o.get("px", "0")),
                    "origQty": str(sz_coins),
                    "executedQty": str(filled_coins),
                    "status": o.get("state", "").upper(),
                    "time": o.get("cTime"),
                    "updateTime": o.get("uTime"),
                    "cachedAt": ts,
                }
            
            pf_compat.set_pf_open_orders(self.uid, open_orders, self.exchange)
            logger.info(f"[{self.uid}][okx] 挂单缓存初始化完成: {len(open_orders)} 个")
            return len(open_orders)
            
        except Exception as e:
            logger.warning(f"[{self.uid}][okx] 初始化挂单缓存失败: {e}")
            logger.exception("初始化挂单缓存失败")
            return 0
    
    def _update_open_orders_cache(self, data: dict) -> None:
        """
        根据 orders WebSocket 事件更新挂单缓存
        
        OKX orders 频道字段说明:
        - ordId: 订单 ID
        - instId: 合约 ID (BTC-USDT-SWAP)
        - side: buy/sell
        - posSide: long/short/net
        - ordType: limit/market/post_only/...
        - px: 委托价格
        - sz: 委托数量（张数）
        - accFillSz: 累计成交数量（张数）
        - state: live/canceled/partially_filled/filled
        """
        try:
            ord_type = (data.get("ordType") or "").lower()
            state = (data.get("state") or "").lower()
            
            # 只处理限价入场单
            if ord_type not in ("limit", "post_only"):
                return
            
            # 跳过 reduceOnly 订单
            reduce_only = data.get("reduceOnly") == "true"
            if reduce_only:
                return
            
            order_id = str(data.get("ordId"))
            inst_id = data.get("instId", "")
            symbol = self._convert_inst_id_to_symbol(inst_id)
            
            open_orders = pf_compat.get_pf_open_orders(self.uid, self.exchange) or {}
            
            if state == "live":
                # 新订单或更新
                from core.okx_public_cache import get_okx_public_cache
                okx_cache = get_okx_public_cache()
                ct_val = okx_cache.get_ct_val(inst_id)
                
                sz_contracts = float(data.get("sz") or 0)
                filled_contracts = float(data.get("accFillSz") or 0)
                sz_coins = sz_contracts * ct_val
                filled_coins = filled_contracts * ct_val
                
                side = "BUY" if data.get("side") == "buy" else "SELL"
                pos_side = (data.get("posSide") or "net").lower()
                if pos_side == "net":
                    position_side = "LONG" if side == "BUY" else "SHORT"
                else:
                    position_side = pos_side.upper()
                
                open_orders[order_id] = {
                    "orderId": order_id,
                    "symbol": symbol,
                    "instId": inst_id,
                    "side": side,
                    "positionSide": position_side,
                    "price": str(data.get("px", "0")),
                    "origQty": str(sz_coins),
                    "executedQty": str(filled_coins),
                    "status": state.upper(),
                    "time": data.get("cTime"),
                    "updateTime": data.get("uTime"),
                    "cachedAt": now_ms(),
                }
                logger.debug(f"[{self.uid}][okx] 挂单缓存更新: {symbol} {order_id}")
                
            elif state in ("canceled", "filled"):
                # 订单完成或取消，从缓存移除
                if order_id in open_orders:
                    del open_orders[order_id]
                    logger.debug(f"[{self.uid}][okx] 挂单缓存移除: {symbol} {order_id} ({state})")
                
                # 如果是撤单，清理 ai_decision_id temp key
                if state == "canceled":
                    from core.pf_compatibility import cleanup_ai_decision_id_for_order
                    cleanup_ai_decision_id_for_order(self.uid, self.exchange, str(order_id))
                    
            elif state == "partially_filled":
                # 部分成交，更新成交数量
                if order_id in open_orders:
                    from core.okx_public_cache import get_okx_public_cache
                    okx_cache = get_okx_public_cache()
                    ct_val = okx_cache.get_ct_val(inst_id)
                    
                    filled_contracts = float(data.get("accFillSz") or 0)
                    filled_coins = filled_contracts * ct_val
                    
                    open_orders[order_id]["executedQty"] = str(filled_coins)
                    open_orders[order_id]["status"] = state.upper()
                    open_orders[order_id]["cachedAt"] = now_ms()
            
            pf_compat.set_pf_open_orders(self.uid, open_orders, self.exchange)
            
        except Exception as e:
            logger.warning(f"[{self.uid}][okx] 更新挂单缓存失败: {e}")
    
    # ========== 生命周期 ==========
    
    def start(self) -> None:
        if self._user_stream:
            return
        
        logger.info(f"[{self.uid}][okx] 启动 CycleStore (testnet={self.is_testnet})")
        
        # 初始化现有仓位的 TP/SL 和盈亏平衡价
        self._init_existing_positions()
        
        # 初始化已有的止盈止损订单
        self._init_existing_algo_orders()
        
        # 初始化挂单缓存
        open_orders_count = self._init_open_orders_cache()
        
        # 用户数据流
        self._user_stream = OKXUserStream(
            api_key=self.api_key,
            api_secret=self.api_secret,
            passphrase=self.passphrase,
            is_testnet=self.is_testnet,
            uid=self.uid,
            on_account=self._on_account,
            on_position=self._on_position,
            on_order=self._on_order,
            on_fill=self._on_fill,  # 成交明细回调 - 用于检测平仓
            on_algo_order=self._on_algo_order,  # 策略订单回调 - 用于获取止盈止损实际成交价
            on_state_change=self._on_state_change,  # 状态回调
        )
        self._user_stream.start()
        
        # 标记价格流
        # 现在 OKXMarkPriceStream 会自动从 Redis 读取活跃持仓的 symbols
        # 没有持仓时不连接，symbols 变化时自动重连
        self._mark_stream = OKXMarkPriceStream(
            redis_conn=self.rds,
            uid=self.uid,
            is_testnet=self.is_testnet,
            on_tick=self._on_mark_tick,
        )
        self._mark_stream.start()
        
        # 展示层 updater
        self._start_mark_cycle_updater(interval_s=1.0)
        
        logger.info(f"[{self.uid}][okx] CycleStore 已启动")
    
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
        
        logger.info(f"[{self.uid}][okx] CycleStore 已停止")
    
    # ========== 标记价格处理 ==========
    
    def _on_mark_tick(self, symbol: str, mark_price: float, timestamp: int):
        """
        处理标记价格更新
        
        参考 Binance/Bitget 实现:
        1. 写入 Redis（供 UI / 兜底轮询读取）
        2. 设置 TTL（防止断流时前端用到陈旧 mark）
        3. 回调 stop_manager 处理移动止损
        """
        try:
            from core.database import RedisKeys
            import json
            
            price_key = RedisKeys.market_prices(symbol)
            price_data = {
                "symbol": symbol,
                "markPrice": str(mark_price),
                "ts": str(timestamp),
            }
            self.rds.set(price_key, json.dumps(price_data, separators=(",", ":")))
            # 设置 TTL（15秒），与 Binance/Bitget 一致
            self.rds.expire(price_key, 15)
            
            # DEBUG: 每100次打印一次，避免日志过多（降级为 DEBUG）
            if not hasattr(self, '_mark_tick_count'):
                self._mark_tick_count = {}
            self._mark_tick_count[symbol] = self._mark_tick_count.get(symbol, 0) + 1
            if self._mark_tick_count[symbol] % 100 == 1:
                logger.debug(f"[{self.uid}][okx] Mark tick: {symbol} = {mark_price}")
            
            # 委托给 stop_manager 处理移动止损
            self.stop_manager.on_mark_tick(symbol, D(str(mark_price)), timestamp, exchange=self.exchange)
        except Exception as e:
            logger.warning(f"[{self.uid}][okx] Mark tick error: {e}")
    
    # ========== 账户更新处理 ==========
    
    def _on_account(self, data: dict) -> None:
        """
        处理 OKX 账户更新
        
        OKX 格式: {"details": [{"ccy": "USDT", "eq": "1000", "availEq": "900", ...}]}
        """
        try:
            ts = now_ms()
            details = data.get("details", [])
            
            wallet = D("0")
            unrealized = D("0")
            
            for detail in details:
                ccy = detail.get("ccy", "")
                if ccy.upper() == "USDT":
                    wallet = D(detail.get("eq", "0") or "0")
                    unrealized = D(detail.get("upl", "0") or "0")
                    break
            
            equity = wallet  # OKX eq 已经包含了 unrealized
            
            # 写初始权益快照
            existing_equity = pf_compat.get_pf_equity_init(self.uid, self.exchange)
            if equity > 0 and not existing_equity:
                obj = {
                    "uid": self.uid,
                    "ts": str(ts),
                    "walletBalance": self._d_to_str(equity),
                    "source": "OKX_WS_ACCOUNT",
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
                "source": "OKX_WS",
                "exchange": self.exchange,
            }
            pf_compat.set_pf_account(self.uid, account_obj, self.exchange)
            
        except Exception as e:
            logger.warning(f"[{self.uid}][okx] 账户更新处理失败: {e}")
    
    # ========== 持仓更新处理 ==========
    
    def _on_position(self, data: dict) -> None:
        """
        处理 OKX 持仓更新
        
        OKX 格式: {
            "instId": "BTC-USDT-SWAP",
            "instType": "SWAP",
            "pos": "1",           # 持仓数量（带正负号）
            "posSide": "long",    # long/short/net
            "avgPx": "50000",     # 平均开仓价
            "upl": "100",         # 未实现盈亏
            "uplRatio": "0.01",
            "lever": "10",
            "mgnMode": "cross",   # cross/isolated
            "cTime": "1234567890000",
            "uTime": "1234567890000",
            ...
        }
        """
        try:
            # 诊断日志：记录所有持仓推送（降级为 DEBUG，生产环境太吵）
            logger.debug(f"[{self.uid}][okx] positions 推送: instId={data.get('instId')} "
                        f"pos={data.get('pos')} posSide={data.get('posSide')} avgPx={data.get('avgPx')} "
                        f"upl={data.get('upl')} uTime={data.get('uTime')}")
            
            # 使用消息中的时间戳，而不是当前时间（与 Binance/Bitget 一致）
            ts = int(data.get("uTime") or data.get("cTime") or now_ms())
            inst_id = data.get("instId", "")
            symbol = self._convert_inst_id_to_symbol(inst_id)
            
            # OKX 的 pos 是张数，需要转换为币数量
            pos_contracts = float(data.get("pos", "0") or "0")
            qty_coins = self._contracts_to_coins(inst_id, abs(pos_contracts))
            
            # 确定方向
            pos_side = (data.get("posSide") or "net").lower()
            if pos_side == "net":
                side = "LONG" if pos_contracts > 0 else "SHORT"
            else:
                side = pos_side.upper()
            
            field = pos_field(symbol, side)
            
            # 获取当前数据
            pos_data = pf_compat.get_pf_pos(self.uid, self.exchange)
            cycle_data = pf_compat.get_pf_cycle(self.uid, self.exchange)
            active_positions = pf_compat.get_pf_pos_active(self.uid, self.exchange)
            
            old = pos_data.get(field, {})
            old_qty = D(old.get("qty", "0")) if old else D("0")
            
            unrealized_pnl = D(data.get("upl", "0") or "0")
            
            # 持仓为 0 -> 删除 + 关闭周期
            if qty_coins == 0:
                # === REST API 兜底：检查 cycle 是否有平仓数据，没有则查询成交记录 ===
                if field in cycle_data:
                    c = cycle_data[field]
                    close_qty = D(c.get("closeQty", "0"))
                    avg_close_price = D(c.get("avgClosePrice", "0"))
                    
                    if close_qty == 0 or avg_close_price == 0:
                        logger.info(f"[{self.uid}][okx] positions 推送仓位清零但 cycle 缺少平仓数据，尝试 REST API 查询: {field}")
                        try:
                            self._backfill_close_from_fills_api(inst_id, field, side)
                        except Exception as e:
                            logger.warning(f"[{self.uid}][okx] REST API 查询成交记录失败: {e}")
                # === REST API 兜底结束 ===
                
                # ⭐ 在删除前保存持仓快照
                pos_snapshot = {field: pos_data[field].copy()} if field in pos_data else {}
                
                if field in pos_data:
                    del pos_data[field]
                    pf_compat.set_pf_pos(self.uid, pos_data, self.exchange)
                
                if field in active_positions:
                    active_positions.remove(field)
                    pf_compat.set_pf_pos_active(self.uid, active_positions, self.exchange)
                
                # 重新获取 cycle_data（可能被 REST API 兜底更新过）
                cycle_data = pf_compat.get_pf_cycle(self.uid, self.exchange)
                if old_qty != 0 and field in cycle_data:
                    self._close_cycle(field, close_time_ms=ts, pos_data_snapshot=pos_snapshot)
                return
            
            # 读取 cycle 信息
            cycle_info = cycle_data.get(field, {})
            cycle_open_time = cycle_info.get("openTimeMs")
            cycle_open_order_type = cycle_info.get("openOrderType", "")
            cycle_open_tif = cycle_info.get("openTimeInForce", "")  # 与 Binance/Bitget 一致
            
            # 盈亏平衡价 (bePx) - OKX WebSocket 可能不推送，需要计算
            be_px = data.get("bePx") or data.get("be_px") or "0"
            if not be_px or be_px == "0":
                # 如果没有推送盈亏平衡价，使用开仓价作为近似值
                # 实际盈亏平衡价 = 开仓价 ± (手续费 / 数量)
                be_px = data.get("avgPx", "0")
            
            pos_obj = {
                "symbol": symbol,
                "side": side,
                "qty": str(qty_coins),
                "entryPrice": str(D(data.get("avgPx", "0") or "0")),
                "breakEvenPrice": str(D(be_px or "0")),  # 盈亏平衡价
                "unrealizedPnl": str(unrealized_pnl),
                "marginType": "cross" if data.get("mgnMode") == "cross" else "isolated",
                "isolatedMargin": str(D(data.get("margin", "0") or "0")),
                "openTimeMs": cycle_open_time or str(data.get("cTime") or ts),
                "updatedAt": str(ts),
                "openOrderType": cycle_open_order_type,
                "openTimeInForce": cycle_open_tif,  # 与 Binance/Bitget 一致
                "exchange": self.exchange,
                "leverage": str(data.get("lever", "1")),
            }
            
            # 从 closeOrderAlgo 解析止盈止损价格
            # OKX positions 推送中包含 closeOrderAlgo 数组，里面有止盈止损订单信息
            close_order_algo = data.get("closeOrderAlgo", [])
            sl_price = None
            tp_price = None
            
            for algo in close_order_algo:
                # OKX closeOrderAlgo 格式:
                # {"algoId": "xxx", "slTriggerPx": "100", "slTriggerPxType": "last", "tpTriggerPx": "200", ...}
                if algo.get("slTriggerPx"):
                    sl_price = str(algo.get("slTriggerPx"))
                if algo.get("tpTriggerPx"):
                    tp_price = str(algo.get("tpTriggerPx"))
            
            # 设置止盈止损价格
            if sl_price and sl_price != "0" and sl_price != "":
                pos_obj["stopLossPrice"] = sl_price
                logger.info(f"[{self.uid}][okx] {field} 从 positions 获取止损价: {sl_price}")
            elif old and old.get("stopLossPrice"):
                # 保留旧的止损价格
                pos_obj["stopLossPrice"] = old["stopLossPrice"]
            
            if tp_price and tp_price != "0" and tp_price != "":
                pos_obj["takeProfitPrice"] = tp_price
                logger.info(f"[{self.uid}][okx] {field} 从 positions 获取止盈价: {tp_price}")
            elif old and old.get("takeProfitPrice"):
                # 保留旧的止盈价格
                pos_obj["takeProfitPrice"] = old["takeProfitPrice"]
            
            # 更新持仓
            pos_data[field] = pos_obj
            pf_compat.set_pf_pos(self.uid, pos_data, self.exchange)
            
            if field not in active_positions:
                active_positions.append(field)
                pf_compat.set_pf_pos_active(self.uid, active_positions, self.exchange)
            
            # 0 -> non-0: 创建新周期
            if old_qty == 0 and field not in cycle_data:
                cycle_obj = self._new_cycle_dict(symbol, side, ts, D(str(qty_coins)), field)
                
                # 从 positions 推送中获取 avgPx 初始化开仓价
                # 这是关键修复：防止 orders 推送丢失导致开仓价为 0
                avg_px = D(data.get("avgPx", "0") or "0")
                if avg_px > 0 and qty_coins > 0:
                    cycle_obj["openQty"] = str(D(str(qty_coins)))
                    cycle_obj["openQuote"] = str(D(str(qty_coins)) * avg_px)
                    cycle_obj["avgOpenPrice"] = str(avg_px)
                    logger.info(f"[{self.uid}][okx] 新周期: {field} (从 positions 初始化 avgPx={avg_px})")
                else:
                    logger.info(f"[{self.uid}][okx] 新周期: {field}")
                
                cycle_data[field] = cycle_obj
                pf_compat.set_pf_cycle(self.uid, cycle_data, self.exchange)
                
                # 动态添加标记价格订阅（如果尚未订阅）
                if self._mark_stream and symbol:
                    self._mark_stream.add_symbol(symbol)
            
            # 同步止盈止损到 cycle 数据
            if field in cycle_data:
                cycle_updated = False
                if pos_obj.get("stopLossPrice"):
                    cycle_data[field]["stopLossPrice"] = pos_obj["stopLossPrice"]
                    cycle_updated = True
                if pos_obj.get("takeProfitPrice"):
                    cycle_data[field]["takeProfitPrice"] = pos_obj["takeProfitPrice"]
                    cycle_updated = True
                if cycle_updated:
                    pf_compat.set_pf_cycle(self.uid, cycle_data, self.exchange)
            
        except Exception as e:
            logger.warning(f"[{self.uid}][okx] 持仓更新处理失败: {e}")
            logger.exception("持仓更新处理失败")
    
    # ========== 成交明细处理 ==========
    
    def _on_fill(self, data: dict) -> None:
        """
        处理 OKX 成交明细更新 (fills 频道)
        
        用于检测平仓成交，比 orders 频道更可靠
        
        OKX fills 数据结构:
        {
            "instId": "BTC-USDT-SWAP",
            "tradeId": "xxx",
            "ordId": "xxx",
            "side": "buy",              # buy/sell
            "posSide": "long",          # long/short/net
            "fillPx": "51000.5",
            "fillSz": "0.01",           # 成交数量（张数）
            "pnl": "10.5",              # 收益
            "fee": "-0.18",             # 手续费（负数）
            "fillTime": "1234567890000",
            ...
        }
        """
        try:
            logger.debug(f"[{self.uid}][okx] fills 推送: {data}")
            
            inst_id = data.get("instId", "")
            symbol = self._convert_inst_id_to_symbol(inst_id)
            side = (data.get("side") or "").lower()
            pos_side = (data.get("posSide") or "net").lower()
            
            if not symbol:
                return
            
            # 确定仓位方向
            if pos_side == "net":
                # 单向持仓模式，根据 pnl 判断
                pnl = D(data.get("pnl", "0") or "0")
                if pnl != 0:
                    # 有 pnl 说明是平仓
                    ps = "LONG" if side == "sell" else "SHORT"
                else:
                    # 无 pnl 说明是开仓
                    ps = "LONG" if side == "buy" else "SHORT"
            else:
                ps = pos_side.upper()
            
            if ps not in ("LONG", "SHORT"):
                logger.debug(f"[{self.uid}][okx] fills 无法确定仓位方向: {data}")
                return
            
            field = f"{symbol}:{ps}"
            
            # 检查是否为平仓成交
            # 平仓: 有 pnl 或者 side 与 posSide 反向
            pnl = D(data.get("pnl", "0") or "0")
            is_close_trade = pnl != 0
            
            # 转换张数为币数量
            fill_contracts = D(data.get("fillSz", "0") or "0")
            if fill_contracts <= 0:
                return
            ct_val = D(str(self._get_ct_val(inst_id)))
            fill_qty = fill_contracts * ct_val
            
            fill_price = D(data.get("fillPx", "0") or "0")
            fee = abs(D(data.get("fee", "0") or "0"))
            
            trade_id = data.get("tradeId", "")
            order_id = data.get("ordId", "")
            t_ms = int(data.get("fillTime") or now_ms())
            
            logger.info(f"[{self.uid}][okx] fills: {field} side={side} is_close={is_close_trade} qty={fill_qty} price={fill_price} pnl={pnl}")
            
            if is_close_trade and fill_qty > 0:
                # 平仓成交 - 更新 cycle 数据
                logger.info(f"[{self.uid}][okx] fills 检测到平仓成交: {field}")
                
                cycle_data = pf_compat.get_pf_cycle(self.uid, self.exchange)
                if field in cycle_data:
                    c = cycle_data[field]
                    
                    # 去重检查 - 使用通用格式，与 algo-orders 频道兼容
                    seen_trades = pf_compat.get_pf_seen_trades(self.uid)
                    # 通用去重 key：基于 symbol:side:时间:数量:价格
                    seen_member_generic = f"{field}:close:{t_ms}:qty:{fill_qty}:px:{fill_price}"
                    # 特定去重 key：基于 tradeId/orderId
                    seen_member_fill = f"{symbol}:{ps}:fill:{trade_id or order_id}:T:{t_ms}"
                    
                    if seen_member_generic not in seen_trades and seen_member_fill not in seen_trades:
                        # 添加平仓成交
                        c = self._add_close_fill_to_cycle_dict(
                            c,
                            qty=fill_qty,
                            price=fill_price,
                            fee=fee,
                            realized=pnl,
                            order_id=order_id,
                        )
                        c["updatedAt"] = str(t_ms)
                        
                        cycle_data[field] = c
                        pf_compat.set_pf_cycle(self.uid, cycle_data, self.exchange)
                        # 添加两个去重 key，确保与 algo-orders 频道兼容
                        pf_compat.add_pf_seen_trades(self.uid, seen_member_generic)
                        pf_compat.add_pf_seen_trades(self.uid, seen_member_fill)
                        
                        logger.info(f"[{self.uid}][okx] fills 已更新 cycle: {field} closeQty={c.get('closeQty')}")
                
                # 延迟检查仓位是否完全平仓
                # 使用线程避免阻塞 WebSocket
                def _delayed_check():
                    time.sleep(0.3)  # 等待可能的后续 fill
                    self._check_position_after_tpsl(inst_id, ps)
                
                t = threading.Thread(target=_delayed_check, daemon=True)
                t.start()
                
        except Exception as e:
            logger.warning(f"[{self.uid}][okx] 处理 fills 失败: {e}")
            logger.exception("处理 fills 失败")
    
    def _on_algo_order(self, data: dict) -> None:
        """
        处理 OKX 策略订单更新 (orders-algo 频道)
        
        当止盈止损触发时，从这里获取实际成交价格
        
        OKX orders-algo 数据结构:
        {
            "instId": "BTC-USDT-SWAP",
            "algoId": "xxx",
            "posSide": "long",
            "ordType": "conditional",
            "state": "effective",       # live/pending/effective/filled/canceled
            "slTriggerPx": "50000",      # 止损触发价
            "tpTriggerPx": "55000",      # 止盈触发价
            "actualPx": "54980.5",       # 实际成交价格 (触发后才有)
            "actualSz": "0.1",           # 实际成交数量-张数 (触发后才有)
            "pnl": "100.5",              # 已实现盈亏
            "fee": "-0.5",               # 手续费
            "triggerTime": "1234567890000",
            ...
        }
        """
        try:
            state = (data.get("state") or "").lower()
            ord_type = (data.get("ordType") or "").lower()
            
            # 只处理已触发/已成交的策略订单
            # effective: 已触发（OKX 特有状态，表示条件已满足，正在执行）
            # filled: 已完全成交
            if state not in ("effective", "filled"):
                # 其他状态（live/pending/canceled）仍由原逻辑处理 TP/SL 价格更新
                self._update_tp_sl_from_order(data, state, ord_type)
                return
            
            # 获取实际成交数据
            actual_px = data.get("actualPx") or ""
            actual_sz = data.get("actualSz") or ""
            
            # 如果没有实际成交数据，可能是刚触发还未成交，等待后续推送
            if not actual_px or not actual_sz or actual_px == "0" or actual_sz == "0":
                logger.info(f"[{self.uid}][okx] orders-algo 触发但无成交数据，等待后续推送: state={state}")
                # 仍然更新 TP/SL 状态
                self._update_tp_sl_from_order(data, state, ord_type)
                return
            
            actual_px_d = D(actual_px)
            actual_sz_d = D(actual_sz)  # 张数
            
            if actual_px_d <= 0 or actual_sz_d <= 0:
                logger.warning(f"[{self.uid}][okx] orders-algo 成交数据无效: actualPx={actual_px} actualSz={actual_sz}")
                self._update_tp_sl_from_order(data, state, ord_type)
                return
            
            # 转换张数为币数量
            inst_id = data.get("instId", "")
            ct_val = D(str(self._get_ct_val(inst_id)))
            fill_qty = actual_sz_d * ct_val
            
            # 获取其他成交数据
            pnl = D(data.get("pnl") or "0")
            fee = abs(D(data.get("fee") or "0"))
            algo_id = data.get("algoId", "")
            t_ms = int(data.get("triggerTime") or data.get("uTime") or now_ms())
            
            # 确定仓位方向
            symbol = self._convert_inst_id_to_symbol(inst_id)
            pos_side = (data.get("posSide") or "net").lower()
            
            if pos_side == "net":
                # 单向持仓模式，根据止盈止损类型判断
                # 有止损触发价说明是止损单，止损单平的是多仓（sell）或空仓（buy）
                # 这里简化处理，从现有持仓中查找
                ps = self._guess_position_side_from_cycle(symbol)
            else:
                ps = pos_side.upper()
            
            if ps not in ("LONG", "SHORT"):
                logger.warning(f"[{self.uid}][okx] orders-algo 无法确定仓位方向: posSide={pos_side}")
                self._update_tp_sl_from_order(data, state, ord_type)
                return
            
            field = f"{symbol}:{ps}"
            
            logger.info(f"[{self.uid}][okx] orders-algo 成交: {field} price={actual_px_d} qty={fill_qty} pnl={pnl} fee={fee}")
            
            # 去重检查 - 使用通用格式，与 fills 频道兼容
            # 这样即使同一笔成交被 fills 和 algo-orders 都推送，也只会处理一次
            seen_trades = pf_compat.get_pf_seen_trades(self.uid)
            # 通用去重 key：基于 symbol:side:时间:数量:价格
            seen_member_generic = f"{field}:close:{t_ms}:qty:{fill_qty}:px:{actual_px_d}"
            # 特定去重 key：基于 algoId
            seen_member_algo = f"{field}:algo:{algo_id}:T:{t_ms}"
            
            if seen_member_generic in seen_trades or seen_member_algo in seen_trades:
                logger.debug(f"[{self.uid}][okx] orders-algo 成交已处理过，跳过")
                return
            
            # 更新 cycle 数据
            cycle_data = pf_compat.get_pf_cycle(self.uid, self.exchange)
            if field in cycle_data:
                c = cycle_data[field]
                c = self._add_close_fill_to_cycle_dict(
                    c,
                    qty=fill_qty,
                    price=actual_px_d,
                    fee=fee,
                    realized=pnl,
                    order_id=algo_id,
                )
                c["updatedAt"] = str(t_ms)
                cycle_data[field] = c
                pf_compat.set_pf_cycle(self.uid, cycle_data, self.exchange)
                # 添加两个去重 key，确保与 fills 频道兼容
                pf_compat.add_pf_seen_trades(self.uid, seen_member_generic)
                pf_compat.add_pf_seen_trades(self.uid, seen_member_algo)
                
                logger.info(f"[{self.uid}][okx] orders-algo 已更新 cycle: {field} "
                           f"closeQty={c.get('closeQty')} avgClosePrice={c.get('avgClosePrice')} "
                           f"realizedPnlEst={c.get('realizedPnlEst')}")
            else:
                logger.warning(f"[{self.uid}][okx] orders-algo 成交但无对应 cycle: {field}")
                # 即使没有 cycle，也要添加去重 key，避免后续重复处理
                pf_compat.add_pf_seen_trades(self.uid, seen_member_generic)
                pf_compat.add_pf_seen_trades(self.uid, seen_member_algo)
            
            # 清除 TP/SL 价格
            self._update_tp_sl_from_order(data, state, ord_type)
            
            # 延迟检查仓位是否完全平仓
            def _delayed_check():
                time.sleep(0.5)
                self._check_position_after_tpsl(inst_id, ps)
            
            t = threading.Thread(target=_delayed_check, daemon=True)
            t.start()
            
        except Exception as e:
            logger.warning(f"[{self.uid}][okx] 处理 orders-algo 失败: {e}")
            logger.exception("处理 orders-algo 失败")
    
    def _guess_position_side_from_cycle(self, symbol: str) -> str:
        """
        从现有 cycle 数据中猜测仓位方向
        用于单向持仓模式下无法从 posSide 确定方向的情况
        """
        cycle_data = pf_compat.get_pf_cycle(self.uid, self.exchange)
        for side in ("LONG", "SHORT"):
            field = f"{symbol}:{side}"
            if field in cycle_data:
                return side
        return "LONG"  # 默认返回 LONG

    def _check_position_after_tpsl(self, inst_id: str, side: str) -> None:
        """
        TPSL 执行后检查仓位是否已平
        
        OKX 有时不会推送 position WebSocket 更新，
        所以需要通过 REST API 主动查询仓位状态
        """
        import requests
        import hmac
        import base64
        from hashlib import sha256
        from datetime import datetime, timezone
        from core.rate_limiter import get_okx_rate_limiter
        
        try:
            symbol = self._convert_inst_id_to_symbol(inst_id)
            field = f"{symbol}:{side}"
            logger.info(f"[{self.uid}][okx] 检查仓位状态: {field}")
            
            # 稍微延迟，等待交易所处理完成
            time.sleep(0.5)
            
            # 使用 API Key 级别限速
            rate_limiter = get_okx_rate_limiter(self.api_key)
            if not rate_limiter.acquire(endpoint="/api/v5/account/positions", timeout=30.0):
                logger.warning(f"[{self.uid}][okx] 检查仓位限速超时")
                return
            
            # 构建签名
            timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
            method = "GET"
            request_path = f"/api/v5/account/positions?instType=SWAP&instId={inst_id}"
            
            message = timestamp + method + request_path
            signature = hmac.new(
                self.api_secret.encode('utf-8'),
                message.encode('utf-8'),
                sha256
            ).digest()
            sign = base64.b64encode(signature).decode('utf-8')
            
            headers = {
                'OK-ACCESS-KEY': self.api_key,
                'OK-ACCESS-SIGN': sign,
                'OK-ACCESS-TIMESTAMP': timestamp,
                'OK-ACCESS-PASSPHRASE': self.passphrase,
                'Content-Type': 'application/json',
            }
            
            if self.is_testnet:
                headers['x-simulated-trading'] = '1'
            
            base_url = "https://www.okx.com"
            resp = requests.get(f"{base_url}{request_path}", headers=headers, timeout=10)
            
            if resp.status_code != 200:
                logger.warning(f"[{self.uid}][okx] 仓位检查 API 失败: HTTP {resp.status_code}")
                return
            
            data = resp.json()
            if data.get('code') != '0':
                logger.warning(f"[{self.uid}][okx] 仓位检查 API 返回错误: {data.get('msg')}")
                return
            
            positions = data.get('data', [])
            
            # 检查目标仓位是否还存在
            position_exists = False
            for pos in positions:
                pos_contracts = float(pos.get("pos", 0))
                pos_side = (pos.get("posSide") or "net").lower()
                
                if pos_side == "net":
                    check_side = "LONG" if pos_contracts > 0 else "SHORT"
                else:
                    check_side = pos_side.upper()
                
                if check_side == side and abs(pos_contracts) > 0:
                    position_exists = True
                    logger.info(f"[{self.uid}][okx] 仓位仍存在: {field} pos={pos_contracts}")
                    break
            
            if not position_exists:
                logger.info(f"[{self.uid}][okx] 仓位已不存在，执行平仓处理: {field}")
                
                # 获取当前 Redis 数据
                pos_data = pf_compat.get_pf_pos(self.uid, self.exchange)
                cycle_data = pf_compat.get_pf_cycle(self.uid, self.exchange)
                active_positions = pf_compat.get_pf_pos_active(self.uid, self.exchange)
                
                old_qty = D(pos_data.get(field, {}).get("qty", "0"))
                ts = now_ms()
                
                # === REST API 兜底：检查 cycle 是否有平仓数据，没有则查询成交记录 ===
                if field in cycle_data:
                    c = cycle_data[field]
                    close_qty = D(c.get("closeQty", "0"))
                    avg_close_price = D(c.get("avgClosePrice", "0"))
                    
                    if close_qty == 0 or avg_close_price == 0:
                        logger.info(f"[{self.uid}][okx] cycle 缺少平仓数据，尝试 REST API 查询成交记录: {field}")
                        try:
                            self._backfill_close_from_fills_api(inst_id, field, side)
                        except Exception as e:
                            logger.warning(f"[{self.uid}][okx] REST API 查询成交记录失败: {e}")
                # === REST API 兜底结束 ===
                
                # ⭐ 在删除前保存持仓快照
                pos_snapshot = {field: pos_data[field].copy()} if field in pos_data else {}
                
                # 删除仓位
                if field in pos_data:
                    del pos_data[field]
                    pf_compat.set_pf_pos(self.uid, pos_data, self.exchange)
                    logger.info(f"[{self.uid}][okx] 已从 pos_data 删除: {field}")
                
                # 从活跃列表删除
                if field in active_positions:
                    active_positions.remove(field)
                    pf_compat.set_pf_pos_active(self.uid, active_positions, self.exchange)
                    logger.info(f"[{self.uid}][okx] 已从 active_positions 删除: {field}")
                
                # 关闭周期（重新获取 cycle_data，因为可能被 REST API 兜底更新过）
                cycle_data = pf_compat.get_pf_cycle(self.uid, self.exchange)
                if old_qty != 0 and field in cycle_data:
                    logger.info(f"[{self.uid}][okx] 关闭周期: {field}")
                    self._close_cycle(field, close_time_ms=ts, pos_data_snapshot=pos_snapshot)
                    
        except Exception as e:
            logger.warning(f"[{self.uid}][okx] 检查仓位状态失败: {e}")
            logger.exception("检查仓位状态失败")
    
    def _backfill_close_from_fills_api(self, inst_id: str, field: str, side: str) -> None:
        """
        通过 REST API 查询成交记录，补充平仓数据
        
        当 orders-algo 推送丢失或数据不完整时，作为兜底方案
        
        OKX API: GET /api/v5/trade/fills
        返回最近 7 天内的成交明细（不需要 VIP6+）
        """
        import requests
        import hmac
        import base64
        from hashlib import sha256
        from datetime import datetime, timezone
        from core.rate_limiter import get_okx_rate_limiter
        
        logger.info(f"[{self.uid}][okx] REST API 兜底查询成交记录: {field}")
        
        # 使用 API Key 级别限速
        rate_limiter = get_okx_rate_limiter(self.api_key)
        if not rate_limiter.acquire(endpoint="/api/v5/trade/fills", timeout=30.0):
            logger.warning(f"[{self.uid}][okx] 查询成交限速超时")
            return
        
        # 构建签名
        timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
        method = "GET"
        request_path = f"/api/v5/trade/fills?instType=SWAP&instId={inst_id}&limit=20"
        
        message = timestamp + method + request_path
        signature = hmac.new(
            self.api_secret.encode('utf-8'),
            message.encode('utf-8'),
            sha256
        ).digest()
        sign = base64.b64encode(signature).decode('utf-8')
        
        headers = {
            'OK-ACCESS-KEY': self.api_key,
            'OK-ACCESS-SIGN': sign,
            'OK-ACCESS-TIMESTAMP': timestamp,
            'OK-ACCESS-PASSPHRASE': self.passphrase,
            'Content-Type': 'application/json',
        }
        
        if self.is_testnet:
            headers['x-simulated-trading'] = '1'
        
        base_url = "https://www.okx.com"
        resp = requests.get(f"{base_url}{request_path}", headers=headers, timeout=10)
        
        if resp.status_code != 200:
            logger.warning(f"[{self.uid}][okx] 成交查询 API 失败: HTTP {resp.status_code}")
            return
        
        data = resp.json()
        if data.get('code') != '0':
            logger.warning(f"[{self.uid}][okx] 成交查询 API 返回错误: {data.get('msg')}")
            return
        
        fills = data.get('data', [])
        if not fills:
            logger.info(f"[{self.uid}][okx] 未查询到成交记录: {field}")
            return
        
        # 筛选平仓成交（最近 30 秒内）
        # 平仓判断：LONG 仓位 sell，SHORT 仓位 buy
        close_side = "sell" if side == "LONG" else "buy"
        now = now_ms()
        time_window = 30000  # 30 秒，考虑网络延迟和处理时间
        
        close_fills = []
        for fill in fills:
            fill_side = (fill.get("side") or "").lower()
            fill_time = int(fill.get("fillTime") or "0")
            
            if fill_side == close_side and (now - fill_time) < time_window:
                close_fills.append(fill)
        
        if not close_fills:
            logger.info(f"[{self.uid}][okx] 未找到匹配的平仓成交: {field} (需要 side={close_side})")
            return
        
        logger.info(f"[{self.uid}][okx] 找到 {len(close_fills)} 条平仓成交记录")
        
        # 应用成交数据到 cycle
        cycle_data = pf_compat.get_pf_cycle(self.uid, self.exchange)
        if field not in cycle_data:
            logger.warning(f"[{self.uid}][okx] cycle 不存在，无法补充: {field}")
            return
        
        c = cycle_data[field]
        seen_trades = pf_compat.get_pf_seen_trades(self.uid)
        updated = False
        
        for fill in close_fills:
            trade_id = fill.get("tradeId", "")
            order_id = fill.get("ordId", "")
            t_ms = int(fill.get("fillTime") or now_ms())
            
            # 转换张数为币数量（先计算，用于去重 key）
            fill_contracts = D(fill.get("fillSz", "0") or "0")
            ct_val = D(str(self._get_ct_val(inst_id)))
            fill_qty = fill_contracts * ct_val
            fill_price = D(fill.get("fillPx", "0") or "0")
            
            # 去重检查 - 使用通用格式，与 _on_fill 和 _on_algo_order 兼容
            seen_member_generic = f"{field}:close:{t_ms}:qty:{fill_qty}:px:{fill_price}"
            seen_member_fill = f"{field}:fill:{trade_id or order_id}:T:{t_ms}"
            
            if seen_member_generic in seen_trades or seen_member_fill in seen_trades:
                continue
            
            fee = abs(D(fill.get("fee", "0") or "0"))
            pnl = D(fill.get("pnl", "0") or "0")
            
            if fill_qty > 0 and fill_price > 0:
                c = self._add_close_fill_to_cycle_dict(
                    c,
                    qty=fill_qty,
                    price=fill_price,
                    fee=fee,
                    realized=pnl,
                    order_id=order_id,
                )
                c["updatedAt"] = str(t_ms)
                # 添加两个去重 key
                pf_compat.add_pf_seen_trades(self.uid, seen_member_generic)
                pf_compat.add_pf_seen_trades(self.uid, seen_member_fill)
                updated = True
                
                logger.info(f"[{self.uid}][okx] REST API 补充平仓成交: {field} "
                           f"qty={fill_qty} price={fill_price} pnl={pnl}")
        
        if updated:
            cycle_data[field] = c
            pf_compat.set_pf_cycle(self.uid, cycle_data, self.exchange)
            logger.info(f"[{self.uid}][okx] REST API 已更新 cycle: {field} "
                       f"closeQty={c.get('closeQty')} avgClosePrice={c.get('avgClosePrice')}")
    
    # ========== 止损止盈订单处理 ==========
    
    def _update_tp_sl_from_order(self, data: dict, state: str, ord_type: str) -> None:
        """
        从止损止盈订单事件中更新持仓的 TP/SL 价格
        
        OKX 止损止盈订单类型:
        - conditional: 条件单（止盈止损）
        - oco: OCO订单
        - trigger: 计划委托
        - move_order_stop: 移动止损
        
        OKX 策略订单数据格式 (orders-algo):
        {
            "instId": "BTC-USDT-SWAP",
            "algoId": "xxx",
            "posSide": "long",
            "ordType": "conditional",
            "state": "live",
            "slTriggerPx": "50000",    # 止损触发价
            "slOrdPx": "-1",           # -1 表示市价
            "tpTriggerPx": "55000",    # 止盈触发价
            "tpOrdPx": "-1",
            ...
        }
        """
        try:
            inst_id = data.get("instId", "")
            symbol = self._convert_inst_id_to_symbol(inst_id)
            pos_side = (data.get("posSide") or "net").lower()
            
            if not symbol or pos_side not in ("long", "short"):
                logger.debug(f"[{self.uid}][okx] 跳过策略订单: symbol={symbol} posSide={pos_side}")
                return
            
            ps = pos_side.upper()
            field = f"{symbol}:{ps}"
            
            # 获取当前持仓数据
            pos_data = pf_compat.get_pf_pos(self.uid, self.exchange)
            if field not in pos_data:
                logger.debug(f"[{self.uid}][okx] 策略订单对应的持仓不存在: {field}")
                return
            
            pos = pos_data[field]
            updated = False
            
            # OKX 的策略订单可能同时包含止损和止盈
            sl_trigger_px = data.get("slTriggerPx")
            tp_trigger_px = data.get("tpTriggerPx")
            
            if state in ("live", "pending"):
                # 订单创建/待触发 - 记录价格
                if sl_trigger_px and sl_trigger_px != "0" and sl_trigger_px != "":
                    pos["stopLossPrice"] = str(sl_trigger_px)
                    updated = True
                    logger.info(f"[{self.uid}][okx] {symbol} {ps} 止损价更新: {sl_trigger_px}")
                
                if tp_trigger_px and tp_trigger_px != "0" and tp_trigger_px != "":
                    pos["takeProfitPrice"] = str(tp_trigger_px)
                    updated = True
                    logger.info(f"[{self.uid}][okx] {symbol} {ps} 止盈价更新: {tp_trigger_px}")
            
            elif state in ("canceled", "cancelled", "expired", "order_failed"):
                # 订单取消/过期 - 清除价格
                if sl_trigger_px and sl_trigger_px != "0" and sl_trigger_px != "":
                    pos["stopLossPrice"] = None
                    updated = True
                    logger.info(f"[{self.uid}][okx] {symbol} {ps} 止损订单取消")
                
                if tp_trigger_px and tp_trigger_px != "0" and tp_trigger_px != "":
                    pos["takeProfitPrice"] = None
                    updated = True
                    logger.info(f"[{self.uid}][okx] {symbol} {ps} 止盈订单取消")
            
            elif state in ("filled", "triggered", "effective"):
                # 订单触发成交 - 清除价格（仓位已经平掉）
                # effective: OKX 特有状态，表示条件已满足正在执行，应视为已触发
                
                # 在清除之前，保存触发价到 cycle 的备用字段（用于兜底计算平仓价）
                cycle_data = pf_compat.get_pf_cycle(self.uid, self.exchange)
                if field in cycle_data:
                    cyc = cycle_data[field]
                    if sl_trigger_px and sl_trigger_px != "0":
                        cyc["lastSlTriggerPx"] = str(sl_trigger_px)
                    if tp_trigger_px and tp_trigger_px != "0":
                        cyc["lastTpTriggerPx"] = str(tp_trigger_px)
                    # 同时清除当前的 TP/SL 价格（后面会统一保存）
                    cyc["stopLossPrice"] = None
                    cyc["takeProfitPrice"] = None
                    cycle_data[field] = cyc
                    pf_compat.set_pf_cycle(self.uid, cycle_data, self.exchange)
                
                if sl_trigger_px and sl_trigger_px != "0" and sl_trigger_px != "":
                    pos["stopLossPrice"] = None
                    updated = True
                    logger.info(f"[{self.uid}][okx] {symbol} {ps} 止损触发")
                
                if tp_trigger_px and tp_trigger_px != "0" and tp_trigger_px != "":
                    pos["takeProfitPrice"] = None
                    updated = True
                    logger.info(f"[{self.uid}][okx] {symbol} {ps} 止盈触发")
            
            if updated:
                # 保存更新
                pos_data[field] = pos
                pf_compat.set_pf_pos(self.uid, pos_data, self.exchange)
                
                # 对于非 filled/triggered/effective 状态，同时更新 cycle 数据
                # （filled/triggered/effective 状态已在上面处理过）
                if state not in ("filled", "triggered", "effective"):
                    cycle_data = pf_compat.get_pf_cycle(self.uid, self.exchange)
                    if field in cycle_data:
                        cyc = cycle_data[field]
                        cyc["stopLossPrice"] = pos.get("stopLossPrice")
                        cyc["takeProfitPrice"] = pos.get("takeProfitPrice")
                        cycle_data[field] = cyc
                        pf_compat.set_pf_cycle(self.uid, cycle_data, self.exchange)
                
        except Exception as e:
            logger.warning(f"[{self.uid}][okx] 更新止损止盈失败: {e}")
            logger.exception("更新止损止盈失败")
    
    # ========== 订单更新处理 ==========
    
    def _on_order(self, data: dict) -> None:
        """
        处理 OKX 订单更新
        
        OKX 格式: {
            "instId": "BTC-USDT-SWAP",
            "ordId": "123456789",
            "clOrdId": "client_order_id",
            "side": "buy",          # buy/sell
            "posSide": "long",      # long/short/net
            "ordType": "limit",     # market/limit/post_only/...
            "state": "filled",      # live/canceled/partially_filled/filled
            "px": "50000",          # 委托价格
            "sz": "1",              # 委托数量
            "fillPx": "50000",      # 成交价格
            "fillSz": "1",          # 成交数量
            "pnl": "0",             # 收益
            "fee": "-0.05",         # 手续费（负数）
            "fillTime": "1234567890000",
            "uTime": "1234567890000",
            ...
        }
        """
        try:
            # 诊断日志：记录所有订单推送（降级为 DEBUG，生产环境太吵）
            logger.debug(f"[{self.uid}][okx] orders 推送: instId={data.get('instId')} "
                        f"ordId={data.get('ordId')} state={data.get('state')} side={data.get('side')} "
                        f"posSide={data.get('posSide')} fillSz={data.get('fillSz')} fillPx={data.get('fillPx')} "
                        f"pnl={data.get('pnl')} fee={data.get('fee')}")
            
            state = (data.get("state") or "").lower()
            ord_type = (data.get("ordType") or "").lower()
            
            # 更新挂单缓存（实时同步）
            self._update_open_orders_cache(data)
            
            # 检查是否是策略订单的状态更新（非成交）
            # 策略订单特征：有 algoId 字段，或者有 slTriggerPx/tpTriggerPx，或者 ordType 是策略类型
            has_algo_id = bool(data.get("algoId"))
            has_tp_sl_px = bool(data.get("slTriggerPx") or data.get("tpTriggerPx"))
            is_algo_ord_type = ord_type in ("conditional", "oco", "trigger", "move_order_stop")
            
            # 关键修复：即使是策略订单触发的市价单，如果有成交数据，也要记录
            # 止盈止损触发后，OKX 会创建市价单执行，这个市价单的 ordType 是 "market"
            # 但可能会带有 algoId 字段，表示是由哪个策略订单触发的
            has_fill_data = (
                state in ("filled", "partially_filled") and 
                D(data.get("fillSz", "0") or "0") > 0 and
                D(data.get("fillPx", "0") or "0") > 0
            )
            
            if (has_algo_id or has_tp_sl_px or is_algo_ord_type) and not has_fill_data:
                # 策略订单状态更新（非成交），只更新 TP/SL 价格
                logger.info(f"[{self.uid}][okx] 策略订单状态更新: state={state} ordType={ord_type} "
                           f"sl={data.get('slTriggerPx')} tp={data.get('tpTriggerPx')}")
                self._update_tp_sl_from_order(data, state, ord_type)
                return
            
            # 如果是策略订单触发的成交，记录日志
            if has_algo_id and has_fill_data:
                logger.info(f"[{self.uid}][okx] 策略订单触发成交: algoId={data.get('algoId')} "
                           f"ordType={ord_type} fillPx={data.get('fillPx')} fillSz={data.get('fillSz')}")
                # 同时更新 TP/SL 状态（清除已触发的止盈止损价格）
                self._update_tp_sl_from_order(data, state, ord_type)
            
            # 只处理已成交的订单
            if state not in ("filled", "partially_filled"):
                return
            
            # OKX 的 fillSz 是张数，需要转换为币数量
            inst_id = data.get("instId", "")
            fill_contracts = D(data.get("fillSz", "0") or "0")
            if fill_contracts <= 0:
                return
            
            # 转换张数为币数量
            ct_val = D(str(self._get_ct_val(inst_id)))
            fill_sz = fill_contracts * ct_val
            
            t_ms = int(data.get("fillTime") or data.get("uTime") or now_ms())
            symbol = self._convert_inst_id_to_symbol(inst_id)
            
            pos_side = (data.get("posSide") or "net").lower()
            trade_side = (data.get("side") or "").lower()
            
            if pos_side == "net":
                # 单向持仓模式：根据 pnl 判断是否为平仓
                pnl = D(data.get("pnl", "0") or "0")
                if pnl != 0:
                    # 有 pnl 说明是平仓
                    ps = "LONG" if trade_side == "sell" else "SHORT"
                else:
                    # 无 pnl 说明是开仓
                    ps = "LONG" if trade_side == "buy" else "SHORT"
            else:
                ps = pos_side.upper()
            
            fill_px = D(data.get("fillPx", "0") or "0")
            
            # OKX 的 fee 和 pnl 是累计值，需要计算增量
            # fee 是负数，pnl 是收益
            acc_fee = abs(D(data.get("fee", "0") or "0"))
            acc_pnl = D(data.get("pnl", "0") or "0")
            
            order_id = data.get("ordId", "")
            
            # 获取上一次的累计值，计算增量
            prev_acc = self._order_acc_cache.get(order_id, {"accFee": D("0"), "accPnl": D("0")})
            delta_fee = acc_fee - prev_acc["accFee"]
            delta_pnl = acc_pnl - prev_acc["accPnl"]
            
            # 更新累计值缓存
            self._order_acc_cache[order_id] = {"accFee": acc_fee, "accPnl": acc_pnl}
            
            # 如果订单完成，清理缓存
            if state == "filled":
                # 延迟清理，避免重复推送
                def _cleanup():
                    time.sleep(5)
                    self._order_acc_cache.pop(order_id, None)
                threading.Thread(target=_cleanup, daemon=True).start()
            
            # 使用增量值
            fee = delta_fee if delta_fee > 0 else D("0")
            realized = delta_pnl
            
            is_open_trade = self._is_open_trade(ps, trade_side)
            
            # 去重：使用累计 fee 作为唯一标识
            seen_trades = pf_compat.get_pf_seen_trades(self.uid)
            raw_fee = data.get("fee", "0") or "0"
            seen_member = f"{symbol}:{ps}:oid:{order_id}:accFee:{raw_fee}"
            
            if seen_member in seen_trades:
                return
            
            field = pos_field(symbol, ps)
            cycle_data = pf_compat.get_pf_cycle(self.uid, self.exchange)
            
            # 没有活跃周期
            if field not in cycle_data:
                if not is_open_trade:
                    # 平仓但没有周期，尝试回补
                    self._backfill_close_trade(
                        field=field,
                        t_ms=t_ms,
                        qty=fill_sz,
                        price=fill_px,
                        fee=fee,
                        realized=realized,
                        order_id=order_id,
                    )
                    pf_compat.add_pf_seen_trades(self.uid, seen_member)
                    return
                
                # 开仓先于持仓更新到达 -> 创建周期和持仓记录
                logger.info(f"[{self.uid}][okx] 订单先于持仓到达，创建 cycle 和 pos: {field}")
                c = self._new_cycle_dict(symbol, ps, t_ms, D("0"), field)
                
                # 同时创建 pos 记录（避免 positions 推送延迟或丢失）
                pos_data = pf_compat.get_pf_pos(self.uid, self.exchange)
                active_positions = pf_compat.get_pf_pos_active(self.uid, self.exchange)
                
                if field not in pos_data:
                    pos_obj = {
                        "symbol": symbol,
                        "side": ps,
                        "qty": str(fill_sz),  # 从订单的 fillSz 计算
                        "entryPrice": str(fill_px),
                        "unrealizedPnl": "0",
                        "marginType": "cross",
                        "openTimeMs": str(t_ms),
                        "updatedAt": str(t_ms),
                        "openOrderType": (data.get("ordType") or "").upper(),
                        "exchange": self.exchange,
                    }
                    pos_data[field] = pos_obj
                    pf_compat.set_pf_pos(self.uid, pos_data, self.exchange)
                    logger.info(f"[{self.uid}][okx] 创建 pos 记录: {field} qty={fill_sz}")
                
                if field not in active_positions:
                    active_positions.append(field)
                    pf_compat.set_pf_pos_active(self.uid, active_positions, self.exchange)
                    logger.info(f"[{self.uid}][okx] 添加到 active_positions: {field}")
            else:
                c = cycle_data[field]
            
            # 先尝试确认 trailing pending（委托给 stop_manager，传递 exchange）
            self.stop_manager.confirm_trailing_from_order({
                "x": "TRADE",  # 模拟 Binance 格式
                "s": symbol,
                "ps": ps,
                "o": data.get("ordType", ""),
            }, exchange=self.exchange)
            
            # 记账
            if is_open_trade:
                # 检测是否为加仓（已有开仓数量 > 0）
                existing_open_qty = D(c.get("openQty", "0"))
                if existing_open_qty > 0:
                    self.stop_manager.reset_trailing_on_add_position(symbol, ps, exchange=self.exchange)
                
                c["openQty"] = str(D(c.get("openQty", "0")) + fill_sz)
                c["openQuote"] = str(D(c.get("openQuote", "0")) + fill_sz * fill_px)
                c["feeTotal"] = str(D(c.get("feeTotal", "0")) + fee)
                c["realizedPnlEst"] = str(D(c.get("realizedPnlEst", "0")) + realized)
                
                # 更新 maxAbsQty（最大持仓数量）
                new_open_qty = D(c.get("openQty", "0"))
                current_max = D(c.get("maxAbsQty", "0"))
                if new_open_qty > current_max:
                    c["maxAbsQty"] = str(new_open_qty)
                
                # 诊断日志：开仓成交记账
                logger.info(f"[{self.uid}][okx] 开仓成交记账: {field} qty={fill_sz} price={fill_px} "
                           f"fee={fee} -> openQty={c.get('openQty')} maxAbsQty={c.get('maxAbsQty')} feeTotal={c.get('feeTotal')}")
                
                # 记录开仓订单类型
                # OKX 没有单独的 timeInForce 字段，使用 ordType 中的信息
                # ordType: market, limit, post_only, fok, ioc 等
                if not c.get("openOrderType"):
                    order_type = (data.get("ordType") or "").upper()
                    c["openOrderType"] = order_type
                    # 从 ordType 推断 timeInForce
                    # post_only -> GTC, fok -> FOK, ioc -> IOC, market -> IOC, limit -> GTC
                    tif_map = {
                        "POST_ONLY": "GTC",
                        "FOK": "FOK",
                        "IOC": "IOC",
                        "MARKET": "IOC",
                        "LIMIT": "GTC",
                    }
                    c["openTimeInForce"] = tif_map.get(order_type, "GTC")
                    c["openOrderId"] = str(order_id)
                    c["openClientOrderId"] = str(data.get("clOrdId") or "")
                    logger.info(f"[{self.uid}][okx][ORDER_META] {symbol} {ps} type={order_type} tif={c.get('openTimeInForce')}")
                
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
                        logger.info(f"[{self.uid}][okx][AI_DECISION] {symbol} {ps} linked to ai_decision_id={ai_decision_id}")
                
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
                    qty=fill_sz,
                    price=fill_px,
                    fee=fee,
                    realized=realized,
                    order_id=order_id,
                )
                # 诊断日志：平仓成交记账
                logger.info(f"[{self.uid}][okx] 平仓成交记账: {field} qty={fill_sz} price={fill_px} "
                           f"fee={fee} realized={realized} -> closeQty={c.get('closeQty')} feeTotal={c.get('feeTotal')}")
                
                # === 关键修复：检查是否完全平仓，主动关闭 cycle ===
                # OKX positions 频道可能不会推送 pos=0（主网行为），所以需要在这里检测
                # 测试网会推送 pos=0，但为了兼容主网，这里也做检测
                open_qty = D(c.get("openQty", "0"))
                close_qty = D(c.get("closeQty", "0"))
                
                if open_qty > 0 and close_qty >= open_qty:
                    # 完全平仓，延迟检查并关闭 cycle
                    # 注意：如果 positions 频道推送了 pos=0，cycle 可能已经被关闭
                    # _check_and_close_position 会检查 cycle 是否存在
                    logger.info(f"[{self.uid}][okx] 检测到完全平仓: {field} openQty={open_qty} closeQty={close_qty}")
                    
                    def _check_and_close():
                        time.sleep(1.0)  # 延迟 1 秒，等待 positions 频道可能的 pos=0 推送
                        # 检查 cycle 是否还存在（可能已被 _on_position 关闭）
                        cycle_data = pf_compat.get_pf_cycle(self.uid, self.exchange)
                        if field not in cycle_data:
                            logger.debug(f"[{self.uid}][okx] cycle 已被关闭，跳过: {field}")
                            return
                        self._check_and_close_position(inst_id, field, ps, t_ms)
                    
                    threading.Thread(target=_check_and_close, daemon=True).start()
            
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
                
                # 更新 pos 的 openOrderType 和 openTimeInForce（与 Binance/Bitget 一致）
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
            
            logger.debug(f"[{self.uid}][okx] 订单成交: {field} {'开仓' if is_open_trade else '平仓'} qty={fill_sz}")
            
        except Exception as e:
            logger.warning(f"[{self.uid}][okx] 订单更新处理失败: {e}")
            logger.exception("订单更新处理失败")
    
    def _check_and_close_position(self, inst_id: str, field: str, side: str, t_ms: int) -> None:
        """
        检查仓位是否已平仓，如果是则关闭 cycle
        
        OKX positions 频道不会推送 pos=0，所以需要在 orders 频道检测到完全平仓后
        主动通过 REST API 查询仓位状态，确认后关闭 cycle
        """
        import requests
        import hmac
        import base64
        from hashlib import sha256
        from datetime import datetime, timezone
        from core.rate_limiter import get_okx_rate_limiter
        
        try:
            logger.info(f"[{self.uid}][okx] 检查仓位状态并关闭 cycle: {field}")
            
            # 使用 API Key 级别限速
            rate_limiter = get_okx_rate_limiter(self.api_key)
            if not rate_limiter.acquire(endpoint="/api/v5/account/positions", timeout=30.0):
                logger.warning(f"[{self.uid}][okx] 检查仓位限速超时，直接关闭 cycle")
                # 限速超时，直接关闭 cycle（因为 orders 频道已经确认完全平仓）
                self._force_close_cycle(field, t_ms)
                return
            
            # 构建签名
            timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
            method = "GET"
            request_path = f"/api/v5/account/positions?instType=SWAP&instId={inst_id}"
            
            message = timestamp + method + request_path
            signature = hmac.new(
                self.api_secret.encode('utf-8'),
                message.encode('utf-8'),
                sha256
            ).digest()
            sign = base64.b64encode(signature).decode('utf-8')
            
            headers = {
                'OK-ACCESS-KEY': self.api_key,
                'OK-ACCESS-SIGN': sign,
                'OK-ACCESS-TIMESTAMP': timestamp,
                'OK-ACCESS-PASSPHRASE': self.passphrase,
                'Content-Type': 'application/json',
            }
            
            if self.is_testnet:
                headers['x-simulated-trading'] = '1'
            
            base_url = "https://www.okx.com"
            resp = requests.get(f"{base_url}{request_path}", headers=headers, timeout=10)
            
            if resp.status_code != 200:
                logger.warning(f"[{self.uid}][okx] 仓位检查 API 失败: HTTP {resp.status_code}，直接关闭 cycle")
                self._force_close_cycle(field, t_ms)
                return
            
            data = resp.json()
            if data.get('code') != '0':
                logger.warning(f"[{self.uid}][okx] 仓位检查 API 返回错误: {data.get('msg')}，直接关闭 cycle")
                self._force_close_cycle(field, t_ms)
                return
            
            positions = data.get('data', [])
            
            # 检查目标仓位是否还存在
            position_exists = False
            for pos in positions:
                pos_contracts = float(pos.get("pos", 0))
                pos_side = (pos.get("posSide") or "net").lower()
                
                if pos_side == "net":
                    check_side = "LONG" if pos_contracts > 0 else "SHORT"
                else:
                    check_side = pos_side.upper()
                
                if check_side == side and abs(pos_contracts) > 0:
                    position_exists = True
                    logger.info(f"[{self.uid}][okx] 仓位仍存在: {field} pos={pos_contracts}")
                    break
            
            if not position_exists:
                logger.info(f"[{self.uid}][okx] 确认仓位已平仓，关闭 cycle: {field}")
                self._force_close_cycle(field, t_ms)
            else:
                logger.info(f"[{self.uid}][okx] 仓位仍存在，不关闭 cycle: {field}")
                
        except Exception as e:
            logger.warning(f"[{self.uid}][okx] 检查仓位状态失败: {e}，直接关闭 cycle")
            logger.exception("检查仓位状态失败")
            # 出错时也关闭 cycle（因为 orders 频道已经确认完全平仓）
            self._force_close_cycle(field, t_ms)
    
    def _force_close_cycle(self, field: str, close_time_ms: int) -> None:
        """
        强制关闭 cycle 并清理仓位数据
        
        当 orders 频道检测到完全平仓时调用
        """
        try:
            # 获取当前数据
            pos_data = pf_compat.get_pf_pos(self.uid, self.exchange)
            cycle_data = pf_compat.get_pf_cycle(self.uid, self.exchange)
            active_positions = pf_compat.get_pf_pos_active(self.uid, self.exchange)
            
            # ⭐ 在删除前保存持仓快照
            pos_snapshot = {field: pos_data[field].copy()} if field in pos_data else {}
            
            # 删除仓位
            if field in pos_data:
                del pos_data[field]
                pf_compat.set_pf_pos(self.uid, pos_data, self.exchange)
                logger.info(f"[{self.uid}][okx] 已从 pos_data 删除: {field}")
            
            # 从活跃列表删除
            if field in active_positions:
                active_positions.remove(field)
                pf_compat.set_pf_pos_active(self.uid, active_positions, self.exchange)
                logger.info(f"[{self.uid}][okx] 已从 active_positions 删除: {field}")
            
            # 关闭周期
            if field in cycle_data:
                logger.info(f"[{self.uid}][okx] 关闭周期: {field}")
                self._close_cycle(field, close_time_ms=close_time_ms, pos_data_snapshot=pos_snapshot)
            else:
                logger.warning(f"[{self.uid}][okx] cycle 不存在，无法关闭: {field}")
                
        except Exception as e:
            logger.warning(f"[{self.uid}][okx] 强制关闭 cycle 失败: {e}")
            logger.exception("强制关闭 cycle 失败")
    
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
            
            # 查找时间窗口内的周期
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
            
            # 选择最近的
            candidates.sort(key=lambda x: x[2], reverse=True)
            cycle_id, c, _ = candidates[0]
            
            c["updatedAt"] = str(max(int(c.get("updatedAt", "0") or "0"), t_ms))
            c = self._add_close_fill_to_cycle_dict(c, qty=qty, price=price, fee=fee, realized=realized, order_id=order_id)
            
            closed_h[cycle_id] = c
            pf_compat.set_pf_closed_h(self.uid, cycle_id, c, self.exchange)
            
            logger.info(f"[{self.uid}][okx][BACKFILL] 更新已关闭周期 {cycle_id}")
            
            # ⚠️ 注意：更新已有周期时，不触发排行榜更新
            # 因为这个周期之前通过 _close_cycle 关闭时已经触发过排行榜更新了
            # 如果再次触发会导致重复计算（total_trades 和 net_profit 会被重复累加）
            # 增量的 pnl 变化会在下次对账时自动修正
            
            return True
            
        except Exception as e:
            logger.warning(f"[{self.uid}][okx][BACKFILL] 查找失败: {e}")
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
            logger.info(f"[{self.uid}][okx][BACKFILL] 创建新记录 {new_cycle_id}")
            
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
                logger.info(f"[{self.uid}][okx][BACKFILL] 排行榜已更新: {new_cycle_id} pnl={net_pnl}")
            except Exception as e:
                logger.warning(f"[{self.uid}][okx][BACKFILL] 排行榜更新失败: {e}")
            
            return True
            
        except Exception as e:
            logger.error(f"[{self.uid}][okx][BACKFILL] 创建失败: {e}")
            return False
    
    def _close_cycle(self, field: str, close_time_ms: int, pos_data_snapshot: dict = None) -> None:
        """关闭周期"""
        cycle_data = pf_compat.get_pf_cycle(self.uid, self.exchange)
        
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
                    logger.info(f"[{self.uid}][okx] Parsed symbol/side from field: {field}")
            except Exception as e:
                logger.warning(f"[{self.uid}][okx] Failed to parse field {field}: {e}")
        
        # ========== 兜底机制：补充缺失的开仓数据 ==========
        # 如果 orders 推送丢失，开仓数据可能为 0，从 pos 数据中获取兜底值
        if D(c.get("avgOpenPrice", "0")) == 0 or D(c.get("openQty", "0")) == 0:
            # ⭐ 优先使用传入的快照，其次从 Redis 获取
            pos_data = pos_data_snapshot if pos_data_snapshot else pf_compat.get_pf_pos(self.uid, self.exchange)
            pos = pos_data.get(field, {})
            entry_price = D(pos.get("entryPrice", "0"))
            pos_qty = D(pos.get("qty", "0"))
            
            if entry_price > 0:
                # 补充开仓价
                if D(c.get("avgOpenPrice", "0")) == 0:
                    c["avgOpenPrice"] = str(entry_price)
                    logger.warning(f"[{self.uid}][okx] 周期关闭时补充开仓价: {field} entryPrice={entry_price}")
                
                # 补充开仓数量和金额
                if D(c.get("openQty", "0")) == 0 and pos_qty > 0:
                    c["openQty"] = str(pos_qty)
                    c["openQuote"] = str(pos_qty * entry_price)
                    logger.warning(f"[{self.uid}][okx] 周期关闭时补充开仓数量: {field} qty={pos_qty}")
                elif D(c.get("openQty", "0")) == 0:
                    # 如果 pos 中也没有数量，使用 closeQty 作为兜底
                    close_qty = D(c.get("closeQty", "0"))
                    if close_qty > 0:
                        c["openQty"] = str(close_qty)
                        c["openQuote"] = str(close_qty * entry_price)
                        logger.warning(f"[{self.uid}][okx] 周期关闭时使用平仓数量作为开仓数量: {field} qty={close_qty}")
        
        # ========== 兜底机制：补充缺失的平仓数据 ==========
        if D(c.get("avgClosePrice", "0")) == 0 and D(c.get("closeQty", "0")) == 0:
            # 如果没有平仓数据，优先使用止盈/止损触发价作为近似平仓价
            # 这比使用开仓价更接近实际成交价
            open_qty = D(c.get("openQty", "0"))
            avg_open_price = D(c.get("avgOpenPrice", "0"))
            
            # 优先级：lastTpTriggerPx > lastSlTriggerPx > takeProfitPrice > stopLossPrice > 开仓价
            # lastTpTriggerPx/lastSlTriggerPx 是触发时保存的备用值
            tp_price = D(c.get("lastTpTriggerPx") or c.get("takeProfitPrice") or "0")
            sl_price = D(c.get("lastSlTriggerPx") or c.get("stopLossPrice") or "0")
            
            # 确定使用哪个价格作为近似平仓价
            fallback_price = D("0")
            price_source = ""
            
            if tp_price > 0:
                fallback_price = tp_price
                price_source = "TP"
            elif sl_price > 0:
                fallback_price = sl_price
                price_source = "SL"
            elif avg_open_price > 0:
                fallback_price = avg_open_price
                price_source = "OPEN"
            
            if open_qty > 0 and fallback_price > 0:
                c["closeQty"] = str(open_qty)
                c["closeQuote"] = str(open_qty * fallback_price)
                c["avgClosePrice"] = str(fallback_price)
                logger.warning(f"[{self.uid}][okx] 周期关闭时补充平仓数据(兜底): {field} "
                              f"qty={open_qty} price={fallback_price} (来源: {price_source})")
        
        c["closeTimeMs"] = str(close_time_ms)
        open_t = int(c.get("openTimeMs", "0") or "0")
        c["durationMs"] = str(max(0, close_time_ms - open_t))
        c["updatedAt"] = str(close_time_ms)
        c["field"] = field
        
        net = D(c.get("realizedPnlEst", "0")) + D(c.get("fundingTotal", "0")) - D(c.get("feeTotal", "0"))
        c["netPnl"] = str(net)
        
        # 诊断日志：数据完整性检查
        open_price = D(c.get("avgOpenPrice", "0"))
        close_price = D(c.get("avgClosePrice", "0"))
        open_qty = D(c.get("openQty", "0"))
        close_qty = D(c.get("closeQty", "0"))
        realized = D(c.get("realizedPnlEst", "0"))
        fee = D(c.get("feeTotal", "0"))
        
        missing_fields = []
        if open_price == 0:
            missing_fields.append("avgOpenPrice")
        if close_price == 0:
            missing_fields.append("avgClosePrice")
        if open_qty == 0:
            missing_fields.append("openQty")
        if close_qty == 0:
            missing_fields.append("closeQty")
        if realized == 0:
            missing_fields.append("realizedPnlEst")
        if fee == 0:
            missing_fields.append("feeTotal")
        
        if missing_fields:
            logger.warning(f"[{self.uid}][okx] 周期关闭时数据不完整: {field} 缺失字段: {missing_fields} "
                          f"(openPrice={open_price}, closePrice={close_price}, openQty={open_qty}, "
                          f"closeQty={close_qty}, realized={realized}, fee={fee})")
        
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
        
        # 诊断日志：打印关闭周期的完整数据
        logger.info(f"[{self.uid}][okx] 周期关闭: {field} | "
                   f"openQty={c.get('openQty')} closeQty={c.get('closeQty')} | "
                   f"avgOpenPrice={c.get('avgOpenPrice')} avgClosePrice={c.get('avgClosePrice')} | "
                   f"realizedPnlEst={c.get('realizedPnlEst')} feeTotal={c.get('feeTotal')} | "
                   f"netPnl={c.get('netPnl')}")
        
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
            logger.warning(f"[{self.uid}][okx] 返佣触发失败: {e}")
