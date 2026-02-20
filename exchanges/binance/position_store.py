# exchanges/binance/position_store.py
"""
Binance 持仓/周期数据存储

职责:
1. 接收 WebSocket 数据并更新 Redis
2. 维护持仓 (positions) 和周期 (cycles) 数据
3. 处理账户更新、订单成交等事件
"""

import json
import logging
import time
import uuid
from decimal import Decimal
from typing import Dict, Optional, List, Any, TYPE_CHECKING

from core.database import redis_client, RedisKeys
from core.utils import D, now_ms, jdump, jload

if TYPE_CHECKING:
    from core.user_context import UserContext

logger = logging.getLogger(__name__)

EXCHANGE_NAME = "binance"


def pos_field(symbol: str, side: str) -> str:
    """生成持仓字段键: BTCUSDT:LONG"""
    return f"{symbol}:{side.upper()}"


def exchange_field(field: str) -> str:
    """生成交易所级字段键: binance:positions"""
    return f"{EXCHANGE_NAME}:{field}"


class BinancePositionStore:
    """
    Binance 持仓数据存储
    
    处理 WebSocket 事件，维护 Redis 中的用户数据
    """
    
    def __init__(
        self,
        uid: str,
        user_context: Optional['UserContext'] = None,
    ):
        self.uid = uid
        self._user_context = user_context
        
        # 止损管理器 (可选注入)
        self._stop_loss_manager = None
    
    def set_stop_loss_manager(self, manager):
        """设置止损管理器"""
        self._stop_loss_manager = manager
    
    # ==================== Redis 字段访问 ====================
    
    def _get_field(self, field: str) -> Any:
        """获取用户交易所级字段"""
        key = RedisKeys.user(self.uid)
        ex_field = exchange_field(field)
        data = redis_client.hget(key, ex_field)
        if data is None:
            return None
        if isinstance(data, bytes):
            data = data.decode('utf-8')
        try:
            return json.loads(data)
        except (json.JSONDecodeError, TypeError):
            return data
    
    def _set_field(self, field: str, value: Any):
        """设置用户交易所级字段"""
        key = RedisKeys.user(self.uid)
        ex_field = exchange_field(field)
        if isinstance(value, (dict, list)):
            value = json.dumps(value, ensure_ascii=False)
        redis_client.hset(key, ex_field, value)
    
    def get_account(self) -> Optional[Dict]:
        """获取账户数据"""
        return self._get_field("account")
    
    def set_account(self, data: Dict):
        """设置账户数据"""
        data["exchange"] = EXCHANGE_NAME
        self._set_field("account", data)
    
    def get_equity_init(self) -> Optional[Dict]:
        """获取初始权益"""
        return self._get_field("equity_init")
    
    def set_equity_init(self, data: Dict):
        """设置初始权益"""
        data["exchange"] = EXCHANGE_NAME
        self._set_field("equity_init", data)
    
    def get_positions(self) -> Dict[str, Dict]:
        """获取所有持仓"""
        return self._get_field("positions") or {}
    
    def set_positions(self, data: Dict[str, Dict]):
        """设置所有持仓"""
        self._set_field("positions", data)
    
    def get_position(self, symbol: str, side: str) -> Optional[Dict]:
        """获取单个持仓"""
        positions = self.get_positions()
        field = pos_field(symbol, side)
        return positions.get(field)
    
    def set_position(self, symbol: str, side: str, data: Dict):
        """设置单个持仓"""
        positions = self.get_positions()
        field = pos_field(symbol, side)
        data["exchange"] = EXCHANGE_NAME
        positions[field] = data
        self.set_positions(positions)
    
    def delete_position(self, symbol: str, side: str):
        """删除单个持仓"""
        positions = self.get_positions()
        field = pos_field(symbol, side)
        if field in positions:
            del positions[field]
            self.set_positions(positions)
    
    def get_positions_active(self) -> List[str]:
        """获取活跃持仓列表"""
        return self._get_field("positions_active") or []
    
    def set_positions_active(self, active_list: List[str]):
        """设置活跃持仓列表"""
        self._set_field("positions_active", active_list)
    
    def get_cycles(self) -> Dict[str, Dict]:
        """获取所有周期"""
        return self._get_field("cycles") or {}
    
    def set_cycles(self, data: Dict[str, Dict]):
        """设置所有周期"""
        self._set_field("cycles", data)
    
    def get_cycle(self, symbol: str, side: str) -> Optional[Dict]:
        """获取单个周期"""
        cycles = self.get_cycles()
        field = pos_field(symbol, side)
        return cycles.get(field)
    
    def set_cycle(self, symbol: str, side: str, data: Dict):
        """设置单个周期"""
        cycles = self.get_cycles()
        field = pos_field(symbol, side)
        data["exchange"] = EXCHANGE_NAME
        cycles[field] = data
        self.set_cycles(cycles)
    
    def delete_cycle(self, symbol: str, side: str):
        """删除单个周期"""
        cycles = self.get_cycles()
        field = pos_field(symbol, side)
        if field in cycles:
            del cycles[field]
            self.set_cycles(cycles)
    
    def get_closed_trades(self) -> Dict[str, Dict]:
        """获取已关闭交易"""
        return self._get_field("closed_trades") or {}
    
    def add_closed_trade(self, cycle_id: str, data: Dict):
        """添加已关闭交易"""
        closed = self.get_closed_trades()
        data["exchange"] = EXCHANGE_NAME
        closed[cycle_id] = data
        self._set_field("closed_trades", closed)
    
    # ==================== 工具方法 ====================
    
    def _d_to_str(self, x: Decimal) -> str:
        """Decimal 转字符串"""
        s = format(x, "f")
        if "." in s:
            s = s.rstrip("0").rstrip(".")
        return s or "0"
    
    def _new_cycle_id(self, symbol: str, side: str) -> str:
        """生成新的周期 ID"""
        return f"{EXCHANGE_NAME}:{symbol}:{side}:{int(time.time())}:{uuid.uuid4().hex[:8]}"
    
    def _is_open_trade(self, position_side: str, trade_side: str) -> bool:
        """判断是否为开仓交易"""
        if position_side == "LONG":
            return trade_side == "BUY"
        if position_side == "SHORT":
            return trade_side == "SELL"
        return False
    
    def _calc_live_pnl(self, side: str, qty: Decimal, entry: Decimal, mark: Decimal) -> Decimal:
        """计算实时盈亏"""
        if qty <= 0 or entry <= 0 or mark <= 0:
            return D("0")
        if side.upper() == "LONG":
            return (mark - entry) * qty
        return (entry - mark) * qty
    
    def _update_peak_and_drawdown(self, c: Dict, current_pnl: Decimal = None):
        """更新峰值和回撤"""
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
    
    # ==================== 事件处理 ====================
    
    def on_account_update(self, msg: Dict):
        """
        处理 ACCOUNT_UPDATE 事件
        
        更新账户余额、权益和持仓
        """
        ts = int(msg.get("T", 0) or 0)
        a = msg.get("a") or {}
        
        # 解析余额
        wallet = D("0")
        for b in (a.get("B") or []):
            if b.get("a") == "USDT":
                wallet = D(b.get("wb", "0"))
                break
        
        # 计算未实现盈亏
        unrealized_all = D("0")
        for pp in (a.get("P") or []):
            unrealized_all += D(pp.get("up", "0"))
        
        equity = wallet + unrealized_all
        
        # 写初始权益（只写一次）
        if equity > 0 and not self.get_equity_init():
            self.set_equity_init({
                "uid": self.uid,
                "ts": str(ts),
                "walletBalance": self._d_to_str(equity),
                "source": "ACCOUNT_UPDATE",
            })
        
        # 更新账户数据
        self.set_account({
            "uid": self.uid,
            "ts": str(ts),
            "walletBalance": self._d_to_str(wallet),
            "equity": self._d_to_str(equity),
            "unrealized": self._d_to_str(unrealized_all),
            "source": "ACCOUNT_UPDATE",
        })
        
        # 处理持仓更新
        positions_data = a.get("P") or []
        if not positions_data:
            return
        
        for p in positions_data:
            symbol = p.get("s")
            side = (p.get("ps") or "").upper()
            if not symbol or side not in ("LONG", "SHORT"):
                continue
            
            pa = D(p.get("pa", "0"))
            qty_abs = abs(pa)
            field = pos_field(symbol, side)
            
            # 获取当前数据
            positions = self.get_positions()
            cycles = self.get_cycles()
            active_list = self.get_positions_active()
            
            old = positions.get(field, {})
            old_qty = D(old.get("qty", "0")) if old else D("0")
            
            # qty=0: 删除持仓，关闭周期
            if qty_abs == 0:
                # ⭐ 在删除前保存持仓快照
                pos_snapshot = {field: positions[field].copy()} if field in positions else {}
                
                if field in positions:
                    del positions[field]
                    self.set_positions(positions)
                
                if field in active_list:
                    active_list.remove(field)
                    self.set_positions_active(active_list)
                
                if old_qty != 0 and field in cycles:
                    self._close_cycle(field, close_time_ms=ts, pos_data_snapshot=pos_snapshot)
                continue
            
            # 获取周期信息
            cycle_info = cycles.get(field, {})
            
            # 构建持仓对象
            pos_obj = {
                "symbol": symbol,
                "side": side,
                "qty": str(qty_abs),
                "entryPrice": str(D(p.get("ep", "0"))),
                "breakEvenPrice": str(D(p.get("bep", "0"))),
                "unrealizedPnl": str(D(p.get("up", "0"))),
                "marginType": (p.get("mt") or "cross").lower(),
                "isolatedMargin": str(D(p.get("iw", "0"))),
                "openTimeMs": cycle_info.get("openTimeMs") or str(ts),
                "updatedAt": str(ts),
                "exchange": EXCHANGE_NAME,
            }
            
            # 保留止盈止损
            if old:
                if old.get("stopLossPrice"):
                    pos_obj["stopLossPrice"] = old["stopLossPrice"]
                if old.get("takeProfitPrice"):
                    pos_obj["takeProfitPrice"] = old["takeProfitPrice"]
            
            # 更新持仓
            positions[field] = pos_obj
            self.set_positions(positions)
            
            # 更新活跃列表
            if field not in active_list:
                active_list.append(field)
                self.set_positions_active(active_list)
            
            # 创建周期（如果不存在）
            if old_qty == 0 and field not in cycles:
                cycle_obj = self._new_cycle_dict(symbol, side, ts, qty_abs, field)
                cycles[field] = cycle_obj
                self.set_cycles(cycles)
    
    def on_order_trade_update(self, msg: Dict):
        """
        处理 ORDER_TRADE_UPDATE 事件
        
        更新周期的成交数据
        """
        o = msg.get("o") or {}
        
        exec_type = (o.get("x") or "").upper()
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
        order_id = int(o.get("i", 0) or 0)
        
        is_open_trade = self._is_open_trade(ps, trade_side)
        field = pos_field(symbol, ps)
        
        cycles = self.get_cycles()
        
        # 没有活跃周期
        if field not in cycles:
            if not is_open_trade:
                return
            # 开仓交易先到 -> 创建周期
            c = self._new_cycle_dict(symbol, ps, t_ms, D("0"), field)
        else:
            c = cycles[field]
        
        # 更新周期数据
        if is_open_trade:
            c["openQty"] = str(D(c.get("openQty", "0")) + qty)
            c["openQuote"] = str(D(c.get("openQuote", "0")) + qty * price)
            c["feeTotal"] = str(D(c.get("feeTotal", "0")) + fee)
            c["realizedPnlEst"] = str(D(c.get("realizedPnlEst", "0")) + realized)
            
            # 记录开仓订单类型
            if not c.get("openOrderType"):
                c["openOrderType"] = (o.get("o") or o.get("ot") or "").upper()
                c["openTimeInForce"] = (o.get("f") or "").upper()
            
            # 设置初始止损
            if c.get("openStopordersFired", "0") != "1" and self._stop_loss_manager:
                oq = D(c.get("openQty", "0"))
                entry = (D(c.get("openQuote", "0")) / oq) if oq else D("0")
                order_type = c.get("openOrderType") or ""
                self._stop_loss_manager.set_initial_stops(symbol, ps, entry, order_type, exchange="binance")
                c["openStopordersFired"] = "1"
        else:
            # 平仓
            c["closeQty"] = str(D(c.get("closeQty", "0")) + qty)
            c["closeQuote"] = str(D(c.get("closeQuote", "0")) + qty * price)
            c["feeTotal"] = str(D(c.get("feeTotal", "0")) + fee)
            c["realizedPnlEst"] = str(D(c.get("realizedPnlEst", "0")) + realized)
        
        # 计算净盈亏
        net = D(c.get("realizedPnlEst", "0")) + D(c.get("fundingTotal", "0")) - D(c.get("feeTotal", "0"))
        c["netPnl"] = str(net)
        
        # 计算均价
        oq = D(c.get("openQty", "0"))
        cq = D(c.get("closeQty", "0"))
        c["avgOpenPrice"] = str(D(c.get("openQuote", "0")) / oq) if oq else "0"
        c["avgClosePrice"] = str(D(c.get("closeQuote", "0")) / cq) if cq else "0"
        c["updatedAt"] = str(t_ms)
        
        # 保存
        cycles[field] = c
        self.set_cycles(cycles)
    
    def _new_cycle_dict(self, symbol: str, side: str, ts: int, qty_abs: Decimal, field: str) -> Dict:
        """创建新周期"""
        return {
            "cycleId": self._new_cycle_id(symbol, side),
            "uid": self.uid,
            "symbol": symbol,
            "side": side,
            "exchange": EXCHANGE_NAME,
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
            "openStopordersFired": "0",
            "field": field,
        }
    
    def _close_cycle(self, field: str, close_time_ms: int, pos_data_snapshot: dict = None):
        """关闭周期"""
        cycles = self.get_cycles()
        c = cycles.get(field)
        if not c:
            return
        
        # ⭐ 检查关键字段是否缺失，如果缺失则尝试从持仓快照回补
        open_time_ms = int(c.get("openTimeMs", "0") or "0")
        open_qty = D(c.get("openQty", "0") or "0")
        avg_open_price = D(c.get("avgOpenPrice", "0") or "0")
        
        data_incomplete = (open_time_ms == 0 or open_qty == 0 or avg_open_price == 0)
        
        if data_incomplete:
            logger.warning(
                f"[{self.uid}][binance] Cycle data incomplete for {field}: "
                f"openTimeMs={open_time_ms}, openQty={open_qty}, avgOpenPrice={avg_open_price}"
            )
            
            # 尝试从持仓快照回补数据
            if pos_data_snapshot and field in pos_data_snapshot:
                pos = pos_data_snapshot[field]
                if open_time_ms == 0:
                    pos_open_time = int(pos.get("openTimeMs", "0") or "0")
                    if pos_open_time > 0:
                        c["openTimeMs"] = str(pos_open_time)
                        logger.info(f"[{self.uid}][binance] Recovered openTimeMs from pos snapshot: {pos_open_time}")
                
                if open_qty == 0:
                    pos_qty = D(pos.get("qty", "0") or "0")
                    if pos_qty > 0:
                        c["openQty"] = str(pos_qty)
                        c["closeQty"] = str(pos_qty)
                        c["maxAbsQty"] = str(pos_qty)
                        open_qty = pos_qty
                        logger.info(f"[{self.uid}][binance] Recovered qty from pos snapshot: {open_qty}")
                
                if avg_open_price == 0:
                    pos_entry = D(pos.get("entryPrice", "0") or "0")
                    if pos_entry > 0:
                        c["avgOpenPrice"] = str(pos_entry)
                        c["openQuote"] = str(open_qty * pos_entry) if open_qty > 0 else "0"
                        logger.info(f"[{self.uid}][binance] Recovered entryPrice from pos snapshot: {pos_entry}")
        
        # 设置关闭时间
        c["closeTimeMs"] = str(close_time_ms)
        open_t = int(c.get("openTimeMs", "0") or "0")
        c["durationMs"] = str(max(0, close_time_ms - open_t))
        c["updatedAt"] = str(close_time_ms)
        c["field"] = field
        
        # 计算最终盈亏
        net = D(c.get("realizedPnlEst", "0")) + D(c.get("fundingTotal", "0")) - D(c.get("feeTotal", "0"))
        c["netPnl"] = str(net)
        
        # 计算回撤
        peak = D(c.get("peakPnl", "0"))
        dd_close = peak - net
        if dd_close < 0:
            dd_close = D("0")
        c["drawdownToClose"] = str(dd_close)
        
        cycle_id = c.get("cycleId") or self._new_cycle_id(c.get("symbol", "UNK"), c.get("side", "UNK"))
        c["cycleId"] = cycle_id
        
        # 保存到已关闭交易
        self.add_closed_trade(cycle_id, c)
        
        # 从活跃周期中删除
        del cycles[field]
        self.set_cycles(cycles)
        
        logger.info(f"[{self.uid}][binance] 周期关闭: {field} netPnl={c['netPnl']}")
        
        # 触发返佣发放和排行榜统计更新
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
            logger.warning(f"[{self.uid}][binance] 返佣触发失败: {e}")
