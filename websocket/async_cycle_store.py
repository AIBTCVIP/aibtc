# websocket/async_cycle_store.py
"""
纯 asyncio 版本的 Binance CycleStore

负责：
1. 管理 WebSocket 连接（使用 AsyncFuturesUserWS）
2. 处理账户/持仓/订单更新
3. 管理止损订单
4. 跟踪交易周期

与旧版 BinanceUMHedgeWSOnlyStore 的区别：
- 完全基于 asyncio，无 threading
- 所有方法都是 async
- 通过 asyncio.Task 运行在主事件循环中
"""

import asyncio
import json
import logging
import time
from typing import Dict, Optional, Callable, Any, List, TYPE_CHECKING
from dataclasses import dataclass, field

from websocket.async_user_stream import AsyncFuturesUserWS, WSConfig, AuthError

if TYPE_CHECKING:
    from core.user_context import UserContext

logger = logging.getLogger(__name__)


@dataclass
class PositionData:
    """持仓数据"""
    symbol: str
    side: str  # LONG / SHORT
    qty: float
    entry_price: float
    unrealized_pnl: float
    margin_type: str = "cross"
    leverage: int = 1
    open_time_ms: int = 0
    updated_at: int = 0


@dataclass
class CycleData:
    """交易周期数据"""
    cycle_id: str
    symbol: str
    side: str
    open_time_ms: int
    open_qty: float
    avg_open_price: float
    fee_total: float = 0
    funding_total: float = 0
    peak_pnl: float = 0
    realized_pnl_est: float = 0  # 累计已实现盈亏（来自 ORDER_TRADE_UPDATE 的 rp 字段）
    stop_loss_price: Optional[float] = None
    take_profit_price: Optional[float] = None
    open_order_type: str = "MARKET"
    close_time_ms: Optional[int] = None
    close_qty: float = 0
    avg_close_price: float = 0
    close_quote: float = 0  # 平仓金额累计（qty * price）


class AsyncBinanceCycleStore:
    """
    纯 asyncio 版本的 Binance CycleStore
    
    生命周期:
    1. 创建实例
    2. await store.start() - 启动 WebSocket 和相关服务
    3. 自动处理数据更新
    4. await store.stop() - 停止所有服务
    """
    
    def __init__(
        self,
        api_key: str,
        api_secret: str,
        redis_conn,
        uid: str,
        is_testnet: bool = False,
        user_context: Optional['UserContext'] = None,
        exchange: str = "binance",
        on_auth_failed: Optional[Callable[[str], Any]] = None,
    ):
        """
        初始化 CycleStore
        
        Args:
            api_key: Binance API Key
            api_secret: Binance API Secret
            redis_conn: Redis 连接
            uid: 用户 ID
            is_testnet: 是否测试网
            user_context: 用户上下文（可选）
            exchange: 交易所标识
            on_auth_failed: 认证失败回调
        """
        self.uid = uid
        self.exchange = exchange
        self._api_key = api_key
        self._api_secret = api_secret
        self._redis = redis_conn
        self._is_testnet = is_testnet
        self._user_context = user_context
        self._on_auth_failed = on_auth_failed
        
        # WebSocket
        self._ws: Optional[AsyncFuturesUserWS] = None
        
        # 数据存储
        self._positions: Dict[str, PositionData] = {}
        self._cycles: Dict[str, CycleData] = {}
        self._account: Dict[str, Any] = {}
        
        # 延迟关闭：等待 ORDER_TRADE_UPDATE 到达后再最终关闭
        # field -> (CycleData, close_ts, asyncio.TimerHandle)
        self._pending_closes: Dict[str, tuple] = {}
        
        # 最近关闭的周期（用于 backfill 迟到的 ORDER_TRADE_UPDATE）
        # field -> (CycleData, close_ts)  保留最近 60 秒内关闭的周期
        self._recently_closed: Dict[str, tuple] = {}
        
        # 已见交易 ID（防止 WebSocket 重连重放导致重复计算）
        self._seen_trades: set = set()
        
        # 状态
        self._is_running = False
        self._is_connected = False
        
        # 锁
        self._lock = asyncio.Lock()
        
        self._log_prefix = f"[{uid}][{exchange}]"
    
    @property
    def is_running(self) -> bool:
        return self._is_running
    
    @property
    def is_connected(self) -> bool:
        return self._is_connected
    
    # =========================================================================
    # 公共方法
    # =========================================================================
    
    async def start(self) -> None:
        """启动 CycleStore"""
        if self._is_running:
            logger.warning(f"{self._log_prefix} CycleStore 已在运行")
            return
        
        self._is_running = True
        
        # 创建 WebSocket
        self._ws = AsyncFuturesUserWS(
            on_message=self._on_ws_message,
            api_key=self._api_key,
            is_testnet=self._is_testnet,
            uid=self.uid,
            on_connect=self._on_ws_connect,
            on_disconnect=self._on_ws_disconnect,
            on_auth_failed=self._handle_auth_failed,
            config=WSConfig(
                keepalive_interval=25 * 60,
                message_timeout=60 * 60,  # 60分钟无消息则重连（与 listenKey 有效期一致）
            )
        )
        
        await self._ws.start()
        logger.info(f"{self._log_prefix} CycleStore 已启动")
    
    async def stop(self) -> None:
        """停止 CycleStore"""
        if not self._is_running:
            return
        
        self._is_running = False
        
        # 停止 WebSocket
        if self._ws:
            await self._ws.stop()
            self._ws = None
        
        self._is_connected = False
        logger.info(f"{self._log_prefix} CycleStore 已停止")
    
    def get_positions(self) -> Dict[str, Dict]:
        """获取当前持仓"""
        result = {}
        for k, v in self._positions.items():
            pos = {
                "symbol": v.symbol,
                "side": v.side,
                "qty": str(v.qty),
                "entryPrice": str(v.entry_price),
                "unrealizedPnl": str(v.unrealized_pnl),
                "marginType": v.margin_type,
                "leverage": v.leverage,
                "openTimeMs": str(v.open_time_ms),
                "updatedAt": str(v.updated_at),
                "exchange": self.exchange,
            }
            # P1 enhancement: include peak_pnl from cycle data for position risk assessment
            cycle = self._cycles.get(k)
            if cycle and cycle.peak_pnl:
                pos["peakPnl"] = str(cycle.peak_pnl)
            result[k] = pos
        return result
    
    def get_cycles(self) -> Dict[str, Dict]:
        """获取当前周期"""
        return {
            k: {
                "cycleId": v.cycle_id,
                "symbol": v.symbol,
                "side": v.side,
                "openTimeMs": str(v.open_time_ms),
                "openQty": str(v.open_qty),
                "avgOpenPrice": str(v.avg_open_price),
                "feeTotal": str(v.fee_total),
                "fundingTotal": str(v.funding_total),
                "peakPnl": str(v.peak_pnl),
                "stopLossPrice": str(v.stop_loss_price) if v.stop_loss_price else None,
                "takeProfitPrice": str(v.take_profit_price) if v.take_profit_price else None,
                "openOrderType": v.open_order_type,
                "exchange": self.exchange,
            }
            for k, v in self._cycles.items()
        }
    
    def get_account(self) -> Dict[str, Any]:
        """获取账户信息"""
        return self._account.copy()
    
    # =========================================================================
    # WebSocket 回调
    # =========================================================================
    
    def _on_ws_connect(self) -> None:
        """WebSocket 连接成功"""
        self._is_connected = True
        logger.info(f"{self._log_prefix} WebSocket 已连接")
    
    def _on_ws_disconnect(self, reason: str) -> None:
        """WebSocket 断开连接"""
        self._is_connected = False
        logger.info(f"{self._log_prefix} WebSocket 断开: {reason}")
    
    async def _handle_auth_failed(self, error_msg: str) -> None:
        """处理认证失败"""
        logger.error(f"{self._log_prefix} 认证失败: {error_msg}")
        
        if self._on_auth_failed:
            result = self._on_auth_failed(error_msg)
            if asyncio.iscoroutine(result):
                await result
    
    def _on_ws_message(self, msg: Dict) -> None:
        """
        处理 WebSocket 消息
        
        注意：此方法会在事件循环中被调用，
        如果需要执行异步操作，应创建任务
        """
        event_type = msg.get("e")
        
        try:
            if event_type == "ACCOUNT_UPDATE":
                self._handle_account_update(msg)
            elif event_type == "ORDER_TRADE_UPDATE":
                self._handle_order_update(msg)
            elif event_type == "listenKeyExpired":
                logger.warning(f"{self._log_prefix} ListenKey 过期")
            elif event_type == "MARGIN_CALL":
                logger.warning(f"{self._log_prefix} 保证金警告: {msg}")
        except Exception as e:
            logger.error(f"{self._log_prefix} 处理消息异常: {e}")
    
    # =========================================================================
    # 消息处理
    # =========================================================================
    
    def _handle_account_update(self, msg: Dict) -> None:
        """
        处理账户更新
        
        事件结构:
        {
            "e": "ACCOUNT_UPDATE",
            "T": 1564745798939,
            "a": {
                "B": [{"a": "USDT", "wb": "100", "cw": "100"}],
                "P": [{"s": "BTCUSDT", "pa": "1", "ep": "50000", ...}]
            }
        }
        """
        ts = msg.get("T", int(time.time() * 1000))
        account_data = msg.get("a", {})
        reason = account_data.get("m", "")
        
        # 处理余额
        for balance in account_data.get("B", []):
            if balance.get("a") == "USDT":
                self._account = {
                    "walletBalance": balance.get("wb", "0"),
                    "crossWalletBalance": balance.get("cw", "0"),
                    "ts": ts,
                    "exchange": self.exchange,
                }
                self._save_account_to_redis()
                break
        
        # M4: FUNDING_FEE 事件 — 更新活跃周期的 funding_total
        if reason == "FUNDING_FEE":
            self._handle_funding_fee(account_data, ts)
            return
        
        # 处理持仓
        for pos in account_data.get("P", []):
            self._process_position_update(pos, ts)
    
    def _handle_funding_fee(self, account_data: Dict, ts: int) -> None:
        """
        M4: 处理资金费事件，更新活跃周期的 funding_total
        
        FUNDING_FEE 的 ACCOUNT_UPDATE 格式:
        {
            "m": "FUNDING_FEE",
            "B": [{"a": "USDT", "wb": "...", "cw": "...", "bc": "1.23"}],
            "P": [...]  // isolated 模式下包含持仓，cross 模式下可能为空
        }
        bc = balance change，正数收入，负数支出
        """
        try:
            # 提取资金费金额
            funding_amount = 0.0
            for b in (account_data.get("B") or []):
                if b.get("a") == "USDT":
                    funding_amount = float(b.get("bc", 0) or 0)
                    break
            
            if abs(funding_amount) < 1e-9:
                return
            
            positions = account_data.get("P") or []
            
            if positions:
                # isolated 模式：资金费事件包含持仓信息，精确分配
                for p in positions:
                    symbol = p.get("s")
                    side = (p.get("ps") or "").upper()
                    if not symbol or side not in ("LONG", "SHORT"):
                        continue
                    field = f"{symbol}:{side}"
                    if field in self._cycles:
                        self._cycles[field].funding_total += funding_amount
                        logger.debug(
                            f"{self._log_prefix} 资金费(isolated): {field} "
                            f"amount={funding_amount} total={self._cycles[field].funding_total}"
                        )
            else:
                # cross 模式：按持仓价值比例分摊到所有活跃周期
                active_cycles = {f: c for f, c in self._cycles.items() if c.open_qty > 0}
                if not active_cycles:
                    logger.debug(f"{self._log_prefix} 资金费无活跃周期可分配: {funding_amount}")
                    return
                
                # 计算各周期的持仓价值
                total_notional = sum(c.open_qty * c.avg_open_price for c in active_cycles.values())
                if total_notional <= 0:
                    # 无法按比例分配，平均分摊
                    share = funding_amount / len(active_cycles)
                    for field, cycle in active_cycles.items():
                        cycle.funding_total += share
                else:
                    for field, cycle in active_cycles.items():
                        notional = cycle.open_qty * cycle.avg_open_price
                        share = funding_amount * (notional / total_notional)
                        cycle.funding_total += share
                
                logger.debug(
                    f"{self._log_prefix} 资金费(cross): amount={funding_amount} "
                    f"分摊到 {len(active_cycles)} 个周期"
                )
            
            # 保存更新后的周期数据到 Redis
            self._save_cycles_to_redis()
            
        except Exception as e:
            logger.warning(f"{self._log_prefix} 资金费处理失败: {e}")
    
    def _process_position_update(self, pos: Dict, ts: int) -> None:
        """处理单个持仓更新"""
        symbol = pos.get("s", "")
        position_amt = float(pos.get("pa", 0))
        entry_price = float(pos.get("ep", 0))
        unrealized_pnl = float(pos.get("up", 0))
        margin_type = pos.get("mt", "cross")
        
        # 确定方向
        pos_side = pos.get("ps", "BOTH")
        if pos_side == "BOTH":
            side = "LONG" if position_amt > 0 else "SHORT"
        else:
            side = pos_side
        
        field = f"{symbol}:{side}"
        
        if abs(position_amt) > 0:
            # 有持仓 - 更新或创建
            if field in self._positions:
                # 更新现有持仓
                self._positions[field].qty = abs(position_amt)
                self._positions[field].entry_price = entry_price
                self._positions[field].unrealized_pnl = unrealized_pnl
                self._positions[field].updated_at = ts
            else:
                # 新持仓
                self._positions[field] = PositionData(
                    symbol=symbol,
                    side=side,
                    qty=abs(position_amt),
                    entry_price=entry_price,
                    unrealized_pnl=unrealized_pnl,
                    margin_type=margin_type,
                    open_time_ms=ts,
                    updated_at=ts,
                )
                
                # 创建新周期
                self._create_cycle(symbol, side, abs(position_amt), entry_price, ts)
            
            # 更新周期峰值
            if field in self._cycles:
                if unrealized_pnl > self._cycles[field].peak_pnl:
                    self._cycles[field].peak_pnl = unrealized_pnl
        else:
            # 平仓 — 延迟关闭，等待 ORDER_TRADE_UPDATE 带来真实 realized_pnl
            if field in self._positions:
                self._defer_close(field, ts)
                del self._positions[field]
        
        # 保存到 Redis
        self._save_positions_to_redis()
    
    def _handle_order_update(self, msg: Dict) -> None:
        """
        处理订单更新
        
        事件结构:
        {
            "e": "ORDER_TRADE_UPDATE",
            "T": 1564745798939,
            "o": {
                "s": "BTCUSDT",
                "S": "BUY",          # 订单方向
                "o": "LIMIT",        # 订单类型
                "X": "NEW",          # 订单状态
                "ps": "LONG",        # 持仓方向
                "l": "0.001",        # 本次成交数量
                "L": "50000",        # 本次成交价格
                "n": "0.02",         # 手续费
                "rp": "1.5",         # 已实现盈亏
                ...
            }
        }
        """
        order = msg.get("o", {})
        symbol = order.get("s", "")
        side = order.get("S", "")  # BUY / SELL
        order_type = order.get("o", "")  # LIMIT / MARKET
        status = order.get("X", "")  # NEW / FILLED / CANCELED / PARTIALLY_FILLED
        exec_type = order.get("x", "")  # TRADE / NEW / CANCELED / ...
        
        # 记录订单日志
        logger.debug(
            f"{self._log_prefix} 订单更新: {symbol} {side} {order_type} -> {status} (exec={exec_type})"
        )
        
        # M5: 使用 exec_type == "TRADE" 检测成交（比 status 更可靠）
        if exec_type == "TRADE":
            self._handle_trade_fill(order)
        
        # 处理止损/止盈订单
        if order_type in ("STOP_MARKET", "TAKE_PROFIT_MARKET"):
            self._handle_stop_order(order)
    
    def _handle_stop_order(self, order: Dict) -> None:
        """处理止损/止盈订单"""
        symbol = order.get("s", "")
        order_type = order.get("o", "")
        stop_price = float(order.get("sp", 0))
        status = order.get("X", "")
        
        # 确定对应的持仓
        for field, cycle in self._cycles.items():
            if cycle.symbol == symbol:
                if order_type == "STOP_MARKET":
                    if status == "NEW":
                        cycle.stop_loss_price = stop_price
                    elif status in ("FILLED", "CANCELED", "EXPIRED"):
                        cycle.stop_loss_price = None
                elif order_type == "TAKE_PROFIT_MARKET":
                    if status == "NEW":
                        cycle.take_profit_price = stop_price
                    elif status in ("FILLED", "CANCELED", "EXPIRED"):
                        cycle.take_profit_price = None
                break
    
    def _handle_trade_fill(self, order: Dict) -> None:
        """
        处理成交回报 - 提取已实现盈亏和手续费
        
        Binance ORDER_TRADE_UPDATE 中的关键字段:
        - s: symbol
        - ps: positionSide (LONG/SHORT/BOTH)
        - S: side (BUY/SELL)
        - l: 本次成交数量
        - L: 本次成交价格
        - n: 手续费
        - rp: 已实现盈亏（平仓时非零）
        - t: 成交 ID（用于去重）
        - x: execution type (TRADE = 成交)
        """
        symbol = order.get("s", "")
        pos_side = order.get("ps", "BOTH")
        trade_side = order.get("S", "")  # BUY / SELL
        exec_type = order.get("x", "")   # M5: 使用 exec_type 而非 status
        fill_qty = float(order.get("l", 0))
        fill_price = float(order.get("L", 0))
        fee = float(order.get("n", 0))
        realized_pnl = float(order.get("rp", 0))
        trade_id = order.get("t", "")     # 成交 ID
        order_id = order.get("i", "")     # 订单 ID
        
        # C4: 去重 — 防止 WebSocket 重连重放
        seen_key = f"{symbol}:{trade_id}"
        if seen_key in self._seen_trades:
            logger.debug(f"{self._log_prefix} 跳过重复成交: {seen_key}")
            return
        self._seen_trades.add(seen_key)
        # 限制 seen_trades 大小，防止内存泄漏
        if len(self._seen_trades) > 10000:
            # 清除一半（简单策略，因为 set 无序）
            to_remove = list(self._seen_trades)[:5000]
            for k in to_remove:
                self._seen_trades.discard(k)
        
        # 确定持仓方向
        if pos_side == "BOTH":
            # 单向持仓模式：根据现有 cycle / pending_close / recently_closed 匹配
            matched_side = None
            for field, cycle in self._cycles.items():
                if cycle.symbol == symbol:
                    matched_side = cycle.side
                    break
            if matched_side is None:
                for field in self._pending_closes:
                    if field.startswith(f"{symbol}:"):
                        matched_side = field.split(":")[1]
                        break
            if matched_side is None:
                for field in self._recently_closed:
                    if field.startswith(f"{symbol}:"):
                        matched_side = field.split(":")[1]
                        break
            if matched_side is None:
                return
            pos_side = matched_side
        
        field = f"{symbol}:{pos_side}"
        
        # 判断是开仓还是平仓
        is_close = (
            (pos_side == "LONG" and trade_side == "SELL") or
            (pos_side == "SHORT" and trade_side == "BUY")
        )
        
        # ---- 路径 1: 活跃周期 ----
        if field in self._cycles:
            cycle = self._cycles[field]
            if is_close:
                cycle.realized_pnl_est += realized_pnl
                cycle.fee_total += abs(fee)
                cycle.close_qty += fill_qty
                cycle.close_quote += fill_qty * fill_price
                if cycle.close_qty > 0:
                    cycle.avg_close_price = cycle.close_quote / cycle.close_qty
                logger.debug(
                    f"{self._log_prefix} 平仓成交: {field} qty={fill_qty} price={fill_price} "
                    f"rp={realized_pnl} fee={fee} total_rp={cycle.realized_pnl_est}"
                )
            else:
                # H2: DCA 开仓累加
                old_qty = cycle.open_qty
                old_quote = cycle.avg_open_price * old_qty
                cycle.open_qty += fill_qty
                new_quote = old_quote + fill_qty * fill_price
                if cycle.open_qty > 0:
                    cycle.avg_open_price = new_quote / cycle.open_qty
                cycle.fee_total += abs(fee)
                logger.debug(
                    f"{self._log_prefix} DCA开仓: {field} +{fill_qty} @ {fill_price}, "
                    f"总qty={cycle.open_qty}, avgPrice={cycle.avg_open_price:.4f}"
                )
            self._save_cycles_to_redis()
            return
        
        # ---- 路径 2: 待关闭周期（ACCOUNT_UPDATE 已到，等待此 ORDER_TRADE_UPDATE）----
        if field in self._pending_closes:
            cycle, close_ts, timer = self._pending_closes.pop(field)
            # 取消定时器
            if timer is not None:
                timer.cancel()
            
            if is_close:
                cycle.realized_pnl_est += realized_pnl
                cycle.fee_total += abs(fee)
                cycle.close_qty += fill_qty
                cycle.close_quote += fill_qty * fill_price
                if cycle.close_qty > 0:
                    cycle.avg_close_price = cycle.close_quote / cycle.close_qty
            
            logger.info(
                f"{self._log_prefix} 延迟关闭完成: {field}, "
                f"realizedPnl={cycle.realized_pnl_est}, fee={cycle.fee_total}"
            )
            self._finalize_close(field, cycle, pnl_source="realizedPnl(deferred)")
            return
        
        # ---- 路径 3: C1 backfill — 周期已关闭，迟到的 ORDER_TRADE_UPDATE ----
        if is_close and field in self._recently_closed:
            cycle, close_ts = self._recently_closed[field]
            old_net = cycle.realized_pnl_est + cycle.funding_total - cycle.fee_total
            
            cycle.realized_pnl_est += realized_pnl
            cycle.fee_total += abs(fee)
            cycle.close_qty += fill_qty
            cycle.close_quote += fill_qty * fill_price
            if cycle.close_qty > 0:
                cycle.avg_close_price = cycle.close_quote / cycle.close_qty
            
            new_net = cycle.realized_pnl_est + cycle.funding_total - cycle.fee_total
            delta = new_net - old_net
            
            logger.info(
                f"{self._log_prefix} [BACKFILL] 迟到成交: {field}, "
                f"rp={realized_pnl}, oldNet={old_net:.4f}, newNet={new_net:.4f}, delta={delta:.4f}"
            )
            
            # 更新 Redis 中的已关闭交易
            self._save_closed_cycle(cycle)
            
            # 修正排行榜（增量更新 delta）
            if abs(delta) > 0.001:
                try:
                    from core.commission_service import trigger_commission_on_trade_close
                    trigger_commission_on_trade_close(
                        uid=self.uid,
                        symbol=cycle.symbol,
                        side=cycle.side,
                        net_pnl=delta,  # 只更新差值
                        fee_total=abs(fee),
                        trade_id=f"{cycle.cycle_id}_backfill"
                    )
                    logger.info(
                        f"{self._log_prefix} [BACKFILL] 排行榜增量修正: delta={delta:.4f}"
                    )
                except Exception as e:
                    logger.warning(f"{self._log_prefix} [BACKFILL] 排行榜修正失败: {e}")
            return
        
        # ---- 路径 4: 完全找不到对应周期 ----
        if is_close and realized_pnl != 0:
            logger.warning(
                f"{self._log_prefix} 成交无对应周期: {field}, rp={realized_pnl}, "
                f"trade_id={trade_id} — 数据可能丢失，需要 reconcile"
            )
    
    # =========================================================================
    # 周期管理
    # =========================================================================
    
    def _create_cycle(
        self,
        symbol: str,
        side: str,
        qty: float,
        price: float,
        ts: int
    ) -> None:
        """创建新的交易周期"""
        field = f"{symbol}:{side}"
        cycle_id = f"{symbol}_{side}_{ts}"
        
        self._cycles[field] = CycleData(
            cycle_id=cycle_id,
            symbol=symbol,
            side=side,
            open_time_ms=ts,
            open_qty=qty,
            avg_open_price=price,
        )
        
        logger.info(f"{self._log_prefix} 开仓: {symbol} {side} {qty} @ {price}")
        self._save_cycles_to_redis()
    
    def _defer_close(self, field: str, ts: int) -> None:
        """
        延迟关闭周期 — 等待 ORDER_TRADE_UPDATE 带来真实 realized_pnl
        
        ACCOUNT_UPDATE (position_amt=0) 通常先于 ORDER_TRADE_UPDATE 到达。
        如果立即关闭，realized_pnl_est 可能还是 0，导致错误的 netPnl。
        
        策略：
        1. 将周期从 _cycles 移到 _pending_closes
        2. 启动 5 秒定时器
        3. 如果 ORDER_TRADE_UPDATE 在 5 秒内到达 → _handle_trade_fill 更新后立即关闭
        4. 如果超时 → 用当前 realized_pnl_est 关闭（不回退到 peak_pnl）
        """
        if field not in self._cycles:
            return
        
        cycle = self._cycles.pop(field)
        cycle.close_time_ms = ts
        
        # 如果已经有 realized_pnl_est（ORDER_TRADE_UPDATE 先到了），直接关闭
        if cycle.realized_pnl_est != 0:
            self._finalize_close(field, cycle)
            self._save_cycles_to_redis()
            return
        
        # 延迟关闭：等待 ORDER_TRADE_UPDATE
        logger.info(
            f"{self._log_prefix} 延迟关闭: {field}, 等待 ORDER_TRADE_UPDATE "
            f"(当前 realizedPnl={cycle.realized_pnl_est})"
        )
        
        # 获取事件循环，设置定时器
        try:
            loop = asyncio.get_event_loop()
            timer = loop.call_later(
                5.0,  # 5 秒超时
                self._on_deferred_close_timeout,
                field
            )
            self._pending_closes[field] = (cycle, ts, timer)
        except RuntimeError:
            # 没有事件循环（不应该发生），直接关闭
            logger.warning(f"{self._log_prefix} 无事件循环，直接关闭: {field}")
            self._finalize_close(field, cycle)
        
        self._save_cycles_to_redis()
    
    def _on_deferred_close_timeout(self, field: str) -> None:
        """延迟关闭超时回调 — 5 秒内没收到 ORDER_TRADE_UPDATE"""
        if field not in self._pending_closes:
            return
        
        cycle, ts, _ = self._pending_closes.pop(field)
        
        if cycle.realized_pnl_est != 0:
            pnl_source = "realizedPnl(late)"
        else:
            pnl_source = "realizedPnl=0(timeout)"
            logger.warning(
                f"{self._log_prefix} 延迟关闭超时: {field}, "
                f"ORDER_TRADE_UPDATE 未到达, realized_pnl_est=0, "
                f"将以 netPnl=0 关闭（避免使用错误的 peakPnl={cycle.peak_pnl}）"
            )
        
        self._finalize_close(field, cycle, pnl_source=pnl_source)
    
    def _finalize_close(self, field: str, cycle: 'CycleData', pnl_source: Optional[str] = None) -> None:
        """
        最终关闭周期 — 计算 netPnl、保存、触发返佣
        
        C2 修复：永远不回退到 peak_pnl。如果 realized_pnl_est 为 0，
        netPnl 就是 funding - fee（通常接近 0），而不是错误的 peak_pnl。
        """
        # 计算净盈亏：始终使用 realized_pnl_est，不回退到 peak_pnl
        net_pnl = cycle.realized_pnl_est + cycle.funding_total - cycle.fee_total
        
        if pnl_source is None:
            pnl_source = "realizedPnl" if cycle.realized_pnl_est != 0 else "realizedPnl=0"
        
        # 保存到 Redis 已关闭交易
        self._save_closed_cycle(cycle)
        
        # 移入 recently_closed 用于 backfill
        self._recently_closed[field] = (cycle, cycle.close_time_ms or int(time.time() * 1000))
        self._cleanup_recently_closed()
        
        logger.info(
            f"{self._log_prefix} 平仓: {cycle.symbol} {cycle.side}, "
            f"realizedPnl: {cycle.realized_pnl_est}, peakPnl: {cycle.peak_pnl}, "
            f"netPnl: {net_pnl} (source={pnl_source})"
        )
        
        # 触发返佣发放和排行榜统计更新
        try:
            from core.commission_service import trigger_commission_on_trade_close
            trigger_commission_on_trade_close(
                uid=self.uid,
                symbol=cycle.symbol,
                side=cycle.side,
                net_pnl=net_pnl,
                fee_total=cycle.fee_total,
                trade_id=cycle.cycle_id
            )
        except Exception as e:
            logger.warning(f"{self._log_prefix} 返佣触发失败: {e}")
    
    def _cleanup_recently_closed(self) -> None:
        """清理超过 60 秒的 recently_closed 条目"""
        now_ms = int(time.time() * 1000)
        expired = [
            f for f, (_, close_ts) in self._recently_closed.items()
            if now_ms - close_ts > 60_000
        ]
        for f in expired:
            del self._recently_closed[f]
    
    def _close_cycle(self, field: str, ts: int) -> None:
        """关闭交易周期（直接关闭，用于非延迟场景）"""
        if field not in self._cycles:
            return
        
        cycle = self._cycles.pop(field)
        cycle.close_time_ms = ts
        self._finalize_close(field, cycle)
        self._save_cycles_to_redis()
    
    # =========================================================================
    # Redis 存储
    # =========================================================================
    
    def _save_account_to_redis(self) -> None:
        """保存账户数据到 Redis"""
        try:
            from core.pf_compatibility import pf_compat
            pf_compat.set_pf_account(self.uid, self._account, self.exchange)
        except Exception as e:
            logger.warning(f"{self._log_prefix} 保存账户数据失败: {e}")
    
    def _save_positions_to_redis(self) -> None:
        """保存持仓数据到 Redis"""
        try:
            from core.pf_compatibility import pf_compat
            
            positions = self.get_positions()
            pf_compat.set_pf_pos(self.uid, positions, self.exchange)
            
            # 更新活跃持仓列表
            active_list = list(positions.keys())
            pf_compat.set_pf_pos_active(self.uid, active_list, self.exchange)
        except Exception as e:
            logger.warning(f"{self._log_prefix} 保存持仓数据失败: {e}")
    
    def _save_cycles_to_redis(self) -> None:
        """保存周期数据到 Redis"""
        try:
            from core.pf_compatibility import pf_compat
            
            cycles = self.get_cycles()
            pf_compat.set_pf_cycle(self.uid, cycles, self.exchange)
        except Exception as e:
            logger.warning(f"{self._log_prefix} 保存周期数据失败: {e}")
    
    def _save_closed_cycle(self, cycle: CycleData) -> None:
        """保存已关闭的周期到 Redis"""
        try:
            from core.pf_compatibility import pf_compat
            
            # 计算 netPnl（与 _close_cycle 保持一致）
            if cycle.realized_pnl_est != 0:
                net_pnl = cycle.realized_pnl_est + cycle.funding_total - cycle.fee_total
            else:
                net_pnl = cycle.peak_pnl + cycle.funding_total - cycle.fee_total
            
            closed_data = {
                "cycleId": cycle.cycle_id,
                "symbol": cycle.symbol,
                "side": cycle.side,
                "openTimeMs": str(cycle.open_time_ms),
                "closeTimeMs": str(cycle.close_time_ms),
                "durationMs": str((cycle.close_time_ms or 0) - cycle.open_time_ms),
                "avgOpenPrice": str(cycle.avg_open_price),
                "avgClosePrice": str(cycle.avg_close_price),
                "openQty": str(cycle.open_qty),
                "closeQty": str(cycle.close_qty or cycle.open_qty),
                "maxAbsQty": str(max(cycle.open_qty, cycle.close_qty)),
                "feeTotal": str(cycle.fee_total),
                "fundingTotal": str(cycle.funding_total),
                "realizedPnlEst": str(cycle.realized_pnl_est),
                "netPnl": str(net_pnl),
                "peakPnl": str(cycle.peak_pnl),
                "drawdownToClose": str(max(0, cycle.peak_pnl - net_pnl)),
                "maxDrawdown": str(0),  # AsyncCycleStore 暂不跟踪 maxDrawdown
                "closeTradeCount": str(0),
                "openOrderType": cycle.open_order_type,
                "exchange": self.exchange,
            }
            
            # 使用兼容层保存
            pf_compat.set_pf_closed_h(self.uid, cycle.cycle_id, closed_data, self.exchange)
        except Exception as e:
            logger.warning(f"{self._log_prefix} 保存已关闭周期失败: {e}")
