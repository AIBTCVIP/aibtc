# account_positions.py
import json
import logging
from typing import TYPE_CHECKING, Optional, Dict, List, Any
from binance.client import Client
from core.config import BINANCE_ENVIRONMENT
from cache.position_cache import position_records
from core.constants import ALL_TP_SL_TYPES, ACTIVE_ORDER_STATUSES

if TYPE_CHECKING:
    from core.user_context import UserContext

logger = logging.getLogger(__name__)

# ============================================================
# 通用工具函数
# ============================================================

def _safe_call(fn, default, label: str = "", uid: str = ""):
    """统一兜底：SDK 报错不影响主流程"""
    try:
        return fn()
    except Exception as e:
        prefix = f"[{uid}] " if uid else ""
        if label:
            logger.warning(f"{prefix}{label} 调用失败: {e}")
        else:
            logger.warning(f"{prefix}调用失败: {e}")
        return default


# ============================================================
# 多用户版本函数（新增）
# ============================================================

def get_all_open_orders_for_user(ctx: 'UserContext') -> List[Dict]:
    """
    获取全账户所有普通挂单（多用户版本）
    
    注意：仅支持 Binance 交易所。如果用户未启用 Binance，返回空列表。
    """
    if ctx.client is None:
        # 用户未启用 Binance，跳过
        return []
    
    # 使用 API Key 级别限速器
    from core.rate_limiter import get_binance_rate_limiter
    api_key = getattr(ctx.client, 'API_KEY', None)
    rate_limiter = get_binance_rate_limiter(api_key)
    rate_limiter.acquire(endpoint="futures_get_open_orders", timeout=30.0)
    
    return _safe_call(
        lambda: ctx.client.futures_get_open_orders(),
        [],
        "futures_get_open_orders(all)",
        ctx.uid
    )


def get_tp_sl_orders_for_user(ctx: 'UserContext', symbol: str, position_side: str) -> List[Dict]:
    """
    查询某持仓方向的所有 TP/SL（多用户版本）
    
    改造说明：
    - 不再使用 REST API 查询，改为从 Redis 缓存读取
    - TP/SL 数据由 WebSocket ORDER_TRADE_UPDATE 事件实时更新
    - 缓存 key: pf:pos:{uid}:{exchange} 中的 tp_price/sl_price 字段
    
    注意：仅支持 Binance 交易所。如果用户未启用 Binance，返回空列表。
    """
    if ctx.client is None:
        return []
    
    orders = []
    
    # 从 Redis 读取 WebSocket 更新的 TP/SL 数据
    try:
        from core.pf_compatibility import pf_compat
        pos_data = pf_compat.get_pf_pos(ctx.uid, "binance")
        
        field = f"{symbol}:{position_side}"
        pos = pos_data.get(field) if pos_data else None
        
        if pos:
            # 从持仓数据中读取 TP/SL 价格（由 WebSocket 更新）
            tp_price = float(pos.get("tp_price") or 0)
            sl_price = float(pos.get("sl_price") or 0)
            
            if tp_price > 0:
                orders.append({
                    "type": "TAKE_PROFIT_MARKET",
                    "positionSide": position_side,
                    "stopPrice": tp_price,
                    "source": "websocket_cache"
                })
            
            if sl_price > 0:
                orders.append({
                    "type": "STOP_MARKET",
                    "positionSide": position_side,
                    "stopPrice": sl_price,
                    "source": "websocket_cache"
                })
    except Exception as e:
        # 如果读取失败，返回空列表（不影响主流程）
        logger.debug(f"[{ctx.uid}] 读取 TP/SL 缓存失败: {e}")
    
    return orders


def get_open_limit_orders_for_user(
    ctx: 'UserContext',
    symbol: str = None,
    position_side: str = None
) -> List[Dict]:
    """
    返回全账户非 reduceOnly/非 closePosition 的 LIMIT 入场挂单（多用户版本）
    """
    open_orders = get_all_open_orders_for_user(ctx)
    
    if not open_orders:
        return []
    
    orders = []
    for o in open_orders:
        order_type = o.get("type")
        reduce_only = o.get("reduceOnly")
        close_position = o.get("closePosition")
        status = o.get("status")
        order_symbol = o.get("symbol")
        order_pos_side = o.get("positionSide")
        
        if order_type != "LIMIT":
            continue
        if reduce_only is True:
            continue
        if close_position is True:
            continue
        if status not in ACTIVE_ORDER_STATUSES:
            continue
        if symbol and order_symbol != symbol:
            continue
        if position_side and order_pos_side != position_side:
            continue
        
        orders.append({
            "symbol": order_symbol,
            "orderId": o.get("orderId"),
            "clientOrderId": o.get("clientOrderId"),
            "side": o.get("side"),
            "positionSide": order_pos_side,
            "price": float(o.get("price") or 0),
            "origQty": float(o.get("origQty") or 0),
            "executedQty": float(o.get("executedQty") or 0),
            "timeInForce": o.get("timeInForce"),
            "goodTillDate": o.get("goodTillDate"),
            "updateTime": int(o.get("updateTime", 0)) / 1000,
            "source": "limit_entry"
        })
    
    return orders


def get_account_status_for_user(ctx: 'UserContext') -> Dict[str, Any]:
    """
    获取账户状态（多用户版本）
    
    结果存入 ctx.account_snapshot 和 ctx.tp_sl_cache
    
    注意：此函数专门用于 Binance 交易所。
    如果用户未启用 Binance，返回空的 account_snapshot。
    """
    uid = ctx.uid
    client = ctx.client
    
    # 如果没有 Binance 客户端，返回空的 snapshot
    if client is None:
        ctx.account_snapshot["balance"] = 0.0
        ctx.account_snapshot["available"] = 0.0
        ctx.account_snapshot["total_unrealized"] = 0.0
        ctx.account_snapshot["positions"] = []
        ctx.account_snapshot["open_limit_orders"] = []
        ctx.account_snapshot["uid"] = uid
        return ctx.account_snapshot
    
    # 获取限速许可（使用 API Key 级别限速）
    from core.rate_limiter import get_binance_rate_limiter
    api_key = getattr(client, 'API_KEY', None)
    rate_limiter = get_binance_rate_limiter(api_key)
    rate_limiter.acquire(endpoint="futures_account", timeout=30.0)
    
    data = _safe_call(lambda: client.futures_account(), {}, "futures_account", uid)
    
    # 检查是否有持仓
    has_positions = any(
        float(p.get("positionAmt") or 0) != 0 
        for p in data.get("positions", [])
    )
    
    # 检查是否有挂单（从 Redis 缓存读取，避免额外 API 调用）
    from core.pf_compatibility import pf_compat
    cached_orders = pf_compat.get_pf_open_orders(uid, "binance")
    has_orders = bool(cached_orders)
    
    # 只有在有持仓或有挂单时才获取标记价格，避免无谓的 API 调用
    mark_dict = {}
    if has_positions or has_orders:
        from core.binance_public_cache import get_binance_public_cache
        cache = get_binance_public_cache()
        mark_dict = cache.get_all_mark_prices()
        
        # 如果 Redis 没有数据，回退到 API（但这种情况应该很少）
        if not mark_dict:
            import logging
            logging.getLogger(__name__).warning(f"[{uid}] Redis 无标记价格数据，回退到 API")
            # 回退调用也需要限速
            rate_limiter.acquire(endpoint="futures_mark_price", timeout=30.0)
            premium = _safe_call(lambda: client.futures_mark_price(), [], "futures_mark_price", uid)
            mark_dict = {
                item["symbol"]: float(item["markPrice"])
                for item in premium
                if "symbol" in item and "markPrice" in item
            }
    
    balance = float(data.get("totalWalletBalance", 0))
    available = float(data.get("availableBalance", 0))
    total_unrealized = float(data.get("totalUnrealizedProfit", 0))
    
    positions = []
    symbols = set()
    
    ctx.tp_sl_cache.clear()
    
    for p in data.get("positions", []):
        size = float(p.get("positionAmt") or 0)
        if size == 0:
            continue
        
        symbol = p.get("symbol", "")
        entry = float(p.get("entryPrice") or 0)
        mark = mark_dict.get(symbol, entry)
        pnl = float(p.get("unrealizedProfit") or 0)
        
        pos_side = "LONG" if size > 0 else "SHORT"
        symbols.add(symbol)
        
        # 查询该 symbol & direction 的 TP/SL
        orders = get_tp_sl_orders_for_user(ctx, symbol, pos_side)
        if symbol not in ctx.tp_sl_cache:
            ctx.tp_sl_cache[symbol] = {}
        ctx.tp_sl_cache[symbol][pos_side] = orders
        
        positions.append({
            "symbol": symbol,
            "size": size,
            "entry": entry,
            "mark_price": mark,
            "leverage": int(float(p.get("leverage") or 0)),
            "pnl": pnl,
            "position_value": abs(size) * mark,
            "last_update_time": int(p.get("updateTime", 0)) / 1000
        })
    
    # 更新用户级缓存
    ctx.position_records.clear()
    ctx.position_records.update(symbols)
    
    ctx.account_snapshot["balance"] = balance
    ctx.account_snapshot["available"] = available
    ctx.account_snapshot["total_unrealized"] = total_unrealized
    ctx.account_snapshot["positions"] = positions
    ctx.account_snapshot["open_limit_orders"] = get_open_limit_orders_for_user(ctx)
    ctx.account_snapshot["uid"] = uid
    
    return ctx.account_snapshot


def get_open_positions_for_user(ctx: 'UserContext') -> List[str]:
    """返回当前持仓涉及的 symbol 列表（多用户版本）"""
    return list(ctx.position_records)