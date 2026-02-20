# context/position_order_formatter.py - 持仓/订单展示用计算，供 LLM 投喂层使用
"""
从持仓/订单原始数据计算展示用字段：distance_pct、pnl_pct、sl_distance_pct、tp_distance_pct、
unrealized_pnl_usdt、drawdown_from_peak_pct 等。供 llm 的 context_builder 与 llm_api 统一调用。
"""
import time
from typing import Dict, List, Any, Optional


def resolve_sl_dist(pos: dict) -> Optional[float]:
    """
    Compute sl_distance_pct from entry and stop_loss (entry-based, for fixed risk measurement).
    Used by build_positions_layer for the positions layer.
    """
    sl = pos.get("stop_loss") or pos.get("stop_loss_price")
    entry = pos.get("entry", 0) or 0
    if sl and entry and entry > 0:
        return round(abs(entry - float(sl)) / entry * 100, 2)
    return None


def enrich_order_for_feed(order: dict, current_price: Optional[float] = None) -> dict:
    """
    Add distance_pct and notional_value to an order dict for feed.
    Preserves all original keys; current_price may come from dataset.
    """
    order_price = float(order.get("price") or 0)
    order_qty = float(order.get("origQty") or 0)
    out = dict(order)
    out["price"] = order_price
    out["origQty"] = order_qty
    if current_price is not None:
        out["current_price"] = current_price
        if current_price > 0 and order_price > 0:
            out["distance_pct"] = round((order_price - current_price) / current_price * 100, 2)
    if order_price and order_qty:
        out["notional_value"] = round(order_price * order_qty, 2)
    return out


def enrich_position_for_feed(p: dict) -> dict:
    """
    From raw exchange position, compute pnl_pct, sl_distance_pct, tp_distance_pct (vs mark),
    position_duration, sl_breached, and build the position dict for feed (output["positions"]).
    """
    size = float(p.get("size") or 0)
    entry = float(p.get("entry") or 0)
    mark = float(p.get("mark_price") or 0)
    stop_loss_price = p.get("stop_loss_price")
    take_profit_price = p.get("take_profit_price")

    pnl_pct = None
    if entry != 0:
        if size > 0:
            pnl_pct = round((mark - entry) / entry * 100, 2)
        elif size < 0:
            pnl_pct = round((entry - mark) / entry * 100, 2)

    sl_distance_pct = None
    tp_distance_pct = None
    sl_breached = False
    if mark > 0:
        is_long = size > 0
        if stop_loss_price:
            sl_price = float(stop_loss_price)
            if is_long:
                raw_dist = (mark - sl_price) / mark * 100
                sl_distance_pct = round(abs(raw_dist), 2)
                if raw_dist < 0:
                    sl_breached = True
            else:
                raw_dist = (sl_price - mark) / mark * 100
                sl_distance_pct = round(abs(raw_dist), 2)
                if raw_dist < 0:
                    sl_breached = True
        if take_profit_price:
            tp_price = float(take_profit_price)
            if is_long:
                tp_distance_pct = round(abs(tp_price - mark) / mark * 100, 2)
            else:
                tp_distance_pct = round(abs(mark - tp_price) / mark * 100, 2)

    position_duration = None
    position_duration_unit = None
    entry_time_ms = p.get("entry_time")
    if entry_time_ms:
        duration_ms = int(time.time() * 1000) - int(entry_time_ms)
        if duration_ms > 0:
            duration_minutes = duration_ms / (1000 * 60)
            if duration_minutes < 60:
                position_duration = round(duration_minutes, 1)
                position_duration_unit = "minutes"
            else:
                position_duration = round(duration_minutes / 60, 1)
                position_duration_unit = "hours"

    return {
        "symbol": p.get("symbol", "UNKNOWN"),
        "side": "LONG" if size > 0 else "SHORT",
        "size": abs(size),
        "position_value": p.get("position_value", 0),
        "entry": entry,
        "mark_price": mark,
        "pnl": float(p.get("pnl") or 0),
        "pnl_pct": pnl_pct,
        "stop_loss": stop_loss_price,
        "take_profit": take_profit_price,
        "sl_distance_pct": sl_distance_pct,
        "sl_breached": sl_breached if sl_breached else None,
        "tp_distance_pct": tp_distance_pct,
        "leverage": p.get("leverage", 1),
        "entry_time": p.get("entry_time"),
        "position_duration": position_duration,
        "position_duration_unit": position_duration_unit,
        "last_update_time": p.get("last_update_time", 0),
        "exchange": p.get("exchange", ""),
        "market_bias": None,
        "market_bias_score": None,
        "market_bias_strength": None,
        "alignment": None,
    }


def build_positions_layer(
    positions: List[dict],
    markets: Dict[str, dict],
    balance_usdt: float = 0,
) -> list:
    """
    Build positions list for LLM layer — data only.
    Uses resolve_sl_dist for entry-based sl_distance_pct; other fields from enriched position or computed here.
    """
    if not positions:
        return []

    result = []
    now_ms = time.time() * 1000

    for pos in positions:
        symbol = pos.get("symbol", "")
        side = (pos.get("side") or "").lower()
        if side == "long" or side == "short":
            pass
        else:
            side = "long" if (pos.get("size", 0) or 0) > 0 else "short"
        entry = pos.get("entry", 0) or 0
        mark = pos.get("mark_price", 0) or 0
        pnl_pct = pos.get("pnl_pct") or 0
        sl = pos.get("stop_loss") or pos.get("stop_loss_price")
        tp = pos.get("take_profit") or pos.get("take_profit_price")
        sl_dist = resolve_sl_dist(pos)
        exchange_leverage = pos.get("leverage", 1) or 1
        position_value = pos.get("position_value", 0) or 0
        leverage = round(position_value / balance_usdt, 2) if balance_usdt > 0 and position_value > 0 else exchange_leverage

        tp_dist = None
        if tp and entry and entry > 0:
            if side == "long":
                tp_dist = round((float(tp) - entry) / entry * 100, 2)
            else:
                tp_dist = round((entry - float(tp)) / entry * 100, 2)

        unrealized_pnl_usdt = None
        if position_value > 0 and pnl_pct:
            unrealized_pnl_usdt = round(position_value * pnl_pct / 100, 2)

        current_rr_ratio = None
        if sl_dist and tp_dist and sl_dist > 0:
            current_rr_ratio = round(tp_dist / sl_dist, 2)

        liquidation_price = None
        if entry > 0 and exchange_leverage > 1:
            margin_ratio = 1 / exchange_leverage
            if side == "long":
                liquidation_price = round(entry * (1 - margin_ratio * 0.9), 2)
            else:
                liquidation_price = round(entry * (1 + margin_ratio * 0.9), 2)

        peak_pnl_pct = pos.get("peak_pnl_pct") or pos.get("peak_pnl")
        if peak_pnl_pct is None:
            peak_pnl = pos.get("peak_pnl")
            if peak_pnl is not None and position_value > 0:
                peak_pnl_pct = round(peak_pnl / position_value * 100, 2)
        drawdown_from_peak_pct = None
        if peak_pnl_pct is not None and peak_pnl_pct > 0:
            drawdown_from_peak_pct = round(peak_pnl_pct - pnl_pct, 2) if pnl_pct < peak_pnl_pct else 0

        funding_accumulated_pct = pos.get("funding_accumulated_pct")
        next_funding_impact_pct = None
        market_data = markets.get(symbol, {})
        quick = market_data.get("_quick", {})
        fr = quick.get("funding_rate") or market_data.get("funding_rate")
        if fr is not None:
            if side == "long":
                next_funding_impact_pct = round(-fr * 100, 4)
            else:
                next_funding_impact_pct = round(fr * 100, 4)

        hold_hours = None
        duration_val = pos.get("position_duration") or pos.get("hold_hours")
        duration_unit = pos.get("position_duration_unit", "minutes")
        if duration_val is not None:
            if duration_unit == "hours":
                hold_hours = round(duration_val, 1)
            elif duration_unit == "days":
                hold_hours = round(duration_val * 24, 1)
            else:
                hold_hours = round(duration_val / 60, 1)

        entry_reason_tags = pos.get("entry_reason_tags") or pos.get("reason_tags") or pos.get("entry_tags") or []

        position_entry = {
            "symbol": symbol,
            "side": side,
            "size_usdt": round(position_value, 2) if position_value else None,
            "leverage": leverage,
            "entry_price": entry,
            "mark_price": mark,
            "liquidation_price": liquidation_price,
            "unrealized_pnl_usdt": unrealized_pnl_usdt,
            "unrealized_pnl_pct": round(pnl_pct, 2) if pnl_pct else 0,
            "peak_pnl_pct": round(peak_pnl_pct, 2) if peak_pnl_pct else None,
            "drawdown_from_peak_pct": drawdown_from_peak_pct,
            "stop_loss": sl,
            "take_profit": tp,
            "sl_distance_pct": round(sl_dist, 2) if sl_dist else None,
            "tp_distance_pct": round(tp_dist, 2) if tp_dist else None,
            "current_rr_ratio": current_rr_ratio,
            "hold_hours": hold_hours,
            "funding_accumulated_pct": funding_accumulated_pct,
            "next_funding_impact_pct": next_funding_impact_pct,
            "entry_reason_tags": entry_reason_tags if entry_reason_tags else None,
        }
        position_entry = {k: v for k, v in position_entry.items() if v is not None}
        result.append(position_entry)

    return result


def format_pending_orders(pending_orders: List[dict]) -> list:
    """
    Format pending orders for LLM layer — symbol, side, type, price, size_usdt, current_price, distance_pct, created_hours_ago.
    """
    result = []
    now_ms = time.time() * 1000

    for order in pending_orders:
        price = order.get("price")
        current = order.get("current_price")
        distance_pct = order.get("distance_pct")
        if distance_pct is None and price and current and current > 0:
            distance_pct = round(abs(price - current) / current * 100, 2)

        created_hours_ago = None
        created_ts = order.get("created_at_ms") or order.get("timestamp_ms") or order.get("time")
        if created_ts:
            created_hours_ago = round((now_ms - created_ts) / 3600000, 1)

        size_usdt = order.get("size_usdt") or order.get("notional_value") or order.get("size")
        if size_usdt:
            size_usdt = round(float(size_usdt), 2)

        entry = {
            "symbol": order.get("symbol"),
            "side": order.get("side"),
            "type": order.get("type", "limit"),
            "price": price,
            "size_usdt": size_usdt,
            "current_price": current,
            "distance_pct": distance_pct,
            "created_hours_ago": created_hours_ago,
        }
        entry = {k: v for k, v in entry.items() if v is not None}
        result.append(entry)

    return result
