# market_context.py - Global Market Context Builder
"""
Global Context Builder (AI Self-Analysis Version)

Provides global context for multi-symbol analysis to help AI make better cross-symbol decisions.

Design principles: Only provide factual data, let AI make judgments
- Remove verdict / score conclusion fields
- Keep factors as factual reference

Usage:
    from analysis.context.market_context import build_global_context
    context = build_global_context(symbols, positions, orders, balance)
"""

from typing import Dict, List, Optional
import logging

# Import config and modules
from analysis.context.time_context import build_time_context
from analysis.specialized.sentiment import build_sentiment_context


# ==========================================================
# Helper Functions
# ==========================================================
def _get_external_data_fallback(symbol: str) -> dict:
    """
    从全局市场数据获取外部数据（funding_rate, oi, change_24h_pct 等）
    
    这是一个 fallback，当 symbols_data 中没有这些数据时使用
    """
    try:
        from analysis.data.global_market_fetcher import get_external_data
        return get_external_data(symbol)
    except Exception:
        return {}


def _extract_technicals(symbol_data: dict, timeframe: str = "tf4h") -> dict:
    """
    Extract technicals for a specific timeframe from symbol data.
    
    Args:
        symbol_data: Symbol payload data
        timeframe: "tf4h" (default) or "tf1h"
    
    Supports multiple data formats:
    1. Flat structure: {ai_enhancement: {technicals: {tf4h: {...}}}}
    2. Markets format: {timeframes: {15m: {indicators: {ai_enhancement: {...}}}}}
    """
    if not symbol_data:
        return {}
    
    ai_enh = symbol_data.get("ai_enhancement", {})
    
    # Path 1: Flat structure (ai_enhancement.technicals) - 当前使用的结构
    technicals = ai_enh.get("technicals", {})
    tf_data = technicals.get(timeframe, {})
    if tf_data.get("available"):
        return tf_data
    
    # Path 2: From timeframes.*.indicators.ai_enhancement (from markets format)
    timeframes = symbol_data.get("timeframes", {})
    for tf in ["4h", "1h", "15m"]:
        tf_d = timeframes.get(tf, {})
        indicators = tf_d.get("indicators", {})
        ai_enh_nested = indicators.get("ai_enhancement", {})
        technicals = ai_enh_nested.get("technicals", {})
        tf_data = technicals.get(timeframe, {})
        if tf_data.get("available"):
            return tf_data
    
    return {}


# ==========================================================
# Main Functions
# ==========================================================


def build_global_context(
        symbols_data: Dict[str, dict],  # {symbol: payload}
        positions: List[dict],
        open_orders: List[dict],
        balance: float,
        total_unrealized: float = 0.0,
        include_external_sentiment: bool = True,
        max_leverage: float = 30.0,
) -> dict:
    """
    Build global context for multi-symbol analysis.

    Args:
        symbols_data: All symbol payload data (should include BTCUSDT/ETHUSDT from Redis)
        positions: Current position list
        open_orders: Open order list
        balance: Account balance
        total_unrealized: Total unrealized PnL (for accurate equity calculation)
        include_external_sentiment: Whether to include external API sentiment data

    Returns:
        Global context dictionary
        
    注意：删除了 symbol_context，因为它与 markets.*.ai_enhancement 重复
    """
    # Extract data for sentiment analysis
    sentiment_data = _extract_sentiment_data(symbols_data)
    
    return {
        # Core context
        "market_regime": _build_market_regime(symbols_data),
        "account_risk": _build_account_risk(positions, open_orders, balance, total_unrealized, max_leverage=max_leverage),
        # 注意：删除了 symbol_context，因为它与 markets.*.ai_enhancement 重复
        "position_distribution": _build_position_distribution(positions),
        
        # 🆕 时间上下文
        "time_context": build_time_context(),
        
        # 🆕 市场情绪
        "market_sentiment": build_sentiment_context(
            sentiment_data, 
            include_external=include_external_sentiment
        ),
    }


def _extract_sentiment_data(symbols_data: Dict[str, dict]) -> Dict[str, dict]:
    """
    Extract sentiment data from symbols_data
    
    Supports multiple data formats:
    1. Direct: {symbol: {funding_rate: x, ...}}
    2. Nested: {symbol: {timeframes: {15m: {indicators: {ai_enhancement: {...}}}}}}
    3. Markets format from payload: {symbol: {price: x, funding_rate: x, timeframes: {...}}}
    """
    result = {}
    for symbol, data in symbols_data.items():
        if not isinstance(data, dict):
            continue
            
        # Try multiple extraction paths
        funding_rate = None
        open_interest = None
        change_pct = None
        
        # Path 1: Direct fields (markets format)
        funding_rate = data.get("funding_rate")
        open_interest = data.get("open_interest")
        change_pct = data.get("24h_change_pct")
        
        # Path 2: From ai_enhancement (new 3-layer structure)
        if funding_rate is None:
            ai_enhancement = data.get("ai_enhancement", {})
            # Flat structure: ai_enhancement.market_context
            market_context = ai_enhancement.get("market_context", {})
            funding_rate = market_context.get("funding_rate")
        
        # Path 3: From nested timeframes structure
        if funding_rate is None:
            timeframes = data.get("timeframes", {})
            for tf in ["15m", "1h", "4h"]:
                tf_data = timeframes.get(tf, {})
                indicators = tf_data.get("indicators", {})
                ai_enh = indicators.get("ai_enhancement", {})
                mkt_ctx = ai_enh.get("market_context", {})
                if mkt_ctx.get("funding_rate"):
                    funding_rate = mkt_ctx.get("funding_rate")
                    break
        
        result[symbol] = {
            "funding_rate": funding_rate,
            "open_interest": open_interest,
            "24h_change_pct": change_pct,
        }
    return result


def _build_market_regime(symbols_data: Dict[str, dict]) -> dict:
    """
    Build market regime (BTC/ETH 主导方案)
    
    设计原则：
    - Regime 只回答"大盘方向"，基于 BTC/ETH 这两个真正代表大盘的币种
    - 移除固定 9 币种投票机制，避免与用户实际币种池脱节
    - 个股是否跟随 regime 由 btc_correlation 字段决定，LLM 自行判断
    
    4h trend_direction 值说明：
    - up: 上涨趋势，价格在 EMA 上方
    - down: 下跌趋势，价格在 EMA 下方
    - neutral: 震荡，无明显趋势
    - weakening: 上涨趋势转弱（EMA 仍上涨但价格已跌破）
    - recovering: 下跌趋势反弹（EMA 仍下跌但价格已突破）
    
    Regime 状态判定（仅基于 BTC/ETH）：
    - BTC+ETH 同向 up/recovering → strong_bullish
    - BTC+ETH 同向 down/weakening → strong_bearish
    - BTC up + ETH neutral/unknown (或反之) → mild_bullish
    - BTC down + ETH neutral/unknown (或反之) → mild_bearish
    - BTC/ETH 方向相反 或 都是 neutral → mixed
    
    Confidence 判定：
    - high: BTC+ETH 同向且都是确认趋势 (up/down)
    - moderate: BTC+ETH 同向但有过渡状态 (recovering/weakening)
    - low: BTC/ETH 分歧或数据不足
    
    风险偏好：
    - BTC up/recovering + bullish regime → risk_on
    - BTC down/weakening + bearish regime → risk_off
    - 其余 → neutral
    """
    btc = symbols_data.get("BTCUSDT", {})
    eth = symbols_data.get("ETHUSDT", {})

    # ── BTC 数据提取 ──
    btc_trend = "unknown"
    btc_change_pct = None
    btc_price_vs_ema = None
    btc_trend_strength = None
    btc_trend_confidence = None
    btc_rsi_4h = None
    btc_adx_4h = None
    btc_atr_pct_1h = None
    btc_funding_rate = btc.get("funding_rate")
    btc_oi_change = btc.get("oi_change_24h_pct")
    
    # Fallback: 从全局市场数据获取外部数据
    if btc_funding_rate is None or btc_oi_change is None or btc_change_pct is None:
        btc_ext = _get_external_data_fallback("BTCUSDT")
        if btc_funding_rate is None:
            btc_funding_rate = btc_ext.get("funding_rate")
        if btc_oi_change is None:
            btc_oi_change = btc_ext.get("oi_change_24h_pct")
        if btc_change_pct is None:
            btc_change_pct = btc_ext.get("change_24h_pct")
    
    # 先提取 1h 数据获取 close 价格
    btc_trend_1h = "unknown"
    btc_tech_1h = _extract_technicals(btc, "tf1h")
    if btc_tech_1h.get("available"):
        btc_trend_1h = btc_tech_1h.get("structure_state", "unknown")
    
    # 价格优先从 tf1h.close 获取，回退到 tf4h.close 或 payload.price
    btc_technicals = _extract_technicals(btc)
    btc_price = btc.get("price") or btc_tech_1h.get("close") or btc_technicals.get("close")
    
    if btc_technicals.get("available"):
        btc_trend = btc_technicals.get("trend_direction", "unknown")
        btc_change_pct = btc_change_pct or btc.get("24h_change_pct")  # 保留 fallback 值
        btc_price_vs_ema = btc_technicals.get("price_vs_ema")
        btc_trend_strength = btc_technicals.get("trend_strength")
        btc_trend_confidence = btc_technicals.get("trend_confidence")
        btc_adx_4h = btc_technicals.get("adx")
        btc_rsi_4h = btc_technicals.get("rsi")  # RSI 来自 4h
    
    # 计算 ATR%
    atr_1h = btc_tech_1h.get("atr")
    if atr_1h and btc_price and btc_price > 0:
        btc_atr_pct_1h = round(atr_1h / btc_price * 100, 3)

    # ── ETH 数据提取 ──
    eth_trend = "unknown"
    eth_change_pct = None
    eth_price_vs_ema = None
    eth_trend_strength = None
    eth_trend_confidence = None
    eth_rsi_4h = None
    eth_adx_4h = None
    eth_atr_pct_1h = None
    eth_funding_rate = eth.get("funding_rate")
    eth_oi_change = eth.get("oi_change_24h_pct")
    
    # Fallback: 从全局市场数据获取外部数据
    if eth_funding_rate is None or eth_oi_change is None or eth_change_pct is None:
        eth_ext = _get_external_data_fallback("ETHUSDT")
        if eth_funding_rate is None:
            eth_funding_rate = eth_ext.get("funding_rate")
        if eth_oi_change is None:
            eth_oi_change = eth_ext.get("oi_change_24h_pct")
        if eth_change_pct is None:
            eth_change_pct = eth_ext.get("change_24h_pct")
    
    # 先提取 1h 数据获取 close 价格
    eth_trend_1h = "unknown"
    eth_tech_1h = _extract_technicals(eth, "tf1h")
    if eth_tech_1h.get("available"):
        eth_trend_1h = eth_tech_1h.get("structure_state", "unknown")
    
    # 价格优先从 tf1h.close 获取，回退到 tf4h.close 或 payload.price
    eth_technicals = _extract_technicals(eth)
    eth_price = eth.get("price") or eth_tech_1h.get("close") or eth_technicals.get("close")
    
    if eth_technicals.get("available"):
        eth_trend = eth_technicals.get("trend_direction", "unknown")
        eth_change_pct = eth_change_pct or eth.get("24h_change_pct")  # 保留 fallback 值
        eth_price_vs_ema = eth_technicals.get("price_vs_ema")
        eth_trend_strength = eth_technicals.get("trend_strength")
        eth_trend_confidence = eth_technicals.get("trend_confidence")
        eth_adx_4h = eth_technicals.get("adx")
        eth_rsi_4h = eth_technicals.get("rsi")  # RSI 来自 4h
    
    # 计算 ATR%
    atr_1h = eth_tech_1h.get("atr")
    if atr_1h and eth_price and eth_price > 0:
        eth_atr_pct_1h = round(atr_1h / eth_price * 100, 3)

    # ── Regime 状态判定（仅基于 BTC/ETH 4h）──
    # 方向分类
    _BULLISH = ("up", "recovering")
    _BEARISH = ("down", "weakening")
    _CONFIRMED_BULLISH = ("up",)
    _CONFIRMED_BEARISH = ("down",)
    
    btc_bullish = btc_trend in _BULLISH
    btc_bearish = btc_trend in _BEARISH
    eth_bullish = eth_trend in _BULLISH
    eth_bearish = eth_trend in _BEARISH
    
    # 判定 regime state
    if btc_bullish and eth_bullish:
        market_sentiment = "strong_bullish"
    elif btc_bearish and eth_bearish:
        market_sentiment = "strong_bearish"
    elif btc_bullish and eth_trend in ("neutral", "unknown"):
        market_sentiment = "mild_bullish"
    elif eth_bullish and btc_trend in ("neutral", "unknown"):
        market_sentiment = "mild_bullish"
    elif btc_bearish and eth_trend in ("neutral", "unknown"):
        market_sentiment = "mild_bearish"
    elif eth_bearish and btc_trend in ("neutral", "unknown"):
        market_sentiment = "mild_bearish"
    else:
        # BTC/ETH 方向相反，或都是 neutral/unknown
        market_sentiment = "mixed"
    
    # ── Confidence 判定 ──
    # high: BTC+ETH 同向且都是确认趋势 (up/down)
    # moderate: BTC+ETH 同向但有过渡状态 (recovering/weakening)
    # low: BTC/ETH 分歧、数据不足、或 mixed regime
    btc_confirmed = btc_trend in _CONFIRMED_BULLISH or btc_trend in _CONFIRMED_BEARISH
    eth_confirmed = eth_trend in _CONFIRMED_BULLISH or eth_trend in _CONFIRMED_BEARISH
    btc_has_data = btc_trend != "unknown"
    eth_has_data = eth_trend != "unknown"
    
    if market_sentiment in ("strong_bullish", "strong_bearish"):
        if btc_confirmed and eth_confirmed:
            confidence = "high"
        else:
            confidence = "moderate"
    elif market_sentiment in ("mild_bullish", "mild_bearish"):
        if btc_has_data and eth_has_data:
            confidence = "moderate"
        else:
            confidence = "low"
    else:
        confidence = "low"
    
    # ── 风险偏好 ──
    if btc_bullish and market_sentiment in ("strong_bullish", "mild_bullish"):
        risk_appetite = "risk_on"
    elif btc_bearish and market_sentiment in ("strong_bearish", "mild_bearish"):
        risk_appetite = "risk_off"
    else:
        risk_appetite = "neutral"
    
    # ── 1h 短期信号（用于检测 4h/1h 分歧）──
    # 1h trend 已转换为简化值: up/down/consolidation/unknown
    btc_1h_bullish = btc_trend_1h == "up"
    btc_1h_bearish = btc_trend_1h == "down"
    eth_1h_bullish = eth_trend_1h == "up"
    eth_1h_bearish = eth_trend_1h == "down"
    
    # 检测 4h vs 1h 冲突 (factual state only)
    trend_conflict = False
    conflict_leaders = []
    conflict_type = None
    
    if market_sentiment in ("strong_bearish", "mild_bearish"):
        # 4h bearish 但 BTC/ETH 1h 出现 bullish
        if btc_1h_bullish or eth_1h_bullish:
            trend_conflict = True
            if btc_1h_bullish:
                conflict_leaders.append("BTC")
            if eth_1h_bullish:
                conflict_leaders.append("ETH")
            conflict_type = "4h_bearish_1h_bullish"
    elif market_sentiment in ("strong_bullish", "mild_bullish"):
        # 4h bullish 但 BTC/ETH 1h 出现 bearish
        if btc_1h_bearish or eth_1h_bearish:
            trend_conflict = True
            if btc_1h_bearish:
                conflict_leaders.append("BTC")
            if eth_1h_bearish:
                conflict_leaders.append("ETH")
            conflict_type = "4h_bullish_1h_bearish"
    elif market_sentiment == "mixed":
        # 4h mixed 但 BTC/ETH 1h 有明确方向
        if btc_1h_bullish and eth_1h_bullish:
            trend_conflict = True
            conflict_leaders = ["BTC", "ETH"]
            conflict_type = "4h_mixed_1h_bullish"
        elif btc_1h_bearish and eth_1h_bearish:
            trend_conflict = True
            conflict_leaders = ["BTC", "ETH"]
            conflict_type = "4h_mixed_1h_bearish"

    # ── 构建结果 ──
    result = {
        "btc_trend": btc_trend,
        "eth_trend": eth_trend,
        "market_sentiment": market_sentiment,
        "confidence": confidence,
        "risk_appetite": risk_appetite,
        # BTC 详细数据
        "btc": {
            "trend_4h": btc_trend,
            "trend_4h_strength": btc_trend_strength,
            "trend_4h_confidence": btc_trend_confidence,
            "trend_1h": btc_trend_1h,
            "price_vs_ema": btc_price_vs_ema,
            "change_24h_pct": btc_change_pct,
            "price": btc_price,
            "rsi_4h": btc_rsi_4h,
            "adx_4h": btc_adx_4h,
            "atr_pct_1h": btc_atr_pct_1h,
            "funding_rate": btc_funding_rate,
            "oi_change_24h_pct": btc_oi_change,
        },
        # ETH 详细数据
        "eth": {
            "trend_4h": eth_trend,
            "trend_4h_strength": eth_trend_strength,
            "trend_4h_confidence": eth_trend_confidence,
            "trend_1h": eth_trend_1h,
            "price_vs_ema": eth_price_vs_ema,
            "change_24h_pct": eth_change_pct,
            "price": eth_price,
            "rsi_4h": eth_rsi_4h,
            "adx_4h": eth_adx_4h,
            "atr_pct_1h": eth_atr_pct_1h,
            "funding_rate": eth_funding_rate,
            "oi_change_24h_pct": eth_oi_change,
        },
    }
    
    # 添加冲突信息 (factual only)
    if trend_conflict:
        result["trend_conflict"] = {
            "exists": True,
            "type": conflict_type,
            "leaders": conflict_leaders,
        }
    
    return result




def _build_account_risk(positions, open_orders, balance, total_unrealized=0.0, max_leverage=30.0) -> dict:
    """
    Build account risk status (by notional exposure / equity)
    
    - positions: Use position_value; fallback to abs(size)*mark_price
    - open_orders: Use price*(origQty-executedQty)
    - equity: balance + total_unrealized
    """
    equity = float(balance) + float(total_unrealized or 0)
    if equity <= 0:
        return {"available": False, "reason": "invalid_equity"}

    # 持仓敞口
    position_exposure = 0.0
    long_exposure = 0.0
    short_exposure = 0.0

    for pos in positions or []:
        # 优先用 position_value（你的数据里就有）
        notional = float(pos.get("position_value") or 0)

        if notional == 0:
            qty = abs(float(pos.get("size") or 0))
            mark = float(pos.get("mark_price") or 0)
            notional = qty * mark

        notional = abs(notional)
        position_exposure += notional

        side = float(pos.get("size") or 0)
        if side > 0:
            long_exposure += notional
        elif side < 0:
            short_exposure += notional

    # 挂单敞口（只统计未成交部分）
    # P1 Fix: 将挂单敞口也计入 long/short exposure
    order_exposure = 0.0
    order_long_exposure = 0.0
    order_short_exposure = 0.0
    
    for o in open_orders or []:
        price = float(o.get("price") or 0)
        orig_qty = float(o.get("origQty") or 0)
        exec_qty = float(o.get("executedQty") or 0)
        remaining_qty = max(0.0, orig_qty - exec_qty)
        order_notional = abs(price * remaining_qty)
        order_exposure += order_notional
        
        # 根据 side 和 positionSide 判断方向
        side = (o.get("side") or "").upper()
        position_side = (o.get("positionSide") or "").upper()
        
        # 判断逻辑 (Hedge Mode - 双向持仓模式):
        # - BUY + LONG -> 开多仓，增加多头敞口
        # - SELL + SHORT -> 开空仓，增加空头敞口
        # - BUY + SHORT -> 平空仓，不增加新敞口
        # - SELL + LONG -> 平多仓，不增加新敞口
        # 
        # One-Way Mode (positionSide = BOTH 或空):
        # - BUY + BOTH/"" -> 开多仓，增加多头敞口
        # - SELL + BOTH/"" -> 开空仓，增加空头敞口
        if side == "BUY":
            if position_side in ("LONG", "BOTH", ""):
                order_long_exposure += order_notional
            # BUY + SHORT = 平空仓，不增加敞口
        elif side == "SELL":
            if position_side in ("SHORT", "BOTH", ""):
                # SELL + SHORT = 开空 (Hedge Mode)
                # SELL + BOTH/"" = 开空 (One-Way Mode)
                order_short_exposure += order_notional
            # SELL + LONG = 平多仓，不增加敞口

    total_exposure = position_exposure + order_exposure
    
    # 合并持仓和挂单的方向敞口
    long_exposure += order_long_exposure
    short_exposure += order_short_exposure

    leverage_used = total_exposure / equity  # 你要的"本金放大倍数"
    max_notional = max_leverage * equity
    utilization = total_exposure / max_notional if max_notional > 0 else None
    remaining_capacity = max(0.0, max_notional - total_exposure)

    # 风险状态（围绕 30x 上限）
    if utilization is None:
        risk_status = "unknown"
    elif utilization >= 0.90:
        risk_status = "critical"
    elif utilization >= 0.75:
        risk_status = "high"
    elif utilization >= 0.50:
        risk_status = "moderate"
    else:
        risk_status = "low"

    return {
        "available": True,
        "equity": round(equity, 2),
        "max_leverage": max_leverage,
        "max_notional": round(max_notional, 2),

        "total_exposure": round(total_exposure, 2),
        "leverage_used": round(leverage_used, 2),  # ✅ 关键：放大倍数
        "utilization": round(utilization, 4) if utilization is not None else None,

        "position_exposure": round(position_exposure, 2),
        "order_exposure": round(order_exposure, 2),
        "long_exposure": round(long_exposure, 2),
        "short_exposure": round(short_exposure, 2),

        "remaining_capacity": round(remaining_capacity, 2),
        "risk_status": risk_status,
        "position_count": len(positions or []),
        "order_count": len(open_orders or []),
    }


def _build_position_distribution(positions: list) -> dict:
    """
    构建持仓分布
    兼容两种格式：
    1. 有 side 字段: {"side": "LONG"} 或 {"side": "SHORT"}
    2. 无 side 字段: 通过 size 正负判断 (size > 0 = LONG, size < 0 = SHORT)
    """
    if not positions:
        return {
            "has_positions": False,
            "long_count": 0,
            "short_count": 0,
            "long_positions": [],
            "short_positions": [],
            "net_direction": "balanced",
        }

    long_positions = []
    short_positions = []

    for pos in positions:
        symbol = pos.get("symbol", "")

        # 🔥 兼容两种格式判断方向
        side = pos.get("side", "").upper()
        if not side:
            # 没有 side 字段，通过 size 正负判断
            size_val = float(pos.get("size", 0))
            side = "LONG" if size_val > 0 else "SHORT" if size_val < 0 else ""

        if not side:
            continue

        # 获取持仓价值和未实现盈亏
        position_value = float(pos.get("position_value", 0))
        unrealized_pnl = float(pos.get("pnl", pos.get("unrealizedPnl", 0)))

        info = {
            "symbol": symbol,
            "size": round(position_value, 2),
            "unrealized_pnl": round(unrealized_pnl, 2),
        }

        if side == "LONG":
            long_positions.append(info)
        elif side == "SHORT":
            short_positions.append(info)

    long_count = len(long_positions)
    short_count = len(short_positions)

    if long_count > short_count:
        net_direction = "long"
    elif short_count > long_count:
        net_direction = "short"
    else:
        net_direction = "balanced"

    return {
        "has_positions": long_count > 0 or short_count > 0,
        "long_count": long_count,
        "short_count": short_count,
        "long_positions": long_positions,
        "short_positions": short_positions,
        "net_direction": net_direction,
    }


# ==========================================================
# D1 fix: enrich_symbol_payload() 已删除
# 原因: 该函数引用了已删除的 symbol_context（与 markets.*.ai_enhancement 重复），
# 且全项目无任何调用方（死代码）。所有全局上下文现在通过 context_builder.py 的
# build_layered_feed() 统一构建和传递。
# ==========================================================