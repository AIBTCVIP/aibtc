# context_builder.py

import time
import logging
import calendar
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


def _convert_kline_fields(kline: Dict) -> Dict:
    """转换K线字段名，支持多种格式:
    - 短格式: o/h/l/c
    - 长格式小写: open/high/low/close
    - 长格式大写: Open/High/Low/Close
    """
    if not kline:
        return {}
    
    # 尝试各种格式
    open_val = kline.get('o') or kline.get('open') or kline.get('Open')
    high_val = kline.get('h') or kline.get('high') or kline.get('High')
    low_val = kline.get('l') or kline.get('low') or kline.get('Low')
    close_val = kline.get('c') or kline.get('close') or kline.get('Close')
    
    return {
        'open': open_val,
        'high': high_val,
        'low': low_val,
        'close': close_val,
    }


OPTIMIZATION_MODULES_AVAILABLE = False
CandleAnalyzer = None
get_candle_analyzer = None
QuickChecker = None
get_quick_checker = None
get_alert_manager = None

try:
    from analysis.specialized.candle_analyzer import CandleAnalyzer, get_candle_analyzer
    from analysis.services.quick_checker import QuickChecker, get_quick_checker, get_alert_manager
    from analysis.services.threshold_manager import get_config_manager, get_threshold_manager
    OPTIMIZATION_MODULES_AVAILABLE = True
except ImportError as e:
    logger.warning(f"Optimization modules not available: {e}")
    OPTIMIZATION_MODULES_AVAILABLE = False

# --------------- Configuration ---------------
FEED_VALIDITY_SECONDS = 900  # 15 minutes

# Default user limits (overridden by UserConfig when available)
DEFAULT_USER_LIMITS = {
    "position_size_pct": 3.0,    # multiplier: max single position = N× balance
    "max_position_multiplier": 3.0,  # alias for position_size_pct
    "max_daily_loss_pct": 30.0,  # % of balance max daily loss
    "max_leverage": 30,          # max leverage
    "max_concurrent_positions": 30,  # max number of concurrent positions
    "min_rr_ratio": 3.0,        # minimum risk-reward ratio to open
    "limit_order_min_distance_pct": 3.0,  # minimum distance for limit orders
}


# ==============================================================
# Single-path access (contract: market_data has top-level ai_enhancement & _quick)
# ==============================================================
def _get_ai_enhancement(market_data: dict) -> dict:
    """Single source for ai_enhancement. Caller (llm_api) must promote to top-level."""
    return market_data.get("ai_enhancement", {}) if market_data else {}


def _extract_quick(market_data: dict) -> dict:
    """Extract _quick dict safely."""
    return market_data.get("_quick", {}) if market_data else {}


# ==============================================================
# Helper: extract bias from market data
# ==============================================================
def _extract_bias(market_data: dict) -> Optional[dict]:
    """Extract bias score from market data (single path: _quick or ai_enhancement)."""
    if not market_data:
        return None

    quick = _extract_quick(market_data)
    if quick.get("bias") is not None:
        return {"bias_score": quick.get("bias_score", 0)}

    ai_enh = _get_ai_enhancement(market_data)
    overall_bias = ai_enh.get("overall_bias", {})
    if overall_bias.get("bias_direction") is not None:
        return {"bias_score": overall_bias.get("bias_score", 0)}

    return None


# _resolve_sl_dist / _enhance_momentum_semantics 已迁至 analysis（position_order_formatter / momentum_semantics）


# ==============================================================
# 1. MARKET LAYER (Flattened structure)
# ==============================================================
def build_market_layer(
    global_context: dict,
    balance_info: dict,
    decision_feedback: Optional[dict] = None,
) -> dict:
    """
    Build market layer - flattened structure for LLM consumption.
    Data only — no strategy recommendations or interpretive text.
    
    Output schema:
    - regime, regime_confidence, risk_appetite
    - fear_greed_index, fear_greed_label
    - aggregate_funding (avg_rate, sentiment, extreme counts)
    - trend_conflict, conflict_type
    - exposure_pct, today_realized_pnl, today_trades
    - btc: {price, change_24h_pct, trend_direction_4h, trend_strength_4h, ...}
    - eth: {price, change_24h_pct, trend_direction_4h, trend_strength_4h, ...}
    """
    # --- Market regime (flattened) ---
    regime = global_context.get("market_regime", {})
    
    result = {
        "regime": regime.get("market_sentiment", "unknown"),
        "regime_confidence": regime.get("confidence", "low"),
        "risk_appetite": regime.get("risk_appetite", "neutral"),
    }
    
    # --- Fear & Greed (flattened) ---
    ms = global_context.get("market_sentiment", {})
    fng = ms.get("fear_greed", {})
    fng_val = fng.get("value") if fng.get("available") else None
    
    if fng_val is not None:
        result["fear_greed_index"] = fng_val
        if fng_val <= 20:
            result["fear_greed_label"] = "extreme_fear"
        elif fng_val <= 40:
            result["fear_greed_label"] = "fear"
        elif fng_val <= 60:
            result["fear_greed_label"] = "neutral"
        elif fng_val <= 80:
            result["fear_greed_label"] = "greed"
        else:
            result["fear_greed_label"] = "extreme_greed"
    
    # --- Aggregate funding (from market_sentiment.funding_sentiment，全量构建) ---
    aggregate_funding = global_context.get("aggregate_funding")
    if not aggregate_funding:
        funding_sentiment = ms.get("funding_sentiment", {})
        if funding_sentiment.get("available"):
            aggregate_funding = funding_sentiment
    if aggregate_funding:
        result["aggregate_funding"] = {
            "avg_funding_rate": aggregate_funding.get("avg_funding_rate"),
            "sentiment": aggregate_funding.get("market_sentiment") or aggregate_funding.get("sentiment", "balanced"),
            "extreme_long_count": aggregate_funding.get("extreme_long_count", 0),
            "extreme_short_count": aggregate_funding.get("extreme_short_count", 0),
            "high_long_count": aggregate_funding.get("high_long_count", 0),
            "high_short_count": aggregate_funding.get("high_short_count", 0),
            "symbols_analyzed": aggregate_funding.get("symbols_analyzed"),
            "long_ratio": aggregate_funding.get("long_ratio"),
            "short_ratio": aggregate_funding.get("short_ratio"),
            "avg_funding_pct": aggregate_funding.get("avg_funding_pct"),
        }
        result["aggregate_funding"] = {k: v for k, v in result["aggregate_funding"].items() if v is not None}
    
    # --- Trend conflict (factual): flatten dict so LLM gets type + leaders ---
    tc = regime.get("trend_conflict")
    if isinstance(tc, dict):
        result["trend_conflict"] = tc.get("exists", False)
        if tc.get("type"):
            result["conflict_type"] = tc["type"]
        if tc.get("leaders"):
            result["conflict_leaders"] = tc["leaders"]
    else:
        result["trend_conflict"] = bool(tc) if tc is not None else False
    if regime.get("conflict_type"):
        result["conflict_note"] = regime.get("conflict_type")
    
    # --- Account exposure (flattened) ---
    account_risk = global_context.get("account_risk", {})
    utilization = account_risk.get("utilization", 0) or 0
    result["exposure_pct"] = round(utilization * 100, 2)
    
    # --- Daily PnL (flattened) ---
    df = decision_feedback or {}
    result["today_realized_pnl"] = df.get("today_realized_pnl", 0)
    result["today_trades"] = df.get("today_trades", 0)
    
    # --- BTC data (expanded) ---
    btc_data = regime.get("btc", {})
    result["btc"] = _build_leader_coin_data(btc_data, "btc", regime)
    
    # --- ETH data (expanded) ---
    eth_data = regime.get("eth", {})
    result["eth"] = _build_leader_coin_data(eth_data, "eth", regime)

    # --- Position distribution (全量构建：market_context 产出) ---
    position_distribution = global_context.get("position_distribution", {})
    if position_distribution:
        result["position_distribution"] = position_distribution

    return result


def _build_leader_coin_data(coin_data: dict, coin: str, regime: dict) -> dict:
    """
    Build expanded BTC/ETH data for market layer.
    
    Output fields:
    - price, change_24h_pct
    - trend_direction_4h, trend_strength_4h, trend_confidence_4h
    - structure_1h, price_vs_ema
    - rsi_4h, adx_4h, atr_pct_1h
    - funding_rate, oi_change_24h_pct
    """
    if coin_data:
        return {
            "price": coin_data.get("price"),
            "change_24h_pct": coin_data.get("change_24h_pct"),
            "trend_direction_4h": coin_data.get("trend_4h") or coin_data.get("trend_direction_4h"),
            "trend_strength_4h": coin_data.get("trend_4h_strength", "unknown"),
            "trend_confidence_4h": coin_data.get("trend_4h_confidence"),
            "structure_1h": coin_data.get("structure_1h") or coin_data.get("trend_1h"),
            "price_vs_ema": coin_data.get("price_vs_ema"),
            "rsi_4h": coin_data.get("rsi_4h"),
            "adx_4h": coin_data.get("adx_4h"),
            "atr_pct_1h": coin_data.get("atr_pct_1h"),
            "funding_rate": coin_data.get("funding_rate"),
            "oi_change_24h_pct": coin_data.get("oi_change_24h_pct"),
        }
    else:
        # Fallback to old format
        prefix = f"{coin}_"
        return {
            "price": regime.get(f"{prefix}price"),
            "change_24h_pct": regime.get(f"{prefix}24h_change_pct"),
            "trend_direction_4h": regime.get(f"{prefix}trend"),
            "trend_strength_4h": "unknown",
            "trend_confidence_4h": None,
            "structure_1h": None,
            "price_vs_ema": regime.get(f"{prefix}price_vs_ema"),
            "rsi_4h": None,
            "adx_4h": None,
            "atr_pct_1h": None,
            "funding_rate": None,
            "oi_change_24h_pct": None,
        }


# ==============================================================
# 1a. CONSTRAINTS LAYER (Separated from market)
# ==============================================================
def build_constraints_layer(
    balance_info: dict,
    user_limits: Optional[dict] = None,
    decision_feedback: Optional[dict] = None,
) -> dict:
    """
    Build constraints layer - risk limits and account balance.
    
    Output schema:
    - max_leverage, min_rr_ratio, min_distance_pct, max_concurrent_positions, max_position_multiplier
    - balance_usdt, available_usdt, daily_loss_remaining_usdt
    """
    limits = user_limits or DEFAULT_USER_LIMITS
    _balance = balance_info.get("balance", 0) or 0
    _available = balance_info.get("available", 0) or 0
    
    # Compute remaining daily loss budget
    max_daily_loss_pct = limits.get("max_daily_loss_pct", DEFAULT_USER_LIMITS["max_daily_loss_pct"])
    max_daily_loss = round(_balance * max_daily_loss_pct / 100, 2)
    
    df = decision_feedback or {}
    today_pnl = df.get("today_realized_pnl", 0) or 0
    today_loss = abs(min(today_pnl, 0))
    daily_loss_remaining = round(max_daily_loss - today_loss, 2)
    
    return {
        "max_leverage": limits.get("max_leverage", DEFAULT_USER_LIMITS["max_leverage"]),
        "min_rr_ratio": limits.get("min_rr_ratio", DEFAULT_USER_LIMITS["min_rr_ratio"]),
        "min_distance_pct": limits.get("limit_order_min_distance_pct", DEFAULT_USER_LIMITS["limit_order_min_distance_pct"]),
        "max_concurrent_positions": limits.get("max_concurrent_positions", 30),
        "max_position_multiplier": limits.get("max_position_multiplier", 3),
        "balance_usdt": round(_balance, 2),
        "available_usdt": round(_available, 2),
        "daily_loss_remaining_usdt": daily_loss_remaining,
    }


# ==============================================================
# 1b. MEMORY LAYER (Trading history and performance)
# ==============================================================

def _parse_time_ago_to_hours(time_ago_str: str) -> Optional[float]:
    """
    Parse time_ago string like '2.5h ago', '30m ago', '1d ago' back to numeric hours.
    Returns None if parsing fails.
    """
    if not time_ago_str or not isinstance(time_ago_str, str):
        return None
    import re
    # Match patterns like "2.5h ago", "30m ago", "1d ago", "just now"
    m = re.match(r'([\d.]+)\s*(h|m|d|min|hr|hour|day)', time_ago_str.lower())
    if m:
        val = float(m.group(1))
        unit = m.group(2)
        if unit in ('h', 'hr', 'hour'):
            return round(val, 1)
        elif unit in ('m', 'min'):
            return round(val / 60, 1)
        elif unit in ('d', 'day'):
            return round(val * 24, 1)
    if 'just now' in time_ago_str.lower() or 'now' in time_ago_str.lower():
        return 0.0
    return None


def build_memory_layer(
    decision_feedback: Optional[dict] = None,
    context_memory: Optional[dict] = None,
    balance: float = 0,
) -> dict:
    """
    Build memory layer - trading history and performance data.
    
    Output schema (matches example):
    - recent_decisions: [{symbol, side, action, time_ago_hours, reason_summary, 
                         reason_tags, status, result, pnl_pct, pnl_usdt, hold_hours, rr_achieved, lesson}]
    - today_stats: {trades, wins, losses, win_rate_pct, total_pnl_usdt, avg_hold_hours, avg_rr_achieved}
    - streak: {type, count, total_pnl_usdt}
    - symbol_performance: {SYMBOL: {trades_7d, win_rate_7d_pct, avg_pnl_pct, last_trade_hours_ago, 
                                    last_trade_result, best_pattern, worst_pattern}}
    - similar_situations: [{date, similarity_pct, context_summary, decision_made, outcome, lesson}]
    - pattern_performance: {pattern_name: {trades, win_rate_pct, avg_pnl_pct}}
    """
    result = {}
    df = decision_feedback or {}
    insights = df.get("insights", {})
    
    # --- Recent decisions (full structure) ---
    # Field mapping: decision_feedback.py outputs:
    #   symbol, side, time_ago (str), action, reason_summary, reason_tags, lesson,
    #   outcome: {status, result, net_pnl, pnl_pct, duration_min, peak_pnl, exit_type, cycle_id}
    decisions_out = []
    recent_decisions = df.get("recent_decisions", [])
    if recent_decisions:
        decisions_out = []
        for d in recent_decisions[:10]:
            # Extract outcome info
            outcome = d.get("outcome", {})
            if isinstance(outcome, dict):
                result_str = outcome.get("result", "unknown")
                pnl_pct = outcome.get("pnl_pct", 0) or 0
                # decision_feedback outputs "net_pnl", not "pnl_usdt"
                pnl_usdt = outcome.get("net_pnl", 0) or outcome.get("pnl_usdt", 0) or 0
                # duration is in minutes (duration_min), not ms
                duration_min = outcome.get("duration_min", 0) or 0
                hold_hours = round(duration_min / 60, 1) if duration_min else None
                status = outcome.get("status", "closed")
            elif isinstance(outcome, str):
                result_str = outcome
                pnl_pct = d.get("pnl_pct", 0) or 0
                pnl_usdt = d.get("pnl_usdt", 0) or d.get("net_pnl", 0) or 0
                hold_hours = None
                status = "closed"
            else:
                result_str = "unknown"
                pnl_pct = 0
                pnl_usdt = 0
                hold_hours = None
                status = "closed"
            
            # time_ago: decision_feedback outputs "time_ago" as string like "2.5h ago"
            # Parse it back to numeric hours, fallback to timestamp_ms if available
            time_ago_hours = None
            time_ago_str = d.get("time_ago", "")
            if time_ago_str:
                time_ago_hours = _parse_time_ago_to_hours(time_ago_str)
            if time_ago_hours is None:
                # Fallback: try timestamp fields
                ts = d.get("timestamp_ms", 0) or d.get("open_time_ms", 0)
                if ts:
                    now_ms = time.time() * 1000
                    time_ago_hours = round((now_ms - ts) / 3600000, 1)
            
            # Extract reason tags from reason text if not provided
            reason_tags = d.get("reason_tags", [])
            
            decisions_out.append({
                "symbol": d.get("symbol", ""),
                "side": d.get("side", ""),
                "action": d.get("action", ""),
                "time_ago_hours": time_ago_hours,
                "reason_summary": d.get("reason_summary") or d.get("reason", ""),
                "reason_tags": reason_tags,
                "status": status,
                "result": result_str,
                "pnl_pct": round(pnl_pct, 2) if pnl_pct else 0,
                "pnl_usdt": round(pnl_usdt, 2) if pnl_usdt else 0,
                "hold_hours": hold_hours,
                "rr_achieved": d.get("rr_achieved"),
                "lesson": d.get("lesson", ""),
            })
        if decisions_out:
            result["recent_decisions"] = decisions_out
    
    # --- Today stats (expanded) ---
    today_trades = df.get("today_trades", 0) or 0
    today_wins = df.get("today_wins", 0) or 0
    today_pnl = df.get("today_realized_pnl", 0) or 0
    
    # Compute avg_hold_hours from recent_decisions (decision_feedback doesn't provide it)
    today_avg_hold = 0.0
    today_avg_rr = 0.0
    consecutive_losses = 0
    if decisions_out:
        hold_values = [d["hold_hours"] for d in decisions_out if d.get("hold_hours")]
        if hold_values:
            today_avg_hold = sum(hold_values) / len(hold_values)
        rr_values = [d["rr_achieved"] for d in decisions_out if d.get("rr_achieved") is not None]
        if rr_values:
            today_avg_rr = sum(rr_values) / len(rr_values)
        # Calculate consecutive losses (from most recent backwards)
        for d in decisions_out:
            if d.get("result") == "loss":
                consecutive_losses += 1
            else:
                break  # Stop at first non-loss
    
    result["today_stats"] = {
        "trades": today_trades,
        "wins": today_wins,
        "losses": today_trades - today_wins,
        "consecutive_losses": consecutive_losses,
        "win_rate_pct": round(today_wins / today_trades * 100, 1) if today_trades > 0 else 0,
        "total_pnl_usdt": round(today_pnl, 2),
        "avg_hold_hours": round(today_avg_hold, 1),
        "avg_rr_achieved": round(today_avg_rr, 2),
    }
    
    # --- Streak info ---
    if insights.get("available"):
        streak = insights.get("streak", {})
        if streak:
            streak_val = streak.get("current_streak", 0)
            if streak_val != 0:
                result["streak"] = {
                    "type": "winning" if streak_val > 0 else "losing",
                    "count": abs(streak_val),
                    "total_pnl_usdt": round(streak.get("streak_pnl", 0) or 0, 2),
                }
    
    # --- Symbol performance (dict keyed by symbol) ---
    symbol_stats = insights.get("symbol_stats", {}) if insights.get("available") else {}
    if symbol_stats:
        symbol_perf = {}
        for sym, stats in symbol_stats.items():
            trades = stats.get("trades", 0) or 0
            if trades >= 2:  # Minimum sample
                wins = stats.get("wins", 0) or 0
                last_trade_ts = stats.get("last_trade_ts", 0)
                last_trade_hours = None
                if last_trade_ts:
                    last_trade_hours = round((time.time() * 1000 - last_trade_ts) / 3600000, 0)
                
                symbol_perf[sym] = {
                    "trades_7d": trades,
                    "win_rate_7d_pct": round(stats.get("win_rate", 0) or 0, 0),
                    "avg_pnl_pct": round(stats.get("avg_pnl_pct", 0) or stats.get("avg_pnl", 0) or 0, 2),
                    "last_trade_hours_ago": last_trade_hours,
                    "last_trade_result": stats.get("last_result", "unknown"),
                    "best_pattern": stats.get("best_pattern"),
                    "worst_pattern": stats.get("worst_pattern"),
                }
        if symbol_perf:
            result["symbol_performance"] = symbol_perf
    
    # --- Similar situations (expanded structure) ---
    if context_memory:
        sits = context_memory.get("similar_situations", [])
        if sits:
            sits_out = []
            for sit in sits[:5]:
                sits_out.append({
                    "date": sit.get("date") or sit.get("timestamp", ""),
                    "similarity_pct": sit.get("similarity_pct") or sit.get("relevance", 0),
                    "context_summary": sit.get("context_summary") or sit.get("description", ""),
                    "decision_made": sit.get("decision_made") or sit.get("action", ""),
                    "outcome": sit.get("outcome", ""),
                    "lesson": sit.get("lesson", ""),
                })
            if sits_out:
                result["similar_situations"] = sits_out
    
    # --- Pattern performance (dict keyed by pattern name) ---
    pattern_stats = insights.get("pattern_stats", {}) if insights.get("available") else {}
    if pattern_stats:
        pattern_perf = {}
        for pat, stats in pattern_stats.items():
            trades = stats.get("trades", 0) or 0
            if trades >= 3:  # Minimum sample
                pattern_perf[pat] = {
                    "trades": trades,
                    "win_rate_pct": round(stats.get("win_rate", 0) or 0, 0),
                    "avg_pnl_pct": round(stats.get("avg_pnl_pct", 0) or stats.get("avg_pnl", 0) or 0, 2),
                }
        if pattern_perf:
            result["pattern_performance"] = pattern_perf

    # 保证 memory 层结构完整，无数据时也输出空键，便于下游契约一致
    result.setdefault("recent_decisions", [])
    result.setdefault("symbol_performance", {})
    result.setdefault("similar_situations", [])
    result.setdefault("pattern_performance", {})

    return result


# ==============================================================
# P1: HISTORY CONTEXT - Recent decisions and trading patterns
# ==============================================================
def _build_history_context(decision_feedback: Optional[dict]) -> Optional[dict]:
    """
    Build history context from decision feedback.
    
    Provides LLM with:
    - Last decision (action, reason, time ago)
    - Recent trade results (win rate, PnL)
    - Streak information (consecutive wins/losses)
    - Pattern warnings (e.g., repeated losses on same symbol)
    """
    if not decision_feedback:
        return None
    
    result = {}
    
    # --- Last decision ---
    recent_decisions = decision_feedback.get("recent_decisions", [])
    if recent_decisions:
        last = recent_decisions[0]  # Most recent
        # decision_feedback.py outputs "time_ago" string (e.g. "30m ago"), not timestamp_ms
        time_ago_hours = None
        time_ago_str = last.get("time_ago", "")
        if time_ago_str:
            time_ago_hours = _parse_time_ago_to_hours(time_ago_str)
        if time_ago_hours is None:
            # Fallback: try timestamp fields
            ts = last.get("timestamp_ms", 0) or last.get("open_time_ms", 0)
            if ts and ts > 0:
                now_ms = time.time() * 1000
                time_ago_hours = round((now_ms - ts) / 3600000, 1)
        
        if time_ago_hours is not None:
            minutes_ago = int(time_ago_hours * 60)
            minutes_ago = max(0, minutes_ago)
        else:
            minutes_ago = None
        
        result["last_decision"] = {
            "action": last.get("action", "unknown"),
            "symbol": last.get("symbol", ""),
            "side": last.get("side", ""),
            "time_ago_minutes": minutes_ago,
        }
        # Add reason if available
        reason = last.get("reason_summary") or last.get("reason") or last.get("reasoning")
        if reason and len(reason) < 100:
            result["last_decision"]["reason"] = reason
    
    # --- Recent trade results ---
    insights = decision_feedback.get("insights", {})
    if insights.get("available"):
        perf = insights.get("performance", {})
        result["recent_trades"] = {
            "count_48h": perf.get("total_trades", 0) or 0,
            "win_rate": perf.get("win_rate_pct", 0) or 0,
            "total_pnl": perf.get("total_pnl", 0) or 0,
        }
        
        # Streak info
        streak = insights.get("streak", {})
        if streak:
            streak_val = streak.get("current_streak", 0)
            if streak_val != 0:
                result["streak"] = {
                    "type": "winning" if streak_val > 0 else "losing",
                    "count": abs(streak_val),
                    "pnl": streak.get("streak_pnl", 0),
                }
    
    # --- Pattern warnings ---
    # Check for repeated losses on same symbol
    warnings = []
    symbol_losses = {}
    for d in recent_decisions[:10]:
        outcome = d.get("outcome", {})
        if outcome.get("result") == "loss":
            sym = d.get("symbol", "")
            if sym:
                symbol_losses[sym] = symbol_losses.get(sym, 0) + 1
    
    for sym, count in symbol_losses.items():
        if count >= 2:
            warnings.append(f"{count}_consecutive_losses_on_{sym}")
    
    if warnings:
        result["pattern_warnings"] = warnings
    
    return result if result else None


# ==============================================================
# 1b. TIME CONTEXT
# ==============================================================

# Binance funding settlement times (UTC hours)
def _build_time_context() -> dict:
    """
    Build time context — flattened structure, data only, no advisory text.
    
    Delegates to analysis.time_context for core logic, then flattens for LLM consumption.

    Output schema:
    - session, session_volatility, session_liquidity, hours_until_session_change
    - weekday, weekday_name, week_phase, is_weekend, is_monday_open, is_friday_close
    - funding_minutes_until, funding_urgency, next_funding_utc
    - candle_15m_close_minutes, candle_1h_close_minutes, candle_4h_close_minutes, candle_4h_progress_pct
    """
    from analysis.context.time_context import build_time_context as build_time_context_core
    
    # Get structured time context from core module
    ctx = build_time_context_core()
    
    # Handle error case
    if not ctx.get("available", True) is False:
        pass  # Normal case, continue
    else:
        # Fallback to minimal context
        now = datetime.now(timezone.utc)
        return {
            "session": "unknown",
            "weekday": now.weekday(),
            "weekday_name": ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"][now.weekday()],
            "error": ctx.get("error", "time_context unavailable"),
        }
    
    # Extract nested structures
    session = ctx.get("session", {})
    week = ctx.get("week", {})
    funding = ctx.get("funding", {})
    kline = ctx.get("kline_countdown", {})
    
    # Calculate hours until session change
    session_end = session.get("end_hour", 24)
    now = datetime.now(timezone.utc)
    minutes_until_session_change = (session_end * 60) - (now.hour * 60 + now.minute)
    if minutes_until_session_change <= 0:
        minutes_until_session_change += 24 * 60
    hours_until_session_change = round(minutes_until_session_change / 60, 1)
    
    # Weekend liquidity downgrade
    weekday = week.get("weekday", now.weekday())
    is_weekend = week.get("is_weekend", False)
    session_liquidity = session.get("liquidity", "moderate")
    if is_weekend:
        session_liquidity = "low"
    
    # Funding urgency classification
    funding_minutes_until = funding.get("minutes_until", 999)
    if funding_minutes_until <= 30:
        funding_urgency = "imminent"
    elif funding_minutes_until <= 60:
        funding_urgency = "approaching"
    else:
        funding_urgency = "normal"
    
    # Extract kline countdowns
    kline_15m = kline.get("15m", {})
    kline_1h = kline.get("1h", {})
    kline_4h = kline.get("4h", {})

    # Session name mapping
    session_names = {
        "asia": "Asian Session",
        "europe": "European Session", 
        "us": "US Session",
        "overlap_asia_europe": "Asia-Europe Overlap",
        "overlap_europe_us": "Europe-US Overlap",
        "weekend": "Weekend Session",
    }
    session_code = session.get("session", "unknown")
    session_name = session_names.get(session_code, session_code.replace("_", " ").title())
    
    return {
        # Session
        "session": session_code,
        "session_name": session_name,
        "session_volatility": session.get("volatility", "moderate"),
        "session_liquidity": session_liquidity,
        "hours_until_session_change": hours_until_session_change,
        # Week
        "weekday": weekday,
        "weekday_name": week.get("weekday_name", "Unknown"),
        "week_phase": week.get("week_phase", "mid"),
        "is_weekend": is_weekend,
        "is_monday_open": week.get("is_monday_open", False),
        "is_friday_close": week.get("is_friday_close", False),
        # Funding
        "funding_minutes_until": funding_minutes_until,
        "funding_urgency": funding_urgency,
        "next_funding_utc": funding.get("next_settlement_utc", "00:00"),
        # Candles (all three timeframes)
        "candle_15m_close_minutes": kline_15m.get("remaining_minutes", 0),
        "candle_1h_close_minutes": kline_1h.get("remaining_minutes", 0),
        "candle_4h_close_minutes": kline_4h.get("remaining_minutes", 0),
        "candle_4h_progress_pct": kline_4h.get("progress_pct", 0),
    }


# ==============================================================
# 2. POSITIONS LAYER（委托 analysis.context.position_order_formatter）
# ==============================================================
def build_positions_layer(
    positions: List[dict],
    markets: Dict[str, dict],
    balance_usdt: float = 0,
) -> list:
    """
    Build positions list — data only. Delegates to analysis.context.position_order_formatter.build_positions_layer.
    """
    from analysis.context.position_order_formatter import build_positions_layer as _build
    return _build(positions, markets, balance_usdt)

# ==============================================================
# 3. DATA QUALITY
# ==============================================================
def build_data_quality(markets: Dict[str, dict]) -> dict:
    """Assess data completeness and quality."""
    total = len(markets)
    if total == 0:
        # R15-4 fix: removed stale_data (never populated in normal path — schema inconsistency)
        return {"completeness": 0, "missing_fields": []}

    missing = []
    complete_count = 0

    for symbol, data in markets.items():
        quick = _extract_quick(data)
        if not quick.get("bias"):
            missing.append(f"{symbol}.bias")
        elif not data.get("price"):
            # R16-7 fix: catch price=0 (invalid for tradeable assets), not just None
            missing.append(f"{symbol}.price")
        else:
            complete_count += 1

    completeness = round(complete_count / total * 100) if total > 0 else 0

    return {
        "completeness": completeness,
        "total_symbols": total,
        "complete_symbols": complete_count,
        "missing_fields": missing[:10],
    }


# ==============================================================
# 4. SYMBOLS + CANDIDATES + PENDING ORDERS
# ==============================================================

def _build_candidate_map(
    markets: Dict[str, dict],
    positions: List[dict],
    pending_orders: List[dict],
) -> Dict[str, dict]:
    """
    Build candidate data keyed by symbol — data only, no advisory.

    ALL symbols (including those with open positions) get candidate data.
    Only provides: score, has_pending_order, pending_order_sides.

    Returns: {symbol: candidate_dict}
    """
    order_symbols = {o.get("symbol") for o in pending_orders}
    # Build order direction map: symbol -> list of sides (BUY/SELL)
    order_side_map: Dict[str, List[str]] = {}
    for o in pending_orders:
        sym = o.get("symbol")
        if sym:
            side = o.get("side", "")
            if sym not in order_side_map:
                order_side_map[sym] = []
            if side and side not in order_side_map[sym]:
                order_side_map[sym].append(side)

    candidate_map = {}
    for symbol, data in markets.items():
        bias = _extract_bias(data)
        if not bias:
            continue

        score = bias.get("bias_score", 0) or 0

        cand = _build_candidate_data(
            symbol=symbol,
            score=score,
            has_pending_order=symbol in order_symbols,
            pending_order_sides=order_side_map.get(symbol, []),
        )
        candidate_map[symbol] = cand

    return candidate_map


def _build_candidate_data(
    symbol: str,
    score: int,
    has_pending_order: bool = False,
    pending_order_sides: Optional[List[str]] = None,
) -> dict:
    """
    Build data-only candidate entry for a single symbol.
    Pure data — only score and pending order facts. No derived levels,
    no advisory text, no pre-judgments. LLM infers everything else.
    """
    result: Dict[str, Any] = {
        "score": score,
    }
    if has_pending_order:
        result["has_pending_order"] = True
        if pending_order_sides:
            result["pending_order_sides"] = pending_order_sides

    return result





# ==============================================================
# Pipeline 重构 Phase 2: 纯映射层（SymbolAnalysis → LLM dict）
# ==============================================================
def _map_symbol_analysis(sa) -> dict:
    """
    单个 SymbolAnalysis（或 to_dict() 的嵌套 dict）→ LLM 投喂 dict，零计算、零 fallback。
    与 Phase 4 投喂契约（SymbolAnalysis 纯映射）兼容。
    """
    from dataclasses import asdict, is_dataclass

    def _get(obj, key, default=None):
        if obj is None:
            return default
        if is_dataclass(type(obj)) and not isinstance(obj, type):
            return getattr(obj, key, default)
        return (obj.get(key, default) if isinstance(obj, dict) else default)

    def _drop_unknown(d: dict) -> dict:
        out = {}
        for k, v in d.items():
            if v is None:
                continue
            if isinstance(v, dict):
                c = _drop_unknown(v)
                if c:
                    out[k] = c
            elif isinstance(v, list):
                if v:
                    out[k] = v
            elif v != "unknown":
                out[k] = v
        return out

    entry = {
        "price": _get(sa, "price"),
        "change_24h_pct": _get(sa, "change_24h_pct"),
        "high_24h": _get(sa, "high_24h"),
        "low_24h": _get(sa, "low_24h"),
        "volume_24h_usdt": _get(sa, "volume_24h_usdt"),
    }
    entry = {k: v for k, v in entry.items() if v is not None}

    t = _get(sa, "trend")
    if t:
        trend = {}
        if _get(t, "direction_4h") != "unknown":
            trend["direction_4h"] = _get(t, "direction_4h")
        if _get(t, "strength_4h") != "unknown":
            trend["strength_4h"] = _get(t, "strength_4h")
        if _get(t, "quality_4h") != "unknown":
            trend["trend_quality_4h"] = _get(t, "quality_4h")
        if _get(t, "continuation_prob_4h") != "unknown":
            trend["continuation_prob_4h"] = _get(t, "continuation_prob_4h")
        if _get(t, "ema_slope_4h") != "flat":
            trend["ema_slope_4h"] = _get(t, "ema_slope_4h")
        if _get(t, "price_vs_ema_4h") != "between":
            trend["price_vs_ema_4h"] = _get(t, "price_vs_ema_4h")
        if _get(t, "structure_1h") != "unknown":
            trend["structure_1h"] = _get(t, "structure_1h")
        if _get(t, "direction_1h") != "unknown":
            trend["direction_1h"] = _get(t, "direction_1h")
        if _get(t, "micro_structure_15m") != "unknown":
            trend["micro_structure_15m"] = _get(t, "micro_structure_15m")
        if _get(t, "direction_15m") != "unknown":
            trend["direction_15m"] = _get(t, "direction_15m")
        if _get(t, "multi_tf_aligned") is not None:
            trend["multi_timeframe_aligned"] = _get(t, "multi_tf_aligned")
        if _get(t, "conflict"):
            trend["trend_conflict_note"] = _get(t, "conflict")
        # 4H 补充
        if _get(t, "exhaustion_signal_4h") not in (None, "none"):
            trend["exhaustion_signal_4h"] = _get(t, "exhaustion_signal_4h")
        if _get(t, "adx_rising_4h") is not None:
            trend["adx_rising_4h"] = _get(t, "adx_rising_4h")
        # 1H 补充
        for k in ("price_location_1h", "trade_space_1h", "volatility_state_1h", "breakout_status_1h"):
            v = _get(t, k)
            if v not in (None, "unknown", "none", "value", "normal"):
                trend[k] = v
        for k in ("space_up_atr", "space_down_atr"):
            v = _get(t, k)
            if v is not None:
                trend[k] = v
        if _get(t, "consolidation_1h") is True:
            trend["consolidation_1h"] = True
        # 15M 补充
        for k in ("volume_confirmation_15m", "obv_direction_15m", "rejection_strength_15m", "rejection_direction_15m"):
            v = _get(t, k)
            if v not in (None, "unknown", "none"):
                trend[k] = v
        kls = _get(t, "key_level_status_15m")
        if kls and kls != "none":
            trend["key_level_status_15m"] = kls
        if trend:
            entry["trend"] = trend

    ind = _get(sa, "indicators")
    if ind:
        indicators = {}
        rsi_1h = _get(ind, "rsi_1h")
        rsi_4h = _get(ind, "rsi_4h")
        rsi_15m = _get(ind, "rsi_15m")
        if rsi_1h is not None:
            indicators["rsi_1h"] = round(rsi_1h, 1)
        if rsi_4h is not None:
            indicators["rsi_4h"] = round(rsi_4h, 1)
        if rsi_15m is not None:
            indicators["rsi_15m"] = round(rsi_15m, 1)
        rsi = rsi_1h or rsi_4h
        if rsi is not None and "rsi_zone" not in indicators:
            indicators["rsi_zone"] = _get(ind, "rsi_zone") or ("overbought" if rsi >= 70 else "oversold" if rsi <= 30 else "neutral")
        if _get(ind, "adx_4h") is not None:
            indicators["adx_4h"] = round(_get(ind, "adx_4h"), 1)
        if _get(ind, "adx_1h") is not None:
            indicators["adx_1h"] = round(_get(ind, "adx_1h"), 1)
        if _get(ind, "di_direction_4h") != "neutral":
            indicators["di_direction"] = _get(ind, "di_direction_4h")
        if _get(ind, "macd_histogram_4h") is not None:
            indicators["macd_histogram_4h"] = round(_get(ind, "macd_histogram_4h"), 4)
        if _get(ind, "macd_histogram_1h") is not None:
            indicators["macd_histogram_1h"] = round(_get(ind, "macd_histogram_1h"), 4)
        if _get(ind, "macd_cross_4h"):
            indicators["macd_cross_4h"] = _get(ind, "macd_cross_4h")
        if _get(ind, "ema_distance_pct_4h") is not None:
            indicators["ema_distance_pct_4h"] = _get(ind, "ema_distance_pct_4h")
        if _get(ind, "ema_distance_pct_1h") is not None:
            indicators["ema_distance_pct_1h"] = _get(ind, "ema_distance_pct_1h")
        if indicators:
            entry["indicators"] = indicators

    s = _get(sa, "structure")
    if s:
        structure = _drop_unknown(s) if isinstance(s, dict) else _drop_unknown(asdict(s))
        if structure:
            entry["structure"] = structure

    lev = _get(sa, "levels")
    if lev:
        levels = _drop_unknown(lev) if isinstance(lev, dict) else _drop_unknown(asdict(lev))
        if levels:
            entry["levels"] = levels

    mom = _get(sa, "momentum")
    if mom:
        momentum = _drop_unknown(mom) if isinstance(mom, dict) else _drop_unknown(asdict(mom))
        if momentum:
            entry["momentum"] = momentum
        # 全量构建：momentum_semantics 语义增强并入 feed
        try:
            from analysis.conclusions.momentum_semantics import enhance_momentum_semantics
            t = _get(sa, "trend")
            mom_raw = _get(mom, "state_4h") or _get(mom, "direction") or "neutral"
            technicals = {
                "momentum": mom_raw if isinstance(mom_raw, str) else "neutral",
                "trend_4h": _get(t, "direction_4h") if t else "unknown",
                "trend_1h": _get(t, "structure_1h") if t else "unknown",
            }
            enhanced = enhance_momentum_semantics(technicals)
            if enhanced and entry.get("momentum") is not None:
                for k in ("direction", "strength", "interpretation"):
                    if enhanced.get(k) not in (None, "unknown", ""):
                        entry["momentum"][k] = enhanced[k]
        except Exception:
            pass

    of = _get(sa, "order_flow")
    if of:
        order_flow = _drop_unknown(of) if isinstance(of, dict) else _drop_unknown(asdict(of))
        if order_flow:
            entry["order_flow"] = order_flow

    vol = _get(sa, "volatility")
    if vol:
        volatility = _drop_unknown(vol) if isinstance(vol, dict) else _drop_unknown(asdict(vol))
        if volatility:
            entry["volatility"] = volatility

    pat = _get(sa, "pattern")
    if pat and (_get(pat, "name") or _get(pat, "confidence") is not None or _get(pat, "win_rate") is not None):
        pattern = {}
        if _get(pat, "name"):
            pattern["pattern_name"] = _get(pat, "name")
        if _get(pat, "pattern_type"):
            pattern["pattern_type"] = _get(pat, "pattern_type")
        if _get(pat, "confidence") is not None:
            pattern["confidence"] = _get(pat, "confidence")
        if _get(pat, "win_rate") is not None:
            pattern["win_rate"] = _get(pat, "win_rate")
        if _get(pat, "sample_size") is not None:
            pattern["sample_size"] = _get(pat, "sample_size")
        if _get(pat, "avg_move_pct") is not None:
            pattern["avg_move_pct"] = _get(pat, "avg_move_pct")
        if _get(pat, "direction"):
            pattern["direction"] = _get(pat, "direction")
        if pattern:
            entry["pattern"] = pattern

    corr = _get(sa, "correlation")
    if corr:
        correlation = _drop_unknown(corr) if isinstance(corr, dict) else _drop_unknown(asdict(corr))
        if correlation:
            entry["correlation"] = correlation

    sent = _get(sa, "sentiment")
    if sent:
        sentiment = _drop_unknown(sent) if isinstance(sent, dict) else _drop_unknown(asdict(sent))
        if sentiment:
            entry["sentiment"] = sentiment

    gui = _get(sa, "guidance")
    if gui:
        guidance = _drop_unknown(gui) if isinstance(gui, dict) else _drop_unknown(asdict(gui))
        if guidance:
            entry["guidance"] = guidance

    bias = _get(sa, "bias")
    if bias:
        bias_out = {}
        if _get(bias, "score") is not None:
            bias_out["score"] = _get(bias, "score")
        if _get(bias, "direction") != "unknown":
            bias_out["direction"] = _get(bias, "direction")
        if _get(bias, "strength") != "unknown":
            bias_out["strength"] = _get(bias, "strength")
        if _get(bias, "factors"):
            bias_out["factors"] = _get(bias, "factors")
        if _get(bias, "reversal_risk") != "unknown":
            bias_out["reversal_risk"] = _get(bias, "reversal_risk")
        if _get(bias, "reversal_score") is not None:
            bias_out["reversal_score"] = _get(bias, "reversal_score")
        if _get(bias, "reversal_factors"):
            bias_out["reversal_factors"] = _get(bias, "reversal_factors")
        if _get(bias, "trend_conflict"):
            bias_out["trend_conflict"] = True
        if _get(bias, "trade_suggestion"):
            bias_out["trade_suggestion"] = _get(bias, "trade_suggestion")
        layer_scores = _get(bias, "layer_scores")
        if layer_scores and isinstance(layer_scores, dict) and layer_scores:
            bias_out["layer_scores"] = layer_scores
        if bias_out:
            entry["bias"] = bias_out

    return entry


def build_symbols_layer_v2(analyses: Dict[str, Any]) -> dict:
    """
    Phase 2: 纯映射层。输入为 symbol -> SymbolAnalysis 或 to_dict() 的嵌套 dict，
    输出 symbols dict，供 build_layered_feed 使用。
    """
    symbols = {}
    for symbol, sa in analyses.items():
        if sa is None:
            continue
        symbols[symbol] = _map_symbol_analysis(sa)
    return symbols


def _build_pending_orders(pending_orders: List[dict]) -> list:
    """Format pending orders for LLM layer. Delegates to analysis.context.position_order_formatter.format_pending_orders."""
    from analysis.context.position_order_formatter import format_pending_orders
    return format_pending_orders(pending_orders)


# ==============================================================
# P2: CROSS-SYMBOL RANKING
# ==============================================================
def _build_symbol_rankings(symbols: dict) -> Optional[dict]:
    """
    Build a compact cross-symbol ranking summary.

    Helps LLM quickly identify:
    - Strongest bullish candidates (highest score)
    - Strongest bearish candidates (lowest score)
    - Most volatile symbols (expansion regime)

    Returns None if insufficient data.
    """
    if not symbols or len(symbols) < 3:
        return None

    scored = []
    for sym, data in symbols.items():
        score = data.get("score")
        if score is not None:
            scored.append((sym, score))

    if not scored:
        return None

    scored.sort(key=lambda x: x[1], reverse=True)

    result = {}

    # Top bullish (score >= 2)
    bullish = [(s, sc) for s, sc in scored if sc >= 2]
    if bullish:
        result["strongest_bullish"] = [s for s, _ in bullish[:3]]

    # Top bearish (score <= -2)
    bearish = [(s, sc) for s, sc in scored if sc <= -2]
    if bearish:
        result["strongest_bearish"] = [s for s, _ in bearish[:3]]

    # High volatility symbols (expansion regime)
    expanding = [
        sym for sym, data in symbols.items()
        if data.get("volatility", {}).get("regime") == "expansion"
    ]
    if expanding:
        result["high_volatility"] = expanding

    return result if result else None


# ==============================================================
# MAIN: build_layered_feed
# ==============================================================
def build_layered_feed(
    global_context: dict,
    positions: List[dict],
    pending_orders: List[dict],
    markets: Dict[str, dict],
    balance_info: dict,
    decision_feedback: Optional[dict] = None,
    context_memory: Optional[dict] = None,
    user_limits: Optional[dict] = None,
    symbol_analyses: Optional[Dict[str, Any]] = None,
) -> dict:
    """
    Build the complete data-only feed structure.
    No advisory content — LLM reasons independently from raw data.

    P2 Restructured output (optimized for LLM cognition):
        1. constraints      -> Trading limits (LLM sees restrictions FIRST)
        2. market           -> Regime, account, sentiment, time (raw data)
        3. positions        -> Current holdings
        4. symbols          -> Per-symbol raw technical data
        5. pending_orders   -> Existing limit orders
        6. _meta            -> Minimal metadata

    Phase 3: 若调用方传入 symbol_analyses（来自 payload.symbol_analysis），
    且覆盖当前 markets 中所有 symbol，则用 build_symbols_layer_v2 构建 symbols 层；
    （Phase 4 后仅用 v2，无 raw 路径。）
    
    Design philosophy: Feed raw/first-order data, let LLM reason.
    No pre-computed conclusions, suggestions, or interpretations.
    """
    try:
        effective_limits = user_limits or DEFAULT_USER_LIMITS
        balance = balance_info.get("balance", 0) or 0
        
        # 1. Constraints (risk limits, balance) - LLM sees restrictions FIRST
        constraints = build_constraints_layer(
            balance_info, effective_limits, decision_feedback,
        )
        
        # 2. Time context (session, funding, candle progress)
        time_ctx = _build_time_context()
        
        # 3. Market context (regime, sentiment, btc/eth, exposure)
        # build_market_layer already returns flattened structure
        market = build_market_layer(
            global_context, balance_info, decision_feedback,
        )
        
        # 4. Memory (trading history, performance, similar situations)
        memory = build_memory_layer(
            decision_feedback, context_memory, balance,
        )

        # 5. Positions (data only)
        positions_out = build_positions_layer(positions, markets, balance_usdt=balance)

        # 6. Pending orders
        pending_orders_out = _build_pending_orders(pending_orders)

        # 7. Symbols layer (Phase 4: 仅 v2 纯映射，不再使用 build_symbols_layer_raw)
        symbol_analyses = symbol_analyses or {}
        symbols = build_symbols_layer_v2(symbol_analyses)
        # 合并 _quick 中的 importance/importance_label（由 llm_api._build_minimal_quick 填充）
        for sym, entry in symbols.items():
            meta = (markets.get(sym) or {}).get("_quick") or {}
            if meta.get("importance") is not None:
                entry["importance"] = meta["importance"]
            if meta.get("importance_label"):
                entry["importance_label"] = meta["importance_label"]

        # 7b. 合并 ai_enhancement / referee_context（Phase 2 修复：之前被 build_symbols_layer_v2 丢弃）
        for sym, entry in symbols.items():
            mkt = markets.get(sym) or {}
            ai_enh = mkt.get("ai_enhancement")
            if ai_enh and isinstance(ai_enh, dict):
                for k in ("market_context", "volatility_regime", "market_structure"):
                    v = ai_enh.get(k)
                    if v and isinstance(v, dict):
                        entry.setdefault(k, v)
            ref_ctx = mkt.get("referee_context")
            if ref_ctx and isinstance(ref_ctx, dict):
                rc = ref_ctx.get("rule_checks")
                if rc and isinstance(rc, dict):
                    entry["rule_checks"] = rc

        # 8. K线智能解读层 (Phase 1)
        if OPTIMIZATION_MODULES_AVAILABLE and get_candle_analyzer:
            try:
                candle_analyzer = get_candle_analyzer()
                for symbol, data in symbols.items():
                    market_data = markets.get(symbol, {})
                    tf_data = market_data.get('timeframes', {})

                    # 15m: 优先 _klines_15m 最后一根，否则用 timeframes.15m.indicators（与 1h/4h 一致）
                    klines_15m = data.get('_klines_15m', [])
                    if klines_15m:
                        candle_15m = _convert_kline_fields(klines_15m[-1] if isinstance(klines_15m[-1], dict) else {})
                    else:
                        ind_15m = tf_data.get('15m', {}).get('indicators', {})
                        candle_15m = {
                            'open': ind_15m.get('open'),
                            'high': ind_15m.get('high'),
                            'low': ind_15m.get('low'),
                            'close': ind_15m.get('close'),
                        } if ind_15m.get('close') is not None else {}
                        candle_15m = _convert_kline_fields(candle_15m)

                    # 1h/4h: 从 timeframes 的 indicators 获取 OHLC
                    ind_1h = tf_data.get('1h', {}).get('indicators', {})
                    ind_4h = tf_data.get('4h', {}).get('indicators', {})
                    candle_1h = {
                        'open': ind_1h.get('open'),
                        'high': ind_1h.get('high'),
                        'low': ind_1h.get('low'),
                        'close': ind_1h.get('close'),
                    } if ind_1h.get('close') is not None else {}
                    candle_4h = {
                        'open': ind_4h.get('open'),
                        'high': ind_4h.get('high'),
                        'low': ind_4h.get('low'),
                        'close': ind_4h.get('close'),
                    } if ind_4h.get('close') is not None else {}

                    # 关键位：LevelsModule 字段为 resistance_price / support_price（无 nearest_*）
                    levels = data.get('levels', {})
                    resistance = levels.get('resistance_price')
                    support = levels.get('support_price')
                    
                    correlation_data = data.get('correlation', {})
                    
                    # klines 用于双根形态检测
                    klines_15m_raw = tf_data.get('15m', {}).get('indicators', {}).get('klines', [])
                    klines_1h_raw = ind_1h.get('klines', [])
                    klines_4h_raw = ind_4h.get('klines', [])
                    
                    analysis = candle_analyzer.analyze(
                        candle_15m, candle_1h, candle_4h,
                        resistance, support, symbol,
                        correlation_data=correlation_data,
                        klines_15m=klines_15m_raw,
                        klines_1h=klines_1h_raw,
                        klines_4h=klines_4h_raw
                    )
                    
                    data['candle_intelligence'] = analysis
                    
                    # 删除临时字段，不输出到最终结构
                    if '_klines_15m' in data:
                        del data['_klines_15m']
            except Exception as e:
                logger.error(f"Error building candle intelligence: {e}")
        
        # 清理未处理的临时字段
        for symbol, data in symbols.items():
            if '_klines_15m' in data:
                del data['_klines_15m']
        
        # 9. 快速异常检测层 (Phase 2)
        quick_checks = {}
        if OPTIMIZATION_MODULES_AVAILABLE and get_quick_checker:
            try:
                quick_checker = get_quick_checker()
                # 传入symbols和market，symbols中已包含candle_intelligence
                quick_checks = quick_checker.check(symbols, market)
                
                # DEBUG
                logger.info(f"Quick checks result: status={quick_checks.get('status')}, total={quick_checks.get('total_count', 0)}")
            except Exception as e:
                logger.error(f"Error building quick checks: {e}", exc_info=True)
        
        # Build final result - 7+ module structure
        result = {
            # 1. User-defined constraints (rules, not computed)
            "constraints": constraints,
            
            # 2. Time context
            "time": time_ctx,
            
            # 3. Market state (raw data)
            "market": market,
            
            # 4. Trading memory/history
            "memory": memory,
            
            # 5. Current positions
            "positions": positions_out,
            
            # 6. Pending orders
            "pending_orders": pending_orders_out,
            
            # 7. Symbol raw data (11 sub-modules each)
            # 注: candle_intelligence 已嵌入每个 symbol 下 (symbols[symbol]['candle_intelligence'])
            "symbols": symbols,
            
            # 8. 快速异常检测 - 全局汇总 (Phase 2)
            "quick_checks": quick_checks,
        }

        return result

    except Exception as e:
        logger.error(f"Error building layered feed: {e}", exc_info=True)
        return {
            "constraints": {},
            "market": {"error": str(e)},
            "positions": [],
            "symbols": {},
            "pending_orders": [],
            "quick_checks": {},
        }


# ==============================================================
# P2: TOP-LEVEL CONSTRAINTS
# ==============================================================
def _build_top_level_constraints(
    user_limits: dict,
    balance: float,
    market: dict,
) -> dict:
    """
    Build top-level constraints that LLM must respect.
    
    Placed at the very top of the feed so LLM sees restrictions
    BEFORE processing any market data.
    """
    daily_pnl = market.get("daily_pnl", {})
    max_leverage = user_limits.get("max_leverage", 30)
    # Support both max_position_multiplier and position_size_pct
    max_pos_mult = user_limits.get("max_position_multiplier") or user_limits.get("position_size_pct", 3.0)
    
    return {
        # Hard limits
        "max_leverage": max_leverage,
        "min_rr_ratio": user_limits.get("min_rr_ratio", 3.0),
        "limit_order_min_distance_pct": user_limits.get("limit_order_min_distance_pct", 3.0),
        
        # Position sizing
        "max_position_multiplier": max_pos_mult,  # 单仓 ≤ N× balance
        "max_total_exposure_multiplier": float(max_leverage),  # 总敞口 ≤ N× balance
        "max_concurrent_positions": user_limits.get("max_concurrent_positions", 30),
        
        # Daily risk budget
        "max_daily_loss_pct": user_limits.get("max_daily_loss_pct", 30.0),
        "daily_loss_remaining_usdt": daily_pnl.get("daily_loss_remaining", balance * 0.3),
        
        # Account context
        "balance_usdt": balance,
        "available_usdt": market.get("account", {}).get("available", balance),
    }


def _build_constraints_raw(
    user_limits: dict,
    balance: float,
    market: dict,
) -> dict:
    """
    Build constraints with user-defined rules only.
    
    No computed blocks or suggestions - just the rules LLM must follow.
    """
    daily_pnl = market.get("daily_pnl", {})
    # Support both max_position_multiplier and position_size_pct
    max_pos_mult = user_limits.get("max_position_multiplier") or user_limits.get("position_size_pct", 3.0)
    
    return {
        # Trading rules (user-defined)
        "max_leverage": user_limits.get("max_leverage", 30),
        "min_rr_ratio": user_limits.get("min_rr_ratio", 3.0),
        "min_distance_pct": user_limits.get("limit_order_min_distance_pct", 3.0),
        "max_concurrent_positions": user_limits.get("max_concurrent_positions", 30),
        "max_position_multiplier": max_pos_mult,  # 单仓 ≤ N× balance
        
        # Account state
        "balance_usdt": balance,
        "available_usdt": market.get("account", {}).get("available", balance),
        "daily_loss_remaining_usdt": daily_pnl.get("daily_loss_remaining", balance * 0.3),
    }


# ==============================================================
# P2: DECISION CONTEXT - Pre-computed guidance
# ==============================================================
def _build_decision_context(
    regime: dict,
    symbols: dict,
    positions: list,
) -> dict:
    """
    Build pre-computed decision context to reduce LLM cognitive load.
    
    Provides:
    - Primary bias (what direction to favor)
    - Blocked actions (what to avoid)
    - Optimal strategy hint
    """
    regime_state = regime.get("state", "unknown")
    
    # Determine primary bias from regime
    if regime_state in ("strong_bearish", "bearish"):
        primary_bias = "short_only"
        avoid_actions = ["long", "counter_trend"]
    elif regime_state in ("strong_bullish", "bullish"):
        primary_bias = "long_only"
        avoid_actions = ["short", "counter_trend"]
    else:
        primary_bias = "neutral"
        avoid_actions = []
    
    # Collect all warnings across symbols
    all_warnings = []
    blocked_symbols = {"long": [], "short": []}
    
    for sym, data in symbols.items():
        guardrails = data.get("guardrails", {})
        
        # Collect blocked actions
        for blocked in guardrails.get("blocked_actions", []):
            action = blocked.get("action")
            if action in blocked_symbols:
                blocked_symbols[action].append(sym)
        
        # Collect warnings
        for warning in guardrails.get("warnings", []):
            w_type = warning.get("type")
            if w_type and w_type not in all_warnings:
                all_warnings.append(w_type)
    
    # Determine optimal strategy based on conditions
    if "squeeze_pending" in all_warnings:
        optimal_strategy = "wait_for_breakout"
    elif regime_state in ("strong_bearish",) and not blocked_symbols["short"]:
        optimal_strategy = "trend_following_short"
    elif regime_state in ("strong_bullish",) and not blocked_symbols["long"]:
        optimal_strategy = "trend_following_long"
    else:
        optimal_strategy = "selective_entries"
    
    result = {
        "primary_bias": primary_bias,
        "optimal_strategy": optimal_strategy,
        "regime_confidence": regime.get("confidence", "low"),
    }
    
    if avoid_actions:
        result["avoid_actions"] = avoid_actions
    
    if blocked_symbols["long"]:
        result["long_blocked_symbols"] = blocked_symbols["long"]
    if blocked_symbols["short"]:
        result["short_blocked_symbols"] = blocked_symbols["short"]
    
    if all_warnings:
        result["active_warnings"] = all_warnings[:5]  # Top 5 warnings
    
    # Position summary
    if positions:
        long_count = sum(1 for p in positions if p.get("side") == "LONG")
        short_count = sum(1 for p in positions if p.get("side") == "SHORT")
        result["position_summary"] = {
            "total": len(positions),
            "long": long_count,
            "short": short_count,
        }
    
    return result


# ==============================================================
# P2: FLATTEN MARKET LAYER
# ==============================================================
def _flatten_market_layer(market: dict) -> dict:
    """
    Flatten market layer to reduce nesting depth.
    
    Before:
        market.regime.state
        market.regime.btc.trend_4h
        market.account.balance
        market.risk_limits.max_leverage
        
    After:
        market.regime_state
        market.btc_trend
        market.balance
        (risk_limits moved to top-level constraints)
    """
    regime = market.get("regime", {})
    account = market.get("account", {})
    sentiment = market.get("sentiment", {})
    time_ctx = market.get("time", {})
    history = market.get("history", {})
    daily_pnl = market.get("daily_pnl", {})
    
    # 提取 BTC/ETH 详细数据
    btc = regime.get("btc", {})
    eth = regime.get("eth", {})
    
    result = {
        # Regime (flattened) - 派生事实，基于 BTC/ETH 计算
        "regime_state": regime.get("state", "unknown"),
        
        # BTC data (完整，保留过渡状态 weakening/recovering)
        "btc_trend": btc.get("trend_4h", "unknown"),
        "btc_trend_strength": btc.get("trend_4h_strength"),
        "btc_trend_1h": btc.get("trend_1h"),
        "btc_price_vs_ema": btc.get("price_vs_ema"),
        "btc_change_24h": btc.get("change_24h_pct"),
        
        # ETH data (完整，保留过渡状态 weakening/recovering)
        "eth_trend": eth.get("trend_4h", "unknown"),
        "eth_trend_strength": eth.get("trend_4h_strength"),
        "eth_trend_1h": eth.get("trend_1h"),
        "eth_price_vs_ema": eth.get("price_vs_ema"),
        "eth_change_24h": eth.get("change_24h_pct"),
        
        # Account (raw)
        "exposure_pct": account.get("exposure_pct", 0),
        "leverage_used": account.get("leverage_used", 0),
        
        # Sentiment (raw number only)
        "fear_greed": sentiment.get("fear_greed"),
        
        # Time context
        "session": time_ctx.get("session", {}).get("current", "unknown"),
        "candle_4h_progress_pct": time_ctx.get("candle_4h", {}).get("progress_pct"),
        "funding_minutes_until": time_ctx.get("funding", {}).get("minutes_until"),
        
        # Daily PnL
        "today_realized_pnl": daily_pnl.get("realized", 0),
        "today_trades": daily_pnl.get("trades", 0),
    }
    
    # Add history if available
    if history:
        result["history"] = history
    
    # Add trend conflict if present
    if regime.get("trend_conflict"):
        result["trend_conflict"] = True
    
    # Add performance if available (don't lose this data)
    performance = market.get("performance", {})
    if performance.get("available"):
        result["performance"] = {
            "win_rate_48h": performance.get("recent_win_rate"),
            "pnl_48h": performance.get("recent_pnl"),
            "trades_48h": performance.get("total_trades"),
        }
    
    # Add context_memory if available
    context_memory = market.get("context_memory")
    if context_memory:
        result["context_memory"] = context_memory
    
    # Remove None values
    result = {k: v for k, v in result.items() if v is not None}
    
    return result
