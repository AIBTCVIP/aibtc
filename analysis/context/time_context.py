# time_context.py - Time Context Module
"""
Time Context Analysis Module

Provides trading-related time dimension information:
1. Trading sessions (Asia/Europe/America)
2. Week position (Monday open/Friday close etc.)
3. Important events countdown (funding settlement etc.)
4. K-line close countdown

Usage:
    from analysis.context.time_context import build_time_context
    context = build_time_context()
"""

import time
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, Optional

from core.config import TRADING_SESSIONS, FUNDING_SETTLEMENT_HOURS

logger = logging.getLogger(__name__)


def get_current_session(utc_hour: int) -> Dict:
    """
    Get current trading session (facts only, no advisory text)
    
    V5-01 fix: 与 context_builder._SESSIONS 和 decision_feedback._get_trading_session() 对齐
    新定义：asia=0-7, europe=7-13, overlap=13-16, us=16-22, late=22-24
    
    Args:
        utc_hour: UTC hour (0-23)
    
    Returns:
        Current session info (factual data only)
    """
    # V5-01: 使用统一的 session 定义，不再依赖 config 中的 overlap_eu_us 特殊处理
    # 按优先级顺序检查（overlap 在 europe/us 之前）
    asia = TRADING_SESSIONS.get("asia", {})
    europe = TRADING_SESSIONS.get("europe", {})
    overlap = TRADING_SESSIONS.get("overlap", {})
    us = TRADING_SESSIONS.get("us", {})
    late = TRADING_SESSIONS.get("late", {})
    
    # 按时间顺序匹配 session
    sessions = [
        ("asia", asia),
        ("europe", europe),
        ("overlap", overlap),
        ("us", us),
        ("late", late),
    ]
    
    for session_key, session_cfg in sessions:
        start = session_cfg.get("start_hour", 0)
        end = session_cfg.get("end_hour", 24)
        if start <= utc_hour < end:
            return {
                "session": session_key,
                "start_hour": start,
                "end_hour": end,
                "volatility": session_cfg.get("volatility", "moderate"),
                "liquidity": session_cfg.get("liquidity", "moderate"),
            }
    
    # Fallback: should not reach here with complete session definitions
    return {
        "session": "late",
        "start_hour": 22,
        "end_hour": 24,
        "volatility": "low",
        "liquidity": "low",
    }


def get_week_position(weekday: int, utc_hour: int) -> Dict:
    """
    Get week position info (facts only)
    
    Args:
        weekday: Day of week (0=Monday, 6=Sunday)
        utc_hour: UTC hour
    
    Returns:
        Week position info (factual data only)
    """
    # Special time points (boolean flags - facts)
    is_monday_open = weekday == 0 and utc_hour < 8
    is_friday_close = weekday == 4 and utc_hour >= 20
    is_weekend = weekday >= 5
    
    # Week phase
    if weekday <= 1:
        week_phase = "early"
    elif weekday <= 3:
        week_phase = "mid"
    else:
        week_phase = "late"
    
    return {
        "weekday": weekday,
        "weekday_name": ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"][weekday],
        "week_phase": week_phase,
        "is_monday_open": is_monday_open,
        "is_friday_close": is_friday_close,
        "is_weekend": is_weekend,
    }


def get_funding_countdown(utc_hour: int, utc_minute: int) -> Dict:
    """
    Get funding rate settlement countdown (facts only)
    
    Returns:
        Countdown info (factual data only)
    """
    current_minutes = utc_hour * 60 + utc_minute
    
    # Find next settlement time
    next_settlement_hour = None
    minutes_until = 0  # Initialize to prevent unbound error
    
    for settlement_hour in FUNDING_SETTLEMENT_HOURS:
        settlement_minutes = settlement_hour * 60
        if settlement_minutes > current_minutes:
            minutes_until = settlement_minutes - current_minutes
            next_settlement_hour = settlement_hour
            break
    
    if next_settlement_hour is None:
        # Next day 00:00
        minutes_until = (24 * 60 - current_minutes)
        next_settlement_hour = 0
    
    hours_until = minutes_until // 60
    mins_until = minutes_until % 60
    
    return {
        "minutes_until": minutes_until,
        "hours_until": hours_until,
        "next_settlement_utc": f"{next_settlement_hour:02d}:00",
        "is_imminent": minutes_until <= 30,
        "is_approaching": minutes_until <= 60,
    }


def get_kline_countdown(interval: str, current_ts_ms: int) -> Dict:
    """
    Get current K-line close countdown (facts only)
    
    Args:
        interval: Time period (15m, 1h, 4h)
        current_ts_ms: Current timestamp (milliseconds)
    
    Returns:
        K-line countdown info
    """
    # Period milliseconds
    interval_ms_map = {
        "1m": 60 * 1000,
        "5m": 5 * 60 * 1000,
        "15m": 15 * 60 * 1000,
        "30m": 30 * 60 * 1000,
        "1h": 60 * 60 * 1000,
        "4h": 4 * 60 * 60 * 1000,
        "1d": 24 * 60 * 60 * 1000,
    }
    
    interval_ms = interval_ms_map.get(interval, 15 * 60 * 1000)
    
    # Calculate current K-line start time
    current_period_start = (current_ts_ms // interval_ms) * interval_ms
    next_period_start = current_period_start + interval_ms
    
    # Remaining time
    remaining_ms = next_period_start - current_ts_ms
    remaining_seconds = remaining_ms // 1000
    remaining_minutes = remaining_seconds // 60
    
    # Progress percentage
    elapsed_ms = current_ts_ms - current_period_start
    progress_pct = (elapsed_ms / interval_ms) * 100
    
    return {
        "remaining_minutes": remaining_minutes,
        "progress_pct": round(progress_pct, 1),
        "is_near_close": remaining_minutes <= 2,
    }


def build_time_context(current_ts_ms: Optional[int] = None) -> Dict:
    """
    Build complete time context (facts only, no advisory text)
    
    Args:
        current_ts_ms: Current timestamp (milliseconds), defaults to now
    
    Returns:
        Time context dictionary with factual data only
    """
    try:
        if current_ts_ms is None:
            current_ts_ms = int(time.time() * 1000)
        
        # Convert to UTC datetime
        utc_dt = datetime.fromtimestamp(current_ts_ms / 1000, tz=timezone.utc)
        utc_hour = utc_dt.hour
        utc_minute = utc_dt.minute
        weekday = utc_dt.weekday()
        
        # Build context parts
        session_info = get_current_session(utc_hour)
        week_info = get_week_position(weekday, utc_hour)
        funding_info = get_funding_countdown(utc_hour, utc_minute)
        
        # K-line countdowns (multiple timeframes)
        kline_countdowns = {}
        for interval in ["15m", "1h", "4h"]:
            kline_countdowns[interval] = get_kline_countdown(interval, current_ts_ms)
        
        return {
            "timestamp_utc": utc_dt.strftime("%Y-%m-%d %H:%M:%S"),
            "utc_hour": utc_hour,
            "session": session_info,
            "week": week_info,
            "funding": funding_info,
            "kline_countdown": kline_countdowns,
        }
    except Exception as e:
        logger.error(f"Error building time context: {e}")
        # Return minimal fallback context
        return {
            "available": False,
            "error": str(e),
            "timestamp_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        }


# ==========================================================
# Convenience Functions
# ==========================================================
def get_session_summary() -> str:
    """Get a short summary of current session (for logging)"""
    try:
        ctx = build_time_context()
        if not ctx.get("available", True):
            return "Time context unavailable"
        
        session = ctx.get("session", {})
        week = ctx.get("week", {})
        funding = ctx.get("funding", {})
        
        return (
            f"{session.get('session', 'unknown')} | "
            f"{week.get('weekday_name', '?')} | "
            f"Funding: {funding.get('minutes_until', '?')}m"
        )
    except Exception as e:
        logger.error(f"Error getting session summary: {e}")
        return "Time context error"
