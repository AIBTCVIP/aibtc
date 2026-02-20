# sentiment.py - Market Sentiment Analysis Module
"""
Market Sentiment Analysis Module

Provides multi-dimensional market sentiment indicators:
1. Funding Rate Sentiment
2. Open Interest Changes
3. Long/Short Ratio (requires external API)
4. Fear & Greed Index (requires external API)
5. Liquidation Data (requires external API)

Usage:
    from analysis.specialized.sentiment import build_sentiment_context
    sentiment = build_sentiment_context(symbols_data)
"""

import time
import logging
import threading
import requests
from typing import Dict, List, Optional, Any

from core.config import (
    FUNDING_RATE_THRESHOLDS,
    OI_CHANGE_THRESHOLDS,
    SENTIMENT_API_URLS,
    SENTIMENT_CACHE_TTL,
    SENTIMENT_API_TIMEOUT,
)

logger = logging.getLogger(__name__)

# ==========================================================
# Thread-safe Cache
# ==========================================================
_cache_lock = threading.Lock()
_cache = {
    "fear_greed": {"data": None, "ts": 0},
    "long_short": {"data": {}, "ts": 0},
}


# ==========================================================
# 1. Funding Rate Sentiment Analysis
# ==========================================================
def analyze_funding_sentiment(funding_rate: Optional[float]) -> Dict:
    """
    Analyze single symbol funding rate sentiment
    
    Args:
        funding_rate: Funding rate (decimal, e.g., 0.0001 = 0.01%)
    
    Returns:
        Sentiment analysis result
    """
    if funding_rate is None:
        return {
            "available": False,
            "reason": "no_funding_data",
        }
    
    # Convert to percentage for display
    funding_pct = funding_rate * 100
    
    # Determine sentiment (factual classification only)
    if funding_rate >= FUNDING_RATE_THRESHOLDS["extreme_long"]:
        sentiment = "extreme_long"
    elif funding_rate >= FUNDING_RATE_THRESHOLDS["high_long"]:
        sentiment = "high_long"
    elif funding_rate >= FUNDING_RATE_THRESHOLDS["neutral_high"]:
        sentiment = "mild_long"
    elif funding_rate <= FUNDING_RATE_THRESHOLDS["extreme_short"]:
        sentiment = "extreme_short"
    elif funding_rate <= FUNDING_RATE_THRESHOLDS["high_short"]:
        sentiment = "high_short"
    elif funding_rate <= FUNDING_RATE_THRESHOLDS["neutral_low"]:
        sentiment = "mild_short"
    else:
        sentiment = "neutral"
    
    return {
        "available": True,
        "funding_rate": funding_rate,
        "funding_pct": round(funding_pct, 4),
        "sentiment": sentiment,
        # Annualized cost (to understand funding rate impact)
        "annualized_cost_pct": round(funding_pct * 3 * 365, 2),  # 3 times daily
    }


def analyze_aggregate_funding(symbols_funding: Dict[str, float]) -> Dict:
    """
    Analyze aggregate funding rate sentiment across multiple symbols
    
    Args:
        symbols_funding: {symbol: funding_rate}
    
    Returns:
        Aggregate sentiment analysis
    """
    if not symbols_funding:
        return {"available": False, "reason": "no_data"}
    
    # Count sentiments
    total_funding = 0
    valid_count = 0
    
    extreme_long_count = 0
    high_long_count = 0
    extreme_short_count = 0
    high_short_count = 0
    
    for symbol, fr in symbols_funding.items():
        if fr is None:
            continue
        
        valid_count += 1
        total_funding += fr
        
        if fr >= FUNDING_RATE_THRESHOLDS["extreme_long"]:
            extreme_long_count += 1
        elif fr >= FUNDING_RATE_THRESHOLDS["high_long"]:
            high_long_count += 1
        elif fr <= FUNDING_RATE_THRESHOLDS["extreme_short"]:
            extreme_short_count += 1
        elif fr <= FUNDING_RATE_THRESHOLDS["high_short"]:
            high_short_count += 1
    
    if valid_count == 0:
        return {"available": False, "reason": "no_valid_data"}
    
    avg_funding = total_funding / valid_count
    
    # Market overall sentiment (factual classification only)
    long_ratio = (extreme_long_count + high_long_count) / valid_count
    short_ratio = (extreme_short_count + high_short_count) / valid_count
    
    if long_ratio >= 0.6:
        market_sentiment = "crowded_long"
    elif short_ratio >= 0.6:
        market_sentiment = "crowded_short"
    elif long_ratio >= 0.4:
        market_sentiment = "leaning_long"
    elif short_ratio >= 0.4:
        market_sentiment = "leaning_short"
    else:
        market_sentiment = "balanced"
    
    return {
        "available": True,
        "symbols_analyzed": valid_count,
        "avg_funding_rate": round(avg_funding, 6),
        "avg_funding_pct": round(avg_funding * 100, 4),
        "extreme_long_count": extreme_long_count,
        "high_long_count": high_long_count,
        "extreme_short_count": extreme_short_count,
        "high_short_count": high_short_count,
        "long_ratio": round(long_ratio, 2),
        "short_ratio": round(short_ratio, 2),
        "market_sentiment": market_sentiment,
    }


# ==========================================================
# 2. Open Interest Change Analysis
# ==========================================================
def analyze_oi_change(
    current_oi: Optional[float],
    prev_oi: Optional[float],
    price_change_pct: Optional[float] = None,
) -> Dict:
    """
    Analyze open interest changes and their implications
    
    Args:
        current_oi: Current open interest
        prev_oi: Previous open interest (e.g., 24h ago)
        price_change_pct: Price change percentage
    
    Returns:
        OI analysis result
    """
    if current_oi is None or prev_oi is None or prev_oi == 0:
        return {"available": False, "reason": "insufficient_data"}
    
    oi_change = current_oi - prev_oi
    oi_change_pct = (oi_change / prev_oi) * 100
    
    # OI change classification
    if oi_change_pct >= OI_CHANGE_THRESHOLDS["surge"]:
        oi_trend = "surge"
    elif oi_change_pct >= OI_CHANGE_THRESHOLDS["increase"]:
        oi_trend = "increase"
    elif oi_change_pct <= OI_CHANGE_THRESHOLDS["plunge"]:
        oi_trend = "plunge"
    elif oi_change_pct <= OI_CHANGE_THRESHOLDS["decrease"]:
        oi_trend = "decrease"
    else:
        oi_trend = "stable"
    
    # Combined analysis with price change - factual divergence state
    oi_price_state = None
    if price_change_pct is not None:
        if oi_change_pct > 5 and price_change_pct > 0:
            oi_price_state = "oi_up_price_up"  # New positions entering with trend
        elif oi_change_pct > 5 and price_change_pct < 0:
            oi_price_state = "oi_up_price_down"  # New positions entering against price
        elif oi_change_pct < -5 and price_change_pct > 0:
            oi_price_state = "oi_down_price_up"  # Positions closing, price rising
        elif oi_change_pct < -5 and price_change_pct < 0:
            oi_price_state = "oi_down_price_down"  # Positions closing, price falling
    
    result = {
        "available": True,
        "current_oi": current_oi,
        "prev_oi": prev_oi,
        "oi_change": round(oi_change, 2),
        "oi_change_pct": round(oi_change_pct, 2),
        "oi_trend": oi_trend,
    }
    
    if oi_price_state:
        result["oi_price_state"] = oi_price_state
    
    return result


# ==========================================================
# 3. Fear & Greed Index (External API)
# ==========================================================
def fetch_fear_greed_index() -> Dict:
    """
    Fetch crypto Fear & Greed Index
    
    Data source: Alternative.me
    """
    # Check cache (thread-safe)
    with _cache_lock:
        cache = _cache["fear_greed"]
        if cache["data"] and time.time() - cache["ts"] < SENTIMENT_CACHE_TTL["fear_greed"]:
            return cache["data"]
    
    try:
        resp = requests.get(SENTIMENT_API_URLS["fear_greed"], timeout=SENTIMENT_API_TIMEOUT)
        data = resp.json()
        
        if data.get("data"):
            fng = data["data"][0]
            value = int(fng.get("value", 50))
            classification = fng.get("value_classification", "Neutral")
            
            # P11 Fix: 统一时间戳格式为毫秒级数字
            # API 返回的是秒级字符串，转换为毫秒级整数
            raw_timestamp = fng.get("timestamp")
            timestamp_ms = None
            if raw_timestamp:
                try:
                    timestamp_ms = int(raw_timestamp) * 1000  # 秒 -> 毫秒
                except (ValueError, TypeError):
                    timestamp_ms = None
            
            result = {
                "available": True,
                "value": value,
                "classification": classification,
                "timestamp": timestamp_ms,  # 毫秒级整数，与其他时间戳格式统一
            }
            
            # Update cache (thread-safe)
            with _cache_lock:
                _cache["fear_greed"]["data"] = result
                _cache["fear_greed"]["ts"] = time.time()
            
            return result
    except Exception as e:
        logger.warning(f"Failed to fetch Fear & Greed Index: {e}")
    
    return {"available": False, "reason": "api_error"}


# ==========================================================
# 4. Long/Short Ratio (Binance API)
# ==========================================================
def fetch_long_short_ratio(symbol: str, period: str = "5m") -> Dict:
    """
    Fetch Binance Long/Short Account Ratio
    
    Args:
        symbol: Trading pair (e.g., BTCUSDT)
        period: Period (5m, 15m, 30m, 1h, 2h, 4h, 6h, 12h, 1d)
    """
    cache_key = f"{symbol}:{period}"
    
    # Check cache (thread-safe)
    with _cache_lock:
        cache = _cache["long_short"]
        if cache["data"].get(cache_key) and time.time() - cache["ts"] < SENTIMENT_CACHE_TTL["long_short"]:
            return cache["data"][cache_key]
    
    try:
        url = f"{SENTIMENT_API_URLS['binance_long_short']}?symbol={symbol}&period={period}&limit=1"
        resp = requests.get(url, timeout=SENTIMENT_API_TIMEOUT)
        data = resp.json()
        
        if data and len(data) > 0:
            latest = data[0]
            long_ratio = float(latest.get("longAccount", 0.5))
            short_ratio = float(latest.get("shortAccount", 0.5))
            ls_ratio = float(latest.get("longShortRatio", 1.0))
            
            # Determine sentiment
            if ls_ratio >= 2.0:
                sentiment = "extreme_long"
            elif ls_ratio >= 1.5:
                sentiment = "high_long"
            elif ls_ratio <= 0.5:
                sentiment = "extreme_short"
            elif ls_ratio <= 0.67:
                sentiment = "high_short"
            else:
                sentiment = "balanced"
            
            result = {
                "available": True,
                "symbol": symbol,
                "period": period,
                "long_ratio": round(long_ratio, 4),
                "short_ratio": round(short_ratio, 4),
                "long_short_ratio": round(ls_ratio, 4),
                "sentiment": sentiment,
                "timestamp": latest.get("timestamp"),
            }
            
            # Update cache (thread-safe)
            with _cache_lock:
                _cache["long_short"]["data"][cache_key] = result
                _cache["long_short"]["ts"] = time.time()
            
            return result
    except Exception as e:
        logger.warning(f"Failed to fetch Long/Short Ratio for {symbol}: {e}")
    
    return {"available": False, "reason": "api_error", "symbol": symbol}


# ==========================================================
# 5. Comprehensive Sentiment Analysis
# ==========================================================
def build_sentiment_context(
    symbols_data: Dict[str, Dict],
    include_external: bool = True,
) -> Dict:
    """
    Build comprehensive market sentiment context
    
    Args:
        symbols_data: {symbol: {funding_rate, open_interest, ...}}
        include_external: Whether to include external API data
    
    Returns:
        Comprehensive sentiment context
    """
    # Collect funding rates
    funding_rates = {}
    for symbol, data in symbols_data.items():
        fr = data.get("funding_rate")
        if fr is not None:
            funding_rates[symbol] = fr
    
    # Funding rate sentiment
    funding_sentiment = analyze_aggregate_funding(funding_rates)
    
    # Single coin funding details (major coins)
    major_funding = {}
    for symbol in ["BTCUSDT", "ETHUSDT"]:
        if symbol in funding_rates:
            major_funding[symbol] = analyze_funding_sentiment(funding_rates[symbol])
    
    result = {
        "funding_sentiment": funding_sentiment,
        "major_coins_funding": major_funding,
    }
    
    # External API data
    if include_external:
        # Fear & Greed Index
        result["fear_greed"] = fetch_fear_greed_index()
        
        # BTC Long/Short Ratio
        result["btc_long_short"] = fetch_long_short_ratio("BTCUSDT")
    
    # Composite sentiment score
    result["composite_sentiment"] = _calculate_composite_sentiment(result)
    
    return result


def _calculate_composite_sentiment(sentiment_data: Dict) -> Dict:
    """
    Calculate composite sentiment score
    
    Combines:
    - Funding rate sentiment
    - Fear & Greed Index
    - Long/Short Ratio
    """
    scores = []
    factors = []
    
    # Funding rate (weight 40%)
    fs = sentiment_data.get("funding_sentiment", {})
    if fs.get("available"):
        sentiment = fs.get("market_sentiment")
        if sentiment == "crowded_long":
            scores.append((-0.8, 0.4))  # Bearish signal
            factors.append("funding_crowded_long")
        elif sentiment == "crowded_short":
            scores.append((0.8, 0.4))   # Bullish signal
            factors.append("funding_crowded_short")
        elif sentiment == "leaning_long":
            scores.append((-0.3, 0.4))
            factors.append("funding_leaning_long")
        elif sentiment == "leaning_short":
            scores.append((0.3, 0.4))
            factors.append("funding_leaning_short")
        else:
            scores.append((0, 0.4))
            factors.append("funding_neutral")
    
    # Fear & Greed Index (weight 35%)
    fng = sentiment_data.get("fear_greed", {})
    if fng.get("available"):
        value = fng.get("value", 50)
        # Normalize to -1 to 1 (inverted, as it's a contrarian indicator)
        fng_score = (50 - value) / 50  # Fear = positive (bullish), Greed = negative (bearish)
        scores.append((fng_score, 0.35))
        factors.append(f"fng_{fng.get('classification', 'neutral').lower().replace(' ', '_')}")
    
    # Long/Short Ratio (weight 25%)
    ls = sentiment_data.get("btc_long_short", {})
    if ls.get("available"):
        ratio = ls.get("long_short_ratio", 1.0)
        # High L/S ratio = bearish signal (contrarian)
        if ratio > 1:
            ls_score = -min((ratio - 1) / 2, 1)  # Max -1
        else:
            ls_score = min((1 - ratio) / 0.5, 1)  # Max 1
        scores.append((ls_score, 0.25))
        factors.append(f"ls_ratio_{ls.get('sentiment', 'balanced')}")
    
    if not scores:
        return {
            "available": False,
            "reason": "no_sentiment_data",
        }
    
    # Weighted average
    total_weight = sum(w for _, w in scores)
    weighted_score = sum(s * w for s, w in scores) / total_weight
    
    # Convert to sentiment
    if weighted_score >= 0.5:
        sentiment = "strong_bullish"
    elif weighted_score >= 0.2:
        sentiment = "mild_bullish"
    elif weighted_score <= -0.5:
        sentiment = "strong_bearish"
    elif weighted_score <= -0.2:
        sentiment = "mild_bearish"
    else:
        sentiment = "neutral"
    
    return {
        "available": True,
        "score": round(weighted_score, 2),
        "sentiment": sentiment,
        "factors": factors,
    }


# ==========================================================
# Convenience Functions
# ==========================================================
def get_sentiment_summary(symbols_data: Dict[str, Dict]) -> str:
    """Get a short summary of market sentiment"""
    ctx = build_sentiment_context(symbols_data, include_external=True)
    
    parts = []
    
    # Composite sentiment
    comp = ctx.get("composite_sentiment", {})
    if comp.get("available"):
        parts.append(f"Composite: {comp['sentiment']} ({comp['score']:+.2f})")
    
    # Fear & Greed
    fng = ctx.get("fear_greed", {})
    if fng.get("available"):
        parts.append(f"FnG: {fng['value']} ({fng['classification']})")
    
    # Funding sentiment
    fs = ctx.get("funding_sentiment", {})
    if fs.get("available"):
        parts.append(f"Funding: {fs['market_sentiment']}")
    
    return " | ".join(parts) if parts else "No sentiment data"
