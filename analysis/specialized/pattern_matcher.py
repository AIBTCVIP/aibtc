# pattern_matcher.py - Historical Pattern Matching Module
"""
Historical Pattern Matching Module

Analyzes current price patterns and finds similar historical patterns:
1. Price pattern similarity (using DTW-like approach)
2. Structure pattern matching (trend sequences)
3. Volume pattern correlation
4. Historical outcome statistics

Usage:
    from analysis.specialized.pattern_matcher import find_similar_patterns
    matches = find_similar_patterns(current_klines, historical_klines)
"""

import logging
import numpy as np
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from enum import Enum
from analysis.data.kline_utils import get_klines_from_redis

logger = logging.getLogger(__name__)

# ==========================================================
# Configuration
# ==========================================================
PATTERN_WINDOW = 20          # Bars to use for pattern matching
MIN_MATCH_SCORE = 0.7        # Minimum similarity score (0-1)
MAX_MATCHES = 5              # Maximum number of matches to return
OUTCOME_WINDOW = 10          # Bars to measure outcome after pattern
MIN_HISTORICAL_BARS = 100    # Minimum bars needed for analysis

# Pattern types
class PatternType(Enum):
    HIGHER_HIGHS_LOWS = "higher_highs_lows"
    LOWER_HIGHS_LOWS = "lower_highs_lows"
    RANGE_BOUND = "range_bound"
    BREAKOUT_UP = "breakout_up"
    BREAKOUT_DOWN = "breakout_down"
    V_BOTTOM = "v_bottom"
    INVERTED_V = "inverted_v"
    CONSOLIDATION = "consolidation"


# ==========================================================
# Helper Functions
# ==========================================================
def _extract_features(klines: List[Dict]) -> Optional[Dict]:
    """Extract features from klines for pattern matching"""
    if not klines or len(klines) < 10:
        return None
    
    try:
        opens = np.array([float(k.get("o", k.get("Open", 0))) for k in klines], dtype=np.float64)
        highs = np.array([float(k.get("h", k.get("High", 0))) for k in klines], dtype=np.float64)
        lows = np.array([float(k.get("l", k.get("Low", 0))) for k in klines], dtype=np.float64)
        closes = np.array([float(k.get("c", k.get("Close", 0))) for k in klines], dtype=np.float64)
        volumes = np.array([float(k.get("v", k.get("Volume", k.get("Vol", 0)))) for k in klines], dtype=np.float64)
        
        # Normalize price to percentage changes from first bar
        base_price = closes[0]
        if base_price <= 0:
            return None
        
        normalized_closes = (closes - base_price) / base_price * 100
        normalized_highs = (highs - base_price) / base_price * 100
        normalized_lows = (lows - base_price) / base_price * 100
        
        # Normalize volume
        avg_volume = np.mean(volumes)
        normalized_volumes = volumes / avg_volume if avg_volume > 0 else volumes
        
        # Calculate returns
        returns = np.diff(closes) / closes[:-1]
        
        # Calculate volatility
        volatility = np.std(returns) if len(returns) > 1 else 0
        
        # Identify swing highs and lows
        swing_highs = []
        swing_lows = []
        for i in range(2, len(highs) - 2):
            if highs[i] > highs[i-1] and highs[i] > highs[i-2] and highs[i] > highs[i+1] and highs[i] > highs[i+2]:
                swing_highs.append((i, highs[i]))
            if lows[i] < lows[i-1] and lows[i] < lows[i-2] and lows[i] < lows[i+1] and lows[i] < lows[i+2]:
                swing_lows.append((i, lows[i]))
        
        return {
            "closes": closes,
            "highs": highs,
            "lows": lows,
            "volumes": volumes,
            "normalized_closes": normalized_closes,
            "normalized_highs": normalized_highs,
            "normalized_lows": normalized_lows,
            "normalized_volumes": normalized_volumes,
            "returns": returns,
            "volatility": volatility,
            "swing_highs": swing_highs,
            "swing_lows": swing_lows,
        }
    except Exception as e:
        logger.debug(f"Feature extraction failed: {e}")
        return None


def _calculate_pattern_similarity(
    pattern1: np.ndarray,
    pattern2: np.ndarray
) -> float:
    """
    Calculate similarity between two patterns using correlation
    
    Returns score between 0 and 1
    """
    if len(pattern1) != len(pattern2):
        # Resample to same length
        min_len = min(len(pattern1), len(pattern2))
        pattern1 = pattern1[-min_len:]
        pattern2 = pattern2[-min_len:]
    
    if len(pattern1) < 5:
        return 0.0
    
    try:
        # Use Pearson correlation
        correlation = np.corrcoef(pattern1, pattern2)[0, 1]
        
        if not np.isfinite(correlation):
            return 0.0
        
        # Convert correlation (-1 to 1) to similarity (0 to 1)
        # We want patterns that move in the same direction
        similarity = (correlation + 1) / 2
        
        return float(similarity)
    except Exception:
        return 0.0


def _calculate_shape_similarity(features1: Dict, features2: Dict) -> float:
    """
    Calculate shape-based similarity (independent of magnitude)
    """
    try:
        # Compare normalized price shapes
        p1 = features1["normalized_closes"]
        p2 = features2["normalized_closes"]
        
        # Scale both to same range
        p1_scaled = (p1 - np.min(p1)) / (np.max(p1) - np.min(p1) + 1e-8)
        p2_scaled = (p2 - np.min(p2)) / (np.max(p2) - np.min(p2) + 1e-8)
        
        # Calculate MSE-based similarity
        if len(p1_scaled) != len(p2_scaled):
            min_len = min(len(p1_scaled), len(p2_scaled))
            p1_scaled = p1_scaled[-min_len:]
            p2_scaled = p2_scaled[-min_len:]
        
        mse = np.mean((p1_scaled - p2_scaled) ** 2)
        similarity = 1 / (1 + mse * 10)  # Scale MSE to 0-1 range
        
        return float(similarity)
    except Exception:
        return 0.0


def _calculate_volume_similarity(features1: Dict, features2: Dict) -> float:
    """Calculate volume pattern similarity"""
    try:
        v1 = features1["normalized_volumes"]
        v2 = features2["normalized_volumes"]
        
        if len(v1) != len(v2):
            min_len = min(len(v1), len(v2))
            v1 = v1[-min_len:]
            v2 = v2[-min_len:]
        
        correlation = np.corrcoef(v1, v2)[0, 1]
        
        if not np.isfinite(correlation):
            return 0.5  # Neutral if can't calculate
        
        return float((correlation + 1) / 2)
    except Exception:
        return 0.5


def _identify_pattern_type(features: Dict) -> PatternType:
    """Identify the type of price pattern"""
    try:
        closes = features["closes"]
        swing_highs = features["swing_highs"]
        swing_lows = features["swing_lows"]
        
        # Check for higher highs and higher lows
        if len(swing_highs) >= 2 and len(swing_lows) >= 2:
            hh = all(swing_highs[i][1] > swing_highs[i-1][1] for i in range(1, len(swing_highs)))
            hl = all(swing_lows[i][1] > swing_lows[i-1][1] for i in range(1, len(swing_lows)))
            
            if hh and hl:
                return PatternType.HIGHER_HIGHS_LOWS
            
            lh = all(swing_highs[i][1] < swing_highs[i-1][1] for i in range(1, len(swing_highs)))
            ll = all(swing_lows[i][1] < swing_lows[i-1][1] for i in range(1, len(swing_lows)))
            
            if lh and ll:
                return PatternType.LOWER_HIGHS_LOWS
        
        # Check for V-bottom or inverted V
        mid_point = len(closes) // 2
        first_half_trend = closes[mid_point] - closes[0]
        second_half_trend = closes[-1] - closes[mid_point]
        
        if first_half_trend < 0 and second_half_trend > 0:
            return PatternType.V_BOTTOM
        if first_half_trend > 0 and second_half_trend < 0:
            return PatternType.INVERTED_V
        
        # Check for consolidation/range
        price_range = np.max(closes) - np.min(closes)
        avg_price = np.mean(closes)
        range_pct = price_range / avg_price * 100
        
        if range_pct < 3:  # Less than 3% range
            return PatternType.CONSOLIDATION
        
        # Check for breakout
        if closes[-1] > np.max(closes[:-5]):
            return PatternType.BREAKOUT_UP
        if closes[-1] < np.min(closes[:-5]):
            return PatternType.BREAKOUT_DOWN
        
        return PatternType.RANGE_BOUND
        
    except Exception:
        return PatternType.RANGE_BOUND


def _calculate_outcome(
    klines: List[Dict],
    pattern_end_idx: int,
    outcome_window: int = OUTCOME_WINDOW
) -> Optional[Dict]:
    """Calculate the outcome after a pattern"""
    try:
        if pattern_end_idx + outcome_window >= len(klines):
            return None
        
        start_price = float(klines[pattern_end_idx].get("c", klines[pattern_end_idx].get("Close", 0)))
        if start_price <= 0:
            return None
        
        outcome_klines = klines[pattern_end_idx + 1:pattern_end_idx + 1 + outcome_window]
        
        outcome_prices = [float(k.get("c", k.get("Close", 0))) for k in outcome_klines]
        outcome_highs = [float(k.get("h", k.get("High", 0))) for k in outcome_klines]
        outcome_lows = [float(k.get("l", k.get("Low", 0))) for k in outcome_klines]
        
        end_price = outcome_prices[-1]
        max_price = max(outcome_highs)
        min_price = min(outcome_lows)
        
        return_pct = (end_price - start_price) / start_price * 100
        max_gain_pct = (max_price - start_price) / start_price * 100
        max_loss_pct = (min_price - start_price) / start_price * 100
        
        direction = "bullish" if return_pct > 0.5 else "bearish" if return_pct < -0.5 else "neutral"
        
        return {
            "return_pct": round(return_pct, 2),
            "max_gain_pct": round(max_gain_pct, 2),
            "max_loss_pct": round(max_loss_pct, 2),
            "direction": direction,
            "bars": outcome_window,
        }
    except Exception:
        return None


def _find_historical_matches(
    current_features: Dict,
    historical_klines: List[Dict],
    pattern_window: int = PATTERN_WINDOW,
    min_score: float = MIN_MATCH_SCORE,
    max_matches: int = MAX_MATCHES
) -> List[Dict]:
    """Find similar patterns in historical data"""
    matches = []
    
    if len(historical_klines) < pattern_window * 2 + OUTCOME_WINDOW:
        return matches
    
    # Slide through historical data
    for i in range(pattern_window, len(historical_klines) - OUTCOME_WINDOW):
        # Extract historical pattern at this position
        hist_klines = historical_klines[i - pattern_window:i]
        hist_features = _extract_features(hist_klines)
        
        if hist_features is None:
            continue
        
        # Calculate similarities
        price_similarity = _calculate_pattern_similarity(
            current_features["normalized_closes"],
            hist_features["normalized_closes"]
        )
        
        shape_similarity = _calculate_shape_similarity(current_features, hist_features)
        volume_similarity = _calculate_volume_similarity(current_features, hist_features)
        
        # Weighted combined score
        combined_score = (
            price_similarity * 0.5 +
            shape_similarity * 0.35 +
            volume_similarity * 0.15
        )
        
        if combined_score >= min_score:
            # Calculate outcome
            outcome = _calculate_outcome(historical_klines, i, OUTCOME_WINDOW)
            
            if outcome:
                matches.append({
                    "position": i,
                    "score": round(combined_score, 3),
                    "price_similarity": round(price_similarity, 3),
                    "shape_similarity": round(shape_similarity, 3),
                    "volume_similarity": round(volume_similarity, 3),
                    "pattern_type": _identify_pattern_type(hist_features).value,
                    "outcome": outcome,
                })
    
    # Sort by score and return top matches
    matches.sort(key=lambda x: x["score"], reverse=True)
    return matches[:max_matches]


def _aggregate_outcomes(matches: List[Dict]) -> Dict:
    """Aggregate outcomes from all matches"""
    if not matches:
        return {"available": False, "reason": "no_matches"}
    
    outcomes = [m["outcome"] for m in matches if m.get("outcome")]
    
    if not outcomes:
        return {"available": False, "reason": "no_outcomes"}
    
    returns = [o["return_pct"] for o in outcomes]
    directions = [o["direction"] for o in outcomes]
    
    bullish_count = sum(1 for d in directions if d == "bullish")
    bearish_count = sum(1 for d in directions if d == "bearish")
    neutral_count = sum(1 for d in directions if d == "neutral")
    
    avg_return = float(np.mean(returns))
    median_return = float(np.median(returns))
    max_return = float(np.max(returns))
    min_return = float(np.min(returns))
    
    win_rate = bullish_count / len(outcomes) if len(outcomes) > 0 else 0
    
    # Determine probable direction
    if bullish_count > bearish_count * 1.5:
        probable_direction = "bullish"
    elif bearish_count > bullish_count * 1.5:
        probable_direction = "bearish"
    else:
        probable_direction = "mixed"
    
    return {
        "available": True,
        "total_matches": len(outcomes),
        "bullish_outcomes": bullish_count,
        "bearish_outcomes": bearish_count,
        "neutral_outcomes": neutral_count,
        "avg_return_pct": round(avg_return, 2),
        "median_return_pct": round(median_return, 2),
        "max_return_pct": round(max_return, 2),
        "min_return_pct": round(min_return, 2),
        # P11 Fix: 统一 win_rate 为百分比格式 (0-100)
        "win_rate": round(win_rate * 100),
        "probable_direction": probable_direction,
        "sample_reliability": _calculate_sample_reliability(win_rate, len(outcomes), avg_return),
    }


def _calculate_sample_reliability(win_rate: float, sample_size: int, avg_return: float) -> str:
    """Calculate sample reliability level based on statistics"""
    if sample_size < 3:
        return "very_low"
    
    # Consider win rate, sample size, and return magnitude
    if win_rate >= 0.7 and sample_size >= 5 and abs(avg_return) > 1:
        return "high"
    elif win_rate >= 0.6 and sample_size >= 4:
        # A1-1 fix: unified to "medium" — pattern_tracker and context_builder both use "medium"
        return "medium"
    elif win_rate >= 0.5 and sample_size >= 3:
        return "low"
    else:
        return "very_low"


# ==========================================================
# Redis Helper - 使用公共工具函数
# ==========================================================
def _get_historical_klines(symbol: str, tf: str = "15m", limit: int = 300) -> Optional[List[Dict]]:
    """获取历史 K 线数据 - 委托给公共工具函数"""
    klines = get_klines_from_redis(symbol, tf, limit)
    return klines if klines and len(klines) >= MIN_HISTORICAL_BARS else None


# ==========================================================
# Main Function
# ==========================================================
def find_similar_patterns(
    current_klines: Optional[List[Dict]] = None,
    historical_klines: Optional[List[Dict]] = None,
    pattern_window: int = PATTERN_WINDOW,
    symbol: str = None,
    timeframe: str = "15m"
) -> Dict:
    """
    Find similar historical patterns and their outcomes
    
    Args:
        current_klines: Recent klines (optional, fetches from Redis if None)
        historical_klines: Historical klines for comparison (fetches from Redis if None)
        pattern_window: Number of bars to use for pattern
        symbol: Symbol name for context (used to fetch data from Redis)
        timeframe: Timeframe for fetching data
    
    Returns:
        Pattern matching analysis dictionary
    """
    # Fetch historical klines from Redis (also serves as current klines source)
    if historical_klines is None and symbol:
        historical_klines = _get_historical_klines(symbol, timeframe, limit=300)
    
    if not historical_klines or len(historical_klines) < MIN_HISTORICAL_BARS:
        return {"available": False, "reason": f"need_at_least_{MIN_HISTORICAL_BARS}_bars"}
    
    # Use recent bars from historical as current pattern if not provided
    if not current_klines or len(current_klines) < pattern_window:
        current_klines = historical_klines[-pattern_window * 2:]  # Use last 40 bars for current
    
    if len(current_klines) < pattern_window:
        return {"available": False, "reason": "insufficient_current_data"}
    
    try:
        # Extract features from current pattern
        current_pattern_klines = current_klines[-pattern_window:]
        current_features = _extract_features(current_pattern_klines)
        
        if current_features is None:
            return {"available": False, "reason": "feature_extraction_failed"}
        
        # Identify current pattern type
        current_pattern_type = _identify_pattern_type(current_features)
        
        # Find similar patterns in history (excluding recent data)
        # Use older data for comparison to avoid lookahead bias
        historical_for_matching = historical_klines[:-pattern_window]
        
        matches = _find_historical_matches(
            current_features,
            historical_for_matching,
            pattern_window,
            MIN_MATCH_SCORE,
            MAX_MATCHES
        )
        
        # Aggregate outcomes
        aggregated = _aggregate_outcomes(matches)
        
        # Build result
        result = {
            "available": True,
            "symbol": symbol,
            "pattern_window": pattern_window,
            "bars_analyzed": len(historical_klines),
            "current_pattern": {
                "type": current_pattern_type.value,
                "volatility": round(current_features["volatility"] * 100, 3),
                "swing_highs_count": len(current_features["swing_highs"]),
                "swing_lows_count": len(current_features["swing_lows"]),
            },
            "matches_found": len(matches),
            "similar_patterns": matches,
            "historical_outcomes": aggregated,
        }
        
        # Add trading insight
        if aggregated.get("available"):
            result["trading_insight"] = _build_trading_insight(
                current_pattern_type, aggregated
            )
        
        return result
        
    except Exception as e:
        logger.error(f"Pattern matching failed: {e}")
        return {"available": False, "reason": str(e)}


def _build_trading_insight(pattern_type: PatternType, outcomes: Dict) -> Dict:
    """Build trading insight from pattern analysis"""
    
    probable_dir = outcomes.get("probable_direction", "mixed")
    win_rate = outcomes.get("win_rate", 0)
    avg_return = outcomes.get("avg_return_pct", 0)
    confidence = outcomes.get("sample_reliability", "very_low")
    
    # Build insight based on pattern type and historical outcomes
    if pattern_type == PatternType.HIGHER_HIGHS_LOWS:
        base_insight = "Uptrend pattern detected"
    elif pattern_type == PatternType.LOWER_HIGHS_LOWS:
        base_insight = "Downtrend pattern detected"
    elif pattern_type == PatternType.V_BOTTOM:
        base_insight = "V-bottom reversal pattern"
    elif pattern_type == PatternType.INVERTED_V:
        base_insight = "Inverted-V top pattern"
    elif pattern_type == PatternType.BREAKOUT_UP:
        base_insight = "Upside breakout pattern"
    elif pattern_type == PatternType.BREAKOUT_DOWN:
        base_insight = "Downside breakout pattern"
    elif pattern_type == PatternType.CONSOLIDATION:
        base_insight = "Consolidation/squeeze pattern"
    else:
        base_insight = "Range-bound pattern"
    
    # Add historical context
    # P1 Fix: win_rate 已经是百分比格式 (0-100)，所以阈值应该是 50 而不是 0.5
    if confidence in ["high", "moderate"] and win_rate > 50:
        if probable_dir == "bullish":
            action_hint = "Historical patterns suggest bullish continuation"
        elif probable_dir == "bearish":
            action_hint = "Historical patterns suggest bearish continuation"
        else:
            action_hint = "Historical outcomes are mixed"
    else:
        action_hint = "Insufficient historical confidence"
    
    return {
        "pattern_insight": base_insight,
        # P1 Fix: win_rate 已经是百分比格式，不需要再乘以 100
        "historical_edge": f"{int(win_rate)}% of similar patterns were profitable",
        "expected_move": f"Average {avg_return:+.1f}% over {OUTCOME_WINDOW} bars",
        "action_hint": action_hint,
        "sample_reliability": confidence,
    }


# ==========================================================
# Simplified Function
# ==========================================================
def get_pattern_summary(current_klines: List[Dict], symbol: str = None) -> Dict:
    """Get a simplified pattern matching summary"""
    result = find_similar_patterns(current_klines, symbol=symbol)
    
    if not result.get("available"):
        return result
    
    outcomes = result.get("historical_outcomes", {})
    insight = result.get("trading_insight", {})
    
    return {
        "available": True,
        "pattern_type": result.get("current_pattern", {}).get("type"),
        "matches_found": result.get("matches_found", 0),
        "win_rate": outcomes.get("win_rate"),
        "avg_return_pct": outcomes.get("avg_return_pct"),
        "probable_direction": outcomes.get("probable_direction"),
        "sample_reliability": outcomes.get("sample_reliability"),
        "action_hint": insight.get("action_hint"),
    }
