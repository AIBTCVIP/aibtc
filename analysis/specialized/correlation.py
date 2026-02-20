# correlation.py - Multi-Coin Correlation Analysis Module
"""
Multi-Coin Correlation Analysis Module

Provides correlation analysis between coins:
1. BTC correlation - How closely the coin follows BTC
2. ETH correlation - How closely the coin follows ETH
3. Lead/Lag detection - Does this coin lead or lag the market
4. Sector analysis - Relative performance vs sector

Usage:
    from analysis.specialized.correlation import analyze_correlation
    corr = analyze_correlation(symbol, symbol_klines, btc_klines, eth_klines)
"""

import logging
import time
import numpy as np
from typing import Dict, List, Optional, Tuple
from analysis.data.kline_utils import get_klines_from_redis, extract_returns

logger = logging.getLogger(__name__)

# ==========================================================
# Configuration
# ==========================================================
CORRELATION_PERIODS = {
    "short": 24,     # 24 bars (6h for 15m, 24h for 1h)
    "medium": 48,    # 48 bars
    "long": 96,      # 96 bars
}

LEAD_LAG_MAX_SHIFT = 5  # Max bars to check for lead/lag
HIGH_CORRELATION_THRESHOLD = 0.7
LOW_CORRELATION_THRESHOLD = 0.3

# Coin sector classification
COIN_SECTORS = {
    "layer1": ["BTCUSDT", "ETHUSDT", "SOLUSDT", "AVAXUSDT", "NEARUSDT", "APTUSDT", "SUIUSDT"],
    "layer2": ["MATICUSDT", "ARBUSDT", "OPUSDT"],
    "defi": ["UNIUSDT", "AAVEUSDT", "MKRUSDT", "COMPUSDT", "SUSHIUSDT"],
    "meme": ["DOGEUSDT", "SHIBUSDT", "PEPEUSDT", "FLOKIUSDT"],
    "gaming": ["AXSUSDT", "SANDUSDT", "MANAUSDT", "GALAUSDT"],
    "infrastructure": ["LINKUSDT", "DOTUSDT", "ATOMUSDT", "ICPUSDT"],
}


# ==========================================================
# Helper Functions
# ==========================================================
def _get_coin_sector(symbol: str) -> str:
    """Get the sector a coin belongs to"""
    for sector, coins in COIN_SECTORS.items():
        if symbol in coins:
            return sector
    return "altcoin"


def _extract_returns(klines: List[Dict]) -> Optional[np.ndarray]:
    """Extract percentage returns from klines"""
    if not klines or len(klines) < 10:
        return None
    
    try:
        closes = np.array([float(k.get("c", k.get("Close", 0))) for k in klines], dtype=np.float64)
        
        # Calculate log returns for better statistical properties
        # But use simple returns for interpretability
        returns = np.diff(closes) / closes[:-1]
        
        return returns
    except Exception as e:
        logger.debug(f"Failed to extract returns: {e}")
        return None


def _calculate_correlation(returns1: np.ndarray, returns2: np.ndarray) -> Optional[float]:
    """Calculate Pearson correlation between two return series"""
    if len(returns1) != len(returns2) or len(returns1) < 10:
        return None
    
    try:
        # Remove any NaN or infinite values
        valid_mask = np.isfinite(returns1) & np.isfinite(returns2)
        r1 = returns1[valid_mask]
        r2 = returns2[valid_mask]
        
        if len(r1) < 10:
            return None
        
        correlation = float(np.corrcoef(r1, r2)[0, 1])
        return correlation if np.isfinite(correlation) else None
    except Exception as e:
        logger.debug(f"Correlation calculation failed: {e}")
        return None


def _calculate_rolling_correlation(
    returns1: np.ndarray,
    returns2: np.ndarray,
    window: int = 24
) -> Tuple[Optional[float], Optional[float]]:
    """Calculate rolling correlation and its trend"""
    if len(returns1) < window * 2:
        return None, None
    
    try:
        # Calculate correlation for recent window and previous window
        recent_corr = _calculate_correlation(returns1[-window:], returns2[-window:])
        prev_corr = _calculate_correlation(returns1[-window*2:-window], returns2[-window*2:-window])
        
        if recent_corr is None:
            return None, None
        
        corr_change = None
        if prev_corr is not None:
            corr_change = recent_corr - prev_corr
        
        return recent_corr, corr_change
    except Exception:
        return None, None


def _detect_lead_lag(
    returns1: np.ndarray,
    returns2: np.ndarray,
    max_shift: int = LEAD_LAG_MAX_SHIFT
) -> Dict:
    """
    Detect if returns1 leads or lags returns2
    
    Returns:
        lead_lag info with direction and strength
    """
    if len(returns1) < 30 or len(returns2) < 30:
        return {"available": False, "reason": "insufficient_data"}
    
    try:
        min_len = min(len(returns1), len(returns2))
        r1 = returns1[-min_len:]
        r2 = returns2[-min_len:]
        
        best_corr = _calculate_correlation(r1, r2)
        best_shift = 0
        
        # Test different shifts
        for shift in range(1, max_shift + 1):
            # Positive shift: r1 leads r2 (r1 happens first)
            corr_lead = _calculate_correlation(r1[:-shift], r2[shift:])
            if corr_lead is not None and (best_corr is None or abs(corr_lead) > abs(best_corr)):
                best_corr = corr_lead
                best_shift = shift
            
            # Negative shift: r1 lags r2 (r2 happens first)
            corr_lag = _calculate_correlation(r1[shift:], r2[:-shift])
            if corr_lag is not None and (best_corr is None or abs(corr_lag) > abs(best_corr)):
                best_corr = corr_lag
                best_shift = -shift
        
        if best_shift > 0:
            relationship = "leading"
            description = f"This coin leads BTC by ~{best_shift} bars"
        elif best_shift < 0:
            relationship = "lagging"
            description = f"This coin lags BTC by ~{abs(best_shift)} bars"
        else:
            relationship = "synchronized"
            description = "Moves in sync with BTC"
        
        return {
            "available": True,
            "relationship": relationship,
            "shift_bars": best_shift,
            "optimized_correlation": round(best_corr, 4) if best_corr else None,
            "description": description,
        }
    except Exception as e:
        logger.debug(f"Lead/lag detection failed: {e}")
        return {"available": False, "reason": str(e)}


def _calculate_beta(returns_asset: np.ndarray, returns_market: np.ndarray) -> Optional[float]:
    """
    Calculate beta (sensitivity to market movements)
    
    Beta > 1: More volatile than market
    Beta < 1: Less volatile than market
    Beta < 0: Inverse relationship
    """
    if len(returns_asset) != len(returns_market) or len(returns_asset) < 20:
        return None
    
    try:
        valid_mask = np.isfinite(returns_asset) & np.isfinite(returns_market)
        ra = returns_asset[valid_mask]
        rm = returns_market[valid_mask]
        
        if len(ra) < 20:
            return None
        
        # Beta = Cov(asset, market) / Var(market)
        covariance = np.cov(ra, rm)[0, 1]
        market_variance = np.var(rm)
        
        if market_variance == 0:
            return None
        
        beta = float(covariance / market_variance)
        return beta if np.isfinite(beta) else None
    except Exception:
        return None


def _calculate_relative_strength(
    symbol_returns: np.ndarray,
    btc_returns: np.ndarray,
    periods: List[int] = [24, 48, 96]
) -> Dict:
    """
    Calculate relative strength vs BTC
    
    RS > 0: Outperforming BTC
    RS < 0: Underperforming BTC
    """
    result = {}
    
    for period in periods:
        if len(symbol_returns) >= period and len(btc_returns) >= period:
            symbol_perf = float(np.sum(symbol_returns[-period:]) * 100)
            btc_perf = float(np.sum(btc_returns[-period:]) * 100)
            rs = symbol_perf - btc_perf
            
            period_label = f"{period}bars"
            result[period_label] = {
                "symbol_return_pct": round(symbol_perf, 2),
                "btc_return_pct": round(btc_perf, 2),
                "relative_strength": round(rs, 2),
                "status": "outperforming" if rs > 1 else "underperforming" if rs < -1 else "neutral",
            }
    
    return result if result else {"available": False}


# ==========================================================
# Redis Helper - 使用公共工具函数
# ==========================================================
def _get_major_klines(symbol: str, tf: str, limit: int = 100) -> Optional[List[Dict]]:
    """获取主流币 K 线数据 - 委托给公共工具函数"""
    klines = get_klines_from_redis(symbol, tf, limit)
    return klines if klines and len(klines) >= 10 else None


# ==========================================================
# Main Function
# ==========================================================
def analyze_correlation(
    symbol: str,
    symbol_klines: Optional[List[Dict]] = None,
    btc_klines: Optional[List[Dict]] = None,
    eth_klines: Optional[List[Dict]] = None,
    timeframe: str = "15m"
) -> Dict:
    """
    Analyze correlation between symbol and major coins
    
    Args:
        symbol: Symbol being analyzed
        symbol_klines: Klines for the symbol (fetches from Redis if None or insufficient)
        btc_klines: BTC klines (will fetch from Redis if None)
        eth_klines: ETH klines (will fetch from Redis if None)
        timeframe: Timeframe for context
    
    Returns:
        Correlation analysis dictionary
    """
    # Skip for BTC itself
    if symbol == "BTCUSDT":
        return {
            "available": True,
            "symbol": symbol,
            "is_btc": True,
            "note": "BTC is the market benchmark",
        }
    
    # Fetch symbol klines from Redis if not provided or insufficient
    if not symbol_klines or len(symbol_klines) < 30:
        symbol_klines = _get_major_klines(symbol, timeframe)
    
    if not symbol_klines or len(symbol_klines) < 30:
        return {"available": False, "reason": "insufficient_symbol_data"}
    
    # Get BTC klines if not provided
    if btc_klines is None:
        btc_klines = _get_major_klines("BTCUSDT", timeframe)
    
    if not btc_klines or len(btc_klines) < 30:
        return {"available": False, "reason": "insufficient_btc_data"}
    
    # Get ETH klines if not provided
    if eth_klines is None:
        eth_klines = _get_major_klines("ETHUSDT", timeframe)
    
    try:
        # Extract returns
        symbol_returns = _extract_returns(symbol_klines)
        btc_returns = _extract_returns(btc_klines)
        eth_returns = _extract_returns(eth_klines) if eth_klines else None
        
        if symbol_returns is None or btc_returns is None:
            return {"available": False, "reason": "returns_extraction_failed"}
        
        # Align lengths
        min_len = min(len(symbol_returns), len(btc_returns))
        symbol_returns = symbol_returns[-min_len:]
        btc_returns = btc_returns[-min_len:]
        if eth_returns is not None:
            eth_returns = eth_returns[-min_len:]
        
        result = {
            "available": True,
            "symbol": symbol,
            "timeframe": timeframe,
            "bars_analyzed": min_len,
            "sector": _get_coin_sector(symbol),
        }
        
        # BTC correlation analysis
        btc_corr, btc_corr_change = _calculate_rolling_correlation(symbol_returns, btc_returns)
        btc_beta = _calculate_beta(symbol_returns, btc_returns)
        btc_lead_lag = _detect_lead_lag(symbol_returns, btc_returns)
        
        result["btc_correlation"] = {
            "correlation": round(btc_corr, 4) if btc_corr else None,
            "correlation_change": round(btc_corr_change, 4) if btc_corr_change else None,
            "correlation_strength": _interpret_correlation(btc_corr),
            "beta": round(btc_beta, 3) if btc_beta else None,
            "beta_interpretation": _interpret_beta(btc_beta),
            "lead_lag": btc_lead_lag,
        }
        
        # ETH correlation (if available)
        if eth_returns is not None:
            eth_corr, eth_corr_change = _calculate_rolling_correlation(symbol_returns, eth_returns)
            eth_beta = _calculate_beta(symbol_returns, eth_returns)
            eth_lead_lag = _detect_lead_lag(symbol_returns, eth_returns)
            result["eth_correlation"] = {
                "correlation": round(eth_corr, 4) if eth_corr else None,
                "correlation_change": round(eth_corr_change, 4) if eth_corr_change else None,
                "correlation_strength": _interpret_correlation(eth_corr),
                "beta": round(eth_beta, 3) if eth_beta else None,
                "beta_interpretation": _interpret_beta(eth_beta),
                "lead_lag": eth_lead_lag,
            }
        
        # Relative strength analysis
        result["relative_strength"] = _calculate_relative_strength(symbol_returns, btc_returns)
        
        # Overall assessment
        result["assessment"] = _build_correlation_assessment(
            symbol, btc_corr, btc_beta, btc_lead_lag, result.get("relative_strength", {})
        )
        
        return result
        
    except Exception as e:
        logger.error(f"Correlation analysis failed for {symbol}: {e}")
        return {"available": False, "reason": str(e)}


def _interpret_correlation(corr: Optional[float]) -> str:
    """Interpret correlation strength"""
    if corr is None:
        return "unknown"
    
    abs_corr = abs(corr)
    if abs_corr >= 0.8:
        return "very_high"
    elif abs_corr >= 0.6:
        return "high"
    elif abs_corr >= 0.4:
        return "moderate"
    elif abs_corr >= 0.2:
        return "low"
    else:
        return "very_low"


def _interpret_beta(beta: Optional[float]) -> str:
    """Interpret beta value"""
    if beta is None:
        return "unknown"
    
    if beta > 1.5:
        return "high_volatility"
    elif beta > 1.1:
        return "above_market"
    elif beta > 0.9:
        return "market_neutral"
    elif beta > 0.5:
        return "defensive"
    elif beta > 0:
        return "low_volatility"
    else:
        return "inverse"


def _build_correlation_assessment(
    symbol: str,
    btc_corr: Optional[float],
    btc_beta: Optional[float],
    lead_lag: Dict,
    rel_strength: Dict
) -> Dict:
    """Build overall correlation assessment"""
    
    # Determine if independent mover
    is_independent = btc_corr is not None and abs(btc_corr) < LOW_CORRELATION_THRESHOLD
    
    # Determine market sensitivity
    if btc_beta is not None:
        if btc_beta > 1.3:
            sensitivity = "amplified"
            sensitivity_desc = "Amplifies BTC moves - higher risk/reward"
        elif btc_beta > 0.8:
            sensitivity = "normal"
            sensitivity_desc = "Follows BTC with similar magnitude"
        elif btc_beta > 0.3:
            sensitivity = "dampened"
            sensitivity_desc = "Dampens BTC moves - lower volatility"
        else:
            sensitivity = "disconnected"
            sensitivity_desc = "Largely independent of BTC"
    else:
        sensitivity = "unknown"
        sensitivity_desc = "Unable to determine market sensitivity"
    
    # Get relative performance
    recent_rs = rel_strength.get("24bars", {})
    rs_status = recent_rs.get("status", "unknown")
    
    # Build trading implication
    if is_independent:
        implication = "Can trade independently of BTC direction"
    elif lead_lag.get("relationship") == "leading":
        implication = "Watch this coin for early signals of market direction"
    elif btc_beta and btc_beta > 1.3:
        implication = "Higher leverage to BTC - manage position size"
    elif rs_status == "outperforming":
        implication = "Showing relative strength - potential continuation"
    elif rs_status == "underperforming":
        implication = "Showing relative weakness - be cautious on longs"
    else:
        implication = "Follows market - trade with BTC direction"
    
    return {
        "is_independent": is_independent,
        "market_sensitivity": sensitivity,
        "sensitivity_description": sensitivity_desc,
        "relative_performance": rs_status,
        "trade_implication": implication,
    }


# ==========================================================
# Simplified Function
# ==========================================================
def get_correlation_summary(symbol: str, symbol_klines: List[Dict]) -> Dict:
    """Get a simplified correlation summary"""
    result = analyze_correlation(symbol, symbol_klines)
    
    if not result.get("available"):
        return result
    
    btc_data = result.get("btc_correlation", {})
    assessment = result.get("assessment", {})
    
    return {
        "available": True,
        "btc_correlation": btc_data.get("correlation"),
        "btc_beta": btc_data.get("beta"),
        "lead_lag": btc_data.get("lead_lag", {}).get("relationship"),
        "is_independent": assessment.get("is_independent"),
        "relative_performance": assessment.get("relative_performance"),
        "trade_implication": assessment.get("trade_implication"),
    }
