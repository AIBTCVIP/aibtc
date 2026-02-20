# payload_builder.py
"""
统一 Payload 构建器（AI 自主分析版）

设计原则：只提供事实数据，让 AI 自主判断
- 删除所有 verdict / strategy_type / bias 等结论字段
- 保留所有原始数值和状态数据

保持原有接口不变：
  - save_unified_payload(symbol, ttl_sec=300) -> Optional[dict]
  - build_unified_payload(symbol) -> Optional[dict]
  
异步接口（性能优化）：
  - build_unified_payload_async(symbol) -> Optional[dict]
  - save_unified_payload_async(symbol, ttl_sec=300) -> Optional[dict]
"""

import json
import math
import asyncio
from typing import Optional, Dict, Any, List
from core.database import redis_client, get_async_redis, RedisKeys


def _get_price_precision(price: float) -> int:
    """
    根据价格量级动态计算合适的小数位数
    
    Examples:
        price=50000 -> 1 位小数 (BTC)
        price=3000 -> 2 位小数 (ETH)
        price=1.5 -> 4 位小数 (XRP)
        price=0.12 -> 5 位小数 (DOGE)
        price=0.00001 -> 8 位小数 (SHIB)
    
    规则: 保证至少 4 位有效数字
    """
    if price <= 0:
        return 2
    
    # 计算价格的数量级
    magnitude = math.floor(math.log10(abs(price)))
    
    # 保证至少 4 位有效数字
    # 例如: price=0.12, magnitude=-1, precision = 4 - (-1) = 5
    # 例如: price=50000, magnitude=4, precision = 4 - 4 = 0, 但至少保留 1 位
    precision = max(1, 4 - magnitude)
    
    # 限制最大精度为 8 位（避免极端情况）
    return min(precision, 8)


def _round_price(price: Optional[float], reference_price: float = 0) -> Optional[float]:
    """
    根据参考价格智能四舍五入
    
    Args:
        price: 要四舍五入的价格（可为 None）
        reference_price: 参考价格（用于确定精度），默认使用 price 自身
    
    Returns:
        四舍五入后的价格，如果输入为 None 则返回 None
    """
    if price is None:
        return None
    ref = reference_price if reference_price and reference_price > 0 else price
    precision = _get_price_precision(ref)
    return round(price, precision)


# ==========================================================
# 1. Redis 快照读取
# ==========================================================
def _get_snapshot(symbol: str, tf: str) -> Optional[dict]:
    key = RedisKeys.signal_snapshot(symbol, tf)
    v = redis_client.get(key)
    if not v:
        return None
    if isinstance(v, (bytes, bytearray)):
        v = v.decode("utf-8", errors="ignore")
    return json.loads(v)


async def _get_snapshot_async(symbol: str, tf: str) -> Optional[dict]:
    """异步版本的快照读取"""
    redis = await get_async_redis()
    key = RedisKeys.signal_snapshot(symbol, tf)
    v = await redis.get(key)
    if not v:
        return None
    if isinstance(v, (bytes, bytearray)):
        v = v.decode("utf-8", errors="ignore")
    return json.loads(v)


# ==========================================================
# 2. 裁判上下文（只提供事实，不给 verdict）
# ==========================================================
def build_referee_context(tf4h: dict, tf1h: dict, tf15m: dict) -> dict:
    """
    构建裁判上下文 - 只提供事实数据，让 AI 自主判断

    ❌ 删除: verdict, strategy_type
    ✅ 保留: rule_checks (事实检查), context (原始数据)
    """
    s4 = (tf4h or {}).get("structure", {})
    s1 = (tf1h or {}).get("structure", {})
    s15 = (tf15m or {}).get("structure", {})

    trend4 = s4.get("trend", "range")
    loc4 = s4.get("range_location", (tf4h or {}).get("range_location", "unknown"))
    sig15 = (tf15m or {}).get("signal", "none")

    # 收集规则检查状态（事实，不是结论）
    rule_checks = {
        # 4H 结构有效性
        "4h_structure_valid": bool(tf4h and tf4h.get("structure") and tf4h["structure"].get("valid")),

        # 4H 位置信息
        "4h_is_range": trend4 == "range",
        "4h_location": loc4,
        "4h_in_middle": trend4 == "range" and loc4 in ("middle", "unknown"),

        # 1H 过渡期检查
        "1h_has_choch": False,
        "1h_last_break": "none",

        # 15m 信号状态
        "15m_signal": sig15,
        "15m_has_trigger": sig15 != "none",
    }

    # 1H 检查
    if tf1h and tf1h.get("structure") and tf1h["structure"].get("valid"):
        br1 = tf1h["structure"].get("last_break", "none")
        rule_checks["1h_has_choch"] = br1.startswith("choch_")
        rule_checks["1h_last_break"] = br1

    # 构建上下文（只有事实）
    return {
        "rule_checks": rule_checks,
        "context": {
            "4h_trend": s4.get("trend"),
            "4h_location": loc4,
            "4h_pos": s4.get("range_pos", (tf4h or {}).get("range_pos")),
            "4h_range_high": s4.get("range_high"),
            "4h_range_low": s4.get("range_low"),
            "1h_trend": s1.get("trend"),
            "1h_break": s1.get("last_break"),
            "15m_signal": sig15,
            "15m_trend": s15.get("trend"),
            "15m_last_break": s15.get("last_break"),
        }
    }


# ==========================================================
# 3. AI 增强模块
# ==========================================================
from analysis.conclusions.technical_analyzer import build_technicals_conclusions, calculate_overall_bias
from analysis.specialized.correlation import analyze_correlation
from analysis.specialized.pattern_matcher import find_similar_patterns


def _build_market_structure(tf4h: dict, tf1h: dict, tf15m: dict) -> dict:
    """
    构建 market_structure 数据，供 context_builder 使用
    
    从各周期 structure 提取关键字段:
    - swing_high/swing_low: 最近摆动高低点
    - last_HH/LL/HL/LH: 最近结构点
    - last_break: 最近突破类型
    - range_high/low: 区间边界
    - trend_confidence: 趋势置信度
    """
    s4 = (tf4h or {}).get("structure", {})
    s1 = (tf1h or {}).get("structure", {})
    s15 = (tf15m or {}).get("structure", {})
    
    # 优先使用 4h 数据，fallback 到 1h
    primary = s4 if s4.get("valid", True) else s1
    
    return {
        # 状态
        "state_4h": s4.get("trend") or s4.get("bias"),
        "state_1h": s1.get("trend") or s1.get("bias"),
        "state_15m": s15.get("trend") or s15.get("bias"),
        
        # 摆动点
        "swing_high": primary.get("swing_high") or s1.get("swing_high"),
        "swing_low": primary.get("swing_low") or s1.get("swing_low"),
        
        # 结构点 (大小写兼容)
        "last_hh": primary.get("last_HH") or primary.get("last_hh"),
        "last_ll": primary.get("last_LL") or primary.get("last_ll"),
        "last_hl": primary.get("last_HL") or primary.get("last_hl"),
        "last_lh": primary.get("last_LH") or primary.get("last_lh"),
        
        # 突破信息
        "last_break": primary.get("last_break", ""),
        
        # 区间
        "range_high_4h": s4.get("range_high"),
        "range_low_4h": s4.get("range_low"),
        
        # 置信度
        "trend_confidence": primary.get("trend_confidence"),
        
        # 健康度
        "structure_health": primary.get("structure_health"),
    }


def _build_ai_enhancement(tf4h: dict, tf1h: dict, tf15m: dict) -> dict:
    """
    Build AI enhancement data - 扁平化单层结构，避免重复
    
    返回字段（全部直接在根级别）:
    - technicals: 各周期技术指标结论
    - overall_bias: 综合偏向和反转风险
    - key_levels: 精简版关键价位（只有 nearest_supports/resistances）
    - order_flow: 订单流分析
    - market_context: 市场上下文
    - volatility_regime: 波动率状态
    - correlation: BTC 相关性
    - pattern_analysis: 历史形态匹配
    
    注意：
    - 删除了 action_summary（与其他字段重复）
    - 删除了 detailed_data 嵌套层（直接扁平化）
    - key_levels 不含 swing/range 原始值（已在 indicators.structure_summary）
    - 已移除 signal_analysis（无下游消费者）
    - 已移除 potential_setups（无下游消费者）
    - 已移除 volume_profile（无下游消费者，且浪费 Redis I/O）
    """

    current_price = (tf15m or {}).get("close", 0)
    symbol = (tf15m or {}).get("symbol", "")

    # 先计算所有分析数据
    technicals = build_technicals_conclusions(tf4h, tf1h, tf15m)
    overall_bias = calculate_overall_bias(technicals)
    key_levels = _build_key_levels(tf4h, tf1h, tf15m, current_price)
    order_flow = _build_order_flow(tf15m)
    market_context = _build_market_context(tf4h, tf15m)
    volatility_regime = _build_volatility_regime(tf4h, tf1h, tf15m, current_price)
    
    # 从 Redis 获取的分析模块
    correlation = {}
    pattern_analysis = {}
    
    if symbol:
        correlation = _build_correlation_safe(symbol)
        pattern_analysis = _build_pattern_analysis_safe(symbol)
    
    # ============================================================
    # AI Enhancement - 单层结构，避免重复
    # ============================================================
    
    # 精简 key_levels：只保留计算后的 nearest_supports/resistances
    key_levels_slim = {
        "current_price": key_levels.get("current_price"),
        "nearest_supports": key_levels.get("nearest_supports"),
        "nearest_resistances": key_levels.get("nearest_resistances"),
    }
    
    # 构建 market_structure（从各周期 structure 提取关键字段）
    market_structure = _build_market_structure(tf4h, tf1h, tf15m)
    
    return {
        "technicals": technicals,
        "overall_bias": overall_bias,
        "key_levels": key_levels_slim,
        "order_flow": order_flow,
        "market_context": market_context,
        "volatility_regime": volatility_regime,
        "correlation": correlation,
        "pattern_analysis": pattern_analysis,
        "market_structure": market_structure,
    }


async def _build_ai_enhancement_async(tf4h: dict, tf1h: dict, tf15m: dict) -> dict:
    """
    Build AI enhancement data - ASYNC version with parallel Redis fetches
    
    Performance optimization: Runs correlation and pattern_analysis
    in parallel using asyncio.gather() instead of sequentially.
    
    Returns same structure as _build_ai_enhancement()
    """
    current_price = (tf15m or {}).get("close", 0)
    symbol = (tf15m or {}).get("symbol", "")

    # CPU-bound calculations (run sequentially - they're fast)
    technicals = build_technicals_conclusions(tf4h, tf1h, tf15m)
    overall_bias = calculate_overall_bias(technicals)
    key_levels = _build_key_levels(tf4h, tf1h, tf15m, current_price)
    order_flow = _build_order_flow(tf15m)
    market_context = _build_market_context(tf4h, tf15m)
    volatility_regime = _build_volatility_regime(tf4h, tf1h, tf15m, current_price)
    
    # Redis-fetching operations - run in PARALLEL
    correlation = {}
    pattern_analysis = {}
    
    if symbol:
        # These functions fetch from Redis independently
        # Running them in parallel can save latency
        correlation, pattern_analysis = await asyncio.gather(
            asyncio.to_thread(_build_correlation_safe, symbol),
            asyncio.to_thread(_build_pattern_analysis_safe, symbol),
        )
    
    # Build result (same structure as sync version)
    key_levels_slim = {
        "current_price": key_levels.get("current_price"),
        "nearest_supports": key_levels.get("nearest_supports"),
        "nearest_resistances": key_levels.get("nearest_resistances"),
    }
    
    # 构建 market_structure
    market_structure = _build_market_structure(tf4h, tf1h, tf15m)
    
    return {
        "technicals": technicals,
        "overall_bias": overall_bias,
        "key_levels": key_levels_slim,
        "order_flow": order_flow,
        "market_context": market_context,
        "volatility_regime": volatility_regime,
        "correlation": correlation,
        "pattern_analysis": pattern_analysis,
        "market_structure": market_structure,
    }


def _build_correlation_safe(symbol: str) -> dict:
    """Safely build correlation analysis (fetches all klines from Redis)"""
    try:
        # 不传 klines，让模块自己从 Redis 获取当前币和 BTC/ETH 的 K 线
        corr = analyze_correlation(symbol, symbol_klines=None, timeframe="15m")
        if corr.get("available"):
            # P1 Fix: BTC 自身无法与自己计算相关性，返回特殊标记
            if corr.get("is_btc"):
                return {
                    "available": False,
                    "reason": "self_reference",
                    "note": "BTC is the market benchmark - correlation is always 1.0 with itself",
                }
            
            btc_data = corr.get("btc_correlation", {})
            eth_data = corr.get("eth_correlation", {})
            assessment = corr.get("assessment", {})
            rel_strength = corr.get("relative_strength", {})
            
            # Get recent relative strength
            recent_rs = rel_strength.get("24bars", {}) if isinstance(rel_strength, dict) else {}
            
            result = {
                "available": True,
                "sector": corr.get("sector"),
                "btc_correlation": btc_data.get("correlation"),
                "btc_beta": btc_data.get("beta"),
                "correlation_strength": btc_data.get("correlation_strength"),
                "beta_interpretation": btc_data.get("beta_interpretation"),
                "lead_lag": btc_data.get("lead_lag", {}).get("relationship"),
                "lead_lag_bars": btc_data.get("lead_lag", {}).get("shift_bars"),
                "is_independent": assessment.get("is_independent"),
                "relative_strength_24bars": recent_rs.get("relative_strength"),
                "relative_performance": recent_rs.get("status"),
                "trade_implication": assessment.get("trade_implication"),
            }
            
            # Add ETH correlation if available
            if eth_data.get("correlation") is not None:
                result["eth_correlation"] = eth_data.get("correlation")
                result["eth_correlation_strength"] = eth_data.get("correlation_strength")
                result["eth_beta"] = eth_data.get("beta")
                eth_lead_lag = eth_data.get("lead_lag", {})
                if eth_lead_lag:
                    result["eth_lead_lag"] = eth_lead_lag.get("relationship")
                    result["eth_lead_lag_bars"] = eth_lead_lag.get("shift_bars")
            
            return result
        return {"available": False, "reason": corr.get("reason", "unknown")}
    except Exception as e:
        return {"available": False, "reason": str(e)}


def _build_pattern_analysis_safe(symbol: str) -> dict:
    """Safely build pattern analysis (fetches all klines from Redis)"""
    try:
        # 不传 klines，让模块自己从 Redis 获取当前和历史数据
        patterns = find_similar_patterns(
            current_klines=None,     # 让模块从 Redis 获取
            historical_klines=None,  # 让模块从 Redis 获取
            symbol=symbol,
            timeframe="15m"
        )
        if patterns.get("available"):
            current_pattern = patterns.get("current_pattern", {})
            outcomes = patterns.get("historical_outcomes", {})
            insight = patterns.get("trading_insight", {})
            matches_found = patterns.get("matches_found", 0)
            
            # P4 Fix: 判断是否有足够数据来计算 win_rate
            # 如果 win_rate=0/None 且 avg_return=0/None 且匹配数少，标记为数据不足
            win_rate = outcomes.get("win_rate")
            avg_return = outcomes.get("avg_return_pct")
            confidence = outcomes.get("sample_reliability")
            
            # P13 Fix: 修复 None 值判断，当没有匹配时 win_rate 和 avg_return 都是 None
            # 判断条件：win_rate=0或None, avg_return=0或None, 且匹配数 <= 2（或无匹配）
            insufficient_data = (
                (win_rate == 0 or win_rate is None) and 
                (avg_return == 0 or avg_return is None) and 
                matches_found <= 2
            )
            
            result = {
                "available": True,
                "current_pattern_type": current_pattern.get("type"),
                "matches_found": matches_found,
                "avg_match_score": _calc_avg_score(patterns.get("similar_patterns", [])),
                "historical_win_rate": win_rate,
                "historical_avg_return": avg_return,
                # P1 Fix: 数据不足时不给方向预测，避免误导 AI
                "probable_direction": None if insufficient_data else outcomes.get("probable_direction"),
                "sample_reliability": confidence,
                "pattern_insight": insight.get("pattern_insight"),
                "action_hint": insight.get("action_hint"),
            }
            
            # P4 Fix: 添加数据可靠性标记
            if insufficient_data:
                result["data_reliability"] = "insufficient"
                result["data_note"] = "Too few pattern matches - direction prediction disabled"
            elif matches_found <= 5:
                result["data_reliability"] = "low"
                # P1 Fix: low 可靠性时也清空方向预测
                result["probable_direction"] = None
                result["data_note"] = "Low confidence - direction prediction disabled"
            elif matches_found <= 15:
                result["data_reliability"] = "moderate"
            else:
                result["data_reliability"] = "good"
            
            return result
        return {"available": False, "reason": patterns.get("reason", "unknown")}
    except Exception as e:
        return {"available": False, "reason": str(e)}


def _calc_avg_score(matches: list) -> float:
    """Calculate average match score"""
    if not matches:
        return 0.0
    scores = [m.get("score", 0) for m in matches]
    return round(sum(scores) / len(scores), 3) if scores else 0.0


def _build_volatility_regime(tf4h: dict, tf1h: dict, tf15m: dict, current_price: float) -> dict:
    """
    Build volatility regime analysis using existing indicator data
    
    Uses:
    - 1H: bbw, bbw_median, atr_sma50
    - All TFs: atr, atr_ratio, volatility state from structure
    """
    if not tf1h and not tf15m:
        return {"available": False, "reason": "no_data"}
    
    # Extract existing volatility data
    bbw = (tf1h or {}).get("bbw")
    bbw_median = (tf1h or {}).get("bbw_median")
    atr_4h = (tf4h or {}).get("atr")
    atr_1h = (tf1h or {}).get("atr")
    atr_sma50 = (tf1h or {}).get("atr_sma50")
    atr_15m = (tf15m or {}).get("atr")
    atr_ratio_15m = (tf15m or {}).get("atr_ratio")
    
    # Get volatility state from structure (if available)
    vol_state_4h = (tf4h or {}).get("volatility", {})
    vol_state_1h = (tf1h or {}).get("volatility", {})
    vol_state_15m = (tf15m or {}).get("volatility", {})
    
    result = {"available": True}
    
    # 1. Bollinger squeeze detection (from 1H data)
    squeeze_info = {"available": False}
    bbw_percentile = (tf1h or {}).get("bbw_percentile")
    is_squeeze_1h = (tf1h or {}).get("is_squeeze", False)
    squeeze_duration = (tf1h or {}).get("squeeze_duration", 0)
    
    if bbw is not None and bbw_median is not None and bbw_median > 0:
        bbw_ratio = bbw / bbw_median
        is_expansion = bbw_ratio > 1.3  # Current BBW > 130% of median = expansion
        
        # P3: 使用阶梯式查表计算 breakout_probability，与 volatility_regime.py 保持一致
        def _get_breakout_probability(duration: int, is_squeeze: bool) -> float:
            if not is_squeeze:
                return 0.3
            # 阶梯式查表 (与 volatility_regime.py:SQUEEZE_BREAKOUT_PROBS 一致)
            if duration >= 30:
                return 0.92
            elif duration >= 20:
                return 0.85
            elif duration >= 15:
                return 0.75
            elif duration >= 10:
                return 0.65
            elif duration >= 6:
                return 0.5
            else:
                return 0.4  # 基础概率
        
        squeeze_info = {
            "available": True,
            "bbw": round(bbw, 6),
            "bbw_median": round(bbw_median, 6),
            "bbw_ratio": round(bbw_ratio, 3),
            "bbw_percentile": bbw_percentile,
            "is_squeeze": is_squeeze_1h,
            "squeeze_duration_bars": squeeze_duration,
            "is_expansion": is_expansion,
            "status": "squeeze" if is_squeeze_1h else "expansion" if is_expansion else "normal",
            # P3: 使用阶梯式查表
            "breakout_probability": _get_breakout_probability(squeeze_duration, is_squeeze_1h),
        }
    result["squeeze_1h"] = squeeze_info
    
    # 2. ATR trend (from 1H data)
    atr_trend_info = {"available": False}
    if atr_1h is not None and atr_sma50 is not None and atr_sma50 > 0:
        atr_ratio_to_ma = atr_1h / atr_sma50
        
        if atr_ratio_to_ma > 1.2:
            atr_trend = "expanding"
        elif atr_ratio_to_ma < 0.8:
            atr_trend = "contracting"
        else:
            atr_trend = "stable"
        
        atr_trend_info = {
            "available": True,
            "atr_1h": round(atr_1h, 6),
            "atr_sma50": round(atr_sma50, 6),
            "atr_ratio_to_ma": round(atr_ratio_to_ma, 3),
            "trend": atr_trend,
        }
    result["atr_trend_1h"] = atr_trend_info
    
    # 3. Multi-timeframe volatility state
    mtf_volatility = {}
    for tf_name, vol_state in [("4h", vol_state_4h), ("1h", vol_state_1h), ("15m", vol_state_15m)]:
        if vol_state and isinstance(vol_state, dict):
            mtf_volatility[tf_name] = {
                "state": vol_state.get("state", "unknown"),
                "multiplier": vol_state.get("multiplier"),
            }
    if mtf_volatility:
        result["mtf_volatility"] = mtf_volatility
    
    # 4. Expected move calculation
    if atr_15m and current_price > 0:
        # 1.5x ATR as expected move
        expected_move_15m = atr_15m * 1.5
        result["expected_move"] = {
            "15m": {
                "atr": round(atr_15m, 4),
                "expected_move": round(expected_move_15m, 4),
                "expected_move_pct": round(expected_move_15m / current_price * 100, 3),
                "upper_target": round(current_price + expected_move_15m, 4),
                "lower_target": round(current_price - expected_move_15m, 4),
            }
        }
        
        if atr_1h:
            expected_move_1h = atr_1h * 1.5
            result["expected_move"]["1h"] = {
                "atr": round(atr_1h, 4),
                "expected_move": round(expected_move_1h, 4),
                "expected_move_pct": round(expected_move_1h / current_price * 100, 3),
                "upper_target": round(current_price + expected_move_1h, 4),
                "lower_target": round(current_price - expected_move_1h, 4),
            }
    
    # 5. Overall assessment
    regime = "normal"
    description = "Normal volatility conditions"
    trade_implication = "Standard position sizing"
    breakout_prob = squeeze_info.get("breakout_probability", 0.3)
    
    is_squeeze_detected = squeeze_info.get("is_squeeze", False)
    atr_trend = atr_trend_info.get("trend", "stable")
    squeeze_bars = squeeze_info.get("squeeze_duration_bars", 0)
    
    if is_squeeze_detected and atr_trend == "contracting":
        regime = "compression"
        description = "Volatility squeeze - consolidation phase, breakout likely"
        trade_implication = "Wait for breakout confirmation, prepare for larger move"
        if squeeze_bars >= 10:
            regime = "extreme_compression"
            description = f"Extended squeeze ({squeeze_bars} bars) - high probability breakout imminent"
    elif is_squeeze_detected:
        regime = "mild_compression"
        description = f"Volatility compression ({squeeze_bars} bars)"
        trade_implication = "Watch for breakout, smaller position size until confirmed"
    elif squeeze_info.get("is_expansion") and atr_trend == "expanding":
        regime = "expansion"
        description = "High volatility - trending or volatile conditions"
        trade_implication = "Wide stops needed, consider taking profits on extended moves"
        breakout_prob = 0.2
    elif atr_trend == "expanding":
        regime = "heating_up"
        description = "Volatility increasing - trend may be developing"
        trade_implication = "Momentum strategies favored"
        breakout_prob = 0.4
    elif atr_trend == "contracting":
        regime = "cooling_down"
        description = "Volatility decreasing - may enter consolidation"
        trade_implication = "Prepare for range-bound conditions"
        breakout_prob = 0.5
    
    # ATR% 供 SymbolAnalysis.volatility 与 LLM 投喂使用
    atr_pct_4h = round(atr_4h / current_price * 100, 3) if atr_4h and current_price and current_price > 0 else None
    atr_pct_1h = round(atr_1h / current_price * 100, 3) if atr_1h and current_price and current_price > 0 else None
    atr_pct_15m = round(atr_15m / current_price * 100, 3) if atr_15m and current_price and current_price > 0 else None

    result["overall"] = {
        "regime": regime,
        "description": description,
        "trade_implication": trade_implication,
        "breakout_probability": breakout_prob,
        "volatility_trend": atr_trend,
        "is_squeeze": is_squeeze_detected,
        "squeeze_duration_bars": squeeze_bars,
        "atr_pct_4h": atr_pct_4h,
        "atr_pct_1h": atr_pct_1h,
        "atr_pct_15m": atr_pct_15m,
    }
    
    return result


def _build_key_levels(tf4h: dict, tf1h: dict, tf15m: dict, current_price: float) -> dict:
    """构建关键价位汇总"""
    s4 = (tf4h or {}).get("structure", {})
    s1 = (tf1h or {}).get("structure", {})
    s15 = (tf15m or {}).get("structure", {})

    levels = {
        "current_price": current_price,
        "4h_range_high": s4.get("range_high"),
        "4h_range_low": s4.get("range_low"),
        "4h_swing_high": s4.get("swing_high"),
        "4h_swing_low": s4.get("swing_low"),
        "1h_swing_high": s1.get("swing_high"),
        "1h_swing_low": s1.get("swing_low"),
        "15m_swing_high": s15.get("swing_high"),
        "15m_swing_low": s15.get("swing_low"),
    }

    # 计算最近支撑阻力
    supports, resistances = [], []
    for key, value in levels.items():
        if value is None or key == "current_price" or current_price <= 0:
            continue
        if value < current_price:
            supports.append((key, value))
        elif value > current_price:
            resistances.append((key, value))

    supports.sort(key=lambda x: current_price - x[1])
    resistances.sort(key=lambda x: x[1] - current_price)

    # P12 Fix: 添加 flipped 标记，标识角色翻转的关键价位
    # swing_low 通常是支撑，如果出现在阻力列表中说明价格已跌破，角色翻转
    # swing_high 通常是阻力，如果出现在支撑列表中说明价格已突破，角色翻转
    def build_level_entry(name: str, price: float, is_support: bool) -> dict:
        entry = {
            "level": name,
            "price": price,
            "distance_pct": round(abs(current_price - price) / current_price * 100, 3)
        }
        
        # 检测角色翻转
        is_swing_low = "swing_low" in name
        is_swing_high = "swing_high" in name
        
        if is_swing_low and not is_support:
            # swing_low 出现在阻力位 = 价格跌破支撑，支撑变阻力
            entry["flipped"] = True
            entry["original_role"] = "support"
            entry["current_role"] = "resistance"
        elif is_swing_high and is_support:
            # swing_high 出现在支撑位 = 价格突破阻力，阻力变支撑
            entry["flipped"] = True
            entry["original_role"] = "resistance"
            entry["current_role"] = "support"
        
        return entry

    levels["nearest_supports"] = [
        build_level_entry(name, price, is_support=True)
        for name, price in supports[:3]
    ]
    levels["nearest_resistances"] = [
        build_level_entry(name, price, is_support=False)
        for name, price in resistances[:3]
    ]

    return levels


def _build_order_flow(tf15m: dict) -> dict:
    """
    构建订单流分析

    ❌ 删除: bias 结论
    ✅ 保留: 原始数值 (buy_sell_ratio, delta 等)
    """
    klines = (tf15m or {}).get("klines", [])
    if not klines:
        return {"available": False, "reason": "no_klines"}

    def analyze_bars(bars: List[dict]) -> dict:
        total_buy = sum(k.get("tbv", 0) for k in bars)
        total_sell = sum(k.get("tsv", 0) for k in bars)
        total_volume = sum(k.get("v", 0) for k in bars)
        delta = total_buy - total_sell
        ratio = total_buy / total_sell if total_sell > 0 else None

        # ❌ 删除 bias 结论，只返回数值
        return {
            "total_buy_volume": round(total_buy, 2),
            "total_sell_volume": round(total_sell, 2),
            "total_volume": round(total_volume, 2),
            "delta": round(delta, 2),
            "buy_sell_ratio": round(ratio, 4) if ratio else None,
        }

    recent_5 = klines[-5:] if len(klines) >= 5 else klines
    recent_10 = klines[-10:] if len(klines) >= 10 else klines

    # 成交量分析
    volumes = [k.get("v", 0) for k in klines]
    avg_volume = sum(volumes) / len(volumes) if volumes else 0
    last_volume = klines[-1].get("v", 0) if klines else 0

    # 最后一根 K 线分析
    last_bar = klines[-1] if klines else {}
    last_bar_analysis = {
        "buy_volume": last_bar.get("tbv", 0),
        "sell_volume": last_bar.get("tsv", 0),
        "delta": last_bar.get("tbv", 0) - last_bar.get("tsv", 0),
    }

    return {
        "available": True,
        "last_5_bars": analyze_bars(recent_5),
        "last_10_bars": analyze_bars(recent_10),
        "last_bar": last_bar_analysis,
        "volume_analysis": {
            "avg_volume": round(avg_volume, 2),
            "last_bar_volume": round(last_volume, 2),
            "volume_ratio": round(last_volume / avg_volume, 2) if avg_volume > 0 else None,
        }
    }

def _build_market_context(tf4h: dict, tf15m: dict) -> dict:
    """
    构建市场上下文

    ❌ 删除: volatility 结论
    ✅ 保留: atr_ratio (原始数值), market_phase
    """
    atr_ratio = (tf15m or {}).get("atr_ratio", 0)
    s4 = (tf4h or {}).get("structure", {})

    # 市场阶段（这是事实描述，不是交易建议）
    trend4 = s4.get("trend", "range")
    loc4 = s4.get("range_location", (tf4h or {}).get("range_location", "unknown"))

    if trend4 == "up":
        phase = "uptrend"
    elif trend4 == "down":
        phase = "downtrend"
    elif loc4 == "near_high":
        phase = "range_near_resistance"
    elif loc4 == "near_low":
        phase = "range_near_support"
    else:
        phase = "range_middle"

    return {
        "atr_ratio": atr_ratio,
        "market_phase": phase,
    }


# ==========================================================
# 4. 主构建函数（保持原有接口）
# ==========================================================
def build_unified_payload(symbol: str) -> Optional[dict]:
    """
    构建统一 payload（含增强数据）

    ✅ 接口不变
    ✅ 输出格式适配「AI 自主分析体」
    """
    tf4h = _get_snapshot(symbol, "4h")
    tf1h = _get_snapshot(symbol, "1h")
    tf15m = _get_snapshot(symbol, "15m")

    # 完全没有数据
    if not tf4h and not tf1h and not tf15m:
        return None

    # 标记缺失
    missing = []
    if not tf4h:
        missing.append("4h")
    if not tf1h:
        missing.append("1h")
    if not tf15m:
        missing.append("15m")

    # 核心数据缺失
    if not tf4h or not tf15m:
        return {
            "symbol": symbol,
            "timestamp": (tf15m or tf1h or tf4h or {}).get("timestamp"),
            "ready": False,
            "missing_timeframes": missing,
            "referee_context": {
                "rule_checks": {
                    "4h_structure_valid": False,
                    "15m_has_trigger": False,
                    "data_missing": True,
                },
                "context": {"missing": missing},
            },
        }

    # 🆕 裁判上下文（只有事实，没有 verdict）
    referee_context = build_referee_context(tf4h, tf1h, tf15m)

    # AI 增强（自动添加）
    ai_enhancement = _build_ai_enhancement(tf4h, tf1h, tf15m)

    # Pipeline 重构 Phase 3：同时写入 symbol_analysis，供 build_symbols_layer_v2 使用
    symbol_analysis_dict = None
    try:
        from analysis.conclusions.technical_analyzer import build_symbol_analysis
        sa = build_symbol_analysis(symbol, tf4h, tf1h, tf15m)
        symbol_analysis_dict = sa.to_dict()
    except Exception:
        pass

    payload = {
        "symbol": symbol,
        "timestamp": tf15m.get("timestamp") or tf15m.get("ts"),
        "ready": True,

        # 🆕 裁判上下文（替代原来的 referee）
        "referee_context": referee_context,

        # AI 增强数据
        "ai_enhancement": ai_enhancement,
        # Phase 3: 上游契约，下游 build_symbols_layer_v2 优先使用
        "symbol_analysis": symbol_analysis_dict,
    }

    if missing:
        payload["missing_timeframes"] = missing

    return payload


def build_unified_payload_v2(symbol: str) -> Optional[dict]:
    """
    Pipeline 重构 Phase 1b：使用 build_symbol_analysis 输出 SymbolAnalysis 契约。

    与 build_unified_payload 并行存在，便于对比输出。
    返回 payload 含 symbol_analysis (dict)，不再含 ai_enhancement 大杂烩。
    """
    tf4h = _get_snapshot(symbol, "4h")
    tf1h = _get_snapshot(symbol, "1h")
    tf15m = _get_snapshot(symbol, "15m")

    if not tf4h and not tf1h and not tf15m:
        return None

    missing = []
    if not tf4h:
        missing.append("4h")
    if not tf1h:
        missing.append("1h")
    if not tf15m:
        missing.append("15m")

    if not tf4h or not tf15m:
        return {
            "symbol": symbol,
            "timestamp": (tf15m or tf1h or tf4h or {}).get("timestamp"),
            "ready": False,
            "missing_timeframes": missing,
            "referee_context": {
                "rule_checks": {"4h_structure_valid": False, "15m_has_trigger": False, "data_missing": True},
                "context": {"missing": missing},
            },
            "symbol_analysis": None,
        }

    from analysis.conclusions.technical_analyzer import build_symbol_analysis

    sa = build_symbol_analysis(symbol, tf4h, tf1h, tf15m)
    referee_context = build_referee_context(tf4h, tf1h, tf15m)

    payload = {
        "symbol": symbol,
        "timestamp": (tf15m or {}).get("timestamp") or (tf15m or {}).get("ts"),
        "ready": True,
        "referee_context": referee_context,
        "symbol_analysis": sa.to_dict(),
    }
    if missing:
        payload["missing_timeframes"] = missing
    return payload


def save_unified_payload(symbol: str, ttl_sec: int = 300) -> Optional[dict]:
    """
    构建并保存统一 payload 到 Redis

    ✅ 接口完全不变
    """
    payload = build_unified_payload(symbol)
    if not payload:
        return None

    redis_client.set(
        f"global:payload:{symbol}",
        json.dumps(payload, ensure_ascii=False),
        ex=ttl_sec,
    )
    return payload


async def build_unified_payload_async(symbol: str) -> Optional[dict]:
    """
    构建统一 payload（含增强数据）- ASYNC 版本
    
    性能优化：使用原生异步 Redis 并行获取数据
    """
    # Snapshot fetches - run in parallel using native async
    tf4h, tf1h, tf15m = await asyncio.gather(
        _get_snapshot_async(symbol, "4h"),
        _get_snapshot_async(symbol, "1h"),
        _get_snapshot_async(symbol, "15m"),
    )

    # 完全没有数据
    if not tf4h and not tf1h and not tf15m:
        return None

    # 标记缺失
    missing = []
    if not tf4h:
        missing.append("4h")
    if not tf1h:
        missing.append("1h")
    if not tf15m:
        missing.append("15m")

    # 核心数据缺失
    if not tf4h or not tf15m:
        return {
            "symbol": symbol,
            "timestamp": (tf15m or tf1h or tf4h or {}).get("timestamp"),
            "ready": False,
            "missing_timeframes": missing,
            "referee_context": {
                "rule_checks": {
                    "4h_structure_valid": False,
                    "15m_has_trigger": False,
                    "data_missing": True,
                },
                "context": {"missing": missing},
            },
        }

    # 裁判上下文（只有事实，没有 verdict）
    referee_context = build_referee_context(tf4h, tf1h, tf15m)

    # AI 增强（使用异步版本，并行获取 Redis 数据）
    ai_enhancement = await _build_ai_enhancement_async(tf4h, tf1h, tf15m)

    # Phase 3: 同时写入 symbol_analysis
    symbol_analysis_dict = None
    try:
        from analysis.conclusions.technical_analyzer import build_symbol_analysis
        sa = build_symbol_analysis(symbol, tf4h, tf1h, tf15m)
        symbol_analysis_dict = sa.to_dict()
    except Exception:
        pass

    payload = {
        "symbol": symbol,
        "timestamp": tf15m.get("timestamp") or tf15m.get("ts"),
        "ready": True,
        "referee_context": referee_context,
        "ai_enhancement": ai_enhancement,
        "symbol_analysis": symbol_analysis_dict,
    }

    if missing:
        payload["missing_timeframes"] = missing

    return payload


async def save_unified_payload_async(symbol: str, ttl_sec: int = 300) -> Optional[dict]:
    """
    构建并保存统一 payload 到 Redis - ASYNC 版本
    
    性能优化：使用 build_unified_payload_async 并行获取数据
    """
    payload = await build_unified_payload_async(symbol)
    if not payload:
        return None

    redis = await get_async_redis()
    await redis.set(
        f"global:payload:{symbol}",
        json.dumps(payload, ensure_ascii=False),
        ex=ttl_sec,
    )
    return payload


# ==========================================================
# 5. 便捷函数（可选使用）
# ==========================================================
def get_ai_ready_payload(symbol: str) -> Optional[dict]:
    """从 Redis 获取已构建的 payload"""
    key = f"global:payload:{symbol}"
    v = redis_client.get(key)
    if not v:
        return None
    if isinstance(v, (bytes, bytearray)):
        v = v.decode("utf-8", errors="ignore")
    return json.loads(v)


def get_ai_decision_summary(symbol: str) -> Optional[dict]:
    """获取简化的 AI 决策摘要"""
    payload = get_ai_ready_payload(symbol)
    if not payload:
        return None

    return {
        "symbol": payload.get("symbol"),
        "timestamp": payload.get("timestamp"),
        "ready": payload.get("ready"),
        "referee_context": payload.get("referee_context"),
        "ai_enhancement": payload.get("ai_enhancement"),
    }