# symbol_analysis_assembler.py
"""
Pipeline 重构 Phase 1a：将 technical_analyzer + payload_builder 输出组装为 SymbolAnalysis。

本模块是唯一同时依赖 technical_analyzer 与 payload_builder 的组装层，
避免 technical_analyzer 直接依赖 payload_builder 造成循环引用。

入口：build_symbol_analysis(symbol, tf4h, tf1h, tf15m) -> SymbolAnalysis
"""

from typing import Optional, Dict, Any

from analysis.assembly.symbol_analysis import (
    SymbolAnalysis,
    TrendModule,
    IndicatorsModule,
    StructureModule,
    LevelsModule,
    MomentumModule,
    OrderFlowModule,
    VolatilityModule,
    PatternModule,
    CorrelationModule,
    SentimentModule,
    BiasModule,
    GuidanceModule,
)


def _map_conclusions_to_trend(conclusions: dict) -> TrendModule:
    """从 technical_analyzer 结论映射到 TrendModule"""
    t = TrendModule()
    c4 = conclusions.get("tf4h") or {}
    c1 = conclusions.get("tf1h") or {}
    c15 = conclusions.get("tf15m") or {}
    if not c4.get("available"):
        return t
    t.direction_4h = c4.get("trend_direction", "unknown")
    t.strength_4h = c4.get("trend_strength", "unknown")
    t.quality_4h = c4.get("trend_quality", "unknown")
    t.continuation_prob_4h = c4.get("continuation_prob", "unknown")
    t.ema_slope_4h = c4.get("ema_slope") or "flat"
    t.price_vs_ema_4h = c4.get("price_vs_ema", "between")
    # 4H 补充
    t.exhaustion_signal_4h = c4.get("exhaustion_signal")
    t.adx_rising_4h = c4.get("adx_rising")
    if c1.get("available"):
        ss = c1.get("structure_state", "unknown")
        t.structure_1h = ss
        t.direction_1h = "up" if ss in ("impulse_up", "pullback_up") else "down" if ss in ("impulse_down", "pullback_down") else "neutral"
        # 1H 补充
        t.price_location_1h = c1.get("price_location")
        t.trade_space_1h = c1.get("trade_space")
        t.space_up_atr = c1.get("space_up_atr")
        t.space_down_atr = c1.get("space_down_atr")
        t.volatility_state_1h = c1.get("volatility_state")
        t.consolidation_1h = c1.get("consolidation")
        t.breakout_status_1h = c1.get("breakout_status")
    if c15.get("available"):
        ms = c15.get("micro_structure", "unclear")
        t.micro_structure_15m = ms
        t.direction_15m = "up" if ms in ("higher_highs_lows", "higher_lows") else "down" if ms in ("lower_highs_lows", "lower_highs") else "neutral"
        # 15M 补充
        t.volume_confirmation_15m = c15.get("volume_confirmation")
        t.obv_direction_15m = c15.get("obv_direction")
        t.rejection_strength_15m = c15.get("rejection_strength")
        t.rejection_direction_15m = c15.get("rejection_direction")
        t.key_level_status_15m = c15.get("key_level_status")
    return t


def _map_to_indicators(conclusions: dict, tf4h: dict, tf1h: dict, tf15m: dict) -> IndicatorsModule:
    """从结论 + 原始 snapshot 映射到 IndicatorsModule"""
    ind = IndicatorsModule()
    c4 = conclusions.get("tf4h") or {}
    if not c4.get("available"):
        return ind
    ind.rsi_4h = c4.get("rsi") or (tf4h or {}).get("rsi14")
    ind.adx_4h = c4.get("adx")
    ind.di_direction_4h = c4.get("di_direction", "neutral")
    ind.macd_histogram_4h = (tf4h or {}).get("macd_hist")
    ind.macd_cross_4h = (tf4h or {}).get("macd_cross") or (tf4h or {}).get("macd_cross_4h")
    ind.rsi_zone = c4.get("momentum_state")  # overbought/oversold/bullish/bearish/neutral
    c1 = conclusions.get("tf1h") or {}
    if c1.get("available"):
        ind.rsi_1h = (tf1h or {}).get("rsi14")
        ind.adx_1h = (tf1h or {}).get("adx", {}).get("adx14") if isinstance((tf1h or {}).get("adx"), dict) else None
        ind.macd_histogram_1h = (tf1h or {}).get("macd_hist")
    ind.rsi_15m = (tf15m or {}).get("rsi14")
    # EMA 距离：若有 close 和 ema 可在此计算，否则留 None
    ema4 = (tf4h or {}).get("ema", {})
    close4 = (tf4h or {}).get("close")
    ema21_4h = ema4.get("EMA_21") or ema4.get("EMA_20")
    if close4 and ema21_4h and ema21_4h != 0:
        ind.ema_distance_pct_4h = round((close4 - ema21_4h) / ema21_4h * 100, 3)
    ema1 = (tf1h or {}).get("ema", {})
    close1 = (tf1h or {}).get("close")
    ema21_1h = ema1.get("EMA_21") or ema1.get("EMA_20")
    if close1 and ema21_1h and ema21_1h != 0:
        ind.ema_distance_pct_1h = round((close1 - ema21_1h) / ema21_1h * 100, 3)
    return ind


def _map_bias_to_module(overall_bias: dict) -> BiasModule:
    """从 calculate_overall_bias 输出映射到 BiasModule"""
    b = BiasModule()
    if not overall_bias.get("available"):
        return b
    b.score = overall_bias.get("bias_score")
    b.direction = (overall_bias.get("bias_direction") or "unknown").replace("bullish", "bullish").replace("bearish", "bearish")
    b.strength = overall_bias.get("bias_strength", "unknown")
    b.factors = overall_bias.get("bias_factors") or []
    b.reversal_risk = overall_bias.get("reversal_risk", "unknown")
    b.reversal_score = overall_bias.get("reversal_score")
    b.reversal_factors = overall_bias.get("reversal_factors") or []
    b.trend_conflict = overall_bias.get("trend_conflict", False)
    b.trade_suggestion = overall_bias.get("trade_suggestion")
    b.layer_scores = overall_bias.get("layer_scores") or {}
    return b


def _map_structure_dict(d: Optional[dict], tf15m: Optional[dict] = None) -> StructureModule:
    """从 _build_market_structure 输出映射到 StructureModule"""
    s = StructureModule()
    if not d:
        return s
    s.state_4h = d.get("state_4h") or "unknown"
    s.state_1h = d.get("state_1h") or "unknown"
    s.state_15m = d.get("state_15m") or "unknown"
    s.swing_high = d.get("swing_high")
    s.swing_low = d.get("swing_low")
    s.last_hh = d.get("last_hh")
    s.last_ll = d.get("last_ll")
    s.last_hl = d.get("last_hl")
    s.last_lh = d.get("last_lh")
    lb = (d.get("last_break") or "").strip().lower()
    if lb:
        s.choch_status = "detected" if "choch" in lb else "none"
        s.bos_status = "detected" if "bos" in lb else "none"
        s.last_break_type = d.get("last_break")
        if "choch" in lb:
            s.choch_direction = "up" if "choch_up" in lb else "down" if "choch_down" in lb else None
        if "bos" in lb:
            s.bos_direction = "up" if "bos_up" in lb else "down" if "bos_down" in lb else None
    s.health_score = d.get("structure_health")
    # 区间信息
    rh = d.get("range_high_4h")
    rl = d.get("range_low_4h")
    if rh and rl:
        s.is_range_4h = s.state_4h in ("range", "unknown") or (rh - rl > 0)
        s.range_location_4h = d.get("range_location_4h")
    # 15M 入场信号
    if tf15m:
        sig = tf15m.get("signal")
        if sig:
            s.has_entry_trigger_15m = sig in ("long", "short")
            s.entry_signal_15m = sig
    return s


def _map_levels_dict(d: Optional[dict], current_price: float) -> LevelsModule:
    """从 _build_key_levels 输出映射到 LevelsModule"""
    lev = LevelsModule()
    if not d or current_price <= 0:
        return lev
    supps = d.get("nearest_supports") or []
    resis = d.get("nearest_resistances") or []
    if supps:
        ne = supps[0]
        lev.support_price = ne.get("price")
        lev.support_distance_pct = ne.get("distance_pct")
    if resis:
        ne = resis[0]
        lev.resistance_price = ne.get("price")
        lev.resistance_distance_pct = ne.get("distance_pct")
    lev.range_high = d.get("4h_range_high")
    lev.range_low = d.get("4h_range_low")
    if lev.range_high and lev.range_low and lev.range_high > lev.range_low:
        lev.range_mid = (lev.range_high + lev.range_low) / 2
        lev.price_in_range_pct = round((current_price - lev.range_low) / (lev.range_high - lev.range_low) * 100, 2)
    if supps and resis:
        ds = supps[0].get("distance_pct")
        dr = resis[0].get("distance_pct")
        if ds is not None and dr is not None:
            lev.nearest_level_type = "support" if ds <= dr else "resistance"
            lev.nearest_level_distance_pct = min(ds, dr)
    return lev


def _map_order_flow_dict(d: Optional[dict]) -> OrderFlowModule:
    """从 _build_order_flow 输出映射到 OrderFlowModule"""
    o = OrderFlowModule()
    if not d or not d.get("available"):
        return o
    lb5 = d.get("last_5_bars") or {}
    o.ratio_5bar = lb5.get("buy_sell_ratio")
    o.delta_5bar = lb5.get("delta")
    va = d.get("volume_analysis") or {}
    o.volume_ratio = va.get("volume_ratio")
    if lb5.get("total_buy_volume") is not None and lb5.get("total_sell_volume") is not None:
        tot = (lb5.get("total_buy_volume") or 0) + (lb5.get("total_sell_volume") or 0)
        if tot > 0:
            o.buy_pressure_pct = round((lb5.get("total_buy_volume") or 0) / tot * 100, 2)
    ratio = o.ratio_5bar
    if ratio is not None:
        if ratio >= 1.5:
            o.imbalance = "strong_buying"
        elif ratio >= 1.1:
            o.imbalance = "buying"
        elif ratio <= 0.67:
            o.imbalance = "strong_selling"
        elif ratio <= 0.9:
            o.imbalance = "selling"
        else:
            o.imbalance = "balanced"
    return o


def _map_volatility_dict(d: Optional[dict]) -> VolatilityModule:
    """从 _build_volatility_regime 输出映射到 VolatilityModule"""
    v = VolatilityModule()
    if not d or not d.get("available"):
        return v
    ov = d.get("overall") or {}
    v.regime = ov.get("regime", "unknown")
    v.trend = ov.get("volatility_trend")
    sq = d.get("squeeze_1h") or {}
    v.squeeze_active = sq.get("is_squeeze", False)
    v.squeeze_status = sq.get("status")
    v.squeeze_bars = sq.get("squeeze_duration_bars")
    v.breakout_probability = sq.get("breakout_probability") or ov.get("breakout_probability")
    v.description = ov.get("description")
    v.trade_implication = ov.get("trade_implication")
    atr_tr = d.get("atr_trend_1h") or {}
    v.atr_ratio_to_ma = atr_tr.get("atr_ratio_to_ma")
    v.atr_pct_4h = ov.get("atr_pct_4h")
    v.atr_pct_1h = ov.get("atr_pct_1h")
    v.atr_pct_15m = ov.get("atr_pct_15m")
    # BBW 补充
    v.bb_width = sq.get("bbw")
    v.bb_width_percentile = sq.get("bbw_percentile")
    v.bbw_ratio = sq.get("bbw_ratio")
    if sq.get("is_squeeze") and v.bb_width_percentile is not None:
        v.squeeze_intensity = "tight" if v.bb_width_percentile < 10 else "moderate" if v.bb_width_percentile < 30 else "loose"
    return v


def _map_pattern_dict(d: Optional[dict]) -> PatternModule:
    """从 _build_pattern_analysis_safe 输出映射到 PatternModule"""
    p = PatternModule()
    if not d or not d.get("available"):
        return p
    p.name = d.get("current_pattern_type")
    p.confidence = d.get("sample_reliability")
    p.win_rate = d.get("historical_win_rate")
    p.avg_move_pct = d.get("historical_avg_return")
    p.sample_size = d.get("matches_found")
    # direction 可从 probable_direction 来
    pd = d.get("probable_direction")
    if pd:
        p.direction = "bullish" if pd in ("up", "bullish") else "bearish" if pd in ("down", "bearish") else None
    return p


def _map_correlation_dict(d: Optional[dict]) -> CorrelationModule:
    """从 _build_correlation_safe 输出映射到 CorrelationModule"""
    c = CorrelationModule()
    if not d or not d.get("available"):
        return c
    c.btc_correlation = d.get("btc_correlation")
    c.eth_correlation = d.get("eth_correlation")
    c.beta = d.get("btc_beta")
    c.lead_lag_bars = d.get("lead_lag_bars")
    c.relative_strength = d.get("relative_strength_24bars")
    c.decoupled = d.get("is_independent", False)
    perf = d.get("relative_performance")
    if perf:
        c.outperforming = "outperforming" in str(perf).lower() or perf == "strong"
    return c


def _map_momentum_from_conclusions(conclusions: dict, overall_bias: dict = None) -> MomentumModule:
    """从 conclusions + overall_bias 推导动量模块"""
    m = MomentumModule()
    c4 = conclusions.get("tf4h") or {}
    if not c4.get("available"):
        return m
    mom = c4.get("momentum_state", "neutral")
    m.state_4h = mom
    m.direction = "bullish" if mom in ("bullish", "overbought") else "bearish" if mom in ("bearish", "oversold") else "neutral"
    # 从 overall_bias 交叉填充反转风险
    if overall_bias and overall_bias.get("available"):
        m.reversal_risk = overall_bias.get("reversal_risk", "unknown")
        m.reversal_score = overall_bias.get("reversal_score")
        m.reversal_factors = overall_bias.get("reversal_factors") or []
    return m


def _build_guidance(technicals: dict, tf4h: dict, tf1h: dict, tf15m: dict,
                    current_price: float, key_levels: Optional[dict]) -> GuidanceModule:
    """从 signal_guidance 构建 GuidanceModule"""
    g = GuidanceModule()
    try:
        from analysis.conclusions.signal_guidance import (
            assess_mtf_alignment, calculate_reversal_risk, generate_action_guidance,
        )
        from analysis.indicators.core import get_mtf_states_for_symbol

        # 构建 tf_states
        c4 = technicals.get("tf4h") or {}
        c1 = technicals.get("tf1h") or {}
        c15 = technicals.get("tf15m") or {}
        tf_states = {}
        for label, c in [("4h", c4), ("1h", c1), ("15m", c15)]:
            if c.get("available"):
                tf_states[label] = {
                    "trend": c.get("trend_direction") or c.get("structure_state", "unknown"),
                    "momentum": c.get("momentum_state", "neutral"),
                    "trend_confidence": c.get("trend_confidence"),
                    "last_break": None,
                    "range_location": None,
                }
        if not tf_states:
            return g

        alignment = assess_mtf_alignment(tf_states)
        g.mtf_conflict = alignment.get("conflict")
        g.mtf_conflict_type = alignment.get("conflict_type")
        g.mtf_alignment_score = alignment.get("alignment_score")
        g.mtf_action_bias = alignment.get("action_bias")

        trend = c4.get("trend_direction", "neutral")
        momentum = c4.get("momentum_state", "neutral")
        rev_conf = c4.get("trend_confidence") or 0.0
        rev_risk = calculate_reversal_risk(trend, momentum, rev_conf, alignment)
        g.reversal_risk_level = rev_risk if isinstance(rev_risk, str) else (rev_risk or {}).get("reversal_risk")

        kl = {}
        if key_levels:
            supps = key_levels.get("nearest_supports") or []
            resis = key_levels.get("nearest_resistances") or []
            if supps:
                kl["support"] = supps[0].get("price", 0)
            if resis:
                kl["resistance"] = resis[0].get("price", 0)

        guidance = generate_action_guidance(
            trend=trend, momentum=momentum,
            reversal_assessment=alignment,
            current_price=current_price or 0,
            key_levels=kl, mtf_alignment=alignment,
        )
        g.primary_bias = guidance.get("primary_bias")
        new_pos = guidance.get("for_new_positions") or {}
        g.for_new_positions = new_pos.get("action") if isinstance(new_pos, dict) else new_pos
        g.scenarios = guidance.get("scenarios")
        g.risk_warnings = guidance.get("risk_warnings") or []
    except Exception:
        pass
    return g


def build_symbol_analysis(
    symbol: str,
    tf4h: dict,
    tf1h: dict,
    tf15m: dict,
    *,
    structure: Optional[dict] = None,
    key_levels: Optional[dict] = None,
    order_flow: Optional[dict] = None,
    volatility_regime: Optional[dict] = None,
    pattern_analysis: Optional[dict] = None,
    correlation: Optional[dict] = None,
    sentiment: Optional[dict] = None,
) -> SymbolAnalysis:
    """
    完整的上游分析，输出 SymbolAnalysis 数据契约。

    若未传入 structure/key_levels/order_flow/volatility_regime/pattern_analysis/correlation，
    则从 payload_builder 拉取（会调用 _build_*），保证单入口即可得到完整 SymbolAnalysis。
    """
    from analysis.conclusions.technical_analyzer import build_technicals_conclusions, calculate_overall_bias
    from analysis.assembly.payload_builder import (
        _build_market_structure,
        _build_key_levels,
        _build_order_flow,
        _build_volatility_regime,
        _build_pattern_analysis_safe,
        _build_correlation_safe,
    )

    technicals = build_technicals_conclusions(tf4h, tf1h, tf15m)
    overall_bias = calculate_overall_bias(technicals)
    current_price = (tf15m or {}).get("close") or (tf4h or {}).get("close") or 0.0

    if structure is None:
        structure = _build_market_structure(tf4h, tf1h, tf15m)
    if key_levels is None:
        key_levels = _build_key_levels(tf4h, tf1h, tf15m, current_price)
    if order_flow is None:
        order_flow = _build_order_flow(tf15m)
    if volatility_regime is None:
        volatility_regime = _build_volatility_regime(tf4h, tf1h, tf15m, current_price)
    if pattern_analysis is None and symbol:
        pattern_analysis = _build_pattern_analysis_safe(symbol)
    if correlation is None and symbol:
        correlation = _build_correlation_safe(symbol)

    price = float(current_price) if current_price else None
    # 24h 变化等若有在 snapshot 中则取，否则留 None
    change_24h = (tf15m or {}).get("change_24h_pct") or (tf4h or {}).get("change_24h_pct")
    high_24h = (tf15m or {}).get("high_24h") or (tf4h or {}).get("high_24h")
    low_24h = (tf15m or {}).get("low_24h") or (tf4h or {}).get("low_24h")
    volume_24h = (tf15m or {}).get("volume_24h_usdt") or (tf4h or {}).get("volume_24h_usdt")

    sentiment_mod = SentimentModule()
    if sentiment and isinstance(sentiment, dict):
        sentiment_mod.funding_rate = sentiment.get("funding_rate")
        sentiment_mod.funding_bias = sentiment.get("funding_bias")
        sentiment_mod.oi_change_1h_pct = sentiment.get("oi_change_1h_pct")
        sentiment_mod.oi_change_4h_pct = sentiment.get("oi_change_4h_pct")
        sentiment_mod.oi_change_24h_pct = sentiment.get("oi_change_24h_pct")
        sentiment_mod.long_short_ratio = sentiment.get("long_short_ratio")
        sentiment_mod.positioning = sentiment.get("positioning")

    # 构建 GuidanceModule（signal_guidance）
    guidance_mod = _build_guidance(technicals, tf4h, tf1h, tf15m, current_price, key_levels)

    return SymbolAnalysis(
        symbol=symbol,
        price=price,
        change_24h_pct=change_24h,
        high_24h=high_24h,
        low_24h=low_24h,
        volume_24h_usdt=volume_24h,
        trend=_map_conclusions_to_trend(technicals),
        indicators=_map_to_indicators(technicals, tf4h, tf1h, tf15m),
        structure=_map_structure_dict(structure, tf15m),
        levels=_map_levels_dict(key_levels, current_price or 0),
        momentum=_map_momentum_from_conclusions(technicals, overall_bias),
        order_flow=_map_order_flow_dict(order_flow),
        volatility=_map_volatility_dict(volatility_regime),
        pattern=_map_pattern_dict(pattern_analysis),
        correlation=_map_correlation_dict(correlation),
        sentiment=sentiment_mod,
        guidance=guidance_mod,
        bias=_map_bias_to_module(overall_bias),
    )
