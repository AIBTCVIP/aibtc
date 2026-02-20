# technical_analyzer.py - 技术指标分析器
"""
多周期技术指标结论构建器

只输出 AI 可直接使用的结论，所有判断逻辑在此模块内完成。
不输出原始值，节省 token，确保判断一致性。

使用方式：
    from analysis.conclusions.technical_analyzer import build_technicals_conclusions
    conclusions = build_technicals_conclusions(tf4h, tf1h, tf15m)
"""

from typing import Dict, Optional, List
import numpy as np

# ==========================================================
# 配置常量（与你的规格一致）
# ==========================================================
# ADX 阈值
ADX_STRONG = 25.0
ADX_WEAK = 18.0

# RSI 阈值
RSI_OVERBOUGHT = 70
RSI_OVERSOLD = 30
RSI_BULL_ZONE_LOW = 55
RSI_BULL_ZONE_HIGH = 70
RSI_BEAR_ZONE_LOW = 30
RSI_BEAR_ZONE_HIGH = 45
RSI_NEUTRAL_LOW = 45
RSI_NEUTRAL_HIGH = 55

# ATR 比例阈值（基准值，会根据币种动态调整）
ATR_EXPANDING_RATIO = 1.2
ATR_CONTRACTING_RATIO = 0.8
ATR_PRICE_LOCATION_K = 0.7
ATR_STRONG_REJECTION_K = 0.8
ATR_WEAK_REJECTION_K = 0.4

# 关键位阈值（按币种类型）
KEY_LEVEL_THRESHOLDS = {
    # 主流币：阈值更严格
    "major": {
        "touch": 0.2,  # 触及：0.2 ATR
        "near": 0.6,  # 接近：0.6 ATR
    },
    # 山寨币：阈值更宽松
    "altcoin": {
        "touch": 0.4,  # 触及：0.4 ATR
        "near": 1.2,  # 接近：1.2 ATR
    },
}

# 主流币列表
MAJOR_COINS = {"BTCUSDT", "ETHUSDT"}


def get_key_level_thresholds(symbol: str = None) -> dict:
    """根据币种返回合适的阈值"""
    if symbol and symbol.upper() in MAJOR_COINS:
        return KEY_LEVEL_THRESHOLDS["major"]
    return KEY_LEVEL_THRESHOLDS["altcoin"]


# 交易空间阈值
TRADE_SPACE_WIDE = 2.0
TRADE_SPACE_MODERATE = 1.0

# 成交量阈值
# U16 fix: 提高阈值，原来 1.2/1.0 导致 ~50% K线都是 moderate
VOLUME_STRONG_RATIO = 1.5
VOLUME_MODERATE_RATIO = 1.2

# L2 cleanup: EMA_SLOPE_LOOKBACK 已移除（M18 fix 改用 EMA_prev5 直接计算）


# ==========================================================
# 主入口函数
# ==========================================================
def build_technicals_conclusions(tf4h: dict, tf1h: dict, tf15m: dict) -> dict:
    """
    构建所有周期的技术指标结论

    返回:
        {
            "tf4h": {...},  # 4H 结论
            "tf1h": {...},  # 1H 结论
            "tf15m": {...}, # 15M 结论
        }
    """
    return {
        "tf4h": _build_4h_conclusions(tf4h),
        "tf1h": _build_1h_conclusions(tf1h, tf15m),  # 1H 需要 15M 的 ATR 做参考
        "tf15m": _build_15m_conclusions(tf15m, tf1h),  # 15M 需要 1H 的关键位
    }


# ==========================================================
# 4H 技术指标结论
# ==========================================================
def _build_4h_conclusions(tf4h: dict) -> dict:
    """
    4H 结论：趋势方向、强度、质量、延续概率、衰竭信号、动量状态

    所需输入字段（斐波那契 EMA）：
    - close, atr, ema (EMA_21, EMA_55, EMA_89, EMA_21_prev)
    - adx (adx14, di_plus, di_minus, adx_prev)
    - rsi14, supertrend_dir
    - macd_hist, macd_hist_history
    """
    if not tf4h:
        return {"available": False, "reason": "no_4h_data"}

    close = tf4h.get("close", 0)
    ema = tf4h.get("ema", {})
    adx_data = tf4h.get("adx", {})
    atr = tf4h.get("atr")  # 用于动态阈值

    # ---------- 1. 趋势方向 (EMA + Supertrend) ----------
    # 使用斐波那契 EMA：21（短期）和 55（中期）
    ema_fast = ema.get("EMA_21") or ema.get("EMA_20")  # 兼容旧配置
    ema_slow = ema.get("EMA_55") or ema.get("EMA_60")  # 兼容旧配置
    ema_fast_prev = ema.get("EMA_21_prev") or ema.get("EMA_20_prev")
    supertrend_dir = tf4h.get("supertrend_dir")

    # EMA 趋势（使用动态 ATR 阈值）
    ema_trend = "neutral"
    ema_slope_dir = "flat"

    if ema_fast and ema_slow:
        # 动态阈值：基于 ATR 占价格比例，最小 0.3%，最大 1%
        # 这样在高波动时需要更大差距才确认趋势，减少假信号
        if atr and close > 0:
            atr_ratio = atr / close
            threshold = max(0.003, min(0.01, atr_ratio * 0.5))
        else:
            threshold = 0.005  # 默认 0.5%

        if ema_fast > ema_slow * (1 + threshold):
            ema_trend = "up"
        elif ema_fast < ema_slow * (1 - threshold):
            ema_trend = "down"

        # M18 fix: 使用 5 根 K 线窗口计算斜率（4H = 20小时），减少单根 K 线噪声
        # 优先使用 EMA_prev5（5 根前），回退到 EMA_prev（1 根前）
        ema_fast_prev5 = ema.get("EMA_21_prev5") or ema.get("EMA_20_prev5")
        slope_ref = ema_fast_prev5 if ema_fast_prev5 and ema_fast_prev5 != 0 else ema_fast_prev
        slope_bars = 5 if ema_fast_prev5 and ema_fast_prev5 != 0 else 1
        if slope_ref and slope_ref != 0:
            slope = (ema_fast - slope_ref) / abs(slope_ref)
            # 多根窗口不需要缩放阈值，单根窗口用更小阈值
            slope_threshold = threshold * (0.3 if slope_bars == 5 else 0.1)
            if slope > slope_threshold:
                ema_slope_dir = "rising"
            elif slope < -slope_threshold:
                ema_slope_dir = "falling"

    # 综合趋势方向（EMA + Supertrend 确认）
    trend_direction = ema_trend
    if supertrend_dir:
        if ema_trend == supertrend_dir:
            trend_direction = ema_trend  # 双重确认
        elif ema_trend == "neutral":
            trend_direction = supertrend_dir

    # ---------- 1.5 价格位置修正（解决 EMA 滞后问题）----------
    # 当价格已经跌破/突破两条 EMA 时，修正趋势判断
    price_vs_ema = "between"  # 价格在两条 EMA 之间
    if ema_fast and ema_slow and close > 0:
        price_above_fast = close > ema_fast
        price_above_slow = close > ema_slow
        
        if price_above_fast and price_above_slow:
            price_vs_ema = "above"  # 价格在两条 EMA 上方
        elif not price_above_fast and not price_above_slow:
            price_vs_ema = "below"  # 价格在两条 EMA 下方
        
        # 价格位置修正趋势判断
        # 情况1: EMA 显示上涨，但价格已跌破两条 EMA → 趋势转弱
        if trend_direction == "up" and price_vs_ema == "below":
            trend_direction = "weakening"
        # 情况2: EMA 显示下跌，但价格已突破两条 EMA → 趋势反弹
        elif trend_direction == "down" and price_vs_ema == "above":
            trend_direction = "recovering"
        # U2 fix (保守方案): EMA 和 Supertrend 冲突 + 价格在 EMA 之间 → 降级
        # 只在价格已回到 EMA 之间（不再是强势位置）且 Supertrend 也翻转时才降级
        # 避免 Supertrend 在高波动币种上的假信号
        elif trend_direction == "up" and supertrend_dir == "down" and price_vs_ema == "between":
            trend_direction = "weakening"
        elif trend_direction == "down" and supertrend_dir == "up" and price_vs_ema == "between":
            trend_direction = "recovering"

    # ---------- 2. 趋势强度 (ADX) ----------
    adx14 = adx_data.get("adx14")
    di_plus = adx_data.get("di_plus")
    di_minus = adx_data.get("di_minus")
    adx_prev = adx_data.get("adx_prev")

    # M6 fix: ADX 无数据时用 "unknown" 而非 "weak"（无数据 ≠ 弱趋势）
    trend_strength = "unknown"
    if adx14 is not None:
        if adx14 >= ADX_STRONG:
            trend_strength = "strong"
        elif adx14 >= ADX_WEAK:
            trend_strength = "moderate"
        else:
            trend_strength = "choppy"

    # DI 方向
    di_direction = "neutral"
    if di_plus and di_minus:
        if di_plus > di_minus * 1.1:
            di_direction = "bullish"
        elif di_minus > di_plus * 1.1:
            di_direction = "bearish"

    # U7/U5/U6 fix: 统一 base_trend 变量
    # weakening 是上升趋势的变体，recovering 是下降趋势的变体
    # 下游的趋势质量、延续概率、衰竭信号都需要基于原始趋势方向判断
    base_trend = trend_direction
    if trend_direction == "weakening":
        base_trend = "up"
    elif trend_direction == "recovering":
        base_trend = "down"

    # ---------- 3. 趋势质量 (ADX + EMA 综合) ----------
    # U5 fix: 使用 base_trend 而非 ema_trend，确保 Supertrend 确认的趋势也被考虑
    # ADX >= 18 + EMA 斜率一致 = good
    # ADX < 18 或斜率矛盾 = poor
    trend_quality = "poor"
    if adx14 is not None and adx14 >= ADX_WEAK:
        if (base_trend == "up" and ema_slope_dir == "rising") or \
                (base_trend == "down" and ema_slope_dir == "falling"):
            trend_quality = "good"
        elif base_trend != "neutral":
            trend_quality = "moderate"

    # ---------- 4. 延续概率 (ADX斜率 + DI一致性) ----------
    # U6 fix: 使用 base_trend，确保 weakening/recovering 时 DI 一致性检查仍能匹配
    # ADX >= 25: +1, ADX 上升: +1, DI 方向一致: +1
    cont_score = 0
    if adx14 is not None and adx14 >= ADX_STRONG:
        cont_score += 1
    if adx_prev is not None and adx14 is not None and adx14 > adx_prev:
        cont_score += 1
    if (base_trend == "up" and di_direction == "bullish") or \
            (base_trend == "down" and di_direction == "bearish"):
        cont_score += 1

    if cont_score >= 3:
        continuation_prob = "high"
    elif cont_score >= 2:
        continuation_prob = "medium"
    else:
        continuation_prob = "low"

    # ---------- 5. 衰竭信号 (RSI + MACD) ----------
    rsi14 = tf4h.get("rsi14")
    macd_hist = tf4h.get("macd_hist")
    macd_hist_history = tf4h.get("macd_hist_history", [])

    exhaustion_signal = "none"

    # U7 fix: RSI 衰竭判断 — 使用 base_trend 确保 weakening/recovering 也能检测衰竭
    rsi_exhaustion = "none"
    if rsi14 is not None:
        if base_trend == "up":
            if rsi14 > RSI_OVERBOUGHT:
                rsi_exhaustion = "early"
            # M4 fix: RSI < 40 才确认上升趋势衰竭（原 < 50 太激进，正常回调也触发）
            elif rsi14 < 40:
                rsi_exhaustion = "confirmed"
        elif base_trend == "down":
            if rsi14 < RSI_OVERSOLD:
                rsi_exhaustion = "early"
            # M4 fix: RSI > 60 才确认下降趋势衰竭（原 > 50 太激进）
            elif rsi14 > 60:
                rsi_exhaustion = "confirmed"
        # V5-03 fix: neutral 趋势下 overbought/oversold 也应触发 early exhaustion
        # B5 fix 移除了 overbought/oversold 对 base_score 的贡献，
        # 但 RSI exhaustion 只检查 base_trend=="up"/"down"。
        # 当 base_trend=="neutral" 且 RSI>70 或 RSI<30 时，
        # 极端 RSI 对评分模型完全不可见 — 系统可能在超买条件下推荐开仓。
        # 修复：neutral 趋势下极端 RSI 也标记为 "early" exhaustion，
        # 通过 reversal_penalty 路径抑制信号。
        elif base_trend == "neutral":
            if rsi14 > RSI_OVERBOUGHT or rsi14 < RSI_OVERSOLD:
                rsi_exhaustion = "early"

    # U7 fix: MACD Histogram 连续走弱判断（最近 4 根）— 使用 base_trend
    hist_weakening = False
    if len(macd_hist_history) >= 4:
        recent_hist = macd_hist_history[-4:]
        
        if base_trend == "up":
            # 上升趋势：histogram 应为正值，衰竭 = 正值递减
            # A9 fix: 也检测已穿零的情况（更强的衰竭信号）
            # 原逻辑要求 all(h > 0)，但 histogram 从正穿零是更强的看跌信号
            if all(h > 0 for h in recent_hist):
                diffs = np.diff(recent_hist)
                # V6+U8 fix: 放宽为 2/3 递减即可（4 个值产生 3 个 diff，3/3 等于 all()）
                # 允许 1 个非递减 diff，捕获更多衰竭信号
                if np.sum(diffs < 0) >= 2:
                    hist_weakening = True
            elif recent_hist[-1] <= 0 and any(h > 0 for h in recent_hist[:2]):
                # A9 fix: histogram 从正值穿越到零或负值 — 比递减更强的衰竭信号
                hist_weakening = True
        elif base_trend == "down":
            # 下降趋势：histogram 应为负值，衰竭 = 负值递增（向零靠近）
            # A9 fix: 也检测已穿零的情况
            if all(h < 0 for h in recent_hist):
                diffs = np.diff(recent_hist)  # 负值递增 = diff > 0
                # V6+U8 fix: 放宽为 2/3 递增即可
                if np.sum(diffs > 0) >= 2:
                    hist_weakening = True
            elif recent_hist[-1] >= 0 and any(h < 0 for h in recent_hist[:2]):
                # A9 fix: histogram 从负值穿越到零或正值
                hist_weakening = True
        else:
            # 中性趋势：使用绝对值递减作为衰竭信号
            diffs = np.diff([abs(x) for x in recent_hist])
            if np.sum(diffs < 0) >= 3:
                hist_weakening = True

    # 综合衰竭信号
    if rsi_exhaustion == "confirmed" and hist_weakening:
        exhaustion_signal = "confirmed"
    elif rsi_exhaustion == "confirmed" or rsi_exhaustion == "early":
        # A11 fix: RSI 有信号时才触发 "early" exhaustion
        # 单独 hist_weakening 不再触发（MACD 在强趋势中自然振荡，假信号频繁）
        exhaustion_signal = "early"
    elif hist_weakening:
        # A11 fix: 纯 histogram weakening 不触发 exhaustion
        # 记录到 exhaustion_signal 为 "hint" 供下游参考，但不触发 reversal_penalty
        pass  # exhaustion_signal remains "none"

    # ---------- 6. 动量状态 (RSI 区间) ----------
    momentum_state = "neutral"
    if rsi14 is not None:
        if rsi14 >= RSI_OVERBOUGHT:
            momentum_state = "overbought"
        elif rsi14 <= RSI_OVERSOLD:
            momentum_state = "oversold"
        elif RSI_BULL_ZONE_LOW <= rsi14 <= RSI_BULL_ZONE_HIGH:
            momentum_state = "bullish"
        elif RSI_BEAR_ZONE_LOW <= rsi14 <= RSI_BEAR_ZONE_HIGH:
            momentum_state = "bearish"
        elif RSI_NEUTRAL_LOW <= rsi14 <= RSI_NEUTRAL_HIGH:
            momentum_state = "neutral"

    # 提取 ADX 值
    adx_value = adx_data.get("adx14") if adx_data else None
    
    # 提取 trend_confidence (来自 market_structure 计算)
    trend_confidence = tf4h.get("trend_confidence")
    
    return {
        "available": True,
        "trend_direction": trend_direction,  # up/down/neutral/weakening/recovering (基于 EMA + 价格位置)
        "trend_strength": trend_strength,  # strong/moderate/choppy/unknown (基于 ADX)
        "trend_confidence": trend_confidence,  # 0.0-1.0 (来自 market_structure)
        "trend_quality": trend_quality,  # good/moderate/poor
        "continuation_prob": continuation_prob,  # high/medium/low
        "exhaustion_signal": exhaustion_signal,  # none/early/confirmed
        "momentum_state": momentum_state,  # bullish/bearish/neutral/overbought/oversold (基于 RSI)
        "di_direction": di_direction,  # bullish/bearish/neutral
        "ema_slope": ema_slope_dir,  # rising/falling/flat
        "price_vs_ema": price_vs_ema,  # above/below/between (价格相对 EMA 位置)
        # 原始指标值 (供 LLM feed 使用)
        "rsi": rsi14,  # RSI14 原始值
        "adx": adx_value,  # ADX14 原始值
        "adx_rising": adx_prev is not None and adx14 is not None and adx14 > adx_prev,  # ADX 是否上升
        "atr": atr,  # ATR 原始值
        "close": close,  # 收盘价 (用于计算 ATR%)
    }


# ==========================================================
# 1H 技术指标结论
# ==========================================================
def _build_1h_conclusions(tf1h: dict, tf15m: dict = None) -> dict:
    """
    1H 结论：波动状态、价格位置、结构状态、交易空间、压缩状态

    所需输入字段（斐波那契 EMA）：
    - close, atr, atr_sma50
    - ema (EMA_13, EMA_21, EMA_55)
    - vwap_d
    - donchian (upper, lower)
    - bbw, bbw_median
    """
    if not tf1h:
        return {"available": False, "reason": "no_1h_data"}

    close = tf1h.get("close", 0)
    atr = tf1h.get("atr")
    atr_sma50 = tf1h.get("atr_sma50")
    ema = tf1h.get("ema", {})

    # ---------- 1. 波动状态 (ATR vs ATR_SMA50) ----------
    volatility_state = "normal"
    if atr and atr_sma50 and atr_sma50 > 0:
        atr_ratio = atr / atr_sma50
        if atr_ratio > ATR_EXPANDING_RATIO:
            volatility_state = "expanding"
        elif atr_ratio < ATR_CONTRACTING_RATIO:
            volatility_state = "contracting"

    # ---------- 2. 价格位置 (VWAP 偏离) ----------
    vwap_d = tf1h.get("vwap_d")
    price_location = "value"

    if vwap_d and atr and atr > 0:
        vwap_deviation = (close - vwap_d) / atr
        if vwap_deviation > ATR_PRICE_LOCATION_K:
            price_location = "premium"
        elif vwap_deviation < -ATR_PRICE_LOCATION_K:
            price_location = "discount"

    # ---------- 3. 结构状态 (EMA 关系) ----------
    # 使用斐波那契 EMA：21（短期）和 55（中期）
    ema_fast = ema.get("EMA_21") or ema.get("EMA_20")  # 兼容旧配置
    ema_slow = ema.get("EMA_55") or ema.get("EMA_60")  # 兼容旧配置
    structure_state = "unknown"

    if ema_fast and ema_slow:
        if close > ema_fast and ema_fast > ema_slow:
            # 多头排列 + 价格在快速 EMA 之上 = 上涨冲动
            structure_state = "impulse_up"
        elif close < ema_fast and ema_fast < ema_slow:
            # 空头排列 + 价格在快速 EMA 之下 = 下跌冲动
            structure_state = "impulse_down"
        # V5-21 fix: 使用 <= 替代 < 消除 EMA 边界间隙
        # 旧逻辑：close == ema_slow 时不匹配任何条件 → 落入 consolidation
        # 新逻辑：close == ema_slow 归入 pullback（更保守的分类）
        elif ema_fast > ema_slow and ema_slow <= close <= ema_fast:
            # 多头排列 + 价格回落到两条 EMA 之间（含边界）= 上升趋势回调
            structure_state = "pullback_up"
        elif ema_fast < ema_slow and ema_fast <= close <= ema_slow:
            # 空头排列 + 价格反弹到两条 EMA 之间（含边界）= 下降趋势反弹
            structure_state = "pullback_down"
        # U10 fix: 新增趋势反转过渡状态
        elif ema_fast > ema_slow and close < ema_slow:
            # 多头排列但价格跌破两条 EMA → 上升趋势可能反转
            structure_state = "weakening_up"
        elif ema_fast < ema_slow and close > ema_slow:
            # 空头排列但价格突破两条 EMA → 下降趋势可能反转
            structure_state = "recovering_down"
        else:
            structure_state = "consolidation"

    # ---------- 4. 交易空间 (Donchian / ATR) ----------
    donchian = tf1h.get("donchian", {})
    donchian_upper = donchian.get("upper")
    donchian_lower = donchian.get("lower")
    trade_space = "unknown"
    # U12 fix: 新增方向性空间原始值，供评分函数按 bias 方向选择
    space_up_atr = None
    space_down_atr = None

    if donchian_upper and donchian_lower and atr and atr > 0:
        # 计算到目标位的空间
        space_up = (donchian_upper - close) / atr
        space_down = (close - donchian_lower) / atr
        max_space = max(space_up, space_down)
        space_up_atr = round(space_up, 2)
        space_down_atr = round(space_down, 2)

        if max_space >= TRADE_SPACE_WIDE:
            trade_space = "wide"
        elif max_space >= TRADE_SPACE_MODERATE:
            trade_space = "moderate"
        else:
            trade_space = "crowded"

    # ---------- 5. 压缩状态 (BBW) ----------
    bbw = tf1h.get("bbw")
    bbw_median = tf1h.get("bbw_median")
    consolidation = False

    if bbw and bbw_median and ema_fast and atr:
        # BBW 低于中位数 + close 靠近快速 EMA
        if bbw < bbw_median and abs(close - ema_fast) <= 0.5 * atr:
            consolidation = True

    # ---------- 6. 突破状态 (Donchian) ----------
    # U13 fix: 使用排除当前K线的 prev 值检测突破
    # 原逻辑用包含当前K线的 upper/lower，close <= high <= upper 永远不成立
    breakout_status = "none"
    donchian_upper_prev = donchian.get("upper_prev") or donchian_upper
    donchian_lower_prev = donchian.get("lower_prev") or donchian_lower
    if donchian_upper_prev and donchian_lower_prev:
        if close > donchian_upper_prev:
            breakout_status = "breakout_up"
        elif close < donchian_lower_prev:
            breakout_status = "breakdown"

    return {
        "available": True,
        "volatility_state": volatility_state,  # expanding/contracting/normal
        "price_location": price_location,  # premium/discount/value
        "structure_state": structure_state,  # impulse_up/impulse_down/pullback_up/pullback_down/weakening_up/recovering_down/consolidation
        "trade_space": trade_space,  # wide/moderate/crowded
        "space_up_atr": space_up_atr,  # U12: 做多方向空间（ATR 单位），None=不可用
        "space_down_atr": space_down_atr,  # U12: 做空方向空间（ATR 单位），None=不可用
        "consolidation": consolidation,  # True/False
        "breakout_status": breakout_status,  # breakout_up/breakdown/none
        # 原始指标值 (供 LLM feed 使用)
        "atr": atr,  # ATR 原始值
        "close": close,  # 收盘价 (用于计算 ATR%)
    }


# ==========================================================
# 15M 技术指标结论
# ==========================================================
def _build_15m_conclusions(tf15m: dict, tf1h: dict = None) -> dict:
    """
    15M 结论：关键位状态、成交量确认、OBV方向、微结构、拒绝强度

    所需输入字段：
    - close, atr, symbol
    - klines (带 v, tbv, tsv)
    - obv, obv_prev
    - 从 1H 读取: donchian, vwap_d, structure (swing points)
    - 从 15M 读取: structure (swing points)
    """
    if not tf15m:
        return {"available": False, "reason": "no_15m_data"}

    close = tf15m.get("close", 0)
    atr = tf15m.get("atr")
    klines = tf15m.get("klines", [])
    symbol = tf15m.get("symbol", "")

    # 根据币种获取动态阈值
    thresholds = get_key_level_thresholds(symbol)
    touch_k = thresholds["touch"]
    near_k = thresholds["near"]

    # 收集所有关键位（支撑和阻力都要）
    key_levels = []

    # 从 1H 获取关键位
    if tf1h:
        # Donchian 通道
        donchian = tf1h.get("donchian", {})
        if donchian.get("upper"):
            key_levels.append(("1h_donchian_upper", donchian["upper"], "resistance"))
        if donchian.get("lower"):
            key_levels.append(("1h_donchian_lower", donchian["lower"], "support"))

        # VWAP
        if tf1h.get("vwap_d"):
            key_levels.append(("1h_vwap", tf1h["vwap_d"], "pivot"))

        # 1H Swing Points
        s1h = tf1h.get("structure", {})
        if s1h.get("swing_high"):
            key_levels.append(("1h_swing_high", s1h["swing_high"], "resistance"))
        if s1h.get("swing_low"):
            key_levels.append(("1h_swing_low", s1h["swing_low"], "support"))

    # 从 15M 获取 swing points
    s15m = tf15m.get("structure", {})
    if s15m.get("swing_high"):
        key_levels.append(("15m_swing_high", s15m["swing_high"], "resistance"))
    if s15m.get("swing_low"):
        key_levels.append(("15m_swing_low", s15m["swing_low"], "support"))

    # ---------- 1. 关键位状态 ----------
    key_level_status = "none"
    nearest_level = None
    nearest_distance = None
    nearest_type = None  # support/resistance/pivot

    if atr and atr > 0 and key_levels:
        # 按距离排序，确保找到最近的关键位
        sorted_levels = sorted(
            [(name, price, ltype, abs(close - price) / atr) for name, price, ltype in key_levels],
            key=lambda x: x[3]  # 按距离排序
        )
        
        for level_name, level_price, level_type, distance in sorted_levels:
            if distance <= touch_k:
                key_level_status = f"touching_{level_name}"
                nearest_level = level_name
                nearest_distance = distance
                nearest_type = level_type
                break
            elif distance <= near_k:
                key_level_status = f"near_{level_name}"
                nearest_level = level_name
                nearest_distance = distance
                nearest_type = level_type
                break  # 已排序，第一个满足条件的就是最近的

    # ---------- 2. 成交量确认 ----------
    volume_confirmation = "unknown"

    # U17: klines 现在有 20 根（由 pack_klines 打包），均量样本更可靠
    if klines and len(klines) >= 3:
        volumes = [k.get("v", 0) for k in klines]
        # 计算历史均量（排除最后一根），需要至少2根K线才能计算
        avg_volume = sum(volumes[:-1]) / len(volumes[:-1]) if len(volumes) > 1 else volumes[0] if volumes else 1
        current_volume = volumes[-1] if volumes else 0

        if avg_volume > 0:
            vol_ratio = current_volume / avg_volume
            if vol_ratio >= VOLUME_STRONG_RATIO:
                volume_confirmation = "strong"
            elif vol_ratio >= VOLUME_MODERATE_RATIO:
                volume_confirmation = "moderate"
            else:
                volume_confirmation = "weak"

    # ---------- 3. OBV 方向 ----------
    obv_direction = "unknown"
    obv = tf15m.get("obv")
    obv_prev = tf15m.get("obv_prev")  # 5 根前的 OBV

    if obv is not None and obv_prev is not None:
        obv_rising = obv > obv_prev

        # 判断价格方向（最近 5 根）
        if klines and len(klines) >= 5:
            price_rising = klines[-1].get("c", 0) > klines[-5].get("c", 0)

            if obv_rising == price_rising:
                obv_direction = "confirming"
            else:
                obv_direction = "diverging"

    # ---------- 4. 微结构 ----------
    # U17 fix: 扩展窗口到 12 根（3小时），使用多段比较减少噪音
    # 将 12 根分为 3 段（每段 4 根），比较段间 high/low 趋势
    micro_structure = "unclear"

    if klines and len(klines) >= 12:
        recent = klines[-12:]
        # 分 3 段：[0:4], [4:8], [8:12]
        seg1_highs = [k.get("h", 0) for k in recent[0:4]]
        seg2_highs = [k.get("h", 0) for k in recent[4:8]]
        seg3_highs = [k.get("h", 0) for k in recent[8:12]]
        seg1_lows = [k.get("l", 0) for k in recent[0:4]]
        seg2_lows = [k.get("l", 0) for k in recent[4:8]]
        seg3_lows = [k.get("l", 0) for k in recent[8:12]]

        # 每段取最高/最低
        h1, h2, h3 = max(seg1_highs), max(seg2_highs), max(seg3_highs)
        l1, l2, l3 = min(seg1_lows), min(seg2_lows), min(seg3_lows)

        # Higher highs & higher lows（两段连续递增）
        hh = h3 > h2 and h2 > h1
        hl = l3 > l2 and l2 > l1

        # Lower highs & lower lows（两段连续递减）
        lh = h3 < h2 and h2 < h1
        ll = l3 < l2 and l2 < l1

        if hh and hl:
            micro_structure = "higher_highs_lows"
        elif lh and ll:
            micro_structure = "lower_highs_lows"
        elif hl and not hh:
            micro_structure = "higher_lows"
        elif lh and not ll:
            micro_structure = "lower_highs"
        else:
            # 检查压缩/扩张：比较最近段 vs 最早段的 range
            seg3_ranges = [k.get("h", 0) - k.get("l", 0) for k in recent[8:12]]
            seg1_ranges = [k.get("h", 0) - k.get("l", 0) for k in recent[0:4]]

            avg_recent = sum(seg3_ranges) / len(seg3_ranges)
            avg_prev = sum(seg1_ranges) / len(seg1_ranges) if seg1_ranges else 1

            if avg_prev > 0:
                if avg_recent < avg_prev * 0.7:
                    micro_structure = "compression"
                elif avg_recent > avg_prev * 1.3:
                    micro_structure = "expansion"

    elif klines and len(klines) >= 5:
        # 回退：数据不足 12 根时用原始 5 根逻辑
        recent = klines[-5:]
        highs = [k.get("h", 0) for k in recent]
        lows = [k.get("l", 0) for k in recent]

        hh = highs[-1] > highs[-3] and highs[-2] > highs[-4]
        hl = lows[-1] > lows[-3] and lows[-2] > lows[-4]
        lh = highs[-1] < highs[-3] and highs[-2] < highs[-4]
        ll = lows[-1] < lows[-3] and lows[-2] < lows[-4]

        if hh and hl:
            micro_structure = "higher_highs_lows"
        elif lh and ll:
            micro_structure = "lower_highs_lows"
        elif hl and not hh:
            micro_structure = "higher_lows"
        elif lh and not ll:
            micro_structure = "lower_highs"
        else:
            recent_ranges = [k.get("h", 0) - k.get("l", 0) for k in recent[-2:]]
            prev_ranges = [k.get("h", 0) - k.get("l", 0) for k in recent[:2]]
            avg_recent = sum(recent_ranges) / 2 if recent_ranges else 0
            avg_prev = sum(prev_ranges) / 2 if prev_ranges else 1
            if avg_prev > 0:
                if avg_recent < avg_prev * 0.7:
                    micro_structure = "compression"
                elif avg_recent > avg_prev * 1.3:
                    micro_structure = "expansion"

    # ---------- 5. 拒绝强度 (Wick 分析) ----------
    rejection_strength = "none"
    rejection_direction = None

    if klines and atr and atr > 0:
        last_k = klines[-1]
        o, h, l, c = last_k.get("o", 0), last_k.get("h", 0), last_k.get("l", 0), last_k.get("c", 0)

        upper_wick = h - max(o, c)
        lower_wick = min(o, c) - l

        # 比较上下影线，取更显著的那个
        # 如果两者都很长且接近（十字星），标记为 doji
        if upper_wick >= ATR_WEAK_REJECTION_K * atr and lower_wick >= ATR_WEAK_REJECTION_K * atr:
            # 两边都有显著影线，检查是否为十字星
            # P1 Fix: 使用大有限数替代 float('inf')，避免 JSON 序列化问题
            wick_ratio = upper_wick / lower_wick if lower_wick > 0 else 999.0
            if 0.5 <= wick_ratio <= 2.0:
                # 上下影线接近，视为十字星（方向不明）
                rejection_strength = "weak"
                rejection_direction = "doji"
            elif upper_wick > lower_wick:
                # 上影线更长
                rejection_strength = "strong" if upper_wick >= ATR_STRONG_REJECTION_K * atr else "weak"
                rejection_direction = "bearish"
            else:
                # 下影线更长
                rejection_strength = "strong" if lower_wick >= ATR_STRONG_REJECTION_K * atr else "weak"
                rejection_direction = "bullish"
        elif upper_wick >= ATR_STRONG_REJECTION_K * atr:
            rejection_strength = "strong"
            rejection_direction = "bearish"
        elif upper_wick >= ATR_WEAK_REJECTION_K * atr:
            rejection_strength = "weak"
            rejection_direction = "bearish"
        elif lower_wick >= ATR_STRONG_REJECTION_K * atr:
            rejection_strength = "strong"
            rejection_direction = "bullish"
        elif lower_wick >= ATR_WEAK_REJECTION_K * atr:
            rejection_strength = "weak"
            rejection_direction = "bullish"

    return {
        "available": True,
        "key_level_status": key_level_status,  # touching_*/near_*/none
        "key_level_type": nearest_type,  # support/resistance/pivot/None
        "volume_confirmation": volume_confirmation,  # strong/moderate/weak/unknown
        "obv_direction": obv_direction,  # confirming/diverging/unknown
        "micro_structure": micro_structure,
        # higher_highs_lows/lower_highs_lows/higher_lows/lower_highs/compression/expansion/unclear
        "rejection_strength": rejection_strength,  # strong/weak/none
        "rejection_direction": rejection_direction,  # bullish/bearish/None
    }


# ==========================================================
# 综合偏向判断（乘法模型重构）
# ==========================================================
def calculate_overall_bias(conclusions: dict) -> dict:
    """
    基于三个周期的结论，使用乘法模型计算综合偏向评分

    架构：bias_score = base_score × quality_mult × environment_mult × timing_mult - reversal_penalty
    范围：-10 ~ +10，threshold=6 表示需要 60% 因子对齐才推荐开仓

    Layer 1 (base_score):       4H 方向性基础分 (-5 ~ +5)
    Layer 2 (quality_mult):     4H 趋势质量系数 (0.4 ~ 1.2)
    Layer 3 (environment_mult): 1H 环境系数 (0.5 ~ 1.3)
    Layer 4 (timing_mult):      15M 时机系数 (0.6 ~ 1.3)
    Layer 5 (reversal_penalty): 反转风险扣减 (0 ~ 3)

    返回:
        {
            "bias_direction": "bullish" / "bearish" / "neutral",
            "bias_strength": "strong" / "moderate" / "weak",
            "bias_score": int,          # -10 ~ +10
            "bias_factors": [...],      # 每个因子的贡献记录
            "reversal_risk": "high" / "medium" / "low",
            "reversal_score": float,
            "reversal_factors": [...],
            "trend_conflict": true/false,
            "trade_suggestion": "..." / null,
        }
    """
    tf4h = conclusions.get("tf4h", {})
    tf1h = conclusions.get("tf1h", {})
    tf15m = conclusions.get("tf15m", {})

    if not tf4h.get("available"):
        return {"available": False, "reason": "no_4h_data"}

    bias_factors = []

    # ==========================================================
    # Step 0: 提取各周期信号
    # ==========================================================
    trend_4h = tf4h.get("trend_direction", "neutral")
    trend_strength = tf4h.get("trend_strength", "weak")
    trend_quality = tf4h.get("trend_quality", "poor")
    continuation_prob = tf4h.get("continuation_prob", "low")
    exhaustion = tf4h.get("exhaustion_signal", "none")
    momentum = tf4h.get("momentum_state", "neutral")
    di_dir = tf4h.get("di_direction", "neutral")

    structure_1h = tf1h.get("structure_state", "unknown")
    price_loc = tf1h.get("price_location", "value")
    vol_state = tf1h.get("volatility_state", "normal")
    trade_space = tf1h.get("trade_space", "moderate")
    space_up = tf1h.get("space_up_atr")
    space_down = tf1h.get("space_down_atr")
    consolidation = tf1h.get("consolidation", False)
    breakout = tf1h.get("breakout_status", "none")

    micro = tf15m.get("micro_structure", "unclear")
    vol_confirm = tf15m.get("volume_confirmation", "unknown")
    obv_dir = tf15m.get("obv_direction", "unknown")
    key_status = tf15m.get("key_level_status", "none")
    key_type = tf15m.get("key_level_type")
    rejection = tf15m.get("rejection_strength", "none")
    rej_dir = tf15m.get("rejection_direction")

    # 推断各周期方向
    # V5-20 fix: 使用 == 替代 in 避免子串误匹配风险
    # 当前 structure_state 值集合中无子串关系，但 in 是潜在维护隐患
    if structure_1h in ("impulse_up", "pullback_up"):
        trend_1h = "up"
    elif structure_1h in ("impulse_down", "pullback_down"):
        trend_1h = "down"
    # A13 fix: weakening_up/recovering_down 不应当 neutral 处理
    elif structure_1h == "weakening_up":
        trend_1h = "down"  # 价格行为看跌（虽然 EMA 结构仍看涨）
    elif structure_1h == "recovering_down":
        trend_1h = "up"  # 价格行为看涨（虽然 EMA 结构仍看跌）
    else:
        trend_1h = "neutral"

    if micro in ("higher_highs_lows", "higher_lows"):
        trend_15m = "up"
    elif micro in ("lower_highs_lows", "lower_highs"):
        trend_15m = "down"
    else:
        trend_15m = "neutral"

    # 4H base_trend（weakening→up, recovering→down）
    if trend_4h in ("up", "weakening"):
        base_trend_4h = "up"
    elif trend_4h in ("down", "recovering"):
        base_trend_4h = "down"
    else:
        base_trend_4h = "neutral"

    # ==========================================================
    # Step 1: 检测趋势冲突
    # ==========================================================
    trend_conflict = False
    trade_suggestion = None

    # H3 fix: 使用 base_trend_4h 替代 trend_4h 做冲突检测
    # 旧代码只检查 "down"/"up"，遗漏了 weakening（base=up）和 recovering（base=down）
    # 过渡状态恰恰是冲突最需要被标记的时候
    if base_trend_4h == "down" and (trend_1h == "up" or trend_15m == "up"):
        trend_conflict = True
        trade_suggestion = "bounce_in_downtrend_no_short"
    elif base_trend_4h == "up" and (trend_1h == "down" or trend_15m == "down"):
        trend_conflict = True
        trade_suggestion = "pullback_in_uptrend_wait_support_long"

    # ==========================================================
    # Layer 1: base_score（4H 方向性基础分，-5 ~ +5）
    # ==========================================================
    base_score = 0.0

    # 4H 趋势方向（核心权重）
    if trend_4h == "up":
        base_score += 3.0
        bias_factors.append("4h_trend_up(+3)")
    elif trend_4h == "down":
        base_score -= 3.0
        bias_factors.append("4h_trend_down(-3)")
    elif trend_4h == "weakening":
        base_score += 1.5
        bias_factors.append("4h_weakening(+1.5)")
    elif trend_4h == "recovering":
        base_score -= 1.5
        bias_factors.append("4h_recovering(-1.5)")

    # DI 方向确认
    if di_dir == "bullish" and base_trend_4h == "up":
        base_score += 1.0
        bias_factors.append("4h_di_confirm(+1)")
    elif di_dir == "bearish" and base_trend_4h == "down":
        base_score -= 1.0
        bias_factors.append("4h_di_confirm(-1)")
    elif di_dir == "bullish" and base_trend_4h == "down":
        base_score += 0.5
        bias_factors.append("4h_di_diverge(+0.5)")
    elif di_dir == "bearish" and base_trend_4h == "up":
        base_score -= 0.5
        bias_factors.append("4h_di_diverge(-0.5)")

    # RSI 动量
    # B5 fix: overbought/oversold 不再加分到 base_score
    # 原逻辑: overbought 同时 +1 base_score 和 +1 reversal_penalty，高乘数时净效果仍为正
    # 修复: overbought/oversold 是衰竭信号，由 Layer 5 reversal_penalty 处理
    # 只有健康的 bullish/bearish 动量才加分
    if momentum == "bullish":
        base_score += 1.0
        bias_factors.append("4h_momentum_bullish(+1)")
    elif momentum == "bearish":
        base_score -= 1.0
        bias_factors.append("4h_momentum_bearish(-1)")
    elif momentum in ("overbought", "oversold"):
        # B5: 不加分，让 reversal_penalty 独立处理
        bias_factors.append(f"4h_momentum_{momentum}(0,handled_by_reversal)")

    # clamp base_score
    base_score = max(-5.0, min(5.0, base_score))

    # H2+A6+B1+B4 fix: 当 base_score 接近零时注入种子分
    # 解决乘法模型的结构性缺陷：
    # - 原 H2: 只在 base_score==0 时触发，但 DI/momentum 可产生 ±0.5/±1.0 的非零小值
    # - A6: 4H neutral + DI bullish(+0.5) → base_score=0.5, seed 不触发, 1H impulse 被忽略
    # - B1: 4H weakening(+1.5) + DI diverge(-0.5) + bearish momentum(-1.0) = 0, 过渡状态死区
    # - B4: base_score=0.5 比 base_score=0 得分更低（非单调不连续）
    # 修复: 扩展到 abs(base_score) < 1.5，覆盖所有过渡/弱信号状态
    # 种子使用 max/min 确保不降低已有信号（解决 B4 非单调问题）
    if abs(base_score) < 1.5 and structure_1h in (
        "impulse_up", "impulse_down", "weakening_up", "recovering_down"
    ):
        if structure_1h in ("impulse_up", "recovering_down"):
            # 1H 看涨信号：impulse_up 或 recovering_down（空头排列但价格突破）
            base_score = max(base_score, 1.5)
            bias_factors.append(f"1h_{structure_1h}_seed(→+1.5)")
        elif structure_1h in ("impulse_down", "weakening_up"):
            # 1H 看跌信号：impulse_down 或 weakening_up（多头排列但价格跌破）
            base_score = min(base_score, -1.5)
            bias_factors.append(f"1h_{structure_1h}_seed(→-1.5)")

    # Pullback seed fix: 当 base_score 接近零时，1H pullback 也应注入种子分
    # 问题：4H neutral + 1H pullback_up/down → base_score=0 → 乘法模型输出永远为 0
    # pullback 是趋势跟踪的核心入场策略，不应因 4H 无方向而完全忽略
    # 种子分 1.0（比 impulse 的 1.5 弱，反映 pullback 信号较弱的特性）
    # 同样使用 max/min 确保不降低已有信号
    if abs(base_score) < 1.0 and structure_1h in ("pullback_up", "pullback_down"):
        if structure_1h == "pullback_up":
            base_score = max(base_score, 1.0)
            bias_factors.append("1h_pullback_up_seed(→+1.0)")
        elif structure_1h == "pullback_down":
            base_score = min(base_score, -1.0)
            bias_factors.append("1h_pullback_down_seed(→-1.0)")

    # ==========================================================
    # Layer 2: quality_mult（4H 趋势质量系数，0.4 ~ 1.2）
    # ==========================================================
    quality_mult = 1.0

    if trend_quality == "good" and trend_strength == "strong" and continuation_prob == "high":
        quality_mult = 1.2
        bias_factors.append("quality_excellent(x1.2)")
    elif trend_quality == "good" and trend_strength in ("strong", "moderate"):
        quality_mult = 1.1
        bias_factors.append("quality_good(x1.1)")
    elif trend_quality == "moderate":
        quality_mult = 1.0
        # 不记录，默认值
    # M1+M6 fix: "unknown" (无 ADX 数据) 最不确定，应先检查
    # 旧代码中 "weak" 分支被 "poor" 先匹配，永远无法执行
    elif trend_strength == "unknown":
        quality_mult = 0.5
        bias_factors.append("quality_no_adx(x0.5)")
    elif trend_quality == "poor" or trend_strength == "choppy":
        quality_mult = 0.7
        bias_factors.append("quality_poor(x0.7)")

    # 趋势冲突额外惩罚
    if trend_conflict:
        quality_mult *= 0.8
        bias_factors.append("trend_conflict(x0.8)")

    # V5 fix: clamp 下限从 0.5 降到 0.4，确保 unknown+conflict 时惩罚生效
    # 旧值 0.5 导致 unknown(0.5) × conflict(0.8) = 0.4 被 clamp 回 0.5，惩罚无效
    quality_mult = max(0.4, min(1.2, quality_mult))

    # ==========================================================
    # Layer 3: environment_mult（1H 环境系数，0.5 ~ 1.3）
    # ==========================================================
    env_mult = 1.0

    # 3a. 结构状态与 bias 方向的一致性
    bias_is_bullish = base_score > 0
    bias_is_bearish = base_score < 0

    if structure_1h == "impulse_up" and bias_is_bullish:
        env_mult *= 1.2
        bias_factors.append("1h_impulse_aligned(x1.2)")
    elif structure_1h == "impulse_down" and bias_is_bearish:
        env_mult *= 1.2
        bias_factors.append("1h_impulse_aligned(x1.2)")
    elif structure_1h == "impulse_up" and bias_is_bearish:
        env_mult *= 0.7
        bias_factors.append("1h_impulse_against(x0.7)")
    elif structure_1h == "impulse_down" and bias_is_bullish:
        env_mult *= 0.7
        bias_factors.append("1h_impulse_against(x0.7)")
    elif structure_1h in ("weakening_up", "recovering_down"):
        env_mult *= 0.85
        bias_factors.append("1h_transitional(x0.85)")
    # B2 fix: pullback 在趋势方向上应有小幅加成
    # pullback entry 是趋势跟踪的核心策略，但之前 env_mult=1.0 导致好的 pullback setup
    # 只能得 5 分（4H up + DI confirm + 1H pullback + 15M aligned），永远不推荐开仓
    elif structure_1h == "pullback_up" and bias_is_bullish:
        env_mult *= 1.05
        bias_factors.append("1h_pullback_aligned(x1.05)")
    elif structure_1h == "pullback_down" and bias_is_bearish:
        env_mult *= 1.05
        bias_factors.append("1h_pullback_aligned(x1.05)")
    # consolidation/unknown → env_mult 不变

    # 3b. 突破确认
    if breakout == "breakout_up" and bias_is_bullish:
        env_mult *= 1.1
        bias_factors.append("1h_breakout_confirm(x1.1)")
    elif breakout == "breakdown" and bias_is_bearish:
        env_mult *= 1.1
        bias_factors.append("1h_breakdown_confirm(x1.1)")
    elif breakout == "breakout_up" and bias_is_bearish:
        env_mult *= 0.85
        bias_factors.append("1h_breakout_against(x0.85)")
    elif breakout == "breakdown" and bias_is_bullish:
        env_mult *= 0.85
        bias_factors.append("1h_breakdown_against(x0.85)")

    # 3c. 方向性交易空间
    if bias_is_bullish and space_up is not None:
        if space_up < 1.0:
            env_mult *= 0.8
            bias_factors.append(f"1h_space_up_crowded({space_up}ATR,x0.8)")
    elif bias_is_bearish and space_down is not None:
        if space_down < 1.0:
            env_mult *= 0.8
            bias_factors.append(f"1h_space_down_crowded({space_down}ATR,x0.8)")

    # 3d. 压缩状态
    if consolidation:
        env_mult *= 0.9
        bias_factors.append("1h_squeeze(x0.9)")

    # 3e. 波动过度延伸
    if vol_state == "expanding" and price_loc in ("premium", "discount"):
        env_mult *= 0.85
        bias_factors.append("1h_overextended(x0.85)")

    env_mult = max(0.5, min(1.3, env_mult))

    # ==========================================================
    # Layer 4: timing_mult（15M 时机系数，0.6 ~ 1.3）
    # ==========================================================
    timing_mult = 1.0

    # 4a. 微结构方向
    if trend_15m == "up" and bias_is_bullish:
        timing_mult *= 1.1
        bias_factors.append("15m_structure_aligned(x1.1)")
    elif trend_15m == "down" and bias_is_bearish:
        timing_mult *= 1.1
        bias_factors.append("15m_structure_aligned(x1.1)")
    elif trend_15m == "up" and bias_is_bearish:
        timing_mult *= 0.8
        bias_factors.append("15m_structure_against(x0.8)")
    elif trend_15m == "down" and bias_is_bullish:
        timing_mult *= 0.8
        bias_factors.append("15m_structure_against(x0.8)")

    # 4b. 量价确认
    if vol_confirm == "strong" and obv_dir == "confirming":
        timing_mult *= 1.15
        bias_factors.append("15m_vol_obv_confirm(x1.15)")
    elif vol_confirm == "strong":
        timing_mult *= 1.05
        bias_factors.append("15m_vol_strong(x1.05)")
    # A10 fix: OBV 背离比成交量弱更严重，分开处理
    # OBV diverging = 量能主动反对价格方向，是更强的反向信号
    # volume weak = 仅仅是成交量低，没有方向性含义
    elif obv_dir == "diverging":
        timing_mult *= 0.75
        bias_factors.append("15m_obv_diverging(x0.75)")
    elif vol_confirm == "weak":
        timing_mult *= 0.9
        bias_factors.append("15m_vol_weak(x0.9)")

    # 4c. 拒绝信号
    if rejection == "strong":
        if (rej_dir == "bullish" and bias_is_bullish) or \
                (rej_dir == "bearish" and bias_is_bearish):
            timing_mult *= 1.1
            bias_factors.append("15m_rejection_confirm(x1.1)")
        elif (rej_dir == "bearish" and bias_is_bullish) or \
                (rej_dir == "bullish" and bias_is_bearish):
            timing_mult *= 0.8
            bias_factors.append("15m_rejection_against(x0.8)")

    # 4d. 关键位交互
    is_touching = "touching_" in key_status
    if is_touching and key_type == "support" and bias_is_bullish:
        timing_mult *= 1.1
        bias_factors.append("15m_at_support_long(x1.1)")
    elif is_touching and key_type == "resistance" and bias_is_bearish:
        timing_mult *= 1.1
        bias_factors.append("15m_at_resistance_short(x1.1)")
    elif is_touching and key_type == "resistance" and bias_is_bullish:
        timing_mult *= 0.85
        bias_factors.append("15m_at_resistance_long(x0.85)")
    elif is_touching and key_type == "support" and bias_is_bearish:
        timing_mult *= 0.85
        bias_factors.append("15m_at_support_short(x0.85)")

    timing_mult = max(0.6, min(1.3, timing_mult))

    # ==========================================================
    # Layer 5: reversal_penalty（反转风险扣减，0 ~ 3）
    # ==========================================================
    reversal_penalty = 0.0
    reversal_factors = []

    # 衰竭信号
    if exhaustion == "confirmed":
        reversal_penalty += 2.0
        reversal_factors.append("confirmed_exhaustion(-2)")
    elif exhaustion == "early":
        reversal_penalty += 1.0
        reversal_factors.append("early_exhaustion(-1)")

    # 波动扩张 + 价格极端
    if vol_state == "expanding" and price_loc in ("premium", "discount"):
        reversal_penalty += 0.5
        reversal_factors.append("expanding_stretched(-0.5)")

    # 交易空间拥挤
    if trade_space == "crowded":
        reversal_penalty += 0.5
        reversal_factors.append("crowded_space(-0.5)")

    # M2 fix: 移除 rejection 的 reversal_penalty
    # rejection 已在 Layer 4 timing_mult 中处理（x0.8），不应双重惩罚
    # 保留注释说明设计决策
    # (rejection penalty removed — already handled by timing_mult *= 0.8 in Layer 4)

    reversal_penalty = min(3.0, reversal_penalty)

    # 反转风险等级
    if reversal_penalty >= 2.5:
        reversal_risk = "high"
    elif reversal_penalty >= 1.5:
        reversal_risk = "medium"
    else:
        reversal_risk = "low"

    # ==========================================================
    # 最终计算：乘法模型
    # ==========================================================
    raw_score = base_score * quality_mult * env_mult * timing_mult

    # A8/B3 fix: 反转惩罚改为混合模式（比例 + 固定）
    # 原逻辑: 平坦减法 raw_score - penalty，对弱信号影响过大，对强信号几乎无效
    # 例: raw=2.5, penalty=2.0 → 0.5 (80%削减); raw=8.0, penalty=2.0 → 6.0 (25%削减)
    # 修复: 使用衰减因子，penalty 0~3 映射到 decay 1.0~0.3
    # 效果: raw=2.5, penalty=2.0 → 2.5*0.6=1.5; raw=8.0, penalty=2.0 → 8.0*0.6=4.8
    # 强趋势的 exhaustion 现在也有显著影响，弱趋势不会被完全归零
    reversal_decay = 1.0
    if reversal_penalty > 0 and raw_score != 0:
        # decay: penalty=0→1.0, penalty=1→0.77, penalty=2→0.53, penalty=3→0.3
        reversal_decay = max(0.3, 1.0 - reversal_penalty * 0.233)
        # V5-19 fix: 移除冗余 if/else 分支（两个分支执行相同操作）
        raw_score *= reversal_decay

    # clamp 到 [-10, +10] 并取整
    bias_score = int(round(max(-10.0, min(10.0, raw_score))))

    # ==========================================================
    # 方向和强度映射（适配 -10 ~ +10 范围）
    # ==========================================================
    if bias_score >= 7:
        bias_direction = "bullish"
        bias_strength = "strong"
    elif bias_score >= 4:
        bias_direction = "bullish"
        bias_strength = "moderate"
    elif bias_score >= 1:
        bias_direction = "bullish"
        bias_strength = "weak"
    elif bias_score <= -7:
        bias_direction = "bearish"
        bias_strength = "strong"
    elif bias_score <= -4:
        bias_direction = "bearish"
        bias_strength = "moderate"
    elif bias_score <= -1:
        bias_direction = "bearish"
        bias_strength = "weak"
    else:
        bias_direction = "neutral"
        bias_strength = "none"

    return {
        "available": True,
        "bias_direction": bias_direction,
        "bias_strength": bias_strength,
        "bias_score": bias_score,
        "bias_factors": bias_factors,
        "reversal_risk": reversal_risk,
        "reversal_score": reversal_penalty,
        "reversal_factors": reversal_factors,
        "trend_conflict": trend_conflict,
        "trade_suggestion": trade_suggestion,
        # 中间层分数 — 让 LLM 理解 bias 是怎么来的
        "layer_scores": {
            "base_score": round(base_score, 2),
            "quality_mult": round(quality_mult, 2),
            "env_mult": round(env_mult, 2),
            "timing_mult": round(timing_mult, 2),
            "reversal_decay": round(reversal_decay, 2),
        },
    }


# ==========================================================
# Phase 1a: 统一入口 build_symbol_analysis（实现在 assembler 避免循环引用）
# ==========================================================
def build_symbol_analysis(symbol: str, tf4h: dict, tf1h: dict, tf15m: dict, **kwargs):
    """
    完整的上游分析，输出 SymbolAnalysis 数据契约。
    实现见 analysis.symbol_analysis_assembler.build_symbol_analysis
    """
    from analysis.assembly.symbol_analysis_assembler import build_symbol_analysis as _assemble
    return _assemble(symbol, tf4h, tf1h, tf15m, **kwargs)
