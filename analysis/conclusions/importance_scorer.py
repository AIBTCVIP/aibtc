# conclusions/importance_scorer.py - 由 bias/pattern 推导 importance，供投喂层使用
"""
由 symbol_analysis 或 ai_enhancement 推导 importance (1-5) 与 importance_label。
归属 analysis.conclusions，供 llm 只读并写入 _quick。
"""
from typing import Dict, Any


IMPORTANCE_LABELS = {1: "low", 2: "below_avg", 3: "medium", 4: "above_avg", 5: "high"}


def compute_importance(market_data: dict) -> Dict[str, Any]:
    """
    从 market_data 中的 symbol_analysis 或 ai_enhancement 计算 importance。

    Args:
        market_data: 需含 symbol_analysis (dict) 或 ai_enhancement。可含已有 _quick（会被合并）。

    Returns:
        {"importance": 1-5, "importance_label": str}。caller 可写入 market_data["_quick"]。
    """
    if not market_data:
        return {"importance": 3, "importance_label": "medium"}

    bias_score = 0
    trend_conflict = False
    pattern_confidence = "very_low"
    pattern_win_rate = 0

    sa = market_data.get("symbol_analysis")
    if isinstance(sa, dict):
        bias_mod = sa.get("bias") or {}
        bias_score = bias_mod.get("score") or 0
        trend_conflict = bias_mod.get("trend_conflict", False)
        pat = sa.get("pattern") or {}
        pattern_confidence = pat.get("confidence") or "very_low"
        if isinstance(pattern_confidence, (int, float)):
            pattern_confidence = "high" if pattern_confidence >= 0.7 else "medium" if pattern_confidence >= 0.4 else "very_low"
        pattern_win_rate = pat.get("win_rate") or 0
        if pattern_win_rate and pattern_win_rate <= 1:
            pattern_win_rate = pattern_win_rate * 100
    else:
        ai_enh = market_data.get("ai_enhancement") or {}
        overall_bias = ai_enh.get("overall_bias") or {}
        bias_score = overall_bias.get("bias_score") or 0
        trend_conflict = overall_bias.get("trend_conflict", False)
        pattern = ai_enh.get("pattern_analysis") or {}
        pattern_confidence = pattern.get("sample_reliability", "very_low")
        pattern_win_rate = pattern.get("historical_win_rate") or 0

    importance = 3
    if abs(bias_score) >= 5:
        importance += 1
    if abs(bias_score) >= 7:
        importance += 1
    if trend_conflict:
        importance -= 1
    if pattern_confidence in ("high", "medium") and pattern_win_rate >= 50:
        importance += 1
    elif pattern_confidence == "very_low":
        importance -= 1
    importance = max(1, min(5, importance))

    return {
        "importance": importance,
        "importance_label": IMPORTANCE_LABELS.get(importance, "medium"),
    }
