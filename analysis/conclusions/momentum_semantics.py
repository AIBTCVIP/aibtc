# conclusions/momentum_semantics.py - 动量语义解释（reversing/recovering → direction/interpretation）
"""
根据 trend_4h/trend_1h 将 momentum 的 reversing/recovering 等解释为 direction/strength/interpretation。
归属 analysis.conclusions，供 assembly 或 llm 使用。
"""
from typing import Dict, Any


def enhance_momentum_semantics(technicals: dict) -> Dict[str, Any]:
    """
    Enhance momentum field with clear directional semantics.
    Converts ambiguous 'reversing' / 'recovering' to explicit direction and interpretation.
    """
    momentum = technicals.get("momentum")
    if not momentum:
        return {}

    trend_4h = technicals.get("trend_4h")
    trend_1h = technicals.get("trend_1h")

    result = {"raw": momentum}

    if momentum == "reversing":
        if trend_4h == "down" or trend_1h in ("impulse_down", "down"):
            result["direction"] = "bearish"
            result["strength"] = "weakening"
            result["interpretation"] = "bearish_momentum_losing_steam"
        elif trend_4h == "up" or trend_1h in ("impulse_up", "up"):
            result["direction"] = "bullish"
            result["strength"] = "weakening"
            result["interpretation"] = "bullish_momentum_losing_steam"
        else:
            result["direction"] = "unclear"
            result["interpretation"] = "momentum_direction_unclear"
    elif momentum == "recovering":
        if trend_4h == "down" or trend_1h in ("impulse_down", "down"):
            result["direction"] = "bearish"
            result["strength"] = "strengthening"
            result["interpretation"] = "bearish_momentum_recovering"
        elif trend_4h == "up" or trend_1h in ("impulse_up", "up"):
            result["direction"] = "bullish"
            result["strength"] = "strengthening"
            result["interpretation"] = "bullish_momentum_recovering"
        else:
            result["direction"] = "bullish"
            result["strength"] = "strengthening"
            result["interpretation"] = "momentum_recovering"
    elif momentum in ("strong_bullish", "bullish"):
        result["direction"] = "bullish"
        result["strength"] = "strong" if "strong" in momentum else "moderate"
        result["interpretation"] = momentum
    elif momentum in ("strong_bearish", "bearish", "weakening"):
        result["direction"] = "bearish"
        if "strong" in momentum:
            result["strength"] = "strong"
        elif momentum == "weakening":
            result["strength"] = "strengthening"
        else:
            result["strength"] = "moderate"
        result["interpretation"] = momentum
    else:
        result["direction"] = "neutral"
        result["interpretation"] = momentum

    return result
