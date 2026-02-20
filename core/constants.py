# constants.py
"""
全局共享常量定义
"""

# 止损/止盈订单类型
TP_SL_TYPES = {
    "sl": ["STOP", "STOP_MARKET"],
    "tp": ["TAKE_PROFIT", "TAKE_PROFIT_MARKET"]
}

# 所有 TP/SL 类型（平铺）
ALL_TP_SL_TYPES = {"STOP", "STOP_MARKET", "TAKE_PROFIT", "TAKE_PROFIT_MARKET"}

# 活跃订单状态
ACTIVE_ORDER_STATUSES = {"NEW", "PARTIALLY_FILLED"}


def decimal_safe(x):
    """安全的 Decimal 转换"""
    from decimal import Decimal
    if x is None or x == "":
        return Decimal("0")
    return Decimal(str(x))
