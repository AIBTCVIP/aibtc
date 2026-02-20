# core/utils.py
"""
共享工具函数模块

提供项目中常用的工具函数，避免代码重复
"""

import json
import time
from decimal import Decimal, getcontext
from typing import Any, Dict

# 设置 Decimal 精度
getcontext().prec = 28


def D(x: Any) -> Decimal:
    """
    安全的 Decimal 转换
    
    Args:
        x: 要转换的值，可以是 None、空字符串、数字或字符串
        
    Returns:
        Decimal 值，无效输入返回 Decimal("0")
    """
    if x is None or x == "":
        return Decimal("0")
    if isinstance(x, Decimal):
        return x
    try:
        return Decimal(str(x))
    except Exception:
        return Decimal("0")


def now_ms() -> int:
    """
    获取当前时间戳（毫秒）
    
    Returns:
        当前 Unix 时间戳（毫秒）
    """
    return int(time.time() * 1000)


def jdump(obj: Dict[str, Any]) -> str:
    """
    紧凑格式的 JSON 序列化
    
    Args:
        obj: 要序列化的字典
        
    Returns:
        JSON 字符串（无空格、ASCII 安全）
    """
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))


def jload(s: Any) -> Dict[str, Any]:
    """
    安全的 JSON 反序列化
    
    Args:
        s: JSON 字符串或 bytes
        
    Returns:
        解析后的字典，无效输入返回空字典
    """
    if s is None:
        return {}
    if isinstance(s, (bytes, bytearray)):
        s = s.decode()
    if not s:
        return {}
    try:
        return json.loads(s)
    except Exception:
        return {}


def d_to_str(x: Decimal) -> str:
    """
    Decimal 转字符串，去除尾部零
    
    Args:
        x: Decimal 值
        
    Returns:
        格式化的字符串，如 "123.45" 而非 "123.450000"
    """
    s = format(x, "f")
    if "." in s:
        s = s.rstrip("0").rstrip(".")
    return s or "0"


def pos_field(symbol: str, side: str) -> str:
    """
    生成仓位字段键
    
    Args:
        symbol: 交易对，如 "BTCUSDT"
        side: 方向，如 "LONG" 或 "SHORT"
        
    Returns:
        格式化的字段键，如 "BTCUSDT:LONG"
    """
    return f"{symbol}:{side}"
