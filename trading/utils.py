# trading/utils.py
"""
交易通用工具函数

包含与具体交易所无关的通用逻辑：
- 止损止盈验证
- Redis 数据同步
- 交易记录保存
"""

import json
import logging
from typing import Optional

from core.database import redis_client

logger = logging.getLogger(__name__)

REDIS_KEY = "trading_records"


def save_trade_record(record: dict):
    """保存交易记录到 Redis"""
    redis_client.lpush(REDIS_KEY, json.dumps(record))


def is_sl_update_valid(
    position_side: str, 
    current_price: float, 
    current_sl: float, 
    new_sl: float
) -> bool:
    """
    验证止损价格更新是否有效
    
    规则:
    - LONG: 新止损必须低于当前价格，且应该比旧止损更高（收紧止损）
    - SHORT: 新止损必须高于当前价格，且应该比旧止损更低（收紧止损）
    
    Args:
        position_side: 持仓方向 (LONG/SHORT)
        current_price: 当前价格
        current_sl: 当前止损价格
        new_sl: 新止损价格
    
    Returns:
        True 如果更新有效
    """
    if new_sl <= 0:
        return False
    
    position_side = (position_side or "").upper()
    
    if position_side == "LONG":
        # LONG 止损必须低于当前价格
        if new_sl >= current_price:
            return False
        # 新止损应该比旧止损更高（收紧）或者旧止损无效
        if current_sl > 0 and new_sl < current_sl:
            return False
        return True
    elif position_side == "SHORT":
        # SHORT 止损必须高于当前价格
        if new_sl <= current_price:
            return False
        # 新止损应该比旧止损更低（收紧）或者旧止损无效
        if current_sl > 0 and new_sl > current_sl:
            return False
        return True
    
    return False


def is_tp_update_valid(
    position_side: str, 
    current_price: float, 
    current_tp: float, 
    new_tp: float
) -> bool:
    """
    验证止盈价格更新是否有效
    
    规则:
    - LONG: 新止盈必须高于当前价格
    - SHORT: 新止盈必须低于当前价格
    
    Args:
        position_side: 持仓方向 (LONG/SHORT)
        current_price: 当前价格
        current_tp: 当前止盈价格
        new_tp: 新止盈价格
    
    Returns:
        True 如果更新有效
    """
    if new_tp <= 0:
        return False
    
    position_side = (position_side or "").upper()
    
    if position_side == "LONG":
        # LONG 止盈必须高于当前价格
        if new_tp <= current_price:
            return False
        return True
    elif position_side == "SHORT":
        # SHORT 止盈必须低于当前价格
        if new_tp >= current_price:
            return False
        return True
    
    return False


def sync_sl_tp_to_position(
    symbol: str, 
    position_side: str, 
    sl: Optional[float] = None, 
    tp: Optional[float] = None, 
    uid: Optional[str] = None,
    exchange: Optional[str] = None
):
    """
    同步止损/止盈价格到 Redis 仓位数据
    
    用于在成功下单后，更新用户仓位缓存中的止损止盈信息
    
    Args:
        symbol: 交易对
        position_side: LONG 或 SHORT
        sl: 止损价格
        tp: 止盈价格
        uid: 用户 ID
        exchange: 交易所名称 (binance, okx, bitget, hyperliquid)
                  如果不指定，会尝试更新所有交易所的数据（向后兼容）
    """
    if not uid or not symbol:
        return
    
    try:
        from core.pf_compatibility import pf_compat
        
        field = f"{symbol}:{position_side.upper()}"
        
        if exchange:
            # 更新指定交易所的数据
            _sync_sl_tp_for_exchange(uid, field, sl, tp, exchange)
        else:
            # 向后兼容：尝试更新所有交易所的数据
            # 首先尝试从全局数据更新（旧架构）
            pos_data = pf_compat.get_pf_pos(uid)
            if pos_data and field in pos_data:
                pos = pos_data[field]
                if sl is not None:
                    pos["stopLossPrice"] = str(sl)
                if tp is not None:
                    pos["takeProfitPrice"] = str(tp)
                pos_data[field] = pos
                pf_compat.set_pf_pos(uid, pos_data)
            
            cycle_data = pf_compat.get_pf_cycle(uid)
            if cycle_data and field in cycle_data:
                cyc = cycle_data[field]
                if sl is not None:
                    cyc["stopLossPrice"] = str(sl)
                if tp is not None:
                    cyc["takeProfitPrice"] = str(tp)
                cycle_data[field] = cyc
                pf_compat.set_pf_cycle(uid, cycle_data)
            
    except Exception as e:
        logger.warning(f"[{uid}] sync_sl_tp_to_position 失败: {e}")


def _sync_sl_tp_for_exchange(
    uid: str,
    field: str,
    sl: Optional[float],
    tp: Optional[float],
    exchange: str
):
    """为指定交易所同步止损止盈"""
    from core.pf_compatibility import pf_compat
    
    # 更新 pos 数据
    pos_data = pf_compat.get_pf_pos(uid, exchange)
    if pos_data and field in pos_data:
        pos = pos_data[field]
        if sl is not None:
            pos["stopLossPrice"] = str(sl)
        if tp is not None:
            pos["takeProfitPrice"] = str(tp)
        pos_data[field] = pos
        pf_compat.set_pf_pos(uid, pos_data, exchange)
    
    # 更新 cycle 数据
    cycle_data = pf_compat.get_pf_cycle(uid, exchange)
    if cycle_data and field in cycle_data:
        cyc = cycle_data[field]
        if sl is not None:
            cyc["stopLossPrice"] = str(sl)
        if tp is not None:
            cyc["takeProfitPrice"] = str(tp)
        cycle_data[field] = cyc
        pf_compat.set_pf_cycle(uid, cycle_data, exchange)


def extract_binance_error_code(e: Exception) -> int:
    """
    从 BinanceAPIException 提取错误码
    
    Args:
        e: 异常对象
    
    Returns:
        错误码 (int)，如果无法解析返回 0
    """
    try:
        if hasattr(e, 'code'):
            return int(e.code)
        # 尝试从消息中解析
        msg = str(e)
        if 'code=' in msg:
            import re
            match = re.search(r'code=(-?\d+)', msg)
            if match:
                return int(match.group(1))
    except Exception as e:
        # P3 Fix: 添加日志
        logger.debug(f"解析错误码异常: {e}")
    return 0
