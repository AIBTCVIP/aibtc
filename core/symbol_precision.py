"""
交易对精度信息服务

统一获取各交易所的价格精度和数量精度，用于前端显示格式化。

支持的交易所：
- Binance: 从 exchange_info 获取 tickSize/stepSize
- OKX: 从 instruments 获取 tickSz/lotSz
- Bitget: 从 contracts 获取 pricePlace/volumePlace
- Hyperliquid: 从 meta 获取 szDecimals
"""

import logging
from typing import Dict, Optional, Any
from decimal import Decimal

logger = logging.getLogger(__name__)


def _count_decimals(value: float) -> int:
    """计算小数位数"""
    if value <= 0:
        return 0
    d = Decimal(str(value))
    return max(0, -d.as_tuple().exponent)


def get_binance_precision(symbol: str) -> Dict[str, int]:
    """
    获取 Binance 交易对精度
    
    Returns:
        {"price_precision": int, "qty_precision": int}
    """
    try:
        from core.binance_public_cache import BinancePublicCache
        cache = BinancePublicCache.get_instance()
        info = cache.get_symbol_info(symbol)
        
        if not info:
            return {"price_precision": 2, "qty_precision": 3}
        
        price_precision = 2
        qty_precision = 3
        
        for f in info.get("filters", []):
            if f.get("filterType") == "PRICE_FILTER":
                tick_size = float(f.get("tickSize", 0.01))
                price_precision = _count_decimals(tick_size)
            elif f.get("filterType") == "LOT_SIZE":
                step_size = float(f.get("stepSize", 0.001))
                qty_precision = _count_decimals(step_size)
        
        return {"price_precision": price_precision, "qty_precision": qty_precision}
    except Exception as e:
        logger.debug(f"[SymbolPrecision] Binance {symbol} 获取精度失败: {e}")
        return {"price_precision": 2, "qty_precision": 3}


def get_okx_precision(symbol: str) -> Dict[str, int]:
    """
    获取 OKX 交易对精度
    
    Returns:
        {"price_precision": int, "qty_precision": int}
    """
    try:
        from core.okx_public_cache import OKXPublicCache
        cache = OKXPublicCache.get_instance()
        
        tick_sz = cache.get_tick_size(symbol)
        lot_sz = cache.get_lot_size(symbol)
        
        price_precision = _count_decimals(tick_sz) if tick_sz else 2
        qty_precision = _count_decimals(lot_sz) if lot_sz else 3
        
        return {"price_precision": price_precision, "qty_precision": qty_precision}
    except Exception as e:
        logger.debug(f"[SymbolPrecision] OKX {symbol} 获取精度失败: {e}")
        return {"price_precision": 2, "qty_precision": 3}


def get_bitget_precision(symbol: str) -> Dict[str, int]:
    """
    获取 Bitget 交易对精度
    
    Returns:
        {"price_precision": int, "qty_precision": int}
    """
    try:
        from core.bitget_public_cache import BitgetPublicCache
        cache = BitgetPublicCache.get_instance()
        
        price_precision = cache.get_price_precision(symbol)
        qty_precision = cache.get_volume_precision(symbol)
        
        return {"price_precision": price_precision, "qty_precision": qty_precision}
    except Exception as e:
        logger.debug(f"[SymbolPrecision] Bitget {symbol} 获取精度失败: {e}")
        return {"price_precision": 2, "qty_precision": 4}


def get_hyperliquid_precision(symbol: str) -> Dict[str, int]:
    """
    获取 Hyperliquid 交易对精度
    
    Hyperliquid 价格精度根据币种不同，数量精度从 szDecimals 获取
    
    Returns:
        {"price_precision": int, "qty_precision": int}
    """
    # Hyperliquid 没有独立的公共缓存，使用默认值
    # 价格精度根据币种估算
    symbol_upper = symbol.upper().replace("USDT", "")
    
    # 主流币种价格精度
    price_precision_map = {
        "BTC": 1,
        "ETH": 2,
        "SOL": 2,
        "BNB": 2,
        "XRP": 4,
        "DOGE": 5,
        "ADA": 4,
        "AVAX": 2,
        "LINK": 3,
        "DOT": 3,
        "MATIC": 4,
        "LTC": 2,
        "SHIB": 8,
        "PEPE": 8,
        "WIF": 4,
        "BONK": 8,
    }
    
    price_precision = price_precision_map.get(symbol_upper, 4)
    qty_precision = 3  # Hyperliquid 默认数量精度
    
    return {"price_precision": price_precision, "qty_precision": qty_precision}


def get_symbol_precision(symbol: str, exchange: str) -> Dict[str, int]:
    """
    获取指定交易所的交易对精度
    
    Args:
        symbol: 交易对（如 BTCUSDT）
        exchange: 交易所名称（binance, okx, bitget, hyperliquid）
    
    Returns:
        {"price_precision": int, "qty_precision": int}
    """
    exchange_lower = (exchange or "binance").lower()
    
    if exchange_lower == "binance":
        return get_binance_precision(symbol)
    elif exchange_lower == "okx":
        return get_okx_precision(symbol)
    elif exchange_lower == "bitget":
        return get_bitget_precision(symbol)
    elif exchange_lower == "hyperliquid":
        return get_hyperliquid_precision(symbol)
    else:
        # 默认精度
        return {"price_precision": 2, "qty_precision": 3}


def get_symbols_precision_batch(symbols_with_exchange: list) -> Dict[str, Dict[str, int]]:
    """
    批量获取多个交易对的精度
    
    Args:
        symbols_with_exchange: [{"symbol": "BTCUSDT", "exchange": "binance"}, ...]
    
    Returns:
        {
            "binance:BTCUSDT": {"price_precision": 2, "qty_precision": 3},
            "okx:ETHUSDT": {"price_precision": 2, "qty_precision": 4},
            ...
        }
    """
    result = {}
    
    for item in symbols_with_exchange:
        symbol = item.get("symbol", "")
        exchange = item.get("exchange", "binance")
        
        if not symbol:
            continue
        
        key = f"{exchange}:{symbol}"
        if key not in result:
            result[key] = get_symbol_precision(symbol, exchange)
    
    return result


async def get_symbols_precision_batch_async(symbols_with_exchange: list) -> Dict[str, Dict[str, int]]:
    """
    异步批量获取多个交易对的精度
    """
    import asyncio
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, get_symbols_precision_batch, symbols_with_exchange)
