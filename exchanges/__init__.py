# exchanges/__init__.py
"""
多交易所支持模块

支持的交易所：
- Binance (币安)
- OKX (欧易)
- Hyperliquid

使用方式：
    from exchanges import create_exchange, ExchangeType
    
    exchange = create_exchange(
        exchange_type=ExchangeType.BINANCE,
        api_key="xxx",
        api_secret="xxx",
        is_testnet=False
    )
"""

from enum import Enum
from typing import Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from exchanges.base import BaseExchange


class ExchangeType(Enum):
    """交易所类型"""
    BINANCE = "binance"
    OKX = "okx"
    HYPERLIQUID = "hyperliquid"
    BITGET = "bitget"


def create_exchange(
    exchange_type: str | ExchangeType,
    api_key: str,
    api_secret: str,
    passphrase: str = None,  # OKX 需要
    is_testnet: bool = False,
    **kwargs
) -> "BaseExchange":
    """
    创建交易所实例
    
    Args:
        exchange_type: 交易所类型
        api_key: API Key
        api_secret: API Secret
        passphrase: API Passphrase (OKX 需要)
        is_testnet: 是否测试网
        
    Returns:
        交易所实例
    """
    if isinstance(exchange_type, ExchangeType):
        exchange_type = exchange_type.value
    
    exchange_type = exchange_type.lower()
    
    if exchange_type == "binance":
        from exchanges.binance_exchange import BinanceExchange
        return BinanceExchange(
            api_key=api_key,
            api_secret=api_secret,
            is_testnet=is_testnet,
            **kwargs
        )
    elif exchange_type == "okx":
        from exchanges.okx_exchange import OKXExchange
        return OKXExchange(
            api_key=api_key,
            api_secret=api_secret,
            passphrase=passphrase,
            is_testnet=is_testnet,
            **kwargs
        )
    elif exchange_type == "hyperliquid":
        from exchanges.hyperliquid_exchange import HyperliquidExchange
        return HyperliquidExchange(
            api_key=api_key,  # Hyperliquid 使用 private key
            api_secret=api_secret,
            is_testnet=is_testnet,
            **kwargs
        )
    elif exchange_type == "bitget":
        from exchanges.bitget_exchange import BitgetExchange
        return BitgetExchange(
            api_key=api_key,
            api_secret=api_secret,
            passphrase=passphrase,
            is_testnet=is_testnet,
            **kwargs
        )
    else:
        raise ValueError(f"不支持的交易所类型: {exchange_type}")


# 支持的交易所列表
SUPPORTED_EXCHANGES = ["binance", "okx", "bitget", "hyperliquid"]

# 交易所显示名称
EXCHANGE_NAMES = {
    "binance": "Binance (币安)",
    "okx": "OKX (欧易)",
    "hyperliquid": "Hyperliquid",
    "bitget": "Bitget",
}

# 交易所是否需要 passphrase
EXCHANGE_REQUIRES_PASSPHRASE = {
    "binance": False,
    "okx": True,
    "hyperliquid": False,
    "bitget": True,
}

# 交易所是否支持测试网
EXCHANGE_HAS_TESTNET = {
    "binance": True,
    "okx": True,
    "hyperliquid": True,
    "bitget": True,
}
