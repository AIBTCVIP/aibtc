# trading/leverage.py
"""
批量设置 Binance 永续合约最大杠杆

使用公共缓存获取交易所信息和杠杆档位，避免重复 API 调用
"""

from binance.client import Client
from binance.exceptions import BinanceAPIException
import time
from core.config import BINANCE_ENVIRONMENT


def get_max_leverage_from_cache(symbol: str) -> int:
    """
    从公共缓存获取最大杠杆
    
    注意：如果缓存未命中，返回默认值 20。
    建议使用 get_max_leverage() 函数，它会在缓存未命中时调用 API。
    """
    from core.binance_public_cache import get_binance_public_cache
    cache = get_binance_public_cache()
    return cache.get_max_leverage(symbol)


def get_max_leverage(client: Client, symbol: str) -> int:
    """
    查询某个永续合约允许的最大杠杆
    
    优先使用公共缓存，缓存未命中时使用传入的 client 调用 API 并更新缓存
    """
    from core.binance_public_cache import get_binance_public_cache
    cache = get_binance_public_cache()
    
    # 先尝试从缓存获取
    brackets = cache.get_leverage_bracket_from_cache(symbol)
    
    if brackets:
        return int(brackets[0].get('initialLeverage', 20))
    
    # 缓存未命中，使用传入的 client 调用 API
    try:
        from core.rate_limiter import get_binance_rate_limiter
        api_key = getattr(client, 'API_KEY', '') or ''
        rate_limiter = get_binance_rate_limiter(api_key)
        rate_limiter.acquire(endpoint="futures_leverage_bracket", timeout=30.0)
        
        result = client.futures_leverage_bracket(symbol=symbol)
        if result and len(result) > 0:
            brackets = result[0].get('brackets', [])
            # 更新缓存
            cache.set_leverage_bracket_cache(symbol, brackets)
            if brackets:
                return int(brackets[0].get('initialLeverage', 20))
    except Exception as e:
        import logging
        logging.getLogger(__name__).warning(f"获取最大杠杆失败 {symbol}: {e}")
    
    return 20  # 默认值


def main():
    import os
    api_key = os.getenv("BINANCE_API_KEY")
    api_secret = os.getenv("BINANCE_API_SECRET")
    if not api_key or not api_secret:
        raise ValueError("请设置环境变量 BINANCE_API_KEY 和 BINANCE_API_SECRET")
    
    client = Client(api_key=api_key, api_secret=api_secret, testnet=BINANCE_ENVIRONMENT)

    # 使用公共缓存获取交易所信息
    from core.binance_public_cache import get_binance_public_cache
    cache = get_binance_public_cache()
    exchange_info = cache.get_exchange_info()

    # 仅筛选 USDT-M 永续合约
    symbols = [
        symbol
        for symbol, info in exchange_info.items()
        if info.get("contractType") == "PERPETUAL"
        and info.get("status") == "TRADING"
        and info.get("quoteAsset") == "USDT"
    ]

    print(f"发现永续合约数量: {len(symbols)}")

    ok, fail = 0, 0

    for i, symbol in enumerate(symbols, 1):
        try:
            max_leverage = get_max_leverage(client, symbol)

            result = client.futures_change_leverage(
                symbol=symbol,
                leverage=max_leverage
            )

            print(
                f"[{i}/{len(symbols)}] {symbol} 杠杆已设置为 {result['leverage']}x"
            )
            ok += 1
            time.sleep(0.05)

        except BinanceAPIException as e:
            print(
                f"[{i}/{len(symbols)}] {symbol} 设置失败: {e.message}"
            )
            fail += 1
            time.sleep(0.05)

        except Exception as e:
            print(
                f"[{i}/{len(symbols)}] {symbol} 未知错误: {str(e)}"
            )
            fail += 1
            time.sleep(0.05)

    print("\n====== 结果统计 ======")
    print(f"成功: {ok}")
    print(f"失败: {fail}")


if __name__ == "__main__":
    main()
