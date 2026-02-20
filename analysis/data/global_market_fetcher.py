# global_market_fetcher.py - 全局市场数据获取服务
"""
全局共享的外部市场数据获取服务

功能:
- 定期获取 funding_rate, open_interest, 24h_change 等外部数据
- 存储到 Redis 供所有用户共享
- 避免多用户重复请求导致 IP 限制

数据存储:
- global:market:external:{symbol} - Hash: funding_rate, oi, change_24h_pct, ...
- global:market:external:last_update - 最后更新时间戳

使用方式:
1. 在 main_async.py 中注册为后台任务
2. 其他模块通过 get_external_data(symbol) 获取数据
"""

import json
import time
import asyncio
import logging
from typing import Dict, Any, List, Optional, Set

from core.database import redis_client, get_async_redis, RedisKeys

logger = logging.getLogger(__name__)

# 配置
FETCH_INTERVAL = 300  # 5 分钟更新一次
REDIS_TTL = 600       # 10 分钟过期


def get_active_symbols() -> Set[str]:
    """
    获取所有活跃交易对（从 K 线数据中提取）
    """
    r = redis_client
    symbols = set()
    
    # 从 klines:hot 键中提取 symbols
    # 格式: global:market:klines:hot:{symbol}:{interval}
    try:
        keys = r.keys("global:market:klines:hot:*:4h")
        for key in keys:
            if isinstance(key, bytes):
                key = key.decode()
            # 解析: global:market:klines:hot:BTCUSDT:4h
            parts = key.split(":")
            if len(parts) >= 5:
                symbol = parts[4]
                if symbol.endswith("USDT"):
                    symbols.add(symbol)
    except Exception as e:
        logger.error(f"获取活跃 symbols 失败: {e}")
    
    return symbols


def save_external_data(symbol: str, data: Dict[str, Any]) -> bool:
    """
    保存外部数据到 Redis
    
    Args:
        symbol: 交易对
        data: {funding_rate, oi, change_24h_pct, volume_24h, ...}
    """
    r = redis_client
    key = RedisKeys.market_external_data(symbol)
    
    try:
        # 转换为字符串存储
        hash_data = {}
        for k, v in data.items():
            if v is not None:
                hash_data[k] = json.dumps(v) if isinstance(v, (dict, list)) else str(v)
        
        if hash_data:
            hash_data["updated_at"] = str(int(time.time()))
            r.hset(key, mapping=hash_data)
            r.expire(key, REDIS_TTL)
            return True
    except Exception as e:
        logger.error(f"保存 {symbol} 外部数据失败: {e}")
    
    return False


def get_external_data(symbol: str) -> Dict[str, Any]:
    """
    获取外部数据（供其他模块调用）
    
    Args:
        symbol: 交易对
    
    Returns:
        {
            "funding_rate": float,
            "oi": float,
            "change_24h_pct": float,
            "volume_24h": float,
            "high_24h": float,
            "low_24h": float,
            "updated_at": int
        }
    """
    r = redis_client
    key = RedisKeys.market_external_data(symbol)
    
    try:
        raw = r.hgetall(key)
        if not raw:
            return {}
        
        result = {}
        for k, v in raw.items():
            if isinstance(k, bytes):
                k = k.decode()
            if isinstance(v, bytes):
                v = v.decode()
            
            # 尝试转换为数值 (先尝试 float，可处理科学计数法如 1e-08)
            try:
                float_val = float(v)
                # 如果是整数且不含科学计数法，转为 int
                if float_val.is_integer() and 'e' not in v.lower():
                    result[k] = int(float_val)
                else:
                    result[k] = float_val
            except ValueError:
                result[k] = v
        
        return result
    except Exception as e:
        logger.error(f"获取 {symbol} 外部数据失败: {e}")
        return {}


async def fetch_and_store_all(symbols: List[str]) -> Dict[str, bool]:
    """
    批量获取并存储所有 symbols 的外部数据
    
    Args:
        symbols: 交易对列表
    
    Returns:
        {symbol: success}
    """
    from analysis.data.volume_stats import batch_fetch_async, get_oi_change_async
    
    results = {}
    
    try:
        # 批量获取基础数据
        data = await batch_fetch_async(symbols)
        
        funding_data = data.get("funding", {})
        p24_data = data.get("p24", {})
        oi_data = data.get("oi", {})
        
        # 批量获取 OI 变化数据 (1h, 4h, 24h)
        oi_change_tasks = []
        for symbol in symbols:
            oi_change_tasks.append(get_oi_change_async(symbol, 1))
            oi_change_tasks.append(get_oi_change_async(symbol, 4))
            oi_change_tasks.append(get_oi_change_async(symbol, 24))
        
        oi_change_results = await asyncio.gather(*oi_change_tasks, return_exceptions=True)
        
        # 整理 OI 变化数据: {symbol: {1h: ..., 4h: ..., 24h: ...}}
        oi_changes = {}
        for i, symbol in enumerate(symbols):
            oi_changes[symbol] = {}
            for j, period in enumerate([1, 4, 24]):
                idx = i * 3 + j
                if idx < len(oi_change_results):
                    result = oi_change_results[idx]
                    if result and not isinstance(result, Exception):
                        oi_changes[symbol][f"{period}h"] = result
        
        # 存储每个 symbol 的数据
        for symbol in symbols:
            symbol_data = {}
            
            # funding rate
            fr = funding_data.get(symbol)
            if fr is not None:
                symbol_data["funding_rate"] = fr
            
            # open interest (当前值)
            oi = oi_data.get(symbol)
            if oi is not None:
                symbol_data["oi"] = oi
            
            # OI 变化百分比
            symbol_oi_changes = oi_changes.get(symbol, {})
            if "1h" in symbol_oi_changes:
                symbol_data["oi_change_1h_pct"] = symbol_oi_changes["1h"].get("oi_change_pct")
            if "4h" in symbol_oi_changes:
                symbol_data["oi_change_4h_pct"] = symbol_oi_changes["4h"].get("oi_change_pct")
            if "24h" in symbol_oi_changes:
                symbol_data["oi_change_24h_pct"] = symbol_oi_changes["24h"].get("oi_change_pct")
            
            # 24h data
            p24 = p24_data.get(symbol)
            if p24 and isinstance(p24, dict):
                if "priceChangePercent" in p24:
                    symbol_data["change_24h_pct"] = p24["priceChangePercent"]
                if "quoteVolume" in p24:
                    symbol_data["volume_24h"] = p24["quoteVolume"]
                if "highPrice" in p24:
                    symbol_data["high_24h"] = p24["highPrice"]
                if "lowPrice" in p24:
                    symbol_data["low_24h"] = p24["lowPrice"]
                if "lastPrice" in p24:
                    symbol_data["last_price"] = p24["lastPrice"]
            
            # 保存
            if symbol_data:
                results[symbol] = save_external_data(symbol, symbol_data)
            else:
                results[symbol] = False
        
        # 更新最后更新时间
        r = redis_client
        r.set(RedisKeys.market_external_last_update(), str(int(time.time())))
        
        success_count = sum(1 for v in results.values() if v)
        logger.info(f"全局市场数据更新完成: {success_count}/{len(symbols)} symbols")
        
    except Exception as e:
        logger.error(f"批量获取外部数据失败: {e}")
    
    return results


async def run_global_market_fetcher():
    """
    后台任务: 定期获取全局市场数据
    
    在 main_async.py 中注册:
        lifecycle.add_background_task(
            "global_market_fetcher",
            run_global_market_fetcher,
            restart_on_failure=True,
            restart_delay=60.0
        )
    """
    logger.info("全局市场数据获取服务启动")
    
    while True:
        try:
            # 获取活跃 symbols
            symbols = get_active_symbols()
            
            if symbols:
                logger.info(f"开始获取 {len(symbols)} 个 symbols 的外部数据")
                await fetch_and_store_all(list(symbols))
            else:
                logger.warning("未找到活跃 symbols，跳过本次更新")
            
        except Exception as e:
            logger.error(f"全局市场数据获取异常: {e}")
        
        # 等待下次更新
        await asyncio.sleep(FETCH_INTERVAL)


# 便捷函数: 获取 BTC/ETH 外部数据
def get_btc_external_data() -> Dict[str, Any]:
    """获取 BTC 外部数据"""
    return get_external_data("BTCUSDT")


def get_eth_external_data() -> Dict[str, Any]:
    """获取 ETH 外部数据"""
    return get_external_data("ETHUSDT")
