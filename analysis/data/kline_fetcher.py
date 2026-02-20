"""
K线数据获取模块 - 支持多用户自定义币种

K线数据存储在 Redis 中，所有用户共享（因为是公开市场数据）
每个用户可以配置自己关注的币种列表
"""
import time
import json
import logging
import asyncio
import threading
import requests
from typing import TYPE_CHECKING, List, Set
from concurrent.futures import ThreadPoolExecutor
from core.config import timeframes, KLINE_LIMITS
from core.database import redis_client, RedisKeys, get_async_redis

if TYPE_CHECKING:
    from core.user_context import UserContext

logger = logging.getLogger(__name__)


def fetch_historical(symbol: str, interval: str, limit: int):
    """
    获取历史K线数据 - 只获取完结K线

    注意：此函数确保只返回已经完结的K线数据，
    不会包含当前正在形成的K线（因为没有指定时间范围时，
    Binance API会返回最新的数据，包括未完结的）。

    Args:
        symbol: 币种
        interval: 时间周期
        limit: 获取条数
    """
    # 计算时间范围，确保只获取完结K线
    current_time = int(time.time() * 1000)  # 毫秒

    # 根据interval计算当前K线周期的长度
    interval_ms = _get_interval_ms(interval)
    current_period_start = (current_time // interval_ms) * interval_ms

    # 结束时间设为当前周期开始时间之前，确保只获取完结K线
    end_time = current_period_start - 1
    start_time = end_time - (limit * interval_ms) + interval_ms

    url = f"https://fapi.binance.com/fapi/v1/klines?symbol={symbol}&interval={interval}&startTime={start_time}&endTime={end_time}&limit={limit}"

    # 存储键 - 使用热数据键名
    from core.database import RedisKeys
    rkey = RedisKeys.market_klines_hot(symbol, interval)

    try:
        data = requests.get(url, timeout=5).json()
        now = int(time.time() * 1000)

        with redis_client.pipeline() as pipe:
            for k in data:
                ts, close_ts = k[0], k[6]
                if close_ts > now:
                    continue

                entry = json.dumps({
                    "Open": float(k[1]),
                    "High": float(k[2]),
                    "Low": float(k[3]),
                    "Close": float(k[4]),
                    "Volume": float(k[5]),
                    "TakerBuyVolume": float(k[9]),
                    "TakerSellVolume": float(k[5]) - float(k[9])
                })

                pipe.hset(rkey, ts, entry)
            pipe.execute()

    except Exception as e:
        logger.warning(f"{symbol} {interval} 历史获取失败: {e}")

def fetch_all(symbols: List[str]):
    """
    获取所有币种的历史K线数据

    Args:
        symbols: 要获取的币种列表
    """
    
    total_requests = len(symbols) * len(timeframes)
    logger.info(f"[K线] 初始化下载 {total_requests} 个请求")

    start_time = time.time()

    with ThreadPoolExecutor(max_workers=8) as exe:
        for s in symbols:
            for tf in timeframes:
                limit = KLINE_LIMITS.get(tf, 301)
                exe.submit(fetch_historical, s, tf, limit)

    elapsed = time.time() - start_time
    logger.info(f"[K线] 初始化完成，耗时 {elapsed:.2f}s")


# ============================================================
# 多用户版本函数
# ============================================================

# 全局已获取的币种集合（避免重复获取）
_fetched_symbols: Set[str] = set()
_fetched_symbols_lock = threading.Lock()


def fetch_all_for_user(ctx: 'UserContext'):
    """
    为用户准备K线数据（全局共享版本）

    K线数据存储在全局键中，用户通过引用访问，避免数据冗余
    """
    from core.redis_manager import RedisDataManager

    user_symbols = ctx.get_monitor_symbols()
    if not user_symbols:
        logger.warning(f"[{ctx.uid}] 未配置监控币种")
        return

    # 直接使用用户设置的监控币种
    active_symbols = user_symbols

    if not active_symbols:
        logger.warning(f"[{ctx.uid}] 用户未设置任何监控币种")
        return

    # 检查哪些币种需要获取K线数据
    missing_data = []
    for symbol in active_symbols:
        # 检查全局是否有此币种的数据
        for interval in ["15m", "1h", "4h"]:
            from core.database import RedisKeys
            global_key = RedisKeys.market_klines_hot(symbol, interval)
            if not redis_client.exists(global_key):
                missing_data.append((symbol, interval))
                break  # 如果任一时间周期缺少数据，就需要获取

    # 获取缺失的K线数据
    fetched_count = 0
    for symbol, interval in missing_data:
        try:
            limit = KLINE_LIMITS.get(interval, 301)
            fetch_historical(symbol, interval, limit)
            fetched_count += 1
        except Exception as e:
            logger.warning(f"获取 {symbol} {interval} K线数据失败: {e}")
            continue

    if fetched_count > 0:
        logger.info(f"[{ctx.uid}] 获取了 {fetched_count} 个K线数据集")

    # 返回用户可以访问的K线数据信息
    return {
        "available_symbols": active_symbols,
        "data_source": "global_cache",
        "total_datasets": len(active_symbols) * 3  # 每个币种3个时间周期
    }

# ==================== 工具函数 ====================
def _get_interval_ms(interval: str) -> int:
    """
    获取时间间隔的毫秒数

    Args:
        interval: 时间周期 (1m, 5m, 15m, 1h, 4h, 1d等)

    Returns:
        毫秒数
    """
    unit = interval[-1]  # m, h, d, w, M
    value = int(interval[:-1]) if len(interval) > 1 else 1

    if unit == 'm':
        return value * 60 * 1000
    elif unit == 'h':
        return value * 60 * 60 * 1000
    elif unit == 'd':
        return value * 24 * 60 * 60 * 1000
    elif unit == 'w':
        return value * 7 * 24 * 60 * 60 * 1000
    elif unit == 'M':
        return value * 30 * 24 * 60 * 60 * 1000  # 近似值
    else:
        raise ValueError(f"Unsupported interval: {interval}")

# ==================== 批量处理函数 ====================

async def batch_download_klines(symbols: List[str]):
    """
    批量下载/更新多个币种的K线数据
    
    使用增量更新逻辑：
    - 首次下载：获取历史数据
    - 后续更新：只获取新的完结K线

    Args:
        symbols: 币种列表
    """
    if not symbols:
        return

    from core.incremental_updater import IncrementalKlineUpdater
    updater = IncrementalKlineUpdater()
    
    # 使用增量更新（会自动判断是首次下载还是增量更新）
    await updater.batch_update_symbols(symbols, intervals=["15m", "1h", "4h"])

async def _download_symbol_klines(symbol: str):
    """
    下载单个币种的所有时间周期K线数据

    Args:
        symbol: 币种名称
    """
    intervals = ["15m", "1h", "4h"]
    downloaded_count = 0

    # 获取异步 Redis 客户端
    redis = await get_async_redis()

    for interval in intervals:
        try:
            # 检查是否已有数据（检查热数据键）
            from core.database import RedisKeys
            hot_key = RedisKeys.market_klines_hot(symbol, interval)
            if await redis.exists(hot_key):
                continue  # 已存在，跳过

            # 下载数据 - 根据时间周期获取对应的K线数量
            # 使用 asyncio.to_thread 包装同步函数，避免阻塞事件循环
            limit = KLINE_LIMITS.get(interval, 301)
            await asyncio.to_thread(fetch_historical, symbol, interval, limit)
            downloaded_count += 1

        except Exception as e:
            logger.warning(f"下载 {symbol} {interval} K线失败: {e}")
            continue

    if downloaded_count > 0:
        logger.info(f"[K线] {symbol} 下载 {downloaded_count} 个周期")


def get_all_monitored_symbols() -> Set[str]:
    """获取所有已监控的币种（用于全局数据刷新）"""
    with _fetched_symbols_lock:
        return _fetched_symbols.copy()


def clear_fetched_cache():
    """清除已获取缓存（重新获取所有数据）"""
    global _fetched_symbols
    with _fetched_symbols_lock:
        _fetched_symbols = set()
