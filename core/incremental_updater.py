#!/usr/bin/env python3
"""
增量K线更新系统
支持增量下载，避免重复获取已有数据
"""
import asyncio
import json
import logging
import time
import requests
from typing import Dict, List, Optional, Tuple, Any
from concurrent.futures import ThreadPoolExecutor
from core.database import redis_client, RedisKeys, get_async_redis
from core.tiered_cache import TieredCacheManager

logger = logging.getLogger(__name__)

# 全局线程池，用于并发下载 (max_workers=8)
_download_executor = ThreadPoolExecutor(max_workers=8)


def shutdown_download_executor():
    """关闭下载线程池（在程序退出时调用）"""
    global _download_executor
    if _download_executor:
        _download_executor.shutdown(wait=False)
        _download_executor = None
        logger.info("[incremental_updater] 下载线程池已关闭")


# P1 Fix: 注册 atexit 确保程序退出时关闭线程池
import atexit
atexit.register(shutdown_download_executor)


class IncrementalKlineUpdater:
    """增量K线更新器 - 只获取完结K线"""

    def __init__(self):
        self.cache_manager = TieredCacheManager()

    def _get_interval_ms(self, interval: str) -> int:
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

    async def update_symbol_klines(self, symbol: str, interval: str) -> Dict[str, int]:
        """
        增量更新指定币种和时间周期的K线数据
        
        逻辑：
        1. 检查 Redis 中最新K线的时间戳
        2. 计算当前应该有的最新完结K线时间
        3. 如果有缺失就去下载

        Args:
            symbol: 币种
            interval: 时间周期

        Returns:
            更新统计信息
        """
        current_time_ms = int(time.time() * 1000)
        interval_ms = self._get_interval_ms(interval)
        
        # 计算当前周期的开始时间（这根K线还没完结）
        current_period_start = (current_time_ms // interval_ms) * interval_ms
        # 最新的完结K线应该是上一个周期
        latest_closed_kline_start = current_period_start - interval_ms
        
        # 获取 Redis 中最新K线的时间戳
        hot_key = RedisKeys.market_klines_hot(symbol, interval)
        redis = await get_async_redis()
        existing_timestamps = await redis.hkeys(hot_key)
        
        new_klines = {}
        total_new = 0
        
        if not existing_timestamps:
            # Redis 中没有数据，首次获取
            # 根据时间周期计算需要的天数
            days_needed = {
                "15m": 5,
                "1h": 25,
                "4h": 140,
            }.get(interval, 30)
            
            logger.info(f"[K线] {symbol} {interval} 首次获取 ({days_needed}天)")
            new_klines, total_new = await self._fetch_recent_klines(symbol, interval, days=days_needed)
        else:
            # 找到 Redis 中最新的K线时间戳
            latest_in_redis = max(int(ts) for ts in existing_timestamps)
            
            # 如果 Redis 中的数据已经是最新的，跳过
            if latest_in_redis >= latest_closed_kline_start:
                return {"new_records": 0, "updated_records": 0, "skipped": True}
            
            # 需要获取从 latest_in_redis 之后到 latest_closed_kline_start 的数据
            # start_time 是 latest_in_redis 的下一个周期
            start_time = latest_in_redis + interval_ms
            end_time = current_period_start - 1  # 不包含当前未完结的K线
            
            if start_time <= end_time:
                new_klines, total_new = await self._fetch_klines_in_range(
                    symbol, interval, start_time, end_time
                )

        # 更新缓存
        if new_klines:
            await self.cache_manager.update_hot_klines(symbol, interval, new_klines)

            if total_new > 0:
                logger.info(f"[K线] {symbol} {interval} 新增 {total_new} 条")

        return {
            "new_records": total_new,
            "updated_records": len(new_klines),
            "symbol": symbol,
            "interval": interval
        }

    async def _fetch_recent_klines(self, symbol: str, interval: str, days: int) -> Tuple[Dict[str, Dict], int]:
        """
        获取最近N天的完结K线数据

        Args:
            symbol: 币种
            interval: 时间周期
            days: 天数

        Returns:
            (K线数据字典, 记录数量)
        """
        # 计算时间范围 - 只获取完结K线
        current_time = int(time.time() * 1000)  # 毫秒

        # 计算当前时间段的开始时间（未完结）
        interval_ms = self._get_interval_ms(interval)
        current_period_start = (current_time // interval_ms) * interval_ms

        # 结束时间设为当前时间段开始时间之前（只获取完结K线）
        end_time = current_period_start - 1
        start_time = end_time - (days * 24 * 60 * 60 * 1000)  # N天前

        return await self._fetch_klines_in_range(symbol, interval, start_time, end_time)

    async def _fetch_incremental_klines(self, symbol: str, interval: str, since_timestamp: float) -> Tuple[Dict[str, Dict], int]:
        """
        获取增量完结K线数据（从指定时间开始，只获取完结K线）

        Args:
            symbol: 币种
            interval: 时间周期
            since_timestamp: 开始时间戳

        Returns:
            (K线数据字典, 记录数量)
        """
        start_time = int(since_timestamp * 1000)  # 转换为毫秒

        # 计算当前时间段的开始时间
        current_time = int(time.time() * 1000)
        interval_ms = self._get_interval_ms(interval)
        current_period_start = (current_time // interval_ms) * interval_ms

        # 结束时间设为当前时间段开始时间之前（只获取完结K线）
        end_time = current_period_start - 1

        # 如果开始时间太接近当前，确保至少获取一些历史数据
        if end_time - start_time < interval_ms:
            end_time = start_time + (24 * 60 * 60 * 1000)  # 至少获取一天的数据

        return await self._fetch_klines_in_range(symbol, interval, start_time, end_time)

    def _sync_fetch_klines(self, symbol: str, interval: str, start_time: int, end_time: int) -> Tuple[Dict[str, Dict], int]:
        """
        同步获取K线数据（在线程池中执行）
        
        Args:
            symbol: 币种
            interval: 时间周期
            start_time: 开始时间（毫秒）
            end_time: 结束时间（毫秒）
            
        Returns:
            (K线数据字典, 记录数量)
        """
        base_url = "https://fapi.binance.com/fapi/v1/klines"
        klines = {}
        total_fetched = 0
        current_time_ms = int(time.time() * 1000)
        current_start = start_time

        while current_start < end_time:
            params = {
                "symbol": symbol,
                "interval": interval,
                "startTime": current_start,
                "endTime": end_time,
                "limit": 1500
            }

            try:
                response = requests.get(base_url, params=params, timeout=10)
                response.raise_for_status()
                data = response.json()

                if not data:
                    break

                for k in data:
                    ts = str(k[0])
                    close_time = k[6]
                    
                    if close_time > current_time_ms:
                        continue

                    klines[ts] = {
                        "Open": float(k[1]),
                        "High": float(k[2]),
                        "Low": float(k[3]),
                        "Close": float(k[4]),
                        "Volume": float(k[5]),
                        "TakerBuyVolume": float(k[9]),
                        "TakerSellVolume": float(k[5]) - float(k[9])
                    }

                total_fetched += len(data)

                if data:
                    current_start = data[-1][0] + 1
                else:
                    break

                if len(data) < 1500:
                    break

            except Exception as e:
                logger.warning(f"获取 {symbol} {interval} K线数据失败: {e}")
                break

        return klines, total_fetched

    async def _fetch_klines_in_range(self, symbol: str, interval: str, start_time: int, end_time: int) -> Tuple[Dict[str, Dict], int]:
        """
        获取指定时间范围内的完结K线数据（使用线程池并发执行）

        注意：此方法确保只返回已经完结的K线数据，不会包含当前正在形成的K线。

        Args:
            symbol: 币种
            interval: 时间周期
            start_time: 开始时间（毫秒）
            end_time: 结束时间（毫秒）- 已经是完结K线的结束时间

        Returns:
            (K线数据字典, 记录数量)
        """
        loop = asyncio.get_event_loop()
        # 在线程池中执行同步 HTTP 请求，不阻塞事件循环
        return await loop.run_in_executor(
            _download_executor,
            self._sync_fetch_klines,
            symbol, interval, start_time, end_time
        )

    async def batch_update_symbols(self, symbols: List[str], intervals: Optional[List[str]] = None) -> Dict[str, Dict]:
        """
        批量更新多个币种的K线数据（并发执行，使用线程池 max_workers=8）

        Args:
            symbols: 币种列表
            intervals: 时间周期列表，默认 ["15m", "1h", "4h"]

        Returns:
            更新统计信息
        """
        if intervals is None:
            intervals = ["15m", "1h", "4h"]

        results = {}
        total_new = 0
        total_updated = 0

        # 记录所有任务对应的 (symbol, interval)
        task_info = []
        for symbol in symbols:
            for interval in intervals:
                task_info.append((symbol, interval))

        # 创建所有任务（线程池 max_workers=8 会自动限制并发数）
        tasks = [
            self.update_symbol_klines(symbol, interval)
            for symbol, interval in task_info
        ]
        
        # 并发执行所有任务
        task_results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # 整理结果
        for i, (symbol, interval) in enumerate(task_info):
            if symbol not in results:
                results[symbol] = {}
            
            result = task_results[i]
            if isinstance(result, Exception):
                results[symbol][interval] = {"error": str(result)}
            else:
                results[symbol][interval] = result
                if not result.get("skipped") and not result.get("error"):
                    total_new += result.get("new_records", 0)
                    total_updated += result.get("updated_records", 0)

        if total_new > 0 or total_updated > 0:
            logger.info(f"[K线] 批量更新完成: {len(symbols)} 币种, 新增 {total_new} 条")

        return results

    async def get_update_status(self, symbol: str, interval: str) -> Dict[str, Any]:
        """
        获取指定币种和时间周期的更新状态

        Args:
            symbol: 币种
            interval: 时间周期

        Returns:
            更新状态信息
        """
        redis = await get_async_redis()
        last_update_key = RedisKeys.market_klines_last_update(symbol, interval)
        last_update_str = await redis.get(last_update_key)

        if last_update_str:
            last_update = float(last_update_str)
            hours_ago = (time.time() - last_update) / 3600

            return {
                "symbol": symbol,
                "interval": interval,
                "last_update": last_update,
                "last_update_readable": time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(last_update)),
                "hours_since_update": round(hours_ago, 1),
                "needs_update": hours_ago > 1
            }
        else:
            return {
                "symbol": symbol,
                "interval": interval,
                "last_update": None,
                "needs_update": True,
                "status": "never_updated"
            }

    async def cleanup_old_data(self, days_to_keep: int = 151):
        """
        清理过期数据（保留最近N天的热数据）

        Args:
            days_to_keep: 保留天数
        """
        logger.info(f"[K线] 清理 {days_to_keep} 天前的过期数据")

        # 获取所有热数据键
        redis = await get_async_redis()
        pattern = "global:market:klines:hot:*:*:*"
        keys = await redis.keys(pattern)

        cleaned_count = 0
        for key in keys:
            # 解析symbol和interval
            parts = key.split(":")
            if len(parts) >= 5:
                symbol = parts[3]
                interval = parts[4]

                try:
                    await self.cache_manager.archive_to_cold_storage(symbol, interval, days_to_keep)
                    cleaned_count += 1
                except Exception as e:
                    logger.warning(f"清理 {symbol} {interval} 失败: {e}")

        logger.info(f"[K线] 清理完成，共处理 {cleaned_count} 个数据集合")

    async def get_system_update_stats(self) -> Dict[str, Any]:
        """
        获取系统更新统计信息

        Returns:
            系统更新统计
        """
        # 获取活跃币种
        redis = await get_async_redis()
        symbols = await redis.smembers(RedisKeys.market_symbols())

        intervals = ["15m", "1h", "4h"]
        total_checks = len(symbols) * len(intervals)

        needs_update = 0
        up_to_date = 0
        never_updated = 0

        symbol_stats = {}

        for symbol in symbols:
            symbol_stats[symbol] = {}

            for interval in intervals:
                status = await self.get_update_status(symbol, interval)
                symbol_stats[symbol][interval] = status

                if status.get("status") == "never_updated":
                    never_updated += 1
                elif status.get("needs_update", False):
                    needs_update += 1
                else:
                    up_to_date += 1

        return {
            "total_symbols": len(symbols),
            "total_intervals": len(intervals),
            "total_checks": total_checks,
            "needs_update": needs_update,
            "up_to_date": up_to_date,
            "never_updated": never_updated,
            "update_percentage": round((up_to_date / total_checks) * 100, 1) if total_checks > 0 else 0,
            "symbol_stats": symbol_stats
        }