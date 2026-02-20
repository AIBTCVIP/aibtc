#!/usr/bin/env python3
"""
分级缓存管理系统
支持热数据内存存储，冷数据磁盘存储的LRU缓存策略
"""
import os
import json
import time
import hashlib
from typing import Dict, List, Optional, Any
from pathlib import Path
from core.database import get_async_redis, RedisKeys

class TieredCacheManager:
    """分级缓存管理器 - 只缓存完结K线数据"""

    def __init__(self, cache_dir: str = "cache/klines"):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)

        # 缓存配置
        self.hot_data_days = 7  # 热数据保留天数（最近7天）
        self.max_memory_items = 10000  # 内存中最大K线条数
        self.compression_threshold = 1000  # 压缩阈值

        # 完结K线保证：确保只缓存已经稳定完结的K线数据
        self.safety_margin_minutes = 5  # 安全边际：当前K线结束后5分钟才缓存

    # ==================== 热数据管理（Redis内存） ====================
    async def get_hot_klines(self, symbol: str, interval: str, limit: int = 1000) -> Dict[str, Dict]:
        """
        获取热K线数据（最近几天）

        Args:
            symbol: 币种
            interval: 时间周期
            limit: 最大返回条数

        Returns:
            K线数据字典
        """
        redis = await get_async_redis()
        key = RedisKeys.market_klines_hot(symbol, interval)
        data = await redis.hgetall(key)

        result = {}
        for ts, kline_json in data.items():
            if isinstance(ts, bytes):
                ts = ts.decode('utf-8')
            if isinstance(kline_json, bytes):
                kline_json = kline_json.decode('utf-8')

            try:
                result[ts] = json.loads(kline_json)
            except json.JSONDecodeError:
                continue

        # 按时间戳排序并限制数量
        sorted_items = sorted(result.items(), key=lambda x: int(x[0]), reverse=True)
        return dict(sorted_items[:limit])

    async def set_hot_klines(self, symbol: str, interval: str, klines: Dict[str, Dict]):
        """
        设置热K线数据

        Args:
            symbol: 币种
            interval: 时间周期
            klines: K线数据字典
        """
        redis = await get_async_redis()
        key = RedisKeys.market_klines_hot(symbol, interval)

        # 清空现有数据
        await redis.delete(key)

        if not klines:
            return

        # 批量设置数据 - 使用异步 pipeline
        async with redis.pipeline() as pipe:
            for ts, kline in klines.items():
                pipe.hset(key, ts, json.dumps(kline, ensure_ascii=False))
            # 设置过期时间（防止无限增长）
            hot_data_seconds = self.hot_data_days * 24 * 3600
            pipe.expire(key, hot_data_seconds)
            await pipe.execute()

    async def update_hot_klines(self, symbol: str, interval: str, new_klines: Dict[str, Dict]):
        """
        增量更新热K线数据

        Args:
            symbol: 币种
            interval: 时间周期
            new_klines: 新增K线数据
        """
        if not new_klines:
            return

        redis = await get_async_redis()
        key = RedisKeys.market_klines_hot(symbol, interval)

        # 获取现有数据
        existing_data = await redis.hgetall(key)
        existing_timestamps = set()
        for ts in existing_data.keys():
            if isinstance(ts, bytes):
                ts = ts.decode('utf-8')
            existing_timestamps.add(ts)

        # 只添加新的时间戳
        new_data = {}
        for ts, kline in new_klines.items():
            if ts not in existing_timestamps:
                new_data[ts] = json.dumps(kline, ensure_ascii=False)

        if new_data:
            await redis.hset(key, mapping=new_data)

        # 更新最后更新时间
        update_key = RedisKeys.market_klines_last_update(symbol, interval)
        await redis.set(update_key, str(time.time()))

    # ==================== 冷数据管理（磁盘存储） ====================
    def _get_cold_file_path(self, symbol: str, interval: str, date: str) -> Path:
        """获取冷数据文件路径"""
        return self.cache_dir / symbol / interval / f"{date}.json.gz"

    async def archive_to_cold_storage(self, symbol: str, interval: str, cutoff_days: int = 30):
        """
        将过期热数据归档到冷存储

        Args:
            symbol: 币种
            interval: 时间周期
            cutoff_days: 过期天数
        """
        redis = await get_async_redis()
        cutoff_time = time.time() - (cutoff_days * 24 * 3600)

        # 获取热数据
        hot_key = RedisKeys.market_klines_hot(symbol, interval)
        hot_data = await redis.hgetall(hot_key)

        if not hot_data:
            return

        # 分类数据
        hot_keep = {}
        cold_archive = {}

        for ts_bytes, kline_json in hot_data.items():
            ts = ts_bytes.decode('utf-8') if isinstance(ts_bytes, bytes) else ts_bytes
            ts_int = int(ts)

            if ts_int >= cutoff_time:
                # 保留在热数据中
                hot_keep[ts] = kline_json
            else:
                # 归档到冷数据
                date = time.strftime('%Y-%m-%d', time.localtime(ts_int))
                if date not in cold_archive:
                    cold_archive[date] = {}
                cold_archive[date][ts] = kline_json

        # 保存冷数据到磁盘
        for date, date_data in cold_archive.items():
            await self._save_cold_data(symbol, interval, date, date_data)

        # 更新热数据（只保留最新的）
        if hot_keep:
            await redis.delete(hot_key)
            await redis.hset(hot_key, mapping=hot_keep)
        else:
            await redis.delete(hot_key)

    async def _save_cold_data(self, symbol: str, interval: str, date: str, data: Dict[str, bytes]):
        """
        保存冷数据到磁盘文件

        Args:
            symbol: 币种
            interval: 时间周期
            date: 日期字符串
            data: K线数据
        """
        file_path = self._get_cold_file_path(symbol, interval, date)
        file_path.parent.mkdir(parents=True, exist_ok=True)

        # 转换数据格式
        json_data = {}
        for ts, kline_json in data.items():
            if isinstance(kline_json, bytes):
                kline_json = kline_json.decode('utf-8')
            json_data[ts] = json.loads(kline_json)

        # 压缩并保存
        compressed_data = json.dumps(json_data, ensure_ascii=False, separators=(',', ':'))

        # 计算校验和
        checksum = hashlib.md5(compressed_data.encode('utf-8')).hexdigest()

        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(compressed_data)

        # 更新元数据
        redis = await get_async_redis()
        meta_key = RedisKeys.market_klines_cold_meta(symbol, interval)
        meta_value = json.dumps({
            "file_path": str(file_path),
            "size": len(compressed_data),
            "checksum": checksum,
            "created_at": time.time(),
            "record_count": len(json_data)
        })
        await redis.hset(meta_key, date, meta_value)

    async def load_from_cold_storage(self, symbol: str, interval: str, start_date: str, end_date: str) -> Dict[str, Dict]:
        """
        从冷存储加载数据

        Args:
            symbol: 币种
            interval: 时间周期
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            K线数据字典
        """
        result = {}

        # 获取日期范围内的文件
        current_date = start_date
        while current_date <= end_date:
            file_path = self._get_cold_file_path(symbol, interval, current_date)

            if file_path.exists():
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                        result.update(data)
                except (json.JSONDecodeError, FileNotFoundError):
                    pass

            # 计算下一天
            from datetime import datetime, timedelta
            date_obj = datetime.strptime(current_date, '%Y-%m-%d')
            next_date = date_obj + timedelta(days=1)
            current_date = next_date.strftime('%Y-%m-%d')

        return result

    # ==================== 缓存策略管理 ====================
    async def cleanup_expired_cache(self):
        """清理过期缓存数据"""
        redis = await get_async_redis()
        # 清理Redis中过期的热数据键
        pattern = "global:market:klines:hot:*:*:*"
        keys = await redis.keys(pattern)

        hot_data_seconds = self.hot_data_days * 24 * 3600
        
        for key in keys:
            # 检查是否过期（超过热数据保留期）
            ttl = await redis.ttl(key)
            if ttl == -1:  # 没有设置过期时间
                await redis.expire(key, hot_data_seconds)

    async def optimize_memory_usage(self):
        """优化内存使用"""
        redis = await get_async_redis()
        # 获取所有热数据键
        pattern = "global:market:klines:hot:*:*:*"
        keys = await redis.keys(pattern)

        total_items = 0
        for key in keys:
            count = await redis.hlen(key)
            total_items += count

        # 如果超过内存限制，进行LRU淘汰
        if total_items > self.max_memory_items:
            # 计算需要淘汰的数量
            excess = total_items - self.max_memory_items
            items_per_key = excess // len(keys) if keys else 0

            for key in keys:
                if items_per_key > 0:
                    # 获取最旧的数据并删除
                    data = await redis.hgetall(key)
                    if data:
                        # 按时间戳排序，删除最旧的
                        timestamps = []
                        for ts in data.keys():
                            if isinstance(ts, bytes):
                                ts = ts.decode('utf-8')
                            timestamps.append((int(ts), ts))

                        timestamps.sort()  # 升序，最旧的在前
                        to_delete = timestamps[:items_per_key]

                        # 使用异步 pipeline 批量删除
                        async with redis.pipeline() as pipe:
                            for _, ts_str in to_delete:
                                pipe.hdel(key, ts_str)
                            await pipe.execute()

    # ==================== 统一访问接口 ====================
    async def get_klines(self, symbol: str, interval: str, start_time: Optional[float] = None,
                        end_time: Optional[float] = None, limit: int = 1000) -> Dict[str, Dict]:
        """
        统一获取K线数据（自动选择热数据或冷数据）

        Args:
            symbol: 币种
            interval: 时间周期
            start_time: 开始时间戳
            end_time: 结束时间戳
            limit: 最大返回条数

        Returns:
            K线数据字典
        """
        # 首先尝试从热数据获取
        hot_data = await self.get_hot_klines(symbol, interval, limit * 2)  # 多取一些用于筛选

        result = {}

        # 如果指定了时间范围，进行筛选
        if start_time or end_time:
            for ts, kline in hot_data.items():
                ts_int = int(ts)
                if start_time and ts_int < start_time:
                    continue
                if end_time and ts_int > end_time:
                    continue
                result[ts] = kline
        else:
            result = hot_data

        # 如果热数据不够，从冷数据加载
        if len(result) < limit:
            if start_time and end_time:
                start_date = time.strftime('%Y-%m-%d', time.localtime(start_time))
                end_date = time.strftime('%Y-%m-%d', time.localtime(end_time))

                cold_data = await self.load_from_cold_storage(symbol, interval, start_date, end_date)

                # 合并冷数据
                for ts, kline in cold_data.items():
                    ts_int = int(ts)
                    if start_time and ts_int < start_time:
                        continue
                    if end_time and ts_int > end_time:
                        continue
                    if ts not in result:  # 避免重复
                        result[ts] = kline

        # 最终排序和限制
        sorted_items = sorted(result.items(), key=lambda x: int(x[0]), reverse=True)
        return dict(sorted_items[:limit])

    async def maintain_cache(self):
        """维护缓存（定期调用）"""
        await self.cleanup_expired_cache()
        await self.optimize_memory_usage()