# local_cache.py
"""
本地缓存层 - 1000+ 用户规模优化

问题：
- 频繁读取 Redis 数据（mark price, 用户配置等）
- 1000 用户场景下 Redis QPS 过高

优化方案：
- 添加本地内存缓存层
- TTL 控制缓存有效期
- LRU 淘汰策略

缓存类型：
1. MarkPriceCache - 标记价格缓存（TTL: 500ms）
2. UserConfigCache - 用户配置缓存（TTL: 5min）
3. ExchangeInfoCache - 交易所信息缓存（TTL: 1hour）
"""

import logging
import threading
import time
from collections import OrderedDict
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, Optional, TypeVar, Generic

logger = logging.getLogger(__name__)

T = TypeVar('T')


@dataclass
class CacheEntry(Generic[T]):
    """缓存条目"""
    value: T
    created_at: float
    ttl: float
    
    def is_expired(self) -> bool:
        return time.time() - self.created_at > self.ttl


class LRUCache(Generic[T]):
    """
    LRU 缓存
    
    特点：
    - 线程安全
    - TTL 过期
    - 最大容量限制
    """
    
    def __init__(
        self,
        max_size: int = 1000,
        default_ttl: float = 60.0,
        name: str = "LRUCache"
    ):
        self._cache: OrderedDict[str, CacheEntry[T]] = OrderedDict()
        self._lock = threading.Lock()
        self._max_size = max_size
        self._default_ttl = default_ttl
        self._name = name
        
        # 统计
        self._hits = 0
        self._misses = 0
    
    def get(self, key: str) -> Optional[T]:
        """获取缓存值"""
        with self._lock:
            entry = self._cache.get(key)
            
            if entry is None:
                self._misses += 1
                return None
            
            if entry.is_expired():
                del self._cache[key]
                self._misses += 1
                return None
            
            # 移动到末尾（最近使用）
            self._cache.move_to_end(key)
            self._hits += 1
            return entry.value
    
    def set(self, key: str, value: T, ttl: Optional[float] = None) -> None:
        """设置缓存值"""
        if ttl is None:
            ttl = self._default_ttl
        
        with self._lock:
            # 如果已存在，先删除
            if key in self._cache:
                del self._cache[key]
            
            # 检查容量
            while len(self._cache) >= self._max_size:
                # 删除最旧的
                self._cache.popitem(last=False)
            
            # 添加新条目
            self._cache[key] = CacheEntry(
                value=value,
                created_at=time.time(),
                ttl=ttl
            )
    
    def delete(self, key: str) -> None:
        """删除缓存"""
        with self._lock:
            if key in self._cache:
                del self._cache[key]
    
    def clear(self) -> None:
        """清空缓存"""
        with self._lock:
            self._cache.clear()
    
    def cleanup_expired(self) -> int:
        """清理过期条目"""
        removed = 0
        with self._lock:
            expired_keys = [
                k for k, v in self._cache.items()
                if v.is_expired()
            ]
            for k in expired_keys:
                del self._cache[k]
                removed += 1
        return removed
    
    def get_stats(self) -> dict:
        """获取统计信息"""
        with self._lock:
            total = self._hits + self._misses
            hit_rate = self._hits / total if total > 0 else 0
            return {
                "name": self._name,
                "size": len(self._cache),
                "max_size": self._max_size,
                "hits": self._hits,
                "misses": self._misses,
                "hit_rate": f"{hit_rate:.2%}",
            }


# ========== 专用缓存 ==========

class MarkPriceCache:
    """
    标记价格缓存
    
    特点：
    - 短 TTL（500ms）
    - 高频访问优化
    """
    
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
        
        self._cache = LRUCache[dict](
            max_size=500,  # 最多缓存 500 个 symbol
            default_ttl=0.5,  # 500ms TTL
            name="MarkPriceCache"
        )
        self._initialized = True
    
    def get(self, symbol: str) -> Optional[dict]:
        """获取标记价格"""
        return self._cache.get(symbol.upper())
    
    def set(self, symbol: str, data: dict) -> None:
        """设置标记价格"""
        self._cache.set(symbol.upper(), data)
    
    def get_mark_price(self, symbol: str) -> Optional[Decimal]:
        """获取标记价格（Decimal）"""
        data = self.get(symbol)
        if data and "markPrice" in data:
            try:
                return Decimal(str(data["markPrice"]))
            except (ValueError, TypeError, InvalidOperation):
                pass
        return None
    
    def get_stats(self) -> dict:
        return self._cache.get_stats()


class UserConfigCache:
    """
    用户配置缓存
    
    特点：
    - 中等 TTL（5分钟）
    - 按用户隔离
    """
    
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
        
        self._cache = LRUCache[dict](
            max_size=2000,  # 最多缓存 2000 个用户
            default_ttl=300.0,  # 5 分钟 TTL
            name="UserConfigCache"
        )
        self._initialized = True
    
    def get(self, uid: str, key: str) -> Optional[Any]:
        """获取用户配置"""
        cache_key = f"{uid}:{key}"
        return self._cache.get(cache_key)
    
    def set(self, uid: str, key: str, value: Any, ttl: Optional[float] = None) -> None:
        """设置用户配置"""
        cache_key = f"{uid}:{key}"
        self._cache.set(cache_key, value, ttl)
    
    def invalidate(self, uid: str, key: Optional[str] = None) -> None:
        """使缓存失效"""
        if key:
            self._cache.delete(f"{uid}:{key}")
        # 如果没有指定 key，需要遍历删除（较慢，但不常用）
    
    def get_stats(self) -> dict:
        return self._cache.get_stats()


class ExchangeInfoCache:
    """
    交易所信息缓存
    
    特点：
    - 长 TTL（1小时）
    - 全局共享
    """
    
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
        
        self._cache = LRUCache[Any](
            max_size=100,  # 交易所信息不多
            default_ttl=3600.0,  # 1 小时 TTL
            name="ExchangeInfoCache"
        )
        self._initialized = True
    
    def get(self, exchange: str, key: str) -> Optional[Any]:
        """获取交易所信息"""
        cache_key = f"{exchange}:{key}"
        return self._cache.get(cache_key)
    
    def set(self, exchange: str, key: str, value: Any, ttl: Optional[float] = None) -> None:
        """设置交易所信息"""
        cache_key = f"{exchange}:{key}"
        self._cache.set(cache_key, value, ttl)
    
    def get_symbol_info(self, exchange: str, symbol: str) -> Optional[dict]:
        """获取交易对信息"""
        return self.get(exchange, f"symbol:{symbol}")
    
    def set_symbol_info(self, exchange: str, symbol: str, info: dict) -> None:
        """设置交易对信息"""
        self.set(exchange, f"symbol:{symbol}", info)
    
    def get_stats(self) -> dict:
        return self._cache.get_stats()


# ========== 全局单例获取函数 ==========

def get_mark_price_cache() -> MarkPriceCache:
    """获取标记价格缓存"""
    return MarkPriceCache()


def get_user_config_cache() -> UserConfigCache:
    """获取用户配置缓存"""
    return UserConfigCache()


def get_exchange_info_cache() -> ExchangeInfoCache:
    """获取交易所信息缓存"""
    return ExchangeInfoCache()


def get_all_cache_stats() -> dict:
    """获取所有缓存统计"""
    return {
        "mark_price": get_mark_price_cache().get_stats(),
        "user_config": get_user_config_cache().get_stats(),
        "exchange_info": get_exchange_info_cache().get_stats(),
    }


# ========== 缓存清理任务 ==========

_cleanup_thread: Optional[threading.Thread] = None
_cleanup_stop = threading.Event()


def start_cache_cleanup(interval: float = 60.0) -> None:
    """启动缓存清理线程"""
    global _cleanup_thread
    
    if _cleanup_thread and _cleanup_thread.is_alive():
        return
    
    _cleanup_stop.clear()
    
    def _cleanup_loop():
        while not _cleanup_stop.is_set():
            try:
                # 清理各缓存的过期条目
                mark_removed = get_mark_price_cache()._cache.cleanup_expired()
                user_removed = get_user_config_cache()._cache.cleanup_expired()
                exchange_removed = get_exchange_info_cache()._cache.cleanup_expired()
                
                total = mark_removed + user_removed + exchange_removed
                if total > 0:
                    logger.debug(f"[LocalCache] 清理过期条目: {total}")
                    
            except Exception as e:
                logger.error(f"[LocalCache] 清理失败: {e}")
            
            _cleanup_stop.wait(interval)
    
    _cleanup_thread = threading.Thread(
        target=_cleanup_loop,
        name="cache-cleanup",
        daemon=True
    )
    _cleanup_thread.start()
    logger.info("[LocalCache] 缓存清理线程已启动")


def stop_cache_cleanup() -> None:
    """停止缓存清理线程"""
    global _cleanup_thread
    _cleanup_stop.set()
    if _cleanup_thread:
        _cleanup_thread.join(timeout=5.0)
        _cleanup_thread = None
