# core/strategy_cache.py
"""
AI 策略配置缓存（纯内存 LRU + TTL）

功能：
1. 缓存策略配置（避免重复数据库查询）
2. 缓存 LLM 客户端（避免重复创建）
3. LRU 淘汰 + TTL 过期，防止内存溢出
4. 支持主动失效（策略更新时立即生效）

内存保护：
- 策略配置缓存：最多 5000 条，TTL 1 小时
- LLM 客户端缓存：最多 2000 个，TTL 30 分钟
- 依赖 LRU 淘汰机制，无定期清理任务
"""

import threading
import time
import logging
from collections import OrderedDict
from dataclasses import dataclass
from typing import Dict, Optional, Any, List

logger = logging.getLogger(__name__)


@dataclass
class CacheEntry:
    """缓存条目"""
    data: Any
    created_at: float
    last_accessed_at: float
    
    def is_expired(self, ttl_seconds: float) -> bool:
        return time.time() - self.last_accessed_at > ttl_seconds


class LRUCache:
    """
    LRU 缓存（带 TTL）
    
    特点：
    - 容量限制：超过 max_size 时淘汰最久未使用的条目
    - TTL 过期：超过 ttl_seconds 未访问的条目自动过期
    - 线程安全：使用 threading.Lock
    """
    
    def __init__(self, max_size: int = 5000, ttl_seconds: float = 3600):
        self._cache: OrderedDict[str, CacheEntry] = OrderedDict()
        self._max_size = max_size
        self._ttl_seconds = ttl_seconds
        self._lock = threading.Lock()
        
        # 统计
        self._hits = 0
        self._misses = 0
    
    def get(self, key: str) -> Optional[Any]:
        """获取缓存（命中时移到末尾，表示最近使用）"""
        with self._lock:
            if key not in self._cache:
                self._misses += 1
                return None
            
            entry = self._cache[key]
            
            # 检查是否过期
            if entry.is_expired(self._ttl_seconds):
                del self._cache[key]
                self._misses += 1
                return None
            
            # 更新访问时间，移到末尾（LRU）
            entry.last_accessed_at = time.time()
            self._cache.move_to_end(key)
            self._hits += 1
            
            return entry.data
    
    def peek(self, key: str) -> Optional[Any]:
        """查看缓存（不更新访问时间，不影响 LRU 顺序）"""
        with self._lock:
            if key not in self._cache:
                return None
            
            entry = self._cache[key]
            
            # 检查是否过期
            if entry.is_expired(self._ttl_seconds):
                return None
            
            return entry.data
    
    def set(self, key: str, value: Any):
        """设置缓存"""
        with self._lock:
            now = time.time()
            
            if key in self._cache:
                # 更新现有条目
                self._cache[key].data = value
                self._cache[key].last_accessed_at = now
                self._cache.move_to_end(key)
            else:
                # 新增条目，先检查容量
                while len(self._cache) >= self._max_size:
                    oldest_key = next(iter(self._cache))
                    del self._cache[oldest_key]
                    logger.debug(f"LRU 淘汰: {oldest_key}")
                
                self._cache[key] = CacheEntry(
                    data=value,
                    created_at=now,
                    last_accessed_at=now,
                )
    
    def delete(self, key: str) -> bool:
        """删除缓存"""
        with self._lock:
            if key in self._cache:
                del self._cache[key]
                return True
            return False
    
    def delete_prefix(self, prefix: str) -> int:
        """删除指定前缀的所有缓存"""
        with self._lock:
            keys_to_delete = [k for k in self._cache.keys() if k.startswith(prefix)]
            for key in keys_to_delete:
                del self._cache[key]
            return len(keys_to_delete)
    
    def pop(self, key: str) -> Optional[Any]:
        """弹出缓存条目（返回数据并删除）"""
        with self._lock:
            if key in self._cache:
                entry = self._cache.pop(key)
                return entry.data
            return None
    
    def pop_prefix(self, prefix: str) -> List[Any]:
        """弹出指定前缀的所有缓存条目"""
        with self._lock:
            keys_to_pop = [k for k in self._cache.keys() if k.startswith(prefix)]
            results = []
            for key in keys_to_pop:
                entry = self._cache.pop(key)
                results.append(entry.data)
            return results
    
    def size(self) -> int:
        """当前缓存大小"""
        with self._lock:
            return len(self._cache)
    
    def get_stats(self) -> Dict:
        """获取统计信息"""
        with self._lock:
            total = self._hits + self._misses
            hit_rate = (self._hits / total * 100) if total > 0 else 0
            return {
                "size": len(self._cache),
                "max_size": self._max_size,
                "ttl_seconds": self._ttl_seconds,
                "hits": self._hits,
                "misses": self._misses,
                "hit_rate": f"{hit_rate:.1f}%",
            }


class StrategyCache:
    """
    AI 策略缓存（单例）
    
    内存保护机制：
    1. 策略配置缓存：最多 5000 条，TTL 1 小时
    2. LLM 客户端不缓存，每次创建新实例并在使用后关闭（避免连接泄漏）
    """
    
    _instance = None
    _lock = threading.Lock()
    
    # 配置参数
    STRATEGY_CACHE_MAX_SIZE = 5000      # 最多缓存 5000 个策略配置
    STRATEGY_CACHE_TTL = 3600           # 策略配置 TTL: 1 小时
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if getattr(self, '_initialized', False):
            return
        
        # 策略配置缓存
        self._strategy_cache = LRUCache(
            max_size=self.STRATEGY_CACHE_MAX_SIZE,
            ttl_seconds=self.STRATEGY_CACHE_TTL,
        )
        
        self._initialized = True
        logger.info(
            f"StrategyCache 已初始化: "
            f"策略缓存(max={self.STRATEGY_CACHE_MAX_SIZE}, ttl={self.STRATEGY_CACHE_TTL}s), "
            f"LLM客户端不缓存(每次新建)"
        )
    
    def _make_key(self, uid: str, strategy_id: str) -> str:
        return f"{uid}:{strategy_id}"
    
    def get_strategy(self, uid: str, strategy_id: str) -> Optional[Dict]:
        """
        获取策略配置（带缓存）
        
        流程：缓存命中 → 返回 | 缓存未命中 → 查数据库 → 写缓存 → 返回
        """
        key = self._make_key(uid, strategy_id)
        
        # 尝试从缓存获取
        cached = self._strategy_cache.get(key)
        if cached is not None:
            return cached
        
        # 缓存未命中，查数据库
        from core.user_db import config_loader
        strategy = config_loader.get_user_ai_strategy(uid, strategy_id)
        
        if strategy:
            self._strategy_cache.set(key, strategy)
            logger.debug(f"[{uid}] 策略 {strategy_id} 已缓存")
        
        return strategy
    
    def get_llm_client(
        self, 
        uid: str, 
        strategy_id: str,
        temperature: float = None,
        top_p: float = None,
        max_tokens: int = 65536,
    ) -> Optional[Any]:
        """
        创建 LLM 客户端（不缓存，每次新建）
        
        注意：调用方需要在使用完毕后调用 client.close() 或 client.close_sync() 关闭客户端
        
        Args:
            uid: 用户 ID
            strategy_id: 策略 ID
            temperature: 温度参数，None 表示使用提供商默认值
            top_p: top_p 参数，None 表示使用提供商默认值
            max_tokens: 最大 token 数
        """
        # 获取策略配置（这个有缓存）
        strategy = self.get_strategy(uid, strategy_id)
        if not strategy:
            logger.warning(f"[{uid}] 策略 {strategy_id} 不存在")
            return None
        
        # 每次创建新的 LLM 客户端
        from llm.llm_client import create_llm_client
        
        provider = strategy.get('llm_provider', 'anthropic')
        model = strategy.get('llm_model', '')
        base_url = strategy.get('llm_base_url', '')
        api_key = strategy.get('llm_api_key', '')
        
        # 优先使用策略中保存的参数，其次使用传入的参数
        final_temperature = strategy.get('temperature') if strategy.get('temperature') is not None else temperature
        final_top_p = strategy.get('top_p') if strategy.get('top_p') is not None else top_p
        final_max_tokens = strategy.get('max_tokens') or max_tokens
        
        logger.info(f"[{uid}] 创建 LLM 客户端: strategy={strategy_id}, provider={provider}, model={model}, temperature={final_temperature}, top_p={final_top_p}, max_tokens={final_max_tokens}")
        
        # 构建参数，只传递非 None 的值
        kwargs = {"max_tokens": final_max_tokens}
        if final_temperature is not None:
            kwargs["temperature"] = final_temperature
        if final_top_p is not None:
            kwargs["top_p"] = final_top_p
        
        client = create_llm_client(
            provider=provider,
            model=model,
            api_key=api_key,
            base_url=base_url,
            **kwargs,
        )
        
        return client
    
    def invalidate(self, uid: str, strategy_id: str):
        """
        失效单个策略的缓存
        
        在策略更新/删除时调用
        """
        key = self._make_key(uid, strategy_id)
        
        # 清除策略配置缓存
        if self._strategy_cache.delete(key):
            logger.info(f"[{uid}] 策略 {strategy_id} 配置缓存已失效")
    
    def invalidate_user(self, uid: str):
        """
        失效用户的所有策略缓存
        
        在用户配置大规模变更时调用
        """
        prefix = f"{uid}:"
        
        # 清除策略配置缓存
        strategy_count = self._strategy_cache.delete_prefix(prefix)
        
        if strategy_count > 0:
            logger.info(f"[{uid}] 用户策略缓存已失效 (策略:{strategy_count})")
    
    def preload_user_strategies(self, uid: str) -> int:
        """
        预加载用户的所有策略配置
        
        用于系统启动时预热缓存
        """
        from core.user_db import config_loader
        
        strategies = config_loader.get_user_ai_strategies_with_keys(uid)
        count = 0
        
        for s in strategies:
            strategy_id = s.get('id')
            if strategy_id:
                key = self._make_key(uid, strategy_id)
                self._strategy_cache.set(key, s)
                count += 1
        
        return count
    
    def preload_all_active_users(self) -> Dict[str, int]:
        """
        预加载所有活跃用户的策略配置
        
        用于系统启动时预热缓存
        """
        from core.user_db import config_loader
        
        active_users = config_loader.get_all_active_users()
        total_strategies = 0
        
        for uid in active_users:
            count = self.preload_user_strategies(uid)
            total_strategies += count
        
        logger.info(f"策略缓存预热完成: {len(active_users)} 用户, {total_strategies} 策略")
        
        return {
            "total_users": len(active_users),
            "total_strategies": total_strategies,
        }
    
    def get_stats(self) -> Dict:
        """获取缓存统计"""
        return {
            "strategy_cache": self._strategy_cache.get_stats(),
        }


# 全局单例
strategy_cache = StrategyCache()


def get_strategy_cache() -> StrategyCache:
    """获取策略缓存单例"""
    return strategy_cache
