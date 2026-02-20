# core/binance_public_cache.py
"""
Binance 公共数据全局缓存

所有用户共享，避免重复请求：
- futures_exchange_info: 1小时 TTL
- futures_leverage_bracket: 24小时 TTL
- 标记价格: 使用 WebSocket 数据（Redis）

设计原则：
- 单例模式，全局共享
- 线程安全
- 支持同步和异步调用
- 优先使用 Redis 缓存，内存缓存作为备份
"""

import json
import logging
import threading
import time
from typing import Dict, Optional, List, Any
from concurrent.futures import ThreadPoolExecutor

logger = logging.getLogger(__name__)


class BinancePublicCache:
    """
    Binance 公共数据缓存（单例）
    
    缓存策略：
    - exchange_info: Redis + 内存，1小时 TTL
    - leverage_bracket: Redis + 内存，24小时 TTL
    - mark_price: 直接从 Redis 读取 WebSocket 数据
    """
    
    _instance: Optional['BinancePublicCache'] = None
    _lock = threading.Lock()
    
    # 缓存 TTL（秒）
    EXCHANGE_INFO_TTL = 3600  # 1小时
    LEVERAGE_BRACKET_TTL = 86400  # 24小时
    
    # Redis Key 前缀
    REDIS_KEY_EXCHANGE_INFO = "binance:public:exchange_info"
    REDIS_KEY_LEVERAGE_BRACKET = "binance:public:leverage_bracket"
    
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
        
        self._initialized = True
        
        # 内存缓存
        self._exchange_info: Dict[str, Any] = {}
        self._exchange_info_time: float = 0
        
        self._leverage_brackets: Dict[str, List] = {}
        self._leverage_brackets_time: float = 0
        
        # 线程锁
        self._exchange_info_lock = threading.Lock()
        self._leverage_lock = threading.Lock()
        
        # 线程池（用于同步转异步）
        self._executor = ThreadPoolExecutor(max_workers=2)
        
        # Binance 客户端（延迟初始化）
        self._client = None
        self._is_testnet = False
        
        logger.info("[BinancePublicCache] 初始化完成")
    
    @classmethod
    def get_instance(cls) -> 'BinancePublicCache':
        """获取单例实例"""
        return cls()
    
    def _get_redis(self):
        """获取 Redis 连接 - P2 Fix: 使用共享连接池"""
        try:
            from core.database import redis_client
            return redis_client
        except Exception as e:
            logger.warning(f"[BinancePublicCache] 获取 Redis 连接失败: {e}")
            return None
    
    def _get_client(self):
        """获取 Binance 客户端（延迟初始化）"""
        if self._client is None:
            try:
                from binance.client import Client
                from core.config import BINANCE_ENVIRONMENT
                self._is_testnet = BINANCE_ENVIRONMENT
                # 公共 API 不需要 API Key
                self._client = Client("", "", testnet=self._is_testnet)
                logger.info(f"[BinancePublicCache] Binance 客户端初始化完成, testnet={self._is_testnet}")
            except Exception as e:
                logger.error(f"[BinancePublicCache] Binance 客户端初始化失败: {e}")
                raise
        return self._client
    
    # ==================== Exchange Info ====================
    
    def get_exchange_info(self, force_refresh: bool = False) -> Dict[str, Any]:
        """
        获取交易所信息（同步）
        
        优先级：内存缓存 -> Redis 缓存 -> API 请求
        """
        now = time.time()
        
        # 1. 检查内存缓存
        if not force_refresh and self._exchange_info and (now - self._exchange_info_time) < self.EXCHANGE_INFO_TTL:
            return self._exchange_info
        
        with self._exchange_info_lock:
            # 双重检查
            if not force_refresh and self._exchange_info and (now - self._exchange_info_time) < self.EXCHANGE_INFO_TTL:
                return self._exchange_info
            
            # 2. 尝试从 Redis 获取
            rds = self._get_redis()
            if rds:
                try:
                    cached = rds.get(self.REDIS_KEY_EXCHANGE_INFO)
                    if cached:
                        data = json.loads(cached)
                        self._exchange_info = data
                        self._exchange_info_time = now
                        logger.debug("[BinancePublicCache] 从 Redis 获取 exchange_info")
                        return self._exchange_info
                except Exception as e:
                    logger.warning(f"[BinancePublicCache] Redis 读取 exchange_info 失败: {e}")
            
            # 3. 从 API 获取
            try:
                client = self._get_client()
                data = client.futures_exchange_info()
                
                # 构建 symbol -> info 映射
                symbol_info = {}
                for s in data.get('symbols', []):
                    symbol_info[s['symbol']] = s
                
                self._exchange_info = symbol_info
                self._exchange_info_time = now
                
                # 写入 Redis
                if rds:
                    try:
                        rds.setex(
                            self.REDIS_KEY_EXCHANGE_INFO,
                            self.EXCHANGE_INFO_TTL,
                            json.dumps(symbol_info)
                        )
                        logger.info(f"[BinancePublicCache] exchange_info 已缓存到 Redis, {len(symbol_info)} 个交易对")
                    except Exception as e:
                        logger.warning(f"[BinancePublicCache] Redis 写入 exchange_info 失败: {e}")
                
                return self._exchange_info
                
            except Exception as e:
                logger.error(f"[BinancePublicCache] 获取 exchange_info 失败: {e}")
                # 返回旧缓存（如果有）
                if self._exchange_info:
                    return self._exchange_info
                raise
    
    async def get_exchange_info_async(self, force_refresh: bool = False) -> Dict[str, Any]:
        """获取交易所信息（异步）"""
        import asyncio
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self._executor, self.get_exchange_info, force_refresh)
    
    def get_symbol_info(self, symbol: str) -> Dict[str, Any]:
        """获取单个交易对信息（同步）"""
        exchange_info = self.get_exchange_info()
        return exchange_info.get(symbol.upper(), {})
    
    async def get_symbol_info_async(self, symbol: str) -> Dict[str, Any]:
        """获取单个交易对信息（异步）"""
        exchange_info = await self.get_exchange_info_async()
        return exchange_info.get(symbol.upper(), {})
    
    # ==================== Leverage Bracket ====================
    
    def get_leverage_bracket_from_cache(self, symbol: str) -> List[Dict]:
        """
        仅从缓存获取杠杆档位信息（不触发 API 调用）
        
        用于避免公共缓存调用私有 API 的问题
        返回空列表表示缓存未命中
        """
        symbol = symbol.upper()
        now = time.time()
        
        # 1. 检查内存缓存
        if symbol in self._leverage_brackets:
            if (now - self._leverage_brackets_time) < self.LEVERAGE_BRACKET_TTL:
                return self._leverage_brackets[symbol]
        
        # 2. 尝试从 Redis 获取
        rds = self._get_redis()
        redis_key = f"{self.REDIS_KEY_LEVERAGE_BRACKET}:{symbol}"
        
        if rds:
            try:
                cached = rds.get(redis_key)
                if cached:
                    data = json.loads(cached)
                    # 更新内存缓存
                    with self._leverage_lock:
                        self._leverage_brackets[symbol] = data
                        self._leverage_brackets_time = now
                    logger.debug(f"[BinancePublicCache] 从 Redis 获取 leverage_bracket: {symbol}")
                    return data
            except Exception as e:
                logger.warning(f"[BinancePublicCache] Redis 读取 leverage_bracket 失败: {e}")
        
        # 缓存未命中，返回空列表（不调用 API）
        return []
    
    def set_leverage_bracket_cache(self, symbol: str, brackets: List[Dict]):
        """
        设置杠杆档位缓存（由外部调用者提供数据）
        
        用于让拥有 API Key 的调用者更新缓存
        """
        symbol = symbol.upper()
        now = time.time()
        
        with self._leverage_lock:
            self._leverage_brackets[symbol] = brackets
            self._leverage_brackets_time = now
        
        # 写入 Redis
        rds = self._get_redis()
        if rds:
            try:
                redis_key = f"{self.REDIS_KEY_LEVERAGE_BRACKET}:{symbol}"
                rds.setex(
                    redis_key,
                    self.LEVERAGE_BRACKET_TTL,
                    json.dumps(brackets)
                )
                logger.debug(f"[BinancePublicCache] leverage_bracket 已缓存: {symbol}")
            except Exception as e:
                logger.warning(f"[BinancePublicCache] Redis 写入 leverage_bracket 失败: {e}")
    
    def get_leverage_bracket(self, symbol: str, force_refresh: bool = False) -> List[Dict]:
        """
        获取杠杆档位信息（同步）
        
        注意：leverage_bracket 是私有 API，需要 API Key。
        此方法仅从缓存获取，不会调用 API。
        如果缓存未命中，返回空列表。
        
        调用者应使用 BinanceExchange._get_max_leverage() 来获取杠杆信息，
        该方法会使用用户的 API Key 调用 API 并更新缓存。
        
        Args:
            symbol: 交易对符号
            force_refresh: 已废弃，保留参数以兼容旧代码
        """
        # 直接调用 get_leverage_bracket_from_cache，避免重复代码
        return self.get_leverage_bracket_from_cache(symbol)
    
    async def get_leverage_bracket_async(self, symbol: str, force_refresh: bool = False) -> List[Dict]:
        """获取杠杆档位信息（异步）"""
        import asyncio
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            self._executor, 
            lambda: self.get_leverage_bracket(symbol, force_refresh)
        )
    
    def get_max_leverage(self, symbol: str) -> int:
        """获取最大杠杆倍数"""
        brackets = self.get_leverage_bracket(symbol)
        if brackets:
            # brackets 按 notionalFloor 升序排列，第一个是最小档位，杠杆最高
            return int(brackets[0].get('initialLeverage', 20))
        return 20  # 默认值
    
    async def get_max_leverage_async(self, symbol: str) -> int:
        """获取最大杠杆倍数（异步）"""
        brackets = await self.get_leverage_bracket_async(symbol)
        if brackets:
            return int(brackets[0].get('initialLeverage', 20))
        return 20
    
    # ==================== Mark Price ====================
    
    def get_mark_price(self, symbol: str) -> Optional[float]:
        """
        获取标记价格（从 Redis WebSocket 数据）
        
        数据来源：WebSocket 推送，存储在 Redis
        """
        symbol = symbol.upper()
        rds = self._get_redis()
        
        if not rds:
            return None
        
        try:
            from core.database import RedisKeys
            key = RedisKeys.market_prices(symbol)
            data = rds.get(key)
            
            if data:
                parsed = json.loads(data)
                mark_price = parsed.get('markPrice')
                if mark_price:
                    return float(mark_price)
        except Exception as e:
            logger.debug(f"[BinancePublicCache] 获取 mark_price 失败 {symbol}: {e}")
        
        return None
    
    async def get_mark_price_async(self, symbol: str) -> Optional[float]:
        """获取标记价格（异步）"""
        import asyncio
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self._executor, self.get_mark_price, symbol)
    
    def get_all_mark_prices(self) -> Dict[str, float]:
        """
        获取所有标记价格（从 Redis）
        
        返回: {symbol: price, ...}
        """
        rds = self._get_redis()
        if not rds:
            return {}
        
        try:
            # 扫描所有标记价格 key (格式: global:market:prices:{symbol})
            pattern = "global:market:prices:*"
            prices = {}
            
            for key in rds.scan_iter(match=pattern, count=100):
                try:
                    data = rds.get(key)
                    if data:
                        parsed = json.loads(data)
                        mark_price = parsed.get('markPrice')
                        if mark_price:
                            # 从 key 提取 symbol: global:market:prices:BTCUSDT -> BTCUSDT
                            symbol = key.split(':')[-1]
                            prices[symbol] = float(mark_price)
                except Exception:
                    continue
            
            return prices
        except Exception as e:
            logger.warning(f"[BinancePublicCache] 获取所有 mark_price 失败: {e}")
            return {}
    
    # ==================== 工具方法 ====================
    
    def clear_cache(self):
        """清除所有缓存"""
        with self._exchange_info_lock:
            self._exchange_info = {}
            self._exchange_info_time = 0
        
        with self._leverage_lock:
            self._leverage_brackets = {}
            self._leverage_brackets_time = 0
        
        # 清除 Redis 缓存
        rds = self._get_redis()
        if rds:
            try:
                rds.delete(self.REDIS_KEY_EXCHANGE_INFO)
                for key in rds.scan_iter(match=f"{self.REDIS_KEY_LEVERAGE_BRACKET}:*"):
                    rds.delete(key)
            except Exception as e:
                logger.warning(f"[BinancePublicCache] 清除 Redis 缓存失败: {e}")
        
        logger.info("[BinancePublicCache] 缓存已清除")
    
    def get_stats(self) -> Dict[str, Any]:
        """获取缓存统计信息"""
        now = time.time()
        return {
            "exchange_info_symbols": len(self._exchange_info),
            "exchange_info_age_seconds": now - self._exchange_info_time if self._exchange_info_time else None,
            "leverage_brackets_symbols": len(self._leverage_brackets),
            "leverage_brackets_age_seconds": now - self._leverage_brackets_time if self._leverage_brackets_time else None,
        }
    
    def close(self):
        """关闭资源（在程序退出时调用）"""
        if self._executor:
            self._executor.shutdown(wait=False)
            self._executor = None
            logger.info("[BinancePublicCache] 线程池已关闭")


# ==================== 便捷函数 ====================

def get_binance_public_cache() -> BinancePublicCache:
    """获取 Binance 公共数据缓存实例"""
    return BinancePublicCache.get_instance()
