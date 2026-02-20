# core/symbol_availability.py
"""
交易所符号可用性管理

功能：
1. 获取各交易所支持的可交易符号列表
2. Redis 缓存 + 定时刷新
3. 在 AI 喂数据时过滤不支持的符号

设计：
- 每个交易所的符号列表单独缓存
- 缓存 TTL = 1 小时
- 服务启动时加载，之后定时刷新
"""

import asyncio
import logging
import threading
import time
from typing import Dict, List, Set, Optional
from datetime import datetime

logger = logging.getLogger(__name__)

# Redis key 前缀
REDIS_KEY_PREFIX = "exchange_symbols:"
CACHE_TTL = 3600  # 1小时


class SymbolAvailabilityManager:
    """
    交易所符号可用性管理器
    
    使用方式：
        manager = SymbolAvailabilityManager()
        await manager.refresh_all()  # 启动时刷新
        
        # 过滤符号
        filtered = manager.filter_symbols("binance", ["BTCUSDT", "AIAUSDT", "FAKEUSDT"])
        # 返回: ["BTCUSDT", "AIAUSDT"]  (假设 FAKEUSDT 不存在)
    """
    
    def __init__(self):
        # 内存缓存: {exchange: set(symbols)}
        self._cache: Dict[str, Set[str]] = {}
        self._last_refresh: Dict[str, float] = {}
        # P1 Fix: 延迟初始化 asyncio.Lock，避免在模块导入时创建
        self._refresh_lock: Optional[asyncio.Lock] = None
        self._refresh_lock_init = threading.Lock()
        self._http_session: Optional["aiohttp.ClientSession"] = None
    
    def _get_refresh_lock(self) -> asyncio.Lock:
        """
        获取刷新锁（延迟初始化）
        
        P1 Fix: 确保 asyncio.Lock 在事件循环中创建，而不是模块导入时
        """
        if self._refresh_lock is None:
            with self._refresh_lock_init:
                if self._refresh_lock is None:
                    self._refresh_lock = asyncio.Lock()
        return self._refresh_lock
    
    async def _get_http_session(self):
        """获取或创建 HTTP session"""
        import aiohttp
        
        if self._http_session is None or self._http_session.closed:
            self._http_session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=30)
            )
        return self._http_session
    
    async def close(self):
        """关闭 HTTP session"""
        if self._http_session and not self._http_session.closed:
            await self._http_session.close()
            self._http_session = None
    
    def filter_symbols(self, exchange: str, symbols: List[str]) -> List[str]:
        """
        过滤出交易所支持的符号
        
        Args:
            exchange: 交易所名称 (binance, bitget, okx, hyperliquid)
            symbols: 待过滤的符号列表
        
        Returns:
            交易所支持的符号列表
        """
        exchange = exchange.lower()
        available = self._cache.get(exchange, set())
        
        if not available:
            # 如果没有缓存数据，返回所有符号（降级策略，避免阻塞）
            logger.warning(f"[SymbolAvailability] {exchange} 无缓存数据，跳过过滤")
            return symbols
        
        filtered = [s for s in symbols if s in available]
        
        # 记录被过滤掉的符号
        removed = set(symbols) - set(filtered)
        if removed:
            logger.info(f"[SymbolAvailability] {exchange} 过滤了 {len(removed)} 个不支持的符号: {list(removed)[:5]}...")
        
        return filtered
    
    def is_symbol_available(self, exchange: str, symbol: str) -> bool:
        """检查单个符号是否可用"""
        exchange = exchange.lower()
        available = self._cache.get(exchange, set())
        
        if not available:
            # 没有缓存数据，默认返回 True（降级策略）
            return True
        
        return symbol in available
    
    def get_available_symbols(self, exchange: str) -> Set[str]:
        """获取交易所所有可用符号"""
        return self._cache.get(exchange.lower(), set())
    
    def get_cache_stats(self) -> Dict:
        """获取缓存统计信息"""
        return {
            exchange: {
                "count": len(symbols),
                "last_refresh": datetime.fromtimestamp(self._last_refresh.get(exchange, 0)).isoformat()
                if self._last_refresh.get(exchange) else None
            }
            for exchange, symbols in self._cache.items()
        }
    
    async def refresh_all(self, force: bool = False):
        """
        刷新所有交易所的符号列表
        
        Args:
            force: 是否强制刷新（忽略缓存TTL）
        """
        async with self._get_refresh_lock():
            now = time.time()
            
            # 检查是否需要刷新（任一交易所缓存过期则刷新全部）
            need_refresh = force
            if not need_refresh:
                for exchange in ["binance", "bitget", "okx", "hyperliquid"]:
                    last = self._last_refresh.get(exchange, 0)
                    if now - last > CACHE_TTL:
                        need_refresh = True
                        break
            
            if not need_refresh:
                logger.debug("[SymbolAvailability] 缓存仍有效，跳过刷新")
                return
            
            logger.info("[SymbolAvailability] 开始刷新所有交易所符号列表...")
            
            tasks = [
                self._refresh_binance(),
                self._refresh_bitget(),
                self._refresh_okx(),
                self._refresh_hyperliquid(),
            ]
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    exchange = ["binance", "bitget", "okx", "hyperliquid"][i]
                    logger.error(f"[SymbolAvailability] {exchange} 刷新失败: {result}")
            
            # 关闭 HTTP session
            await self.close()
            
            # 统计
            stats = self.get_cache_stats()
            logger.info(f"[SymbolAvailability] 刷新完成: {stats}")
    
    async def refresh_exchange(self, exchange: str):
        """刷新单个交易所的符号列表"""
        exchange = exchange.lower()
        
        refresh_methods = {
            "binance": self._refresh_binance,
            "bitget": self._refresh_bitget,
            "okx": self._refresh_okx,
            "hyperliquid": self._refresh_hyperliquid,
        }
        
        method = refresh_methods.get(exchange)
        if method:
            await method()
    
    async def _refresh_binance(self):
        """刷新 Binance 符号列表"""
        try:
            url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
            
            session = await self._get_http_session()
            async with session.get(url) as resp:
                data = await resp.json()
            
            symbols = set()
            for s in data.get("symbols", []):
                # 只保留交易中的 USDT 永续合约
                if (s.get("status") == "TRADING" and 
                    s.get("contractType") == "PERPETUAL" and
                    s.get("quoteAsset") == "USDT"):
                    symbols.add(s["symbol"])
            
            self._cache["binance"] = symbols
            self._last_refresh["binance"] = time.time()
            
            logger.info(f"[SymbolAvailability] Binance 刷新完成: {len(symbols)} 个符号")
            
            # 保存到 Redis
            await self._save_to_redis("binance", symbols)
            
        except Exception as e:
            logger.error(f"[SymbolAvailability] Binance 刷新失败: {e}")
            # 尝试从 Redis 加载
            await self._load_from_redis("binance")
    
    async def _refresh_bitget(self):
        """刷新 Bitget 符号列表"""
        try:
            url = "https://api.bitget.com/api/v2/mix/market/contracts"
            params = {"productType": "USDT-FUTURES"}
            
            session = await self._get_http_session()
            async with session.get(url, params=params) as resp:
                data = await resp.json()
            
            if data.get("code") != "00000":
                raise Exception(f"API Error: {data.get('msg')}")
            
            symbols = set()
            for s in data.get("data", []):
                # symbol 格式: BTCUSDT
                symbol = s.get("symbol", "")
                if symbol and s.get("symbolStatus") == "normal":
                    symbols.add(symbol)
            
            self._cache["bitget"] = symbols
            self._last_refresh["bitget"] = time.time()
            
            logger.info(f"[SymbolAvailability] Bitget 刷新完成: {len(symbols)} 个符号")
            
            await self._save_to_redis("bitget", symbols)
            
        except Exception as e:
            logger.error(f"[SymbolAvailability] Bitget 刷新失败: {e}")
            await self._load_from_redis("bitget")
    
    async def _refresh_okx(self):
        """刷新 OKX 符号列表"""
        try:
            url = "https://www.okx.com/api/v5/public/instruments"
            params = {"instType": "SWAP"}
            
            session = await self._get_http_session()
            async with session.get(url, params=params) as resp:
                data = await resp.json()
            
            if data.get("code") != "0":
                raise Exception(f"API Error: {data.get('msg')}")
            
            symbols = set()
            for s in data.get("data", []):
                # instId 格式: BTC-USDT-SWAP
                # 转换为标准格式: BTCUSDT
                inst_id = s.get("instId", "")
                if inst_id.endswith("-USDT-SWAP") and s.get("state") == "live":
                    # BTC-USDT-SWAP -> BTCUSDT
                    base = inst_id.replace("-USDT-SWAP", "")
                    symbols.add(f"{base}USDT")
            
            self._cache["okx"] = symbols
            self._last_refresh["okx"] = time.time()
            
            logger.info(f"[SymbolAvailability] OKX 刷新完成: {len(symbols)} 个符号")
            
            await self._save_to_redis("okx", symbols)
            
        except Exception as e:
            logger.error(f"[SymbolAvailability] OKX 刷新失败: {e}")
            await self._load_from_redis("okx")
    
    async def _refresh_hyperliquid(self):
        """刷新 Hyperliquid 符号列表"""
        try:
            url = "https://api.hyperliquid.xyz/info"
            payload = {"type": "meta"}
            
            session = await self._get_http_session()
            async with session.post(url, json=payload) as resp:
                data = await resp.json()
            
            symbols = set()
            for asset in data.get("universe", []):
                # name 格式: BTC, ETH
                # 转换为标准格式: BTCUSDT
                name = asset.get("name", "")
                if name:
                    symbols.add(f"{name}USDT")
            
            self._cache["hyperliquid"] = symbols
            self._last_refresh["hyperliquid"] = time.time()
            
            logger.info(f"[SymbolAvailability] Hyperliquid 刷新完成: {len(symbols)} 个符号")
            
            await self._save_to_redis("hyperliquid", symbols)
            
        except Exception as e:
            logger.error(f"[SymbolAvailability] Hyperliquid 刷新失败: {e}")
            await self._load_from_redis("hyperliquid")
    
    async def _save_to_redis(self, exchange: str, symbols: Set[str]):
        """保存到 Redis（原生异步）"""
        try:
            from core.database import get_async_redis
            
            redis = await get_async_redis()
            key = f"{REDIS_KEY_PREFIX}{exchange}"
            
            # 使用 SADD 存储集合
            if symbols:
                await redis.delete(key)
                await redis.sadd(key, *symbols)
                await redis.expire(key, CACHE_TTL * 2)  # Redis TTL 设长一些，作为备份
            
        except Exception as e:
            logger.warning(f"[SymbolAvailability] 保存到 Redis 失败 {exchange}: {e}")
    
    async def _load_from_redis(self, exchange: str) -> bool:
        """从 Redis 加载（原生异步）"""
        try:
            from core.database import get_async_redis
            
            redis = await get_async_redis()
            key = f"{REDIS_KEY_PREFIX}{exchange}"
            symbols = await redis.smembers(key)
            
            if symbols:
                # redis 已配置 decode_responses=True，直接使用
                self._cache[exchange] = set(symbols)
                logger.info(f"[SymbolAvailability] 从 Redis 加载 {exchange}: {len(symbols)} 个符号")
                return True
            
            return False
            
        except Exception as e:
            logger.warning(f"[SymbolAvailability] 从 Redis 加载失败 {exchange}: {e}")
            return False
    
    async def load_from_redis_all(self):
        """从 Redis 加载所有交易所的缓存"""
        for exchange in ["binance", "bitget", "okx", "hyperliquid"]:
            await self._load_from_redis(exchange)


# 全局单例
_manager: Optional[SymbolAvailabilityManager] = None


def get_symbol_manager() -> SymbolAvailabilityManager:
    """获取符号管理器单例"""
    global _manager
    if _manager is None:
        _manager = SymbolAvailabilityManager()
    return _manager


async def refresh_symbol_availability():
    """刷新所有交易所符号列表（供外部调用）"""
    manager = get_symbol_manager()
    await manager.refresh_all()


def filter_symbols_for_exchange(exchange: str, symbols: List[str]) -> List[str]:
    """
    过滤出交易所支持的符号（同步接口）
    
    Args:
        exchange: 交易所名称
        symbols: 待过滤的符号列表
    
    Returns:
        交易所支持的符号列表
    """
    manager = get_symbol_manager()
    return manager.filter_symbols(exchange, symbols)
