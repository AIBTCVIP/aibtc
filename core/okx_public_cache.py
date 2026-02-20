# core/okx_public_cache.py
"""
OKX 公共数据全局缓存

所有用户共享，避免重复请求：
- instruments: 交易对信息（合约乘数 ctVal 等），1小时 TTL
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
import aiohttp
import asyncio
from typing import Dict, Optional, List, Any
from concurrent.futures import ThreadPoolExecutor

logger = logging.getLogger(__name__)


class OKXPublicCache:
    """
    OKX 公共数据缓存（单例）
    
    缓存策略：
    - instruments: Redis + 内存，1小时 TTL
    - mark_price: 直接从 Redis 读取 WebSocket 数据
    """
    
    _instance: Optional['OKXPublicCache'] = None
    _lock = threading.Lock()
    
    # 缓存 TTL（秒）
    INSTRUMENTS_TTL = 3600  # 1小时
    
    # Redis Key 前缀
    REDIS_KEY_INSTRUMENTS = "okx:public:instruments"
    
    # OKX API URL
    MAINNET_API = "https://www.okx.com"
    
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
        # instruments: {instId: {...}, ...} 例如 {"BTC-USDT-SWAP": {"ctVal": "0.01", ...}}
        self._instruments: Dict[str, Any] = {}
        self._instruments_time: float = 0
        
        # 线程锁
        self._instruments_lock = threading.Lock()
        
        # 异步锁（延迟初始化）
        self._async_lock: Optional[asyncio.Lock] = None
        
        # 线程池（用于同步转异步）
        self._executor = ThreadPoolExecutor(max_workers=2)
        
        logger.info("[OKXPublicCache] 初始化完成")
    
    @classmethod
    def get_instance(cls) -> 'OKXPublicCache':
        """获取单例实例"""
        return cls()
    
    def _get_redis(self):
        """获取 Redis 连接"""
        try:
            from core.database import redis_client
            return redis_client
        except Exception as e:
            logger.warning(f"[OKXPublicCache] 获取 Redis 连接失败: {e}")
            return None
    
    # ==================== Instruments ====================
    
    def _convert_symbol_to_inst_id(self, symbol: str) -> str:
        """
        将 Binance 格式的 symbol 转换为 OKX instId
        
        BTCUSDT -> BTC-USDT-SWAP
        """
        symbol = symbol.upper()
        if symbol.endswith("USDT"):
            base = symbol[:-4]
            return f"{base}-USDT-SWAP"
        return symbol
    
    def _convert_inst_id_to_symbol(self, inst_id: str) -> str:
        """
        将 OKX instId 转换为 Binance 格式的 symbol
        
        BTC-USDT-SWAP -> BTCUSDT
        """
        parts = inst_id.split("-")
        if len(parts) >= 2:
            return f"{parts[0]}{parts[1]}"
        return inst_id
    
    def _fetch_instruments_sync(self) -> Dict[str, Any]:
        """从 OKX API 获取所有 SWAP 合约信息（同步版本，使用 requests）"""
        import requests
        try:
            url = f"{self.MAINNET_API}/api/v5/public/instruments"
            params = {"instType": "SWAP"}
            
            resp = requests.get(url, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            
            if data.get("code") != "0":
                raise Exception(f"OKX API 错误: {data.get('msg')}")
            
            # 构建 instId -> info 映射
            instruments = {}
            for inst in data.get("data", []):
                inst_id = inst.get("instId")
                if inst_id:
                    instruments[inst_id] = inst
            
            return instruments
            
        except Exception as e:
            logger.error(f"[OKXPublicCache] 获取 instruments 失败: {e}")
            raise
    
    async def _fetch_instruments(self) -> Dict[str, Any]:
        """从 OKX API 获取所有 SWAP 合约信息（异步版本）"""
        # 每次创建新的 session，避免跨事件循环复用问题
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as session:
            try:
                url = f"{self.MAINNET_API}/api/v5/public/instruments"
                params = {"instType": "SWAP"}
                
                async with session.get(url, params=params) as resp:
                    data = await resp.json()
                
                if data.get("code") != "0":
                    raise Exception(f"OKX API 错误: {data.get('msg')}")
                
                # 构建 instId -> info 映射
                instruments = {}
                for inst in data.get("data", []):
                    inst_id = inst.get("instId")
                    if inst_id:
                        instruments[inst_id] = inst
                
                return instruments
                
            except Exception as e:
                logger.error(f"[OKXPublicCache] 获取 instruments 失败: {e}")
                raise
    
    def get_instruments(self, force_refresh: bool = False) -> Dict[str, Any]:
        """
        获取所有合约信息（同步）
        
        优先级：内存缓存 -> Redis 缓存 -> API 请求
        
        返回: {instId: {...}, ...}
        """
        now = time.time()
        
        # 1. 检查内存缓存
        if not force_refresh and self._instruments and (now - self._instruments_time) < self.INSTRUMENTS_TTL:
            return self._instruments
        
        with self._instruments_lock:
            # 双重检查
            if not force_refresh and self._instruments and (now - self._instruments_time) < self.INSTRUMENTS_TTL:
                return self._instruments
            
            # 2. 尝试从 Redis 获取
            rds = self._get_redis()
            if rds:
                try:
                    cached = rds.get(self.REDIS_KEY_INSTRUMENTS)
                    if cached:
                        data = json.loads(cached)
                        self._instruments = data
                        self._instruments_time = now
                        logger.debug("[OKXPublicCache] 从 Redis 获取 instruments")
                        return self._instruments
                except Exception as e:
                    logger.warning(f"[OKXPublicCache] Redis 读取 instruments 失败: {e}")
            
            # 3. 从 API 获取（使用同步版本，避免 asyncio 上下文问题）
            try:
                instruments = self._fetch_instruments_sync()
                
                self._instruments = instruments
                self._instruments_time = now
                
                # 写入 Redis
                if rds:
                    try:
                        rds.setex(
                            self.REDIS_KEY_INSTRUMENTS,
                            self.INSTRUMENTS_TTL,
                            json.dumps(instruments)
                        )
                        logger.info(f"[OKXPublicCache] instruments 已缓存到 Redis, {len(instruments)} 个合约")
                    except Exception as e:
                        logger.warning(f"[OKXPublicCache] Redis 写入 instruments 失败: {e}")
                
                return self._instruments
                
            except Exception as e:
                logger.error(f"[OKXPublicCache] 获取 instruments 失败: {e}")
                # 返回旧缓存（如果有）
                if self._instruments:
                    return self._instruments
                raise
    
    async def get_instruments_async(self, force_refresh: bool = False) -> Dict[str, Any]:
        """获取所有合约信息（异步）"""
        now = time.time()
        
        # 1. 检查内存缓存
        if not force_refresh and self._instruments and (now - self._instruments_time) < self.INSTRUMENTS_TTL:
            return self._instruments
        
        # 使用实例级别的异步锁（延迟初始化）
        if self._async_lock is None:
            self._async_lock = asyncio.Lock()
        
        async with self._async_lock:
            # 双重检查
            if not force_refresh and self._instruments and (now - self._instruments_time) < self.INSTRUMENTS_TTL:
                return self._instruments
            
            # 2. 尝试从 Redis 获取
            rds = self._get_redis()
            if rds:
                try:
                    cached = rds.get(self.REDIS_KEY_INSTRUMENTS)
                    if cached:
                        data = json.loads(cached)
                        self._instruments = data
                        self._instruments_time = now
                        logger.debug("[OKXPublicCache] 从 Redis 获取 instruments")
                        return self._instruments
                except Exception as e:
                    logger.warning(f"[OKXPublicCache] Redis 读取 instruments 失败: {e}")
            
            # 3. 从 API 获取
            try:
                instruments = await self._fetch_instruments()
                
                self._instruments = instruments
                self._instruments_time = now
                
                # 写入 Redis
                if rds:
                    try:
                        rds.setex(
                            self.REDIS_KEY_INSTRUMENTS,
                            self.INSTRUMENTS_TTL,
                            json.dumps(instruments)
                        )
                        logger.info(f"[OKXPublicCache] instruments 已缓存到 Redis, {len(instruments)} 个合约")
                    except Exception as e:
                        logger.warning(f"[OKXPublicCache] Redis 写入 instruments 失败: {e}")
                
                return self._instruments
                
            except Exception as e:
                logger.error(f"[OKXPublicCache] 获取 instruments 失败: {e}")
                if self._instruments:
                    return self._instruments
                raise
    
    def get_instrument_info(self, symbol: str) -> Dict[str, Any]:
        """
        获取单个合约信息（同步）
        
        Args:
            symbol: Binance 格式 (BTCUSDT) 或 OKX 格式 (BTC-USDT-SWAP)
        """
        instruments = self.get_instruments()
        
        # 尝试直接匹配
        if symbol in instruments:
            return instruments[symbol]
        
        # 转换格式后匹配
        inst_id = self._convert_symbol_to_inst_id(symbol)
        return instruments.get(inst_id, {})
    
    async def get_instrument_info_async(self, symbol: str) -> Dict[str, Any]:
        """获取单个合约信息（异步）"""
        instruments = await self.get_instruments_async()
        
        if symbol in instruments:
            return instruments[symbol]
        
        inst_id = self._convert_symbol_to_inst_id(symbol)
        return instruments.get(inst_id, {})
    
    def get_ct_val(self, symbol: str) -> float:
        """
        获取合约乘数（ctVal）
        
        OKX 的数量单位是"张"，需要乘以 ctVal 得到币数量
        例如：BTC-USDT-SWAP 的 ctVal = 0.01，1张 = 0.01 BTC
        
        Args:
            symbol: Binance 格式 (BTCUSDT) 或 OKX 格式 (BTC-USDT-SWAP)
        
        Returns:
            合约乘数，默认 1.0
        """
        info = self.get_instrument_info(symbol)
        ct_val = info.get("ctVal")
        if ct_val:
            return float(ct_val)
        return 1.0
    
    async def get_ct_val_async(self, symbol: str) -> float:
        """获取合约乘数（异步）"""
        info = await self.get_instrument_info_async(symbol)
        ct_val = info.get("ctVal")
        if ct_val:
            return float(ct_val)
        return 1.0
    
    def get_tick_size(self, symbol: str) -> float:
        """获取价格精度（tickSz）"""
        info = self.get_instrument_info(symbol)
        tick_sz = info.get("tickSz")
        if tick_sz:
            return float(tick_sz)
        return 0.01
    
    def get_lot_size(self, symbol: str) -> float:
        """获取数量精度（lotSz）"""
        info = self.get_instrument_info(symbol)
        lot_sz = info.get("lotSz")
        if lot_sz:
            return float(lot_sz)
        return 1.0
    
    def get_min_size(self, symbol: str) -> float:
        """获取最小下单数量（minSz）"""
        info = self.get_instrument_info(symbol)
        min_sz = info.get("minSz")
        if min_sz:
            return float(min_sz)
        return 1.0
    
    # ==================== Mark Price ====================
    
    def get_mark_price(self, symbol: str) -> Optional[float]:
        """
        获取标记价格（从 Redis WebSocket 数据）
        
        数据来源：WebSocket 推送，存储在 Redis
        """
        rds = self._get_redis()
        if not rds:
            return None
        
        try:
            # OKX 标记价格 key 格式: okx:mark:{instId}
            inst_id = self._convert_symbol_to_inst_id(symbol)
            key = f"okx:mark:{inst_id}"
            data = rds.get(key)
            
            if data:
                parsed = json.loads(data)
                mark_price = parsed.get("markPx")
                if mark_price:
                    return float(mark_price)
        except Exception as e:
            logger.debug(f"[OKXPublicCache] 获取 mark_price 失败 {symbol}: {e}")
        
        return None
    
    def get_all_mark_prices(self) -> Dict[str, float]:
        """
        获取所有标记价格（从 Redis）
        
        返回: {symbol: price, ...} (Binance 格式的 symbol)
        """
        rds = self._get_redis()
        if not rds:
            return {}
        
        try:
            pattern = "okx:mark:*"
            prices = {}
            
            for key in rds.scan_iter(match=pattern, count=100):
                try:
                    data = rds.get(key)
                    if data:
                        parsed = json.loads(data)
                        mark_price = parsed.get("markPx")
                        if mark_price:
                            # 从 key 提取 instId: okx:mark:BTC-USDT-SWAP -> BTC-USDT-SWAP
                            inst_id = key.split(":")[-1]
                            # 转换为 Binance 格式
                            symbol = self._convert_inst_id_to_symbol(inst_id)
                            prices[symbol] = float(mark_price)
                except Exception:
                    continue
            
            return prices
        except Exception as e:
            logger.warning(f"[OKXPublicCache] 获取所有 mark_price 失败: {e}")
            return {}
    
    # ==================== 工具方法 ====================
    
    def clear_cache(self):
        """清除所有缓存"""
        with self._instruments_lock:
            self._instruments = {}
            self._instruments_time = 0
        
        # 清除 Redis 缓存
        rds = self._get_redis()
        if rds:
            try:
                rds.delete(self.REDIS_KEY_INSTRUMENTS)
            except Exception as e:
                logger.warning(f"[OKXPublicCache] 清除 Redis 缓存失败: {e}")
        
        logger.info("[OKXPublicCache] 缓存已清除")
    
    def get_stats(self) -> Dict[str, Any]:
        """获取缓存统计信息"""
        now = time.time()
        return {
            "instruments_count": len(self._instruments),
            "instruments_age_seconds": now - self._instruments_time if self._instruments_time else None,
        }
    
    def close(self):
        """关闭资源（在程序退出时调用）"""
        if self._executor:
            self._executor.shutdown(wait=False)
            self._executor = None
            logger.info("[OKXPublicCache] 线程池已关闭")


# ==================== 便捷函数 ====================

def get_okx_public_cache() -> OKXPublicCache:
    """获取 OKX 公共数据缓存实例"""
    return OKXPublicCache.get_instance()
