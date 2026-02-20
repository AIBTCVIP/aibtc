# core/bitget_public_cache.py
"""
Bitget 公共数据全局缓存

所有用户共享，避免重复请求：
- contracts: 交易对信息（合约乘数等），1小时 TTL
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
import requests
from typing import Dict, Optional, List, Any
from concurrent.futures import ThreadPoolExecutor

logger = logging.getLogger(__name__)


class BitgetPublicCache:
    """
    Bitget 公共数据缓存（单例）
    
    缓存策略：
    - contracts: Redis + 内存，1小时 TTL
    - mark_price: 直接从 Redis 读取 WebSocket 数据
    """
    
    _instance: Optional['BitgetPublicCache'] = None
    _lock = threading.Lock()
    
    # 缓存 TTL（秒）
    CONTRACTS_TTL = 3600  # 1小时
    
    # Redis Key 前缀
    REDIS_KEY_CONTRACTS = "bitget:public:contracts"
    
    # Bitget API URL
    MAINNET_API = "https://api.bitget.com"
    
    # 产品类型
    PRODUCT_TYPE = "USDT-FUTURES"
    
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
        # contracts: {symbol: {...}, ...} 例如 {"BTCUSDT": {"sizeMultiplier": "0.001", ...}}
        self._contracts: Dict[str, Any] = {}
        self._contracts_time: float = 0
        
        # 线程锁
        self._contracts_lock = threading.Lock()
        
        # 线程池（用于同步转异步）
        self._executor = ThreadPoolExecutor(max_workers=2)
        
        logger.info("[BitgetPublicCache] 初始化完成")
    
    @classmethod
    def get_instance(cls) -> 'BitgetPublicCache':
        """获取单例实例"""
        return cls()
    
    def _get_redis(self):
        """获取 Redis 连接 - P2 Fix: 使用共享连接池"""
        try:
            from core.database import redis_client
            return redis_client
        except Exception as e:
            logger.warning(f"[BitgetPublicCache] 获取 Redis 连接失败: {e}")
            return None
    
    def _convert_symbol_to_bitget(self, symbol: str) -> str:
        """
        将 Binance 格式的 symbol 转换为 Bitget 格式
        
        BTCUSDT -> BTCUSDT (Bitget V2 API 使用相同格式)
        """
        return symbol.upper()
    
    def _fetch_contracts(self) -> Dict[str, Any]:
        """从 Bitget API 获取所有合约信息"""
        try:
            url = f"{self.MAINNET_API}/api/v2/mix/market/contracts"
            params = {"productType": self.PRODUCT_TYPE}
            
            resp = requests.get(url, params=params, timeout=30)
            data = resp.json()
            
            if data.get("code") != "00000":
                raise Exception(f"Bitget API 错误: {data.get('msg')}")
            
            # 构建 symbol -> info 映射
            contracts = {}
            for contract in data.get("data", []):
                symbol = contract.get("symbol", "")
                if symbol:
                    # 转换为 Binance 格式的 symbol 作为 key
                    # BTCUSDT_UMCBL -> BTCUSDT
                    normalized_symbol = symbol.replace("_UMCBL", "").replace("USDT", "") + "USDT"
                    contracts[normalized_symbol] = contract
                    # 同时保留原始 symbol 作为 key
                    contracts[symbol] = contract
            
            return contracts
            
        except Exception as e:
            logger.error(f"[BitgetPublicCache] 获取 contracts 失败: {e}")
            raise
    
    def get_contracts(self, force_refresh: bool = False) -> Dict[str, Any]:
        """
        获取所有合约信息（同步）
        
        优先级：内存缓存 -> Redis 缓存 -> API 请求
        
        返回: {symbol: {...}, ...}
        """
        now = time.time()
        
        # 1. 检查内存缓存
        if not force_refresh and self._contracts and (now - self._contracts_time) < self.CONTRACTS_TTL:
            return self._contracts
        
        with self._contracts_lock:
            # 双重检查
            if not force_refresh and self._contracts and (now - self._contracts_time) < self.CONTRACTS_TTL:
                return self._contracts
            
            # 2. 尝试从 Redis 获取
            rds = self._get_redis()
            if rds:
                try:
                    cached = rds.get(self.REDIS_KEY_CONTRACTS)
                    if cached:
                        data = json.loads(cached)
                        self._contracts = data
                        self._contracts_time = now
                        logger.debug("[BitgetPublicCache] 从 Redis 获取 contracts")
                        return self._contracts
                except Exception as e:
                    logger.warning(f"[BitgetPublicCache] Redis 读取 contracts 失败: {e}")
            
            # 3. 从 API 获取
            try:
                contracts = self._fetch_contracts()
                
                self._contracts = contracts
                self._contracts_time = now
                
                # 写入 Redis
                if rds:
                    try:
                        rds.setex(
                            self.REDIS_KEY_CONTRACTS,
                            self.CONTRACTS_TTL,
                            json.dumps(contracts)
                        )
                        logger.info(f"[BitgetPublicCache] contracts 已缓存到 Redis, {len(contracts)} 个合约")
                    except Exception as e:
                        logger.warning(f"[BitgetPublicCache] Redis 写入 contracts 失败: {e}")
                
                return self._contracts
                
            except Exception as e:
                logger.error(f"[BitgetPublicCache] 获取 contracts 失败: {e}")
                # 返回旧缓存（如果有）
                if self._contracts:
                    return self._contracts
                raise
    
    async def get_contracts_async(self, force_refresh: bool = False) -> Dict[str, Any]:
        """获取所有合约信息（异步）"""
        import asyncio
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self._executor, lambda: self.get_contracts(force_refresh))
    
    def get_contract_info(self, symbol: str) -> Dict[str, Any]:
        """
        获取单个合约信息（同步）
        
        Args:
            symbol: Binance 格式 (BTCUSDT) 或 Bitget 格式 (BTCUSDT_UMCBL)
        """
        contracts = self.get_contracts()
        
        # 尝试直接匹配
        if symbol in contracts:
            return contracts[symbol]
        
        # 转换格式后匹配
        normalized = self._convert_symbol_to_bitget(symbol)
        return contracts.get(normalized, {})
    
    async def get_contract_info_async(self, symbol: str) -> Dict[str, Any]:
        """获取单个合约信息（异步）"""
        contracts = await self.get_contracts_async()
        
        if symbol in contracts:
            return contracts[symbol]
        
        normalized = self._convert_symbol_to_bitget(symbol)
        return contracts.get(normalized, {})
    
    def get_size_multiplier(self, symbol: str) -> float:
        """
        获取合约乘数（sizeMultiplier）
        
        Bitget 的数量单位可能需要乘以 sizeMultiplier
        
        Args:
            symbol: Binance 格式 (BTCUSDT) 或 Bitget 格式
        
        Returns:
            合约乘数，默认 1.0
        """
        info = self.get_contract_info(symbol)
        multiplier = info.get("sizeMultiplier")
        if multiplier:
            return float(multiplier)
        return 1.0
    
    async def get_size_multiplier_async(self, symbol: str) -> float:
        """获取合约乘数（异步）"""
        info = await self.get_contract_info_async(symbol)
        multiplier = info.get("sizeMultiplier")
        if multiplier:
            return float(multiplier)
        return 1.0
    
    def get_price_precision(self, symbol: str) -> int:
        """获取价格精度"""
        info = self.get_contract_info(symbol)
        precision = info.get("pricePlace")
        if precision:
            return int(precision)
        return 2
    
    def get_volume_precision(self, symbol: str) -> int:
        """获取数量精度"""
        info = self.get_contract_info(symbol)
        precision = info.get("volumePlace")
        if precision:
            return int(precision)
        return 4
    
    def get_min_trade_num(self, symbol: str) -> float:
        """获取最小下单数量"""
        info = self.get_contract_info(symbol)
        min_num = info.get("minTradeNum")
        if min_num:
            return float(min_num)
        return 0.001
    
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
            # Bitget 标记价格 key 格式: bitget:mark:{symbol}
            key = f"bitget:mark:{symbol}"
            data = rds.get(key)
            
            if data:
                parsed = json.loads(data)
                mark_price = parsed.get("markPrice")
                if mark_price:
                    return float(mark_price)
        except Exception as e:
            logger.debug(f"[BitgetPublicCache] 获取 mark_price 失败 {symbol}: {e}")
        
        return None
    
    def get_all_mark_prices(self) -> Dict[str, float]:
        """
        获取所有标记价格（从 Redis）
        
        返回: {symbol: price, ...}
        """
        rds = self._get_redis()
        if not rds:
            return {}
        
        try:
            pattern = "bitget:mark:*"
            prices = {}
            
            for key in rds.scan_iter(match=pattern, count=100):
                try:
                    data = rds.get(key)
                    if data:
                        parsed = json.loads(data)
                        mark_price = parsed.get("markPrice")
                        if mark_price:
                            # 从 key 提取 symbol: bitget:mark:BTCUSDT -> BTCUSDT
                            symbol = key.split(":")[-1]
                            prices[symbol] = float(mark_price)
                except Exception:
                    continue
            
            return prices
        except Exception as e:
            logger.warning(f"[BitgetPublicCache] 获取所有 mark_price 失败: {e}")
            return {}
    
    # ==================== 工具方法 ====================
    
    def clear_cache(self):
        """清除所有缓存"""
        with self._contracts_lock:
            self._contracts = {}
            self._contracts_time = 0
        
        # 清除 Redis 缓存
        rds = self._get_redis()
        if rds:
            try:
                rds.delete(self.REDIS_KEY_CONTRACTS)
            except Exception as e:
                logger.warning(f"[BitgetPublicCache] 清除 Redis 缓存失败: {e}")
        
        logger.info("[BitgetPublicCache] 缓存已清除")
    
    def get_stats(self) -> Dict[str, Any]:
        """获取缓存统计信息"""
        now = time.time()
        return {
            "contracts_count": len(self._contracts),
            "contracts_age_seconds": now - self._contracts_time if self._contracts_time else None,
        }
    
    def close(self):
        """关闭资源（在程序退出时调用）"""
        if self._executor:
            self._executor.shutdown(wait=False)
            self._executor = None
            logger.info("[BitgetPublicCache] 线程池已关闭")


# ==================== 便捷函数 ====================

def get_bitget_public_cache() -> BitgetPublicCache:
    """获取 Bitget 公共数据缓存实例"""
    return BitgetPublicCache.get_instance()
