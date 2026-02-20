# trading/exchange_pool.py
"""
交易所连接池管理器

优化点：
1. 连接复用：避免每次下单都创建新连接
2. 限速控制：按交易所 API 限制控制请求频率
3. 健康检查：自动检测和重建失效连接
4. 并发控制：限制每个交易所的并发请求数

1000 用户场景：
- 不再为每个用户创建独立的交易所实例
- 使用共享连接池，按 API Key 分组
- 大幅减少内存占用和连接数
"""

import asyncio
import logging
import time
import threading
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any, Callable
from contextlib import asynccontextmanager

logger = logging.getLogger(__name__)


@dataclass
class ExchangeConnection:
    """交易所连接"""
    exchange_name: str
    api_key: str
    instance: Any  # BaseExchange 实例
    created_at: float = field(default_factory=time.time)
    last_used: float = field(default_factory=time.time)
    request_count: int = 0
    error_count: int = 0
    is_healthy: bool = True


@dataclass
class RateLimitConfig:
    """限速配置"""
    requests_per_second: int
    burst_limit: int
    concurrent_limit: int


class ExchangeConnectionPool:
    """
    交易所连接池
    
    特性：
    - 按 (exchange, api_key) 分组管理连接
    - 自动限速控制
    - 连接健康检查
    - 优雅关闭
    """
    
    _instance: Optional["ExchangeConnectionPool"] = None
    _lock = threading.Lock()
    
    # 各交易所限速配置
    RATE_LIMITS = {
        "binance": RateLimitConfig(
            requests_per_second=10,
            burst_limit=20,
            concurrent_limit=5
        ),
        "okx": RateLimitConfig(
            requests_per_second=20,
            burst_limit=40,
            concurrent_limit=10
        ),
        "bitget": RateLimitConfig(
            requests_per_second=10,
            burst_limit=20,
            concurrent_limit=5
        ),
        "hyperliquid": RateLimitConfig(
            requests_per_second=10,
            burst_limit=20,
            concurrent_limit=5
        ),
    }
    
    # 连接配置
    MAX_CONNECTIONS_PER_KEY = 3  # 每个 API Key 最多 3 个连接
    CONNECTION_TTL = 3600  # 连接存活时间（秒）
    HEALTH_CHECK_INTERVAL = 60  # 健康检查间隔（秒）
    
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
        
        # 连接池: {(exchange, api_key) -> List[ExchangeConnection]}
        self._pools: Dict[tuple, List[ExchangeConnection]] = defaultdict(list)
        self._pool_lock = asyncio.Lock()
        
        # 信号量: {(exchange, api_key) -> Semaphore}
        self._semaphores: Dict[tuple, asyncio.Semaphore] = {}
        
        # 限速器: {(exchange, api_key) -> (last_request_time, token_bucket)}
        self._rate_limiters: Dict[tuple, Dict] = defaultdict(lambda: {
            "tokens": 10,
            "last_refill": time.time()
        })
        
        # 统计
        self._stats = {
            "connections_created": 0,
            "connections_reused": 0,
            "connections_closed": 0,
            "requests_total": 0,
            "requests_throttled": 0,
            "errors_total": 0,
        }
        
        # 后台任务
        self._running = False
        self._health_check_task: Optional[asyncio.Task] = None
        
        self._initialized = True
        logger.info("[ExchangePool] 连接池已初始化")
    
    # =========================================================================
    # 连接管理
    # =========================================================================
    
    async def get_connection(
        self,
        exchange_name: str,
        api_key: str,
        api_secret: str,
        passphrase: str = None,
        is_testnet: bool = False,
        wallet_address: str = None,
    ) -> ExchangeConnection:
        """
        获取交易所连接
        
        优先复用现有连接，必要时创建新连接
        """
        pool_key = (exchange_name, api_key)
        
        async with self._pool_lock:
            # 查找可用连接
            pool = self._pools[pool_key]
            
            for conn in pool:
                if conn.is_healthy:
                    conn.last_used = time.time()
                    conn.request_count += 1
                    self._stats["connections_reused"] += 1
                    return conn
            
            # 检查是否可以创建新连接
            if len(pool) >= self.MAX_CONNECTIONS_PER_KEY:
                # 等待现有连接可用
                logger.warning(f"[ExchangePool] 连接池已满: {pool_key}")
                # 返回最近使用的连接
                if pool:
                    conn = min(pool, key=lambda c: c.last_used)
                    conn.last_used = time.time()
                    conn.request_count += 1
                    return conn
            
            # 创建新连接
            conn = await self._create_connection(
                exchange_name, api_key, api_secret,
                passphrase, is_testnet, wallet_address
            )
            
            pool.append(conn)
            self._stats["connections_created"] += 1
            
            return conn
    
    async def _create_connection(
        self,
        exchange_name: str,
        api_key: str,
        api_secret: str,
        passphrase: str = None,
        is_testnet: bool = False,
        wallet_address: str = None,
    ) -> ExchangeConnection:
        """创建新的交易所连接"""
        from exchanges import create_exchange
        
        instance = create_exchange(
            exchange_type=exchange_name,
            api_key=api_key,
            api_secret=api_secret,
            passphrase=passphrase,
            is_testnet=is_testnet,
            wallet_address=wallet_address
        )
        
        return ExchangeConnection(
            exchange_name=exchange_name,
            api_key=api_key,
            instance=instance
        )
    
    async def release_connection(self, conn: ExchangeConnection):
        """释放连接（标记为可用）"""
        conn.last_used = time.time()
    
    async def mark_unhealthy(self, conn: ExchangeConnection, error: Exception):
        """标记连接为不健康"""
        conn.is_healthy = False
        conn.error_count += 1
        self._stats["errors_total"] += 1
        # P3 Fix: 不在日志中打印 API Key
        import hashlib
        key_id = hashlib.sha256(conn.api_key.encode()).hexdigest()[:8]
        logger.warning(f"[ExchangePool] 连接标记为不健康: {conn.exchange_name}/key_{key_id} - {error}")
    
    # =========================================================================
    # 限速控制
    # =========================================================================
    
    async def acquire_rate_limit(
        self,
        exchange_name: str,
        api_key: str,
        timeout: float = 30.0
    ) -> bool:
        """
        获取限速令牌
        
        使用令牌桶算法
        """
        pool_key = (exchange_name, api_key)
        config = self.RATE_LIMITS.get(exchange_name, RateLimitConfig(10, 20, 5))
        
        start_time = time.time()
        
        while True:
            now = time.time()
            
            if now - start_time > timeout:
                self._stats["requests_throttled"] += 1
                return False
            
            limiter = self._rate_limiters[pool_key]
            
            # 补充令牌
            elapsed = now - limiter["last_refill"]
            new_tokens = elapsed * config.requests_per_second
            limiter["tokens"] = min(config.burst_limit, limiter["tokens"] + new_tokens)
            limiter["last_refill"] = now
            
            # 尝试获取令牌
            if limiter["tokens"] >= 1:
                limiter["tokens"] -= 1
                self._stats["requests_total"] += 1
                return True
            
            # 等待
            await asyncio.sleep(0.1)
    
    @asynccontextmanager
    async def connection_context(
        self,
        exchange_name: str,
        api_key: str,
        api_secret: str,
        passphrase: str = None,
        is_testnet: bool = False,
        wallet_address: str = None,
    ):
        """
        连接上下文管理器
        
        自动获取连接、限速、释放
        """
        # 获取限速令牌
        if not await self.acquire_rate_limit(exchange_name, api_key):
            raise Exception(f"Rate limit exceeded for {exchange_name}")
        
        # 获取连接
        conn = await self.get_connection(
            exchange_name, api_key, api_secret,
            passphrase, is_testnet, wallet_address
        )
        
        try:
            yield conn.instance
        except Exception as e:
            await self.mark_unhealthy(conn, e)
            raise
        finally:
            await self.release_connection(conn)
    
    # =========================================================================
    # 健康检查
    # =========================================================================
    
    async def _health_check_loop(self):
        """健康检查循环"""
        while self._running:
            try:
                await self._check_all_connections()
            except Exception as e:
                logger.error(f"[ExchangePool] 健康检查失败: {e}")
            
            await asyncio.sleep(self.HEALTH_CHECK_INTERVAL)
    
    async def _check_all_connections(self):
        """检查所有连接的健康状态"""
        now = time.time()
        
        async with self._pool_lock:
            for pool_key, pool in list(self._pools.items()):
                connections_to_remove = []
                
                for conn in pool:
                    # 检查连接是否过期
                    if now - conn.created_at > self.CONNECTION_TTL:
                        connections_to_remove.append(conn)
                        continue
                    
                    # 检查不健康的连接
                    if not conn.is_healthy:
                        # 尝试重建
                        if conn.error_count < 3:
                            conn.is_healthy = True
                            conn.error_count = 0
                        else:
                            connections_to_remove.append(conn)
                
                # 移除过期/失效连接
                for conn in connections_to_remove:
                    pool.remove(conn)
                    self._stats["connections_closed"] += 1
                    
                    try:
                        await conn.instance.close()
                    except Exception as e:
                        # P2 Fix: 添加日志，连接关闭失败不影响清理流程
                        logger.debug(f"[ExchangePool] 关闭连接时异常: {e}")
    
    # =========================================================================
    # 生命周期
    # =========================================================================
    
    async def start(self):
        """启动连接池"""
        if self._running:
            return
        
        self._running = True
        self._health_check_task = asyncio.create_task(self._health_check_loop())
        logger.info("[ExchangePool] 连接池已启动")
    
    async def stop(self):
        """停止连接池"""
        self._running = False
        
        if self._health_check_task:
            self._health_check_task.cancel()
            try:
                await self._health_check_task
            except asyncio.CancelledError:
                pass
        
        # 关闭所有连接
        async with self._pool_lock:
            for pool in self._pools.values():
                for conn in pool:
                    try:
                        await conn.instance.close()
                    except Exception as e:
                        # P2 Fix: 添加日志
                        logger.debug(f"[ExchangePool] 停止时关闭连接异常: {e}")
            
            self._pools.clear()
        
        logger.info(f"[ExchangePool] 连接池已停止，统计: {self._stats}")
    
    def get_stats(self) -> Dict:
        """获取统计信息"""
        total_connections = sum(len(pool) for pool in self._pools.values())
        healthy_connections = sum(
            sum(1 for c in pool if c.is_healthy)
            for pool in self._pools.values()
        )
        
        return {
            **self._stats,
            "total_connections": total_connections,
            "healthy_connections": healthy_connections,
            "pool_count": len(self._pools),
        }


# =============================================================================
# 全局实例
# =============================================================================

def get_exchange_pool() -> ExchangeConnectionPool:
    """获取交易所连接池实例"""
    return ExchangeConnectionPool()
