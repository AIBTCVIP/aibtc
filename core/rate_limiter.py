# rate_limiter.py
"""
全局 API 限速器

用于控制对外部 API（如 Binance）的请求速率，
确保多用户场景下不会超出限制。

限速策略：
1. IP 级别限速（全局共享）- 所有用户共享
2. API Key 级别限速（每个用户独立）- Binance 专用

Binance 限制：
- IP 级别：2400 请求/分钟
- API Key 级别：根据端点不同有不同限制，保守设置 1200/分钟
"""
import asyncio
import hashlib
import threading
import time
import logging
from typing import Dict, Optional
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class RateLimitConfig:
    """限速配置"""
    requests_per_minute: int = 2000  # 每分钟最大请求数
    burst_size: int = 100  # 允许的突发请求数
    min_interval_ms: int = 30  # 最小请求间隔（毫秒）


class TokenBucketRateLimiter:
    """
    令牌桶限速器
    
    - 每分钟补充固定数量的令牌
    - 支持突发请求（burst）
    - 线程安全 + asyncio 兼容
    - P2 优化：支持动态限速降级
    """
    
    # 降级配置
    DEGRADATION_FACTOR = 0.5  # 每次降级减少 50%
    MIN_RATE_FACTOR = 0.1  # 最低降到原始速率的 10%
    RECOVERY_TIME = 300  # 5 分钟后尝试恢复
    
    def __init__(self, config: RateLimitConfig = None):
        self.config = config or RateLimitConfig()
        self._original_config = RateLimitConfig(
            requests_per_minute=self.config.requests_per_minute,
            burst_size=self.config.burst_size,
            min_interval_ms=self.config.min_interval_ms,
        )
        
        # 令牌桶
        self._tokens = float(self.config.burst_size)
        self._max_tokens = float(self.config.burst_size)
        self._refill_rate = self.config.requests_per_minute / 60.0  # 每秒补充的令牌数
        self._last_refill = time.monotonic()
        self._last_request = 0.0
        
        # 线程安全
        self._lock = threading.Lock()
        self._async_lock: Optional[asyncio.Lock] = None
        # P3 Fix: 用于保护 _async_lock 创建的线程锁
        self._async_lock_creation_lock = threading.Lock()
        
        # 统计
        self._total_requests = 0
        self._total_waits = 0
        self._total_wait_time = 0.0
        
        # 动态降级状态
        self._degradation_level = 0  # 当前降级级别（0=正常）
        self._last_rate_limit_error = 0.0  # 上次限速错误时间
        self._rate_limit_errors = 0  # 限速错误计数
    
    def _get_async_lock(self) -> asyncio.Lock:
        """
        获取或创建 asyncio 锁
        
        P3 Fix: 使用 threading.Lock 保护创建过程，避免竞态条件
        """
        if self._async_lock is None:
            with self._async_lock_creation_lock:
                if self._async_lock is None:
                    self._async_lock = asyncio.Lock()
        return self._async_lock
    
    def _refill(self) -> None:
        """补充令牌"""
        now = time.monotonic()
        elapsed = now - self._last_refill
        
        # 按时间补充令牌
        new_tokens = elapsed * self._refill_rate
        self._tokens = min(self._max_tokens, self._tokens + new_tokens)
        self._last_refill = now
    
    def _calculate_wait_time(self) -> float:
        """计算需要等待的时间（秒）"""
        self._refill()
        
        if self._tokens >= 1.0:
            # 检查最小间隔
            now = time.monotonic()
            min_interval = self.config.min_interval_ms / 1000.0
            time_since_last = now - self._last_request
            
            if time_since_last < min_interval:
                return min_interval - time_since_last
            return 0.0
        
        # 需要等待令牌补充
        tokens_needed = 1.0 - self._tokens
        wait_time = tokens_needed / self._refill_rate
        return wait_time
    
    def acquire(self, timeout: float = 30.0) -> bool:
        """
        同步获取一个令牌
        
        Args:
            timeout: 最大等待时间（秒）
        
        Returns:
            是否成功获取
        
        P0 Fix: 重构锁管理，避免在 context manager 内手动 release/acquire 导致的潜在死锁
        """
        start_time = time.monotonic()
        
        while True:
            with self._lock:
                wait_time = self._calculate_wait_time()
                
                if wait_time <= 0:
                    # 有令牌可用
                    self._tokens -= 1.0
                    self._last_request = time.monotonic()
                    self._total_requests += 1
                    return True
                
                # 检查超时
                elapsed = time.monotonic() - start_time
                if elapsed + wait_time > timeout:
                    logger.warning(f"[RateLimiter] 获取令牌超时 (waited {elapsed:.2f}s)")
                    return False
                
                # 记录等待统计
                self._total_waits += 1
                self._total_wait_time += min(wait_time, 0.1)
            
            # 锁已释放，安全地等待
            time.sleep(min(wait_time, 0.1))  # 最多等 100ms 后重新检查
    
    async def acquire_async(self, timeout: float = 30.0) -> bool:
        """
        异步获取一个令牌
        
        P2 Fix: 移除混合锁，只使用 threading.Lock 保护状态
        避免在持有 asyncio.Lock 时获取 threading.Lock 导致的潜在死锁
        
        Args:
            timeout: 最大等待时间（秒）
        
        Returns:
            是否成功获取
        """
        start_time = time.monotonic()
        
        while True:
            # 只使用 threading.Lock 保护状态读写
            with self._lock:
                wait_time = self._calculate_wait_time()
                
                if wait_time <= 0:
                    # 有令牌可用
                    self._tokens -= 1.0
                    self._last_request = time.monotonic()
                    self._total_requests += 1
                    return True
            
            # 检查超时（在锁外）
            elapsed = time.monotonic() - start_time
            if elapsed + wait_time > timeout:
                logger.warning(f"[RateLimiter] 获取令牌超时 (waited {elapsed:.2f}s)")
                return False
            
            # 异步等待（在锁外）
            self._total_waits += 1
            self._total_wait_time += min(wait_time, 0.1)
            await asyncio.sleep(min(wait_time, 0.1))
    
    def get_stats(self) -> Dict:
        """获取统计信息"""
        with self._lock:
            self._refill()
            return {
                "available_tokens": self._tokens,
                "max_tokens": self._max_tokens,
                "requests_per_minute": self.config.requests_per_minute,
                "original_rpm": self._original_config.requests_per_minute,
                "total_requests": self._total_requests,
                "total_waits": self._total_waits,
                "total_wait_time": self._total_wait_time,
                "avg_wait_time": self._total_wait_time / max(1, self._total_waits),
                "degradation_level": self._degradation_level,
                "rate_limit_errors": self._rate_limit_errors,
            }
    
    def report_rate_limit_error(self) -> None:
        """
        报告限速错误（触发降级）
        
        当收到交易所 429/418 等限速错误时调用此方法
        """
        now = time.time()
        
        with self._lock:
            self._rate_limit_errors += 1
            self._last_rate_limit_error = now
            
            # 触发降级
            self._degrade()
            
            logger.warning(
                f"[RateLimiter] 限速错误，已降级到 level {self._degradation_level}, "
                f"当前速率: {self.config.requests_per_minute}/min "
                f"(原始: {self._original_config.requests_per_minute}/min)"
            )
    
    def _degrade(self) -> None:
        """执行降级"""
        # 计算新的速率
        min_rate = int(self._original_config.requests_per_minute * self.MIN_RATE_FACTOR)
        
        if self.config.requests_per_minute <= min_rate:
            # 已经是最低速率
            return
        
        self._degradation_level += 1
        
        # 降低速率
        new_rate = int(self.config.requests_per_minute * self.DEGRADATION_FACTOR)
        new_rate = max(new_rate, min_rate)
        
        self.config.requests_per_minute = new_rate
        self._refill_rate = new_rate / 60.0
        
        # 同时降低突发大小
        new_burst = max(5, int(self.config.burst_size * self.DEGRADATION_FACTOR))
        self.config.burst_size = new_burst
        self._max_tokens = float(new_burst)
        self._tokens = min(self._tokens, self._max_tokens)
    
    def try_recover(self) -> bool:
        """
        尝试恢复速率
        
        如果距离上次限速错误已过 RECOVERY_TIME，尝试恢复一级
        
        Returns:
            是否成功恢复
        """
        now = time.time()
        
        with self._lock:
            if self._degradation_level <= 0:
                return False
            
            if now - self._last_rate_limit_error < self.RECOVERY_TIME:
                return False
            
            # 恢复一级
            self._degradation_level -= 1
            
            if self._degradation_level == 0:
                # 完全恢复
                self.config.requests_per_minute = self._original_config.requests_per_minute
                self.config.burst_size = self._original_config.burst_size
            else:
                # 部分恢复
                factor = self.DEGRADATION_FACTOR ** self._degradation_level
                self.config.requests_per_minute = int(
                    self._original_config.requests_per_minute * factor
                )
                self.config.burst_size = max(5, int(
                    self._original_config.burst_size * factor
                ))
            
            self._refill_rate = self.config.requests_per_minute / 60.0
            self._max_tokens = float(self.config.burst_size)
            
            logger.info(
                f"[RateLimiter] 速率恢复到 level {self._degradation_level}, "
                f"当前速率: {self.config.requests_per_minute}/min"
            )
            return True
    
    def reset_degradation(self) -> None:
        """重置降级状态（完全恢复）"""
        with self._lock:
            self._degradation_level = 0
            self.config.requests_per_minute = self._original_config.requests_per_minute
            self.config.burst_size = self._original_config.burst_size
            self.config.min_interval_ms = self._original_config.min_interval_ms
            self._refill_rate = self.config.requests_per_minute / 60.0
            self._max_tokens = float(self.config.burst_size)
            logger.info("[RateLimiter] 降级状态已重置")


# =============================================================================
# 全局限速器实例（按交易所）
# =============================================================================

_rate_limiters: Dict[str, TokenBucketRateLimiter] = {}
_global_lock = threading.Lock()

# 各交易所的限速配置
EXCHANGE_RATE_LIMITS = {
    "binance": RateLimitConfig(
        requests_per_minute=2000,  # Binance: 2400/min，留 400 缓冲
        burst_size=100,
        min_interval_ms=25,  # ~2400/min
    ),
    "okx": RateLimitConfig(
        requests_per_minute=500,  # OKX 限制更严格
        burst_size=50,
        min_interval_ms=100,
    ),
    "bitget": RateLimitConfig(
        requests_per_minute=500,
        burst_size=50,
        min_interval_ms=100,
    ),
    "hyperliquid": RateLimitConfig(
        requests_per_minute=1000,
        burst_size=80,
        min_interval_ms=50,
    ),
}


def get_rate_limiter(exchange: str) -> TokenBucketRateLimiter:
    """
    获取指定交易所的限速器（单例）
    
    Args:
        exchange: 交易所名称（binance, okx, bitget, hyperliquid）
    
    Returns:
        该交易所的限速器实例
    """
    exchange = exchange.lower()
    
    with _global_lock:
        if exchange not in _rate_limiters:
            config = EXCHANGE_RATE_LIMITS.get(exchange, RateLimitConfig())
            _rate_limiters[exchange] = TokenBucketRateLimiter(config)
            logger.info(f"[RateLimiter] 创建 {exchange} 限速器: {config.requests_per_minute}/min")
        
        return _rate_limiters[exchange]


def get_all_stats() -> Dict[str, Dict]:
    """获取所有限速器的统计信息"""
    with _global_lock:
        return {
            exchange: limiter.get_stats()
            for exchange, limiter in _rate_limiters.items()
        }


# =============================================================================
# Binance 专用限速器（支持 API Key 级别限速）
# =============================================================================

# Binance 端点权重表（用户私有 API）
# 参考：https://binance-docs.github.io/apidocs/futures/cn/#ip
BINANCE_ENDPOINT_WEIGHTS = {
    # 账户相关（权重较高）
    "futures_account": 5,
    "futures_account_balance": 5,
    "futures_position_information": 5,
    "futures_account_trades": 5,
    "futures_get_all_orders": 5,
    "futures_income_history": 30,
    
    # 订单相关（权重较低）
    "futures_create_order": 1,
    "futures_cancel_order": 1,
    "futures_cancel_all_open_orders": 1,
    "futures_get_order": 1,
    "futures_get_open_orders": 1,
    
    # 杠杆/保证金
    "futures_change_leverage": 1,
    "futures_change_margin_type": 1,
    "futures_change_position_margin": 1,
    
    # Listen Key（权重低）
    "futures_stream_get_listen_key": 1,
    "futures_stream_keepalive": 1,
    "futures_stream_close": 1,
    
    # 公共数据（通常不需要限速，但以防万一）
    "futures_exchange_info": 1,
    "futures_mark_price": 1,
    "futures_ticker": 1,
    "futures_leverage_bracket": 1,
    
    # 默认权重
    "_default": 1,
}


# API Key 级别限速器缓存
_binance_key_limiters: Dict[str, TokenBucketRateLimiter] = {}
_binance_key_lock = threading.Lock()

# Binance API Key 限速配置
BINANCE_KEY_RATE_LIMIT = RateLimitConfig(
    requests_per_minute=1200,  # 每个 Key 1200/min（保守值）
    burst_size=60,
    min_interval_ms=50,
)

# Binance IP 级别全局限速器（所有用户共享）
_binance_ip_limiter: Optional[TokenBucketRateLimiter] = None
_binance_ip_lock = threading.Lock()

# Binance IP 级别限速配置（更保守，避免触发 418）
BINANCE_IP_RATE_LIMIT = RateLimitConfig(
    requests_per_minute=1800,  # Binance 限制 2400/min，留 600 缓冲
    burst_size=50,  # 降低突发，避免瞬间过载
    min_interval_ms=35,  # ~1700/min
)


def _get_binance_ip_limiter() -> TokenBucketRateLimiter:
    """获取 Binance IP 级别全局限速器（单例）"""
    global _binance_ip_limiter
    with _binance_ip_lock:
        if _binance_ip_limiter is None:
            _binance_ip_limiter = TokenBucketRateLimiter(BINANCE_IP_RATE_LIMIT)
            logger.info(f"[BinanceRateLimiter] 创建 IP 级别全局限速器: {BINANCE_IP_RATE_LIMIT.requests_per_minute}/min")
        return _binance_ip_limiter


class BinanceRateLimiter:
    """
    Binance 专用限速器
    
    特点：
    - 支持 API Key 级别限速（每个用户独立）
    - 支持端点权重
    - 同时检查 IP 级别和 Key 级别限制
    
    使用方式：
        limiter = get_binance_rate_limiter(api_key)
        if await limiter.acquire_async(endpoint="futures_account"):
            # 执行 API 调用
    """
    
    def __init__(self, api_key: str = None):
        """
        初始化 Binance 限速器
        
        Args:
            api_key: Binance API Key，用于 Key 级别限速
        """
        self._api_key = api_key
        self._key_limiter: Optional[TokenBucketRateLimiter] = None
        
        # 获取 Key 级别限速器
        if api_key:
            self._key_limiter = self._get_or_create_key_limiter(api_key)
    
    @staticmethod
    def _get_or_create_key_limiter(api_key: str) -> TokenBucketRateLimiter:
        """获取或创建 API Key 专用限速器"""
        # 使用 API Key 的哈希作为标识（避免存储完整 Key）
        key_hash = hashlib.md5(api_key.encode()).hexdigest()[:12]
        limiter_name = f"binance_key_{key_hash}"
        
        with _binance_key_lock:
            if limiter_name not in _binance_key_limiters:
                _binance_key_limiters[limiter_name] = TokenBucketRateLimiter(BINANCE_KEY_RATE_LIMIT)
                logger.debug(f"[BinanceRateLimiter] 创建 Key 限速器: {limiter_name}")
            return _binance_key_limiters[limiter_name]
    
    @staticmethod
    def get_endpoint_weight(endpoint: str) -> int:
        """获取端点权重"""
        return BINANCE_ENDPOINT_WEIGHTS.get(endpoint, BINANCE_ENDPOINT_WEIGHTS["_default"])
    
    def acquire(self, endpoint: str = None, timeout: float = 30.0) -> bool:
        """
        同步获取限速许可
        
        Args:
            endpoint: API 端点名称（用于计算权重）
            timeout: 最大等待时间（秒）
        
        Returns:
            是否成功获取许可
        """
        weight = self.get_endpoint_weight(endpoint) if endpoint else 1
        
        # 1. 先检查 IP 级别全局限速（所有用户共享）
        ip_limiter = _get_binance_ip_limiter()
        for _ in range(weight):
            if not ip_limiter.acquire(timeout=timeout):
                logger.warning(f"[BinanceRateLimiter] IP 级别限速器超时: {endpoint}")
                return False
        
        # 2. 再检查 Key 级别限速（如果有）
        if self._key_limiter:
            for _ in range(weight):
                if not self._key_limiter.acquire(timeout=timeout):
                    logger.warning(f"[BinanceRateLimiter] Key 限速器超时: {endpoint}")
                    return False
        
        return True
    
    async def acquire_async(self, endpoint: str = None, timeout: float = 30.0) -> bool:
        """
        异步获取限速许可
        
        Args:
            endpoint: API 端点名称（用于计算权重）
            timeout: 最大等待时间（秒）
        
        Returns:
            是否成功获取许可
        """
        weight = self.get_endpoint_weight(endpoint) if endpoint else 1
        
        # 1. 先检查 IP 级别全局限速（所有用户共享）
        ip_limiter = _get_binance_ip_limiter()
        for _ in range(weight):
            if not await ip_limiter.acquire_async(timeout=timeout):
                logger.warning(f"[BinanceRateLimiter] IP 级别限速器超时: {endpoint}")
                return False
        
        # 2. 再检查 Key 级别限速（如果有）
        if self._key_limiter:
            for _ in range(weight):
                if not await self._key_limiter.acquire_async(timeout=timeout):
                    logger.warning(f"[BinanceRateLimiter] Key 限速器超时: {endpoint}")
                    return False
        
        return True
    
    def get_stats(self) -> Dict:
        """获取统计信息"""
        stats = {
            "api_key_hash": hashlib.md5(self._api_key.encode()).hexdigest()[:8] if self._api_key else None,
        }
        
        if self._key_limiter:
            stats["key_limiter"] = self._key_limiter.get_stats()
        
        return stats


def get_binance_rate_limiter(api_key: str = None) -> BinanceRateLimiter:
    """
    获取 Binance 限速器实例
    
    Args:
        api_key: Binance API Key，用于 Key 级别限速
    
    Returns:
        BinanceRateLimiter 实例
    
    使用示例：
        # 用户级限速
        limiter = get_binance_rate_limiter(user_api_key)
        if await limiter.acquire_async(endpoint="futures_account"):
            result = await client.futures_account()
    """
    return BinanceRateLimiter(api_key)


def get_binance_key_limiter_stats() -> Dict[str, Dict]:
    """获取所有 Binance Key 限速器的统计信息"""
    with _binance_key_lock:
        return {
            name: limiter.get_stats()
            for name, limiter in _binance_key_limiters.items()
        }


# =============================================================================
# OKX 专用限速器（支持 API Key 级别限速）
# =============================================================================

# OKX 端点权重表
# 参考：https://www.okx.com/docs-v5/zh/#overview-rate-limit
# OKX 限速规则：
# - 私有接口：按 API Key 限速
# - 公共接口：按 IP 限速
# - 不同端点有不同的限速规则
OKX_ENDPOINT_WEIGHTS = {
    # 账户相关（权重较高）
    "/api/v5/account/balance": 10,           # 获取账户余额 10次/2s
    "/api/v5/account/positions": 10,         # 获取持仓 10次/2s
    "/api/v5/account/account-position-risk": 10,  # 账户风险 10次/2s
    "/api/v5/account/config": 5,             # 账户配置 5次/2s
    "/api/v5/account/set-leverage": 20,      # 设置杠杆 20次/2s
    
    # 交易相关
    "/api/v5/trade/order": 60,               # 下单 60次/2s
    "/api/v5/trade/cancel-order": 60,        # 撤单 60次/2s
    "/api/v5/trade/orders-pending": 60,      # 获取未成交订单 60次/2s
    "/api/v5/trade/orders-history": 40,      # 历史订单 40次/2s
    "/api/v5/trade/fills": 60,               # 成交明细 60次/2s
    "/api/v5/trade/order-algo": 20,          # 策略委托 20次/2s
    "/api/v5/trade/cancel-algos": 20,        # 撤销策略委托 20次/2s
    "/api/v5/trade/orders-algo-pending": 20, # 获取未完成策略委托 20次/2s
    
    # 公共数据（按 IP 限速，权重较低）
    "/api/v5/public/instruments": 20,        # 获取交易产品 20次/2s
    "/api/v5/market/ticker": 20,             # 获取行情 20次/2s
    "/api/v5/market/mark-price": 10,         # 获取标记价格 10次/2s
    
    # 默认权重
    "_default": 10,
}

# OKX API Key 级别限速器缓存
_okx_key_limiters: Dict[str, TokenBucketRateLimiter] = {}
_okx_key_lock = threading.Lock()

# OKX API Key 限速配置
# OKX 大部分私有接口是 10-60次/2s，我们使用保守值
OKX_KEY_RATE_LIMIT = RateLimitConfig(
    requests_per_minute=300,  # 每个 Key 300/min（保守值，约 5/s）
    burst_size=30,
    min_interval_ms=100,  # 最小间隔 100ms
)

# OKX IP 级别全局限速器（所有用户共享）
_okx_ip_limiter: Optional[TokenBucketRateLimiter] = None
_okx_ip_lock = threading.Lock()

# OKX IP 级别限速配置
# 1000+ 用户场景下需要更保守的配置，避免触发 IP 封禁
OKX_IP_RATE_LIMIT = RateLimitConfig(
    requests_per_minute=300,  # 降低到 300/min，1000 用户共享
    burst_size=20,            # 降低突发，避免瞬间过载
    min_interval_ms=150,      # 增加最小间隔
)


def _get_okx_ip_limiter() -> TokenBucketRateLimiter:
    """获取 OKX IP 级别全局限速器（单例）"""
    global _okx_ip_limiter
    with _okx_ip_lock:
        if _okx_ip_limiter is None:
            _okx_ip_limiter = TokenBucketRateLimiter(OKX_IP_RATE_LIMIT)
            logger.info(f"[OKXRateLimiter] 创建 IP 级别全局限速器: {OKX_IP_RATE_LIMIT.requests_per_minute}/min")
        return _okx_ip_limiter


class OKXRateLimiter:
    """
    OKX 专用限速器
    
    特点：
    - 支持 API Key 级别限速（每个用户独立）
    - 支持端点权重
    
    使用方式：
        limiter = get_okx_rate_limiter(api_key)
        if await limiter.acquire_async(endpoint="/api/v5/account/balance"):
            # 执行 API 调用
    """
    
    def __init__(self, api_key: str = None):
        """
        初始化 OKX 限速器
        
        Args:
            api_key: OKX API Key，用于 Key 级别限速
        """
        self._api_key = api_key
        self._key_limiter: Optional[TokenBucketRateLimiter] = None
        
        # 获取 Key 级别限速器
        if api_key:
            self._key_limiter = self._get_or_create_key_limiter(api_key)
    
    @staticmethod
    def _get_or_create_key_limiter(api_key: str) -> TokenBucketRateLimiter:
        """获取或创建 API Key 专用限速器"""
        # 使用 API Key 的哈希作为标识（避免存储完整 Key）
        key_hash = hashlib.md5(api_key.encode()).hexdigest()[:12]
        limiter_name = f"okx_key_{key_hash}"
        
        with _okx_key_lock:
            if limiter_name not in _okx_key_limiters:
                _okx_key_limiters[limiter_name] = TokenBucketRateLimiter(OKX_KEY_RATE_LIMIT)
                logger.debug(f"[OKXRateLimiter] 创建 Key 限速器: {limiter_name}")
            return _okx_key_limiters[limiter_name]
    
    @staticmethod
    def get_endpoint_weight(endpoint: str) -> int:
        """
        获取端点权重
        
        OKX 的权重表示每 2 秒允许的请求数，数值越大限制越宽松
        我们将其转换为消耗的令牌数：权重越高消耗越少
        """
        # 直接返回 1，因为 OKX 的限速是按端点独立计算的
        # 我们使用统一的保守限速策略
        return 1
    
    def acquire(self, endpoint: str = None, timeout: float = 30.0) -> bool:
        """
        同步获取限速许可
        
        Args:
            endpoint: API 端点路径（用于日志）
            timeout: 最大等待时间（秒）
        
        Returns:
            是否成功获取许可
        """
        # 1. 先检查 IP 级别全局限速（所有用户共享）
        ip_limiter = _get_okx_ip_limiter()
        if not ip_limiter.acquire(timeout=timeout):
            logger.warning(f"[OKXRateLimiter] IP 级别限速器超时: {endpoint}")
            return False
        
        # 2. 再检查 Key 级别限速（如果有）
        if self._key_limiter:
            if not self._key_limiter.acquire(timeout=timeout):
                logger.warning(f"[OKXRateLimiter] Key 限速器超时: {endpoint}")
                return False
        
        return True
    
    async def acquire_async(self, endpoint: str = None, timeout: float = 30.0) -> bool:
        """
        异步获取限速许可
        
        Args:
            endpoint: API 端点路径（用于日志）
            timeout: 最大等待时间（秒）
        
        Returns:
            是否成功获取许可
        """
        # 1. 先检查 IP 级别全局限速（所有用户共享）
        ip_limiter = _get_okx_ip_limiter()
        if not await ip_limiter.acquire_async(timeout=timeout):
            logger.warning(f"[OKXRateLimiter] IP 级别限速器超时: {endpoint}")
            return False
        
        # 2. 再检查 Key 级别限速（如果有）
        if self._key_limiter:
            if not await self._key_limiter.acquire_async(timeout=timeout):
                logger.warning(f"[OKXRateLimiter] Key 限速器超时: {endpoint}")
                return False
        
        return True
    
    def get_stats(self) -> Dict:
        """获取统计信息"""
        stats = {
            "api_key_hash": hashlib.md5(self._api_key.encode()).hexdigest()[:8] if self._api_key else None,
        }
        
        if self._key_limiter:
            stats["key_limiter"] = self._key_limiter.get_stats()
        
        return stats


def get_okx_rate_limiter(api_key: str = None) -> OKXRateLimiter:
    """
    获取 OKX 限速器实例
    
    Args:
        api_key: OKX API Key，用于 Key 级别限速
    
    Returns:
        OKXRateLimiter 实例
    
    使用示例：
        # 用户级限速
        limiter = get_okx_rate_limiter(user_api_key)
        if await limiter.acquire_async(endpoint="/api/v5/account/balance"):
            result = await client.get_account()
    """
    return OKXRateLimiter(api_key)


def get_okx_key_limiter_stats() -> Dict[str, Dict]:
    """获取所有 OKX Key 限速器的统计信息"""
    with _okx_key_lock:
        return {
            name: limiter.get_stats()
            for name, limiter in _okx_key_limiters.items()
        }


# =============================================================================
# Bitget 专用限速器（支持 API Key 级别限速）
# =============================================================================

# Bitget 端点权重表
# 参考：https://www.bitget.com/api-doc/common/rate-limit
# Bitget 限速规则：
# - 私有接口：按 API Key 限速
# - 公共接口：按 IP 限速
BITGET_ENDPOINT_WEIGHTS = {
    # 账户相关
    "/api/v2/mix/account/account": 10,           # 获取账户信息
    "/api/v2/mix/account/accounts": 10,          # 获取所有账户
    "/api/v2/mix/position/all-position": 5,      # 获取所有持仓
    "/api/v2/mix/position/single-position": 10,  # 获取单个持仓
    
    # 交易相关
    "/api/v2/mix/order/place-order": 10,         # 下单
    "/api/v2/mix/order/cancel-order": 10,        # 撤单
    "/api/v2/mix/order/orders-pending": 20,      # 获取未成交订单
    "/api/v2/mix/order/orders-history": 10,      # 历史订单
    "/api/v2/mix/order/fills": 10,               # 成交明细
    
    # 计划委托
    "/api/v2/mix/order/place-plan-order": 10,    # 计划委托下单
    "/api/v2/mix/order/cancel-plan-order": 10,   # 撤销计划委托
    "/api/v2/mix/order/orders-plan-pending": 10, # 获取计划委托
    
    # 止盈止损
    "/api/v2/mix/order/place-tpsl-order": 10,    # 止盈止损下单
    "/api/v2/mix/order/modify-tpsl-order": 10,   # 修改止盈止损
    
    # 公共数据
    "/api/v2/mix/market/contracts": 20,          # 获取合约信息
    "/api/v2/mix/market/ticker": 20,             # 获取行情
    "/api/v2/mix/market/mark-price": 20,         # 获取标记价格
    
    # 默认权重
    "_default": 10,
}

# Bitget API Key 级别限速器缓存
_bitget_key_limiters: Dict[str, TokenBucketRateLimiter] = {}
_bitget_key_lock = threading.Lock()

# Bitget API Key 限速配置
# Bitget 大部分私有接口是 10-20次/s，我们使用保守值
BITGET_KEY_RATE_LIMIT = RateLimitConfig(
    requests_per_minute=300,  # 每个 Key 300/min（保守值，约 5/s）
    burst_size=30,
    min_interval_ms=100,  # 最小间隔 100ms
)

# Bitget IP 级别全局限速器（所有用户共享）
_bitget_ip_limiter: Optional[TokenBucketRateLimiter] = None
_bitget_ip_lock = threading.Lock()

# Bitget IP 级别限速配置
# 1000+ 用户场景下需要更保守的配置，避免触发 IP 封禁
BITGET_IP_RATE_LIMIT = RateLimitConfig(
    requests_per_minute=300,  # 降低到 300/min，1000 用户共享
    burst_size=20,            # 降低突发，避免瞬间过载
    min_interval_ms=150,      # 增加最小间隔
)


def _get_bitget_ip_limiter() -> TokenBucketRateLimiter:
    """获取 Bitget IP 级别全局限速器（单例）"""
    global _bitget_ip_limiter
    with _bitget_ip_lock:
        if _bitget_ip_limiter is None:
            _bitget_ip_limiter = TokenBucketRateLimiter(BITGET_IP_RATE_LIMIT)
            logger.info(f"[BitgetRateLimiter] 创建 IP 级别全局限速器: {BITGET_IP_RATE_LIMIT.requests_per_minute}/min")
        return _bitget_ip_limiter


class BitgetRateLimiter:
    """
    Bitget 专用限速器
    
    特点：
    - 支持 API Key 级别限速（每个用户独立）
    - 支持端点权重
    
    使用方式：
        limiter = get_bitget_rate_limiter(api_key)
        if await limiter.acquire_async(endpoint="/api/v2/mix/account/account"):
            # 执行 API 调用
    """
    
    def __init__(self, api_key: str = None):
        """
        初始化 Bitget 限速器
        
        Args:
            api_key: Bitget API Key，用于 Key 级别限速
        """
        self._api_key = api_key
        self._key_limiter: Optional[TokenBucketRateLimiter] = None
        
        # 获取 Key 级别限速器
        if api_key:
            self._key_limiter = self._get_or_create_key_limiter(api_key)
    
    @staticmethod
    def _get_or_create_key_limiter(api_key: str) -> TokenBucketRateLimiter:
        """获取或创建 API Key 专用限速器"""
        # 使用 API Key 的哈希作为标识（避免存储完整 Key）
        key_hash = hashlib.md5(api_key.encode()).hexdigest()[:12]
        limiter_name = f"bitget_key_{key_hash}"
        
        with _bitget_key_lock:
            if limiter_name not in _bitget_key_limiters:
                _bitget_key_limiters[limiter_name] = TokenBucketRateLimiter(BITGET_KEY_RATE_LIMIT)
                logger.debug(f"[BitgetRateLimiter] 创建 Key 限速器: {limiter_name}")
            return _bitget_key_limiters[limiter_name]
    
    @staticmethod
    def get_endpoint_weight(endpoint: str) -> int:
        """获取端点权重"""
        return BITGET_ENDPOINT_WEIGHTS.get(endpoint, BITGET_ENDPOINT_WEIGHTS["_default"])
    
    def acquire(self, endpoint: str = None, timeout: float = 30.0) -> bool:
        """
        同步获取限速许可
        
        Args:
            endpoint: API 端点路径（用于日志）
            timeout: 最大等待时间（秒）
        
        Returns:
            是否成功获取许可
        """
        # 1. 先检查 IP 级别全局限速（所有用户共享）
        ip_limiter = _get_bitget_ip_limiter()
        if not ip_limiter.acquire(timeout=timeout):
            logger.warning(f"[BitgetRateLimiter] IP 级别限速器超时: {endpoint}")
            return False
        
        # 2. 再检查 Key 级别限速（如果有）
        if self._key_limiter:
            if not self._key_limiter.acquire(timeout=timeout):
                logger.warning(f"[BitgetRateLimiter] Key 限速器超时: {endpoint}")
                return False
        
        return True
    
    async def acquire_async(self, endpoint: str = None, timeout: float = 30.0) -> bool:
        """
        异步获取限速许可
        
        Args:
            endpoint: API 端点路径（用于日志）
            timeout: 最大等待时间（秒）
        
        Returns:
            是否成功获取许可
        """
        # 1. 先检查 IP 级别全局限速（所有用户共享）
        ip_limiter = _get_bitget_ip_limiter()
        if not await ip_limiter.acquire_async(timeout=timeout):
            logger.warning(f"[BitgetRateLimiter] IP 级别限速器超时: {endpoint}")
            return False
        
        # 2. 再检查 Key 级别限速（如果有）
        if self._key_limiter:
            if not await self._key_limiter.acquire_async(timeout=timeout):
                logger.warning(f"[BitgetRateLimiter] Key 限速器超时: {endpoint}")
                return False
        
        return True
    
    def get_stats(self) -> Dict:
        """获取统计信息"""
        stats = {
            "api_key_hash": hashlib.md5(self._api_key.encode()).hexdigest()[:8] if self._api_key else None,
        }
        
        if self._key_limiter:
            stats["key_limiter"] = self._key_limiter.get_stats()
        
        return stats


def get_bitget_rate_limiter(api_key: str = None) -> BitgetRateLimiter:
    """
    获取 Bitget 限速器实例
    
    Args:
        api_key: Bitget API Key，用于 Key 级别限速
    
    Returns:
        BitgetRateLimiter 实例
    
    使用示例：
        # 用户级限速
        limiter = get_bitget_rate_limiter(user_api_key)
        if await limiter.acquire_async(endpoint="/api/v2/mix/account/account"):
            result = await client.get_account()
    """
    return BitgetRateLimiter(api_key)


def get_bitget_key_limiter_stats() -> Dict[str, Dict]:
    """获取所有 Bitget Key 限速器的统计信息"""
    with _bitget_key_lock:
        return {
            name: limiter.get_stats()
            for name, limiter in _bitget_key_limiters.items()
        }

