# volume_stats.py - 市场数据 API 封装 (异步版本)
"""
外部 API 数据获取模块 - 异步版本

提供资金费率、持仓量、24小时变化等数据获取功能。
使用 aiohttp 实现异步请求，包含重试、超时、熔断机制。

异步函数:
    - get_funding_rate_async(symbol) -> Optional[float]
    - get_open_interest_async(symbol) -> Optional[float]
    - get_24hr_change_async(symbol) -> Optional[Dict]
    - batch_fetch_async(symbols) -> Dict

同步函数 (兼容旧代码):
    - get_funding_rate(symbol) -> Optional[float]
    - get_open_interest(symbol) -> Optional[float]
    - get_24hr_change(symbol) -> Optional[Dict]
"""

import time
import asyncio
import logging
import threading
import aiohttp
from typing import Optional, Dict, Any, List
from core.config import OI_BASE_URL as BASE

logger = logging.getLogger(__name__)

# ==========================================================
# URL mapping
# ==========================================================
URLS = {
    "OPEN_INTEREST": BASE + "/fapi/v1/openInterest?symbol={symbol}",
    "OPEN_INTEREST_HIST": BASE + "/futures/data/openInterestHist",
    "FUNDING_RATE": BASE + "/fapi/v1/premiumIndex?symbol={symbol}",
    "TICKER_24HR": BASE + "/fapi/v1/ticker/24hr?symbol={symbol}",
}

# ==========================================================
# HTTP 请求配置
# ==========================================================
HTTP_TIMEOUT = 8          # 请求超时 (秒) - 增加到 8 秒
MAX_RETRIES = 2           # 最大重试次数
RETRY_DELAY = 0.5         # 重试间隔 (秒) - 增加间隔
BATCH_CONCURRENCY = 50     # 批量请求并发数限制

# 熔断器配置
CIRCUIT_BREAKER_THRESHOLD = 10  # 连续失败次数阈值 - 增加容忍度
CIRCUIT_BREAKER_RESET = 60      # 熔断重置时间 (秒)

# ==========================================================
# 熔断器状态 (线程安全)
# ==========================================================
_circuit_breaker_lock = threading.Lock()
_circuit_breaker = {
    "failures": 0,
    "last_failure": 0,
    "is_open": False,
}

def _check_circuit_breaker() -> bool:
    """检查熔断器状态，返回 True 表示可以请求"""
    with _circuit_breaker_lock:
        if not _circuit_breaker["is_open"]:
            return True
        
        # 检查是否可以重置
        if time.time() - _circuit_breaker["last_failure"] > CIRCUIT_BREAKER_RESET:
            _circuit_breaker["is_open"] = False
            _circuit_breaker["failures"] = 0
            logger.info("Circuit breaker reset - API calls enabled")
            return True
        
        return False

def _record_failure(reason: str = "unknown"):
    """记录失败，必要时触发熔断"""
    with _circuit_breaker_lock:
        _circuit_breaker["failures"] += 1
        _circuit_breaker["last_failure"] = time.time()
        current_failures = _circuit_breaker["failures"]
        
        if current_failures >= CIRCUIT_BREAKER_THRESHOLD:
            _circuit_breaker["is_open"] = True
            logger.warning(f"Circuit breaker OPEN - API calls disabled for {CIRCUIT_BREAKER_RESET}s (reason: {reason})")
        else:
            logger.debug(f"API failure recorded ({current_failures}/{CIRCUIT_BREAKER_THRESHOLD}): {reason}")

def _record_success():
    """记录成功，重置失败计数"""
    with _circuit_breaker_lock:
        _circuit_breaker["failures"] = 0


# ==========================================================
# 全局 aiohttp session (按事件循环管理)
# ==========================================================
_session_lock = threading.Lock()  # 使用线程锁，避免模块加载时 asyncio.Lock() 报错
_sessions: Dict[int, aiohttp.ClientSession] = {}  # loop_id -> session


async def _get_session() -> aiohttp.ClientSession:
    """
    获取当前事件循环的 aiohttp session
    
    每个事件循环维护独立的 session，避免跨循环使用导致的问题
    """
    global _sessions
    
    try:
        loop = asyncio.get_running_loop()
        loop_id = id(loop)
    except RuntimeError:
        raise RuntimeError("No running event loop")
    
    with _session_lock:
        session = _sessions.get(loop_id)
        if session is None or session.closed:
            timeout = aiohttp.ClientTimeout(total=HTTP_TIMEOUT)
            connector = aiohttp.TCPConnector(
                limit=20,  # 连接池大小
                limit_per_host=10,
                enable_cleanup_closed=True,  # 清理已关闭的连接
            )
            session = aiohttp.ClientSession(
                timeout=timeout,
                connector=connector,
            )
            _sessions[loop_id] = session
            logger.debug(f"Created new aiohttp session for loop {loop_id}")
        return session





async def close_session():
    """关闭当前事件循环的 aiohttp session"""
    global _sessions
    
    try:
        loop = asyncio.get_running_loop()
        loop_id = id(loop)
    except RuntimeError:
        return
    
    with _session_lock:
        session = _sessions.pop(loop_id, None)
        if session and not session.closed:
            await session.close()
            # 给 Windows 一点时间完成底层清理
            await asyncio.sleep(0.1)
            logger.debug(f"Closed aiohttp session for loop {loop_id}")


def close_all_sessions():
    """关闭所有 session (同步版本，用于程序退出)"""
    global _sessions
    with _session_lock:
        for loop_id, session in list(_sessions.items()):
            if session and not session.closed:
                try:
                    # 尝试在 session 所属的循环中关闭
                    session._connector.close()
                except Exception as e:
                    # P3 Fix: 添加日志
                    logger.debug(f"关闭 HTTP session 异常: {e}")
        _sessions.clear()


# ==========================================================
# 异步 HTTP 请求 (带重试)
# ==========================================================
async def _request_async(url: str, retries: int = MAX_RETRIES) -> Optional[Dict]:
    """
    异步 HTTP GET 请求 (带重试和熔断)
    
    Args:
        url: 请求 URL
        retries: 重试次数
    
    Returns:
        JSON 响应字典，失败返回 None
    """
    # 检查熔断器
    if not _check_circuit_breaker():
        logger.debug("Circuit breaker is open, skipping API call")
        return None
    
    try:
        session = await _get_session()
    except RuntimeError as e:
        logger.debug(f"Cannot get session: {e}")
        return None
    
    last_error = None
    
    for attempt in range(retries + 1):
        try:
            async with session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    _record_success()
                    return data
                elif response.status == 429:
                    # 限流错误，记录失败并等待更长时间
                    last_error = "Rate limited (429)"
                    logger.warning(f"API rate limited: {url}")
                    if attempt < retries:
                        await asyncio.sleep(2.0)  # 限流时等待更长
                    continue
                elif response.status >= 400 and response.status < 500:
                    # 其他客户端错误，不重试但记录
                    logger.debug(f"API client error {response.status}: {url}")
                    break
                else:
                    last_error = f"HTTP {response.status}"
                    logger.debug(f"API server error (attempt {attempt + 1}/{retries + 1}): {url}")
                    
        except asyncio.TimeoutError:
            last_error = "Timeout"
            logger.debug(f"API timeout (attempt {attempt + 1}/{retries + 1}): {url}")
            
        except aiohttp.ClientError as e:
            last_error = f"Client error: {e}"
            logger.debug(f"API client error (attempt {attempt + 1}/{retries + 1}): {url}")
            
        except Exception as e:
            last_error = f"Unexpected error: {e}"
            logger.debug(f"API unexpected error (attempt {attempt + 1}/{retries + 1}): {url} - {e}")
        
        # 重试前等待 (指数退避)
        if attempt < retries:
            await asyncio.sleep(RETRY_DELAY * (attempt + 1))
    
    # 所有重试都失败
    _record_failure(last_error or "unknown")
    logger.debug(f"API call failed after {retries + 1} attempts: {last_error}")
    return None


# ==========================================================
# Thread-safe in-memory cache
# ==========================================================
_cache_lock = threading.Lock()
_cached = {
    "oi": {},
    "funding": {},
    "24hr": {},
    "oi_change_1h": {},
    "oi_change_4h": {},
    "oi_change_24h": {},
}

def _cache_get(group: str, key: str, ttl: int) -> Optional[Any]:
    """Thread-safe cache read"""
    with _cache_lock:
        item = _cached[group].get(key)
        if not item:
            return None
        if time.time() - item["ts"] > ttl:
            return None
        return item["value"]

def _cache_set(group: str, key: str, value: Any):
    """Thread-safe cache write"""
    with _cache_lock:
        _cached[group][key] = {
            "value": value,
            "ts": time.time()
        }


# ==========================================================
# 异步 API 函数
# ==========================================================
async def get_open_interest_async(symbol: str) -> Optional[float]:
    """异步获取持仓量"""
    cached = _cache_get("oi", symbol, ttl=300)  # 5分钟缓存，全局共享
    if cached is not None:
        return cached

    value = None
    data = await _request_async(URLS["OPEN_INTEREST"].format(symbol=symbol))
    
    if data:
        oi = data.get("openInterest")
        value = float(oi) if oi is not None else None

    _cache_set("oi", symbol, value)
    return value


async def get_oi_change_async(symbol: str, period_hours: int = 24) -> Optional[Dict[str, float]]:
    """
    异步获取 OI 变化百分比
    
    Args:
        symbol: 交易对
        period_hours: 计算周期（小时），支持 1, 4, 24
    
    Returns:
        {
            "oi_current": float,
            "oi_past": float,
            "oi_change_pct": float,
            "period_hours": int
        }
    """
    cache_key = f"oi_change_{period_hours}h"
    cached = _cache_get(cache_key, symbol, ttl=300)
    if cached is not None:
        return cached
    
    result = None
    
    # 获取历史 OI 数据
    # period: 5m, 15m, 30m, 1h, 2h, 4h, 6h, 12h, 1d
    api_period = "1h" if period_hours <= 24 else "4h"
    limit = min(period_hours + 2, 50)  # 多取几条以防数据缺失
    
    url = f"{URLS['OPEN_INTEREST_HIST']}?symbol={symbol}&period={api_period}&limit={limit}"
    data = await _request_async(url)
    
    if data and isinstance(data, list) and len(data) >= 2:
        try:
            # 最新的 OI
            oi_current = float(data[-1].get("sumOpenInterest", 0))
            
            # 找到 period_hours 前的 OI
            # 数据按时间升序排列，间隔为 api_period
            target_idx = max(0, len(data) - period_hours - 1)
            oi_past = float(data[target_idx].get("sumOpenInterest", 0))
            
            if oi_past > 0:
                oi_change_pct = ((oi_current - oi_past) / oi_past) * 100
                result = {
                    "oi_current": oi_current,
                    "oi_past": oi_past,
                    "oi_change_pct": round(oi_change_pct, 2),
                    "period_hours": period_hours
                }
        except (ValueError, TypeError, IndexError) as e:
            logger.debug(f"计算 {symbol} OI 变化失败: {e}")
    
    _cache_set(cache_key, symbol, result)
    return result


async def get_funding_rate_async(symbol: str) -> Optional[float]:
    """异步获取资金费率"""
    cached = _cache_get("funding", symbol, ttl=300)  # 5分钟缓存，全局共享
    if cached is not None:
        return cached

    value = None
    data = await _request_async(URLS["FUNDING_RATE"].format(symbol=symbol))
    
    if data:
        fr = data.get("lastFundingRate")
        value = float(fr) if fr is not None else None

    _cache_set("funding", symbol, value)
    return value


async def get_24hr_change_async(symbol: str) -> Optional[Dict]:
    """异步获取24小时行情变化"""
    cached = _cache_get("24hr", symbol, ttl=300)  # 5分钟缓存，全局共享
    if cached is not None:
        return cached

    result = None
    data = await _request_async(URLS["TICKER_24HR"].format(symbol=symbol))
    
    if data:
        result = {
            "priceChange": float(data.get("priceChange", 0)),
            "priceChangePercent": float(data.get("priceChangePercent", 0)),
            "lastPrice": float(data.get("lastPrice", 0)),
            "highPrice": float(data.get("highPrice", 0)),
            "lowPrice": float(data.get("lowPrice", 0)),
            "volume": float(data.get("volume", 0)),
            "quoteVolume": float(data.get("quoteVolume", 0)),
        }

    _cache_set("24hr", symbol, result)
    return result


async def batch_fetch_async(symbols: List[str]) -> Dict[str, Dict[str, Any]]:
    """
    批量异步获取所有数据（带并发控制）
    
    Args:
        symbols: 交易对列表
    
    Returns:
        {
            "funding": {symbol: value, ...},
            "p24": {symbol: {...}, ...},
            "oi": {symbol: value, ...}
        }
    """
    results = {"funding": {}, "p24": {}, "oi": {}}
    
    # 使用信号量限制并发数
    semaphore = asyncio.Semaphore(BATCH_CONCURRENCY)
    
    async def fetch_with_semaphore(coro, symbol: str, data_type: str):
        """带并发控制的请求包装"""
        async with semaphore:
            try:
                result = await coro
                return (symbol, data_type, result)
            except Exception as e:
                logger.debug(f"Error fetching {data_type} for {symbol}: {e}")
                return (symbol, data_type, None)
    
    # 创建所有任务
    tasks = []
    
    for symbol in symbols:
        tasks.append(fetch_with_semaphore(
            get_funding_rate_async(symbol), symbol, "funding"
        ))
        tasks.append(fetch_with_semaphore(
            get_24hr_change_async(symbol), symbol, "p24"
        ))
        tasks.append(fetch_with_semaphore(
            get_open_interest_async(symbol), symbol, "oi"
        ))
    
    # 并发执行（受信号量限制）
    completed = await asyncio.gather(*tasks, return_exceptions=True)
    
    # 整理结果
    for item in completed:
        if item is None or isinstance(item, BaseException):
            if isinstance(item, BaseException):
                logger.debug(f"Batch fetch error: {item}")
            continue
        try:
            symbol, data_type, value = item
            results[data_type][symbol] = value
        except (TypeError, ValueError):
            continue
    
    return results


# ==========================================================
# 同步 API 函数 (兼容旧代码)
# ==========================================================
def _run_async(coro):
    """在同步上下文中运行异步函数"""
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            # 如果已在事件循环中，创建新任务
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor() as pool:
                future = pool.submit(asyncio.run, coro)
                return future.result(timeout=HTTP_TIMEOUT + 2)
        else:
            return loop.run_until_complete(coro)
    except RuntimeError:
        # 没有事件循环，创建新的
        return asyncio.run(coro)


def get_open_interest(symbol: str) -> Optional[float]:
    """同步获取持仓量 (兼容旧代码)"""
    cached = _cache_get("oi", symbol, ttl=300)  # 5分钟缓存
    if cached is not None:
        return cached
    return _run_async(get_open_interest_async(symbol))


def get_funding_rate(symbol: str) -> Optional[float]:
    """同步获取资金费率 (兼容旧代码)"""
    cached = _cache_get("funding", symbol, ttl=300)  # 5分钟缓存
    if cached is not None:
        return cached
    return _run_async(get_funding_rate_async(symbol))


def get_24hr_change(symbol: str) -> Optional[Dict]:
    """同步获取24小时行情变化 (兼容旧代码)"""
    cached = _cache_get("24hr", symbol, ttl=300)  # 5分钟缓存
    if cached is not None:
        return cached
    return _run_async(get_24hr_change_async(symbol))


# ==========================================================
# 健康检查
# ==========================================================
def get_api_health() -> Dict:
    """获取 API 健康状态"""
    with _circuit_breaker_lock:
        breaker_status = {
            "is_open": _circuit_breaker["is_open"],
            "failures": _circuit_breaker["failures"],
            "last_failure_ago": time.time() - _circuit_breaker["last_failure"] if _circuit_breaker["last_failure"] else None,
        }
    
    with _cache_lock:
        cache_stats = {
            "oi_entries": len(_cached["oi"]),
            "funding_entries": len(_cached["funding"]),
            "24hr_entries": len(_cached["24hr"]),
        }
    
    with _session_lock:
        active_sessions = sum(1 for s in _sessions.values() if s and not s.closed)
    
    return {
        "circuit_breaker": breaker_status,
        "cache": cache_stats,
        "active_sessions": active_sessions,
    }
