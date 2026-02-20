# core/async_helpers.py
"""
异步工具函数

提供纯 asyncio 架构所需的工具函数，包括：
1. 同步函数异步包装
2. 超时和重试装饰器
3. 并发执行工具
4. 用户任务隔离执行器
5. CPU密集型任务线程池（避免阻塞事件循环）
"""

import asyncio
import functools
import logging
import time
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import (
    Any, Callable, Coroutine, Dict, List, Optional, 
    TypeVar, Union, Awaitable, Tuple
)
from dataclasses import dataclass

logger = logging.getLogger(__name__)

T = TypeVar('T')

# =============================================================================
# CPU 密集型任务线程池
# =============================================================================

# 专用于 CPU 密集型任务的线程池（JSON序列化、数据处理等）
# 6核CPU配置，设置8个worker以充分利用多核
_cpu_executor = ThreadPoolExecutor(max_workers=8, thread_name_prefix="cpu_heavy")


def shutdown_cpu_executor():
    """关闭 CPU 密集型任务线程池（在程序退出时调用）"""
    global _cpu_executor
    if _cpu_executor:
        _cpu_executor.shutdown(wait=False)
        _cpu_executor = None
        logger.info("[async_helpers] CPU 线程池已关闭")


# P1 Fix: 注册 atexit 确保程序退出时关闭线程池
import atexit
atexit.register(shutdown_cpu_executor)


async def run_cpu_bound(func: Callable[..., T], *args, **kwargs) -> T:
    """
    在线程池中执行 CPU 密集型任务，避免阻塞 asyncio 事件循环
    
    适用于：
    - JSON 序列化/反序列化大型数据
    - 数据处理和转换
    - 技术指标计算
    - 字符串处理
    
    使用示例:
    ```python
    # 方式1：直接调用
    result = await run_cpu_bound(json.dumps, large_dict)
    
    # 方式2：带关键字参数
    result = await run_cpu_bound(process_data, data, precision=6)
    ```
    
    注意：此函数专为 CPU 密集型任务设计，I/O 操作应使用 run_sync
    """
    loop = asyncio.get_running_loop()
    
    # 使用 functools.partial 包装函数和参数
    # 这样可以正确处理所有参数情况
    if args or kwargs:
        func_with_args = functools.partial(func, *args, **kwargs)
        return await loop.run_in_executor(_cpu_executor, func_with_args)
    
    return await loop.run_in_executor(_cpu_executor, func)


# =============================================================================
# 同步函数异步包装
# =============================================================================

async def run_sync(func: Callable[..., T], *args, **kwargs) -> T:
    """
    在线程池中运行同步函数，不阻塞事件循环
    
    用于包装同步的第三方库（如 hyperliquid SDK）
    
    使用示例:
    ```python
    result = await run_sync(sync_api.get_account, address)
    ```
    """
    return await asyncio.to_thread(func, *args, **kwargs)


def sync_to_async(func: Callable[..., T]) -> Callable[..., Awaitable[T]]:
    """
    装饰器：将同步函数转换为异步函数
    
    使用示例:
    ```python
    @sync_to_async
    def blocking_operation(x):
        time.sleep(1)
        return x * 2
    
    result = await blocking_operation(5)  # 不会阻塞事件循环
    ```
    """
    @functools.wraps(func)
    async def wrapper(*args, **kwargs) -> T:
        return await asyncio.to_thread(func, *args, **kwargs)
    return wrapper


# =============================================================================
# 超时和重试
# =============================================================================

async def with_timeout(
    coro: Coroutine[Any, Any, T],
    timeout: float,
    default: T = None,
    raise_on_timeout: bool = False
) -> T:
    """
    为协程添加超时保护
    
    Args:
        coro: 协程
        timeout: 超时时间（秒）
        default: 超时时返回的默认值
        raise_on_timeout: 超时时是否抛出异常
    
    Returns:
        协程结果或默认值
    """
    try:
        return await asyncio.wait_for(coro, timeout=timeout)
    except asyncio.TimeoutError:
        if raise_on_timeout:
            raise
        return default


def async_retry(
    max_retries: int = 3,
    delay: float = 1.0,
    backoff: float = 2.0,
    exceptions: Tuple[type, ...] = (Exception,),
):
    """
    装饰器：异步函数重试
    
    Args:
        max_retries: 最大重试次数
        delay: 初始延迟（秒）
        backoff: 延迟倍增系数
        exceptions: 需要重试的异常类型
    
    使用示例:
    ```python
    @async_retry(max_retries=3, delay=1.0)
    async def fetch_data():
        ...
    ```
    """
    def decorator(func: Callable[..., Awaitable[T]]) -> Callable[..., Awaitable[T]]:
        @functools.wraps(func)
        async def wrapper(*args, **kwargs) -> T:
            current_delay = delay
            last_exception = None
            
            for attempt in range(max_retries + 1):
                try:
                    return await func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    if attempt < max_retries:
                        logger.warning(
                            f"{func.__name__} 失败 (尝试 {attempt + 1}/{max_retries + 1}): {e}"
                        )
                        await asyncio.sleep(current_delay)
                        current_delay *= backoff
            
            raise last_exception
        return wrapper
    return decorator


# =============================================================================
# 并发执行
# =============================================================================

@dataclass
class TaskResult:
    """任务执行结果"""
    name: str
    success: bool
    result: Any = None
    error: Optional[Exception] = None
    duration_ms: float = 0


async def gather_with_results(
    tasks: Dict[str, Coroutine],
    timeout: Optional[float] = None,
) -> Dict[str, TaskResult]:
    """
    并发执行多个任务，返回详细结果
    
    与 asyncio.gather 不同，此函数：
    1. 总是返回所有结果（不因单个失败而中断）
    2. 包含执行时间和错误信息
    3. 支持超时保护
    
    Args:
        tasks: {任务名: 协程} 字典
        timeout: 总超时时间（秒）
    
    Returns:
        {任务名: TaskResult} 字典
    
    使用示例:
    ```python
    results = await gather_with_results({
        "user_a": process_user("a"),
        "user_b": process_user("b"),
    }, timeout=60)
    
    for name, result in results.items():
        if result.success:
            print(f"{name}: {result.result}")
        else:
            print(f"{name} failed: {result.error}")
    ```
    """
    async def wrapped_task(name: str, coro: Coroutine) -> TaskResult:
        start = time.time()
        try:
            result = await coro
            return TaskResult(
                name=name,
                success=True,
                result=result,
                duration_ms=(time.time() - start) * 1000
            )
        except Exception as e:
            return TaskResult(
                name=name,
                success=False,
                error=e,
                duration_ms=(time.time() - start) * 1000
            )
    
    wrapped = {name: wrapped_task(name, coro) for name, coro in tasks.items()}
    
    if timeout:
        try:
            results = await asyncio.wait_for(
                asyncio.gather(*wrapped.values()),
                timeout=timeout
            )
        except asyncio.TimeoutError:
            # 返回已完成的结果，未完成的标记为超时
            results = []
            for name, task in wrapped.items():
                if isinstance(task, asyncio.Task) and task.done():
                    results.append(task.result())
                else:
                    results.append(TaskResult(
                        name=name,
                        success=False,
                        error=TimeoutError("Task timed out")
                    ))
    else:
        results = await asyncio.gather(*wrapped.values())
    
    return {r.name: r for r in results}


async def run_in_batches(
    items: List[T],
    processor: Callable[[T], Awaitable[Any]],
    batch_size: int = 10,
    batch_delay: float = 0.1,
) -> List[Any]:
    """
    分批并发处理列表项
    
    Args:
        items: 要处理的项列表
        processor: 处理函数
        batch_size: 每批大小
        batch_delay: 批次间延迟（秒）
    
    Returns:
        处理结果列表（与输入顺序一致）
    
    使用示例:
    ```python
    async def process_user(uid):
        ...
    
    results = await run_in_batches(
        user_ids,
        process_user,
        batch_size=50,
        batch_delay=1.0
    )
    ```
    """
    results = []
    
    for i in range(0, len(items), batch_size):
        batch = items[i:i + batch_size]
        batch_results = await asyncio.gather(
            *[processor(item) for item in batch],
            return_exceptions=True
        )
        results.extend(batch_results)
        
        # 批次间延迟
        if i + batch_size < len(items) and batch_delay > 0:
            await asyncio.sleep(batch_delay)
    
    return results


# =============================================================================
# 用户隔离执行器
# =============================================================================

@dataclass
class UserExecutionResult:
    """用户执行结果"""
    uid: str
    success: bool
    result: Any = None
    error: Optional[str] = None
    duration_ms: float = 0


class UserTaskExecutor:
    """
    用户任务隔离执行器
    
    确保单个用户的错误不会影响其他用户
    
    使用示例:
    ```python
    executor = UserTaskExecutor(timeout=300, max_concurrent=50)
    
    async def process_user(uid: str) -> bool:
        ...
    
    results = await executor.execute_all(
        user_ids,
        process_user
    )
    ```
    """
    
    def __init__(
        self,
        timeout: float = 300.0,
        max_concurrent: int = 50,
        error_handler: Optional[Callable[[str, Exception], Awaitable]] = None,
    ):
        """
        Args:
            timeout: 单用户超时时间（秒）
            max_concurrent: 最大并发数
            error_handler: 错误处理回调（可选）
        """
        self.timeout = timeout
        self.max_concurrent = max_concurrent
        self.error_handler = error_handler
        self._semaphore: Optional[asyncio.Semaphore] = None
    
    async def execute_single(
        self,
        uid: str,
        task_func: Callable[[str], Awaitable[Any]],
    ) -> UserExecutionResult:
        """
        执行单个用户任务（带隔离和超时保护）
        """
        start = time.time()
        
        try:
            async with asyncio.timeout(self.timeout):
                result = await task_func(uid)
                return UserExecutionResult(
                    uid=uid,
                    success=True,
                    result=result,
                    duration_ms=(time.time() - start) * 1000
                )
        
        except asyncio.TimeoutError:
            error_msg = f"执行超时 ({self.timeout}s)"
            logger.error(f"[{uid}] {error_msg}")
            if self.error_handler:
                try:
                    await self.error_handler(uid, TimeoutError(error_msg))
                except Exception:
                    pass
            return UserExecutionResult(
                uid=uid,
                success=False,
                error=error_msg,
                duration_ms=(time.time() - start) * 1000
            )
        
        except Exception as e:
            error_msg = str(e)
            logger.error(f"[{uid}] 执行异常: {error_msg}")
            if self.error_handler:
                try:
                    await self.error_handler(uid, e)
                except Exception:
                    pass
            return UserExecutionResult(
                uid=uid,
                success=False,
                error=error_msg,
                duration_ms=(time.time() - start) * 1000
            )
    
    async def execute_all(
        self,
        uids: List[str],
        task_func: Callable[[str], Awaitable[Any]],
    ) -> List[UserExecutionResult]:
        """
        并发执行所有用户任务（带限流）
        
        Args:
            uids: 用户 ID 列表
            task_func: 任务函数，接收 uid 参数
        
        Returns:
            执行结果列表（顺序与输入一致）
        """
        if not uids:
            return []
        
        semaphore = asyncio.Semaphore(self.max_concurrent)
        
        async def limited_execute(uid: str) -> UserExecutionResult:
            async with semaphore:
                return await self.execute_single(uid, task_func)
        
        results = await asyncio.gather(
            *[limited_execute(uid) for uid in uids]
        )
        
        return list(results)
    
    async def execute_in_batches(
        self,
        uids: List[str],
        task_func: Callable[[str], Awaitable[Any]],
        batch_size: Optional[int] = None,
        batch_delay: float = 1.0,
    ) -> List[UserExecutionResult]:
        """
        分批执行用户任务
        
        Args:
            uids: 用户 ID 列表
            task_func: 任务函数
            batch_size: 每批大小（默认使用 max_concurrent）
            batch_delay: 批次间延迟（秒）
        
        Returns:
            执行结果列表
        """
        if not uids:
            return []
        
        batch_size = batch_size or self.max_concurrent
        results = []
        
        for i in range(0, len(uids), batch_size):
            batch = uids[i:i + batch_size]
            batch_results = await asyncio.gather(
                *[self.execute_single(uid, task_func) for uid in batch]
            )
            results.extend(batch_results)
            
            # 批次间延迟
            if i + batch_size < len(uids) and batch_delay > 0:
                await asyncio.sleep(batch_delay)
        
        return results


# =============================================================================
# 定时任务
# =============================================================================

class AsyncPeriodicTask:
    """
    异步定时任务
    
    使用示例:
    ```python
    async def audit_positions():
        ...
    
    task = AsyncPeriodicTask(
        name="position_audit",
        interval=60,
        task_func=audit_positions
    )
    await task.start()
    
    # 稍后
    await task.stop()
    ```
    """
    
    def __init__(
        self,
        name: str,
        interval: float,
        task_func: Callable[[], Awaitable],
        run_immediately: bool = False,
    ):
        """
        Args:
            name: 任务名称
            interval: 执行间隔（秒）
            task_func: 任务函数
            run_immediately: 启动时是否立即执行一次
        """
        self.name = name
        self.interval = interval
        self.task_func = task_func
        self.run_immediately = run_immediately
        
        self._task: Optional[asyncio.Task] = None
        self._running = False
    
    async def start(self) -> None:
        """启动定时任务"""
        if self._running:
            return
        
        self._running = True
        self._task = asyncio.create_task(
            self._run_loop(),
            name=f"periodic-{self.name}"
        )
        logger.info(f"定时任务 {self.name} 已启动 (间隔 {self.interval}s)")
    
    async def stop(self) -> None:
        """停止定时任务"""
        self._running = False
        
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        
        logger.info(f"定时任务 {self.name} 已停止")
    
    async def _run_loop(self) -> None:
        """定时执行循环"""
        if self.run_immediately:
            await self._execute_safe()
        
        while self._running:
            try:
                await asyncio.sleep(self.interval)
            except asyncio.CancelledError:
                break
            
            if not self._running:
                break
            
            await self._execute_safe()
    
    async def _execute_safe(self) -> None:
        """安全执行任务"""
        try:
            await self.task_func()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"定时任务 {self.name} 执行异常: {e}")


# =============================================================================
# 异步锁工具
# =============================================================================

class AsyncLockManager:
    """
    异步锁管理器（支持命名锁）
    
    用于确保同一用户的操作不会并发执行
    
    使用示例:
    ```python
    lock_manager = AsyncLockManager()
    
    async with lock_manager.acquire(f"user:{uid}"):
        await process_user(uid)
    ```
    """
    
    def __init__(self):
        self._locks: Dict[str, asyncio.Lock] = {}
        # P1 Fix: 延迟初始化 asyncio.Lock，避免在模块导入时创建
        self._lock: Optional[asyncio.Lock] = None
        self._thread_lock = threading.Lock()  # 保护 get_lock 和 _get_internal_lock 的线程安全
    
    def _get_internal_lock(self) -> asyncio.Lock:
        """
        获取内部管理锁（延迟初始化）
        
        P1 Fix: 确保 asyncio.Lock 在事件循环中创建，而不是模块导入时
        """
        if self._lock is None:
            with self._thread_lock:
                if self._lock is None:
                    self._lock = asyncio.Lock()
        return self._lock
    
    async def acquire(self, name: str) -> asyncio.Lock:
        """获取命名锁"""
        async with self._get_internal_lock():
            if name not in self._locks:
                self._locks[name] = asyncio.Lock()
            return self._locks[name]
    
    def get_lock(self, name: str) -> asyncio.Lock:
        """
        获取锁对象（同步方法，用于 async with）
        
        注意：如果锁不存在会创建新的，使用 threading.Lock 保护线程安全
        """
        with self._thread_lock:
            if name not in self._locks:
                self._locks[name] = asyncio.Lock()
            return self._locks[name]
    
    async def cleanup_unused(self) -> int:
        """清理未使用的锁"""
        async with self._get_internal_lock():
            unused = [
                name for name, lock in self._locks.items()
                if not lock.locked()
            ]
            for name in unused:
                del self._locks[name]
            return len(unused)


# 全局锁管理器
lock_manager = AsyncLockManager()
