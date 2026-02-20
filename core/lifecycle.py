# core/lifecycle.py
"""
应用生命周期管理器

负责统一管理应用的启动、运行和关闭，确保：
1. 所有组件在单一 asyncio 事件循环中运行
2. 优雅关闭时按正确顺序停止所有服务
3. 信号处理和异常恢复

设计原则：
- 单一事件循环：所有异步操作共享同一个事件循环
- 统一生命周期：通过装饰器注册启动/关闭处理器
- 后台任务管理：支持自动重启的后台任务
"""

import asyncio
import signal
import logging
import sys
from typing import Dict, List, Callable, Awaitable, Optional, Any
from dataclasses import dataclass, field
from enum import Enum

logger = logging.getLogger(__name__)


class LifecycleState(Enum):
    """应用生命周期状态"""
    CREATED = "created"
    STARTING = "starting"
    RUNNING = "running"
    STOPPING = "stopping"
    STOPPED = "stopped"


@dataclass
class BackgroundTask:
    """后台任务定义"""
    name: str
    coro_func: Callable[[], Awaitable]
    restart_on_failure: bool = True
    restart_delay: float = 5.0
    task: Optional[asyncio.Task] = None


class ApplicationLifecycle:
    """
    应用生命周期管理器
    
    使用方式:
    ```python
    lifecycle = ApplicationLifecycle()
    
    @lifecycle.on_startup
    async def init_database():
        ...
    
    @lifecycle.on_shutdown
    async def close_database():
        ...
    
    lifecycle.add_background_task("scheduler", scheduler.start)
    
    async def main():
        await lifecycle.run()
    
    asyncio.run(main())
    ```
    """
    
    def __init__(self):
        self._startup_handlers: List[Callable[[], Awaitable]] = []
        self._shutdown_handlers: List[Callable[[], Awaitable]] = []
        self._background_tasks: Dict[str, BackgroundTask] = {}
        
        self._shutdown_event = asyncio.Event()
        self._state = LifecycleState.CREATED
        self._is_shutting_down = False
        
        # 错误记录
        self._startup_errors: List[Exception] = []
        self._shutdown_errors: List[Exception] = []
    
    @property
    def state(self) -> LifecycleState:
        """获取当前状态"""
        return self._state
    
    @property
    def is_running(self) -> bool:
        """是否正在运行"""
        return self._state == LifecycleState.RUNNING
    
    def on_startup(self, handler: Callable[[], Awaitable]) -> Callable[[], Awaitable]:
        """
        注册启动处理器（装饰器）
        
        处理器按注册顺序执行，任一处理器失败会阻止后续执行
        """
        self._startup_handlers.append(handler)
        return handler
    
    def on_shutdown(self, handler: Callable[[], Awaitable]) -> Callable[[], Awaitable]:
        """
        注册关闭处理器（装饰器）
        
        处理器按注册的逆序执行，即使某个处理器失败也会继续执行其他处理器
        """
        self._shutdown_handlers.append(handler)
        return handler
    
    def add_background_task(
        self,
        name: str,
        coro_func: Callable[[], Awaitable],
        restart_on_failure: bool = True,
        restart_delay: float = 5.0,
    ) -> None:
        """
        添加后台任务
        
        Args:
            name: 任务名称（用于日志和管理）
            coro_func: 协程函数（每次重启会调用此函数创建新协程）
            restart_on_failure: 任务异常退出时是否自动重启
            restart_delay: 重启前等待时间（秒）
        """
        if name in self._background_tasks:
            logger.warning(f"后台任务 {name} 已存在，将被替换")
        
        self._background_tasks[name] = BackgroundTask(
            name=name,
            coro_func=coro_func,
            restart_on_failure=restart_on_failure,
            restart_delay=restart_delay,
        )
        logger.debug(f"注册后台任务: {name}")
    
    def remove_background_task(self, name: str) -> bool:
        """
        移除后台任务
        
        如果任务正在运行，会先取消它
        """
        if name not in self._background_tasks:
            return False
        
        task_info = self._background_tasks[name]
        if task_info.task and not task_info.task.done():
            task_info.task.cancel()
        
        del self._background_tasks[name]
        logger.debug(f"移除后台任务: {name}")
        return True
    
    async def startup(self) -> bool:
        """
        执行启动流程
        
        Returns:
            是否成功启动所有组件
        """
        if self._state != LifecycleState.CREATED:
            logger.warning(f"无法启动：当前状态为 {self._state}")
            return False
        
        self._state = LifecycleState.STARTING
        self._startup_errors.clear()
        
        logger.info("=" * 50)
        logger.info("应用启动中...")
        logger.info("=" * 50)
        
        # 执行启动处理器
        for i, handler in enumerate(self._startup_handlers):
            handler_name = handler.__name__ if hasattr(handler, '__name__') else f"handler_{i}"
            try:
                logger.info(f"执行启动处理器: {handler_name}")
                await handler()
                logger.info(f"  [OK] {handler_name}")
            except Exception as e:
                logger.error(f"  [FAIL] {handler_name}: {e}")
                self._startup_errors.append(e)
                # 启动失败，需要回滚
                await self._rollback_startup(i)
                self._state = LifecycleState.STOPPED
                return False
        
        # 启动后台任务
        for name, task_info in self._background_tasks.items():
            self._start_background_task(task_info)
            logger.info(f"启动后台任务: {name}")
        
        self._state = LifecycleState.RUNNING
        logger.info("=" * 50)
        logger.info("应用启动完成")
        logger.info("=" * 50)
        
        return True
    
    async def _rollback_startup(self, failed_index: int) -> None:
        """回滚已执行的启动处理器"""
        logger.warning("启动失败，执行回滚...")
        
        # 逆序执行已成功的处理器对应的关闭处理器
        for i in range(min(failed_index, len(self._shutdown_handlers)) - 1, -1, -1):
            handler = self._shutdown_handlers[i]
            handler_name = handler.__name__ if hasattr(handler, '__name__') else f"handler_{i}"
            try:
                await asyncio.wait_for(handler(), timeout=10.0)
            except Exception as e:
                logger.warning(f"回滚处理器 {handler_name} 失败: {e}")
    
    async def shutdown(self) -> None:
        """
        执行关闭流程
        
        即使某个处理器失败也会继续执行其他处理器
        """
        if self._is_shutting_down:
            logger.debug("已在关闭中，跳过")
            return
        
        if self._state == LifecycleState.STOPPED:
            return
        
        self._is_shutting_down = True
        self._state = LifecycleState.STOPPING
        self._shutdown_errors.clear()
        
        logger.info("=" * 50)
        logger.info("应用关闭中...")
        logger.info("=" * 50)
        
        # 1. 停止所有后台任务
        logger.info("停止后台任务...")
        await self._stop_all_background_tasks()
        
        # 2. 执行关闭处理器（逆序）
        for i, handler in enumerate(reversed(self._shutdown_handlers)):
            handler_name = handler.__name__ if hasattr(handler, '__name__') else f"handler_{i}"
            try:
                logger.info(f"执行关闭处理器: {handler_name}")
                await asyncio.wait_for(handler(), timeout=30.0)
                logger.info(f"  [OK] {handler_name}")
            except asyncio.TimeoutError:
                logger.warning(f"  [TIMEOUT] {handler_name}")
                self._shutdown_errors.append(TimeoutError(f"{handler_name} 超时"))
            except Exception as e:
                logger.error(f"  [FAIL] {handler_name}: {e}")
                self._shutdown_errors.append(e)
        
        self._state = LifecycleState.STOPPED
        logger.info("=" * 50)
        logger.info("应用已关闭")
        logger.info("=" * 50)
    
    async def _stop_all_background_tasks(self) -> None:
        """停止所有后台任务"""
        for name, task_info in self._background_tasks.items():
            if task_info.task and not task_info.task.done():
                logger.debug(f"取消后台任务: {name}")
                task_info.task.cancel()
        
        # 等待所有任务完成
        tasks = [
            t.task for t in self._background_tasks.values()
            if t.task and not t.task.done()
        ]
        
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
    
    def _start_background_task(self, task_info: BackgroundTask) -> None:
        """启动单个后台任务"""
        
        async def task_wrapper():
            """任务包装器，支持自动重启"""
            while not self._is_shutting_down:
                try:
                    await task_info.coro_func()
                    # 正常退出
                    logger.info(f"后台任务 {task_info.name} 正常退出")
                    break
                except asyncio.CancelledError:
                    logger.info(f"后台任务 {task_info.name} 被取消")
                    break
                except Exception as e:
                    logger.error(f"后台任务 {task_info.name} 异常: {e}")
                    
                    if not task_info.restart_on_failure:
                        logger.info(f"后台任务 {task_info.name} 不重启")
                        break
                    
                    if self._is_shutting_down:
                        break
                    
                    logger.info(f"后台任务 {task_info.name} 将在 {task_info.restart_delay}s 后重启")
                    try:
                        await asyncio.sleep(task_info.restart_delay)
                    except asyncio.CancelledError:
                        break
        
        task_info.task = asyncio.create_task(
            task_wrapper(),
            name=task_info.name
        )
    
    def request_shutdown(self) -> None:
        """请求关闭（线程安全）"""
        self._shutdown_event.set()
    
    async def wait_for_shutdown(self) -> None:
        """等待关闭信号"""
        await self._shutdown_event.wait()
    
    def setup_signal_handlers(self) -> None:
        """
        设置信号处理器
        
        注意：必须在事件循环启动后调用
        """
        loop = asyncio.get_running_loop()
        
        def signal_handler(sig: signal.Signals):
            logger.info(f"收到信号 {sig.name}")
            self.request_shutdown()
        
        # Unix 信号
        if sys.platform != "win32":
            for sig in (signal.SIGINT, signal.SIGTERM):
                loop.add_signal_handler(sig, lambda s=sig: signal_handler(s))
            logger.debug("信号处理器已设置 (Unix)")
        else:
            # Windows: 使用 signal.signal (有限支持)
            def win_handler(signum, frame):
                signal_handler(signal.Signals(signum))
            
            signal.signal(signal.SIGINT, win_handler)
            signal.signal(signal.SIGTERM, win_handler)
            logger.debug("信号处理器已设置 (Windows)")
    
    async def run(self) -> None:
        """
        运行应用（主入口）
        
        阻塞直到收到关闭信号
        """
        # 设置信号处理器
        try:
            self.setup_signal_handlers()
        except Exception as e:
            logger.warning(f"设置信号处理器失败: {e}")
        
        # 启动
        if not await self.startup():
            logger.error("启动失败，退出")
            return
        
        # 等待关闭信号
        try:
            await self.wait_for_shutdown()
        except asyncio.CancelledError:
            pass
        
        # 关闭
        await self.shutdown()
    
    async def run_with_server(
        self,
        server_coro: Callable[[], Awaitable],
        server_name: str = "server"
    ) -> None:
        """
        运行应用，同时启动一个服务器
        
        Args:
            server_coro: 服务器协程函数
            server_name: 服务器名称
        """
        # 设置信号处理器
        try:
            self.setup_signal_handlers()
        except Exception as e:
            logger.warning(f"设置信号处理器失败: {e}")
        
        # 启动
        if not await self.startup():
            logger.error("启动失败，退出")
            return
        
        # 创建服务器任务
        server_task = asyncio.create_task(server_coro(), name=server_name)
        shutdown_task = asyncio.create_task(self.wait_for_shutdown(), name="shutdown-wait")
        
        try:
            # 等待任一任务完成
            done, pending = await asyncio.wait(
                [server_task, shutdown_task],
                return_when=asyncio.FIRST_COMPLETED
            )
            
            # 如果是关闭信号，取消服务器
            if shutdown_task in done:
                if not server_task.done():
                    server_task.cancel()
                    try:
                        await server_task
                    except asyncio.CancelledError:
                        pass
        except asyncio.CancelledError:
            pass
        finally:
            # 关闭
            await self.shutdown()
    
    def get_task_status(self) -> Dict[str, Any]:
        """获取所有后台任务的状态"""
        status = {}
        for name, task_info in self._background_tasks.items():
            if task_info.task:
                status[name] = {
                    "running": not task_info.task.done(),
                    "cancelled": task_info.task.cancelled() if task_info.task.done() else False,
                    "restart_on_failure": task_info.restart_on_failure,
                }
            else:
                status[name] = {
                    "running": False,
                    "cancelled": False,
                    "restart_on_failure": task_info.restart_on_failure,
                }
        return status


# 全局实例
lifecycle = ApplicationLifecycle()
