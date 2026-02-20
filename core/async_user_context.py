# async_user_context.py
"""
异步用户上下文管理模块

核心设计：
1. AsyncUserContext: 纯 asyncio 的用户上下文
2. AsyncUserContextManager: 异步管理所有用户上下文
3. 完全隔离：每个用户的错误不影响其他用户

与 user_context.py 的区别：
- 所有 I/O 操作都是 async
- 使用 asyncio.Lock 替代 threading.Lock
- 每个用户有独立的 batch_cache（不共享全局）
- 支持 TaskGroup 隔离执行
"""

import asyncio
import time
import threading
from typing import Dict, Optional, List, Any, Callable, Awaitable
from dataclasses import dataclass, field
import logging
import traceback

from core.user_context import UserConfig, UserContext

logger = logging.getLogger(__name__)


class AsyncUserContext:
    """
    异步用户上下文 - 纯 asyncio 版本
    
    特点：
    1. 所有 I/O 操作都是 async
    2. 独立的 batch_cache（不与其他用户共享）
    3. 支持优雅的错误隔离
    """
    
    def __init__(self, config: UserConfig, sync_ctx: Optional[UserContext] = None):
        """
        初始化异步用户上下文
        
        Args:
            config: 用户配置
            sync_ctx: 可选的同步上下文引用（用于访问同步 API）
        """
        self.config = config
        self.uid = config.uid
        self._sync_ctx = sync_ctx  # 可选：引用同步上下文
        
        # 用户级缓存（独立，不共享）
        self.account_snapshot: Dict[str, Any] = {
            "balance": 0.0,
            "available": 0.0,
            "total_unrealized": 0.0,
            "positions": [],
            "open_limit_orders": []
        }
        self.tp_sl_cache: Dict[str, Dict] = {}
        self.position_records: set = set()
        
        # 用户独立的 batch_cache（关键：不共享全局）
        self.batch_cache: Dict[str, Dict] = {}
        
        # 异步锁
        self._lock = asyncio.Lock()
        self._batch_cache_lock = asyncio.Lock()
        
        # LLM 客户端（懒加载）
        self._llm_client = None  # 全局配置的 client
        self._strategy_llm_clients: Dict[str, Any] = {}  # 策略ID -> LLM client 缓存
        self._llm_lock = asyncio.Lock()
        
        # 多交易所交易器（异步版）
        self._multi_trader = None
        self._trader_lock = asyncio.Lock()
        
        # 状态
        self.is_running = False
        self.last_active_at = time.time()
        self.error_count = 0
        self.last_error: Optional[str] = None
        
        # 用户任务（用于隔离执行）
        self._user_tasks: Dict[str, asyncio.Task] = {}
        
        logger.debug(f"[{self.uid}] AsyncUserContext 已创建")
    
    @classmethod
    async def create(cls, config: UserConfig, sync_ctx: Optional[UserContext] = None) -> "AsyncUserContext":
        """
        异步工厂方法（可进行异步初始化）
        """
        ctx = cls(config, sync_ctx)
        # 可在此执行异步初始化
        return ctx
    
    @property
    def sync_ctx(self) -> Optional[UserContext]:
        """获取同步上下文引用"""
        return self._sync_ctx
    
    def set_sync_ctx(self, sync_ctx: UserContext):
        """设置同步上下文引用"""
        self._sync_ctx = sync_ctx
    
    async def get_multi_trader(self):
        """获取多交易所交易器（懒加载，线程安全）"""
        if self._multi_trader is None:
            async with self._trader_lock:
                if self._multi_trader is None:
                    from trading.multi_exchange_trader import MultiExchangeTrader
                    self._multi_trader = MultiExchangeTrader(
                        self.uid,
                        on_auth_failed=self._on_trader_auth_failed
                    )
        return self._multi_trader
    
    def _on_trader_auth_failed(self, exchange_name: str, error_msg: str):
        """
        MultiExchangeTrader 认证失败回调
        
        当交易执行时检测到认证错误，触发停止该交易所的服务
        """
        logger.error(f"[{self.uid}] AsyncUserContext MultiExchangeTrader 检测到 {exchange_name} 认证失败: {error_msg}")
        if self._sync_ctx:
            self._sync_ctx.auto_stop_exchange_on_auth_failure(exchange_name, error_msg)
        else:
            logger.warning(f"[{self.uid}] 无法处理认证失败，sync_ctx 不存在")
    
    async def get_llm_client(self, strategy_id: str = None):
        """
        获取用户的 LLM 客户端
        
        Args:
            strategy_id: 如果指定策略ID，则创建新客户端（调用方需要关闭）。
                        如果不指定，则使用全局配置（缓存的客户端）。
        """
        async with self._llm_lock:
            # 如果指定了策略ID，每次创建新客户端
            if strategy_id:
                from core.strategy_cache import get_strategy_cache
                cache = get_strategy_cache()
                
                # 创建新的 LLM 客户端（不缓存，调用方需要关闭）
                # temperature 和 top_p 使用提供商默认值
                return cache.get_llm_client(
                    uid=self.uid,
                    strategy_id=strategy_id,
                    max_tokens=self.config.max_tokens,
                )
            
            # 使用全局配置
            if self._llm_client is None:
                self._llm_client = await self._create_llm_client_async()
            return self._llm_client
    
    async def _get_ai_strategy_async(self, strategy_id: str) -> Optional[Dict]:
        """异步获取 AI 策略配置（使用缓存）"""
        from core.strategy_cache import get_strategy_cache
        return get_strategy_cache().get_strategy(self.uid, strategy_id)
    
    async def _create_llm_client_async(self):
        """异步创建 LLM 客户端"""
        from llm.llm_client import create_llm_client
        # temperature 和 top_p 使用提供商默认值
        return create_llm_client(
            provider=self.config.llm_provider,
            model=self.config.llm_model,
            api_key=self.config.llm_api_key,
            base_url=self.config.llm_base_url,
            max_tokens=self.config.max_tokens,
        )
    
    async def _create_llm_client_from_strategy_async(self, strategy: Dict):
        """异步根据 AI 策略创建 LLM 客户端"""
        from llm.llm_client import create_llm_client
        
        # 用户自定义 LLM 参数（None 表示使用提供商默认值）
        temperature = strategy.get('temperature')
        top_p = strategy.get('top_p')
        max_tokens = strategy.get('max_tokens') or self.config.max_tokens
        
        return create_llm_client(
            provider=strategy.get('llm_provider', 'anthropic'),
            model=strategy.get('llm_model', ''),
            api_key=strategy.get('llm_api_key'),
            base_url=strategy.get('llm_base_url', ''),
            temperature=temperature,
            top_p=top_p,
            max_tokens=max_tokens,
        )
    
    async def update_batch_cache(self, symbol: str, data: Dict):
        """
        更新用户的 batch_cache（线程安全）
        
        与全局 batch_cache 不同，这是用户独立的缓存
        """
        async with self._batch_cache_lock:
            self.batch_cache[symbol] = data
    
    async def get_batch_cache(self, symbols: Optional[List[str]] = None) -> Dict[str, Dict]:
        """
        获取用户的 batch_cache
        
        Args:
            symbols: 如果指定，只返回这些币种的数据
        """
        async with self._batch_cache_lock:
            if symbols is None:
                return self.batch_cache.copy()
            return {s: self.batch_cache[s] for s in symbols if s in self.batch_cache}
    
    async def clear_batch_cache(self):
        """清空用户的 batch_cache"""
        async with self._batch_cache_lock:
            self.batch_cache.clear()
    
    async def copy_from_global_batch_cache(self, global_cache: Dict[str, Dict], symbols: List[str]):
        """
        从全局 batch_cache 复制数据到用户的独立缓存
        
        Args:
            global_cache: 全局 batch_cache
            symbols: 用户监控的币种列表
        """
        async with self._batch_cache_lock:
            for sym in symbols:
                if sym in global_cache:
                    # 深拷贝，确保用户数据完全隔离
                    self.batch_cache[sym] = global_cache[sym].copy()
    
    async def refresh_account_snapshot(self) -> Dict[str, Any]:
        """
        异步刷新账户快照
        """
        from core.async_helpers import run_sync
        from trading.account_positions import get_account_status_for_user
        
        if self._sync_ctx:
            snapshot = await run_sync(lambda: get_account_status_for_user(self._sync_ctx))
            if snapshot:
                self.account_snapshot = snapshot
        return self.account_snapshot
    
    async def get_monitor_symbols(self, exchange: str = None) -> List[str]:
        """
        异步获取用户监控的币种列表
        """
        if self._sync_ctx:
            # 使用同步上下文的方法
            return self._sync_ctx.get_monitor_symbols(exchange)
        
        # 回退：从配置获取
        return self.config.monitor_symbols or []
    
    async def get_system_prompt(self, strategy_id: str = None) -> str:
        """
        异步获取系统提示词
        
        v5.0: 从 user_strategy_configs 表加载用户策略
        """
        if self._sync_ctx:
            return self._sync_ctx.get_system_prompt(strategy_id)
        
        # 回退：使用最小化模板（无用户策略）
        from llm.prompt_templates import build_system_prompt
        return build_system_prompt(None)
    
    def touch(self):
        """更新最后活跃时间"""
        self.last_active_at = time.time()
    
    def is_stale(self, ttl_seconds: int = 3600) -> bool:
        """检查是否过期"""
        return time.time() - self.last_active_at > ttl_seconds
    
    def record_error(self, error: Exception):
        """记录错误"""
        self.error_count += 1
        self.last_error = str(error)
    
    def reset_errors(self):
        """重置错误计数"""
        self.error_count = 0
        self.last_error = None
    
    async def close(self):
        """关闭用户上下文，释放资源"""
        # 取消所有用户任务
        for task_name, task in self._user_tasks.items():
            if not task.done():
                task.cancel()
                try:
                    await asyncio.wait_for(task, timeout=5.0)
                except (asyncio.CancelledError, asyncio.TimeoutError):
                    pass
        self._user_tasks.clear()
        
        # 关闭 LLM 客户端（全局配置的）
        if self._llm_client:
            try:
                await self._llm_client.close()
            except Exception as e:
                # P3 Fix: 添加日志
                logger.debug(f"[{self.uid}] 关闭 LLM 客户端异常: {e}")
            self._llm_client = None
        
        # 注意：策略 LLM 客户端由全局 StrategyCache 管理，不在这里关闭
        # 只清空本地引用（如果有的话）
        self._strategy_llm_clients.clear()
        
        # 关闭多交易所交易器
        if self._multi_trader:
            try:
                await self._multi_trader.close()
            except Exception as e:
                # P3 Fix: 添加日志
                logger.debug(f"[{self.uid}] 关闭多交易所交易器异常: {e}")
            self._multi_trader = None
        
        # 清空缓存
        self.batch_cache.clear()
        
        self.is_running = False
        logger.debug(f"[{self.uid}] AsyncUserContext 已关闭")


class AsyncUserContextManager:
    """
    异步用户上下文管理器 - 单例模式
    
    功能：
    1. 按需创建/获取异步用户上下文
    2. 支持 TaskGroup 隔离执行
    3. LRU 淘汰长期不活跃的用户
    """
    
    _instance = None
    # P0 Fix: 延迟创建 asyncio.Lock，避免在模块加载时创建（此时可能无事件循环）
    _lock = None
    # P3 Fix: 用于保护 _lock 创建的线程锁
    _lock_creation_lock = threading.Lock()
    
    @classmethod
    def _get_lock(cls) -> asyncio.Lock:
        """
        获取类级别的 asyncio.Lock（延迟创建）
        
        P0 Fix: 在首次需要时创建锁，确保在事件循环中创建
        P3 Fix: 使用 threading.Lock 保护创建过程，避免竞态条件
        """
        if cls._lock is None:
            with cls._lock_creation_lock:
                if cls._lock is None:
                    cls._lock = asyncio.Lock()
        return cls._lock
    
    # 单例锁（线程安全）
    _singleton_lock = threading.Lock()
    
    def __new__(cls):
        if cls._instance is None:
            with cls._singleton_lock:  # 双重检查锁定
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if getattr(self, '_initialized', False):
            return
        
        self._contexts: Dict[str, AsyncUserContext] = {}
        # P0 Fix: 延迟创建实例级别的锁
        self._context_lock = None
        self._context_lock_init = threading.Lock()  # P2 Fix: 保护 _context_lock 创建
        self._config_loader = None
        self._max_contexts = 100
        self._context_ttl = 3600
        
        # 同步上下文管理器引用
        self._sync_manager = None
        
        self._initialized = True
        logger.info("AsyncUserContextManager 已初始化")
    
    def _get_context_lock(self) -> asyncio.Lock:
        """
        获取实例级别的 asyncio.Lock（延迟创建）
        
        P0 Fix: 在首次需要时创建锁，确保在事件循环中创建
        P2 Fix: 使用 threading.Lock 保护创建过程，避免竞态条件
        """
        if self._context_lock is None:
            with self._context_lock_init:
                if self._context_lock is None:
                    self._context_lock = asyncio.Lock()
        return self._context_lock
    
    def set_config_loader(self, loader):
        """设置配置加载器"""
        self._config_loader = loader
    
    def set_sync_manager(self, sync_manager):
        """设置同步上下文管理器引用"""
        self._sync_manager = sync_manager
    
    async def get_context(self, uid: str) -> Optional[AsyncUserContext]:
        """
        获取异步用户上下文（懒加载）
        """
        if uid in self._contexts:
            ctx = self._contexts[uid]
            ctx.touch()
            return ctx
        
        async with self._get_context_lock():
            # 双重检查
            if uid in self._contexts:
                ctx = self._contexts[uid]
                ctx.touch()
                return ctx
            
            # 加载配置
            config = await self._load_user_config(uid)
            if not config:
                logger.warning(f"用户 {uid} 配置不存在")
                return None
            
            # 检查容量
            if len(self._contexts) >= self._max_contexts:
                await self._evict_stale_contexts()
            
            # 获取同步上下文引用
            sync_ctx = None
            if self._sync_manager:
                sync_ctx = self._sync_manager.get_context(uid, auto_start=False, init_exchanges=False)
            
            # 创建异步上下文
            ctx = await AsyncUserContext.create(config, sync_ctx)
            self._contexts[uid] = ctx
            
            return ctx
    
    async def _load_user_config(self, uid: str) -> Optional[UserConfig]:
        """异步加载用户配置"""
        if self._config_loader is None:
            logger.error("配置加载器未设置")
            return None
        
        from core.async_helpers import run_sync
        return await run_sync(lambda: self._config_loader.load(uid))
    
    async def _evict_stale_contexts(self):
        """淘汰过期的上下文"""
        stale_uids = [
            uid for uid, ctx in self._contexts.items()
            if ctx.is_stale(self._context_ttl)
        ]
        
        for uid in stale_uids:
            await self.remove_context(uid)
            logger.info(f"[{uid}] 异步上下文已淘汰（不活跃）")
        
        # 如果仍然超限，按 LRU 淘汰
        if len(self._contexts) >= self._max_contexts:
            sorted_contexts = sorted(
                self._contexts.items(),
                key=lambda x: x[1].last_active_at
            )
            to_evict = len(self._contexts) - self._max_contexts + 10
            for uid, ctx in sorted_contexts[:to_evict]:
                await self.remove_context(uid)
                logger.info(f"[{uid}] 异步上下文已淘汰（超限）")
    
    async def remove_context(self, uid: str):
        """移除用户上下文"""
        if uid in self._contexts:
            await self._contexts[uid].close()
            del self._contexts[uid]
    
    def get_all_uids(self) -> List[str]:
        """获取所有用户 ID"""
        return list(self._contexts.keys())
    
    async def stop_all(self):
        """停止所有用户服务"""
        uids = list(self._contexts.keys())
        for uid in uids:
            await self.remove_context(uid)
        logger.info("所有异步用户服务已停止")
    
    def get_stats(self) -> Dict:
        """获取管理器统计信息"""
        return {
            "total_contexts": len(self._contexts),
            "max_contexts": self._max_contexts,
            "context_ttl": self._context_ttl,
        }


# 全局单例
async_context_manager = AsyncUserContextManager()


async def get_async_user_context(uid: str) -> Optional[AsyncUserContext]:
    """便捷函数：获取异步用户上下文"""
    return await async_context_manager.get_context(uid)


# ==================== 用户隔离执行器 ====================

class UserIsolatedExecutor:
    """
    用户隔离执行器
    
    使用 TaskGroup 确保每个用户的任务在隔离环境中执行，
    一个用户的错误不会影响其他用户。
    """
    
    def __init__(self, timeout: float = 300.0, max_concurrent: int = 50):
        """
        Args:
            timeout: 单用户执行超时（秒）
            max_concurrent: 最大并发用户数
        """
        self.timeout = timeout
        self.max_concurrent = max_concurrent
        self._semaphore = asyncio.Semaphore(max_concurrent)
    
    async def execute_for_users(
        self,
        uids: List[str],
        user_task: Callable[[AsyncUserContext], Awaitable[Any]],
        on_success: Optional[Callable[[str, Any], None]] = None,
        on_error: Optional[Callable[[str, Exception], None]] = None,
    ) -> Dict[str, Any]:
        """
        为多个用户执行任务（隔离执行）
        
        Args:
            uids: 用户 ID 列表
            user_task: 要执行的异步任务函数
            on_success: 成功回调 (uid, result)
            on_error: 错误回调 (uid, error)
        
        Returns:
            {uid: result or error}
        """
        results: Dict[str, Any] = {}
        results_lock = asyncio.Lock()
        
        async def run_user(uid: str):
            """单用户执行（带信号量限流）"""
            async with self._semaphore:
                ctx = await get_async_user_context(uid)
                if not ctx:
                    error = ValueError(f"无法获取用户上下文: {uid}")
                    async with results_lock:
                        results[uid] = {"error": str(error)}
                    if on_error:
                        on_error(uid, error)
                    return
                
                try:
                    # 执行任务（带超时）
                    result = await asyncio.wait_for(
                        user_task(ctx),
                        timeout=self.timeout
                    )
                    
                    async with results_lock:
                        results[uid] = {"success": True, "result": result}
                    
                    ctx.reset_errors()
                    if on_success:
                        on_success(uid, result)
                        
                except asyncio.TimeoutError as e:
                    error_msg = f"执行超时 ({self.timeout}s)"
                    logger.error(f"[{uid}] {error_msg}")
                    ctx.record_error(e)
                    async with results_lock:
                        results[uid] = {"error": error_msg}
                    if on_error:
                        on_error(uid, e)
                        
                except Exception as e:
                    logger.error(f"[{uid}] 执行异常: {e}")
                    ctx.record_error(e)
                    async with results_lock:
                        results[uid] = {"error": str(e)}
                    if on_error:
                        on_error(uid, e)
        
        # 使用 asyncio.gather 隔离执行（兼容 Python 3.10+）
        # return_exceptions=True 确保一个任务失败不会影响其他任务
        await asyncio.gather(*[run_user(uid) for uid in uids], return_exceptions=True)
        
        return results
    
    async def execute_for_user(
        self,
        uid: str,
        user_task: Callable[[AsyncUserContext], Awaitable[Any]],
    ) -> Any:
        """
        为单个用户执行任务
        
        Args:
            uid: 用户 ID
            user_task: 要执行的异步任务函数
        
        Returns:
            任务结果
        
        Raises:
            执行过程中的异常
        """
        ctx = await get_async_user_context(uid)
        if not ctx:
            raise ValueError(f"无法获取用户上下文: {uid}")
        
        try:
            result = await asyncio.wait_for(
                user_task(ctx),
                timeout=self.timeout
            )
            ctx.reset_errors()
            return result
        except Exception as e:
            ctx.record_error(e)
            raise


# 全局执行器实例
user_executor = UserIsolatedExecutor()
