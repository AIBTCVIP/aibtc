# websocket/async_base_stream.py
"""
纯 asyncio WebSocket 基类

提供通用的 WebSocket 功能：
- 自动重连（指数退避）
- 心跳保持
- 状态管理
- 认证失败处理

子类需要实现：
- _get_ws_url() - WebSocket URL
- _authenticate() - 认证逻辑
- _subscribe() - 订阅频道
- _handle_message() - 消息处理
"""

import asyncio
import json
import logging
import random
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Callable, Optional, Any, Dict

try:
    import websockets
    from websockets.exceptions import ConnectionClosed
except ImportError:
    websockets = None
    ConnectionClosed = Exception

logger = logging.getLogger(__name__)


class ConnectionState(Enum):
    """WebSocket 连接状态"""
    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    RECONNECTING = "reconnecting"
    AUTH_FAILED = "auth_failed"
    STOPPED = "stopped"


class AuthError(Exception):
    """认证错误（不应重试）"""
    pass


@dataclass
class WSConfig:
    """WebSocket 配置"""
    ping_interval: float = 25.0           # Ping 间隔（秒）
    ping_timeout: float = 20.0            # Ping 超时
    message_timeout: float = 3600.0       # 60分钟无消息则重连（与 listenKey 有效期一致）
    base_reconnect_delay: float = 1.0     # 基础重连延迟
    max_reconnect_delay: float = 60.0     # 最大重连延迟
    reconnect_multiplier: float = 2.0     # 退避倍数
    reconnect_jitter: float = 0.1         # 抖动比例
    max_reconnect_attempts: int = 0       # 0=无限重连


class AsyncWebSocketBase(ABC):
    """
    纯 asyncio WebSocket 基类
    
    使用方式：
    ```python
    ws = MyAsyncWebSocket(...)
    await ws.start()
    # ...
    await ws.stop()
    ```
    """
    
    def __init__(
        self,
        uid: str = "",
        exchange: str = "unknown",
        on_state_change: Optional[Callable[[ConnectionState, Optional[str]], None]] = None,
        config: Optional[WSConfig] = None,
    ):
        self.uid = uid
        self.exchange = exchange
        self.on_state_change = on_state_change
        self.config = config or WSConfig()
        
        self._ws = None
        self._running = False
        self._state = ConnectionState.DISCONNECTED
        self._reconnect_delay = self.config.base_reconnect_delay
        self._reconnect_count = 0
        self._last_message_time = 0
        
        # 任务
        self._main_task: Optional[asyncio.Task] = None
        self._ping_task: Optional[asyncio.Task] = None
    
    @property
    def state(self) -> ConnectionState:
        return self._state
    
    @property
    def last_message_time(self) -> int:
        return self._last_message_time
    
    @property
    def reconnect_count(self) -> int:
        return self._reconnect_count
    
    @property
    def is_connected(self) -> bool:
        return self._state == ConnectionState.CONNECTED
    
    def _set_state(self, state: ConnectionState, error: str = None):
        """设置状态并触发回调"""
        old_state = self._state
        self._state = state
        if old_state != state:
            logger.info(f"[{self.uid}][{self.exchange}] State: {old_state.value} -> {state.value}")
            if self.on_state_change:
                try:
                    self.on_state_change(state, error)
                except Exception as e:
                    logger.error(f"[{self.uid}][{self.exchange}] State callback error: {e}")
    
    def _get_reconnect_delay(self) -> float:
        """计算重连延迟（带抖动）"""
        delay = self._reconnect_delay
        jitter = delay * self.config.reconnect_jitter * (random.random() * 2 - 1)
        return delay + jitter
    
    def _increase_reconnect_delay(self):
        """增加重连延迟"""
        self._reconnect_delay = min(
            self._reconnect_delay * self.config.reconnect_multiplier,
            self.config.max_reconnect_delay
        )
        self._reconnect_count += 1
    
    def _reset_reconnect_delay(self):
        """重置重连延迟"""
        self._reconnect_delay = self.config.base_reconnect_delay
        self._reconnect_count = 0
    
    @abstractmethod
    def _get_ws_url(self) -> str:
        """获取 WebSocket URL"""
        pass
    
    @abstractmethod
    async def _authenticate(self, ws) -> bool:
        """
        认证逻辑
        
        Returns:
            True: 认证成功
            False: 认证失败但可重试
        
        Raises:
            AuthError: 认证失败且不应重试（如 API Key 无效）
        """
        pass
    
    @abstractmethod
    async def _subscribe(self, ws) -> bool:
        """订阅频道"""
        pass
    
    @abstractmethod
    async def _handle_message(self, message: Dict[str, Any]):
        """处理收到的消息"""
        pass
    
    async def _ping_loop(self):
        """心跳循环"""
        while self._running and self._ws:
            try:
                await asyncio.sleep(self.config.ping_interval)
                if self._ws and not self._ws.closed:
                    await self._ws.ping()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.debug(f"[{self.uid}][{self.exchange}] Ping error: {e}")
    
    async def _message_loop(self, ws):
        """消息接收循环"""
        async for message in ws:
            if not self._running:
                break
            
            self._last_message_time = int(time.time() * 1000)
            
            try:
                if isinstance(message, bytes):
                    message = message.decode('utf-8')
                
                data = json.loads(message)
                await self._handle_message(data)
            except json.JSONDecodeError:
                logger.warning(f"[{self.uid}][{self.exchange}] Invalid JSON: {message[:100]}")
            except Exception as e:
                logger.error(f"[{self.uid}][{self.exchange}] Message handling error: {e}")
    
    async def _connect_once(self) -> bool:
        """
        单次连接尝试
        
        Returns:
            True: 连接成功并保持
            False: 连接失败，需要重试
        
        Raises:
            AuthError: 认证失败，不应重试
        """
        ws_url = self._get_ws_url()
        logger.info(f"[{self.uid}][{self.exchange}] Connecting to {ws_url}")
        
        self._set_state(ConnectionState.CONNECTING)
        
        try:
            async with websockets.connect(
                ws_url,
                open_timeout=30,  # 增加握手超时时间
                ping_interval=self.config.ping_interval,
                ping_timeout=self.config.ping_timeout,
            ) as ws:
                self._ws = ws
                
                # 认证
                try:
                    if not await self._authenticate(ws):
                        logger.error(f"[{self.uid}][{self.exchange}] Auth failed")
                        return False
                except AuthError as e:
                    logger.error(f"[{self.uid}][{self.exchange}] Auth error (no retry): {e}")
                    self._set_state(ConnectionState.AUTH_FAILED, str(e))
                    raise
                
                # 订阅
                if not await self._subscribe(ws):
                    logger.error(f"[{self.uid}][{self.exchange}] Subscribe failed")
                    return False
                
                # 连接成功
                self._set_state(ConnectionState.CONNECTED)
                self._reset_reconnect_delay()
                logger.info(f"[{self.uid}][{self.exchange}] Connected")
                
                # 启动心跳
                self._ping_task = asyncio.create_task(self._ping_loop())
                
                try:
                    # 消息循环
                    await self._message_loop(ws)
                finally:
                    if self._ping_task:
                        self._ping_task.cancel()
                        try:
                            await self._ping_task
                        except asyncio.CancelledError:
                            pass
                
                return True
                
        except AuthError:
            raise
        except ConnectionClosed as e:
            logger.warning(f"[{self.uid}][{self.exchange}] Connection closed: {e}")
            return False
        except Exception as e:
            logger.error(f"[{self.uid}][{self.exchange}] Connection error: {e}")
            return False
        finally:
            self._ws = None
    
    async def _run_with_reconnect(self):
        """主循环（带自动重连）"""
        while self._running:
            try:
                success = await self._connect_once()
                if not self._running:
                    break
                    
            except AuthError:
                # 认证失败，停止重连
                self._running = False
                break
                
            except asyncio.CancelledError:
                break
            
            # 重连
            if self._running:
                delay = self._get_reconnect_delay()
                self._increase_reconnect_delay()
                self._set_state(ConnectionState.RECONNECTING)
                logger.info(f"[{self.uid}][{self.exchange}] Reconnecting in {delay:.1f}s (attempt {self._reconnect_count})")
                
                try:
                    await asyncio.sleep(delay)
                except asyncio.CancelledError:
                    break
        
        if self._state != ConnectionState.AUTH_FAILED:
            self._set_state(ConnectionState.STOPPED)
    
    async def start(self):
        """启动 WebSocket"""
        if self._running:
            logger.warning(f"[{self.uid}][{self.exchange}] Already running")
            return
        
        self._running = True
        self._main_task = asyncio.create_task(self._run_with_reconnect())
        logger.info(f"[{self.uid}][{self.exchange}] Started")
    
    async def stop(self):
        """停止 WebSocket"""
        self._running = False
        
        if self._ws and not self._ws.closed:
            try:
                await self._ws.close()
            except Exception:
                pass
        
        if self._main_task:
            self._main_task.cancel()
            try:
                await self._main_task
            except asyncio.CancelledError:
                pass
        
        self._set_state(ConnectionState.STOPPED)
        logger.info(f"[{self.uid}][{self.exchange}] Stopped")
