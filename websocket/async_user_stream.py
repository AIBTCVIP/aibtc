# websocket/async_user_stream.py
"""
纯 asyncio 版本的 Binance User Data Stream WebSocket

特点：
1. 不使用 threading，完全基于 asyncio
2. 作为 asyncio.Task 运行在主事件循环中
3. 自动重连和心跳（listenKey 续期）
4. 认证失败时不重试，通知上层处理

使用方式：
```python
ws = AsyncFuturesUserWS(
    on_message=handle_message,
    api_key="your_api_key",
    uid="user_123"
)
await ws.start()

# 稍后停止
await ws.stop()
```
"""

import asyncio
import json
import logging
import time
from typing import Callable, Optional, Dict, Any, Union
from dataclasses import dataclass
from enum import Enum

try:
    import aiohttp
except ImportError:
    aiohttp = None

try:
    import websockets
    from websockets.exceptions import ConnectionClosed
except ImportError:
    websockets = None

logger = logging.getLogger(__name__)


class ConnectionState(Enum):
    """连接状态"""
    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    RECONNECTING = "reconnecting"
    STOPPED = "stopped"


class AuthError(Exception):
    """认证错误（不应重试）"""
    pass


# Binance 认证相关错误码
BINANCE_AUTH_ERROR_CODES = {
    -2015,  # Invalid API-key, IP, or permissions for action
    -2014,  # API-key format invalid
    -1022,  # Signature for this request is not valid
    -1002,  # You are not authorized to execute this request
    -1021,  # Timestamp for this request is outside of the recvWindow
}


@dataclass
class WSConfig:
    """WebSocket 配置"""
    keepalive_interval: float = 25 * 60  # 25分钟续期（留5分钟缓冲）
    message_timeout: float = 60 * 60     # 60分钟无消息则重连（与 listenKey 有效期一致）
    max_reconnect_attempts: int = 0      # 0=无限重连
    base_reconnect_delay: float = 1.0
    max_reconnect_delay: float = 60.0
    ping_interval: float = 20.0
    ping_timeout: float = 20.0


class AsyncFuturesUserWS:
    """
    纯 asyncio 版本的 Binance Futures User Data Stream WebSocket
    
    生命周期:
    1. 创建实例
    2. await ws.start() - 启动连接
    3. 自动处理重连和心跳
    4. await ws.stop() - 停止连接
    """
    
    def __init__(
        self,
        on_message: Callable[[dict], Any],
        *,
        api_key: str,
        is_testnet: bool = False,
        uid: str = "default",
        on_connect: Optional[Callable[[], Any]] = None,
        on_disconnect: Optional[Callable[[str], Any]] = None,
        on_auth_failed: Optional[Callable[[str], Any]] = None,
        config: Optional[WSConfig] = None,
    ):
        """
        初始化 WebSocket 客户端
        
        Args:
            on_message: 消息回调（必需）
            api_key: Binance API Key（必需）
            is_testnet: 是否使用测试网
            uid: 用户标识（用于日志）
            on_connect: 连接成功回调
            on_disconnect: 断开连接回调（参数为断开原因）
            on_auth_failed: 认证失败回调（参数为错误信息）
            config: WebSocket 配置
        """
        if not api_key:
            raise ValueError("api_key is required")
        
        self.on_message = on_message
        self.on_connect = on_connect
        self.on_disconnect = on_disconnect
        self.on_auth_failed = on_auth_failed
        
        self._api_key = api_key
        self._is_testnet = is_testnet
        self._uid = uid
        self._config = config or WSConfig()
        
        # 状态
        self._state = ConnectionState.DISCONNECTED
        self._running = False
        self._task: Optional[asyncio.Task] = None
        self._ws = None
        self._http_session: Optional[aiohttp.ClientSession] = None
        
        # 统计
        self._reconnect_count = 0
        self._last_message_time: float = 0
        self._connect_time: float = 0
        
        self._log_prefix = f"[WS:{uid}]"
    
    @property
    def state(self) -> ConnectionState:
        """当前连接状态"""
        return self._state
    
    @property
    def is_connected(self) -> bool:
        """是否已连接"""
        return self._state == ConnectionState.CONNECTED
    
    @property
    def stats(self) -> Dict[str, Any]:
        """连接统计"""
        return {
            "state": self._state.value,
            "reconnect_count": self._reconnect_count,
            "last_message_time": self._last_message_time,
            "connect_time": self._connect_time,
            "uptime": time.time() - self._connect_time if self._connect_time else 0,
        }
    
    # =========================================================================
    # 公共方法
    # =========================================================================
    
    async def start(self) -> None:
        """启动 WebSocket 连接"""
        if self._running:
            logger.warning(f"{self._log_prefix} 已在运行中")
            return
        
        self._running = True
        self._reconnect_count = 0
        self._state = ConnectionState.CONNECTING
        
        # 创建 HTTP Session
        if aiohttp:
            self._http_session = aiohttp.ClientSession()
        
        # 创建主任务
        self._task = asyncio.create_task(
            self._main_loop(),
            name=f"ws-{self._uid}"
        )
        
        logger.info(f"{self._log_prefix} 已启动")
    
    async def stop(self) -> None:
        """停止 WebSocket 连接"""
        if not self._running:
            return
        
        self._running = False
        self._state = ConnectionState.STOPPED
        
        # 关闭 WebSocket
        if self._ws:
            try:
                await self._ws.close()
            except Exception as e:
                # P3 Fix: 添加日志
                logger.debug(f"{self._log_prefix} 关闭 WebSocket 异常: {e}")
            self._ws = None
        
        # 取消任务
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        
        # 关闭 HTTP Session
        if self._http_session:
            await self._http_session.close()
            self._http_session = None
        
        logger.info(f"{self._log_prefix} 已停止")
    
    # =========================================================================
    # 内部方法
    # =========================================================================
    
    async def _main_loop(self) -> None:
        """主循环：连接、接收、重连"""
        backoff = self._config.base_reconnect_delay
        
        while self._running:
            listen_key = None
            disconnect_reason = "unknown"
            
            try:
                # 1. 创建 listenKey
                self._state = ConnectionState.CONNECTING
                listen_key = await self._create_listen_key()
                logger.info(f"{self._log_prefix} ListenKey 创建成功")
                
                # 2. 连接 WebSocket
                url = self._get_ws_url(listen_key)
                
                if websockets:
                    async with websockets.connect(
                        url,
                        open_timeout=30,  # 增加握手超时时间
                        ping_interval=self._config.ping_interval,
                        ping_timeout=self._config.ping_timeout,
                        close_timeout=5,
                        max_size=10 * 1024 * 1024,
                    ) as ws:
                        self._ws = ws
                        self._state = ConnectionState.CONNECTED
                        self._connect_time = time.time()
                        self._reconnect_count = 0
                        backoff = self._config.base_reconnect_delay
                        
                        logger.info(f"{self._log_prefix} 连接成功")
                        
                        # 触发连接回调
                        await self._safe_callback(self.on_connect)
                        
                        # 3. 启动 keepalive
                        keepalive_task = asyncio.create_task(
                            self._keepalive_loop(listen_key),
                            name=f"ws-keepalive-{self._uid}"
                        )
                        
                        try:
                            # 4. 接收消息
                            disconnect_reason = await self._receive_loop(ws)
                        finally:
                            keepalive_task.cancel()
                            try:
                                await keepalive_task
                            except asyncio.CancelledError:
                                pass
                else:
                    raise ImportError("websockets library is required")
            
            except asyncio.CancelledError:
                disconnect_reason = "cancelled"
                break
            
            except AuthError as e:
                disconnect_reason = f"auth_failed: {e}"
                logger.error(f"{self._log_prefix} 认证失败（不重试）: {e}")
                await self._safe_callback(self.on_auth_failed, str(e))
                break  # 认证失败不重试
            
            except Exception as e:
                disconnect_reason = f"error: {type(e).__name__}: {e}"
                logger.error(f"{self._log_prefix} 错误: {e}")
                self._reconnect_count += 1
            
            finally:
                was_connected = self._state == ConnectionState.CONNECTED
                self._state = ConnectionState.DISCONNECTED
                self._ws = None
                self._connect_time = 0
                
                # 触发断开回调
                if was_connected:
                    await self._safe_callback(self.on_disconnect, disconnect_reason)
                
                # 关闭 listenKey
                if listen_key:
                    try:
                        await self._close_listen_key(listen_key)
                    except Exception as e:
                        # P3 Fix: 添加日志
                        logger.debug(f"{self._log_prefix} 关闭 listenKey 异常: {e}")
            
            # 检查是否继续
            if not self._running:
                break
            
            # 检查重连次数
            if self._config.max_reconnect_attempts > 0:
                if self._reconnect_count >= self._config.max_reconnect_attempts:
                    logger.warning(f"{self._log_prefix} 达到最大重连次数")
                    break
            
            # 重连等待（指数退避）
            self._state = ConnectionState.RECONNECTING
            logger.info(f"{self._log_prefix} {backoff:.1f}s 后重连 (第 {self._reconnect_count} 次)")
            
            try:
                await asyncio.sleep(backoff)
            except asyncio.CancelledError:
                break
            
            backoff = min(self._config.max_reconnect_delay, backoff * 1.5)
        
        self._state = ConnectionState.STOPPED
        
        # 清理 HTTP Session（如果 _main_loop 自然退出而非通过 stop() 退出）
        if self._http_session and not self._http_session.closed:
            try:
                await self._http_session.close()
            except Exception as e:
                # P3 Fix: 添加日志
                logger.debug(f"{self._log_prefix} 关闭 HTTP Session 异常: {e}")
            self._http_session = None
        
        logger.info(f"{self._log_prefix} 主循环结束")
    
    async def _receive_loop(self, ws) -> str:
        """消息接收循环"""
        self._last_message_time = time.time()
        
        while self._running:
            try:
                # 使用超时接收
                msg = await asyncio.wait_for(
                    ws.recv(),
                    timeout=self._config.message_timeout
                )
                
                self._last_message_time = time.time()
                
                if not msg:
                    continue
                
                # 解析 JSON
                try:
                    data = json.loads(msg)
                except json.JSONDecodeError:
                    logger.warning(f"{self._log_prefix} JSON 解析失败: {msg[:100]}")
                    continue
                
                # 忽略非事件消息
                if not isinstance(data, dict) or "e" not in data:
                    continue
                
                # 调用消息回调
                await self._safe_callback(self.on_message, data)
                
            except asyncio.TimeoutError:
                elapsed = time.time() - self._last_message_time
                logger.warning(f"{self._log_prefix} {elapsed:.0f}s 无消息，重连")
                return "message_timeout"
            
            except ConnectionClosed as e:
                logger.warning(f"{self._log_prefix} 连接关闭: {e.code} {e.reason}")
                return f"connection_closed: {e.code}"
            
            except asyncio.CancelledError:
                return "cancelled"
            
            except Exception as e:
                logger.error(f"{self._log_prefix} 接收错误: {e}")
                return f"receive_error: {type(e).__name__}"
        
        return "stopped"
    
    async def _keepalive_loop(self, listen_key: str) -> None:
        """listenKey 续期循环"""
        while self._running:
            try:
                await asyncio.sleep(self._config.keepalive_interval)
                
                if not self._running:
                    break
                
                await self._keepalive_listen_key(listen_key)
                logger.debug(f"{self._log_prefix} Keepalive 成功")
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"{self._log_prefix} Keepalive 失败: {e}")
                # 不立即断开，让主循环处理
    
    async def _safe_callback(
        self,
        callback: Optional[Callable],
        *args
    ) -> None:
        """安全执行回调"""
        if not callback:
            return
        
        try:
            result = callback(*args)
            if asyncio.iscoroutine(result):
                await result
        except Exception as e:
            logger.error(f"{self._log_prefix} 回调错误: {e}")
    
    # =========================================================================
    # API 方法
    # =========================================================================
    
    def _get_api_url(self) -> str:
        """获取 API URL"""
        if self._is_testnet:
            return "https://testnet.binancefuture.com"
        return "https://fapi.binance.com"
    
    def _get_ws_url(self, listen_key: str) -> str:
        """获取 WebSocket URL"""
        if self._is_testnet:
            return f"wss://stream.binancefuture.com/ws/{listen_key}"
        return f"wss://fstream.binance.com/ws/{listen_key}"
    
    async def _create_listen_key(self) -> str:
        """创建 listenKey"""
        url = f"{self._get_api_url()}/fapi/v1/listenKey"
        headers = {"X-MBX-APIKEY": self._api_key}
        
        if self._http_session:
            async with self._http_session.post(url, headers=headers, timeout=10) as resp:
                if resp.status in (401, 403) or resp.status >= 400:
                    try:
                        data = await resp.json()
                        error_code = data.get("code")
                        error_msg = data.get("msg", "Unknown error")
                        if error_code in BINANCE_AUTH_ERROR_CODES:
                            raise AuthError(f"认证失败 (code {error_code}): {error_msg}")
                    except (ValueError, KeyError) as e:
                        logger.debug(f"[{self._uid}] 解析 Binance 认证错误响应失败: {e}")
                    raise AuthError(f"HTTP {resp.status}")
                
                data = await resp.json()
                return data["listenKey"]
        else:
            # 回退到 requests（同步）
            import requests
            resp = requests.post(url, headers=headers, timeout=10)
            if resp.status_code in (401, 403) or resp.status_code >= 400:
                try:
                    data = resp.json()
                    error_code = data.get("code")
                    error_msg = data.get("msg", "Unknown error")
                    if error_code in BINANCE_AUTH_ERROR_CODES:
                        raise AuthError(f"认证失败 (code {error_code}): {error_msg}")
                except (ValueError, KeyError) as e:
                    logger.debug(f"[{self._uid}] 解析 Binance 认证错误响应失败: {e}")
                raise AuthError(f"HTTP {resp.status_code}")
            
            data = resp.json()
            return data["listenKey"]
    
    async def _keepalive_listen_key(self, listen_key: str) -> None:
        """续期 listenKey"""
        url = f"{self._get_api_url()}/fapi/v1/listenKey"
        headers = {"X-MBX-APIKEY": self._api_key}
        params = {"listenKey": listen_key}
        
        if self._http_session:
            async with self._http_session.put(
                url, headers=headers, params=params, timeout=10
            ) as resp:
                resp.raise_for_status()
        else:
            import requests
            resp = requests.put(url, headers=headers, params=params, timeout=10)
            resp.raise_for_status()
    
    async def _close_listen_key(self, listen_key: str) -> None:
        """关闭 listenKey"""
        url = f"{self._get_api_url()}/fapi/v1/listenKey"
        headers = {"X-MBX-APIKEY": self._api_key}
        params = {"listenKey": listen_key}
        
        try:
            if self._http_session and not self._http_session.closed:
                async with self._http_session.delete(
                    url, headers=headers, params=params, timeout=5
                ) as resp:
                    pass  # 忽略结果
            else:
                import requests
                requests.delete(url, headers=headers, params=params, timeout=5)
        except Exception as e:
            # P3 Fix: 添加日志
            logger.debug(f"{self._log_prefix} 关闭 listenKey 请求异常: {e}")
    
    def __del__(self):
        """析构函数 - 清理未关闭的 session"""
        if self._http_session and not self._http_session.closed:
            # 在析构时无法 await，直接置空让 GC 清理
            # 这会产生警告，但比泄漏好
            self._http_session = None


# =============================================================================
# 工厂函数
# =============================================================================

async def create_user_ws_async(
    uid: str,
    api_key: str,
    on_message: Callable[[dict], Any],
    is_testnet: bool = False,
    **kwargs
) -> AsyncFuturesUserWS:
    """
    创建并启动用户 WebSocket
    
    Args:
        uid: 用户 ID
        api_key: API Key
        on_message: 消息回调
        is_testnet: 是否测试网
        **kwargs: 其他参数传递给 AsyncFuturesUserWS
    
    Returns:
        已启动的 AsyncFuturesUserWS 实例
    """
    ws = AsyncFuturesUserWS(
        on_message=on_message,
        api_key=api_key,
        is_testnet=is_testnet,
        uid=uid,
        **kwargs
    )
    await ws.start()
    return ws
