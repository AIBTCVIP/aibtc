# user_stream.py - 用户数据 WebSocket 流
import asyncio
import json
import logging
import random
import threading
import time
from typing import Callable, Optional
import requests
import websockets
from core.config import BINANCE_ENVIRONMENT

logger = logging.getLogger(__name__)


class BinanceAuthError(Exception):
    """Binance 认证失败异常（无效 API Key、权限不足等）"""
    pass


class BinanceIPBanError(Exception):
    """Binance IP 封禁异常（HTTP 418）"""
    pass


# Binance 认证相关错误码（不应重试）
BINANCE_AUTH_ERROR_CODES = {
    -2015,  # Invalid API-key, IP, or permissions for action
    -2014,  # API-key format invalid
    -1022,  # Signature for this request is not valid
    -1002,  # You are not authorized to execute this request
    -1021,  # Timestamp for this request is outside of the recvWindow
}

# HTTP 418 IP 封禁等待时间（秒）
IP_BAN_WAIT_SECONDS = 120  # 2 分钟

# listenKey 操作限速（使用 API Key 级别限速器）
def _acquire_binance_rate_limit(api_key: str = None, endpoint: str = None, timeout: float = 30.0) -> bool:
    """获取 Binance API 限速许可"""
    try:
        from core.rate_limiter import get_binance_rate_limiter
        limiter = get_binance_rate_limiter(api_key)
        return limiter.acquire(endpoint=endpoint, timeout=timeout)
    except Exception:
        return True  # 如果限速器不可用，允许通过


def _fapi_base_url(is_testnet: bool = None) -> str:
    """获取 Futures API 基础 URL"""
    testnet = is_testnet if is_testnet is not None else BINANCE_ENVIRONMENT
    return "https://testnet.binancefuture.com" if testnet else "https://fapi.binance.com"


def _ws_base_url(is_testnet: bool = None) -> str:
    """获取 WebSocket 基础 URL"""
    testnet = is_testnet if is_testnet is not None else BINANCE_ENVIRONMENT
    return "wss://stream.binancefuture.com/ws" if testnet else "wss://fstream.binance.com/ws"


def _create_listen_key_sync(api_key: str, is_testnet: bool = None, timeout: int = 10) -> str:
    """
    同步创建 listenKey

    Args:
        api_key: Binance API Key（必需参数）
        is_testnet: 是否使用测试网
        timeout: 超时秒数
    
    Raises:
        BinanceAuthError: 认证失败（无效 API Key 等）
        BinanceIPBanError: IP 被封禁（HTTP 418）
        requests.exceptions.RequestException: 网络或其他 API 错误
    """
    # 获取限速许可
    _acquire_binance_rate_limit(api_key=api_key, endpoint="futures_stream_get_listen_key", timeout=30.0)
    
    key = api_key
    url = f"{_fapi_base_url(is_testnet)}/fapi/v1/listenKey"
    headers = {"X-MBX-APIKEY": key}
    r = requests.post(url, headers=headers, timeout=timeout)
    
    # 检查 IP 封禁（HTTP 418）
    if r.status_code == 418:
        raise BinanceIPBanError(f"IP banned by Binance (HTTP 418). Wait {IP_BAN_WAIT_SECONDS}s before retry.")
    
    # 检查认证错误
    if r.status_code in (401, 403) or r.status_code >= 400:
        try:
            data = r.json()
            error_code = data.get("code")
            error_msg = data.get("msg", "Unknown error")
            if error_code in BINANCE_AUTH_ERROR_CODES:
                raise BinanceAuthError(f"Binance auth failed (code {error_code}): {error_msg}")
        except (ValueError, KeyError) as e:
            logger.debug(f"解析 Binance 认证错误响应失败: {e}")
    
    r.raise_for_status()
    data = r.json() or {}
    lk = data.get("listenKey")
    if not lk:
        raise RuntimeError(f"Failed to get futures listenKey: {data}")
    return lk


def _keepalive_listen_key_sync(listen_key: str, api_key: str, is_testnet: bool = None, timeout: int = 10) -> None:
    """
    同步续期 listenKey
    
    Args:
        listen_key: 要续期的 listenKey
        api_key: Binance API Key（必需参数）
        is_testnet: 是否使用测试网
        timeout: 超时秒数
    """
    # 获取限速许可
    _acquire_binance_rate_limit(api_key=api_key, endpoint="futures_stream_keepalive", timeout=30.0)
    
    if not api_key:
        raise ValueError("api_key is required for keepalive_listen_key_sync")
    key = api_key
    url = f"{_fapi_base_url(is_testnet)}/fapi/v1/listenKey"
    headers = {"X-MBX-APIKEY": key}
    params = {"listenKey": listen_key}
    r = requests.put(url, headers=headers, params=params, timeout=timeout)
    r.raise_for_status()


def _close_listen_key_sync(listen_key: str, api_key: str, is_testnet: bool = None, timeout: int = 10) -> None:
    """
    同步关闭 listenKey

    Args:
        listen_key: 要关闭的 listenKey
        api_key: Binance API Key（必需参数）
        is_testnet: 是否使用测试网
        timeout: 超时秒数
    """
    # 获取限速许可
    _acquire_binance_rate_limit(api_key=api_key, endpoint="futures_stream_close", timeout=30.0)
    
    key = api_key
    url = f"{_fapi_base_url(is_testnet)}/fapi/v1/listenKey"
    headers = {"X-MBX-APIKEY": key}
    params = {"listenKey": listen_key}
    r = requests.delete(url, headers=headers, params=params, timeout=timeout)
    try:
        r.raise_for_status()
    except Exception as e:
        # P3 Fix: 添加日志
        logger.debug(f"关闭 listenKey 请求异常: {e}")


class FuturesUserWS:
    """
    Binance Futures User Data Stream WebSocket 客户端

    功能:
    - 自动创建和管理 listenKey
    - 自动续期(每 25 分钟)
    - 断线自动重连(指数退避)
    - 心跳检测和超时保护
    - 支持多用户（自定义 API Key）
    """

    def __init__(
            self,
            on_message: Callable[[dict], None],
            *,
            api_key: Optional[str] = None,  # 自定义 API Key（多用户支持）
            is_testnet: Optional[bool] = None,  # 是否使用测试网
            uid: Optional[str] = None,  # 用户标识（日志用）
            on_connect: Optional[Callable[[], None]] = None,
            on_disconnect: Optional[Callable[[str], None]] = None,  # 参数为断开原因
            on_auth_failed: Optional[Callable[[str], None]] = None,  # 认证失败回调
            keepalive_interval_s: int = 30 * 60,  # 30分钟续期一次(留30分钟缓冲，减少API调用)
            base_reconnect_sleep_s: float = 5.0,  # 基础重连等待 5 秒（防止过快重连）
            max_reconnect_sleep_s: float = 120.0,  # 最大重连等待 2 分钟
            message_timeout_s: float = 60 * 60,  # 60分钟无消息则认为连接异常（与 listenKey 有效期一致）
            max_reconnect_attempts: int = 0,  # 0表示无限重连
    ):
        """
        初始化 Futures User WebSocket 客户端

        Args:
            on_message: 收到消息时的回调函数
            api_key: Binance API Key（为 None 时使用全局配置）
            is_testnet: 是否使用测试网（为 None 时使用全局配置）
            uid: 用户标识（用于日志）
            on_connect: 连接成功时的回调函数
            on_disconnect: 断开连接时的回调函数(参数为断开原因)
            on_auth_failed: 认证失败时的回调函数(参数为错误信息)
            keepalive_interval_s: listenKey 续期间隔(秒)
            base_reconnect_sleep_s: 重连基础等待时间(秒)
            max_reconnect_sleep_s: 重连最大等待时间(秒)
            message_timeout_s: 消息超时时间(秒),超过此时间未收到消息则重连
            max_reconnect_attempts: 最大重连次数,0表示无限重连
        """
        self.on_message = on_message
        self.on_connect = on_connect
        self.on_disconnect = on_disconnect
        self.on_auth_failed = on_auth_failed
        self.keepalive_interval_s = keepalive_interval_s
        self.base_reconnect_sleep_s = base_reconnect_sleep_s
        self.max_reconnect_sleep_s = max_reconnect_sleep_s
        self.message_timeout_s = message_timeout_s
        self.max_reconnect_attempts = max_reconnect_attempts
        
        # 多用户支持
        self._api_key = api_key
        self._is_testnet = is_testnet
        self._uid = uid or "default"
        self._log_prefix = f"[WS:{self._uid}]" if uid else "[WS]"

        self._thread: Optional[threading.Thread] = None
        self._stop_flag = threading.Event()
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._is_connected = False
        self._reconnect_count = 0

    def start(self):
        """启动 WebSocket 连接(非阻塞)"""
        if not self._api_key:
            logger.error(f"{self._log_prefix} api_key is required, cannot start")
            return
        if self._thread and self._thread.is_alive():
            logger.warning(f"{self._log_prefix} Already running")
            return
        self._stop_flag.clear()
        self._reconnect_count = 0
        self._thread = threading.Thread(
            target=self._run, 
            name=f"futures-user-ws-{self._uid}", 
            daemon=True
        )
        self._thread.start()

    def stop(self, timeout: float = 5.0):
        """
        停止 WebSocket 连接

        Args:
            timeout: 等待线程结束的超时时间(秒)
        """
        self._stop_flag.set()
        if self._loop and self._loop.is_running():
            self._loop.call_soon_threadsafe(lambda: None)
        if self._thread:
            self._thread.join(timeout=timeout)

    def is_connected(self) -> bool:
        """返回当前连接状态"""
        return self._is_connected

    def _run(self):
        """线程入口函数"""
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        try:
            self._loop.run_until_complete(self._main())
        finally:
            # 安全关闭事件循环：先取消所有 pending 任务
            try:
                # 获取所有 pending 任务
                pending = asyncio.all_tasks(self._loop)
                for task in pending:
                    task.cancel()
                # 等待所有任务取消完成
                if pending:
                    self._loop.run_until_complete(
                        asyncio.gather(*pending, return_exceptions=True)
                    )
            except Exception as e:
                # P3 Fix: 添加日志
                logger.debug(f"{self._log_prefix} 停止事件循环异常: {e}")
            finally:
                self._loop.close()

    async def _main(self):
        """主循环:创建连接、处理重连"""
        sleep_s = self.base_reconnect_sleep_s

        while not self._stop_flag.is_set():
            # 检查是否超过最大重连次数
            if self.max_reconnect_attempts > 0 and self._reconnect_count >= self.max_reconnect_attempts:
                logger.warning(f"{self._log_prefix} Max reconnect attempts ({self.max_reconnect_attempts}) reached, stopping")
                break

            listen_key = None
            keepalive_task = None
            keepalive_failed = asyncio.Event()
            disconnect_reason = "unknown"

            try:
                # 1) 创建 listenKey
                logger.info(f"{self._log_prefix} Creating listenKey...")
                listen_key = await asyncio.to_thread(
                    _create_listen_key_sync, 
                    self._api_key, 
                    self._is_testnet, 
                    10
                )
                # P3 Fix: 不在日志中打印 ListenKey
                logger.info(f"{self._log_prefix} ListenKey created successfully")

                # 2) 启动 keepalive 定时任务
                keepalive_task = asyncio.create_task(
                    self._keepalive_loop(listen_key, keepalive_failed)
                )

                # 3) 连接 WebSocket
                url = f"{_ws_base_url(self._is_testnet)}/{listen_key}"
                async with websockets.connect(
                        url,
                        open_timeout=30,  # 增加握手超时时间
                        ping_interval=20,
                        ping_timeout=20,
                        close_timeout=5,
                        max_queue=1024,
                        max_size=10 * 1024 * 1024,
                ) as ws:
                    # 连接成功,重置重连计数和退避时间
                    self._reconnect_count = 0
                    sleep_s = self.base_reconnect_sleep_s
                    self._is_connected = True

                    logger.info(f"{self._log_prefix} Connected successfully")

                    # 触发连接回调
                    if self.on_connect:
                        try:
                            self.on_connect()
                        except Exception as e:
                            logger.error(f"{self._log_prefix} on_connect error: {repr(e)}")

                    # 4) 消息接收循环
                    disconnect_reason = await self._receive_loop(ws, keepalive_failed)

            except asyncio.CancelledError:
                disconnect_reason = "cancelled"
                break
            
            except BinanceAuthError as e:
                # 认证失败，不重试
                disconnect_reason = f"auth_failed: {str(e)}"
                logger.error(f"{self._log_prefix} Authentication failed (will not retry): {repr(e)}")
                # 触发认证失败回调
                if self.on_auth_failed:
                    try:
                        self.on_auth_failed(str(e))
                    except Exception as cb_err:
                        logger.error(f"{self._log_prefix} on_auth_failed callback error: {repr(cb_err)}")
                break  # 不重试，直接退出
            
            except BinanceIPBanError as e:
                # IP 被封禁，等待较长时间后重试
                disconnect_reason = f"ip_banned: HTTP 418"
                logger.error(f"{self._log_prefix} IP banned by Binance! Waiting {IP_BAN_WAIT_SECONDS}s before retry...")
                self._reconnect_count += 1
                # 强制等待 IP 封禁时间，不使用普通的退避策略
                sleep_s = IP_BAN_WAIT_SECONDS

            except requests.exceptions.RequestException as e:
                # 网络错误或 API 错误（包括 429 限流）
                disconnect_reason = f"api_error: {type(e).__name__}"
                logger.error(f"{self._log_prefix} API error: {repr(e)}")
                self._reconnect_count += 1
                # 如果是 429 限流，也等待较长时间
                if "429" in str(e):
                    sleep_s = max(sleep_s, 60.0)
                    logger.warning(f"{self._log_prefix} Rate limited (429), waiting {sleep_s}s")

            except websockets.exceptions.WebSocketException as e:
                # WebSocket 连接错误
                disconnect_reason = f"ws_error: {type(e).__name__}"
                logger.error(f"{self._log_prefix} WS error: {repr(e)}")
                self._reconnect_count += 1

            except Exception as e:
                # 其他未知错误
                disconnect_reason = f"unknown_error: {type(e).__name__}"
                logger.error(f"{self._log_prefix} Unknown error: {repr(e)}")
                self._reconnect_count += 1

            finally:
                # 标记为未连接
                was_connected = self._is_connected
                self._is_connected = False

                # 触发断开回调（认证失败时已经触发过专门的回调，这里也触发disconnect以便清理状态）
                if was_connected and self.on_disconnect:
                    try:
                        self.on_disconnect(disconnect_reason)
                    except Exception as e:
                        logger.error(f"{self._log_prefix} on_disconnect error: {repr(e)}")

                # 清理 keepalive 任务
                if keepalive_task:
                    keepalive_task.cancel()
                    try:
                        await asyncio.wait([keepalive_task], timeout=1.0)
                    except Exception as e:
                        # P3 Fix: 添加日志
                        logger.debug(f"{self._log_prefix} 取消 keepalive 任务异常: {e}")

                # 关闭 listenKey
                if listen_key:
                    try:
                        await asyncio.to_thread(
                            _close_listen_key_sync, 
                            listen_key, 
                            self._api_key, 
                            self._is_testnet, 
                            5
                        )
                        logger.info(f"{self._log_prefix} ListenKey closed")
                    except Exception as e:
                        logger.warning(f"{self._log_prefix} Failed to close listenKey: {repr(e)}")

            # 如果需要停止,退出循环
            if self._stop_flag.is_set():
                break

            # 重连等待(指数退避 + 随机抖动)
            if self.max_reconnect_attempts == 0 or self._reconnect_count < self.max_reconnect_attempts:
                # 添加随机抖动 (0-30秒)，避免所有用户同时重连导致限流
                jitter = random.uniform(0, 30)
                actual_sleep = sleep_s + jitter
                logger.info(f"{self._log_prefix} Reconnecting in {actual_sleep:.1f}s... (attempt {self._reconnect_count}, jitter={jitter:.1f}s)")
                await asyncio.sleep(actual_sleep)
                sleep_s = min(self.max_reconnect_sleep_s, sleep_s * 1.5)

        logger.info(f"{self._log_prefix} WebSocket stopped")

    async def _receive_loop(self, ws, keepalive_failed: asyncio.Event) -> str:
        """
        消息接收循环

        Returns:
            断开原因字符串
        """
        last_message_time = time.time()

        while not self._stop_flag.is_set():
            try:
                # 同时等待: WebSocket 消息 / keepalive 失败 / 消息超时
                recv_task = asyncio.create_task(ws.recv())
                fail_task = asyncio.create_task(keepalive_failed.wait())

                done, pending = await asyncio.wait(
                    {recv_task, fail_task},
                    timeout=self.message_timeout_s,
                    return_when=asyncio.FIRST_COMPLETED,
                )

                # 取消未完成的任务
                for t in pending:
                    t.cancel()

                # 情况1: keepalive 失败
                if fail_task in done and keepalive_failed.is_set():
                    logger.warning(f"{self._log_prefix} Keepalive failed -> reconnect")
                    return "keepalive_failed"

                # 情况2: 消息超时
                if recv_task not in done:
                    elapsed = time.time() - last_message_time
                    logger.info(f"{self._log_prefix} No message for {elapsed:.0f}s -> reconnect")
                    return "message_timeout"

                # 情况3: 收到消息
                msg = recv_task.result()
                last_message_time = time.time()

                if not msg:
                    continue

                # 解析 JSON
                try:
                    data = json.loads(msg)
                except json.JSONDecodeError as e:
                    logger.error(f"{self._log_prefix} JSON decode error: {repr(e)}: {msg[:100]}")
                    continue

                # 忽略非事件消息(如 ACK)
                if not isinstance(data, dict) or "e" not in data:
                    continue

                # 调用用户回调
                try:
                    self.on_message(data)
                except Exception as e:
                    logger.error(f"{self._log_prefix} on_message error: {repr(e)}")

            except asyncio.CancelledError:
                return "cancelled"
            except Exception as e:
                logger.error(f"{self._log_prefix} receive_loop error: {repr(e)}")
                return f"receive_error: {type(e).__name__}"

        return "stopped"

    async def _keepalive_loop(self, listen_key: str, failed_evt: asyncio.Event):
        """
        定期续期 listenKey

        Args:
            listen_key: 要续期的 listenKey
            failed_evt: 失败时设置此事件通知主循环
        """
        while not self._stop_flag.is_set():
            try:
                await asyncio.sleep(self.keepalive_interval_s)

                if self._stop_flag.is_set():
                    break

                # 执行续期操作
                await asyncio.wait_for(
                    asyncio.to_thread(
                        _keepalive_listen_key_sync, 
                        listen_key, 
                        self._api_key, 
                        self._is_testnet, 
                        10
                    ),
                    timeout=12.0,
                )
                logger.debug(f"{self._log_prefix} Keepalive successful")

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"{self._log_prefix} Keepalive error: {repr(e)}")
                failed_evt.set()
                break


# ============================================================
# 多用户工厂函数
# ============================================================

def create_user_ws(
    ctx: 'UserContext',
    on_message: Callable[[dict], None],
    **kwargs
) -> Optional[FuturesUserWS]:
    """
    为指定用户创建 WebSocket 实例
    
    Args:
        ctx: 用户上下文
        on_message: 消息回调函数
        **kwargs: 其他 FuturesUserWS 参数
    
    Returns:
        FuturesUserWS 实例，如果用户未配置 Binance 则返回 None
    """
    # 从数据库获取 Binance 交易所配置
    from core.user_db import config_loader
    
    binance_config = config_loader.get_user_exchange_config(ctx.uid, 'binance')
    if not binance_config or not binance_config.get('api_key'):
        logger.warning(f"[{ctx.uid}] 未配置 Binance API Key，无法创建 WebSocket")
        return None
    
    return FuturesUserWS(
        on_message=on_message,
        api_key=binance_config['api_key'],
        is_testnet=binance_config.get('is_testnet', False),
        uid=ctx.uid,
        **kwargs
    )


# 类型提示
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from core.user_context import UserContext
