# exchanges/okx/websocket.py
"""
OKX WebSocket 数据流

包含:
- Private Channel: 账户更新、持仓更新
- Public Channel: 标记价格更新

OKX WebSocket 文档: https://www.okx.com/docs-v5/en/#websocket-api

特性:
- 指数退避重连 (1s -> 2s -> 4s -> ... -> 60s)
- 连接状态跟踪 (connected/disconnected/reconnecting/error)
- 状态回调支持 ExchangeMonitor 集成
"""

import asyncio
import base64
import hashlib
import hmac
import json
import logging
import random
import threading
import time
from datetime import datetime, timezone
from enum import Enum
from typing import Callable, Dict, Optional, Any

logger = logging.getLogger(__name__)


class ConnectionState(Enum):
    """WebSocket 连接状态"""
    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    RECONNECTING = "reconnecting"
    ERROR = "error"
    AUTH_FAILED = "auth_failed"  # 认证失败，不重试
    STOPPED = "stopped"


class AuthenticationError(Exception):
    """认证失败错误 - 不应该重试"""
    pass


class OKXUserStream:
    """
    OKX Private WebSocket Stream
    
    接收账户和持仓的实时更新
    
    重连策略:
    - 指数退避: 1s -> 2s -> 4s -> 8s -> ... -> 60s (最大)
    - 随机抖动: ±10% 防止惊群效应
    - 连接成功后重置退避时间
    """
    
    # 重连配置
    RECONNECT_MIN_DELAY = 1.0      # 最小重连延迟 (秒)
    RECONNECT_MAX_DELAY = 60.0     # 最大重连延迟 (秒)
    RECONNECT_MULTIPLIER = 2.0    # 退避倍数
    RECONNECT_JITTER = 0.1        # 抖动比例 (10%)
    
    def __init__(
        self,
        api_key: str,
        api_secret: str,
        passphrase: str,
        is_testnet: bool = False,
        uid: str = "",
        on_account: Callable[[Dict], None] = None,
        on_position: Callable[[Dict], None] = None,
        on_order: Callable[[Dict], None] = None,
        on_fill: Callable[[Dict], None] = None,  # 成交明细回调
        on_algo_order: Callable[[Dict], None] = None,  # 策略订单回调（止盈止损）
        on_state_change: Callable[[ConnectionState, Optional[str]], None] = None,
    ):
        self.api_key = api_key
        self.api_secret = api_secret
        self.passphrase = passphrase
        self.is_testnet = is_testnet
        self.uid = uid
        
        self.on_account = on_account
        self.on_position = on_position
        self.on_order = on_order
        self.on_fill = on_fill  # 成交明细回调
        self.on_algo_order = on_algo_order  # 策略订单回调（止盈止损）
        self.on_state_change = on_state_change  # 状态变化回调
        
        self._ws = None
        self._running = False
        self._loop = None  # 保存事件循环引用
        self._ping_task = None  # 使用 asyncio task 替代 thread
        
        # 连接状态
        self._state = ConnectionState.DISCONNECTED
        self._reconnect_delay = self.RECONNECT_MIN_DELAY
        self._reconnect_count = 0
        self._last_message_time = 0
        
        # WebSocket URL
        if is_testnet:
            self.ws_url = "wss://wspap.okx.com:8443/ws/v5/private?brokerId=9999"
        else:
            self.ws_url = "wss://ws.okx.com:8443/ws/v5/private"
    
    @property
    def state(self) -> ConnectionState:
        """当前连接状态"""
        return self._state
    
    @property
    def last_message_time(self) -> int:
        """最后一次收到消息的时间戳 (ms)"""
        return self._last_message_time
    
    @property
    def reconnect_count(self) -> int:
        """重连次数"""
        return self._reconnect_count
    
    def _set_state(self, state: ConnectionState, error: str = None):
        """设置连接状态并触发回调"""
        old_state = self._state
        self._state = state
        if old_state != state:
            logger.info(f"[{self.uid}][okx] State: {old_state.value} -> {state.value}")
            if self.on_state_change:
                try:
                    self.on_state_change(state, error)
                except Exception as e:
                    logger.error(f"[{self.uid}][okx] State callback error: {e}")
    
    def _get_reconnect_delay(self) -> float:
        """计算下次重连延迟（带抖动）"""
        delay = self._reconnect_delay
        # 添加随机抖动
        jitter = delay * self.RECONNECT_JITTER * (random.random() * 2 - 1)
        return delay + jitter
    
    def _increase_reconnect_delay(self):
        """增加重连延迟（指数退避）"""
        self._reconnect_delay = min(
            self._reconnect_delay * self.RECONNECT_MULTIPLIER,
            self.RECONNECT_MAX_DELAY
        )
        self._reconnect_count += 1
    
    def _reset_reconnect_delay(self):
        """重置重连延迟（连接成功后）"""
        self._reconnect_delay = self.RECONNECT_MIN_DELAY
        self._reconnect_count = 0
    
    def _sign(self, timestamp: str, method: str, request_path: str) -> str:
        """生成签名"""
        message = timestamp + method + request_path
        signature = hmac.new(
            self.api_secret.encode('utf-8'),
            message.encode('utf-8'),
            hashlib.sha256
        ).digest()
        return base64.b64encode(signature).decode('utf-8')
    
    def start(self):
        """启动 WebSocket 连接"""
        if self._running:
            return
        
        self._running = True
        self._set_state(ConnectionState.CONNECTING)
        thread = threading.Thread(
            target=self._run_async_loop,
            daemon=True,
            name=f"okx-user-ws-{self.uid}"
        )
        thread.start()
    
    def _run_async_loop(self):
        """在新线程中运行异步事件循环"""
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        try:
            self._loop.run_until_complete(self._run())
        except Exception as e:
            logger.error(f"[{self.uid}][okx] WebSocket loop error: {e}")
            self._set_state(ConnectionState.ERROR, str(e))
        finally:
            # 安全关闭事件循环：先取消所有 pending 任务
            try:
                pending = asyncio.all_tasks(self._loop)
                for task in pending:
                    task.cancel()
                if pending:
                    self._loop.run_until_complete(
                        asyncio.gather(*pending, return_exceptions=True)
                    )
            except Exception:
                pass
            finally:
                self._loop.close()
                self._loop = None
    
    async def _run(self):
        """WebSocket 运行循环"""
        import websockets
        
        while self._running:
            try:
                if self._reconnect_count > 0:
                    self._set_state(ConnectionState.RECONNECTING)
                else:
                    self._set_state(ConnectionState.CONNECTING)
                
                async with websockets.connect(
                    self.ws_url,
                    open_timeout=30,  # 增加握手超时时间
                    ping_interval=None,  # 我们自己管理 ping
                    close_timeout=10,
                ) as ws:
                    self._ws = ws
                    logger.info(f"[{self.uid}][okx] WebSocket connected")
                    
                    # 登录认证
                    await self._authenticate(ws)
                    
                    # 订阅频道
                    await self._subscribe(ws)
                    
                    # 连接成功，重置退避
                    self._reset_reconnect_delay()
                    self._set_state(ConnectionState.CONNECTED)
                    
                    # 启动 ping 保活 (使用 asyncio task)
                    self._ping_task = asyncio.create_task(self._ping_loop(ws))
                    
                    # 接收消息
                    async for message in ws:
                        self._last_message_time = int(time.time() * 1000)
                        await self._handle_message(message)
                        
            except asyncio.CancelledError:
                logger.info(f"[{self.uid}][okx] WebSocket cancelled")
                break
            except AuthenticationError as e:
                # 认证失败，不重试
                error_msg = str(e)
                logger.error(f"[{self.uid}][okx] Authentication failed (will not retry): {error_msg}")
                self._set_state(ConnectionState.AUTH_FAILED, error_msg)
                self._running = False
                break
            except Exception as e:
                error_msg = str(e)
                logger.error(f"[{self.uid}][okx] WebSocket error: {error_msg}")
                self._set_state(ConnectionState.ERROR, error_msg)
                
                if self._running:
                    delay = self._get_reconnect_delay()
                    logger.info(f"[{self.uid}][okx] Reconnecting in {delay:.1f}s (attempt #{self._reconnect_count + 1})")
                    self._increase_reconnect_delay()
                    await asyncio.sleep(delay)
            finally:
                # 取消 ping task
                if self._ping_task and not self._ping_task.done():
                    self._ping_task.cancel()
                    try:
                        await self._ping_task
                    except asyncio.CancelledError:
                        pass
                self._ping_task = None
                self._ws = None
        
        self._set_state(ConnectionState.STOPPED)
    
    async def _authenticate(self, ws):
        """发送登录认证"""
        timestamp = str(int(time.time()))
        sign = self._sign(timestamp, "GET", "/users/self/verify")
        
        login_msg = {
            "op": "login",
            "args": [{
                "apiKey": self.api_key,
                "passphrase": self.passphrase,
                "timestamp": timestamp,
                "sign": sign
            }]
        }
        
        await ws.send(json.dumps(login_msg))
        
        # 等待登录响应
        response = await ws.recv()
        data = json.loads(response)
        
        if data.get("event") == "login" and data.get("code") == "0":
            logger.info(f"[{self.uid}][okx] WebSocket authenticated")
        else:
            error_code = data.get("code")
            error_msg = data.get("msg", "Unknown error")
            logger.error(f"[{self.uid}][okx] WebSocket auth failed: {data}")
            # OKX error codes for auth failures:
            # 60001: Invalid OK_ACCESS_KEY
            # 60002: Invalid OK_ACCESS_SIGN
            # 60005: Invalid OK_ACCESS_PASSPHRASE
            # 60014: Invalid API Key (read-only)
            if error_code in ("60001", "60002", "60005", "60014"):
                raise AuthenticationError(f"OKX auth failed (code {error_code}): {error_msg}")
            raise Exception(f"OKX WebSocket auth failed: {error_msg}")
    
    async def _subscribe(self, ws):
        """订阅频道"""
        # 订阅账户、持仓、订单、成交频道
        subscribe_msg = {
            "op": "subscribe",
            "args": [
                {"channel": "account"},           # 账户余额更新
                {"channel": "positions", "instType": "SWAP"},  # 持仓更新
                {"channel": "orders", "instType": "SWAP"},     # 订单更新
                {"channel": "fills", "instType": "SWAP"},      # 成交明细 - 用于检测平仓成交
            ]
        }
        
        await ws.send(json.dumps(subscribe_msg))
        logger.info(f"[{self.uid}][okx] Subscribed to private channels (fills requires VIP6+)")
        
        # 注意：OKX 没有专门的策略订单 WebSocket 频道
        # 止盈止损信息通过 positions 频道的 closeOrderAlgo 字段获取
        # 止盈止损触发后的实际成交价格，通过 REST API 兜底查询
    
    async def _handle_message(self, message: str):
        """处理接收到的消息"""
        try:
            # Handle bytes message
            if isinstance(message, bytes):
                message = message.decode('utf-8')
            
            # 跳过空消息
            if not message or not message.strip():
                return
            
            # 处理纯文本 pong 响应（在 JSON 解析前检查）
            if message == "pong":
                return
            
            data = json.loads(message)
            
            # Ping/Pong (JSON 格式)
            if data == "pong":
                return
            
            # 事件消息
            event = data.get("event")
            if event:
                if event == "subscribe":
                    logger.debug(f"[{self.uid}][okx] Subscribed: {data.get('arg')}")
                elif event == "error":
                    error_code = data.get("code", "")
                    error_msg = data.get("msg", "")
                    # VIP6+ required for fills channel (code 60029) - gracefully ignore
                    if error_code == "60029":
                        logger.warning(f"[{self.uid}][okx] fills channel requires VIP6+ (code 60029), will use orders channel instead")
                    else:
                        logger.error(f"[{self.uid}][okx] WebSocket error: {data}")
                return
            
            # 数据推送
            arg = data.get("arg", {})
            channel = arg.get("channel")
            push_data = data.get("data", [])
            
            # 诊断日志：消息计数统计
            if not hasattr(self, '_msg_count'):
                self._msg_count = {'account': 0, 'positions': 0, 'orders': 0, 'fills': 0, 'algo-orders': 0}
            
            if channel == "account":
                self._msg_count['account'] += len(push_data)
                for item in push_data:
                    if self.on_account:
                        self.on_account(item)
                        
            elif channel == "positions":
                self._msg_count['positions'] += len(push_data)
                for item in push_data:
                    if self.on_position:
                        self.on_position(item)
                        
            elif channel == "orders":
                self._msg_count['orders'] += len(push_data)
                for item in push_data:
                    if self.on_order:
                        self.on_order(item)
            
            elif channel == "fills":
                self._msg_count['fills'] += len(push_data)
                # 成交明细频道 - 用于检测平仓成交
                for item in push_data:
                    if self.on_fill:
                        self.on_fill(item)
            
            elif channel == "orders-algo":
                self._msg_count['algo-orders'] += len(push_data)
                # 策略订单频道 - 止盈止损/条件单/移动止损
                # 使用独立的 on_algo_order 回调处理，获取实际成交价格
                for item in push_data:
                    state = (item.get("state") or "").lower()
                    logger.info(f"[{self.uid}][okx] orders-algo 推送: state={state} "
                               f"instId={item.get('instId')} posSide={item.get('posSide')} "
                               f"actualPx={item.get('actualPx')} actualSz={item.get('actualSz')} "
                               f"slTriggerPx={item.get('slTriggerPx')} tpTriggerPx={item.get('tpTriggerPx')}")
                    if self.on_algo_order:
                        self.on_algo_order(item)
            
            # 每 50 条消息打印一次统计（用于诊断）
            total_msgs = sum(self._msg_count.values())
            if total_msgs > 0 and total_msgs % 50 == 0:
                logger.info(f"[{self.uid}][okx] WS 消息统计: {self._msg_count}")
                        
        except Exception as e:
            logger.error(f"[{self.uid}][okx] Message handling error: {e}")
    
    async def _ping_loop(self, ws):
        """异步 ping 保活循环"""
        try:
            while self._running:
                await asyncio.sleep(25)  # 每25秒发送一次 ping
                if not self._running or not ws:
                    break
                # 兼容新旧版本 websockets 库
                try:
                    is_closed = ws.closed if hasattr(ws, 'closed') else (ws.state.name == 'CLOSED')
                except Exception:
                    is_closed = True
                if is_closed:
                    break
                try:
                    await ws.send("ping")
                    logger.debug(f"[{self.uid}][okx] Ping sent")
                except Exception as e:
                    logger.debug(f"[{self.uid}][okx] Ping error: {e}")
                    break
        except asyncio.CancelledError:
            pass
    
    def stop(self):
        """停止 WebSocket 连接"""
        logger.info(f"[{self.uid}][okx] Stopping WebSocket...")
        self._running = False
        self._set_state(ConnectionState.STOPPED)
        
        # 取消 ping task
        if self._ping_task and not self._ping_task.done():
            self._ping_task.cancel()
        
        # 关闭 WebSocket
        if self._ws and self._loop:
            try:
                asyncio.run_coroutine_threadsafe(
                    self._ws.close(),
                    self._loop
                ).result(timeout=5)
            except Exception:
                pass
        self._ws = None


class OKXMarkPriceStream:
    """
    OKX Mark Price Stream (公开频道)
    
    接收标记价格实时更新
    
    业务逻辑（参考 Binance/Bitget）:
    - 从 pf:pos:active:{uid} 读取当前持仓 symbols
    - 只订阅有持仓的 symbols（不订阅无关币种）
    - 定期检查持仓变化，symbols 变化时重连更新订阅
    - 没有持仓时不连接
    
    重连策略: 指数退避 (1s -> 60s)
    """
    
    # 重连配置
    RECONNECT_MIN_DELAY = 1.0
    RECONNECT_MAX_DELAY = 60.0
    RECONNECT_MULTIPLIER = 2.0
    RECONNECT_JITTER = 0.1
    
    # 持仓检查间隔
    REFRESH_SYMBOLS_INTERVAL_S = 2.0
    
    def __init__(
        self,
        redis_conn,
        uid: str,
        is_testnet: bool = False,
        on_tick: Callable[[str, float, int], None] = None,
        on_state_change: Callable[[ConnectionState, Optional[str]], None] = None,
    ):
        self.rds = redis_conn
        self.uid = uid
        self.is_testnet = is_testnet
        self.on_tick = on_tick
        self.on_state_change = on_state_change
        
        self._ws = None
        self._running = False
        self._loop = None
        self._ping_task = None
        self._current_symbols: set = set()  # 当前已订阅的 symbols
        
        # 连接状态
        self._state = ConnectionState.DISCONNECTED
        self._reconnect_delay = self.RECONNECT_MIN_DELAY
        self._reconnect_count = 0
        
        # 公开 WebSocket URL
        if is_testnet:
            self.ws_url = "wss://wspap.okx.com:8443/ws/v5/public?brokerId=9999"
        else:
            self.ws_url = "wss://ws.okx.com:8443/ws/v5/public"
    
    @property
    def state(self) -> ConnectionState:
        return self._state
    
    def _load_active_symbols(self) -> set:
        """
        从活跃持仓和挂单中提取需要订阅的 symbols
        
        活跃持仓格式: ["SOLUSDT:SHORT", "BTCUSDT:LONG"]
        提取 symbol: SOLUSDT, BTCUSDT
        然后转换为 OKX instId 格式: SOL-USDT-SWAP, BTC-USDT-SWAP
        """
        from core.pf_compatibility import pf_compat
        
        symbols: set = set()
        
        # 1. 从活跃持仓获取 symbols
        fields = pf_compat.get_pf_pos_active(self.uid, "okx")
        
        for f in fields:
            if isinstance(f, (bytes, bytearray)):
                f = f.decode()
            f = str(f).strip()
            if not f:
                continue
            
            # 格式: "SYMBOL:SIDE" -> 取第一部分
            parts = f.split(":")
            if parts:
                sym = parts[0].upper()
                if sym and sym not in ("LONG", "SHORT"):
                    # 转换为 OKX instId 格式
                    # BTCUSDT -> BTC-USDT-SWAP
                    if sym.endswith("USDT"):
                        base = sym[:-4]
                        inst_id = f"{base}-USDT-SWAP"
                        symbols.add(inst_id)
                    elif sym.endswith("USDC"):
                        base = sym[:-4]
                        inst_id = f"{base}-USDC-SWAP"
                        symbols.add(inst_id)
                    else:
                        # 不识别的格式，直接使用
                        symbols.add(sym)
        
        # 2. 从挂单获取 symbols
        try:
            open_orders = pf_compat.get_pf_open_orders(self.uid, "okx")
            if open_orders and isinstance(open_orders, dict):
                for order_id, order in open_orders.items():
                    if isinstance(order, dict):
                        # 优先使用 instId（OKX 格式）
                        inst_id = order.get("instId")
                        if inst_id:
                            symbols.add(inst_id)
                        else:
                            # 回退到 symbol（Binance 格式），需要转换
                            sym = order.get("symbol", "").upper()
                            if sym:
                                if sym.endswith("USDT"):
                                    base = sym[:-4]
                                    inst_id = f"{base}-USDT-SWAP"
                                    symbols.add(inst_id)
                                elif sym.endswith("USDC"):
                                    base = sym[:-4]
                                    inst_id = f"{base}-USDC-SWAP"
                                    symbols.add(inst_id)
        except Exception as e:
            logger.debug(f"[OKXMarkPriceStream:{self.uid}] 获取挂单 symbols 失败: {e}")
        
        return symbols
    
    def add_symbol(self, symbol: str):
        """
        动态添加订阅符号（向后兼容）
        注意：现在主要通过 _load_active_symbols 自动检测
        """
        # 转换为 OKX instId 格式
        if not symbol.endswith("-SWAP"):
            if symbol.endswith("USDT"):
                base = symbol[:-4]
                inst_id = f"{base}-USDT-SWAP"
            else:
                inst_id = symbol
        else:
            inst_id = symbol
        
        if inst_id in self._current_symbols:
            return
        
        # 如果已连接，发送订阅请求
        if self._ws and self._loop and self._state == ConnectionState.CONNECTED:
            self._current_symbols.add(inst_id)
            
            async def _subscribe_symbol():
                try:
                    subscribe_msg = {
                        "op": "subscribe",
                        "args": [{"channel": "mark-price", "instId": inst_id}]
                    }
                    await self._ws.send(json.dumps(subscribe_msg))
                    logger.info(f"[{self.uid}][okx] 动态订阅标记价格: {inst_id}")
                except Exception as e:
                    logger.warning(f"[{self.uid}][okx] 动态订阅失败 {inst_id}: {e}")
            
            try:
                asyncio.run_coroutine_threadsafe(_subscribe_symbol(), self._loop)
            except Exception as e:
                logger.warning(f"[{self.uid}][okx] 发送订阅请求失败 {inst_id}: {e}")
    
    def _set_state(self, state: ConnectionState, error: str = None):
        old_state = self._state
        self._state = state
        if old_state != state:
            logger.debug(f"[{self.uid}][okx] Mark price state: {old_state.value} -> {state.value}")
            if self.on_state_change:
                try:
                    self.on_state_change(state, error)
                except Exception as e:
                    logger.error(f"[{self.uid}][okx] Mark price state callback error: {e}")
    
    def _get_reconnect_delay(self) -> float:
        delay = self._reconnect_delay
        jitter = delay * self.RECONNECT_JITTER * (random.random() * 2 - 1)
        return delay + jitter
    
    def _increase_reconnect_delay(self):
        self._reconnect_delay = min(
            self._reconnect_delay * self.RECONNECT_MULTIPLIER,
            self.RECONNECT_MAX_DELAY
        )
        self._reconnect_count += 1
    
    def _reset_reconnect_delay(self):
        self._reconnect_delay = self.RECONNECT_MIN_DELAY
        self._reconnect_count = 0
    
    def start(self):
        """启动 WebSocket 连接"""
        if self._running:
            return
        
        self._running = True
        self._set_state(ConnectionState.CONNECTING)
        thread = threading.Thread(
            target=self._run_async_loop,
            daemon=True,
            name="okx-mark-price-ws"
        )
        thread.start()
    
    def _run_async_loop(self):
        """在新线程中运行异步事件循环"""
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        try:
            self._loop.run_until_complete(self._run())
        except Exception as e:
            logger.error(f"[okx] Mark price WebSocket loop error: {e}")
            self._set_state(ConnectionState.ERROR, str(e))
        finally:
            # 安全关闭事件循环：先取消所有 pending 任务
            try:
                pending = asyncio.all_tasks(self._loop)
                for task in pending:
                    task.cancel()
                if pending:
                    self._loop.run_until_complete(
                        asyncio.gather(*pending, return_exceptions=True)
                    )
            except Exception:
                pass
            finally:
                self._loop.close()
                self._loop = None
    
    async def _run(self):
        """
        WebSocket 运行循环
        
        参考 Binance/Bitget 的逻辑:
        1. 从持仓中加载 symbols
        2. 没有持仓时等待，不连接
        3. symbols 变化时重连更新订阅
        """
        import websockets
        
        while self._running:
            # 1. 加载当前持仓的 symbols
            symbols = self._load_active_symbols()
            
            # 2. 没有持仓就等待，不连接
            if not symbols:
                self._current_symbols = set()
                self._set_state(ConnectionState.DISCONNECTED)
                await asyncio.sleep(self.REFRESH_SYMBOLS_INTERVAL_S)
                continue
            
            try:
                if self._reconnect_count > 0:
                    self._set_state(ConnectionState.RECONNECTING)
                else:
                    self._set_state(ConnectionState.CONNECTING)
                
                async with websockets.connect(
                    self.ws_url,
                    open_timeout=30,  # 增加握手超时时间
                    ping_interval=20,
                    ping_timeout=10,
                ) as ws:
                    self._ws = ws
                    self._current_symbols = set(symbols)
                    
                    logger.info(f"[{self.uid}][okx] Mark price WebSocket connected, symbols: {symbols}")
                    
                    # 订阅当前持仓的 symbols
                    await self._subscribe(ws, symbols)
                    
                    # 重置退避
                    self._reset_reconnect_delay()
                    self._set_state(ConnectionState.CONNECTED)
                    
                    # 启动 ping 保活
                    self._ping_task = asyncio.create_task(self._ping_loop(ws))
                    
                    # 记录上次检查时间
                    last_check = time.time()
                    
                    while self._running:
                        # 定期检查 symbols 是否变化
                        if time.time() - last_check >= self.REFRESH_SYMBOLS_INTERVAL_S:
                            new_symbols = self._load_active_symbols()
                            last_check = time.time()
                            
                            if new_symbols != self._current_symbols:
                                logger.info(f"[{self.uid}][okx] Symbols changed: {self._current_symbols} -> {new_symbols}")
                                # symbols 变化，断开重连
                                break
                        
                        # 接收消息（带超时）
                        try:
                            message = await asyncio.wait_for(ws.recv(), timeout=30.0)
                            await self._handle_message(message)
                        except asyncio.TimeoutError:
                            # 超时，检查连接状态并继续
                            continue
                        
            except asyncio.CancelledError:
                break
            except Exception as e:
                error_msg = str(e)
                logger.error(f"[{self.uid}][okx] Mark price WebSocket error: {error_msg}")
                self._set_state(ConnectionState.ERROR, error_msg)
                
                if self._running:
                    delay = self._get_reconnect_delay()
                    logger.info(f"[{self.uid}][okx] Mark price reconnecting in {delay:.1f}s")
                    self._increase_reconnect_delay()
                    await asyncio.sleep(delay)
            finally:
                if hasattr(self, '_ping_task') and self._ping_task and not self._ping_task.done():
                    self._ping_task.cancel()
                    try:
                        await self._ping_task
                    except asyncio.CancelledError:
                        pass
                self._ping_task = None
                self._ws = None
        
        self._set_state(ConnectionState.STOPPED)
    
    async def _ping_loop(self, ws):
        """
        应用层 ping 保活循环
        """
        try:
            while self._running:
                await asyncio.sleep(25)  # 每 25 秒发送一次 ping
                if not self._running or not ws:
                    break
                # 检查 ws 是否已关闭
                try:
                    is_closed = ws.closed if hasattr(ws, 'closed') else (ws.state.name == 'CLOSED')
                except Exception:
                    is_closed = True
                if is_closed:
                    break
                try:
                    await ws.send("ping")
                    logger.debug(f"[{self.uid}][okx] Mark price ping sent")
                except Exception as e:
                    logger.debug(f"[{self.uid}][okx] Mark price ping error: {e}")
                    break
        except asyncio.CancelledError:
            pass
    
    async def _subscribe(self, ws, symbols: set):
        """订阅标记价格频道"""
        if not symbols:
            logger.warning(f"[{self.uid}][okx] 无 symbols 可订阅")
            return
        
        args = [
            {"channel": "mark-price", "instId": s}
            for s in symbols
        ]
        
        subscribe_msg = {"op": "subscribe", "args": args}
        await ws.send(json.dumps(subscribe_msg))
        logger.info(f"[{self.uid}][okx] 订阅标记价格: {symbols}")
    
    async def _handle_message(self, message):
        """处理标记价格消息"""
        try:
            if isinstance(message, bytes):
                message = message.decode('utf-8')
            
            # 跳过空消息
            if not message or not message.strip():
                return
            
            # 处理纯文本 pong 响应
            if message == "pong":
                return
            
            data = json.loads(message)
            
            if data.get("event"):
                return
            
            arg = data.get("arg", {})
            if arg.get("channel") != "mark-price":
                return
            
            for item in data.get("data", []):
                inst_id = item.get("instId", "")
                mark_price = float(item.get("markPx", 0))
                timestamp = int(item.get("ts", 0))
                
                # 转换为标准 symbol 格式 (BTC-USDT-SWAP -> BTCUSDT)
                parts = inst_id.split("-")
                if len(parts) >= 2:
                    symbol = f"{parts[0]}{parts[1]}"
                else:
                    symbol = inst_id
                
                if self.on_tick and symbol and mark_price > 0:
                    self.on_tick(symbol, mark_price, timestamp)
                    
        except Exception as e:
            logger.debug(f"[okx] Mark price message error: {e}")
    
    def stop(self):
        """停止 WebSocket 连接"""
        self._running = False
        self._set_state(ConnectionState.STOPPED)
        if self._ws and self._loop:
            try:
                asyncio.run_coroutine_threadsafe(
                    self._ws.close(),
                    self._loop
                ).result(timeout=5)
            except Exception:
                pass
            self._ws = None


class OKXWebSocket:
    """
    OKX WebSocket 管理器
    
    统一管理用户数据流和标记价格流
    """
    
    def __init__(
        self,
        api_key: str,
        api_secret: str,
        passphrase: str,
        is_testnet: bool = False,
        uid: str = "",
        callbacks: Dict[str, Callable] = None,
    ):
        self.api_key = api_key
        self.api_secret = api_secret
        self.passphrase = passphrase
        self.is_testnet = is_testnet
        self.uid = uid
        self.callbacks = callbacks or {}
        
        self._user_stream: Optional[OKXUserStream] = None
        self._mark_price_stream: Optional[OKXMarkPriceStream] = None
    
    def start(self):
        """启动 WebSocket 连接"""
        # 用户数据流
        self._user_stream = OKXUserStream(
            api_key=self.api_key,
            api_secret=self.api_secret,
            passphrase=self.passphrase,
            is_testnet=self.is_testnet,
            uid=self.uid,
            on_account=self.callbacks.get('on_account'),
            on_position=self.callbacks.get('on_position'),
            on_order=self.callbacks.get('on_order'),
        )
        self._user_stream.start()
        
        # 标记价格流（如果有回调）
        if self.callbacks.get('on_mark_price'):
            self._mark_price_stream = OKXMarkPriceStream(
                is_testnet=self.is_testnet,
                on_tick=self.callbacks.get('on_mark_price'),
            )
            self._mark_price_stream.start()
    
    def stop(self):
        """停止 WebSocket 连接"""
        if self._user_stream:
            self._user_stream.stop()
            self._user_stream = None
        
        if self._mark_price_stream:
            self._mark_price_stream.stop()
            self._mark_price_stream = None
