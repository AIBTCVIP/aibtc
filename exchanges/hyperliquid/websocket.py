# exchanges/hyperliquid/websocket.py
"""
Hyperliquid WebSocket 数据流

包含:
- HyperliquidUserStream: 用户事件流 (fills, funding, liquidation)
- HyperliquidMarkPriceStream: 标记价格流 (allMids)

Hyperliquid WebSocket 文档: https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket

WebSocket 端点: wss://api.hyperliquid.xyz/ws (主网)
              wss://api.hyperliquid-testnet.xyz/ws (测试网)

订阅类型:
- userEvents: 用户事件 (fills, funding, liquidation, nonUserCancel)
- allMids: 所有币种中间价 (用于 mark price)
- l2Book: 订单簿
- trades: 成交

特性:
- 指数退避重连 (1s -> 2s -> 4s -> ... -> 60s)
- 连接状态跟踪
- 状态回调支持 ExchangeMonitor 集成
"""

import asyncio
import json
import logging
import random
import threading
import time
from enum import Enum
from typing import Callable, Dict, Optional, Set

logger = logging.getLogger(__name__)


class ConnectionState(Enum):
    """WebSocket 连接状态"""
    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    RECONNECTING = "reconnecting"
    ERROR = "error"
    AUTH_FAILED = "auth_failed"
    STOPPED = "stopped"


class HyperliquidUserStream:
    """
    Hyperliquid User Events WebSocket Stream
    
    接收用户事件的实时推送:
    - fills: 成交明细
    - funding: 资金费率
    - liquidation: 清算
    - nonUserCancel: 非用户取消
    
    重连策略:
    - 指数退避: 1s -> 2s -> 4s -> 8s -> ... -> 60s (最大)
    - 随机抖动: ±10% 防止惊群效应
    - 连接成功后重置退避时间
    """
    
    # 重连配置
    RECONNECT_MIN_DELAY = 1.0
    RECONNECT_MAX_DELAY = 60.0
    RECONNECT_MULTIPLIER = 2.0
    RECONNECT_JITTER = 0.1
    
    # Ping 间隔 (Hyperliquid 要求 50 秒内发送心跳)
    PING_INTERVAL = 30
    
    def __init__(
        self,
        wallet_address: str,
        is_testnet: bool = False,
        uid: str = "",
        on_fill: Callable[[Dict], None] = None,
        on_funding: Callable[[Dict], None] = None,
        on_liquidation: Callable[[Dict], None] = None,
        on_order_update: Callable[[Dict], None] = None,
        on_state_change: Callable[[ConnectionState, Optional[str]], None] = None,
    ):
        self.wallet_address = wallet_address
        self.is_testnet = is_testnet
        self.uid = uid
        
        self.on_fill = on_fill
        self.on_funding = on_funding
        self.on_liquidation = on_liquidation
        self.on_order_update = on_order_update
        self.on_state_change = on_state_change
        
        self._ws = None
        self._running = False
        self._loop = None
        self._ping_task = None
        
        # 连接状态
        self._state = ConnectionState.DISCONNECTED
        self._reconnect_delay = self.RECONNECT_MIN_DELAY
        self._reconnect_count = 0
        self._last_message_time = 0
        
        # WebSocket URL
        if is_testnet:
            self.ws_url = "wss://api.hyperliquid-testnet.xyz/ws"
        else:
            self.ws_url = "wss://api.hyperliquid.xyz/ws"
    
    @property
    def state(self) -> ConnectionState:
        return self._state
    
    @property
    def last_message_time(self) -> int:
        return self._last_message_time
    
    @property
    def reconnect_count(self) -> int:
        return self._reconnect_count
    
    def _set_state(self, state: ConnectionState, error: str = None):
        old_state = self._state
        self._state = state
        if old_state != state:
            logger.info(f"[{self.uid}][hyperliquid] UserStream state: {old_state.value} -> {state.value}")
            if self.on_state_change:
                try:
                    self.on_state_change(state, error)
                except Exception as e:
                    logger.error(f"[{self.uid}][hyperliquid] State callback error: {e}")
    
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
        
        if not self.wallet_address:
            logger.error(f"[{self.uid}][hyperliquid] 钱包地址未设置，无法启动 UserStream")
            return
        
        self._running = True
        self._set_state(ConnectionState.CONNECTING)
        thread = threading.Thread(
            target=self._run_async_loop,
            daemon=True,
            name=f"hyperliquid-user-ws-{self.uid}"
        )
        thread.start()
    
    def _run_async_loop(self):
        """在新线程中运行异步事件循环"""
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        try:
            self._loop.run_until_complete(self._run())
        except Exception as e:
            logger.error(f"[{self.uid}][hyperliquid] WebSocket loop error: {e}")
            self._set_state(ConnectionState.ERROR, str(e))
        finally:
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
                    logger.info(f"[{self.uid}][hyperliquid] UserStream WebSocket connected")
                    
                    # 订阅用户事件
                    await self._subscribe(ws)
                    
                    # 连接成功，重置退避
                    self._reset_reconnect_delay()
                    self._set_state(ConnectionState.CONNECTED)
                    
                    # 启动 ping 保活
                    self._ping_task = asyncio.create_task(self._ping_loop(ws))
                    
                    # 接收消息
                    async for message in ws:
                        self._last_message_time = int(time.time() * 1000)
                        await self._handle_message(message)
                        
            except asyncio.CancelledError:
                logger.info(f"[{self.uid}][hyperliquid] UserStream cancelled")
                break
            except Exception as e:
                error_msg = str(e)
                logger.error(f"[{self.uid}][hyperliquid] UserStream error: {error_msg}")
                self._set_state(ConnectionState.ERROR, error_msg)
                
                if self._running:
                    delay = self._get_reconnect_delay()
                    logger.info(f"[{self.uid}][hyperliquid] UserStream reconnecting in {delay:.1f}s (attempt #{self._reconnect_count + 1})")
                    self._increase_reconnect_delay()
                    await asyncio.sleep(delay)
            finally:
                if self._ping_task and not self._ping_task.done():
                    self._ping_task.cancel()
                    try:
                        await self._ping_task
                    except asyncio.CancelledError:
                        pass
                self._ping_task = None
                self._ws = None
        
        self._set_state(ConnectionState.STOPPED)
    
    async def _subscribe(self, ws):
        """订阅用户事件频道"""
        # 1. 订阅 userEvents (fills, funding, liquidation)
        subscribe_msg = {
            "method": "subscribe",
            "subscription": {
                "type": "userEvents",
                "user": self.wallet_address
            }
        }
        await ws.send(json.dumps(subscribe_msg))
        logger.info(f"[{self.uid}][hyperliquid] Subscribed to userEvents for {self.wallet_address[:10]}...")
        
        # 2. 订阅 orderUpdates (订单状态变化)
        order_updates_msg = {
            "method": "subscribe",
            "subscription": {
                "type": "orderUpdates",
                "user": self.wallet_address
            }
        }
        await ws.send(json.dumps(order_updates_msg))
        logger.info(f"[{self.uid}][hyperliquid] Subscribed to orderUpdates for {self.wallet_address[:10]}...")
    
    async def _handle_message(self, message):
        """处理接收到的消息"""
        try:
            if isinstance(message, bytes):
                message = message.decode('utf-8')
            
            if not message or not message.strip():
                return
            
            data = json.loads(message)
            
            # 订阅响应
            channel = data.get("channel")
            if channel == "subscriptionResponse":
                logger.debug(f"[{self.uid}][hyperliquid] Subscription response: {data}")
                return
            
            # Pong 响应
            if channel == "pong":
                return
            
            # 用户事件
            if channel == "userEvents":
                event_data = data.get("data", {})
                
                # fills: 成交明细
                if "fills" in event_data:
                    for fill in event_data["fills"]:
                        if self.on_fill:
                            self.on_fill(fill)
                
                # funding: 资金费率
                if "funding" in event_data:
                    if self.on_funding:
                        self.on_funding(event_data["funding"])
                
                # liquidation: 清算
                if "liquidation" in event_data:
                    if self.on_liquidation:
                        self.on_liquidation(event_data["liquidation"])
                
                # nonUserCancel: 非用户取消 (系统取消等)
                if "nonUserCancel" in event_data:
                    logger.debug(f"[{self.uid}][hyperliquid] nonUserCancel: {event_data['nonUserCancel']}")
            
            # 订单更新
            if channel == "orderUpdates":
                orders = data.get("data", [])
                if isinstance(orders, list):
                    for order in orders:
                        if self.on_order_update:
                            self.on_order_update(order)
                        
        except Exception as e:
            logger.error(f"[{self.uid}][hyperliquid] Message handling error: {e}")
    
    async def _ping_loop(self, ws):
        """Ping 保活循环"""
        try:
            while self._running:
                await asyncio.sleep(self.PING_INTERVAL)
                if not self._running or not ws:
                    break
                try:
                    is_closed = ws.closed if hasattr(ws, 'closed') else (ws.state.name == 'CLOSED')
                except Exception:
                    is_closed = True
                if is_closed:
                    break
                try:
                    # Hyperliquid ping 格式
                    ping_msg = {"method": "ping"}
                    await ws.send(json.dumps(ping_msg))
                    logger.debug(f"[{self.uid}][hyperliquid] Ping sent")
                except Exception as e:
                    logger.debug(f"[{self.uid}][hyperliquid] Ping error: {e}")
                    break
        except asyncio.CancelledError:
            pass
    
    def stop(self):
        """停止 WebSocket 连接"""
        logger.info(f"[{self.uid}][hyperliquid] Stopping UserStream...")
        self._running = False
        self._set_state(ConnectionState.STOPPED)
        
        if self._ping_task and not self._ping_task.done():
            self._ping_task.cancel()
        
        if self._ws and self._loop:
            try:
                asyncio.run_coroutine_threadsafe(
                    self._ws.close(),
                    self._loop
                ).result(timeout=5)
            except Exception:
                pass
        self._ws = None


class HyperliquidMarkPriceStream:
    """
    Hyperliquid Mark Price Stream (公开频道)
    
    使用 allMids 订阅获取所有币种的中间价作为标记价格
    
    业务逻辑（参考 Binance/Bitget MarkPriceWS）:
    - 从 pf:pos:active:{uid} 读取当前持仓 symbols
    - 只处理有持仓的 symbols（过滤无关币种）
    - 定期检查持仓变化
    - 没有持仓时也保持连接（因为 allMids 是全量订阅）
    
    重连策略: 指数退避 (1s -> 60s)
    """
    
    RECONNECT_MIN_DELAY = 1.0
    RECONNECT_MAX_DELAY = 60.0
    RECONNECT_MULTIPLIER = 2.0
    RECONNECT_JITTER = 0.1
    
    PING_INTERVAL = 30
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
        self._current_symbols: Set[str] = set()
        
        self._state = ConnectionState.DISCONNECTED
        self._reconnect_delay = self.RECONNECT_MIN_DELAY
        self._reconnect_count = 0
        
        if is_testnet:
            self.ws_url = "wss://api.hyperliquid-testnet.xyz/ws"
        else:
            self.ws_url = "wss://api.hyperliquid.xyz/ws"
    
    @property
    def state(self) -> ConnectionState:
        return self._state
    
    def _load_active_symbols(self) -> Set[str]:
        """
        从活跃持仓中提取需要处理的 symbols
        
        活跃持仓格式: ["BTCUSDT:LONG", "ETHUSDT:SHORT"]
        提取 symbol (Hyperliquid 格式): BTC, ETH
        """
        from core.pf_compatibility import pf_compat
        
        fields = pf_compat.get_pf_pos_active(self.uid, "hyperliquid")
        symbols: Set[str] = set()
        
        for f in fields:
            if isinstance(f, (bytes, bytearray)):
                f = f.decode()
            f = str(f).strip()
            if not f:
                continue
            
            parts = f.split(":")
            if parts:
                sym = parts[0].upper()
                # 转换为 Hyperliquid 格式: BTCUSDT -> BTC
                if sym.endswith('USDT'):
                    sym = sym[:-4]
                elif sym.endswith('USDC'):
                    sym = sym[:-4]
                if sym and sym not in ("LONG", "SHORT"):
                    symbols.add(sym)
        
        return symbols
    
    def add_symbol(self, symbol: str):
        """动态添加符号（allMids 已订阅所有，这里只需更新过滤列表）"""
        # 转换为 Hyperliquid 格式
        if symbol.endswith('USDT'):
            symbol = symbol[:-4]
        elif symbol.endswith('USDC'):
            symbol = symbol[:-4]
        self._current_symbols.add(symbol)
        logger.debug(f"[{self.uid}][hyperliquid] Added symbol to filter: {symbol}")
    
    def _set_state(self, state: ConnectionState, error: str = None):
        old_state = self._state
        self._state = state
        if old_state != state:
            logger.debug(f"[{self.uid}][hyperliquid] MarkPrice state: {old_state.value} -> {state.value}")
            if self.on_state_change:
                try:
                    self.on_state_change(state, error)
                except Exception as e:
                    logger.error(f"[{self.uid}][hyperliquid] State callback error: {e}")
    
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
            name=f"hyperliquid-mark-price-ws-{self.uid}"
        )
        thread.start()
    
    def _run_async_loop(self):
        """在新线程中运行异步事件循环"""
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        try:
            self._loop.run_until_complete(self._run())
        except Exception as e:
            logger.error(f"[{self.uid}][hyperliquid] MarkPrice WebSocket loop error: {e}")
            self._set_state(ConnectionState.ERROR, str(e))
        finally:
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
            # 加载当前持仓的 symbols
            self._current_symbols = self._load_active_symbols()
            
            # 没有持仓时等待，不连接（节省资源）
            if not self._current_symbols:
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
                    ping_interval=None,
                    close_timeout=10,
                ) as ws:
                    self._ws = ws
                    logger.info(f"[{self.uid}][hyperliquid] MarkPrice WebSocket connected, symbols: {self._current_symbols}")
                    
                    # 订阅 allMids
                    await self._subscribe(ws)
                    
                    self._reset_reconnect_delay()
                    self._set_state(ConnectionState.CONNECTED)
                    
                    # 启动 ping 保活
                    self._ping_task = asyncio.create_task(self._ping_loop(ws))
                    
                    last_check = time.time()
                    
                    while self._running:
                        # 定期刷新 symbols 列表
                        if time.time() - last_check >= self.REFRESH_SYMBOLS_INTERVAL_S:
                            self._current_symbols = self._load_active_symbols()
                            last_check = time.time()
                            
                            # 没有持仓了，断开重连
                            if not self._current_symbols:
                                logger.info(f"[{self.uid}][hyperliquid] No active positions, disconnecting MarkPrice stream")
                                break
                        
                        try:
                            message = await asyncio.wait_for(ws.recv(), timeout=30.0)
                            await self._handle_message(message)
                        except asyncio.TimeoutError:
                            continue
                        
            except asyncio.CancelledError:
                break
            except Exception as e:
                error_msg = str(e)
                logger.error(f"[{self.uid}][hyperliquid] MarkPrice WebSocket error: {error_msg}")
                self._set_state(ConnectionState.ERROR, error_msg)
                
                if self._running:
                    delay = self._get_reconnect_delay()
                    logger.info(f"[{self.uid}][hyperliquid] MarkPrice reconnecting in {delay:.1f}s")
                    self._increase_reconnect_delay()
                    await asyncio.sleep(delay)
            finally:
                if self._ping_task and not self._ping_task.done():
                    self._ping_task.cancel()
                    try:
                        await self._ping_task
                    except asyncio.CancelledError:
                        pass
                self._ping_task = None
                self._ws = None
        
        self._set_state(ConnectionState.STOPPED)
    
    async def _subscribe(self, ws):
        """订阅 allMids 频道"""
        subscribe_msg = {
            "method": "subscribe",
            "subscription": {
                "type": "allMids"
            }
        }
        
        await ws.send(json.dumps(subscribe_msg))
        logger.info(f"[{self.uid}][hyperliquid] Subscribed to allMids")
    
    async def _handle_message(self, message):
        """处理标记价格消息"""
        try:
            if isinstance(message, bytes):
                message = message.decode('utf-8')
            
            if not message or not message.strip():
                return
            
            data = json.loads(message)
            
            channel = data.get("channel")
            
            # 订阅响应
            if channel == "subscriptionResponse":
                return
            
            # Pong
            if channel == "pong":
                return
            
            # allMids 数据
            if channel == "allMids":
                mids_data = data.get("data", {})
                mids = mids_data.get("mids", {})
                ts = int(time.time() * 1000)
                
                # 只处理有持仓的 symbols
                for hl_symbol in self._current_symbols:
                    if hl_symbol in mids:
                        try:
                            mark_price = float(mids[hl_symbol])
                            if self.on_tick and mark_price > 0:
                                # 转换回标准格式: BTC -> BTCUSDT
                                std_symbol = f"{hl_symbol}USDT"
                                self.on_tick(std_symbol, mark_price, ts)
                        except (ValueError, TypeError):
                            pass
                        
        except Exception as e:
            logger.debug(f"[{self.uid}][hyperliquid] MarkPrice message error: {e}")
    
    async def _ping_loop(self, ws):
        """Ping 保活循环"""
        try:
            while self._running:
                await asyncio.sleep(self.PING_INTERVAL)
                if not self._running or not ws:
                    break
                try:
                    is_closed = ws.closed if hasattr(ws, 'closed') else (ws.state.name == 'CLOSED')
                except Exception:
                    is_closed = True
                if is_closed:
                    break
                try:
                    ping_msg = {"method": "ping"}
                    await ws.send(json.dumps(ping_msg))
                    logger.debug(f"[{self.uid}][hyperliquid] MarkPrice ping sent")
                except Exception as e:
                    logger.debug(f"[{self.uid}][hyperliquid] MarkPrice ping error: {e}")
                    break
        except asyncio.CancelledError:
            pass
    
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


class HyperliquidWebSocket:
    """
    Hyperliquid WebSocket 管理器
    
    统一管理用户数据流和标记价格流
    """
    
    def __init__(
        self,
        wallet_address: str,
        redis_conn,
        uid: str,
        is_testnet: bool = False,
        callbacks: Dict[str, Callable] = None,
    ):
        self.wallet_address = wallet_address
        self.rds = redis_conn
        self.uid = uid
        self.is_testnet = is_testnet
        self.callbacks = callbacks or {}
        
        self._user_stream: Optional[HyperliquidUserStream] = None
        self._mark_price_stream: Optional[HyperliquidMarkPriceStream] = None
    
    def start(self):
        """启动 WebSocket 连接"""
        # 用户数据流
        self._user_stream = HyperliquidUserStream(
            wallet_address=self.wallet_address,
            is_testnet=self.is_testnet,
            uid=self.uid,
            on_fill=self.callbacks.get('on_fill'),
            on_funding=self.callbacks.get('on_funding'),
            on_liquidation=self.callbacks.get('on_liquidation'),
            on_state_change=self.callbacks.get('on_user_state_change'),
        )
        self._user_stream.start()
        
        # 标记价格流
        if self.callbacks.get('on_mark_price'):
            self._mark_price_stream = HyperliquidMarkPriceStream(
                redis_conn=self.rds,
                uid=self.uid,
                is_testnet=self.is_testnet,
                on_tick=self.callbacks.get('on_mark_price'),
                on_state_change=self.callbacks.get('on_mark_state_change'),
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
