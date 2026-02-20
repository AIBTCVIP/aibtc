# exchanges/binance/websocket.py
"""
Binance WebSocket 数据流

包含:
- User Data Stream: 账户更新、订单更新
- Mark Price Stream: 标记价格更新
"""

import json
import logging
import threading
import time
from typing import Callable, Dict, Optional, Any

logger = logging.getLogger(__name__)


class BinanceUserStream:
    """
    Binance User Data Stream
    
    接收账户和订单的实时更新
    """
    
    # 重连配置
    RECONNECT_DELAY_BASE = 5      # 基础重连延迟（秒）
    RECONNECT_DELAY_MAX = 60      # 最大重连延迟（秒）
    KEEPALIVE_INTERVAL = 20 * 60  # Listen key keepalive 间隔（20分钟，Binance 要求 < 60分钟）
    
    def __init__(
        self,
        api_key: str,
        api_secret: str = "",
        is_testnet: bool = False,
        uid: str = "",
        on_message: Callable[[Dict], None] = None,
    ):
        self.api_key = api_key
        self.api_secret = api_secret
        self.is_testnet = is_testnet
        self.uid = uid
        self.on_message = on_message
        
        self._ws = None
        self._listen_key = None
        self._running = False
        self._reconnect_count = 0
        self._keepalive_thread = None
        self._keepalive_stop_event = threading.Event()
        self._client = None  # 保存客户端引用
        self._lock = threading.Lock()
        
        # WebSocket URL
        if is_testnet:
            self.ws_url = "wss://stream.binancefuture.com/ws"
        else:
            self.ws_url = "wss://fstream.binance.com/ws"
    
    def start(self):
        """启动 WebSocket 连接"""
        if self._running:
            return
        
        self._running = True
        self._reconnect_count = 0
        
        # 在后台线程中运行（使用循环而非递归）
        thread = threading.Thread(target=self._run_loop, daemon=True, name=f"binance-user-ws-{self.uid}")
        thread.start()
    
    def _run_loop(self):
        """WebSocket 运行循环（非递归）"""
        while self._running:
            try:
                self._connect_and_run()
            except Exception as e:
                logger.error(f"[{self.uid}][binance] WebSocket 运行异常: {e}")
            
            if self._running:
                # 计算重连延迟（指数退避）
                delay = min(
                    self.RECONNECT_DELAY_BASE * (2 ** min(self._reconnect_count, 5)),
                    self.RECONNECT_DELAY_MAX
                )
                self._reconnect_count += 1
                logger.info(f"[{self.uid}][binance] WebSocket 将在 {delay} 秒后重连 (第 {self._reconnect_count} 次)")
                time.sleep(delay)
    
    def _connect_and_run(self):
        """建立连接并运行"""
        import websocket
        from binance.client import Client
        from core.rate_limiter import get_binance_rate_limiter
        
        # 停止旧的 keepalive 线程
        self._stop_keepalive()
        
        # 创建客户端获取 listen key（带限速）
        self._client = Client(api_key=self.api_key, api_secret=self.api_secret, testnet=self.is_testnet)
        
        # 获取限速许可
        rate_limiter = get_binance_rate_limiter(self.api_key)
        rate_limiter.acquire(endpoint="futures_stream_get_listen_key", timeout=30.0)
        
        self._listen_key = self._client.futures_stream_get_listen_key()
        logger.info(f"[{self.uid}][binance] 获取 listen key 成功")
        
        # 启动 keepalive 线程
        self._start_keepalive()
        
        # 连接 WebSocket
        ws_url = f"{self.ws_url}/{self._listen_key}"
        
        def on_message(ws, message):
            try:
                if not message or not message.strip():
                    return
                data = json.loads(message)
                if self.on_message:
                    self.on_message(data)
            except Exception as e:
                logger.error(f"[{self.uid}][binance] WebSocket 消息处理错误: {e}")
        
        def on_error(ws, error):
            logger.error(f"[{self.uid}][binance] WebSocket 错误: {error}")
        
        def on_close(ws, close_status_code, close_msg):
            logger.warning(f"[{self.uid}][binance] WebSocket 已断开: code={close_status_code} msg={close_msg}")
        
        def on_open(ws):
            logger.info(f"[{self.uid}][binance] WebSocket 已连接")
            self._reconnect_count = 0  # 连接成功，重置重连计数
        
        self._ws = websocket.WebSocketApp(
            ws_url,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
            on_open=on_open
        )
        
        # 运行 WebSocket（阻塞直到断开）
        self._ws.run_forever(ping_interval=30, ping_timeout=10)
    
    def _start_keepalive(self):
        """启动 listen key 保活线程"""
        self._keepalive_stop_event.clear()
        
        def keepalive():
            from core.rate_limiter import get_binance_rate_limiter
            
            while not self._keepalive_stop_event.is_set():
                # 等待指定间隔或被停止
                if self._keepalive_stop_event.wait(self.KEEPALIVE_INTERVAL):
                    break  # 被停止了
                
                if not self._running or not self._listen_key:
                    break
                
                try:
                    with self._lock:
                        if self._client and self._listen_key:
                            # 获取限速许可
                            rate_limiter = get_binance_rate_limiter(self.api_key)
                            rate_limiter.acquire(endpoint="futures_stream_keepalive", timeout=30.0)
                            
                            self._client.futures_stream_keepalive(self._listen_key)
                            logger.debug(f"[{self.uid}][binance] Listen key keepalive 成功")
                except Exception as e:
                    logger.error(f"[{self.uid}][binance] Listen key keepalive 失败: {e}")
                    # Keepalive 失败，可能 listen key 已过期，需要重连
                    if self._ws:
                        self._ws.close()
                    break
        
        self._keepalive_thread = threading.Thread(target=keepalive, daemon=True, name=f"binance-keepalive-{self.uid}")
        self._keepalive_thread.start()
    
    def _stop_keepalive(self):
        """停止 keepalive 线程"""
        self._keepalive_stop_event.set()
        if self._keepalive_thread and self._keepalive_thread.is_alive():
            self._keepalive_thread.join(timeout=2)
        self._keepalive_thread = None
    
    def stop(self):
        """停止 WebSocket 连接"""
        logger.info(f"[{self.uid}][binance] 正在停止 WebSocket...")
        self._running = False
        self._stop_keepalive()
        if self._ws:
            self._ws.close()
            self._ws = None
        logger.info(f"[{self.uid}][binance] WebSocket 已停止")


class BinanceMarkPriceStream:
    """
    Binance Mark Price Stream
    
    接收所有交易对的标记价格更新
    """
    
    # 重连配置
    RECONNECT_DELAY_BASE = 5
    RECONNECT_DELAY_MAX = 60
    
    def __init__(
        self,
        is_testnet: bool = False,
        on_tick: Callable[[str, float, int], None] = None,
        uid: str = "",
    ):
        self.is_testnet = is_testnet
        self.on_tick = on_tick  # callback(symbol, mark_price, timestamp)
        self.uid = uid
        
        self._ws = None
        self._running = False
        self._reconnect_count = 0
        
        # WebSocket URL
        if is_testnet:
            self.ws_url = "wss://stream.binancefuture.com/ws/!markPrice@arr@1s"
        else:
            self.ws_url = "wss://fstream.binance.com/ws/!markPrice@arr@1s"
    
    def start(self):
        """启动 WebSocket 连接"""
        if self._running:
            return
        
        self._running = True
        self._reconnect_count = 0
        thread = threading.Thread(target=self._run_loop, daemon=True, name=f"binance-mark-price-ws-{self.uid}")
        thread.start()
    
    def _run_loop(self):
        """WebSocket 运行循环（非递归）"""
        import websocket
        
        while self._running:
            try:
                def on_message(ws, message):
                    try:
                        if not message or not message.strip():
                            return
                        data = json.loads(message)
                        if isinstance(data, list):
                            for item in data:
                                symbol = item.get('s')
                                mark_price = float(item.get('p', 0))
                                timestamp = int(item.get('E', 0))
                                if self.on_tick and symbol:
                                    self.on_tick(symbol, mark_price, timestamp)
                    except Exception as e:
                        logger.error(f"[{self.uid}][binance] Mark price 消息处理错误: {e}")
                
                def on_error(ws, error):
                    logger.error(f"[{self.uid}][binance] Mark price WebSocket 错误: {error}")
                
                def on_close(ws, close_status_code, close_msg):
                    logger.warning(f"[{self.uid}][binance] Mark price WebSocket 已断开: code={close_status_code}")
                
                def on_open(ws):
                    logger.info(f"[{self.uid}][binance] Mark price WebSocket 已连接")
                    self._reconnect_count = 0
                
                self._ws = websocket.WebSocketApp(
                    self.ws_url,
                    on_message=on_message,
                    on_error=on_error,
                    on_close=on_close,
                    on_open=on_open
                )
                
                self._ws.run_forever(ping_interval=30, ping_timeout=10)
                
            except Exception as e:
                logger.error(f"[{self.uid}][binance] Mark price WebSocket 异常: {e}")
            
            if self._running:
                delay = min(
                    self.RECONNECT_DELAY_BASE * (2 ** min(self._reconnect_count, 5)),
                    self.RECONNECT_DELAY_MAX
                )
                self._reconnect_count += 1
                logger.info(f"[{self.uid}][binance] Mark price WebSocket 将在 {delay} 秒后重连")
                time.sleep(delay)
    
    def stop(self):
        """停止 WebSocket 连接"""
        self._running = False
        if self._ws:
            self._ws.close()
            self._ws = None


class BinanceWebSocket:
    """
    Binance WebSocket 管理器
    
    统一管理 User Stream 和 Mark Price Stream
    """
    
    def __init__(
        self,
        api_key: str,
        api_secret: str = "",
        is_testnet: bool = False,
        uid: str = "",
        callbacks: Dict[str, Callable] = None,
    ):
        self.api_key = api_key
        self.api_secret = api_secret
        self.is_testnet = is_testnet
        self.uid = uid
        self.callbacks = callbacks or {}
        
        self._user_stream: Optional[BinanceUserStream] = None
        self._mark_price_stream: Optional[BinanceMarkPriceStream] = None
    
    def start(self):
        """启动所有 WebSocket 连接"""
        logger.info(f"[{self.uid}][binance] 启动 WebSocket 连接...")
        
        # User Stream
        on_user_data = self.callbacks.get('on_user_data')
        if on_user_data:
            self._user_stream = BinanceUserStream(
                api_key=self.api_key,
                api_secret=self.api_secret,
                is_testnet=self.is_testnet,
                uid=self.uid,
                on_message=on_user_data
            )
            self._user_stream.start()
        
        # Mark Price Stream (如果有回调)
        on_mark_price = self.callbacks.get('on_mark_price')
        if on_mark_price:
            self._mark_price_stream = BinanceMarkPriceStream(
                is_testnet=self.is_testnet,
                on_tick=on_mark_price,
                uid=self.uid
            )
            self._mark_price_stream.start()
    
    def stop(self):
        """停止所有 WebSocket 连接"""
        logger.info(f"[{self.uid}][binance] 停止 WebSocket 连接...")
        
        if self._user_stream:
            self._user_stream.stop()
            self._user_stream = None
        
        if self._mark_price_stream:
            self._mark_price_stream.stop()
            self._mark_price_stream = None
        
        logger.info(f"[{self.uid}][binance] WebSocket 已全部停止")
