# websocket/async_bitget_stream.py
"""
纯 asyncio 版本的 Bitget User Data Stream WebSocket

基于 async_base_stream.py 实现

特点：
1. 不使用 threading，完全基于 asyncio
2. 自动重连和心跳
3. 认证失败时不重试

Bitget WebSocket 文档: https://www.bitget.com/api-doc/contract/websocket/overview
"""

import asyncio
import base64
import hashlib
import hmac
import json
import logging
import time
from typing import Callable, Dict, Optional, Any

from websocket.async_base_stream import (
    AsyncWebSocketBase,
    ConnectionState,
    AuthError,
    WSConfig,
)

logger = logging.getLogger(__name__)


# Bitget 认证错误代码
BITGET_AUTH_ERROR_CODES = {
    "30001",  # Invalid signature
    "30002",  # Invalid apiKey
    "30003",  # Invalid timestamp
    "30004",  # Signature verification failed
    "30005",  # Invalid passphrase
    "30006",  # Invalid IP
    "30007",  # No permission
    "30008",  # User is frozen
    "30009",  # User does not exist
    "30010",  # API is disabled
}


class AsyncBitgetUserStream(AsyncWebSocketBase):
    """
    Bitget Private WebSocket Stream (纯 asyncio 版本)
    
    接收账户和持仓的实时更新
    """
    
    # 产品类型
    PRODUCT_TYPE = "USDT-FUTURES"
    INST_TYPE = "USDT-FUTURES"
    
    def __init__(
        self,
        api_key: str,
        api_secret: str,
        passphrase: str,
        is_testnet: bool = False,
        uid: str = "",
        on_account: Optional[Callable[[Dict], None]] = None,
        on_position: Optional[Callable[[Dict], None]] = None,
        on_order: Optional[Callable[[Dict], None]] = None,
        on_order_algo: Optional[Callable[[Dict], None]] = None,
        on_fill: Optional[Callable[[Dict], None]] = None,
        on_state_change: Optional[Callable[[ConnectionState, Optional[str]], None]] = None,
        config: Optional[WSConfig] = None,
    ):
        super().__init__(
            uid=uid,
            exchange="bitget",
            on_state_change=on_state_change,
            config=config or WSConfig(ping_interval=25.0),
        )
        
        self.api_key = api_key
        self.api_secret = api_secret
        self.passphrase = passphrase
        self.is_testnet = is_testnet
        
        self.on_account = on_account
        self.on_position = on_position
        self.on_order = on_order
        self.on_order_algo = on_order_algo
        self.on_fill = on_fill
        
        # WebSocket URL
        if is_testnet:
            self._ws_url = "wss://wspap.bitget.com/v2/ws/private"
        else:
            self._ws_url = "wss://ws.bitget.com/v2/ws/private"
    
    def _get_ws_url(self) -> str:
        return self._ws_url
    
    def _generate_signature(self, timestamp: str) -> str:
        """生成 Bitget 签名"""
        message = f"{timestamp}GET/user/verify"
        mac = hmac.new(
            self.api_secret.encode('utf-8'),
            message.encode('utf-8'),
            hashlib.sha256
        )
        return base64.b64encode(mac.digest()).decode('utf-8')
    
    async def _authenticate(self, ws) -> bool:
        """Bitget 认证"""
        timestamp = str(int(time.time()))
        sign = self._generate_signature(timestamp)
        
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
        
        # 等待认证响应
        try:
            response = await asyncio.wait_for(ws.recv(), timeout=10.0)
            data = json.loads(response)
            
            if data.get("event") == "login":
                if data.get("code") == "0" or data.get("code") == 0:
                    logger.info(f"[{self.uid}][bitget] Auth success")
                    return True
                else:
                    code = str(data.get("code", ""))
                    msg = data.get("msg", "Unknown error")
                    
                    if code in BITGET_AUTH_ERROR_CODES:
                        raise AuthError(f"Bitget auth failed: {code} - {msg}")
                    
                    logger.error(f"[{self.uid}][bitget] Auth failed: {code} - {msg}")
                    return False
            
            logger.warning(f"[{self.uid}][bitget] Unexpected auth response: {data}")
            return False
            
        except asyncio.TimeoutError:
            logger.error(f"[{self.uid}][bitget] Auth timeout")
            return False
    
    async def _subscribe(self, ws) -> bool:
        """订阅 Bitget 私有频道"""
        # 订阅账户、持仓、订单频道
        subscribe_msg = {
            "op": "subscribe",
            "args": [
                {"instType": self.INST_TYPE, "channel": "account", "coin": "default"},
                {"instType": self.INST_TYPE, "channel": "positions", "coin": "default"},
                {"instType": self.INST_TYPE, "channel": "orders", "coin": "default"},
                {"instType": self.INST_TYPE, "channel": "orders-algo", "coin": "default"},
                {"instType": self.INST_TYPE, "channel": "fill", "coin": "default"},
            ]
        }
        
        await ws.send(json.dumps(subscribe_msg))
        
        # 等待订阅确认
        subscribed_count = 0
        expected_channels = {"account", "positions", "orders", "orders-algo", "fill"}
        
        try:
            for _ in range(15):
                response = await asyncio.wait_for(ws.recv(), timeout=5.0)
                data = json.loads(response)
                
                if data.get("event") == "subscribe":
                    channel = data.get("arg", {}).get("channel")
                    if channel in expected_channels:
                        subscribed_count += 1
                        logger.debug(f"[{self.uid}][bitget] Subscribed to {channel}")
                
                if subscribed_count >= len(expected_channels):
                    break
                    
        except asyncio.TimeoutError:
            pass
        
        if subscribed_count > 0:
            logger.info(f"[{self.uid}][bitget] Subscribed to {subscribed_count} channels")
            return True
        
        logger.warning(f"[{self.uid}][bitget] No channels subscribed")
        return False
    
    async def _handle_message(self, data: Dict[str, Any]):
        """处理 Bitget 消息"""
        # 处理推送数据
        if "data" in data and "arg" in data:
            channel = data["arg"].get("channel")
            
            if channel == "account":
                if self.on_account:
                    for item in data.get("data", []):
                        try:
                            self.on_account(item)
                        except Exception as e:
                            logger.error(f"[{self.uid}][bitget] Account callback error: {e}")
            
            elif channel == "positions":
                if self.on_position:
                    for item in data.get("data", []):
                        try:
                            self.on_position(item)
                        except Exception as e:
                            logger.error(f"[{self.uid}][bitget] Position callback error: {e}")
            
            elif channel == "orders":
                if self.on_order:
                    for item in data.get("data", []):
                        try:
                            self.on_order(item)
                        except Exception as e:
                            logger.error(f"[{self.uid}][bitget] Order callback error: {e}")
            
            elif channel == "orders-algo":
                if self.on_order_algo:
                    for item in data.get("data", []):
                        try:
                            self.on_order_algo(item)
                        except Exception as e:
                            logger.error(f"[{self.uid}][bitget] Order-algo callback error: {e}")
            
            elif channel == "fill":
                if self.on_fill:
                    for item in data.get("data", []):
                        try:
                            self.on_fill(item)
                        except Exception as e:
                            logger.error(f"[{self.uid}][bitget] Fill callback error: {e}")
        
        # 处理 pong
        elif data.get("event") == "pong":
            pass
        
        # 处理错误
        elif data.get("event") == "error":
            code = str(data.get("code", ""))
            msg = data.get("msg", "Unknown error")
            logger.error(f"[{self.uid}][bitget] Error: {code} - {msg}")
            
            if code in BITGET_AUTH_ERROR_CODES:
                raise AuthError(f"Bitget error: {code} - {msg}")
