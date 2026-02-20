# websocket/async_okx_stream.py
"""
纯 asyncio 版本的 OKX User Data Stream WebSocket

基于 async_base_stream.py 实现

特点：
1. 不使用 threading，完全基于 asyncio
2. 自动重连和心跳
3. 认证失败时不重试

OKX WebSocket 文档: https://www.okx.com/docs-v5/en/#websocket-api
"""

import asyncio
import base64
import hashlib
import hmac
import json
import logging
import time
from datetime import datetime, timezone
from typing import Callable, Dict, Optional, Any

from websocket.async_base_stream import (
    AsyncWebSocketBase,
    ConnectionState,
    AuthError,
    WSConfig,
)

logger = logging.getLogger(__name__)


# OKX 认证错误代码
OKX_AUTH_ERROR_CODES = {
    "60001",  # Invalid sign
    "60002",  # Invalid API Key
    "60003",  # Invalid Request Path
    "60004",  # Invalid Timestamp
    "60005",  # Invalid OK-ACCESS-KEY
    "60006",  # Invalid OK-ACCESS-PASSPHRASE
    "60007",  # Invalid OK-ACCESS-TIMESTAMP
    "60008",  # Invalid OK-ACCESS-SIGN
    "60009",  # Invalid Request Body
    "60010",  # Invalid Request
    "60011",  # User is not in whitelist
    "60012",  # Login failed
    "60014",  # Requests too frequent
}


class AsyncOKXUserStream(AsyncWebSocketBase):
    """
    OKX Private WebSocket Stream (纯 asyncio 版本)
    
    接收账户和持仓的实时更新
    """
    
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
        on_fill: Optional[Callable[[Dict], None]] = None,
        on_state_change: Optional[Callable[[ConnectionState, Optional[str]], None]] = None,
        config: Optional[WSConfig] = None,
    ):
        super().__init__(
            uid=uid,
            exchange="okx",
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
        self.on_fill = on_fill
        
        # WebSocket URL
        if is_testnet:
            self._ws_url = "wss://wspap.okx.com:8443/ws/v5/private?brokerId=9999"
        else:
            self._ws_url = "wss://ws.okx.com:8443/ws/v5/private"
    
    def _get_ws_url(self) -> str:
        return self._ws_url
    
    def _generate_signature(self, timestamp: str) -> str:
        """生成 OKX 签名"""
        message = f"{timestamp}GET/users/self/verify"
        mac = hmac.new(
            self.api_secret.encode('utf-8'),
            message.encode('utf-8'),
            hashlib.sha256
        )
        return base64.b64encode(mac.digest()).decode('utf-8')
    
    async def _authenticate(self, ws) -> bool:
        """OKX 认证"""
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
                if data.get("code") == "0":
                    logger.info(f"[{self.uid}][okx] Auth success")
                    return True
                else:
                    code = data.get("code", "")
                    msg = data.get("msg", "Unknown error")
                    
                    if code in OKX_AUTH_ERROR_CODES:
                        raise AuthError(f"OKX auth failed: {code} - {msg}")
                    
                    logger.error(f"[{self.uid}][okx] Auth failed: {code} - {msg}")
                    return False
            
            logger.warning(f"[{self.uid}][okx] Unexpected auth response: {data}")
            return False
            
        except asyncio.TimeoutError:
            logger.error(f"[{self.uid}][okx] Auth timeout")
            return False
    
    async def _subscribe(self, ws) -> bool:
        """订阅 OKX 私有频道"""
        # 订阅账户、持仓、订单频道
        subscribe_msg = {
            "op": "subscribe",
            "args": [
                {"channel": "account", "ccy": "USDT"},
                {"channel": "positions", "instType": "SWAP"},
                {"channel": "orders", "instType": "SWAP"},
                {"channel": "fills", "instType": "SWAP"},
            ]
        }
        
        await ws.send(json.dumps(subscribe_msg))
        
        # 等待订阅确认（可能收到多个响应）
        subscribed_count = 0
        expected_channels = {"account", "positions", "orders", "fills"}
        
        try:
            for _ in range(10):  # 最多等待 10 个响应
                response = await asyncio.wait_for(ws.recv(), timeout=5.0)
                data = json.loads(response)
                
                if data.get("event") == "subscribe":
                    channel = data.get("arg", {}).get("channel")
                    if channel in expected_channels:
                        subscribed_count += 1
                        logger.debug(f"[{self.uid}][okx] Subscribed to {channel}")
                
                if subscribed_count >= len(expected_channels):
                    break
                    
        except asyncio.TimeoutError:
            pass
        
        if subscribed_count > 0:
            logger.info(f"[{self.uid}][okx] Subscribed to {subscribed_count} channels")
            return True
        
        logger.warning(f"[{self.uid}][okx] No channels subscribed")
        return False
    
    async def _handle_message(self, data: Dict[str, Any]):
        """处理 OKX 消息"""
        # 处理推送数据
        if "data" in data and "arg" in data:
            channel = data["arg"].get("channel")
            
            if channel == "account":
                if self.on_account:
                    for item in data.get("data", []):
                        try:
                            self.on_account(item)
                        except Exception as e:
                            logger.error(f"[{self.uid}][okx] Account callback error: {e}")
            
            elif channel == "positions":
                if self.on_position:
                    for item in data.get("data", []):
                        try:
                            self.on_position(item)
                        except Exception as e:
                            logger.error(f"[{self.uid}][okx] Position callback error: {e}")
            
            elif channel == "orders":
                if self.on_order:
                    for item in data.get("data", []):
                        try:
                            self.on_order(item)
                        except Exception as e:
                            logger.error(f"[{self.uid}][okx] Order callback error: {e}")
            
            elif channel == "fills":
                if self.on_fill:
                    for item in data.get("data", []):
                        try:
                            self.on_fill(item)
                        except Exception as e:
                            logger.error(f"[{self.uid}][okx] Fill callback error: {e}")
        
        # 处理 pong
        elif data.get("event") == "pong":
            pass
        
        # 处理错误
        elif data.get("event") == "error":
            code = data.get("code", "")
            msg = data.get("msg", "Unknown error")
            logger.error(f"[{self.uid}][okx] Error: {code} - {msg}")
            
            if code in OKX_AUTH_ERROR_CODES:
                raise AuthError(f"OKX error: {code} - {msg}")
