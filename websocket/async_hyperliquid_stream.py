# websocket/async_hyperliquid_stream.py
"""
纯 asyncio 版本的 Hyperliquid User Events WebSocket

基于 async_base_stream.py 实现

特点：
1. 不使用 threading，完全基于 asyncio
2. 自动重连和心跳
3. 订阅 userEvents 获取 fills、funding、liquidation

Hyperliquid WebSocket 文档: https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket
"""

import asyncio
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


class AsyncHyperliquidUserStream(AsyncWebSocketBase):
    """
    Hyperliquid User Events WebSocket Stream (纯 asyncio 版本)
    
    接收用户事件的实时推送:
    - fills: 成交明细
    - funding: 资金费率
    - liquidation: 清算
    - nonUserCancel: 非用户取消
    """
    
    def __init__(
        self,
        wallet_address: str,
        is_testnet: bool = False,
        uid: str = "",
        on_fill: Optional[Callable[[Dict], None]] = None,
        on_funding: Optional[Callable[[Dict], None]] = None,
        on_liquidation: Optional[Callable[[Dict], None]] = None,
        on_state_change: Optional[Callable[[ConnectionState, Optional[str]], None]] = None,
        config: Optional[WSConfig] = None,
    ):
        super().__init__(
            uid=uid,
            exchange="hyperliquid",
            on_state_change=on_state_change,
            config=config or WSConfig(ping_interval=30.0),  # Hyperliquid 要求 50s 内心跳
        )
        
        self.wallet_address = wallet_address
        self.is_testnet = is_testnet
        
        self.on_fill = on_fill
        self.on_funding = on_funding
        self.on_liquidation = on_liquidation
        
        # WebSocket URL
        if is_testnet:
            self._ws_url = "wss://api.hyperliquid-testnet.xyz/ws"
        else:
            self._ws_url = "wss://api.hyperliquid.xyz/ws"
    
    def _get_ws_url(self) -> str:
        return self._ws_url
    
    async def _authenticate(self, ws) -> bool:
        """
        Hyperliquid 不需要签名认证
        只需要钱包地址订阅 userEvents
        """
        # 直接返回成功，认证在订阅时进行
        return True
    
    async def _subscribe(self, ws) -> bool:
        """订阅 Hyperliquid userEvents"""
        if not self.wallet_address:
            logger.error(f"[{self.uid}][hyperliquid] No wallet address")
            raise AuthError("No wallet address configured")
        
        subscribe_msg = {
            "method": "subscribe",
            "subscription": {
                "type": "userEvents",
                "user": self.wallet_address
            }
        }
        
        await ws.send(json.dumps(subscribe_msg))
        
        # 等待订阅确认
        try:
            response = await asyncio.wait_for(ws.recv(), timeout=10.0)
            data = json.loads(response)
            
            # Hyperliquid 返回 {"channel": "subscriptionResponse", "data": {"method": "subscribe", "subscription": {...}}}
            if data.get("channel") == "subscriptionResponse":
                logger.info(f"[{self.uid}][hyperliquid] Subscribed to userEvents")
                return True
            
            # 也可能直接返回数据
            if data.get("channel") == "userEvents":
                logger.info(f"[{self.uid}][hyperliquid] Subscribed (got userEvents)")
                # 处理这条消息
                await self._handle_message(data)
                return True
            
            logger.warning(f"[{self.uid}][hyperliquid] Unexpected subscribe response: {data}")
            return True  # 仍然认为成功，可能是服务端实现差异
            
        except asyncio.TimeoutError:
            logger.warning(f"[{self.uid}][hyperliquid] Subscribe timeout, continuing...")
            return True  # Hyperliquid 可能不返回确认
    
    async def _handle_message(self, data: Dict[str, Any]):
        """处理 Hyperliquid 消息"""
        channel = data.get("channel")
        
        if channel == "userEvents":
            events = data.get("data", {})
            
            # 处理 fills
            fills = events.get("fills", [])
            if fills and self.on_fill:
                for fill in fills:
                    try:
                        self.on_fill(fill)
                    except Exception as e:
                        logger.error(f"[{self.uid}][hyperliquid] Fill callback error: {e}")
            
            # 处理 funding
            funding = events.get("funding", {})
            if funding and self.on_funding:
                try:
                    self.on_funding(funding)
                except Exception as e:
                    logger.error(f"[{self.uid}][hyperliquid] Funding callback error: {e}")
            
            # 处理 liquidation
            liquidation = events.get("liquidation")
            if liquidation and self.on_liquidation:
                try:
                    self.on_liquidation(liquidation)
                except Exception as e:
                    logger.error(f"[{self.uid}][hyperliquid] Liquidation callback error: {e}")
        
        elif channel == "pong":
            pass
        
        elif channel == "error":
            error_msg = data.get("data", "Unknown error")
            logger.error(f"[{self.uid}][hyperliquid] Error: {error_msg}")
    
    async def _ping_loop(self):
        """
        Hyperliquid 心跳循环
        
        发送 {"method": "ping"}
        """
        while self._running and self._ws:
            try:
                await asyncio.sleep(self.config.ping_interval)
                if self._ws and not self._ws.closed:
                    await self._ws.send(json.dumps({"method": "ping"}))
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.debug(f"[{self.uid}][hyperliquid] Ping error: {e}")


class AsyncHyperliquidMarkPriceStream(AsyncWebSocketBase):
    """
    Hyperliquid Mark Price Stream (纯 asyncio 版本)
    
    订阅 allMids 获取所有币种的中间价（作为 mark price）
    """
    
    def __init__(
        self,
        is_testnet: bool = False,
        uid: str = "",
        on_price: Optional[Callable[[Dict[str, float]], None]] = None,
        on_state_change: Optional[Callable[[ConnectionState, Optional[str]], None]] = None,
        config: Optional[WSConfig] = None,
    ):
        super().__init__(
            uid=uid,
            exchange="hyperliquid-price",
            on_state_change=on_state_change,
            config=config or WSConfig(ping_interval=30.0),
        )
        
        self.is_testnet = is_testnet
        self.on_price = on_price
        
        # WebSocket URL
        if is_testnet:
            self._ws_url = "wss://api.hyperliquid-testnet.xyz/ws"
        else:
            self._ws_url = "wss://api.hyperliquid.xyz/ws"
    
    def _get_ws_url(self) -> str:
        return self._ws_url
    
    async def _authenticate(self, ws) -> bool:
        """allMids 是公共频道，不需要认证"""
        return True
    
    async def _subscribe(self, ws) -> bool:
        """订阅 allMids"""
        subscribe_msg = {
            "method": "subscribe",
            "subscription": {
                "type": "allMids"
            }
        }
        
        await ws.send(json.dumps(subscribe_msg))
        
        try:
            response = await asyncio.wait_for(ws.recv(), timeout=10.0)
            data = json.loads(response)
            
            if data.get("channel") in ("subscriptionResponse", "allMids"):
                logger.info(f"[{self.uid}][hyperliquid] Subscribed to allMids")
                if data.get("channel") == "allMids":
                    await self._handle_message(data)
                return True
            
            logger.warning(f"[{self.uid}][hyperliquid] Unexpected response: {data}")
            return True
            
        except asyncio.TimeoutError:
            return True
    
    async def _handle_message(self, data: Dict[str, Any]):
        """处理 allMids 消息"""
        if data.get("channel") == "allMids":
            mids = data.get("data", {}).get("mids", {})
            
            if mids and self.on_price:
                # 转换为 {coin: price} 格式
                prices = {}
                for coin, price_str in mids.items():
                    try:
                        prices[coin] = float(price_str)
                    except (ValueError, TypeError):
                        pass
                
                if prices:
                    try:
                        self.on_price(prices)
                    except Exception as e:
                        logger.error(f"[{self.uid}][hyperliquid] Price callback error: {e}")
    
    async def _ping_loop(self):
        """Hyperliquid 心跳"""
        while self._running and self._ws:
            try:
                await asyncio.sleep(self.config.ping_interval)
                if self._ws and not self._ws.closed:
                    await self._ws.send(json.dumps({"method": "ping"}))
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.debug(f"[{self.uid}][hyperliquid] Ping error: {e}")
