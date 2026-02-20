# exchanges/binance/client.py
"""
Binance API 客户端

封装 python-binance 库，提供统一的交易接口
"""

import asyncio
import logging
import math
import time
import uuid
from typing import Dict, List, Optional, Any

from binance.client import Client
from binance.exceptions import BinanceAPIException

from exchanges.base import (
    BaseExchange,
    OrderResult,
    Position,
    AccountInfo,
)

logger = logging.getLogger(__name__)


class BinanceClient(BaseExchange):
    """
    Binance 交易所客户端
    
    使用 python-binance 库实现
    """
    
    EXCHANGE_NAME = "binance"
    
    # API 端点
    MAINNET_API = "https://fapi.binance.com"
    TESTNET_API = "https://testnet.binancefuture.com"
    
    # Broker ID 前缀 (用于标识订单来源)
    # 格式: x-f5AYjh5a + 唯一后缀，总长度不超过36字符
    BROKER_ID_PREFIX = "x-f5AYjh5a"
    
    def __init__(
        self,
        api_key: str,
        api_secret: str,
        is_testnet: bool = False,
        **kwargs
    ):
        super().__init__(api_key, api_secret, is_testnet=is_testnet, **kwargs)
        
        # 创建 Binance 客户端
        self._client = Client(
            api_key=api_key,
            api_secret=api_secret,
            testnet=is_testnet
        )
        
        # 缓存交易对信息
        self._symbol_info_cache: Dict[str, Dict] = {}
        self._cache_time = 0
        self._cache_ttl = 3600  # 1小时缓存
    
    @property
    def client(self) -> Client:
        """获取底层 Binance 客户端"""
        return self._client
    
    async def _async_call(self, method_name: str, *args, **kwargs):
        """将同步方法异步化（带 API Key 级别限速）"""
        from core.rate_limiter import get_binance_rate_limiter
        
        # 使用 API Key 级别限速器
        rate_limiter = get_binance_rate_limiter(self.api_key)
        if not await rate_limiter.acquire_async(endpoint=method_name, timeout=30.0):
            raise Exception(f"Binance API 限速器超时: {method_name}")
        
        method = getattr(self._client, method_name)
        return await asyncio.to_thread(method, *args, **kwargs)
    
    def _generate_client_order_id(self) -> str:
        # 生成唯一后缀: 时间戳(10位) + 随机字符(最多16位)
        timestamp = str(int(time.time()))[-10:]  # 取时间戳后10位
        random_suffix = uuid.uuid4().hex[:16]    # 取UUID前16位
        unique_suffix = f"{timestamp}{random_suffix}"
        
        # 组合: 前缀(10) + 后缀(26) = 36字符
        client_order_id = f"{self.BROKER_ID_PREFIX}{unique_suffix}"
        
        # 确保不超过36字符
        return client_order_id[:36]
    
    # ==================== 账户相关 ====================
    
    async def get_account(self) -> AccountInfo:
        """获取账户信息"""
        data = await self._async_call('futures_account')
        
        positions = []
        for p in data.get('positions', []):
            amt = float(p.get('positionAmt', 0))
            if amt == 0:
                continue
            
            positions.append(Position(
                symbol=p.get('symbol'),
                side='LONG' if amt > 0 else 'SHORT',
                qty=abs(amt),
                entry_price=float(p.get('entryPrice', 0)),
                unrealized_pnl=float(p.get('unrealizedProfit', 0)),
                leverage=int(p.get('leverage', 1)),
                margin_type=p.get('marginType', 'cross'),
                liquidation_price=float(p.get('liquidationPrice', 0)),
                exchange=self.EXCHANGE_NAME,
                raw=p
            ))
        
        return AccountInfo(
            total_balance=float(data.get('totalWalletBalance', 0)),
            available_balance=float(data.get('availableBalance', 0)),
            unrealized_pnl=float(data.get('totalUnrealizedProfit', 0)),
            margin_balance=float(data.get('totalMarginBalance', 0)),
            positions=positions,
            exchange=self.EXCHANGE_NAME,
            raw=data
        )
    
    async def get_balance(self) -> float:
        """获取可用余额"""
        data = await self._async_call('futures_account_balance')
        for asset in data:
            if asset.get('asset') == 'USDT':
                return float(asset.get('availableBalance', 0))
        return 0.0
    
    async def get_positions(self) -> List[Position]:
        """获取所有持仓"""
        account = await self.get_account()
        return account.positions or []
    
    async def get_position(self, symbol: str) -> Optional[Position]:
        """获取指定币种的持仓"""
        positions = await self.get_positions()
        for p in positions:
            if p.symbol == symbol:
                return p
        return None
    
    # ==================== 下单相关 ====================
    
    async def market_open(
        self,
        symbol: str,
        side: str,
        qty: float,
        leverage: int = None,
    ) -> OrderResult:
        """市价开仓"""
        try:
            # 设置杠杆
            if leverage:
                await self.set_leverage(symbol, leverage)
            
            # 确定订单方向
            order_side = 'BUY' if side.upper() == 'LONG' else 'SELL'
            position_side = side.upper()
            
            # 格式化数量
            qty = await self._format_quantity(symbol, qty)
            
            result = await self._async_call(
                'futures_create_order',
                symbol=symbol,
                side=order_side,
                positionSide=position_side,
                type='MARKET',
                quantity=qty,
                newClientOrderId=self._generate_client_order_id()
            )
            
            return OrderResult(
                success=True,
                order_id=str(result.get('orderId')),
                symbol=symbol,
                side=order_side,
                position_side=position_side,
                qty=float(result.get('executedQty', 0)),
                avg_price=float(result.get('avgPrice', 0)),
                status=result.get('status'),
                raw=result
            )
            
        except BinanceAPIException as e:
            logger.error(f"[binance] market_open 失败: {e}")
            return OrderResult(
                success=False,
                error=str(e),
                symbol=symbol
            )
        except Exception as e:
            logger.error(f"[binance] market_open 异常: {e}")
            return OrderResult(
                success=False,
                error=str(e),
                symbol=symbol
            )
    
    async def market_close(
        self,
        symbol: str,
        side: str,
        qty: float = None,
    ) -> OrderResult:
        """市价平仓"""
        try:
            # 平仓方向与开仓相反
            order_side = 'SELL' if side.upper() == 'LONG' else 'BUY'
            position_side = side.upper()
            
            # 如果没有指定数量，获取当前持仓数量
            if qty is None:
                position = await self.get_position(symbol)
                if not position or position.side != position_side:
                    return OrderResult(
                        success=False,
                        error=f"没有 {symbol} {position_side} 持仓",
                        symbol=symbol
                    )
                qty = position.qty
            
            # 格式化数量
            qty = await self._format_quantity(symbol, qty)
            
            result = await self._async_call(
                'futures_create_order',
                symbol=symbol,
                side=order_side,
                positionSide=position_side,
                type='MARKET',
                quantity=qty,
                newClientOrderId=self._generate_client_order_id()
            )
            
            return OrderResult(
                success=True,
                order_id=str(result.get('orderId')),
                symbol=symbol,
                side=order_side,
                position_side=position_side,
                qty=float(result.get('executedQty', 0)),
                avg_price=float(result.get('avgPrice', 0)),
                status=result.get('status'),
                raw=result
            )
            
        except BinanceAPIException as e:
            logger.error(f"[binance] market_close 失败: {e}")
            return OrderResult(
                success=False,
                error=str(e),
                symbol=symbol
            )
        except Exception as e:
            logger.error(f"[binance] market_close 异常: {e}")
            return OrderResult(
                success=False,
                error=str(e),
                symbol=symbol
            )
    
    async def limit_open(
        self,
        symbol: str,
        side: str,
        qty: float,
        price: float,
        leverage: int = None,
    ) -> OrderResult:
        """限价开仓 (GTC - 持续有效直到手动取消)"""
        try:
            # 设置杠杆
            if leverage:
                await self.set_leverage(symbol, leverage)
            
            order_side = 'BUY' if side.upper() == 'LONG' else 'SELL'
            position_side = side.upper()
            
            # 格式化数量和价格
            qty = await self._format_quantity(symbol, qty)
            price = await self._format_price(symbol, price)
            
            # 构建订单参数
            order_params = {
                'symbol': symbol,
                'side': order_side,
                'positionSide': position_side,
                'type': 'LIMIT',
                'timeInForce': 'GTC',
                'quantity': qty,
                'price': price,
                'newClientOrderId': self._generate_client_order_id(),
            }
            
            result = await self._async_call('futures_create_order', **order_params)
            
            return OrderResult(
                success=True,
                order_id=str(result.get('orderId')),
                symbol=symbol,
                side=order_side,
                position_side=position_side,
                qty=float(result.get('origQty', 0)),
                price=float(result.get('price', 0)),
                status=result.get('status'),
                raw=result
            )
            
        except BinanceAPIException as e:
            logger.error(f"[binance] limit_open 失败: {e}")
            return OrderResult(success=False, error=str(e), symbol=symbol)
        except Exception as e:
            logger.error(f"[binance] limit_open 异常: {e}")
            return OrderResult(success=False, error=str(e), symbol=symbol)
    
    async def set_stop_loss(
        self,
        symbol: str,
        side: str,
        qty: float,
        stop_price: float,
    ) -> OrderResult:
        """设置止损"""
        try:
            # 止损方向与持仓相反
            order_side = 'SELL' if side.upper() == 'LONG' else 'BUY'
            position_side = side.upper()
            
            qty = await self._format_quantity(symbol, qty)
            stop_price = await self._format_price(symbol, stop_price)
            
            result = await self._async_call(
                'futures_create_order',
                symbol=symbol,
                side=order_side,
                positionSide=position_side,
                type='STOP_MARKET',
                stopPrice=stop_price,
                quantity=qty,
                reduceOnly=True,
                newClientOrderId=self._generate_client_order_id()
            )
            
            return OrderResult(
                success=True,
                order_id=str(result.get('orderId')),
                symbol=symbol,
                side=order_side,
                position_side=position_side,
                qty=float(result.get('origQty', 0)),
                price=stop_price,
                status=result.get('status'),
                raw=result
            )
            
        except BinanceAPIException as e:
            logger.error(f"[binance] set_stop_loss 失败: {e}")
            return OrderResult(success=False, error=str(e), symbol=symbol)
        except Exception as e:
            logger.error(f"[binance] set_stop_loss 异常: {e}")
            return OrderResult(success=False, error=str(e), symbol=symbol)
    
    async def set_take_profit(
        self,
        symbol: str,
        side: str,
        qty: float,
        tp_price: float,
    ) -> OrderResult:
        """设置止盈"""
        try:
            order_side = 'SELL' if side.upper() == 'LONG' else 'BUY'
            position_side = side.upper()
            
            qty = await self._format_quantity(symbol, qty)
            tp_price = await self._format_price(symbol, tp_price)
            
            result = await self._async_call(
                'futures_create_order',
                symbol=symbol,
                side=order_side,
                positionSide=position_side,
                type='TAKE_PROFIT_MARKET',
                stopPrice=tp_price,
                quantity=qty,
                reduceOnly=True,
                newClientOrderId=self._generate_client_order_id()
            )
            
            return OrderResult(
                success=True,
                order_id=str(result.get('orderId')),
                symbol=symbol,
                side=order_side,
                position_side=position_side,
                qty=float(result.get('origQty', 0)),
                price=tp_price,
                status=result.get('status'),
                raw=result
            )
            
        except BinanceAPIException as e:
            logger.error(f"[binance] set_take_profit 失败: {e}")
            return OrderResult(success=False, error=str(e), symbol=symbol)
        except Exception as e:
            logger.error(f"[binance] set_take_profit 异常: {e}")
            return OrderResult(success=False, error=str(e), symbol=symbol)
    
    async def cancel_order(self, symbol: str, order_id: str) -> bool:
        """取消订单"""
        try:
            await self._async_call(
                'futures_cancel_order',
                symbol=symbol,
                orderId=int(order_id)
            )
            return True
        except Exception as e:
            logger.error(f"[binance] cancel_order 失败: {e}")
            return False
    
    async def cancel_all_orders(self, symbol: str) -> int:
        """取消指定币种的所有订单"""
        try:
            result = await self._async_call(
                'futures_cancel_all_open_orders',
                symbol=symbol
            )
            return len(result) if isinstance(result, list) else 1
        except Exception as e:
            logger.error(f"[binance] cancel_all_orders 失败: {e}")
            return 0
    
    # ==================== 设置相关 ====================
    
    async def set_leverage(self, symbol: str, leverage: int) -> bool:
        """设置杠杆"""
        try:
            await self._async_call(
                'futures_change_leverage',
                symbol=symbol,
                leverage=leverage
            )
            return True
        except BinanceAPIException as e:
            # -4028: 杠杆已经是这个值了
            if e.code == -4028:
                return True
            logger.error(f"[binance] set_leverage 失败: {e}")
            return False
        except Exception as e:
            logger.error(f"[binance] set_leverage 异常: {e}")
            return False
    
    async def set_margin_type(self, symbol: str, margin_type: str) -> bool:
        """设置保证金模式"""
        try:
            await self._async_call(
                'futures_change_margin_type',
                symbol=symbol,
                marginType=margin_type.upper()
            )
            return True
        except BinanceAPIException as e:
            # -4046: 保证金类型已经是这个了
            if e.code == -4046:
                return True
            logger.error(f"[binance] set_margin_type 失败: {e}")
            return False
        except Exception as e:
            logger.error(f"[binance] set_margin_type 异常: {e}")
            return False
    
    # ==================== 市场数据 ====================
    
    async def get_ticker_price(self, symbol: str) -> float:
        """
        获取最新标记价格
        
        优先从 Redis 获取 WebSocket 推送的数据，避免 REST API 调用
        """
        try:
            # 1. 优先从 Redis 获取 WebSocket 数据
            from core.binance_public_cache import get_binance_public_cache
            cache = get_binance_public_cache()
            mark_price = cache.get_mark_price(symbol)
            if mark_price and mark_price > 0:
                return mark_price
            
            # 2. 回退到 REST API（会消耗限速配额）
            logger.debug(f"[binance] Redis 无标记价格，使用 REST API: {symbol}")
            result = await self._async_call('futures_symbol_ticker', symbol=symbol)
            return float(result.get('price', 0))
        except Exception as e:
            logger.error(f"[binance] get_ticker_price 失败: {e}")
            return 0.0
    
    async def get_symbol_info(self, symbol: str) -> Dict:
        """
        获取交易对信息
        
        使用全局公共缓存，所有用户共享
        """
        from core.binance_public_cache import get_binance_public_cache
        cache = get_binance_public_cache()
        return await cache.get_symbol_info_async(symbol)
    
    async def _ensure_symbol_cache(self):
        """确保交易对缓存有效（已废弃，使用公共缓存）"""
        # 保留方法以兼容旧代码，但实际使用公共缓存
        pass
    
    async def _format_quantity(self, symbol: str, qty: float) -> float:
        """格式化数量精度"""
        info = await self.get_symbol_info(symbol)
        if not info:
            return qty
        
        for f in info.get('filters', []):
            if f['filterType'] == 'LOT_SIZE':
                step_size = float(f['stepSize'])
                precision = int(round(-math.log10(step_size)))
                return round(qty, precision)
        return qty
    
    async def _format_price(self, symbol: str, price: float) -> float:
        """格式化价格精度"""
        info = await self.get_symbol_info(symbol)
        if not info:
            return price
        
        for f in info.get('filters', []):
            if f['filterType'] == 'PRICE_FILTER':
                tick_size = float(f['tickSize'])
                precision = int(round(-math.log10(tick_size)))
                return round(price, precision)
        return price
    
    # ==================== WebSocket ====================
    
    def create_ws_client(self, uid: str = "", callbacks: Dict[str, callable] = None) -> Any:
        """创建 WebSocket 客户端"""
        from exchanges.binance.websocket import BinanceWebSocket
        return BinanceWebSocket(
            api_key=self.api_key,
            api_secret=self.api_secret,
            is_testnet=self.is_testnet,
            uid=uid,
            callbacks=callbacks
        )
    
    async def close(self):
        """关闭连接"""
        pass
