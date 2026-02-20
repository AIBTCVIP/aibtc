# exchanges/binance_exchange.py
"""
Binance 交易所实现

基于现有 trading/trader.py 的逻辑封装
"""

import asyncio
import logging
import math
import time
import uuid
from decimal import Decimal, ROUND_HALF_UP
from typing import Dict, List, Optional, Any

from binance.client import Client
from binance.exceptions import BinanceAPIException

from exchanges.base import (
    BaseExchange, 
    OrderResult, 
    Position, 
    AccountInfo,
    OrderSide,
    PositionSide,
)

logger = logging.getLogger(__name__)


class BinanceExchange(BaseExchange):
    """
    Binance 交易所实现
    
    使用 python-binance 库
    """
    
    EXCHANGE_NAME = "binance"
    
    # API 端点
    MAINNET_API = "https://fapi.binance.com"
    TESTNET_API = "https://testnet.binancefuture.com"
    
    # Broker ID 前缀 (用于标识订单来源)
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
        timestamp = str(int(time.time()))[-10:]
        random_suffix = uuid.uuid4().hex[:16]
        unique_suffix = f"{timestamp}{random_suffix}"
        client_order_id = f"{self.BROKER_ID_PREFIX}{unique_suffix}"
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
        return account.positions
    
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
            # 设置杠杆（如果指定）
            if leverage:
                await self.set_leverage(symbol, leverage)
            
            # 规范化数量
            qty = await self._normalize_qty(symbol, qty)
            
            # 确定订单方向
            order_side = 'BUY' if side.upper() == 'LONG' else 'SELL'
            position_side = side.upper()
            
            # 下单
            order = await self._async_call(
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
                order_id=str(order.get('orderId')),
                symbol=symbol,
                side=order_side,
                position_side=position_side,
                qty=qty,
                avg_price=float(order.get('avgPrice', 0)),
                status=order.get('status'),
                raw=order
            )
            
        except BinanceAPIException as e:
            error_msg = str(e)
            # -2027: Exceeded the maximum allowable position at current leverage
            # 尝试调整数量到最大允许值
            if '-2027' in error_msg:
                try:
                    # 获取当前价格用于计算
                    current_price = await self.get_ticker_price(symbol)
                    adjusted_qty = await self._adjust_qty_to_max_allowed(symbol, qty, current_price, leverage)
                    if adjusted_qty and adjusted_qty > 0 and adjusted_qty < qty:
                        logger.info(f"[Binance] {symbol} 仓位超限，调整数量 {qty} -> {adjusted_qty}")
                        # 递归调用，使用调整后的数量
                        return await self.market_open(
                            symbol=symbol,
                            side=side,
                            qty=adjusted_qty,
                            leverage=None  # 杠杆已设置，不需要再设置
                        )
                except Exception as adjust_e:
                    logger.warning(f"[Binance] 调整仓位失败 {symbol}: {adjust_e}")
            
            logger.error(f"[Binance] 开仓失败 {symbol}: {e}")
            return OrderResult(
                success=False,
                symbol=symbol,
                error=str(e)
            )
        except Exception as e:
            logger.error(f"[Binance] 开仓异常 {symbol}: {e}")
            return OrderResult(
                success=False,
                symbol=symbol,
                error=str(e)
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
            # 设置杠杆（如果指定）
            if leverage:
                await self.set_leverage(symbol, leverage)
            
            # 规范化数量和价格
            qty = await self._normalize_qty(symbol, qty)
            price = await self._normalize_price(symbol, price)
            
            # 确定订单方向
            order_side = 'BUY' if side.upper() == 'LONG' else 'SELL'
            position_side = side.upper()
            
            # 构建订单参数
            order_params = {
                'symbol': symbol,
                'side': order_side,
                'positionSide': position_side,
                'type': 'LIMIT',
                'timeInForce': 'GTC',
                'quantity': qty,
                'price': str(price),
                'newOrderRespType': 'RESULT',
                'newClientOrderId': self._generate_client_order_id()
            }
            
            # 下单
            order = await self._async_call('futures_create_order', **order_params)
            
            return OrderResult(
                success=True,
                order_id=str(order.get('orderId')),
                symbol=symbol,
                side=order_side,
                position_side=position_side,
                qty=qty,
                price=price,
                status=order.get('status'),
                raw=order
            )
            
        except BinanceAPIException as e:
            error_msg = str(e)
            # -2027: Exceeded the maximum allowable position at current leverage
            # 尝试调整数量到最大允许值
            if '-2027' in error_msg:
                try:
                    adjusted_qty = await self._adjust_qty_to_max_allowed(symbol, qty, price, leverage)
                    if adjusted_qty and adjusted_qty > 0 and adjusted_qty < qty:
                        logger.info(f"[Binance] {symbol} 仓位超限，调整数量 {qty} -> {adjusted_qty}")
                        # 递归调用，使用调整后的数量
                        return await self.limit_open(
                            symbol=symbol,
                            side=side,
                            qty=adjusted_qty,
                            price=price,
                            leverage=None,  # 杠杆已设置，不需要再设置
                        )
                except Exception as adjust_e:
                    logger.warning(f"[Binance] 调整仓位失败 {symbol}: {adjust_e}")
            
            logger.error(f"[Binance] 限价开仓失败 {symbol}: {e}")
            return OrderResult(success=False, symbol=symbol, error=str(e))
        except Exception as e:
            logger.error(f"[Binance] 限价开仓异常 {symbol}: {e}")
            return OrderResult(success=False, symbol=symbol, error=str(e))
    
    async def market_close(
        self,
        symbol: str,
        side: str,
        qty: float = None,
        cancel_tp_sl: bool = True,
    ) -> OrderResult:
        """市价平仓
        
        Args:
            symbol: 交易对
            side: 持仓方向 (LONG/SHORT)
            qty: 平仓数量，None 表示全部平仓
            cancel_tp_sl: 是否取消止盈止损单（默认 True，减仓时传 False）
        """
        try:
            # 如果没指定数量，获取当前持仓数量
            if qty is None:
                position = await self.get_position(symbol)
                if position and position.side == side.upper():
                    qty = position.qty
                else:
                    return OrderResult(
                        success=False,
                        symbol=symbol,
                        error="无持仓"
                    )
            
            # 规范化数量
            qty = await self._normalize_qty(symbol, qty)
            
            # 平仓方向与开仓相反
            order_side = 'SELL' if side.upper() == 'LONG' else 'BUY'
            position_side = side.upper()
            
            order = await self._async_call(
                'futures_create_order',
                symbol=symbol,
                side=order_side,
                positionSide=position_side,
                type='MARKET',
                quantity=qty,
                newClientOrderId=self._generate_client_order_id()
            )
            
            # 平仓成功后，取消该币种该方向的止盈止损单（减仓时不取消）
            if cancel_tp_sl:
                try:
                    await self._cancel_tp_sl(symbol, position_side, cancel_sl=True, cancel_tp=True)
                    logger.info(f"[Binance] {symbol} {position_side} 平仓后已取消止盈止损单")
                except Exception as cancel_err:
                    logger.warning(f"[Binance] {symbol} 取消止盈止损单失败: {cancel_err}")
            
            return OrderResult(
                success=True,
                order_id=str(order.get('orderId')),
                symbol=symbol,
                side=order_side,
                position_side=position_side,
                qty=qty,
                avg_price=float(order.get('avgPrice', 0)),
                status=order.get('status'),
                raw=order
            )
            
        except BinanceAPIException as e:
            logger.error(f"[Binance] 平仓失败 {symbol}: {e}")
            return OrderResult(success=False, symbol=symbol, error=str(e))
        except Exception as e:
            logger.error(f"[Binance] 平仓异常 {symbol}: {e}")
            return OrderResult(success=False, symbol=symbol, error=str(e))
    
    async def set_stop_loss(
        self,
        symbol: str,
        side: str,
        qty: float,
        stop_price: float,
    ) -> OrderResult:
        """
        设置止损 (使用 Algo 条件单，closePosition=true 全仓平仓)
        """
        try:
            stop_price = await self._normalize_price(symbol, stop_price)
            position_side = side.upper()
            
            # 从 Redis 获取 WS 标记价格进行验证
            mark_price = self._get_mark_price_from_redis(symbol)
            if mark_price and mark_price > 0:
                # 验证止损价格是否有效（不会立即触发）
                if position_side == 'LONG' and stop_price >= mark_price:
                    error_msg = f"多头止损价 {stop_price} 必须低于当前标记价 {mark_price}"
                    logger.warning(f"[Binance] {symbol} {error_msg}")
                    return OrderResult(success=False, symbol=symbol, error=error_msg)
                if position_side == 'SHORT' and stop_price <= mark_price:
                    error_msg = f"空头止损价 {stop_price} 必须高于当前标记价 {mark_price}"
                    logger.warning(f"[Binance] {symbol} {error_msg}")
                    return OrderResult(success=False, symbol=symbol, error=error_msg)
            
            # 止损方向与持仓相反
            order_side = 'SELL' if position_side == 'LONG' else 'BUY'
            
            # 先取消旧的止损单
            await self._cancel_tp_sl(symbol, position_side, cancel_sl=True, cancel_tp=False)
            
            # 使用 Algo 条件单，closePosition=true
            order = await self._async_call(
                'futures_create_algo_order',
                algoType='CONDITIONAL',
                symbol=symbol,
                side=order_side,
                positionSide=position_side,
                type='STOP_MARKET',
                triggerPrice=str(stop_price),
                closePosition='true',
                workingType='MARK_PRICE',
                timeInForce='GTC',
                newOrderRespType='RESULT',
                newClientOrderId=self._generate_client_order_id()
            )
            
            logger.info(f"[Binance] 止损单已创建 {symbol} {position_side} trigger={stop_price} algoId={order.get('algoId')}")
            
            return OrderResult(
                success=True,
                order_id=str(order.get('algoId', '')),
                symbol=symbol,
                side=order_side,
                position_side=position_side,
                price=stop_price,
                status=order.get('algoStatus'),
                raw=order
            )
            
        except BinanceAPIException as e:
            logger.error(f"[Binance] 设置止损失败 {symbol}: {e}")
            return OrderResult(success=False, symbol=symbol, error=str(e))
        except Exception as e:
            logger.error(f"[Binance] 设置止损异常 {symbol}: {e}")
            return OrderResult(success=False, symbol=symbol, error=str(e))
    
    def _get_mark_price_from_redis(self, symbol: str) -> Optional[float]:
        """从 Redis 获取 WS 标记价格"""
        try:
            from core.database import redis_client, RedisKeys
            import json
            
            key = RedisKeys.market_prices(symbol.upper())
            data = redis_client.get(key)
            
            if data:
                parsed = json.loads(data)
                return float(parsed.get("markPrice", 0))
        except Exception as e:
            logger.debug(f"[Binance] 获取 Redis 标记价格失败 {symbol}: {e}")
        
        return None
    
    async def set_take_profit(
        self,
        symbol: str,
        side: str,
        qty: float,
        tp_price: float,
    ) -> OrderResult:
        """
        设置止盈 (使用 Algo 条件单，closePosition=true 全仓平仓)
        """
        try:
            tp_price = await self._normalize_price(symbol, tp_price)
            
            order_side = 'SELL' if side.upper() == 'LONG' else 'BUY'
            position_side = side.upper()
            
            # 从 Redis 获取 WS 标记价格进行验证
            mark_price = self._get_mark_price_from_redis(symbol)
            if mark_price and mark_price > 0:
                # 验证止盈价格是否有效（不会立即触发）
                if position_side == 'LONG' and tp_price <= mark_price:
                    error_msg = f"多头止盈价 {tp_price} 必须高于当前标记价 {mark_price}"
                    logger.warning(f"[Binance] {symbol} {error_msg}")
                    return OrderResult(success=False, symbol=symbol, error=error_msg)
                if position_side == 'SHORT' and tp_price >= mark_price:
                    error_msg = f"空头止盈价 {tp_price} 必须低于当前标记价 {mark_price}"
                    logger.warning(f"[Binance] {symbol} {error_msg}")
                    return OrderResult(success=False, symbol=symbol, error=error_msg)
            
            # 先取消旧的止盈单
            await self._cancel_tp_sl(symbol, position_side, cancel_sl=False, cancel_tp=True)
            
            # 使用 Algo 条件单，closePosition=true
            order = await self._async_call(
                'futures_create_algo_order',
                algoType='CONDITIONAL',
                symbol=symbol,
                side=order_side,
                positionSide=position_side,
                type='TAKE_PROFIT_MARKET',
                triggerPrice=str(tp_price),
                closePosition='true',
                workingType='MARK_PRICE',
                timeInForce='GTC',
                newOrderRespType='RESULT',
                newClientOrderId=self._generate_client_order_id()
            )
            
            logger.info(f"[Binance] 止盈单已创建 {symbol} {position_side} trigger={tp_price} algoId={order.get('algoId')}")
            
            return OrderResult(
                success=True,
                order_id=str(order.get('algoId', '')),
                symbol=symbol,
                side=order_side,
                position_side=position_side,
                price=tp_price,
                status=order.get('algoStatus'),
                raw=order
            )
            
        except BinanceAPIException as e:
            logger.error(f"[Binance] 设置止盈失败 {symbol}: {e}")
            return OrderResult(success=False, symbol=symbol, error=str(e))
        except Exception as e:
            logger.error(f"[Binance] 设置止盈异常 {symbol}: {e}")
            return OrderResult(success=False, symbol=symbol, error=str(e))
    
    async def _cancel_tp_sl(
        self, 
        symbol: str, 
        position_side: str, 
        cancel_sl: bool = True, 
        cancel_tp: bool = True
    ):
        """
        取消止盈止损单 (包括普通订单和 Algo 条件单)
        """
        TP_SL_TYPES = {
            "sl": ["STOP", "STOP_MARKET"],
            "tp": ["TAKE_PROFIT", "TAKE_PROFIT_MARKET"]
        }
        
        types_to_cancel = []
        if cancel_sl:
            types_to_cancel += TP_SL_TYPES["sl"]
        if cancel_tp:
            types_to_cancel += TP_SL_TYPES["tp"]
        if not types_to_cancel:
            return
        
        tasks = []
        
        # 1) 取消普通挂单
        try:
            open_orders = await self._async_call('futures_get_open_orders', symbol=symbol)
            for o in open_orders:
                if (
                    o.get("positionSide") == position_side
                    and o.get("type") in types_to_cancel
                    and o.get("status") in ["NEW", "PARTIALLY_FILLED"]
                ):
                    oid = o.get("orderId")
                    if oid:
                        tasks.append(self._async_call('futures_cancel_order', symbol=symbol, orderId=oid))
        except Exception as e:
            logger.warning(f"[Binance] 获取普通挂单失败: {e}")
        
        # 2) 取消 Algo 条件单
        try:
            algo_orders_all = await self._async_call('futures_get_open_algo_orders')
            algo_orders = [o for o in algo_orders_all if o.get("symbol") == symbol]
            
            for o in algo_orders:
                if (
                    o.get("positionSide") == position_side
                    and o.get("orderType") in types_to_cancel
                    and o.get("algoStatus") == "NEW"
                ):
                    algo_id = o.get("algoId")
                    if algo_id:
                        tasks.append(self._cancel_algo_order(symbol, algo_id))
        except Exception as e:
            logger.warning(f"[Binance] 获取 Algo 条件单失败: {e}")
        
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
    
    async def _cancel_algo_order(self, symbol: str, algo_id: str):
        """取消单个 Algo 条件单"""
        try:
            await self._async_call(
                'futures_cancel_algo_order',
                symbol=symbol,
                algoId=algo_id
            )
            logger.info(f"[Binance] 已取消 Algo 条件单 {symbol} algoId={algo_id}")
        except Exception as e:
            # -2011 通常表示订单已触发/已取消
            if "-2011" in str(e):
                logger.debug(f"[Binance] Algo 条件单可能已触发或已取消: {algo_id}")
            else:
                logger.warning(f"[Binance] 取消 Algo 条件单失败 {symbol} {algo_id}: {e}")
    
    async def cancel_order(self, symbol: str, order_id: str) -> bool:
        """取消订单"""
        try:
            await self._async_call(
                'futures_cancel_order',
                symbol=symbol,
                orderId=order_id
            )
            return True
        except Exception as e:
            logger.error(f"[Binance] 取消订单失败 {symbol} {order_id}: {e}")
            return False
    
    async def cancel_all_orders(self, symbol: str) -> int:
        """取消指定币种的所有订单 (包括普通订单和 Algo 条件单)"""
        cancelled = 0
        
        # 1) 取消普通订单
        try:
            result = await self._async_call(
                'futures_cancel_all_open_orders',
                symbol=symbol
            )
            cancelled += len(result) if isinstance(result, list) else 1
        except Exception as e:
            logger.warning(f"[Binance] 取消普通订单失败 {symbol}: {e}")
        
        # 2) 取消所有 Algo 条件单
        try:
            algo_orders_all = await self._async_call('futures_get_open_algo_orders')
            algo_orders = [o for o in algo_orders_all if o.get("symbol") == symbol]
            
            tasks = []
            for o in algo_orders:
                algo_id = o.get("algoId")
                if algo_id:
                    tasks.append(self._cancel_algo_order(symbol, algo_id))
            
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
                cancelled += len(tasks)
        except Exception as e:
            logger.warning(f"[Binance] 取消 Algo 条件单失败 {symbol}: {e}")
        
        return cancelled
    
    # ==================== 设置相关 ====================
    
    async def set_leverage(self, symbol: str, leverage: int) -> bool:
        """设置杠杆，如果失败则尝试使用最大支持杠杆"""
        try:
            await self._async_call(
                'futures_change_leverage',
                symbol=symbol,
                leverage=leverage
            )
            logger.debug(f"[Binance] 设置杠杆成功 {symbol} {leverage}x")
            return True
        except Exception as e:
            error_msg = str(e)
            # 检查是否是杠杆超限错误
            if 'Leverage' in error_msg or '-4028' in error_msg or 'leverage' in error_msg.lower():
                # 尝试获取最大杠杆并重试
                try:
                    max_leverage = await self._get_max_leverage(symbol)
                    if max_leverage and max_leverage < leverage:
                        logger.info(f"[Binance] {symbol} 杠杆 {leverage}x 超限，使用最大杠杆 {max_leverage}x")
                        await self._async_call(
                            'futures_change_leverage',
                            symbol=symbol,
                            leverage=max_leverage
                        )
                        return True
                except Exception as retry_e:
                    logger.warning(f"[Binance] 设置最大杠杆也失败 {symbol}: {retry_e}")
            logger.warning(f"[Binance] 设置杠杆失败 {symbol} {leverage}x: {e}")
            return False
    
    async def _get_max_leverage(self, symbol: str) -> int:
        """
        获取交易对最大杠杆
        
        优先从缓存获取，缓存未命中时使用用户客户端调用 API
        （leverage_bracket 是私有 API，需要 API Key）
        """
        try:
            from core.binance_public_cache import get_binance_public_cache
            cache = get_binance_public_cache()
            
            # 先尝试从缓存获取（不触发 API 调用）
            brackets = cache.get_leverage_bracket_from_cache(symbol)
            
            if not brackets:
                # 缓存未命中，使用用户客户端调用 API
                result = await self._async_call('futures_leverage_bracket', symbol=symbol)
                if result and len(result) > 0:
                    brackets = result[0].get('brackets', [])
                    # 更新缓存供其他地方使用
                    cache.set_leverage_bracket_cache(symbol, brackets)
            
            if brackets:
                # brackets 按 notionalFloor 升序排列，第一个是最小档位，杠杆最高
                return int(brackets[0].get('initialLeverage', 20))
        except Exception as e:
            logger.debug(f"[Binance] 获取最大杠杆失败 {symbol}: {e}")
        return 20  # 默认值
    
    async def _get_max_notional_at_leverage(self, symbol: str, leverage: int) -> Optional[float]:
        """
        获取指定杠杆下的最大允许名义价值
        
        优先从缓存获取，缓存未命中时使用用户客户端调用 API
        """
        try:
            from core.binance_public_cache import get_binance_public_cache
            cache = get_binance_public_cache()
            
            # 先尝试从缓存获取
            brackets = cache.get_leverage_bracket_from_cache(symbol)
            
            if not brackets:
                # 缓存未命中，使用用户客户端调用 API
                result = await self._async_call('futures_leverage_bracket', symbol=symbol)
                if result and len(result) > 0:
                    brackets = result[0].get('brackets', [])
                    # 更新缓存
                    cache.set_leverage_bracket_cache(symbol, brackets)
            
            if brackets:
                for bracket in brackets:
                    # 找到当前杠杆对应的档位
                    if int(bracket.get('initialLeverage', 0)) >= leverage:
                        # notionalCap 是该档位的最大名义价值
                        return float(bracket.get('notionalCap', 0))
                # 如果没找到匹配的，返回最后一个档位的 cap
                if brackets:
                    return float(brackets[-1].get('notionalCap', 0))
        except Exception as e:
            logger.debug(f"[Binance] 获取最大名义价值失败 {symbol}: {e}")
        return None
    
    async def _adjust_qty_to_max_allowed(
        self,
        symbol: str,
        qty: float,
        price: float,
        leverage: int = None
    ) -> Optional[float]:
        """
        调整数量到最大允许值
        
        当 -2027 错误发生时，计算在当前杠杆下允许的最大数量
        
        Args:
            symbol: 交易对
            qty: 原始数量
            price: 价格
            leverage: 杠杆倍数
        
        Returns:
            调整后的数量，如果无法调整返回 None
        """
        try:
            # 获取当前杠杆（如果没有传入）
            if not leverage:
                # 尝试从持仓获取当前杠杆
                position = await self.get_position(symbol)
                if position:
                    leverage = position.leverage
                else:
                    leverage = 20  # 默认杠杆
            
            # 获取该杠杆下的最大名义价值
            max_notional = await self._get_max_notional_at_leverage(symbol, leverage)
            if not max_notional:
                return None
            
            # 计算当前订单的名义价值
            current_notional = qty * price
            
            if current_notional <= max_notional:
                # 当前订单没有超限，可能是已有仓位导致的
                # 获取已有仓位的名义价值
                position = await self.get_position(symbol)
                existing_notional = 0
                if position:
                    existing_notional = position.qty * position.entry_price
                
                # 可用名义价值 = 最大名义价值 - 已有仓位名义价值
                available_notional = max_notional - existing_notional
                
                if available_notional <= 0:
                    logger.warning(f"[Binance] {symbol} 已达最大仓位限制，无法开仓")
                    return None
                
                # 计算可开的最大数量
                max_qty = available_notional / price
            else:
                # 订单本身就超限，直接按最大名义价值计算
                max_qty = max_notional / price
            
            # 规范化数量（向下取整到最小单位）
            max_qty = await self._normalize_qty(symbol, max_qty)
            
            # 留一点余量（95%），避免边界问题
            max_qty = max_qty * 0.95
            max_qty = await self._normalize_qty(symbol, max_qty)
            
            if max_qty <= 0:
                return None
            
            logger.info(
                f"[Binance] {symbol} 仓位调整: 原始={qty}, 价格={price}, "
                f"最大名义={max_notional}, 调整后={max_qty}"
            )
            
            return max_qty
            
        except Exception as e:
            logger.error(f"[Binance] 计算最大允许仓位失败 {symbol}: {e}")
            return None
    
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
            # -4046 表示已经是该模式
            if '-4046' in str(e):
                return True
            logger.warning(f"[Binance] 设置保证金模式失败 {symbol}: {e}")
            return False
        except Exception as e:
            logger.warning(f"[Binance] 设置保证金模式失败 {symbol}: {e}")
            return False
    
    # ==================== 市场数据 ====================
    
    async def get_ticker_price(self, symbol: str) -> float:
        """
        获取最新标记价格
        
        优先从 Redis 获取 WebSocket 推送的数据，避免 REST API 调用
        """
        # 1. 优先从 Redis 获取 WebSocket 数据
        from core.binance_public_cache import get_binance_public_cache
        cache = get_binance_public_cache()
        mark_price = cache.get_mark_price(symbol)
        if mark_price and mark_price > 0:
            return mark_price
        
        # 2. 回退到 REST API（会消耗限速配额）
        logger.debug(f"[Binance] Redis 无标记价格，使用 REST API: {symbol}")
        data = await self._async_call('futures_mark_price', symbol=symbol)
        return float(data.get('markPrice', 0))
    
    async def get_symbol_info(self, symbol: str) -> Dict:
        """
        获取交易对信息
        
        使用全局公共缓存，所有用户共享
        """
        from core.binance_public_cache import get_binance_public_cache
        cache = get_binance_public_cache()
        return await cache.get_symbol_info_async(symbol)
    
    async def _refresh_symbol_cache(self):
        """刷新交易对缓存（已废弃，使用公共缓存）"""
        # 保留方法以兼容旧代码，但实际使用公共缓存
        pass
    
    # ==================== WebSocket ====================
    
    def create_ws_client(self, callbacks: Dict[str, callable] = None) -> Any:
        """创建 WebSocket 客户端"""
        # 使用现有的 user_stream.py
        from websocket.user_stream import BinanceUserStreamManager
        
        return BinanceUserStreamManager(
            api_key=self.api_key,
            api_secret=self.api_secret,
            is_testnet=self.is_testnet,
            callbacks=callbacks
        )
    
    # ==================== 辅助方法 ====================
    
    async def _normalize_qty(self, symbol: str, qty: float) -> float:
        """规范化数量"""
        info = await self.get_symbol_info(symbol)
        if not info:
            return qty
        
        step_size = 1.0
        min_qty = 0.0
        
        for f in info.get('filters', []):
            if f.get('filterType') == 'LOT_SIZE':
                step_size = float(f.get('stepSize', 1))
                min_qty = float(f.get('minQty', 0))
        
        decimals = max(0, -int(math.log10(step_size))) if step_size > 0 else 0
        qty = max(qty, min_qty)
        qty = math.ceil(qty / step_size) * step_size
        qty = round(qty, decimals)
        
        return qty
    
    async def _normalize_price(self, symbol: str, price: float) -> float:
        """
        规范化价格（使用 Decimal 保持精度，四舍五入到 tick_size）
        
        对于小价格币种（如 SHIB、PEPE），浮点数精度不足，使用 Decimal 计算
        """
        info = await self.get_symbol_info(symbol)
        if not info:
            return float(price)
        
        tick_size_str = "0.01"
        
        for f in info.get('filters', []):
            if f.get('filterType') == 'PRICE_FILTER':
                tick_size_str = str(f.get('tickSize', '0.01'))
        
        # 使用 Decimal 进行高精度计算
        price_d = Decimal(str(price))
        tick_d = Decimal(tick_size_str)
        
        # 四舍五入到 tick_size
        normalized = (price_d / tick_d).quantize(Decimal('1'), rounding=ROUND_HALF_UP) * tick_d
        
        return float(normalized)
    
    async def close(self):
        """关闭连接"""
        if self._client:
            self._client.close_connection()
    
    def close_sync(self):
        """同步关闭（安全版本）"""
        if self._client:
            self._client.close_connection()
