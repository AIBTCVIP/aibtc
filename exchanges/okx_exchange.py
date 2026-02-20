# exchanges/okx_exchange.py
"""
OKX 交易所实现

OKX API 文档: https://www.okx.com/docs-v5/
"""

import asyncio
import logging
import math
import hmac
import base64
import json
import time
import uuid
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
from hashlib import sha256
from typing import Dict, List, Optional, Any
from urllib.parse import urlencode

import aiohttp

from exchanges.base import (
    BaseExchange,
    OrderResult,
    Position,
    AccountInfo,
    OrderSide,
    PositionSide,
)

logger = logging.getLogger(__name__)


class OKXExchange(BaseExchange):
    """
    OKX 交易所实现
    
    使用 OKX V5 REST API
    支持 USDT 永续合约 (SWAP)
    """
    
    EXCHANGE_NAME = "okx"
    
    # API 端点
    MAINNET_API = "https://www.okx.com"
    TESTNET_API = "https://www.okx.com"  # OKX testnet uses same domain with flag
    
    # Broker Code (用于经纪商订单追踪)
    BROKER_CODE = "d62ea9d964aaBCDE"
    
    def __init__(
        self,
        api_key: str,
        api_secret: str,
        passphrase: str = None,
        is_testnet: bool = False,
        **kwargs
    ):
        super().__init__(api_key, api_secret, passphrase, is_testnet, **kwargs)
        
        if not passphrase:
            raise ValueError("OKX requires passphrase")
        
        self.base_url = self.MAINNET_API
        self._session: Optional[aiohttp.ClientSession] = None
        
        # 缓存交易对信息
        self._symbol_info_cache: Dict[str, Dict] = {}
        self._cache_time = 0
        self._cache_ttl = 3600  # 1小时缓存
    
    def _generate_client_order_id(self) -> str:
        timestamp = str(int(time.time()))[-6:]
        random_suffix = uuid.uuid4().hex[:10]
        unique_suffix = f"{timestamp}{random_suffix}"
        client_order_id = f"{self.BROKER_CODE}{unique_suffix}"
        return client_order_id[:32]
    
    async def _get_session(self) -> aiohttp.ClientSession:
        """获取或创建 aiohttp session"""
        # 检查 session 是否有效（包括检查绑定的事件循环）
        need_new_session = False
        if self._session is None or self._session.closed:
            need_new_session = True
        else:
            # 检查 session 绑定的循环是否与当前循环一致
            try:
                current_loop = asyncio.get_running_loop()
                # aiohttp 3.x session 的 _loop 属性
                session_loop = getattr(self._session, '_loop', None)
                if session_loop is not None and session_loop is not current_loop:
                    # session 绑定的循环与当前循环不同，需要关闭旧的创建新的
                    try:
                        await self._session.close()
                    except Exception:
                        pass
                    need_new_session = True
            except RuntimeError:
                pass
        
        if need_new_session:
            self._session = aiohttp.ClientSession()
        return self._session
    
    def _get_timestamp(self) -> str:
        """获取 ISO 格式时间戳"""
        return datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
    
    def _sign(self, timestamp: str, method: str, request_path: str, body: str = '') -> str:
        """生成签名"""
        message = timestamp + method.upper() + request_path + body
        signature = hmac.new(
            self.api_secret.encode('utf-8'),
            message.encode('utf-8'),
            sha256
        ).digest()
        return base64.b64encode(signature).decode('utf-8')
    
    async def _request(
        self,
        method: str,
        path: str,
        params: Dict = None,
        data: Dict = None
    ) -> Dict:
        """发送请求（带 API Key 级别限速）"""
        from core.rate_limiter import get_okx_rate_limiter
        
        # 使用 API Key 级别限速器
        rate_limiter = get_okx_rate_limiter(self.api_key)
        if not await rate_limiter.acquire_async(endpoint=path, timeout=30.0):
            raise Exception(f"OKX API 限速器超时: {method} {path}")
        
        session = await self._get_session()
        
        timestamp = self._get_timestamp()
        
        # 构建请求路径
        request_path = path
        if params:
            request_path += '?' + urlencode(params)
        
        # 构建请求体
        body = ''
        if data:
            body = json.dumps(data)
        
        # 签名
        sign = self._sign(timestamp, method, request_path, body)
        
        # 请求头
        headers = {
            'OK-ACCESS-KEY': self.api_key,
            'OK-ACCESS-SIGN': sign,
            'OK-ACCESS-TIMESTAMP': timestamp,
            'OK-ACCESS-PASSPHRASE': self.passphrase,
            'Content-Type': 'application/json',
        }
        
        # 模拟盘标记
        if self.is_testnet:
            headers['x-simulated-trading'] = '1'
        
        url = self.base_url + request_path
        
        try:
            async with session.request(
                method, 
                url, 
                headers=headers, 
                data=body if body else None,
                timeout=aiohttp.ClientTimeout(total=30)
            ) as resp:
                result = await resp.json()
                
                if result.get('code') != '0':
                    logger.error(f"[OKX] API Error: {result}")
                    # 包含错误码便于后续处理
                    error_code = result.get('code', '')
                    error_msg = result.get('msg', 'Unknown error')
                    raise Exception(f"OKX API Error [{error_code}]: {error_msg}")
                
                return result
        except aiohttp.ClientError as e:
            logger.error(f"[OKX] Request failed: {e}")
            raise
    
    def _convert_symbol(self, symbol: str) -> str:
        """
        将 Binance 格式转为 OKX 格式
        BTCUSDT -> BTC-USDT-SWAP
        """
        if symbol.endswith('USDT'):
            base = symbol[:-4]
            return f"{base}-USDT-SWAP"
        return symbol
    
    def _parse_symbol(self, okx_symbol: str) -> str:
        """
        将 OKX 格式转为 Binance 格式
        BTC-USDT-SWAP -> BTCUSDT
        """
        parts = okx_symbol.split('-')
        if len(parts) >= 2:
            return f"{parts[0]}{parts[1]}"
        return okx_symbol
    
    def normalize_symbol(self, symbol: str) -> str:
        return self._convert_symbol(symbol)
    
    def denormalize_symbol(self, symbol: str) -> str:
        return self._parse_symbol(symbol)
    
    # ==================== 账户相关 ====================
    
    async def get_account(self) -> AccountInfo:
        """获取账户信息"""
        # 获取账户余额
        balance_data = await self._request('GET', '/api/v5/account/balance')
        
        total_balance = 0.0
        available_balance = 0.0
        unrealized_pnl = 0.0
        
        for detail in balance_data.get('data', [{}])[0].get('details', []):
            if detail.get('ccy') == 'USDT':
                total_balance = float(detail.get('eq', 0))
                available_balance = float(detail.get('availBal', 0))
                unrealized_pnl = float(detail.get('upl', 0))
                break
        
        # 获取持仓
        positions = await self.get_positions()
        
        return AccountInfo(
            total_balance=total_balance,
            available_balance=available_balance,
            unrealized_pnl=unrealized_pnl,
            margin_balance=total_balance,
            positions=positions,
            exchange=self.EXCHANGE_NAME,
            raw=balance_data
        )
    
    async def get_balance(self) -> float:
        """获取可用余额"""
        data = await self._request('GET', '/api/v5/account/balance')
        
        for detail in data.get('data', [{}])[0].get('details', []):
            if detail.get('ccy') == 'USDT':
                return float(detail.get('availBal', 0))
        return 0.0
    
    async def get_positions(self) -> List[Position]:
        """获取所有持仓"""
        data = await self._request('GET', '/api/v5/account/positions', params={'instType': 'SWAP'})
        
        positions = []
        for p in data.get('data', []):
            pos_contracts = float(p.get('pos', 0))  # pos 是张数，不是币数量
            if pos_contracts == 0:
                continue
            
            # OKX 的 posSide: long, short, net
            pos_side = p.get('posSide', 'net')
            if pos_side == 'net':
                side = 'LONG' if pos_contracts > 0 else 'SHORT'
            else:
                side = pos_side.upper()
            
            # 将张数转换为币数量
            inst_id = p.get('instId', '')
            qty_coins = await self._contracts_to_coins(inst_id, abs(pos_contracts))
            
            positions.append(Position(
                symbol=self._parse_symbol(inst_id),
                side=side,
                qty=qty_coins,
                entry_price=float(p.get('avgPx', 0)),
                unrealized_pnl=float(p.get('upl', 0)),
                leverage=int(float(p.get('lever', 1))),
                margin_type='cross' if p.get('mgnMode') == 'cross' else 'isolated',
                liquidation_price=float(p.get('liqPx', 0)) if p.get('liqPx') else 0,
                break_even_price=float(p.get('bePx', 0)) if p.get('bePx') else 0,
                exchange=self.EXCHANGE_NAME,
                raw=p
            ))
        
        return positions
    
    async def get_position(self, symbol: str) -> Optional[Position]:
        """获取指定币种的持仓"""
        okx_symbol = self._convert_symbol(symbol)
        data = await self._request('GET', '/api/v5/account/positions', params={
            'instType': 'SWAP',
            'instId': okx_symbol
        })
        
        for p in data.get('data', []):
            pos_contracts = float(p.get('pos', 0))  # pos 是张数，不是币数量
            if pos_contracts == 0:
                continue
            
            pos_side = p.get('posSide', 'net')
            if pos_side == 'net':
                side = 'LONG' if pos_contracts > 0 else 'SHORT'
            else:
                side = pos_side.upper()
            
            # 将张数转换为币数量
            qty_coins = await self._contracts_to_coins(okx_symbol, abs(pos_contracts))
            
            return Position(
                symbol=symbol,
                side=side,
                qty=qty_coins,
                entry_price=float(p.get('avgPx', 0)),
                unrealized_pnl=float(p.get('upl', 0)),
                leverage=int(float(p.get('lever', 1))),
                margin_type='cross' if p.get('mgnMode') == 'cross' else 'isolated',
                liquidation_price=float(p.get('liqPx', 0)) if p.get('liqPx') else 0,
                exchange=self.EXCHANGE_NAME,
                raw=p
            )
        
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
            okx_symbol = self._convert_symbol(symbol)
            
            # 设置杠杆（如果指定）
            if leverage:
                await self.set_leverage(symbol, leverage)
            
            # 规范化数量
            qty = await self._normalize_qty(symbol, qty)
            
            # OKX 使用 buy/sell 和 posSide
            # 开多: side=buy, posSide=long
            # 开空: side=sell, posSide=short
            order_side = 'buy' if side.upper() == 'LONG' else 'sell'
            pos_side = side.lower()
            
            order_data = {
                'instId': okx_symbol,
                'tdMode': 'cross',  # 全仓模式
                'side': order_side,
                'posSide': pos_side,
                'ordType': 'market',
                'sz': str(qty),
                'clOrdId': self._generate_client_order_id(),
                'tag': self.BROKER_CODE,
            }
            
            result = await self._request('POST', '/api/v5/trade/order', data=order_data)
            
            order_info = result.get('data', [{}])[0]
            
            return OrderResult(
                success=True,
                order_id=order_info.get('ordId', ''),
                symbol=symbol,
                side=order_side.upper(),
                position_side=side.upper(),
                qty=qty,
                status='FILLED',
                raw=result
            )
            
        except Exception as e:
            logger.error(f"[OKX] 开仓失败 {symbol}: {e}")
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
            okx_symbol = self._convert_symbol(symbol)
            
            # 设置杠杆（如果指定）
            if leverage:
                await self.set_leverage(symbol, leverage)
            
            # 规范化数量和价格
            qty = await self._normalize_qty(symbol, qty)
            price = await self._normalize_price(symbol, price)
            
            # OKX 使用 buy/sell 和 posSide
            order_side = 'buy' if side.upper() == 'LONG' else 'sell'
            pos_side = side.lower()
            
            order_data = {
                'instId': okx_symbol,
                'tdMode': 'cross',
                'side': order_side,
                'posSide': pos_side,
                'ordType': 'limit',
                'sz': str(qty),
                'px': str(price),
                'clOrdId': self._generate_client_order_id(),
                'tag': self.BROKER_CODE,
            }
            
            result = await self._request('POST', '/api/v5/trade/order', data=order_data)
            
            order_info = result.get('data', [{}])[0]
            
            return OrderResult(
                success=True,
                order_id=order_info.get('ordId', ''),
                symbol=symbol,
                side=order_side.upper(),
                position_side=side.upper(),
                qty=qty,
                price=price,
                status='NEW',
                raw=result
            )
            
        except Exception as e:
            logger.error(f"[OKX] 限价开仓失败 {symbol}: {e}")
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
            okx_symbol = self._convert_symbol(symbol)
            
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
            # 平多: side=sell, posSide=long
            # 平空: side=buy, posSide=short
            order_side = 'sell' if side.upper() == 'LONG' else 'buy'
            pos_side = side.lower()
            
            order_data = {
                'instId': okx_symbol,
                'tdMode': 'cross',
                'side': order_side,
                'posSide': pos_side,
                'ordType': 'market',
                'sz': str(qty),
                'clOrdId': self._generate_client_order_id(),
                'tag': self.BROKER_CODE,
            }
            
            result = await self._request('POST', '/api/v5/trade/order', data=order_data)
            
            order_info = result.get('data', [{}])[0]
            
            # 平仓成功后，取消该币种的所有挂单（止盈止损单）（减仓时不取消）
            if cancel_tp_sl:
                try:
                    cancelled = await self.cancel_all_orders(symbol)
                    if cancelled > 0:
                        logger.info(f"[OKX] {symbol} 平仓后取消了 {cancelled} 个挂单")
                except Exception as cancel_err:
                    logger.warning(f"[OKX] {symbol} 取消挂单失败: {cancel_err}")
            
            return OrderResult(
                success=True,
                order_id=order_info.get('ordId', ''),
                symbol=symbol,
                side=order_side.upper(),
                position_side=side.upper(),
                qty=qty,
                status='FILLED',
                raw=result
            )
            
        except Exception as e:
            logger.error(f"[OKX] 平仓失败 {symbol}: {e}")
            return OrderResult(success=False, symbol=symbol, error=str(e))
    
    async def set_stop_loss(
        self,
        symbol: str,
        side: str,
        qty: float,
        stop_price: float,
    ) -> OrderResult:
        """设置止损"""
        try:
            okx_symbol = self._convert_symbol(symbol)
            qty = await self._normalize_qty(symbol, qty)
            stop_price = await self._normalize_price(symbol, stop_price)
            position_side = side.upper()
            
            # 从 Redis 获取 WS 标记价格进行验证
            mark_price = self._get_mark_price_from_redis(symbol)
            if mark_price and mark_price > 0:
                # 验证止损价格是否有效（不会立即触发）
                if position_side == 'LONG' and stop_price >= mark_price:
                    error_msg = f"多头止损价 {stop_price} 必须低于当前标记价 {mark_price}"
                    logger.warning(f"[OKX] {symbol} {error_msg}")
                    return OrderResult(success=False, symbol=symbol, error=error_msg)
                if position_side == 'SHORT' and stop_price <= mark_price:
                    error_msg = f"空头止损价 {stop_price} 必须高于当前标记价 {mark_price}"
                    logger.warning(f"[OKX] {symbol} {error_msg}")
                    return OrderResult(success=False, symbol=symbol, error=error_msg)
            
            # 止损方向与持仓相反
            order_side = 'sell' if position_side == 'LONG' else 'buy'
            pos_side = side.lower()
            
            # 先取消旧的止损单
            await self._cancel_algo_orders(okx_symbol, pos_side, cancel_sl=True, cancel_tp=False)
            
            # OKX 使用条件单
            order_data = {
                'instId': okx_symbol,
                'tdMode': 'cross',
                'side': order_side,
                'posSide': pos_side,
                'ordType': 'conditional',
                'sz': str(qty),
                'slTriggerPx': str(stop_price),
                'slOrdPx': '-1',  # -1 表示市价
                'algoClOrdId': self._generate_client_order_id(),
                'tag': self.BROKER_CODE,
            }
            
            result = await self._request('POST', '/api/v5/trade/order-algo', data=order_data)
            
            order_info = result.get('data', [{}])[0]
            
            logger.info(f"[OKX] 止损单已创建 {symbol} {side} trigger={stop_price} algoId={order_info.get('algoId')}")
            
            return OrderResult(
                success=True,
                order_id=order_info.get('algoId', ''),
                symbol=symbol,
                side=order_side.upper(),
                position_side=position_side,
                qty=qty,
                price=stop_price,
                status='NEW',
                raw=result
            )
            
        except Exception as e:
            logger.error(f"[OKX] 设置止损失败 {symbol}: {e}")
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
            logger.debug(f"[OKX] 获取 Redis 标记价格失败 {symbol}: {e}")
        
        return None
    
    async def set_take_profit(
        self,
        symbol: str,
        side: str,
        qty: float,
        tp_price: float,
    ) -> OrderResult:
        """设置止盈"""
        try:
            okx_symbol = self._convert_symbol(symbol)
            qty = await self._normalize_qty(symbol, qty)
            tp_price = await self._normalize_price(symbol, tp_price)
            position_side = side.upper()
            
            # 从 Redis 获取 WS 标记价格进行验证
            mark_price = self._get_mark_price_from_redis(symbol)
            if mark_price and mark_price > 0:
                # 验证止盈价格是否有效（不会立即触发）
                if position_side == 'LONG' and tp_price <= mark_price:
                    error_msg = f"多头止盈价 {tp_price} 必须高于当前标记价 {mark_price}"
                    logger.warning(f"[OKX] {symbol} {error_msg}")
                    return OrderResult(success=False, symbol=symbol, error=error_msg)
                if position_side == 'SHORT' and tp_price >= mark_price:
                    error_msg = f"空头止盈价 {tp_price} 必须低于当前标记价 {mark_price}"
                    logger.warning(f"[OKX] {symbol} {error_msg}")
                    return OrderResult(success=False, symbol=symbol, error=error_msg)
            
            order_side = 'sell' if position_side == 'LONG' else 'buy'
            pos_side = side.lower()
            
            # 先取消旧的止盈单
            await self._cancel_algo_orders(okx_symbol, pos_side, cancel_sl=False, cancel_tp=True)
            
            order_data = {
                'instId': okx_symbol,
                'tdMode': 'cross',
                'side': order_side,
                'posSide': pos_side,
                'ordType': 'conditional',
                'sz': str(qty),
                'tpTriggerPx': str(tp_price),
                'tpOrdPx': '-1',  # -1 表示市价
                'algoClOrdId': self._generate_client_order_id(),
                'tag': self.BROKER_CODE,
            }
            
            result = await self._request('POST', '/api/v5/trade/order-algo', data=order_data)
            
            order_info = result.get('data', [{}])[0]
            
            logger.info(f"[OKX] 止盈单已创建 {symbol} {side} trigger={tp_price} algoId={order_info.get('algoId')}")
            
            return OrderResult(
                success=True,
                order_id=order_info.get('algoId', ''),
                symbol=symbol,
                side=order_side.upper(),
                position_side=side.upper(),
                qty=qty,
                price=tp_price,
                status='NEW',
                raw=result
            )
            
        except Exception as e:
            logger.error(f"[OKX] 设置止盈失败 {symbol}: {e}")
            return OrderResult(success=False, symbol=symbol, error=str(e))
    
    async def _cancel_algo_orders(
        self,
        okx_symbol: str,
        pos_side: str,
        cancel_sl: bool = True,
        cancel_tp: bool = True
    ):
        """
        取消指定方向的止盈止损 algo 订单
        
        Args:
            okx_symbol: OKX 格式的交易对 (如 BTC-USDT-SWAP)
            pos_side: 持仓方向 (long/short)
            cancel_sl: 是否取消止损单
            cancel_tp: 是否取消止盈单
        """
        if not cancel_sl and not cancel_tp:
            return
        
        try:
            # 获取所有 algo 订单
            algo_data = await self._request('GET', '/api/v5/trade/orders-algo-pending', params={
                'instType': 'SWAP',
                'instId': okx_symbol,
                'ordType': 'conditional'
            })
            
            algo_orders = algo_data.get('data', [])
            if not algo_orders:
                return
            
            # 筛选需要取消的订单
            orders_to_cancel = []
            for o in algo_orders:
                # 检查持仓方向
                if o.get('posSide') != pos_side:
                    continue
                
                # 检查是否是止损单
                has_sl = o.get('slTriggerPx') and float(o.get('slTriggerPx', 0)) > 0
                # 检查是否是止盈单
                has_tp = o.get('tpTriggerPx') and float(o.get('tpTriggerPx', 0)) > 0
                
                if (cancel_sl and has_sl) or (cancel_tp and has_tp):
                    algo_id = o.get('algoId')
                    if algo_id:
                        orders_to_cancel.append({
                            'instId': okx_symbol,
                            'algoId': algo_id
                        })
            
            if orders_to_cancel:
                # 批量取消 algo 订单
                await self._request('POST', '/api/v5/trade/cancel-algos', data=orders_to_cancel)
                logger.info(f"[OKX] 已取消 {len(orders_to_cancel)} 个 algo 订单 {okx_symbol} {pos_side}")
                
        except Exception as e:
            logger.warning(f"[OKX] 取消 algo 订单失败 {okx_symbol}: {e}")
    
    async def cancel_order(self, symbol: str, order_id: str) -> bool:
        """取消订单"""
        try:
            okx_symbol = self._convert_symbol(symbol)
            
            await self._request('POST', '/api/v5/trade/cancel-order', data={
                'instId': okx_symbol,
                'ordId': order_id
            })
            return True
        except Exception as e:
            logger.error(f"[OKX] 取消订单失败 {symbol} {order_id}: {e}")
            return False
    
    async def cancel_all_orders(self, symbol: str) -> int:
        """取消指定币种的所有订单"""
        try:
            okx_symbol = self._convert_symbol(symbol)
            
            # 获取所有挂单
            orders_data = await self._request('GET', '/api/v5/trade/orders-pending', params={
                'instType': 'SWAP',
                'instId': okx_symbol
            })
            
            orders = orders_data.get('data', [])
            if not orders:
                return 0
            
            # 批量取消
            cancel_data = [{'instId': okx_symbol, 'ordId': o['ordId']} for o in orders]
            await self._request('POST', '/api/v5/trade/cancel-batch-orders', data=cancel_data)
            
            return len(orders)
        except Exception as e:
            logger.error(f"[OKX] 取消所有订单失败 {symbol}: {e}")
            return 0
    
    # ==================== 设置相关 ====================
    
    async def set_leverage(self, symbol: str, leverage: int) -> bool:
        """设置杠杆，如果失败则尝试使用最大支持杠杆"""
        okx_symbol = self._convert_symbol(symbol)
        
        try:
            await self._request('POST', '/api/v5/account/set-leverage', data={
                'instId': okx_symbol,
                'lever': str(leverage),
                'mgnMode': 'cross'
            })
            logger.debug(f"[OKX] 设置杠杆成功 {symbol} {leverage}x")
            return True
        except Exception as e:
            error_msg = str(e)
            # 检查是否是杠杆超限错误
            # OKX 错误码: 51010 (杠杆倍数超过允许范围), 59102 (杠杆超过最大限制)
            leverage_error_codes = ['51010', '59102', '59350']
            is_leverage_error = any(code in error_msg for code in leverage_error_codes)
            is_leverage_keyword = 'lever' in error_msg.lower() or 'maximum' in error_msg.lower()
            
            if is_leverage_error or is_leverage_keyword:
                try:
                    max_leverage = await self._get_max_leverage(okx_symbol)
                    if max_leverage and max_leverage < leverage:
                        logger.info(f"[OKX] {symbol} 杠杆 {leverage}x 超限，使用最大杠杆 {max_leverage}x")
                        await self._request('POST', '/api/v5/account/set-leverage', data={
                            'instId': okx_symbol,
                            'lever': str(max_leverage),
                            'mgnMode': 'cross'
                        })
                        return True
                except Exception as retry_e:
                    logger.warning(f"[OKX] 设置最大杠杆也失败 {symbol}: {retry_e}")
            logger.warning(f"[OKX] 设置杠杆失败 {symbol} {leverage}x: {e}")
            return False
    
    async def _get_max_leverage(self, okx_symbol: str) -> Optional[int]:
        """
        获取交易对最大杠杆
        
        OKX API /api/v5/public/position-tiers 需要以下参数之一:
        - instId: 产品ID (如 BTC-USDT-SWAP)
        - uly: 标的指数 (如 BTC-USDT)
        - instFamily: 交易品种 (如 BTC-USDT)
        
        对于 SWAP 合约，使用 uly 参数更可靠
        """
        try:
            # 从 BTC-USDT-SWAP 提取 uly (标的) = BTC-USDT
            parts = okx_symbol.split('-')
            if len(parts) >= 2:
                uly = f"{parts[0]}-{parts[1]}"  # BTC-USDT
            else:
                uly = okx_symbol
            
            # OKX 通过 position-tiers 获取杠杆档位
            data = await self._request('GET', '/api/v5/public/position-tiers', params={
                'instType': 'SWAP',
                'uly': uly,
                'tdMode': 'cross'
            })
            tiers = data.get('data', [])
            if tiers:
                # 第一档通常有最大杠杆
                return int(tiers[0].get('maxLever', 125))
        except Exception as e:
            logger.debug(f"[OKX] 获取最大杠杆失败 {okx_symbol}: {e}")
        return None
    
    async def set_margin_type(self, symbol: str, margin_type: str) -> bool:
        """设置保证金模式"""
        try:
            okx_symbol = self._convert_symbol(symbol)
            
            # OKX 需要在设置杠杆时指定 mgnMode
            # 这里只是记录意图，实际在开仓时会使用对应的 mgnMode
            logger.info(f"[OKX] 保证金模式将在下单时应用: {margin_type}")
            return True
        except Exception as e:
            logger.warning(f"[OKX] 设置保证金模式失败 {symbol}: {e}")
            return False
    
    # ==================== 市场数据 ====================
    
    async def get_ticker_price(self, symbol: str) -> float:
        """获取最新价格"""
        okx_symbol = self._convert_symbol(symbol)
        data = await self._request('GET', '/api/v5/market/ticker', params={'instId': okx_symbol})
        
        if data.get('data'):
            return float(data['data'][0].get('last', 0))
        return 0.0
    
    async def get_symbol_info(self, symbol: str) -> Dict:
        """
        获取交易对信息
        
        使用全局公共缓存，所有用户共享
        """
        from core.okx_public_cache import get_okx_public_cache
        cache = get_okx_public_cache()
        return await cache.get_instrument_info_async(symbol)
    
    async def _refresh_symbol_cache(self):
        """
        刷新交易对缓存（已废弃，使用全局公共缓存）
        
        保留此方法以兼容旧代码，但实际不再使用实例级缓存
        """
        # 使用全局公共缓存，无需刷新实例级缓存
        pass
    
    # ==================== WebSocket ====================
    
    def create_ws_client(self, uid: str = "", callbacks: Dict[str, callable] = None) -> Any:
        """
        创建 WebSocket 客户端
        
        Args:
            uid: 用户 ID
            callbacks: 回调函数字典
                - on_account: 账户更新回调
                - on_position: 持仓更新回调  
                - on_order: 订单更新回调
        
        Returns:
            OKXWebSocket 实例
        """
        from exchanges.okx.websocket import OKXWebSocket
        
        ws = OKXWebSocket(
            api_key=self.api_key,
            api_secret=self.api_secret,
            passphrase=self.passphrase,
            is_testnet=self.is_testnet,
            uid=uid,
            callbacks=callbacks,
        )
        return ws
    
    # ==================== 辅助方法 ====================
    
    async def _normalize_qty(self, symbol: str, qty: float) -> float:
        """
        规范化数量（币数量 -> 张数）
        
        OKX SWAP 合约的 sz 参数是张数（contracts），不是币数量！
        需要将币数量转换为张数：张数 = 币数量 / ctVal
        
        例如：
        - BTC-USDT-SWAP: ctVal = 0.01，买 0.1 BTC = 10 张
        - ETH-USDT-SWAP: ctVal = 0.1，买 1 ETH = 10 张
        """
        info = await self.get_symbol_info(symbol)
        if not info:
            return qty
        
        # 获取合约面值 (ctVal = 每张合约代表的币数量)
        ct_val = float(info.get('ctVal', 1))
        
        # 将币数量转换为张数
        contracts = qty / ct_val
        
        # OKX 使用 lotSz (最小交易单位，即最小张数)
        lot_sz = float(info.get('lotSz', 1))
        min_sz = float(info.get('minSz', 0))
        
        # 张数向上取整到 lotSz 的倍数
        contracts = max(contracts, min_sz)
        contracts = math.ceil(contracts / lot_sz) * lot_sz
        
        # 张数通常是整数，但某些小币种可能有小数
        decimals = 0
        if lot_sz < 1:
            decimals = len(str(lot_sz).split('.')[-1])
        contracts = round(contracts, decimals)
        
        logger.debug(f"[OKX] {symbol}: {qty} coins -> {contracts} contracts (ctVal={ct_val}, lotSz={lot_sz})")
        
        return contracts
    
    async def _contracts_to_coins(self, symbol: str, contracts: float) -> float:
        """
        将张数转换为币数量
        
        OKX 持仓返回的 pos 是张数，需要转换为币数量
        币数量 = 张数 * ctVal
        
        例如：
        - BTC-USDT-SWAP: ctVal = 0.01，10 张 = 0.1 BTC
        - ETH-USDT-SWAP: ctVal = 0.1，10 张 = 1 ETH
        """
        info = await self.get_symbol_info(symbol)
        if not info:
            return contracts
        
        ct_val = float(info.get('ctVal', 1))
        coins = contracts * ct_val
        
        logger.debug(f"[OKX] {symbol}: {contracts} contracts -> {coins} coins (ctVal={ct_val})")
        
        return coins
    
    async def _normalize_price(self, symbol: str, price: float) -> float:
        """
        规范化价格（使用 Decimal 保持精度，四舍五入到 tick_size）
        
        对于小价格币种，浮点数精度不足，使用 Decimal 计算
        """
        info = await self.get_symbol_info(symbol)
        if not info:
            return float(price)
        
        # OKX 使用 tickSz (最小价格单位)
        tick_sz_str = str(info.get('tickSz', '0.01'))
        
        # 使用 Decimal 进行高精度计算
        price_d = Decimal(str(price))
        tick_d = Decimal(tick_sz_str)
        
        # 四舍五入到 tick_size
        normalized = (price_d / tick_d).quantize(Decimal('1'), rounding=ROUND_HALF_UP) * tick_d
        
        return float(normalized)
    
    async def close(self):
        """关闭连接"""
        # 先获取 session 引用并置空，避免重复关闭
        session = self._session
        self._session = None
        
        if session and not session.closed:
            try:
                await session.close()
            except Exception as e:
                logger.debug(f"[OKX] 关闭 session 时异常: {e}")
            
            # 给 SSL transport 一点时间完成清理
            try:
                await asyncio.sleep(0.25)
            except Exception:
                pass
    
    def close_sync(self):
        """
        同步关闭（安全版本，避免跨事件循环问题）
        
        尝试在新事件循环中关闭 session，如果失败则直接置空。
        """
        if self._session is None or self._session.closed:
            self._session = None
            return
        
        session = self._session
        self._session = None
        
        # 尝试在新事件循环中关闭 session
        try:
            import asyncio
            
            async def _close():
                try:
                    await session.close()
                    await asyncio.sleep(0.1)  # 给 SSL transport 时间清理
                except Exception:
                    pass
            
            # 检查是否有运行中的事件循环
            try:
                loop = asyncio.get_running_loop()
                # 有运行中的循环，不能在这里创建新循环
                # 让 GC 清理，会产生警告但不影响功能
                logger.debug("[OKX] 有运行中的事件循环，跳过同步关闭")
            except RuntimeError:
                # 没有运行中的循环，可以创建新的
                loop = asyncio.new_event_loop()
                try:
                    loop.run_until_complete(_close())
                finally:
                    loop.close()
        except Exception as e:
            logger.debug(f"[OKX] 同步关闭 session 失败: {e}")
    
    def __del__(self):
        """析构函数 - 清理未关闭的 session"""
        if self._session and not self._session.closed:
            # 直接置空，让 GC 清理
            # 在析构时无法 await，尝试创建任务关闭只会导致更多问题
            self._session = None
