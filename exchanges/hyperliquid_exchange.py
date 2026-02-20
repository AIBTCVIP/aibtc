# exchanges/hyperliquid_exchange.py
"""
Hyperliquid 交易所实现 - 使用官方 Python SDK

Hyperliquid API 文档: https://hyperliquid.gitbook.io/hyperliquid-docs/
Hyperliquid 是一个去中心化的永续合约交易所

特点：
- 使用以太坊私钥签名交易（支持 API Wallet 代理模式）
- Symbol 格式: BTC, ETH (不带USDT)
- 所有合约都是 USDC 计价
- 使用官方 hyperliquid-python-sdk 处理签名
"""

import asyncio
import logging
import math
import time
from decimal import Decimal, ROUND_HALF_UP
from typing import Dict, List, Optional, Any
from concurrent.futures import ThreadPoolExecutor

from exchanges.base import (
    BaseExchange,
    OrderResult,
    Position,
    AccountInfo,
)

logger = logging.getLogger(__name__)

# 检查官方 SDK
try:
    from hyperliquid.info import Info
    from hyperliquid.exchange import Exchange
    from hyperliquid.utils import constants
    HAS_HL_SDK = True
except ImportError:
    HAS_HL_SDK = False
    logger.warning("hyperliquid-python-sdk 未安装，请运行: pip install hyperliquid-python-sdk")

# 用于在异步环境中运行同步 SDK
_executor = ThreadPoolExecutor(max_workers=4)


def shutdown_executor():
    """关闭 Hyperliquid 线程池（在程序退出时调用）"""
    global _executor
    if _executor:
        _executor.shutdown(wait=False)
        _executor = None
        logger.info("[HyperliquidExchange] 线程池已关闭")


# P1 Fix: 注册 atexit 确保程序退出时关闭线程池
import atexit
atexit.register(shutdown_executor)


class HyperliquidExchange(BaseExchange):
    """
    Hyperliquid 交易所实现 - 基于官方 SDK
    
    支持两种模式：
    1. 主钱包模式：api_key = 主钱包私钥
    2. API Wallet 模式：api_key = API Wallet 私钥, wallet_address = 主钱包地址
    """
    
    EXCHANGE_NAME = "hyperliquid"
    
    def __init__(
        self,
        api_key: str,  # 私钥 (主钱包或 API Wallet)
        api_secret: str = None,  # 不需要
        is_testnet: bool = False,
        wallet_address: str = None,  # API Wallet 模式需要提供主钱包地址
        **kwargs
    ):
        super().__init__(api_key, api_secret or "", is_testnet=is_testnet, **kwargs)
        
        if not HAS_HL_SDK:
            raise Exception("请安装 hyperliquid-python-sdk: pip install hyperliquid-python-sdk")
        
        self.private_key = api_key
        self.is_testnet = is_testnet
        self.base_url = constants.TESTNET_API_URL if is_testnet else constants.MAINNET_API_URL
        
        # wallet_address: 用于查询账户信息的地址
        # 如果是 API Wallet 模式，这应该是主钱包地址
        # 如果是主钱包模式，可以留空（SDK 会自动从私钥派生）
        self._wallet_address = wallet_address
        
        # 初始化官方 SDK
        self._info: Optional[Info] = None
        self._exchange: Optional[Exchange] = None
        self._init_sdk()
        
        # 缓存
        self._meta_cache: Dict = {}
        self._sz_decimals: Dict[str, int] = {}
        self._asset_to_index: Dict[str, int] = {}
        self._cache_time = 0
        self._cache_ttl = 3600
    
    def _init_sdk(self):
        """初始化官方 SDK"""
        try:
            from eth_account import Account
            
            # Info 客户端 (只读操作)
            self._info = Info(self.base_url, skip_ws=True)
            
            # 从私钥创建 wallet
            wallet = Account.from_key(self.private_key)
            
            # Exchange 客户端 (需要签名的操作)
            # account_address 参数用于 API Wallet 模式 - 指定主钱包地址
            self._exchange = Exchange(
                wallet=wallet,
                base_url=self.base_url,
                account_address=self._wallet_address  # API Wallet 模式的主钱包地址
            )
            
            # 确定最终使用的钱包地址
            # 如果提供了 wallet_address（API Wallet 模式），使用它
            # 否则使用从私钥派生的地址
            if self._wallet_address:
                self.wallet_address = self._wallet_address
            else:
                self.wallet_address = wallet.address
            
            logger.info(f"[Hyperliquid] SDK 初始化成功, wallet={self.wallet_address}, testnet={self.is_testnet}")
            
        except Exception as e:
            logger.error(f"[Hyperliquid] SDK 初始化失败: {e}")
            raise
    
    @property
    def wallet_address(self) -> Optional[str]:
        return self._wallet_address or (self._exchange.wallet.address if self._exchange and self._exchange.wallet else None)
    
    @wallet_address.setter
    def wallet_address(self, value: str):
        self._wallet_address = value
    
    async def _run_sync(self, func, *args, **kwargs):
        """在线程池中运行同步函数（带限速）"""
        from core.rate_limiter import get_rate_limiter
        
        # 使用全局限速器
        rate_limiter = get_rate_limiter("hyperliquid")
        if not await rate_limiter.acquire_async(timeout=30.0):
            raise Exception(f"Hyperliquid API 限速器超时")
        
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(_executor, lambda: func(*args, **kwargs))
    
    def _convert_symbol(self, symbol: str) -> str:
        """将 Binance 格式转为 Hyperliquid 格式: BTCUSDT -> BTC"""
        if symbol.endswith('USDT'):
            return symbol[:-4]
        if symbol.endswith('USDC'):
            return symbol[:-4]
        return symbol
    
    def _parse_symbol(self, hl_symbol: str) -> str:
        """将 Hyperliquid 格式转为 Binance 格式: BTC -> BTCUSDT"""
        if not hl_symbol.endswith('USDT') and not hl_symbol.endswith('USDC'):
            return f"{hl_symbol}USDT"
        return hl_symbol
    
    def normalize_symbol(self, symbol: str) -> str:
        return self._convert_symbol(symbol)
    
    def denormalize_symbol(self, symbol: str) -> str:
        return self._parse_symbol(symbol)
    
    # ==================== 账户相关 ====================
    
    async def get_account(self) -> AccountInfo:
        """获取账户信息"""
        address = self.wallet_address
        if not address:
            raise Exception("钱包地址未设置")
        
        # 使用官方 SDK 获取账户状态
        data = await self._run_sync(self._info.user_state, address)
        
        margin_summary = data.get('marginSummary', {})
        
        total_balance = float(margin_summary.get('accountValue', 0))
        available_balance = float(data.get('withdrawable', 0))
        unrealized_pnl = float(margin_summary.get('totalNtlPos', 0))
        
        # 解析持仓
        positions = []
        for pos in data.get('assetPositions', []):
            position_data = pos.get('position', {})
            szi = float(position_data.get('szi', 0))
            
            if szi == 0:
                continue
            
            positions.append(Position(
                symbol=self._parse_symbol(position_data.get('coin', '')),
                side='LONG' if szi > 0 else 'SHORT',
                qty=abs(szi),
                entry_price=float(position_data.get('entryPx', 0)),
                unrealized_pnl=float(position_data.get('unrealizedPnl', 0)),
                leverage=int(float(position_data.get('leverage', {}).get('value', 1))),
                margin_type='cross' if position_data.get('leverage', {}).get('type') == 'cross' else 'isolated',
                liquidation_price=float(position_data.get('liquidationPx', 0)) if position_data.get('liquidationPx') else 0,
                exchange=self.EXCHANGE_NAME,
                raw=pos
            ))
        
        return AccountInfo(
            total_balance=total_balance,
            available_balance=available_balance,
            unrealized_pnl=unrealized_pnl,
            margin_balance=total_balance,
            positions=positions,
            exchange=self.EXCHANGE_NAME,
            raw=data
        )
    
    async def get_balance(self) -> float:
        """获取可用余额"""
        account = await self.get_account()
        return account.available_balance
    
    async def get_positions(self) -> List[Position]:
        """获取所有持仓"""
        account = await self.get_account()
        return account.positions
    
    async def get_position(self, symbol: str) -> Optional[Position]:
        """获取指定币种的持仓"""
        positions = await self.get_positions()
        for p in positions:
            if p.symbol == symbol or self._convert_symbol(p.symbol) == self._convert_symbol(symbol):
                return p
        return None
    
    # ==================== 下单相关 ====================
    
    async def _refresh_meta(self):
        """刷新元数据缓存"""
        if time.time() - self._cache_time < self._cache_ttl:
            return
        
        try:
            meta = await self._run_sync(self._info.meta)
            self._meta_cache = meta
            
            # 构建映射
            for i, asset in enumerate(meta.get('universe', [])):
                name = asset['name']
                self._sz_decimals[name] = asset.get('szDecimals', 0)
                self._asset_to_index[name] = i
            
            self._cache_time = time.time()
        except Exception as e:
            logger.error(f"[Hyperliquid] 刷新元数据失败: {e}")
    
    async def market_open(
        self,
        symbol: str,
        side: str,
        qty: float,
        leverage: int = None,
    ) -> OrderResult:
        """市价开仓"""
        try:
            hl_symbol = self._convert_symbol(symbol)
            await self._refresh_meta()
            
            # 设置杠杆
            if leverage:
                await self.set_leverage(symbol, leverage)
            
            # 规范化数量
            qty = self._normalize_qty(hl_symbol, qty)
            
            # 使用官方 SDK 下单
            is_buy = side.upper() == 'LONG'
            
            # 获取当前价格用于市价单（SDK 需要价格来计算滑点）
            current_price = await self.get_ticker_price(symbol)
            if not current_price:
                return OrderResult(success=False, symbol=symbol, error="无法获取当前价格")
            
            logger.info(f"[Hyperliquid] market_open 参数: symbol={hl_symbol}, is_buy={is_buy}, qty={qty}, price={current_price}")
            
            result = await self._run_sync(
                self._exchange.market_open,
                hl_symbol,
                is_buy,
                qty,
                current_price,  # px - SDK 需要价格来计算滑点
                0.05,  # slippage 5%
            )
            
            logger.info(f"[Hyperliquid] market_open result: {result}")
            
            # 解析结果
            if result.get('status') == 'ok':
                response = result.get('response', {})
                data = response.get('data', {})
                statuses = data.get('statuses', [{}])
                
                if statuses and statuses[0].get('filled'):
                    filled = statuses[0]['filled']
                    return OrderResult(
                        success=True,
                        order_id=str(filled.get('oid', '')),
                        symbol=symbol,
                        side='BUY' if is_buy else 'SELL',
                        position_side=side.upper(),
                        qty=float(filled.get('totalSz', qty)),
                        avg_price=float(filled.get('avgPx', 0)),
                        status='FILLED',
                        raw=result
                    )
                elif statuses and statuses[0].get('error'):
                    return OrderResult(
                        success=False,
                        symbol=symbol,
                        error=statuses[0]['error'],
                        raw=result
                    )
            
            return OrderResult(
                success=False,
                symbol=symbol,
                error=str(result),
                raw=result
            )
            
        except Exception as e:
            logger.error(f"[Hyperliquid] 开仓失败 {symbol}: {e}")
            return OrderResult(success=False, symbol=symbol, error=str(e))
    
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
            hl_symbol = self._convert_symbol(symbol)
            await self._refresh_meta()
            
            if leverage:
                await self.set_leverage(symbol, leverage)
            
            qty = self._normalize_qty(hl_symbol, qty)
            price = self._normalize_price(price)
            
            is_buy = side.upper() == 'LONG'
            
            # 使用官方 SDK
            result = await self._run_sync(
                self._exchange.order,
                hl_symbol,
                is_buy,
                qty,
                price,
                {"limit": {"tif": "Gtc"}},  # order_type
                False,  # reduce_only
            )
            
            logger.debug(f"[Hyperliquid] limit_open result: {result}")
            
            if result.get('status') == 'ok':
                response = result.get('response', {})
                data = response.get('data', {})
                statuses = data.get('statuses', [{}])
                
                if statuses and (statuses[0].get('resting') or statuses[0].get('filled')):
                    order_data = statuses[0].get('resting', statuses[0].get('filled', {}))
                    return OrderResult(
                        success=True,
                        order_id=str(order_data.get('oid', '')),
                        symbol=symbol,
                        side='BUY' if is_buy else 'SELL',
                        position_side=side.upper(),
                        qty=qty,
                        price=price,
                        status='NEW' if statuses[0].get('resting') else 'FILLED',
                        raw=result
                    )
                elif statuses and statuses[0].get('error'):
                    return OrderResult(success=False, symbol=symbol, error=statuses[0]['error'], raw=result)
            
            return OrderResult(success=False, symbol=symbol, error=str(result), raw=result)
            
        except Exception as e:
            logger.error(f"[Hyperliquid] 限价开仓失败 {symbol}: {e}")
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
            hl_symbol = self._convert_symbol(symbol)
            await self._refresh_meta()
            
            # 如果没指定数量，获取当前持仓数量
            if qty is None:
                position = await self.get_position(symbol)
                if position and position.side == side.upper():
                    qty = position.qty
                else:
                    return OrderResult(success=False, symbol=symbol, error="无持仓")
            
            qty = self._normalize_qty(hl_symbol, qty)
            
            # 获取当前价格
            current_price = await self.get_ticker_price(symbol)
            
            # 使用官方 SDK 平仓
            result = await self._run_sync(
                self._exchange.market_close,
                hl_symbol,
                qty,
                current_price,  # px
                0.05,  # slippage 5%
            )
            
            logger.debug(f"[Hyperliquid] market_close result: {result}")
            
            if result.get('status') == 'ok':
                response = result.get('response', {})
                data = response.get('data', {})
                statuses = data.get('statuses', [{}])
                
                if statuses and statuses[0].get('filled'):
                    filled = statuses[0]['filled']
                    
                    # 平仓成功后取消挂单（减仓时不取消）
                    if cancel_tp_sl:
                        try:
                            cancelled = await self.cancel_all_orders(symbol)
                            if cancelled > 0:
                                logger.info(f"[Hyperliquid] {symbol} 平仓后取消了 {cancelled} 个挂单")
                        except Exception as cancel_err:
                            logger.warning(f"[Hyperliquid] {symbol} 取消挂单失败: {cancel_err}")
                    
                    return OrderResult(
                        success=True,
                        order_id=str(filled.get('oid', '')),
                        symbol=symbol,
                        side='BUY' if side.upper() == 'SHORT' else 'SELL',
                        position_side=side.upper(),
                        qty=float(filled.get('totalSz', qty)),
                        avg_price=float(filled.get('avgPx', 0)),
                        status='FILLED',
                        raw=result
                    )
                elif statuses and statuses[0].get('error'):
                    return OrderResult(success=False, symbol=symbol, error=statuses[0]['error'], raw=result)
            
            return OrderResult(success=False, symbol=symbol, error=str(result), raw=result)
            
        except Exception as e:
            logger.error(f"[Hyperliquid] 平仓失败 {symbol}: {e}")
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
            hl_symbol = self._convert_symbol(symbol)
            await self._refresh_meta()
            
            qty = self._normalize_qty(hl_symbol, qty)
            stop_price = self._normalize_price(stop_price)
            position_side = side.upper()
            
            # 从 Redis 获取 WS 标记价格进行验证
            mark_price = self._get_mark_price_from_redis(symbol)
            if mark_price and mark_price > 0:
                # 验证止损价格是否有效（不会立即触发）
                if position_side == 'LONG' and stop_price >= mark_price:
                    error_msg = f"多头止损价 {stop_price} 必须低于当前标记价 {mark_price}"
                    logger.warning(f"[Hyperliquid] {symbol} {error_msg}")
                    return OrderResult(success=False, symbol=symbol, error=error_msg)
                if position_side == 'SHORT' and stop_price <= mark_price:
                    error_msg = f"空头止损价 {stop_price} 必须高于当前标记价 {mark_price}"
                    logger.warning(f"[Hyperliquid] {symbol} {error_msg}")
                    return OrderResult(success=False, symbol=symbol, error=error_msg)
            
            # 先取消旧的止损单
            logger.info(f"[Hyperliquid] 设置止损前先取消旧止损单 {hl_symbol}")
            await self._cancel_tp_sl_orders(hl_symbol, cancel_sl=True, cancel_tp=False)
            
            # 止损方向与持仓相反
            is_buy = position_side == 'SHORT'
            
            # 使用官方 SDK 设置止损
            result = await self._run_sync(
                self._exchange.order,
                hl_symbol,
                is_buy,
                qty,
                stop_price,
                {"trigger": {"isMarket": True, "triggerPx": float(stop_price), "tpsl": "sl"}},
                True,  # reduce_only
            )
            
            logger.debug(f"[Hyperliquid] set_stop_loss result: {result}")
            
            if result.get('status') == 'ok':
                response = result.get('response', {})
                data = response.get('data', {})
                statuses = data.get('statuses', [{}])
                
                if statuses and (statuses[0].get('resting') or statuses[0].get('filled')):
                    order_data = statuses[0].get('resting', statuses[0].get('filled', {}))
                    logger.info(f"[Hyperliquid] 止损单已创建 {symbol} {side} trigger={stop_price}")
                    return OrderResult(
                        success=True,
                        order_id=str(order_data.get('oid', '')),
                        symbol=symbol,
                        side='BUY' if is_buy else 'SELL',
                        position_side=position_side,
                        qty=qty,
                        price=stop_price,
                        status='NEW',
                        raw=result
                    )
                elif statuses and statuses[0].get('error'):
                    return OrderResult(success=False, symbol=symbol, error=statuses[0]['error'], raw=result)
            
            return OrderResult(success=False, symbol=symbol, error=str(result), raw=result)
            
        except Exception as e:
            logger.error(f"[Hyperliquid] 设置止损失败 {symbol}: {e}")
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
            logger.debug(f"[Hyperliquid] 获取 Redis 标记价格失败 {symbol}: {e}")
        
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
            hl_symbol = self._convert_symbol(symbol)
            await self._refresh_meta()
            
            qty = self._normalize_qty(hl_symbol, qty)
            tp_price = self._normalize_price(tp_price)
            
            # 验证止盈价格不会立即触发
            position_side = side.upper()
            mark_price = self._get_mark_price_from_redis(symbol)
            if mark_price and mark_price > 0:
                if position_side == 'LONG' and tp_price <= mark_price:
                    logger.warning(f"[Hyperliquid] 多头止盈价 {tp_price} <= 标记价 {mark_price}, 会立即触发")
                    return OrderResult(
                        success=False,
                        symbol=symbol,
                        error=f"多头止盈价必须高于当前标记价 ({mark_price:.4f})"
                    )
                elif position_side == 'SHORT' and tp_price >= mark_price:
                    logger.warning(f"[Hyperliquid] 空头止盈价 {tp_price} >= 标记价 {mark_price}, 会立即触发")
                    return OrderResult(
                        success=False,
                        symbol=symbol,
                        error=f"空头止盈价必须低于当前标记价 ({mark_price:.4f})"
                    )
            
            # 先取消旧的止盈单
            logger.info(f"[Hyperliquid] 设置止盈前先取消旧止盈单 {hl_symbol}")
            await self._cancel_tp_sl_orders(hl_symbol, cancel_sl=False, cancel_tp=True)
            
            is_buy = side.upper() == 'SHORT'
            
            result = await self._run_sync(
                self._exchange.order,
                hl_symbol,
                is_buy,
                qty,
                tp_price,
                {"trigger": {"isMarket": True, "triggerPx": float(tp_price), "tpsl": "tp"}},
                True,  # reduce_only
            )
            
            logger.debug(f"[Hyperliquid] set_take_profit result: {result}")
            
            if result.get('status') == 'ok':
                response = result.get('response', {})
                data = response.get('data', {})
                statuses = data.get('statuses', [{}])
                
                if statuses and (statuses[0].get('resting') or statuses[0].get('filled')):
                    order_data = statuses[0].get('resting', statuses[0].get('filled', {}))
                    logger.info(f"[Hyperliquid] 止盈单已创建 {symbol} {side} trigger={tp_price}")
                    return OrderResult(
                        success=True,
                        order_id=str(order_data.get('oid', '')),
                        symbol=symbol,
                        side='BUY' if is_buy else 'SELL',
                        position_side=side.upper(),
                        qty=qty,
                        price=tp_price,
                        status='NEW',
                        raw=result
                    )
                elif statuses and statuses[0].get('error'):
                    return OrderResult(success=False, symbol=symbol, error=statuses[0]['error'], raw=result)
            
            return OrderResult(success=False, symbol=symbol, error=str(result), raw=result)
            
        except Exception as e:
            logger.error(f"[Hyperliquid] 设置止盈失败 {symbol}: {e}")
            return OrderResult(success=False, symbol=symbol, error=str(e))
    
    async def _cancel_tp_sl_orders(
        self,
        hl_symbol: str,
        cancel_sl: bool = True,
        cancel_tp: bool = True
    ):
        """取消指定币种的止盈止损订单"""
        if not cancel_sl and not cancel_tp:
            return
        
        address = self.wallet_address
        if not address:
            return
        
        try:
            # 使用 frontend_open_orders 获取完整订单信息（包含 orderType）
            orders_data = await self._run_sync(self._info.frontend_open_orders, address)
            
            logger.debug(f"[Hyperliquid] 当前挂单数量: {len(orders_data)}")
            
            orders_to_cancel = []
            for o in orders_data:
                if o.get('coin') != hl_symbol:
                    continue
                
                order_type = o.get('orderType', '')
                is_trigger = o.get('isTrigger', False)
                
                # Hyperliquid 的止盈止损订单类型：
                # - "Stop Market" / "Stop Limit" (止损)
                # - "Take Profit Market" / "Take Profit Limit" (止盈)
                is_sl = 'Stop' in order_type and 'Take' not in order_type
                is_tp = 'Take Profit' in order_type
                
                logger.debug(f"[Hyperliquid] 订单: oid={o.get('oid')}, type={order_type}, is_trigger={is_trigger}, is_sl={is_sl}, is_tp={is_tp}")
                
                if (cancel_sl and is_sl) or (cancel_tp and is_tp):
                    oid = o.get('oid')
                    if oid:
                        orders_to_cancel.append({"coin": hl_symbol, "oid": int(oid)})
                        logger.info(f"[Hyperliquid] 准备取消订单: oid={oid}, type={order_type}")
            
            if orders_to_cancel:
                for order in orders_to_cancel:
                    try:
                        await self._run_sync(
                            self._exchange.cancel,
                            order['coin'],
                            order['oid']
                        )
                        logger.info(f"[Hyperliquid] 已取消订单 oid={order['oid']}")
                    except Exception as cancel_e:
                        logger.warning(f"[Hyperliquid] 取消订单 {order['oid']} 失败: {cancel_e}")
                logger.info(f"[Hyperliquid] 已取消 {len(orders_to_cancel)} 个止盈止损单 {hl_symbol}")
            else:
                logger.debug(f"[Hyperliquid] {hl_symbol} 没有需要取消的止盈止损单")
                
        except Exception as e:
            logger.warning(f"[Hyperliquid] 取消止盈止损单失败 {hl_symbol}: {e}")
    
    async def cancel_order(self, symbol: str, order_id: str) -> bool:
        """取消订单"""
        try:
            hl_symbol = self._convert_symbol(symbol)
            await self._run_sync(self._exchange.cancel, hl_symbol, int(order_id))
            return True
        except Exception as e:
            logger.error(f"[Hyperliquid] 取消订单失败 {symbol} {order_id}: {e}")
            return False
    
    async def cancel_all_orders(self, symbol: str) -> int:
        """取消指定币种的所有订单"""
        try:
            hl_symbol = self._convert_symbol(symbol)
            address = self.wallet_address
            
            if not address:
                return 0
            
            orders_data = await self._run_sync(self._info.open_orders, address)
            orders_to_cancel = [o for o in orders_data if o.get('coin') == hl_symbol]
            
            if not orders_to_cancel:
                return 0
            
            for order in orders_to_cancel:
                await self._run_sync(self._exchange.cancel, hl_symbol, int(order['oid']))
            
            return len(orders_to_cancel)
        except Exception as e:
            logger.error(f"[Hyperliquid] 取消所有订单失败 {symbol}: {e}")
            return 0
    
    # ==================== 设置相关 ====================
    
    async def set_leverage(self, symbol: str, leverage: int) -> bool:
        """设置杠杆，如果失败则尝试使用最大支持杠杆"""
        hl_symbol = self._convert_symbol(symbol)
        
        try:
            result = await self._run_sync(
                self._exchange.update_leverage,
                leverage,
                hl_symbol,
                True  # is_cross
            )
            
            logger.debug(f"[Hyperliquid] set_leverage result: {result}")
            if result.get('status') == 'ok':
                return True
            
            # 检查是否有错误信息
            error_msg = str(result)
            raise Exception(error_msg)
        except Exception as e:
            error_msg = str(e)
            # 检查是否是杠杆超限错误
            if 'leverage' in error_msg.lower() or 'max' in error_msg.lower():
                try:
                    max_leverage = await self._get_max_leverage(hl_symbol)
                    if max_leverage and max_leverage < leverage:
                        logger.info(f"[Hyperliquid] {symbol} 杠杆 {leverage}x 超限，使用最大杠杆 {max_leverage}x")
                        result = await self._run_sync(
                            self._exchange.update_leverage,
                            max_leverage,
                            hl_symbol,
                            True
                        )
                        return result.get('status') == 'ok'
                except Exception as retry_e:
                    logger.warning(f"[Hyperliquid] 设置最大杠杆也失败 {symbol}: {retry_e}")
            logger.warning(f"[Hyperliquid] 设置杠杆失败 {symbol} {leverage}x: {e}")
            return False
    
    async def _get_max_leverage(self, hl_symbol: str) -> int:
        """获取交易对最大杠杆"""
        try:
            await self._refresh_meta()
            if self._meta:
                universe = self._meta.get('universe', [])
                for asset in universe:
                    if asset.get('name') == hl_symbol:
                        return int(asset.get('maxLeverage', 50))
        except Exception as e:
            logger.debug(f"[Hyperliquid] 获取最大杠杆失败 {hl_symbol}: {e}")
        return None
    
    async def set_margin_type(self, symbol: str, margin_type: str) -> bool:
        """设置保证金模式"""
        try:
            hl_symbol = self._convert_symbol(symbol)
            is_cross = margin_type.lower() == 'cross'
            
            result = await self._run_sync(
                self._exchange.update_leverage,
                10,  # 需要同时指定杠杆
                hl_symbol,
                is_cross
            )
            
            return result.get('status') == 'ok'
        except Exception as e:
            logger.warning(f"[Hyperliquid] 设置保证金模式失败 {symbol}: {e}")
            return False
    
    # ==================== 市场数据 ====================
    
    async def get_ticker_price(self, symbol: str) -> float:
        """获取最新价格"""
        hl_symbol = self._convert_symbol(symbol)
        data = await self._run_sync(self._info.all_mids)
        return float(data.get(hl_symbol, 0))
    
    async def get_symbol_info(self, symbol: str) -> Dict:
        """获取交易对信息"""
        await self._refresh_meta()
        hl_symbol = self._convert_symbol(symbol)
        
        for asset in self._meta_cache.get('universe', []):
            if asset.get('name') == hl_symbol:
                return asset
        
        return {}
    
    # ==================== 辅助方法 ====================
    
    def _normalize_qty(self, hl_symbol: str, qty: float) -> float:
        """规范化数量"""
        sz_decimals = self._sz_decimals.get(hl_symbol, 0)
        return round(qty, sz_decimals)
    
    def _normalize_price(self, price: float) -> float:
        """
        规范化价格 - Hyperliquid 使用 5 位有效数字
        
        使用 Decimal 保持精度
        """
        price_d = Decimal(str(price))
        
        if price >= 1:
            decimals = max(0, 5 - len(str(int(price))))
        else:
            decimals = 5
        
        quantize_str = '0.' + '0' * decimals if decimals > 0 else '1'
        normalized = price_d.quantize(Decimal(quantize_str), rounding=ROUND_HALF_UP)
        
        return float(normalized)
    
    # ==================== WebSocket ====================
    
    def create_ws_client(self, callbacks: Dict[str, callable] = None) -> Any:
        """创建 WebSocket 客户端"""
        logger.warning("[Hyperliquid] WebSocket 暂未实现")
        return None
    
    async def close(self):
        """关闭连接"""
        pass  # SDK 不需要显式关闭
    
    def close_sync(self):
        """同步关闭（安全版本）"""
        pass  # SDK 不需要显式关闭
