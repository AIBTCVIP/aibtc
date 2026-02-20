# exchanges/bitget_exchange.py
"""
Bitget 交易所实现

使用 Bitget 官方 V2 API
API 文档: https://bitgetlimited.github.io/apidoc/en/mix/

Bitget 合约产品类型:
- USDT-FUTURES: USDT 永续合约
- COIN-FUTURES: 币本位永续合约
- USDC-FUTURES: USDC 永续合约
"""

import asyncio
import hashlib
import hmac
import base64
import json
import logging
import math
import time
from decimal import Decimal, ROUND_HALF_UP
from typing import Dict, List, Optional, Any
from concurrent.futures import ThreadPoolExecutor

import requests

from exchanges.base import (
    BaseExchange,
    OrderResult,
    Position,
    AccountInfo,
)

logger = logging.getLogger(__name__)


class BitgetExchange(BaseExchange):
    """
    Bitget 交易所实现
    
    使用 Bitget 官方 V2 REST API
    支持 USDT 永续合约
    """
    
    EXCHANGE_NAME = "bitget"
    
    # API URLs
    API_URL = "https://api.bitget.com"
    
    # 产品类型
    PRODUCT_TYPE = "USDT-FUTURES"
    MARGIN_COIN = "USDT"
    
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
            raise ValueError("Bitget requires passphrase")
        
        self._api_key = api_key
        self._api_secret = api_secret
        self._passphrase = passphrase
        self._is_testnet = is_testnet
        
        # 测试网/模拟盘配置
        # 
        # 重要：Bitget 有两种模拟交易模式，我们使用模式1：
        # 
        # 模式 1 (经典模拟盘 - Demo Trading Environment)：
        #   - 在 https://www.bitget.com/zh-CN/futures/BTCUSDT 页面点击右上角"模拟交易"
        #   - 在模拟盘环境中创建 API Key
        #   - API 请求头需要添加 paptrading: 1
        #   - 产品类型使用正常的 USDT-FUTURES (不是 SUSDT-FUTURES!)
        #   - 保证金币种使用 USDT (不是 SUSDT!)
        # 
        # 模式 2 (Demo Coin - 在正式环境中使用模拟币对)：
        #   - 不需要 paptrading 请求头
        #   - 使用模拟币对符号如 SBTCSUSDT
        #   - 产品类型使用 SUSDT-FUTURES
        #   - 保证金币种使用 SUSDT
        # 
        # 我们使用模式 1，因为它更接近真实交易环境
        if is_testnet:
            # 模式 1：使用正常的 productType，通过 paptrading 头区分模拟盘
            self.PRODUCT_TYPE = "USDT-FUTURES"  # 模拟盘仍使用 USDT-FUTURES
            self.MARGIN_COIN = "USDT"  # 模拟盘仍使用 USDT
        
        # 线程池用于同步转异步
        self._executor = ThreadPoolExecutor(max_workers=5)
        
        # 缓存交易对信息
        self._symbol_info_cache: Dict[str, Dict] = {}
        self._cache_time = 0
        self._cache_ttl = 3600  # 1小时缓存
        
        logger.info(f"[Bitget] 初始化成功, testnet={is_testnet}, productType={self.PRODUCT_TYPE}, marginCoin={self.MARGIN_COIN}")
    
    # ==================== HTTP 请求 ====================
    
    def _sign(self, timestamp: str, method: str, request_path: str, body: str = "") -> str:
        """生成签名"""
        message = timestamp + method.upper() + request_path + body
        mac = hmac.new(
            self._api_secret.encode('utf-8'),
            message.encode('utf-8'),
            hashlib.sha256
        )
        return base64.b64encode(mac.digest()).decode('utf-8')
    
    def _get_headers(self, method: str, request_path: str, body: str = "") -> dict:
        """生成请求头"""
        timestamp = str(int(time.time() * 1000))
        sign = self._sign(timestamp, method, request_path, body)
        
        headers = {
            "ACCESS-KEY": self._api_key,
            "ACCESS-SIGN": sign,
            "ACCESS-TIMESTAMP": timestamp,
            "ACCESS-PASSPHRASE": self._passphrase,
            "Content-Type": "application/json",
            "locale": "en-US"
        }
        
        # 模拟盘需要添加 paptrading 请求头
        if self._is_testnet:
            headers["paptrading"] = "1"
        
        return headers
    
    def _request(self, method: str, path: str, params: dict = None) -> dict:
        """发送 HTTP 请求（带 API Key 级别限速）"""
        from core.rate_limiter import get_bitget_rate_limiter
        
        # 使用 API Key 级别限速器
        rate_limiter = get_bitget_rate_limiter(self.api_key)
        if not rate_limiter.acquire(endpoint=path, timeout=30.0):
            raise Exception(f"Bitget API 限速器超时: {method} {path}")
        
        url = self.API_URL + path
        body = ""
        
        if method == "GET" and params:
            query = "&".join([f"{k}={v}" for k, v in params.items()])
            path = f"{path}?{query}"
            url = self.API_URL + path
        elif method == "POST" and params:
            body = json.dumps(params)
        
        headers = self._get_headers(method, path, body)
        
        # 调试日志：打印请求信息
        logger.debug(f"[Bitget] 请求: {method} {url}")
        logger.debug(f"[Bitget] Headers: paptrading={headers.get('paptrading', 'N/A')}")
        if body:
            logger.debug(f"[Bitget] Body: {body}")
        
        try:
            if method == "GET":
                response = requests.get(url, headers=headers, timeout=10)
            else:
                response = requests.post(url, data=body, headers=headers, timeout=10)
            
            data = response.json()
            
            # 调试日志：打印完整响应
            logger.debug(f"[Bitget] 响应: {data}")
            
            if data.get("code") != "00000":
                error_msg = data.get("msg", "Unknown error")
                error_code = data.get("code", "")
                raise Exception(f"API Error(code={error_code}): {error_msg}")
            
            return data.get("data", {})
            
        except requests.exceptions.RequestException as e:
            raise Exception(f"Request failed: {e}")
    
    async def _run_sync(self, func, *args, **kwargs):
        """在线程池中运行同步函数"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            self._executor,
            lambda: func(*args, **kwargs)
        )
    
    def _convert_symbol(self, symbol: str) -> str:
        """
        将 Binance 格式转为 Bitget 格式
        BTCUSDT -> BTCUSDT
        """
        return symbol.upper()
    
    def _parse_symbol(self, bitget_symbol: str) -> str:
        """
        将 Bitget 格式转为标准格式
        ETHUSDT_UMCBL -> ETHUSDT
        ETHUSDT -> ETHUSDT
        """
        # 去掉可能的后缀（如 _UMCBL, _DMCBL 等）
        if "_" in bitget_symbol:
            return bitget_symbol.split("_")[0].upper()
        return bitget_symbol.upper()
    
    # ==================== 账户相关 ====================
    
    async def get_account(self) -> AccountInfo:
        """获取账户信息"""
        try:
            # 获取账户列表
            data = await self._run_sync(
                self._request, "GET", "/api/v2/mix/account/accounts",
                {"productType": self.PRODUCT_TYPE}
            )
            
            logger.debug(f"[Bitget] get_account 返回数据: {data}")
            
            # 找到对应的保证金账户
            total_balance = 0.0
            available_balance = 0.0
            unrealized_pnl = 0.0
            matched_account = None
            
            if isinstance(data, list):
                for account in data:
                    account_margin_coin = account.get("marginCoin", "")
                    # 大小写不敏感匹配
                    if account_margin_coin.upper() == self.MARGIN_COIN.upper():
                        matched_account = account
                        break
                
                # 如果没找到匹配的，使用第一个账户
                if not matched_account and data:
                    matched_account = data[0]
                    logger.warning(f"[Bitget] 未找到 {self.MARGIN_COIN} 账户，使用第一个账户: {matched_account.get('marginCoin')}")
                
                if matched_account:
                    # 模拟盘可能使用不同的字段名
                    total_balance = float(matched_account.get("usdtEquity", 0) or matched_account.get("equity", 0) or 0)
                    available_balance = float(matched_account.get("available", 0) or matched_account.get("crossedMaxAvailable", 0) or 0)
                    unrealized_pnl = float(matched_account.get("unrealizedPL", 0) or 0)
                    logger.info(f"[Bitget] 账户信息: total={total_balance}, available={available_balance}, unrealizedPL={unrealized_pnl}")
            
            # 获取持仓
            positions = await self.get_positions()
            
            return AccountInfo(
                total_balance=total_balance,
                available_balance=available_balance,
                unrealized_pnl=unrealized_pnl,
                margin_balance=total_balance,
                positions=positions,
                exchange=self.EXCHANGE_NAME,
                raw=data if isinstance(data, dict) else {"accounts": data}
            )
        except Exception as e:
            logger.error(f"[Bitget] 获取账户信息失败: {e}")
            return AccountInfo(
                total_balance=0,
                available_balance=0,
                unrealized_pnl=0,
                margin_balance=0,
                positions=[],
                exchange=self.EXCHANGE_NAME,
                raw={}
            )
    
    async def get_balance(self) -> float:
        """获取可用余额"""
        try:
            params = {"productType": self.PRODUCT_TYPE}
            logger.info(f"[Bitget] 获取账户请求参数: {params}, is_testnet={self._is_testnet}")
            
            data = await self._run_sync(
                self._request, "GET", "/api/v2/mix/account/accounts",
                params
            )
            
            logger.info(f"[Bitget] 获取账户返回数据: {data}")
            
            if isinstance(data, list):
                for account in data:
                    account_margin_coin = account.get("marginCoin", "")
                    logger.info(f"[Bitget] 账户 marginCoin={account_margin_coin}, 期望={self.MARGIN_COIN}")
                    # 大小写不敏感匹配
                    if account_margin_coin.upper() == self.MARGIN_COIN.upper():
                        available = float(account.get("available", 0))
                        logger.info(f"[Bitget] 获取余额成功: {available} {self.MARGIN_COIN}")
                        return available
                
                # 如果没有找到匹配的账户，尝试返回第一个账户的余额
                if data:
                    first_account = data[0]
                    available = float(first_account.get("available", 0))
                    logger.warning(f"[Bitget] 未找到 {self.MARGIN_COIN} 账户，使用第一个账户: marginCoin={first_account.get('marginCoin')}, available={available}")
                    return available
            
            logger.warning(f"[Bitget] 账户数据格式异常或为空: {type(data)}")
            return 0.0
        except Exception as e:
            logger.error(f"[Bitget] 获取余额失败: {e}")
            return 0.0
    
    async def get_positions(self) -> List[Position]:
        """获取所有持仓"""
        try:
            # 模拟盘不支持 marginCoin 参数
            params = {"productType": self.PRODUCT_TYPE}
            if not self._is_testnet:
                params["marginCoin"] = self.MARGIN_COIN
            
            data = await self._run_sync(
                self._request, "GET", "/api/v2/mix/position/all-position",
                params
            )
            
            logger.debug(f"[Bitget] get_positions 原始数据: {data}")
            
            positions = []
            if not isinstance(data, list):
                logger.warning(f"[Bitget] get_positions 数据不是列表: {type(data)}")
                return positions
            
            for p in data:
                total = float(p.get("total", 0))
                if total == 0:
                    continue
                
                # holdSide: long/short
                hold_side = p.get("holdSide", "").upper()
                raw_symbol = p.get("symbol", "")
                parsed_symbol = self._parse_symbol(raw_symbol)
                
                # 提取盈亏平衡价
                bep = p.get("breakEvenPrice")
                break_even = float(bep) if bep else 0
                
                logger.debug(f"[Bitget] 持仓: raw_symbol={raw_symbol}, parsed_symbol={parsed_symbol}, side={hold_side}, qty={total}, breakEvenPrice={bep}")
                
                positions.append(Position(
                    symbol=parsed_symbol,
                    side=hold_side,
                    qty=total,
                    entry_price=float(p.get("openPriceAvg", 0)),
                    unrealized_pnl=float(p.get("unrealizedPL", 0)),
                    leverage=int(float(p.get("leverage", 1))),
                    margin_type='cross' if p.get("marginMode") == 'crossed' else 'isolated',
                    liquidation_price=float(p.get("liquidationPrice", 0)) if p.get("liquidationPrice") else 0,
                    break_even_price=break_even,
                    exchange=self.EXCHANGE_NAME,
                    raw=p
                ))
            
            logger.info(f"[Bitget] 获取到 {len(positions)} 个持仓: {[(p.symbol, p.side, p.qty) for p in positions]}")
            return positions
        except Exception as e:
            logger.error(f"[Bitget] 获取持仓失败: {e}")
            return []
    
    async def get_position(self, symbol: str) -> Optional[Position]:
        """获取指定币种的持仓"""
        positions = await self.get_positions()
        # 标准化输入的 symbol
        normalized_symbol = symbol.upper()
        for p in positions:
            # _parse_symbol 已经返回标准格式，直接比较
            if p.symbol == normalized_symbol:
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
            bitget_symbol = self._convert_symbol(symbol)
            
            # 设置杠杆（如果指定）
            if leverage:
                await self.set_leverage(symbol, leverage)
            
            # 规范化数量
            qty = await self._normalize_qty(symbol, qty)
            
            # V2 API 参数
            # side: buy/sell
            # tradeSide: open/close
            order_side = "buy" if side.upper() == "LONG" else "sell"
            
            params = {
                "symbol": bitget_symbol,
                "productType": self.PRODUCT_TYPE,
                "marginMode": "crossed",
                "marginCoin": self.MARGIN_COIN,  # V2 API 必须包含 marginCoin
                "size": str(qty),
                "side": order_side,
                "tradeSide": "open",
                "orderType": "market"
            }
            
            result = await self._run_sync(
                self._request, "POST", "/api/v2/mix/order/place-order", params
            )
            
            order_id = result.get("orderId", "") if isinstance(result, dict) else ""
            
            logger.info(f"[Bitget] 市价开仓成功 {symbol} {side} qty={qty} orderId={order_id}")
            
            return OrderResult(
                success=True,
                order_id=str(order_id),
                symbol=symbol,
                side=order_side.upper(),
                position_side=side.upper(),
                qty=qty,
                status='FILLED',
                raw=result
            )
            
        except Exception as e:
            logger.error(f"[Bitget] 开仓失败 {symbol}: {e}")
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
            bitget_symbol = self._convert_symbol(symbol)
            
            if leverage:
                await self.set_leverage(symbol, leverage)
            
            qty = await self._normalize_qty(symbol, qty)
            price = await self._normalize_price(symbol, price)
            
            order_side = "buy" if side.upper() == "LONG" else "sell"
            
            params = {
                "symbol": bitget_symbol,
                "productType": self.PRODUCT_TYPE,
                "marginMode": "crossed",
                "marginCoin": self.MARGIN_COIN,  # V2 API 必须包含 marginCoin
                "size": str(qty),
                "price": str(price),
                "side": order_side,
                "tradeSide": "open",
                "orderType": "limit"
            }
            
            result = await self._run_sync(
                self._request, "POST", "/api/v2/mix/order/place-order", params
            )
            
            order_id = result.get("orderId", "") if isinstance(result, dict) else ""
            
            return OrderResult(
                success=True,
                order_id=str(order_id),
                symbol=symbol,
                side=order_side.upper(),
                position_side=side.upper(),
                qty=qty,
                price=price,
                status='NEW',
                raw=result
            )
            
        except Exception as e:
            logger.error(f"[Bitget] 限价开仓失败 {symbol}: {e}")
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
            bitget_symbol = self._convert_symbol(symbol)
            
            # 先从 API 获取当前持仓，确认持仓存在
            all_positions = await self.get_positions()
            logger.info(f"[Bitget] 平仓前检查持仓: {[(p.symbol, p.side, p.qty) for p in all_positions]}")
            
            # 查找匹配的持仓
            position = None
            for p in all_positions:
                if p.symbol == symbol.upper() and p.side == side.upper():
                    position = p
                    break
            
            if not position:
                logger.warning(f"[Bitget] 未找到 {symbol} {side} 持仓，现有持仓: {[(p.symbol, p.side) for p in all_positions]}")
                return OrderResult(success=False, symbol=symbol, error=f"无 {side} 持仓")
            
            # 如果没指定数量，使用持仓的全部数量
            if qty is None:
                qty = position.qty
                logger.info(f"[Bitget] 使用持仓数量: {qty}")
            
            qty = await self._normalize_qty(symbol, qty)
            
            # 检查数量是否有效
            if not qty or qty <= 0:
                error_msg = f"平仓数量无效: {qty}"
                logger.error(f"[Bitget] {symbol} {error_msg}")
                return OrderResult(success=False, symbol=symbol, error=error_msg)
            
            # Bitget V2 API hedge_mode 平仓规则：
            # - 平仓多单 (close long): side="buy", tradeSide="close"
            # - 平仓空单 (close short): side="sell", tradeSide="close"
            # 注意：side 与持仓方向一致，不是相反！
            order_side = "buy" if side.upper() == "LONG" else "sell"
            # holdSide: 持仓方向，必须是 "long" 或 "short"
            hold_side = "long" if side.upper() == "LONG" else "short"
            
            logger.info(f"[Bitget] 平仓请求: symbol={bitget_symbol}, side={order_side}, holdSide={hold_side}, qty={qty}, productType={self.PRODUCT_TYPE}")
            
            params = {
                "symbol": bitget_symbol,
                "productType": self.PRODUCT_TYPE,
                "marginMode": "crossed",
                "marginCoin": self.MARGIN_COIN,  # V2 API 必须包含 marginCoin
                "size": str(qty),
                "side": order_side,
                "tradeSide": "close",
                "holdSide": hold_side,  # V2 API 双向持仓需要指定 holdSide
                "orderType": "market"
            }
            
            result = await self._run_sync(
                self._request, "POST", "/api/v2/mix/order/place-order", params
            )
            
            order_id = result.get("orderId", "") if isinstance(result, dict) else ""
            
            # 平仓成功后，取消止盈止损单（减仓时不取消）
            if cancel_tp_sl:
                try:
                    await self._cancel_plan_orders(bitget_symbol, side.upper())
                    logger.info(f"[Bitget] {symbol} {side} 平仓后已取消计划委托单")
                except Exception as cancel_err:
                    logger.warning(f"[Bitget] {symbol} 取消计划委托单失败: {cancel_err}")
            
            return OrderResult(
                success=True,
                order_id=str(order_id),
                symbol=symbol,
                side=order_side.upper(),
                position_side=side.upper(),
                qty=qty,
                status='FILLED',
                raw=result
            )
            
        except Exception as e:
            logger.error(f"[Bitget] 平仓失败 {symbol}: {e}")
            return OrderResult(success=False, symbol=symbol, error=str(e))
    
    async def _get_existing_plan_orders(
        self,
        bitget_symbol: str,
        position_side: str
    ) -> tuple[Optional[dict], Optional[dict]]:
        """
        获取现有的止盈止损计划单 (normal_plan 类型)
        
        注意：此方法仅用于 _cancel_plan_orders()，用于平仓后清理计划单。
        新的止盈止损设置使用 place-tpsl-order API (pos_profit/pos_loss 类型)。
        
        Returns:
            (sl_order, tp_order) - 止损单和止盈单，如果不存在则为 None
        """
        try:
            current_price = await self.get_ticker_price(bitget_symbol)
            if not current_price:
                return None, None
            
            plan_data = await self._run_sync(
                self._request, "GET", "/api/v2/mix/order/orders-plan-pending",
                {
                    "productType": self.PRODUCT_TYPE,
                    "planType": "normal_plan"
                }
            )
            
            if isinstance(plan_data, dict):
                plan_orders = plan_data.get("entrustedList", []) or []
            elif isinstance(plan_data, list):
                plan_orders = plan_data
            else:
                return None, None
            
            sl_order = None
            tp_order = None
            
            for o in plan_orders:
                if o.get("symbol", "").upper() != bitget_symbol.upper():
                    continue
                if o.get("tradeSide") != "close":
                    continue
                
                trigger_price = float(o.get("triggerPrice", 0))
                if not trigger_price:
                    continue
                
                # 判断是止损还是止盈
                if position_side == "LONG":
                    if trigger_price < current_price:
                        sl_order = o
                    else:
                        tp_order = o
                elif position_side == "SHORT":
                    if trigger_price > current_price:
                        sl_order = o
                    else:
                        tp_order = o
            
            return sl_order, tp_order
            
        except Exception as e:
            logger.warning(f"[Bitget] 获取现有计划单失败 {bitget_symbol}: {e}")
            return None, None
    
    async def _find_stop_loss_order(
        self,
        bitget_symbol: str,
        position_side: str,
        new_stop_price: float = None  # 不再依赖新价格判断
    ) -> Optional[dict]:
        """
        查找止损单（仅用于更新止损时）
        
        判断逻辑：根据当前市价判断
                 对于 LONG 仓位，止损单的触发价 < 当前市价
                 对于 SHORT 仓位，止损单的触发价 > 当前市价
        """
        try:
            # 获取当前市价用于判断
            symbol = self._parse_symbol(bitget_symbol)
            current_price = await self.get_ticker_price(symbol)
            if not current_price:
                logger.warning(f"[Bitget] 无法获取 {symbol} 当前价格，跳过止损单查找")
                return None
            
            plan_data = await self._run_sync(
                self._request, "GET", "/api/v2/mix/order/orders-plan-pending",
                {
                    "productType": self.PRODUCT_TYPE,
                    "planType": "normal_plan"
                }
            )
            
            if isinstance(plan_data, dict):
                plan_orders = plan_data.get("entrustedList", []) or []
            elif isinstance(plan_data, list):
                plan_orders = plan_data
            else:
                return None
            
            # 筛选该币种的平仓订单
            close_orders = []
            for o in plan_orders:
                if o.get("symbol", "").upper() != bitget_symbol.upper():
                    continue
                if o.get("tradeSide") != "close":
                    continue
                trigger_price = float(o.get("triggerPrice", 0))
                if trigger_price > 0:
                    close_orders.append((trigger_price, o))
            
            if not close_orders:
                logger.debug(f"[Bitget] 未找到 {bitget_symbol} 的平仓计划单")
                return None
            
            logger.debug(f"[Bitget] 找到 {len(close_orders)} 个平仓计划单，当前价={current_price}")
            
            # 根据当前市价判断哪个是止损单
            # LONG 止损：触发价 < 当前市价（价格跌到止损价触发）
            # SHORT 止损：触发价 > 当前市价（价格涨到止损价触发）
            for trigger_price, o in close_orders:
                order_id = o.get("orderId", "")
                if position_side == "LONG":
                    if trigger_price < current_price:
                        logger.debug(f"[Bitget] 找到 LONG 止损单: orderId={order_id}, trigger={trigger_price} < current={current_price}")
                        return o
                else:  # SHORT
                    if trigger_price > current_price:
                        logger.debug(f"[Bitget] 找到 SHORT 止损单: orderId={order_id}, trigger={trigger_price} > current={current_price}")
                        return o
            
            logger.debug(f"[Bitget] 未找到符合条件的止损单")
            return None
            
        except Exception as e:
            logger.warning(f"[Bitget] 查找止损单失败 {bitget_symbol}: {e}")
            return None
    
    async def _find_take_profit_order(
        self,
        bitget_symbol: str,
        position_side: str,
        new_tp_price: float = None  # 不再依赖新价格判断
    ) -> Optional[dict]:
        """
        查找止盈单（仅用于更新止盈时）
        
        判断逻辑：根据当前市价判断
                 对于 LONG 仓位，止盈单的触发价 > 当前市价
                 对于 SHORT 仓位，止盈单的触发价 < 当前市价
        """
        try:
            # 获取当前市价用于判断
            symbol = self._parse_symbol(bitget_symbol)
            current_price = await self.get_ticker_price(symbol)
            if not current_price:
                logger.warning(f"[Bitget] 无法获取 {symbol} 当前价格，跳过止盈单查找")
                return None
            
            plan_data = await self._run_sync(
                self._request, "GET", "/api/v2/mix/order/orders-plan-pending",
                {
                    "productType": self.PRODUCT_TYPE,
                    "planType": "normal_plan"
                }
            )
            
            if isinstance(plan_data, dict):
                plan_orders = plan_data.get("entrustedList", []) or []
            elif isinstance(plan_data, list):
                plan_orders = plan_data
            else:
                return None
            
            # 筛选该币种的平仓订单
            close_orders = []
            for o in plan_orders:
                if o.get("symbol", "").upper() != bitget_symbol.upper():
                    continue
                if o.get("tradeSide") != "close":
                    continue
                trigger_price = float(o.get("triggerPrice", 0))
                if trigger_price > 0:
                    close_orders.append((trigger_price, o))
            
            if not close_orders:
                logger.debug(f"[Bitget] 未找到 {bitget_symbol} 的平仓计划单")
                return None
            
            logger.debug(f"[Bitget] 找到 {len(close_orders)} 个平仓计划单，当前价={current_price}")
            
            # 根据当前市价判断哪个是止盈单
            # LONG 止盈：触发价 > 当前市价（价格涨到止盈价触发）
            # SHORT 止盈：触发价 < 当前市价（价格跌到止盈价触发）
            for trigger_price, o in close_orders:
                order_id = o.get("orderId", "")
                if position_side == "LONG":
                    if trigger_price > current_price:
                        logger.debug(f"[Bitget] 找到 LONG 止盈单: orderId={order_id}, trigger={trigger_price} > current={current_price}")
                        return o
                else:  # SHORT
                    if trigger_price < current_price:
                        logger.debug(f"[Bitget] 找到 SHORT 止盈单: orderId={order_id}, trigger={trigger_price} < current={current_price}")
                        return o
            
            logger.debug(f"[Bitget] 未找到符合条件的止盈单")
            return None
            
        except Exception as e:
            logger.warning(f"[Bitget] 查找止盈单失败 {bitget_symbol}: {e}")
            return None
    
    async def set_stop_loss(
        self,
        symbol: str,
        side: str,
        qty: float,
        stop_price: float,
    ) -> OrderResult:
        """
        设置止损
        
        - 正式盘：使用 place-tpsl-order API (pos_loss)，自动替换旧止损
        - 模拟盘：使用 place-plan-order API (normal_plan)，需要先取消旧止损
        """
        try:
            bitget_symbol = self._convert_symbol(symbol)
            stop_price = await self._normalize_price(symbol, stop_price)
            qty = await self._normalize_qty(symbol, qty)
            position_side = side.upper()
            
            # 检查止损价格是否有效
            if not stop_price or stop_price <= 0:
                error_msg = f"止损价格无效: {stop_price}"
                logger.error(f"[Bitget] {symbol} {error_msg}")
                return OrderResult(success=False, symbol=symbol, error=error_msg)
            
            # 从 Redis 获取 WS 标记价格进行验证
            mark_price = self._get_mark_price_from_redis(symbol)
            if mark_price and mark_price > 0:
                # 验证止损价格是否有效（不会立即触发）
                if position_side == 'LONG' and stop_price >= mark_price:
                    error_msg = f"多头止损价 {stop_price} 必须低于当前标记价 {mark_price}"
                    logger.warning(f"[Bitget] {symbol} {error_msg}")
                    return OrderResult(success=False, symbol=symbol, error=error_msg)
                if position_side == 'SHORT' and stop_price <= mark_price:
                    error_msg = f"空头止损价 {stop_price} 必须高于当前标记价 {mark_price}"
                    logger.warning(f"[Bitget] {symbol} {error_msg}")
                    return OrderResult(success=False, symbol=symbol, error=error_msg)
            
            if self._is_testnet:
                # 模拟盘：使用 normal_plan
                return await self._set_stop_loss_demo(bitget_symbol, symbol, side, qty, stop_price)
            else:
                # 正式盘：使用 pos_loss
                return await self._set_stop_loss_live(bitget_symbol, symbol, side, stop_price)
            
        except Exception as e:
            logger.error(f"[Bitget] 设置止损失败 {symbol}: {e}")
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
            logger.debug(f"[Bitget] 获取 Redis 标记价格失败 {symbol}: {e}")
        
        return None
    
    async def _set_stop_loss_live(
        self,
        bitget_symbol: str,
        symbol: str,
        side: str,
        stop_price: float
    ) -> OrderResult:
        """正式盘：使用 place-tpsl-order API"""
        # 标准化 holdSide: 必须是 "long" 或 "short"
        side_upper = (side or "").upper()
        if side_upper == "LONG":
            hold_side = "long"
        elif side_upper == "SHORT":
            hold_side = "short"
        else:
            logger.error(f"[Bitget] 无效的 side 参数: {side}, 期望 LONG/SHORT")
            return OrderResult(success=False, symbol=symbol, error=f"Invalid side: {side}")
        
        params = {
            "symbol": bitget_symbol,
            "productType": self.PRODUCT_TYPE,
            "marginCoin": self.MARGIN_COIN,
            "planType": "pos_loss",
            "triggerPrice": str(stop_price),
            "triggerType": "fill_price",
            "holdSide": hold_side
        }
        
        result = await self._run_sync(
            self._request, "POST", "/api/v2/mix/order/place-tpsl-order", params
        )
        
        order_id = result.get("orderId", "") if isinstance(result, dict) else ""
        logger.info(f"[Bitget] 仓位止损已设置 {symbol} {side} trigger={stop_price} orderId={order_id}")
        
        return OrderResult(
            success=True,
            order_id=str(order_id),
            symbol=symbol,
            position_side=side.upper(),
            price=stop_price,
            status='NEW',
            raw=result
        )
    
    async def _set_stop_loss_demo(
        self,
        bitget_symbol: str,
        symbol: str,
        side: str,
        qty: float,
        stop_price: float
    ) -> OrderResult:
        """模拟盘：使用 place-plan-order API (normal_plan)"""
        existing_sl = await self._find_stop_loss_order(bitget_symbol, side.upper())
        if existing_sl:
            existing_trigger = float(existing_sl.get("triggerPrice", 0))
            existing_order_id = existing_sl.get("orderId", "")
            
            # 价格相同，跳过（使用相对比较，避免小价格币种误判）
            if existing_trigger > 0:
                relative_diff = abs(existing_trigger - stop_price) / existing_trigger
                if relative_diff < 0.001:  # 0.1% 以内视为相同
                    logger.info(f"[Bitget] 止损单已存在且价格相同 {symbol} trigger={existing_trigger}，跳过")
                    return OrderResult(
                        success=True,
                        order_id=existing_order_id,
                        symbol=symbol,
                        price=existing_trigger,
                        status='EXISTS',
                        raw=existing_sl
                    )
            
            # 价格不同，先取消旧止损单
            logger.info(f"[Bitget] 止损价格变化 {symbol}: {existing_trigger} -> {stop_price}，先取消旧止损单 {existing_order_id}")
            try:
                cancel_params = {
                    "symbol": bitget_symbol,
                    "productType": self.PRODUCT_TYPE,
                    "marginCoin": self.MARGIN_COIN,
                    "orderId": existing_order_id
                }
                await self._run_sync(
                    self._request, "POST", "/api/v2/mix/order/cancel-plan-order", cancel_params
                )
                logger.info(f"[Bitget] 已取消旧止损单 {existing_order_id}")
            except Exception as e:
                logger.warning(f"[Bitget] 取消旧止损单失败 {existing_order_id}: {e}")
        
        order_side = "sell" if side.upper() == "LONG" else "buy"
        
        params = {
            "symbol": bitget_symbol,
            "productType": self.PRODUCT_TYPE,
            "marginMode": "crossed",
            "marginCoin": self.MARGIN_COIN,
            "size": str(qty),
            "side": order_side,
            "tradeSide": "close",
            "orderType": "market",
            "triggerPrice": str(stop_price),
            "triggerType": "fill_price",
            "planType": "normal_plan"
        }
        
        result = await self._run_sync(
            self._request, "POST", "/api/v2/mix/order/place-plan-order", params
        )
        
        order_id = result.get("orderId", "") if isinstance(result, dict) else ""
        logger.info(f"[Bitget] 止损单已创建(模拟盘) {symbol} {side} trigger={stop_price} orderId={order_id}")
        
        return OrderResult(
            success=True,
            order_id=str(order_id),
            symbol=symbol,
            side=order_side.upper(),
            position_side=side.upper(),
            price=stop_price,
            status='NEW',
            raw=result
        )
    
    async def set_take_profit(
        self,
        symbol: str,
        side: str,
        qty: float,
        tp_price: float,
    ) -> OrderResult:
        """
        设置止盈
        
        - 正式盘：使用 place-tpsl-order API (pos_profit)，自动替换旧止盈
        - 模拟盘：使用 place-plan-order API (normal_plan)，需要先取消旧止盈
        """
        try:
            bitget_symbol = self._convert_symbol(symbol)
            tp_price = await self._normalize_price(symbol, tp_price)
            qty = await self._normalize_qty(symbol, qty)
            position_side = side.upper()
            
            # 检查止盈价格是否有效
            if not tp_price or tp_price <= 0:
                error_msg = f"止盈价格无效: {tp_price}"
                logger.error(f"[Bitget] {symbol} {error_msg}")
                return OrderResult(success=False, symbol=symbol, error=error_msg)
            
            # 从 Redis 获取 WS 标记价格进行验证
            mark_price = self._get_mark_price_from_redis(symbol)
            if mark_price and mark_price > 0:
                # 验证止盈价格是否有效（不会立即触发）
                if position_side == 'LONG' and tp_price <= mark_price:
                    error_msg = f"多头止盈价 {tp_price} 必须高于当前标记价 {mark_price}"
                    logger.warning(f"[Bitget] {symbol} {error_msg}")
                    return OrderResult(success=False, symbol=symbol, error=error_msg)
                if position_side == 'SHORT' and tp_price >= mark_price:
                    error_msg = f"空头止盈价 {tp_price} 必须低于当前标记价 {mark_price}"
                    logger.warning(f"[Bitget] {symbol} {error_msg}")
                    return OrderResult(success=False, symbol=symbol, error=error_msg)
            
            if self._is_testnet:
                # 模拟盘：使用 normal_plan
                return await self._set_take_profit_demo(bitget_symbol, symbol, side, qty, tp_price)
            else:
                # 正式盘：使用 pos_profit
                return await self._set_take_profit_live(bitget_symbol, symbol, side, tp_price)
            
        except Exception as e:
            logger.error(f"[Bitget] 设置止盈失败 {symbol}: {e}")
            return OrderResult(success=False, symbol=symbol, error=str(e))
    
    async def _set_take_profit_live(
        self,
        bitget_symbol: str,
        symbol: str,
        side: str,
        tp_price: float
    ) -> OrderResult:
        """正式盘：使用 place-tpsl-order API"""
        # 标准化 holdSide: 必须是 "long" 或 "short"
        side_upper = (side or "").upper()
        if side_upper == "LONG":
            hold_side = "long"
        elif side_upper == "SHORT":
            hold_side = "short"
        else:
            logger.error(f"[Bitget] 无效的 side 参数: {side}, 期望 LONG/SHORT")
            return OrderResult(success=False, symbol=symbol, error=f"Invalid side: {side}")
        
        params = {
            "symbol": bitget_symbol,
            "productType": self.PRODUCT_TYPE,
            "marginCoin": self.MARGIN_COIN,
            "planType": "pos_profit",
            "triggerPrice": str(tp_price),
            "triggerType": "fill_price",
            "holdSide": hold_side
        }
        
        result = await self._run_sync(
            self._request, "POST", "/api/v2/mix/order/place-tpsl-order", params
        )
        
        order_id = result.get("orderId", "") if isinstance(result, dict) else ""
        logger.info(f"[Bitget] 仓位止盈已设置 {symbol} {side} trigger={tp_price} orderId={order_id}")
        
        return OrderResult(
            success=True,
            order_id=str(order_id),
            symbol=symbol,
            position_side=side.upper(),
            price=tp_price,
            status='NEW',
            raw=result
        )
    
    async def _set_take_profit_demo(
        self,
        bitget_symbol: str,
        symbol: str,
        side: str,
        qty: float,
        tp_price: float
    ) -> OrderResult:
        """模拟盘：使用 place-plan-order API (normal_plan)"""
        existing_tp = await self._find_take_profit_order(bitget_symbol, side.upper())
        if existing_tp:
            existing_trigger = float(existing_tp.get("triggerPrice", 0))
            existing_order_id = existing_tp.get("orderId", "")
            
            # 价格相同，跳过（使用相对比较，避免小价格币种误判）
            if existing_trigger > 0:
                relative_diff = abs(existing_trigger - tp_price) / existing_trigger
                if relative_diff < 0.001:  # 0.1% 以内视为相同
                    logger.info(f"[Bitget] 止盈单已存在且价格相同 {symbol} trigger={existing_trigger}，跳过")
                    return OrderResult(
                        success=True,
                        order_id=existing_order_id,
                        symbol=symbol,
                        price=existing_trigger,
                        status='EXISTS',
                        raw=existing_tp
                    )
            
            # 价格不同，先取消旧止盈单
            logger.info(f"[Bitget] 止盈价格变化 {symbol}: {existing_trigger} -> {tp_price}，先取消旧止盈单 {existing_order_id}")
            try:
                cancel_params = {
                    "symbol": bitget_symbol,
                    "productType": self.PRODUCT_TYPE,
                    "marginCoin": self.MARGIN_COIN,
                    "orderId": existing_order_id
                }
                await self._run_sync(
                    self._request, "POST", "/api/v2/mix/order/cancel-plan-order", cancel_params
                )
                logger.info(f"[Bitget] 已取消旧止盈单 {existing_order_id}")
            except Exception as e:
                logger.warning(f"[Bitget] 取消旧止盈单失败 {existing_order_id}: {e}")
        
        order_side = "sell" if side.upper() == "LONG" else "buy"
        
        params = {
            "symbol": bitget_symbol,
            "productType": self.PRODUCT_TYPE,
            "marginMode": "crossed",
            "marginCoin": self.MARGIN_COIN,
            "size": str(qty),
            "side": order_side,
            "tradeSide": "close",
            "orderType": "market",
            "triggerPrice": str(tp_price),
            "triggerType": "fill_price",
            "planType": "normal_plan"
        }
        
        result = await self._run_sync(
            self._request, "POST", "/api/v2/mix/order/place-plan-order", params
        )
        
        order_id = result.get("orderId", "") if isinstance(result, dict) else ""
        logger.info(f"[Bitget] 止盈单已创建(模拟盘) {symbol} {side} trigger={tp_price} orderId={order_id}")
        
        return OrderResult(
            success=True,
            order_id=str(order_id),
            symbol=symbol,
            side=order_side.upper(),
            position_side=side.upper(),
            price=tp_price,
            status='NEW',
            raw=result
        )
    
    async def set_position_tpsl(
        self,
        symbol: str,
        side: str,
        stop_loss: Optional[float] = None,
        take_profit: Optional[float] = None,
    ) -> OrderResult:
        """
        同时设置仓位的止盈止损
        
        - 正式盘：使用 place-pos-tpsl API
        - 模拟盘：分别调用 set_stop_loss 和 set_take_profit
        """
        try:
            if not stop_loss and not take_profit:
                return OrderResult(success=False, symbol=symbol, error="必须指定止损或止盈价格")
            
            # 模拟盘不支持 place-pos-tpsl，分别设置
            if self._is_testnet:
                qty = 0.01  # 模拟盘需要数量，使用默认值
                position = await self.get_position(symbol)
                if position:
                    qty = position.qty
                
                if stop_loss:
                    await self.set_stop_loss(symbol, side, qty, stop_loss)
                if take_profit:
                    await self.set_take_profit(symbol, side, qty, take_profit)
                
                return OrderResult(
                    success=True,
                    symbol=symbol,
                    position_side=side.upper(),
                    status='NEW'
                )
            
            # 正式盘使用 place-pos-tpsl API
            bitget_symbol = self._convert_symbol(symbol)
            
            # 标准化 holdSide: 必须是 "long" 或 "short"
            side_upper = (side or "").upper()
            if side_upper == "LONG":
                hold_side = "long"
            elif side_upper == "SHORT":
                hold_side = "short"
            else:
                logger.error(f"[Bitget] 无效的 side 参数: {side}, 期望 LONG/SHORT")
                return OrderResult(success=False, symbol=symbol, error=f"Invalid side: {side}")
            
            params = {
                "symbol": bitget_symbol,
                "productType": self.PRODUCT_TYPE,
                "marginCoin": self.MARGIN_COIN,
                "holdSide": hold_side
            }
            
            if stop_loss:
                stop_loss = await self._normalize_price(symbol, stop_loss)
                if not stop_loss or stop_loss <= 0:
                    return OrderResult(success=False, symbol=symbol, error=f"止损价格无效: {stop_loss}")
                params["stopLossTriggerPrice"] = str(stop_loss)
                params["stopLossExecutePrice"] = "0"
                params["stopLossTriggerType"] = "fill_price"
            
            if take_profit:
                take_profit = await self._normalize_price(symbol, take_profit)
                if not take_profit or take_profit <= 0:
                    return OrderResult(success=False, symbol=symbol, error=f"止盈价格无效: {take_profit}")
                params["stopSurplusTriggerPrice"] = str(take_profit)
                params["stopSurplusExecutePrice"] = "0"
                params["stopSurplusTriggerType"] = "fill_price"
            
            result = await self._run_sync(
                self._request, "POST", "/api/v2/mix/order/place-pos-tpsl", params
            )
            
            logger.info(f"[Bitget] 仓位止盈止损已设置 {symbol} {side} SL={stop_loss} TP={take_profit}")
            
            return OrderResult(
                success=True,
                order_id=result.get("orderId", "") if isinstance(result, dict) else "",
                symbol=symbol,
                position_side=side.upper(),
                status='NEW',
                raw=result
            )
            
        except Exception as e:
            logger.error(f"[Bitget] 设置仓位止盈止损失败 {symbol}: {e}")
            return OrderResult(success=False, symbol=symbol, error=str(e))
    
    # NOTE: _cancel_tpsl_orders() 已删除
    # Bitget 模拟盘不支持 TPSL API (/api/v2/mix/order/orders-tpsl-pending 返回 404)
    # 如果将来需要支持正式盘，可以参考 git history 恢复此方法
    
    async def _cancel_plan_orders(
        self,
        bitget_symbol: str,
        position_side: str,
        cancel_sl: bool = True,
        cancel_tp: bool = True
    ):
        """
        取消普通计划委托单 (止损/止盈)
        
        用途：
        - 在 market_close() 平仓后清理所有相关的计划委托单
        - 在 cancel_all_orders() 取消所有订单时使用
        
        注意：此方法会取消指定仓位的所有计划单。
        由于 Bitget 模拟盘的 normal_plan 取消一个会级联取消其他的问题，
        此方法仅在平仓后用于清理，不在 set_stop_loss/set_take_profit 中使用。
        
        根据触发价格和当前价格判断是止损还是止盈：
        - LONG 持仓：止损价 < 当前价，止盈价 > 当前价
        - SHORT 持仓：止损价 > 当前价，止盈价 < 当前价
        """
        if not cancel_sl and not cancel_tp:
            return
        
        try:
            # 获取当前价格
            current_price = await self.get_ticker_price(bitget_symbol)
            if not current_price:
                return
            
            # 获取所有计划委托单
            plan_data = await self._run_sync(
                self._request, "GET", "/api/v2/mix/order/orders-plan-pending",
                {
                    "productType": self.PRODUCT_TYPE,
                    "planType": "normal_plan"
                }
            )
            
            # 解析返回数据
            if isinstance(plan_data, dict):
                plan_orders = plan_data.get("entrustedList", []) or []
            elif isinstance(plan_data, list):
                plan_orders = plan_data
            else:
                return
            
            if not plan_orders:
                return
            
            orders_to_cancel = []
            
            logger.debug(f"[Bitget] _cancel_plan_orders: symbol={bitget_symbol} side={position_side} cancel_sl={cancel_sl} cancel_tp={cancel_tp}")
            logger.debug(f"[Bitget] 共有 {len(plan_orders)} 个计划委托单，当前价={current_price}")
            
            for o in plan_orders:
                order_symbol = o.get("symbol", "")
                order_id = o.get("orderId", "")[-6:]
                trigger_price_raw = o.get("triggerPrice", 0)
                trade_side = o.get("tradeSide", "")
                
                logger.debug(f"[Bitget]   检查订单 id={order_id} symbol={order_symbol} trigger={trigger_price_raw} tradeSide={trade_side}")
                
                if order_symbol.upper() != bitget_symbol.upper():
                    logger.debug(f"[Bitget]     -> 跳过: symbol 不匹配")
                    continue
                
                # 只处理平仓的计划单
                if trade_side != "close":
                    logger.debug(f"[Bitget]     -> 跳过: tradeSide={trade_side} 不是 close")
                    continue
                
                trigger_price = float(trigger_price_raw) if trigger_price_raw else 0
                if not trigger_price:
                    logger.debug(f"[Bitget]     -> 跳过: trigger_price 无效")
                    continue
                
                # 每次循环重置判断变量
                is_sl = False
                is_tp = False
                should_cancel = False
                
                if position_side == "LONG":
                    # LONG: 止损价 < 当前价，止盈价 > 当前价
                    if trigger_price < current_price:
                        is_sl = True
                    else:
                        is_tp = True
                elif position_side == "SHORT":
                    # SHORT: 止损价 > 当前价，止盈价 < 当前价
                    if trigger_price > current_price:
                        is_sl = True
                    else:
                        is_tp = True
                
                logger.debug(f"[Bitget]     -> 判断结果: is_sl={is_sl} is_tp={is_tp}")
                
                # 根据参数决定是否取消（互斥判断）
                if is_sl and cancel_sl:
                    should_cancel = True
                    logger.debug(f"[Bitget]     -> 将取消止损单 trigger={trigger_price} current={current_price}")
                elif is_tp and cancel_tp:
                    should_cancel = True
                    logger.debug(f"[Bitget]     -> 将取消止盈单 trigger={trigger_price} current={current_price}")
                else:
                    logger.debug(f"[Bitget]     -> 保留此订单")
                
                if should_cancel:
                    order_id_full = o.get("orderId")
                    if order_id_full:
                        orders_to_cancel.append(order_id_full)
            
            # 取消订单
            for order_id in orders_to_cancel:
                try:
                    params = {
                        "symbol": bitget_symbol,
                        "productType": self.PRODUCT_TYPE,
                        "marginCoin": self.MARGIN_COIN,  # V2 API cancel-plan-order 必须包含 marginCoin
                        "orderId": order_id
                    }
                    
                    await self._run_sync(
                        self._request, "POST", "/api/v2/mix/order/cancel-plan-order",
                        params
                    )
                    logger.debug(f"[Bitget] 已取消计划委托 {bitget_symbol} orderId={order_id}")
                except Exception as e:
                    logger.warning(f"[Bitget] 取消计划委托失败 {order_id}: {e}")
            
            if orders_to_cancel:
                logger.info(f"[Bitget] 已取消 {len(orders_to_cancel)} 个计划委托单 {bitget_symbol}")
                
        except Exception as e:
            logger.warning(f"[Bitget] 获取/取消计划委托单失败 {bitget_symbol}: {e}")
    
    async def cancel_order(self, symbol: str, order_id: str) -> bool:
        """取消订单"""
        try:
            bitget_symbol = self._convert_symbol(symbol)
            
            await self._run_sync(
                self._request, "POST", "/api/v2/mix/order/cancel-order",
                {
                    "symbol": bitget_symbol,
                    "productType": self.PRODUCT_TYPE,
                    "orderId": order_id
                }
            )
            return True
        except Exception as e:
            logger.error(f"[Bitget] 取消订单失败 {symbol} {order_id}: {e}")
            return False
    
    async def cancel_all_orders(self, symbol: str) -> int:
        """取消指定币种的所有订单"""
        try:
            bitget_symbol = self._convert_symbol(symbol)
            cancelled = 0
            
            # 取消普通订单
            try:
                params = {
                    "symbol": bitget_symbol,
                    "productType": self.PRODUCT_TYPE,
                    "marginCoin": self.MARGIN_COIN  # V2 API batch-cancel-orders 必须包含 marginCoin
                }
                
                await self._run_sync(
                    self._request, "POST", "/api/v2/mix/order/batch-cancel-orders",
                    params
                )
                cancelled += 1
            except Exception as e:
                logger.warning(f"[Bitget] 取消普通订单失败: {e}")
            
            # 取消计划委托单
            try:
                await self._cancel_plan_orders(bitget_symbol, "LONG", True, True)
                await self._cancel_plan_orders(bitget_symbol, "SHORT", True, True)
            except Exception as e:
                logger.warning(f"[Bitget] 取消计划委托失败: {e}")
            
            return cancelled
        except Exception as e:
            logger.error(f"[Bitget] 取消所有订单失败 {symbol}: {e}")
            return 0
    
    # ==================== 设置相关 ====================
    
    async def set_leverage(self, symbol: str, leverage: int) -> bool:
        """设置杠杆，如果失败则尝试使用最大支持杠杆"""
        bitget_symbol = self._convert_symbol(symbol)
        
        async def _do_set_leverage(lev: int) -> bool:
            for hold_side in ["long", "short"]:
                params = {
                    "symbol": bitget_symbol,
                    "productType": self.PRODUCT_TYPE,
                    "marginCoin": self.MARGIN_COIN,
                    "leverage": str(lev),
                    "holdSide": hold_side
                }
                await self._run_sync(
                    self._request, "POST", "/api/v2/mix/account/set-leverage",
                    params
                )
            return True
        
        try:
            await _do_set_leverage(leverage)
            logger.debug(f"[Bitget] 设置杠杆成功 {symbol} {leverage}x")
            return True
        except Exception as e:
            error_msg = str(e)
            # 检查是否是杠杆超限错误
            if 'lever' in error_msg.lower() or '40762' in error_msg:
                try:
                    max_leverage = await self._get_max_leverage(bitget_symbol)
                    if max_leverage and max_leverage < leverage:
                        logger.info(f"[Bitget] {symbol} 杠杆 {leverage}x 超限，使用最大杠杆 {max_leverage}x")
                        await _do_set_leverage(max_leverage)
                        return True
                except Exception as retry_e:
                    logger.warning(f"[Bitget] 设置最大杠杆也失败 {symbol}: {retry_e}")
            logger.warning(f"[Bitget] 设置杠杆失败 {symbol} {leverage}x: {e}")
            return False
    
    async def _get_max_leverage(self, bitget_symbol: str) -> int:
        """获取交易对最大杠杆"""
        try:
            info = await self.get_symbol_info(bitget_symbol.replace("USDT", ""))
            if info:
                # Bitget symbol info 中有 maxLever 或 maxLeverage 字段
                max_lev = info.get('maxLeverage') or info.get('maxLever') or info.get('leverageRange', '').split(',')[-1]
                if max_lev:
                    return int(float(max_lev))
        except Exception as e:
            logger.debug(f"[Bitget] 获取最大杠杆失败 {bitget_symbol}: {e}")
        return None
    
    async def set_margin_type(self, symbol: str, margin_type: str) -> bool:
        """设置保证金模式"""
        try:
            bitget_symbol = self._convert_symbol(symbol)
            
            # Bitget: crossed (全仓) / isolated (逐仓)
            margin_mode = "crossed" if margin_type.lower() == "cross" else "isolated"
            
            params = {
                "symbol": bitget_symbol,
                "productType": self.PRODUCT_TYPE,
                "marginCoin": self.MARGIN_COIN,  # V2 API set-margin-mode 必须包含 marginCoin
                "marginMode": margin_mode
            }
            
            await self._run_sync(
                self._request, "POST", "/api/v2/mix/account/set-margin-mode",
                params
            )
            return True
        except Exception as e:
            # 可能已经是该模式
            if "already" in str(e).lower() or "same" in str(e).lower():
                return True
            logger.warning(f"[Bitget] 设置保证金模式失败 {symbol}: {e}")
            return False
    
    # ==================== 市场数据 ====================
    
    async def get_ticker_price(self, symbol: str) -> float:
        """获取最新价格"""
        try:
            bitget_symbol = self._convert_symbol(symbol)
            
            # 使用公开 API，不需要签名
            # 注意：模拟盘和正式盘的行情数据相同，使用 USDT-FUTURES
            product_type = "USDT-FUTURES"  # 行情 API 始终使用正式盘 productType
            url = f"{self.API_URL}/api/v2/mix/market/ticker?symbol={bitget_symbol}&productType={product_type}"
            response = requests.get(url, timeout=10)
            data = response.json()
            
            if data.get("code") == "00000":
                ticker_data = data.get("data", [])
                if isinstance(ticker_data, list) and ticker_data:
                    return float(ticker_data[0].get("lastPr", 0))
                elif isinstance(ticker_data, dict):
                    return float(ticker_data.get("lastPr", 0))
            
            return 0.0
        except Exception as e:
            logger.error(f"[Bitget] 获取价格失败 {symbol}: {e}")
            return 0.0
    
    async def get_symbol_info(self, symbol: str) -> Dict:
        """
        获取交易对信息
        
        使用全局公共缓存，所有用户共享
        """
        from core.bitget_public_cache import get_bitget_public_cache
        cache = get_bitget_public_cache()
        return await cache.get_contract_info_async(symbol)
    
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
            BitgetWebSocket 实例
        """
        from exchanges.bitget.websocket import BitgetWebSocket
        
        ws = BitgetWebSocket(
            api_key=self._api_key,
            api_secret=self._api_secret,
            passphrase=self._passphrase,
            is_testnet=self._is_testnet,
            uid=uid,
            callbacks=callbacks,
        )
        return ws
    
    # ==================== 辅助方法 ====================
    
    async def _normalize_qty(self, symbol: str, qty: float) -> float:
        """规范化数量"""
        info = await self.get_symbol_info(symbol)
        if not info:
            return round(qty, 4)
        
        # Bitget V2 使用 sizeMultiplier 和 minTradeNum
        size_multiplier = float(info.get("sizeMultiplier", 1))
        min_trade_num = float(info.get("minTradeNum", 0.001))
        
        qty = max(qty, min_trade_num)
        
        if size_multiplier > 0:
            qty = math.ceil(qty / size_multiplier) * size_multiplier
        
        # 精度处理
        volume_place = int(info.get("volumePlace", 4))
        qty = round(qty, volume_place)
        
        return qty
    
    async def _normalize_price(self, symbol: str, price: float) -> float:
        """
        规范化价格（使用 Decimal 保持精度，四舍五入到指定小数位）
        
        对于小价格币种，浮点数精度不足，使用 Decimal 计算
        """
        info = await self.get_symbol_info(symbol)
        if not info:
            return round(float(price), 2)
        
        # 只用 pricePlace 控制小数位数
        price_place = int(info.get("pricePlace", 2))
        
        # 使用 Decimal 进行高精度四舍五入
        price_d = Decimal(str(price))
        quantize_str = '0.' + '0' * price_place if price_place > 0 else '1'
        normalized = price_d.quantize(Decimal(quantize_str), rounding=ROUND_HALF_UP)
        
        return float(normalized)
    
    async def close(self):
        """关闭连接"""
        if self._executor:
            self._executor.shutdown(wait=False)
            self._executor = None
    
    def close_sync(self):
        """同步关闭（安全版本）"""
        if self._executor:
            self._executor.shutdown(wait=False)
            self._executor = None
    
    def __del__(self):
        """析构函数 - 确保线程池被关闭"""
        if hasattr(self, '_executor') and self._executor:
            try:
                self._executor.shutdown(wait=False)
            except Exception:
                pass
            self._executor = None
