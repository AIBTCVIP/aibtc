# trading/multi_exchange_trader.py
"""
多交易所交易执行模块

支持同时在多个交易所执行相同的交易决策
"""

import asyncio
import logging
import time
from typing import Dict, List, Optional
from dataclasses import dataclass

from exchanges import create_exchange
from exchanges.base import BaseExchange, OrderResult
from core.user_db import config_loader

logger = logging.getLogger(__name__)


@dataclass
class MultiExchangeResult:
    """多交易所执行结果"""
    symbol: str
    action: str
    results: Dict[str, OrderResult]  # exchange -> OrderResult
    success_count: int
    failure_count: int
    
    def to_dict(self) -> Dict:
        return {
            "symbol": self.symbol,
            "action": self.action,
            "results": {
                ex: r.to_dict() for ex, r in self.results.items()
            },
            "success_count": self.success_count,
            "failure_count": self.failure_count
        }


class MultiExchangeTrader:
    """
    多交易所交易执行器
    
    同一个 AI 决策在所有启用的交易所上并行执行
    """
    
    # 各交易所认证相关错误码
    AUTH_ERROR_CODES = {
        'binance': {'-2015', '-2014', '-1022', '-1002', '-1021'},
        'okx': {'50111', '50113', '50114', '50115'},
        'bitget': {'40001', '40002', '40003', '40004', '40005', '40006', '40007', '40008', '40009', '40010'},
        'hyperliquid': set(),  # Hyperliquid 使用关键词检测
    }
    
    def __init__(self, uid: str, on_auth_failed: Optional[callable] = None):
        self.uid = uid
        self._exchanges: Dict[str, BaseExchange] = {}
        self._initialized = False
        self.on_auth_failed = on_auth_failed  # 认证失败回调: (exchange_name, error_msg) -> None
        self._auth_failed_exchanges: set = set()  # 已触发认证失败的交易所（避免重复触发）
    
    async def initialize(self) -> bool:
        """
        初始化所有启用的交易所连接
        
        Returns:
            是否成功初始化至少一个交易所
        """
        if self._initialized:
            return bool(self._exchanges)
        
        # 获取用户启用的交易所列表
        enabled_exchanges = config_loader.get_enabled_exchanges(self.uid)
        
        if not enabled_exchanges:
            logger.warning(f"[{self.uid}] 没有启用的交易所")
            return False
        
        for exchange_name in enabled_exchanges:
            try:
                # 获取交易所配置
                config = config_loader.get_user_exchange_config(self.uid, exchange_name)
                if not config or not config.get("api_key"):
                    logger.warning(f"[{self.uid}] {exchange_name} 配置不完整，跳过")
                    continue
                
                # 创建交易所实例
                exchange = create_exchange(
                    exchange_type=exchange_name,
                    api_key=config["api_key"],
                    api_secret=config.get("api_secret", ""),
                    passphrase=config.get("passphrase"),
                    is_testnet=config.get("is_testnet", False),
                    wallet_address=config.get("wallet_address")
                )
                
                self._exchanges[exchange_name] = exchange
                logger.debug(f"[{self.uid}] {exchange_name} 交易所已初始化")
                
            except ValueError as e:
                # 配置错误（如缺少 passphrase）- 不可恢复
                logger.error(f"[{self.uid}] {exchange_name} 配置错误: {e}")
            except Exception as e:
                error_msg = str(e).lower()
                # 检测认证相关错误
                if any(kw in error_msg for kw in ["auth", "key", "secret", "signature", "permission", "invalid api"]):
                    logger.error(f"[{self.uid}] {exchange_name} 认证失败: {e}")
                elif any(kw in error_msg for kw in ["timeout", "connect", "network", "connection"]):
                    logger.warning(f"[{self.uid}] {exchange_name} 网络错误（可重试）: {e}")
                else:
                    logger.error(f"[{self.uid}] 初始化 {exchange_name} 失败: {e}")
        
        self._initialized = True
        return bool(self._exchanges)
    
    async def close(self):
        """关闭所有交易所连接"""
        for exchange_name, exchange in self._exchanges.items():
            try:
                await exchange.close()
            except Exception as e:
                logger.warning(f"[{self.uid}] 关闭 {exchange_name} 连接失败: {e}")
        
        self._exchanges.clear()
        self._initialized = False
        self._auth_failed_exchanges.clear()
    
    def _is_auth_error(self, exchange_name: str, error_msg: str) -> bool:
        """
        检查是否是认证错误
        
        Args:
            exchange_name: 交易所名称
            error_msg: 错误信息
        
        Returns:
            True 如果是认证错误
        """
        if not error_msg:
            return False
        
        error_lower = error_msg.lower()
        
        # 通用认证关键词
        auth_keywords = ['invalid api', 'api-key', 'apikey', 'signature', 'not authorized', 
                         'permission denied', 'authentication', 'invalid key']
        for keyword in auth_keywords:
            if keyword in error_lower:
                return True
        
        # 交易所特定错误码
        codes = self.AUTH_ERROR_CODES.get(exchange_name, set())
        for code in codes:
            if code in error_msg:
                return True
        
        return False
    
    def _handle_auth_failure(self, exchange_name: str, error_msg: str):
        """
        处理认证失败
        
        Args:
            exchange_name: 交易所名称
            error_msg: 错误信息
        """
        # 避免重复触发
        if exchange_name in self._auth_failed_exchanges:
            return
        
        self._auth_failed_exchanges.add(exchange_name)
        logger.error(f"[{self.uid}] {exchange_name} 交易认证失败，触发停止回调: {error_msg}")
        
        # 从可用交易所中移除
        if exchange_name in self._exchanges:
            del self._exchanges[exchange_name]
        
        # 触发回调
        if self.on_auth_failed:
            try:
                self.on_auth_failed(exchange_name, error_msg)
            except Exception as e:
                logger.warning(f"[{self.uid}] 认证失败回调执行异常: {e}")
    
    @property
    def exchanges(self) -> Dict[str, BaseExchange]:
        """获取所有交易所实例"""
        return self._exchanges
    
    @property
    def exchange_names(self) -> List[str]:
        """获取所有交易所名称"""
        return list(self._exchanges.keys())
    
    def _sync_sl_tp_to_redis(
        self, 
        symbol: str, 
        position_side: str, 
        sl: Optional[float] = None, 
        tp: Optional[float] = None,
        exchange: Optional[str] = None
    ):
        """
        同步止盈止损价格到 Redis
        
        这样前端持仓列表才能显示止盈止损价
        
        Args:
            symbol: 交易对
            position_side: 持仓方向
            sl: 止损价格
            tp: 止盈价格
            exchange: 交易所名称
        """
        try:
            from trading.utils import sync_sl_tp_to_position
            sync_sl_tp_to_position(
                symbol=symbol,
                position_side=position_side,
                sl=sl,
                tp=tp,
                uid=self.uid,
                exchange=exchange
            )
            logger.debug(f"[{self.uid}][{exchange}] 止盈止损已同步到 Redis: {symbol} {position_side} SL={sl} TP={tp}")
        except Exception as e:
            logger.warning(f"[{self.uid}] 同步止盈止损到 Redis 失败: {e}")
    
    async def execute_trade(
        self,
        symbol: str,
        action: str,
        stop_loss: Optional[float] = None,
        take_profit: Optional[float] = None,
        quantity: Optional[float] = None,
        position_size: Optional[float] = None,
        leverage: Optional[int] = None,
        entry_price: Optional[float] = None,
        target_exchanges: Optional[List[str]] = None,
        ai_decision_id: Optional[int] = None,
    ) -> MultiExchangeResult:
        """
        在所有启用的交易所上执行交易
        
        Args:
            symbol: 交易对 (Binance 格式, 如 BTCUSDT)
            action: 动作类型
                - 开仓: open_long, open_short, open_long_market, open_short_market
                - 平仓: close_long, close_short
                - 止盈止损: update_stop_loss, update_take_profit, stop_orders
                - 加减仓: increase_position, decrease_position
                - 其他: cancel
            stop_loss: 止损价格
            take_profit: 止盈价格
            quantity: 数量
            position_size: 仓位金额 (USDT)
            leverage: 杠杆倍数
            entry_price: 入场价格 (限价单使用，当前暂不支持)
            target_exchanges: 目标交易所列表（可选）。如果指定，只在这些交易所执行。
            ai_decision_id: AI 决策记录 ID（用于关联交易和 AI 决策）
        
        Returns:
            MultiExchangeResult
        """
        if not self._exchanges:
            await self.initialize()
        
        if not self._exchanges:
            logger.error(f"[{self.uid}] 没有可用的交易所")
            return MultiExchangeResult(
                symbol=symbol,
                action=action,
                results={},
                success_count=0,
                failure_count=0
            )
        
        # 解析动作类型
        action_info = self._parse_action(action)
        
        if not action_info:
            logger.warning(f"[{self.uid}] 未支持的动作: {action}")
            return MultiExchangeResult(
                symbol=symbol,
                action=action,
                results={},
                success_count=0,
                failure_count=0
            )
        
        action_type = action_info["type"]
        side = action_info.get("side")
        
        # 确定要执行的交易所
        exchanges_to_execute = self._exchanges
        if target_exchanges:
            # 只在指定的交易所执行
            exchanges_to_execute = {
                name: ex for name, ex in self._exchanges.items() 
                if name in target_exchanges
            }
            if not exchanges_to_execute:
                logger.warning(f"[{self.uid}] 目标交易所 {target_exchanges} 都不可用")
                return MultiExchangeResult(
                    symbol=symbol,
                    action=action,
                    results={},
                    success_count=0,
                    failure_count=0
                )
        
        # 并行执行
        tasks = []
        for exchange_name, exchange in exchanges_to_execute.items():
            task = self._execute_action(
                exchange_name=exchange_name,
                exchange=exchange,
                symbol=symbol,
                action_type=action_type,
                side=side,
                stop_loss=stop_loss,
                take_profit=take_profit,
                quantity=quantity,
                position_size=position_size,
                leverage=leverage,
                entry_price=entry_price,
                ai_decision_id=ai_decision_id,
            )
            tasks.append((exchange_name, task))
        
        # 收集结果
        results = {}
        success_count = 0
        failure_count = 0
        
        gathered = await asyncio.gather(*[t[1] for t in tasks], return_exceptions=True)
        
        for i, (exchange_name, _) in enumerate(tasks):
            result = gathered[i]
            if isinstance(result, Exception):
                logger.error(f"[{self.uid}] {exchange_name} 执行失败: {result}")
                results[exchange_name] = OrderResult(
                    success=False,
                    symbol=symbol,
                    error=str(result)
                )
                failure_count += 1
            elif isinstance(result, OrderResult):
                results[exchange_name] = result
                if result.success:
                    success_count += 1
                else:
                    failure_count += 1
            else:
                logger.warning(f"[{self.uid}] {exchange_name} 返回未知结果类型: {type(result)}")
                failure_count += 1
        
        return MultiExchangeResult(
            symbol=symbol,
            action=action,
            results=results,
            success_count=success_count,
            failure_count=failure_count
        )
    
    def _parse_action(self, action: str) -> Optional[Dict]:
        """
        解析动作类型
        
        Returns:
            {"type": "open"|"open_limit"|"close"|"update_sl"|"update_tp"|"stop_orders"|"increase"|"decrease"|"cancel", "side": "LONG"|"SHORT"|None}
        """
        action = action.lower()
        
        # 市价开仓动作
        if action in ["open_long", "open_long_market"]:
            return {"type": "open", "side": "LONG"}
        elif action in ["open_short", "open_short_market"]:
            return {"type": "open", "side": "SHORT"}
        
        # 限价开仓
        elif action == "open_long_limit":
            return {"type": "open_limit", "side": "LONG"}
        elif action == "open_short_limit":
            return {"type": "open_limit", "side": "SHORT"}
        
        # 平仓动作
        elif action == "close_long":
            return {"type": "close", "side": "LONG"}
        elif action == "close_short":
            return {"type": "close", "side": "SHORT"}
        
        # 止损止盈动作
        elif action == "update_stop_loss":
            return {"type": "update_sl", "side": None}
        elif action == "update_take_profit":
            return {"type": "update_tp", "side": None}
        elif action == "stop_orders":
            return {"type": "stop_orders", "side": None}
        
        # 加减仓动作
        elif action == "increase_position":
            return {"type": "increase", "side": None}
        elif action == "decrease_position":
            return {"type": "decrease", "side": None}
        
        # 取消订单
        elif action == "cancel":
            return {"type": "cancel", "side": None}
        
        return None
    
    async def _execute_action(
        self,
        exchange_name: str,
        exchange: BaseExchange,
        symbol: str,
        action_type: str,
        side: Optional[str] = None,
        stop_loss: Optional[float] = None,
        take_profit: Optional[float] = None,
        quantity: Optional[float] = None,
        position_size: Optional[float] = None,
        leverage: Optional[int] = None,
        entry_price: Optional[float] = None,
        ai_decision_id: Optional[int] = None,
    ) -> OrderResult:
        """
        在单个交易所执行指定动作
        """
        try:
            if action_type == "open":
                return await self._execute_open(
                    exchange_name, exchange, symbol, side,
                    stop_loss, take_profit, quantity, position_size, leverage,
                    ai_decision_id=ai_decision_id
                )
            
            elif action_type == "open_limit":
                return await self._execute_limit_open(
                    exchange_name, exchange, symbol, side,
                    entry_price, stop_loss, take_profit, quantity, position_size, leverage,
                    ai_decision_id=ai_decision_id
                )
            
            elif action_type == "close":
                return await self._execute_close(
                    exchange_name, exchange, symbol, side, quantity
                )
            
            elif action_type == "update_sl":
                return await self._execute_update_sl(
                    exchange_name, exchange, symbol, stop_loss
                )
            
            elif action_type == "update_tp":
                return await self._execute_update_tp(
                    exchange_name, exchange, symbol, take_profit
                )
            
            elif action_type == "stop_orders":
                return await self._execute_stop_orders(
                    exchange_name, exchange, symbol, stop_loss, take_profit
                )
            
            elif action_type == "increase":
                return await self._execute_increase(
                    exchange_name, exchange, symbol, quantity, position_size, leverage
                )
            
            elif action_type == "decrease":
                return await self._execute_decrease(
                    exchange_name, exchange, symbol, quantity, position_size
                )
            
            elif action_type == "cancel":
                return await self._execute_cancel(
                    exchange_name, exchange, symbol
                )
            
            else:
                return OrderResult(success=False, symbol=symbol, error=f"未知动作类型: {action_type}")
        
        except Exception as e:
            error_msg = str(e)
            error_lower = error_msg.lower()
            
            # 区分错误类型以便调试
            if any(kw in error_lower for kw in ["timeout", "connect", "network"]):
                logger.warning(f"[{self.uid}] {exchange_name} 执行 {action_type} 网络错误: {e}")
            elif self._is_auth_error(exchange_name, error_msg):
                logger.error(f"[{self.uid}] {exchange_name} 执行 {action_type} 认证错误: {e}")
                # 触发认证失败处理
                self._handle_auth_failure(exchange_name, error_msg)
            elif any(kw in error_lower for kw in ["insufficient", "balance", "margin"]):
                logger.warning(f"[{self.uid}] {exchange_name} 执行 {action_type} 余额不足: {e}")
            else:
                logger.error(f"[{self.uid}] {exchange_name} 执行 {action_type} 异常: {e}")
            return OrderResult(success=False, symbol=symbol, error=error_msg)
    
    async def _execute_open(
        self,
        exchange_name: str,
        exchange: BaseExchange,
        symbol: str,
        side: str,
        stop_loss: Optional[float],
        take_profit: Optional[float],
        quantity: Optional[float],
        position_size: Optional[float],
        leverage: Optional[int],
        ai_decision_id: Optional[int] = None,
    ) -> OrderResult:
        """执行开仓"""
        # 获取价格计算数量
        price = await exchange.get_ticker_price(symbol)
        if not price:
            return OrderResult(success=False, symbol=symbol, error="无法获取价格")
        
        qty = quantity
        if not qty and position_size:
            qty = position_size / price
        
        if not qty:
            return OrderResult(success=False, symbol=symbol, error="无法确定数量")
        
        # ⚠️ 重要：在市价单执行之前存储 ai_decision_id
        # 市价单通常在毫秒级成交，WebSocket 收到成交事件时需要能读取到这个 key
        # 如果在 market_open 之后才存储，可能会因为竞态条件导致 ai_decision_id 丢失
        if ai_decision_id:
            self._store_ai_decision_id(exchange_name, symbol, side, ai_decision_id)
        
        # 市价开仓
        result = await exchange.market_open(
            symbol=symbol,
            side=side,
            qty=qty,
            leverage=leverage
        )
        
        # 设置止损止盈
        if result.success:
            sl_success = False
            tp_success = False
            
            if stop_loss:
                sl_result = await exchange.set_stop_loss(symbol, side, qty, stop_loss)
                sl_success = sl_result.success
                if not sl_success:
                    logger.warning(f"[{self.uid}] {exchange_name} 设置止损失败: {sl_result.error}")
            
            if take_profit:
                tp_result = await exchange.set_take_profit(symbol, side, qty, take_profit)
                tp_success = tp_result.success
                if not tp_success:
                    logger.warning(f"[{self.uid}] {exchange_name} 设置止盈失败: {tp_result.error}")
            
            # 同步止盈止损到 Redis
            if sl_success or tp_success:
                self._sync_sl_tp_to_redis(
                    symbol=symbol,
                    position_side=side,
                    sl=stop_loss if sl_success else None,
                    tp=take_profit if tp_success else None,
                    exchange=exchange_name
                )
        else:
            # 开仓失败，清理预先存储的 ai_decision_id key
            if ai_decision_id:
                try:
                    from core.database import redis_client
                    key = f"ai_decision:market:{self.uid}:{exchange_name}:{symbol}:{side}"
                    redis_client.delete(key)
                    logger.debug(f"[{self.uid}] 开仓失败，已清理 ai_decision_id key: {key}")
                except Exception as e:
                    logger.debug(f"[{self.uid}] 清理 ai_decision_id key 失败: {e}")
        
        return result
    
    async def _execute_limit_open(
        self,
        exchange_name: str,
        exchange: BaseExchange,
        symbol: str,
        side: str,
        entry_price: Optional[float],
        stop_loss: Optional[float],
        take_profit: Optional[float],
        quantity: Optional[float],
        position_size: Optional[float],
        leverage: Optional[int],
        ai_decision_id: Optional[int] = None,
    ) -> OrderResult:
        """执行限价开仓"""
        if not entry_price:
            return OrderResult(success=False, symbol=symbol, error="限价开仓需要指定 entry_price")
        
        # 计算数量
        qty = quantity
        if not qty and position_size:
            qty = position_size / entry_price
        
        if not qty:
            return OrderResult(success=False, symbol=symbol, error="无法确定数量")
        
        # 限价开仓
        result = await exchange.limit_open(
            symbol=symbol,
            side=side,
            qty=qty,
            price=entry_price,
            leverage=leverage
        )
        
        # 注意：限价单成交后才能设置止盈止损
        # 这里只记录日志，止盈止损需要在订单成交后通过 WebSocket 回调设置
        if result.success:
            logger.info(
                f"[{self.uid}] {exchange_name} 限价单已挂出: {symbol} {side} "
                f"price={entry_price} qty={qty} order_id={result.order_id}"
            )
            if stop_loss or take_profit:
                logger.info(
                    f"[{self.uid}] 止损止盈将在订单成交后设置: SL={stop_loss} TP={take_profit}"
                )
            
            # 存储 ai_decision_id 到限价单数据（成交后会转移到 cycle）
            if ai_decision_id:
                self._store_ai_decision_id_for_order(
                    exchange_name, symbol, side, result.order_id, ai_decision_id
                )
        
        return result
    
    async def _execute_close(
        self,
        exchange_name: str,
        exchange: BaseExchange,
        symbol: str,
        side: str,
        quantity: Optional[float],
    ) -> OrderResult:
        """执行平仓"""
        # 获取当前持仓
        positions = await exchange.get_positions()
        pos = None
        for p in positions:
            if p.symbol == symbol and p.side == side:
                pos = p
                break
        
        if not pos or pos.qty <= 0:
            return OrderResult(success=False, symbol=symbol, error=f"没有 {side} 持仓")
        
        # 如果指定了数量就用指定的，否则全部平仓
        close_qty = quantity if quantity else pos.qty
        
        return await exchange.market_close(
            symbol=symbol,
            side=side,
            qty=close_qty
        )
    
    def _store_ai_decision_id(
        self,
        exchange_name: str,
        symbol: str,
        side: str,
        ai_decision_id: int
    ):
        """
        存储 ai_decision_id 到临时 key
        
        市价单开仓成功后调用，WebSocket 收到成交事件创建 cycle 时会读取此 key
        
        注意：市价单没有 order_id，所以使用 symbol:side 作为 key
        """
        try:
            from core.database import redis_client
            import json
            
            # 存储到临时 key，格式: ai_decision:market:{uid}:{exchange}:{symbol}:{side}
            key = f"ai_decision:market:{self.uid}:{exchange_name}:{symbol}:{side}"
            data = {
                "symbol": symbol,
                "side": side,
                "ai_decision_id": ai_decision_id,
                "created_at": int(time.time() * 1000)
            }
            
            # 设置 7 天过期
            redis_client.setex(key, 7 * 24 * 3600, json.dumps(data))
            logger.info(f"[{self.uid}] 已存储市价单 ai_decision_id: {symbol}:{side} -> decision={ai_decision_id}")
            
        except Exception as e:
            logger.warning(f"[{self.uid}] 存储 ai_decision_id 失败: {e}")
    
    def _store_ai_decision_id_for_order(
        self,
        exchange_name: str,
        symbol: str,
        side: str,
        order_id: str,
        ai_decision_id: int
    ):
        """
        存储 ai_decision_id 到限价单数据
        
        限价单挂出后调用，当订单成交时会转移到 cycle 数据
        """
        try:
            from core.database import redis_client
            import json
            
            # 存储到专门的 key，格式: ai_decision:order:{uid}:{exchange}:{order_id}
            key = f"ai_decision:order:{self.uid}:{exchange_name}:{order_id}"
            data = {
                "symbol": symbol,
                "side": side,
                "ai_decision_id": ai_decision_id,
                "created_at": int(time.time() * 1000)
            }
            
            # 设置 7 天过期（限价单为 GTC 模式，可能长时间挂单）
            redis_client.setex(key, 7 * 24 * 3600, json.dumps(data))
            logger.info(f"[{self.uid}] 已存储限价单 ai_decision_id: order={order_id}, decision={ai_decision_id}")
            
        except Exception as e:
            logger.warning(f"[{self.uid}] 存储限价单 ai_decision_id 失败: {e}")
    
    def _is_sl_update_valid(
        self,
        position_side: str,
        current_price: float,
        current_sl: Optional[float],
        new_sl: float
    ) -> bool:
        """
        检查止损更新是否有效（只能朝有利方向移动，相同价格不更新）
        
        - 多单：新止损必须在价格下方，且严格更高（更接近当前价）
        - 空单：新止损必须在价格上方，且严格更低（更接近当前价）
        """
        if current_sl is None:
            # 没有旧止损，允许设置
            return True
        
        # 价格相同不更新
        if new_sl == current_sl:
            return False
        
        if position_side == "LONG":
            # 多单：止损在价格下方，新止损要严格更高（更接近当前价）
            return (new_sl < current_price) and (new_sl > current_sl)
        else:  # SHORT
            # 空单：止损在价格上方，新止损要严格更低（更接近当前价）
            return (new_sl > current_price) and (new_sl < current_sl)
    
    def _is_tp_update_valid(
        self,
        position_side: str,
        current_price: float,
        current_tp: Optional[float],
        new_tp: float
    ) -> bool:
        """
        检查止盈更新是否有效（不能让止盈更容易触发，相同价格不更新）
        
        - 多单：TP 必须 > 当前价，且新 TP 严格 > 旧 TP（不能拉近）
        - 空单：TP 必须 < 当前价，且新 TP 严格 < 旧 TP（不能拉近）
        """
        if current_tp is None:
            # 没有旧止盈，允许设置
            return True
        
        # 价格相同不更新
        if new_tp == current_tp:
            return False
        
        if position_side == "LONG":
            return (new_tp > current_price) and (new_tp > current_tp)
        else:  # SHORT
            return (new_tp < current_price) and (new_tp < current_tp)
    
    def _get_mark_price_from_redis(self, symbol: str) -> Optional[float]:
        """
        从 Redis 获取 WS 标记价格
        
        Binance 止损单使用 mark price 触发，所以验证时也应该使用 mark price
        """
        try:
            from core.database import redis_client, RedisKeys
            import json
            
            key = RedisKeys.market_prices(symbol.upper())
            data = redis_client.get(key)
            
            if data:
                parsed = json.loads(data)
                mark_price = parsed.get("markPrice")
                if mark_price:
                    return float(mark_price)
        except Exception as e:
            logger.debug(f"[{self.uid}] 获取 Redis 标记价格失败 {symbol}: {e}")
        
        return None
    
    async def _get_current_sl_tp_from_redis(
        self,
        symbol: str,
        exchange_name: Optional[str] = None
    ) -> tuple:
        """
        从 Redis 获取当前止损止盈价格
        
        Args:
            symbol: 交易对
            exchange_name: 交易所名称（可选，用于多交易所架构）
        
        Returns:
            (current_sl, current_tp)
        """
        try:
            from core.pf_compatibility import pf_compat
            
            # 使用 pf_compat 获取仓位数据
            pos_data = pf_compat.get_pf_pos(self.uid, exchange_name)
            
            if not pos_data:
                logger.debug(f"[{self.uid}] Redis 未找到 {symbol} 的持仓数据")
                return (None, None)
            
            # 尝试获取 LONG 和 SHORT 方向的持仓
            for side in ["LONG", "SHORT"]:
                field = f"{symbol}:{side}"
                if field in pos_data:
                    pos_obj = pos_data[field]
                    sl = pos_obj.get("stopLossPrice")
                    tp = pos_obj.get("takeProfitPrice")
                    current_sl = float(sl) if sl and sl != "0" and sl != "" else None
                    current_tp = float(tp) if tp and tp != "0" and tp != "" else None
                    logger.info(f"[{self.uid}] Redis 获取 {symbol}:{side} SL={current_sl} TP={current_tp}")
                    return (current_sl, current_tp)
            
            logger.debug(f"[{self.uid}] Redis 未找到 {symbol} 的持仓数据")
            return (None, None)
        except Exception as e:
            error_msg = str(e).lower()
            if any(kw in error_msg for kw in ["timeout", "connect", "network"]):
                logger.debug(f"[{self.uid}] 从 Redis 获取止损止盈网络错误: {e}")
            else:
                logger.warning(f"[{self.uid}] 从 Redis 获取止损止盈失败: {e}")
            return (None, None)
    
    async def _get_current_sl_tp(
        self,
        symbol: str,
        exchange: BaseExchange,
        exchange_name: Optional[str] = None
    ) -> tuple:
        """
        获取当前止损止盈价格（优先从 Redis，备选从交易所）
        
        Args:
            symbol: 交易对
            exchange: 交易所实例
            exchange_name: 交易所名称（用于 Redis 查询）
        
        Returns:
            (current_sl, current_tp)
        """
        # 先尝试从 Redis 获取
        current_sl, current_tp = await self._get_current_sl_tp_from_redis(symbol, exchange_name)
        
        if current_sl is not None or current_tp is not None:
            return (current_sl, current_tp)
        
        # 如果 Redis 没有，尝试从交易所获取
        try:
            # 从交易所获取当前止盈止损订单
            sl, tp = await self._get_sl_tp_from_exchange(symbol, exchange)
            if sl is not None or tp is not None:
                logger.info(f"[{self.uid}] 从交易所获取 {symbol} 当前 SL={sl} TP={tp}")
            return (sl, tp)
        except Exception as e:
            logger.warning(f"[{self.uid}] 从交易所获取止损止盈失败: {e}")
        
        logger.warning(f"[{self.uid}] 无法获取 {symbol} 当前止损止盈，校验将被跳过")
        return (None, None)
    
    async def _get_sl_tp_from_exchange(
        self,
        symbol: str,
        exchange: BaseExchange
    ) -> tuple:
        """从交易所获取当前止盈止损价格"""
        current_sl = None
        current_tp = None
        
        exchange_name = getattr(exchange, 'EXCHANGE_NAME', 'unknown')
        
        try:
            if exchange_name == 'binance':
                return await self._get_sl_tp_from_binance(symbol, exchange)
            elif exchange_name == 'okx':
                return await self._get_sl_tp_from_okx(symbol, exchange)
            elif exchange_name == 'hyperliquid':
                return await self._get_sl_tp_from_hyperliquid(symbol, exchange)
            elif exchange_name == 'bitget':
                return await self._get_sl_tp_from_bitget(symbol, exchange)
            else:
                return (None, None)
        except Exception as e:
            logger.warning(f"[{self.uid}] 从 {exchange_name} 获取止盈止损失败: {e}")
            return (None, None)
    
    async def _get_sl_tp_from_binance(
        self,
        symbol: str,
        exchange: BaseExchange
    ) -> tuple:
        """从 Binance 获取当前止盈止损价格"""
        current_sl = None
        current_tp = None
        
        try:
            algo_orders = await exchange._async_call('futures_get_open_algo_orders')
            
            for o in algo_orders:
                if o.get('symbol') != symbol:
                    continue
                
                order_type = o.get('orderType', '')
                trigger_price = o.get('triggerPrice') or o.get('stopPrice')
                
                if not trigger_price:
                    continue
                
                try:
                    price = float(trigger_price)
                except (ValueError, TypeError):
                    continue
                
                # 止损单类型
                if order_type in ['STOP', 'STOP_MARKET']:
                    if current_sl is None:
                        current_sl = price
                    else:
                        pos_side = o.get('positionSide', '')
                        if pos_side == 'LONG':
                            current_sl = max(current_sl, price)
                        else:
                            current_sl = min(current_sl, price)
                
                # 止盈单类型
                if order_type in ['TAKE_PROFIT', 'TAKE_PROFIT_MARKET']:
                    if current_tp is None:
                        current_tp = price
                    else:
                        pos_side = o.get('positionSide', '')
                        if pos_side == 'LONG':
                            current_tp = min(current_tp, price)
                        else:
                            current_tp = max(current_tp, price)
            
            return (current_sl, current_tp)
        except Exception as e:
            logger.warning(f"[{self.uid}] 从 Binance 获取 algo 订单失败: {e}")
            return (None, None)
    
    async def _get_sl_tp_from_okx(
        self,
        symbol: str,
        exchange: BaseExchange
    ) -> tuple:
        """从 OKX 获取当前止盈止损价格"""
        current_sl = None
        current_tp = None
        
        try:
            okx_symbol = exchange._convert_symbol(symbol)
            
            # 获取 algo 订单
            algo_data = await exchange._request('GET', '/api/v5/trade/orders-algo-pending', params={
                'instType': 'SWAP',
                'instId': okx_symbol,
                'ordType': 'conditional'
            })
            
            for o in algo_data.get('data', []):
                # 检查止损
                sl_trigger = o.get('slTriggerPx')
                if sl_trigger and float(sl_trigger) > 0:
                    price = float(sl_trigger)
                    if current_sl is None:
                        current_sl = price
                    else:
                        pos_side = o.get('posSide', '')
                        if pos_side == 'long':
                            current_sl = max(current_sl, price)
                        else:
                            current_sl = min(current_sl, price)
                
                # 检查止盈
                tp_trigger = o.get('tpTriggerPx')
                if tp_trigger and float(tp_trigger) > 0:
                    price = float(tp_trigger)
                    if current_tp is None:
                        current_tp = price
                    else:
                        pos_side = o.get('posSide', '')
                        if pos_side == 'long':
                            current_tp = min(current_tp, price)
                        else:
                            current_tp = max(current_tp, price)
            
            return (current_sl, current_tp)
        except Exception as e:
            logger.warning(f"[{self.uid}] 从 OKX 获取 algo 订单失败: {e}")
            return (None, None)
    
    async def _get_sl_tp_from_hyperliquid(
        self,
        symbol: str,
        exchange: BaseExchange
    ) -> tuple:
        """从 Hyperliquid 获取当前止盈止损价格"""
        current_sl = None
        current_tp = None
        
        try:
            # Hyperliquid 交易所有特定的属性
            wallet_address = getattr(exchange, 'wallet_address', None)
            if not wallet_address:
                return (None, None)
            
            # 转换 symbol 格式
            convert_symbol = getattr(exchange, '_convert_symbol', None)
            if not convert_symbol:
                return (None, None)
            hl_symbol = convert_symbol(symbol)
            
            # 使用 SDK 获取订单
            _info = getattr(exchange, '_info', None)
            _run_sync = getattr(exchange, '_run_sync', None)
            if not _info or not _run_sync:
                return (None, None)
            
            # 使用 frontend_open_orders 获取完整订单信息（包含 orderType）
            orders_data = await _run_sync(_info.frontend_open_orders, wallet_address)
            
            for o in orders_data:
                if o.get('coin') != hl_symbol:
                    continue
                
                order_type = o.get('orderType', '')
                trigger_px = o.get('triggerPx')
                
                if not trigger_px:
                    continue
                
                try:
                    price = float(trigger_px)
                except (ValueError, TypeError):
                    continue
                
                # 判断止损/止盈
                # "Stop Market" / "Stop Limit" = 止损
                # "Take Profit Market" / "Take Profit Limit" = 止盈
                is_sl = 'Stop' in order_type and 'Take' not in order_type
                is_tp = 'Take Profit' in order_type
                
                if is_sl:
                    if current_sl is None:
                        current_sl = price
                
                if is_tp:
                    if current_tp is None:
                        current_tp = price
            
            logger.debug(f"[{self.uid}] Hyperliquid {symbol} 当前 SL={current_sl} TP={current_tp}")
            return (current_sl, current_tp)
        except Exception as e:
            logger.warning(f"[{self.uid}] 从 Hyperliquid 获取订单失败: {e}")
            return (None, None)
    
    async def _get_sl_tp_from_bitget(
        self,
        symbol: str,
        exchange: BaseExchange
    ) -> tuple:
        """
        从 Bitget 获取当前止盈止损价格
        
        优先查询 TPSL 订单 (pos_loss/pos_profit 类型)，
        如果没有，再查询 normal_plan 类型的计划委托单
        """
        current_sl = None
        current_tp = None
        
        try:
            # Bitget 特定属性
            _request = getattr(exchange, '_request', None)
            _run_sync = getattr(exchange, '_run_sync', None)
            _convert_symbol = getattr(exchange, '_convert_symbol', None)
            
            if not _request or not _run_sync or not _convert_symbol:
                return (None, None)
            
            bitget_symbol = _convert_symbol(symbol)
            product_type = getattr(exchange, 'PRODUCT_TYPE', 'USDT-FUTURES')
            
            # 获取当前持仓方向
            positions = await exchange.get_positions() or []
            pos_side = None
            current_price = await exchange.get_ticker_price(symbol)
            
            for p in positions:
                p_symbol = (p.symbol or "").upper()
                if p_symbol == bitget_symbol.upper() or p_symbol == symbol.upper():
                    pos_side = p.side  # LONG or SHORT
                    break
            
            # 1. 优先查询 TPSL 订单 (使用 place-tpsl-order 创建的 pos_loss/pos_profit)
            try:
                tpsl_data = await _run_sync(
                    _request, "GET", "/api/v2/mix/order/orders-tpsl-pending",
                    {
                        "productType": product_type,
                        "symbol": bitget_symbol
                    }
                )
                
                tpsl_orders = []
                if isinstance(tpsl_data, dict):
                    tpsl_orders = tpsl_data.get('entrustedList', []) or []
                elif isinstance(tpsl_data, list):
                    tpsl_orders = tpsl_data
                
                for o in tpsl_orders:
                    if o.get('symbol', '').upper() != bitget_symbol.upper():
                        continue
                    
                    plan_type = o.get('planType', '')
                    trigger_price = o.get('triggerPrice')
                    hold_side = o.get('holdSide', '').upper()
                    
                    # 检查持仓方向匹配
                    if pos_side and hold_side != pos_side:
                        continue
                    
                    if not trigger_price:
                        continue
                    
                    try:
                        price = float(trigger_price)
                    except (ValueError, TypeError):
                        continue
                    if plan_type == 'pos_loss':
                        current_sl = price
                    elif plan_type == 'pos_profit':
                        current_tp = price
                
                if current_sl is not None or current_tp is not None:
                    logger.debug(f"[{self.uid}] Bitget {symbol} TPSL订单: SL={current_sl} TP={current_tp}")
                    return (current_sl, current_tp)
                    
            except Exception as e:
                logger.debug(f"[{self.uid}] Bitget TPSL查询失败 (可能是模拟盘): {e}")
            
            # 2. 备选：查询 normal_plan 类型的计划委托单
            try:
                plan_data = await _run_sync(
                    _request, "GET", "/api/v2/mix/order/orders-plan-pending",
                    {
                        "productType": product_type,
                        "planType": "normal_plan"
                    }
                )
            except Exception as e:
                # P5 Fix: 记录获取计划单失败
                logger.debug(f"[Bitget] 获取计划单失败: {e}")
                plan_data = None
            
            plan_orders = []
            if plan_data is not None:
                if isinstance(plan_data, dict):
                    plan_orders = plan_data.get('entrustedList', []) or []
                elif isinstance(plan_data, list):
                    plan_orders = plan_data
            
            for o in (plan_orders or []):
                if o.get('symbol') != bitget_symbol:
                    continue
                
                trigger_price = o.get('triggerPrice')
                if not trigger_price:
                    continue
                
                try:
                    price = float(trigger_price)
                except (ValueError, TypeError):
                    continue
                
                trade_side = o.get('tradeSide', '')
                
                # 只看平仓的计划单
                if trade_side != 'close':
                    continue
                
                # 根据触发价格和当前价格判断是止损还是止盈
                if pos_side == 'LONG':
                    if price < current_price:
                        current_sl = price
                    else:
                        current_tp = price
                elif pos_side == 'SHORT':
                    if price > current_price:
                        current_sl = price
                    else:
                        current_tp = price
                else:
                    if current_sl is None:
                        current_sl = price
                    elif current_tp is None:
                        current_tp = price
            
            logger.debug(f"[{self.uid}] Bitget {symbol} 计划委托: SL={current_sl} TP={current_tp}")
            return (current_sl, current_tp)
        except Exception as e:
            logger.warning(f"[{self.uid}] 从 Bitget 获取止盈止损失败: {e}")
            return (None, None)
    
    async def _execute_update_sl(
        self,
        exchange_name: str,
        exchange: BaseExchange,
        symbol: str,
        stop_loss: Optional[float],
    ) -> OrderResult:
        """更新止损"""
        if not stop_loss:
            return OrderResult(success=False, symbol=symbol, error="未指定止损价格")
        
        # 确保 stop_loss 是 float 类型
        try:
            stop_loss = float(stop_loss)
        except (TypeError, ValueError):
            return OrderResult(success=False, symbol=symbol, error=f"止损价格格式错误: {stop_loss}")
        
        # ======== 提前检查 Redis，避免不必要的 API 调用 ========
        redis_sl, _ = await self._get_current_sl_tp_from_redis(symbol, exchange_name)
        
        # 价格相同则无需更新（使用相对比较，避免小价格币种误判）
        if redis_sl is not None and redis_sl > 0:
            relative_diff = abs(stop_loss - redis_sl) / redis_sl
            if relative_diff < 0.0001:  # 0.01% 以内视为相同
                logger.debug(f"[{self.uid}] {exchange_name} {symbol} 止损无需更新 (SL: {redis_sl} -> {stop_loss})")
                return OrderResult(success=True, symbol=symbol, error=None)
        
        # ======== 获取当前持仓 ========
        positions = await exchange.get_positions()
        pos = None
        symbol_upper = symbol.upper()
        for p in positions:
            p_symbol = p.symbol.upper() if p.symbol else ""
            if p_symbol == symbol_upper:
                pos = p
                break
        
        if not pos:
            return OrderResult(success=False, symbol=symbol, error="没有持仓")
        
        # 获取当前价格（优先使用 mark price，因为 Binance 止损单使用 mark price 触发）
        current_price = self._get_mark_price_from_redis(symbol)
        if not current_price or current_price <= 0:
            current_price = await exchange.get_ticker_price(symbol)
        current_sl = redis_sl  # 使用已经获取的 Redis 数据
        
        logger.info(f"[{self.uid}] {exchange_name} 更新止损: {symbol} {pos.side} current_sl={current_sl} new_sl={stop_loss} price={current_price}")
        
        # 校验止损更新是否有效
        if current_sl is not None and current_price:
            if not self._is_sl_update_valid(pos.side, current_price, current_sl, stop_loss):
                error_msg = f"拒绝止损更新: {pos.side} current_sl={current_sl} new_sl={stop_loss} price={current_price}"
                logger.warning(f"[{self.uid}] {exchange_name} {error_msg}")
                return OrderResult(success=False, symbol=symbol, error=error_msg)
        
        result = await exchange.set_stop_loss(symbol, pos.side, pos.qty, stop_loss)
        
        if result.success:
            self._sync_sl_tp_to_redis(
                symbol=symbol,
                position_side=pos.side,
                sl=stop_loss,
                tp=None,
                exchange=exchange_name
            )
        
        return result
    
    async def _execute_update_tp(
        self,
        exchange_name: str,
        exchange: BaseExchange,
        symbol: str,
        take_profit: Optional[float],
    ) -> OrderResult:
        """更新止盈"""
        if not take_profit:
            return OrderResult(success=False, symbol=symbol, error="未指定止盈价格")
        
        # 确保 take_profit 是 float 类型
        try:
            take_profit = float(take_profit)
        except (TypeError, ValueError):
            return OrderResult(success=False, symbol=symbol, error=f"止盈价格格式错误: {take_profit}")
        
        # ======== 提前检查 Redis，避免不必要的 API 调用 ========
        _, redis_tp = await self._get_current_sl_tp_from_redis(symbol, exchange_name)
        
        # 价格相同则无需更新（使用相对比较，避免小价格币种误判）
        if redis_tp is not None and redis_tp > 0:
            relative_diff = abs(take_profit - redis_tp) / redis_tp
            if relative_diff < 0.0001:  # 0.01% 以内视为相同
                logger.debug(f"[{self.uid}] {exchange_name} {symbol} 止盈无需更新 (TP: {redis_tp} -> {take_profit})")
                return OrderResult(success=True, symbol=symbol, error=None)
        
        # ======== 获取当前持仓 ========
        positions = await exchange.get_positions()
        pos = None
        symbol_upper = symbol.upper()
        for p in positions:
            p_symbol = p.symbol.upper() if p.symbol else ""
            if p_symbol == symbol_upper:
                pos = p
                break
        
        if not pos:
            return OrderResult(success=False, symbol=symbol, error="没有持仓")
        
        # 获取当前价格（优先使用 mark price，因为 Binance 止盈单使用 mark price 触发）
        current_price = self._get_mark_price_from_redis(symbol)
        if not current_price or current_price <= 0:
            current_price = await exchange.get_ticker_price(symbol)
        current_tp = redis_tp  # 使用已经获取的 Redis 数据
        
        logger.info(f"[{self.uid}] {exchange_name} 更新止盈: {symbol} {pos.side} current_tp={current_tp} new_tp={take_profit} price={current_price}")
        
        # 校验止盈更新是否有效
        if current_tp is not None and current_price:
            if not self._is_tp_update_valid(pos.side, current_price, current_tp, take_profit):
                error_msg = f"拒绝止盈更新: {pos.side} current_tp={current_tp} new_tp={take_profit} price={current_price}"
                logger.warning(f"[{self.uid}] {exchange_name} {error_msg}")
                return OrderResult(success=False, symbol=symbol, error=error_msg)
        
        result = await exchange.set_take_profit(symbol, pos.side, pos.qty, take_profit)
        
        if result.success:
            self._sync_sl_tp_to_redis(
                symbol=symbol,
                position_side=pos.side,
                sl=None,
                tp=take_profit,
                exchange=exchange_name
            )
        
        return result
    
    async def _execute_stop_orders(
        self,
        exchange_name: str,
        exchange: BaseExchange,
        symbol: str,
        stop_loss: Optional[float],
        take_profit: Optional[float],
    ) -> OrderResult:
        """同时设置止损止盈"""
        if not stop_loss and not take_profit:
            return OrderResult(success=False, symbol=symbol, error="未指定止损或止盈价格")
        
        # 确保 stop_loss 和 take_profit 是 float 类型
        try:
            if stop_loss:
                stop_loss = float(stop_loss)
        except (TypeError, ValueError):
            return OrderResult(success=False, symbol=symbol, error=f"止损价格格式错误: {stop_loss}")
        
        try:
            if take_profit:
                take_profit = float(take_profit)
        except (TypeError, ValueError):
            return OrderResult(success=False, symbol=symbol, error=f"止盈价格格式错误: {take_profit}")
        
        # ======== 提前检查 Redis，避免不必要的 API 调用 ========
        # 先从 Redis 获取当前止损止盈
        redis_sl, redis_tp = await self._get_current_sl_tp_from_redis(symbol, exchange_name)
        
        # 检查是否需要更新（使用相对比较，避免小价格币种误判）
        need_update_sl = stop_loss is not None
        if need_update_sl and redis_sl is not None and redis_sl > 0:
            relative_diff = abs(stop_loss - redis_sl) / redis_sl
            if relative_diff < 0.0001:  # 0.01% 以内视为相同
                need_update_sl = False
        
        need_update_tp = take_profit is not None
        if need_update_tp and redis_tp is not None and redis_tp > 0:
            relative_diff = abs(take_profit - redis_tp) / redis_tp
            if relative_diff < 0.0001:  # 0.01% 以内视为相同
                need_update_tp = False
        
        if not need_update_sl and not need_update_tp:
            logger.debug(f"[{self.uid}] {exchange_name} {symbol} 止损止盈无需更新 (SL: {redis_sl} -> {stop_loss}, TP: {redis_tp} -> {take_profit})")
            return OrderResult(success=True, symbol=symbol, error=None)
        
        # ======== 获取当前持仓 ========
        positions = await exchange.get_positions()
        pos = None
        symbol_upper = symbol.upper()
        for p in positions:
            # 支持多种 symbol 格式匹配
            p_symbol = p.symbol.upper() if p.symbol else ""
            if p_symbol == symbol_upper:
                pos = p
                break
        
        if not pos:
            logger.warning(f"[{self.uid}] {exchange_name} 没有找到 {symbol} 的持仓, 现有持仓: {[p.symbol for p in positions]}")
            return OrderResult(success=False, symbol=symbol, error="没有持仓")
        
        # 获取当前价格（优先使用 mark price，因为止损止盈单使用 mark price 触发）
        current_price = self._get_mark_price_from_redis(symbol)
        if not current_price or current_price <= 0:
            current_price = await exchange.get_ticker_price(symbol)
        current_sl, current_tp = redis_sl, redis_tp  # 使用已经获取的 Redis 数据
        
        logger.info(f"[{self.uid}] {exchange_name} 更新止损止盈: {symbol} {pos.side} current_sl={current_sl} current_tp={current_tp} new_sl={stop_loss} new_tp={take_profit} price={current_price}")
        
        sl_success = False
        tp_success = False
        errors = []
        
        # 处理止损
        if stop_loss:
            # 校验止损更新是否有效
            if current_sl is not None and current_price:
                if not self._is_sl_update_valid(pos.side, current_price, current_sl, stop_loss):
                    errors.append(f"止损: 拒绝更新 current={current_sl} new={stop_loss}")
                    stop_loss = None  # 跳过止损设置
            
            if stop_loss:
                sl_result = await exchange.set_stop_loss(symbol, pos.side, pos.qty, stop_loss)
                sl_success = sl_result.success
                if not sl_success:
                    errors.append(f"止损: {sl_result.error}")
        
        # 处理止盈
        if take_profit:
            # 校验止盈更新是否有效
            if current_tp is not None and current_price:
                if not self._is_tp_update_valid(pos.side, current_price, current_tp, take_profit):
                    errors.append(f"止盈: 拒绝更新 current={current_tp} new={take_profit}")
                    take_profit = None  # 跳过止盈设置
            
            if take_profit:
                tp_result = await exchange.set_take_profit(symbol, pos.side, pos.qty, take_profit)
                tp_success = tp_result.success
                if not tp_success:
                    errors.append(f"止盈: {tp_result.error}")
        
        # 同步到 Redis
        if sl_success or tp_success:
            self._sync_sl_tp_to_redis(
                symbol=symbol,
                position_side=pos.side,
                sl=stop_loss if sl_success else None,
                tp=take_profit if tp_success else None,
                exchange=exchange_name
            )
        
        success = sl_success or tp_success
        error = "; ".join(errors) if errors else None
        
        return OrderResult(
            success=success,
            symbol=symbol,
            position_side=pos.side,
            error=error
        )
    
    async def _execute_increase(
        self,
        exchange_name: str,
        exchange: BaseExchange,
        symbol: str,
        quantity: Optional[float],
        position_size: Optional[float],
        leverage: Optional[int],
    ) -> OrderResult:
        """加仓"""
        # 获取当前持仓
        positions = await exchange.get_positions()
        pos = None
        for p in positions:
            if p.symbol == symbol:
                pos = p
                break
        
        if not pos:
            return OrderResult(success=False, symbol=symbol, error="没有持仓，无法加仓")
        
        # 计算加仓数量
        price = await exchange.get_ticker_price(symbol)
        if not price:
            return OrderResult(success=False, symbol=symbol, error="无法获取价格")
        
        qty = quantity
        if not qty and position_size:
            qty = position_size / price
        
        if not qty:
            return OrderResult(success=False, symbol=symbol, error="无法确定加仓数量")
        
        # 市价开仓加仓
        return await exchange.market_open(
            symbol=symbol,
            side=pos.side,
            qty=qty,
            leverage=leverage
        )
    
    async def _execute_decrease(
        self,
        exchange_name: str,
        exchange: BaseExchange,
        symbol: str,
        quantity: Optional[float],
        position_size: Optional[float],
    ) -> OrderResult:
        """减仓"""
        # 获取当前持仓
        positions = await exchange.get_positions()
        pos = None
        
        logger.debug(f"[{self.uid}][{exchange_name}] 减仓查找持仓: symbol={symbol}, 持仓列表={[(p.symbol, p.side, p.qty) for p in positions]}")
        
        for p in positions:
            if p.symbol == symbol:
                pos = p
                break
        
        if not pos:
            logger.warning(f"[{self.uid}][{exchange_name}] 未找到 {symbol} 的持仓，无法减仓")
            return OrderResult(success=False, symbol=symbol, error="没有持仓，无法减仓")
        
        # 计算减仓数量
        qty = quantity
        if not qty and position_size:
            price = await exchange.get_ticker_price(symbol)
            if not price:
                return OrderResult(success=False, symbol=symbol, error="无法获取价格")
            qty = position_size / price
        
        if not qty:
            return OrderResult(success=False, symbol=symbol, error="无法确定减仓数量")
        
        # 不能减超过持仓
        if qty > pos.qty:
            qty = pos.qty
        
        # 市价平仓减仓（不取消止盈止损单）
        return await exchange.market_close(
            symbol=symbol,
            side=pos.side,
            qty=qty,
            cancel_tp_sl=False  # 减仓时保留止盈止损单
        )
    
    async def _execute_cancel(
        self,
        exchange_name: str,
        exchange: BaseExchange,
        symbol: str,
    ) -> OrderResult:
        """取消所有挂单"""
        try:
            cancelled = await exchange.cancel_all_orders(symbol)
            return OrderResult(
                success=True,
                symbol=symbol,
                status=f"取消了 {cancelled} 个订单"
            )
        except Exception as e:
            return OrderResult(success=False, symbol=symbol, error=str(e))
    
    async def get_all_positions(self) -> Dict[str, List]:
        """
        获取所有交易所的持仓
        
        Returns:
            {exchange_name: [Position, ...]}
        """
        if not self._exchanges:
            await self.initialize()
        
        results = {}
        tasks = []
        
        for exchange_name, exchange in self._exchanges.items():
            tasks.append((exchange_name, exchange.get_positions()))
        
        gathered = await asyncio.gather(*[t[1] for t in tasks], return_exceptions=True)
        
        for i, (exchange_name, _) in enumerate(tasks):
            result = gathered[i]
            if isinstance(result, Exception):
                logger.error(f"[{self.uid}] 获取 {exchange_name} 持仓失败: {result}")
                results[exchange_name] = []
            else:
                results[exchange_name] = result
        
        return results
    
    async def get_all_balances(self) -> Dict[str, float]:
        """
        获取所有交易所的余额
        
        Returns:
            {exchange_name: balance}
        """
        if not self._exchanges:
            await self.initialize()
        
        results = {}
        tasks = []
        
        for exchange_name, exchange in self._exchanges.items():
            tasks.append((exchange_name, exchange.get_balance()))
        
        gathered = await asyncio.gather(*[t[1] for t in tasks], return_exceptions=True)
        
        for i, (exchange_name, _) in enumerate(tasks):
            result = gathered[i]
            if isinstance(result, Exception):
                logger.error(f"[{self.uid}] 获取 {exchange_name} 余额失败: {result}")
                results[exchange_name] = 0.0
            else:
                results[exchange_name] = result
        
        return results


# 用户 -> MultiExchangeTrader 实例缓存
_traders: Dict[str, MultiExchangeTrader] = {}


def get_multi_exchange_trader(uid: str) -> MultiExchangeTrader:
    """获取或创建用户的多交易所交易执行器"""
    if uid not in _traders:
        _traders[uid] = MultiExchangeTrader(uid)
    return _traders[uid]


async def remove_multi_exchange_trader(uid: str):
    """移除用户的多交易所交易执行器"""
    if uid in _traders:
        await _traders[uid].close()
        del _traders[uid]


async def execute_multi_exchange_trade(
    uid: str,
    symbol: str,
    action: str,
    exchange: str = None,
    **kwargs
) -> MultiExchangeResult:
    """
    便捷函数：在用户的所有启用交易所上执行交易
    
    Args:
        uid: 用户 ID
        symbol: 交易对
        action: 动作类型
        exchange: 指定单个交易所（可选）。如果指定，只在该交易所执行。
        **kwargs: 其他参数 (stop_loss, take_profit, quantity, position_size, leverage)
    
    Returns:
        MultiExchangeResult
    """
    trader = get_multi_exchange_trader(uid)
    
    # 如果指定了单个交易所，转换为 target_exchanges 列表
    if exchange:
        kwargs['target_exchanges'] = [exchange]
    
    return await trader.execute_trade(symbol, action, **kwargs)
