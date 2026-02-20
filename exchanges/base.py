# exchanges/base.py
"""
交易所抽象基类

定义统一的交易接口，所有交易所实现必须继承此类
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from enum import Enum


class OrderSide(Enum):
    """订单方向"""
    BUY = "BUY"
    SELL = "SELL"


class PositionSide(Enum):
    """持仓方向"""
    LONG = "LONG"
    SHORT = "SHORT"
    BOTH = "BOTH"  # 单向持仓模式


class OrderType(Enum):
    """订单类型"""
    MARKET = "MARKET"
    LIMIT = "LIMIT"
    STOP_MARKET = "STOP_MARKET"
    TAKE_PROFIT_MARKET = "TAKE_PROFIT_MARKET"


@dataclass
class OrderResult:
    """下单结果"""
    success: bool
    order_id: str = None
    symbol: str = None
    side: str = None
    position_side: str = None
    qty: float = 0
    price: float = 0
    avg_price: float = 0
    status: str = None
    error: str = None
    raw: Dict = None  # 原始响应
    
    def to_dict(self) -> Dict:
        return {
            "success": self.success,
            "order_id": self.order_id,
            "symbol": self.symbol,
            "side": self.side,
            "position_side": self.position_side,
            "qty": self.qty,
            "price": self.price,
            "avg_price": self.avg_price,
            "status": self.status,
            "error": self.error,
        }


@dataclass
class Position:
    """持仓信息"""
    symbol: str
    side: str  # LONG / SHORT
    qty: float
    entry_price: float
    unrealized_pnl: float = 0
    leverage: int = 1
    margin_type: str = "cross"
    liquidation_price: float = 0
    break_even_price: float = 0  # 盈亏平衡价
    exchange: str = None
    raw: Dict = None
    
    def to_dict(self) -> Dict:
        return {
            "symbol": self.symbol,
            "side": self.side,
            "qty": self.qty,
            "entry_price": self.entry_price,
            "unrealized_pnl": self.unrealized_pnl,
            "leverage": self.leverage,
            "margin_type": self.margin_type,
            "liquidation_price": self.liquidation_price,
            "break_even_price": self.break_even_price,
            "exchange": self.exchange,
        }


@dataclass
class AccountInfo:
    """账户信息"""
    total_balance: float
    available_balance: float
    unrealized_pnl: float
    margin_balance: float = 0
    positions: List[Position] = None
    exchange: str = None
    raw: Dict = None
    
    def to_dict(self) -> Dict:
        return {
            "total_balance": self.total_balance,
            "available_balance": self.available_balance,
            "unrealized_pnl": self.unrealized_pnl,
            "margin_balance": self.margin_balance,
            "positions": [p.to_dict() for p in (self.positions or [])],
            "exchange": self.exchange,
        }


class BaseExchange(ABC):
    """
    交易所抽象基类
    
    所有交易所实现必须继承此类并实现所有抽象方法
    """
    
    # 交易所名称
    EXCHANGE_NAME: str = "base"
    
    def __init__(
        self,
        api_key: str,
        api_secret: str,
        passphrase: str = None,
        is_testnet: bool = False,
        **kwargs
    ):
        self.api_key = api_key
        self.api_secret = api_secret
        self.passphrase = passphrase
        self.is_testnet = is_testnet
        self._client = None
    
    @property
    def exchange_name(self) -> str:
        return self.EXCHANGE_NAME
    
    # ==================== 账户相关 ====================
    
    @abstractmethod
    async def get_account(self) -> AccountInfo:
        """
        获取账户信息
        
        Returns:
            AccountInfo 对象
        """
        pass
    
    @abstractmethod
    async def get_balance(self) -> float:
        """
        获取可用余额
        
        Returns:
            可用余额 (USDT)
        """
        pass
    
    @abstractmethod
    async def get_positions(self) -> List[Position]:
        """
        获取所有持仓
        
        Returns:
            Position 列表
        """
        pass
    
    @abstractmethod
    async def get_position(self, symbol: str) -> Optional[Position]:
        """
        获取指定币种的持仓
        
        Args:
            symbol: 交易对
            
        Returns:
            Position 对象，无持仓返回 None
        """
        pass
    
    # ==================== 下单相关 ====================
    
    @abstractmethod
    async def market_open(
        self,
        symbol: str,
        side: str,  # LONG / SHORT
        qty: float,
        leverage: int = None,
    ) -> OrderResult:
        """
        市价开仓
        
        Args:
            symbol: 交易对
            side: 方向 (LONG/SHORT)
            qty: 数量
            leverage: 杠杆倍数（可选，不传则使用当前设置）
            
        Returns:
            OrderResult 对象
        """
        pass
    
    @abstractmethod
    async def market_close(
        self,
        symbol: str,
        side: str,  # LONG / SHORT
        qty: float = None,  # None 表示全部平仓
        cancel_tp_sl: bool = True,  # 是否取消止盈止损单
    ) -> OrderResult:
        """
        市价平仓
        
        Args:
            symbol: 交易对
            side: 方向 (LONG/SHORT)
            qty: 数量，None 表示全部平仓
            cancel_tp_sl: 是否取消止盈止损单（默认 True，减仓时传 False）
            
        Returns:
            OrderResult 对象
        """
        pass
    
    @abstractmethod
    async def limit_open(
        self,
        symbol: str,
        side: str,  # LONG / SHORT
        qty: float,
        price: float,
        leverage: int = None,
    ) -> OrderResult:
        """
        限价开仓 (GTC - 持续有效直到手动取消)
        
        Args:
            symbol: 交易对
            side: 方向 (LONG/SHORT)
            qty: 数量
            price: 限价价格
            leverage: 杠杆倍数（可选）
            
        Returns:
            OrderResult 对象
        """
        pass
    
    @abstractmethod
    async def set_stop_loss(
        self,
        symbol: str,
        side: str,  # LONG / SHORT
        qty: float,
        stop_price: float,
    ) -> OrderResult:
        """
        设置止损
        
        Args:
            symbol: 交易对
            side: 持仓方向 (LONG/SHORT)
            qty: 数量
            stop_price: 止损价格
            
        Returns:
            OrderResult 对象
        """
        pass
    
    @abstractmethod
    async def set_take_profit(
        self,
        symbol: str,
        side: str,  # LONG / SHORT
        qty: float,
        tp_price: float,
    ) -> OrderResult:
        """
        设置止盈
        
        Args:
            symbol: 交易对
            side: 持仓方向 (LONG/SHORT)
            qty: 数量
            tp_price: 止盈价格
            
        Returns:
            OrderResult 对象
        """
        pass
    
    @abstractmethod
    async def cancel_order(
        self,
        symbol: str,
        order_id: str,
    ) -> bool:
        """
        取消订单
        
        Args:
            symbol: 交易对
            order_id: 订单ID
            
        Returns:
            是否成功
        """
        pass
    
    @abstractmethod
    async def cancel_all_orders(
        self,
        symbol: str,
    ) -> int:
        """
        取消指定币种的所有订单
        
        Args:
            symbol: 交易对
            
        Returns:
            取消的订单数量
        """
        pass
    
    # ==================== 设置相关 ====================
    
    @abstractmethod
    async def set_leverage(
        self,
        symbol: str,
        leverage: int,
    ) -> bool:
        """
        设置杠杆
        
        Args:
            symbol: 交易对
            leverage: 杠杆倍数
            
        Returns:
            是否成功
        """
        pass
    
    @abstractmethod
    async def set_margin_type(
        self,
        symbol: str,
        margin_type: str,  # cross / isolated
    ) -> bool:
        """
        设置保证金模式
        
        Args:
            symbol: 交易对
            margin_type: 保证金类型 (cross/isolated)
            
        Returns:
            是否成功
        """
        pass
    
    # ==================== 市场数据 ====================
    
    @abstractmethod
    async def get_ticker_price(self, symbol: str) -> float:
        """
        获取最新价格
        
        Args:
            symbol: 交易对
            
        Returns:
            最新价格
        """
        pass
    
    @abstractmethod
    async def get_symbol_info(self, symbol: str) -> Dict:
        """
        获取交易对信息（精度、最小数量等）
        
        Args:
            symbol: 交易对
            
        Returns:
            交易对信息
        """
        pass
    
    # ==================== WebSocket ====================
    
    @abstractmethod
    def create_ws_client(self, callbacks: Dict[str, callable] = None) -> Any:
        """
        创建 WebSocket 客户端
        
        Args:
            callbacks: 回调函数字典
                - on_account_update: 账户更新回调
                - on_order_update: 订单更新回调
                - on_position_update: 持仓更新回调
                
        Returns:
            WebSocket 客户端实例
        """
        pass
    
    # ==================== 辅助方法 ====================
    
    def normalize_symbol(self, symbol: str) -> str:
        """
        标准化交易对格式
        
        不同交易所可能有不同的格式：
        - Binance: BTCUSDT
        - OKX: BTC-USDT-SWAP
        - Hyperliquid: BTC
        
        Args:
            symbol: 输入的交易对（统一使用 BTCUSDT 格式）
            
        Returns:
            该交易所的交易对格式
        """
        return symbol
    
    def denormalize_symbol(self, symbol: str) -> str:
        """
        反标准化交易对格式（从交易所格式转为统一格式）
        
        Args:
            symbol: 交易所格式的交易对
            
        Returns:
            统一格式 (BTCUSDT)
        """
        return symbol
    
    async def close(self):
        """关闭连接"""
        pass
    
    def __repr__(self):
        return f"<{self.__class__.__name__} testnet={self.is_testnet}>"
