# exchanges/binance/__init__.py
"""
Binance 交易所模块

包含:
- client.py: API 客户端
- websocket.py: WebSocket 数据流
- position_store.py: 持仓/周期数据存储
- stop_loss.py: 止损管理
- auditor.py: 持仓审计
"""

from exchanges.binance.client import BinanceClient
from exchanges.binance.websocket import BinanceWebSocket
from exchanges.binance.position_store import BinancePositionStore

__all__ = [
    "BinanceClient",
    "BinanceWebSocket",
    "BinancePositionStore",
]

EXCHANGE_NAME = "binance"
