# exchanges/bitget/__init__.py
"""
Bitget 交易所模块

包含:
- websocket.py: WebSocket 数据流 (用户数据 + 标记价格)
- cycle_store.py: 周期跟踪 (CycleStore)
- position_store.py: 持仓数据存储 (REST 轮询备份)
- position_auditor.py: 持仓审计器
"""

EXCHANGE_NAME = "bitget"

from exchanges.bitget.websocket import (
    BitgetUserStream,
    BitgetMarkPriceStream,
    ConnectionState,
    AuthenticationError,
)
from exchanges.bitget.cycle_store import BitgetCycleStore
from exchanges.bitget.position_auditor import BitgetPositionAuditor

__all__ = [
    "EXCHANGE_NAME",
    "BitgetUserStream",
    "BitgetMarkPriceStream",
    "ConnectionState",
    "AuthenticationError",
    "BitgetCycleStore",
    "BitgetPositionAuditor",
]
