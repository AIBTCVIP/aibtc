# exchanges/okx/__init__.py
"""
OKX 交易所模块

包含:
- websocket.py: WebSocket 数据流 (用户数据 + 标记价格)
- cycle_store.py: 周期跟踪 (CycleStore)
- position_store.py: 持仓数据存储 (REST 轮询备份)
- position_auditor.py: 持仓审计器
"""

EXCHANGE_NAME = "okx"

from exchanges.okx.websocket import (
    OKXUserStream,
    OKXMarkPriceStream,
    ConnectionState,
    AuthenticationError,
)
from exchanges.okx.cycle_store import OKXCycleStore
from exchanges.okx.position_auditor import OKXPositionAuditor

__all__ = [
    "EXCHANGE_NAME",
    "OKXUserStream",
    "OKXMarkPriceStream",
    "ConnectionState",
    "AuthenticationError",
    "OKXCycleStore",
    "OKXPositionAuditor",
]
