# exchanges/hyperliquid/__init__.py
"""
Hyperliquid 交易所模块

包含:
- websocket.py: WebSocket 数据流 (userEvents, allMids)
- cycle_store.py: 周期跟踪 (WebSocket + REST 混合模式)
- position_store.py: 持仓数据存储 (REST 轮询备份)
- position_auditor.py: 持仓审计器

WebSocket 订阅:
- userEvents: fills, funding, liquidation
- allMids: 标记价格

注意: Hyperliquid WebSocket 不推送持仓变化，仍需 REST 轮询补充
"""

EXCHANGE_NAME = "hyperliquid"

from exchanges.hyperliquid.websocket import (
    HyperliquidUserStream,
    HyperliquidMarkPriceStream,
    HyperliquidWebSocket,
    ConnectionState,
)
from exchanges.hyperliquid.cycle_store import HyperliquidCycleStore
from exchanges.hyperliquid.position_auditor import HyperliquidPositionAuditor

__all__ = [
    "EXCHANGE_NAME",
    "HyperliquidUserStream",
    "HyperliquidMarkPriceStream",
    "HyperliquidWebSocket",
    "ConnectionState",
    "HyperliquidCycleStore",
    "HyperliquidPositionAuditor",
]
