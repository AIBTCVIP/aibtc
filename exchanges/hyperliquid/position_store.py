# exchanges/hyperliquid/position_store.py
"""
Hyperliquid 持仓数据存储

通过 REST API 轮询同步持仓数据
"""

import logging
from typing import Dict, List, Optional, Any

from exchanges.base_position_store import BasePositionStore

logger = logging.getLogger(__name__)


class HyperliquidPositionStore(BasePositionStore):
    """
    Hyperliquid 持仓数据存储
    
    通过 REST API 轮询更新持仓和账户数据
    """
    
    EXCHANGE_NAME = "hyperliquid"
    
    async def _fetch_positions_from_exchange(self) -> List[Dict]:
        """从 Hyperliquid 获取持仓数据"""
        try:
            positions = await self.client.get_positions()
            
            result = []
            for pos in positions:
                result.append({
                    "symbol": pos.symbol,
                    "side": pos.side,
                    "qty": pos.qty,
                    "entryPrice": pos.entry_price,
                    "unrealizedPnl": pos.unrealized_pnl,
                    "leverage": pos.leverage,
                    "marginType": pos.margin_type,
                    # Hyperliquid API 不提供 breakEvenPrice，使用 entryPrice 作为近似值
                    "breakEvenPrice": pos.entry_price,
                })
            
            return result
            
        except Exception as e:
            logger.warning(f"[{self.uid}][hyperliquid] 获取持仓失败: {e}")
            return []
    
    async def _fetch_account_from_exchange(self) -> Dict:
        """从 Hyperliquid 获取账户数据"""
        try:
            account = await self.client.get_account()
            
            return {
                "walletBalance": account.total_balance,
                "equity": account.total_balance,
                "unrealized": account.unrealized_pnl,
            }
            
        except Exception as e:
            logger.warning(f"[{self.uid}][hyperliquid] 获取账户失败: {e}")
            return {}
