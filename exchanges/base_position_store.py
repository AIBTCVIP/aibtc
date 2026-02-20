# exchanges/base_position_store.py
"""
通用持仓数据存储基类

提供持仓、周期数据的 Redis 存储功能，
用于没有 WebSocket 支持的交易所（通过 REST API 轮询）
"""

import json
import logging
import threading
import time
import uuid
from abc import ABC, abstractmethod
from decimal import Decimal
from typing import Dict, List, Optional, Any, TYPE_CHECKING

from core.database import redis_client, RedisKeys
from core.pf_compatibility import pf_compat
from core.utils import D, now_ms

if TYPE_CHECKING:
    from core.user_context import UserContext

logger = logging.getLogger(__name__)


class BasePositionStore(ABC):
    """
    持仓数据存储基类
    
    功能:
    1. 通过 REST API 轮询更新持仓数据
    2. 维护 Redis 中的持仓和周期数据
    3. 使用多交易所隔离存储架构
    """
    
    # 子类需要覆盖
    EXCHANGE_NAME = "base"
    
    def __init__(
        self,
        uid: str,
        exchange_client: Any,
        *,
        poll_interval_s: float = 10.0,
        user_context: Optional['UserContext'] = None,
    ):
        self.uid = uid
        self.exchange = self.EXCHANGE_NAME
        self.client = exchange_client
        self.poll_interval_s = poll_interval_s
        self._user_context = user_context
        
        # 轮询线程
        self._stop_event = threading.Event()
        self._poll_thread: Optional[threading.Thread] = None
        
        # 上次同步的持仓数据（用于检测变化）
        self._last_positions: Dict[str, Dict] = {}
        
        logger.info(f"[{uid}][{self.exchange}] PositionStore 已创建")
    
    # ==================== 抽象方法 ====================
    
    @abstractmethod
    async def _fetch_positions_from_exchange(self) -> List[Dict]:
        """
        从交易所获取持仓数据
        
        Returns:
            List of position dicts with keys:
            - symbol: str
            - side: "LONG" | "SHORT"
            - qty: float
            - entryPrice: float
            - unrealizedPnl: float
            - leverage: int
            - marginType: str
        """
        pass
    
    @abstractmethod
    async def _fetch_account_from_exchange(self) -> Dict:
        """
        从交易所获取账户数据
        
        Returns:
            Dict with keys:
            - walletBalance: float
            - equity: float
            - unrealized: float
        """
        pass
    
    # ==================== 数据访问方法 ====================
    
    def get_positions(self) -> Dict[str, Dict]:
        """获取所有持仓"""
        return pf_compat.get_pf_pos(self.uid, self.exchange) or {}
    
    def get_position(self, symbol: str, side: str) -> Optional[Dict]:
        """获取单个持仓"""
        positions = self.get_positions()
        field = f"{symbol}:{side.upper()}"
        return positions.get(field)
    
    def get_cycles(self) -> Dict[str, Dict]:
        """获取所有周期"""
        return pf_compat.get_pf_cycle(self.uid, self.exchange) or {}
    
    def get_cycle(self, symbol: str, side: str) -> Optional[Dict]:
        """获取单个周期"""
        cycles = self.get_cycles()
        field = f"{symbol}:{side.upper()}"
        return cycles.get(field)
    
    def get_closed_trades(self) -> Dict[str, Dict]:
        """获取已关闭交易"""
        return pf_compat.get_pf_closed_h(self.uid, self.exchange) or {}
    
    def get_account(self) -> Optional[Dict]:
        """获取账户数据"""
        return pf_compat.get_pf_account(self.uid, self.exchange)
    
    # ==================== 同步逻辑 ====================
    
    def _sync_once(self):
        """执行一次同步"""
        import asyncio
        
        try:
            # 创建事件循环运行异步代码
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            try:
                # 同步账户数据
                loop.run_until_complete(self._sync_account())
                
                # 同步持仓数据
                loop.run_until_complete(self._sync_positions())
                
            finally:
                loop.close()
                
        except Exception as e:
            logger.error(f"[{self.uid}][{self.exchange}] 同步失败: {e}")
            logger.exception("同步失败")
    
    async def _sync_account(self):
        """同步账户数据"""
        try:
            account_data = await self._fetch_account_from_exchange()
            
            if account_data:
                ts = now_ms()
                account_obj = {
                    "uid": self.uid,
                    "ts": str(ts),
                    "walletBalance": str(account_data.get("walletBalance", 0)),
                    "equity": str(account_data.get("equity", 0)),
                    "unrealized": str(account_data.get("unrealized", 0)),
                    "source": "REST_POLL",
                    "exchange": self.exchange,
                }
                pf_compat.set_pf_account(self.uid, account_obj, self.exchange)
                
                # 设置初始权益（只设置一次）
                existing_equity = pf_compat.get_pf_equity_init(self.uid, self.exchange)
                if not existing_equity:
                    equity_init = {
                        "uid": self.uid,
                        "ts": str(ts),
                        "walletBalance": str(account_data.get("equity", 0)),
                        "source": "REST_POLL_INIT",
                        "exchange": self.exchange,
                    }
                    pf_compat.set_pf_equity_init(self.uid, equity_init, self.exchange)
                    
        except Exception as e:
            logger.warning(f"[{self.uid}][{self.exchange}] 同步账户失败: {e}")
    
    async def _sync_positions(self):
        """同步持仓数据"""
        try:
            exchange_positions = await self._fetch_positions_from_exchange()
            
            # 转换为 field -> position 映射
            current_positions: Dict[str, Dict] = {}
            for pos in exchange_positions:
                symbol = pos.get("symbol")
                side = pos.get("side", "").upper()
                qty = float(pos.get("qty", 0))
                
                if not symbol or not side or qty <= 0:
                    continue
                
                field = f"{symbol}:{side}"
                current_positions[field] = pos
            
            # 获取 Redis 中的数据
            redis_positions = self.get_positions()
            redis_cycles = self.get_cycles()
            active_list = pf_compat.get_pf_pos_active(self.uid, self.exchange) or []
            
            ts = now_ms()
            
            # 检测新开仓
            for field, pos in current_positions.items():
                if field not in redis_positions or field not in self._last_positions:
                    # 新持仓
                    self._on_position_opened(field, pos, ts, redis_positions, redis_cycles, active_list)
                else:
                    # 更新现有持仓
                    self._on_position_updated(field, pos, ts, redis_positions, redis_cycles)
            
            # 检测平仓
            for field in list(redis_positions.keys()):
                if field not in current_positions:
                    # 持仓已关闭
                    self._on_position_closed(field, ts, redis_positions, redis_cycles, active_list)
            
            # 保存更新后的数据
            pf_compat.set_pf_pos(self.uid, redis_positions, self.exchange)
            pf_compat.set_pf_cycle(self.uid, redis_cycles, self.exchange)
            pf_compat.set_pf_pos_active(self.uid, active_list, self.exchange)
            
            # 更新缓存
            self._last_positions = current_positions.copy()
            
        except Exception as e:
            logger.warning(f"[{self.uid}][{self.exchange}] 同步持仓失败: {e}")
    
    def _on_position_opened(
        self,
        field: str,
        pos: Dict,
        ts: int,
        redis_positions: Dict,
        redis_cycles: Dict,
        active_list: List[str],
    ):
        """处理新开仓"""
        symbol = pos.get("symbol")
        side = pos.get("side", "").upper()
        qty = float(pos.get("qty", 0))
        entry_price = float(pos.get("entryPrice", 0))
        
        # 创建持仓对象
        pos_obj = {
            "symbol": symbol,
            "side": side,
            "qty": str(qty),
            "entryPrice": str(entry_price),
            "breakEvenPrice": str(pos.get("breakEvenPrice", 0)),
            "unrealizedPnl": str(pos.get("unrealizedPnl", 0)),
            "marginType": pos.get("marginType", "cross"),
            "openTimeMs": str(ts),
            "updatedAt": str(ts),
            "exchange": self.exchange,
        }
        redis_positions[field] = pos_obj
        
        # 创建周期对象
        cycle_id = f"{self.exchange}:{symbol}:{side}:{int(ts // 1000)}:{uuid.uuid4().hex[:8]}"
        cycle_obj = {
            "cycleId": cycle_id,
            "uid": self.uid,
            "symbol": symbol,
            "side": side,
            "exchange": self.exchange,
            "openTimeMs": str(ts),
            "closeTimeMs": "0",
            "durationMs": "0",
            "openQty": str(qty),
            "openQuote": str(qty * entry_price) if entry_price > 0 else "0",
            "avgOpenPrice": str(entry_price),
            "closeQty": "0",
            "closeQuote": "0",
            "avgClosePrice": "0",
            "feeTotal": "0",
            "fundingTotal": "0",
            "realizedPnlEst": "0",
            "netPnl": "0",
            "peakPnl": "0",
            "minPnlAfterPeak": "0",
            "maxDrawdown": "0",
            "maxAbsQty": str(qty),
            "updatedAt": str(ts),
            "field": field,
            "source": "REST_POLL_OPEN",
        }
        redis_cycles[field] = cycle_obj
        
        # 添加到活跃列表
        if field not in active_list:
            active_list.append(field)
        
        logger.info(f"[{self.uid}][{self.exchange}] 检测到新持仓: {field} qty={qty}")
    
    def _on_position_updated(
        self,
        field: str,
        pos: Dict,
        ts: int,
        redis_positions: Dict,
        redis_cycles: Dict,
    ):
        """处理持仓更新"""
        old_pos = redis_positions.get(field, {})
        
        # 更新持仓数据
        redis_positions[field] = {
            **old_pos,
            "qty": str(pos.get("qty", 0)),
            "entryPrice": str(pos.get("entryPrice", 0)),
            "breakEvenPrice": str(pos.get("breakEvenPrice", 0)),
            "unrealizedPnl": str(pos.get("unrealizedPnl", 0)),
            "updatedAt": str(ts),
        }
        
        # 更新周期数据（如果存在）
        if field in redis_cycles:
            cycle = redis_cycles[field]
            qty = float(pos.get("qty", 0))
            old_max = float(cycle.get("maxAbsQty", 0))
            if qty > old_max:
                cycle["maxAbsQty"] = str(qty)
            cycle["updatedAt"] = str(ts)
    
    def _on_position_closed(
        self,
        field: str,
        ts: int,
        redis_positions: Dict,
        redis_cycles: Dict,
        active_list: List[str],
    ):
        """处理平仓"""
        # 关闭周期
        if field in redis_cycles:
            cycle = redis_cycles[field]
            
            # 设置关闭时间
            cycle["closeTimeMs"] = str(ts)
            open_t = int(cycle.get("openTimeMs", "0") or "0")
            cycle["durationMs"] = str(max(0, ts - open_t))
            cycle["updatedAt"] = str(ts)
            cycle["closeSource"] = "REST_POLL_CLOSE"
            
            # 获取或生成 cycle_id
            cycle_id = cycle.get("cycleId")
            if not cycle_id:
                symbol = cycle.get("symbol", "UNK")
                side = cycle.get("side", "UNK")
                cycle_id = f"{self.exchange}:{symbol}:{side}:{int(ts // 1000)}:{uuid.uuid4().hex[:8]}"
                cycle["cycleId"] = cycle_id
            
            # 保存到已关闭交易
            pf_compat.set_pf_closed_h(self.uid, cycle_id, cycle, self.exchange)
            
            # 从活跃周期中删除
            del redis_cycles[field]
            
            logger.info(f"[{self.uid}][{self.exchange}] 持仓已关闭: {field} netPnl={cycle.get('netPnl', '0')}")
            
            # 触发返佣发放和排行榜统计更新
            try:
                net_pnl = float(cycle.get("netPnl", "0") or "0")
                from core.commission_service import trigger_commission_on_trade_close
                trigger_commission_on_trade_close(
                    uid=self.uid,
                    symbol=cycle.get("symbol", ""),
                    side=cycle.get("side", ""),
                    net_pnl=net_pnl,
                    realized_pnl=float(cycle.get("realizedPnlEst", "0") or "0"),
                    fee_total=float(cycle.get("feeTotal", "0") or "0"),
                    trade_id=cycle_id
                )
            except Exception as e:
                logger.warning(f"[{self.uid}][{self.exchange}] 返佣触发失败: {e}")
        
        # 删除持仓
        if field in redis_positions:
            del redis_positions[field]
        
        # 从活跃列表中删除
        if field in active_list:
            active_list.remove(field)
    
    # ==================== 生命周期 ====================
    
    def start(self):
        """启动轮询"""
        if self._poll_thread and self._poll_thread.is_alive():
            return
        
        self._stop_event.clear()
        self._poll_thread = threading.Thread(
            target=self._poll_loop,
            name=f"PositionStore-{self.uid}-{self.exchange}",
            daemon=True,
        )
        self._poll_thread.start()
        logger.info(f"[{self.uid}][{self.exchange}] PositionStore 轮询已启动 (间隔 {self.poll_interval_s}s)")
    
    def stop(self):
        """停止轮询"""
        self._stop_event.set()
        if self._poll_thread:
            self._poll_thread.join(timeout=5.0)
        self._poll_thread = None
        logger.info(f"[{self.uid}][{self.exchange}] PositionStore 已停止")
    
    def _poll_loop(self):
        """轮询循环"""
        # 启动后等待一小段时间
        self._stop_event.wait(2.0)
        
        while not self._stop_event.is_set():
            try:
                self._sync_once()
            except Exception as e:
                logger.error(f"[{self.uid}][{self.exchange}] 轮询异常: {e}")
            
            self._stop_event.wait(self.poll_interval_s)
