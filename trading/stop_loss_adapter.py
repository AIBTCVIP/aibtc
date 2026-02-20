# trading/stop_loss_adapter.py
"""
止损管理器适配器

让现有的 StopLossManager 调用自动路由到 GlobalStopLossProcessor

迁移策略：
1. 保持原有 API 不变
2. 内部自动注册到全局处理器
3. 支持渐进式迁移（可配置使用新/旧架构）

使用方式：
    # 方式1：直接替换（推荐）
    from trading.stop_loss_adapter import StopLossManagerAdapter as StopLossManager
    
    # 方式2：通过环境变量控制
    USE_GLOBAL_SLP=1  # 使用新架构
    USE_GLOBAL_SLP=0  # 使用旧架构
"""

from __future__ import annotations

import os
import logging
from decimal import Decimal
from typing import Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from core.user_context import UserContext

logger = logging.getLogger(__name__)

# 是否使用全局处理器
USE_GLOBAL_SLP = os.environ.get("USE_GLOBAL_SLP", "1") == "1"


class StopLossManagerAdapter:
    """
    止损管理器适配器
    
    兼容原有 StopLossManager API，内部使用 GlobalStopLossProcessor
    """
    
    # 保持与原 StopLossManager 相同的类属性
    SL_PCT = Decimal("0.05")
    TP_PCT = Decimal("0.05")
    TRAIL_STAGES = [
        (Decimal("2.2"), Decimal("0.01")),
        (Decimal("3.5"), Decimal("0.02")),
        (Decimal("5.5"), Decimal("0.04")),
        (Decimal("7.5"), Decimal("0.055")),
        (Decimal("10.0"), Decimal("0.08")),
    ]
    
    def __init__(
        self,
        redis_conn,
        uid: str,
        execute_trade_func=None,
        user_context: Optional["UserContext"] = None,
    ):
        self.rds = redis_conn
        self.uid = uid
        self._execute_trade_func = execute_trade_func
        self._user_context = user_context
        
        # 当前交易所（由 cycle_store 设置）
        self._current_exchange: Optional[str] = None
        
        if USE_GLOBAL_SLP:
            # 注册到全局处理器
            self._register_to_global()
        else:
            # 使用原有实现
            from trading.stop_loss_manager import StopLossManager
            self._legacy_manager = StopLossManager(
                redis_conn, uid, execute_trade_func, user_context
            )
    
    def _register_to_global(self):
        """注册到全局处理器"""
        try:
            from trading.global_stop_loss_processor import get_global_stop_loss_processor
            processor = get_global_stop_loss_processor()
            # 注册将在 set_exchange 时完成
            logger.debug(f"[SLAdapter] 用户 {self.uid} 准备注册到全局处理器")
        except Exception as e:
            logger.error(f"[SLAdapter] 注册失败: {e}")
    
    def set_exchange(self, exchange: str):
        """设置当前交易所"""
        self._current_exchange = exchange
        
        if USE_GLOBAL_SLP:
            from trading.global_stop_loss_processor import get_global_stop_loss_processor
            processor = get_global_stop_loss_processor()
            processor.register_user(self.uid, exchange)
    
    # =========================================================================
    # 原有 API 兼容
    # =========================================================================
    
    def set_initial_stops(
        self,
        symbol: str,
        side: str,
        entry: Decimal,
        order_type: Optional[str] = None,
        exchange: Optional[str] = None,
    ) -> tuple[str, str]:
        """设置初始止盈止损"""
        if USE_GLOBAL_SLP:
            return self._set_initial_stops_global(symbol, side, entry, order_type, exchange)
        else:
            return self._legacy_manager.set_initial_stops(symbol, side, entry, order_type, exchange)
    
    def _set_initial_stops_global(
        self,
        symbol: str,
        side: str,
        entry: Decimal,
        order_type: Optional[str] = None,
        exchange: Optional[str] = None,
    ) -> tuple[str, str]:
        """使用全局处理器设置初始止盈止损"""
        from core.utils import d_to_str
        
        # 市价单不设置止盈止损
        if order_type:
            order_type_upper = str(order_type).strip().upper()
            if order_type_upper in ("MARKET", "STOP_MARKET"):
                return "0", "0"
        
        # 计算止损止盈价格
        if entry <= 0:
            return "0", "0"
        
        if (side or "").upper() == "LONG":
            sl = entry * (Decimal("1") - self.SL_PCT)
            tp = entry * (Decimal("1") + self.TP_PCT)
        else:
            sl = entry * (Decimal("1") + self.SL_PCT)
            tp = entry * (Decimal("1") - self.TP_PCT)
        
        stop_loss = d_to_str(sl)
        take_profit = d_to_str(tp)
        
        # 通过全局处理器下单
        from trading.global_stop_loss_processor import get_global_stop_loss_processor, OrderTask
        processor = get_global_stop_loss_processor()
        
        processor._enqueue_order(OrderTask(
            uid=self.uid,
            exchange=exchange or self._current_exchange or "binance",
            symbol=symbol,
            action="stop_orders",
            stop_loss=stop_loss,
            take_profit=take_profit,
            priority=1  # 高优先级
        ))
        
        logger.info(f"[SLAdapter] 设置初始止盈止损: {symbol} {side} sl={stop_loss} tp={take_profit}")
        
        return stop_loss, take_profit
    
    def on_mark_tick(
        self,
        sym: str,
        mark: Decimal,
        ts: int,
        exchange: str = None
    ) -> None:
        """Mark 价格更新回调"""
        if USE_GLOBAL_SLP:
            # 全局处理器会批量处理，这里只需确保用户已注册
            if exchange and exchange != self._current_exchange:
                self.set_exchange(exchange)
            
            # 调用全局处理器（单币种模式）
            from trading.global_stop_loss_processor import get_global_stop_loss_processor
            processor = get_global_stop_loss_processor()
            processor.on_mark_tick(sym, mark, ts, exchange)
        else:
            self._legacy_manager.on_mark_tick(sym, mark, ts, exchange)
    
    def reset_trailing_on_add_position(
        self,
        symbol: str,
        side: str,
        exchange: str = None
    ) -> None:
        """加仓后重置移动止损阶段"""
        if USE_GLOBAL_SLP:
            # 清除本地缓存，让全局处理器重新读取
            from trading.global_stop_loss_processor import get_global_stop_loss_processor
            processor = get_global_stop_loss_processor()
            processor._clear_user_cache(self.uid, exchange)
            logger.info(f"[SLAdapter] 重置移动止损: {symbol} {side}")
        else:
            self._legacy_manager.reset_trailing_on_add_position(symbol, side, exchange)
    
    def confirm_trailing_from_order(self, o: dict, exchange: str = None) -> None:
        """从订单回报确认移动止损"""
        if USE_GLOBAL_SLP:
            # 全局处理器会通过 Redis 更新自动感知
            # 这里只需要更新 Redis 中的确认状态
            self._confirm_trailing_global(o, exchange)
        else:
            self._legacy_manager.confirm_trailing_from_order(o, exchange)
    
    def _confirm_trailing_global(self, o: dict, exchange: str = None) -> None:
        """全局模式下确认移动止损"""
        from core.pf_compatibility import pf_compat
        from core.utils import now_ms
        
        exec_type = (o.get("x") or "").upper()
        if exec_type not in ("NEW", "TRADE"):
            return
        
        symbol = o.get("s")
        ps = (o.get("ps") or "").upper()
        if not symbol or ps not in ("LONG", "SHORT"):
            return
        
        order_type = (o.get("o") or "").upper()
        is_stop_like = any(k in order_type for k in ("STOP", "STOP_MARKET", "STOP_LOSS"))
        if not is_stop_like:
            return
        
        field = f"{symbol}:{ps}"
        exchange = exchange or self._current_exchange
        
        try:
            cycle_data = pf_compat.get_pf_cycle(self.uid, exchange=exchange)
            cyc = cycle_data.get(field, {})
            
            if not cyc:
                return
            
            p_stage = int(cyc.get("slTrailPendingStage", "0") or "0")
            if p_stage <= 0:
                return
            
            # pending -> 正式
            cyc["slTrailStage"] = str(p_stage)
            cyc["slTrailStopLoss"] = str(cyc.get("slTrailPendingStopLoss", "0") or "0")
            cyc["slTrailLastTs"] = str(cyc.get("slTrailPendingTs", "0") or "0")
            
            # 清空 pending
            cyc["slTrailPendingStage"] = "0"
            cyc["slTrailPendingStopLoss"] = "0"
            cyc["slTrailPendingTs"] = "0"
            cyc["slTrailPendingMark"] = "0"
            cyc["slTrailPendingPct"] = "0"
            
            cycle_data[field] = cyc
            pf_compat.set_pf_cycle(self.uid, cycle_data, exchange=exchange)
            
            # 同步 pos
            pos_data = pf_compat.get_pf_pos(self.uid, exchange=exchange)
            pos = pos_data.get(field, {})
            if pos:
                pos["slTrailStage"] = cyc["slTrailStage"]
                pos["slTrailStopLoss"] = cyc["slTrailStopLoss"]
                pos["slTrailLastTs"] = cyc["slTrailLastTs"]
                pos["slTrailPendingStage"] = "0"
                pos["slTrailPendingStopLoss"] = "0"
                pos["slTrailPendingTs"] = "0"
                pos["updatedAt"] = str(now_ms())
                pos_data[field] = pos
                pf_compat.set_pf_pos(self.uid, pos_data, exchange=exchange)
            
            logger.info(f"[SLAdapter] 确认移动止损: {symbol} {ps} stage={p_stage}")
            
        except Exception as e:
            logger.error(f"[SLAdapter] 确认移动止损失败: {e}")
    
    def update_config(
        self,
        *,
        sl_pct: Optional[Decimal] = None,
        tp_pct: Optional[Decimal] = None,
        trail_stages: Optional[list] = None,
    ) -> None:
        """动态更新止损配置"""
        from core.utils import D
        
        if sl_pct is not None:
            self.SL_PCT = D(sl_pct)
        if tp_pct is not None:
            self.TP_PCT = D(tp_pct)
        if trail_stages is not None:
            self.TRAIL_STAGES = [(D(t[0]), D(t[1])) for t in trail_stages]
        
        if USE_GLOBAL_SLP:
            # 更新全局处理器配置
            from trading.global_stop_loss_processor import get_global_stop_loss_processor
            processor = get_global_stop_loss_processor()
            processor.TRAIL_STAGES = self.TRAIL_STAGES
    
    # =========================================================================
    # 类方法兼容
    # =========================================================================
    
    @classmethod
    def get_pending_tasks_count(cls) -> int:
        """获取待处理任务数量"""
        if USE_GLOBAL_SLP:
            from trading.global_stop_loss_processor import get_global_stop_loss_processor
            processor = get_global_stop_loss_processor()
            return processor.get_stats().get("pending_orders", 0)
        else:
            from trading.stop_loss_manager import StopLossManager
            return StopLossManager.get_pending_tasks_count()
    
    @classmethod
    async def wait_pending_tasks(cls, timeout: float = 10.0) -> int:
        """等待所有待处理任务完成"""
        if USE_GLOBAL_SLP:
            import asyncio
            from trading.global_stop_loss_processor import get_global_stop_loss_processor
            processor = get_global_stop_loss_processor()
            
            start = asyncio.get_event_loop().time()
            while asyncio.get_event_loop().time() - start < timeout:
                pending = processor.get_stats().get("pending_orders", 0)
                if pending == 0:
                    return 0
                await asyncio.sleep(0.1)
            
            return processor.get_stats().get("pending_orders", 0)
        else:
            from trading.stop_loss_manager import StopLossManager
            return await StopLossManager.wait_pending_tasks(timeout)
    
    def cleanup(self):
        """
        显式清理资源
        
        应在 cycle_store.stop() 时调用，而非依赖 __del__
        """
        if USE_GLOBAL_SLP and self._current_exchange:
            try:
                from trading.global_stop_loss_processor import get_global_stop_loss_processor
                processor = get_global_stop_loss_processor()
                processor.unregister_user(self.uid, self._current_exchange)
                logger.debug(f"[SLAdapter] 用户 {self.uid}/{self._current_exchange} 已注销")
            except Exception as e:
                logger.debug(f"[SLAdapter] 清理失败: {e}")
    
    def __del__(self):
        """
        析构函数 - 仅作为备用清理
        
        注意：不应依赖此方法，应显式调用 cleanup()
        """
        # 不在 __del__ 中做复杂操作，避免循环引用问题
        pass


# =============================================================================
# 便捷函数
# =============================================================================

def create_stop_loss_manager(
    redis_conn,
    uid: str,
    execute_trade_func=None,
    user_context=None,
    exchange: str = None,
) -> StopLossManagerAdapter:
    """
    创建止损管理器
    
    自动选择使用全局处理器或传统实现
    """
    manager = StopLossManagerAdapter(
        redis_conn, uid, execute_trade_func, user_context
    )
    
    if exchange:
        manager.set_exchange(exchange)
    
    return manager
