# commission_service.py
"""
返佣服务模块

功能：
1. 在交易完成时触发返佣发放
2. 更新用户收益统计（排行榜）- 实时更新，无需定时对账
3. 定期结算返佣

改造说明：
- 支持纯 asyncio 模式（run_async 方法）
- 保留 threading 模式兼容旧代码
- 排行榜统计由 on_trade_closed() 实时更新，保留手动对账作为数据修复工具
"""

import asyncio
import logging
import threading
import time
from datetime import datetime
from typing import Optional
from decimal import Decimal

logger = logging.getLogger(__name__)


class CommissionService:
    """
    返佣服务
    
    职责：
    1. 监听交易完成事件，发放返佣
    2. 更新用户收益统计（实时更新）
    3. 定时结算返佣（pending -> settled）
    
    注意：排行榜数据由 on_trade_closed() 实时更新，不再需要定时对账。
    保留手动对账功能作为数据修复工具。
    """
    
    def __init__(self, settlement_interval_s: float = 300.0, reconcile_interval_s: float = 3600.0):
        """
        Args:
            settlement_interval_s: 返佣结算间隔（秒），默认5分钟
            reconcile_interval_s: 对账间隔（秒），已废弃，保留参数兼容性
        """
        self.settlement_interval_s = settlement_interval_s
        self.reconcile_interval_s = reconcile_interval_s  # 保留但不再使用
        self._stop_event = threading.Event()
        self._settlement_thread: Optional[threading.Thread] = None
        self._reconcile_thread: Optional[threading.Thread] = None  # 保留但不再启动
        self._last_reconcile_time: Optional[datetime] = None
        
        # 异步模式状态
        self._async_running = False
        
    def start(self):
        """启动返佣服务"""
        if self._settlement_thread and self._settlement_thread.is_alive():
            return
        
        self._stop_event.clear()
        
        # 启动结算线程
        self._settlement_thread = threading.Thread(
            target=self._settlement_loop,
            name="CommissionSettlement",
            daemon=True
        )
        self._settlement_thread.start()
        
        # 注意：不再启动定时对账线程，排行榜数据由 on_trade_closed() 实时更新
        # 保留手动对账功能 reconcile_now() 作为数据修复工具
        
        logger.info("返佣服务已启动（排行榜实时更新，无定时对账）")
    
    def stop(self):
        """停止返佣服务"""
        self._stop_event.set()
        if self._settlement_thread:
            self._settlement_thread.join(timeout=5)
        if self._reconcile_thread:
            self._reconcile_thread.join(timeout=5)
        logger.info("返佣服务已停止")
    
    def _settlement_loop(self):
        """定时结算循环"""
        from core.referral_db import referral_db
        
        while not self._stop_event.is_set():
            try:
                # 结算所有待结算的返佣
                count = referral_db.settle_commissions()
                if count > 0:
                    logger.info(f"定时结算: {count} 条返佣记录")
            except Exception as e:
                logger.error(f"返佣结算异常: {e}")
            
            # 等待下一次结算
            self._stop_event.wait(self.settlement_interval_s)
    
    def _reconcile_loop(self):
        """
        定时对账循环 - 已废弃
        
        排行榜数据现在由 on_trade_closed() 实时更新，不再需要定时对账。
        保留此方法但不再调用，手动对账请使用 reconcile_now()。
        """
        pass  # 不再执行定时对账
    
    def reconcile_now(self) -> dict:
        """立即执行对账（手动触发）"""
        from core.referral_db import referral_db
        
        logger.info("手动触发排行榜对账...")
        try:
            results = referral_db.reconcile_all_users()
            self._last_reconcile_time = datetime.utcnow()
            return {"success": True, "users": len(results), "details": results}
        except Exception as e:
            logger.error(f"手动对账失败: {e}")
            return {"success": False, "error": str(e)}
    
    def get_reconcile_status(self) -> dict:
        """获取对账状态（定时对账已废弃，仅保留手动对账）"""
        return {
            "last_reconcile_time": self._last_reconcile_time.isoformat() if self._last_reconcile_time else None,
            "auto_reconcile_enabled": False,  # 定时对账已废弃
            "note": "排行榜数据由 on_trade_closed() 实时更新，定时对账已废弃。可通过 reconcile_now() 手动触发对账修复数据。"
        }
    
    def on_trade_closed(
        self,
        uid: str,
        symbol: str,
        side: str,
        net_pnl: float,
        realized_pnl: float = None,
        fee_total: float = None,
        trade_id: str = None
    ):
        """
        交易完成回调 - 发放返佣并更新统计
        
        Args:
            uid: 用户ID
            symbol: 交易对
            side: 方向 (LONG/SHORT)
            net_pnl: 净收益
            realized_pnl: 已实现收益（可选）
            fee_total: 手续费（可选）
            trade_id: 交易ID（可选）
        """
        from core.referral_db import referral_db, CommissionSourceType
        
        try:
            # 1. 发放返佣（基于净收益，只有盈利才发放）
            if net_pnl > 0:
                commissions = referral_db.distribute_commission(
                    from_uid=uid,
                    source_type=CommissionSourceType.TRADING_PROFIT.value,
                    source_amount=net_pnl,
                    source_id=trade_id
                )
                
                if commissions:
                    total_commission = sum(c["amount"] for c in commissions)
                    logger.info(f"[{uid}] 交易 {symbol} 盈利 {net_pnl:.2f}, "
                               f"发放返佣 {len(commissions)} 层, 总额 {total_commission:.4f}")
            
            # 2. 更新收益统计（排行榜）
            is_win = net_pnl > 0
            
            # 更新各周期的统计
            for period in ["daily", "weekly", "monthly", "all_time"]:
                try:
                    referral_db.update_user_profit_stats(
                        uid=uid,
                        period_type=period,
                        profit=net_pnl,
                        is_win=is_win
                    )
                except Exception as e:
                    logger.warning(f"[{uid}] 更新 {period} 统计失败: {e}")
            
            logger.debug(f"[{uid}] 交易统计已更新: {symbol} {side} pnl={net_pnl:.2f}")
            
        except Exception as e:
            logger.error(f"[{uid}] 处理交易完成事件失败: {e}")
    
    # ==================== 纯 Asyncio 模式 ====================
    
    async def run_async(self):
        """
        纯 asyncio 模式运行
        
        在 main_async.py 中通过 lifecycle 启动：
        lifecycle.add_background_task(commission_service.run_async, "commission")
        """
        if self._async_running:
            logger.warning("CommissionService 已在 async 模式运行")
            return
        
        self._async_running = True
        logger.info("返佣服务启动 (asyncio 模式)")
        
        try:
            # 只运行结算任务，不再运行定时对账
            # 排行榜数据由 on_trade_closed() 实时更新
            await self._async_settlement_loop()
        finally:
            self._async_running = False
            logger.info("返佣服务停止 (asyncio 模式)")
    
    async def stop_async(self):
        """停止异步模式（用于 graceful shutdown）"""
        self._async_running = False
    
    async def _async_settlement_loop(self):
        """异步结算循环"""
        from core.referral_db import referral_db
        from core.async_helpers import run_sync
        
        while self._async_running:
            try:
                # 在线程池中执行同步数据库操作
                count = await run_sync(referral_db.settle_commissions)
                if count > 0:
                    logger.info(f"定时结算: {count} 条返佣记录")
            except asyncio.CancelledError:
                logger.info("结算任务被取消")
                break
            except Exception as e:
                logger.error(f"返佣结算异常: {e}")
            
            # 异步等待
            try:
                await asyncio.sleep(self.settlement_interval_s)
            except asyncio.CancelledError:
                break
    
    async def _async_reconcile_loop(self):
        """
        异步对账循环 - 已废弃
        
        排行榜数据现在由 on_trade_closed() 实时更新，不再需要定时对账。
        保留此方法但不再调用，手动对账请使用 reconcile_now_async()。
        """
        pass  # 不再执行定时对账
    
    async def reconcile_now_async(self) -> dict:
        """立即执行对账（异步版本）"""
        from core.referral_db import referral_db
        from core.async_helpers import run_sync
        
        logger.info("手动触发排行榜对账...")
        try:
            results = await run_sync(referral_db.reconcile_all_users)
            self._last_reconcile_time = datetime.utcnow()
            return {"success": True, "users": len(results) if results else 0, "details": results}
        except Exception as e:
            logger.error(f"手动对账失败: {e}")
            return {"success": False, "error": str(e)}
    
    # ==================== 事件回调（同步/异步通用） ====================
    
    def on_fee_charged(
        self,
        uid: str,
        fee_amount: float,
        fee_type: str = "trading",
        source_id: str = None
    ):
        """
        手续费回调 - 可选：基于手续费发放返佣
        
        Args:
            uid: 用户ID
            fee_amount: 手续费金额
            fee_type: 手续费类型
            source_id: 来源ID
        """
        from core.referral_db import referral_db, CommissionSourceType
        
        if fee_amount <= 0:
            return
        
        try:
            # 基于手续费发放返佣
            commissions = referral_db.distribute_commission(
                from_uid=uid,
                source_type=CommissionSourceType.TRADING_FEE.value,
                source_amount=fee_amount,
                source_id=source_id
            )
            
            if commissions:
                logger.debug(f"[{uid}] 手续费返佣: {len(commissions)} 层")
                
        except Exception as e:
            logger.error(f"[{uid}] 处理手续费返佣失败: {e}")


# 全局服务实例
commission_service = CommissionService()


def trigger_commission_on_trade_close(
    uid: str,
    symbol: str,
    side: str,
    net_pnl: float,
    **kwargs
):
    """
    便捷函数：在交易完成时触发返佣
    
    可以在 cycle_store.py 的 _close_cycle 方法中调用
    """
    commission_service.on_trade_closed(
        uid=uid,
        symbol=symbol,
        side=side,
        net_pnl=net_pnl,
        **kwargs
    )
