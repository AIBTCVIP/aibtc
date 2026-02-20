# positions.py
"""
仓位服务入口模块

整合:
1. WS 数据存储 (cycle_store)
2. 止损管理 (stop_loss_manager)
3. 资金费轮询
4. 仓位审计 (position_auditor)
"""

import redis
import logging
import traceback
import threading
import time
from typing import Optional, TYPE_CHECKING

from binance.client import Client
from websocket.cycle_store import BinanceUMHedgeWSOnlyStore
from cache.position_auditor import PositionAuditor, AuditReport
from core.config import REDIS_HOST, REDIS_PORT, REDIS_DB
from core.utils import D

if TYPE_CHECKING:
    from core.user_context import UserContext

logger = logging.getLogger(__name__)


# 移除旧的pf:*键函数，现在使用兼容层
# funding数据现在存储在用户聚合键的cache字段中


class PositionService:
    def __init__(
        self,
        uid="aibtcvip",
        client: Client = None,  # 支持传入 client（多用户模式）
        funding_every_s: float = 60.0,
        audit_every_s: float = 61.0,
        audit_dry_run: bool = False,
        user_context: Optional['UserContext'] = None,  # 多用户上下文
    ):
        self.uid = uid
        self.funding_every_s = funding_every_s
        self.audit_every_s = audit_every_s
        self.audit_dry_run = audit_dry_run
        self._user_context = user_context

        # 支持传入 client（多用户模式）或使用全局配置（单用户模式）
        if client is not None:
            self.client = client
        else:
            # 多用户模式下不应使用全局配置
            raise ValueError("多用户模式下必须提供 client 参数")

        # P2 Fix: 使用共享 Redis 连接池
        from core.database import redis_client
        self.rds = redis_client

        # WS-only store (包含止损管理)
        self.store = BinanceUMHedgeWSOnlyStore(self.client, self.rds, uid=uid, user_context=user_context)
        self.store.start()

        # 仓位审计器
        self.auditor = PositionAuditor(
            client=client,
            redis_conn=self.rds,
            uid=uid,
            audit_interval_s=audit_every_s,
            dry_run=audit_dry_run,
            on_report=self._on_audit_report,
        )

    def _on_audit_report(self, report):
        """审计报告回调"""
        # 只在有问题时打印详细信息
        if report.has_issues():
            print(report.summary())
        # 可以在这里添加告警逻辑，比如发送到监控系统

    # -----------------------
    # 资金费轮询（已废弃 - 改用 WebSocket）
    # -----------------------
    def poll_funding_once(self):
        """
        [已废弃] 拉取资金费（FUNDING_FEE）并写入 active cycle
        
        改造说明：
        - 资金费现在通过 WebSocket ACCOUNT_UPDATE 事件实时推送
        - 处理逻辑已移至 websocket/cycle_store.py 的 _on_funding_fee() 方法
        - 此方法保留但不再执行实际操作，避免重复 API 调用
        
        如需回退到 REST 轮询，取消下面的 return 注释
        """
        # 已改用 WebSocket 推送，不再使用 REST 轮询
        # 这可以节省大量 API 调用（1000用户 = -1000次/分钟）
        return
        
        # ====== 以下为原 REST 轮询代码（已禁用）======
        # uid = self.uid
        # from core.pf_compatibility import pf_compat
        # cache_data = pf_compat.get_pf_cache(uid)
        # raw_last = cache_data.get("funding_last_ts") if cache_data else None
        # if raw_last:
        #     start_ms = int(raw_last)
        # else:
        #     start_ms = int(time.time() * 1000) - 24 * 3600 * 1000
        # 
        # incomes = self.client.futures_income_history(
        #     incomeType="FUNDING_FEE",
        #     startTime=start_ms,
        #     limit=100
        # )
        # ... (省略剩余代码)

    def run_loop(self, stop_event: threading.Event):
        next_funding_ts = 0.0

        # 启动审计器
        self.auditor.start()

        while not stop_event.is_set():
            try:
                now = time.time()
                if now >= next_funding_ts:
                    self.poll_funding_once()
                    next_funding_ts = now + float(self.funding_every_s)
            except Exception as e:
                print("loop ERROR:", repr(e))
                logger.exception("仓位服务主循环错误")

            stop_event.wait(1.0)

        # 停止审计器
        self.auditor.stop()
        # print("📌 PositionService stopped (WS-only + funding poll + auditor)")

    def stop(self):
        """停止所有组件"""
        self.store.stop()
        self.auditor.stop()

    # -----------------------
    # 审计相关接口
    # -----------------------
    def audit_now(self, dry_run: bool = None) -> 'AuditReport':
        """
        立即执行一次审计

        Args:
            dry_run: 是否只报告不修复（None 使用默认配置）

        Returns:
            AuditReport
        """
        return self.auditor.audit_once(dry_run=dry_run)

    def get_last_audit_report(self):
        """获取最近一次审计报告"""
        return self.auditor.get_last_report()

    def set_audit_dry_run(self, dry_run: bool):
        """设置审计模式"""
        self.auditor.set_dry_run(dry_run)

    def set_audit_interval(self, interval_s: float):
        """设置审计间隔"""
        self.auditor.set_interval(interval_s)

def start_position_service(
    stop_event: threading.Event,
    uid: str = "aibtcvip",
    client: Client = None,  # 支持传入 client（多用户模式）
    funding_every_s: float = 60.0,
    audit_every_s: float = 61.0,
    audit_dry_run: bool = False,
) -> tuple[PositionService, threading.Thread]:

    service = PositionService(
        uid=uid,
        client=client,
        funding_every_s=funding_every_s,
        audit_every_s=audit_every_s,
        audit_dry_run=audit_dry_run,
    )

    t = threading.Thread(
        target=service.run_loop,
        args=(stop_event,),
        name=f"PositionService({uid})",
        daemon=True,
    )
    t.start()
    return service, t