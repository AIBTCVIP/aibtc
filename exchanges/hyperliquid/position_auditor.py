# exchanges/hyperliquid/position_auditor.py
"""
Hyperliquid 持仓审计器

定期比对 Redis 中的持仓数据和 Hyperliquid 交易所的实际持仓，修复不一致
"""

from __future__ import annotations

import json
import logging
import threading
import traceback
import uuid
from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import Dict, List, Optional, Any

from core.utils import D, now_ms, jdump, jload
from core.pf_compatibility import pf_compat
from core.database import RedisKeys

logger = logging.getLogger(__name__)


def k_ghost_first(uid: str, field: str) -> str:
    return f"pf:audit:ghost:first:{uid}:{field}"


class IssueType(Enum):
    """问题类型"""
    GHOST_POSITION = "ghost_position"
    MISSING_POSITION = "missing_position"
    QTY_MISMATCH = "qty_mismatch"
    ENTRY_PRICE_MISMATCH = "entry_price_mismatch"
    ORPHAN_CYCLE = "orphan_cycle"
    ORPHAN_ACTIVE = "orphan_active"
    MISSING_CYCLE = "missing_cycle"


@dataclass
class AuditIssue:
    issue_type: IssueType
    field: str
    description: str
    redis_data: Optional[dict] = None
    exchange_data: Optional[dict] = None
    auto_fixable: bool = True
    fixed: bool = False


@dataclass
class AuditReport:
    timestamp: int
    duration_ms: int
    redis_positions: int
    exchange_positions: int
    issues: List[AuditIssue] = field(default_factory=list)
    fixed_count: int = 0
    error: Optional[str] = None

    def has_issues(self) -> bool:
        return len(self.issues) > 0

    def summary(self) -> str:
        lines = [
            f"[HYPERLIQUID AUDIT REPORT] ts={self.timestamp} duration={self.duration_ms}ms",
            f"  Redis positions: {self.redis_positions}",
            f"  Exchange positions: {self.exchange_positions}",
            f"  Issues found: {len(self.issues)}",
            f"  Issues fixed: {self.fixed_count}",
        ]
        if self.error:
            lines.append(f"  Error: {self.error}")

        for issue in self.issues:
            status = "✓ FIXED" if issue.fixed else ("⚠ SKIPPED" if not issue.auto_fixable else "✗ UNFIXED")
            lines.append(f"  - [{issue.issue_type.value}] {issue.field}: {issue.description} [{status}]")
        return "\n".join(lines)


class HyperliquidPositionAuditor:
    """
    Hyperliquid 持仓审计器
    
    定期比对 Redis 中的持仓数据和交易所的实际持仓，修复不一致
    """
    
    EXCHANGE_NAME = "hyperliquid"
    PRICE_TOLERANCE = Decimal("0.001")
    QTY_TOLERANCE = Decimal("0.00001")
    GHOST_GRACE_MS = 1 * 60 * 1000

    # Hyperliquid 认证相关错误关键词
    AUTH_ERROR_KEYWORDS = ['Invalid signature', 'invalid signature', 'Unauthorized', 'unauthorized', 'API key', 'api key']

    def __init__(
        self,
        exchange_client,  # HyperliquidExchange instance
        redis_conn,
        uid: str,
        *,
        audit_interval_s: float = 61.0,
        dry_run: bool = False,
        on_report: Optional[callable] = None,
        on_auth_failed: Optional[callable] = None,
    ):
        self.client = exchange_client
        self.rds = redis_conn
        self.uid = uid
        self.audit_interval_s = audit_interval_s
        self.dry_run = dry_run
        self.on_report = on_report
        self.on_auth_failed = on_auth_failed
        self.exchange = self.EXCHANGE_NAME

        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._last_report: Optional[AuditReport] = None
        self._auth_failed = False  # 认证失败标记

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._run_loop,
            name=f"hyperliquid-position-auditor-{self.uid}",
            daemon=True,
        )
        self._thread.start()
        logger.info(f"[{self.uid}][hyperliquid] PositionAuditor 已启动")

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=5.0)
        self._thread = None
        logger.info(f"[{self.uid}][hyperliquid] PositionAuditor 已停止")

    def _is_auth_error(self, error_msg: str) -> bool:
        """检查是否是认证错误"""
        if not error_msg:
            return False
        for keyword in self.AUTH_ERROR_KEYWORDS:
            if keyword in error_msg:
                return True
        return False

    def _run_loop(self) -> None:
        self._stop.wait(10.0)

        while not self._stop.is_set():
            # 如果认证失败，停止审计
            if self._auth_failed:
                logger.warning(f"[{self.uid}][hyperliquid] PositionAuditor 因认证失败已停止")
                break
            
            try:
                report = self.audit_once(dry_run=self.dry_run)
                self._last_report = report
                
                # 检查是否是认证错误
                if report.error and self._is_auth_error(report.error):
                    self._auth_failed = True
                    logger.error(f"[{self.uid}][hyperliquid] PositionAuditor 检测到认证错误，停止审计: {report.error}")
                    if self.on_auth_failed:
                        try:
                            self.on_auth_failed(report.error)
                        except Exception:
                            pass
                    break

                if self.on_report:
                    try:
                        self.on_report(report)
                    except Exception as e:
                        logger.warning(f"[{self.uid}][hyperliquid] on_report 回调失败: {e}")

            except Exception as e:
                logger.error(f"[{self.uid}][hyperliquid] 审计失败: {e}")
                logger.exception("Hyperliquid 审计失败")

            self._stop.wait(self.audit_interval_s)

    def audit_once(self, *, dry_run: bool = False) -> AuditReport:
        """执行一次审计"""
        import asyncio
        
        start_ts = now_ms()
        issues: List[AuditIssue] = []
        fixed_count = 0
        error_msg = None

        try:
            # 获取交易所持仓（异步转同步）
            loop = asyncio.new_event_loop()
            try:
                exchange_positions = loop.run_until_complete(self.client.get_positions())
            finally:
                loop.close()
            
            # 转换为标准格式
            exchange_pos_map: Dict[str, dict] = {}
            for pos in exchange_positions:
                symbol = pos.symbol
                side = pos.side.upper()
                field = f"{symbol}:{side}"
                exchange_pos_map[field] = {
                    "symbol": symbol,
                    "side": side,
                    "qty": str(pos.qty),
                    "entryPrice": str(pos.entry_price),
                }

            # 获取 Redis 持仓
            redis_pos = pf_compat.get_pf_pos(self.uid, self.exchange)
            redis_cycle = pf_compat.get_pf_cycle(self.uid, self.exchange)
            redis_active = pf_compat.get_pf_pos_active(self.uid, self.exchange)

            # 1. 检查幽灵仓位（Redis有，交易所没有）
            for field, pos in redis_pos.items():
                if field not in exchange_pos_map:
                    issue = AuditIssue(
                        issue_type=IssueType.GHOST_POSITION,
                        field=field,
                        description=f"Redis有仓位但交易所没有: qty={pos.get('qty')}",
                        redis_data=pos,
                    )
                    issues.append(issue)

                    if not dry_run:
                        if self._fix_ghost_position(field):
                            issue.fixed = True
                            fixed_count += 1

            # 2. 检查缺失仓位（交易所有，Redis没有）
            for field, ex_pos in exchange_pos_map.items():
                if field not in redis_pos:
                    issue = AuditIssue(
                        issue_type=IssueType.MISSING_POSITION,
                        field=field,
                        description=f"交易所有仓位但Redis没有: qty={ex_pos.get('qty')}",
                        exchange_data=ex_pos,
                    )
                    issues.append(issue)

                    if not dry_run:
                        if self._fix_missing_position(field, ex_pos):
                            issue.fixed = True
                            fixed_count += 1

            # 3. 检查数量不匹配
            for field in set(redis_pos.keys()) & set(exchange_pos_map.keys()):
                r_pos = redis_pos[field]
                e_pos = exchange_pos_map[field]

                r_qty = D(r_pos.get("qty", "0"))
                e_qty = D(e_pos.get("qty", "0"))

                if abs(r_qty - e_qty) > self.QTY_TOLERANCE:
                    issue = AuditIssue(
                        issue_type=IssueType.QTY_MISMATCH,
                        field=field,
                        description=f"数量不匹配: Redis={r_qty}, Exchange={e_qty}",
                        redis_data=r_pos,
                        exchange_data=e_pos,
                    )
                    issues.append(issue)

                    if not dry_run:
                        if self._fix_qty_mismatch(field, e_pos):
                            issue.fixed = True
                            fixed_count += 1

            # 4. 检查孤立的 active 记录
            for field in redis_active:
                if field not in redis_pos and field not in exchange_pos_map:
                    issue = AuditIssue(
                        issue_type=IssueType.ORPHAN_ACTIVE,
                        field=field,
                        description="active中有但pos和交易所都没有",
                    )
                    issues.append(issue)

                    if not dry_run:
                        if self._fix_orphan_active(field):
                            issue.fixed = True
                            fixed_count += 1

            # 5. 检查孤立的 cycle 记录
            for field in redis_cycle:
                if field not in redis_pos and field not in exchange_pos_map:
                    issue = AuditIssue(
                        issue_type=IssueType.ORPHAN_CYCLE,
                        field=field,
                        description="cycle中有但pos和交易所都没有",
                    )
                    issues.append(issue)

                    if not dry_run:
                        if self._fix_orphan_cycle(field):
                            issue.fixed = True
                            fixed_count += 1

        except Exception as e:
            error_msg = str(e)
            logger.error(f"[{self.uid}][hyperliquid] 审计错误: {e}")
            logger.exception("Hyperliquid 审计错误")

        duration = now_ms() - start_ts

        return AuditReport(
            timestamp=start_ts,
            duration_ms=duration,
            redis_positions=len(redis_pos) if 'redis_pos' in dir() else 0,
            exchange_positions=len(exchange_pos_map) if 'exchange_pos_map' in dir() else 0,
            issues=issues,
            fixed_count=fixed_count,
            error=error_msg,
        )

    def _fix_ghost_position(self, field: str) -> bool:
        """修复幽灵仓位"""
        try:
            ts = now_ms()
            ghost_key = k_ghost_first(self.uid, field)
            first_seen = self.rds.get(ghost_key)

            if not first_seen:
                # TTL 设为 180 秒，确保在多次审计周期内有效
                self.rds.set(ghost_key, str(ts), ex=180)
                logger.debug(f"[{self.uid}][hyperliquid] Ghost position {field} 首次发现，开始宽限期")
                return False

            try:
                first_ts = int(first_seen)
            except Exception:
                first_ts = ts

            elapsed_ms = ts - first_ts
            if elapsed_ms < self.GHOST_GRACE_MS:
                self.rds.expire(ghost_key, 180)
                logger.debug(f"[{self.uid}][hyperliquid] Ghost position {field} 宽限期内 ({elapsed_ms}ms < {self.GHOST_GRACE_MS}ms)")
                return False

            logger.info(f"[{self.uid}][hyperliquid] Ghost position {field} 宽限期已过 ({elapsed_ms}ms)，开始修复")

            # 删除幽灵仓位
            pos_data = pf_compat.get_pf_pos(self.uid, self.exchange)
            if field in pos_data:
                del pos_data[field]
                pf_compat.set_pf_pos(self.uid, pos_data, self.exchange)

            active = pf_compat.get_pf_pos_active(self.uid, self.exchange)
            if field in active:
                active.remove(field)
                pf_compat.set_pf_pos_active(self.uid, active, self.exchange)

            # 关闭对应 cycle
            cycle_data = pf_compat.get_pf_cycle(self.uid, self.exchange)
            if field in cycle_data:
                c = cycle_data[field]
                c["closeTimeMs"] = str(ts)
                c["closeSource"] = "audit_ghost_fix"
                cycle_id = c.get("cycleId") or f"{field}:{ts}"
                pf_compat.set_pf_closed_h(self.uid, cycle_id, c, self.exchange)
                del cycle_data[field]
                pf_compat.set_pf_cycle(self.uid, cycle_data, self.exchange)

            self.rds.delete(ghost_key)
            logger.info(f"[{self.uid}][hyperliquid][AUDIT] 修复幽灵仓位: {field}")
            return True

        except Exception as e:
            logger.warning(f"[{self.uid}][hyperliquid][AUDIT] 修复幽灵仓位失败 {field}: {e}")
            return False

    def _fix_missing_position(self, field: str, ex_pos: dict) -> bool:
        """修复缺失仓位"""
        try:
            ts = now_ms()
            symbol = ex_pos["symbol"]
            side = ex_pos["side"]
            qty = ex_pos["qty"]
            entry_price = ex_pos["entryPrice"]

            pos_obj = {
                "symbol": symbol,
                "side": side,
                "qty": qty,
                "entryPrice": entry_price,
                "unrealizedPnl": "0",
                "marginType": "cross",
                "openTimeMs": str(ts),
                "updatedAt": str(ts),
                "exchange": self.exchange,
                "source": "audit_missing_fix",
            }

            pos_data = pf_compat.get_pf_pos(self.uid, self.exchange)
            pos_data[field] = pos_obj
            pf_compat.set_pf_pos(self.uid, pos_data, self.exchange)

            # ⭐ 同时创建 cycle 数据，确保平仓时能生成 closed records
            qty_d = D(qty)
            entry_d = D(entry_price)
            notional = str(qty_d * entry_d) if entry_d > 0 else "0"
            
            cycle_id = f"{symbol}:{side}:{int(ts // 1000)}:{uuid.uuid4().hex[:8]}"
            cyc = {
                "cycleId": cycle_id,
                "uid": self.uid,
                "symbol": symbol,
                "side": side,
                "exchange": self.exchange,
                "openTimeMs": str(ts),
                "closeTimeMs": "0",
                "durationMs": "0",
                "openQty": qty,
                "openQuote": notional,
                "avgOpenPrice": entry_price,
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
                "maxAbsQty": qty,
                "updatedAt": str(ts),
                "closeTradeCount": "0",
                "closeOrderIds": "[]",
                "field": field,
                "source": "audit_missing_fix",
            }
            
            cycle_data = pf_compat.get_pf_cycle(self.uid, self.exchange)
            cycle_data[field] = cyc
            pf_compat.set_pf_cycle(self.uid, cycle_data, self.exchange)

            active = pf_compat.get_pf_pos_active(self.uid, self.exchange)
            if field not in active:
                active.append(field)
                pf_compat.set_pf_pos_active(self.uid, active, self.exchange)

            logger.info(f"[{self.uid}][hyperliquid][AUDIT] 修复缺失仓位: {field}")
            return True

        except Exception as e:
            logger.warning(f"[{self.uid}][hyperliquid][AUDIT] 修复缺失仓位失败 {field}: {e}")
            return False

    def _fix_qty_mismatch(self, field: str, ex_pos: dict) -> bool:
        """修复数量不匹配"""
        try:
            ts = now_ms()

            pos_data = pf_compat.get_pf_pos(self.uid, self.exchange)
            if field in pos_data:
                pos_data[field]["qty"] = ex_pos["qty"]
                pos_data[field]["entryPrice"] = ex_pos["entryPrice"]
                pos_data[field]["updatedAt"] = str(ts)
                pf_compat.set_pf_pos(self.uid, pos_data, self.exchange)

            logger.info(f"[{self.uid}][hyperliquid][AUDIT] 修复数量不匹配: {field}")
            return True

        except Exception as e:
            logger.warning(f"[{self.uid}][hyperliquid][AUDIT] 修复数量不匹配失败 {field}: {e}")
            return False

    def _fix_orphan_active(self, field: str) -> bool:
        """修复孤立的 active 记录"""
        try:
            active = pf_compat.get_pf_pos_active(self.uid, self.exchange)
            if field in active:
                active.remove(field)
                pf_compat.set_pf_pos_active(self.uid, active, self.exchange)

            logger.info(f"[{self.uid}][hyperliquid][AUDIT] 修复孤立active: {field}")
            return True

        except Exception as e:
            logger.warning(f"[{self.uid}][hyperliquid][AUDIT] 修复孤立active失败 {field}: {e}")
            return False

    def _fix_orphan_cycle(self, field: str) -> bool:
        """修复孤立的 cycle 记录"""
        try:
            ts = now_ms()

            cycle_data = pf_compat.get_pf_cycle(self.uid, self.exchange)
            if field in cycle_data:
                c = cycle_data[field]
                c["closeTimeMs"] = str(ts)
                c["closeSource"] = "audit_orphan_fix"
                cycle_id = c.get("cycleId") or f"{field}:{ts}"
                pf_compat.set_pf_closed_h(self.uid, cycle_id, c, self.exchange)
                del cycle_data[field]
                pf_compat.set_pf_cycle(self.uid, cycle_data, self.exchange)

            logger.info(f"[{self.uid}][hyperliquid][AUDIT] 修复孤立cycle: {field}")
            return True

        except Exception as e:
            logger.warning(f"[{self.uid}][hyperliquid][AUDIT] 修复孤立cycle失败 {field}: {e}")
            return False
