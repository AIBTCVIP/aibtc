# exchanges/okx/position_auditor.py
"""
OKX 持仓审计器

定期比对 Redis 中的持仓数据和 OKX 交易所的实际持仓，修复不一致
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


def k_ghost_cleaned(uid: str, exchange: str, field: str) -> str:
    """
    幽灵持仓已清理标记 key
    
    用于防止竞态条件：审计器清理幽灵持仓后，其他组件（如 mark_cycle_updater）
    可能会把已删除的持仓重新写回 Redis。
    
    设置此标记后，其他组件在写入持仓前会检查，如果标记存在则跳过写入。
    """
    return f"pf:ghost_cleaned:{uid}:{exchange}:{field}"


def is_ghost_cleaned(redis_conn, uid: str, exchange: str, field: str) -> bool:
    """
    检查持仓是否刚被审计器清理（防止竞态条件导致重新写入）
    
    Args:
        redis_conn: Redis 连接
        uid: 用户 ID
        exchange: 交易所
        field: 持仓字段 (symbol:side)
    
    Returns:
        True 如果持仓刚被清理，不应该重新写入
    """
    try:
        key = k_ghost_cleaned(uid, exchange, field)
        return redis_conn.exists(key) > 0
    except Exception:
        return False


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
            f"[OKX AUDIT REPORT] ts={self.timestamp} duration={self.duration_ms}ms",
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


class OKXPositionAuditor:
    """
    OKX 持仓审计器
    
    定期比对 Redis 中的持仓数据和交易所的实际持仓，修复不一致
    """
    
    EXCHANGE_NAME = "okx"
    PRICE_TOLERANCE = Decimal("0.001")
    QTY_TOLERANCE = Decimal("0.00001")
    GHOST_GRACE_MS = 1 * 60 * 1000
    
    # OKX 认证相关错误码（不应重试）
    AUTH_ERROR_CODES = {'50105', '50111', '50112', '50113', '50114', '60005'}

    def __init__(
        self,
        exchange_client,  # OKXExchange instance
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
        
        # 持久事件循环（在审计线程中使用）
        self._loop: Optional[Any] = None

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._run_loop,
            name=f"okx-position-auditor-{self.uid}",
            daemon=True,
        )
        self._thread.start()
        logger.info(f"[{self.uid}][okx] PositionAuditor 已启动")

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=5.0)
        self._thread = None
        
        # 关闭事件循环
        if self._loop and not self._loop.is_closed():
            try:
                self._loop.close()
            except Exception:
                pass
        self._loop = None
        
        logger.info(f"[{self.uid}][okx] PositionAuditor 已停止")

    def _run_loop(self) -> None:
        self._stop.wait(10.0)

        while not self._stop.is_set():
            # 如果认证失败，停止审计
            if self._auth_failed:
                logger.warning(f"[{self.uid}][okx] PositionAuditor 因认证失败已停止")
                break
            
            try:
                report = self.audit_once(dry_run=self.dry_run)
                self._last_report = report
                
                # 检查是否是认证错误
                if report.error and self._is_auth_error(report.error):
                    self._auth_failed = True
                    logger.error(f"[{self.uid}][okx] PositionAuditor 检测到认证错误，停止审计: {report.error}")
                    if self.on_auth_failed:
                        try:
                            self.on_auth_failed(report.error)
                        except Exception:
                            pass
                    break

                if self.on_report:
                    try:
                        self.on_report(report)
                    except Exception:
                        pass

            except Exception:
                logger.exception("OKX 审计循环错误")

            self._stop.wait(self.audit_interval_s)
    
    def _is_auth_error(self, error_msg: str) -> bool:
        """检查是否是认证错误"""
        if not error_msg:
            return False
        # 检查错误码
        for code in self.AUTH_ERROR_CODES:
            if code in error_msg:
                return True
        # 检查关键词
        auth_keywords = ['Invalid OK-ACCESS', 'Invalid apiKey', 'PASSPHRASE incorrect']
        for keyword in auth_keywords:
            if keyword in error_msg:
                return True
        return False

    def audit_once(self, *, dry_run: Optional[bool] = None) -> AuditReport:
        if dry_run is None:
            dry_run = self.dry_run

        start_ts = now_ms()
        issues: List[AuditIssue] = []

        try:
            exchange_positions = self._fetch_exchange_positions()
            redis_positions = self._fetch_redis_positions()

            issues.extend(self._compare_positions(redis_positions, exchange_positions))
            issues.extend(self._check_cycle_consistency(redis_positions))
            issues.extend(self._check_active_set_consistency(redis_positions))

            fixed_count = 0
            if not dry_run:
                fixed_count = self._fix_issues(issues, exchange_positions)
                fixed_count += self._patch_cycle_max_abs_qty()

            duration = now_ms() - start_ts
            return AuditReport(
                timestamp=start_ts,
                duration_ms=duration,
                redis_positions=len(redis_positions),
                exchange_positions=len(exchange_positions),
                issues=issues,
                fixed_count=fixed_count,
            )

        except Exception as e:
            logger.exception("OKX 审计失败")
            return AuditReport(
                timestamp=start_ts,
                duration_ms=now_ms() - start_ts,
                redis_positions=0,
                exchange_positions=0,
                issues=issues,
                error=f"{type(e).__name__}: {e}",
            )

    def _fetch_exchange_positions(self) -> Dict[str, dict]:
        """从 OKX 获取实际持仓"""
        positions: Dict[str, dict] = {}
        
        try:
            import asyncio
            
            async def _get_positions():
                return await self.client.get_positions()
            
            # 使用持久的事件循环，避免每次创建新循环导致 session 失效
            if self._loop is None or self._loop.is_closed():
                self._loop = asyncio.new_event_loop()
            
            result = self._loop.run_until_complete(_get_positions())
            
            for p in result:
                # p 是 Position 对象，不是字典
                symbol = p.symbol
                qty = abs(p.qty)
                if qty <= 0:
                    continue
                
                side = p.side.upper()
                
                field = f"{symbol}:{side}"
                positions[field] = {
                    "symbol": symbol,
                    "side": side,
                    "qty": D(str(qty)),
                    "entryPrice": D(str(p.entry_price)),
                    "unrealizedPnl": D(str(p.unrealized_pnl)),
                    "leverage": p.leverage,
                    "marginType": p.margin_type,
                    "raw": p.raw,
                }
                
        except Exception as e:
            logger.error(f"[{self.uid}][okx] 获取持仓失败: {e}")
            logger.exception("OKX 获取持仓失败")
        
        return positions

    def _fetch_redis_positions(self) -> Dict[str, dict]:
        """从 Redis 获取持仓数据"""
        positions: Dict[str, dict] = {}
        pos_data = pf_compat.get_pf_pos(self.uid, self.exchange)

        for field, p in pos_data.items():
            qty = D(p.get("qty", "0"))
            if qty <= 0:
                continue
            positions[field] = {
                "symbol": p.get("symbol"),
                "side": p.get("side"),
                "qty": qty,
                "entryPrice": D(p.get("entryPrice", "0")),
                "raw": p,
            }
        return positions

    def _compare_positions(self, redis_pos: Dict[str, dict], exchange_pos: Dict[str, dict]) -> List[AuditIssue]:
        """比对持仓数据"""
        issues: List[AuditIssue] = []
        all_fields = set(redis_pos.keys()) | set(exchange_pos.keys())

        for field in all_fields:
            r = redis_pos.get(field)
            e = exchange_pos.get(field)

            if r and not e:
                issues.append(AuditIssue(
                    issue_type=IssueType.GHOST_POSITION,
                    field=field,
                    description=f"Redis has position (qty={r['qty']}) but exchange doesn't",
                    redis_data=r.get("raw"),
                    auto_fixable=True,
                ))
                continue

            if e and not r:
                issues.append(AuditIssue(
                    issue_type=IssueType.MISSING_POSITION,
                    field=field,
                    description=f"Exchange has position (qty={e['qty']}) but Redis doesn't",
                    exchange_data=e.get("raw"),
                    auto_fixable=True,
                ))
                continue

            if r and e:
                qty_diff = abs(r["qty"] - e["qty"])
                if qty_diff > self.QTY_TOLERANCE:
                    issues.append(AuditIssue(
                        issue_type=IssueType.QTY_MISMATCH,
                        field=field,
                        description=f"Qty mismatch: Redis={r['qty']} vs Exchange={e['qty']}",
                        redis_data=r.get("raw"),
                        exchange_data=e.get("raw"),
                        auto_fixable=True,
                    ))

                if r["entryPrice"] > 0 and e["entryPrice"] > 0:
                    price_diff = abs(r["entryPrice"] - e["entryPrice"]) / e["entryPrice"]
                    if price_diff > self.PRICE_TOLERANCE:
                        issues.append(AuditIssue(
                            issue_type=IssueType.ENTRY_PRICE_MISMATCH,
                            field=field,
                            description=f"Entry price mismatch: Redis={r['entryPrice']} vs Exchange={e['entryPrice']}",
                            redis_data=r.get("raw"),
                            exchange_data=e.get("raw"),
                            auto_fixable=True,
                        ))

        return issues

    def _check_cycle_consistency(self, redis_pos: Dict[str, dict]) -> List[AuditIssue]:
        """检查周期一致性"""
        issues: List[AuditIssue] = []
        cycle_data = pf_compat.get_pf_cycle(self.uid, self.exchange)
        pos_data = pf_compat.get_pf_pos(self.uid, self.exchange)

        all_cycles = cycle_data or {}
        for field, raw in all_cycles.items():
            cyc = raw if isinstance(raw, dict) else jload(raw)
            close_time = int(cyc.get("closeTimeMs", "0") or "0")

            if close_time != 0:
                continue

            if field not in redis_pos:
                pos_raw = jdump(pos_data.get(field, {})) if pos_data.get(field) else None
                if not pos_raw:
                    issues.append(AuditIssue(
                        issue_type=IssueType.ORPHAN_CYCLE,
                        field=field,
                        description="Active cycle exists but position doesn't",
                        redis_data=cyc,
                        auto_fixable=True,
                    ))

        for field in redis_pos:
            if field not in cycle_data:
                issues.append(AuditIssue(
                    issue_type=IssueType.MISSING_CYCLE,
                    field=field,
                    description="Position exists but no active cycle",
                    redis_data=redis_pos[field].get("raw"),
                    auto_fixable=True,
                ))

        return issues

    def _check_active_set_consistency(self, redis_pos: Dict[str, dict]) -> List[AuditIssue]:
        """检查活跃集合一致性"""
        issues: List[AuditIssue] = []
        active_list = pf_compat.get_pf_pos_active(self.uid, self.exchange)
        pos_data = pf_compat.get_pf_pos(self.uid, self.exchange)

        active_members = set(active_list) if active_list else set()

        for field in active_members:
            if field not in redis_pos:
                pos_raw = jdump(pos_data.get(field, {})) if pos_data.get(field) else None
                if not pos_raw:
                    issues.append(AuditIssue(
                        issue_type=IssueType.ORPHAN_ACTIVE,
                        field=field,
                        description="Field in active set but position doesn't exist",
                        auto_fixable=True,
                    ))

        return issues

    def _patch_cycle_max_abs_qty(self) -> int:
        """修补周期的 maxAbsQty"""
        patched = 0
        pos_data = pf_compat.get_pf_pos(self.uid, self.exchange)
        cycle_data = pf_compat.get_pf_cycle(self.uid, self.exchange)

        fields = list(cycle_data.keys()) if cycle_data else []
        if not fields:
            return 0

        for field in fields:
            cyc_raw = cycle_data.get(field)
            if not cyc_raw:
                continue

            c = cyc_raw if isinstance(cyc_raw, dict) else jload(cyc_raw) or {}

            try:
                if int(c.get("closeTimeMs", "0") or "0") != 0:
                    continue
            except Exception:
                pass

            oq = D(c.get("openQty", "0"))
            cq = D(c.get("closeQty", "0"))
            net_qty = abs(oq - cq)

            old_max = D(c.get("maxAbsQty", "0"))

            pos_raw = pos_data.get(field)
            pos_qty = D("0")
            if pos_raw:
                p = pos_raw if isinstance(pos_raw, dict) else jload(pos_raw) or {}
                pos_qty = D(p.get("qty", "0"))

            new_max = max(old_max, oq, cq, net_qty, pos_qty)

            if new_max > old_max:
                c["maxAbsQty"] = str(new_max)
                c["updatedAt"] = str(now_ms())
                cycle_data[field] = c
                patched += 1

        if patched:
            pf_compat.set_pf_cycle(self.uid, cycle_data, self.exchange)

        return patched

    def _fix_issues(self, issues: List[AuditIssue], exchange_pos: Dict[str, dict]) -> int:
        """修复问题"""
        fixed = 0

        for issue in issues:
            if not issue.auto_fixable:
                continue

            try:
                if issue.issue_type == IssueType.GHOST_POSITION:
                    ok = self._fix_ghost_position(issue)
                    if ok:
                        issue.fixed = True
                        fixed += 1
                    else:
                        issue.auto_fixable = False
                        issue.description += f" (grace={int(self.GHOST_GRACE_MS / 1000)}s, pending delete)"

                elif issue.issue_type == IssueType.MISSING_POSITION:
                    self._fix_missing_position(issue, exchange_pos.get(issue.field))
                    issue.fixed = True
                    fixed += 1

                elif issue.issue_type == IssueType.QTY_MISMATCH:
                    self._fix_qty_mismatch(issue, exchange_pos.get(issue.field))
                    issue.fixed = True
                    fixed += 1

                elif issue.issue_type == IssueType.ENTRY_PRICE_MISMATCH:
                    self._fix_entry_price_mismatch(issue, exchange_pos.get(issue.field))
                    issue.fixed = True
                    fixed += 1

                elif issue.issue_type == IssueType.ORPHAN_CYCLE:
                    self._fix_orphan_cycle(issue)
                    issue.fixed = True
                    fixed += 1

                elif issue.issue_type == IssueType.ORPHAN_ACTIVE:
                    self._fix_orphan_active(issue)
                    issue.fixed = True
                    fixed += 1

                elif issue.issue_type == IssueType.MISSING_CYCLE:
                    self._fix_missing_cycle(issue)
                    issue.fixed = True
                    fixed += 1

            except Exception as e:
                logger.error(f"Failed to fix {issue.issue_type.value} for {issue.field}: {e}")
                logger.exception("OKX 修复问题失败")

        return fixed

    def _fix_ghost_position(self, issue: AuditIssue) -> bool:
        """修复幽灵持仓"""
        field = issue.field
        now = now_ms()
        first_key = k_ghost_first(self.uid, field)

        raw_first = self.rds.get(first_key)
        if not raw_first:
            # TTL 设为 180 秒，确保在多次审计周期内有效（审计间隔 120 秒）
            self.rds.set(first_key, str(now), ex=180)
            logger.debug(f"[{self.uid}][{self.exchange}] Ghost position {field} 首次发现，开始宽限期")
            return False

        try:
            first_ts = int(raw_first)
        except Exception:
            first_ts = now

        elapsed_ms = now - first_ts
        if elapsed_ms < self.GHOST_GRACE_MS:
            self.rds.expire(first_key, 180)
            logger.debug(f"[{self.uid}][{self.exchange}] Ghost position {field} 宽限期内 ({elapsed_ms}ms < {self.GHOST_GRACE_MS}ms)")
            return False
        
        logger.info(f"[{self.uid}][{self.exchange}] Ghost position {field} 宽限期已过 ({elapsed_ms}ms)，开始修复")
        return self._close_ghost_cycle_and_cleanup(field, now)

    def _close_ghost_cycle_and_cleanup(self, field: str, close_time_ms: int) -> bool:
        """关闭幽灵周期并清理"""
        pos_data = pf_compat.get_pf_pos(self.uid, self.exchange)
        cycle_data = pf_compat.get_pf_cycle(self.uid, self.exchange)
        active_list = pf_compat.get_pf_pos_active(self.uid, self.exchange)

        cycle_raw = cycle_data.get(field) if cycle_data else None
        
        # ⭐ 先设置"已清理"标记，防止其他组件（如 mark_cycle_updater）在清理过程中重新写入
        # TTL=300秒（5分钟），足够覆盖审计周期（120秒）+ 安全余量
        cleaned_key = k_ghost_cleaned(self.uid, self.exchange, field)
        try:
            self.rds.set(cleaned_key, str(close_time_ms), ex=300)
            logger.debug(f"[{self.uid}][{self.exchange}] 设置幽灵持仓清理标记: {field}")
        except Exception as e:
            logger.warning(f"[{self.uid}][{self.exchange}] 设置清理标记失败: {e}")

        if not cycle_raw:
            if field in pos_data:
                del pos_data[field]
                pf_compat.set_pf_pos(self.uid, pos_data, self.exchange, skip_ghost_check=True)

            if field in active_list:
                active_list.remove(field)
                pf_compat.set_pf_pos_active(self.uid, active_list, self.exchange)

            return True

        cyc = cycle_raw if isinstance(cycle_raw, dict) else jload(cycle_raw) or {}

        if int(cyc.get("closeTimeMs", "0") or "0") != 0:
            if field in pos_data:
                del pos_data[field]
                pf_compat.set_pf_pos(self.uid, pos_data, self.exchange, skip_ghost_check=True)

            if field in cycle_data:
                del cycle_data[field]
                pf_compat.set_pf_cycle(self.uid, cycle_data, self.exchange)

            if field in active_list:
                active_list.remove(field)
                pf_compat.set_pf_pos_active(self.uid, active_list, self.exchange)

            return True

        symbol = cyc.get("symbol")
        side = cyc.get("side")

        # 计算最终 netPnl
        net = (
            D(cyc.get("realizedPnlEst", "0"))
            + D(cyc.get("fundingTotal", "0"))
            - D(cyc.get("feeTotal", "0"))
        )
        cyc["netPnl"] = str(net)

        # 计算 drawdown
        peak = D(cyc.get("peakPnl", "0"))
        dd_close = peak - net
        if dd_close < 0:
            dd_close = D("0")
        cyc["drawdownToClose"] = str(dd_close)

        # 关闭 cycle
        cyc["closeTimeMs"] = str(close_time_ms)
        open_t = int(cyc.get("openTimeMs", "0") or 0)
        cyc["durationMs"] = str(max(0, close_time_ms - open_t))
        cyc["updatedAt"] = str(close_time_ms)
        cyc["field"] = field
        cyc["closeSource"] = "auditor_ghost_recovery"

        if not cyc.get("cycleId"):
            cyc["cycleId"] = f"{symbol}:{side}:{int(close_time_ms // 1000)}:{uuid.uuid4().hex[:8]}"

        cycle_id = cyc["cycleId"]

        # 写入 closed records
        pf_compat.set_pf_closed_h(self.uid, cycle_id, cyc, self.exchange)

        # 清理
        if field in pos_data:
            del pos_data[field]
            pf_compat.set_pf_pos(self.uid, pos_data, self.exchange, skip_ghost_check=True)

        if field in cycle_data:
            del cycle_data[field]
            pf_compat.set_pf_cycle(self.uid, cycle_data, self.exchange)

        if field in active_list:
            active_list.remove(field)
            pf_compat.set_pf_pos_active(self.uid, active_list, self.exchange)

        logger.info(f"[{self.uid}][okx] Ghost cycle closed: {field} cycleId={cycle_id} netPnl={cyc['netPnl']}")

        return True

    def _fix_missing_position(self, issue: AuditIssue, exchange_data: Optional[dict]) -> None:
        """修复缺失的持仓"""
        if not exchange_data:
            return

        field = issue.field
        
        # 先清除可能存在的清理标记（因为交易所确实有这个持仓）
        try:
            cleaned_key = k_ghost_cleaned(self.uid, self.exchange, field)
            self.rds.delete(cleaned_key)
        except Exception:
            pass
        
        pos_data = pf_compat.get_pf_pos(self.uid, self.exchange)
        cycle_data = pf_compat.get_pf_cycle(self.uid, self.exchange)
        active_list = pf_compat.get_pf_pos_active(self.uid, self.exchange)

        raw = exchange_data.get("raw") or {}

        symbol = exchange_data.get("symbol")
        side = exchange_data.get("side")
        qty = exchange_data.get("qty", D("0"))
        entry_price = exchange_data.get("entryPrice", D("0"))

        ts = now_ms()

        pos_obj = {
            "symbol": symbol,
            "side": side,
            "qty": str(qty),
            "entryPrice": str(entry_price),
            "unrealizedPnl": str(exchange_data.get("unrealizedPnl", D("0"))),
            "marginType": exchange_data.get("marginType", "cross"),
            "openTimeMs": str(ts),
            "updatedAt": str(ts),
            "source": "auditor_snapshot_recover",
            "exchange": self.exchange,
        }

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
            "closeTradeCount": "0",
            "closeOrderIds": "[]",
            "openStopordersFired": "0",
            "field": field,
            "source": "auditor_snapshot_recover",
        }

        pos_data[field] = pos_obj
        pf_compat.set_pf_pos(self.uid, pos_data, self.exchange, skip_ghost_check=True)

        cycle_data[field] = cyc
        pf_compat.set_pf_cycle(self.uid, cycle_data, self.exchange)

        if field not in active_list:
            active_list.append(field)
            pf_compat.set_pf_pos_active(self.uid, active_list, self.exchange)

    def _fix_qty_mismatch(self, issue: AuditIssue, exchange_data: Optional[dict]) -> None:
        """修复数量不匹配"""
        if not exchange_data:
            return

        field = issue.field
        pos_data = pf_compat.get_pf_pos(self.uid, self.exchange)

        if field not in pos_data:
            return

        pos_data[field]["qty"] = str(exchange_data["qty"])
        pos_data[field]["updatedAt"] = str(now_ms())
        pos_data[field]["lastAuditFix"] = "qty_mismatch"
        pf_compat.set_pf_pos(self.uid, pos_data, self.exchange, skip_ghost_check=True)

    def _fix_entry_price_mismatch(self, issue: AuditIssue, exchange_data: Optional[dict]) -> None:
        """修复入场价格不匹配"""
        if not exchange_data:
            return

        field = issue.field
        pos_data = pf_compat.get_pf_pos(self.uid, self.exchange)

        if field not in pos_data:
            return

        pos_data[field]["entryPrice"] = str(exchange_data["entryPrice"])
        pos_data[field]["updatedAt"] = str(now_ms())
        pos_data[field]["lastAuditFix"] = "entry_price_mismatch"
        pf_compat.set_pf_pos(self.uid, pos_data, self.exchange, skip_ghost_check=True)

    def _fix_orphan_cycle(self, issue: AuditIssue) -> None:
        """修复孤儿周期"""
        field = issue.field
        cycle_data = pf_compat.get_pf_cycle(self.uid, self.exchange)
        if field in cycle_data:
            del cycle_data[field]
            pf_compat.set_pf_cycle(self.uid, cycle_data, self.exchange)

    def _fix_orphan_active(self, issue: AuditIssue) -> None:
        """修复孤儿活跃记录"""
        field = issue.field
        active_list = pf_compat.get_pf_pos_active(self.uid, self.exchange)
        if field in active_list:
            active_list.remove(field)
            pf_compat.set_pf_pos_active(self.uid, active_list, self.exchange)

    def _fix_missing_cycle(self, issue: AuditIssue) -> None:
        """修复缺失的周期"""
        field = issue.field
        pos_data = pf_compat.get_pf_pos(self.uid, self.exchange)
        cycle_data = pf_compat.get_pf_cycle(self.uid, self.exchange)
        active_list = pf_compat.get_pf_pos_active(self.uid, self.exchange)

        if field not in pos_data:
            return
        pos = pos_data[field]

        symbol = pos.get("symbol")
        side = (pos.get("side") or "").upper()
        qty = D(pos.get("qty", "0"))
        entry_price = D(pos.get("entryPrice", "0"))

        ts = now_ms()
        cycle_id = f"{symbol}:{side}:{int(ts // 1000)}:{uuid.uuid4().hex[:8]}"

        cyc = {
            "cycleId": cycle_id,
            "uid": self.uid,
            "symbol": symbol,
            "side": side,
            "exchange": self.exchange,
            "openTimeMs": str(int(pos.get("openTimeMs", ts) or ts)),
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
            "closeTradeCount": "0",
            "closeOrderIds": "[]",
            "openStopordersFired": "0",
            "field": field,
            "source": "auditor_missing_cycle_patch",
        }

        cycle_data[field] = cyc
        pf_compat.set_pf_cycle(self.uid, cycle_data, self.exchange)

        if field not in active_list:
            active_list.append(field)
            pf_compat.set_pf_pos_active(self.uid, active_list, self.exchange)

    def get_last_report(self) -> Optional[AuditReport]:
        return self._last_report

    def set_dry_run(self, dry_run: bool) -> None:
        self.dry_run = dry_run

    def set_interval(self, interval_s: float) -> None:
        self.audit_interval_s = interval_s
