# position_auditor.py
"""
持仓审计器

功能：
1. 定期比对 Redis 持仓与交易所实际持仓
2. 自动修复不一致的数据
3. 多交易所支持

改造说明：
- 新增 run_async() 方法支持纯 asyncio 模式
- 保留 threading 模式兼容旧代码
"""
from __future__ import annotations
import asyncio
import json
import logging
import threading
import traceback
import uuid
from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import Dict, List, Optional
from binance.client import Client

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
            f"[AUDIT REPORT] ts={self.timestamp} duration={self.duration_ms}ms",
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


class PositionAuditor:
    """
    持仓审计器
    
    定期比对 Redis 中的持仓数据和交易所的实际持仓，修复不一致
    
    多交易所支持:
    - exchange 参数指定交易所名称（默认 "binance"）
    - 数据读写使用交易所特定的 Redis 字段
    """
    
    PRICE_TOLERANCE = Decimal("0.001")
    QTY_TOLERANCE = Decimal("0.00001")
    GHOST_GRACE_MS = 1 * 60 * 1000

    # Binance 认证相关错误码
    AUTH_ERROR_CODES = {'-2015', '-2014', '-1022', '-1002', '-1021'}

    def __init__(
            self,
            client: Client,
            redis_conn,
            uid: str,
            *,
            audit_interval_s: float = 61.0,
            dry_run: bool = False,
            on_report: Optional[callable] = None,
            exchange: str = "binance",
            on_auth_failed: Optional[callable] = None,
    ):
        self.client = client
        self.rds = redis_conn
        self.uid = uid
        self.audit_interval_s = audit_interval_s
        self.dry_run = dry_run
        self.on_report = on_report
        self.exchange = exchange  # 交易所标识
        self.on_auth_failed = on_auth_failed

        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._last_report: Optional[AuditReport] = None
        self._auth_failed = False  # 认证失败标记

    def _is_auth_error(self, error_msg: str) -> bool:
        """检查是否是认证错误"""
        if not error_msg:
            return False
        for code in self.AUTH_ERROR_CODES:
            if code in error_msg:
                return True
        auth_keywords = ['Invalid API-key', 'API-key format invalid', 'Signature', 'not authorized']
        for keyword in auth_keywords:
            if keyword in error_msg:
                return True
        return False

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._run_loop,
            name=f"position-auditor-{self.uid}",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=5.0)
        self._thread = None

    def _run_loop(self) -> None:
        import random
        # 随机延迟启动（10-60s），避免所有用户同时触发审计
        initial_delay = 10.0 + random.uniform(0, 50)
        self._stop.wait(initial_delay)

        while not self._stop.is_set():
            # 如果认证失败，停止审计
            if self._auth_failed:
                logger.warning(f"[{self.uid}][{self.exchange}] PositionAuditor 因认证失败已停止")
                break
            
            try:
                report = self.audit_once(dry_run=self.dry_run)
                self._last_report = report

                # 检查是否是认证错误
                if report.error and self._is_auth_error(report.error):
                    self._auth_failed = True
                    logger.error(f"[{self.uid}][{self.exchange}] PositionAuditor 检测到认证错误，停止审计: {report.error}")
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
                        # P3 Fix: 添加日志
                        logger.warning(f"审计报告回调异常: {e}")

            except Exception:
                logger.exception("审计循环错误")

            self._stop.wait(self.audit_interval_s)

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
            logger.exception("审计失败")
            return AuditReport(
                timestamp=start_ts,
                duration_ms=now_ms() - start_ts,
                redis_positions=0,
                exchange_positions=0,
                issues=issues,
                error=f"{type(e).__name__}: {e}",
            )

    def _fetch_exchange_positions(self) -> Dict[str, dict]:
        positions: Dict[str, dict] = {}
        
        # 使用 API Key 级别限速器
        from core.rate_limiter import get_binance_rate_limiter
        # 从 client 获取 API Key
        api_key = getattr(self.client, 'API_KEY', None)
        rate_limiter = get_binance_rate_limiter(api_key)
        
        # 添加重试机制，应对偶发的 API 错误
        max_retries = 3
        last_error = None
        
        for attempt in range(max_retries):
            try:
                # 等待限速器许可
                if not rate_limiter.acquire(endpoint="futures_position_information", timeout=30.0):
                    raise Exception("API 限速器超时，无法获取许可")
                
                result = self.client.futures_position_information()
                break
            except Exception as e:
                last_error = e
                if attempt < max_retries - 1:
                    import time
                    wait_time = (attempt + 1) * 2  # 2s, 4s
                    logger.warning(
                        f"[{self.uid}][{self.exchange}] 获取持仓失败 (尝试 {attempt + 1}/{max_retries}): {e}, "
                        f"{wait_time}s 后重试"
                    )
                    time.sleep(wait_time)
                else:
                    logger.error(f"[{self.uid}][{self.exchange}] 获取持仓失败，已重试 {max_retries} 次: {e}")
                    raise last_error
        
        for p in result:
            symbol = p.get("symbol")
            side = (p.get("positionSide") or "").upper()
            if not symbol or side not in ("LONG", "SHORT"):
                continue

            qty = abs(D(p.get("positionAmt", "0")))
            if qty <= 0:
                continue

            field = f"{symbol}:{side}"
            positions[field] = {
                "symbol": symbol,
                "side": side,
                "qty": qty,
                "entryPrice": D(p.get("entryPrice", "0")),
                "unrealizedPnl": D(p.get("unRealizedProfit", "0")),
                "leverage": p.get("leverage"),
                "marginType": (p.get("marginType") or "").lower(),
                "raw": p,
            }
        return positions

    def _fetch_redis_positions(self) -> Dict[str, dict]:
        positions: Dict[str, dict] = {}
        pos_data = pf_compat.get_pf_pos(self.uid, self.exchange)

        # 使用兼容层获取的数据
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
        issues: List[AuditIssue] = []
        cycle_data = pf_compat.get_pf_cycle(self.uid, self.exchange)
        pos_data = pf_compat.get_pf_pos(self.uid, self.exchange)

        all_cycles = cycle_data or {}
        for field, raw in all_cycles.items():
            # raw 现在可能是已经解析的字典（由于pf_compat.get_pf_cycle()的修复）
            if isinstance(raw, dict):
                cyc = raw
            else:
                cyc = jload(raw)
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
        patched = 0
        pos_data = pf_compat.get_pf_pos(self.uid, self.exchange)
        cycle_data = pf_compat.get_pf_cycle(self.uid, self.exchange)

        fields = list(cycle_data.keys()) if cycle_data else []
        if not fields:
            return 0

        pos_raw_list = [pos_data.get(field) for field in fields]
        cyc_raw_list = [cycle_data.get(field) for field in fields]

        pipe = self.rds.pipeline()

        for field, pos_raw, cyc_raw in zip(fields, pos_raw_list, cyc_raw_list):
            if not cyc_raw:
                continue

            c = cyc_raw if isinstance(cyc_raw, dict) else jload(cyc_raw) or {}

            try:
                if int(c.get("closeTimeMs", "0") or "0") != 0:
                    continue
            except Exception as e:
                logger.debug(f"[{self.uid}][{self.exchange}] 解析 closeTimeMs 失败: {e}")

            oq = D(c.get("openQty", "0"))
            cq = D(c.get("closeQty", "0"))
            net_qty = abs(oq - cq)

            old_max = D(c.get("maxAbsQty", "0"))

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
                logger.exception("修复问题失败")

        return fixed

    def _fix_ghost_position(self, issue: AuditIssue) -> bool:
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
            # Remove from positions and active list
            if field in pos_data:
                del pos_data[field]
                pf_compat.set_pf_pos(self.uid, pos_data, self.exchange, skip_ghost_check=True)

            if field in active_list:
                active_list.remove(field)
                pf_compat.set_pf_pos_active(self.uid, active_list, self.exchange)

            return True

        cyc = cycle_raw if isinstance(cycle_raw, dict) else jload(cycle_raw) or {}

        if int(cyc.get("closeTimeMs", "0") or "0") != 0:
            # Remove from positions, cycles, and active list
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

        if not symbol or not side:
            logger.warning(f"Missing symbol or side for {field}, skipping trade recovery")
            return True

        # ⭐ 关键改动:拉取完整交易记录
        open_fills, close_fills = self._fetch_all_trades_from_exchange(
            symbol=str(symbol),
            side=str(side),
            open_time_ms=int(cyc.get("openTimeMs", "0") or "0"),
            close_time_ms=close_time_ms,
        )

        logger.info(f"Ghost recovery for {field}: found {len(open_fills)} open + {len(close_fills)} close trades")

        # 读取已记录的订单ID
        existing_close_ids = set()
        try:
            close_ids = json.loads(cyc.get("closeOrderIds") or "[]")
            for x in close_ids:
                try:
                    existing_close_ids.add(int(x))
                except Exception as e:
                    logger.debug(f"[{self.uid}][{self.exchange}] 解析 closeOrderId 失败: {x}, error: {e}")
        except Exception as e:
            logger.debug(f"[{self.uid}][{self.exchange}] 解析 closeOrderIds JSON 失败: {e}")

        # 回补开仓交易
        backfilled_open = 0
        if open_fills:
            for fill in open_fills:
                cyc["openQty"] = str(D(cyc.get("openQty", "0")) + fill["qty"])
                cyc["openQuote"] = str(D(cyc.get("openQuote", "0")) + fill["qty"] * fill["price"])
                cyc["feeTotal"] = str(D(cyc.get("feeTotal", "0")) + fill["fee"])
                cyc["realizedPnlEst"] = str(D(cyc.get("realizedPnlEst", "0")) + fill["realized"])
                backfilled_open += 1

        # 回补平仓交易
        backfilled_close = 0
        if close_fills:
            for fill in close_fills:
                order_id = fill.get("orderId", 0)
                if order_id > 0 and order_id in existing_close_ids:
                    continue

                cyc = self._add_close_fill_to_cycle_dict(
                    cyc,
                    qty=fill["qty"],
                    price=fill["price"],
                    fee=fill["fee"],
                    realized=fill["realized"],
                    order_id=order_id,
                )
                backfilled_close += 1

        # 重新计算平均价格
        oq = D(cyc.get("openQty", "0"))
        cq = D(cyc.get("closeQty", "0"))
        cyc["avgOpenPrice"] = str(D(cyc.get("openQuote", "0")) / oq) if oq > 0 else "0"
        cyc["avgClosePrice"] = str(D(cyc.get("closeQuote", "0")) / cq) if cq > 0 else "0"

        # ⭐ 计算真实的 maxAbsQty
        max_abs_qty = self._calculate_max_position(open_fills, close_fills, str(side))
        if max_abs_qty > D(cyc.get("maxAbsQty", "0")):
            cyc["maxAbsQty"] = str(max_abs_qty)

        if backfilled_open > 0 or backfilled_close > 0:
            logger.info(f"Backfilled {backfilled_open} open + {backfilled_close} close trades for {field}")
            logger.debug(f"Updated maxAbsQty={cyc['maxAbsQty']} openQty={cyc['openQty']} closeQty={cyc['closeQty']}")
        elif not open_fills and not close_fills:
            logger.warning(f"No trades found for {field}, closing with existing data only")

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
        closed_trades = pf_compat.get_pf_closed_h(self.uid, self.exchange)
        closed_trades[cycle_id] = cyc
        pf_compat.set_pf_closed_h(self.uid, cycle_id, cyc, self.exchange)

        # Remove from positions, cycles, and active list
        if field in pos_data:
            del pos_data[field]
            pf_compat.set_pf_pos(self.uid, pos_data, self.exchange, skip_ghost_check=True)

        if field in cycle_data:
            del cycle_data[field]
            pf_compat.set_pf_cycle(self.uid, cycle_data, self.exchange)

        if field in active_list:
            active_list.remove(field)
            pf_compat.set_pf_pos_active(self.uid, active_list, self.exchange)

        logger.info(f"Ghost cycle closed: {field} cycleId={cycle_id} netPnl={cyc['netPnl']}")

        return True

    def _fetch_all_trades_from_exchange(
            self,
            symbol: str,
            side: str,
            open_time_ms: int,
            close_time_ms: int,
    ) -> tuple[List[dict], List[dict]]:
        """拉取完整的开仓+平仓交易"""
        if not symbol or not side:
            return [], []

        try:
            start_time = max(0, open_time_ms - 60000)
            end_time = close_time_ms + 5 * 60000

            # 使用 API Key 级别限速器
            from core.rate_limiter import get_binance_rate_limiter
            api_key = getattr(self.client, 'API_KEY', None)
            rate_limiter = get_binance_rate_limiter(api_key)
            rate_limiter.acquire(endpoint="futures_account_trades", timeout=30.0)

            trades = self.client.futures_account_trades(
                symbol=symbol,
                startTime=start_time,
                endTime=end_time,
                limit=1000,
            )

            if not trades:
                return [], []

            open_side = "BUY" if side.upper() == "LONG" else "SELL"
            close_side = "SELL" if side.upper() == "LONG" else "BUY"

            open_fills = []
            close_fills = []

            for t in trades:
                trade_side = (t.get("side") or "").upper()
                position_side = (t.get("positionSide") or "").upper()

                if position_side != side.upper():
                    continue

                trade_time = int(t.get("time", 0) or 0)
                if trade_time < open_time_ms or trade_time > close_time_ms + 5 * 60000:
                    continue

                qty = D(t.get("qty", "0"))
                price = D(t.get("price", "0"))
                if qty <= 0 or price <= 0:
                    continue

                fill_data = {
                    "qty": qty,
                    "price": price,
                    "fee": abs(D(t.get("commission", "0"))),
                    "realized": D(t.get("realizedPnl", "0")),
                    "orderId": int(t.get("orderId", 0) or 0),
                    "tradeId": int(t.get("id", 0) or 0),
                    "time": trade_time,
                }

                if trade_side == open_side:
                    open_fills.append(fill_data)
                elif trade_side == close_side:
                    close_fills.append(fill_data)

            open_fills.sort(key=lambda x: x["time"])
            close_fills.sort(key=lambda x: x["time"])

            return open_fills, close_fills

        except Exception as e:
            logger.error(f"Failed to fetch trades for {symbol}:{side}: {e}")
            logger.exception("获取成交记录失败")
            return [], []

    def _calculate_max_position(
            self,
            open_fills: List[dict],
            close_fills: List[dict],
            side: str,
    ) -> Decimal:
        """模拟持仓变化,计算峰值"""
        if not open_fills and not close_fills:
            return D("0")

        all_trades = []
        for fill in open_fills:
            all_trades.append({
                "time": fill["time"],
                "type": "open",
                "qty": fill["qty"],
            })
        for fill in close_fills:
            all_trades.append({
                "time": fill["time"],
                "type": "close",
                "qty": fill["qty"],
            })

        all_trades.sort(key=lambda x: x["time"])

        current_pos = D("0")
        max_pos = D("0")

        for trade in all_trades:
            if trade["type"] == "open":
                current_pos += trade["qty"]
            else:
                current_pos -= trade["qty"]

            abs_pos = abs(current_pos)
            if abs_pos > max_pos:
                max_pos = abs_pos

        return max_pos

    def _add_close_fill_to_cycle_dict(
            self,
            c: dict,
            *,
            qty: Decimal,
            price: Decimal,
            fee: Decimal,
            realized: Decimal,
            order_id: int,
    ) -> dict:
        c["closeQty"] = str(D(c.get("closeQty", "0")) + qty)
        c["closeQuote"] = str(D(c.get("closeQuote", "0")) + qty * price)

        if "closeOrderIds" not in c:
            c["closeOrderIds"] = "[]"
        if "closeTradeCount" not in c:
            c["closeTradeCount"] = "0"

        if order_id > 0:
            try:
                ids = json.loads(c.get("closeOrderIds") or "[]")
                s = set()
                for x in ids:
                    try:
                        s.add(int(x))
                    except Exception as e:
                        logger.debug(f"[{self.uid}][{self.exchange}] 解析 closeOrderId 失败: {x}, error: {e}")
            except Exception as e:
                logger.debug(f"[{self.uid}][{self.exchange}] 解析 closeOrderIds JSON 失败: {e}")
                s = set()
            s.add(order_id)
            c["closeOrderIds"] = json.dumps(sorted(s), separators=(",", ":"))
            c["closeTradeCount"] = str(len(s))

        c["feeTotal"] = str(D(c.get("feeTotal", "0")) + fee)
        c["realizedPnlEst"] = str(D(c.get("realizedPnlEst", "0")) + realized)

        cq = D(c.get("closeQty", "0"))
        c["avgClosePrice"] = str(D(c.get("closeQuote", "0")) / cq) if cq > 0 else "0"

        return c

    def _fix_missing_position(self, issue: AuditIssue, exchange_data: Optional[dict]) -> None:
        if not exchange_data:
            return

        field = issue.field
        
        # 先清除可能存在的清理标记（因为交易所确实有这个持仓）
        try:
            cleaned_key = k_ghost_cleaned(self.uid, self.exchange, field)
            self.rds.delete(cleaned_key)
        except Exception as e:
            # P3 Fix: 添加日志
            logger.debug(f"[{self.uid}] 清除清理标记异常: {e}")
        
        pos_data = pf_compat.get_pf_pos(self.uid, self.exchange)
        cycle_data = pf_compat.get_pf_cycle(self.uid, self.exchange)
        active_list = pf_compat.get_pf_pos_active(self.uid, self.exchange)

        raw = exchange_data.get("raw") or {}

        symbol = exchange_data.get("symbol") or raw.get("symbol")
        side = (exchange_data.get("side") or raw.get("positionSide") or "").upper()
        qty = abs(D(exchange_data.get("qty", raw.get("positionAmt", "0"))))
        entry_price = D(exchange_data.get("entryPrice", raw.get("entryPrice", "0")))

        break_even = D(raw.get("breakEvenPrice", "0"))
        unrealized = D(raw.get("unRealizedProfit", raw.get("unrealizedProfit", "0")))
        margin_type = (raw.get("marginType") or exchange_data.get("marginType") or "cross").lower()
        isolated_margin = D(raw.get("isolatedMargin", "0"))

        ts = now_ms()

        pos_obj = {
            "symbol": symbol,
            "side": side,
            "qty": str(qty),
            "entryPrice": str(entry_price),
            "breakEvenPrice": str(break_even),
            "unrealizedPnl": str(unrealized),
            "marginType": margin_type,
            "isolatedMargin": str(isolated_margin),
            "openTimeMs": str(ts),
            "updatedAt": str(ts),
            "source": "auditor_snapshot_recover",
            "exchange": self.exchange,  # 交易所标识
        }

        cycle_id = f"{symbol}:{side}:{int(ts // 1000)}:{uuid.uuid4().hex[:8]}"
        cyc = {
            "cycleId": cycle_id,
            "uid": self.uid,
            "symbol": symbol,
            "side": side,
            "exchange": self.exchange,  # 交易所标识
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

        # Update positions
        pos_data[field] = pos_obj
        pf_compat.set_pf_pos(self.uid, pos_data, self.exchange, skip_ghost_check=True)

        # Update cycles
        cycle_data[field] = cyc
        pf_compat.set_pf_cycle(self.uid, cycle_data, self.exchange)

        # Update active list
        if field not in active_list:
            active_list.append(field)
            pf_compat.set_pf_pos_active(self.uid, active_list, self.exchange)

    def _fix_qty_mismatch(self, issue: AuditIssue, exchange_data: Optional[dict]) -> None:
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
        field = issue.field
        cycle_data = pf_compat.get_pf_cycle(self.uid, self.exchange)
        if field in cycle_data:
            del cycle_data[field]
            pf_compat.set_pf_cycle(self.uid, cycle_data, self.exchange)

    def _fix_orphan_active(self, issue: AuditIssue) -> None:
        field = issue.field
        active_list = pf_compat.get_pf_pos_active(self.uid, self.exchange)
        if field in active_list:
            active_list.remove(field)
            pf_compat.set_pf_pos_active(self.uid, active_list, self.exchange)

    def _fix_missing_cycle(self, issue: AuditIssue) -> None:
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
            "exchange": self.exchange,  # 交易所标识
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

        # Update cycles
        cycle_data[field] = cyc
        pf_compat.set_pf_cycle(self.uid, cycle_data, self.exchange)

        # Update active list
        if field not in active_list:
            active_list.append(field)
            pf_compat.set_pf_pos_active(self.uid, active_list, self.exchange)

    def get_last_report(self) -> Optional[AuditReport]:
        return self._last_report

    def set_dry_run(self, dry_run: bool) -> None:
        self.dry_run = dry_run

    def set_interval(self, interval_s: float) -> None:
        self.audit_interval_s = interval_s
    
    # ==================== 纯 Asyncio 模式 ====================
    
    async def run_async(self) -> None:
        """
        纯 asyncio 模式运行
        
        使用方法：
        asyncio.create_task(auditor.run_async())
        """
        import random
        from core.async_helpers import run_sync
        
        # 启动前等待系统稳定 + 随机延迟（避免所有用户同时触发审计）
        # 基础延迟 10s + 随机 0-50s，分散请求
        random_delay = random.uniform(0, 50)
        total_delay = 10.0 + random_delay
        try:
            await asyncio.sleep(total_delay)
        except asyncio.CancelledError:
            return
        
        logger.info(f"[{self.uid}][{self.exchange}] 持仓审计器启动 (asyncio 模式, 延迟 {total_delay:.1f}s)")
        
        try:
            while True:
                try:
                    # 在线程池中执行同步审计操作（因为依赖同步的 binance.client）
                    report = await run_sync(
                        lambda: self.audit_once(dry_run=self.dry_run)
                    )
                    self._last_report = report
                    
                    # 如果有问题，记录日志
                    if report.has_issues():
                        logger.info(f"[{self.uid}][{self.exchange}] 审计报告:\n{report.summary()}")
                    
                    # 回调
                    if self.on_report:
                        try:
                            self.on_report(report)
                        except Exception as e:
                            # P3 Fix: 添加日志
                            logger.warning(f"[{self.uid}][{self.exchange}] 审计报告回调异常: {e}")
                    
                except asyncio.CancelledError:
                    logger.info(f"[{self.uid}][{self.exchange}] 持仓审计器被取消")
                    break
                except Exception as e:
                    logger.error(f"[{self.uid}][{self.exchange}] 审计异常: {e}")
                    logger.exception("审计异常")
                
                # 等待下一次审计
                try:
                    await asyncio.sleep(self.audit_interval_s)
                except asyncio.CancelledError:
                    break
        finally:
            logger.info(f"[{self.uid}][{self.exchange}] 持仓审计器停止 (asyncio 模式)")
    
    async def audit_once_async(self, *, dry_run: Optional[bool] = None) -> AuditReport:
        """
        异步执行一次审计（包装同步方法）
        """
        from core.async_helpers import run_sync
        
        if dry_run is None:
            dry_run = self.dry_run
        
        return await run_sync(lambda: self.audit_once(dry_run=dry_run))


def start_position_auditor(
        client: Client,
        redis_conn,
        uid: str,
        stop_event: threading.Event,
        *,
        audit_interval_s: float = 61.0,
        dry_run: bool = False,
        on_report: Optional[callable] = None,
        exchange: str = "binance",
        on_auth_failed: Optional[callable] = None,
) -> tuple[PositionAuditor, threading.Thread]:
    auditor = PositionAuditor(
        client=client,
        redis_conn=redis_conn,
        uid=uid,
        audit_interval_s=audit_interval_s,
        dry_run=dry_run,
        on_report=on_report,
        exchange=exchange,
        on_auth_failed=on_auth_failed,
    )

    def _run():
        auditor.start()
        stop_event.wait()
        auditor.stop()

    t = threading.Thread(
        target=_run,
        name=f"PositionAuditor({uid})",
        daemon=True,
    )
    t.start()

    return auditor, t