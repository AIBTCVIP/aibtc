# global_position_auditor.py
"""
全局 Position Auditor - 1000+ 用户规模优化

问题：
- 原实现：每用户独立线程，每 61 秒调用 API
- 1000 用户 = 1000 线程 + 16 次/秒 API 调用

优化方案：
- 单例模式，一个线程处理所有用户
- 分批处理，避免 API 限速
- 错开审计时间，平滑负载

架构：
1. GlobalPositionAuditor (单例)
   - 维护所有注册用户的列表
   - 单线程循环处理所有用户
   - 分批 API 调用

2. 注册/注销接口
   - register_user(uid, exchange, client) - 注册用户
   - unregister_user(uid, exchange) - 注销用户
"""

import logging
import os
import random
import threading
import time
import traceback
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Dict, List, Optional, Set, Tuple, Any

from core.utils import D, now_ms
from core.pf_compatibility import pf_compat

logger = logging.getLogger(__name__)


@dataclass
class UserAuditInfo:
    """用户审计信息"""
    uid: str
    exchange: str
    client: Any  # 交易所客户端
    last_audit_ts: float = 0
    audit_count: int = 0
    error_count: int = 0
    last_error: Optional[str] = None
    on_auth_failed: Optional[Any] = None  # 认证失败回调


class GlobalPositionAuditor:
    """
    全局 Position Auditor
    
    单例模式，一个线程处理所有用户的持仓审计
    """
    
    _instance = None
    _lock = threading.Lock()
    
    # 配置
    DEFAULT_AUDIT_INTERVAL = 120  # 每用户审计间隔（秒）- 比原来的 61s 更长
    BATCH_SIZE = 20  # 每批处理的用户数
    BATCH_DELAY = 2.0  # 批次间延迟（秒）
    
    # Binance 认证相关错误码
    AUTH_ERROR_CODES = {'-2015', '-2014', '-1022', '-1002', '-1021'}
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
        
        # 注册的用户: {(uid, exchange): UserAuditInfo}
        self._users: Dict[Tuple[str, str], UserAuditInfo] = {}
        self._users_lock = threading.Lock()
        
        # 处理线程
        self._auditor_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        
        # 配置
        self._audit_interval = self.DEFAULT_AUDIT_INTERVAL
        
        # 统计
        self._stats = {
            "total_audits": 0,
            "total_issues_found": 0,
            "total_issues_fixed": 0,
            "total_errors": 0,
            "last_cycle_time_ms": 0,
        }
        
        self._initialized = True
        logger.info("[GlobalPositionAuditor] 初始化完成")
    
    def start(self) -> None:
        """启动全局审计器"""
        if self._auditor_thread and self._auditor_thread.is_alive():
            return
        
        self._stop_event.clear()
        self._auditor_thread = threading.Thread(
            target=self._run_loop,
            name="global-position-auditor",
            daemon=True
        )
        self._auditor_thread.start()
        logger.info("[GlobalPositionAuditor] 已启动")
    
    def stop(self) -> None:
        """停止全局审计器"""
        self._stop_event.set()
        if self._auditor_thread:
            self._auditor_thread.join(timeout=10.0)
            self._auditor_thread = None
        logger.info("[GlobalPositionAuditor] 已停止")
    
    def register_user(
        self,
        uid: str,
        exchange: str,
        client: Any,
        on_auth_failed: Optional[Any] = None,
    ) -> None:
        """注册用户"""
        with self._users_lock:
            key = (uid, exchange)
            if key not in self._users:
                self._users[key] = UserAuditInfo(
                    uid=uid,
                    exchange=exchange,
                    client=client,
                    # 随机化首次审计时间，避免同时审计
                    last_audit_ts=time.time() - random.uniform(0, self._audit_interval),
                    on_auth_failed=on_auth_failed,
                )
                logger.debug(f"[GlobalPositionAuditor] 注册用户: {uid}:{exchange}")
            else:
                # 更新回调（如果用户已注册但回调变了）
                self._users[key].on_auth_failed = on_auth_failed
        
        # 确保审计器已启动
        self.start()
    
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
    
    def unregister_user(self, uid: str, exchange: str) -> None:
        """注销用户"""
        with self._users_lock:
            key = (uid, exchange)
            if key in self._users:
                del self._users[key]
                logger.debug(f"[GlobalPositionAuditor] 注销用户: {uid}:{exchange}")
    
    def get_stats(self) -> dict:
        """获取统计信息"""
        with self._users_lock:
            user_count = len(self._users)
        
        return {
            **self._stats,
            "registered_users": user_count,
            "is_running": self._auditor_thread is not None and self._auditor_thread.is_alive(),
            "audit_interval": self._audit_interval,
        }
    
    def force_audit(self, uid: str, exchange: str) -> Optional[dict]:
        """强制立即审计指定用户"""
        with self._users_lock:
            key = (uid, exchange)
            info = self._users.get(key)
            if not info:
                return None
        
        return self._audit_single_user(info)
    
    # ========== 内部方法 ==========
    
    def _run_loop(self) -> None:
        """主循环"""
        logger.info("[GlobalPositionAuditor] 主循环开始")
        
        # 启动延迟（30-90秒），等待系统稳定
        initial_delay = 30.0 + random.uniform(0, 60)
        logger.info(f"[GlobalPositionAuditor] 等待 {initial_delay:.0f}s 后开始审计")
        self._stop_event.wait(initial_delay)
        
        while not self._stop_event.is_set():
            try:
                start_time = time.monotonic()
                
                # 获取需要审计的用户
                users_to_audit = self._get_users_to_audit()
                
                if users_to_audit:
                    logger.debug(f"[GlobalPositionAuditor] 本轮审计 {len(users_to_audit)} 个用户")
                    
                    # 分批处理
                    for i in range(0, len(users_to_audit), self.BATCH_SIZE):
                        if self._stop_event.is_set():
                            break
                        
                        batch = users_to_audit[i:i + self.BATCH_SIZE]
                        self._process_batch(batch)
                        
                        # 批次间延迟
                        if i + self.BATCH_SIZE < len(users_to_audit):
                            self._stop_event.wait(self.BATCH_DELAY)
                
                # 更新统计
                self._stats["last_cycle_time_ms"] = int((time.monotonic() - start_time) * 1000)
                
                # 等待下一个周期（短间隔检查，快速响应新用户）
                self._stop_event.wait(10.0)
                
            except Exception as e:
                logger.error(f"[GlobalPositionAuditor] 主循环错误: {e}")
                logger.exception("全局持仓审计主循环错误")
                self._stats["total_errors"] += 1
                self._stop_event.wait(30.0)
        
        logger.info("[GlobalPositionAuditor] 主循环结束")
    
    def _get_users_to_audit(self) -> List[UserAuditInfo]:
        """获取需要审计的用户列表"""
        now = time.time()
        users_to_audit = []
        
        with self._users_lock:
            for info in self._users.values():
                # 检查是否到了审计时间
                if now - info.last_audit_ts >= self._audit_interval:
                    users_to_audit.append(info)
        
        # 按上次审计时间排序（最久未审计的优先）
        users_to_audit.sort(key=lambda x: x.last_audit_ts)
        
        return users_to_audit
    
    def _process_batch(self, users: List[UserAuditInfo]) -> None:
        """处理一批用户"""
        for info in users:
            if self._stop_event.is_set():
                break
            
            try:
                self._audit_single_user(info)
            except Exception as e:
                error_msg = str(e)
                logger.error(f"[GlobalPositionAuditor] 审计用户 {info.uid}:{info.exchange} 失败: {error_msg}")
                info.error_count += 1
                info.last_error = error_msg
                self._stats["total_errors"] += 1
                
                # 检查是否是认证错误
                if self._is_auth_error(error_msg):
                    logger.error(f"[GlobalPositionAuditor] {info.uid}:{info.exchange} 检测到认证错误，注销用户")
                    # 触发回调
                    if info.on_auth_failed:
                        try:
                            info.on_auth_failed(error_msg)
                        except Exception:
                            pass
                    # 注销用户
                    self.unregister_user(info.uid, info.exchange)
    
    def _audit_single_user(self, info: UserAuditInfo) -> dict:
        """审计单个用户"""
        uid = info.uid
        exchange = info.exchange
        client = info.client
        
        start_ts = now_ms()
        issues_found = 0
        issues_fixed = 0
        
        try:
            # 获取交易所持仓
            exchange_positions = self._fetch_exchange_positions(client, exchange, uid)
            
            # 获取 Redis 持仓
            redis_positions = self._fetch_redis_positions(uid, exchange)
            
            # 比对并修复
            issues_found, issues_fixed = self._compare_and_fix(
                uid, exchange, redis_positions, exchange_positions, client
            )
            
            # 更新统计
            info.last_audit_ts = time.time()
            info.audit_count += 1
            self._stats["total_audits"] += 1
            self._stats["total_issues_found"] += issues_found
            self._stats["total_issues_fixed"] += issues_fixed
            
            duration = now_ms() - start_ts
            
            if issues_found > 0:
                logger.info(
                    f"[GlobalPositionAuditor] {uid}:{exchange} 审计完成: "
                    f"issues={issues_found}, fixed={issues_fixed}, duration={duration}ms"
                )
            
            return {
                "uid": uid,
                "exchange": exchange,
                "duration_ms": duration,
                "redis_positions": len(redis_positions),
                "exchange_positions": len(exchange_positions),
                "issues_found": issues_found,
                "issues_fixed": issues_fixed,
            }
            
        except Exception as e:
            info.error_count += 1
            info.last_error = str(e)
            raise
    
    def _fetch_exchange_positions(
        self,
        client: Any,
        exchange: str,
        uid: str
    ) -> Dict[str, dict]:
        """获取交易所持仓"""
        positions: Dict[str, dict] = {}
        
        # 使用限速器
        if exchange == "binance":
            from core.rate_limiter import get_binance_rate_limiter
            api_key = getattr(client, 'API_KEY', None)
            rate_limiter = get_binance_rate_limiter(api_key)
            rate_limiter.acquire(endpoint="futures_position_information", timeout=30.0)
            
            result = client.futures_position_information()
            
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
                    # 额外字段 - 用于恢复缺失持仓
                    "unrealizedPnl": p.get("unRealizedProfit", "0"),
                    "initialMargin": p.get("initialMargin", "0"),
                    "markPrice": p.get("markPrice", "0"),
                    "notional": p.get("notional", "0"),
                    "leverage": p.get("leverage", "1"),
                    "updateTime": p.get("updateTime", 0),
                    "liquidationPrice": p.get("liquidationPrice", "0"),
                    "isolatedMargin": p.get("isolatedMargin", "0"),
                }
        
        # 注意：当前仅支持 Binance 交易所
        # 其他交易所（OKX, Bitget, Hyperliquid）的持仓审计
        # 将在 P3 水平扩展阶段实现
        
        return positions
    
    def _fetch_redis_positions(self, uid: str, exchange: str) -> Dict[str, dict]:
        """获取 Redis 持仓"""
        positions: Dict[str, dict] = {}
        pos_data = pf_compat.get_pf_pos(uid, exchange)
        
        for field, p in pos_data.items():
            qty = D(p.get("qty", "0"))
            if qty <= 0:
                continue
            positions[field] = {
                "symbol": p.get("symbol"),
                "side": p.get("side"),
                "qty": qty,
                "entryPrice": D(p.get("entryPrice", "0")),
            }
        
        return positions
    
    def _compare_and_fix(
        self,
        uid: str,
        exchange: str,
        redis_pos: Dict[str, dict],
        exchange_pos: Dict[str, dict],
        client: Any
    ) -> Tuple[int, int]:
        """比对并修复持仓"""
        issues_found = 0
        issues_fixed = 0
        
        all_fields = set(redis_pos.keys()) | set(exchange_pos.keys())
        
        for field in all_fields:
            r = redis_pos.get(field)
            e = exchange_pos.get(field)
            
            # Ghost position: Redis 有但交易所没有
            if r and not e:
                issues_found += 1
                # 委托给原来的 PositionAuditor 处理幽灵仓位（包含宽限期、交易回补、写入 closed records）
                fixed = self._handle_ghost_position(uid, exchange, field, client)
                if fixed:
                    issues_fixed += 1
                    logger.warning(f"[GlobalPositionAuditor] {uid}:{exchange} 处理幽灵持仓: {field}")
                else:
                    logger.debug(f"[GlobalPositionAuditor] {uid}:{exchange} 幽灵持仓宽限期内: {field}")
            
            # Missing position: 交易所有但 Redis 没有
            elif e and not r:
                issues_found += 1
                # 添加缺失的持仓到 Redis
                self._add_missing_position(uid, exchange, field, e)
                issues_fixed += 1
                logger.warning(f"[GlobalPositionAuditor] {uid}:{exchange} 添加缺失持仓: {field}")
            
            # 数量不匹配
            elif r and e:
                qty_diff = abs(r["qty"] - e["qty"])
                if qty_diff > Decimal("0.00001"):
                    issues_found += 1
                    # 更新数量
                    self._update_position_qty(uid, exchange, field, e["qty"], e["entryPrice"])
                    issues_fixed += 1
                    logger.warning(
                        f"[GlobalPositionAuditor] {uid}:{exchange} 修复数量: {field} "
                        f"{r['qty']} -> {e['qty']}"
                    )
        
        return issues_found, issues_fixed
    
    def _handle_ghost_position(self, uid: str, exchange: str, field: str, client: Any) -> bool:
        """
        处理幽灵仓位 - 委托给原来的 PositionAuditor
        
        包含：
        1. 宽限期检查（避免误删正在平仓的持仓）
        2. 从交易所拉取完整交易记录
        3. 回补开仓/平仓交易，计算 netPnl
        4. 关闭 cycle 并写入 closed records
        5. 清理 Redis 数据
        
        Returns:
            True 如果已修复，False 如果在宽限期内
        """
        try:
            from cache.position_auditor import PositionAuditor, AuditIssue, IssueType
            from core.database import redis_client
            
            # 创建临时的 PositionAuditor 实例
            auditor = PositionAuditor(uid=uid, exchange=exchange, client=client, redis_conn=redis_client)
            
            # 创建 AuditIssue 对象
            issue = AuditIssue(
                field=field,
                issue_type=IssueType.GHOST_POSITION,
                redis_data={"field": field},
                exchange_data=None,
                description=f"Ghost position: {field}"
            )
            
            # 调用原来的修复方法（包含宽限期、交易回补等完整逻辑）
            return auditor._fix_ghost_position(issue)
            
        except Exception as e:
            logger.exception(f"[GlobalPositionAuditor] 处理幽灵仓位失败 {uid}:{exchange}:{field}: {e}")
            return False
    
    def _add_missing_position(
        self,
        uid: str,
        exchange: str,
        field: str,
        exchange_data: dict
    ) -> None:
        """
        添加缺失的持仓
        
        注意：从交易所 API 恢复的持仓会丢失以下信息（因为 API 不提供）：
        - 开仓时间 (openTimeMs) - 使用当前时间作为近似值
        - 止损价格 (stopLossPrice) - 需要用户重新设置
        - 止盈价格 (takeProfitPrice) - 需要用户重新设置
        
        这些信息只能通过 WebSocket 实时流获取，API 恢复无法获得。
        """
        from core.database import redis_client
        
        # 先清除可能存在的清理标记（因为交易所确实有这个持仓）
        try:
            cleaned_key = f"pf:ghost_cleaned:{uid}:{exchange}:{field}"
            redis_client.delete(cleaned_key)
        except Exception as e:
            # P3 Fix: 添加日志
            logger.debug(f"[{uid}] 清除清理标记异常: {e}")
        
        pos_data = pf_compat.get_pf_pos(uid, exchange)
        
        # 使用 updateTime 作为开仓时间的近似值（如果有的话）
        # 否则使用当前时间
        update_time = exchange_data.get("updateTime", 0)
        open_time_ms = str(update_time) if update_time else str(now_ms())
        
        pos_data[field] = {
            "symbol": exchange_data["symbol"],
            "side": exchange_data["side"],
            "qty": str(exchange_data["qty"]),
            "entryPrice": str(exchange_data["entryPrice"]),
            "unrealizedPnl": str(exchange_data.get("unrealizedPnl", "0")),
            "exchange": exchange,
            "updatedAt": str(now_ms()),
            "source": "AUDIT_RECOVERY",
            # 额外字段
            "openTimeMs": open_time_ms,  # 使用 updateTime 或当前时间
            "markPrice": str(exchange_data.get("markPrice", "0")),
            "notional": str(exchange_data.get("notional", "0")),
            "leverage": str(exchange_data.get("leverage", "1")),
            "initialMargin": str(exchange_data.get("initialMargin", "0")),
            "liquidationPrice": str(exchange_data.get("liquidationPrice", "0")),
            "isolatedMargin": str(exchange_data.get("isolatedMargin", "0")),
            # 止损止盈无法从 API 恢复，设为 None
            "stopLossPrice": None,
            "takeProfitPrice": None,
        }
        
        # 审计器写入时跳过幽灵检查（因为这是审计器自己在写入）
        pf_compat.set_pf_pos(uid, pos_data, exchange, skip_ghost_check=True)
        
        # ⭐ 同时创建 cycle 数据，确保平仓时能生成 closed records
        import uuid
        symbol = exchange_data["symbol"]
        side = exchange_data["side"]
        qty = str(exchange_data["qty"])
        entry_price = str(exchange_data["entryPrice"])
        notional = str(Decimal(qty) * Decimal(entry_price))
        
        cycle_data = pf_compat.get_pf_cycle(uid, exchange)
        cycle_data[field] = {
            "cycleId": f"{symbol}:{side}:{int(int(open_time_ms) // 1000)}:{uuid.uuid4().hex[:8]}",
            "symbol": symbol,
            "side": side,
            "exchange": exchange,
            "openTimeMs": open_time_ms,
            "closeTimeMs": "0",
            "openQty": qty,
            "closeQty": "0",
            "openQuote": notional,
            "closeQuote": "0",
            "avgOpenPrice": entry_price,
            "avgClosePrice": "0",
            "maxAbsQty": qty,
            "feeTotal": "0",
            "fundingTotal": "0",
            "realizedPnlEst": "0",
            "netPnl": "0",
            "peakPnl": "0",
            "drawdownToClose": "0",
            "durationMs": "0",
            "updatedAt": str(now_ms()),
            "field": field,
            "source": "AUDIT_RECOVERY",
        }
        pf_compat.set_pf_cycle(uid, cycle_data, exchange)
        
        # 添加到活跃列表
        active = pf_compat.get_pf_pos_active(uid, exchange)
        if field not in active:
            active.append(field)
            pf_compat.set_pf_pos_active(uid, active, exchange)
        
        # 记录警告，提醒用户需要重新设置止损止盈
        logger.warning(
            f"[GlobalPositionAuditor] {uid}:{exchange} 恢复持仓 {field} - "
            f"注意：止损/止盈需要重新设置（API 无法恢复）"
        )
    
    def _update_position_qty(
        self,
        uid: str,
        exchange: str,
        field: str,
        qty: Decimal,
        entry_price: Decimal
    ) -> None:
        """更新持仓数量"""
        pos_data = pf_compat.get_pf_pos(uid, exchange)
        
        if field in pos_data:
            pos_data[field]["qty"] = str(qty)
            pos_data[field]["entryPrice"] = str(entry_price)
            pos_data[field]["updatedAt"] = str(now_ms())
            # 审计器写入时跳过幽灵检查
            pf_compat.set_pf_pos(uid, pos_data, exchange, skip_ghost_check=True)


# ========== 全局单例 ==========

_global_auditor: Optional[GlobalPositionAuditor] = None
_global_auditor_lock = threading.Lock()


def get_global_position_auditor() -> GlobalPositionAuditor:
    """获取全局 Position Auditor 单例"""
    global _global_auditor
    
    with _global_auditor_lock:
        if _global_auditor is None:
            _global_auditor = GlobalPositionAuditor()
        return _global_auditor


def is_global_auditor_enabled() -> bool:
    """检查是否启用全局审计器"""
    return os.environ.get("USE_GLOBAL_AUDITOR", "1") == "1"


# ========== 兼容适配器 ==========

class PositionAuditorAdapter:
    """
    Position Auditor 适配器
    
    提供与原 PositionAuditor 相同的接口，内部委托给全局处理器
    """
    
    def __init__(
        self,
        client,
        redis_conn,
        uid: str,
        *,
        audit_interval_s: float = 61.0,
        dry_run: bool = False,
        on_report=None,
        exchange: str = "binance",
        on_auth_failed=None,
    ):
        self.client = client
        self.rds = redis_conn
        self.uid = uid
        self.exchange = exchange
        self.dry_run = dry_run
        self.on_report = on_report
        self.on_auth_failed = on_auth_failed
        
        self._use_global = is_global_auditor_enabled()
    
    def start(self) -> None:
        """启动审计器"""
        if self._use_global:
            auditor = get_global_position_auditor()
            auditor.register_user(self.uid, self.exchange, self.client, self.on_auth_failed)
        else:
            # 原实现（保留兼容）
            pass
    
    def stop(self) -> None:
        """停止审计器"""
        if self._use_global:
            auditor = get_global_position_auditor()
            auditor.unregister_user(self.uid, self.exchange)
