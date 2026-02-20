# core/exchange_monitor.py
"""
交易所连接监控模块

职责:
1. 监控各交易所的连接状态
2. 检测数据过期/断开
3. 触发告警（日志 + 可选 webhook）
4. 提供状态查询接口
"""

import logging
import threading
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, Optional, List, Callable
from datetime import datetime

from core.pf_compatibility import pf_compat
from core.config import REDIS_HOST, REDIS_PORT, REDIS_DB

logger = logging.getLogger(__name__)


class ConnectionStatus(Enum):
    """连接状态"""
    CONNECTED = "connected"
    STALE = "stale"
    DISCONNECTED = "disconnected"
    ERROR = "error"
    UNKNOWN = "unknown"


@dataclass
class ExchangeStatus:
    """交易所状态"""
    exchange: str
    status: ConnectionStatus
    last_update_ms: int = 0
    age_ms: int = 0
    error: Optional[str] = None
    position_count: int = 0
    cycle_count: int = 0
    
    def to_dict(self) -> dict:
        return {
            "exchange": self.exchange,
            "status": self.status.value,
            "last_update_ms": self.last_update_ms,
            "age_ms": self.age_ms,
            "error": self.error,
            "position_count": self.position_count,
            "cycle_count": self.cycle_count,
        }


@dataclass
class AlertEvent:
    """告警事件"""
    timestamp: int
    uid: str
    exchange: str
    event_type: str  # "disconnected", "stale", "reconnected", "error"
    message: str
    details: Optional[dict] = None


class ExchangeMonitor:
    """
    交易所监控器
    
    定期检查各交易所的连接状态，触发告警
    """
    
    # 阈值配置
    STALE_THRESHOLD_MS = 60 * 1000  # 60秒无更新视为过期
    DISCONNECT_THRESHOLD_MS = 5 * 60 * 1000  # 5分钟无更新视为断开
    CHECK_INTERVAL_S = 30  # 检查间隔
    
    SUPPORTED_EXCHANGES = ["binance", "okx", "bitget", "hyperliquid"]
    
    def __init__(
        self,
        uid: str,
        *,
        on_alert: Optional[Callable[[AlertEvent], None]] = None,
        check_interval_s: float = 30,
    ):
        self.uid = uid
        self.on_alert = on_alert
        self.check_interval_s = check_interval_s
        
        # 状态缓存
        self._status_cache: Dict[str, ExchangeStatus] = {}
        self._last_status: Dict[str, ConnectionStatus] = {}
        
        # 告警历史
        self._alert_history: List[AlertEvent] = []
        self._max_history = 100
        
        # 线程控制
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
    
    def start(self) -> None:
        """启动监控"""
        if self._thread and self._thread.is_alive():
            return
        
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run_loop,
            name=f"exchange-monitor-{self.uid}",
            daemon=True
        )
        self._thread.start()
        logger.info(f"[{self.uid}] ExchangeMonitor 已启动")
    
    def stop(self) -> None:
        """停止监控"""
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=5.0)
        self._thread = None
        logger.info(f"[{self.uid}] ExchangeMonitor 已停止")
    
    def _run_loop(self) -> None:
        """监控循环"""
        while not self._stop_event.is_set():
            try:
                self._check_all_exchanges()
            except Exception as e:
                logger.error(f"[{self.uid}] ExchangeMonitor 检查失败: {e}")
            
            self._stop_event.wait(self.check_interval_s)
    
    def _check_all_exchanges(self) -> None:
        """检查所有交易所状态"""
        now_ms = int(time.time() * 1000)
        
        for exchange in self.SUPPORTED_EXCHANGES:
            try:
                status = self._check_exchange(exchange, now_ms)
                old_status = self._last_status.get(exchange)
                
                # 状态变化检测
                if old_status and old_status != status.status:
                    self._handle_status_change(exchange, old_status, status)
                
                self._status_cache[exchange] = status
                self._last_status[exchange] = status.status
                
            except Exception as e:
                logger.debug(f"[{self.uid}] 检查 {exchange} 状态失败: {e}")
                self._status_cache[exchange] = ExchangeStatus(
                    exchange=exchange,
                    status=ConnectionStatus.ERROR,
                    error=str(e),
                )
    
    def _check_exchange(self, exchange: str, now_ms: int) -> ExchangeStatus:
        """检查单个交易所状态"""
        # 获取账户数据
        account = pf_compat.get_pf_account(self.uid, exchange)
        pos_data = pf_compat.get_pf_pos(self.uid, exchange)
        cycle_data = pf_compat.get_pf_cycle(self.uid, exchange)
        
        # 确定最后更新时间
        last_update_ms = 0
        
        if account:
            ts = int(account.get("ts", 0) or 0)
            if ts > last_update_ms:
                last_update_ms = ts
        
        if pos_data:
            for field, pos in pos_data.items():
                ts = int(pos.get("updatedAt", 0) or 0)
                if ts > last_update_ms:
                    last_update_ms = ts
        
        if cycle_data:
            for field, cycle in cycle_data.items():
                ts = int(cycle.get("updatedAt", 0) or 0)
                if ts > last_update_ms:
                    last_update_ms = ts
        
        # 计算状态
        age_ms = now_ms - last_update_ms if last_update_ms > 0 else 0
        
        if last_update_ms == 0:
            status = ConnectionStatus.DISCONNECTED
        elif age_ms > self.DISCONNECT_THRESHOLD_MS:
            status = ConnectionStatus.DISCONNECTED
        elif age_ms > self.STALE_THRESHOLD_MS:
            status = ConnectionStatus.STALE
        else:
            status = ConnectionStatus.CONNECTED
        
        return ExchangeStatus(
            exchange=exchange,
            status=status,
            last_update_ms=last_update_ms,
            age_ms=age_ms,
            position_count=len(pos_data) if pos_data else 0,
            cycle_count=len(cycle_data) if cycle_data else 0,
        )
    
    def _handle_status_change(
        self,
        exchange: str,
        old_status: ConnectionStatus,
        new_status: ExchangeStatus
    ) -> None:
        """处理状态变化"""
        now_ms = int(time.time() * 1000)
        
        # 确定事件类型
        if new_status.status == ConnectionStatus.DISCONNECTED:
            event_type = "disconnected"
            message = f"{exchange} 连接已断开"
            log_level = logging.WARNING
        elif new_status.status == ConnectionStatus.STALE:
            event_type = "stale"
            message = f"{exchange} 数据已过期 ({new_status.age_ms // 1000}秒)"
            log_level = logging.WARNING
        elif new_status.status == ConnectionStatus.CONNECTED:
            if old_status in (ConnectionStatus.DISCONNECTED, ConnectionStatus.STALE):
                event_type = "reconnected"
                message = f"{exchange} 已重新连接"
                log_level = logging.INFO
            else:
                return  # 不需要告警
        elif new_status.status == ConnectionStatus.ERROR:
            event_type = "error"
            message = f"{exchange} 连接错误: {new_status.error}"
            log_level = logging.ERROR
        else:
            return
        
        # 创建告警事件
        alert = AlertEvent(
            timestamp=now_ms,
            uid=self.uid,
            exchange=exchange,
            event_type=event_type,
            message=message,
            details=new_status.to_dict(),
        )
        
        # 记录日志
        logger.log(log_level, f"[{self.uid}][{exchange}] {message}")
        
        # 保存历史
        self._alert_history.append(alert)
        if len(self._alert_history) > self._max_history:
            self._alert_history = self._alert_history[-self._max_history:]
        
        # 触发回调
        if self.on_alert:
            try:
                self.on_alert(alert)
            except Exception as e:
                logger.error(f"[{self.uid}] 告警回调失败: {e}")
    
    def get_status(self, exchange: str = None) -> Dict:
        """获取状态"""
        if exchange:
            status = self._status_cache.get(exchange)
            return status.to_dict() if status else {"exchange": exchange, "status": "unknown"}
        
        return {
            ex: (self._status_cache[ex].to_dict() if ex in self._status_cache else {"exchange": ex, "status": "unknown"})
            for ex in self.SUPPORTED_EXCHANGES
        }
    
    def get_alerts(self, limit: int = 20) -> List[dict]:
        """获取最近的告警"""
        alerts = self._alert_history[-limit:]
        return [
            {
                "timestamp": a.timestamp,
                "uid": a.uid,
                "exchange": a.exchange,
                "event_type": a.event_type,
                "message": a.message,
                "details": a.details,
            }
            for a in reversed(alerts)
        ]


class MultiUserExchangeMonitor:
    """
    多用户交易所监控器
    
    管理多个用户的监控实例
    """
    
    def __init__(self):
        self._monitors: Dict[str, ExchangeMonitor] = {}
        self._lock = threading.Lock()
    
    def get_or_create_monitor(
        self,
        uid: str,
        on_alert: Optional[Callable[[AlertEvent], None]] = None
    ) -> ExchangeMonitor:
        """获取或创建用户监控器"""
        with self._lock:
            if uid not in self._monitors:
                self._monitors[uid] = ExchangeMonitor(uid, on_alert=on_alert)
            return self._monitors[uid]
    
    def start_monitor(self, uid: str) -> None:
        """启动用户监控"""
        monitor = self.get_or_create_monitor(uid)
        monitor.start()
    
    def stop_monitor(self, uid: str) -> None:
        """停止用户监控"""
        with self._lock:
            if uid in self._monitors:
                self._monitors[uid].stop()
                del self._monitors[uid]
    
    def stop_all(self) -> None:
        """停止所有监控"""
        with self._lock:
            for uid, monitor in self._monitors.items():
                try:
                    monitor.stop()
                except Exception as e:
                    logger.error(f"停止 {uid} 监控失败: {e}")
            self._monitors.clear()
    
    def get_all_status(self) -> Dict[str, Dict]:
        """获取所有用户的状态"""
        result = {}
        with self._lock:
            for uid, monitor in self._monitors.items():
                result[uid] = monitor.get_status()
        return result


# 全局监控器实例
_global_monitor: Optional[MultiUserExchangeMonitor] = None


def get_exchange_monitor() -> MultiUserExchangeMonitor:
    """获取全局监控器"""
    global _global_monitor
    if _global_monitor is None:
        _global_monitor = MultiUserExchangeMonitor()
    return _global_monitor
