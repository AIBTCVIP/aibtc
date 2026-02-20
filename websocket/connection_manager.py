# connection_manager.py
"""
WebSocket 连接管理器 - 1000+ 用户规模优化

功能：
1. 统一监控所有 WebSocket 连接状态
2. 连接数限制和告警
3. 健康检查和自动清理
4. 连接统计和指标导出

架构：
- WebSocketConnectionManager (单例)
- 注册/注销接口
- 健康检查线程
- 指标导出
"""

import logging
import threading
import time
from dataclasses import dataclass, field
from typing import Dict, Optional, Set, Callable
from enum import Enum

logger = logging.getLogger(__name__)


class ConnectionType(Enum):
    """连接类型"""
    BINANCE_USER_STREAM = "binance_user_stream"
    OKX_USER_STREAM = "okx_user_stream"
    BITGET_USER_STREAM = "bitget_user_stream"
    HYPERLIQUID_USER_STREAM = "hyperliquid_user_stream"
    BINANCE_MARK_PRICE = "binance_mark_price"
    OKX_MARK_PRICE = "okx_mark_price"
    BITGET_MARK_PRICE = "bitget_mark_price"
    HYPERLIQUID_MARK_PRICE = "hyperliquid_mark_price"


class ConnectionState(Enum):
    """连接状态"""
    CONNECTING = "connecting"
    CONNECTED = "connected"
    DISCONNECTED = "disconnected"
    RECONNECTING = "reconnecting"
    FAILED = "failed"


@dataclass
class ConnectionInfo:
    """连接信息"""
    uid: str
    conn_type: ConnectionType
    state: ConnectionState = ConnectionState.DISCONNECTED
    created_at: float = field(default_factory=time.time)
    last_message_at: float = 0
    reconnect_count: int = 0
    error_count: int = 0
    last_error: Optional[str] = None


class WebSocketConnectionManager:
    """
    WebSocket 连接管理器
    
    单例模式，统一管理所有用户的 WebSocket 连接
    """
    
    _instance = None
    _lock = threading.Lock()
    
    # 连接数限制
    MAX_CONNECTIONS_PER_TYPE = {
        ConnectionType.BINANCE_USER_STREAM: 1500,
        ConnectionType.OKX_USER_STREAM: 1500,
        ConnectionType.BITGET_USER_STREAM: 1500,
        ConnectionType.HYPERLIQUID_USER_STREAM: 1500,
        # Mark Price 是共享的，只有 1 个连接
        ConnectionType.BINANCE_MARK_PRICE: 1,
        ConnectionType.OKX_MARK_PRICE: 1,
        ConnectionType.BITGET_MARK_PRICE: 1,
        ConnectionType.HYPERLIQUID_MARK_PRICE: 1,
    }
    
    # 告警阈值（达到限制的百分比）
    ALERT_THRESHOLD = 0.8
    
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
        
        # 连接注册表: {conn_id: ConnectionInfo}
        self._connections: Dict[str, ConnectionInfo] = {}
        self._conn_lock = threading.Lock()
        
        # 按类型索引: {ConnectionType: Set[conn_id]}
        self._type_index: Dict[ConnectionType, Set[str]] = {
            t: set() for t in ConnectionType
        }
        
        # 告警回调
        self._alert_callbacks: list = []
        
        # 健康检查线程
        self._health_check_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._health_check_interval = 60  # 60秒检查一次
        
        # 统计
        self._stats = {
            "total_registered": 0,
            "total_unregistered": 0,
            "alerts_triggered": 0,
        }
        
        self._initialized = True
        logger.info("[WSConnectionManager] 初始化完成")
    
    def start_health_check(self) -> None:
        """启动健康检查线程"""
        if self._health_check_thread and self._health_check_thread.is_alive():
            return
        
        self._stop_event.clear()
        self._health_check_thread = threading.Thread(
            target=self._health_check_loop,
            name="ws-health-check",
            daemon=True
        )
        self._health_check_thread.start()
        logger.info("[WSConnectionManager] 健康检查已启动")
    
    def stop_health_check(self) -> None:
        """停止健康检查线程"""
        self._stop_event.set()
        if self._health_check_thread:
            self._health_check_thread.join(timeout=5.0)
            self._health_check_thread = None
    
    def register_connection(
        self,
        uid: str,
        conn_type: ConnectionType,
        conn_id: Optional[str] = None
    ) -> str:
        """
        注册连接
        
        Args:
            uid: 用户ID
            conn_type: 连接类型
            conn_id: 连接ID（可选，默认自动生成）
        
        Returns:
            连接ID
        """
        if conn_id is None:
            conn_id = f"{uid}:{conn_type.value}:{int(time.time()*1000)}"
        
        with self._conn_lock:
            # 检查连接数限制
            current_count = len(self._type_index[conn_type])
            max_count = self.MAX_CONNECTIONS_PER_TYPE.get(conn_type, 1500)
            
            if current_count >= max_count:
                logger.warning(
                    f"[WSConnectionManager] 连接数已达上限: "
                    f"{conn_type.value} = {current_count}/{max_count}"
                )
                # 仍然允许注册，但触发告警
                self._trigger_alert(conn_type, current_count, max_count)
            elif current_count >= max_count * self.ALERT_THRESHOLD:
                # 接近上限，触发告警
                self._trigger_alert(conn_type, current_count, max_count)
            
            # 注册连接
            info = ConnectionInfo(
                uid=uid,
                conn_type=conn_type,
                state=ConnectionState.CONNECTING,
            )
            self._connections[conn_id] = info
            self._type_index[conn_type].add(conn_id)
            self._stats["total_registered"] += 1
        
        logger.debug(f"[WSConnectionManager] 注册连接: {conn_id}")
        return conn_id
    
    def unregister_connection(self, conn_id: str) -> None:
        """注销连接"""
        with self._conn_lock:
            if conn_id not in self._connections:
                return
            
            info = self._connections.pop(conn_id)
            self._type_index[info.conn_type].discard(conn_id)
            self._stats["total_unregistered"] += 1
        
        logger.debug(f"[WSConnectionManager] 注销连接: {conn_id}")
    
    def update_state(
        self,
        conn_id: str,
        state: ConnectionState,
        error: Optional[str] = None
    ) -> None:
        """更新连接状态"""
        with self._conn_lock:
            if conn_id not in self._connections:
                return
            
            info = self._connections[conn_id]
            info.state = state
            
            if state == ConnectionState.CONNECTED:
                info.last_message_at = time.time()
            elif state == ConnectionState.RECONNECTING:
                info.reconnect_count += 1
            elif state == ConnectionState.FAILED:
                info.error_count += 1
                info.last_error = error
    
    def record_message(self, conn_id: str) -> None:
        """记录收到消息"""
        with self._conn_lock:
            if conn_id in self._connections:
                self._connections[conn_id].last_message_at = time.time()
    
    def add_alert_callback(self, callback: Callable) -> None:
        """添加告警回调"""
        self._alert_callbacks.append(callback)
    
    def get_stats(self) -> dict:
        """获取统计信息"""
        with self._conn_lock:
            type_counts = {
                t.value: len(ids) for t, ids in self._type_index.items()
            }
            
            state_counts = {}
            for info in self._connections.values():
                state = info.state.value
                state_counts[state] = state_counts.get(state, 0) + 1
            
            return {
                "total_connections": len(self._connections),
                "by_type": type_counts,
                "by_state": state_counts,
                "stats": self._stats.copy(),
            }
    
    def get_connections_by_type(self, conn_type: ConnectionType) -> list:
        """获取指定类型的所有连接"""
        with self._conn_lock:
            conn_ids = list(self._type_index[conn_type])
            return [
                {
                    "conn_id": cid,
                    "uid": self._connections[cid].uid,
                    "state": self._connections[cid].state.value,
                    "last_message_at": self._connections[cid].last_message_at,
                }
                for cid in conn_ids if cid in self._connections
            ]
    
    def get_unhealthy_connections(self, timeout_s: float = 300) -> list:
        """获取不健康的连接（超过指定时间未收到消息）"""
        now = time.time()
        unhealthy = []
        
        with self._conn_lock:
            for conn_id, info in self._connections.items():
                if info.state == ConnectionState.CONNECTED:
                    if info.last_message_at > 0:
                        idle_time = now - info.last_message_at
                        if idle_time > timeout_s:
                            unhealthy.append({
                                "conn_id": conn_id,
                                "uid": info.uid,
                                "type": info.conn_type.value,
                                "idle_seconds": idle_time,
                            })
        
        return unhealthy
    
    def _trigger_alert(
        self,
        conn_type: ConnectionType,
        current: int,
        max_count: int
    ) -> None:
        """触发告警"""
        self._stats["alerts_triggered"] += 1
        
        alert_msg = (
            f"WebSocket 连接数告警: {conn_type.value} "
            f"= {current}/{max_count} ({current/max_count*100:.1f}%)"
        )
        logger.warning(f"[WSConnectionManager] {alert_msg}")
        
        # 调用告警回调
        for callback in self._alert_callbacks:
            try:
                callback(conn_type, current, max_count, alert_msg)
            except Exception as e:
                logger.error(f"[WSConnectionManager] 告警回调失败: {e}")
    
    def _health_check_loop(self) -> None:
        """健康检查循环"""
        while not self._stop_event.is_set():
            try:
                # 检查不健康的连接
                unhealthy = self.get_unhealthy_connections(timeout_s=300)
                if unhealthy:
                    logger.warning(
                        f"[WSConnectionManager] 发现 {len(unhealthy)} 个不健康连接"
                    )
                    for conn in unhealthy[:5]:  # 只记录前 5 个
                        logger.warning(
                            f"  - {conn['uid']}:{conn['type']} "
                            f"idle={conn['idle_seconds']:.0f}s"
                        )
                
                # 记录统计
                stats = self.get_stats()
                logger.info(
                    f"[WSConnectionManager] 连接统计: "
                    f"total={stats['total_connections']}, "
                    f"by_state={stats['by_state']}"
                )
                
            except Exception as e:
                logger.error(f"[WSConnectionManager] 健康检查错误: {e}")
            
            self._stop_event.wait(self._health_check_interval)


# ========== 全局单例 ==========

_manager: Optional[WebSocketConnectionManager] = None
_manager_lock = threading.Lock()


def get_ws_connection_manager() -> WebSocketConnectionManager:
    """获取 WebSocket 连接管理器单例"""
    global _manager
    
    with _manager_lock:
        if _manager is None:
            _manager = WebSocketConnectionManager()
        return _manager
