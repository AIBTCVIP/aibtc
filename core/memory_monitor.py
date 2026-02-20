# memory_monitor.py
"""
内存监控模块 - 1000+ 用户规模优化

功能：
1. 定期监控内存使用情况
2. 内存告警（超过阈值时）
3. 内存统计导出
4. 自动触发 GC

使用方式：
    from core.memory_monitor import start_memory_monitor, get_memory_stats
    
    # 启动监控
    start_memory_monitor()
    
    # 获取统计
    stats = get_memory_stats()
"""

import gc
import logging
import os
import threading
import time
from dataclasses import dataclass
from typing import Callable, Dict, List, Optional

logger = logging.getLogger(__name__)

# 尝试导入 psutil（可选依赖）
try:
    import psutil
    HAS_PSUTIL = True
except ImportError:
    HAS_PSUTIL = False
    logger.warning("[MemoryMonitor] psutil 未安装，部分功能不可用")


@dataclass
class MemoryStats:
    """内存统计"""
    timestamp: float
    rss_mb: float  # 常驻内存 (MB)
    vms_mb: float  # 虚拟内存 (MB)
    percent: float  # 内存使用百分比
    gc_count: tuple  # GC 计数 (gen0, gen1, gen2)
    thread_count: int  # 线程数


class MemoryMonitor:
    """
    内存监控器
    
    单例模式，定期监控内存使用情况
    """
    
    _instance = None
    _lock = threading.Lock()
    
    # 默认配置
    DEFAULT_INTERVAL = 60  # 监控间隔（秒）
    DEFAULT_WARNING_THRESHOLD = 0.7  # 70% 内存使用告警
    DEFAULT_CRITICAL_THRESHOLD = 0.85  # 85% 内存使用严重告警
    DEFAULT_GC_THRESHOLD = 0.8  # 80% 时触发 GC
    
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
        
        # 配置
        self._interval = self.DEFAULT_INTERVAL
        self._warning_threshold = self.DEFAULT_WARNING_THRESHOLD
        self._critical_threshold = self.DEFAULT_CRITICAL_THRESHOLD
        self._gc_threshold = self.DEFAULT_GC_THRESHOLD
        
        # 监控线程
        self._monitor_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        
        # 历史记录
        self._history: List[MemoryStats] = []
        self._max_history = 60  # 保留最近 60 条记录
        
        # 告警回调
        self._alert_callbacks: List[Callable] = []
        
        # 统计
        self._stats = {
            "total_checks": 0,
            "warnings_triggered": 0,
            "criticals_triggered": 0,
            "gc_triggered": 0,
        }
        
        self._initialized = True
        logger.info("[MemoryMonitor] 初始化完成")
    
    def start(self) -> None:
        """启动监控"""
        if self._monitor_thread and self._monitor_thread.is_alive():
            return
        
        self._stop_event.clear()
        self._monitor_thread = threading.Thread(
            target=self._monitor_loop,
            name="memory-monitor",
            daemon=True
        )
        self._monitor_thread.start()
        logger.info("[MemoryMonitor] 已启动")
    
    def stop(self) -> None:
        """停止监控"""
        self._stop_event.set()
        if self._monitor_thread:
            self._monitor_thread.join(timeout=5.0)
            self._monitor_thread = None
        logger.info("[MemoryMonitor] 已停止")
    
    def add_alert_callback(self, callback: Callable) -> None:
        """添加告警回调"""
        self._alert_callbacks.append(callback)
    
    def get_current_stats(self) -> Optional[MemoryStats]:
        """获取当前内存统计"""
        try:
            if HAS_PSUTIL:
                process = psutil.Process()
                mem_info = process.memory_info()
                mem_percent = process.memory_percent()
                
                return MemoryStats(
                    timestamp=time.time(),
                    rss_mb=mem_info.rss / (1024 * 1024),
                    vms_mb=mem_info.vms / (1024 * 1024),
                    percent=mem_percent,
                    gc_count=gc.get_count(),
                    thread_count=threading.active_count(),
                )
            else:
                # 没有 psutil，使用基本信息
                return MemoryStats(
                    timestamp=time.time(),
                    rss_mb=0,
                    vms_mb=0,
                    percent=0,
                    gc_count=gc.get_count(),
                    thread_count=threading.active_count(),
                )
        except Exception as e:
            logger.error(f"[MemoryMonitor] 获取内存统计失败: {e}")
            return None
    
    def get_stats(self) -> dict:
        """获取监控统计"""
        current = self.get_current_stats()
        
        return {
            "current": {
                "rss_mb": current.rss_mb if current else 0,
                "vms_mb": current.vms_mb if current else 0,
                "percent": current.percent if current else 0,
                "thread_count": current.thread_count if current else 0,
            } if current else None,
            "history_count": len(self._history),
            "stats": self._stats.copy(),
            "thresholds": {
                "warning": self._warning_threshold,
                "critical": self._critical_threshold,
                "gc": self._gc_threshold,
            },
        }
    
    def get_history(self, limit: int = 10) -> List[dict]:
        """获取历史记录"""
        return [
            {
                "timestamp": s.timestamp,
                "rss_mb": s.rss_mb,
                "percent": s.percent,
                "thread_count": s.thread_count,
            }
            for s in self._history[-limit:]
        ]
    
    def force_gc(self) -> dict:
        """强制执行 GC"""
        before = gc.get_count()
        collected = gc.collect()
        after = gc.get_count()
        
        logger.info(f"[MemoryMonitor] 强制 GC: collected={collected}, before={before}, after={after}")
        
        return {
            "collected": collected,
            "before": before,
            "after": after,
        }
    
    # ========== 内部方法 ==========
    
    def _monitor_loop(self) -> None:
        """监控循环"""
        while not self._stop_event.is_set():
            try:
                stats = self.get_current_stats()
                if stats:
                    self._process_stats(stats)
                
                self._stats["total_checks"] += 1
                
            except Exception as e:
                logger.error(f"[MemoryMonitor] 监控循环错误: {e}")
            
            self._stop_event.wait(self._interval)
    
    def _process_stats(self, stats: MemoryStats) -> None:
        """处理统计数据"""
        # 保存历史
        self._history.append(stats)
        if len(self._history) > self._max_history:
            self._history.pop(0)
        
        # 检查阈值
        if HAS_PSUTIL:
            percent = stats.percent / 100.0  # psutil 返回的是百分比数值
            
            if percent >= self._critical_threshold:
                self._trigger_alert("critical", stats)
                self._stats["criticals_triggered"] += 1
            elif percent >= self._warning_threshold:
                self._trigger_alert("warning", stats)
                self._stats["warnings_triggered"] += 1
            
            # 自动 GC
            if percent >= self._gc_threshold:
                self._auto_gc()
    
    def _trigger_alert(self, level: str, stats: MemoryStats) -> None:
        """触发告警"""
        msg = (
            f"[MemoryMonitor] {level.upper()} - "
            f"RSS: {stats.rss_mb:.1f}MB, "
            f"Percent: {stats.percent:.1f}%, "
            f"Threads: {stats.thread_count}"
        )
        
        if level == "critical":
            logger.error(msg)
        else:
            logger.warning(msg)
        
        # 调用回调
        for callback in self._alert_callbacks:
            try:
                callback(level, stats)
            except Exception as e:
                logger.error(f"[MemoryMonitor] 告警回调失败: {e}")
    
    def _auto_gc(self) -> None:
        """自动 GC"""
        collected = gc.collect()
        self._stats["gc_triggered"] += 1
        logger.info(f"[MemoryMonitor] 自动 GC: collected={collected}")


# ========== 全局单例 ==========

_monitor: Optional[MemoryMonitor] = None
_monitor_lock = threading.Lock()


def get_memory_monitor() -> MemoryMonitor:
    """获取内存监控器单例"""
    global _monitor
    
    with _monitor_lock:
        if _monitor is None:
            _monitor = MemoryMonitor()
        return _monitor


def start_memory_monitor(interval: int = 60) -> None:
    """启动内存监控"""
    monitor = get_memory_monitor()
    monitor._interval = interval
    monitor.start()


def stop_memory_monitor() -> None:
    """停止内存监控"""
    monitor = get_memory_monitor()
    monitor.stop()


def get_memory_stats() -> dict:
    """获取内存统计"""
    return get_memory_monitor().get_stats()


def force_gc() -> dict:
    """强制 GC"""
    return get_memory_monitor().force_gc()
