# core/logging_config.py
"""
统一日志配置模块 - 优化版

功能：
1. 集中管理所有日志配置
2. 按功能模块分离日志文件
3. 控制台只显示关键信息
4. 支持日志轮转（按大小）
5. 支持环境变量配置

日志文件分类：
- main.log      - 所有日志（完整记录）
- error.log     - 仅 ERROR 及以上级别
- startup.log   - 启动/关闭相关（lifecycle, __main__）
- scheduler.log - 调度器相关
- trading.log   - 交易执行相关
- api.log       - API 请求相关
- database.log  - 数据库操作相关
- llm.log       - LLM 调用相关
- websocket.log - WebSocket 连接相关
- cache.log     - 缓存相关

使用方式：
    from core.logging_config import setup_logging, get_logger
    setup_logging()
    logger = get_logger(__name__)

环境变量：
    LOG_LEVEL       - 日志级别 (DEBUG/INFO/WARNING/ERROR)，默认 INFO
    LOG_DIR         - 日志目录，默认 logs
    LOG_TO_FILE     - 是否输出到文件 (1/0)，默认 1
    LOG_TO_CONSOLE  - 是否输出到控制台 (1/0)，默认 1
    LOG_MAX_BYTES   - 单个日志文件最大字节数，默认 10MB
    LOG_BACKUP_COUNT - 保留的历史日志文件数，默认 5
    LOG_JSON        - 是否使用 JSON 格式 (1/0)，默认 0
    LOG_CONSOLE_LEVEL - 控制台日志级别，默认 INFO
"""

import os
import sys
import logging
import json
import threading
from datetime import datetime
from logging.handlers import RotatingFileHandler
from typing import Optional, Dict, Any, List

# ============================================================
# 配置常量（从环境变量读取）
# ============================================================

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
LOG_DIR = os.getenv("LOG_DIR", "logs")
LOG_TO_FILE = os.getenv("LOG_TO_FILE", "1") == "1"
LOG_TO_CONSOLE = os.getenv("LOG_TO_CONSOLE", "1") == "1"
LOG_MAX_BYTES = int(os.getenv("LOG_MAX_BYTES", 10 * 1024 * 1024))  # 10MB
LOG_BACKUP_COUNT = int(os.getenv("LOG_BACKUP_COUNT", 5))
LOG_JSON = os.getenv("LOG_JSON", "0") == "1"
LOG_CONSOLE_LEVEL = os.getenv("LOG_CONSOLE_LEVEL", "INFO").upper()

# ============================================================
# 日志文件配置 - 按功能模块分类
# ============================================================

LOG_FILES = {
    # 主日志 - 记录所有
    "main": {
        "filename": "main.log",
        "level": "DEBUG",
        "loggers": None,  # None = 所有 logger
    },
    # 错误日志 - 仅 ERROR 及以上
    "error": {
        "filename": "error.log",
        "level": "ERROR",
        "loggers": None,
    },
    # 启动/关闭日志
    "startup": {
        "filename": "startup.log",
        "level": "INFO",
        "loggers": [
            "__main__",
            "core.lifecycle",
        ],
    },
    # 调度器日志
    "scheduler": {
        "filename": "scheduler.log",
        "level": "DEBUG",
        "loggers": [
            "core.async_multi_user_scheduler",
            "core.multi_user_scheduler",
            "core.commission_service",
        ],
    },
    # ============================================================
    # 交易所日志 - 按功能细分
    # ============================================================
    # 交易执行日志（下单、平仓、杠杆设置等）
    "exchange_trading": {
        "filename": "exchange_trading.log",
        "level": "DEBUG",
        "loggers": [
            "exchanges.binance_exchange",
            "exchanges.okx_exchange",
            "exchanges.bitget_exchange",
            "exchanges.hyperliquid_exchange",
            "exchanges.binance.client",
            "exchanges.base",
            "trading.multi_exchange_trader",
            "trading.global_stop_loss_processor",
            "trading.stop_loss_manager",
            "trading.stop_loss_adapter",
            "trading.exchange_pool",
            "trading.utils",
            "core.exchange_context",
            "core.exchange_monitor",
            "core.rate_limiter",
        ],
    },
    # 交易所 WebSocket 日志（实时数据推送）
    "exchange_websocket": {
        "filename": "exchange_websocket.log",
        "level": "DEBUG",
        "loggers": [
            "exchanges.binance.websocket",
            "exchanges.okx.websocket",
            "exchanges.bitget.websocket",
            "exchanges.hyperliquid.websocket",
            "websocket",
        ],
    },
    # 持仓管理日志（持仓存储、审计）
    "exchange_position": {
        "filename": "exchange_position.log",
        "level": "DEBUG",
        "loggers": [
            "exchanges.binance.position_store",
            "exchanges.okx.position_store",
            "exchanges.bitget.position_store",
            "exchanges.hyperliquid.position_store",
            "exchanges.base_position_store",
            "exchanges.okx.position_auditor",
            "exchanges.bitget.position_auditor",
            "exchanges.hyperliquid.position_auditor",
            "cache.global_position_auditor",
        ],
    },
    # 周期存储日志（Mark Cycle 等）
    "exchange_cycle": {
        "filename": "exchange_cycle.log",
        "level": "DEBUG",
        "loggers": [
            "exchanges.okx.cycle_store",
            "exchanges.bitget.cycle_store",
            "exchanges.hyperliquid.cycle_store",
            "trading.global_mark_cycle_updater",
        ],
    },
    # ============================================================
    # 其他日志
    # ============================================================
    # API 请求日志
    "api": {
        "filename": "api.log",
        "level": "DEBUG",
        "loggers": [
            "api",
            "uvicorn",
            "fastapi",
        ],
    },
    # 数据库操作日志
    "database": {
        "filename": "database.log",
        "level": "DEBUG",
        "loggers": [
            "core.user_db",
            "core.referral_db",
            "core.notifications_db",
            "core.closed_trades_db",
            "core.ai_decision_db",
            "core.shared_db_engine",
            "core.async_redis",
            "core.redis_manager",
            "core.database",
        ],
    },
    # LLM 调用日志
    "llm": {
        "filename": "llm.log",
        "level": "DEBUG",
        "loggers": [
            "llm",
            "core.strategy_cache",
        ],
    },
    # 缓存日志
    "cache": {
        "filename": "cache.log",
        "level": "DEBUG",
        "loggers": [
            "core.local_cache",
            "core.binance_public_cache",
            "core.okx_public_cache",
            "core.bitget_public_cache",
            "core.symbol_availability",
            "core.symbol_precision",
            "cache",
        ],
    },
    # 用户上下文日志
    "user": {
        "filename": "user.log",
        "level": "DEBUG",
        "loggers": [
            "core.user_context",
            "core.async_user_context",
            "user",
        ],
    },
}

# 控制台显示的关键 logger（其他降级为 WARNING）
CONSOLE_KEY_LOGGERS = [
    "__main__",
    "core.lifecycle",
    "core.async_multi_user_scheduler",
    "core.multi_user_scheduler",
    "api.web",
    "trading.global_stop_loss_processor",
    "trading.multi_exchange_trader",
]

# ============================================================
# 自定义 Formatter
# ============================================================

class StandardFormatter(logging.Formatter):
    """标准日志格式化器（用于文件）"""
    
    def __init__(self):
        super().__init__(
            fmt="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )


class CompactFormatter(logging.Formatter):
    """紧凑日志格式化器（用于控制台）"""
    
    # 日志级别颜色（ANSI）
    COLORS = {
        'DEBUG': '\033[36m',     # 青色
        'INFO': '\033[32m',      # 绿色
        'WARNING': '\033[33m',   # 黄色
        'ERROR': '\033[31m',     # 红色
        'CRITICAL': '\033[35m',  # 紫色
    }
    RESET = '\033[0m'
    
    def __init__(self, use_color: bool = True):
        super().__init__(
            fmt="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            datefmt="%H:%M:%S"
        )
        # Windows 控制台颜色支持
        self.use_color = use_color
        if use_color and sys.platform == 'win32':
            try:
                import ctypes
                kernel32 = ctypes.windll.kernel32
                kernel32.SetConsoleMode(kernel32.GetStdHandle(-11), 7)
            except Exception:
                self.use_color = False
    
    def format(self, record: logging.LogRecord) -> str:
        # 缩短模块名（保留首尾）
        original_name = record.name
        if record.name and len(record.name) > 35:
            parts = record.name.split('.')
            if len(parts) > 2:
                record.name = f"{parts[0]}.{parts[-1]}"
        
        # 添加颜色
        original_levelname = record.levelname
        if self.use_color:
            color = self.COLORS.get(record.levelname, '')
            record.levelname = f"{color}{record.levelname}{self.RESET}"
        
        result = super().format(record)
        
        # 恢复原始值
        record.name = original_name
        record.levelname = original_levelname
        
        return result


class JSONFormatter(logging.Formatter):
    """JSON 格式化器（用于日志聚合）"""
    
    def format(self, record: logging.LogRecord) -> str:
        log_data = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }
        
        # 添加异常信息
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)
        
        # 添加额外字段
        if hasattr(record, 'uid'):
            log_data["uid"] = record.uid
        if hasattr(record, 'exchange'):
            log_data["exchange"] = record.exchange
        
        return json.dumps(log_data, ensure_ascii=False)


# ============================================================
# 自定义 Filter
# ============================================================

class LoggerNameFilter(logging.Filter):
    """按 logger 名称过滤（用于分类日志文件）"""
    
    def __init__(self, logger_names: List[str]):
        super().__init__()
        self.logger_names = logger_names
    
    def filter(self, record: logging.LogRecord) -> bool:
        if not self.logger_names:
            return True
        for name in self.logger_names:
            if record.name == name or record.name.startswith(name + "."):
                return True
        return False


class ConsoleFilter(logging.Filter):
    """控制台过滤器 - 只显示关键 logger 的 INFO，其他只显示 WARNING+"""
    
    def __init__(self, key_loggers: List[str]):
        super().__init__()
        self.key_loggers = key_loggers
    
    def filter(self, record: logging.LogRecord) -> bool:
        # WARNING 及以上总是显示
        if record.levelno >= logging.WARNING:
            return True
        
        # 检查是否是关键 logger
        for name in self.key_loggers:
            if record.name == name or record.name.startswith(name + "."):
                return True
        
        # 非关键 logger 的 INFO/DEBUG 不显示
        return False


class NoisyLoggerFilter(logging.Filter):
    """过滤噪音日志"""
    
    # 需要过滤的日志模式
    NOISY_PATTERNS = [
        # WebSocket 心跳
        ("websocket", "ping"),
        ("websocket", "pong"),
        ("websocket", "heartbeat"),
        # HTTP 连接池
        ("urllib3", "connection"),
        ("httpx", "connection"),
        ("httpcore", "connection"),
        # 常规状态检查
        ("core.lifecycle", "健康检查"),
    ]
    
    def filter(self, record: logging.LogRecord) -> bool:
        msg = record.getMessage().lower()
        for logger_prefix, pattern in self.NOISY_PATTERNS:
            if record.name.startswith(logger_prefix) and pattern in msg:
                # 将 INFO 降级为 DEBUG
                if record.levelno == logging.INFO:
                    record.levelno = logging.DEBUG
                    record.levelname = "DEBUG"
        return True


# ============================================================
# 主配置函数
# ============================================================

_logging_configured = False

def setup_logging(
    level: Optional[str] = None,
    log_dir: Optional[str] = None,
    to_file: Optional[bool] = None,
    to_console: Optional[bool] = None,
    console_level: Optional[str] = None,
) -> None:
    """
    配置日志系统
    
    Args:
        level: 文件日志级别，覆盖环境变量
        log_dir: 日志目录，覆盖环境变量
        to_file: 是否输出到文件，覆盖环境变量
        to_console: 是否输出到控制台，覆盖环境变量
        console_level: 控制台日志级别，覆盖环境变量
    """
    global _logging_configured
    
    if _logging_configured:
        return
    
    # 使用参数或环境变量
    _level = level or LOG_LEVEL
    _log_dir = log_dir or LOG_DIR
    _to_file = to_file if to_file is not None else LOG_TO_FILE
    _to_console = to_console if to_console is not None else LOG_TO_CONSOLE
    _console_level = console_level or LOG_CONSOLE_LEVEL
    
    # 创建日志目录
    if _to_file:
        os.makedirs(_log_dir, exist_ok=True)
    
    # 获取根 logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)  # 根 logger 设为 DEBUG，由 handler 控制实际级别
    
    # 清除已有的 handlers
    root_logger.handlers.clear()
    
    # 噪音过滤器
    noise_filter = NoisyLoggerFilter()
    
    # 1. 控制台 Handler
    if _to_console:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(getattr(logging, _console_level))
        console_handler.setFormatter(CompactFormatter(use_color=True))
        console_handler.addFilter(noise_filter)
        console_handler.addFilter(ConsoleFilter(CONSOLE_KEY_LOGGERS))
        root_logger.addHandler(console_handler)
    
    # 2. 文件 Handlers
    if _to_file:
        # 选择格式化器
        file_formatter = JSONFormatter() if LOG_JSON else StandardFormatter()
        
        for log_name, config in LOG_FILES.items():
            filepath = os.path.join(_log_dir, config["filename"])
            handler = RotatingFileHandler(
                filepath,
                maxBytes=LOG_MAX_BYTES,
                backupCount=LOG_BACKUP_COUNT,
                encoding='utf-8',
            )
            handler.setLevel(getattr(logging, config["level"]))
            handler.setFormatter(file_formatter)
            handler.addFilter(noise_filter)
            
            # 添加 logger 名称过滤器（除了 main 和 error）
            if config["loggers"]:
                handler.addFilter(LoggerNameFilter(config["loggers"]))
            
            root_logger.addHandler(handler)
    
    # 3. 降低第三方库日志级别
    _configure_third_party_loggers()
    
    _logging_configured = True
    
    # 记录日志系统启动
    logger = logging.getLogger(__name__)
    logger.info(f"日志系统已初始化: level={_level}, dir={_log_dir}, file={_to_file}, console={_to_console}")


def _configure_third_party_loggers():
    """配置第三方库的日志级别"""
    # 降低噪音库的日志级别
    noisy_loggers = [
        "urllib3",
        "httpx",
        "httpcore",
        "asyncio",
        "aiohttp",
        "websockets",
        "sqlalchemy.engine",
        "sqlalchemy.pool",
        "redis",
        "ccxt",
        "uvicorn.access",
        "uvicorn.error",
    ]
    
    for name in noisy_loggers:
        logging.getLogger(name).setLevel(logging.WARNING)


# ============================================================
# 辅助函数
# ============================================================

def get_logger(name: str) -> logging.Logger:
    """
    获取 logger（确保日志系统已初始化）
    
    用法：
        from core.logging_config import get_logger
        logger = get_logger(__name__)
    """
    if not _logging_configured:
        setup_logging()
    return logging.getLogger(name)


def get_user_logger(uid: str, exchange: str = None) -> logging.LoggerAdapter:
    """
    获取带用户上下文的 logger
    
    用法：
        logger = get_user_logger("abc123", "binance")
        logger.info("下单成功")  # 输出: [abc123][binance] 下单成功
    """
    base_logger = logging.getLogger("user")
    extra = {"uid": uid[:12] if len(uid) > 12 else uid}
    if exchange:
        extra["exchange"] = exchange
    
    return UserLoggerAdapter(base_logger, extra)


class UserLoggerAdapter(logging.LoggerAdapter):
    """用户日志适配器，自动添加 uid 前缀"""
    
    def process(self, msg, kwargs):
        uid = self.extra.get("uid", "")
        exchange = self.extra.get("exchange", "")
        
        if exchange:
            prefix = f"[{uid}][{exchange}]"
        else:
            prefix = f"[{uid}]"
        
        return f"{prefix} {msg}", kwargs


# ============================================================
# 日志统计
# ============================================================

class LogStats:
    """日志统计（用于监控，线程安全）"""
    
    _instance = None
    _instance_lock = threading.Lock()  # P4 Fix: 单例锁
    
    def __new__(cls):
        if cls._instance is None:
            with cls._instance_lock:
                if cls._instance is None:  # Double-check locking
                    cls._instance = super().__new__(cls)
                    cls._instance._counts = {
                        "DEBUG": 0,
                        "INFO": 0,
                        "WARNING": 0,
                        "ERROR": 0,
                        "CRITICAL": 0,
                    }
                    cls._instance._counts_lock = threading.Lock()  # P4 Fix: 计数锁
        return cls._instance
    
    def increment(self, level: str):
        if level in self._counts:
            with self._counts_lock:  # P4 Fix: 原子操作
                self._counts[level] += 1
    
    def get_stats(self) -> Dict[str, int]:
        with self._counts_lock:  # P4 Fix: 读取时也加锁
            return self._counts.copy()
    
    def reset(self):
        with self._counts_lock:  # P4 Fix: 重置时加锁
            for key in self._counts:
                self._counts[key] = 0


class StatsHandler(logging.Handler):
    """统计日志数量的 Handler"""
    
    def __init__(self):
        super().__init__()
        self.stats = LogStats()
    
    def emit(self, record: logging.LogRecord):
        self.stats.increment(record.levelname)


def enable_log_stats():
    """启用日志统计"""
    stats_handler = StatsHandler()
    stats_handler.setLevel(logging.DEBUG)
    logging.getLogger().addHandler(stats_handler)


def get_log_stats() -> Dict[str, int]:
    """获取日志统计"""
    return LogStats().get_stats()


# ============================================================
# 便捷函数
# ============================================================

def set_module_level(module_name: str, level: str):
    """
    动态设置某个模块的日志级别
    
    用法：
        set_module_level("trading", "DEBUG")
    """
    logging.getLogger(module_name).setLevel(getattr(logging, level.upper()))


def list_log_files() -> Dict[str, str]:
    """列出所有日志文件及其用途"""
    return {
        config["filename"]: f"{name} - {config['loggers'] or 'all'}"
        for name, config in LOG_FILES.items()
    }
