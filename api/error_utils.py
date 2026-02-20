"""
安全错误处理工具

P2 Fix: 防止 HTTP 响应泄露内部错误信息
- 记录完整错误到日志
- 返回通用错误消息给用户
- 生成唯一错误 ID 便于排查
"""
import logging
import secrets
import time
from typing import Optional

logger = logging.getLogger(__name__)


def safe_error_response(
    e: Exception,
    operation: str = "操作",
    log_level: str = "error",
    include_error_id: bool = True
) -> str:
    """
    生成安全的错误响应消息
    
    Args:
        e: 异常对象
        operation: 操作描述（如 "登录", "注册", "获取数据"）
        log_level: 日志级别 ("error", "warning", "info")
        include_error_id: 是否包含错误 ID
    
    Returns:
        安全的错误消息字符串
    """
    # 生成唯一错误 ID
    error_id = f"E{int(time.time())}{secrets.token_hex(2).upper()}"
    
    # 记录完整错误到日志
    log_func = getattr(logger, log_level, logger.error)
    log_func(f"[{error_id}] {operation}失败: {type(e).__name__}: {e}")
    
    # 返回安全的错误消息
    if include_error_id:
        return f"{operation}失败 (错误ID: {error_id})"
    return f"{operation}失败"


def safe_error_detail(
    e: Exception,
    operation: str = "操作"
) -> str:
    """
    生成安全的 HTTPException detail
    
    简化版本，用于 HTTPException(status_code=500, detail=...)
    """
    return safe_error_response(e, operation, include_error_id=True)


# 预定义的操作名称映射
OPERATION_NAMES = {
    "register": "注册",
    "login": "登录",
    "reset_password": "重置密码",
    "update": "更新",
    "delete": "删除",
    "fetch": "获取数据",
    "connect": "连接",
    "audit": "审计",
    "export": "导出",
    "import": "导入",
    "sync": "同步",
    "validate": "验证",
}
