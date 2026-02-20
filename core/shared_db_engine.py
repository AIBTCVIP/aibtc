# core/shared_db_engine.py
"""
共享数据库引擎模块

P0 Fix: 解决连接池碎片化问题
- 原问题：5个DB类各自创建连接池，总连接数可能达190+
- 解决方案：创建共享引擎，所有DB类复用同一个连接池

使用方式：
    from core.shared_db_engine import get_shared_engine, get_shared_session

    # 获取共享引擎
    engine = get_shared_engine()
    
    # 获取 Session 类
    Session = get_shared_session()
"""

import os
import logging
import threading
from typing import Optional

logger = logging.getLogger(__name__)

# 全局共享引擎
_shared_engine = None
_shared_session = None
_engine_lock = threading.Lock()


def get_shared_engine(db_url: str = None):
    """
    获取共享数据库引擎（单例）
    
    Args:
        db_url: 数据库连接字符串（仅首次调用时有效）
        
    Returns:
        SQLAlchemy Engine 实例
    """
    global _shared_engine
    
    if _shared_engine is not None:
        return _shared_engine
    
    with _engine_lock:
        if _shared_engine is not None:
            return _shared_engine
        
        try:
            from sqlalchemy import create_engine
            from sqlalchemy.pool import QueuePool
        except ImportError:
            logger.error("SQLAlchemy not installed")
            return None
        
        # 仅支持 MySQL，必须配置 DATABASE_URL 环境变量
        db_url = db_url or os.getenv("DATABASE_URL")
        
        if not db_url:
            logger.error("[SharedDB] DATABASE_URL 环境变量未配置")
            raise ValueError("DATABASE_URL 环境变量未配置，请在 .env 文件中设置 MySQL 连接字符串")
        
        # MySQL: 使用优化的共享连接池
        # P0 Fix: 合并后的连接池配置
        # 原来 5 个 DB 类总计: pool_size=70, max_overflow=120
        # 现在共享: pool_size=30, max_overflow=50 (足够且更高效)
        _shared_engine = create_engine(
            db_url,
            poolclass=QueuePool,
            pool_pre_ping=True,
            pool_size=30,           # 基础连接数（原来总计70）
            max_overflow=50,        # 允许溢出连接数（原来总计120）
            pool_timeout=60,        # 等待连接超时（秒）
            pool_recycle=1800,      # 连接回收时间（30分钟）
        )
        # 隐藏密码
        safe_url = db_url.split('@')[-1] if '@' in db_url else db_url
        logger.info(f"[SharedDB] MySQL 引擎已创建: {safe_url}, pool_size=30, max_overflow=50")
        
        return _shared_engine


def get_shared_session():
    """
    获取共享 Session 类
    
    Returns:
        SQLAlchemy sessionmaker 实例
    """
    global _shared_session
    
    if _shared_session is not None:
        return _shared_session
    
    with _engine_lock:
        if _shared_session is not None:
            return _shared_session
        
        engine = get_shared_engine()
        if engine is None:
            return None
        
        try:
            from sqlalchemy.orm import sessionmaker
            _shared_session = sessionmaker(bind=engine)
            logger.info("[SharedDB] Session 类已创建")
            return _shared_session
        except ImportError:
            logger.error("SQLAlchemy not installed")
            return None


def get_engine_stats() -> dict:
    """
    获取连接池统计信息
    
    Returns:
        包含连接池状态的字典
    """
    if _shared_engine is None:
        return {"status": "not_initialized"}
    
    pool = _shared_engine.pool
    
    try:
        return {
            "status": "running",
            "pool_size": pool.size(),
            "checked_in": pool.checkedin(),
            "checked_out": pool.checkedout(),
            "overflow": pool.overflow(),
            "invalid": pool.invalidatedcount() if hasattr(pool, 'invalidatedcount') else 0,
        }
    except Exception as e:
        return {"status": "error", "error": str(e)}


def close_shared_engine():
    """
    关闭共享引擎（应用关闭时调用）
    """
    global _shared_engine, _shared_session
    
    with _engine_lock:
        if _shared_engine is not None:
            try:
                _shared_engine.dispose()
                logger.info("[SharedDB] 共享引擎已关闭")
            except Exception as e:
                logger.warning(f"[SharedDB] 关闭引擎失败: {e}")
            finally:
                _shared_engine = None
                _shared_session = None
