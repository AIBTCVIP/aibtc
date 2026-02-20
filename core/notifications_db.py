"""
通知历史数据库访问层

将通知数据存储到 MySQL，支持高效分页查询和已读状态管理
"""

import os
import json
import time
import logging
import threading
from typing import Optional, List, Dict, Any, Tuple
from contextlib import contextmanager

try:
    from sqlalchemy import create_engine, text
    from sqlalchemy.orm import sessionmaker
    from sqlalchemy.pool import NullPool
    HAS_SQLALCHEMY = True
except ImportError:
    HAS_SQLALCHEMY = False

logger = logging.getLogger(__name__)


class NotificationsDB:
    """
    通知历史数据库访问类
    
    提供高效的分页查询和已读状态管理
    """
    
    _instance = None
    _singleton_lock = threading.Lock()
    
    def __new__(cls, *args, **kwargs):
        """单例模式（线程安全）"""
        if cls._instance is None:
            with cls._singleton_lock:  # 双重检查锁定
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance
    
    def __init__(self, db_url: str = None):
        """
        Args:
            db_url: 数据库连接字符串
        
        P0 Fix: 使用共享数据库引擎，避免连接池碎片化
        """
        if self._initialized:
            return
            
        self.db_url = db_url or os.getenv(
            "DATABASE_URL",
            "sqlite:///users.db"
        )
        
        if not HAS_SQLALCHEMY:
            logger.error("SQLAlchemy not installed, NotificationsDB will not work")
            self._initialized = True
            return
        
        # P0 Fix: 使用共享数据库引擎
        from core.shared_db_engine import get_shared_engine, get_shared_session
        self.engine = get_shared_engine(self.db_url)
        self.Session = get_shared_session()
        
        self._initialized = True
        
        # 自动创建表
        self._ensure_table_exists()
        
        logger.info(f"NotificationsDB initialized: {self.db_url.split('@')[-1] if '@' in self.db_url else self.db_url}")
    
    def _ensure_table_exists(self):
        """确保 notifications 表存在"""
        try:
            if "sqlite" in self.db_url:
                create_sql = """
                    CREATE TABLE IF NOT EXISTS notifications (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        uid VARCHAR(64) NOT NULL,
                        type VARCHAR(50) NOT NULL,
                        title VARCHAR(255),
                        message TEXT,
                        data TEXT,
                        is_read INTEGER DEFAULT 0,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                    )
                """
                index_sql = [
                    "CREATE INDEX IF NOT EXISTS idx_notifications_uid_created ON notifications (uid, created_at DESC)",
                    "CREATE INDEX IF NOT EXISTS idx_notifications_uid_unread ON notifications (uid, is_read, created_at DESC)",
                    "CREATE INDEX IF NOT EXISTS idx_notifications_type ON notifications (type)",
                ]
            else:
                # MySQL
                create_sql = """
                    CREATE TABLE IF NOT EXISTS notifications (
                        id BIGINT AUTO_INCREMENT PRIMARY KEY,
                        uid VARCHAR(64) NOT NULL,
                        type VARCHAR(50) NOT NULL,
                        title VARCHAR(255),
                        message TEXT,
                        data LONGTEXT,
                        is_read TINYINT DEFAULT 0,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                        INDEX idx_uid_created (uid, created_at DESC),
                        INDEX idx_uid_unread (uid, is_read, created_at DESC),
                        INDEX idx_type (type)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                """
                index_sql = []
            
            with self._get_connection() as conn:
                conn.execute(text(create_sql))
                for idx_sql in index_sql:
                    try:
                        conn.execute(text(idx_sql))
                    except Exception:
                        pass
                conn.commit()
                logger.info("notifications table ensured")
        except Exception as e:
            logger.error(f"Failed to create notifications table: {e}")
    
    @contextmanager
    def _get_connection(self):
        """
        获取数据库连接
        
        P0 Fix: 添加异常时的回滚处理
        注意：调用方仍需显式调用 conn.commit()
        """
        conn = self.engine.connect()
        try:
            yield conn
        except Exception:
            # 尝试回滚任何未提交的事务
            try:
                conn.rollback()
            except Exception:
                pass
            raise
        finally:
            conn.close()
    
    def add_notification(
        self,
        uid: str,
        notification_type: str,
        title: str,
        message: str = None,
        data: Dict = None
    ) -> Optional[int]:
        """
        添加通知
        
        Args:
            uid: 用户 ID
            notification_type: 通知类型 (trade, signal, system, error, etc.)
            title: 通知标题
            message: 通知内容
            data: 附加数据
            
        Returns:
            插入的记录 ID
        """
        if not HAS_SQLALCHEMY:
            return None
        
        data_json = None
        if data:
            try:
                import orjson
                data_json = orjson.dumps(data).decode('utf-8')
            except Exception:
                data_json = json.dumps(data, ensure_ascii=False, default=str)
        
        sql = text("""
            INSERT INTO notifications (uid, type, title, message, data)
            VALUES (:uid, :type, :title, :message, :data)
        """)
        
        try:
            with self._get_connection() as conn:
                result = conn.execute(sql, {
                    "uid": uid,
                    "type": notification_type,
                    "title": title,
                    "message": message,
                    "data": data_json,
                })
                conn.commit()
                return result.lastrowid
        except Exception as e:
            logger.error(f"Failed to add notification for {uid}: {e}")
            return None
    
    def get_notifications_paginated(
        self,
        uid: str,
        offset: int = 0,
        limit: int = 50,
        notification_type: str = None,
        unread_only: bool = False
    ) -> Tuple[List[Dict], int]:
        """
        分页获取通知
        
        Args:
            uid: 用户 ID
            offset: 偏移量
            limit: 每页数量
            notification_type: 通知类型筛选
            unread_only: 只获取未读
            
        Returns:
            (records, total)
        """
        if not HAS_SQLALCHEMY:
            return [], 0
        
        conditions = ["uid = :uid"]
        params = {"uid": uid, "limit": limit, "offset": offset}
        
        if notification_type:
            conditions.append("type = :type")
            params["type"] = notification_type
        
        if unread_only:
            conditions.append("is_read = 0")
        
        where_clause = " AND ".join(conditions)
        
        try:
            with self._get_connection() as conn:
                # 获取总数
                count_sql = text(f"SELECT COUNT(*) FROM notifications WHERE {where_clause}")
                total = conn.execute(count_sql, params).scalar() or 0
                
                if total == 0 or total <= offset:
                    return [], total
                
                # 获取分页数据
                data_sql = text(f"""
                    SELECT id, type, title, message, data, is_read, created_at
                    FROM notifications
                    WHERE {where_clause}
                    ORDER BY created_at DESC
                    LIMIT :limit OFFSET :offset
                """)
                
                rows = conn.execute(data_sql, params).fetchall()
                
                records = []
                for row in rows:
                    record = {
                        "id": row[0],
                        "type": row[1],
                        "title": row[2],
                        "message": row[3],
                        "data": json.loads(row[4]) if row[4] else None,
                        "is_read": bool(row[5]),
                        "created_at": str(row[6]) if row[6] else None,
                    }
                    records.append(record)
                
                return records, total
                
        except Exception as e:
            logger.error(f"Failed to get notifications for {uid}: {e}")
            return [], 0
    
    def mark_as_read(self, uid: str, notification_ids: List[int] = None) -> int:
        """
        标记通知为已读
        
        Args:
            uid: 用户 ID
            notification_ids: 通知 ID 列表，None 表示全部标记
            
        Returns:
            更新的记录数
        """
        if not HAS_SQLALCHEMY:
            return 0
        
        try:
            with self._get_connection() as conn:
                if notification_ids:
                    # 标记指定通知
                    placeholders = ",".join([f":id{i}" for i in range(len(notification_ids))])
                    sql = text(f"""
                        UPDATE notifications 
                        SET is_read = 1 
                        WHERE uid = :uid AND id IN ({placeholders})
                    """)
                    params = {"uid": uid}
                    for i, nid in enumerate(notification_ids):
                        params[f"id{i}"] = nid
                else:
                    # 标记全部
                    sql = text("UPDATE notifications SET is_read = 1 WHERE uid = :uid AND is_read = 0")
                    params = {"uid": uid}
                
                result = conn.execute(sql, params)
                conn.commit()
                return result.rowcount
        except Exception as e:
            logger.error(f"Failed to mark notifications as read for {uid}: {e}")
            return 0
    
    def get_unread_count(self, uid: str) -> int:
        """获取未读通知数量"""
        if not HAS_SQLALCHEMY:
            return 0
        
        try:
            with self._get_connection() as conn:
                sql = text("SELECT COUNT(*) FROM notifications WHERE uid = :uid AND is_read = 0")
                return conn.execute(sql, {"uid": uid}).scalar() or 0
        except Exception as e:
            logger.error(f"Failed to get unread count for {uid}: {e}")
            return 0
    
    def delete_notifications(
        self,
        uid: str,
        notification_ids: List[int] = None,
        older_than_days: int = None
    ) -> int:
        """
        删除通知
        
        Args:
            uid: 用户 ID
            notification_ids: 指定删除的通知 ID
            older_than_days: 删除 N 天前的通知
            
        Returns:
            删除的记录数
        """
        if not HAS_SQLALCHEMY:
            return 0
        
        try:
            with self._get_connection() as conn:
                if notification_ids:
                    placeholders = ",".join([f":id{i}" for i in range(len(notification_ids))])
                    sql = text(f"DELETE FROM notifications WHERE uid = :uid AND id IN ({placeholders})")
                    params = {"uid": uid}
                    for i, nid in enumerate(notification_ids):
                        params[f"id{i}"] = nid
                elif older_than_days:
                    if "sqlite" in self.db_url:
                        sql = text("""
                            DELETE FROM notifications 
                            WHERE uid = :uid AND created_at < datetime('now', :days || ' days')
                        """)
                        params = {"uid": uid, "days": f"-{older_than_days}"}
                    else:
                        sql = text("""
                            DELETE FROM notifications 
                            WHERE uid = :uid AND created_at < DATE_SUB(NOW(), INTERVAL :days DAY)
                        """)
                        params = {"uid": uid, "days": older_than_days}
                else:
                    sql = text("DELETE FROM notifications WHERE uid = :uid")
                    params = {"uid": uid}
                
                result = conn.execute(sql, params)
                conn.commit()
                deleted = result.rowcount
                logger.info(f"Deleted {deleted} notifications for {uid}")
                return deleted
        except Exception as e:
            logger.error(f"Failed to delete notifications for {uid}: {e}")
            return 0
    
    def get_notification_stats(self, uid: str) -> Dict[str, Any]:
        """获取通知统计"""
        if not HAS_SQLALCHEMY:
            return {}
        
        try:
            with self._get_connection() as conn:
                sql = text("""
                    SELECT 
                        COUNT(*) as total,
                        SUM(CASE WHEN is_read = 0 THEN 1 ELSE 0 END) as unread,
                        type,
                        COUNT(*) as type_count
                    FROM notifications
                    WHERE uid = :uid
                    GROUP BY type
                """)
                
                rows = conn.execute(sql, {"uid": uid}).fetchall()
                
                total = 0
                unread = 0
                by_type = {}
                
                for row in rows:
                    total += row[3]
                    unread += row[1] or 0
                    by_type[row[2]] = row[3]
                
                return {
                    "total": total,
                    "unread": unread,
                    "by_type": by_type
                }
        except Exception as e:
            logger.error(f"Failed to get notification stats for {uid}: {e}")
            return {}


# 全局单例
_notifications_db: Optional[NotificationsDB] = None


def get_notifications_db() -> NotificationsDB:
    """获取 NotificationsDB 单例"""
    global _notifications_db
    if _notifications_db is None:
        _notifications_db = NotificationsDB()
    return _notifications_db
