"""
AI 决策历史数据库访问层

将 AI 决策数据存储到 MySQL，支持高效分页查询
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


class AIDecisionDB:
    """
    AI 决策历史数据库访问类
    
    提供高效的分页查询，替代 Redis 中的大 JSON blob
    """
    
    _instance = None
    _singleton_lock = threading.Lock()  # P0 Fix: 单例锁保护
    
    def __new__(cls, *args, **kwargs):
        """单例模式 - 双重检查锁定"""
        if cls._instance is None:
            with cls._singleton_lock:
                # 双重检查，防止多线程竞争
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance
    
    def __init__(self, db_url: str = None):
        """
        Args:
            db_url: 数据库连接字符串
                - MySQL: "mysql+pymysql://user:pass@host/dbname"
        
        P0 Fix: 使用共享数据库引擎，避免连接池碎片化
        """
        if self._initialized:
            return
            
        self.db_url = db_url or os.getenv(
            "DATABASE_URL",
            "sqlite:///users.db"
        )
        
        if not HAS_SQLALCHEMY:
            logger.error("SQLAlchemy not installed, AIDecisionDB will not work")
            self._initialized = True
            return
        
        # P0 Fix: 使用共享数据库引擎
        from core.shared_db_engine import get_shared_engine, get_shared_session
        self.engine = get_shared_engine(self.db_url)
        self.Session = get_shared_session()
        
        self._initialized = True
        
        # 自动创建表
        self._ensure_table_exists()
        
        logger.info(f"AIDecisionDB initialized (using shared engine)")
    
    def _ensure_table_exists(self):
        """确保 ai_decisions 表存在"""
        try:
            if "sqlite" in self.db_url:
                create_sql = """
                    CREATE TABLE IF NOT EXISTS ai_decisions (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        uid VARCHAR(64) NOT NULL,
                        timestamp REAL NOT NULL,
                        request_data TEXT,
                        response_data TEXT,
                        signals_count INTEGER DEFAULT 0,
                        total_tokens INTEGER DEFAULT 0,
                        prompt_tokens INTEGER DEFAULT 0,
                        completion_tokens INTEGER DEFAULT 0,
                        response_time_ms INTEGER DEFAULT 0,
                        http_status INTEGER DEFAULT 200,
                        llm_model VARCHAR(128),
                        is_grouped INTEGER DEFAULT 0,
                        group_count INTEGER DEFAULT 1,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                    )
                """
                index_sql = [
                    "CREATE INDEX IF NOT EXISTS idx_ai_decisions_uid_timestamp ON ai_decisions (uid, timestamp DESC)",
                    "CREATE INDEX IF NOT EXISTS idx_ai_decisions_timestamp ON ai_decisions (timestamp DESC)"
                ]
            else:
                # MySQL
                # 注意：使用 LONGTEXT 而非 JSON，因为 MySQL JSON 列会自动按字母排序 key，
                # 导致存储后读取的 JSON key 顺序与原始不一致。LONGTEXT 保持原始字节顺序。
                create_sql = """
                    CREATE TABLE IF NOT EXISTS ai_decisions (
                        id BIGINT AUTO_INCREMENT PRIMARY KEY,
                        uid VARCHAR(64) NOT NULL,
                        timestamp DOUBLE NOT NULL,
                        request_data LONGTEXT,
                        response_data LONGTEXT,
                        signals_count INT DEFAULT 0,
                        total_tokens INT DEFAULT 0,
                        prompt_tokens INT DEFAULT 0,
                        completion_tokens INT DEFAULT 0,
                        response_time_ms INT DEFAULT 0,
                        http_status INT DEFAULT 200,
                        llm_model VARCHAR(128),
                        is_grouped TINYINT DEFAULT 0,
                        group_count INT DEFAULT 1,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                        INDEX idx_uid_timestamp (uid, timestamp DESC),
                        INDEX idx_timestamp (timestamp DESC)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                """
                index_sql = []  # MySQL 索引在建表时创建
            
            with self._get_connection() as conn:
                conn.execute(text(create_sql))
                for idx_sql in index_sql:
                    try:
                        conn.execute(text(idx_sql))
                    except Exception:
                        pass  # 索引可能已存在
                conn.commit()
                logger.info("ai_decisions table ensured")
        except Exception as e:
            logger.error(f"Failed to create ai_decisions table: {e}")
    
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
    
    def add_decision(
        self,
        uid: str,
        request_data: Dict,
        response_data: Dict,
        timestamp: float = None
    ) -> Optional[int]:
        """
        添加 AI 决策记录
        
        Args:
            uid: 用户 ID
            request_data: 投喂给 LLM 的数据
            response_data: LLM 返回的数据
            timestamp: 时间戳（默认当前时间）
            
        Returns:
            插入的记录 ID，失败返回 None
        """
        if not HAS_SQLALCHEMY:
            return None
            
        if timestamp is None:
            timestamp = time.time()
        
        # 处理 None 数据
        if request_data is None:
            request_data = {}
        if response_data is None:
            response_data = {}
        
        # 提取统计信息
        signals = response_data.get("signals", []) or []
        usage = response_data.get("usage", {}) or {}
        
        # 检查是否是分组格式
        is_grouped = "groups" in response_data
        group_count = len(response_data.get("groups", [])) if is_grouped else 1
        
        # 如果是分组格式，合并统计
        if is_grouped:
            total_signals = 0
            total_tokens = 0
            prompt_tokens = 0
            completion_tokens = 0
            response_time_ms = 0
            llm_model = None
            http_status = 200
            
            for g in response_data.get("groups", []):
                g_resp = g.get("response", {})
                total_signals += len(g_resp.get("signals", []))
                g_usage = g_resp.get("usage", {})
                total_tokens += int(g_usage.get("total_tokens") or 0)
                prompt_tokens += int(g_usage.get("prompt_tokens") or 0)
                completion_tokens += int(g_usage.get("completion_tokens") or 0)
                response_time_ms += int(g_resp.get("response_time_ms") or 0)
                if llm_model is None:
                    llm_model = g_usage.get("model")
                if g_resp.get("http_status"):
                    http_status = g_resp.get("http_status")
            
            signals_count = total_signals
        else:
            signals_count = len(signals)
            total_tokens = int(usage.get("total_tokens") or 0)
            prompt_tokens = int(usage.get("prompt_tokens") or 0)
            completion_tokens = int(usage.get("completion_tokens") or 0)
            response_time_ms = int(response_data.get("response_time_ms") or 0)
            llm_model = usage.get("model")
            http_status = response_data.get("http_status", 200)
        
        try:
            import orjson
            request_json = orjson.dumps(request_data).decode('utf-8')
            response_json = orjson.dumps(response_data).decode('utf-8')
        except Exception:
            request_json = json.dumps(request_data, ensure_ascii=False)
            response_json = json.dumps(response_data, ensure_ascii=False)
        
        sql = text("""
            INSERT INTO ai_decisions (
                uid, timestamp, request_data, response_data,
                signals_count, total_tokens, prompt_tokens, completion_tokens,
                response_time_ms, http_status, llm_model, is_grouped, group_count
            ) VALUES (
                :uid, :timestamp, :request_data, :response_data,
                :signals_count, :total_tokens, :prompt_tokens, :completion_tokens,
                :response_time_ms, :http_status, :llm_model, :is_grouped, :group_count
            )
        """)
        
        try:
            with self._get_connection() as conn:
                result = conn.execute(sql, {
                    "uid": uid,
                    "timestamp": timestamp,
                    "request_data": request_json,
                    "response_data": response_json,
                    "signals_count": signals_count,
                    "total_tokens": total_tokens,
                    "prompt_tokens": prompt_tokens,
                    "completion_tokens": completion_tokens,
                    "response_time_ms": response_time_ms,
                    "http_status": http_status or 200,
                    "llm_model": llm_model,
                    "is_grouped": 1 if is_grouped else 0,
                    "group_count": group_count,
                })
                conn.commit()
                return result.lastrowid
        except Exception as e:
            logger.error(f"Failed to add AI decision for {uid}: {e}")
            return None
    
    def get_decisions_paginated(
        self,
        uid: str,
        offset: int = 0,
        limit: int = 50
    ) -> Tuple[List[Dict], int]:
        """
        分页获取 AI 决策历史
        
        Args:
            uid: 用户 ID
            offset: 偏移量
            limit: 每页数量
            
        Returns:
            (records, total) - 记录列表和总数
        """
        if not HAS_SQLALCHEMY:
            return [], 0
        
        try:
            with self._get_connection() as conn:
                # 获取总数
                count_sql = text("SELECT COUNT(*) FROM ai_decisions WHERE uid = :uid")
                total = conn.execute(count_sql, {"uid": uid}).scalar() or 0
                
                if total == 0 or total <= offset:
                    return [], total
                
                # 获取分页数据
                data_sql = text("""
                    SELECT timestamp, request_data, response_data
                    FROM ai_decisions
                    WHERE uid = :uid
                    ORDER BY timestamp DESC
                    LIMIT :limit OFFSET :offset
                """)
                
                rows = conn.execute(data_sql, {
                    "uid": uid,
                    "limit": limit,
                    "offset": offset
                }).fetchall()
                
                # 解析结果
                records = []
                for row in rows:
                    try:
                        request_data = json.loads(row[1]) if row[1] else {}
                        response_data = json.loads(row[2]) if row[2] else {}
                        records.append({
                            "request": request_data,
                            "response": response_data,
                            "timestamp": row[0]
                        })
                    except (json.JSONDecodeError, TypeError) as e:
                        logger.warning(f"Failed to parse decision record: {e}")
                        continue
                
                return records, total
                
        except Exception as e:
            logger.error(f"Failed to get AI decisions for {uid}: {e}")
            return [], 0
    
    def get_recent_decisions(
        self,
        uid: str,
        limit: int = 200,
        lookback_hours: float = None
    ) -> List[Dict]:
        """
        获取最近的 AI 决策（用于 decision_feedback）
        
        Args:
            uid: 用户 ID
            limit: 最大数量
            lookback_hours: 回看小时数（可选）
            
        Returns:
            决策记录列表
        """
        if not HAS_SQLALCHEMY:
            return []
        
        try:
            with self._get_connection() as conn:
                if lookback_hours:
                    cutoff_ts = time.time() - (lookback_hours * 3600)
                    sql = text("""
                        SELECT id, timestamp, request_data, response_data
                        FROM ai_decisions
                        WHERE uid = :uid AND timestamp >= :cutoff
                        ORDER BY timestamp DESC
                        LIMIT :limit
                    """)
                    rows = conn.execute(sql, {
                        "uid": uid,
                        "cutoff": cutoff_ts,
                        "limit": limit
                    }).fetchall()
                else:
                    sql = text("""
                        SELECT id, timestamp, request_data, response_data
                        FROM ai_decisions
                        WHERE uid = :uid
                        ORDER BY timestamp DESC
                        LIMIT :limit
                    """)
                    rows = conn.execute(sql, {
                        "uid": uid,
                        "limit": limit
                    }).fetchall()
                
                records = []
                for row in rows:
                    try:
                        request_data = json.loads(row[2]) if row[2] else {}
                        response_data = json.loads(row[3]) if row[3] else {}
                        records.append({
                            "id": row[0],  # AI 决策 ID
                            "request": request_data,
                            "response": response_data,
                            "timestamp": row[1]
                        })
                    except (json.JSONDecodeError, TypeError):
                        continue
                
                return records
                
        except Exception as e:
            logger.error(f"Failed to get recent decisions for {uid}: {e}")
            return []
    
    def get_stats(self, uid: str, days: int = 30) -> Dict[str, Any]:
        """
        获取用户 AI 决策统计
        
        Args:
            uid: 用户 ID
            days: 统计天数
            
        Returns:
            统计信息
        """
        if not HAS_SQLALCHEMY:
            return {}
        
        cutoff_ts = time.time() - (days * 24 * 3600)
        
        try:
            with self._get_connection() as conn:
                sql = text("""
                    SELECT 
                        COUNT(*) as total_decisions,
                        SUM(signals_count) as total_signals,
                        SUM(total_tokens) as total_tokens,
                        SUM(response_time_ms) as total_response_time,
                        AVG(response_time_ms) as avg_response_time,
                        MIN(timestamp) as first_decision,
                        MAX(timestamp) as last_decision
                    FROM ai_decisions
                    WHERE uid = :uid AND timestamp >= :cutoff
                """)
                
                row = conn.execute(sql, {"uid": uid, "cutoff": cutoff_ts}).fetchone()
                
                if row:
                    return {
                        "total_decisions": row[0] or 0,
                        "total_signals": row[1] or 0,
                        "total_tokens": row[2] or 0,
                        "total_response_time_ms": row[3] or 0,
                        "avg_response_time_ms": round(row[4] or 0, 2),
                        "first_decision_ts": row[5],
                        "last_decision_ts": row[6],
                        "period_days": days
                    }
                return {}
                
        except Exception as e:
            logger.error(f"Failed to get AI decision stats for {uid}: {e}")
            return {}
    
    def get_decision_by_id(self, decision_id: int) -> Optional[Dict]:
        """
        通过 ID 获取单条 AI 决策记录
        
        Args:
            decision_id: 决策记录 ID
            
        Returns:
            决策记录，包含 request 和 response 数据
        """
        if not HAS_SQLALCHEMY:
            return None
        
        try:
            with self._get_connection() as conn:
                sql = text("""
                    SELECT id, uid, timestamp, request_data, response_data
                    FROM ai_decisions
                    WHERE id = :id
                """)
                row = conn.execute(sql, {"id": decision_id}).fetchone()
                
                if row:
                    try:
                        request_data = json.loads(row[3]) if row[3] else {}
                        response_data = json.loads(row[4]) if row[4] else {}
                        return {
                            "id": row[0],
                            "uid": row[1],
                            "timestamp": row[2],
                            "request": request_data,
                            "response": response_data
                        }
                    except (json.JSONDecodeError, TypeError):
                        return None
                return None
        except Exception as e:
            logger.error(f"Failed to get AI decision by id {decision_id}: {e}")
            return None
    
    def get_decisions_by_ids(self, decision_ids: List[int], uid: str = None) -> Dict[int, Dict]:
        """
        批量通过 ID 获取 AI 决策记录
        
        Args:
            decision_ids: 决策记录 ID 列表
            uid: 用户 ID（可选，用于安全验证，确保不会跨用户查询）
            
        Returns:
            {decision_id: decision_record} 映射
        """
        if not HAS_SQLALCHEMY or not decision_ids:
            return {}
        
        try:
            with self._get_connection() as conn:
                # 使用 IN 查询批量获取
                placeholders = ", ".join([f":id_{i}" for i in range(len(decision_ids))])
                
                # 如果提供了 uid，添加用户过滤条件（安全验证）
                if uid:
                    sql = text(f"""
                        SELECT id, uid, timestamp, request_data, response_data
                        FROM ai_decisions
                        WHERE id IN ({placeholders}) AND uid = :uid
                    """)
                    params = {f"id_{i}": did for i, did in enumerate(decision_ids)}
                    params["uid"] = uid
                else:
                    sql = text(f"""
                        SELECT id, uid, timestamp, request_data, response_data
                        FROM ai_decisions
                        WHERE id IN ({placeholders})
                    """)
                    params = {f"id_{i}": did for i, did in enumerate(decision_ids)}
                
                rows = conn.execute(sql, params).fetchall()
                
                result = {}
                for row in rows:
                    try:
                        request_data = json.loads(row[3]) if row[3] else {}
                        response_data = json.loads(row[4]) if row[4] else {}
                        result[row[0]] = {
                            "id": row[0],
                            "uid": row[1],
                            "timestamp": row[2],
                            "request": request_data,
                            "response": response_data
                        }
                    except (json.JSONDecodeError, TypeError):
                        continue
                return result
        except Exception as e:
            logger.error(f"Failed to get AI decisions by ids: {e}")
            return {}
    
    def get_user_record_count(self, uid: str) -> int:
        """获取用户的记录数"""
        if not HAS_SQLALCHEMY:
            return 0
        
        try:
            with self._get_connection() as conn:
                sql = text("SELECT COUNT(*) FROM ai_decisions WHERE uid = :uid")
                result = conn.execute(sql, {"uid": uid}).scalar()
                return result or 0
        except Exception as e:
            logger.error(f"Failed to get record count for {uid}: {e}")
            return 0
    
    def delete_user_decisions(self, uid: str) -> int:
        """
        删除用户所有 AI 决策记录
        
        Args:
            uid: 用户 ID
            
        Returns:
            删除的记录数
        """
        if not HAS_SQLALCHEMY:
            return 0
        
        try:
            with self._get_connection() as conn:
                sql = text("DELETE FROM ai_decisions WHERE uid = :uid")
                result = conn.execute(sql, {"uid": uid})
                conn.commit()
                deleted = result.rowcount
                logger.info(f"Deleted {deleted} AI decisions for {uid}")
                return deleted
        except Exception as e:
            logger.error(f"Failed to delete AI decisions for {uid}: {e}")
            return 0
    
    def get_decisions_summary_paginated(
        self,
        uid: str,
        offset: int = 0,
        limit: int = 50
    ) -> Tuple[List[Dict], int]:
        """
        分页获取 AI 决策摘要（不含完整 request/response，用于列表展示）
        
        相比 get_decisions_paginated，此方法：
        - 不返回完整的 request_data 和 response_data
        - 只返回摘要字段，数据量减少 90%+
        - 适合列表展示，详情按需加载
        
        Args:
            uid: 用户 ID
            offset: 偏移量
            limit: 每页数量
            
        Returns:
            (records, total) - 摘要记录列表和总数
        """
        if not HAS_SQLALCHEMY:
            return [], 0
        
        try:
            with self._get_connection() as conn:
                # 获取总数
                count_sql = text("SELECT COUNT(*) FROM ai_decisions WHERE uid = :uid")
                total = conn.execute(count_sql, {"uid": uid}).scalar() or 0
                
                if total == 0 or total <= offset:
                    return [], total
                
                # 只获取摘要字段，不获取完整 JSON
                data_sql = text("""
                    SELECT id, timestamp, signals_count, total_tokens, prompt_tokens, 
                           completion_tokens, response_time_ms, http_status, llm_model,
                           is_grouped, group_count
                    FROM ai_decisions
                    WHERE uid = :uid
                    ORDER BY timestamp DESC
                    LIMIT :limit OFFSET :offset
                """)
                
                rows = conn.execute(data_sql, {
                    "uid": uid,
                    "limit": limit,
                    "offset": offset
                }).fetchall()
                
                records = []
                for row in rows:
                    records.append({
                        "id": row[0],
                        "timestamp": row[1],
                        "signals_count": row[2] or 0,
                        "total_tokens": row[3] or 0,
                        "prompt_tokens": row[4] or 0,
                        "completion_tokens": row[5] or 0,
                        "response_time_ms": row[6] or 0,
                        "http_status": row[7] or 200,
                        "llm_model": row[8],
                        "is_grouped": bool(row[9]),
                        "group_count": row[10] or 1,
                    })
                
                return records, total
                
        except Exception as e:
            logger.error(f"Failed to get AI decision summaries for {uid}: {e}")
            return [], 0
    
    def get_decision_by_id(self, uid: str, decision_id: int) -> Optional[Dict]:
        """
        根据 ID 获取单条 AI 决策详情（含完整 request/response）
        
        Args:
            uid: 用户 ID（用于权限验证）
            decision_id: 决策记录 ID
            
        Returns:
            完整的决策记录，包含 request 和 response
        """
        if not HAS_SQLALCHEMY:
            return None
        
        try:
            with self._get_connection() as conn:
                sql = text("""
                    SELECT timestamp, request_data, response_data
                    FROM ai_decisions
                    WHERE id = :id AND uid = :uid
                """)
                
                row = conn.execute(sql, {"id": decision_id, "uid": uid}).fetchone()
                
                if not row:
                    return None
                
                request_data = json.loads(row[1]) if row[1] else {}
                response_data = json.loads(row[2]) if row[2] else {}
                
                return {
                    "id": decision_id,
                    "timestamp": row[0],
                    "request": request_data,
                    "response": response_data,
                }
                
        except Exception as e:
            logger.error(f"Failed to get AI decision {decision_id} for {uid}: {e}")
            return None


# 全局单例
_ai_decision_db: Optional[AIDecisionDB] = None


def get_ai_decision_db() -> AIDecisionDB:
    """获取 AIDecisionDB 单例"""
    global _ai_decision_db
    if _ai_decision_db is None:
        _ai_decision_db = AIDecisionDB()
    return _ai_decision_db
