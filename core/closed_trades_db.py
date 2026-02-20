"""
已平仓交易数据库访问层

将已平仓交易数据存储到 MySQL，支持高效分页查询和统计分析
"""

import os
import json
import time
import logging
import threading
from typing import Optional, List, Dict, Any, Tuple
from contextlib import contextmanager
from decimal import Decimal

try:
    from sqlalchemy import create_engine, text
    from sqlalchemy.orm import sessionmaker
    from sqlalchemy.pool import NullPool
    HAS_SQLALCHEMY = True
except ImportError:
    HAS_SQLALCHEMY = False

logger = logging.getLogger(__name__)


class ClosedTradesDB:
    """
    已平仓交易数据库访问类
    
    提供高效的分页查询和统计分析，替代 Redis 中的大 JSON blob
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
            logger.error("SQLAlchemy not installed, ClosedTradesDB will not work")
            self._initialized = True
            return
        
        # P0 Fix: 使用共享数据库引擎
        from core.shared_db_engine import get_shared_engine, get_shared_session
        self.engine = get_shared_engine(self.db_url)
        self.Session = get_shared_session()
        
        self._initialized = True
        
        # 自动创建表
        self._ensure_table_exists()
        
        logger.info(f"ClosedTradesDB initialized (using shared engine)")
    
    def _ensure_table_exists(self):
        """确保 closed_trades 表存在"""
        try:
            if "sqlite" in self.db_url:
                create_sql = """
                    CREATE TABLE IF NOT EXISTS closed_trades (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        uid VARCHAR(64) NOT NULL,
                        exchange VARCHAR(20) NOT NULL,
                        cycle_id VARCHAR(128) NOT NULL,
                        symbol VARCHAR(30) NOT NULL,
                        side VARCHAR(10) NOT NULL,
                        entry_price REAL,
                        exit_price REAL,
                        quantity REAL,
                        realized_pnl REAL,
                        pnl_pct REAL,
                        fee_total REAL DEFAULT 0,
                        funding_total REAL DEFAULT 0,
                        open_time_ms BIGINT,
                        close_time_ms BIGINT,
                        duration_minutes INTEGER,
                        leverage INTEGER DEFAULT 1,
                        ai_decision_id VARCHAR(64),
                        raw_data TEXT,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(uid, exchange, cycle_id)
                    )
                """
                index_sql = [
                    "CREATE INDEX IF NOT EXISTS idx_closed_trades_uid_exchange ON closed_trades (uid, exchange)",
                    "CREATE INDEX IF NOT EXISTS idx_closed_trades_uid_close_time ON closed_trades (uid, close_time_ms DESC)",
                    "CREATE INDEX IF NOT EXISTS idx_closed_trades_symbol ON closed_trades (symbol)",
                    "CREATE INDEX IF NOT EXISTS idx_closed_trades_uid_pnl ON closed_trades (uid, realized_pnl)",
                ]
            else:
                # MySQL
                create_sql = """
                    CREATE TABLE IF NOT EXISTS closed_trades (
                        id BIGINT AUTO_INCREMENT PRIMARY KEY,
                        uid VARCHAR(64) NOT NULL,
                        exchange VARCHAR(20) NOT NULL,
                        cycle_id VARCHAR(128) NOT NULL,
                        symbol VARCHAR(30) NOT NULL,
                        side VARCHAR(10) NOT NULL,
                        entry_price DECIMAL(20, 8),
                        exit_price DECIMAL(20, 8),
                        quantity DECIMAL(20, 8),
                        realized_pnl DECIMAL(20, 8),
                        pnl_pct DECIMAL(10, 4),
                        fee_total DECIMAL(20, 8) DEFAULT 0,
                        funding_total DECIMAL(20, 8) DEFAULT 0,
                        open_time_ms BIGINT,
                        close_time_ms BIGINT,
                        duration_minutes INT,
                        leverage INT DEFAULT 1,
                        ai_decision_id VARCHAR(64),
                        raw_data LONGTEXT,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE KEY uk_cycle (uid, exchange, cycle_id),
                        INDEX idx_uid_exchange (uid, exchange),
                        INDEX idx_uid_close_time (uid, close_time_ms DESC),
                        INDEX idx_symbol (symbol),
                        INDEX idx_uid_pnl (uid, realized_pnl)
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
                logger.info("closed_trades table ensured")
        except Exception as e:
            logger.error(f"Failed to create closed_trades table: {e}")
    
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
    
    def add_trade(
        self,
        uid: str,
        exchange: str,
        trade_data: Dict[str, Any]
    ) -> Optional[int]:
        """
        添加已平仓交易记录
        
        Args:
            uid: 用户 ID
            exchange: 交易所
            trade_data: 交易数据（原始格式）
            
        Returns:
            插入的记录 ID，失败返回 None
        """
        if not HAS_SQLALCHEMY:
            return None
        
        # 解析交易数据
        cycle_id = trade_data.get("cycleId") or trade_data.get("cycle_id") or ""
        symbol = trade_data.get("symbol") or ""
        side = trade_data.get("side") or ""
        
        # 如果缺少 symbol 或 side，尝试从 field 或 cycle_id 解析
        if not symbol or not side:
            # 尝试从 field 解析 (格式: "BTCUSDT:LONG")
            field = trade_data.get("field", "")
            if field and ":" in field:
                parts = field.split(":")
                if len(parts) >= 2:
                    symbol = symbol or parts[0]
                    side = side or parts[1]
            
            # 如果还是没有，尝试从 cycle_id 解析 (格式: "BTCUSDT:LONG:1234567890")
            if (not symbol or not side) and cycle_id and ":" in cycle_id:
                parts = cycle_id.split(":")
                if len(parts) >= 2:
                    symbol = symbol or parts[0]
                    side = side or parts[1]
            
            if symbol or side:
                logger.info(f"[{uid}][{exchange}] Parsed missing fields from field/cycle_id: symbol={symbol}, side={side}")
        
        # 如果仍然缺少必填字段，记录警告但仍然尝试写入（保留原始数据）
        if not symbol or not side:
            logger.warning(
                f"[{uid}][{exchange}] Closed trade has missing fields: "
                f"symbol={symbol!r}, side={side!r}, cycle_id={cycle_id!r}. "
                f"Writing anyway to preserve data."
            )
        
        # 价格和数量 - 支持多种字段名格式
        entry_price = self._parse_decimal(
            trade_data.get("entryPrice") or 
            trade_data.get("avgOpenPrice") or
            trade_data.get("entry_price")
        )
        exit_price = self._parse_decimal(
            trade_data.get("exitPrice") or 
            trade_data.get("avgClosePrice") or
            trade_data.get("exit_price")
        )
        quantity = self._parse_decimal(
            trade_data.get("quantity") or 
            trade_data.get("positionAmt") or
            trade_data.get("openQty") or
            trade_data.get("closeQty")
        )
        
        # 盈亏
        realized_pnl = self._parse_decimal(
            trade_data.get("realizedPnlEst") or 
            trade_data.get("realizedPnl") or
            trade_data.get("netPnl")
        )
        
        # 计算 pnl_pct（如果没有直接提供）
        pnl_pct = self._parse_decimal(trade_data.get("pnlPct"))
        if pnl_pct is None and entry_price and quantity and realized_pnl:
            # pnl_pct = realized_pnl / (entry_price * quantity) * 100
            cost = entry_price * abs(quantity)
            if cost > 0:
                pnl_pct = round(realized_pnl / cost * 100, 4)
        
        # 费用
        fee_total = self._parse_decimal(trade_data.get("feeTotal", 0))
        funding_total = self._parse_decimal(trade_data.get("fundingTotal", 0))
        
        # 时间
        open_time_ms = int(trade_data.get("openTimeMs", 0) or 0)
        close_time_ms = int(trade_data.get("closeTimeMs", 0) or 0)
        
        # 计算持仓时长（分钟）
        duration_minutes = None
        if open_time_ms and close_time_ms:
            duration_minutes = int((close_time_ms - open_time_ms) / 60000)
        
        # 杠杆
        leverage = int(trade_data.get("leverage", 1) or 1)
        
        # AI 决策 ID（从 cycle 数据中获取）
        ai_decision_id = trade_data.get("aiDecisionId") or trade_data.get("ai_decision_id")
        
        # 原始数据
        try:
            import orjson
            raw_json = orjson.dumps(trade_data).decode('utf-8')
        except Exception:
            raw_json = json.dumps(trade_data, ensure_ascii=False, default=str)
        
        sql = text("""
            INSERT INTO closed_trades (
                uid, exchange, cycle_id, symbol, side,
                entry_price, exit_price, quantity, realized_pnl, pnl_pct,
                fee_total, funding_total, open_time_ms, close_time_ms,
                duration_minutes, leverage, ai_decision_id, raw_data
            ) VALUES (
                :uid, :exchange, :cycle_id, :symbol, :side,
                :entry_price, :exit_price, :quantity, :realized_pnl, :pnl_pct,
                :fee_total, :funding_total, :open_time_ms, :close_time_ms,
                :duration_minutes, :leverage, :ai_decision_id, :raw_data
            )
            ON DUPLICATE KEY UPDATE
                entry_price = VALUES(entry_price),
                exit_price = VALUES(exit_price),
                quantity = VALUES(quantity),
                realized_pnl = VALUES(realized_pnl),
                pnl_pct = VALUES(pnl_pct),
                fee_total = VALUES(fee_total),
                funding_total = VALUES(funding_total),
                close_time_ms = VALUES(close_time_ms),
                duration_minutes = VALUES(duration_minutes),
                ai_decision_id = COALESCE(VALUES(ai_decision_id), ai_decision_id),
                raw_data = VALUES(raw_data)
        """)
        
        try:
            with self._get_connection() as conn:
                result = conn.execute(sql, {
                    "uid": uid,
                    "exchange": exchange,
                    "cycle_id": cycle_id,
                    "symbol": symbol,
                    "side": side,
                    "entry_price": entry_price,
                    "exit_price": exit_price,
                    "quantity": quantity,
                    "realized_pnl": realized_pnl,
                    "pnl_pct": pnl_pct,
                    "fee_total": fee_total,
                    "funding_total": funding_total,
                    "open_time_ms": open_time_ms,
                    "close_time_ms": close_time_ms,
                    "duration_minutes": duration_minutes,
                    "leverage": leverage,
                    "ai_decision_id": ai_decision_id,
                    "raw_data": raw_json,
                })
                conn.commit()
                return result.lastrowid
        except Exception as e:
            logger.error(f"Failed to add closed trade for {uid}: {e}")
            return None
    
    def _parse_decimal(self, value) -> Optional[float]:
        """解析数值"""
        if value is None:
            return None
        try:
            if isinstance(value, (int, float)):
                return float(value)
            if isinstance(value, str):
                return float(value) if value else None
            if isinstance(value, Decimal):
                return float(value)
            return None
        except (ValueError, TypeError):
            return None
    
    def get_trades_paginated(
        self,
        uid: str,
        exchange: str = None,
        offset: int = 0,
        limit: int = 50,
        symbol: str = None,
        side: str = None,
        pnl_filter: str = None,  # "profit", "loss", "all"
        start_time_ms: int = None,
        end_time_ms: int = None,
        order_by: str = "close_time_ms",
        order_dir: str = "DESC"
    ) -> Tuple[List[Dict], int]:
        """
        分页获取已平仓交易
        
        Args:
            uid: 用户 ID
            exchange: 交易所（可选，None 表示所有）
            offset: 偏移量
            limit: 每页数量
            symbol: 币种筛选
            side: 方向筛选 (LONG/SHORT)
            pnl_filter: 盈亏筛选 (profit/loss/all)
            start_time_ms: 开始时间
            end_time_ms: 结束时间
            order_by: 排序字段
            order_dir: 排序方向
            
        Returns:
            (records, total) - 记录列表和总数
        """
        if not HAS_SQLALCHEMY:
            return [], 0
        
        # 构建 WHERE 条件
        conditions = ["uid = :uid"]
        params = {"uid": uid, "limit": limit, "offset": offset}
        
        if exchange:
            conditions.append("exchange = :exchange")
            params["exchange"] = exchange
        
        if symbol:
            conditions.append("symbol = :symbol")
            params["symbol"] = symbol
        
        if side:
            conditions.append("side = :side")
            params["side"] = side
        
        if pnl_filter == "profit":
            conditions.append("realized_pnl > 0")
        elif pnl_filter == "loss":
            conditions.append("realized_pnl < 0")
        
        if start_time_ms:
            conditions.append("close_time_ms >= :start_time")
            params["start_time"] = start_time_ms
        
        if end_time_ms:
            conditions.append("close_time_ms <= :end_time")
            params["end_time"] = end_time_ms
        
        where_clause = " AND ".join(conditions)
        
        # 验证排序字段
        valid_order_fields = ["close_time_ms", "realized_pnl", "pnl_pct", "symbol", "duration_minutes"]
        if order_by not in valid_order_fields:
            order_by = "close_time_ms"
        order_dir = "DESC" if order_dir.upper() == "DESC" else "ASC"
        
        try:
            with self._get_connection() as conn:
                # 获取总数
                count_sql = text(f"SELECT COUNT(*) FROM closed_trades WHERE {where_clause}")
                total = conn.execute(count_sql, params).scalar() or 0
                
                if total == 0 or total <= offset:
                    return [], total
                
                # 获取分页数据
                data_sql = text(f"""
                    SELECT id, uid, exchange, cycle_id, symbol, side,
                           entry_price, exit_price, quantity, realized_pnl, pnl_pct,
                           fee_total, funding_total, open_time_ms, close_time_ms,
                           duration_minutes, leverage, ai_decision_id, raw_data, created_at
                    FROM closed_trades
                    WHERE {where_clause}
                    ORDER BY {order_by} {order_dir}
                    LIMIT :limit OFFSET :offset
                """)
                
                rows = conn.execute(data_sql, params).fetchall()
                
                records = []
                for row in rows:
                    record = {
                        "id": row[0],
                        "uid": row[1],
                        "exchange": row[2],
                        "cycleId": row[3],
                        "symbol": row[4],
                        "side": row[5],
                        "entryPrice": str(row[6]) if row[6] else "0",
                        "exitPrice": str(row[7]) if row[7] else "0",
                        "quantity": str(row[8]) if row[8] else "0",
                        "realizedPnl": str(row[9]) if row[9] else "0",
                        "pnlPct": str(row[10]) if row[10] else "0",
                        "feeTotal": str(row[11]) if row[11] else "0",
                        "fundingTotal": str(row[12]) if row[12] else "0",
                        "openTimeMs": str(row[13]) if row[13] else "0",
                        "closeTimeMs": str(row[14]) if row[14] else "0",
                        "durationMinutes": row[15],
                        "leverage": row[16],
                        "aiDecisionId": row[17],  # AI 决策 ID
                    }
                    
                    # 从 raw_data 中提取额外字段（peakPnl, maxDrawdown 等）
                    raw_data = row[18]  # 索引调整
                    if raw_data:
                        try:
                            raw = json.loads(raw_data) if isinstance(raw_data, str) else raw_data
                            record["peakPnl"] = str(raw.get("peakPnl", "0"))
                            record["maxDrawdown"] = str(raw.get("maxDrawdown", "0"))
                            record["avgOpenPrice"] = str(raw.get("avgOpenPrice", record["entryPrice"]))
                            record["avgClosePrice"] = str(raw.get("avgClosePrice", record["exitPrice"]))
                            record["openQty"] = str(raw.get("openQty", record["quantity"]))
                        except (json.JSONDecodeError, TypeError):
                            record["peakPnl"] = "0"
                            record["maxDrawdown"] = "0"
                    else:
                        record["peakPnl"] = "0"
                        record["maxDrawdown"] = "0"
                    
                    records.append(record)
                
                return records, total
                
        except Exception as e:
            logger.error(f"Failed to get closed trades for {uid}: {e}")
            return [], 0
    
    def get_trade_by_cycle_id(
        self,
        uid: str,
        exchange: str,
        cycle_id: str
    ) -> Optional[Dict]:
        """
        根据 cycle_id 获取单条交易详情（含原始数据）
        """
        if not HAS_SQLALCHEMY:
            return None
        
        try:
            with self._get_connection() as conn:
                sql = text("""
                    SELECT raw_data FROM closed_trades
                    WHERE uid = :uid AND exchange = :exchange AND cycle_id = :cycle_id
                """)
                
                row = conn.execute(sql, {
                    "uid": uid,
                    "exchange": exchange,
                    "cycle_id": cycle_id
                }).fetchone()
                
                if not row:
                    return None
                
                return json.loads(row[0]) if row[0] else {}
                
        except Exception as e:
            logger.error(f"Failed to get trade {cycle_id} for {uid}: {e}")
            return None
    
    def get_statistics(
        self,
        uid: str,
        exchange: str = None,
        days: int = 30,
        symbol: str = None
    ) -> Dict[str, Any]:
        """
        获取交易统计
        
        Args:
            uid: 用户 ID
            exchange: 交易所（可选）
            days: 统计天数
            symbol: 币种筛选
            
        Returns:
            统计信息
        """
        if not HAS_SQLALCHEMY:
            return {}
        
        cutoff_ms = int((time.time() - days * 24 * 3600) * 1000)
        
        conditions = ["uid = :uid", "close_time_ms >= :cutoff"]
        params = {"uid": uid, "cutoff": cutoff_ms}
        
        if exchange:
            conditions.append("exchange = :exchange")
            params["exchange"] = exchange
        
        if symbol:
            conditions.append("symbol = :symbol")
            params["symbol"] = symbol
        
        where_clause = " AND ".join(conditions)
        
        try:
            with self._get_connection() as conn:
                sql = text(f"""
                    SELECT 
                        COUNT(*) as total_trades,
                        SUM(CASE WHEN realized_pnl > 0 THEN 1 ELSE 0 END) as winning_trades,
                        SUM(CASE WHEN realized_pnl < 0 THEN 1 ELSE 0 END) as losing_trades,
                        SUM(realized_pnl) as total_pnl,
                        SUM(CASE WHEN realized_pnl > 0 THEN realized_pnl ELSE 0 END) as total_profit,
                        SUM(CASE WHEN realized_pnl < 0 THEN realized_pnl ELSE 0 END) as total_loss,
                        AVG(realized_pnl) as avg_pnl,
                        AVG(CASE WHEN realized_pnl > 0 THEN realized_pnl ELSE NULL END) as avg_profit,
                        AVG(CASE WHEN realized_pnl < 0 THEN realized_pnl ELSE NULL END) as avg_loss,
                        MAX(realized_pnl) as max_profit,
                        MIN(realized_pnl) as max_loss,
                        AVG(duration_minutes) as avg_duration,
                        SUM(fee_total) as total_fees,
                        SUM(funding_total) as total_funding
                    FROM closed_trades
                    WHERE {where_clause}
                """)
                
                row = conn.execute(sql, params).fetchone()
                
                if not row or not row[0]:
                    return {
                        "total_trades": 0,
                        "winning_trades": 0,
                        "losing_trades": 0,
                        "win_rate": 0,
                        "total_pnl": 0,
                        "period_days": days
                    }
                
                total_trades = row[0] or 0
                winning_trades = row[1] or 0
                losing_trades = row[2] or 0
                
                win_rate = round(winning_trades / total_trades * 100, 2) if total_trades > 0 else 0
                
                # 计算盈亏比
                avg_profit = abs(row[7] or 0)
                avg_loss = abs(row[8] or 0)
                profit_factor = round(avg_profit / avg_loss, 2) if avg_loss > 0 else 0
                
                return {
                    "total_trades": total_trades,
                    "winning_trades": winning_trades,
                    "losing_trades": losing_trades,
                    "win_rate": win_rate,
                    "total_pnl": round(row[3] or 0, 2),
                    "total_profit": round(row[4] or 0, 2),
                    "total_loss": round(row[5] or 0, 2),
                    "avg_pnl": round(row[6] or 0, 2),
                    "avg_profit": round(row[7] or 0, 2),
                    "avg_loss": round(row[8] or 0, 2),
                    "max_profit": round(row[9] or 0, 2),
                    "max_loss": round(row[10] or 0, 2),
                    "profit_factor": profit_factor,
                    "avg_duration_minutes": round(row[11] or 0, 1),
                    "total_fees": round(row[12] or 0, 4),
                    "total_funding": round(row[13] or 0, 4),
                    "period_days": days,
                    "exchange": exchange,
                    "symbol": symbol
                }
                
        except Exception as e:
            logger.error(f"Failed to get statistics for {uid}: {e}")
            return {}
    
    def get_symbol_breakdown(
        self,
        uid: str,
        exchange: str = None,
        days: int = 30
    ) -> List[Dict]:
        """
        获取按币种分组的统计
        """
        if not HAS_SQLALCHEMY:
            return []
        
        cutoff_ms = int((time.time() - days * 24 * 3600) * 1000)
        
        conditions = ["uid = :uid", "close_time_ms >= :cutoff"]
        params = {"uid": uid, "cutoff": cutoff_ms}
        
        if exchange:
            conditions.append("exchange = :exchange")
            params["exchange"] = exchange
        
        where_clause = " AND ".join(conditions)
        
        try:
            with self._get_connection() as conn:
                sql = text(f"""
                    SELECT 
                        symbol,
                        COUNT(*) as trades,
                        SUM(CASE WHEN realized_pnl > 0 THEN 1 ELSE 0 END) as wins,
                        SUM(realized_pnl) as total_pnl,
                        AVG(realized_pnl) as avg_pnl
                    FROM closed_trades
                    WHERE {where_clause}
                    GROUP BY symbol
                    ORDER BY total_pnl DESC
                """)
                
                rows = conn.execute(sql, params).fetchall()
                
                results = []
                for row in rows:
                    trades = row[1] or 0
                    wins = row[2] or 0
                    results.append({
                        "symbol": row[0],
                        "trades": trades,
                        "wins": wins,
                        "win_rate": round(wins / trades * 100, 2) if trades > 0 else 0,
                        "total_pnl": round(row[3] or 0, 2),
                        "avg_pnl": round(row[4] or 0, 2)
                    })
                
                return results
                
        except Exception as e:
            logger.error(f"Failed to get symbol breakdown for {uid}: {e}")
            return []
    
    def get_daily_pnl(
        self,
        uid: str,
        exchange: str = None,
        days: int = 30
    ) -> List[Dict]:
        """
        获取每日盈亏统计
        """
        if not HAS_SQLALCHEMY:
            return []
        
        cutoff_ms = int((time.time() - days * 24 * 3600) * 1000)
        
        conditions = ["uid = :uid", "close_time_ms >= :cutoff"]
        params = {"uid": uid, "cutoff": cutoff_ms}
        
        if exchange:
            conditions.append("exchange = :exchange")
            params["exchange"] = exchange
        
        where_clause = " AND ".join(conditions)
        
        try:
            with self._get_connection() as conn:
                # MySQL 日期格式化
                if "sqlite" in self.db_url:
                    date_expr = "date(close_time_ms/1000, 'unixepoch')"
                else:
                    date_expr = "DATE(FROM_UNIXTIME(close_time_ms/1000))"
                
                sql = text(f"""
                    SELECT 
                        {date_expr} as trade_date,
                        COUNT(*) as trades,
                        SUM(CASE WHEN realized_pnl > 0 THEN 1 ELSE 0 END) as wins,
                        SUM(realized_pnl) as total_pnl
                    FROM closed_trades
                    WHERE {where_clause}
                    GROUP BY trade_date
                    ORDER BY trade_date DESC
                """)
                
                rows = conn.execute(sql, params).fetchall()
                
                results = []
                for row in rows:
                    trades = row[1] or 0
                    wins = row[2] or 0
                    results.append({
                        "date": str(row[0]),
                        "trades": trades,
                        "wins": wins,
                        "win_rate": round(wins / trades * 100, 2) if trades > 0 else 0,
                        "pnl": round(row[3] or 0, 2)
                    })
                
                return results
                
        except Exception as e:
            logger.error(f"Failed to get daily PnL for {uid}: {e}")
            return []
    
    def get_trades_as_dict(
        self,
        uid: str,
        exchange: str = None,
        limit: int = 500,
        add_exchange_field: bool = False
    ) -> Dict[str, Dict]:
        """
        获取交易记录，返回 Dict[cycle_id, trade_data] 格式
        
        兼容旧的 Redis 格式，用于 pf_compatibility.get_pf_closed_h()
        
        Args:
            uid: 用户 ID
            exchange: 交易所（可选，None 表示所有）
            limit: 最大返回数量
            add_exchange_field: 是否在每条记录中添加 exchange 字段
            
        Returns:
            {cycle_id: trade_data, ...}
        """
        if not HAS_SQLALCHEMY:
            return {}
        
        try:
            conditions = ["uid = :uid"]
            params = {"uid": uid, "limit": limit}
            
            if exchange:
                conditions.append("exchange = :exchange")
                params["exchange"] = exchange
            
            where_clause = " AND ".join(conditions)
            
            with self._get_connection() as conn:
                sql = text(f"""
                    SELECT cycle_id, exchange, raw_data FROM closed_trades
                    WHERE {where_clause}
                    ORDER BY close_time_ms DESC
                    LIMIT :limit
                """)
                
                rows = conn.execute(sql, params).fetchall()
                
                result = {}
                for row in rows:
                    cycle_id = row[0]
                    ex = row[1]
                    raw_data = row[2]
                    
                    if raw_data:
                        try:
                            trade_data = json.loads(raw_data) if isinstance(raw_data, str) else raw_data
                            if add_exchange_field:
                                trade_data["exchange"] = ex
                            result[cycle_id] = trade_data
                        except (json.JSONDecodeError, TypeError):
                            continue
                
                return result
                
        except Exception as e:
            logger.error(f"Failed to get trades as dict for {uid}: {e}")
            return {}
    
    def get_trades_by_exchange(
        self,
        uid: str,
        limit: int = 500
    ) -> Dict[str, Dict[str, Dict]]:
        """
        获取交易记录，按交易所分组
        
        用于替代 Redis 的 get_closed_trades_by_exchange()
        
        Args:
            uid: 用户 ID
            limit: 每个交易所的最大返回数量
            
        Returns:
            {"binance": {cycle_id: trade_data, ...}, "okx": {...}, ...}
        """
        if not HAS_SQLALCHEMY:
            return {}
        
        try:
            with self._get_connection() as conn:
                # 获取用户所有交易所的交易
                sql = text("""
                    SELECT cycle_id, exchange, raw_data FROM closed_trades
                    WHERE uid = :uid
                    ORDER BY close_time_ms DESC
                """)
                
                rows = conn.execute(sql, {"uid": uid}).fetchall()
                
                # 按交易所分组
                result: Dict[str, Dict[str, Dict]] = {}
                exchange_counts: Dict[str, int] = {}
                
                for row in rows:
                    cycle_id = row[0]
                    exchange = row[1]
                    raw_data = row[2]
                    
                    # 限制每个交易所的数量
                    if exchange not in exchange_counts:
                        exchange_counts[exchange] = 0
                    if exchange_counts[exchange] >= limit:
                        continue
                    
                    if raw_data:
                        try:
                            trade_data = json.loads(raw_data) if isinstance(raw_data, str) else raw_data
                            if exchange not in result:
                                result[exchange] = {}
                            result[exchange][cycle_id] = trade_data
                            exchange_counts[exchange] += 1
                        except (json.JSONDecodeError, TypeError):
                            continue
                
                return result
                
        except Exception as e:
            logger.error(f"Failed to get trades by exchange for {uid}: {e}")
            return {}
    
    def delete_user_trades(self, uid: str, exchange: str = None) -> int:
        """
        删除用户的交易记录
        
        Args:
            uid: 用户 ID
            exchange: 交易所（可选，None 表示所有）
            
        Returns:
            删除的记录数
        """
        if not HAS_SQLALCHEMY:
            return 0
        
        try:
            with self._get_connection() as conn:
                if exchange:
                    sql = text("DELETE FROM closed_trades WHERE uid = :uid AND exchange = :exchange")
                    result = conn.execute(sql, {"uid": uid, "exchange": exchange})
                else:
                    sql = text("DELETE FROM closed_trades WHERE uid = :uid")
                    result = conn.execute(sql, {"uid": uid})
                conn.commit()
                deleted = result.rowcount
                logger.info(f"Deleted {deleted} closed trades for {uid}" + (f" ({exchange})" if exchange else ""))
                return deleted
        except Exception as e:
            logger.error(f"Failed to delete closed trades for {uid}: {e}")
            return 0
    
    def get_user_trade_count(self, uid: str, exchange: str = None) -> int:
        """获取用户的交易记录数"""
        if not HAS_SQLALCHEMY:
            return 0
        
        try:
            with self._get_connection() as conn:
                if exchange:
                    sql = text("SELECT COUNT(*) FROM closed_trades WHERE uid = :uid AND exchange = :exchange")
                    result = conn.execute(sql, {"uid": uid, "exchange": exchange}).scalar()
                else:
                    sql = text("SELECT COUNT(*) FROM closed_trades WHERE uid = :uid")
                    result = conn.execute(sql, {"uid": uid}).scalar()
                return result or 0
        except Exception as e:
            logger.error(f"Failed to get trade count for {uid}: {e}")
            return 0
    
    def get_all_users_with_trades(self) -> List[str]:
        """
        获取所有有交易记录的用户 ID 列表
        
        用于排行榜对账时获取需要处理的用户列表
        
        Returns:
            用户 ID 列表
        """
        if not HAS_SQLALCHEMY:
            return []
        
        try:
            with self._get_connection() as conn:
                sql = text("SELECT DISTINCT uid FROM closed_trades")
                result = conn.execute(sql)
                return [row[0] for row in result.fetchall()]
        except Exception as e:
            logger.error(f"Failed to get all users with trades: {e}")
            return []
    
    def get_advanced_statistics(
        self,
        uid: str,
        start_date: str = None,
        end_date: str = None,
        exchanges: List[str] = None,
        symbols: List[str] = None,
        pnl_filter: str = "all",  # all, profit, loss
        side_filter: str = "all",  # all, long, short
        page: int = 1,
        page_size: int = 50
    ) -> Dict[str, Any]:
        """
        高级统计查询
        
        Args:
            uid: 用户 ID
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)
            exchanges: 交易所列表
            symbols: 币种列表
            pnl_filter: 盈亏筛选 (all/profit/loss)
            side_filter: 方向筛选 (all/long/short)
            page: 页码
            page_size: 每页数量
            
        Returns:
            {
                "summary": {...},  # 汇总统计
                "by_symbol": [...],  # 按币种分布
                "by_date": [...],  # 按日期分布
                "trades": [...],  # 交易列表
                "pagination": {...}  # 分页信息
            }
        """
        if not HAS_SQLALCHEMY:
            return {"error": "SQLAlchemy not available"}
        
        # 构建查询条件
        conditions = ["uid = :uid"]
        params = {"uid": uid}
        
        # 时间范围
        if start_date:
            start_ms = int(time.mktime(time.strptime(start_date, "%Y-%m-%d")) * 1000)
            conditions.append("close_time_ms >= :start_ms")
            params["start_ms"] = start_ms
        
        if end_date:
            # 结束日期包含当天，所以加一天
            end_ms = int((time.mktime(time.strptime(end_date, "%Y-%m-%d")) + 86400) * 1000)
            conditions.append("close_time_ms < :end_ms")
            params["end_ms"] = end_ms
        
        # 交易所筛选
        if exchanges and len(exchanges) > 0:
            placeholders = ", ".join([f":ex_{i}" for i in range(len(exchanges))])
            conditions.append(f"exchange IN ({placeholders})")
            for i, ex in enumerate(exchanges):
                params[f"ex_{i}"] = ex
        
        # 币种筛选
        if symbols and len(symbols) > 0:
            placeholders = ", ".join([f":sym_{i}" for i in range(len(symbols))])
            conditions.append(f"symbol IN ({placeholders})")
            for i, sym in enumerate(symbols):
                params[f"sym_{i}"] = sym
        
        # 盈亏筛选 (net_pnl = realized_pnl - fee_total + funding_total)
        if pnl_filter == "profit":
            conditions.append("(realized_pnl - fee_total + funding_total) > 0")
        elif pnl_filter == "loss":
            conditions.append("(realized_pnl - fee_total + funding_total) < 0")
        
        # 方向筛选
        if side_filter == "long":
            conditions.append("side = 'LONG'")
        elif side_filter == "short":
            conditions.append("side = 'SHORT'")
        
        where_clause = " AND ".join(conditions)
        
        try:
            with self._get_connection() as conn:
                # 1. 汇总统计 (net_pnl = realized_pnl - fee_total + funding_total)
                summary_sql = text(f"""
                    SELECT 
                        COUNT(*) as total_trades,
                        SUM(CASE WHEN (realized_pnl - fee_total + funding_total) > 0 THEN 1 ELSE 0 END) as winning_trades,
                        SUM(CASE WHEN (realized_pnl - fee_total + funding_total) < 0 THEN 1 ELSE 0 END) as losing_trades,
                        SUM(realized_pnl - fee_total + funding_total) as total_pnl,
                        SUM(CASE WHEN (realized_pnl - fee_total + funding_total) > 0 THEN (realized_pnl - fee_total + funding_total) ELSE 0 END) as total_profit,
                        SUM(CASE WHEN (realized_pnl - fee_total + funding_total) < 0 THEN ABS(realized_pnl - fee_total + funding_total) ELSE 0 END) as total_loss,
                        AVG(realized_pnl - fee_total + funding_total) as avg_pnl,
                        MAX(realized_pnl - fee_total + funding_total) as max_profit,
                        MIN(realized_pnl - fee_total + funding_total) as max_loss,
                        AVG(duration_minutes) as avg_duration,
                        SUM(fee_total) as total_fees,
                        SUM(funding_total) as total_funding
                    FROM closed_trades
                    WHERE {where_clause}
                """)
                
                row = conn.execute(summary_sql, params).fetchone()
                
                total_trades = row[0] or 0
                winning_trades = row[1] or 0
                losing_trades = row[2] or 0
                total_profit = float(row[4] or 0)
                total_loss = float(row[5] or 0)
                
                summary = {
                    "total_trades": total_trades,
                    "winning_trades": winning_trades,
                    "losing_trades": losing_trades,
                    "win_rate": round(winning_trades / total_trades * 100, 2) if total_trades > 0 else 0,
                    "total_pnl": round(float(row[3] or 0), 2),
                    "total_profit": round(total_profit, 2),
                    "total_loss": round(total_loss, 2),
                    "profit_factor": round(total_profit / total_loss, 2) if total_loss > 0 else 0,
                    "avg_pnl": round(float(row[6] or 0), 2),
                    "max_profit": round(float(row[7] or 0), 2),
                    "max_loss": round(float(row[8] or 0), 2),
                    "avg_duration_minutes": round(float(row[9] or 0), 1),
                    "total_fees": round(float(row[10] or 0), 4),
                    "total_funding": round(float(row[11] or 0), 4),
                }
                
                # 2. 按币种分布
                symbol_sql = text(f"""
                    SELECT 
                        symbol,
                        COUNT(*) as trades,
                        SUM(CASE WHEN (realized_pnl - fee_total + funding_total) > 0 THEN 1 ELSE 0 END) as wins,
                        SUM(realized_pnl - fee_total + funding_total) as total_pnl,
                        AVG(realized_pnl - fee_total + funding_total) as avg_pnl
                    FROM closed_trades
                    WHERE {where_clause}
                    GROUP BY symbol
                    ORDER BY total_pnl DESC
                """)
                
                symbol_rows = conn.execute(symbol_sql, params).fetchall()
                by_symbol = []
                for r in symbol_rows:
                    trades = r[1] or 0
                    wins = r[2] or 0
                    by_symbol.append({
                        "symbol": r[0],
                        "trades": trades,
                        "wins": wins,
                        "win_rate": round(wins / trades * 100, 2) if trades > 0 else 0,
                        "total_pnl": round(float(r[3] or 0), 2),
                        "avg_pnl": round(float(r[4] or 0), 2)
                    })
                
                # 3. 按日期分布
                if "sqlite" in self.db_url:
                    date_expr = "date(close_time_ms/1000, 'unixepoch')"
                else:
                    date_expr = "DATE(FROM_UNIXTIME(close_time_ms/1000))"
                
                date_sql = text(f"""
                    SELECT 
                        {date_expr} as trade_date,
                        COUNT(*) as trades,
                        SUM(CASE WHEN (realized_pnl - fee_total + funding_total) > 0 THEN 1 ELSE 0 END) as wins,
                        SUM(realized_pnl - fee_total + funding_total) as total_pnl
                    FROM closed_trades
                    WHERE {where_clause}
                    GROUP BY trade_date
                    ORDER BY trade_date ASC
                """)
                
                date_rows = conn.execute(date_sql, params).fetchall()
                by_date = []
                cumulative_pnl = 0
                for r in date_rows:
                    trades = r[1] or 0
                    wins = r[2] or 0
                    pnl = float(r[3] or 0)
                    cumulative_pnl += pnl
                    by_date.append({
                        "date": str(r[0]),
                        "trades": trades,
                        "wins": wins,
                        "win_rate": round(wins / trades * 100, 2) if trades > 0 else 0,
                        "pnl": round(pnl, 2),
                        "cumulative_pnl": round(cumulative_pnl, 2)
                    })
                
                # 4. 交易列表（分页）
                offset = (page - 1) * page_size
                trades_sql = text(f"""
                    SELECT 
                        cycle_id, exchange, symbol, side,
                        entry_price, exit_price, quantity,
                        realized_pnl, (realized_pnl - fee_total + funding_total) as net_pnl, fee_total, funding_total,
                        open_time_ms, close_time_ms, duration_minutes
                    FROM closed_trades
                    WHERE {where_clause}
                    ORDER BY close_time_ms DESC
                    LIMIT :limit OFFSET :offset
                """)
                
                params["limit"] = page_size
                params["offset"] = offset
                
                trade_rows = conn.execute(trades_sql, params).fetchall()
                trades = []
                for r in trade_rows:
                    trades.append({
                        "cycleId": r[0],
                        "exchange": r[1],
                        "symbol": r[2],
                        "side": r[3],
                        "entryPrice": str(r[4]) if r[4] else "0",
                        "exitPrice": str(r[5]) if r[5] else "0",
                        "qty": str(r[6]) if r[6] else "0",
                        "realizedPnl": str(round(float(r[7] or 0), 4)),
                        "netPnl": str(round(float(r[8] or 0), 4)),
                        "feeTotal": str(round(float(r[9] or 0), 6)),
                        "fundingTotal": str(round(float(r[10] or 0), 6)),
                        "openTimeMs": str(r[11]) if r[11] else "0",
                        "closeTimeMs": str(r[12]) if r[12] else "0",
                        "durationMinutes": round(float(r[13] or 0), 1)
                    })
                
                # 5. 获取可用的筛选选项
                # 获取所有币种
                symbols_sql = text(f"""
                    SELECT DISTINCT symbol FROM closed_trades WHERE uid = :uid ORDER BY symbol
                """)
                all_symbols = [r[0] for r in conn.execute(symbols_sql, {"uid": uid}).fetchall()]
                
                # 获取所有交易所
                exchanges_sql = text(f"""
                    SELECT DISTINCT exchange FROM closed_trades WHERE uid = :uid ORDER BY exchange
                """)
                all_exchanges = [r[0] for r in conn.execute(exchanges_sql, {"uid": uid}).fetchall()]
                
                return {
                    "summary": summary,
                    "by_symbol": by_symbol,
                    "by_date": by_date,
                    "trades": trades,
                    "pagination": {
                        "page": page,
                        "page_size": page_size,
                        "total": total_trades,
                        "total_pages": (total_trades + page_size - 1) // page_size if page_size > 0 else 0
                    },
                    "filters": {
                        "available_symbols": all_symbols,
                        "available_exchanges": all_exchanges
                    }
                }
                
        except Exception as e:
            logger.error(f"Failed to get advanced statistics for {uid}: {e}")
            logger.exception("获取高级统计数据失败")
            return {"error": str(e)}


# 全局单例
_closed_trades_db: Optional[ClosedTradesDB] = None


def get_closed_trades_db() -> ClosedTradesDB:
    """获取 ClosedTradesDB 单例"""
    global _closed_trades_db
    if _closed_trades_db is None:
        _closed_trades_db = ClosedTradesDB()
    return _closed_trades_db
