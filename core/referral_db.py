# referral_db.py
"""
邀请返佣系统数据库层

功能：
1. 15层邀请关系管理
2. 返佣比例配置
3. 返佣记录和余额管理
4. 用户收益排行榜
"""

import os
import json
import time
import secrets
import logging
from typing import Optional, List, Dict, Any, Tuple
from decimal import Decimal
from datetime import datetime, timedelta
from contextlib import contextmanager
from enum import Enum

try:
    from sqlalchemy import create_engine, text
    from sqlalchemy.orm import sessionmaker
    HAS_SQLALCHEMY = True
except ImportError:
    HAS_SQLALCHEMY = False
    import sqlite3

logger = logging.getLogger(__name__)


class CommissionStatus(Enum):
    """返佣状态"""
    PENDING = "pending"      # 待结算
    SETTLED = "settled"      # 已结算
    PAID = "paid"           # 已提现
    CANCELLED = "cancelled"  # 已取消


class CommissionSourceType(Enum):
    """返佣来源类型"""
    TRADING_PROFIT = "trading_profit"   # 交易利润
    TRADING_FEE = "trading_fee"         # 交易手续费
    SUBSCRIPTION = "subscription"       # 订阅费
    OTHER = "other"


class LeaderboardPeriod(Enum):
    """排行榜周期"""
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"
    ALL_TIME = "all_time"


class ReferralDB:
    """
    邀请返佣数据库管理
    
    P0 Fix: 使用共享数据库引擎，避免连接池碎片化
    """
    
    MAX_REFERRAL_LEVELS = 15  # 最大返佣层级
    
    def __init__(self, db_url: str = None):
        self.db_url = db_url or os.getenv("DATABASE_URL", "sqlite:///users.db")
        
        if HAS_SQLALCHEMY:
            # P0 Fix: 使用共享数据库引擎
            from core.shared_db_engine import get_shared_engine, get_shared_session
            self.engine = get_shared_engine(self.db_url)
            self.Session = get_shared_session()
        else:
            self._sqlite_path = self.db_url.replace("sqlite:///", "")
        
        logger.info(f"ReferralDB 已初始化 (using shared engine)")
    
    @contextmanager
    def _get_connection(self):
        """获取数据库连接"""
        if HAS_SQLALCHEMY:
            session = self.Session()
            try:
                yield session
                session.commit()
            except Exception:
                session.rollback()
                raise
            finally:
                session.close()
        else:
            conn = sqlite3.connect(self._sqlite_path)
            conn.row_factory = sqlite3.Row
            try:
                yield conn
                conn.commit()
            finally:
                conn.close()
    
    def _is_sqlite(self) -> bool:
        return "sqlite" in self.db_url.lower()
    
    # ============================================================
    # 表初始化
    # ============================================================
    
    def init_tables(self):
        """初始化邀请返佣相关表"""
        is_sqlite = self._is_sqlite()
        
        with self._get_connection() as conn:
            if is_sqlite:
                self._init_sqlite_tables(conn)
            else:
                self._init_mysql_tables(conn)
        
        # 初始化默认返佣比例
        self._init_default_commission_rates()
        
        # 迁移：添加新列（如果不存在）
        self._migrate_add_public_dashboard_columns()
        
        logger.info("邀请返佣表已初始化")
    
    def _migrate_add_public_dashboard_columns(self):
        """迁移：为 user_leaderboard_settings 表添加 public_dashboard 列"""
        is_sqlite = self._is_sqlite()
        
        with self._get_connection() as conn:
            try:
                # 检查列是否存在
                if HAS_SQLALCHEMY:
                    if is_sqlite:
                        result = conn.execute(text(
                            "PRAGMA table_info(user_leaderboard_settings)"
                        )).fetchall()
                        columns = [row[1] for row in result]  # column name is at index 1
                    else:
                        result = conn.execute(text("""
                            SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS 
                            WHERE TABLE_NAME = 'user_leaderboard_settings'
                        """)).fetchall()
                        columns = [row[0] for row in result]
                else:
                    result = conn.execute("PRAGMA table_info(user_leaderboard_settings)").fetchall()
                    columns = [row[1] for row in result]
                
                # 添加 public_dashboard 列
                if 'public_dashboard' not in columns:
                    if HAS_SQLALCHEMY:
                        if is_sqlite:
                            conn.execute(text(
                                "ALTER TABLE user_leaderboard_settings ADD COLUMN public_dashboard INTEGER DEFAULT 0"
                            ))
                        else:
                            conn.execute(text(
                                "ALTER TABLE user_leaderboard_settings ADD COLUMN public_dashboard TINYINT DEFAULT 0"
                            ))
                    else:
                        conn.execute(
                            "ALTER TABLE user_leaderboard_settings ADD COLUMN public_dashboard INTEGER DEFAULT 0"
                        )
                    logger.info("已添加 public_dashboard 列")
                
                # 添加 public_dashboard_token 列
                if 'public_dashboard_token' not in columns:
                    if HAS_SQLALCHEMY:
                        if is_sqlite:
                            conn.execute(text(
                                "ALTER TABLE user_leaderboard_settings ADD COLUMN public_dashboard_token TEXT"
                            ))
                        else:
                            conn.execute(text(
                                "ALTER TABLE user_leaderboard_settings ADD COLUMN public_dashboard_token VARCHAR(64)"
                            ))
                    else:
                        conn.execute(
                            "ALTER TABLE user_leaderboard_settings ADD COLUMN public_dashboard_token TEXT"
                        )
                    logger.info("已添加 public_dashboard_token 列")
                    
            except Exception as e:
                # 表可能不存在（首次运行），忽略错误
                logger.debug(f"迁移 public_dashboard 列时出错（可忽略）: {e}")
    
    def _init_sqlite_tables(self, conn):
        """SQLite 表结构"""
        execute = lambda sql: conn.execute(text(sql)) if HAS_SQLALCHEMY else conn.execute(sql)
        
        # 1. 用户邀请关系表
        execute("""
            CREATE TABLE IF NOT EXISTS user_referrals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                uid TEXT NOT NULL UNIQUE,
                referrer_uid TEXT,
                referral_code TEXT UNIQUE NOT NULL,
                referred_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                level_1_uid TEXT,
                level_2_uid TEXT,
                level_3_uid TEXT,
                level_4_uid TEXT,
                level_5_uid TEXT,
                level_6_uid TEXT,
                level_7_uid TEXT,
                level_8_uid TEXT,
                level_9_uid TEXT,
                level_10_uid TEXT,
                level_11_uid TEXT,
                level_12_uid TEXT,
                level_13_uid TEXT,
                level_14_uid TEXT,
                level_15_uid TEXT
            )
        """)
        
        # 创建索引
        try:
            execute("CREATE INDEX IF NOT EXISTS idx_referrals_referrer ON user_referrals(referrer_uid)")
            execute("CREATE INDEX IF NOT EXISTS idx_referrals_code ON user_referrals(referral_code)")
        except Exception:
            pass
        
        # 2. 返佣比例配置表
        execute("""
            CREATE TABLE IF NOT EXISTS referral_commission_rates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                level INTEGER NOT NULL UNIQUE,
                rate REAL NOT NULL DEFAULT 0,
                min_amount REAL DEFAULT 0,
                is_enabled INTEGER DEFAULT 1,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_by TEXT
            )
        """)
        
        # 3. 返佣记录表
        execute("""
            CREATE TABLE IF NOT EXISTS referral_commissions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                uid TEXT NOT NULL,
                from_uid TEXT NOT NULL,
                level INTEGER NOT NULL,
                source_type TEXT NOT NULL,
                source_id TEXT,
                source_amount REAL NOT NULL,
                commission_rate REAL NOT NULL,
                commission_amount REAL NOT NULL,
                status TEXT DEFAULT 'pending',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                settled_at TIMESTAMP,
                notes TEXT
            )
        """)
        
        try:
            execute("CREATE INDEX IF NOT EXISTS idx_commissions_uid ON referral_commissions(uid)")
            execute("CREATE INDEX IF NOT EXISTS idx_commissions_from_uid ON referral_commissions(from_uid)")
            execute("CREATE INDEX IF NOT EXISTS idx_commissions_status ON referral_commissions(status)")
            execute("CREATE INDEX IF NOT EXISTS idx_commissions_created ON referral_commissions(created_at)")
        except Exception:
            pass
        
        # 4. 用户返佣余额表
        execute("""
            CREATE TABLE IF NOT EXISTS user_commission_balance (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                uid TEXT NOT NULL UNIQUE,
                total_earned REAL DEFAULT 0,
                total_withdrawn REAL DEFAULT 0,
                available_balance REAL DEFAULT 0,
                frozen_balance REAL DEFAULT 0,
                last_settlement_at TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # 5. 用户收益统计表（用于排行榜）
        execute("""
            CREATE TABLE IF NOT EXISTS user_profit_stats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                uid TEXT NOT NULL,
                period_type TEXT NOT NULL,
                period_start TIMESTAMP NOT NULL,
                period_end TIMESTAMP NOT NULL,
                total_profit REAL DEFAULT 0,
                total_loss REAL DEFAULT 0,
                net_profit REAL DEFAULT 0,
                total_trades INTEGER DEFAULT 0,
                winning_trades INTEGER DEFAULT 0,
                losing_trades INTEGER DEFAULT 0,
                win_rate REAL DEFAULT 0,
                max_profit REAL DEFAULT 0,
                max_loss REAL DEFAULT 0,
                max_drawdown REAL DEFAULT 0,
                sharpe_ratio REAL DEFAULT 0,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(uid, period_type, period_start)
            )
        """)
        
        try:
            execute("CREATE INDEX IF NOT EXISTS idx_profit_stats_uid ON user_profit_stats(uid)")
            execute("CREATE INDEX IF NOT EXISTS idx_profit_stats_period ON user_profit_stats(period_type, period_start)")
            execute("CREATE INDEX IF NOT EXISTS idx_profit_stats_net_profit ON user_profit_stats(net_profit DESC)")
        except Exception:
            pass
        
        # 6. 用户排行榜设置表
        execute("""
            CREATE TABLE IF NOT EXISTS user_leaderboard_settings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                uid TEXT NOT NULL UNIQUE,
                show_on_leaderboard INTEGER DEFAULT 1,
                display_name TEXT,
                hide_profit_amount INTEGER DEFAULT 0,
                public_dashboard INTEGER DEFAULT 0,
                public_dashboard_token TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # 7. 提现记录表
        execute("""
            CREATE TABLE IF NOT EXISTS commission_withdrawals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                uid TEXT NOT NULL,
                amount REAL NOT NULL,
                status TEXT DEFAULT 'pending',
                withdraw_address TEXT,
                withdraw_type TEXT DEFAULT 'USDT',
                tx_hash TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                processed_at TIMESTAMP,
                processed_by TEXT,
                notes TEXT
            )
        """)
    
    def _init_mysql_tables(self, conn):
        """MySQL 表结构"""
        # 1. 用户邀请关系表
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS user_referrals (
                id INT PRIMARY KEY AUTO_INCREMENT,
                uid VARCHAR(32) NOT NULL UNIQUE,
                referrer_uid VARCHAR(32),
                referral_code VARCHAR(16) UNIQUE NOT NULL,
                referred_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                level_1_uid VARCHAR(32),
                level_2_uid VARCHAR(32),
                level_3_uid VARCHAR(32),
                level_4_uid VARCHAR(32),
                level_5_uid VARCHAR(32),
                level_6_uid VARCHAR(32),
                level_7_uid VARCHAR(32),
                level_8_uid VARCHAR(32),
                level_9_uid VARCHAR(32),
                level_10_uid VARCHAR(32),
                level_11_uid VARCHAR(32),
                level_12_uid VARCHAR(32),
                level_13_uid VARCHAR(32),
                level_14_uid VARCHAR(32),
                level_15_uid VARCHAR(32),
                INDEX idx_referrer (referrer_uid),
                INDEX idx_code (referral_code)
            )
        """))
        
        # 2. 返佣比例配置表
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS referral_commission_rates (
                id INT PRIMARY KEY AUTO_INCREMENT,
                level INT NOT NULL UNIQUE,
                rate DECIMAL(10,6) NOT NULL DEFAULT 0,
                min_amount DECIMAL(20,8) DEFAULT 0,
                is_enabled TINYINT DEFAULT 1,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                updated_by VARCHAR(32)
            )
        """))
        
        # 3. 返佣记录表
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS referral_commissions (
                id BIGINT PRIMARY KEY AUTO_INCREMENT,
                uid VARCHAR(32) NOT NULL,
                from_uid VARCHAR(32) NOT NULL,
                level INT NOT NULL,
                source_type VARCHAR(32) NOT NULL,
                source_id VARCHAR(64),
                source_amount DECIMAL(20,8) NOT NULL,
                commission_rate DECIMAL(10,6) NOT NULL,
                commission_amount DECIMAL(20,8) NOT NULL,
                status VARCHAR(16) DEFAULT 'pending',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                settled_at TIMESTAMP NULL,
                notes TEXT,
                INDEX idx_uid (uid),
                INDEX idx_from_uid (from_uid),
                INDEX idx_status (status),
                INDEX idx_created (created_at)
            )
        """))
        
        # 4. 用户返佣余额表
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS user_commission_balance (
                id INT PRIMARY KEY AUTO_INCREMENT,
                uid VARCHAR(32) NOT NULL UNIQUE,
                total_earned DECIMAL(20,8) DEFAULT 0,
                total_withdrawn DECIMAL(20,8) DEFAULT 0,
                available_balance DECIMAL(20,8) DEFAULT 0,
                frozen_balance DECIMAL(20,8) DEFAULT 0,
                last_settlement_at TIMESTAMP NULL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            )
        """))
        
        # 5. 用户收益统计表
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS user_profit_stats (
                id BIGINT PRIMARY KEY AUTO_INCREMENT,
                uid VARCHAR(32) NOT NULL,
                period_type VARCHAR(16) NOT NULL,
                period_start TIMESTAMP NOT NULL,
                period_end TIMESTAMP NOT NULL,
                total_profit DECIMAL(20,8) DEFAULT 0,
                total_loss DECIMAL(20,8) DEFAULT 0,
                net_profit DECIMAL(20,8) DEFAULT 0,
                total_trades INT DEFAULT 0,
                winning_trades INT DEFAULT 0,
                losing_trades INT DEFAULT 0,
                win_rate DECIMAL(5,4) DEFAULT 0,
                max_profit DECIMAL(20,8) DEFAULT 0,
                max_loss DECIMAL(20,8) DEFAULT 0,
                max_drawdown DECIMAL(20,8) DEFAULT 0,
                sharpe_ratio DECIMAL(10,4) DEFAULT 0,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                UNIQUE KEY uk_uid_period (uid, period_type, period_start),
                INDEX idx_uid (uid),
                INDEX idx_period (period_type, period_start),
                INDEX idx_net_profit (net_profit DESC)
            )
        """))
        
        # 6. 用户排行榜设置表
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS user_leaderboard_settings (
                id INT PRIMARY KEY AUTO_INCREMENT,
                uid VARCHAR(32) NOT NULL UNIQUE,
                show_on_leaderboard TINYINT DEFAULT 1,
                display_name VARCHAR(64),
                hide_profit_amount TINYINT DEFAULT 0,
                public_dashboard TINYINT DEFAULT 0,
                public_dashboard_token VARCHAR(64),
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            )
        """))
        
        # 7. 提现记录表
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS commission_withdrawals (
                id BIGINT PRIMARY KEY AUTO_INCREMENT,
                uid VARCHAR(32) NOT NULL,
                amount DECIMAL(20,8) NOT NULL,
                status VARCHAR(16) DEFAULT 'pending',
                withdraw_address VARCHAR(128),
                withdraw_type VARCHAR(16) DEFAULT 'USDT',
                tx_hash VARCHAR(128),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                processed_at TIMESTAMP NULL,
                processed_by VARCHAR(32),
                notes TEXT,
                INDEX idx_uid (uid),
                INDEX idx_status (status)
            )
        """))
    
    def _init_default_commission_rates(self):
        """初始化默认返佣比例"""
        # 默认比例：逐层递减
        default_rates = [
            (1, 0.10),   # 第1层 10%
            (2, 0.05),   # 第2层 5%
            (3, 0.03),   # 第3层 3%
            (4, 0.02),   # 第4层 2%
            (5, 0.01),   # 第5层 1%
            (6, 0.008),  # 第6层 0.8%
            (7, 0.006),  # 第7层 0.6%
            (8, 0.005),  # 第8层 0.5%
            (9, 0.004),  # 第9层 0.4%
            (10, 0.003), # 第10层 0.3%
            (11, 0.002), # 第11层 0.2%
            (12, 0.002), # 第12层 0.2%
            (13, 0.001), # 第13层 0.1%
            (14, 0.001), # 第14层 0.1%
            (15, 0.001), # 第15层 0.1%
        ]
        
        with self._get_connection() as conn:
            for level, rate in default_rates:
                try:
                    if HAS_SQLALCHEMY:
                        # 检查是否已存在
                        existing = conn.execute(text(
                            "SELECT id FROM referral_commission_rates WHERE level = :level"
                        ), {"level": level}).fetchone()
                        
                        if not existing:
                            conn.execute(text("""
                                INSERT INTO referral_commission_rates (level, rate, is_enabled)
                                VALUES (:level, :rate, 1)
                            """), {"level": level, "rate": rate})
                    else:
                        conn.execute("""
                            INSERT OR IGNORE INTO referral_commission_rates (level, rate, is_enabled)
                            VALUES (?, ?, 1)
                        """, (level, rate))
                except Exception as e:
                    logger.debug(f"初始化第{level}层返佣比例: {e}")
    
    # ============================================================
    # 邀请码管理
    # ============================================================
    
    def generate_referral_code(self, uid: str) -> str:
        """为用户生成邀请码"""
        # 检查是否已有邀请码
        existing = self.get_user_referral_info(uid)
        if existing and existing.get("referral_code"):
            return existing["referral_code"]
        
        # 生成新邀请码 (8位字母数字)
        code = secrets.token_urlsafe(6)[:8].upper()
        
        with self._get_connection() as conn:
            try:
                if HAS_SQLALCHEMY:
                    conn.execute(text("""
                        INSERT INTO user_referrals (uid, referral_code)
                        VALUES (:uid, :code)
                        ON CONFLICT(uid) DO UPDATE SET referral_code = :code
                    """) if self._is_sqlite() else text("""
                        INSERT INTO user_referrals (uid, referral_code)
                        VALUES (:uid, :code)
                        ON DUPLICATE KEY UPDATE referral_code = :code
                    """), {"uid": uid, "code": code})
                else:
                    conn.execute("""
                        INSERT OR REPLACE INTO user_referrals (uid, referral_code)
                        VALUES (?, ?)
                    """, (uid, code))
                
                logger.info(f"[{uid}] 生成邀请码: {code}")
                return code
            except Exception as e:
                logger.error(f"生成邀请码失败: {e}")
                # 可能是重复，重新生成
                return self.generate_referral_code(uid)
    
    def get_uid_by_referral_code(self, code: str) -> Optional[str]:
        """通过邀请码获取用户ID"""
        with self._get_connection() as conn:
            if HAS_SQLALCHEMY:
                result = conn.execute(text(
                    "SELECT uid FROM user_referrals WHERE referral_code = :code"
                ), {"code": code.upper()}).fetchone()
            else:
                cursor = conn.execute(
                    "SELECT uid FROM user_referrals WHERE referral_code = ?",
                    (code.upper(),)
                )
                result = cursor.fetchone()
            
            return result[0] if result else None
    
    def bind_referrer(self, uid: str, referrer_code: str) -> Tuple[bool, str]:
        """
        绑定邀请人
        
        Returns:
            (success, message)
        """
        # 查找邀请人
        referrer_uid = self.get_uid_by_referral_code(referrer_code)
        if not referrer_uid:
            return False, "邀请码无效"
        
        if referrer_uid == uid:
            return False, "不能邀请自己"
        
        # 检查是否已绑定
        existing = self.get_user_referral_info(uid)
        if existing and existing.get("referrer_uid"):
            return False, "已绑定邀请人，不能重复绑定"
        
        # 获取邀请人的上级链
        referrer_info = self.get_user_referral_info(referrer_uid)
        
        # 构建15层上级关系
        level_uids = {f"level_{i}_uid": None for i in range(1, 16)}
        level_uids["level_1_uid"] = referrer_uid
        
        if referrer_info:
            # 继承邀请人的上级链（向上移一位）
            for i in range(2, 16):
                prev_level = f"level_{i-1}_uid"
                curr_level = f"level_{i}_uid"
                level_uids[curr_level] = referrer_info.get(prev_level)
        
        with self._get_connection() as conn:
            try:
                if HAS_SQLALCHEMY:
                    # 更新或插入
                    conn.execute(text("""
                        INSERT INTO user_referrals (uid, referrer_uid, referral_code,
                            level_1_uid, level_2_uid, level_3_uid, level_4_uid, level_5_uid,
                            level_6_uid, level_7_uid, level_8_uid, level_9_uid, level_10_uid,
                            level_11_uid, level_12_uid, level_13_uid, level_14_uid, level_15_uid)
                        VALUES (:uid, :referrer_uid, :code,
                            :l1, :l2, :l3, :l4, :l5, :l6, :l7, :l8, :l9, :l10, :l11, :l12, :l13, :l14, :l15)
                        ON CONFLICT(uid) DO UPDATE SET
                            referrer_uid = :referrer_uid,
                            level_1_uid = :l1, level_2_uid = :l2, level_3_uid = :l3,
                            level_4_uid = :l4, level_5_uid = :l5, level_6_uid = :l6,
                            level_7_uid = :l7, level_8_uid = :l8, level_9_uid = :l9,
                            level_10_uid = :l10, level_11_uid = :l11, level_12_uid = :l12,
                            level_13_uid = :l13, level_14_uid = :l14, level_15_uid = :l15
                    """) if self._is_sqlite() else text("""
                        INSERT INTO user_referrals (uid, referrer_uid, referral_code,
                            level_1_uid, level_2_uid, level_3_uid, level_4_uid, level_5_uid,
                            level_6_uid, level_7_uid, level_8_uid, level_9_uid, level_10_uid,
                            level_11_uid, level_12_uid, level_13_uid, level_14_uid, level_15_uid)
                        VALUES (:uid, :referrer_uid, :code,
                            :l1, :l2, :l3, :l4, :l5, :l6, :l7, :l8, :l9, :l10, :l11, :l12, :l13, :l14, :l15)
                        ON DUPLICATE KEY UPDATE
                            referrer_uid = :referrer_uid,
                            level_1_uid = :l1, level_2_uid = :l2, level_3_uid = :l3,
                            level_4_uid = :l4, level_5_uid = :l5, level_6_uid = :l6,
                            level_7_uid = :l7, level_8_uid = :l8, level_9_uid = :l9,
                            level_10_uid = :l10, level_11_uid = :l11, level_12_uid = :l12,
                            level_13_uid = :l13, level_14_uid = :l14, level_15_uid = :l15
                    """), {
                        "uid": uid,
                        "referrer_uid": referrer_uid,
                        "code": existing.get("referral_code") if existing else secrets.token_urlsafe(6)[:8].upper(),
                        "l1": level_uids["level_1_uid"],
                        "l2": level_uids["level_2_uid"],
                        "l3": level_uids["level_3_uid"],
                        "l4": level_uids["level_4_uid"],
                        "l5": level_uids["level_5_uid"],
                        "l6": level_uids["level_6_uid"],
                        "l7": level_uids["level_7_uid"],
                        "l8": level_uids["level_8_uid"],
                        "l9": level_uids["level_9_uid"],
                        "l10": level_uids["level_10_uid"],
                        "l11": level_uids["level_11_uid"],
                        "l12": level_uids["level_12_uid"],
                        "l13": level_uids["level_13_uid"],
                        "l14": level_uids["level_14_uid"],
                        "l15": level_uids["level_15_uid"],
                    })
                else:
                    code = existing.get("referral_code") if existing else secrets.token_urlsafe(6)[:8].upper()
                    conn.execute("""
                        INSERT OR REPLACE INTO user_referrals 
                        (uid, referrer_uid, referral_code,
                         level_1_uid, level_2_uid, level_3_uid, level_4_uid, level_5_uid,
                         level_6_uid, level_7_uid, level_8_uid, level_9_uid, level_10_uid,
                         level_11_uid, level_12_uid, level_13_uid, level_14_uid, level_15_uid)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (uid, referrer_uid, code,
                          level_uids["level_1_uid"], level_uids["level_2_uid"],
                          level_uids["level_3_uid"], level_uids["level_4_uid"],
                          level_uids["level_5_uid"], level_uids["level_6_uid"],
                          level_uids["level_7_uid"], level_uids["level_8_uid"],
                          level_uids["level_9_uid"], level_uids["level_10_uid"],
                          level_uids["level_11_uid"], level_uids["level_12_uid"],
                          level_uids["level_13_uid"], level_uids["level_14_uid"],
                          level_uids["level_15_uid"]))
                
                logger.info(f"[{uid}] 绑定邀请人: {referrer_uid}")
                return True, "绑定成功"
                
            except Exception as e:
                logger.error(f"绑定邀请人失败: {e}")
                return False, f"绑定失败: {str(e)}"
    
    def get_user_referral_info(self, uid: str) -> Optional[Dict]:
        """获取用户邀请信息"""
        with self._get_connection() as conn:
            if HAS_SQLALCHEMY:
                result = conn.execute(text(
                    "SELECT * FROM user_referrals WHERE uid = :uid"
                ), {"uid": uid}).fetchone()
                if result:
                    return dict(result._mapping)
            else:
                cursor = conn.execute(
                    "SELECT * FROM user_referrals WHERE uid = ?", (uid,)
                )
                result = cursor.fetchone()
                if result:
                    return dict(result)
            
            return None
    
    def get_direct_referrals(self, uid: str, limit: int = 100, offset: int = 0) -> List[Dict]:
        """获取直接邀请的用户列表"""
        with self._get_connection() as conn:
            if HAS_SQLALCHEMY:
                results = conn.execute(text("""
                    SELECT r.uid, r.referred_at, u.username
                    FROM user_referrals r
                    LEFT JOIN users u ON r.uid = u.uid
                    WHERE r.referrer_uid = :uid
                    ORDER BY r.referred_at DESC
                    LIMIT :limit OFFSET :offset
                """), {"uid": uid, "limit": limit, "offset": offset}).fetchall()
                return [dict(r._mapping) for r in results]
            else:
                cursor = conn.execute("""
                    SELECT r.uid, r.referred_at, u.username
                    FROM user_referrals r
                    LEFT JOIN users u ON r.uid = u.uid
                    WHERE r.referrer_uid = ?
                    ORDER BY r.referred_at DESC
                    LIMIT ? OFFSET ?
                """, (uid, limit, offset))
                return [dict(r) for r in cursor.fetchall()]
    
    def get_team_stats(self, uid: str) -> Dict:
        """获取团队统计（各层人数）"""
        stats = {
            "total_team": 0,
            "direct_count": 0,
            "level_counts": {i: 0 for i in range(1, 16)},
        }
        
        with self._get_connection() as conn:
            for level in range(1, 16):
                level_col = f"level_{level}_uid"
                if HAS_SQLALCHEMY:
                    result = conn.execute(text(f"""
                        SELECT COUNT(*) as cnt FROM user_referrals WHERE {level_col} = :uid
                    """), {"uid": uid}).fetchone()
                    count = result[0] if result else 0
                else:
                    cursor = conn.execute(f"""
                        SELECT COUNT(*) as cnt FROM user_referrals WHERE {level_col} = ?
                    """, (uid,))
                    result = cursor.fetchone()
                    count = result[0] if result else 0
                
                stats["level_counts"][level] = count
                stats["total_team"] += count
                
                if level == 1:
                    stats["direct_count"] = count
        
        return stats
    
    # ============================================================
    # 返佣比例管理（管理员）
    # ============================================================
    
    def get_commission_rates(self) -> List[Dict]:
        """获取所有层级的返佣比例"""
        with self._get_connection() as conn:
            if HAS_SQLALCHEMY:
                results = conn.execute(text("""
                    SELECT level, rate, min_amount, is_enabled, updated_at, updated_by
                    FROM referral_commission_rates
                    ORDER BY level
                """)).fetchall()
                return [dict(r._mapping) for r in results]
            else:
                cursor = conn.execute("""
                    SELECT level, rate, min_amount, is_enabled, updated_at, updated_by
                    FROM referral_commission_rates
                    ORDER BY level
                """)
                return [dict(r) for r in cursor.fetchall()]
    
    def update_commission_rate(
        self,
        level: int,
        rate: float,
        min_amount: float = 0,
        is_enabled: bool = True,
        updated_by: str = None
    ) -> bool:
        """更新指定层级的返佣比例"""
        if level < 1 or level > self.MAX_REFERRAL_LEVELS:
            return False
        
        with self._get_connection() as conn:
            try:
                if HAS_SQLALCHEMY:
                    conn.execute(text("""
                        UPDATE referral_commission_rates
                        SET rate = :rate, min_amount = :min_amount, is_enabled = :enabled,
                            updated_at = CURRENT_TIMESTAMP, updated_by = :updated_by
                        WHERE level = :level
                    """), {
                        "level": level,
                        "rate": rate,
                        "min_amount": min_amount,
                        "enabled": 1 if is_enabled else 0,
                        "updated_by": updated_by
                    })
                else:
                    conn.execute("""
                        UPDATE referral_commission_rates
                        SET rate = ?, min_amount = ?, is_enabled = ?,
                            updated_at = CURRENT_TIMESTAMP, updated_by = ?
                        WHERE level = ?
                    """, (rate, min_amount, 1 if is_enabled else 0, updated_by, level))
                
                logger.info(f"更新第{level}层返佣比例: {rate*100}%")
                return True
            except Exception as e:
                logger.error(f"更新返佣比例失败: {e}")
                return False
    
    def batch_update_commission_rates(self, rates: List[Dict], updated_by: str = None) -> bool:
        """批量更新返佣比例"""
        try:
            for item in rates:
                self.update_commission_rate(
                    level=item["level"],
                    rate=item["rate"],
                    min_amount=item.get("min_amount", 0),
                    is_enabled=item.get("is_enabled", True),
                    updated_by=updated_by
                )
            return True
        except Exception as e:
            logger.error(f"批量更新返佣比例失败: {e}")
            return False
    
    # ============================================================
    # 返佣发放
    # ============================================================
    
    def distribute_commission(
        self,
        from_uid: str,
        source_type: str,
        source_amount: float,
        source_id: str = None
    ) -> List[Dict]:
        """
        发放返佣（向上15层）
        
        Args:
            from_uid: 产生收益的用户
            source_type: 收益来源类型
            source_amount: 原始收益金额
            source_id: 来源ID（如交易ID）
        
        Returns:
            发放记录列表
        """
        if source_amount <= 0:
            return []
        
        # 获取用户的上级链
        user_info = self.get_user_referral_info(from_uid)
        if not user_info:
            return []
        
        # 获取返佣比例配置
        rates = {r["level"]: r for r in self.get_commission_rates()}
        
        commissions = []
        
        with self._get_connection() as conn:
            for level in range(1, 16):
                level_uid = user_info.get(f"level_{level}_uid")
                if not level_uid:
                    continue
                
                rate_config = rates.get(level)
                if not rate_config or not rate_config.get("is_enabled"):
                    continue
                
                # 转换 Decimal 为 float（MySQL DECIMAL 类型返回 decimal.Decimal）
                rate = float(rate_config["rate"]) if rate_config["rate"] is not None else 0.0
                min_amount = float(rate_config.get("min_amount", 0) or 0)
                
                commission_amount = source_amount * rate
                
                # 检查最小金额
                if commission_amount < min_amount:
                    continue
                
                try:
                    if HAS_SQLALCHEMY:
                        conn.execute(text("""
                            INSERT INTO referral_commissions
                            (uid, from_uid, level, source_type, source_id, source_amount,
                             commission_rate, commission_amount, status)
                            VALUES (:uid, :from_uid, :level, :source_type, :source_id,
                                    :source_amount, :rate, :amount, 'pending')
                        """), {
                            "uid": level_uid,
                            "from_uid": from_uid,
                            "level": level,
                            "source_type": source_type,
                            "source_id": source_id,
                            "source_amount": source_amount,
                            "rate": rate,
                            "amount": commission_amount
                        })
                    else:
                        conn.execute("""
                            INSERT INTO referral_commissions
                            (uid, from_uid, level, source_type, source_id, source_amount,
                             commission_rate, commission_amount, status)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending')
                        """, (level_uid, from_uid, level, source_type, source_id,
                              source_amount, rate, commission_amount))
                    
                    commissions.append({
                        "uid": level_uid,
                        "level": level,
                        "rate": rate,
                        "amount": commission_amount
                    })
                    
                except Exception as e:
                    logger.error(f"发放第{level}层返佣失败: {e}")
        
        if commissions:
            logger.info(f"[{from_uid}] 发放返佣: {len(commissions)}层, "
                       f"总额 {sum(c['amount'] for c in commissions):.4f}")
        
        return commissions
    
    def settle_commissions(self, uid: str = None) -> int:
        """
        结算返佣（pending -> settled，更新余额）
        
        Args:
            uid: 指定用户ID，为None则结算所有
        
        Returns:
            结算的记录数
        """
        with self._get_connection() as conn:
            # 获取待结算记录
            if HAS_SQLALCHEMY:
                if uid:
                    pending = conn.execute(text("""
                        SELECT id, uid, commission_amount FROM referral_commissions
                        WHERE uid = :uid AND status = 'pending'
                    """), {"uid": uid}).fetchall()
                else:
                    pending = conn.execute(text("""
                        SELECT id, uid, commission_amount FROM referral_commissions
                        WHERE status = 'pending'
                    """)).fetchall()
            else:
                if uid:
                    cursor = conn.execute("""
                        SELECT id, uid, commission_amount FROM referral_commissions
                        WHERE uid = ? AND status = 'pending'
                    """, (uid,))
                else:
                    cursor = conn.execute("""
                        SELECT id, uid, commission_amount FROM referral_commissions
                        WHERE status = 'pending'
                    """)
                pending = cursor.fetchall()
            
            if not pending:
                return 0
            
            # 按用户汇总
            user_amounts = {}
            commission_ids = []
            
            for record in pending:
                if HAS_SQLALCHEMY:
                    record = record._mapping
                record_id = record["id"]
                record_uid = record["uid"]
                amount = float(record["commission_amount"])
                
                commission_ids.append(record_id)
                user_amounts[record_uid] = user_amounts.get(record_uid, 0) + amount
            
            # 更新余额和状态
            for uid, amount in user_amounts.items():
                if HAS_SQLALCHEMY:
                    # 更新或插入余额
                    conn.execute(text("""
                        INSERT INTO user_commission_balance (uid, total_earned, available_balance)
                        VALUES (:uid, :amount, :amount)
                        ON CONFLICT(uid) DO UPDATE SET
                            total_earned = total_earned + :amount,
                            available_balance = available_balance + :amount,
                            last_settlement_at = CURRENT_TIMESTAMP,
                            updated_at = CURRENT_TIMESTAMP
                    """) if self._is_sqlite() else text("""
                        INSERT INTO user_commission_balance (uid, total_earned, available_balance)
                        VALUES (:uid, :amount, :amount)
                        ON DUPLICATE KEY UPDATE
                            total_earned = total_earned + :amount,
                            available_balance = available_balance + :amount,
                            last_settlement_at = CURRENT_TIMESTAMP,
                            updated_at = CURRENT_TIMESTAMP
                    """), {"uid": uid, "amount": amount})
                else:
                    conn.execute("""
                        INSERT INTO user_commission_balance (uid, total_earned, available_balance)
                        VALUES (?, ?, ?)
                        ON CONFLICT(uid) DO UPDATE SET
                            total_earned = total_earned + ?,
                            available_balance = available_balance + ?,
                            last_settlement_at = CURRENT_TIMESTAMP,
                            updated_at = CURRENT_TIMESTAMP
                    """, (uid, amount, amount, amount, amount))
            
            # 更新状态
            for cid in commission_ids:
                if HAS_SQLALCHEMY:
                    conn.execute(text("""
                        UPDATE referral_commissions
                        SET status = 'settled', settled_at = CURRENT_TIMESTAMP
                        WHERE id = :id
                    """), {"id": cid})
                else:
                    conn.execute("""
                        UPDATE referral_commissions
                        SET status = 'settled', settled_at = CURRENT_TIMESTAMP
                        WHERE id = ?
                    """, (cid,))
            
            logger.info(f"结算返佣: {len(commission_ids)} 条记录, {len(user_amounts)} 个用户")
            return len(commission_ids)
    
    def get_user_commission_balance(self, uid: str) -> Dict:
        """获取用户返佣余额"""
        with self._get_connection() as conn:
            if HAS_SQLALCHEMY:
                result = conn.execute(text("""
                    SELECT * FROM user_commission_balance WHERE uid = :uid
                """), {"uid": uid}).fetchone()
                if result:
                    return dict(result._mapping)
            else:
                cursor = conn.execute("""
                    SELECT * FROM user_commission_balance WHERE uid = ?
                """, (uid,))
                result = cursor.fetchone()
                if result:
                    return dict(result)
            
            return {
                "uid": uid,
                "total_earned": 0,
                "total_withdrawn": 0,
                "available_balance": 0,
                "frozen_balance": 0
            }
    
    def get_commission_history(
        self,
        uid: str,
        status: str = None,
        limit: int = 50,
        offset: int = 0
    ) -> List[Dict]:
        """获取用户返佣历史"""
        with self._get_connection() as conn:
            if HAS_SQLALCHEMY:
                if status:
                    results = conn.execute(text("""
                        SELECT * FROM referral_commissions
                        WHERE uid = :uid AND status = :status
                        ORDER BY created_at DESC
                        LIMIT :limit OFFSET :offset
                    """), {"uid": uid, "status": status, "limit": limit, "offset": offset}).fetchall()
                else:
                    results = conn.execute(text("""
                        SELECT * FROM referral_commissions
                        WHERE uid = :uid
                        ORDER BY created_at DESC
                        LIMIT :limit OFFSET :offset
                    """), {"uid": uid, "limit": limit, "offset": offset}).fetchall()
                return [dict(r._mapping) for r in results]
            else:
                if status:
                    cursor = conn.execute("""
                        SELECT * FROM referral_commissions
                        WHERE uid = ? AND status = ?
                        ORDER BY created_at DESC
                        LIMIT ? OFFSET ?
                    """, (uid, status, limit, offset))
                else:
                    cursor = conn.execute("""
                        SELECT * FROM referral_commissions
                        WHERE uid = ?
                        ORDER BY created_at DESC
                        LIMIT ? OFFSET ?
                    """, (uid, limit, offset))
                return [dict(r) for r in cursor.fetchall()]
    
    # ============================================================
    # 排行榜
    # ============================================================
    
    def update_user_profit_stats(
        self,
        uid: str,
        period_type: str,
        profit: float,
        is_win: bool
    ):
        """更新用户收益统计"""
        now = datetime.utcnow()
        
        # 计算周期起止时间
        if period_type == "daily":
            period_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
            period_end = period_start + timedelta(days=1)
        elif period_type == "weekly":
            period_start = now - timedelta(days=now.weekday())
            period_start = period_start.replace(hour=0, minute=0, second=0, microsecond=0)
            period_end = period_start + timedelta(weeks=1)
        elif period_type == "monthly":
            period_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            if now.month == 12:
                period_end = period_start.replace(year=now.year+1, month=1)
            else:
                period_end = period_start.replace(month=now.month+1)
        else:  # all_time
            period_start = datetime(2020, 1, 1)
            # MySQL TIMESTAMP 最大值是 2038-01-19，使用 2038-01-01 作为 all_time 的结束时间
            period_end = datetime(2038, 1, 1)
        
        with self._get_connection() as conn:
            # 使用 INSERT ... ON DUPLICATE KEY UPDATE 实现原子操作
            # 这样可以避免并发时的唯一键冲突
            conn.execute(text("""
                INSERT INTO user_profit_stats
                (uid, period_type, period_start, period_end, total_profit, total_loss,
                 net_profit, total_trades, winning_trades, losing_trades, win_rate,
                 max_profit, max_loss)
                VALUES (:uid, :period_type, :period_start, :period_end, :add_profit,
                        :add_loss, :profit, 1, :add_win, :add_lose, :init_win_rate,
                        :max_profit_val, :max_loss_val)
                ON DUPLICATE KEY UPDATE
                    total_profit = total_profit + VALUES(total_profit),
                    total_loss = total_loss + VALUES(total_loss),
                    net_profit = net_profit + VALUES(net_profit),
                    total_trades = total_trades + 1,
                    winning_trades = winning_trades + VALUES(winning_trades),
                    losing_trades = losing_trades + VALUES(losing_trades),
                    win_rate = (winning_trades + VALUES(winning_trades)) * 1.0 / (total_trades + 1),
                    max_profit = GREATEST(max_profit, VALUES(max_profit)),
                    max_loss = GREATEST(max_loss, VALUES(max_loss)),
                    updated_at = CURRENT_TIMESTAMP
            """), {
                "uid": uid, 
                "period_type": period_type, 
                "period_start": period_start,
                "period_end": period_end,
                "add_profit": profit if profit > 0 else 0,
                "add_loss": abs(profit) if profit < 0 else 0,
                "profit": profit,
                "add_win": 1 if is_win else 0,
                "add_lose": 0 if is_win else 1,
                "init_win_rate": 1.0 if is_win else 0.0,
                "max_profit_val": profit if profit > 0 else 0,
                "max_loss_val": abs(profit) if profit < 0 else 0
            })
            
            conn.commit()
    
    def get_leaderboard(
        self,
        period_type: str = "daily",
        limit: int = 100,
        offset: int = 0,
        min_trades: int = 1
    ) -> List[Dict]:
        """获取排行榜"""
        now = datetime.utcnow()
        
        # 计算周期起始时间
        if period_type == "daily":
            period_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        elif period_type == "weekly":
            period_start = now - timedelta(days=now.weekday())
            period_start = period_start.replace(hour=0, minute=0, second=0, microsecond=0)
        elif period_type == "monthly":
            period_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        else:  # all_time
            period_start = datetime(2020, 1, 1)
        
        with self._get_connection() as conn:
            if HAS_SQLALCHEMY:
                # 检测数据库类型，SQLite 使用 || 连接，MySQL 使用 CONCAT
                is_sqlite = self._is_sqlite()
                if is_sqlite:
                    concat_expr = "SUBSTR(p.uid, 1, 4) || '****' || SUBSTR(p.uid, -4)"
                else:
                    concat_expr = "CONCAT(SUBSTR(p.uid, 1, 4), '****', SUBSTR(p.uid, -4))"
                
                results = conn.execute(text(f"""
                    SELECT 
                        p.uid,
                        COALESCE(s.display_name, {concat_expr}) as display_name,
                        CASE WHEN COALESCE(s.hide_profit_amount, 0) = 1 THEN NULL ELSE p.net_profit END as net_profit,
                        p.total_trades,
                        p.win_rate,
                        p.winning_trades,
                        p.losing_trades,
                        u.tier,
                        CASE WHEN COALESCE(s.public_dashboard, 0) = 1 THEN s.public_dashboard_token ELSE NULL END as public_token
                    FROM user_profit_stats p
                    LEFT JOIN user_leaderboard_settings s ON p.uid = s.uid
                    LEFT JOIN users u ON p.uid = u.uid
                    WHERE p.period_type = :period_type
                        AND p.period_start = :period_start
                        AND p.total_trades >= :min_trades
                        AND COALESCE(s.show_on_leaderboard, 1) = 1
                    ORDER BY p.net_profit DESC
                    LIMIT :limit OFFSET :offset
                """), {
                    "period_type": period_type,
                    "period_start": period_start,
                    "min_trades": min_trades,
                    "limit": limit,
                    "offset": offset
                }).fetchall()
                
                return [
                    {
                        "rank": offset + i + 1,
                        **dict(r._mapping)
                    }
                    for i, r in enumerate(results)
                ]
            else:
                # SQLite 版本
                cursor = conn.execute("""
                    SELECT 
                        p.uid,
                        COALESCE(s.display_name, SUBSTR(p.uid, 1, 4) || '****' || SUBSTR(p.uid, -4)) as display_name,
                        CASE WHEN COALESCE(s.hide_profit_amount, 0) = 1 THEN NULL ELSE p.net_profit END as net_profit,
                        p.total_trades,
                        p.win_rate,
                        p.winning_trades,
                        p.losing_trades,
                        u.tier,
                        CASE WHEN COALESCE(s.public_dashboard, 0) = 1 THEN s.public_dashboard_token ELSE NULL END as public_token
                    FROM user_profit_stats p
                    LEFT JOIN user_leaderboard_settings s ON p.uid = s.uid
                    LEFT JOIN users u ON p.uid = u.uid
                    WHERE p.period_type = ?
                        AND p.period_start = ?
                        AND p.total_trades >= ?
                        AND COALESCE(s.show_on_leaderboard, 1) = 1
                    ORDER BY p.net_profit DESC
                    LIMIT ? OFFSET ?
                """, (period_type, period_start.isoformat(), min_trades, limit, offset))
                
                return [
                    {
                        "rank": offset + i + 1,
                        **dict(r)
                    }
                    for i, r in enumerate(cursor.fetchall())
                ]
    
    def get_user_rank(self, uid: str, period_type: str = "daily") -> Optional[Dict]:
        """获取用户排名"""
        now = datetime.utcnow()
        
        if period_type == "daily":
            period_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        elif period_type == "weekly":
            period_start = now - timedelta(days=now.weekday())
            period_start = period_start.replace(hour=0, minute=0, second=0, microsecond=0)
        elif period_type == "monthly":
            period_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        else:
            period_start = datetime(2020, 1, 1)
        
        with self._get_connection() as conn:
            if HAS_SQLALCHEMY:
                # 获取用户统计
                user_stats = conn.execute(text("""
                    SELECT * FROM user_profit_stats
                    WHERE uid = :uid AND period_type = :period_type AND period_start = :period_start
                """), {"uid": uid, "period_type": period_type, "period_start": period_start}).fetchone()
                
                if not user_stats:
                    return None
                
                user_stats = dict(user_stats._mapping)
                
                # 计算排名 (rank 是 MySQL 保留字，需要反引号)
                # 只统计 show_on_leaderboard = 1 的用户，与排行榜保持一致
                rank_result = conn.execute(text("""
                    SELECT COUNT(*) + 1 as `rank` FROM user_profit_stats p
                    LEFT JOIN user_leaderboard_settings s ON p.uid = s.uid
                    WHERE p.period_type = :period_type
                        AND p.period_start = :period_start
                        AND p.net_profit > :net_profit
                        AND COALESCE(s.show_on_leaderboard, 1) = 1
                """), {
                    "period_type": period_type,
                    "period_start": period_start,
                    "net_profit": user_stats["net_profit"]
                }).fetchone()
                
                user_stats["rank"] = rank_result[0] if rank_result else 0
                return user_stats
            else:
                # SQLite 版本类似
                cursor = conn.execute("""
                    SELECT * FROM user_profit_stats
                    WHERE uid = ? AND period_type = ? AND period_start = ?
                """, (uid, period_type, period_start.isoformat()))
                result = cursor.fetchone()
                
                if not result:
                    return None
                
                user_stats = dict(result)
                
                # 只统计 show_on_leaderboard = 1 的用户，与排行榜保持一致
                cursor = conn.execute("""
                    SELECT COUNT(*) + 1 as `rank` FROM user_profit_stats p
                    LEFT JOIN user_leaderboard_settings s ON p.uid = s.uid
                    WHERE p.period_type = ? AND p.period_start = ? AND p.net_profit > ?
                        AND COALESCE(s.show_on_leaderboard, 1) = 1
                """, (period_type, period_start.isoformat(), user_stats["net_profit"]))
                rank_result = cursor.fetchone()
                
                user_stats["rank"] = rank_result[0] if rank_result else 0
                return user_stats
    
    def delete_user_profit_stats(self, uid: str) -> int:
        """
        删除用户的排行榜统计数据
        
        当用户重置已平仓交易时，应同时调用此方法清除排行榜统计，
        保持数据一致性。
        
        Args:
            uid: 用户 ID
            
        Returns:
            删除的记录数
        """
        with self._get_connection() as conn:
            try:
                if HAS_SQLALCHEMY:
                    result = conn.execute(
                        text("DELETE FROM user_profit_stats WHERE uid = :uid"),
                        {"uid": uid}
                    )
                    deleted = result.rowcount
                else:
                    cursor = conn.execute(
                        "DELETE FROM user_profit_stats WHERE uid = ?",
                        (uid,)
                    )
                    deleted = cursor.rowcount
                
                if deleted > 0:
                    logger.info(f"[{uid}] 已删除 {deleted} 条排行榜统计记录")
                return deleted
            except Exception as e:
                logger.error(f"[{uid}] 删除排行榜统计失败: {e}")
                return 0
    
    def update_leaderboard_settings(
        self,
        uid: str,
        show_on_leaderboard: bool = None,
        display_name: str = None,
        hide_profit_amount: bool = None
    ) -> bool:
        """更新用户排行榜设置"""
        updates = []
        params = {"uid": uid}
        
        if show_on_leaderboard is not None:
            updates.append("show_on_leaderboard = :show")
            params["show"] = 1 if show_on_leaderboard else 0
        if display_name is not None:
            updates.append("display_name = :name")
            params["name"] = display_name
        if hide_profit_amount is not None:
            updates.append("hide_profit_amount = :hide")
            params["hide"] = 1 if hide_profit_amount else 0
        
        if not updates:
            return True
        
        with self._get_connection() as conn:
            try:
                if HAS_SQLALCHEMY:
                    # 先尝试插入
                    conn.execute(text("""
                        INSERT INTO user_leaderboard_settings (uid, show_on_leaderboard, display_name, hide_profit_amount)
                        VALUES (:uid, 1, NULL, 0)
                        ON CONFLICT(uid) DO NOTHING
                    """) if self._is_sqlite() else text("""
                        INSERT IGNORE INTO user_leaderboard_settings (uid, show_on_leaderboard, display_name, hide_profit_amount)
                        VALUES (:uid, 1, NULL, 0)
                    """), {"uid": uid})
                    
                    # 再更新
                    conn.execute(text(f"""
                        UPDATE user_leaderboard_settings
                        SET {', '.join(updates)}, updated_at = CURRENT_TIMESTAMP
                        WHERE uid = :uid
                    """), params)
                else:
                    conn.execute("""
                        INSERT OR IGNORE INTO user_leaderboard_settings (uid)
                        VALUES (?)
                    """, (uid,))
                    
                    # 构建SQLite版本的更新语句
                    sqlite_updates = []
                    sqlite_params = []
                    if show_on_leaderboard is not None:
                        sqlite_updates.append("show_on_leaderboard = ?")
                        sqlite_params.append(1 if show_on_leaderboard else 0)
                    if display_name is not None:
                        sqlite_updates.append("display_name = ?")
                        sqlite_params.append(display_name)
                    if hide_profit_amount is not None:
                        sqlite_updates.append("hide_profit_amount = ?")
                        sqlite_params.append(1 if hide_profit_amount else 0)
                    sqlite_params.append(uid)
                    
                    conn.execute(f"""
                        UPDATE user_leaderboard_settings
                        SET {', '.join(sqlite_updates)}, updated_at = CURRENT_TIMESTAMP
                        WHERE uid = ?
                    """, tuple(sqlite_params))
                
                return True
            except Exception as e:
                logger.error(f"更新排行榜设置失败: {e}")
                return False

    # ============================================================
    # 公开仪表盘设置
    # ============================================================
    
    def get_public_dashboard_settings(self, uid: str) -> Optional[Dict]:
        """获取用户的公开仪表盘设置"""
        with self._get_connection() as conn:
            if HAS_SQLALCHEMY:
                result = conn.execute(text("""
                    SELECT public_dashboard, public_dashboard_token, display_name
                    FROM user_leaderboard_settings WHERE uid = :uid
                """), {"uid": uid}).fetchone()
                
                if result:
                    return {
                        "public_dashboard": bool(result._mapping["public_dashboard"]),
                        "public_dashboard_token": result._mapping["public_dashboard_token"],
                        "display_name": result._mapping["display_name"]
                    }
            else:
                cursor = conn.execute("""
                    SELECT public_dashboard, public_dashboard_token, display_name
                    FROM user_leaderboard_settings WHERE uid = ?
                """, (uid,))
                result = cursor.fetchone()
                
                if result:
                    return {
                        "public_dashboard": bool(result["public_dashboard"]),
                        "public_dashboard_token": result["public_dashboard_token"],
                        "display_name": result["display_name"]
                    }
            
            return {"public_dashboard": False, "public_dashboard_token": None, "display_name": None}
    
    def update_public_dashboard_settings(self, uid: str, enabled: bool) -> Dict:
        """
        更新公开仪表盘设置
        
        Args:
            uid: 用户ID
            enabled: 是否开启
            
        Returns:
            {"success": bool, "token": str or None}
        """
        import secrets
        
        with self._get_connection() as conn:
            try:
                # 如果开启，生成唯一 token
                token = secrets.token_urlsafe(16) if enabled else None
                
                if HAS_SQLALCHEMY:
                    # 检查是否存在记录
                    existing = conn.execute(text(
                        "SELECT id FROM user_leaderboard_settings WHERE uid = :uid"
                    ), {"uid": uid}).fetchone()
                    
                    if existing:
                        conn.execute(text("""
                            UPDATE user_leaderboard_settings
                            SET public_dashboard = :enabled, public_dashboard_token = :token,
                                updated_at = CURRENT_TIMESTAMP
                            WHERE uid = :uid
                        """), {"uid": uid, "enabled": 1 if enabled else 0, "token": token})
                    else:
                        conn.execute(text("""
                            INSERT INTO user_leaderboard_settings (uid, public_dashboard, public_dashboard_token)
                            VALUES (:uid, :enabled, :token)
                        """), {"uid": uid, "enabled": 1 if enabled else 0, "token": token})
                else:
                    cursor = conn.execute(
                        "SELECT id FROM user_leaderboard_settings WHERE uid = ?", (uid,)
                    )
                    existing = cursor.fetchone()
                    
                    if existing:
                        conn.execute("""
                            UPDATE user_leaderboard_settings
                            SET public_dashboard = ?, public_dashboard_token = ?,
                                updated_at = CURRENT_TIMESTAMP
                            WHERE uid = ?
                        """, (1 if enabled else 0, token, uid))
                    else:
                        conn.execute("""
                            INSERT INTO user_leaderboard_settings (uid, public_dashboard, public_dashboard_token)
                            VALUES (?, ?, ?)
                        """, (uid, 1 if enabled else 0, token))
                
                logger.info(f"[{uid}] 公开仪表盘设置已更新: enabled={enabled}")
                return {"success": True, "token": token, "enabled": enabled}
            except Exception as e:
                logger.error(f"更新公开仪表盘设置失败: {e}")
                return {"success": False, "error": str(e)}
    
    def get_user_by_dashboard_token(self, token: str) -> Optional[str]:
        """通过公开仪表盘 token 获取用户 ID"""
        if not token:
            return None
            
        with self._get_connection() as conn:
            if HAS_SQLALCHEMY:
                result = conn.execute(text("""
                    SELECT uid FROM user_leaderboard_settings
                    WHERE public_dashboard_token = :token AND public_dashboard = 1
                """), {"token": token}).fetchone()
                
                return result._mapping["uid"] if result else None
            else:
                cursor = conn.execute("""
                    SELECT uid FROM user_leaderboard_settings
                    WHERE public_dashboard_token = ? AND public_dashboard = 1
                """, (token,))
                result = cursor.fetchone()
                
                return result["uid"] if result else None

    # ============================================================
    # 定时对账：从已平仓交易重新计算统计
    # ============================================================
    
    def reconcile_user_profit_stats(self, uid: str) -> Dict[str, int]:
        """
        对账单个用户的收益统计
        
        从 MySQL closed_trades 表读取已平仓交易，重新计算各周期的统计数据，
        然后覆盖写入 user_profit_stats 表。
        
        Returns:
            {"daily": count, "weekly": count, ...} 各周期的交易数
        """
        from core.pf_compatibility import get_closed_trades_by_exchange
        
        # 获取所有已平仓交易
        closed_by_exchange = get_closed_trades_by_exchange(uid)
        
        if not closed_by_exchange:
            logger.debug(f"[{uid}] 无已平仓交易，跳过对账")
            return {}
        
        now = datetime.utcnow()
        
        # 计算各周期的起始时间
        periods = {
            "daily": now.replace(hour=0, minute=0, second=0, microsecond=0),
            "weekly": (now - timedelta(days=now.weekday())).replace(hour=0, minute=0, second=0, microsecond=0),
            "monthly": now.replace(day=1, hour=0, minute=0, second=0, microsecond=0),
            "all_time": datetime(2020, 1, 1)
        }
        
        # 计算各周期的结束时间
        period_ends = {
            "daily": periods["daily"] + timedelta(days=1),
            "weekly": periods["weekly"] + timedelta(weeks=1),
            "monthly": periods["monthly"].replace(month=periods["monthly"].month % 12 + 1) if periods["monthly"].month < 12 else periods["monthly"].replace(year=periods["monthly"].year + 1, month=1),
            # MySQL TIMESTAMP 最大值是 2038-01-19，使用 2038-01-01
            "all_time": datetime(2038, 1, 1)
        }
        
        # 统计各周期的数据
        stats = {p: {"total_profit": 0, "total_loss": 0, "winning": 0, "losing": 0, "max_profit": 0, "max_loss": 0} for p in periods}
        
        # 遍历所有已平仓交易
        for exchange, trades in closed_by_exchange.items():
            for trade_id, trade in trades.items():
                try:
                    # 获取净盈亏和平仓时间
                    net_pnl = float(trade.get("netPnl", 0))
                    close_time_ms = int(trade.get("closeTimeMs", 0))
                    
                    # M3: closeTimeMs==0 时用 updatedAt 或 openTimeMs 作为 fallback
                    if close_time_ms == 0:
                        close_time_ms = int(trade.get("updatedAt", 0) or 0)
                    if close_time_ms == 0:
                        close_time_ms = int(trade.get("openTimeMs", 0) or 0)
                    if close_time_ms == 0:
                        # 仍然无法确定时间，跳过
                        logger.debug(f"[{uid}] 跳过无时间戳交易: {trade_id}")
                        continue
                    
                    close_time = datetime.utcfromtimestamp(close_time_ms / 1000)
                    
                    # 判断属于哪些周期
                    for period, period_start in periods.items():
                        if close_time >= period_start:
                            s = stats[period]
                            if net_pnl > 0:
                                s["total_profit"] += net_pnl
                                s["winning"] += 1
                                s["max_profit"] = max(s["max_profit"], net_pnl)
                            elif net_pnl < 0:
                                # M1: net_pnl == 0 不计入盈亏，只有 < 0 才算亏损
                                s["total_loss"] += abs(net_pnl)
                                s["losing"] += 1
                                s["max_loss"] = max(s["max_loss"], abs(net_pnl))
                            # net_pnl == 0: 不计入 winning 也不计入 losing（中性交易）
                            # 但仍然计入 total_trades（通过 winning + losing 不包含，需单独计数）
                except Exception as e:
                    logger.warning(f"[{uid}] 处理交易 {trade_id} 失败: {e}")
                    continue
        
        # 更新数据库
        result = {}
        with self._get_connection() as conn:
            for period, s in stats.items():
                total_trades = s["winning"] + s["losing"]
                if total_trades == 0:
                    # M2: 删除旧的零交易行（可能是过期数据）
                    try:
                        if HAS_SQLALCHEMY:
                            conn.execute(text("""
                                DELETE FROM user_profit_stats
                                WHERE uid = :uid AND period_type = :period_type AND period_start = :period_start
                            """), {"uid": uid, "period_type": period, "period_start": periods[period]})
                        else:
                            conn.execute("""
                                DELETE FROM user_profit_stats
                                WHERE uid = ? AND period_type = ? AND period_start = ?
                            """, (uid, period, periods[period].isoformat()))
                    except Exception as e:
                        logger.debug(f"[{uid}] 清理 {period} 零交易行: {e}")
                    result[period] = 0
                    continue
                
                net_profit = s["total_profit"] - s["total_loss"]
                win_rate = s["winning"] / total_trades if total_trades > 0 else 0
                
                try:
                    if HAS_SQLALCHEMY:
                        # H1: SELECT FOR UPDATE 防止并发写入竞争
                        existing = conn.execute(text("""
                            SELECT id FROM user_profit_stats
                            WHERE uid = :uid AND period_type = :period_type AND period_start = :period_start
                            FOR UPDATE
                        """), {"uid": uid, "period_type": period, "period_start": periods[period]}).fetchone()
                        
                        if existing:
                            conn.execute(text("""
                                UPDATE user_profit_stats
                                SET total_profit = :total_profit, total_loss = :total_loss,
                                    net_profit = :net_profit, total_trades = :total_trades,
                                    winning_trades = :winning, losing_trades = :losing,
                                    win_rate = :win_rate, max_profit = :max_profit, max_loss = :max_loss,
                                    updated_at = CURRENT_TIMESTAMP
                                WHERE uid = :uid AND period_type = :period_type AND period_start = :period_start
                            """), {
                                "uid": uid, "period_type": period, "period_start": periods[period],
                                "total_profit": s["total_profit"], "total_loss": s["total_loss"],
                                "net_profit": net_profit, "total_trades": total_trades,
                                "winning": s["winning"], "losing": s["losing"],
                                "win_rate": win_rate, "max_profit": s["max_profit"], "max_loss": s["max_loss"]
                            })
                        else:
                            conn.execute(text("""
                                INSERT INTO user_profit_stats
                                (uid, period_type, period_start, period_end, total_profit, total_loss,
                                 net_profit, total_trades, winning_trades, losing_trades, win_rate,
                                 max_profit, max_loss)
                                VALUES (:uid, :period_type, :period_start, :period_end, :total_profit,
                                        :total_loss, :net_profit, :total_trades, :winning, :losing, :win_rate,
                                        :max_profit, :max_loss)
                            """), {
                                "uid": uid, "period_type": period,
                                "period_start": periods[period], "period_end": period_ends[period],
                                "total_profit": s["total_profit"], "total_loss": s["total_loss"],
                                "net_profit": net_profit, "total_trades": total_trades,
                                "winning": s["winning"], "losing": s["losing"],
                                "win_rate": win_rate, "max_profit": s["max_profit"], "max_loss": s["max_loss"]
                            })
                    else:
                        # SQLite 无 SQLAlchemy 版本
                        period_start_str = periods[period].isoformat()
                        period_end_str = period_ends[period].isoformat()
                        
                        cursor = conn.execute("""
                            SELECT id FROM user_profit_stats
                            WHERE uid = ? AND period_type = ? AND period_start = ?
                        """, (uid, period, period_start_str))
                        existing = cursor.fetchone()
                        
                        if existing:
                            conn.execute("""
                                UPDATE user_profit_stats
                                SET total_profit = ?, total_loss = ?,
                                    net_profit = ?, total_trades = ?,
                                    winning_trades = ?, losing_trades = ?,
                                    win_rate = ?, max_profit = ?, max_loss = ?,
                                    updated_at = CURRENT_TIMESTAMP
                                WHERE uid = ? AND period_type = ? AND period_start = ?
                            """, (s["total_profit"], s["total_loss"], net_profit, total_trades,
                                  s["winning"], s["losing"], win_rate, s["max_profit"], s["max_loss"],
                                  uid, period, period_start_str))
                        else:
                            conn.execute("""
                                INSERT INTO user_profit_stats
                                (uid, period_type, period_start, period_end, total_profit, total_loss,
                                 net_profit, total_trades, winning_trades, losing_trades, win_rate,
                                 max_profit, max_loss)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """, (uid, period, period_start_str, period_end_str,
                                  s["total_profit"], s["total_loss"], net_profit, total_trades,
                                  s["winning"], s["losing"], win_rate, s["max_profit"], s["max_loss"]))
                    
                    result[period] = total_trades
                except Exception as e:
                    logger.error(f"[{uid}] 更新 {period} 统计失败: {e}")
                    result[period] = -1
        
        logger.info(f"[{uid}] 对账完成: {result}")
        return result
    
    def reconcile_all_users(self) -> Dict[str, Dict]:
        """
        对账所有有交易记录的用户（手动触发的数据修复工具）
        
        从 MySQL closed_trades 表获取有交易的用户列表，
        然后重新计算各周期的统计数据。
        
        Returns:
            {uid: {"daily": count, ...}, ...}
        """
        from core.closed_trades_db import get_closed_trades_db
        
        results = {}
        
        # 从 MySQL 获取所有有交易记录的用户
        try:
            db = get_closed_trades_db()
            all_uids = db.get_all_users_with_trades()
            logger.info(f"对账: 从 MySQL 获取到 {len(all_uids)} 个有交易的用户")
        except Exception as e:
            logger.error(f"获取用户列表失败: {e}")
            return results
        
        for uid in all_uids:
            try:
                result = self.reconcile_user_profit_stats(uid)
                if result:  # 只记录有交易的用户
                    results[uid] = result
            except Exception as e:
                logger.error(f"[{uid}] 对账失败: {e}")
                results[uid] = {"error": str(e)}
        
        logger.info(f"全量对账完成，处理用户数: {len(results)}")
        return results


# 全局实例
referral_db = ReferralDB()
