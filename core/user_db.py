# user_db.py
"""
用户数据库访问层

支持 MySQL 和 SQLite（开发环境）
API 密钥使用 AES 加密存储
"""

import os
import json
import hashlib
import base64
from typing import Optional, List, Dict, Any
from dataclasses import asdict
from cryptography.fernet import Fernet
from contextlib import contextmanager
import logging

# 可选：使用 SQLAlchemy 或直接用 pymysql/sqlite3
try:
    from sqlalchemy import create_engine, text
    from sqlalchemy.orm import sessionmaker
    HAS_SQLALCHEMY = True
except ImportError:
    HAS_SQLALCHEMY = False
    import sqlite3

from core.user_context import UserConfig

logger = logging.getLogger(__name__)


class CryptoHelper:
    """API 密钥加密/解密工具"""
    
    def __init__(self, secret_key: str = None):
        """
        Args:
            secret_key: 加密密钥（建议从环境变量读取）
        
        P2 Fix: 移除默认密钥，生产环境必须设置 ENCRYPTION_KEY
        """
        if secret_key is None:
            secret_key = os.getenv("ENCRYPTION_KEY")
        
        if not secret_key:
            _is_production = os.getenv("ENVIRONMENT", "development").lower() == "production"
            if _is_production:
                raise RuntimeError(
                    "生产环境必须设置 ENCRYPTION_KEY 环境变量！"
                    "请使用强随机密钥，如: openssl rand -base64 32"
                )
            else:
                import warnings
                warnings.warn(
                    "ENCRYPTION_KEY 环境变量未设置！使用开发环境默认密钥（不安全）。"
                    "生产环境请设置 ENCRYPTION_KEY 环境变量。",
                    RuntimeWarning
                )
                secret_key = "dev-only-key-do-not-use-in-production"
        
        # 从密钥生成 Fernet key
        key = hashlib.sha256(secret_key.encode()).digest()
        self._fernet = Fernet(base64.urlsafe_b64encode(key))
    
    def encrypt(self, plaintext: str) -> str:
        """加密"""
        return self._fernet.encrypt(plaintext.encode()).decode()
    
    def decrypt(self, ciphertext: str) -> str:
        """解密"""
        return self._fernet.decrypt(ciphertext.encode()).decode()


class UserConfigLoader:
    """
    用户配置加载器
    
    从数据库加载用户配置，支持缓存
    
    P0 Fix: 使用共享数据库引擎，避免连接池碎片化
    """
    
    def __init__(
        self,
        db_url: str = None,
        crypto_key: str = None,
    ):
        """
        Args:
            db_url: 数据库连接字符串
                - MySQL: "mysql+pymysql://user:pass@host/dbname"
            crypto_key: 加密密钥
        """
        self.db_url = db_url or os.getenv("DATABASE_URL")
        
        if not self.db_url:
            raise ValueError(
                "DATABASE_URL 环境变量未设置！请设置 MySQL 连接字符串，例如：\n"
                "DATABASE_URL=mysql+pymysql://user:pass@host:3306/dbname?charset=utf8mb4"
            )
        
        self.crypto = CryptoHelper(crypto_key)
        
        # P0 Fix: 使用共享数据库引擎
        if HAS_SQLALCHEMY:
            from core.shared_db_engine import get_shared_engine, get_shared_session
            self.engine = get_shared_engine(self.db_url)
            self.Session = get_shared_session()
        else:
            # 简单的 SQLite 支持
            self._sqlite_path = self.db_url.replace("sqlite:///", "")
        
        # 配置缓存
        self._cache: Dict[str, UserConfig] = {}
        self._cache_ttl = 300  # 5分钟缓存
        self._cache_ts: Dict[str, float] = {}
        

        
        # 安全日志：隐藏密码
        safe_url = self.db_url.split('@')[-1] if '@' in self.db_url else self.db_url
        logger.info(f"UserConfigLoader 已初始化: {safe_url}")
    
    @contextmanager
    def _get_connection(self):
        """获取数据库连接"""
        import time as _time
        _conn_start = _time.perf_counter()
        
        if HAS_SQLALCHEMY:
            session = self.Session()
            _conn_elapsed = (_time.perf_counter() - _conn_start) * 1000
            if _conn_elapsed > 100:
                logger.warning(f"[DB SLOW] 获取连接耗时: {_conn_elapsed:.1f}ms")
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
    

    
    def user_exists(self, uid: str) -> bool:
        """
        检查用户是否存在且状态为 active
        
        Args:
            uid: 用户ID
        
        Returns:
            True 如果用户存在且状态为 active，否则 False
        """
        try:
            with self._get_connection() as conn:
                if HAS_SQLALCHEMY:
                    result = conn.execute(text(
                        "SELECT 1 FROM users WHERE uid = :uid AND status = 'active'"
                    ), {"uid": uid}).fetchone()
                else:
                    result = conn.execute(
                        "SELECT 1 FROM users WHERE uid = ? AND status = 'active'",
                        (uid,)
                    ).fetchone()
                return result is not None
        except Exception as e:
            logger.error(f"检查用户存在性失败: {e}")
            return False
    
    def load(self, uid: str, use_cache: bool = True) -> Optional[UserConfig]:
        """
        加载用户配置
        
        Args:
            uid: 用户ID
            use_cache: 是否使用缓存
        
        Returns:
            UserConfig 或 None
        """
        import time
        
        # 检查缓存
        if use_cache and uid in self._cache:
            if time.time() - self._cache_ts.get(uid, 0) < self._cache_ttl:
                return self._cache[uid]
        
        try:
            with self._get_connection() as conn:
                # 查询用户基本信息（不再 JOIN user_api_keys）
                if HAS_SQLALCHEMY:
                    result = conn.execute(text("""
                        SELECT u.uid, u.status, u.tier,
                               c.max_positions, c.position_size_pct, c.default_leverage,
                               c.monitor_symbols, c.ai_enabled, c.ai500_enabled,
                               c.llm_provider, c.llm_model, c.llm_api_key_encrypted,
                               c.llm_base_url, c.temperature, c.max_tokens,
                               c.telegram_bot_token_encrypted, c.telegram_chat_id, c.telegram_topic_id, c.telegram_enabled,
                               c.min_rr_ratio, c.limit_order_min_distance_pct
                        FROM users u
                        LEFT JOIN user_trading_config c ON u.uid = c.uid
                        WHERE u.uid = :uid AND u.status = 'active'
                    """), {"uid": uid}).fetchone()
                else:
                    cursor = conn.execute("""
                        SELECT u.uid, u.status, u.tier,
                               c.max_positions, c.position_size_pct, c.default_leverage,
                               c.monitor_symbols, c.ai_enabled, c.ai500_enabled,
                               c.llm_provider, c.llm_model, c.llm_api_key_encrypted,
                               c.llm_base_url, c.temperature, c.max_tokens,
                               c.telegram_bot_token_encrypted, c.telegram_chat_id, c.telegram_topic_id, c.telegram_enabled,
                               c.min_rr_ratio, c.limit_order_min_distance_pct
                        FROM users u
                        LEFT JOIN user_trading_config c ON u.uid = c.uid
                        WHERE u.uid = ? AND u.status = 'active'
                    """, (uid,))
                    result = cursor.fetchone()
                
                if not result:
                    return None
                
                # SQLAlchemy Row 需要转换为 dict
                if HAS_SQLALCHEMY:
                    result = result._mapping
                
                # 解析 monitor_symbols
                monitor_symbols = []
                if result["monitor_symbols"]:
                    try:
                        monitor_symbols = json.loads(result["monitor_symbols"])
                    except Exception as e:
                        logger.error(f"Error parsing monitor_symbols for uid {uid}: {e}")
                        pass
                
                # 解密 LLM API Key（如果有）
                llm_api_key = None
                if result["llm_api_key_encrypted"]:
                    try:
                        llm_api_key = self.crypto.decrypt(result["llm_api_key_encrypted"])
                    except Exception as e:
                        logger.error(f"Error decrypting llm_api_key for uid {uid}: {e}")
                        pass
                
                # 获取启用的交易所列表
                enabled_exchanges = self.get_enabled_exchanges(uid)
                
                config = UserConfig(
                    uid=result["uid"],
                    max_positions=result["max_positions"] or 5,
                    position_size_pct=float(result["position_size_pct"] or 3.0),
                    default_leverage=result["default_leverage"] or 20,
                    monitor_symbols=monitor_symbols,
                    ai500_enabled=bool(result["ai500_enabled"]) if result.get("ai500_enabled") else False,
                    ai_enabled=bool(result["ai_enabled"]) if result["ai_enabled"] is not None else True,
                    # LLM 配置
                    llm_provider=result["llm_provider"] or "anthropic",
                    llm_model=result["llm_model"] or "claude-sonnet-4-20250514",
                    llm_api_key=llm_api_key,
                    llm_base_url=result["llm_base_url"],
                    # system_prompt 已迁移到 user_strategy_configs 表
                    # temperature 已废弃，由 llm_client 根据提供商自动设置
                    max_tokens=int(result["max_tokens"] or 65536),
                    # 通知配置
                    telegram_bot_token=self._decrypt(result["telegram_bot_token_encrypted"]) if result.get("telegram_bot_token_encrypted") else None,
                    telegram_chat_id=result["telegram_chat_id"],
                    telegram_topic_id=result.get("telegram_topic_id"),
                    telegram_enabled=bool(result["telegram_enabled"]) if result["telegram_enabled"] else False,
                    tier=result["tier"] or "free",
                    # 执行约束配置
                    min_rr_ratio=float(result.get("min_rr_ratio") or 3.0),
                    limit_order_min_distance_pct=float(result.get("limit_order_min_distance_pct") or 3.0),
                    # 启用的交易所
                    enabled_exchanges=enabled_exchanges,
                )
                
                # 更新缓存
                self._cache[uid] = config
                self._cache_ts[uid] = time.time()
                
                return config
                
        except Exception as e:
            logger.error(f"加载用户配置失败 [{uid}]: {e}")
            return None
    
    def get_all_active_uids(self) -> List[str]:
        """获取所有活跃且已启用交易的用户ID"""
        try:
            with self._get_connection() as conn:
                if HAS_SQLALCHEMY:
                    # 只返回 trading_enabled=1 的用户
                    result = conn.execute(text("""
                        SELECT u.uid FROM users u
                        LEFT JOIN user_trading_config c ON u.uid = c.uid
                        WHERE u.status = 'active' AND COALESCE(c.trading_enabled, 0) = 1
                    """)).fetchall()
                    # SQLAlchemy Row 对象可以用索引或 _mapping 访问
                    return [r[0] for r in result]
                else:
                    cursor = conn.execute("""
                        SELECT u.uid FROM users u
                        LEFT JOIN user_trading_config c ON u.uid = c.uid
                        WHERE u.status = 'active' AND COALESCE(c.trading_enabled, 0) = 1
                    """)
                    return [r["uid"] for r in cursor.fetchall()]
        except Exception as e:
            logger.error(f"获取活跃用户列表失败: {e}")
            return []
    
    def load_batch(self, uids: List[str]) -> Dict[str, UserConfig]:
        """
        批量加载用户配置（优化 N+1 查询问题）
        
        1000+ 用户规模优化：
        - 原实现：每用户 2 次查询（load + get_enabled_exchanges）
        - 优化后：2 次批量查询（用户配置 + 交易所配置）
        
        Args:
            uids: 用户ID列表
        
        Returns:
            {uid: UserConfig, ...}
        """
        import time
        
        if not uids:
            return {}
        
        result: Dict[str, UserConfig] = {}
        uids_to_load = []
        
        # 检查缓存
        for uid in uids:
            if uid in self._cache:
                if time.time() - self._cache_ts.get(uid, 0) < self._cache_ttl:
                    result[uid] = self._cache[uid]
                    continue
            uids_to_load.append(uid)
        
        if not uids_to_load:
            return result
        
        try:
            with self._get_connection() as conn:
                # 1. 批量获取用户配置
                user_configs = self._batch_load_user_configs(conn, uids_to_load)
                
                # 2. 批量获取交易所配置
                exchange_configs = self._batch_load_exchange_configs(conn, uids_to_load)
                
                # 3. 组装 UserConfig
                for uid in uids_to_load:
                    user_data = user_configs.get(uid)
                    if not user_data:
                        continue
                    
                    enabled_exchanges = exchange_configs.get(uid, [])
                    
                    config = self._build_user_config(user_data, enabled_exchanges)
                    if config:
                        result[uid] = config
                        self._cache[uid] = config
                        self._cache_ts[uid] = time.time()
                
                logger.info(f"批量加载用户配置完成: {len(result)}/{len(uids)} 成功")
                return result
                
        except Exception as e:
            logger.error(f"批量加载用户配置失败: {e}")
            # 回退到单个加载
            for uid in uids_to_load:
                config = self.load(uid)
                if config:
                    result[uid] = config
            return result
    
    def _batch_load_user_configs(self, conn, uids: List[str]) -> Dict[str, dict]:
        """批量加载用户基本配置"""
        if not HAS_SQLALCHEMY:
            # SQLite 不支持 IN 参数化，回退到单个查询
            result = {}
            for uid in uids:
                config = self.load(uid, use_cache=False)
                if config:
                    result[uid] = {"config": config}
            return result
        
        # 构建 IN 子句
        placeholders = ", ".join([f":uid_{i}" for i in range(len(uids))])
        params = {f"uid_{i}": uid for i, uid in enumerate(uids)}
        
        query = text(f"""
            SELECT u.uid, u.status, u.tier,
                   c.max_positions, c.position_size_pct, c.default_leverage,
                   c.monitor_symbols, c.ai_enabled, c.ai500_enabled,
                   c.llm_provider, c.llm_model, c.llm_api_key_encrypted,
                   c.llm_base_url, c.system_prompt, c.temperature, c.max_tokens,
                   c.telegram_bot_token_encrypted, c.telegram_chat_id, 
                   c.telegram_topic_id, c.telegram_enabled,
                   c.min_rr_ratio, c.limit_order_min_distance_pct
            FROM users u
            LEFT JOIN user_trading_config c ON u.uid = c.uid
            WHERE u.uid IN ({placeholders}) AND u.status = 'active'
        """)
        
        rows = conn.execute(query, params).fetchall()
        
        result = {}
        for row in rows:
            row_dict = row._mapping
            result[row_dict["uid"]] = dict(row_dict)
        
        return result
    
    def _batch_load_exchange_configs(self, conn, uids: List[str]) -> Dict[str, List[str]]:
        """批量加载用户启用的交易所列表"""
        if not HAS_SQLALCHEMY:
            result = {}
            for uid in uids:
                result[uid] = self.get_enabled_exchanges(uid)
            return result
        
        placeholders = ", ".join([f":uid_{i}" for i in range(len(uids))])
        params = {f"uid_{i}": uid for i, uid in enumerate(uids)}
        
        query = text(f"""
            SELECT uid, exchange FROM user_exchange_config
            WHERE uid IN ({placeholders}) 
              AND is_enabled = 1 
              AND api_key_encrypted IS NOT NULL
        """)
        
        rows = conn.execute(query, params).fetchall()
        
        result: Dict[str, List[str]] = {uid: [] for uid in uids}
        for row in rows:
            uid = row[0]
            exchange = row[1]
            if uid in result:
                result[uid].append(exchange)
        
        return result
    
    def _build_user_config(self, data: dict, enabled_exchanges: List[str]) -> Optional[UserConfig]:
        """从数据字典构建 UserConfig"""
        try:
            # 解析 monitor_symbols
            monitor_symbols = []
            if data.get("monitor_symbols"):
                try:
                    monitor_symbols = json.loads(data["monitor_symbols"])
                except Exception as e:
                    logger.debug(f"[{data.get('uid')}] 解析 monitor_symbols 失败: {e}")
            
            # 解密 LLM API Key
            llm_api_key = None
            if data.get("llm_api_key_encrypted"):
                try:
                    llm_api_key = self.crypto.decrypt(data["llm_api_key_encrypted"])
                except Exception as e:
                    logger.debug(f"[{data.get('uid')}] 解密 LLM API Key 失败: {e}")
            
            return UserConfig(
                uid=data["uid"],
                max_positions=data.get("max_positions") or 5,
                position_size_pct=float(data.get("position_size_pct") or 3.0),
                default_leverage=data.get("default_leverage") or 20,
                monitor_symbols=monitor_symbols,
                ai500_enabled=bool(data.get("ai500_enabled")),
                ai_enabled=bool(data.get("ai_enabled")) if data.get("ai_enabled") is not None else True,
                llm_provider=data.get("llm_provider") or "anthropic",
                llm_model=data.get("llm_model") or "claude-sonnet-4-20250514",
                llm_api_key=llm_api_key,
                llm_base_url=data.get("llm_base_url"),
                # system_prompt 已迁移到 user_ai_strategies，不再加载到 UserConfig
                # temperature 已废弃，由 llm_client 根据提供商自动设置
                max_tokens=int(data.get("max_tokens") or 65536),
                telegram_bot_token=self._decrypt(data.get("telegram_bot_token_encrypted")),
                telegram_chat_id=data.get("telegram_chat_id"),
                telegram_topic_id=data.get("telegram_topic_id"),
                telegram_enabled=bool(data.get("telegram_enabled")),
                tier=data.get("tier") or "free",
                min_rr_ratio=float(data.get("min_rr_ratio") or 3.0),
                limit_order_min_distance_pct=float(data.get("limit_order_min_distance_pct") or 3.0),
                enabled_exchanges=enabled_exchanges,
            )
        except Exception as e:
            logger.error(f"构建 UserConfig 失败 [{data.get('uid')}]: {e}")
            return None
    
    def set_trading_enabled(self, uid: str, enabled: bool) -> bool:
        """设置用户交易启用状态（持久化）"""
        try:
            with self._get_connection() as conn:
                if HAS_SQLALCHEMY:
                    conn.execute(text("""
                        UPDATE user_trading_config SET trading_enabled = :enabled WHERE uid = :uid
                    """), {"enabled": 1 if enabled else 0, "uid": uid})
                else:
                    conn.execute("""
                        UPDATE user_trading_config SET trading_enabled = ? WHERE uid = ?
                    """, (1 if enabled else 0, uid))
            
            # 清除缓存
            self._cache.pop(uid, None)
            logger.info(f"[{uid}] 交易启用状态已设置为 {enabled}")
            return True
        except Exception as e:
            logger.error(f"设置交易启用状态失败 [{uid}]: {e}")
            return False
    
    def is_trading_enabled(self, uid: str) -> bool:
        """检查用户交易是否启用"""
        try:
            with self._get_connection() as conn:
                if HAS_SQLALCHEMY:
                    result = conn.execute(text("""
                        SELECT trading_enabled FROM user_trading_config WHERE uid = :uid
                    """), {"uid": uid}).fetchone()
                    return bool(result[0]) if result else False
                else:
                    cursor = conn.execute("""
                        SELECT trading_enabled FROM user_trading_config WHERE uid = ?
                    """, (uid,))
                    result = cursor.fetchone()
                    return bool(result["trading_enabled"]) if result else False
        except Exception as e:
            logger.error(f"检查交易启用状态失败 [{uid}]: {e}")
            return False
    
    def _is_sqlite(self) -> bool:
        """检查是否使用 SQLite"""
        return "sqlite" in self.db_url.lower()
    
    def init_tables(self):
        """初始化数据库表（包含所有列）"""
        is_sqlite = self._is_sqlite()
        
        with self._get_connection() as conn:
            if is_sqlite:
                # SQLite 语法
                if HAS_SQLALCHEMY:
                    conn.execute(text("""
                        CREATE TABLE IF NOT EXISTS users (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            uid TEXT UNIQUE NOT NULL,
                            username TEXT UNIQUE NOT NULL,
                            password_hash TEXT NOT NULL,
                            email TEXT,
                            totp_secret_encrypted TEXT,
                            status TEXT DEFAULT 'active',
                            tier TEXT DEFAULT 'free',
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        )
                    """))
                    conn.execute(text("""
                        CREATE TABLE IF NOT EXISTS user_exchange_config (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            uid TEXT NOT NULL,
                            exchange TEXT NOT NULL,
                            api_key_encrypted TEXT,
                            api_secret_encrypted TEXT,
                            passphrase_encrypted TEXT,
                            wallet_address TEXT,
                            is_testnet INTEGER DEFAULT 0,
                            is_enabled INTEGER DEFAULT 0,
                            is_running INTEGER DEFAULT 0,
                            leverage INTEGER DEFAULT 20,
                            monitor_symbols TEXT,
                            ai500_enabled INTEGER DEFAULT 0,
                            ai_strategy_id TEXT,
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            UNIQUE(uid, exchange)
                        )
                    """))
                    conn.execute(text("""
                        CREATE TABLE IF NOT EXISTS user_trading_config (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            uid TEXT UNIQUE NOT NULL,
                            max_positions INTEGER DEFAULT 30,
                            position_size_pct REAL DEFAULT 3.0,
                            default_leverage INTEGER DEFAULT 30,
                            monitor_symbols TEXT,
                            ai_enabled INTEGER DEFAULT 0,
                            ai500_enabled INTEGER DEFAULT 0,
                            trading_enabled INTEGER DEFAULT 0,
                            llm_provider TEXT DEFAULT 'anthropic',
                            llm_model TEXT DEFAULT '',
                            llm_api_key_encrypted TEXT,
                            llm_base_url TEXT,
                            system_prompt TEXT,
                            temperature REAL DEFAULT 0.01,
                            max_tokens INTEGER DEFAULT 65536,
                            telegram_bot_token_encrypted TEXT,
                            telegram_chat_id TEXT,
                            telegram_topic_id TEXT,
                            telegram_enabled INTEGER DEFAULT 0,
                            min_rr_ratio REAL DEFAULT 3.0,
                            limit_order_min_distance_pct REAL DEFAULT 3.0
                        )
                    """))
                    conn.execute(text("""
                        CREATE TABLE IF NOT EXISTS user_ai_strategies (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            uid TEXT NOT NULL,
                            strategy_id TEXT NOT NULL,
                            name TEXT NOT NULL,
                            llm_provider TEXT DEFAULT 'anthropic',
                            llm_model TEXT DEFAULT '',
                            llm_api_key_encrypted TEXT,
                            llm_base_url TEXT,
                            temperature REAL DEFAULT NULL,
                            top_p REAL DEFAULT NULL,
                            max_tokens INTEGER DEFAULT NULL,
                            strategy_preset TEXT DEFAULT 'default',
                            strategy_overrides TEXT,
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            UNIQUE(uid, strategy_id)
                        )
                    """))
                else:
                    # 原生 sqlite3
                    conn.execute("""
                        CREATE TABLE IF NOT EXISTS users (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            uid TEXT UNIQUE NOT NULL,
                            username TEXT UNIQUE NOT NULL,
                            password_hash TEXT NOT NULL,
                            email TEXT,
                            totp_secret_encrypted TEXT,
                            status TEXT DEFAULT 'active',
                            tier TEXT DEFAULT 'free',
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        )
                    """)
                    conn.execute("""
                        CREATE TABLE IF NOT EXISTS user_exchange_config (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            uid TEXT NOT NULL,
                            exchange TEXT NOT NULL,
                            api_key_encrypted TEXT,
                            api_secret_encrypted TEXT,
                            passphrase_encrypted TEXT,
                            wallet_address TEXT,
                            is_testnet INTEGER DEFAULT 0,
                            is_enabled INTEGER DEFAULT 0,
                            is_running INTEGER DEFAULT 0,
                            leverage INTEGER DEFAULT 20,
                            monitor_symbols TEXT,
                            ai500_enabled INTEGER DEFAULT 0,
                            ai_strategy_id TEXT,
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            UNIQUE(uid, exchange)
                        )
                    """)
                    conn.execute("""
                        CREATE TABLE IF NOT EXISTS user_trading_config (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            uid TEXT UNIQUE NOT NULL,
                            max_positions INTEGER DEFAULT 30,
                            position_size_pct REAL DEFAULT 3.0,
                            default_leverage INTEGER DEFAULT 30,
                            monitor_symbols TEXT,
                            ai_enabled INTEGER DEFAULT 0,
                            ai500_enabled INTEGER DEFAULT 0,
                            trading_enabled INTEGER DEFAULT 0,
                            llm_provider TEXT DEFAULT 'anthropic',
                            llm_model TEXT DEFAULT '',
                            llm_api_key_encrypted TEXT,
                            llm_base_url TEXT,
                            system_prompt TEXT,
                            temperature REAL DEFAULT 0.01,
                            max_tokens INTEGER DEFAULT 65536,
                            telegram_bot_token_encrypted TEXT,
                            telegram_chat_id TEXT,
                            telegram_topic_id TEXT,
                            telegram_enabled INTEGER DEFAULT 0,
                            min_rr_ratio REAL DEFAULT 3.0,
                            limit_order_min_distance_pct REAL DEFAULT 3.0
                        )
                    """)
                    conn.execute("""
                        CREATE TABLE IF NOT EXISTS user_ai_strategies (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            uid TEXT NOT NULL,
                            strategy_id TEXT NOT NULL,
                            name TEXT NOT NULL,
                            llm_provider TEXT DEFAULT 'anthropic',
                            llm_model TEXT DEFAULT '',
                            llm_api_key_encrypted TEXT,
                            llm_base_url TEXT,
                            temperature REAL DEFAULT NULL,
                            top_p REAL DEFAULT NULL,
                            max_tokens INTEGER DEFAULT NULL,
                            strategy_preset TEXT DEFAULT 'default',
                            strategy_overrides TEXT,
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            UNIQUE(uid, strategy_id)
                        )
                    """)
            else:
                # MySQL 语法
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS users (
                        id INT PRIMARY KEY AUTO_INCREMENT,
                        uid VARCHAR(32) UNIQUE NOT NULL,
                        username VARCHAR(64) UNIQUE NOT NULL,
                        password_hash VARCHAR(128) NOT NULL,
                        email VARCHAR(128),
                        totp_secret_encrypted TEXT,
                        status VARCHAR(16) DEFAULT 'active',
                        tier VARCHAR(16) DEFAULT 'free',
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """))
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS user_exchange_config (
                        id INT PRIMARY KEY AUTO_INCREMENT,
                        uid VARCHAR(32) NOT NULL,
                        exchange VARCHAR(32) NOT NULL,
                        api_key_encrypted TEXT,
                        api_secret_encrypted TEXT,
                        passphrase_encrypted TEXT,
                        wallet_address VARCHAR(128),
                        is_testnet TINYINT DEFAULT 0,
                        is_enabled TINYINT DEFAULT 0,
                        is_running TINYINT DEFAULT 0,
                        leverage INT DEFAULT 20,
                        monitor_symbols TEXT,
                        ai500_enabled TINYINT DEFAULT 0,
                        ai_strategy_id VARCHAR(32),
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                        UNIQUE KEY unique_uid_exchange (uid, exchange)
                    )
                """))
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS user_trading_config (
                        id INT PRIMARY KEY AUTO_INCREMENT,
                        uid VARCHAR(32) UNIQUE NOT NULL,
                        max_positions INT DEFAULT 30,
                        position_size_pct DECIMAL(5,2) DEFAULT 3.0,
                        default_leverage INT DEFAULT 30,
                        monitor_symbols TEXT,
                        ai_enabled TINYINT DEFAULT 0,
                        ai500_enabled TINYINT DEFAULT 0,
                        trading_enabled TINYINT DEFAULT 0,
                        llm_provider VARCHAR(32) DEFAULT 'anthropic',
                        llm_model VARCHAR(64) DEFAULT '',
                        llm_api_key_encrypted TEXT,
                        llm_base_url VARCHAR(256),
                        system_prompt TEXT,
                        temperature DECIMAL(4,3) DEFAULT 0.01,
                        max_tokens INT DEFAULT 65536,
                        telegram_bot_token_encrypted TEXT,
                        telegram_chat_id VARCHAR(64),
                        telegram_topic_id VARCHAR(32),
                        telegram_enabled TINYINT DEFAULT 0,
                        min_rr_ratio DECIMAL(5,2) DEFAULT 3.0,
                        limit_order_min_distance_pct DECIMAL(5,2) DEFAULT 3.0
                    )
                """))
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS user_ai_strategies (
                        id INT PRIMARY KEY AUTO_INCREMENT,
                        uid VARCHAR(32) NOT NULL,
                        strategy_id VARCHAR(32) NOT NULL,
                        name VARCHAR(64) NOT NULL,
                        llm_provider VARCHAR(32) DEFAULT 'anthropic',
                        llm_model VARCHAR(64) DEFAULT '',
                        llm_api_key_encrypted TEXT,
                        llm_base_url VARCHAR(256),
                        temperature DECIMAL(3,2) DEFAULT NULL,
                        top_p DECIMAL(3,2) DEFAULT NULL,
                        max_tokens INT DEFAULT NULL,
                        strategy_preset VARCHAR(64) DEFAULT 'default',
                        strategy_overrides LONGTEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                        UNIQUE KEY unique_uid_strategy (uid, strategy_id)
                    )
                """))
            
            # 创建系统设置表
            self._init_system_settings_table(conn, is_sqlite)
        
        logger.info("数据库表已初始化")
    
    
    def _init_system_settings_table(self, conn, is_sqlite: bool):
        """初始化系统设置表"""
        try:
            if is_sqlite:
                if HAS_SQLALCHEMY:
                    conn.execute(text("""
                        CREATE TABLE IF NOT EXISTS system_settings (
                            key TEXT PRIMARY KEY,
                            value TEXT,
                            description TEXT,
                            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        )
                    """))
                else:
                    conn.execute("""
                        CREATE TABLE IF NOT EXISTS system_settings (
                            key TEXT PRIMARY KEY,
                            value TEXT,
                            description TEXT,
                            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        )
                    """)
            else:
                # MySQL
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS system_settings (
                        `key` VARCHAR(64) PRIMARY KEY,
                        value TEXT,
                        description TEXT,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                    )
                """))
            
            # 插入默认设置（如果不存在）
            self._init_default_settings(conn, is_sqlite)
            
            # 迁移：添加 is_running 字段（如果不存在）
            self._migrate_add_is_running(conn, is_sqlite)
            
            # 迁移：添加 execution_constraints 字段（如果不存在）
            self._migrate_add_execution_constraints(conn, is_sqlite)
            
            # 迁移：添加 user_ai_strategies LLM 参数字段（如果不存在）
            self._migrate_add_llm_params(conn, is_sqlite)
            
            # 迁移：添加 v5.0 策略相关表和字段
            self._migrate_add_strategy_tables(conn, is_sqlite)
            self._migrate_add_strategy_preset_fields(conn, is_sqlite)
            
            # 迁移：创建 llm_models 表
            self._migrate_add_llm_models_table(conn, is_sqlite)
            
            # 迁移：创建 trade autopsy 相关表 (Phase 4)
            self._init_trade_autopsy_tables(conn, is_sqlite)
            
        except Exception as e:
            logger.warning(f"初始化系统设置表失败: {e}")
    
    def _init_trade_autopsy_tables(self, conn, is_sqlite: bool):
        """初始化交易尸检相关表"""
        try:
            if is_sqlite:
                if HAS_SQLALCHEMY:
                    # Trade snapshots table
                    conn.execute(text("""
                        CREATE TABLE IF NOT EXISTS trade_snapshots (
                            trade_id VARCHAR(64) PRIMARY KEY,
                            timestamp DATETIME NOT NULL,
                            symbol VARCHAR(32) NOT NULL,
                            side VARCHAR(16) NOT NULL,
                            entry_price DECIMAL(20, 8) NOT NULL,
                            decision VARCHAR(32) NOT NULL,
                            confidence DECIMAL(5, 4),
                            reasoning_json TEXT,
                            bias_score INT,
                            bias_direction VARCHAR(16),
                            candle_type VARCHAR(32),
                            rejection_signals_json TEXT,
                            close_position_json TEXT,
                            vs_levels_json TEXT,
                            quick_checks_status VARCHAR(32),
                            had_critical_alert INTEGER,
                            had_high_alert INTEGER,
                            alerts_triggered_json TEXT,
                            btc_state VARCHAR(32),
                            eth_state VARCHAR(32),
                            regime VARCHAR(32),
                            exit_price DECIMAL(20, 8),
                            pnl_pct DECIMAL(10, 4),
                            result VARCHAR(16),
                            hold_duration_minutes INT,
                            failure_type VARCHAR(64),
                            failure_analysis TEXT,
                            lessons_learned TEXT,
                            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
                        )
                    """))
                    conn.execute(text("CREATE INDEX IF NOT EXISTS idx_ts_symbol ON trade_snapshots(symbol)"))
                    conn.execute(text("CREATE INDEX IF NOT EXISTS idx_ts_result ON trade_snapshots(result)"))
                    conn.execute(text("CREATE INDEX IF NOT EXISTS idx_ts_timestamp ON trade_snapshots(timestamp)"))
                    
                    # Failure patterns table
                    conn.execute(text("""
                        CREATE TABLE IF NOT EXISTS failure_patterns (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            pattern_type VARCHAR(64) NOT NULL,
                            description TEXT NOT NULL,
                            conditions_json TEXT NOT NULL,
                            occurrence_count INT DEFAULT 0,
                            win_rate_pct DECIMAL(6, 2) DEFAULT 0,
                            avg_pnl_pct DECIMAL(10, 4) DEFAULT 0,
                            first_seen DATETIME,
                            last_seen DATETIME,
                            is_active INTEGER DEFAULT 1,
                            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                        )
                    """))
                    conn.execute(text("CREATE INDEX IF NOT EXISTS idx_fp_type ON failure_patterns(pattern_type)"))
                    
                    # Weekly reports table
                    conn.execute(text("""
                        CREATE TABLE IF NOT EXISTS weekly_reports (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            week_start DATE NOT NULL,
                            week_end DATE NOT NULL,
                            total_trades INT DEFAULT 0,
                            wins INT DEFAULT 0,
                            losses INT DEFAULT 0,
                            win_rate_pct DECIMAL(6, 2) DEFAULT 0,
                            total_pnl_pct DECIMAL(10, 4) DEFAULT 0,
                            failures_analyzed INT DEFAULT 0,
                            patterns_identified_json TEXT,
                            recommendations_json TEXT,
                            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                        )
                    """))
                    conn.execute(text("CREATE INDEX IF NOT EXISTS idx_wr_week ON weekly_reports(week_start)"))
                conn.commit()
                logger.info("Trade autopsy tables initialized (SQLite)")
            else:
                # MySQL 语法
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS trade_snapshots (
                        trade_id VARCHAR(64) PRIMARY KEY,
                        timestamp DATETIME NOT NULL,
                        symbol VARCHAR(32) NOT NULL,
                        side VARCHAR(16) NOT NULL,
                        entry_price DECIMAL(20, 8) NOT NULL,
                        decision VARCHAR(32) NOT NULL,
                        confidence DECIMAL(5, 4),
                        reasoning_json LONGTEXT,
                        bias_score INT,
                        bias_direction VARCHAR(16),
                        candle_type VARCHAR(32),
                        rejection_signals_json LONGTEXT,
                        close_position_json LONGTEXT,
                        vs_levels_json LONGTEXT,
                        quick_checks_status VARCHAR(32),
                        had_critical_alert TINYINT,
                        had_high_alert TINYINT,
                        alerts_triggered_json LONGTEXT,
                        btc_state VARCHAR(32),
                        eth_state VARCHAR(32),
                        regime VARCHAR(32),
                        exit_price DECIMAL(20, 8),
                        pnl_pct DECIMAL(10, 4),
                        result VARCHAR(16),
                        hold_duration_minutes INT,
                        failure_type VARCHAR(64),
                        failure_analysis LONGTEXT,
                        lessons_learned LONGTEXT,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                        INDEX idx_symbol (symbol),
                        INDEX idx_result (result),
                        INDEX idx_timestamp (timestamp)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """))
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS failure_patterns (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        pattern_type VARCHAR(64) NOT NULL,
                        description TEXT NOT NULL,
                        conditions_json LONGTEXT NOT NULL,
                        occurrence_count INT DEFAULT 0,
                        win_rate_pct DECIMAL(6, 2) DEFAULT 0,
                        avg_pnl_pct DECIMAL(10, 4) DEFAULT 0,
                        first_seen DATETIME,
                        last_seen DATETIME,
                        is_active TINYINT DEFAULT 1,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                        INDEX idx_pattern_type (pattern_type)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """))
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS weekly_reports (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        week_start DATE NOT NULL,
                        week_end DATE NOT NULL,
                        total_trades INT DEFAULT 0,
                        wins INT DEFAULT 0,
                        losses INT DEFAULT 0,
                        win_rate_pct DECIMAL(6, 2) DEFAULT 0,
                        total_pnl_pct DECIMAL(10, 4) DEFAULT 0,
                        failures_analyzed INT DEFAULT 0,
                        patterns_identified_json LONGTEXT,
                        recommendations_json LONGTEXT,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                        INDEX idx_week (week_start)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """))
                conn.commit()
                logger.info("Trade autopsy tables initialized (MySQL)")
        except Exception as e:
            logger.warning(f"初始化 trade autopsy 表失败: {e}")
    
    def _migrate_add_is_running(self, conn, is_sqlite: bool):
        """迁移：为 user_exchange_config 添加 is_running 字段"""
        try:
            if HAS_SQLALCHEMY:
                # 检查字段是否存在
                if is_sqlite:
                    result = conn.execute(text(
                        "PRAGMA table_info(user_exchange_config)"
                    )).fetchall()
                    columns = [row[1] for row in result]
                else:
                    result = conn.execute(text("""
                        SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS 
                        WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'user_exchange_config' AND COLUMN_NAME = 'is_running'
                    """)).fetchone()
                    columns = ['is_running'] if result else []
                
                if 'is_running' not in columns:
                    if is_sqlite:
                        conn.execute(text(
                            "ALTER TABLE user_exchange_config ADD COLUMN is_running INTEGER DEFAULT 0"
                        ))
                    else:
                        conn.execute(text(
                            "ALTER TABLE user_exchange_config ADD COLUMN is_running TINYINT DEFAULT 0"
                        ))
                    logger.info("已添加 is_running 字段到 user_exchange_config 表")
            else:
                # 原生 sqlite3
                result = conn.execute("PRAGMA table_info(user_exchange_config)").fetchall()
                columns = [row[1] for row in result]
                if 'is_running' not in columns:
                    conn.execute(
                        "ALTER TABLE user_exchange_config ADD COLUMN is_running INTEGER DEFAULT 0"
                    )
                    logger.info("已添加 is_running 字段到 user_exchange_config 表")
        except Exception as e:
            logger.debug(f"迁移 is_running 字段时出错（可能已存在）: {e}")
    
    def _migrate_add_execution_constraints(self, conn, is_sqlite: bool):
        """迁移：为 user_trading_config 添加 execution_constraints 相关字段"""
        try:
            if HAS_SQLALCHEMY:
                # 检查字段是否存在
                if is_sqlite:
                    result = conn.execute(text(
                        "PRAGMA table_info(user_trading_config)"
                    )).fetchall()
                    columns = [row[1] for row in result]
                else:
                    result = conn.execute(text("""
                        SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS 
                        WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'user_trading_config'
                    """)).fetchall()
                    columns = [row[0] for row in result]
                
                # 添加 min_rr_ratio 字段
                if 'min_rr_ratio' not in columns:
                    if is_sqlite:
                        conn.execute(text(
                            "ALTER TABLE user_trading_config ADD COLUMN min_rr_ratio REAL DEFAULT 3.0"
                        ))
                    else:
                        conn.execute(text(
                            "ALTER TABLE user_trading_config ADD COLUMN min_rr_ratio DECIMAL(5,2) DEFAULT 3.0"
                        ))
                    logger.info("已添加 min_rr_ratio 字段到 user_trading_config 表")
                
                # 添加 limit_order_min_distance_pct 字段
                if 'limit_order_min_distance_pct' not in columns:
                    if is_sqlite:
                        conn.execute(text(
                            "ALTER TABLE user_trading_config ADD COLUMN limit_order_min_distance_pct REAL DEFAULT 3.0"
                        ))
                    else:
                        conn.execute(text(
                            "ALTER TABLE user_trading_config ADD COLUMN limit_order_min_distance_pct DECIMAL(5,2) DEFAULT 3.0"
                        ))
                    logger.info("已添加 limit_order_min_distance_pct 字段到 user_trading_config 表")
            else:
                # 原生 sqlite3
                result = conn.execute("PRAGMA table_info(user_trading_config)").fetchall()
                columns = [row[1] for row in result]
                
                if 'min_rr_ratio' not in columns:
                    conn.execute(
                        "ALTER TABLE user_trading_config ADD COLUMN min_rr_ratio REAL DEFAULT 3.0"
                    )
                    logger.info("已添加 min_rr_ratio 字段到 user_trading_config 表")
                
                if 'limit_order_min_distance_pct' not in columns:
                    conn.execute(
                        "ALTER TABLE user_trading_config ADD COLUMN limit_order_min_distance_pct REAL DEFAULT 3.0"
                    )
                    logger.info("已添加 limit_order_min_distance_pct 字段到 user_trading_config 表")
        except Exception as e:
            logger.debug(f"迁移 execution_constraints 字段时出错（可能已存在）: {e}")
    
    def _migrate_add_llm_params(self, conn, is_sqlite: bool):
        """迁移：为 user_ai_strategies 添加 temperature, top_p, max_tokens 字段"""
        try:
            if HAS_SQLALCHEMY:
                # 检查字段是否存在
                if is_sqlite:
                    result = conn.execute(text(
                        "PRAGMA table_info(user_ai_strategies)"
                    )).fetchall()
                    columns = [row[1] for row in result]
                else:
                    result = conn.execute(text("""
                        SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS 
                        WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'user_ai_strategies'
                    """)).fetchall()
                    columns = [row[0] for row in result]
                
                # 添加 temperature 字段
                if 'temperature' not in columns:
                    if is_sqlite:
                        conn.execute(text(
                            "ALTER TABLE user_ai_strategies ADD COLUMN temperature REAL DEFAULT NULL"
                        ))
                    else:
                        conn.execute(text(
                            "ALTER TABLE user_ai_strategies ADD COLUMN temperature DECIMAL(3,2) DEFAULT NULL COMMENT 'Temperature参数 (0-2)'"
                        ))
                    logger.info("已添加 temperature 字段到 user_ai_strategies 表")
                
                # 添加 top_p 字段
                if 'top_p' not in columns:
                    if is_sqlite:
                        conn.execute(text(
                            "ALTER TABLE user_ai_strategies ADD COLUMN top_p REAL DEFAULT NULL"
                        ))
                    else:
                        conn.execute(text(
                            "ALTER TABLE user_ai_strategies ADD COLUMN top_p DECIMAL(3,2) DEFAULT NULL COMMENT 'Top P参数 (0-1)'"
                        ))
                    logger.info("已添加 top_p 字段到 user_ai_strategies 表")
                
                # 添加 max_tokens 字段
                if 'max_tokens' not in columns:
                    if is_sqlite:
                        conn.execute(text(
                            "ALTER TABLE user_ai_strategies ADD COLUMN max_tokens INTEGER DEFAULT NULL"
                        ))
                    else:
                        conn.execute(text(
                            "ALTER TABLE user_ai_strategies ADD COLUMN max_tokens INT DEFAULT NULL COMMENT '最大输出tokens'"
                        ))
                    logger.info("已添加 max_tokens 字段到 user_ai_strategies 表")
            else:
                # 原生 sqlite3
                result = conn.execute("PRAGMA table_info(user_ai_strategies)").fetchall()
                columns = [row[1] for row in result]
                
                if 'temperature' not in columns:
                    conn.execute(
                        "ALTER TABLE user_ai_strategies ADD COLUMN temperature REAL DEFAULT NULL"
                    )
                    logger.info("已添加 temperature 字段到 user_ai_strategies 表")
                
                if 'top_p' not in columns:
                    conn.execute(
                        "ALTER TABLE user_ai_strategies ADD COLUMN top_p REAL DEFAULT NULL"
                    )
                    logger.info("已添加 top_p 字段到 user_ai_strategies 表")
                
                if 'max_tokens' not in columns:
                    conn.execute(
                        "ALTER TABLE user_ai_strategies ADD COLUMN max_tokens INTEGER DEFAULT NULL"
                    )
                    logger.info("已添加 max_tokens 字段到 user_ai_strategies 表")
        except Exception as e:
            logger.debug(f"迁移 LLM 参数字段时出错（可能已存在）: {e}")
    
    def _migrate_add_strategy_tables(self, conn, is_sqlite: bool):
        """迁移：创建 v5.0 策略模板表"""
        try:
            if HAS_SQLALCHEMY:
                if is_sqlite:
                    # SQLite: strategy_templates
                    conn.execute(text("""
                        CREATE TABLE IF NOT EXISTS strategy_templates (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            preset_name TEXT NOT NULL,
                            category TEXT NOT NULL,
                            content TEXT NOT NULL,
                            display_order INTEGER DEFAULT 0,
                            is_enabled INTEGER DEFAULT 1,
                            description TEXT,
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            UNIQUE(preset_name, category)
                        )
                    """))
                    # SQLite: user_strategy_templates
                    conn.execute(text("""
                        CREATE TABLE IF NOT EXISTS user_strategy_templates (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            uid TEXT NOT NULL,
                            preset_name TEXT NOT NULL,
                            category TEXT NOT NULL,
                            content TEXT NOT NULL,
                            display_order INTEGER DEFAULT 0,
                            description TEXT,
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            UNIQUE(uid, preset_name, category)
                        )
                    """))
                else:
                    # MySQL: strategy_templates
                    conn.execute(text("""
                        CREATE TABLE IF NOT EXISTS strategy_templates (
                            id INT AUTO_INCREMENT PRIMARY KEY,
                            preset_name VARCHAR(64) NOT NULL COMMENT '预设名称',
                            category VARCHAR(32) NOT NULL COMMENT '分类',
                            content TEXT NOT NULL COMMENT '模板内容',
                            display_order INT DEFAULT 0 COMMENT '显示顺序',
                            is_enabled TINYINT DEFAULT 1 COMMENT '是否启用',
                            description VARCHAR(256) COMMENT '预设描述',
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                            UNIQUE KEY uk_preset_category (preset_name, category),
                            INDEX idx_preset_name (preset_name),
                            INDEX idx_is_enabled (is_enabled)
                        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                        COMMENT='LLM策略模板表 - 系统预设策略'
                    """))
                    # MySQL: user_strategy_templates
                    conn.execute(text("""
                        CREATE TABLE IF NOT EXISTS user_strategy_templates (
                            id INT AUTO_INCREMENT PRIMARY KEY,
                            uid VARCHAR(64) NOT NULL COMMENT '用户ID',
                            preset_name VARCHAR(64) NOT NULL COMMENT '预设名称',
                            category VARCHAR(32) NOT NULL COMMENT '分类',
                            content TEXT NOT NULL COMMENT '模板内容',
                            display_order INT DEFAULT 0 COMMENT '显示顺序',
                            description VARCHAR(256) COMMENT '预设描述',
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                            UNIQUE KEY uk_uid_preset_category (uid, preset_name, category),
                            INDEX idx_uid (uid),
                            INDEX idx_preset_name (preset_name)
                        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                        COMMENT='用户自定义策略模板表'
                    """))
                logger.info("已创建 strategy_templates 和 user_strategy_templates 表")
            else:
                # 原生 sqlite3
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS strategy_templates (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        preset_name TEXT NOT NULL,
                        category TEXT NOT NULL,
                        content TEXT NOT NULL,
                        display_order INTEGER DEFAULT 0,
                        is_enabled INTEGER DEFAULT 1,
                        description TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(preset_name, category)
                    )
                """)
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS user_strategy_templates (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        uid TEXT NOT NULL,
                        preset_name TEXT NOT NULL,
                        category TEXT NOT NULL,
                        content TEXT NOT NULL,
                        display_order INTEGER DEFAULT 0,
                        description TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(uid, preset_name, category)
                    )
                """)
                logger.info("已创建 strategy_templates 和 user_strategy_templates 表")
        except Exception as e:
            logger.debug(f"创建策略模板表时出错（可能已存在）: {e}")
    
    def _migrate_add_strategy_preset_fields(self, conn, is_sqlite: bool):
        """迁移：为 user_ai_strategies 添加 strategy_preset 和 strategy_overrides 字段"""
        try:
            if HAS_SQLALCHEMY:
                # 检查字段是否存在
                if is_sqlite:
                    result = conn.execute(text(
                        "PRAGMA table_info(user_ai_strategies)"
                    )).fetchall()
                    columns = [row[1] for row in result]
                else:
                    result = conn.execute(text("""
                        SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS 
                        WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'user_ai_strategies'
                    """)).fetchall()
                    columns = [row[0] for row in result]
                
                # 添加 strategy_preset 字段
                if 'strategy_preset' not in columns:
                    if is_sqlite:
                        conn.execute(text(
                            "ALTER TABLE user_ai_strategies ADD COLUMN strategy_preset TEXT DEFAULT 'default'"
                        ))
                    else:
                        conn.execute(text(
                            "ALTER TABLE user_ai_strategies ADD COLUMN strategy_preset VARCHAR(64) DEFAULT 'default' COMMENT '预设策略名称'"
                        ))
                    logger.info("已添加 strategy_preset 字段到 user_ai_strategies 表")
                
                # 添加 strategy_overrides 字段
                if 'strategy_overrides' not in columns:
                    if is_sqlite:
                        conn.execute(text(
                            "ALTER TABLE user_ai_strategies ADD COLUMN strategy_overrides TEXT DEFAULT NULL"
                        ))
                    else:
                        conn.execute(text(
                            "ALTER TABLE user_ai_strategies ADD COLUMN strategy_overrides LONGTEXT COMMENT '自定义覆盖 (JSON)'"
                        ))
                    logger.info("已添加 strategy_overrides 字段到 user_ai_strategies 表")
            else:
                # 原生 sqlite3
                result = conn.execute("PRAGMA table_info(user_ai_strategies)").fetchall()
                columns = [row[1] for row in result]
                
                if 'strategy_preset' not in columns:
                    conn.execute(
                        "ALTER TABLE user_ai_strategies ADD COLUMN strategy_preset TEXT DEFAULT 'default'"
                    )
                    logger.info("已添加 strategy_preset 字段到 user_ai_strategies 表")
                
                if 'strategy_overrides' not in columns:
                    conn.execute(
                        "ALTER TABLE user_ai_strategies ADD COLUMN strategy_overrides TEXT DEFAULT NULL"
                    )
                    logger.info("已添加 strategy_overrides 字段到 user_ai_strategies 表")
        except Exception as e:
            logger.debug(f"迁移 strategy_preset/strategy_overrides 字段时出错（可能已存在）: {e}")
    
    def _migrate_add_llm_models_table(self, conn, is_sqlite: bool):
        """迁移：创建 llm_models 表"""
        try:
            if HAS_SQLALCHEMY:
                if is_sqlite:
                    conn.execute(text("""
                        CREATE TABLE IF NOT EXISTS llm_models (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            provider TEXT NOT NULL,
                            model_id TEXT NOT NULL,
                            display_name TEXT NOT NULL,
                            description TEXT,
                            temperature REAL DEFAULT 0.3,
                            top_p REAL DEFAULT 0.9,
                            max_tokens INTEGER DEFAULT 8192,
                            context_window INTEGER DEFAULT 128000,
                            supports_vision INTEGER DEFAULT 0,
                            supports_function_call INTEGER DEFAULT 0,
                            supports_streaming INTEGER DEFAULT 1,
                            supports_json_mode INTEGER DEFAULT 0,
                            input_price REAL DEFAULT 0,
                            output_price REAL DEFAULT 0,
                            is_enabled INTEGER DEFAULT 1,
                            is_recommended INTEGER DEFAULT 0,
                            display_order INTEGER DEFAULT 100,
                            release_date DATE,
                            deprecated_date DATE,
                            notes TEXT,
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            UNIQUE(provider, model_id)
                        )
                    """))
                else:
                    conn.execute(text("""
                        CREATE TABLE IF NOT EXISTS llm_models (
                            id INT AUTO_INCREMENT PRIMARY KEY,
                            provider VARCHAR(64) NOT NULL COMMENT 'LLM提供商',
                            model_id VARCHAR(128) NOT NULL COMMENT '模型ID',
                            display_name VARCHAR(128) NOT NULL COMMENT '显示名称',
                            description TEXT COMMENT '模型描述',
                            temperature DECIMAL(3,2) DEFAULT 0.30 COMMENT '默认温度',
                            top_p DECIMAL(3,2) DEFAULT 0.90 COMMENT '默认Top P',
                            max_tokens INT DEFAULT 8192 COMMENT '默认最大输出tokens',
                            context_window INT DEFAULT 128000 COMMENT '上下文窗口大小',
                            supports_vision TINYINT DEFAULT 0 COMMENT '是否支持视觉',
                            supports_function_call TINYINT DEFAULT 0 COMMENT '是否支持函数调用',
                            supports_streaming TINYINT DEFAULT 1 COMMENT '是否支持流式输出',
                            supports_json_mode TINYINT DEFAULT 0 COMMENT '是否支持JSON模式',
                            input_price DECIMAL(10,6) DEFAULT 0 COMMENT '输入价格 ($/M tokens)',
                            output_price DECIMAL(10,6) DEFAULT 0 COMMENT '输出价格 ($/M tokens)',
                            is_enabled TINYINT DEFAULT 1 COMMENT '是否启用',
                            is_recommended TINYINT DEFAULT 0 COMMENT '是否推荐',
                            display_order INT DEFAULT 100 COMMENT '显示顺序',
                            release_date DATE COMMENT '发布日期',
                            deprecated_date DATE COMMENT '废弃日期',
                            notes TEXT COMMENT '备注',
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                            UNIQUE KEY uk_provider_model (provider, model_id),
                            INDEX idx_provider (provider),
                            INDEX idx_is_enabled (is_enabled)
                        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                        COMMENT='LLM模型配置表'
                    """))
                logger.info("已创建 llm_models 表")
            else:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS llm_models (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        provider TEXT NOT NULL,
                        model_id TEXT NOT NULL,
                        display_name TEXT NOT NULL,
                        description TEXT,
                        temperature REAL DEFAULT 0.3,
                        top_p REAL DEFAULT 0.9,
                        max_tokens INTEGER DEFAULT 8192,
                        context_window INTEGER DEFAULT 128000,
                        supports_vision INTEGER DEFAULT 0,
                        supports_function_call INTEGER DEFAULT 0,
                        supports_streaming INTEGER DEFAULT 1,
                        supports_json_mode INTEGER DEFAULT 0,
                        input_price REAL DEFAULT 0,
                        output_price REAL DEFAULT 0,
                        is_enabled INTEGER DEFAULT 1,
                        is_recommended INTEGER DEFAULT 0,
                        display_order INTEGER DEFAULT 100,
                        release_date DATE,
                        deprecated_date DATE,
                        notes TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(provider, model_id)
                    )
                """)
                logger.info("已创建 llm_models 表")
        except Exception as e:
            logger.debug(f"创建 llm_models 表时出错（可能已存在）: {e}")
    
    def _init_default_settings(self, conn, is_sqlite: bool):
        """初始化默认系统设置"""
        default_settings = [
            ("registration_enabled", "1", "是否开放用户注册 (1=开放, 0=关闭)"),
            ("require_totp", "1", "注册是否必须绑定 Google Authenticator (1=必须, 0=可选)"),
            ("require_invite_code", "0", "注册是否必须使用邀请码 (1=必须, 0=可选)"),
        ]
        
        # MySQL 中 key 是保留字，需要用反引号转义
        key_col = "key" if is_sqlite else "`key`"
        
        for key, value, description in default_settings:
            try:
                if HAS_SQLALCHEMY:
                    # 检查是否存在
                    existing = conn.execute(text(
                        f"SELECT 1 FROM system_settings WHERE {key_col} = :key"
                    ), {"key": key}).fetchone()
                    
                    if not existing:
                        conn.execute(text(f"""
                            INSERT INTO system_settings ({key_col}, value, description)
                            VALUES (:key, :value, :description)
                        """), {"key": key, "value": value, "description": description})
                else:
                    cursor = conn.execute(
                        "SELECT 1 FROM system_settings WHERE key = ?", (key,)
                    )
                    if not cursor.fetchone():
                        conn.execute("""
                            INSERT INTO system_settings (key, value, description)
                            VALUES (?, ?, ?)
                        """, (key, value, description))
            except Exception as e:
                # P2 Fix: 添加日志，忽略重复插入错误
                logger.debug(f"系统设置初始化跳过 (可能已存在): {e}")
    
    def get_system_setting(self, key: str, default: str = None) -> Optional[str]:
        """获取系统设置"""
        try:
            is_sqlite = self._is_sqlite()
            # MySQL 中 key 是保留字，需要用反引号转义
            key_col = "key" if is_sqlite else "`key`"
            
            with self._get_connection() as conn:
                if HAS_SQLALCHEMY:
                    result = conn.execute(text(
                        f"SELECT value FROM system_settings WHERE {key_col} = :key"
                    ), {"key": key}).fetchone()
                else:
                    cursor = conn.execute(
                        "SELECT value FROM system_settings WHERE key = ?", (key,)
                    )
                    result = cursor.fetchone()
                
                if result:
                    return result[0]
                return default
        except Exception as e:
            logger.error(f"获取系统设置失败 [{key}]: {e}")
            return default
    
    def set_system_setting(self, key: str, value: str, description: str = None) -> bool:
        """设置系统设置"""
        try:
            is_sqlite = self._is_sqlite()
            
            with self._get_connection() as conn:
                if HAS_SQLALCHEMY:
                    if is_sqlite:
                        # SQLite 使用 ON CONFLICT
                        conn.execute(text("""
                            INSERT INTO system_settings (key, value, description, updated_at)
                            VALUES (:key, :value, :description, CURRENT_TIMESTAMP)
                            ON CONFLICT(key) DO UPDATE SET 
                                value = :value,
                                updated_at = CURRENT_TIMESTAMP
                        """), {"key": key, "value": value, "description": description or ""})
                    else:
                        # MySQL: key 是保留字，需要反引号；使用 ON DUPLICATE KEY UPDATE
                        conn.execute(text("""
                            INSERT INTO system_settings (`key`, value, description, updated_at)
                            VALUES (:key, :value, :description, CURRENT_TIMESTAMP)
                            ON DUPLICATE KEY UPDATE 
                                value = VALUES(value),
                                updated_at = CURRENT_TIMESTAMP
                        """), {"key": key, "value": value, "description": description or ""})
                else:
                    conn.execute("""
                        INSERT OR REPLACE INTO system_settings (key, value, description, updated_at)
                        VALUES (?, ?, ?, CURRENT_TIMESTAMP)
                    """, (key, value, description or ""))
            return True
        except Exception as e:
            logger.error(f"设置系统设置失败 [{key}]: {e}")
            return False
    
    def get_all_system_settings(self) -> Dict[str, Dict]:
        """获取所有系统设置"""
        try:
            is_sqlite = self._is_sqlite()
            # MySQL 中 key 是保留字，需要用反引号转义
            key_col = "key" if is_sqlite else "`key`"
            
            with self._get_connection() as conn:
                if HAS_SQLALCHEMY:
                    results = conn.execute(text(
                        f"SELECT {key_col}, value, description FROM system_settings"
                    )).fetchall()
                    return {
                        r[0]: {"value": r[1], "description": r[2]}
                        for r in results
                    }
                else:
                    cursor = conn.execute(
                        "SELECT key, value, description FROM system_settings"
                    )
                    return {
                        r[0]: {"value": r[1], "description": r[2]}
                        for r in cursor.fetchall()
                    }
        except Exception as e:
            logger.error(f"获取所有系统设置失败: {e}")
            return {}
    
    def is_registration_enabled(self) -> bool:
        """检查是否开放注册"""
        value = self.get_system_setting("registration_enabled", "1")
        return value == "1"
    
    def is_invite_code_required(self) -> bool:
        """检查注册是否需要邀请码"""
        value = self.get_system_setting("require_invite_code", "0")
        return value == "1"
    
    # ==================== 多交易所配置管理 ====================
    
    def _decrypt(self, value: str) -> Optional[str]:
        """解密辅助方法"""
        if not value:
            return None
        try:
            return self.crypto.decrypt(value)
        except Exception:
            return None
    
    def get_user_exchange_configs(self, uid: str) -> List[Dict]:
        """
        获取用户的所有交易所配置
        
        Returns:
            [
                {
                    "exchange": "binance",
                    "is_enabled": True,
                    "is_testnet": False,
                    "has_api_key": True,
                    "leverage": 20,
                    "monitor_symbols": ["BTCUSDT"],
                    "ai500_enabled": False,
                    "ai_enabled": False,
                    "llm_provider": "anthropic",
                    "llm_model": "",
                    "has_llm_api_key": False,
                    "llm_base_url": "",
                    "system_prompt": "",
                    "ai_strategy_id": "abc123"
                },
                ...
            ]
        """
        try:
            with self._get_connection() as conn:
                if HAS_SQLALCHEMY:
                    results = conn.execute(text("""
                        SELECT exchange, api_key_encrypted, is_testnet, is_enabled, leverage, 
                               wallet_address, monitor_symbols, ai500_enabled, ai_strategy_id
                        FROM user_exchange_config
                        WHERE uid = :uid
                    """), {"uid": uid}).fetchall()
                else:
                    cursor = conn.execute("""
                        SELECT exchange, api_key_encrypted, is_testnet, is_enabled, leverage, 
                               wallet_address, monitor_symbols, ai500_enabled, ai_strategy_id
                        FROM user_exchange_config
                        WHERE uid = ?
                    """, (uid,))
                    results = cursor.fetchall()
                
                configs = []
                for r in results:
                    # 解析 monitor_symbols JSON
                    monitor_symbols = []
                    if r[6]:
                        try:
                            monitor_symbols = json.loads(r[6])
                        except Exception as e:
                            logger.debug(f"解析 monitor_symbols 失败: {e}")
                    
                    configs.append({
                        "exchange": r[0],
                        "has_api_key": bool(r[1]),
                        "is_testnet": bool(r[2]),
                        "is_enabled": bool(r[3]),
                        "leverage": r[4] or 20,
                        "wallet_address": r[5] or "",
                        "monitor_symbols": monitor_symbols,
                        "ai500_enabled": bool(r[7]) if r[7] is not None else False,
                        # AI 策略池
                        "ai_strategy_id": r[8] or None
                    })
                return configs
        except Exception as e:
            logger.error(f"获取用户交易所配置失败 [{uid}]: {e}")
            return []
    
    def get_user_exchange_config(self, uid: str, exchange: str) -> Optional[Dict]:
        """
        获取用户指定交易所的完整配置（包含解密后的密钥）
        
        Returns:
            {
                "exchange": "okx",
                "api_key": "xxx",
                "api_secret": "xxx",
                "passphrase": "xxx",  # OKX
                "wallet_address": "0x...",  # Hyperliquid
                "is_testnet": False,
                "is_enabled": True,
                "leverage": 20,
                "monitor_symbols": ["BTCUSDT", "ETHUSDT"],
                "ai500_enabled": False,
                "ai_strategy_id": "abc123"
            }
        """
        try:
            with self._get_connection() as conn:
                if HAS_SQLALCHEMY:
                    result = conn.execute(text("""
                        SELECT api_key_encrypted, api_secret_encrypted, passphrase_encrypted,
                               wallet_address, is_testnet, is_enabled, leverage,
                               monitor_symbols, ai500_enabled, ai_strategy_id
                        FROM user_exchange_config
                        WHERE uid = :uid AND exchange = :exchange
                    """), {"uid": uid, "exchange": exchange}).fetchone()
                else:
                    cursor = conn.execute("""
                        SELECT api_key_encrypted, api_secret_encrypted, passphrase_encrypted,
                               wallet_address, is_testnet, is_enabled, leverage,
                               monitor_symbols, ai500_enabled, ai_strategy_id
                        FROM user_exchange_config
                        WHERE uid = ? AND exchange = ?
                    """, (uid, exchange))
                    result = cursor.fetchone()
                
                if not result:
                    return None
                
                # 解析 monitor_symbols JSON
                monitor_symbols = []
                if result[7]:
                    try:
                        monitor_symbols = json.loads(result[7])
                    except Exception as e:
                        logger.debug(f"解析 monitor_symbols 失败: {e}")
                
                return {
                    "exchange": exchange,
                    "api_key": self._decrypt(result[0]) or "",
                    "api_secret": self._decrypt(result[1]) or "",
                    "passphrase": self._decrypt(result[2]) or "",
                    "wallet_address": result[3] or "",
                    "is_testnet": bool(result[4]),
                    "is_enabled": bool(result[5]),
                    "leverage": result[6] or 20,
                    "monitor_symbols": monitor_symbols,
                    "ai500_enabled": bool(result[8]) if result[8] is not None else False,
                    "ai_strategy_id": result[9] or None
                }
        except Exception as e:
            logger.error(f"获取用户交易所配置失败 [{uid}][{exchange}]: {e}")
            return None
    
    def save_user_exchange_config(
        self,
        uid: str,
        exchange: str,
        api_key: str = None,
        api_secret: str = None,
        passphrase: str = None,
        wallet_address: str = None,
        is_testnet: bool = False,
        is_enabled: bool = True
    ) -> bool:
        """
        保存用户交易所配置
        
        Args:
            uid: 用户ID
            exchange: 交易所名称 (binance/okx/hyperliquid)
            api_key: API Key (Hyperliquid 为私钥)
            api_secret: API Secret
            passphrase: API Passphrase (OKX 需要)
            wallet_address: 钱包地址 (Hyperliquid 可选)
            is_testnet: 是否测试网
            is_enabled: 是否启用
        """
        try:
            # 先获取现有配置，用于保留未更新的字段
            existing = self.get_user_exchange_config(uid, exchange)
            
            # 清理输入（移除前后空格和换行符）
            if api_key:
                api_key = api_key.strip()
            if api_secret:
                api_secret = api_secret.strip()
            if passphrase:
                passphrase = passphrase.strip()
            
            # 如果没有传入新密钥，保留原有的
            if api_key:
                encrypted_key = self.crypto.encrypt(api_key)
            elif existing and existing.get('api_key'):
                # 保留原有的加密值（需要重新加密因为 get 时解密了）
                encrypted_key = self.crypto.encrypt(existing['api_key'])
            else:
                encrypted_key = None
                
            if api_secret:
                encrypted_secret = self.crypto.encrypt(api_secret)
            elif existing and existing.get('api_secret'):
                encrypted_secret = self.crypto.encrypt(existing['api_secret'])
            else:
                encrypted_secret = None
                
            if passphrase:
                encrypted_passphrase = self.crypto.encrypt(passphrase)
            elif existing and existing.get('passphrase'):
                encrypted_passphrase = self.crypto.encrypt(existing['passphrase'])
            else:
                encrypted_passphrase = None
            
            # wallet_address 也保留原有值
            if not wallet_address and existing:
                wallet_address = existing.get('wallet_address', '')
            
            is_sqlite = self._is_sqlite()
            
            with self._get_connection() as conn:
                if is_sqlite:
                    # 使用 INSERT ... ON CONFLICT ... DO UPDATE 保留其他字段
                    # 注意：不能使用 INSERT OR REPLACE，因为它会删除整行再插入新行，导致其他字段丢失
                    if HAS_SQLALCHEMY:
                        conn.execute(text("""
                            INSERT INTO user_exchange_config 
                            (uid, exchange, api_key_encrypted, api_secret_encrypted, 
                             passphrase_encrypted, wallet_address, is_testnet, is_enabled, updated_at)
                            VALUES (:uid, :exchange, :api_key, :api_secret, 
                                    :passphrase, :wallet, :testnet, :enabled, CURRENT_TIMESTAMP)
                            ON CONFLICT(uid, exchange) DO UPDATE SET
                                api_key_encrypted = :api_key,
                                api_secret_encrypted = :api_secret,
                                passphrase_encrypted = :passphrase,
                                wallet_address = :wallet,
                                is_testnet = :testnet,
                                is_enabled = :enabled,
                                updated_at = CURRENT_TIMESTAMP
                        """), {
                            "uid": uid,
                            "exchange": exchange,
                            "api_key": encrypted_key,
                            "api_secret": encrypted_secret,
                            "passphrase": encrypted_passphrase,
                            "wallet": wallet_address,
                            "testnet": 1 if is_testnet else 0,
                            "enabled": 1 if is_enabled else 0
                        })
                    else:
                        conn.execute("""
                            INSERT INTO user_exchange_config 
                            (uid, exchange, api_key_encrypted, api_secret_encrypted, 
                             passphrase_encrypted, wallet_address, is_testnet, is_enabled, updated_at)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                            ON CONFLICT(uid, exchange) DO UPDATE SET
                                api_key_encrypted = excluded.api_key_encrypted,
                                api_secret_encrypted = excluded.api_secret_encrypted,
                                passphrase_encrypted = excluded.passphrase_encrypted,
                                wallet_address = excluded.wallet_address,
                                is_testnet = excluded.is_testnet,
                                is_enabled = excluded.is_enabled,
                                updated_at = CURRENT_TIMESTAMP
                        """, (uid, exchange, encrypted_key, encrypted_secret, 
                              encrypted_passphrase, wallet_address, 
                              1 if is_testnet else 0, 1 if is_enabled else 0))
                else:
                    # MySQL
                    conn.execute(text("""
                        INSERT INTO user_exchange_config 
                        (uid, exchange, api_key_encrypted, api_secret_encrypted, 
                         passphrase_encrypted, wallet_address, is_testnet, is_enabled)
                        VALUES (:uid, :exchange, :api_key, :api_secret, 
                                :passphrase, :wallet, :testnet, :enabled)
                        ON DUPLICATE KEY UPDATE
                            api_key_encrypted = VALUES(api_key_encrypted),
                            api_secret_encrypted = VALUES(api_secret_encrypted),
                            passphrase_encrypted = VALUES(passphrase_encrypted),
                            wallet_address = VALUES(wallet_address),
                            is_testnet = VALUES(is_testnet),
                            is_enabled = VALUES(is_enabled),
                            updated_at = CURRENT_TIMESTAMP
                    """), {
                        "uid": uid,
                        "exchange": exchange,
                        "api_key": encrypted_key,
                        "api_secret": encrypted_secret,
                        "passphrase": encrypted_passphrase,
                        "wallet": wallet_address,
                        "testnet": is_testnet,
                        "enabled": is_enabled
                    })
            
            # 清除缓存
            self._cache.pop(uid, None)
            logger.info(f"交易所配置已保存 [{uid}][{exchange}]")
            return True
            
        except Exception as e:
            logger.error(f"保存交易所配置失败 [{uid}][{exchange}]: {e}")
            return False
    
    def set_exchange_enabled(self, uid: str, exchange: str, enabled: bool) -> bool:
        """启用/禁用交易所"""
        try:
            with self._get_connection() as conn:
                if HAS_SQLALCHEMY:
                    conn.execute(text("""
                        UPDATE user_exchange_config 
                        SET is_enabled = :enabled, updated_at = CURRENT_TIMESTAMP
                        WHERE uid = :uid AND exchange = :exchange
                    """), {"enabled": 1 if enabled else 0, "uid": uid, "exchange": exchange})
                else:
                    conn.execute("""
                        UPDATE user_exchange_config 
                        SET is_enabled = ?, updated_at = CURRENT_TIMESTAMP
                        WHERE uid = ? AND exchange = ?
                    """, (1 if enabled else 0, uid, exchange))
            
            logger.info(f"[{uid}] 交易所 {exchange} 启用状态已设置为 {enabled}")
            return True
        except Exception as e:
            logger.error(f"设置交易所启用状态失败 [{uid}][{exchange}]: {e}")
            return False
    
    def set_exchange_running(self, uid: str, exchange: str, running: bool) -> bool:
        """
        设置交易所运行状态（持久化）
        
        用于重启后恢复只运行之前在运行的交易所
        """
        try:
            with self._get_connection() as conn:
                if HAS_SQLALCHEMY:
                    conn.execute(text("""
                        UPDATE user_exchange_config 
                        SET is_running = :running, updated_at = CURRENT_TIMESTAMP
                        WHERE uid = :uid AND exchange = :exchange
                    """), {"running": 1 if running else 0, "uid": uid, "exchange": exchange})
                else:
                    conn.execute("""
                        UPDATE user_exchange_config 
                        SET is_running = ?, updated_at = CURRENT_TIMESTAMP
                        WHERE uid = ? AND exchange = ?
                    """, (1 if running else 0, uid, exchange))
            
            logger.debug(f"[{uid}] 交易所 {exchange} 运行状态已设置为 {running}")
            return True
        except Exception as e:
            logger.error(f"设置交易所运行状态失败 [{uid}][{exchange}]: {e}")
            return False
    
    def get_running_exchanges(self, uid: str) -> List[str]:
        """
        获取用户正在运行的交易所列表
        
        返回 is_running=1 的交易所
        """
        try:
            with self._get_connection() as conn:
                if HAS_SQLALCHEMY:
                    results = conn.execute(text("""
                        SELECT exchange FROM user_exchange_config
                        WHERE uid = :uid AND is_running = 1
                    """), {"uid": uid}).fetchall()
                else:
                    cursor = conn.execute("""
                        SELECT exchange FROM user_exchange_config
                        WHERE uid = ? AND is_running = 1
                    """, (uid,))
                    results = cursor.fetchall()
                
                return [r[0] for r in results]
        except Exception as e:
            logger.error(f"获取运行中交易所失败 [{uid}]: {e}")
            return []
    
    def get_running_exchanges_batch(self, uids: List[str]) -> Dict[str, List[str]]:
        """
        批量获取用户正在运行的交易所列表
        
        1000+ 用户优化：
        - 原实现：每用户 1 次查询 = 1000 查询
        - 优化后：1 次批量查询
        
        Args:
            uids: 用户ID列表
        
        Returns:
            {uid: [exchange1, exchange2, ...], ...}
        """
        if not uids:
            return {}
        
        result: Dict[str, List[str]] = {uid: [] for uid in uids}
        
        try:
            with self._get_connection() as conn:
                if HAS_SQLALCHEMY:
                    placeholders = ", ".join([f":uid_{i}" for i in range(len(uids))])
                    params = {f"uid_{i}": uid for i, uid in enumerate(uids)}
                    
                    query = text(f"""
                        SELECT uid, exchange FROM user_exchange_config
                        WHERE uid IN ({placeholders}) AND is_running = 1
                    """)
                    
                    rows = conn.execute(query, params).fetchall()
                    for row in rows:
                        uid = row[0]
                        exchange = row[1]
                        if uid in result:
                            result[uid].append(exchange)
                else:
                    # SQLite 回退到单个查询
                    for uid in uids:
                        result[uid] = self.get_running_exchanges(uid)
                
                logger.debug(f"批量获取 running_exchanges: {len(uids)} 用户")
                return result
                
        except Exception as e:
            logger.error(f"批量获取运行中交易所失败: {e}")
            # 回退到单个查询
            for uid in uids:
                result[uid] = self.get_running_exchanges(uid)
            return result
    
    def stop_all_exchanges(self, uid: str) -> bool:
        """
        停止用户所有交易所（设置 is_running=0）
        """
        try:
            with self._get_connection() as conn:
                if HAS_SQLALCHEMY:
                    conn.execute(text("""
                        UPDATE user_exchange_config 
                        SET is_running = 0, updated_at = CURRENT_TIMESTAMP
                        WHERE uid = :uid
                    """), {"uid": uid})
                else:
                    conn.execute("""
                        UPDATE user_exchange_config 
                        SET is_running = 0, updated_at = CURRENT_TIMESTAMP
                        WHERE uid = ?
                    """, (uid,))
            
            logger.info(f"[{uid}] 所有交易所运行状态已重置")
            return True
        except Exception as e:
            logger.error(f"停止所有交易所失败 [{uid}]: {e}")
            return False
    
    def set_exchange_monitor_config(
        self,
        uid: str,
        exchange: str,
        monitor_symbols: List[str] = None,
        ai500_enabled: bool = None
    ) -> bool:
        """
        设置交易所的监控配置（监控币种和 AI500）
        
        Args:
            uid: 用户ID
            exchange: 交易所名称
            monitor_symbols: 监控币种列表
            ai500_enabled: 是否启用 AI500
        """
        try:
            updates = {}
            params = {"uid": uid, "exchange": exchange}
            
            if monitor_symbols is not None:
                updates["monitor_symbols"] = ":symbols"
                params["symbols"] = json.dumps(monitor_symbols)
            
            if ai500_enabled is not None:
                updates["ai500_enabled"] = ":ai500"
                params["ai500"] = 1 if ai500_enabled else 0
            
            if not updates:
                return True
            
            set_clause = ", ".join([f"{k} = {v}" for k, v in updates.items()])
            
            with self._get_connection() as conn:
                if HAS_SQLALCHEMY:
                    conn.execute(text(f"""
                        UPDATE user_exchange_config 
                        SET {set_clause}, updated_at = CURRENT_TIMESTAMP
                        WHERE uid = :uid AND exchange = :exchange
                    """), params)
                else:
                    # SQLite - 需要替换参数格式
                    set_clause_sqlite = set_clause.replace(":symbols", "?").replace(":ai500", "?")
                    values = []
                    if monitor_symbols is not None:
                        values.append(json.dumps(monitor_symbols))
                    if ai500_enabled is not None:
                        values.append(1 if ai500_enabled else 0)
                    values.extend([uid, exchange])
                    
                    conn.execute(f"""
                        UPDATE user_exchange_config 
                        SET {set_clause_sqlite}, updated_at = CURRENT_TIMESTAMP
                        WHERE uid = ? AND exchange = ?
                    """, values)
            
            logger.info(f"[{uid}][{exchange}] 监控配置已更新")
            return True
        except Exception as e:
            logger.error(f"设置交易所监控配置失败 [{uid}][{exchange}]: {e}")
            return False
    
    def set_exchange_ai_config(
        self,
        uid: str,
        exchange: str,
        ai_enabled: bool = None,
        llm_provider: str = None,
        llm_model: str = None,
        llm_api_key: str = None,
        llm_base_url: str = None,
        system_prompt: str = None
    ) -> bool:
        """
        设置交易所的 AI 配置
        
        Args:
            uid: 用户ID
            exchange: 交易所名称
            ai_enabled: 是否启用 AI 交易
            llm_provider: LLM 提供商
            llm_model: 模型名称
            llm_api_key: API Key
            llm_base_url: 自定义 Base URL
            system_prompt: 系统提示词
        """
        try:
            updates = {}
            params = {"uid": uid, "exchange": exchange}
            
            if ai_enabled is not None:
                updates["ai_enabled"] = ":ai_enabled"
                params["ai_enabled"] = 1 if ai_enabled else 0
            
            if llm_provider is not None:
                updates["llm_provider"] = ":llm_provider"
                params["llm_provider"] = llm_provider
            
            if llm_model is not None:
                updates["llm_model"] = ":llm_model"
                params["llm_model"] = llm_model
            
            if llm_api_key is not None:
                updates["llm_api_key_encrypted"] = ":llm_api_key"
                params["llm_api_key"] = self.crypto.encrypt(llm_api_key) if llm_api_key else None
            
            if llm_base_url is not None:
                updates["llm_base_url"] = ":llm_base_url"
                params["llm_base_url"] = llm_base_url
            
            if system_prompt is not None:
                updates["system_prompt"] = ":system_prompt"
                params["system_prompt"] = system_prompt
            
            if not updates:
                return True
            
            set_clause = ", ".join([f"{k} = {v}" for k, v in updates.items()])
            
            with self._get_connection() as conn:
                if HAS_SQLALCHEMY:
                    conn.execute(text(f"""
                        UPDATE user_exchange_config 
                        SET {set_clause}, updated_at = CURRENT_TIMESTAMP
                        WHERE uid = :uid AND exchange = :exchange
                    """), params)
                else:
                    # SQLite - 需要替换参数格式并构建 values 列表
                    set_parts = []
                    values = []
                    if ai_enabled is not None:
                        set_parts.append("ai_enabled = ?")
                        values.append(1 if ai_enabled else 0)
                    if llm_provider is not None:
                        set_parts.append("llm_provider = ?")
                        values.append(llm_provider)
                    if llm_model is not None:
                        set_parts.append("llm_model = ?")
                        values.append(llm_model)
                    if llm_api_key is not None:
                        set_parts.append("llm_api_key_encrypted = ?")
                        values.append(self.crypto.encrypt(llm_api_key) if llm_api_key else None)
                    if llm_base_url is not None:
                        set_parts.append("llm_base_url = ?")
                        values.append(llm_base_url)
                    if system_prompt is not None:
                        set_parts.append("system_prompt = ?")
                        values.append(system_prompt)
                    
                    set_clause_sqlite = ", ".join(set_parts)
                    values.extend([uid, exchange])
                    
                    conn.execute(f"""
                        UPDATE user_exchange_config 
                        SET {set_clause_sqlite}, updated_at = CURRENT_TIMESTAMP
                        WHERE uid = ? AND exchange = ?
                    """, values)
            
            logger.info(f"[{uid}][{exchange}] AI 配置已更新")
            return True
        except Exception as e:
            logger.error(f"设置交易所 AI 配置失败 [{uid}][{exchange}]: {e}")
            return False
    
    def delete_user_exchange_config(self, uid: str, exchange: str) -> bool:
        """删除用户交易所配置"""
        try:
            with self._get_connection() as conn:
                if HAS_SQLALCHEMY:
                    conn.execute(text("""
                        DELETE FROM user_exchange_config 
                        WHERE uid = :uid AND exchange = :exchange
                    """), {"uid": uid, "exchange": exchange})
                else:
                    conn.execute("""
                        DELETE FROM user_exchange_config 
                        WHERE uid = ? AND exchange = ?
                    """, (uid, exchange))
            
            logger.info(f"交易所配置已删除 [{uid}][{exchange}]")
            return True
        except Exception as e:
            logger.error(f"删除交易所配置失败 [{uid}][{exchange}]: {e}")
            return False
    
    def get_enabled_exchanges(self, uid: str) -> List[str]:
        """获取用户已启用的交易所列表"""
        try:
            with self._get_connection() as conn:
                if HAS_SQLALCHEMY:
                    results = conn.execute(text("""
                        SELECT exchange FROM user_exchange_config
                        WHERE uid = :uid AND is_enabled = 1 AND api_key_encrypted IS NOT NULL
                    """), {"uid": uid}).fetchall()
                    return [r[0] for r in results]
                else:
                    cursor = conn.execute("""
                        SELECT exchange FROM user_exchange_config
                        WHERE uid = ? AND is_enabled = 1 AND api_key_encrypted IS NOT NULL
                    """, (uid,))
                    return [r[0] for r in cursor.fetchall()]
        except Exception as e:
            logger.error(f"获取启用交易所列表失败 [{uid}]: {e}")
            return []
    
    # ==================== AI 策略池管理 ====================
    
    def get_user_ai_strategies(self, uid: str) -> List[Dict]:
        """
        获取用户的所有 AI 策略 (v5.0)
        
        Returns:
            [
                {
                    "id": "strategy_id",
                    "name": "保守型策略",
                    "llm_provider": "anthropic",
                    "llm_model": "claude-sonnet-4-20250514",
                    "has_api_key": True,
                    "llm_base_url": "",
                    "strategy_preset": "conservative",
                    "has_overrides": True,
                },
                ...
            ]
        """
        try:
            with self._get_connection() as conn:
                if HAS_SQLALCHEMY:
                    results = conn.execute(text("""
                        SELECT strategy_id, name, llm_provider, llm_model, 
                               llm_api_key_encrypted, llm_base_url, 
                               strategy_preset, strategy_overrides
                        FROM user_ai_strategies
                        WHERE uid = :uid
                        ORDER BY created_at ASC
                    """), {"uid": uid}).fetchall()
                else:
                    cursor = conn.execute("""
                        SELECT strategy_id, name, llm_provider, llm_model, 
                               llm_api_key_encrypted, llm_base_url,
                               strategy_preset, strategy_overrides
                        FROM user_ai_strategies
                        WHERE uid = ?
                        ORDER BY created_at ASC
                    """, (uid,))
                    results = cursor.fetchall()
                
                strategies = []
                for r in results:
                    strategies.append({
                        "id": r[0],
                        "name": r[1],
                        "llm_provider": r[2] or "anthropic",
                        "llm_model": r[3] or "",
                        "has_api_key": bool(r[4]),
                        "llm_base_url": r[5] or "",
                        # v5.0: 策略配置
                        "strategy_preset": r[6] or "default",
                        "has_overrides": bool(r[7]),
                    })
                return strategies
        except Exception as e:
            logger.error(f"获取用户 AI 策略失败 [{uid}]: {e}")
            return []
    
    def get_user_ai_strategy(self, uid: str, strategy_id: str) -> Optional[Dict]:
        """
        获取单个 AI 策略的详细信息（包含解密的 API Key）(v5.0)
        
        注意：此方法已更新为 v5.0 格式，返回 strategy_preset 和 strategy_overrides
        推荐使用 get_ai_strategy_with_preset() 方法
        
        Returns:
            {
                "id": "strategy_id",
                "name": "保守型策略",
                "llm_provider": "anthropic",
                "llm_model": "claude-sonnet-4-20250514",
                "llm_api_key": "sk-xxx...",
                "llm_base_url": "",
                "strategy_preset": "conservative",
                "strategy_overrides": {...} or None,
            }
        """
        import json
        
        try:
            with self._get_connection() as conn:
                if HAS_SQLALCHEMY:
                    result = conn.execute(text("""
                        SELECT strategy_id, name, llm_provider, llm_model, 
                               llm_api_key_encrypted, llm_base_url,
                               temperature, top_p, max_tokens,
                               strategy_preset, strategy_overrides
                        FROM user_ai_strategies
                        WHERE uid = :uid AND strategy_id = :strategy_id
                    """), {"uid": uid, "strategy_id": strategy_id}).fetchone()
                else:
                    cursor = conn.execute("""
                        SELECT strategy_id, name, llm_provider, llm_model, 
                               llm_api_key_encrypted, llm_base_url,
                               temperature, top_p, max_tokens,
                               strategy_preset, strategy_overrides
                        FROM user_ai_strategies
                        WHERE uid = ? AND strategy_id = ?
                    """, (uid, strategy_id))
                    result = cursor.fetchone()
                
                if not result:
                    return None
                
                # 解析 strategy_overrides
                overrides = None
                if result[10]:
                    try:
                        overrides = json.loads(result[10]) if isinstance(result[10], str) else result[10]
                    except (json.JSONDecodeError, ValueError) as e:
                        logger.debug(f"解析 strategy_overrides 失败: {e}")
                
                return {
                    "id": result[0],
                    "name": result[1],
                    "llm_provider": result[2] or "anthropic",
                    "llm_model": result[3] or "",
                    "llm_api_key": self._decrypt(result[4]) or "",
                    "llm_base_url": result[5] or "",
                    # LLM 参数
                    "temperature": float(result[6]) if result[6] is not None else None,
                    "top_p": float(result[7]) if result[7] is not None else None,
                    "max_tokens": int(result[8]) if result[8] is not None else None,
                    # v5.0: 策略配置
                    "strategy_preset": result[9] or "default",
                    "strategy_overrides": overrides,
                }
        except Exception as e:
            logger.error(f"获取 AI 策略失败 [{uid}][{strategy_id}]: {e}")
            return None
    
    def save_user_ai_strategy(
        self,
        uid: str,
        strategy_id: str = None,
        name: str = None,
        llm_provider: str = None,
        llm_model: str = None,
        llm_api_key: str = None,
        llm_base_url: str = None,
        trading_philosophy: str = None,
        decision_principles: str = None,
        data_interpretation: str = None,
        decision_feedback: str = None,
    ) -> Optional[str]:
        """
        保存 AI 策略（创建或更新）
        
        **DEPRECATED**: 此方法使用旧的 4-part prompt 结构，已废弃。
        请使用 save_ai_strategy() 方法，该方法使用 v5.0 的 strategy_preset + strategy_overrides 结构。
        
        此方法保留用于向后兼容，会将旧格式转换为新格式存储。
        """
        import warnings
        warnings.warn(
            "save_user_ai_strategy() is deprecated. Use save_ai_strategy() with strategy_preset/strategy_overrides instead.",
            DeprecationWarning,
            stacklevel=2
        )
        
        import secrets
        
        # 如果没有 strategy_id，生成一个新的
        if not strategy_id:
            strategy_id = secrets.token_hex(8)
            if not name:
                name = "新策略"
        
        # 将旧的 4-part prompt 转换为 strategy_overrides
        # 映射关系：
        # trading_philosophy -> role
        # decision_principles -> risk_rules + entry_conditions
        # data_interpretation -> exit_conditions + position_sizing
        # decision_feedback -> adaptive_rules
        overrides = {}
        if trading_philosophy:
            overrides["role"] = trading_philosophy
        if decision_principles:
            overrides["risk_rules"] = decision_principles
        if data_interpretation:
            overrides["entry_conditions"] = data_interpretation
        if decision_feedback:
            overrides["adaptive_rules"] = decision_feedback
        
        import json
        overrides_json = json.dumps(overrides, ensure_ascii=False) if overrides else None
        
        # 调用新方法
        success = self.save_ai_strategy(
            uid=uid,
            strategy_id=strategy_id,
            name=name or "新策略",
            llm_provider=llm_provider or "anthropic",
            llm_model=llm_model or "",
            llm_api_key=llm_api_key,
            llm_base_url=llm_base_url,
            strategy_preset="default",  # 使用默认预设
            strategy_overrides=overrides_json,
        )
        
        return strategy_id if success else None
    
    def delete_user_ai_strategy(self, uid: str, strategy_id: str) -> bool:
        """
        删除 AI 策略
        
        注意：如果有交易所正在使用该策略，应该先清除关联
        """
        try:
            with self._get_connection() as conn:
                if HAS_SQLALCHEMY:
                    # 先清除使用该策略的交易所关联
                    conn.execute(text("""
                        UPDATE user_exchange_config 
                        SET ai_strategy_id = NULL 
                        WHERE uid = :uid AND ai_strategy_id = :strategy_id
                    """), {"uid": uid, "strategy_id": strategy_id})
                    
                    # 删除策略
                    conn.execute(text("""
                        DELETE FROM user_ai_strategies 
                        WHERE uid = :uid AND strategy_id = :strategy_id
                    """), {"uid": uid, "strategy_id": strategy_id})
                else:
                    conn.execute("""
                        UPDATE user_exchange_config 
                        SET ai_strategy_id = NULL 
                        WHERE uid = ? AND ai_strategy_id = ?
                    """, (uid, strategy_id))
                    
                    conn.execute("""
                        DELETE FROM user_ai_strategies 
                        WHERE uid = ? AND strategy_id = ?
                    """, (uid, strategy_id))
            
            logger.info(f"[{uid}] AI 策略已删除: {strategy_id}")
            return True
        except Exception as e:
            logger.error(f"删除 AI 策略失败 [{uid}][{strategy_id}]: {e}")
            return False
    
    def set_exchange_ai_strategy(self, uid: str, exchange: str, strategy_id: str = None) -> bool:
        """
        设置交易所使用的 AI 策略
        
        Args:
            uid: 用户ID
            exchange: 交易所名称
            strategy_id: 策略ID（为空则清除策略关联）
        """
        try:
            with self._get_connection() as conn:
                if HAS_SQLALCHEMY:
                    conn.execute(text("""
                        UPDATE user_exchange_config 
                        SET ai_strategy_id = :strategy_id, updated_at = CURRENT_TIMESTAMP
                        WHERE uid = :uid AND exchange = :exchange
                    """), {"strategy_id": strategy_id, "uid": uid, "exchange": exchange})
                else:
                    conn.execute("""
                        UPDATE user_exchange_config 
                        SET ai_strategy_id = ?, updated_at = CURRENT_TIMESTAMP
                        WHERE uid = ? AND exchange = ?
                    """, (strategy_id, uid, exchange))
            
            if strategy_id:
                logger.info(f"[{uid}][{exchange}] 已设置 AI 策略: {strategy_id}")
            else:
                logger.info(f"[{uid}][{exchange}] 已清除 AI 策略")
            return True
        except Exception as e:
            logger.error(f"设置交易所 AI 策略失败 [{uid}][{exchange}]: {e}")
            return False
    
    def get_exchange_ai_strategy_id(self, uid: str, exchange: str) -> Optional[str]:
        """获取交易所使用的 AI 策略ID"""
        try:
            with self._get_connection() as conn:
                if HAS_SQLALCHEMY:
                    result = conn.execute(text("""
                        SELECT ai_strategy_id FROM user_exchange_config
                        WHERE uid = :uid AND exchange = :exchange
                    """), {"uid": uid, "exchange": exchange}).fetchone()
                else:
                    cursor = conn.execute("""
                        SELECT ai_strategy_id FROM user_exchange_config
                        WHERE uid = ? AND exchange = ?
                    """, (uid, exchange))
                    result = cursor.fetchone()
                
                return result[0] if result and result[0] else None
        except Exception as e:
            logger.error(f"获取交易所 AI 策略ID失败 [{uid}][{exchange}]: {e}")
            return None
    
    def get_testnet_users(self, admin_usernames: List[str] = None) -> List[Dict]:
        """
        获取所有使用测试网的用户（排除管理员）
        
        Args:
            admin_usernames: 管理员用户名列表
            
        Returns:
            [
                {
                    "uid": "xxx",
                    "username": "user1",
                    "exchange": "binance",
                    "is_running": True
                },
                ...
            ]
        """
        if admin_usernames is None:
            from core.config import ADMIN_USERS
            admin_usernames = ADMIN_USERS
        
        try:
            with self._get_connection() as conn:
                if HAS_SQLALCHEMY:
                    results = conn.execute(text("""
                        SELECT u.uid, u.username, e.exchange, e.is_running
                        FROM user_exchange_config e
                        JOIN users u ON e.uid = u.uid
                        WHERE e.is_testnet = 1
                    """)).fetchall()
                else:
                    cursor = conn.execute("""
                        SELECT u.uid, u.username, e.exchange, e.is_running
                        FROM user_exchange_config e
                        JOIN users u ON e.uid = u.uid
                        WHERE e.is_testnet = 1
                    """)
                    results = cursor.fetchall()
                
                # 过滤掉管理员
                testnet_users = []
                for r in results:
                    username = r[1]
                    if username not in admin_usernames:
                        testnet_users.append({
                            "uid": r[0],
                            "username": username,
                            "exchange": r[2],
                            "is_running": bool(r[3])
                        })
                
                return testnet_users
        except Exception as e:
            logger.error(f"获取测试网用户失败: {e}")
            return []
    
    def disable_testnet_for_non_admins(self, admin_usernames: List[str] = None) -> Dict:
        """
        批量关闭非管理员用户的测试网配置
        
        Args:
            admin_usernames: 管理员用户名列表
            
        Returns:
            {
                "affected_users": [{"uid": "xxx", "username": "user1", "exchange": "binance"}],
                "count": 5
            }
        """
        if admin_usernames is None:
            from core.config import ADMIN_USERS
            admin_usernames = ADMIN_USERS
        
        # 先获取受影响的用户
        affected = self.get_testnet_users(admin_usernames)
        
        if not affected:
            return {"affected_users": [], "count": 0}
        
        try:
            with self._get_connection() as conn:
                # 批量更新：将所有非管理员的测试网配置关闭
                # 使用子查询排除管理员
                if admin_usernames:
                    placeholders = ", ".join([f":admin{i}" for i in range(len(admin_usernames))])
                    params = {f"admin{i}": name for i, name in enumerate(admin_usernames)}
                    
                    if HAS_SQLALCHEMY:
                        conn.execute(text(f"""
                            UPDATE user_exchange_config 
                            SET is_testnet = 0, is_running = 0, updated_at = CURRENT_TIMESTAMP
                            WHERE is_testnet = 1 
                            AND uid IN (
                                SELECT uid FROM users WHERE username NOT IN ({placeholders})
                            )
                        """), params)
                    else:
                        sqlite_placeholders = ", ".join(["?" for _ in admin_usernames])
                        conn.execute(f"""
                            UPDATE user_exchange_config 
                            SET is_testnet = 0, is_running = 0, updated_at = CURRENT_TIMESTAMP
                            WHERE is_testnet = 1 
                            AND uid IN (
                                SELECT uid FROM users WHERE username NOT IN ({sqlite_placeholders})
                            )
                        """, tuple(admin_usernames))
                else:
                    # 没有管理员，关闭所有测试网
                    if HAS_SQLALCHEMY:
                        conn.execute(text("""
                            UPDATE user_exchange_config 
                            SET is_testnet = 0, is_running = 0, updated_at = CURRENT_TIMESTAMP
                            WHERE is_testnet = 1
                        """))
                    else:
                        conn.execute("""
                            UPDATE user_exchange_config 
                            SET is_testnet = 0, is_running = 0, updated_at = CURRENT_TIMESTAMP
                            WHERE is_testnet = 1
                        """)
            
            logger.warning(f"已关闭 {len(affected)} 个非管理员用户的测试网配置")
            for u in affected:
                logger.info(f"  - [{u['uid']}] {u['username']} / {u['exchange']}")
            
            return {"affected_users": affected, "count": len(affected)}
        except Exception as e:
            logger.error(f"批量关闭测试网配置失败: {e}")
            return {"affected_users": [], "count": 0, "error": str(e)}
    
    def get_all_active_users(self) -> List[str]:
        """
        获取所有活跃用户的 UID
        
        活跃用户定义：有启用的交易所配置
        
        用于策略缓存预热
        """
        try:
            with self._get_connection() as conn:
                if HAS_SQLALCHEMY:
                    results = conn.execute(text("""
                        SELECT DISTINCT uid FROM user_exchange_config
                        WHERE is_enabled = 1
                    """)).fetchall()
                else:
                    cursor = conn.execute("""
                        SELECT DISTINCT uid FROM user_exchange_config
                        WHERE is_enabled = 1
                    """)
                    results = cursor.fetchall()
                
                return [r[0] for r in results]
        except Exception as e:
            logger.error(f"获取活跃用户失败: {e}")
            return []
    
    def get_user_ai_strategies_with_keys(self, uid: str) -> List[Dict]:
        """
        获取用户的所有 AI 策略（包含解密的 API Key）
        
        v5.0: 使用 strategy_preset + strategy_overrides 替代 system_prompt
        
        用于缓存预热，返回完整配置
        
        Returns:
            [
                {
                    "id": "strategy_id",
                    "name": "策略名称",
                    "llm_provider": "anthropic",
                    "llm_model": "claude-sonnet-4-20250514",
                    "llm_api_key": "sk-xxx...",  # 解密后的 API Key
                    "llm_base_url": "",
                    "strategy_preset": "default",
                    "strategy_overrides": {...},
                    "has_api_key": True,
                },
                ...
            ]
        """
        import json
        
        try:
            with self._get_connection() as conn:
                if HAS_SQLALCHEMY:
                    results = conn.execute(text("""
                        SELECT strategy_id, name, llm_provider, llm_model, 
                               llm_api_key_encrypted, llm_base_url, 
                               temperature, top_p, max_tokens,
                               strategy_preset, strategy_overrides
                        FROM user_ai_strategies
                        WHERE uid = :uid
                        ORDER BY created_at ASC
                    """), {"uid": uid}).fetchall()
                else:
                    cursor = conn.execute("""
                        SELECT strategy_id, name, llm_provider, llm_model, 
                               llm_api_key_encrypted, llm_base_url,
                               temperature, top_p, max_tokens,
                               strategy_preset, strategy_overrides
                        FROM user_ai_strategies
                        WHERE uid = ?
                        ORDER BY created_at ASC
                    """, (uid,))
                    results = cursor.fetchall()
                
                strategies = []
                for r in results:
                    # 解密 API Key
                    api_key = ""
                    if r[4]:
                        try:
                            api_key = self._decrypt(r[4]) or ""
                        except Exception:
                            pass
                    
                    # 解析 strategy_overrides JSON
                    overrides = {}
                    if r[10]:
                        try:
                            overrides = json.loads(r[10]) if isinstance(r[10], str) else r[10]
                        except (json.JSONDecodeError, ValueError):
                            pass
                    
                    strategies.append({
                        "id": r[0],
                        "name": r[1],
                        "llm_provider": r[2] or "anthropic",
                        "llm_model": r[3] or "",
                        "llm_api_key": api_key,
                        "llm_base_url": r[5] or "",
                        # LLM 参数
                        "temperature": float(r[6]) if r[6] is not None else None,
                        "top_p": float(r[7]) if r[7] is not None else None,
                        "max_tokens": int(r[8]) if r[8] is not None else None,
                        "strategy_preset": r[9] or "default",
                        "strategy_overrides": overrides,
                        "has_api_key": bool(api_key),
                    })
                
                return strategies
        except Exception as e:
            logger.error(f"获取用户 AI 策略失败 [{uid}]: {e}")
            return []
    
    # ============================================================
    # 策略配置方法 (v5.0 - 从 user_ai_strategies 表加载)
    # ============================================================
    
    def get_ai_strategy_with_preset(self, uid: str, strategy_id: str) -> Optional[Dict]:
        """
        获取用户 AI 策略（包含 LLM 配置和策略配置）
        
        v5.0: 从 user_ai_strategies 表加载，包含 strategy_preset 和 strategy_overrides
        
        Args:
            uid: 用户 ID
            strategy_id: 策略 ID
        
        Returns:
            策略配置字典，包含 LLM 配置和策略配置
        """
        try:
            with self._get_connection() as conn:
                if HAS_SQLALCHEMY:
                    result = conn.execute(text("""
                        SELECT strategy_id, name, 
                               llm_provider, llm_model, llm_api_key_encrypted, llm_base_url,
                               temperature, top_p, max_tokens,
                               strategy_preset, strategy_overrides
                        FROM user_ai_strategies
                        WHERE uid = :uid AND strategy_id = :strategy_id
                    """), {"uid": uid, "strategy_id": strategy_id}).fetchone()
                else:
                    cursor = conn.execute("""
                        SELECT strategy_id, name, 
                               llm_provider, llm_model, llm_api_key_encrypted, llm_base_url,
                               temperature, top_p, max_tokens,
                               strategy_preset, strategy_overrides
                        FROM user_ai_strategies
                        WHERE uid = ? AND strategy_id = ?
                    """, (uid, strategy_id))
                    result = cursor.fetchone()
                
                if not result:
                    return None
                
                # 解密 API Key
                api_key = None
                if result[4]:
                    try:
                        api_key = self.crypto.decrypt(result[4])
                    except Exception as e:
                        # P5 Fix: 记录解密失败，可能是密钥问题
                        logger.warning(f"[{uid}] API Key 解密失败: {e}")
                
                return {
                    "strategy_id": result[0],
                    "name": result[1],
                    "llm_provider": result[2] or "anthropic",
                    "llm_model": result[3] or "",
                    "llm_api_key": api_key,
                    "llm_base_url": result[5] or "",
                    "temperature": float(result[6]) if result[6] is not None else None,
                    "top_p": float(result[7]) if result[7] is not None else None,
                    "max_tokens": int(result[8]) if result[8] is not None else None,
                    "strategy_preset": result[9],
                    "strategy_overrides": result[10]
                }
        except Exception as e:
            logger.error(f"获取 AI 策略失败 [{uid}/{strategy_id}]: {e}")
            return None
    
    def get_strategy_templates(self, preset_name: str) -> List[Dict]:
        """
        获取预设策略模板
        
        从 strategy_templates 表加载指定预设的所有分类内容
        
        Args:
            preset_name: 预设名称 (conservative/aggressive/trend_following/mean_reversion/default)
        
        Returns:
            模板列表 [{category, content, display_order}]
        """
        try:
            with self._get_connection() as conn:
                if HAS_SQLALCHEMY:
                    results = conn.execute(text("""
                        SELECT category, content, display_order
                        FROM strategy_templates
                        WHERE preset_name = :preset_name AND is_enabled = 1
                        ORDER BY display_order
                    """), {"preset_name": preset_name}).fetchall()
                else:
                    cursor = conn.execute("""
                        SELECT category, content, display_order
                        FROM strategy_templates
                        WHERE preset_name = ? AND is_enabled = 1
                        ORDER BY display_order
                    """, (preset_name,))
                    results = cursor.fetchall()
                
                return [
                    {"category": r[0], "content": r[1], "display_order": r[2]}
                    for r in results
                ]
        except Exception as e:
            logger.error(f"获取策略模板失败 [{preset_name}]: {e}")
            return []
    
    def save_ai_strategy(
        self,
        uid: str,
        strategy_id: str,
        name: str,
        llm_provider: str = "anthropic",
        llm_model: str = "",
        llm_api_key: Optional[str] = None,
        llm_base_url: Optional[str] = None,
        temperature: Optional[float] = None,
        top_p: Optional[float] = None,
        max_tokens: Optional[int] = None,
        strategy_preset: Optional[str] = None,
        strategy_overrides: Optional[str] = None
    ) -> bool:
        """
        保存用户 AI 策略
        
        Args:
            uid: 用户 ID
            strategy_id: 策略 ID
            name: 策略名称
            llm_provider: LLM 提供商
            llm_model: 模型名称
            llm_api_key: API Key（明文，会加密存储）
            llm_base_url: 自定义 API 地址
            temperature: Temperature 参数 (0-2)
            top_p: Top P 参数 (0-1)
            max_tokens: Max Tokens 参数
            strategy_preset: 预设策略名称
            strategy_overrides: 自定义覆盖 (JSON 字符串)
        
        Returns:
            是否保存成功
        """
        try:
            # 加密 API Key
            encrypted_api_key = None
            if llm_api_key:
                encrypted_api_key = self.crypto.encrypt(llm_api_key)
            
            is_sqlite = self._is_sqlite()
            
            with self._get_connection() as conn:
                if HAS_SQLALCHEMY:
                    if is_sqlite:
                        # SQLite with SQLAlchemy
                        conn.execute(text("""
                            INSERT OR REPLACE INTO user_ai_strategies 
                            (uid, strategy_id, name, llm_provider, llm_model,
                             llm_api_key_encrypted, llm_base_url, temperature, top_p, max_tokens,
                             strategy_preset, strategy_overrides)
                            VALUES (:uid, :strategy_id, :name, :llm_provider, :llm_model,
                                    :api_key, :base_url, :temperature, :top_p, :max_tokens,
                                    :preset, :overrides)
                        """), {
                            "uid": uid, "strategy_id": strategy_id, "name": name,
                            "llm_provider": llm_provider, "llm_model": llm_model,
                            "api_key": encrypted_api_key, "base_url": llm_base_url,
                            "temperature": temperature, "top_p": top_p, "max_tokens": max_tokens,
                            "preset": strategy_preset, "overrides": strategy_overrides
                        })
                    else:
                        # MySQL with SQLAlchemy
                        conn.execute(text("""
                            INSERT INTO user_ai_strategies 
                            (uid, strategy_id, name, llm_provider, llm_model, 
                             llm_api_key_encrypted, llm_base_url, temperature, top_p, max_tokens,
                             strategy_preset, strategy_overrides)
                            VALUES (:uid, :strategy_id, :name, :llm_provider, :llm_model,
                                    :api_key, :base_url, :temperature, :top_p, :max_tokens,
                                    :preset, :overrides)
                            ON DUPLICATE KEY UPDATE
                                name = VALUES(name),
                                llm_provider = VALUES(llm_provider),
                                llm_model = VALUES(llm_model),
                                llm_api_key_encrypted = VALUES(llm_api_key_encrypted),
                                llm_base_url = VALUES(llm_base_url),
                                temperature = VALUES(temperature),
                                top_p = VALUES(top_p),
                                max_tokens = VALUES(max_tokens),
                                strategy_preset = VALUES(strategy_preset),
                                strategy_overrides = VALUES(strategy_overrides),
                                updated_at = CURRENT_TIMESTAMP
                        """), {
                            "uid": uid, "strategy_id": strategy_id, "name": name,
                            "llm_provider": llm_provider, "llm_model": llm_model,
                            "api_key": encrypted_api_key, "base_url": llm_base_url,
                            "temperature": temperature, "top_p": top_p, "max_tokens": max_tokens,
                            "preset": strategy_preset, "overrides": strategy_overrides
                        })
                else:
                    # SQLite without SQLAlchemy
                    conn.execute("""
                        INSERT OR REPLACE INTO user_ai_strategies 
                        (uid, strategy_id, name, llm_provider, llm_model,
                         llm_api_key_encrypted, llm_base_url, temperature, top_p, max_tokens,
                         strategy_preset, strategy_overrides)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (uid, strategy_id, name, llm_provider, llm_model,
                          encrypted_api_key, llm_base_url, temperature, top_p, max_tokens,
                          strategy_preset, strategy_overrides))
                    conn.commit()
                return True
        except Exception as e:
            logger.error(f"保存 AI 策略失败 [{uid}/{strategy_id}]: {e}")
            return False
    
    def update_strategy_overrides(self, uid: str, strategy_id: str, strategy_overrides: Optional[str]) -> bool:
        """
        更新策略的 strategy_overrides 字段
        
        用于重载策略时，从预设表重新加载最新的模板内容
        
        Args:
            uid: 用户 ID
            strategy_id: 策略 ID
            strategy_overrides: 新的覆盖内容 (JSON 字符串)
        
        Returns:
            是否更新成功
        """
        try:
            with self._get_connection() as conn:
                if HAS_SQLALCHEMY:
                    conn.execute(text("""
                        UPDATE user_ai_strategies 
                        SET strategy_overrides = :overrides, updated_at = CURRENT_TIMESTAMP
                        WHERE uid = :uid AND strategy_id = :strategy_id
                    """), {"uid": uid, "strategy_id": strategy_id, "overrides": strategy_overrides})
                else:
                    conn.execute("""
                        UPDATE user_ai_strategies 
                        SET strategy_overrides = ?, updated_at = CURRENT_TIMESTAMP
                        WHERE uid = ? AND strategy_id = ?
                    """, (strategy_overrides, uid, strategy_id))
                    conn.commit()
                
                logger.info(f"[{uid}] 策略 {strategy_id} 的 strategy_overrides 已更新")
                return True
        except Exception as e:
            logger.error(f"更新 strategy_overrides 失败 [{uid}/{strategy_id}]: {e}")
            return False
    
    def list_strategy_presets(self) -> List[Dict]:
        """
        列出所有可用的预设策略
        
        只返回所有分类都启用的预设（整体启用）
        
        Returns:
            预设列表 [{name, description, category_count}]
        """
        try:
            with self._get_connection() as conn:
                if HAS_SQLALCHEMY:
                    # 只返回所有分类都启用的预设
                    # 通过比较总分类数和启用分类数来判断
                    results = conn.execute(text("""
                        SELECT preset_name as name, 
                               COUNT(*) as category_count,
                               SUM(CASE WHEN is_enabled = 1 THEN 1 ELSE 0 END) as enabled_count,
                               MAX(description) as description
                        FROM strategy_templates
                        GROUP BY preset_name
                        HAVING COUNT(*) = SUM(CASE WHEN is_enabled = 1 THEN 1 ELSE 0 END)
                        ORDER BY preset_name
                    """)).fetchall()
                else:
                    cursor = conn.execute("""
                        SELECT preset_name, COUNT(*), 
                               SUM(CASE WHEN is_enabled = 1 THEN 1 ELSE 0 END),
                               MAX(description)
                        FROM strategy_templates
                        GROUP BY preset_name
                        HAVING COUNT(*) = SUM(CASE WHEN is_enabled = 1 THEN 1 ELSE 0 END)
                        ORDER BY preset_name
                    """)
                    results = cursor.fetchall()
                
                return [
                    {"name": r[0], "category_count": r[1], "description": r[3]}
                    for r in results
                ]
        except Exception as e:
            logger.error(f"获取策略预设列表失败: {e}")
            return []

    # ============================================================
    # 用户自定义策略模板管理
    # ============================================================
    
    def list_user_strategy_presets(self, uid: str) -> List[Dict]:
        """
        列出用户的自定义策略预设
        
        Returns:
            预设列表 [{name, description, category_count, is_user_preset: True}]
        """
        try:
            with self._get_connection() as conn:
                if HAS_SQLALCHEMY:
                    results = conn.execute(text("""
                        SELECT preset_name as name, 
                               COUNT(*) as category_count,
                               MAX(description) as description
                        FROM user_strategy_templates
                        WHERE uid = :uid
                        GROUP BY preset_name
                        ORDER BY preset_name
                    """), {"uid": uid}).fetchall()
                else:
                    cursor = conn.execute("""
                        SELECT preset_name, COUNT(*), MAX(description)
                        FROM user_strategy_templates
                        WHERE uid = ?
                        GROUP BY preset_name
                        ORDER BY preset_name
                    """, (uid,))
                    results = cursor.fetchall()
                
                return [
                    {"name": r[0], "category_count": r[1], "description": r[2], "is_user_preset": True}
                    for r in results
                ]
        except Exception as e:
            logger.error(f"获取用户策略预设列表失败: {e}")
            return []
    
    def get_user_strategy_templates(self, uid: str, preset_name: str) -> List[Dict]:
        """
        获取用户自定义预设的所有分类内容
        
        Returns:
            模板列表 [{category, content, display_order}]
        """
        try:
            with self._get_connection() as conn:
                if HAS_SQLALCHEMY:
                    results = conn.execute(text("""
                        SELECT category, content, display_order, description
                        FROM user_strategy_templates
                        WHERE uid = :uid AND preset_name = :preset_name
                        ORDER BY display_order, category
                    """), {"uid": uid, "preset_name": preset_name}).fetchall()
                else:
                    cursor = conn.execute("""
                        SELECT category, content, display_order, description
                        FROM user_strategy_templates
                        WHERE uid = ? AND preset_name = ?
                        ORDER BY display_order, category
                    """, (uid, preset_name))
                    results = cursor.fetchall()
                
                return [
                    {"category": r[0], "content": r[1], "display_order": r[2], "description": r[3]}
                    for r in results
                ]
        except Exception as e:
            logger.error(f"获取用户策略模板失败: {e}")
            return []
    
    def save_user_strategy_preset(self, uid: str, preset_name: str, description: str, categories: Dict[str, str]) -> bool:
        """
        保存用户自定义策略预设
        
        Args:
            uid: 用户ID
            preset_name: 预设名称
            description: 预设描述
            categories: {category: content} 字典
            
        Returns:
            是否成功
        """
        category_order = {
            "role": 1, "risk_rules": 2, "entry_conditions": 3, "exit_conditions": 4,
            "position_sizing": 5, "market_preferences": 6, "adaptive_rules": 7
        }
        
        try:
            with self._get_connection() as conn:
                if HAS_SQLALCHEMY:
                    # 先删除旧的
                    conn.execute(text("""
                        DELETE FROM user_strategy_templates 
                        WHERE uid = :uid AND preset_name = :preset_name
                    """), {"uid": uid, "preset_name": preset_name})
                    
                    # 插入新的
                    for category, content in categories.items():
                        if content:  # 只保存非空内容
                            conn.execute(text("""
                                INSERT INTO user_strategy_templates 
                                (uid, preset_name, category, content, display_order, description)
                                VALUES (:uid, :preset_name, :category, :content, :display_order, :description)
                            """), {
                                "uid": uid,
                                "preset_name": preset_name,
                                "category": category,
                                "content": content,
                                "display_order": category_order.get(category, 99),
                                "description": description
                            })
                else:
                    # SQLite 分支
                    conn.execute("""
                        DELETE FROM user_strategy_templates 
                        WHERE uid = ? AND preset_name = ?
                    """, (uid, preset_name))
                    
                    for category, content in categories.items():
                        if content:
                            conn.execute("""
                                INSERT INTO user_strategy_templates 
                                (uid, preset_name, category, content, display_order, description)
                                VALUES (?, ?, ?, ?, ?, ?)
                            """, (uid, preset_name, category, content, 
                                  category_order.get(category, 99), description))
                
                return True
        except Exception as e:
            logger.error(f"保存用户策略预设失败: {e}")
            return False
    
    def delete_user_strategy_preset(self, uid: str, preset_name: str) -> bool:
        """
        删除用户自定义策略预设
        """
        try:
            with self._get_connection() as conn:
                if HAS_SQLALCHEMY:
                    result = conn.execute(text("""
                        DELETE FROM user_strategy_templates 
                        WHERE uid = :uid AND preset_name = :preset_name
                    """), {"uid": uid, "preset_name": preset_name})
                    deleted = result.rowcount
                else:
                    cursor = conn.execute("""
                        DELETE FROM user_strategy_templates 
                        WHERE uid = ? AND preset_name = ?
                    """, (uid, preset_name))
                    deleted = cursor.rowcount
                
                return deleted > 0
        except Exception as e:
            logger.error(f"删除用户策略预设失败: {e}")
            return False


# 全局配置加载器
config_loader = UserConfigLoader()
