#!/usr/bin/env python3
"""
Redis数据访问层 - 支持新的聚合键结构
提供向后兼容的接口
"""
import json
import time
import logging
from typing import Dict, List, Optional, Any, Union
from core.database import redis_client, RedisKeys

logger = logging.getLogger(__name__)

class RedisDataManager:
    """Redis数据管理器 - 终极优化：每个用户一个超级聚合键"""

    # ==================== 用户生命周期 ====================
    @staticmethod
    def init_user(uid: str) -> bool:
        """初始化新用户，设置默认数据"""
        key = RedisKeys.user(uid)

        # 检查用户是否已存在
        if redis_client.exists(key):
            return False

        # 设置默认元数据
        RedisDataManager.set_user_field(uid, RedisKeys.field_metadata(), RedisKeys.default_metadata())

        # 初始化空数据结构
        empty_structures = {
            RedisKeys.field_positions(): {},
            RedisKeys.field_positions_active(): [],
            RedisKeys.field_trades(): [],
            RedisKeys.field_trades_closed(): {},
            RedisKeys.field_ai_history(): {},
            RedisKeys.field_decision_feedback(): {},
            RedisKeys.field_notifications(): [],
            RedisKeys.field_cache(): {}
        }

        for field, value in empty_structures.items():
            RedisDataManager.set_user_field(uid, field, value)

        return True

    # ==================== 基础字段操作 ====================
    @staticmethod
    def get_user_field(uid: str, field: str) -> Any:
        """从用户聚合键获取字段值"""
        key = RedisKeys.user(uid)
        data = redis_client.hget(key, field)
        if data is None:
            return None
        if isinstance(data, bytes):
            data = data.decode('utf-8')
        # 尝试JSON解析
        try:
            return json.loads(data)
        except (json.JSONDecodeError, TypeError):
            return data

    @staticmethod
    def set_user_field(uid: str, field: str, value: Any, update_active: bool = True):
        """设置用户聚合键字段值"""
        key = RedisKeys.user(uid)
        if isinstance(value, (dict, list)):
            value = json.dumps(value, ensure_ascii=False)

        redis_client.hset(key, field, value)

        # 更新最后活跃时间（避免递归）
        if update_active and field != RedisKeys.field_metadata():
            RedisDataManager.update_last_active(uid)

    @staticmethod
    def get_user_all(uid: str) -> Dict[str, Any]:
        """获取用户所有数据"""
        key = RedisKeys.user(uid)
        data = redis_client.hgetall(key)
        result = {}
        for k, v in data.items():
            field = k.decode('utf-8') if isinstance(k, bytes) else k
            value_str = v.decode('utf-8') if isinstance(v, bytes) else v
            # 尝试JSON解析
            try:
                result[field] = json.loads(value_str)
            except (json.JSONDecodeError, TypeError):
                result[field] = value_str
        return result

    @staticmethod
    def update_last_active(uid: str):
        """更新用户最后活跃时间"""
        metadata = RedisDataManager.get_user_field(uid, RedisKeys.field_metadata()) or RedisKeys.default_metadata()
        metadata["last_active"] = time.time()
        RedisDataManager.set_user_field(uid, RedisKeys.field_metadata(), metadata)

    # ==================== 账户和持仓管理 ====================
    @staticmethod
    def get_user_account(uid: str) -> Optional[str]:
        """获取用户账户数据"""
        return RedisDataManager.get_user_field(uid, RedisKeys.field_account())

    @staticmethod
    def set_user_account(uid: str, account_data: str):
        """设置用户账户数据"""
        RedisDataManager.set_user_field(uid, RedisKeys.field_account(), account_data)

    @staticmethod
    def get_user_positions(uid: str) -> Dict:
        """获取用户持仓数据"""
        return RedisDataManager.get_user_field(uid, RedisKeys.field_positions()) or {}

    @staticmethod
    def set_user_positions(uid: str, positions_data: Dict):
        """设置用户持仓数据"""
        RedisDataManager.set_user_field(uid, RedisKeys.field_positions(), positions_data)

    @staticmethod
    def get_user_positions_active(uid: str) -> List[str]:
        """获取用户活跃持仓"""
        return RedisDataManager.get_user_field(uid, RedisKeys.field_positions_active()) or []

    @staticmethod
    def set_user_positions_active(uid: str, active_positions: List[str]):
        """设置用户活跃持仓"""
        RedisDataManager.set_user_field(uid, RedisKeys.field_positions_active(), active_positions)

    @staticmethod
    def get_user_equity_init(uid: str) -> Optional[str]:
        """获取用户初始权益"""
        return RedisDataManager.get_user_field(uid, RedisKeys.field_equity_init())

    @staticmethod
    def set_user_equity_init(uid: str, equity_data: str):
        """设置用户初始权益"""
        RedisDataManager.set_user_field(uid, RedisKeys.field_equity_init(), equity_data)

    # ==================== 交易记录管理 ====================
    @staticmethod
    def add_user_trade(uid: str, trade_record: Dict):
        """添加用户交易记录（永久保存）"""
        trades = RedisDataManager.get_user_field(uid, RedisKeys.field_trades()) or []
        if not isinstance(trades, list):
            trades = []
        trades.insert(0, trade_record)  # 新交易放在前面
        # 永久保存，不限制数量
        RedisDataManager.set_user_field(uid, RedisKeys.field_trades(), trades)
        RedisDataManager._update_data_count(uid, "trades", len(trades))

    @staticmethod
    def get_user_trades(uid: str, limit: int = 50, offset: int = 0) -> List[Dict]:
        """获取用户交易记录"""
        trades = RedisDataManager.get_user_field(uid, RedisKeys.field_trades()) or []
        if not isinstance(trades, list):
            return []

        return trades[offset:offset + limit]

    @staticmethod
    def add_closed_trade(uid: str, trade_data: Dict, timestamp: Optional[float] = None):
        """添加已关闭交易记录（永久保存）"""
        if timestamp is None:
            timestamp = time.time()

        closed_trades = RedisDataManager.get_user_field(uid, RedisKeys.field_trades_closed()) or {}
        if not isinstance(closed_trades, dict):
            closed_trades = {}

        # 添加新记录
        closed_trades[str(timestamp)] = trade_data

        # 永久保存，不清理
        RedisDataManager.set_user_field(uid, RedisKeys.field_trades_closed(), closed_trades)
        RedisDataManager._update_data_count(uid, "trades_closed", len(closed_trades))

    @staticmethod
    def get_closed_trades(uid: str, limit: int = 50, offset: int = 0) -> List[Dict]:
        """获取已关闭交易记录"""
        closed_trades = RedisDataManager.get_user_field(uid, RedisKeys.field_trades_closed()) or {}
        if not isinstance(closed_trades, dict):
            return []

        # 按时间戳倒序排序
        sorted_trades = sorted(closed_trades.items(), key=lambda x: float(x[0]), reverse=True)
        paginated_items = sorted_trades[offset:offset + limit]

        return [trade_data for _, trade_data in paginated_items]

    # ==================== AI历史管理 ====================
    @staticmethod
    def add_ai_history(uid: str, request_data: Dict, response_data: Dict, timestamp: Optional[float] = None) -> Optional[int]:
        """
        添加AI历史记录到 MySQL（永久保存）
        
        注意：已完全迁移到 MySQL，不再写入 Redis
        
        Returns:
            decision_id: 插入的记录 ID，失败返回 None
        """
        if timestamp is None:
            timestamp = time.time()
        
        try:
            from core.ai_decision_db import get_ai_decision_db
            db = get_ai_decision_db()
            return db.add_decision(uid, request_data, response_data, timestamp)
        except Exception as e:
            logger.error(f"[AI-HISTORY] Failed to write MySQL for uid {uid}: {e}")
            return None

    @staticmethod
    def get_ai_history(uid: str, limit: int = 50, offset: int = 0) -> List[Dict]:
        """
        [废弃] 从 Redis 获取 AI 历史记录
        
        注意：AI 历史已迁移到 MySQL，请使用 core.async_redis.get_ai_history_paginated_async
        此方法保留仅为兼容性，返回空列表
        """
        logger.warning(f"[DEPRECATED] get_ai_history called for {uid}, use MySQL instead")
        return []

    @staticmethod
    def get_ai_history_paginated(uid: str, offset: int = 0, limit: int = 50) -> tuple[List[Dict], int]:
        """
        [废弃] 从 Redis 获取 AI 历史记录（分页）
        
        注意：AI 历史已迁移到 MySQL，请使用 core.async_redis.get_ai_history_paginated_async
        此方法保留仅为兼容性，返回空结果
        """
        logger.warning(f"[DEPRECATED] get_ai_history_paginated called for {uid}, use MySQL instead")
        return [], 0

    # ==================== K线数据管理 ====================
    # K线数据完全全局共享，用户直接从全局存储读取，无需任何用户级存储

    @staticmethod
    def get_global_klines(symbol: str, interval: str) -> Dict[str, Dict]:
        """从全局存储获取K线数据"""
        key = RedisKeys.market_klines(symbol, interval)
        kline_data = redis_client.hgetall(key)

        result = {}
        for ts, data_json in kline_data.items():
            ts_str = ts.decode('utf-8') if isinstance(ts, bytes) else ts
            data_str = data_json.decode('utf-8') if isinstance(data_json, bytes) else data_json
            try:
                result[ts_str] = json.loads(data_str)
            except json.JSONDecodeError:
                continue
        return result

    @staticmethod
    def set_global_klines(symbol: str, interval: str, kline_data: Dict[str, Dict]):
        """设置全局K线数据"""
        key = RedisKeys.market_klines(symbol, interval)
        # 清空现有数据
        redis_client.delete(key)

        # 批量设置新数据
        if kline_data:
            pipeline = redis_client.pipeline()
            for ts, data in kline_data.items():
                pipeline.hset(key, ts, json.dumps(data, ensure_ascii=False))
            pipeline.execute()

    @staticmethod
    def get_user_klines(uid: str, symbol: str, interval: str) -> Dict[str, Dict]:
        """获取用户的K线数据（直接从全局存储获取）"""
        return RedisDataManager.get_global_klines(symbol, interval)

    # ==================== 决策反馈管理 ====================
    @staticmethod
    def set_decision_feedback(uid: str, feedback_data: Dict):
        """设置决策反馈数据"""
        RedisDataManager.set_user_field(uid, RedisKeys.field_decision_feedback(), feedback_data)

    @staticmethod
    def get_decision_feedback(uid: str) -> Dict:
        """获取决策反馈数据"""
        return RedisDataManager.get_user_field(uid, RedisKeys.field_decision_feedback()) or {}

    @staticmethod
    def add_decision_feedback(uid: str, key: str, feedback: Dict):
        """添加单个决策反馈"""
        feedback_data = RedisDataManager.get_decision_feedback(uid)
        feedback_data[key] = feedback
        RedisDataManager.set_user_field(uid, RedisKeys.field_decision_feedback(), feedback_data)

    # ==================== 通知管理 ====================
    @staticmethod
    def add_notification(uid: str, notification: Dict):
        """添加用户通知"""
        notifications = RedisDataManager.get_user_field(uid, RedisKeys.field_notifications()) or []
        if not isinstance(notifications, list):
            notifications = []

        notifications.insert(0, notification)  # 新通知放在前面

        # 自动清理过期通知
        notifications = RedisDataManager._cleanup_list_data(notifications, 200)

        RedisDataManager.set_user_field(uid, RedisKeys.field_notifications(), notifications)
        RedisDataManager._update_data_count(uid, "notifications", len(notifications))

    @staticmethod
    def get_notifications(uid: str, limit: int = 20, unread_only: bool = False) -> List[Dict]:
        """获取用户通知"""
        notifications = RedisDataManager.get_user_field(uid, RedisKeys.field_notifications()) or []
        if not isinstance(notifications, list):
            return []

        if unread_only:
            notifications = [n for n in notifications if not n.get("read", False)]

        return notifications[:limit]

    # ==================== 缓存管理 ====================
    @staticmethod
    def set_user_cache(uid: str, cache_key: str, data: Any, ttl_seconds: int = 3600):
        """设置用户缓存数据"""
        cache = RedisDataManager.get_user_field(uid, RedisKeys.field_cache()) or {}
        if not isinstance(cache, dict):
            cache = {}

        cache[cache_key] = {
            "data": data,
            "expires_at": time.time() + ttl_seconds
        }

        RedisDataManager.set_user_field(uid, RedisKeys.field_cache(), cache)

    @staticmethod
    def get_user_cache(uid: str, cache_key: str) -> Any:
        """获取用户缓存数据"""
        cache = RedisDataManager.get_user_field(uid, RedisKeys.field_cache()) or {}
        if not isinstance(cache, dict):
            return None

        cache_item = cache.get(cache_key)
        if not cache_item:
            return None

        # 检查是否过期
        if time.time() > cache_item.get("expires_at", 0):
            # 清理过期缓存
            del cache[cache_key]
            RedisDataManager.set_user_field(uid, RedisKeys.field_cache(), cache)
            return None

        return cache_item.get("data")

    # ==================== 数据清理和维护 ====================
    @staticmethod
    def _cleanup_list_data(data: List, max_count: int) -> List:
        """清理列表数据，保留最新的max_count条"""
        return data[:max_count]

    @staticmethod
    def _cleanup_dict_data(data: Dict, max_count: int) -> Dict:
        """清理字典数据，保留最新的max_count条"""
        if len(data) <= max_count:
            return data

        # 按键排序（假设键是时间戳），保留最新的
        try:
            sorted_items = sorted(data.items(), key=lambda x: float(x[0]), reverse=True)
            return dict(sorted_items[:max_count])
        except (ValueError, TypeError):
            # 如果键不是数字，保留前max_count个
            items = list(data.items())[:max_count]
            return dict(items)

    @staticmethod
    def _update_data_count(uid: str, field: str, count: int):
        """更新元数据中的数据计数"""
        metadata = RedisDataManager.get_user_field(uid, RedisKeys.field_metadata()) or RedisKeys.default_metadata()
        if "data_counts" not in metadata:
            metadata["data_counts"] = {}
        metadata["data_counts"][field] = count
        RedisDataManager.set_user_field(uid, RedisKeys.field_metadata(), metadata)

    @staticmethod
    def cleanup_expired_data(uid: str):
        """清理用户过期数据（只清理缓存和通知）"""
        # 清理过期缓存
        cache = RedisDataManager.get_user_field(uid, RedisKeys.field_cache()) or {}
        if isinstance(cache, dict):
            current_time = time.time()
            expired_keys = [k for k, v in cache.items()
                          if isinstance(v, dict) and current_time > v.get("expires_at", 0)]

            for key in expired_keys:
                del cache[key]

            if expired_keys:
                RedisDataManager.set_user_field(uid, RedisKeys.field_cache(), cache)

        # 清理过期通知（保留最近200条）
        notifications = RedisDataManager.get_user_field(uid, RedisKeys.field_notifications()) or []
        if isinstance(notifications, list) and len(notifications) > 200:
            notifications = notifications[:200]
            RedisDataManager.set_user_field(uid, RedisKeys.field_notifications(), notifications)

    @staticmethod
    def get_user_stats(uid: str) -> Dict:
        """
        获取用户数据统计
        
        包括：
        - 交易记录数量（从 MySQL）
        - 已平仓交易数量（从 MySQL）
        - AI 历史数量（从 MySQL）
        - 数据大小估算
        """
        import time as _time
        _start = _time.perf_counter()
        
        user_key = f"user:{uid}"
        
        # 获取 Redis 中的基础字段
        pipe = redis_client.pipeline()
        pipe.hget(user_key, RedisKeys.field_metadata())
        pipe.hlen(user_key)  # 获取 hash 中的字段数量
        pipe.hget(user_key, RedisKeys.field_notifications())
        results = pipe.execute()
        
        # 解析结果
        metadata = {}
        if results[0]:
            try:
                metadata = json.loads(results[0])
            except (json.JSONDecodeError, ValueError) as e:
                logger.debug(f"[redis-stats] 解析 metadata 失败: {e}")
        
        total_fields = results[1] or 0
        
        # 通知数量
        notifications_count = 0
        if results[2]:
            try:
                notifications = json.loads(results[2])
                if isinstance(notifications, list):
                    notifications_count = len(notifications)
            except (json.JSONDecodeError, ValueError) as e:
                logger.debug(f"[redis-stats] 解析 notifications 失败: {e}")
        
        # 从 MySQL 获取已平仓交易数量
        closed_trades_count = 0
        try:
            from core.closed_trades_db import get_closed_trades_db
            db = get_closed_trades_db()
            closed_trades_count = db.get_user_trade_count(uid)
        except Exception as e:
            logger.warning(f"[redis-stats] Failed to get closed trades count from MySQL: {e}")
        
        # 从 MySQL 获取 AI 历史数量
        ai_history_count = 0
        try:
            from core.ai_decision_db import get_ai_decision_db
            db = get_ai_decision_db()
            ai_history_count = db.get_user_record_count(uid)
        except Exception as e:
            logger.warning(f"[redis-stats] Failed to get AI history count from MySQL: {e}")
        
        _elapsed = (_time.perf_counter() - _start) * 1000
        logger.info(f"[redis-stats] uid={uid}: {_elapsed:.1f}ms, closed_trades={closed_trades_count}, ai_history={ai_history_count}")

        return {
            "user_id": uid,
            "created_at": metadata.get("created_at") if metadata else None,
            "last_active": metadata.get("last_active") if metadata else None,
            "trades_count": closed_trades_count,  # 交易记录数 = 已平仓交易数
            "closed_trades_count": closed_trades_count,
            "ai_history_count": ai_history_count,
            "notifications_count": notifications_count,
            "total_keys": 1,
            "total_fields": total_fields,
            "data_size": 0  # 不再从 Redis 统计，closed_trades 已迁移到 MySQL
        }

    # ==================== 系统级操作 ====================
    @staticmethod
    def get_all_users() -> List[str]:
        """获取所有用户ID（通过键模式匹配）"""
        keys = redis_client.keys("user:*")
        uids = []
        for key in keys:
            if isinstance(key, bytes):
                key_str = key.decode('utf-8')
            else:
                key_str = str(key)
            if key_str.startswith("user:"):
                uid = key_str.split(":", 1)[1]
                uids.append(uid)
        return uids

    @staticmethod
    def delete_user(uid: str) -> bool:
        """删除用户所有数据"""
        key = RedisKeys.user(uid)
        return bool(redis_client.delete(key))

    # ==================== 批量操作（性能优化）====================
    
    @staticmethod
    def get_user_fields_batch(uid: str, fields: List[str]) -> Dict[str, Any]:
        """
        批量获取用户多个字段（使用 HMGET，单次网络往返）
        
        Args:
            uid: 用户 ID
            fields: 字段列表
            
        Returns:
            {field: value, ...} 字典，不存在的字段值为 None
        """
        if not fields:
            return {}
        
        key = RedisKeys.user(uid)
        values = redis_client.hmget(key, fields)
        
        result = {}
        for i, field in enumerate(fields):
            data = values[i]
            if data is None:
                result[field] = None
                continue
            if isinstance(data, bytes):
                data = data.decode('utf-8')
            # 尝试 JSON 解析
            try:
                result[field] = json.loads(data)
            except (json.JSONDecodeError, TypeError):
                result[field] = data
        
        return result
    
    @staticmethod
    def _get_redis():
        """获取 Redis 客户端（供内部使用）"""
        return redis_client

