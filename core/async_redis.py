# core/async_redis.py
"""
异步 Redis 客户端模块

专门用于 Web API 层的异步读取操作，不影响执行层的同步写入。

设计原则：
1. 只用于读取操作（web.py 等 API 层）
2. 执行层继续使用同步客户端（database.py）
3. 两个客户端连接同一个 Redis 实例，互不干扰

使用方式：
    from core.async_redis import get_async_redis, async_pf_compat
    
    # 方式1：直接使用异步客户端
    async_redis = await get_async_redis()
    data = await async_redis.hget("user:xxx", "field")
    
    # 方式2：使用异步兼容层（推荐）
    positions = await async_pf_compat.get_pf_pos_async(uid, exchange)
"""

import json
import logging
import asyncio
import threading
from typing import Dict, List, Any, Optional

logger = logging.getLogger(__name__)

# 异步 Redis 客户端（延迟初始化）
_async_redis_client = None
_async_redis_lock = None  # 延迟初始化，避免在模块加载时创建
_async_redis_init_lock = threading.Lock()  # 用于保护 asyncio.Lock 的创建


async def get_async_redis():
    """
    获取异步 Redis 客户端（单例，延迟初始化）
    
    使用 redis.asyncio 模块，需要 redis>=4.2.0
    """
    global _async_redis_client, _async_redis_lock
    
    if _async_redis_client is None:
        # 延迟初始化 asyncio.Lock，使用 threading.Lock 保护
        if _async_redis_lock is None:
            with _async_redis_init_lock:
                if _async_redis_lock is None:
                    _async_redis_lock = asyncio.Lock()
        
        async with _async_redis_lock:
            if _async_redis_client is None:
                try:
                    import redis.asyncio as aioredis
                    from core.config import REDIS_HOST, REDIS_PORT, REDIS_DB
                    
                    _async_redis_client = aioredis.Redis(
                        host=REDIS_HOST,
                        port=REDIS_PORT,
                        db=REDIS_DB,
                        decode_responses=True,
                        # 连接池配置
                        max_connections=20,
                        socket_timeout=5.0,
                        socket_connect_timeout=5.0,
                    )
                    logger.info(f"[AsyncRedis] 异步 Redis 客户端已初始化: {REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}")
                except ImportError:
                    logger.error("[AsyncRedis] 需要 redis>=4.2.0，请运行: pip install redis>=4.2.0")
                    raise
    
    return _async_redis_client


async def close_async_redis():
    """关闭异步 Redis 客户端"""
    global _async_redis_client
    
    if _async_redis_client is not None:
        await _async_redis_client.close()
        _async_redis_client = None
        logger.info("[AsyncRedis] 异步 Redis 客户端已关闭")


# =============================================================================
# 异步 Redis 数据管理器
# =============================================================================

class AsyncRedisDataManager:
    """
    异步 Redis 数据管理器
    
    提供与 RedisDataManager 相同的接口，但使用异步操作
    """
    
    @staticmethod
    async def get_user_field(uid: str, field: str) -> Any:
        """异步获取用户单个字段"""
        redis = await get_async_redis()
        from core.database import RedisKeys
        
        key = RedisKeys.user(uid)
        data = await redis.hget(key, field)
        
        if data is None:
            return None
        
        # 尝试 JSON 解析
        try:
            return json.loads(data)
        except (json.JSONDecodeError, TypeError):
            return data
    
    @staticmethod
    async def get_user_fields_batch(uid: str, fields: List[str]) -> Dict[str, Any]:
        """
        异步批量获取用户多个字段（使用 HMGET，单次网络往返）
        """
        if not fields:
            return {}
        
        redis = await get_async_redis()
        from core.database import RedisKeys
        
        key = RedisKeys.user(uid)
        values = await redis.hmget(key, fields)
        
        result = {}
        for i, field in enumerate(fields):
            data = values[i]
            if data is None:
                result[field] = None
                continue
            # 尝试 JSON 解析
            try:
                result[field] = json.loads(data)
            except (json.JSONDecodeError, TypeError):
                result[field] = data
        
        return result
    
    @staticmethod
    async def get_user_all(uid: str) -> Dict[str, Any]:
        """异步获取用户所有字段"""
        redis = await get_async_redis()
        from core.database import RedisKeys
        
        key = RedisKeys.user(uid)
        data = await redis.hgetall(key)
        
        result = {}
        for field, value in data.items():
            try:
                result[field] = json.loads(value)
            except (json.JSONDecodeError, TypeError):
                result[field] = value
        
        return result


# =============================================================================
# 异步 PF 兼容层
# =============================================================================

# 支持的交易所列表
SUPPORTED_EXCHANGES = ["binance", "okx", "bitget", "hyperliquid"]


class AsyncPFCompatibilityLayer:
    """
    异步版本的 pf:* 键兼容层
    
    提供与 PFCompatibilityLayer 相同的接口，但使用异步操作
    """
    
    @staticmethod
    def _get_user_enabled_exchanges(uid: str) -> List[str]:
        """获取用户启用的交易所列表（同步，因为是配置读取）"""
        try:
            from core.user_db import config_loader
            return config_loader.get_enabled_exchanges(uid) or []
        except Exception:
            return SUPPORTED_EXCHANGES
    
    @staticmethod
    async def _merge_exchange_data_async(
        uid: str, 
        field_suffix: str, 
        exchanges: Optional[List[str]] = None, 
        add_exchange_field: bool = False
    ) -> Dict[str, Any]:
        """
        异步合并多个交易所的数据
        """
        if exchanges is None:
            exchanges = AsyncPFCompatibilityLayer._get_user_enabled_exchanges(uid)
        if not exchanges:
            exchanges = list(SUPPORTED_EXCHANGES)
        
        from core.database import RedisKeys
        
        # 构建所有需要读取的字段列表
        fields = [RedisKeys.exchange_field(ex, field_suffix) for ex in exchanges]
        
        # 异步批量读取
        batch_data = await AsyncRedisDataManager.get_user_fields_batch(uid, fields)
        
        merged = {}
        for i, exchange in enumerate(exchanges):
            field = fields[i]
            data = batch_data.get(field)
            if data and isinstance(data, dict):
                if add_exchange_field:
                    for key, value in data.items():
                        merged_key = f"{exchange}:{key}"
                        if isinstance(value, dict):
                            value_copy = value.copy()
                            value_copy["exchange"] = exchange
                            merged[merged_key] = value_copy
                        else:
                            merged[merged_key] = value
                else:
                    merged.update(data)
        
        return merged
    
    @staticmethod
    async def _merge_exchange_lists_async(
        uid: str, 
        field_suffix: str, 
        exchanges: Optional[List[str]] = None, 
        add_exchange_prefix: bool = False
    ) -> List[str]:
        """
        异步合并多个交易所的列表数据
        """
        if exchanges is None:
            exchanges = AsyncPFCompatibilityLayer._get_user_enabled_exchanges(uid)
        if not exchanges:
            exchanges = list(SUPPORTED_EXCHANGES)
        
        from core.database import RedisKeys
        
        fields = [RedisKeys.exchange_field(ex, field_suffix) for ex in exchanges]
        batch_data = await AsyncRedisDataManager.get_user_fields_batch(uid, fields)
        
        merged: List[str] = []
        for i, exchange in enumerate(exchanges):
            field = fields[i]
            data = batch_data.get(field)
            if data and isinstance(data, list):
                if add_exchange_prefix:
                    merged.extend([f"{exchange}:{item}" for item in data])
                else:
                    merged.extend(data)
        
        return merged
    
    # ==================== 异步读取接口 ====================
    
    @staticmethod
    async def get_pf_pos_async(uid: str, exchange: str = None, add_exchange_field: bool = False) -> Dict[str, Any]:
        """异步获取用户的持仓数据"""
        try:
            from core.database import RedisKeys
            
            if exchange:
                field = RedisKeys.exchange_positions(exchange)
                data = await AsyncRedisDataManager.get_user_field(uid, field)
                if data and add_exchange_field and isinstance(data, dict):
                    for key in data:
                        if isinstance(data[key], dict):
                            data[key]["exchange"] = exchange
                return data if data else {}
            else:
                return await AsyncPFCompatibilityLayer._merge_exchange_data_async(
                    uid, "positions", add_exchange_field=add_exchange_field
                )
        except Exception as e:
            logger.error(f"[AsyncPF] get_pf_pos_async error for uid {uid}: {e}")
            return {}
    
    @staticmethod
    async def get_pf_cycle_async(uid: str, exchange: str = None, add_exchange_prefix: bool = False) -> Dict[str, Any]:
        """异步获取用户的交易周期数据"""
        try:
            from core.database import RedisKeys
            
            if exchange:
                field = RedisKeys.exchange_cycles(exchange)
                data = await AsyncRedisDataManager.get_user_field(uid, field)
                return data if data else {}
            else:
                return await AsyncPFCompatibilityLayer._merge_exchange_data_async(
                    uid, "cycles", add_exchange_field=add_exchange_prefix
                )
        except Exception as e:
            logger.error(f"[AsyncPF] get_pf_cycle_async error for uid {uid}: {e}")
            return {}
    
    @staticmethod
    async def get_pf_account_async(uid: str, exchange: str = None) -> Dict[str, Any]:
        """异步获取用户的账户数据"""
        try:
            from core.database import RedisKeys
            
            if exchange:
                field = RedisKeys.exchange_account(exchange)
                data = await AsyncRedisDataManager.get_user_field(uid, field)
                return data if data else {}
            else:
                # 合并所有交易所的账户数据
                exchanges = AsyncPFCompatibilityLayer._get_user_enabled_exchanges(uid)
                if not exchanges:
                    exchanges = list(SUPPORTED_EXCHANGES)
                
                fields = [RedisKeys.exchange_account(ex) for ex in exchanges]
                batch_data = await AsyncRedisDataManager.get_user_fields_batch(uid, fields)
                
                # 合并所有交易所的账户数据
                total_balance = 0.0
                total_equity = 0.0
                total_unrealized = 0.0
                latest_ts = 0
                
                for i, ex in enumerate(exchanges):
                    data = batch_data.get(fields[i])
                    if data:
                        if isinstance(data, str):
                            import json
                            data = json.loads(data)
                        total_balance += float(data.get("walletBalance") or 0)
                        total_equity += float(data.get("equity") or 0)
                        total_unrealized += float(data.get("unrealized") or 0)
                        ts = int(data.get("ts") or 0)
                        if ts > latest_ts:
                            latest_ts = ts
                
                if total_balance == 0 and total_equity == 0:
                    return {}
                
                return {
                    "walletBalance": str(total_balance),
                    "equity": str(total_equity),
                    "unrealized": str(total_unrealized),
                    "ts": str(latest_ts),
                    "_merged": True,
                }
        except Exception as e:
            logger.error(f"[AsyncPF] get_pf_account_async error for uid {uid}: {e}")
            return {}
    
    @staticmethod
    async def get_pf_equity_init_async(uid: str, exchange: str = None) -> Dict[str, Any]:
        """异步获取用户的初始资金数据"""
        try:
            from core.database import RedisKeys
            
            if exchange:
                field = RedisKeys.exchange_equity_init(exchange)
                data = await AsyncRedisDataManager.get_user_field(uid, field)
                return data if data else {}
            else:
                # 合并所有交易所的初始资金
                exchanges = AsyncPFCompatibilityLayer._get_user_enabled_exchanges(uid)
                if not exchanges:
                    exchanges = list(SUPPORTED_EXCHANGES)
                
                fields = [RedisKeys.exchange_equity_init(ex) for ex in exchanges]
                batch_data = await AsyncRedisDataManager.get_user_fields_batch(uid, fields)
                
                total_init = 0.0
                earliest_ts = float('inf')
                
                for i, ex in enumerate(exchanges):
                    data = batch_data.get(fields[i])
                    if data:
                        if isinstance(data, str):
                            import json
                            data = json.loads(data)
                        total_init += float(data.get("walletBalance") or data.get("initialEquity") or 0)
                        ts = int(data.get("ts") or float('inf'))
                        if ts < earliest_ts:
                            earliest_ts = ts
                
                if total_init == 0:
                    return {}
                
                return {
                    "walletBalance": str(total_init),
                    "initialEquity": str(total_init),
                    "ts": str(int(earliest_ts)) if earliest_ts != float('inf') else None,
                    "_merged": True,
                }
        except Exception as e:
            logger.error(f"[AsyncPF] get_pf_equity_init_async error for uid {uid}: {e}")
            return {}
    
    @staticmethod
    async def get_pf_pos_active_async(uid: str, exchange: str = None) -> List[str]:
        """异步获取用户的活跃持仓列表"""
        try:
            from core.database import RedisKeys
            
            if exchange:
                field = RedisKeys.exchange_positions_active(exchange)
                data = await AsyncRedisDataManager.get_user_field(uid, field)
                return data if isinstance(data, list) else []
            else:
                return await AsyncPFCompatibilityLayer._merge_exchange_lists_async(
                    uid, "positions_active", add_exchange_prefix=True
                )
        except Exception as e:
            logger.error(f"[AsyncPF] get_pf_pos_active_async error for uid {uid}: {e}")
            return []
    
    @staticmethod
    async def get_pf_closed_h_async(uid: str, exchange: str = None, add_exchange_field: bool = False) -> Dict[str, Any]:
        """
        异步获取用户的已关闭交易历史
        
        从 MySQL 读取（主存储），使用 asyncio.to_thread 避免阻塞
        """
        try:
            import asyncio
            from core.closed_trades_db import get_closed_trades_db
            
            db = get_closed_trades_db()
            # 使用 to_thread 将同步调用转为异步
            result = await asyncio.to_thread(
                db.get_trades_as_dict, 
                uid, 
                exchange=exchange, 
                add_exchange_field=add_exchange_field
            )
            return result
        except Exception as e:
            logger.error(f"[AsyncPF] get_pf_closed_h_async error for uid {uid}: {e}")
            return {}
    
    @staticmethod
    async def get_pf_open_orders_async(uid: str, exchange: str = None) -> Dict[str, Any]:
        """异步获取用户的挂单数据"""
        try:
            from core.database import RedisKeys
            
            if exchange:
                field = RedisKeys.exchange_open_orders(exchange)
                data = await AsyncRedisDataManager.get_user_field(uid, field)
                return data if data else {}
            else:
                return await AsyncPFCompatibilityLayer._merge_exchange_data_async(
                    uid, "open_orders", add_exchange_field=True
                )
        except Exception as e:
            logger.error(f"[AsyncPF] get_pf_open_orders_async error for uid {uid}: {e}")
            return {}


# =============================================================================
# 异步批量价格获取
# =============================================================================

async def get_mark_price_async(symbol: str) -> Optional[float]:
    """异步获取单个币种的标记价格"""
    try:
        redis = await get_async_redis()
        from core.database import RedisKeys
        
        raw = await redis.get(RedisKeys.market_prices(symbol))
        if raw:
            data = json.loads(raw)
            return float(data.get("price", 0))
    except Exception as e:
        logger.debug(f"[AsyncRedis] get_mark_price_async error for {symbol}: {e}")
    return None


async def get_mark_prices_batch_async(symbols: List[str]) -> Dict[str, float]:
    """
    异步批量获取多个币种的标记价格
    
    使用 MGET 一次性获取所有价格，减少网络往返
    """
    if not symbols:
        return {}
    
    try:
        redis = await get_async_redis()
        from core.database import RedisKeys
        
        # 构建所有 key
        keys = [RedisKeys.market_prices(s) for s in symbols]
        
        # 批量获取
        values = await redis.mget(keys)
        
        result = {}
        for i, symbol in enumerate(symbols):
            raw = values[i]
            if raw:
                try:
                    data = json.loads(raw)
                    result[symbol] = float(data.get("price", 0))
                except (json.JSONDecodeError, ValueError) as e:
                    logger.debug(f"[AsyncRedis] 解析 mark_price 失败: symbol={symbol}, error={e}")
        
        return result
    except Exception as e:
        logger.error(f"[AsyncRedis] get_mark_prices_batch_async error: {e}")
        return {}


# =============================================================================
# 异步 AI 历史记录 - 从 MySQL 读取
# =============================================================================

async def get_ai_history_paginated_async(uid: str, offset: int = 0, limit: int = 50) -> tuple:
    """
    异步获取 AI 历史记录（分页）- 从 MySQL 读取
    
    Returns:
        (items, total) - 历史记录列表和总数
    """
    import time
    
    t0 = time.perf_counter()
    
    try:
        from core.ai_decision_db import get_ai_decision_db
        db = get_ai_decision_db()
        
        # MySQL 查询放到线程池，避免阻塞事件循环
        records, total = await asyncio.to_thread(
            db.get_decisions_paginated, uid, offset, limit
        )
        
        t1 = time.perf_counter()
        total_ms = (t1 - t0) * 1000
        
        if total_ms > 100:
            logger.warning(f"[AI-HISTORY-MySQL] query={total_ms:.0f}ms, items={len(records)}, total={total}")
        
        return records, total
        
    except Exception as e:
        logger.error(f"[AI-HISTORY] MySQL read failed for {uid}: {e}")
        return [], 0


async def get_ai_history_summary_paginated_async(uid: str, offset: int = 0, limit: int = 50) -> tuple:
    """
    异步获取 AI 历史摘要（分页）- 从 MySQL 读取
    
    相比 get_ai_history_paginated_async，此方法：
    - 不返回完整的 request/response JSON
    - 只返回摘要字段，数据量减少 90%+
    - 适合列表展示
    
    Returns:
        (items, total) - 摘要记录列表和总数
    """
    import time
    
    t0 = time.perf_counter()
    
    try:
        from core.ai_decision_db import get_ai_decision_db
        db = get_ai_decision_db()
        
        # MySQL 查询放到线程池，避免阻塞事件循环
        records, total = await asyncio.to_thread(
            db.get_decisions_summary_paginated, uid, offset, limit
        )
        
        t1 = time.perf_counter()
        total_ms = (t1 - t0) * 1000
        
        if total_ms > 50:
            logger.info(f"[AI-HISTORY-SUMMARY] query={total_ms:.0f}ms, items={len(records)}, total={total}")
        
        return records, total
        
    except Exception as e:
        logger.error(f"[AI-HISTORY-SUMMARY] MySQL read failed for {uid}: {e}")
        return [], 0


async def get_ai_decision_detail_async(uid: str, decision_id: int) -> dict:
    """
    异步获取单条 AI 决策详情 - 从 MySQL 读取
    
    Args:
        uid: 用户 ID
        decision_id: 决策记录 ID
        
    Returns:
        完整的决策记录，包含 request 和 response
    """
    import time
    
    t0 = time.perf_counter()
    
    try:
        from core.ai_decision_db import get_ai_decision_db
        db = get_ai_decision_db()
        
        # MySQL 查询放到线程池
        record = await asyncio.to_thread(
            db.get_decision_by_id, uid, decision_id
        )
        
        t1 = time.perf_counter()
        total_ms = (t1 - t0) * 1000
        
        if total_ms > 50:
            logger.info(f"[AI-HISTORY-DETAIL] query={total_ms:.0f}ms, id={decision_id}")
        
        return record or {}
        
    except Exception as e:
        logger.error(f"[AI-HISTORY-DETAIL] MySQL read failed for {uid}/{decision_id}: {e}")
        return {}


# =============================================================================
# 异步多交易所数据获取
# =============================================================================

async def get_all_exchanges_data_async(uid: str) -> Dict[str, Dict[str, Any]]:
    """
    异步获取所有交易所的数据
    
    Returns:
        {
            "binance": {"account": {...}, "positions": {...}, "closed_trades": {...}, ...},
            "okx": {...},
            ...
        }
    """
    try:
        from core.database import RedisKeys
        
        exchanges = AsyncPFCompatibilityLayer._get_user_enabled_exchanges(uid)
        if not exchanges:
            exchanges = list(SUPPORTED_EXCHANGES)
        
        # 构建所有需要读取的字段
        fields = []
        field_map = {}  # field -> (exchange, data_type)
        
        for ex in exchanges:
            for data_type in ["account", "positions", "cycles", "closed_trades", "equity_init", "positions_active"]:
                field = RedisKeys.exchange_field(ex, data_type)
                fields.append(field)
                field_map[field] = (ex, data_type)
        
        # 批量读取
        batch_data = await AsyncRedisDataManager.get_user_fields_batch(uid, fields)
        
        # 组织结果
        result = {ex: {} for ex in exchanges}
        for field, value in batch_data.items():
            if value and field in field_map:
                ex, data_type = field_map[field]
                result[ex][data_type] = value
        
        # 过滤空的交易所
        return {ex: data for ex, data in result.items() if data}
    except Exception as e:
        logger.error(f"[AsyncRedis] get_all_exchanges_data_async error for uid {uid}: {e}")
        return {}


async def get_positions_by_exchange_async(uid: str) -> Dict[str, Dict[str, Any]]:
    """
    异步获取按交易所分组的持仓数据
    
    Returns:
        {
            "binance": {"BTCUSDT": {...}, "ETHUSDT": {...}},
            "okx": {...},
            ...
        }
    """
    try:
        from core.database import RedisKeys
        
        exchanges = AsyncPFCompatibilityLayer._get_user_enabled_exchanges(uid)
        if not exchanges:
            exchanges = list(SUPPORTED_EXCHANGES)
        
        # 构建字段列表
        fields = [RedisKeys.exchange_positions(ex) for ex in exchanges]
        
        # 批量读取
        batch_data = await AsyncRedisDataManager.get_user_fields_batch(uid, fields)
        
        result = {}
        for i, ex in enumerate(exchanges):
            data = batch_data.get(fields[i])
            if data and isinstance(data, dict):
                result[ex] = data
        
        return result
    except Exception as e:
        logger.error(f"[AsyncRedis] get_positions_by_exchange_async error for uid {uid}: {e}")
        return {}


async def get_closed_trades_by_exchange_async(uid: str) -> Dict[str, Dict[str, Any]]:
    """
    异步获取按交易所分组的已关闭交易（从 MySQL 读取）
    
    Returns:
        {"binance": {cycleId: {...}, ...}, "okx": {...}, ...}
    """
    try:
        from core.closed_trades_db import get_closed_trades_db
        db = get_closed_trades_db()
        # 使用 asyncio.to_thread 在线程池中执行同步 MySQL 查询
        result = await asyncio.to_thread(db.get_trades_by_exchange, uid)
        return result
    except Exception as e:
        logger.error(f"[AsyncRedis] get_closed_trades_by_exchange_async error for uid {uid}: {e}")
        return {}


# =============================================================================
# 全局实例
# =============================================================================

async_pf_compat = AsyncPFCompatibilityLayer()
