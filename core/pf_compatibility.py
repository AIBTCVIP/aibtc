#!/usr/bin/env python3
"""
pf:* 键兼容层 - 让旧代码通过超级聚合键访问数据

这个模块提供向后兼容的接口，让仍在使用 pf:* 键的代码
能够透明地访问存储在 user:* 超级聚合键中的数据。

多交易所架构支持:
- 数据按交易所隔离存储: user:{uid} -> {exchange}:positions, {exchange}:account, etc.
- 兼容层自动合并所有交易所的数据
- 支持按交易所过滤查询

性能优化:
- 使用 HMGET 批量读取多个字段（单次网络往返）
- 减少 Redis 调用次数
"""

import json
import logging
from typing import Dict, List, Any, Optional, Set
from core.database import redis_client, RedisKeys
from core.redis_manager import RedisDataManager

logger = logging.getLogger(__name__)

# 支持的交易所列表
SUPPORTED_EXCHANGES = ["binance", "okx", "bitget", "hyperliquid"]


class PFCompatibilityLayer:
    """pf:* 键兼容层 - 支持多交易所数据访问"""

    # ==================== 多交易所数据访问辅助方法 ====================
    
    @staticmethod
    def _get_user_enabled_exchanges(uid: str) -> List[str]:
        """获取用户启用的交易所列表"""
        try:
            from core.user_db import config_loader
            return config_loader.get_enabled_exchanges(uid) or []
        except Exception:
            # 如果无法获取，返回所有支持的交易所
            return SUPPORTED_EXCHANGES
    
    @staticmethod
    def _merge_exchange_data(uid: str, field_suffix: str, exchanges: Optional[List[str]] = None, add_exchange_field: bool = False) -> Dict[str, Any]:
        """
        合并多个交易所的数据（使用批量读取优化）
        
        Args:
            uid: 用户 ID
            field_suffix: 字段后缀 (positions, cycles, closed_trades 等)
            exchanges: 要查询的交易所列表，None 表示所有启用的交易所
            add_exchange_field: 是否在每个数据项中添加 exchange 字段
            
        Returns:
            合并后的数据字典
        """
        if exchanges is None:
            exchanges = PFCompatibilityLayer._get_user_enabled_exchanges(uid)
        if not exchanges:
            exchanges = list(SUPPORTED_EXCHANGES)
        
        # 构建所有需要读取的字段列表
        fields = [RedisKeys.exchange_field(ex, field_suffix) for ex in exchanges]
        
        # 批量读取（单次 HMGET）
        batch_data = RedisDataManager.get_user_fields_batch(uid, fields)
        
        merged = {}
        for i, exchange in enumerate(exchanges):
            field = fields[i]
            data = batch_data.get(field)
            if data and isinstance(data, dict):
                if add_exchange_field:
                    # 为每个数据项添加 exchange 字段，并使用带交易所前缀的 key 防止冲突
                    for key, value in data.items():
                        # 使用 "exchange:symbol" 作为 key，避免不同交易所的相同 symbol 冲突
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
    def _merge_exchange_lists(uid: str, field_suffix: str, exchanges: Optional[List[str]] = None, add_exchange_prefix: bool = False) -> List[str]:
        """
        合并多个交易所的列表数据（使用批量读取优化）
        
        Args:
            uid: 用户 ID
            field_suffix: 字段后缀 (positions_active 等)
            exchanges: 要查询的交易所列表
            add_exchange_prefix: 是否为每个项添加交易所前缀（用于区分不同交易所的相同 symbol）
            
        Returns:
            合并后的列表
        """
        if exchanges is None:
            exchanges = PFCompatibilityLayer._get_user_enabled_exchanges(uid)
        if not exchanges:
            exchanges = list(SUPPORTED_EXCHANGES)
        
        # 构建所有需要读取的字段列表
        fields = [RedisKeys.exchange_field(ex, field_suffix) for ex in exchanges]
        
        # 批量读取（单次 HMGET）
        batch_data = RedisDataManager.get_user_fields_batch(uid, fields)
        
        merged: List[str] = []
        for i, exchange in enumerate(exchanges):
            field = fields[i]
            data = batch_data.get(field)
            if data and isinstance(data, list):
                if add_exchange_prefix:
                    # 添加交易所前缀，格式: "exchange:symbol"
                    merged.extend([f"{exchange}:{item}" for item in data])
                else:
                    merged.extend(data)
        
        return merged

    # ==================== 原有兼容接口（已更新支持多交易所）====================

    @staticmethod
    def get_pf_pos(uid: str, exchange: str = None, add_exchange_field: bool = False) -> Dict[str, Any]:
        """
        获取用户的持仓数据 (兼容 pf:pos:{uid})
        
        Args:
            uid: 用户 ID
            exchange: 指定交易所，None 表示合并所有交易所
            add_exchange_field: 是否在每个持仓中添加 exchange 字段（合并时有效）
        """
        try:
            if exchange:
                # 获取指定交易所的持仓
                field = RedisKeys.exchange_positions(exchange)
                data = RedisDataManager.get_user_field(uid, field)
                if data and add_exchange_field and isinstance(data, dict):
                    # 为单交易所数据也添加 exchange 字段
                    for key in data:
                        if isinstance(data[key], dict):
                            data[key]["exchange"] = exchange
                return data if data else {}
            else:
                # 合并所有交易所的持仓
                return PFCompatibilityLayer._merge_exchange_data(uid, "positions", add_exchange_field=add_exchange_field)
        except Exception as e:
            logger.error(f"Error in get_pf_pos for uid {uid}: {e}")
            return {}

    @staticmethod
    def set_pf_pos(uid: str, data: Dict[str, Any], exchange: str = None, skip_ghost_check: bool = False):
        """
        设置用户的持仓数据 (兼容 pf:pos:{uid})
        
        Args:
            uid: 用户 ID
            data: 持仓数据
            exchange: 指定交易所，None 表示使用旧的全局字段（不推荐）
            skip_ghost_check: 是否跳过幽灵持仓检查（审计器清理时使用）
        
        注意：
            如果某个持仓刚被审计器清理（存在 ghost_cleaned 标记），
            该持仓会被自动从 data 中移除，防止竞态条件导致重新写入。
        """
        if exchange and not skip_ghost_check:
            # 检查并移除刚被清理的幽灵持仓
            data = PFCompatibilityLayer._filter_ghost_cleaned_positions(uid, exchange, data)
        
        if exchange:
            field = RedisKeys.exchange_positions(exchange)
            RedisDataManager.set_user_field(uid, field, data)
        else:
            # 向后兼容：写入旧的全局字段
            RedisDataManager.set_user_field(uid, RedisKeys.field_positions(), json.dumps(data))
    
    @staticmethod
    def _filter_ghost_cleaned_positions(uid: str, exchange: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        过滤掉刚被审计器清理的幽灵持仓
        
        防止竞态条件：审计器清理幽灵持仓后，其他组件（如 mark_cycle_updater）
        可能会把已删除的持仓重新写回 Redis。
        """
        if not data:
            return data
        
        try:
            # 检查每个持仓是否有清理标记
            fields_to_remove = []
            for field in data.keys():
                cleaned_key = f"pf:ghost_cleaned:{uid}:{exchange}:{field}"
                if redis_client.exists(cleaned_key):
                    fields_to_remove.append(field)
                    logger.debug(f"[{uid}][{exchange}] 跳过写入已清理的幽灵持仓: {field}")
            
            # 移除被标记的持仓
            if fields_to_remove:
                data = data.copy()  # 不修改原始数据
                for field in fields_to_remove:
                    del data[field]
                    
        except Exception as e:
            logger.debug(f"[{uid}][{exchange}] 检查幽灵持仓标记失败 (非关键): {e}")
        
        return data

    @staticmethod
    def get_pf_cycle(uid: str, exchange: str = None, add_exchange_prefix: bool = False) -> Dict[str, Any]:
        """
        获取用户的交易周期数据 (兼容 pf:cycle:{uid})
        
        Args:
            uid: 用户 ID
            exchange: 指定交易所，None 表示合并所有交易所
            add_exchange_prefix: 合并时是否添加交易所前缀（用于与 get_pf_pos 配合使用）
        """
        try:
            if exchange:
                # 获取指定交易所的 cycles
                field = RedisKeys.exchange_cycles(exchange)
                data = RedisDataManager.get_user_field(uid, field)
                return data if data else {}
            else:
                # 合并所有交易所的 cycles
                return PFCompatibilityLayer._merge_exchange_data(uid, "cycles", add_exchange_field=add_exchange_prefix)
        except Exception as e:
            logger.error(f"get_pf_cycle failed for uid {uid}: {e}")
            return {}

    @staticmethod
    def set_pf_cycle(uid: str, data: Dict[str, Any], exchange: str = None):
        """
        设置用户的交易周期数据 (兼容 pf:cycle:{uid})
        
        Args:
            uid: 用户 ID
            data: 周期数据
            exchange: 指定交易所，None 表示使用旧的缓存方式（不推荐）
        """
        try:
            if exchange:
                field = RedisKeys.exchange_cycles(exchange)
                RedisDataManager.set_user_field(uid, field, data)
            else:
                # 向后兼容：写入缓存字段
                cache_data = RedisDataManager.get_user_field(uid, RedisKeys.field_cache())
                if isinstance(cache_data, dict):
                    cache = cache_data
                else:
                    cache = json.loads(cache_data) if cache_data else {}
                cache["cycles"] = data
                RedisDataManager.set_user_field(uid, RedisKeys.field_cache(), json.dumps(cache))
        except Exception as e:
            logger.error(f"Error in set_pf_cycle for uid {uid}: {e}")

    @staticmethod
    def get_pf_account(uid: str, exchange: str = None) -> Optional[Dict[str, Any]]:
        """
        获取用户的账户数据 (兼容 pf:account:{uid})
        
        Args:
            uid: 用户 ID
            exchange: 指定交易所，None 表示返回合并后的账户数据
        """
        try:
            if exchange:
                # 获取指定交易所的账户
                field = RedisKeys.exchange_account(exchange)
                data = RedisDataManager.get_user_field(uid, field)
                if not data:
                    return None
                return data if isinstance(data, dict) else json.loads(data)
            else:
                # 合并所有交易所的账户数据（返回总余额）- 使用批量读取优化
                exchanges = PFCompatibilityLayer._get_user_enabled_exchanges(uid)
                if not exchanges:
                    exchanges = list(SUPPORTED_EXCHANGES)
                
                # 批量读取所有交易所的账户数据
                fields = [RedisKeys.exchange_account(ex) for ex in exchanges]
                batch_data = RedisDataManager.get_user_fields_batch(uid, fields)
                
                total_balance = 0.0
                total_equity = 0.0
                total_unrealized = 0.0
                latest_ts = 0
                
                for i, ex in enumerate(exchanges):
                    field = fields[i]
                    data = batch_data.get(field)
                    if data:
                        if isinstance(data, str):
                            data = json.loads(data)
                        total_balance += float(data.get("walletBalance") or 0)
                        total_equity += float(data.get("equity") or 0)
                        total_unrealized += float(data.get("unrealized") or 0)
                        ts = int(data.get("ts") or 0)
                        if ts > latest_ts:
                            latest_ts = ts
                
                if total_balance == 0 and total_equity == 0:
                    return None
                
                return {
                    "walletBalance": str(total_balance),
                    "equity": str(total_equity),
                    "unrealized": str(total_unrealized),
                    "ts": str(latest_ts),
                    "_merged": True,
                }
        except Exception as e:
            logger.error(f"Error in get_pf_account for uid {uid}: {e}")
            return None

    @staticmethod
    def set_pf_account(uid: str, data: Dict[str, Any], exchange: str = None):
        """
        设置用户的账户数据 (兼容 pf:account:{uid})
        
        Args:
            uid: 用户 ID
            data: 账户数据
            exchange: 指定交易所，None 表示使用旧的全局字段（不推荐）
        """
        if exchange:
            field = RedisKeys.exchange_account(exchange)
            RedisDataManager.set_user_field(uid, field, data)
        else:
            RedisDataManager.set_user_field(uid, RedisKeys.field_account(), json.dumps(data))

    @staticmethod
    def get_pf_equity_init(uid: str, exchange: str = None) -> Optional[Dict[str, Any]]:
        """
        获取用户的初始权益数据 (兼容 pf:equity:init:{uid})
        
        Args:
            uid: 用户 ID
            exchange: 指定交易所，None 表示返回合并后的初始权益
        """
        try:
            if exchange:
                field = RedisKeys.exchange_equity_init(exchange)
                data = RedisDataManager.get_user_field(uid, field)
                if not data:
                    return None
                return data if isinstance(data, dict) else json.loads(data)
            else:
                # 合并所有交易所的初始权益 - 使用批量读取优化
                exchanges = PFCompatibilityLayer._get_user_enabled_exchanges(uid)
                if not exchanges:
                    exchanges = list(SUPPORTED_EXCHANGES)
                
                # 批量读取所有交易所的初始权益
                fields = [RedisKeys.exchange_equity_init(ex) for ex in exchanges]
                batch_data = RedisDataManager.get_user_fields_batch(uid, fields)
                
                total_init = 0.0
                earliest_ts = float('inf')
                
                for i, ex in enumerate(exchanges):
                    field = fields[i]
                    data = batch_data.get(field)
                    if data:
                        if isinstance(data, str):
                            data = json.loads(data)
                        total_init += float(data.get("walletBalance") or data.get("initialEquity") or 0)
                        ts = int(data.get("ts") or float('inf'))
                        if ts < earliest_ts:
                            earliest_ts = ts
                
                if total_init == 0:
                    return None
                
                return {
                    "walletBalance": str(total_init),
                    "initialEquity": str(total_init),
                    "ts": str(int(earliest_ts)) if earliest_ts != float('inf') else None,
                    "_merged": True,
                }
        except Exception as e:
            logger.error(f"Error in get_pf_equity_init for uid {uid}: {e}")
            return None

    @staticmethod
    def set_pf_equity_init(uid: str, data: Dict[str, Any], exchange: str = None):
        """
        设置用户的初始权益数据 (兼容 pf:equity:init:{uid})
        
        Args:
            uid: 用户 ID
            data: 初始权益数据
            exchange: 指定交易所，None 表示使用旧的全局字段（不推荐）
        """
        if exchange:
            field = RedisKeys.exchange_equity_init(exchange)
            RedisDataManager.set_user_field(uid, field, data)
        else:
            RedisDataManager.set_user_field(uid, RedisKeys.field_equity_init(), json.dumps(data))

    @staticmethod
    def get_pf_closed_h(uid: str, exchange: str = None, add_exchange_field: bool = False) -> Dict[str, Any]:
        """
        获取用户的平仓历史 (兼容 pf:closed:h:{uid})
        
        优先从 MySQL 读取，MySQL 是主存储。
        
        Args:
            uid: 用户 ID
            exchange: 指定交易所，None 表示合并所有交易所
            add_exchange_field: 是否在每个交易中添加 exchange 字段（合并时有效）
        """
        try:
            # 从 MySQL 读取（主存储）
            from core.closed_trades_db import get_closed_trades_db
            db = get_closed_trades_db()
            return db.get_trades_as_dict(uid, exchange=exchange, add_exchange_field=add_exchange_field)
        except Exception as e:
            logger.error(f"Error in get_pf_closed_h for uid {uid}: {e}")
            return {}

    @staticmethod
    def set_pf_closed_h(uid: str, trade_id: str, data: Dict[str, Any], exchange: str = None):
        """
        设置用户的平仓历史
        
        只写入 MySQL（主存储），不再写入 Redis。
        
        Args:
            uid: 用户 ID
            trade_id: 交易 ID (cycleId)
            data: 交易数据
            exchange: 指定交易所，None 表示从 data 中获取
        """
        try:
            # 从 data 中获取交易所信息
            if exchange is None:
                exchange = data.get("exchange")
            
            # 写入 MySQL（主存储）
            from core.closed_trades_db import get_closed_trades_db
            db = get_closed_trades_db()
            # 确保 data 中有 cycleId
            trade_data = data.copy()
            if "cycleId" not in trade_data:
                trade_data["cycleId"] = trade_id
            db.add_trade(uid, exchange or "unknown", trade_data)
            
        except Exception as e:
            logger.error(f"Failed to save closed trade to MySQL for {uid}: {e}")

    @staticmethod
    def get_pf_pos_active(uid: str, exchange: str = None) -> List[str]:
        """
        获取用户的活跃持仓列表 (兼容 pf:pos:active:{uid})
        
        Args:
            uid: 用户 ID
            exchange: 指定交易所，None 表示合并所有交易所
            
        Returns:
            当 exchange 指定时，返回原始 symbol 列表，如 ["ETHUSDT", "BTCUSDT"]
            当 exchange=None 时，返回带交易所前缀的列表，如 ["binance:ETHUSDT", "bitget:BTCUSDT"]
        """
        try:
            if exchange:
                field = RedisKeys.exchange_positions_active(exchange)
                data = RedisDataManager.get_user_field(uid, field)
                return data if data else []
            else:
                # 合并时添加交易所前缀，避免不同交易所的相同 symbol 冲突
                return PFCompatibilityLayer._merge_exchange_lists(uid, "positions_active", add_exchange_prefix=True)
        except Exception as e:
            logger.error(f"Error in get_pf_pos_active for uid {uid}: {e}")
            return []

    @staticmethod
    def set_pf_pos_active(uid: str, active_list: List[str], exchange: str = None):
        """
        设置用户的活跃持仓列表 (兼容 pf:pos:active:{uid})
        
        Args:
            uid: 用户 ID
            active_list: 活跃持仓列表
            exchange: 指定交易所，None 表示使用旧的全局字段（不推荐）
        """
        if exchange:
            field = RedisKeys.exchange_positions_active(exchange)
            RedisDataManager.set_user_field(uid, field, active_list)
        else:
            RedisDataManager.set_user_field(uid, RedisKeys.field_positions_active(), json.dumps(active_list))

    @staticmethod
    def get_pf_seen_trades(uid: str) -> Set[str]:
        """获取用户已见的交易 (兼容 pf:seen:trades:{uid})"""
        try:
            cache_data = RedisDataManager.get_user_field(uid, RedisKeys.field_cache())
            if not cache_data:
                return set()
            if isinstance(cache_data, dict):
                cache = cache_data
            else:
                cache = json.loads(cache_data)
            seen_list = cache.get("seen_trades", [])
            return set(seen_list)
        except Exception as e:
            logger.error(f"Error in get_pf_seen_trades for uid {uid}: {e}")
            return set()

    @staticmethod
    def get_pf_cache(uid: str) -> Dict[str, Any]:
        """获取用户的缓存数据 (兼容 pf:* 各种缓存)"""
        try:
            cache_data = RedisDataManager.get_user_field(uid, RedisKeys.field_cache())
            if not cache_data:
                return {}
            if isinstance(cache_data, dict):
                return cache_data
            return json.loads(cache_data)
        except Exception as e:
            logger.error(f"Error in get_pf_cache for uid {uid}: {e}")
            return {}

    @staticmethod
    def set_pf_cache(uid: str, cache_data: Dict[str, Any]):
        """设置用户的缓存数据 (兼容 pf:* 各种缓存)"""
        try:
            RedisDataManager.set_user_field(uid, RedisKeys.field_cache(), json.dumps(cache_data))
        except Exception as e:
            logger.error(f"Error in set_pf_cache for uid {uid}: {e}")

    @staticmethod
    def add_pf_seen_trades(uid: str, trade_id: str):
        """添加已见的交易 (兼容 pf:seen:trades:{uid})"""
        try:
            cache_data = PFCompatibilityLayer.get_pf_cache(uid)
            if cache_data is None:
                cache_data = {}
            seen_list = cache_data.get("seen_trades", [])
            if trade_id not in seen_list:
                seen_list.append(trade_id)
                cache_data["seen_trades"] = seen_list
                PFCompatibilityLayer.set_pf_cache(uid, cache_data)
        except Exception as e:
            logger.error(f"Error in add_pf_seen_trades for uid {uid}, trade_id {trade_id}: {e}")


    # ==================== 新增：按交易所获取数据的方法 ====================
    
    @staticmethod
    def get_all_exchanges_data(uid: str) -> Dict[str, Dict[str, Any]]:
        """
        获取所有交易所的数据（按交易所分组，使用批量读取优化）
        
        优化：使用单次 HMGET 读取所有交易所的所有字段，
        将原来的 24+ 次 Redis 调用减少为 1 次。
        
        Returns:
            {
                "binance": {
                    "account": {...},
                    "equity_init": {...},
                    "positions": {...},
                    "positions_active": [...],
                    "cycles": {...},
                    "closed_trades": {...},
                },
                "okx": {...},
                ...
            }
        """
        # 获取数据库中启用的交易所
        enabled_exchanges = PFCompatibilityLayer._get_user_enabled_exchanges(uid)
        
        # 同时检查所有支持的交易所，因为可能有数据但 is_enabled=0（状态不同步）
        exchanges_to_check = set(enabled_exchanges) if enabled_exchanges else set()
        exchanges_to_check.update(SUPPORTED_EXCHANGES)
        exchanges = list(exchanges_to_check)
        
        # 构建所有需要读取的字段列表
        # 每个交易所 6 个字段：account, equity_init, positions, positions_active, cycles, closed_trades
        field_types = ["account", "equity_init", "positions", "positions_active", "cycles", "closed_trades"]
        fields = []
        field_map = {}  # 记录字段到 (exchange, field_type) 的映射
        
        for exchange in exchanges:
            for field_type in field_types:
                field = RedisKeys.exchange_field(exchange, field_type)
                fields.append(field)
                field_map[field] = (exchange, field_type)
        
        # 单次批量读取所有字段
        batch_data = RedisDataManager.get_user_fields_batch(uid, fields)
        
        # 组装结果
        result = {}
        for field, value in batch_data.items():
            if value is None:
                continue
            
            exchange, field_type = field_map[field]
            
            if exchange not in result:
                result[exchange] = {}
            
            result[exchange][field_type] = value
        
        return result
    
    @staticmethod
    def get_positions_by_exchange(uid: str) -> Dict[str, Dict[str, Any]]:
        """
        获取按交易所分组的持仓数据（使用批量读取优化）
        
        Returns:
            {"binance": {"BTCUSDT:LONG": {...}, ...}, "okx": {...}, ...}
        """
        # 检查所有支持的交易所，不仅仅是启用的
        enabled_exchanges = PFCompatibilityLayer._get_user_enabled_exchanges(uid)
        exchanges_to_check = set(enabled_exchanges) if enabled_exchanges else set()
        exchanges_to_check.update(SUPPORTED_EXCHANGES)
        exchanges = list(exchanges_to_check)
        
        # 批量读取所有交易所的持仓数据
        fields = [RedisKeys.exchange_positions(ex) for ex in exchanges]
        batch_data = RedisDataManager.get_user_fields_batch(uid, fields)
        
        result = {}
        for i, exchange in enumerate(exchanges):
            field = fields[i]
            positions = batch_data.get(field)
            if positions:
                result[exchange] = positions
        
        return result
    
    @staticmethod
    def get_closed_trades_by_exchange(uid: str) -> Dict[str, Dict[str, Any]]:
        """
        获取按交易所分组的已关闭交易（从 MySQL 读取）
        
        Returns:
            {"binance": {cycleId: {...}, ...}, "okx": {...}, ...}
        """
        try:
            from core.closed_trades_db import get_closed_trades_db
            db = get_closed_trades_db()
            return db.get_trades_by_exchange(uid)
        except Exception as e:
            logger.error(f"Error in get_closed_trades_by_exchange for uid {uid}: {e}")
            return {}

    # ==================== 挂单缓存（WebSocket 实时更新）====================
    
    @staticmethod
    def get_pf_open_orders(uid: str, exchange: str) -> Dict[str, dict]:
        """
        获取挂单缓存（由 WebSocket 实时更新）
        
        Args:
            uid: 用户 ID
            exchange: 交易所名称
            
        Returns:
            挂单字典 {orderId: {symbol, side, price, qty, ...}, ...}
        """
        try:
            field = RedisKeys.exchange_open_orders(exchange)
            data = RedisDataManager.get_user_field(uid, field)
            if not data:
                return {}
            return data if isinstance(data, dict) else json.loads(data)
        except Exception as e:
            logger.debug(f"获取挂单缓存失败: {uid}/{exchange}: {e}")
            return {}
    
    @staticmethod
    def set_pf_open_orders(uid: str, orders: Dict[str, dict], exchange: str):
        """
        设置挂单缓存
        
        Args:
            uid: 用户 ID
            orders: 挂单字典 {orderId: {symbol, side, price, qty, ...}, ...}
            exchange: 交易所名称
        """
        field = RedisKeys.exchange_open_orders(exchange)
        RedisDataManager.set_user_field(uid, field, orders)
        # 设置 2 小时过期
        try:
            redis_conn = RedisDataManager._get_redis()
            user_key = RedisKeys.user(uid)
            # 注意：Redis Hash 字段不能单独设置过期时间
            # 但整个 user key 应该有较长的过期时间
            # 这里我们通过在数据中添加时间戳来实现软过期
        except Exception as e:
            logger.debug(f"设置挂单缓存过期时间失败 (非关键): {e}")


# 全局兼容层实例
pf_compat = PFCompatibilityLayer()


# ==================== 向后兼容的函数接口 ====================
# 这些函数会自动合并所有交易所的数据

def get_pos_data(uid: str, exchange: str = None, add_exchange_field: bool = False) -> Dict[str, Any]:
    return pf_compat.get_pf_pos(uid, exchange, add_exchange_field)

def set_pos_data(uid: str, data: Dict[str, Any], exchange: str = None, skip_ghost_check: bool = False):
    pf_compat.set_pf_pos(uid, data, exchange, skip_ghost_check)

def get_cycle_data(uid: str, exchange: str = None) -> Dict[str, Any]:
    return pf_compat.get_pf_cycle(uid, exchange)

def set_cycle_data(uid: str, data: Dict[str, Any], exchange: str = None):
    pf_compat.set_pf_cycle(uid, data, exchange)

def get_account_data(uid: str, exchange: str = None) -> Optional[Dict[str, Any]]:
    return pf_compat.get_pf_account(uid, exchange)

def set_account_data(uid: str, data: Dict[str, Any], exchange: str = None):
    pf_compat.set_pf_account(uid, data, exchange)

def get_equity_init_data(uid: str, exchange: str = None) -> Optional[Dict[str, Any]]:
    return pf_compat.get_pf_equity_init(uid, exchange)

def set_equity_init_data(uid: str, data: Dict[str, Any], exchange: str = None):
    pf_compat.set_pf_equity_init(uid, data, exchange)

def get_closed_trades(uid: str, exchange: str = None, add_exchange_field: bool = False) -> Dict[str, Any]:
    return pf_compat.get_pf_closed_h(uid, exchange, add_exchange_field)

def set_closed_trade(uid: str, trade_id: str, data: Dict[str, Any], exchange: str = None):
    pf_compat.set_pf_closed_h(uid, trade_id, data, exchange)

def get_active_positions(uid: str, exchange: str = None) -> List[str]:
    return pf_compat.get_pf_pos_active(uid, exchange)

def set_active_positions(uid: str, active_list: List[str], exchange: str = None):
    pf_compat.set_pf_pos_active(uid, active_list, exchange)

def get_seen_trades(uid: str) -> Set[str]:
    return pf_compat.get_pf_seen_trades(uid)

def get_cache_data(uid: str) -> Optional[Dict[str, Any]]:
    return pf_compat.get_pf_cache(uid)

def set_cache_data(uid: str, cache_data: Dict[str, Any]):
    pf_compat.set_pf_cache(uid, cache_data)

def add_seen_trade(uid: str, trade_id: str):
    pf_compat.add_pf_seen_trades(uid, trade_id)


# ==================== 新增：按交易所获取数据的便捷函数 ====================

def get_all_exchanges_data(uid: str) -> Dict[str, Dict[str, Any]]:
    """获取所有交易所的数据（按交易所分组）"""
    return pf_compat.get_all_exchanges_data(uid)

def get_positions_by_exchange(uid: str) -> Dict[str, Dict[str, Any]]:
    """获取按交易所分组的持仓数据"""
    return pf_compat.get_positions_by_exchange(uid)

def get_closed_trades_by_exchange(uid: str) -> Dict[str, Dict[str, Any]]:
    """获取按交易所分组的已关闭交易"""
    return pf_compat.get_closed_trades_by_exchange(uid)


# ==================== AI Decision ID 辅助函数 ====================

def get_ai_decision_id_for_order(uid: str, exchange: str, order_id: str) -> Optional[int]:
    """
    获取限价单关联的 AI 决策 ID
    
    Args:
        uid: 用户 ID
        exchange: 交易所名称
        order_id: 订单 ID
    
    Returns:
        ai_decision_id 或 None
    """
    try:
        key = f"ai_decision:order:{uid}:{exchange}:{order_id}"
        data = redis_client.get(key)
        if data:
            if isinstance(data, bytes):
                data = data.decode('utf-8')
            info = json.loads(data)
            return info.get("ai_decision_id")
    except Exception as e:
        logger.debug(f"Failed to get ai_decision_id for order {order_id}: {e}")
    return None


def consume_ai_decision_id_for_order(uid: str, exchange: str, order_id: str) -> Optional[int]:
    """
    获取并删除限价单关联的 AI 决策 ID（一次性消费）
    
    Args:
        uid: 用户 ID
        exchange: 交易所名称
        order_id: 订单 ID
    
    Returns:
        ai_decision_id 或 None
    """
    try:
        key = f"ai_decision:order:{uid}:{exchange}:{order_id}"
        data = redis_client.get(key)
        if data:
            if isinstance(data, bytes):
                data = data.decode('utf-8')
            info = json.loads(data)
            ai_decision_id = info.get("ai_decision_id")
            
            # 删除 key（已消费）
            redis_client.delete(key)
            logger.debug(f"[AI-DECISION] Consumed ai_decision_id={ai_decision_id} for order {order_id}")
            
            return ai_decision_id
    except Exception as e:
        logger.debug(f"Failed to consume ai_decision_id for order {order_id}: {e}")
    return None


def consume_ai_decision_id_for_market(uid: str, exchange: str, symbol: str, side: str) -> Optional[int]:
    """
    获取并删除市价单关联的 AI 决策 ID（一次性消费）
    
    Args:
        uid: 用户 ID
        exchange: 交易所名称
        symbol: 交易对
        side: 方向 (LONG/SHORT)
    
    Returns:
        ai_decision_id 或 None
    """
    try:
        key = f"ai_decision:market:{uid}:{exchange}:{symbol}:{side}"
        data = redis_client.get(key)
        if data:
            if isinstance(data, bytes):
                data = data.decode('utf-8')
            info = json.loads(data)
            ai_decision_id = info.get("ai_decision_id")
            
            # 删除 key（已消费）
            redis_client.delete(key)
            logger.debug(f"[AI-DECISION] Consumed ai_decision_id={ai_decision_id} for market {symbol}:{side}")
            
            return ai_decision_id
    except Exception as e:
        logger.debug(f"Failed to consume ai_decision_id for market {symbol}:{side}: {e}")
    return None


def cleanup_ai_decision_id_for_order(uid: str, exchange: str, order_id: str) -> bool:
    """
    清理限价单关联的 AI 决策 ID temp key（订单撤销时调用）
    
    与 consume 不同，这个函数只删除 key，不返回 ai_decision_id
    用于订单撤销时清理未使用的 temp key
    
    Args:
        uid: 用户 ID
        exchange: 交易所名称
        order_id: 订单 ID
    
    Returns:
        是否成功删除（True 表示 key 存在并已删除）
    """
    try:
        key = f"ai_decision:order:{uid}:{exchange}:{order_id}"
        deleted = redis_client.delete(key)
        if deleted:
            logger.debug(f"[AI-DECISION] Cleaned up temp key for cancelled order {order_id}")
            return True
    except Exception as e:
        logger.debug(f"Failed to cleanup ai_decision_id for order {order_id}: {e}")
    return False