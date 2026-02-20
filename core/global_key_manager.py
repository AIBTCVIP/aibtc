#!/usr/bin/env python3
"""
全局键管理器 - 确保全局数据的正确清理策略

数据层次结构：
1. 全局客观数据 (所有用户共享)：
   - market:* - 市场K线数据
   - signals:* - 技术指标快照（客观计算结果）
   - pf:mark:* - 标记价格

2. 用户私有数据 (每个用户独立)：
   - user:* - 用户超级聚合键（包含AI信号、持仓等）
   - 其他pf:*键（已迁移到user:*）

全局数据清理原则：
- 只有当没有用户监控该币种时，才清理对应的全局数据
- 技术指标快照是客观的，所有用户看到的都一样
- AI信号是主观的，每个用户的模型和偏好都不同
"""

from typing import Set, List
from core.database import redis_client
from core.user_db import config_loader


class GlobalKeyManager:
    """全局键管理器"""

    @staticmethod
    def get_all_monitored_symbols() -> Set[str]:
        """获取所有用户监控的币种"""
        active_uids = config_loader.get_all_active_uids()
        monitored_symbols = set()

        for uid in active_uids:
            try:
                from core.user_context import get_user_context
                ctx = get_user_context(uid)
                if ctx:
                    symbols = ctx.get_monitor_symbols()
                    monitored_symbols.update(symbols)
            except Exception:
                pass

        # 总是包含系统基准币种（全局市场指标币种）
        from core.config import GLOBAL_MARKET_SYMBOLS
        system_symbols = set(GLOBAL_MARKET_SYMBOLS)
        monitored_symbols.update(system_symbols)

        return monitored_symbols

    @staticmethod
    def get_safe_to_delete_global_keys() -> List[str]:
        """
        获取可以安全删除的全局键

        返回策略：
        - 市场数据：只有当币种没有被任何用户监控时才删除
        - 信号数据：只有当币种没有被任何用户监控时才删除
        - 其他全局数据：通常不删除，除非明确标识为临时数据
        """
        all_keys = redis_client.keys("*")
        monitored_symbols = GlobalKeyManager.get_all_monitored_symbols()

        safe_to_delete = []

        for key in all_keys:
            if key.startswith("market:"):
                # market:klines:hot:BTCUSDT:15m
                parts = key.split(":")
                if len(parts) >= 4:
                    symbol = parts[3]
                    if symbol not in monitored_symbols:
                        safe_to_delete.append(key)

            elif key.startswith("signals:"):
                # signals:snapshot:BTCUSDT:15m
                parts = key.split(":")
                if len(parts) >= 3:
                    symbol = parts[2]
                    if symbol not in monitored_symbols:
                        safe_to_delete.append(key)

            elif key.startswith("unified_payload:"):
                # unified_payload:BTCUSDT
                parts = key.split(":")
                if len(parts) >= 2:
                    symbol = parts[1]
                    if symbol not in monitored_symbols:
                        safe_to_delete.append(key)

            # system:* 和 cluster:* 键永远不删除

        return safe_to_delete

    @staticmethod
    def cleanup_unused_global_data() -> int:
        """
        清理不再需要的全局数据

        返回删除的键数量
        """
        safe_to_delete = GlobalKeyManager.get_safe_to_delete_global_keys()
        monitored_symbols = GlobalKeyManager.get_all_monitored_symbols()

        if safe_to_delete:
            deleted_count = redis_client.delete(*safe_to_delete)
            print(f"🗑️ 清理 {deleted_count} 个不再使用的全局键")
            print(f"📊 当前监控币种: {sorted(monitored_symbols)}")

            # 详细说明清理的内容
            market_keys = [k for k in safe_to_delete if k.startswith("market:")]
            signal_keys = [k for k in safe_to_delete if k.startswith("signals:")]
            payload_keys = [k for k in safe_to_delete if k.startswith("unified_payload:")]

            if market_keys:
                print(f"  市场数据: {len(market_keys)} 个键")
            if signal_keys:
                print(f"  信号数据: {len(signal_keys)} 个键")
            if payload_keys:
                print(f"  负载数据: {len(payload_keys)} 个键")

            return deleted_count
        else:
            print("✅ 没有找到可以清理的全局数据")
            print(f"📊 当前监控币种: {sorted(monitored_symbols)}")
            return 0

    @staticmethod
    def get_global_data_usage_stats():
        """获取全局数据使用统计"""
        all_keys = redis_client.keys("*")
        monitored_symbols = GlobalKeyManager.get_all_monitored_symbols()

        stats = {
            "total_keys": len(all_keys),
            "monitored_symbols": sorted(monitored_symbols),
            "market_keys": len([k for k in all_keys if k.startswith("market:")]),
            "signal_keys": len([k for k in all_keys if k.startswith("signals:")]),
            "payload_keys": len([k for k in all_keys if k.startswith("unified_payload:")]),
            "system_keys": len([k for k in all_keys if k.startswith("system:")]),
            "cluster_keys": len([k for k in all_keys if k.startswith("cluster:")]),
            "user_keys": len([k for k in all_keys if k.startswith("user:")]),
        }

        # 计算每个币种的数据键数量
        symbol_stats = {}
        for symbol in monitored_symbols:
            symbol_stats[symbol] = {
                "market": len([k for k in all_keys if f"market:*:{symbol}:" in k]),
                "signals": len([k for k in all_keys if f"signals:*:{symbol}:" in k]),
                "payload": len([k for k in all_keys if f"unified_payload:{symbol}" in k]),
            }

        stats["symbol_stats"] = symbol_stats
        return stats


# 便捷函数
def cleanup_unused_global_data():
    """清理不再使用的全局数据"""
    return GlobalKeyManager.cleanup_unused_global_data()

def get_global_data_stats():
    """获取全局数据统计"""
    return GlobalKeyManager.get_global_data_usage_stats()