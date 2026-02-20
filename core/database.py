import asyncio
import threading
import redis
from redis import ConnectionPool
from redis import asyncio as aioredis
from core.config import REDIS_HOST, REDIS_PORT, REDIS_DB

# =============================================================================
# 同步 Redis 客户端（给同步代码使用）
# P0 Fix: 增加连接池大小，从 50 增加到 150，支持 1000+ 用户
# =============================================================================
redis_pool = ConnectionPool(
    host=REDIS_HOST,
    port=REDIS_PORT,
    db=REDIS_DB,
    max_connections=150,  # P0 Fix: 从 50 增加到 150
    decode_responses=True,
    socket_timeout=5.0,
    socket_connect_timeout=5.0,
)

redis_client = redis.StrictRedis(connection_pool=redis_pool)

# =============================================================================
# 异步 Redis 客户端（给异步代码使用）
# =============================================================================
_async_redis_client: "aioredis.Redis | None" = None
_async_redis_lock: "asyncio.Lock | None" = None  # P4 Fix: 异步锁保护
_async_redis_init_lock = threading.Lock()  # P4 Fix: 保护 asyncio.Lock 的创建


async def get_async_redis() -> aioredis.Redis:
    """
    获取异步 Redis 客户端（单例模式，线程安全）
    
    使用方式:
        redis = await get_async_redis()
        await redis.get("key")
    
    P0 Fix: 增加连接池大小，从 50 增加到 150
    P4 Fix: 添加异步锁保护，使用 threading.Lock 保护 asyncio.Lock 的创建
    """
    global _async_redis_client, _async_redis_lock
    
    # 使用 threading.Lock 保护 asyncio.Lock 的创建
    if _async_redis_lock is None:
        with _async_redis_init_lock:
            if _async_redis_lock is None:
                _async_redis_lock = asyncio.Lock()
    
    if _async_redis_client is None:
        async with _async_redis_lock:
            if _async_redis_client is None:  # Double-check locking
                _async_redis_client = await aioredis.from_url(
                    f"redis://{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}",
                    encoding="utf-8",
                    decode_responses=True,
                    max_connections=150,  # P0 Fix: 从 50 增加到 150
                    socket_timeout=5.0,
                    socket_connect_timeout=5.0,
                )
    return _async_redis_client


async def close_async_redis():
    """关闭异步 Redis 连接（应用退出时调用）"""
    global _async_redis_client
    if _async_redis_client is not None:
        await _async_redis_client.close()
        _async_redis_client = None

# === Redis Key 保留策略 ===
# 1️⃣ 永久保留：全局共享数据（所有用户都需要）
GLOBAL_KEEP_PREFIXES = (
    "market:",           # 市场K线数据 - 所有用户都需要
    "signals:",          # 技术指标快照 - 客观计算结果，所有用户都需要
    "pf:mark:",          # 标记价格 - 客观市场价格，所有用户都需要
    "system:",           # 系统配置 - 全局配置
    "cluster:",          # 集群信息 - 分布式系统需要
)

# 2️⃣ 用户私有保留：每个用户独立的数据
USER_PRIVATE_PREFIXES = (
    "user:",             # 用户超级聚合键
)

# 2️⃣ 条件保留：用户相关数据（至少一个活跃用户需要）
USER_KEEP_PREFIXES = (
    "user:",             # 用户超级聚合键 - 每个用户都有自己的
)

# 3️⃣ 历史数据保留：核心业务历史（用于分析和审计）
HISTORY_KEEP_PREFIXES = (
    "deepseek_analysis_request_history",
    "deepseek_analysis_response_history",
    "trading_records",
)

KEEP_PREFIXES = GLOBAL_KEEP_PREFIXES + USER_KEEP_PREFIXES + HISTORY_KEEP_PREFIXES

def smart_clear_redis():
    """
    智能清理Redis键 - 确保全局数据不被误删

    清理策略：
    1. 永久保留全局共享数据（所有用户都需要）
    2. 保留活跃用户的私有数据
    3. 清理孤立的临时数据和过期缓存
    4. 使用全局键管理器确保市场数据不被误删
    """
    from core.user_db import config_loader
    from core.global_key_manager import GlobalKeyManager

    all_keys = redis_client.keys("*")
    print(f"🔍 开始智能清理Redis，共 {len(all_keys)} 个键")

    # 获取所有活跃用户
    active_uids = config_loader.get_all_active_uids()
    active_user_keys = {f"user:{uid}" for uid in active_uids}
    print(f"👥 当前活跃用户数: {len(active_uids)}")

    # 获取所有用户监控的币种
    monitored_symbols = GlobalKeyManager.get_all_monitored_symbols()
    print(f"📊 当前监控币种: {sorted(monitored_symbols)}")

    # 分类键
    global_keys = []      # 永久保留的全局键
    user_keys = []        # 用户键
    temp_keys = []        # 可以清理的临时键
    orphaned_keys = []    # 孤立的键（用户已不存在）

    for key in all_keys:
        key_prefix = key.split(':')[0] + ':'

        if key_prefix in GLOBAL_KEEP_PREFIXES:
            # 使用全局键管理器判断是否需要保留
            if key_prefix in ["market:", "signals:", "unified_payload:"]:
                # 这些键只有在币种不再被监控时才可以清理
                parts = key.split(':')
                if len(parts) >= 2:
                    symbol = parts[1] if key_prefix == "unified_payload:" else parts[3] if len(parts) >= 4 else None
                    if symbol and symbol in monitored_symbols:
                        global_keys.append(key)
                        continue

                # 如果币种不再被监控，标记为可清理
                temp_keys.append(key)
            else:
                # system:* 和 cluster:* 等其他全局键直接保留
                global_keys.append(key)

        elif key_prefix in USER_KEEP_PREFIXES:
            # 检查是否是活跃用户的键
            if key in active_user_keys:
                user_keys.append(key)
            else:
                orphaned_keys.append(key)

        elif key_prefix in HISTORY_KEEP_PREFIXES:
            # 历史数据保留
            global_keys.append(key)

        else:
            # 检查是否是pf:*键（遗留的旧键）
            if key.startswith("pf:"):
                # pf:*键现在应该已经迁移，但如果还有残留，删除它们
                orphaned_keys.append(key)
            # 检查是否是已知的不重要临时键
            elif any(temp_prefix in key for temp_prefix in ["temp:", "cache:", "session:"]):
                temp_keys.append(key)
            else:
                # 未知类型的键，谨慎处理
                orphaned_keys.append(key)

    # 清理策略
    deleted_count = 0

    # 1. 清理不再使用的全局数据（临时键中包含的全局数据）
    unused_global_keys = [k for k in temp_keys if k.startswith(("market:", "signals:", "unified_payload:"))]
    if unused_global_keys:
        redis_client.delete(*unused_global_keys)
        deleted_count += len(unused_global_keys)
        print(f"🗑️ 清理 {len(unused_global_keys)} 个不再使用的全局键")

    # 2. 清理孤立的用户键（用户已不存在）
    if orphaned_keys:
        redis_client.delete(*orphaned_keys)
        deleted_count += len(orphaned_keys)
        print(f"🗑️ 清理 {len(orphaned_keys)} 个孤立键（包括遗留的pf:*键）")

    # 3. 清理其他临时键（不包括全局数据）
    other_temp_keys = [k for k in temp_keys if not k.startswith(("market:", "signals:", "unified_payload:"))]
    if other_temp_keys:
        redis_client.delete(*other_temp_keys)
        deleted_count += len(other_temp_keys)
        print(f"🗑️ 清理 {len(other_temp_keys)} 个临时键")

    # 统计信息
    print("\n📊 清理结果:")
    print(f"  ✅ 保留全局键: {len(global_keys)} 个")
    print(f"  ✅ 保留用户键: {len(user_keys)} 个")
    print(f"  🗑️ 删除键: {deleted_count} 个")
    total_retained = len(global_keys) + len(user_keys)
    if all_keys:
        retention_rate = total_retained / len(all_keys) * 100
        print(f"  📈 保留率: {retention_rate:.1f}%")

    return deleted_count

def cleanup_unused_global_data():
    """
    清理不再使用的全局数据

    这个函数会检查所有全局键，只有当某个币种不再被任何用户监控时，
    才会清理该币种相关的全局数据（市场数据、信号数据等）。
    """
    from core.global_key_manager import GlobalKeyManager
    return GlobalKeyManager.cleanup_unused_global_data()

# 向后兼容
def clear_redis():
    """向后兼容的清理函数 - 现在使用智能清理"""
    return smart_clear_redis()


# ====================================================================
# Redis 键名规范化 - 终极优化版：分级缓存架构
# ====================================================================

class RedisKeys:
    """Redis 键名统一管理 - 分级缓存架构"""

    # ==================== 全局共享数据 ====================
    @staticmethod
    def market_symbols() -> str:
        """活跃交易币种 - List"""
        return "market:symbols:active"

    @staticmethod
    def market_klines_hot(symbol: str, interval: str) -> str:
        """热K线数据 - Hash: timestamp -> json_data（内存）"""
        return f"global:market:klines:hot:{symbol}:{interval}"

    @staticmethod
    def market_klines_cold_meta(symbol: str, interval: str) -> str:
        """冷K线元数据 - Hash: 日期 -> file_path, size, checksum"""
        return f"global:market:klines:cold:meta:{symbol}:{interval}"

    @staticmethod
    def market_klines_last_update(symbol: str, interval: str) -> str:
        """K线最后更新时间戳 - String"""
        return f"global:market:update:{symbol}:{interval}"

    @staticmethod
    def market_prices(symbol: str) -> str:
        """标记价格数据 - String: json_data（全局共享）"""
        return f"global:market:prices:{symbol}"

    @staticmethod
    def signal_snapshot(symbol: str, interval: str) -> str:
        """技术指标快照 - String: json_data（全局共享）"""
        return f"global:signals:snapshot:{symbol}:{interval}"

    @staticmethod
    def market_external_data(symbol: str) -> str:
        """外部市场数据 - Hash: funding_rate, oi, change_24h_pct 等（全局共享）"""
        return f"global:market:external:{symbol}"

    @staticmethod
    def market_external_last_update() -> str:
        """外部市场数据最后更新时间 - String"""
        return "global:market:external:last_update"

    # ==================== 分布式处理 ====================
    @staticmethod
    def cluster_nodes() -> str:
        """集群节点列表 - Set: node_id"""
        return "cluster:nodes"

    @staticmethod
    def cluster_node_info(node_id: str) -> str:
        """节点信息 - Hash: status, last_heartbeat, capacity, etc"""
        return f"cluster:node:{node_id}"

    @staticmethod
    def cluster_task_queue() -> str:
        """分布式任务队列 - List: task_json"""
        return "cluster:tasks:queue"

    @staticmethod
    def cluster_task_result(task_id: str) -> str:
        """任务结果 - String: result_json"""
        return f"cluster:task:result:{task_id}"

    @staticmethod
    def cluster_task_status(task_id: str) -> str:
        """任务状态 - String: status_json"""
        return f"cluster:task:status:{task_id}"

    # ==================== 系统级数据 ====================
    @staticmethod
    def system_config() -> str:
        """系统配置 - Hash"""
        return "system:config"

    @staticmethod
    def system_stats() -> str:
        """系统统计 - Hash"""
        return "system:stats"

    # ==================== 用户超级聚合键 ====================
    @staticmethod
    def user(uid: str) -> str:
        """用户超级聚合键 - Hash: 包含用户所有相关数据"""
        return f"user:{uid}"

    # ==================== 用户数据字段定义 ====================
    @staticmethod
    def field_metadata() -> str:
        """元数据: 创建时间、最后活跃时间、版本等"""
        return "metadata"

    @staticmethod
    def field_account() -> str:
        """账户数据"""
        return "account"

    @staticmethod
    def field_equity_init() -> str:
        """初始权益"""
        return "equity_init"

    @staticmethod
    def field_positions() -> str:
        """持仓数据"""
        return "positions"

    @staticmethod
    def field_positions_active() -> str:
        """活跃持仓列表"""
        return "positions_active"

    @staticmethod
    def field_trades() -> str:
        """交易记录列表"""
        return "trades"

    @staticmethod
    def field_trades_closed() -> str:
        """已关闭交易字典 (按时间戳索引)"""
        return "trades_closed"

    @staticmethod
    def field_ai_history() -> str:
        """AI对话历史字典 (按时间戳索引) - 旧格式，保留兼容"""
        return "ai_history"

    @staticmethod
    def ai_history_zset(uid: str) -> str:
        """AI历史记录排序索引 - ZSET: timestamp -> timestamp (score=timestamp)"""
        return f"user:{uid}:ai_history:index"

    @staticmethod
    def ai_history_data(uid: str) -> str:
        """AI历史记录数据 - HASH: timestamp -> json_data"""
        return f"user:{uid}:ai_history:data"

    @staticmethod
    def field_decision_feedback() -> str:
        """决策反馈数据字典"""
        return "decision_feedback"

    @staticmethod
    def field_notifications() -> str:
        """通知历史列表"""
        return "notifications"

    @staticmethod
    def field_cache() -> str:
        """用户缓存数据字典"""
        return "cache"

    # ==================== 交易所级字段（新架构）====================
    # 格式: {exchange}:{field}
    # 例如: binance:account, okx:positions
    
    @staticmethod
    def exchange_field(exchange: str, field: str) -> str:
        """
        生成交易所级字段键
        
        Args:
            exchange: 交易所名称 (binance, okx, bitget, hyperliquid)
            field: 字段名 (account, equity_init, positions, cycles, closed_trades 等)
            
        Returns:
            格式化的字段键，如 "binance:positions"
        """
        return f"{exchange}:{field}"
    
    @staticmethod
    def exchange_account(exchange: str) -> str:
        """交易所账户数据"""
        return f"{exchange}:account"
    
    @staticmethod
    def exchange_equity_init(exchange: str) -> str:
        """交易所初始权益"""
        return f"{exchange}:equity_init"
    
    @staticmethod
    def exchange_positions(exchange: str) -> str:
        """交易所持仓数据"""
        return f"{exchange}:positions"
    
    @staticmethod
    def exchange_positions_active(exchange: str) -> str:
        """交易所活跃持仓列表"""
        return f"{exchange}:positions_active"
    
    @staticmethod
    def exchange_cycles(exchange: str) -> str:
        """交易所周期数据"""
        return f"{exchange}:cycles"
    
    @staticmethod
    def exchange_closed_trades(exchange: str) -> str:
        """交易所已关闭交易"""
        return f"{exchange}:closed_trades"
    
    @staticmethod
    def exchange_open_orders(exchange: str) -> str:
        """交易所挂单缓存（由 WebSocket 实时更新）"""
        return f"{exchange}:open_orders"

    # ==================== 字段默认值 ====================
    @staticmethod
    def default_metadata() -> dict:
        """默认元数据"""
        import time
        return {
            "created_at": time.time(),
            "last_active": time.time(),
            "version": "1.0",
            "data_counts": {
                "trades": 0,
                "trades_closed": 0,
                "ai_history": 0,
                "notifications": 0
            }
        }

