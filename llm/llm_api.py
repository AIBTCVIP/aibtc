# llm_api.py
"""
LLM API 模块 - 多用户版本

负责：
1. AI 投喂数据构建
2. HTTP Session 管理
3. JSON 解析 AI 响应
4. 批量数据缓存
"""

from __future__ import annotations

import json
import asyncio
import copy
import logging
import threading
import math
import aiohttp
from decimal import Decimal
import time
import re
from typing import TYPE_CHECKING, Optional, Dict, List, Any, Tuple

from core.database import redis_client, RedisKeys
from core.redis_manager import RedisDataManager
from core.config import GLOBAL_MARKET_SYMBOLS
from analysis.data.volume_stats import batch_fetch_async
from analysis.context.market_context import build_global_context
from analysis.context.decision_feedback import get_feedback_for_payload
from llm.llm_client import LLMAPIError

if TYPE_CHECKING:
    from core.user_context import UserContext
    from core.async_user_context import AsyncUserContext
    from trading.multi_exchange_trader import MultiExchangeTrader

logger = logging.getLogger(__name__)

# 全局 batch_cache，用于指标计算结果的临时存储
# P0 Fix: 添加线程锁保护，防止多用户并发访问时的数据竞争
_batch_cache_lock = threading.Lock()  # N21 fix: removed duplicate import (already imported at line 17)
batch_cache: Dict[str, Any] = {}

# 基准币种（用于 global_context 的 market_regime 计算）
# 引用 core/config.py 中的 GLOBAL_MARKET_SYMBOLS，转为 set 用于快速查找
BASE_SYMBOLS = set(GLOBAL_MARKET_SYMBOLS)

# 分组配置
MAX_SYMBOLS_PER_GROUP = 40  # 每组最多 40 个币种


def _first_valid(*values, default: Any = 0):
    """
    M3 fix: Return the first value that is not None.
    
    Unlike `a or b or c`, this correctly handles 0 and 0.0 as valid values.
    Only None is treated as "missing".
    
    Usage:
        _first_valid(data.get("fieldA"), data.get("fieldB"), default=0)
    """
    for v in values:
        if v is not None:
            return v
    return default


def _round_floats_recursive(obj: Any, precision: int = 6) -> Any:
    """
    递归处理数据结构中的所有浮点数，统一精度
    
    P1 Fix: 同时处理 inf/nan 特殊值，避免 JSON 序列化错误
    
    Args:
        obj: 任意数据结构（dict, list, float, etc.）
        precision: 保留的小数位数，默认6位
    
    Returns:
        处理后的数据结构
    """
    if isinstance(obj, dict):
        return {k: _round_floats_recursive(v, precision) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [_round_floats_recursive(item, precision) for item in obj]
    elif isinstance(obj, float):
        # P1 Fix: 处理特殊浮点值
        if math.isnan(obj) or math.isinf(obj):
            return None
        return round(obj, precision)
    elif isinstance(obj, Decimal):
        # E12 fix: Decimal 值不会被 isinstance(obj, float) 捕获，
        # 导致 Decimal 原样传递到 JSON 序列化，可能引发 TypeError。
        # 转换为 float 后统一精度处理。
        if not obj.is_finite():
            return None
        return round(float(obj), precision)
    else:
        return obj


def _normalize_timestamp_ms(ts) -> Optional[int]:
    """
    P0 Fix: 统一时间戳格式为毫秒级 epoch 整数
    
    支持输入格式：
    - None/0/"" -> None
    - 毫秒级整数/字符串 (13位): 直接返回
    - 秒级整数/字符串 (10位): 乘以 1000
    - 微秒级整数 (16位): 除以 1000
    """
    if ts is None or ts == "" or ts == 0:
        return None
    
    try:
        ts_num = int(ts)
        if ts_num == 0:
            return None
        # M16 fix: negative timestamps are invalid — return None instead of passing through
        if ts_num < 0:
            return None
        
        # 判断时间戳位数
        ts_str = str(abs(ts_num))
        digits = len(ts_str)
        
        if digits == 13:  # 毫秒
            return ts_num
        elif digits == 10:  # 秒
            return ts_num * 1000
        elif digits == 16:  # 微秒
            return ts_num // 1000
        elif digits < 10:  # 太短，可能是错误数据
            return None
        else:  # 其他情况，假设是毫秒
            return ts_num
    except (ValueError, TypeError):
        return None


# ================== 全局 HTTP Session ==================
_http_session: Optional[aiohttp.ClientSession] = None
_http_session_lock: Optional[asyncio.Lock] = None  # P4 Fix: 异步锁保护
_http_session_init_lock = threading.Lock()  # P4 Fix: 保护 asyncio.Lock 的创建

# P0 Fix: HTTP Session 超时配置
# - total: 总超时时间（包括连接、发送、接收）
# - connect: 连接超时
# - sock_read: 读取超时
HTTP_SESSION_TIMEOUT = aiohttp.ClientTimeout(
    total=120,      # 总超时 2 分钟（LLM 响应可能较慢）
    connect=15,     # 连接超时 15 秒
    sock_read=90    # 读取超时 90 秒
)


async def init_http_session() -> None:
    """初始化全局 HTTP Session（线程安全）"""
    global _http_session, _http_session_lock
    
    # P7 fix: create asyncio.Lock inside async context to ensure correct event loop binding.
    # On Python <3.10, creating asyncio.Lock() outside a running loop could bind to wrong loop.
    if _http_session_lock is None:
        with _http_session_init_lock:
            if _http_session_lock is None:
                # Safe: we're inside an async function, so an event loop is running
                _http_session_lock = asyncio.Lock()
    
    async with _http_session_lock:
        if _http_session is None or _http_session.closed:
            # P0 Fix: 添加超时配置，避免无限等待
            _http_session = aiohttp.ClientSession(timeout=HTTP_SESSION_TIMEOUT)
            logger.info("Global HTTP Session initialized (with timeout config)")


async def get_http_session() -> aiohttp.ClientSession:
    """获取全局 HTTP Session"""
    if _http_session is None or _http_session.closed:
        raise RuntimeError("HTTP Session 尚未初始化，请先调用 init_http_session()")
    return _http_session


async def close_http_session() -> None:
    """关闭全局 HTTP Session"""
    global _http_session
    # 先获取引用并置空，避免重复关闭
    session = _http_session
    _http_session = None
    
    if session is not None:
        try:
            await session.close()
            logger.info("Global HTTP Session closed")
        except Exception as e:
            logger.debug(f"关闭 HTTP Session 时异常: {e}")
        
        # 给 SSL transport 一点时间完成清理
        try:
            await asyncio.sleep(0.25)
        except Exception:
            pass


def json_safe_dumps(obj: Any) -> str:
    """
    安全的 JSON 序列化，处理 Decimal 类型和特殊浮点值
    
    P1 Fix: 处理 float('inf'), float('nan'), float('-inf') 等特殊值
    
    使用 orjson 加速（比标准库快 5-10 倍）
    """
    def _safe_default(x):
        if isinstance(x, Decimal):
            # V5-30 fix: Decimal('NaN')/Decimal('Inf') 转 float 后 orjson 会报错
            # 先检查 is_finite()，非有限值返回 None
            if not x.is_finite():
                return None
            return float(x)
        # N10 fix: orjson natively handles float but raises on inf/nan.
        # The isinstance(x, float) branch was dead code under orjson.
        # Now we handle non-serializable types by converting to string.
        return str(x)
    
    def _sanitize_for_json(obj):
        """Pre-process to replace inf/nan floats and Decimals that orjson can't handle."""
        if isinstance(obj, float):
            if math.isnan(obj) or math.isinf(obj):
                return None
            return obj
        # R15-4 fix: handle Decimal('Infinity') and Decimal('NaN')
        if isinstance(obj, Decimal):
            if not obj.is_finite():
                return None
            return float(obj)
        if isinstance(obj, dict):
            return {k: _sanitize_for_json(v) for k, v in obj.items()}
        if isinstance(obj, (list, tuple)):
            return [_sanitize_for_json(item) for item in obj]
        return obj
    
    try:
        import orjson
        return orjson.dumps(_sanitize_for_json(obj), default=_safe_default).decode('utf-8')
    except ImportError:
        # R15-1 fix: apply _sanitize_for_json before json.dumps too
        # json.dumps also raises ValueError for inf/nan floats
        return json.dumps(_sanitize_for_json(obj), ensure_ascii=False, default=_safe_default)


async def json_safe_dumps_async(obj: Any) -> str:
    """
    异步版本的 JSON 序列化，在线程池中执行以避免阻塞事件循环
    
    用于大型数据结构的序列化（如 LLM 投喂数据）
    """
    from core.async_helpers import run_cpu_bound
    return await run_cpu_bound(json_safe_dumps, obj)


# ================== 分组工具函数 ==================
def _split_into_groups(symbols: List[str], max_per_group: int = MAX_SYMBOLS_PER_GROUP) -> List[List[str]]:
    """
    将币种列表拆分为多个组，每组不超过 max_per_group 个

    Args:
        symbols: 币种列表
        max_per_group: 每组最大数量

    Returns:
        [[group1_symbols], [group2_symbols], ...]
    """
    groups = []
    for i in range(0, len(symbols), max_per_group):
        groups.append(symbols[i:i + max_per_group])
    return groups


# ================== API 预加载 ==================
async def preload_all_api(symbols: List[str]) -> Dict[str, Dict[str, Any]]:
    """
    预加载所有 API 数据（funding rate, 24h change, open interest）

    使用 volume_stats.batch_fetch_async() 实现真正的异步并发请求
    """
    return await batch_fetch_async(symbols)


def _get_balance_info_for_exchanges(uid: str, target_exchanges: Optional[List[str]] = None) -> Dict[str, Any]:
    """
    获取指定交易所的账户余额信息（从 Redis 读取 CycleStore 更新的数据）

    Args:
        uid: 用户 ID
        target_exchanges: 目标交易所列表，None 表示获取所有启用交易所的合并数据

    Returns:
        {
            "balance": float,      # 钱包余额
            "available": float,    # 可用余额
            "total_unrealized": float,  # 总未实现盈亏
            "exchanges": ["binance", ...]  # 数据来源交易所
        }
    """
    from core.pf_compatibility import pf_compat

    total_balance = 0.0
    total_available = 0.0
    total_unrealized = 0.0
    data_sources = []
    missing_exchanges = []  # 记录 Redis 中没有数据的交易所

    if target_exchanges:
        # 获取指定交易所的数据
        exchanges_to_query = target_exchanges
    else:
        # 获取所有启用交易所的数据
        from core.user_db import config_loader
        exchanges_to_query = config_loader.get_enabled_exchanges(uid) or []

    for exchange in exchanges_to_query:
        account = pf_compat.get_pf_account(uid, exchange)
        if account:
            # 不同交易所的字段名可能不同，统一处理
            # M3 fix: use _first_valid() instead of `or` chain to handle 0 correctly
            balance = float(_first_valid(
                account.get("walletBalance"), account.get("balance"), default=0
            ))
            # available 可能叫 availableBalance 或 available
            available = float(_first_valid(
                account.get("availableBalance"),
                account.get("available"),
                account.get("availableMargin"),
                default=balance  # 如果没有可用余额字段，使用钱包余额
            ))
            unrealized = float(_first_valid(
                account.get("unrealized"),
                account.get("totalUnrealizedProfit"),
                account.get("unrealizedPnl"),
                default=0
            ))

            total_balance += balance
            total_available += available
            total_unrealized += unrealized
            data_sources.append(exchange)
        else:
            # Redis 中没有数据，记录下来
            missing_exchanges.append(exchange)

    # 如果 Redis 完全没有数据，记录警告
    if not data_sources and exchanges_to_query:
        logger.warning(
            f"[{uid}] balance_info: Redis 无账户数据 (查询交易所: {exchanges_to_query}), "
            f"可能原因: 1.CycleStore未启动 2.WebSocket未连接 3.API认证失败"
        )

    return {
        "balance": round(total_balance, 2),
        "available": round(total_available, 2),
        "total_unrealized": round(total_unrealized, 2),
        "exchanges": data_sources,
        # N8 fix: track whether Redis *had* unrealized data, not whether it was non-zero.
        # Previously total_unrealized==0 (valid: positions net to zero PnL) was treated as "no data".
        "_unrealized_from_redis": len(data_sources) > 0,
        # 调试信息：记录哪些交易所没有数据
        "_missing_exchanges": missing_exchanges if missing_exchanges else None,
    }


async def _fetch_balance_realtime_async(
    multi_trader: 'MultiExchangeTrader',
    target_exchanges: Optional[List[str]] = None
) -> Dict[str, Any]:
    """
    通过 API 实时获取账户余额（用于 Redis 数据缺失时的 fallback）
    
    注意: exchange.get_balance() 返回的是 availableBalance（可用余额），
    不是 walletBalance（钱包余额）。因此这里 balance 和 available 相同。
    
    Args:
        multi_trader: 已初始化的多交易所交易器
        target_exchanges: 目标交易所列表（只获取这些交易所的数据）
    
    Returns:
        与 _get_balance_info_for_exchanges 相同格式的数据
    """
    if not multi_trader:
        return {
            "balance": 0.0,
            "available": 0.0,
            "total_unrealized": 0.0,
            "exchanges": [],
            "_source": "no_trader",
        }
    
    try:
        # 使用 MultiExchangeTrader 获取所有交易所的余额
        # 注意: get_all_balances() 返回的是 availableBalance
        balances = await multi_trader.get_all_balances()
        
        total_available = 0.0
        data_sources = []
        
        for exchange, balance in balances.items():
            if target_exchanges and exchange not in target_exchanges:
                continue
            # V5-16 fix: balance >= 0 替代 balance > 0
            # 旧逻辑：balance == 0（用户提走全部资金）的交易所被跳过，
            # 不出现在 data_sources 中，调用方误以为该交易所无数据。
            if isinstance(balance, (int, float)) and balance >= 0:
                total_available += float(balance)
                data_sources.append(exchange)
        
        if data_sources:
            logger.info(f"[{multi_trader.uid}] 实时获取余额成功: {total_available:.2f} USDT (来源: {data_sources})")
        
        return {
            # 注意: API 返回的是 availableBalance，这里 balance 近似等于 available
            # 实际 walletBalance = availableBalance + 保证金占用，但 API 不直接返回
            "balance": round(total_available, 2),
            "available": round(total_available, 2),
            "total_unrealized": 0.0,  # API 不返回未实现盈亏，后续从持仓计算
            "exchanges": data_sources,
            "_source": "realtime_api",
            "_unrealized_from_redis": False,  # 标记需要从持仓计算
        }
    except Exception as e:
        error_msg = str(e).lower()
        # 区分错误类型
        if any(kw in error_msg for kw in ["auth", "key", "permission", "signature", "invalid api"]):
            logger.error(f"实时获取余额认证失败: {e}")
        elif any(kw in error_msg for kw in ["timeout", "connect", "network"]):
            logger.warning(f"实时获取余额网络错误（可重试）: {e}")
        else:
            logger.error(f"实时获取余额失败: {e}")
        return {
            "balance": 0.0,
            "available": 0.0,
            "total_unrealized": 0.0,
            "exchanges": [],
            "_source": "error",
            "_error": str(e),
        }


def _get_positions_for_exchanges(uid: str, target_exchanges: Optional[List[str]] = None) -> List[Dict[str, Any]]:
    """
    获取指定交易所的持仓数据（从 Redis 读取 CycleStore 更新的数据）

    Args:
        uid: 用户 ID
        target_exchanges: 目标交易所列表，None 表示获取所有启用交易所的合并数据

    Returns:
        持仓列表，每个持仓包含 exchange 字段
    """
    from core.pf_compatibility import pf_compat

    all_positions = []

    if target_exchanges:
        exchanges_to_query = target_exchanges
    else:
        from core.user_db import config_loader
        exchanges_to_query = config_loader.get_enabled_exchanges(uid) or []

    for exchange in exchanges_to_query:
        # 获取该交易所的持仓数据（字典格式 {symbol:side: position_data}）
        positions_dict = pf_compat.get_pf_pos(uid, exchange, add_exchange_field=True)

        if positions_dict and isinstance(positions_dict, dict):
            for key, pos_data in positions_dict.items():
                if not isinstance(pos_data, dict):
                    continue

                # R15-3 fix: wrap individual position parsing in try/except
                # so one corrupt position doesn't crash the entire function
                try:
                    # key 格式可能是 "ETHUSDT:SHORT" 或 "ETHUSDT"
                    # 从 pos_data 中获取 symbol，如果没有则从 key 中提取
                    symbol = pos_data.get("symbol") or key.split(":")[0]

                    # 获取 size，需要考虑 side 来确定正负
                    # M3 fix: use _first_valid() to handle qty=0 correctly
                    qty = float(_first_valid(
                        pos_data.get("qty"), pos_data.get("size"), pos_data.get("positionAmt"), default=0
                    ))
                    side = pos_data.get("side", "").upper()
                    if side == "SHORT" and qty > 0:
                        qty = -qty  # SHORT 仓位用负数表示

                    # 获取基础字段
                    # M3 fix: use _first_valid() to handle 0 values correctly
                    entry_price = float(_first_valid(
                        pos_data.get("entryPrice"), pos_data.get("entry"), pos_data.get("avgPrice"), default=0
                    ))
                    pnl = float(_first_valid(
                        pos_data.get("unrealizedPnl"), pos_data.get("pnl"), pos_data.get("unrealizedProfit"), default=0
                    ))

                    # 获取 mark_price，如果没有则从 entry + pnl 计算
                    # M3 fix: use _first_valid() to handle markPrice=0 correctly
                    mark_price = float(_first_valid(
                        pos_data.get("markPrice"), pos_data.get("mark"), pos_data.get("lastPrice"), default=0
                    ))
                    if mark_price == 0 and entry_price > 0 and qty != 0:
                        # 根据 PnL 反算 mark_price
                        # LONG: mark_price = entry + pnl/qty
                        # SHORT: qty < 0, so formula still works: mark_price = entry + pnl/qty (pnl/negative = negative adjustment)
                        mark_price = entry_price + (pnl / qty)

                    # 获取止损止盈价格
                    stop_loss_price = pos_data.get("stopLossPrice")
                    take_profit_price = pos_data.get("takeProfitPrice")

                    # 获取入场时间（不同交易所字段名不同）
                    # N4 fix: use _first_valid() to handle 0 correctly (same class as M3)
                    entry_time = _first_valid(
                        pos_data.get("openTimeMs"),   # Binance (从 CycleStore)
                        pos_data.get("openTime"),     # Bybit
                        pos_data.get("createTime"),   # OKX
                        pos_data.get("createdAt"),
                        pos_data.get("entryTime"),
                        default=0
                    )

                    # 获取 last_update_time
                    # N4 fix: use _first_valid() to handle 0 correctly
                    raw_update_time = _first_valid(
                        pos_data.get("updatedAt"),
                        pos_data.get("ts"),
                        pos_data.get("updateTime"),
                        default=0
                    )

                    # 统一字段格式
                    position = {
                        "symbol": symbol,
                        "exchange": exchange,
                        "size": qty,
                        "entry": entry_price,
                        "mark_price": mark_price,
                        "pnl": pnl,
                        # N2+N3 fix: use _first_valid() to handle 0 correctly (same class as M3)
                        "leverage": int(_first_valid(pos_data.get("leverage"), pos_data.get("lev"), default=1)),
                        "position_value": float(_first_valid(pos_data.get("notional"), pos_data.get("positionValue"), default=0)),
                        "entry_time": _normalize_timestamp_ms(entry_time),  # P1 Fix + P0 Fix: 标准化为毫秒
                        "last_update_time": _normalize_timestamp_ms(raw_update_time),  # P0 Fix: 标准化为毫秒
                        # 止损止盈
                        "stop_loss_price": float(stop_loss_price) if stop_loss_price else None,
                        "take_profit_price": float(take_profit_price) if take_profit_price else None,
                    }

                    # P1 enhancement: peak_pnl for position risk assessment
                    # Helps LLM detect "gave back profits" scenarios (e.g. DOGE case)
                    peak_pnl_raw = pos_data.get("peakPnl")
                    if peak_pnl_raw is not None:
                        try:
                            position["peak_pnl"] = float(peak_pnl_raw)
                        except (ValueError, TypeError):
                            pass

                    # 计算 position_value（如果没有）
                    if position["position_value"] == 0 and position["mark_price"] > 0:
                        position["position_value"] = abs(position["size"]) * position["mark_price"]

                    # 只添加有效持仓（size != 0）
                    if position["size"] != 0:
                        all_positions.append(position)
                except (ValueError, TypeError, KeyError) as e:
                    # R15-3 fix: skip malformed positions instead of crashing
                    logger.warning(f"[{uid}] Skipping malformed position {key} on {exchange}: {e}")
                    continue

    return all_positions


async def _get_open_limit_orders_for_exchanges(
        multi_trader: 'MultiExchangeTrader',
        target_exchanges: Optional[List[str]] = None
) -> List[Dict[str, Any]]:
    """
    从各交易所 API 获取挂单（限价入场单）

    Args:
        multi_trader: 已初始化的 MultiExchangeTrader
        target_exchanges: 目标交易所列表

    Returns:
        挂单列表，每个挂单包含 exchange 字段
    """
    if not multi_trader:
        return []

    all_orders = []
    exchanges_to_query = target_exchanges or list(multi_trader._exchanges.keys())

    # 从 multi_trader 获取 uid，用于 Binance 从 Redis 缓存读取挂单
    uid = getattr(multi_trader, 'uid', None)
    
    for exchange_name in exchanges_to_query:
        exchange = multi_trader._exchanges.get(exchange_name)
        if not exchange:
            continue

        try:
            orders = await _get_exchange_open_orders(exchange, exchange_name, uid)
            all_orders.extend(orders)
        except Exception as e:
            logger.warning(f"获取 {exchange_name} 挂单失败: {e}")

    return all_orders


async def _get_exchange_open_orders(exchange, exchange_name: str, uid: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    从单个交易所获取挂单（根据交易所类型调用不同 API）
    
    Binance: 从 Redis 缓存读取（WebSocket 实时更新，零 API 调用）
    其他交易所: 仍使用 REST API
    
    Args:
        exchange: 交易所实例
        exchange_name: 交易所名称
        uid: 用户 ID（用于 Binance 从 Redis 缓存读取）
    """
    orders = []

    try:
        # N24 fix: deduplicated binance/okx/bitget order parsing — all use same Redis cache path
        if exchange_name in ("binance", "okx", "bitget"):
            from core.pf_compatibility import pf_compat
            
            if uid:
                cached_orders = pf_compat.get_pf_open_orders(uid, exchange_name)
                for order_id, o in (cached_orders or {}).items():
                    orders.append({
                        "exchange": exchange_name,
                        "symbol": o.get("symbol"),
                        "orderId": o.get("orderId"),
                        "side": o.get("side"),
                        "positionSide": o.get("positionSide"),
                        "price": float(o.get("price") or 0),
                        "origQty": float(o.get("origQty") or 0),
                        "executedQty": float(o.get("executedQty") or 0),
                    })
            else:
                logger.warning(f"获取 {exchange_name} 挂单失败: 无法获取 uid（需从 multi_trader 传入）")

        elif exchange_name == "hyperliquid":
            # Hyperliquid: 使用 frontend_open_orders API
            try:
                address = exchange.wallet_address
                if address:
                    raw_orders = await exchange._run_sync(exchange._info.frontend_open_orders, address)
                    for o in raw_orders or []:
                        # 只保留限价入场单（排除 trigger 订单，即止盈止损）
                        order_type = o.get('orderType', '')
                        is_trigger = o.get('isTrigger', False)

                        # 跳过止盈止损单（trigger 类型）
                        if is_trigger or 'Stop' in order_type or 'Take Profit' in order_type:
                            continue

                        # 转换 symbol (如 BTC -> BTCUSDT)
                        coin = o.get('coin', '')
                        symbol = f"{coin}USDT" if coin else ""

                        # 判断方向
                        sz = float(o.get('sz', 0))
                        # P8 fix: skip orders with sz=0 (cancelled/filled phantom orders)
                        if sz == 0:
                            continue
                        side = "BUY" if sz > 0 else "SELL"
                        position_side = "LONG" if sz > 0 else "SHORT"

                        orders.append({
                            "exchange": exchange_name,
                            "symbol": symbol,
                            "orderId": o.get('oid'),
                            "side": side,
                            "positionSide": position_side,
                            "price": float(o.get('limitPx') or 0),
                            "origQty": abs(sz),
                            "executedQty": 0,  # Hyperliquid 不返回已成交数量
                        })
            except Exception as e:
                logger.debug(f"Hyperliquid 获取挂单失败: {e}")

    except Exception as e:
        logger.warning(f"获取 {exchange_name} 挂单异常: {e}")

    return orders


# ================== P1 Fix: 提示注入防护 ==================
def _sanitize_string_for_prompt(s: str, max_length: int = 1000) -> str:
    """
    清理字符串，防止提示注入攻击
    
    Args:
        s: 输入字符串
        max_length: 最大长度限制
    
    Returns:
        清理后的字符串
    """
    if not isinstance(s, str):
        return s
    
    # 移除可能的提示注入标记
    dangerous_patterns = [
        '</JSON>', '<JSON>', '</decision>', '<decision>',
        'IGNORE PREVIOUS INSTRUCTIONS', 'IGNORE ALL INSTRUCTIONS',
        'SYSTEM:', 'USER:', 'ASSISTANT:',
        '```system', '```user', '```assistant',
        '---\nrole:', '###SYSTEM', '###USER',
    ]
    result = s
    # N30 fix: use case-insensitive replacement to catch mixed-case injection patterns
    # Previously only checked original case and lowercase, missing "System:", "IGNORE previous", etc.
    for pattern in dangerous_patterns:
        result = re.sub(re.escape(pattern), '', result, flags=re.IGNORECASE)
    
    # 限制字符串长度
    if len(result) > max_length:
        result = result[:max_length]
    
    return result


def _sanitize_for_prompt(obj: Any) -> Any:
    """
    递归清理数据结构中的字符串，防止提示注入
    
    Args:
        obj: 任意数据结构
    
    Returns:
        清理后的数据结构
    """
    if isinstance(obj, dict):
        return {k: _sanitize_for_prompt(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [_sanitize_for_prompt(item) for item in obj]
    elif isinstance(obj, str):
        return _sanitize_string_for_prompt(obj)
    return obj


# ================== 数据格式化 ==================
def build_llm_user_prompt(market_snapshot: Dict[str, Any]) -> str:
    """
    构建 LLM 用户 prompt（同步版本）
    
    P1 Fix: 添加提示注入防护
    """
    # 清理数据，防止提示注入
    sanitized = _sanitize_for_prompt(market_snapshot)
    return f"""
<JSON>
{json_safe_dumps(sanitized)}
</JSON>
""".strip()


async def build_llm_user_prompt_async(market_snapshot: Dict[str, Any]) -> str:
    """
    构建 LLM 用户 prompt（异步版本）
    
    将 JSON 序列化放入线程池执行，避免阻塞事件循环
    """
    from core.async_helpers import run_cpu_bound
    return await run_cpu_bound(build_llm_user_prompt, market_snapshot)


# ================== LLM 理解优化：Summary 和 Quick Access ==================
def _extract_bias_from_market(market_data: dict) -> Optional[dict]:
    """
    从 market_data 中提取 bias 信息

    Phase 4: 优先 symbol_analysis.bias，再 _quick / ai_enhancement。
    """
    if not market_data:
        return None

    # Path 0: 从 symbol_analysis.bias 获取（上游契约）
    sa = market_data.get("symbol_analysis")
    if isinstance(sa, dict):
        bias_mod = sa.get("bias") or {}
        direction = bias_mod.get("direction")
        if direction and direction != "unknown":
            return {
                "bias": direction.replace("bullish", "bullish").replace("bearish", "bearish"),
                "bias_score": bias_mod.get("score"),
                "bias_strength": bias_mod.get("strength"),
                "trend_conflict": bias_mod.get("trend_conflict", False),
                "trade_suggestion": bias_mod.get("trade_suggestion"),
            }

    # Path 1: 从 _quick 获取（Phase 4 最小 _quick 可能不含 bias，兼容旧 payload）
    quick = market_data.get("_quick", {})
    if quick.get("bias"):
        return {
            "bias": quick.get("bias"),
            "bias_score": quick.get("bias_score", 0),
            "bias_strength": quick.get("bias_strength"),
            "trend_conflict": quick.get("trend_conflict", False),
            "trade_suggestion": quick.get("trade_suggestion"),  # P14 Fix: 添加 trade_suggestion
        }
    
    # Path 2: 从顶层 ai_enhancement 获取
    ai_enh = market_data.get("ai_enhancement", {})
    overall_bias = ai_enh.get("overall_bias", {})
    if overall_bias.get("bias_direction"):
        return {
            "bias": overall_bias.get("bias_direction"),
            "bias_score": overall_bias.get("bias_score", 0),
            "bias_strength": overall_bias.get("bias_strength"),
            "trend_conflict": overall_bias.get("trend_conflict", False),
            "trade_suggestion": overall_bias.get("trade_suggestion"),  # P14 Fix
        }
    
    # Path 3: 从 timeframes 嵌套路径获取
    timeframes = market_data.get("timeframes", {})
    for tf in ["15m", "1h", "4h"]:
        tf_data = timeframes.get(tf, {})
        indicators = tf_data.get("indicators", {})
        ai_enh = indicators.get("ai_enhancement", {})
        overall_bias = ai_enh.get("overall_bias", {})
        if overall_bias.get("bias_direction"):
            return {
                "bias": overall_bias.get("bias_direction"),
                "bias_score": overall_bias.get("bias_score", 0),
                "bias_strength": overall_bias.get("bias_strength"),
                "trend_conflict": overall_bias.get("trend_conflict", False),
                "trade_suggestion": overall_bias.get("trade_suggestion"),  # P14 Fix
            }
    
    return None


def _build_minimal_quick(market_data: dict) -> dict:
    """
    Phase 4: 仅填充 _quick 的 importance/importance_label，供持仓/挂单与 symbols 层使用。
    委托 analysis.conclusions.importance_scorer；has_position / has_open_order 由后续逻辑写入。
    """
    if not market_data:
        return market_data
    from analysis.conclusions.importance_scorer import compute_importance
    quick = dict(market_data.get("_quick") or {})
    quick.update(compute_importance(market_data))
    market_data["_quick"] = quick
    return market_data


def _normalize_market_data_for_feed(market_data: Dict[str, Any], symbol: str) -> Dict[str, Any]:
    """
    规范化 market_data，保证传入 context_builder 的数据形状一致。
    Phase 4: 不再调用 _flatten_symbol_data；仅保证 _quick 含 importance/importance_label（由 _build_minimal_quick 填充）。

    1. 若无顶层 ai_enhancement，从 timeframes.*.indicators.ai_enhancement 提升并写入顶层。
    2. 若无 referee_context 且存在 timeframes，用各周期 indicators 构建 referee_context。
    3. 调用 _build_minimal_quick 保证 _quick 存在（供 importance / has_position / has_open_order 使用）。
    """
    if not market_data:
        return market_data

    # 1. 提升 ai_enhancement 到顶层（若缺失）
    if not market_data.get("ai_enhancement") and market_data.get("timeframes"):
        for tf in ("15m", "1h", "4h"):
            ind = market_data["timeframes"].get(tf, {}).get("indicators", {})
            ai_enh = ind.get("ai_enhancement") if ind else None
            if ai_enh:
                market_data["ai_enhancement"] = _round_floats_recursive(ai_enh, precision=6)
                break

    # 2. 若无 referee_context，从 timeframes 的 indicators 构建
    if not market_data.get("referee_context") and market_data.get("timeframes"):
        try:
            from analysis.assembly.payload_builder import build_referee_context
            tf4h = market_data["timeframes"].get("4h", {}).get("indicators", {})
            tf1h = market_data["timeframes"].get("1h", {}).get("indicators", {})
            tf15m = market_data["timeframes"].get("15m", {}).get("indicators", {})
            if tf4h or tf15m:
                market_data["referee_context"] = build_referee_context(tf4h, tf1h, tf15m)
        except Exception:
            pass

    # 3. Phase 4: 仅填充最小 _quick（importance/importance_label），后续会写入 has_position / has_open_order
    _build_minimal_quick(market_data)
    return market_data


# ================== 全局 batch_cache 操作 ==================
def add_to_batch(symbol: str, interval: str, indicators: Optional[Dict[str, Any]] = None) -> None:
    """
    添加数据到全局 batch_cache（单用户兼容版本）
    
    P0 Fix: 使用锁保护，防止并发写入导致的数据竞争
    """
    global batch_cache
    with _batch_cache_lock:
        if symbol not in batch_cache:
            batch_cache[symbol] = {}

        payload: Dict[str, Any] = {}
        if indicators is not None:
            payload["indicators"] = indicators

        batch_cache[symbol][interval] = payload


def get_batch_cache_copy() -> Dict[str, Any]:
    """
    获取全局 batch_cache 的线程安全副本
    
    P0 Fix: 新增函数，用于安全读取 batch_cache
    E7 fix: 使用 copy.deepcopy 替代 v.copy()。
    v.copy() 只做浅拷贝 — 嵌套的 dict/list 仍然是共享引用，
    调用方修改嵌套结构会污染全局 cache。
    """
    with _batch_cache_lock:
        return copy.deepcopy(batch_cache)


def clear_batch_cache() -> None:
    """
    清空全局 batch_cache
    
    P0 Fix: 新增函数，用于安全清空 batch_cache
    """
    global batch_cache
    with _batch_cache_lock:
        batch_cache.clear()


# ================== 多用户版本函数 ==================
def add_to_batch_for_user(ctx: 'UserContext', symbol: str, interval: str,
                          indicators: Optional[Dict[str, Any]] = None) -> None:
    """
    添加数据到用户的 batch_cache（多用户版本）
    """
    if symbol not in ctx.batch_cache:
        ctx.batch_cache[symbol] = {}

    payload: Dict[str, Any] = {}
    if indicators is not None:
        payload["indicators"] = indicators

    ctx.batch_cache[symbol][interval] = payload


async def _build_feed_json_for_user(
        ctx: 'UserContext',
        dataset: Dict[str, Any],
        preloaded: Dict[str, Any],
        global_context: Dict[str, Any],
        group_symbols: Optional[List[str]] = None,
        target_exchanges: Optional[List[str]] = None,
        multi_trader: Optional['MultiExchangeTrader'] = None,
        preloaded_positions: Optional[List[Dict[str, Any]]] = None,
        preloaded_orders: Optional[List[Dict[str, Any]]] = None,
        preloaded_balance_info: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    构建投喂 JSON（多用户版本）

    Args:
        ctx: 用户上下文
        dataset: 市场数据
        preloaded: 预加载的 API 数据
        global_context: 全局上下文
        group_symbols: 本组币种（可选）
        target_exchanges: 目标交易所列表（用于获取对应的账户余额和持仓）
        multi_trader: 已初始化的多交易所交易器（用于获取挂单）
        preloaded_positions: 预加载的持仓数据（避免重复获取）
        preloaded_orders: 预加载的挂单数据（避免重复获取）
        preloaded_balance_info: 预加载的余额数据（避免重复获取）
    """
    # M10 fix: use preloaded balance_info if available, avoid double fetch
    # N1 fix: .copy() to avoid mutating the caller's dict (which is shared with global_context)
    if preloaded_balance_info is not None:
        balance_info = preloaded_balance_info.copy()
    else:
        # 获取账户数据：从目标交易所的 Redis 数据获取
        balance_info = _get_balance_info_for_exchanges(ctx.uid, target_exchanges)
        
        # Fallback: 如果 Redis 没有余额数据，尝试实时获取
        # 修复: 只要有缺失的交易所就尝试补充，而不是要求 balance=0
        if balance_info.get("_missing_exchanges") and multi_trader:
            logger.info(f"[{ctx.uid}] _build_feed_json: 部分交易所无缓存数据 ({balance_info.get('_missing_exchanges')})，尝试实时获取...")
            try:
                realtime_balance = await _fetch_balance_realtime_async(multi_trader, balance_info.get("_missing_exchanges"))
                if realtime_balance.get("balance", 0) > 0:
                    # 合并实时获取的数据
                    balance_info["balance"] = round(balance_info.get("balance", 0) + realtime_balance.get("balance", 0), 2)
                    balance_info["available"] = round(balance_info.get("available", 0) + realtime_balance.get("available", 0), 2)
                    balance_info["exchanges"] = balance_info.get("exchanges", []) + realtime_balance.get("exchanges", [])
                    balance_info["_has_realtime_supplement"] = True
                    logger.info(f"[{ctx.uid}] 实时补充余额成功，当前总余额: {balance_info.get('balance')} USDT")
            except Exception as e:
                logger.warning(f"[{ctx.uid}] _build_feed_json: 实时获取余额失败: {e}")
    
    # M4 fix: balance_info_clean is created AFTER unrealized recalculation (see below)
    # to ensure the LLM sees the recalculated total_unrealized value.

    symbols_to_include = set(group_symbols) if group_symbols else set(dataset.keys())

    # P0 Fix: 统一时间戳格式为毫秒级 epoch（整数），便于 LLM 进行时间计算
    output: Dict[str, Any] = {
        "timestamp": int(time.time() * 1000),  # epoch milliseconds
        "timestamp_utc": time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime()),  # human-readable backup
        "uid": ctx.uid,
    }

    # 全局上下文（过滤本组币种）
    # R15-5 fix: use copy.deepcopy to prevent downstream mutations from corrupting
    # the original global_context (shared across concurrent async calls)
    # V5-15 fix: 移除函数内 import copy — 已在模块顶部导入
    filtered_global_context = copy.deepcopy(global_context)

    if "opportunity_ranking" in filtered_global_context:
        orig_ranking = filtered_global_context["opportunity_ranking"]
        filtered_ranking = orig_ranking.copy()
        if "ranking" in orig_ranking:
            filtered_ranking["ranking"] = [
                r for r in orig_ranking["ranking"]
                if r.get("symbol") in symbols_to_include
            ]
            filtered_ranking["total_analyzed"] = len(filtered_ranking["ranking"])
        filtered_global_context["opportunity_ranking"] = filtered_ranking

    output["global_context"] = filtered_global_context

    # 挂单（使用预加载数据或实时获取）
    # 注意：不过滤币种，让 AI 看到所有挂单（可能有非监控币种的挂单）
    if preloaded_orders is not None:
        all_orders = preloaded_orders
    elif multi_trader:
        try:
            all_orders = await _get_open_limit_orders_for_exchanges(multi_trader, target_exchanges)
        except Exception as e:
            logger.warning(f"获取挂单失败: {e}")
            all_orders = []
    else:
        all_orders = []

    # P6 Fix: 增强挂单数据，添加 distance_pct 和 notional_value（委托 analysis.context.position_order_formatter）
    from analysis.context.position_order_formatter import enrich_order_for_feed
    enhanced_orders = []
    for o in all_orders:
        symbol = o.get("symbol")
        current_price = None
        if symbol in dataset:
            ind_15m = dataset[symbol].get("15m", {}).get("indicators", {})
            current_price = ind_15m.get("close")
        enhanced_orders.append(enrich_order_for_feed(o, current_price))
    output["open_limit_orders"] = enhanced_orders

    # 持仓（使用预加载数据或实时获取）
    # 注意：不过滤币种，让 AI 看到所有持仓（可能有非监控币种的持仓）
    if preloaded_positions is not None:
        positions = preloaded_positions
    else:
        positions = _get_positions_for_exchanges(ctx.uid, target_exchanges)

    # P0 Fix: 如果 Redis 返回的 unrealized=0 但持仓有 PnL，则从持仓计算
    # 这解决了 Redis 数据可能未及时同步的问题
    if not balance_info.get("_unrealized_from_redis", True) and positions:
        calculated_unrealized = sum(float(p.get("pnl") or 0) for p in positions)
        if calculated_unrealized != 0:
            balance_info["total_unrealized"] = round(calculated_unrealized, 2)
            balance_info["_unrealized_source"] = "calculated_from_positions"
    # 清理内部标记字段，不暴露给 LLM
    balance_info.pop("_unrealized_from_redis", None)

    # M4 fix: create balance_info_clean AFTER unrealized recalculation
    # so the LLM sees the corrected total_unrealized value
    # Note: balance_info is used by build_market_layer, not stored in _meta (redundant with market.account)
    balance_info_clean = {k: v for k, v in balance_info.items() if not k.startswith("_")}

    # 持仓展示字段委托 analysis.context.position_order_formatter.enrich_position_for_feed
    from analysis.context.position_order_formatter import enrich_position_for_feed
    output["positions"] = []
    for p in positions:
        size = float(p.get("size") or 0)
        if size == 0:
            continue
        output["positions"].append(enrich_position_for_feed(p))

    # 市场数据
    # symbols_to_include 现在包含：监控币种 ∪ 持仓币种 ∪ 挂单币种
    # 优先用 Redis 的完整 payload（含 symbol_analysis），保证 structure/levels/sentiment 等上游结构进入 feed
    from analysis.assembly.payload_builder import get_ai_ready_payload
    from analysis.indicators import get_tf_snapshot

    output["markets"] = {}
    symbol_count = 0
    for symbol in symbols_to_include:
        p24 = preloaded.get("p24", {}).get(symbol)
        fr = preloaded.get("funding", {}).get(symbol)
        oi = preloaded.get("oi", {}).get(symbol)

        kline_close = None
        fallback_24h_change = None
        fallback_funding = None
        market_data: Dict[str, Any] = {}

        # 优先从 Redis 取完整 payload（含完整 symbol_analysis：structure/levels/order_flow/volatility/pattern/correlation/sentiment）
        payload = get_ai_ready_payload(symbol)

        if payload and payload.get("ready") and payload.get("symbol_analysis"):
            # 使用上游完整 symbol_analysis，保证 feed 不丢结构数据
            tf15m_snapshot = get_tf_snapshot(symbol, "15m")
            kline_close = tf15m_snapshot.get("close") if tf15m_snapshot else None
            if symbol in dataset and (dataset[symbol].get("15m") or {}).get("close") is not None:
                kline_close = (dataset[symbol]["15m"].get("close") or dataset[symbol]["15m"].get("indicators", {}).get("close")) or kline_close
            ai_enh = _round_floats_recursive(payload.get("ai_enhancement") or {}, precision=6)
            market_data = {
                "price": kline_close or (p24.get("lastPrice") if p24 else None),
                "24h_change_pct": p24.get("priceChangePercent") if p24 else None,
                "funding_rate": fr if fr is not None else None,
                "high_24h": p24.get("highPrice") if p24 else None,
                "low_24h": p24.get("lowPrice") if p24 else None,
                "volume_24h_usdt": p24.get("quoteVolume") if p24 else None,
                "timeframes": {},
                "ai_enhancement": ai_enh,
                "referee_context": payload.get("referee_context", {}),
                "symbol_analysis": payload.get("symbol_analysis"),
            }
            for tf in ["4h", "1h", "15m"]:
                tf_snapshot = get_tf_snapshot(symbol, tf)
                if tf_snapshot:
                    filtered = {k: v for k, v in tf_snapshot.items() if k not in ("symbol", "tf", "timestamp")}
                    market_data["timeframes"][tf] = {"indicators": _round_floats_recursive(filtered, precision=6)}
        elif symbol in dataset:
            # 无 payload 时：监控币种用 dataset 构建 market_data，symbol_analysis 用 Redis snapshot 现算（snapshot 含 structure 等）
            cycles = dataset[symbol]
            ai_enh = {}
            if "15m" in cycles:
                c15 = cycles["15m"]
                kline_close = c15.get("close") or (c15.get("indicators") or {}).get("close")
                ai_enh = c15.get("ai_enhancement", {})
                if ai_enh:
                    mkt_ctx = ai_enh.get("market_context", {})
                    if not fr and mkt_ctx.get("funding_rate") is not None:
                        fallback_funding = mkt_ctx.get("funding_rate")
                    if mkt_ctx.get("24h_change_pct") is not None:
                        fallback_24h_change = mkt_ctx.get("24h_change_pct")

            market_data = {
                "price": kline_close,
                "24h_change_pct": p24.get("priceChangePercent") if p24 else fallback_24h_change,
                "funding_rate": fr if fr is not None else fallback_funding,
                "high_24h": p24.get("highPrice") if p24 else None,
                "low_24h": p24.get("lowPrice") if p24 else None,
                "volume_24h_usdt": p24.get("quoteVolume") if p24 else None,
                "timeframes": {},
            }
            for interval, data in cycles.items():
                ind = data.get("indicators") or data
                if isinstance(ind, dict) and ("close" in ind or "structure" in ind):
                    market_data["timeframes"][interval] = {"indicators": _round_floats_recursive(ind, precision=6)}
                else:
                    market_data["timeframes"][interval] = {"indicators": _round_floats_recursive(data.get("indicators") or {}, precision=6)}
            # 用 Redis snapshot 构建完整 symbol_analysis（snapshot 含 structure/close 等，与 payload 一致）
            try:
                from analysis.conclusions.technical_analyzer import build_symbol_analysis
                tf4h = get_tf_snapshot(symbol, "4h")
                tf1h = get_tf_snapshot(symbol, "1h")
                tf15m = get_tf_snapshot(symbol, "15m")
                if tf4h or tf1h or tf15m:
                    sentiment = None
                    if fr is not None or (oi is not None and isinstance(oi, dict)):
                        sentiment = {"funding_rate": fr, "oi_change_24h_pct": oi.get("change_pct") if isinstance(oi, dict) else oi}
                    sa = build_symbol_analysis(symbol, tf4h or {}, tf1h or {}, tf15m or {}, sentiment=sentiment)
                    market_data["symbol_analysis"] = sa.to_dict()
            except Exception:
                pass
        else:
            # 未监控且无 payload：有持仓/挂单的币种，仅 Redis 有则用
            if not payload or not payload.get("ready"):
                logger.warning(f"No market data available for {symbol}")
                continue
            tf15m_snapshot = get_tf_snapshot(symbol, "15m")
            kline_close = tf15m_snapshot.get("close") if tf15m_snapshot else None
            ai_enh = _round_floats_recursive(payload.get("ai_enhancement") or {}, precision=6)
            market_data = {
                "price": kline_close,
                "24h_change_pct": p24.get("priceChangePercent") if p24 else None,
                "funding_rate": fr if fr is not None else None,
                "high_24h": p24.get("highPrice") if p24 else None,
                "low_24h": p24.get("lowPrice") if p24 else None,
                "volume_24h_usdt": p24.get("quoteVolume") if p24 else None,
                "timeframes": {},
                "ai_enhancement": ai_enh,
                "referee_context": payload.get("referee_context", {}),
                "symbol_analysis": payload.get("symbol_analysis"),
            }
            for tf in ["4h", "1h", "15m"]:
                tf_snapshot = get_tf_snapshot(symbol, tf)
                if tf_snapshot:
                    filtered = {k: v for k, v in tf_snapshot.items() if k not in ("symbol", "tf", "timestamp")}
                    market_data["timeframes"][tf] = {"indicators": _round_floats_recursive(filtered, precision=6)}

        # Phase 2: 统一规范化 — 提升 ai_enhancement、补全 referee_context、生成 _quick
        market_data = _normalize_market_data_for_feed(market_data, symbol)
        output["markets"][symbol] = market_data
        
        # 每处理10个币种让出一次事件循环，避免阻塞前端API
        symbol_count += 1
        if symbol_count % 10 == 0:
            await asyncio.sleep(0)

    # ================== 为持仓添加市场偏向信息 ==================
    # 在 markets 构建完成后，回填 positions 的 market_bias 和 alignment
    for pos in output["positions"]:
        symbol = pos.get("symbol")
        side = pos.get("side")
        market_data = output["markets"].get(symbol, {})
        bias_info = _extract_bias_from_market(market_data)
        
        if bias_info:
            bias_score = bias_info.get("bias_score")
            bias_strength = bias_info.get("bias_strength", "unknown")
            pos["market_bias"] = bias_info.get("bias")
            pos["market_bias_score"] = bias_score
            pos["market_bias_strength"] = bias_strength  # P8 Fix: 添加偏向强度
            
            # 判断 alignment（阈值适配 -10~+10 评分范围）
            if bias_score is not None:
                if side == "LONG":
                    if bias_score >= 4:
                        pos["alignment"] = "ALIGNED"
                    elif bias_score <= -4:
                        pos["alignment"] = "CONFLICT"
                    else:
                        pos["alignment"] = "NEUTRAL"
                elif side == "SHORT":
                    if bias_score <= -4:
                        pos["alignment"] = "ALIGNED"
                    elif bias_score >= 4:
                        pos["alignment"] = "CONFLICT"
                    else:
                        pos["alignment"] = "NEUTRAL"
        
        # P9 Fix: 为持仓币种提升 importance（+1），因为需要优先关注已持仓的决策
        if symbol in output["markets"] and "_quick" in output["markets"][symbol]:
            quick = output["markets"][symbol]["_quick"]
            current_importance = quick.get("importance", 3)
            # 持仓币种 +1，但最高不超过5
            new_importance = min(5, current_importance + 1)
            quick["importance"] = new_importance
            # P9 Fix: 同步更新 importance_label
            importance_labels = {1: "low", 2: "below_avg", 3: "medium", 4: "above_avg", 5: "high"}
            quick["importance_label"] = importance_labels.get(new_importance, "medium")
            quick["has_position"] = True  # 标记有持仓

    # P10 Fix: 为有挂单的币种标记 has_open_order
    for order in output["open_limit_orders"]:
        symbol = order.get("symbol")
        if symbol in output["markets"] and "_quick" in output["markets"][symbol]:
            output["markets"][symbol]["_quick"]["has_open_order"] = True

    # ================== 构建分层投喂结构 ==================
    # 将 ~213KB 的扁平数据压缩为 ~10KB 的分层决策结构
    from llm.context_builder import build_layered_feed

    # 从 global_context 中提取 decision_feedback（在外层已注入）
    decision_feedback = filtered_global_context.get("decision_feedback")

    # 构建用户风控参数（从 UserConfig 传入，替代硬编码值）
    user_limits = {
        "position_size_pct": ctx.config.position_size_pct,      # 单仓最大倍数 (x倍余额，如 3.0 = 单仓 ≤ 3× balance)
        "max_daily_loss_pct": 30.0,                              # 日亏损上限（暂无用户配置项，保留默认）
        "max_leverage": ctx.config.default_leverage,             # 最大杠杆
        "min_rr_ratio": ctx.config.min_rr_ratio,                # 最小风险回报比 (W13 fix)
        "limit_order_min_distance_pct": ctx.config.limit_order_min_distance_pct,  # 限价单最小距离 (P2 fix)
    }

    # Phase 4: 始终传 symbol_analyses（可能部分；build_layered_feed 仅用 v2）
    symbol_analyses = {
        sym: m["symbol_analysis"] for sym, m in output["markets"].items()
        if m.get("symbol_analysis") is not None
    }

    layered = build_layered_feed(
        global_context=filtered_global_context,
        positions=output["positions"],
        pending_orders=output["open_limit_orders"],
        markets=output["markets"],
        balance_info=balance_info_clean,
        decision_feedback=decision_feedback,
        context_memory=filtered_global_context.get("_context_memory"),
        user_limits=user_limits,
        symbol_analyses=symbol_analyses,
    )

    # 重新构建 output，原始数据结构
    # 设计哲学：投喂原始/一阶数据，让 LLM 推理
    # Python 3.7+ dict 保持插入顺序
    # 完整 8 模块结构：constraints, time, market, memory, positions, pending_orders, symbols, quick_checks
    # 注: candle_intelligence 已嵌入每个 symbol 下 (symbols[symbol]['candle_intelligence'])
    ordered_output = {
        # ---- 用户定义的约束（规则，非计算结果）----
        "constraints": layered.get("constraints", {}),
        # ---- 时间上下文 ----
        "time": layered.get("time", {}),
        # ---- 市场状态（原始）----
        "market": layered.get("market", {}),
        # ---- 交易记忆/历史 ----
        "memory": layered.get("memory", {}),
        # ---- 当前持仓 ----
        "positions": layered.get("positions", []),
        # ---- 挂单 ----
        "pending_orders": layered.get("pending_orders", []),
        # ---- 每币种原始技术数据（含 candle_intelligence）----
        "symbols": layered.get("symbols", {}),
        # ---- 快速异常检测 (Phase 2 新增) ----
        "quick_checks": layered.get("quick_checks", {}),
    }

    return ordered_output


async def push_batch_to_ai_for_user(
        ctx: 'UserContext',
        strategy_id: Optional[str] = None,
        symbols: Optional[List[str]] = None,
        target_exchanges: Optional[List[str]] = None,
        multi_trader: Optional['MultiExchangeTrader'] = None
) -> List[Dict[str, Any]]:
    """
    投喂 AI（多用户版本，支持按策略分组投喂）

    使用用户自定义的 LLM 配置和 System Prompt

    Args:
        ctx: 用户上下文
        strategy_id: AI 策略 ID（可选）。如果指定，则使用该策略的配置。
        symbols: 要投喂的币种列表（可选）。如果不指定，使用用户所有监控币种。
        target_exchanges: 目标交易所列表（可选）。用于历史记录标记。
        multi_trader: 已初始化的多交易所交易器（用于获取挂单）
    """
    from llm.llm_client import call_llm_for_user
    from core.user_db import config_loader

    uid = ctx.uid
    # V5-06 fix: 使用 copy.deepcopy 替代 v.copy()（与 E7 fix 对 global cache 的处理一致）
    # P5 fix 注释说 "deeper copy" 但实际仍是浅拷贝 — 嵌套的 indicators dict 是共享引用。
    # 下游 _build_minimal_quick 等会原地修改 market_data，污染 ctx.batch_cache。
    dataset = copy.deepcopy(ctx.batch_cache)
    # 注意：不清空 batch_cache，因为可能有多次投喂调用
    # ctx.batch_cache.clear()

    if not dataset:
        logger.warning(f"[{uid}] batch_cache is empty, cannot feed")
        return []

    # 确定要处理的币种
    if symbols:
        # 使用指定的币种列表
        feed_symbols = set(symbols)
    else:
        # 使用用户所有监控的币种
        feed_symbols = set(ctx.get_monitor_symbols())

    # 过滤 dataset
    if feed_symbols:
        dataset = {k: v for k, v in dataset.items() if k in feed_symbols}

    if not dataset:
        logger.warning(f"[{uid}] no eligible symbols")
        return []

    # 获取策略信息（用于历史记录，使用缓存）
    strategy_info = None
    if strategy_id:
        from core.strategy_cache import get_strategy_cache
        strategy_info = get_strategy_cache().get_strategy(uid, strategy_id)

    # 获取实时持仓数据（用于 global_context 和 feed_json，保持一致）
    realtime_positions = _get_positions_for_exchanges(uid, target_exchanges)

    # 获取实时挂单数据
    realtime_orders = []
    if multi_trader:
        try:
            realtime_orders = await _get_open_limit_orders_for_exchanges(multi_trader, target_exchanges)
        except Exception as e:
            logger.warning(f"获取挂单失败: {e}")

    # 收集需要投喂的币种 = 监控列表 ∪ 持仓币种 ∪ 挂单币种
    # 即使用户没有监控某币种，如果有持仓或挂单，也应该投喂
    position_symbols: set[str] = {str(p.get("symbol")) for p in realtime_positions if p.get("symbol")}
    order_symbols: set[str] = {str(o.get("symbol")) for o in realtime_orders if o.get("symbol")}

    # 合并所有需要投喂的币种
    all_symbols: set[str] = set(dataset.keys()) | position_symbols | order_symbols
    logger.info(
        f"[{uid}] preparing to feed {len(all_symbols)} symbols (monitor:{len(dataset)}, pos:{len(position_symbols)}, orders:{len(order_symbols)})")
    
    # 预加载时包含 BASE_SYMBOLS（用于 market_regime 计算），但不加入投喂列表
    symbols_to_preload = all_symbols | BASE_SYMBOLS

    # 预加载 API 数据（funding rate, 24h change, OI）
    # 使用 symbols_to_preload 而非 all_symbols，确保 BASE_SYMBOLS 的 24h 数据也被加载
    preloaded = await preload_all_api(list(symbols_to_preload))

    # 构建全局上下文（包含 funding_rate 用于情绪分析）
    all_symbols_data: Dict[str, Any] = {}

    # 1. 添加用户监控的币种（从 batch_cache 获取完整指标）
    for symbol, cycles in dataset.items():
        if cycles and "15m" in cycles:
            indicators = cycles.get("15m", {}).get("indicators", {})
            if indicators:
                # 合并 indicators 和 preloaded 数据（funding_rate, open_interest 等）
                symbol_data = indicators.copy()
                # preloaded 结构: {"funding": {symbol: rate}, "p24": {symbol: data}, "oi": {symbol: value}}
                symbol_data["funding_rate"] = preloaded.get("funding", {}).get(symbol)
                symbol_data["open_interest"] = preloaded.get("oi", {}).get(symbol)
                p24_data = preloaded.get("p24", {}).get(symbol)
                if p24_data:
                    symbol_data["24h_change_pct"] = p24_data.get("priceChangePercent")
                all_symbols_data[symbol] = symbol_data

    # 2. 添加持仓/挂单中但未监控的币种（从 Redis payload 获取）
    # BTC/ETH 等全局币种的 K 线是默认下载的，所以可以从 Redis 获取
    from analysis.assembly.payload_builder import get_ai_ready_payload
    extra_symbols = (position_symbols | order_symbols) - set(dataset.keys())
    for symbol in extra_symbols:
        if symbol not in all_symbols_data:
            # 尝试从 Redis 获取完整 payload
            payload = get_ai_ready_payload(symbol)
            if payload and payload.get("ready"):
                all_symbols_data[symbol] = payload
                logger.info(f"[{uid}] Added {symbol} from Redis payload (has position/order but not monitored)")
            else:
                # 如果没有 payload，至少添加基础信息
                logger.warning(f"[{uid}] {symbol} has position/order but no payload available")

    # 3. 确保全局市场指标币种在 all_symbols_data 中（用于 market_regime 计算）
    #    这些币种用于计算全局市场情绪，确保所有用户看到一致的 market_bias 和 risk_appetite
    for base_symbol in BASE_SYMBOLS:
        if base_symbol not in all_symbols_data:
            base_payload = get_ai_ready_payload(base_symbol)
            if base_payload and base_payload.get("ready"):
                # 补充 24h_change_pct（Redis payload 不包含此字段，需要从 preloaded 获取）
                p24_data = preloaded.get("p24", {}).get(base_symbol)
                if p24_data:
                    base_payload["24h_change_pct"] = p24_data.get("priceChangePercent")
                all_symbols_data[base_symbol] = base_payload
                logger.debug(f"[{uid}] Added {base_symbol} from Redis payload (for market_regime)")

    # 获取账户余额信息（优先从 Redis，如果没有则实时 API 获取）
    balance_info = _get_balance_info_for_exchanges(uid, target_exchanges)
    
    # Fallback: 如果有缺失的交易所数据，尝试实时获取补充
    # 修复: 只要有缺失的交易所就尝试补充，而不是要求 balance=0
    if balance_info.get("_missing_exchanges") and multi_trader:
        logger.info(f"[{uid}] 部分交易所无缓存数据 ({balance_info.get('_missing_exchanges')})，尝试实时 API 获取...")
        try:
            realtime_balance = await _fetch_balance_realtime_async(multi_trader, balance_info.get("_missing_exchanges"))
            if realtime_balance.get("balance", 0) > 0:
                # 合并实时获取的数据
                balance_info["balance"] = round(balance_info.get("balance", 0) + realtime_balance.get("balance", 0), 2)
                balance_info["available"] = round(balance_info.get("available", 0) + realtime_balance.get("available", 0), 2)
                balance_info["exchanges"] = balance_info.get("exchanges", []) + realtime_balance.get("exchanges", [])
                balance_info["_has_realtime_supplement"] = True
                logger.info(f"[{uid}] 实时补充余额成功，当前总余额: {balance_info.get('balance')} USDT")
        except Exception as e:
            logger.warning(f"[{uid}] 实时获取余额失败: {e}")

    global_context = build_global_context(
        symbols_data=all_symbols_data,
        positions=realtime_positions,  # 使用实时持仓
        open_orders=realtime_orders,  # 使用实时挂单
        balance=float(balance_info.get("balance", 0)),  # 注意: key 是 "balance" 不是 "total_balance"
        total_unrealized=float(balance_info.get("total_unrealized", 0)),
        # 注意：删除了 symbol_context（与 markets.ai_enhancement 重复），不再需要 user_symbols
        max_leverage=float(ctx.config.default_leverage),
    )

    # 决策反馈：使用当前用户自己的数据和目标交易所
    # 每个交易所独立投喂，所以 target_exchanges 通常只有一个元素
    # 使用第一个交易所的反馈数据，确保 AI 只看到对应交易所的历史
    feedback_exchange = target_exchanges[0] if target_exchanges else None
    if not feedback_exchange:
        # 获取用户启用的交易所列表
        enabled_exchanges = config_loader.get_enabled_exchanges(uid)
        feedback_exchange = enabled_exchanges[0] if enabled_exchanges else "binance"
    
    decision_feedback = get_feedback_for_payload(
        uid=uid,  # 使用当前用户
        exchange=feedback_exchange  # 使用目标交易所（单个）
    )
    global_context["decision_feedback"] = decision_feedback

    # 历史情境匹配：从 ai_decisions 表查找相似的历史场景
    from analysis.context.decision_feedback import find_similar_situations
    try:
        context_memory = find_similar_situations(
            uid=uid,
            current_context=global_context,
            exchange=feedback_exchange,
            lookback_days=30,
            top_n=3,
        )
        if context_memory:
            global_context["_context_memory"] = context_memory
    except Exception as e:
        logger.debug(f"[{uid}] context_memory failed: {e}")

    # 构建投喂数据（传入预先获取的持仓和挂单，避免重复获取）
    # M10 fix: also pass balance_info to avoid double fetch
    # all_symbols 是 set，需要转换为 list
    all_symbols_list = [s for s in all_symbols if s]  # 过滤 None
    feed_json = await _build_feed_json_for_user(
        ctx, dataset, preloaded, global_context, all_symbols_list, target_exchanges, multi_trader,
        preloaded_positions=realtime_positions, preloaded_orders=realtime_orders,
        preloaded_balance_info=balance_info,
    )

    # 添加策略和交易所标记到投喂数据的 _meta 中
    # 注意：合并而不是覆盖，保留 _build_feed_json_for_user 中构建的 timestamp, uid, balance_info
    # execution_constraints 已合并到顶层 constraints 字段
    # _meta 已移除，symbols 列表在 symbols 对象的 keys 中

    # 使用异步版本构建 prompt，避免阻塞事件循环
    user_message = await build_llm_user_prompt_async(feed_json)
    
    # 获取 system_prompt 用于保存历史
    system_prompt = ctx.get_system_prompt(strategy_id=strategy_id)

    # 使用用户的 LLM 客户端调用（可使用策略特定的配置）
    try:
        signals, response = await call_llm_for_user(ctx, user_message, strategy_id=strategy_id)

        strategy_name = strategy_info.get("name") if strategy_info else "全局配置"
        exchanges_str = ",".join(target_exchanges) if target_exchanges else "all"
        logger.info(f"[{uid}] [{strategy_name}] [{exchanges_str}] LLM returned {len(signals)} signals | "
                    f"tokens={response.usage.get('total_tokens', 0) if response.usage else 0}")

        # 保存历史（使用新的带标签结构）
        usage_data: Dict[str, Any] = dict(response.usage) if response.usage else {}
        if "model" not in usage_data and response.model:
            usage_data["model"] = response.model

        response_data = {
            "timestamp": time.time(),
            "signals": signals,
            "content": response.content,
            "usage": usage_data,
            "finish_reason": response.finish_reason,
            "response_time_ms": response.response_time_ms,
            "http_status": response.http_status,
            # 新增标签字段
            "strategy_id": strategy_id,
            "strategy_name": strategy_name,
            "target_exchanges": target_exchanges or [],
        }

        # 在请求数据中也添加标签（方便前端展示）
        # P2 Fix: 移除重复字段 - 这些字段已经在 feed_json["_meta"] 中存在
        # 保留 feed_json 的 copy 以避免修改原始数据，但不再重复添加已在 _meta 中的字段
        # Note: strategy_id, strategy_name, target_exchanges, symbols 已在 _meta 中
        
        # 构建请求数据：上文（system_prompt）在前，下文（feed_json）在后
        # 将 system_prompt 按分隔符拆分成数组，便于数据库查看
        system_prompt_parts = system_prompt.split("\n\n---\n\n") if system_prompt else []
        request_data = {
            "_system_prompt": system_prompt_parts,
            **feed_json
        }

        RedisDataManager.add_ai_history(uid, request_data, response_data)

        return signals

    except Exception as e:
        logger.error(f"[{uid}] LLM call failed: {e}")

        strategy_name = strategy_info.get("name") if strategy_info else "全局配置"

        # 提取详细的 API 错误信息
        http_status = 0
        error_code = ""
        error_data = {}
        provider = ""
        model = ""

        if isinstance(e, LLMAPIError):
            # 使用自定义异常中的详细信息
            http_status = e.http_status
            error_code = e.error_code
            error_data = e.error_data
            provider = e.provider
            model = e.model

        error_response_data = {
            "timestamp": time.time(),
            "signals": [],
            "content": None,
            "error": True,  # 标记这是一个错误记录
            "error_message": str(e),
            "error_type": type(e).__name__,
            # API 级别的错误详情
            "http_status": http_status,
            "error_code": error_code,  # API 错误码 (如 rate_limit_exceeded, invalid_api_key)
            "error_data": error_data,  # API 返回的原始错误 JSON
            "provider": provider,  # LLM 提供商
            "model": model,  # 使用的模型
            # 其他字段
            "usage": None,
            "finish_reason": "error",
            "response_time_ms": 0,
            "strategy_id": strategy_id,
            "strategy_name": strategy_name,
            "target_exchanges": target_exchanges or [],
        }

        # 构建简化的请求数据（避免存储过大的投喂数据）
        error_request_data = {
            "strategy_id": strategy_id,
            "strategy_name": strategy_name,
            "target_exchanges": target_exchanges or [],
            "symbols": list(all_symbols) if all_symbols else [],
            "error_occurred": True,  # 标记
        }

        try:
            RedisDataManager.add_ai_history(uid, error_request_data, error_response_data)
            logger.info(f"[{uid}] LLM error saved to ai_history: http_status={http_status}, error_code={error_code}")
        except Exception as save_err:
            logger.warning(f"[{uid}] Failed to save LLM error to ai_history: {save_err}")

        # 发送 Telegram 通知
        try:
            from notifications.notifier import send_telegram_message_for_user

            # 构建用户友好的错误消息
            error_details = []
            if provider:
                error_details.append(f"Provider: {provider}")
            if model:
                error_details.append(f"Model: {model}")
            if http_status:
                error_details.append(f"HTTP: {http_status}")
            if error_code:
                error_details.append(f"Code: {error_code}")

            details_str = " | ".join(error_details) if error_details else "Unknown error"

            tg_message = (
                f"⚠️ LLM 调用失败\n"
                f"策略: {strategy_name}\n"
                f"错误: {str(e)[:200]}\n"  # 截断过长的错误消息
                f"详情: {details_str}"
            )

            send_telegram_message_for_user(ctx, tg_message)
            logger.info(f"[{uid}] LLM error notification sent to Telegram")
        except Exception as tg_err:
            logger.warning(f"[{uid}] Failed to send LLM error to Telegram: {tg_err}")

        return []


# ================== AsyncUserContext 版本（用户隔离） ==================

async def push_batch_to_ai_for_user_async(
        ctx: 'AsyncUserContext',
        strategy_id: Optional[str] = None,
        symbols: Optional[List[str]] = None,
        target_exchanges: Optional[List[str]] = None,
        multi_trader: Optional['MultiExchangeTrader'] = None
) -> Tuple[List[Dict[str, Any]], Optional[int]]:
    """
    投喂 AI（AsyncUserContext 版本，支持用户隔离）
    
    与 push_batch_to_ai_for_user 的区别：
    - 使用 AsyncUserContext 而不是 UserContext
    - 从用户独立的 batch_cache 获取数据（不共享全局）
    - 完全异步操作
    
    Args:
        ctx: 异步用户上下文（AsyncUserContext）
        strategy_id: AI 策略 ID（可选）
        symbols: 要投喂的币种列表（可选）
        target_exchanges: 目标交易所列表（可选）
        multi_trader: 已初始化的多交易所交易器（用于获取挂单）
    
    Returns:
        (signals, decision_id): AI 返回的信号列表和决策记录 ID
    """
    from llm.llm_client import call_llm_for_user
    from core.user_db import config_loader
    from core.async_helpers import run_sync
    
    uid = ctx.uid
    
    # 从用户独立的 batch_cache 获取数据
    dataset = await ctx.get_batch_cache()
    
    if not dataset:
        logger.warning(f"[{uid}] async batch_cache is empty, cannot feed")
        return [], None
    
    # 确定要处理的币种
    if symbols:
        feed_symbols = set(symbols)
    else:
        feed_symbols = set(await ctx.get_monitor_symbols())
    
    # 过滤 dataset
    if feed_symbols:
        dataset = {k: v for k, v in dataset.items() if k in feed_symbols}
    
    if not dataset:
        logger.warning(f"[{uid}] no eligible symbols in async mode")
        return [], None
    
    # 获取策略信息（使用缓存）
    strategy_info = None
    if strategy_id:
        from core.strategy_cache import get_strategy_cache
        strategy_info = get_strategy_cache().get_strategy(uid, strategy_id)
    
    # 获取实时持仓数据
    realtime_positions = await run_sync(lambda: _get_positions_for_exchanges(uid, target_exchanges))
    
    # 获取实时挂单数据
    realtime_orders = []
    if multi_trader:
        try:
            realtime_orders = await _get_open_limit_orders_for_exchanges(multi_trader, target_exchanges)
        except Exception as e:
            logger.warning(f"获取挂单失败: {e}")
    
    # 收集需要投喂的币种
    position_symbols: set[str] = {str(p.get("symbol")) for p in realtime_positions if p.get("symbol")}
    order_symbols: set[str] = {str(o.get("symbol")) for o in realtime_orders if o.get("symbol")}
    
    all_symbols: set[str] = set(dataset.keys()) | position_symbols | order_symbols
    
    logger.info(
        f"[{uid}] async preparing to feed {len(all_symbols)} symbols "
        f"(monitor:{len(dataset)}, pos:{len(position_symbols)}, orders:{len(order_symbols)})"
    )
    
    # 预加载时包含 BASE_SYMBOLS（用于 market_regime 计算），但不加入投喂列表
    symbols_to_preload = all_symbols | BASE_SYMBOLS
    
    # 预加载 API 数据
    preloaded = await preload_all_api(list(symbols_to_preload))
    
    # 构建全局上下文
    all_symbols_data: Dict[str, Any] = {}
    
    for symbol, cycles in dataset.items():
        if cycles and "15m" in cycles:
            indicators = cycles.get("15m", {}).get("indicators", {})
            if indicators:
                symbol_data = indicators.copy()
                symbol_data["funding_rate"] = preloaded.get("funding", {}).get(symbol)
                symbol_data["open_interest"] = preloaded.get("oi", {}).get(symbol)
                p24_data = preloaded.get("p24", {}).get(symbol)
                if p24_data:
                    symbol_data["24h_change_pct"] = p24_data.get("priceChangePercent")
                    symbol_data["high_24h"] = p24_data.get("highPrice")
                    symbol_data["low_24h"] = p24_data.get("lowPrice")
                all_symbols_data[symbol] = symbol_data
    
    # 添加持仓/挂单中但未监控的币种
    from analysis.assembly.payload_builder import get_ai_ready_payload
    extra_symbols = (position_symbols | order_symbols) - set(dataset.keys())
    for symbol in extra_symbols:
        if symbol not in all_symbols_data:
            payload = await run_sync(lambda s=symbol: get_ai_ready_payload(s))
            if payload and payload.get("ready"):
                all_symbols_data[symbol] = payload
    
    # 确保全局市场指标币种在数据中（用于 market_regime 计算）
    for base_symbol in BASE_SYMBOLS:
        if base_symbol not in all_symbols_data:
            base_payload = await run_sync(lambda s=base_symbol: get_ai_ready_payload(s))
            if base_payload and base_payload.get("ready"):
                # 补充 24h_change_pct（Redis payload 不包含此字段，需要从 preloaded 获取）
                p24_data = preloaded.get("p24", {}).get(base_symbol)
                if p24_data:
                    base_payload["24h_change_pct"] = p24_data.get("priceChangePercent")
                all_symbols_data[base_symbol] = base_payload
    
    # 获取账户余额信息
    balance_info = await run_sync(lambda: _get_balance_info_for_exchanges(uid, target_exchanges))
    
    # N5 fix: async version was missing balance fallback for missing exchanges
    # Same logic as sync version (lines 1629-1641)
    if balance_info.get("_missing_exchanges") and multi_trader:
        logger.info(f"[{uid}] [async] 部分交易所无缓存数据 ({balance_info.get('_missing_exchanges')})，尝试实时 API 获取...")
        try:
            realtime_balance = await _fetch_balance_realtime_async(multi_trader, balance_info.get("_missing_exchanges"))
            if realtime_balance.get("balance", 0) > 0:
                balance_info["balance"] = round(balance_info.get("balance", 0) + realtime_balance.get("balance", 0), 2)
                balance_info["available"] = round(balance_info.get("available", 0) + realtime_balance.get("available", 0), 2)
                balance_info["exchanges"] = balance_info.get("exchanges", []) + realtime_balance.get("exchanges", [])
                balance_info["_has_realtime_supplement"] = True
                logger.info(f"[{uid}] [async] 实时补充余额成功，当前总余额: {balance_info.get('balance')} USDT")
        except Exception as e:
            logger.warning(f"[{uid}] [async] 实时获取余额失败: {e}")
    
    global_context = build_global_context(
        symbols_data=all_symbols_data,
        positions=realtime_positions,
        open_orders=realtime_orders,
        balance=float(balance_info.get("balance", 0)),
        total_unrealized=float(balance_info.get("total_unrealized", 0)),
        max_leverage=float(ctx.config.default_leverage),
    )
    
    # 决策反馈：使用当前用户自己的数据和目标交易所
    # 每个交易所独立投喂，所以 target_exchanges 通常只有一个元素
    # 使用第一个交易所的反馈数据，确保 AI 只看到对应交易所的历史
    feedback_exchange = target_exchanges[0] if target_exchanges else None
    if not feedback_exchange:
        enabled_exchanges = config_loader.get_enabled_exchanges(uid)
        feedback_exchange = enabled_exchanges[0] if enabled_exchanges else "binance"
    
    decision_feedback = await run_sync(lambda: get_feedback_for_payload(
        uid=uid,  # 使用当前用户
        exchange=feedback_exchange  # 使用目标交易所（单个）
    ))
    global_context["decision_feedback"] = decision_feedback

    # 历史情境匹配
    from analysis.context.decision_feedback import find_similar_situations
    try:
        context_memory = await run_sync(lambda: find_similar_situations(
            uid=uid,
            current_context=global_context,
            exchange=feedback_exchange,
            lookback_days=30,
            top_n=3,
        ))
        if context_memory:
            global_context["_context_memory"] = context_memory
    except Exception as e:
        logger.debug(f"[{uid}] async context_memory failed: {e}")

    # 需要使用同步上下文来构建 feed_json
    sync_ctx = ctx.sync_ctx
    if not sync_ctx:
        logger.error(f"[{uid}] sync_ctx not available for async feed")
        return [], None
    
    # 构建投喂数据
    # M10 fix: pass balance_info to avoid double fetch
    all_symbols_list = [s for s in all_symbols if s]
    feed_json = await _build_feed_json_for_user(
        sync_ctx, dataset, preloaded, global_context, all_symbols_list, target_exchanges, multi_trader,
        preloaded_positions=realtime_positions, preloaded_orders=realtime_orders,
        preloaded_balance_info=balance_info,
    )
    
    # _meta 已移除，symbols 列表在 symbols 对象的 keys 中
    
    # 使用异步版本构建 prompt，避免阻塞事件循环
    user_message = await build_llm_user_prompt_async(feed_json)
    
    # 获取 system_prompt 用于保存历史
    system_prompt = sync_ctx.get_system_prompt(strategy_id=strategy_id)
    
    # 调用 LLM（使用同步上下文）
    try:
        signals, response = await call_llm_for_user(sync_ctx, user_message, strategy_id=strategy_id)
        
        strategy_name = strategy_info.get("name") if strategy_info else "全局配置"
        exchanges_str = ",".join(target_exchanges) if target_exchanges else "all"
        logger.info(
            f"[{uid}] [async] [{strategy_name}] [{exchanges_str}] LLM returned {len(signals)} signals | "
            f"tokens={response.usage.get('total_tokens', 0) if response.usage else 0}"
        )
        
        # 保存历史
        usage_data: Dict[str, Any] = dict(response.usage) if response.usage else {}
        if "model" not in usage_data and response.model:
            usage_data["model"] = response.model
        
        response_data = {
            "timestamp": time.time(),
            "signals": signals,
            "content": response.content,
            "usage": usage_data,
            "finish_reason": response.finish_reason,
            "response_time_ms": response.response_time_ms,
            "http_status": response.http_status,
            "strategy_id": strategy_id,
            "strategy_name": strategy_name,
            "target_exchanges": target_exchanges or [],
        }
        
        # P2 Fix: 不再在根级别添加重复字段，这些字段已在 _meta 中
        # 构建请求数据：上文（system_prompt）在前，下文（feed_json）在后
        # 将 system_prompt 按分隔符拆分成数组，便于数据库查看
        system_prompt_parts = system_prompt.split("\n\n---\n\n") if system_prompt else []
        request_data = {
            "_system_prompt": system_prompt_parts,
            **feed_json
        }
        
        decision_id = await run_sync(lambda: RedisDataManager.add_ai_history(uid, request_data, response_data))
        
        return signals, decision_id
        
    except Exception as e:
        logger.error(f"[{uid}] [async] LLM call failed: {e}")
        
        strategy_name = strategy_info.get("name") if strategy_info else "全局配置"
        
        # 提取错误信息
        http_status = 0
        error_code = ""
        error_data = {}
        provider = ""
        model = ""
        
        if isinstance(e, LLMAPIError):
            http_status = e.http_status
            error_code = e.error_code
            error_data = e.error_data
            provider = e.provider
            model = e.model
        
        error_response_data = {
            "timestamp": time.time(),
            "signals": [],
            "content": None,
            "error": True,
            "error_message": str(e),
            "error_type": type(e).__name__,
            "http_status": http_status,
            "error_code": error_code,
            "error_data": error_data,
            "provider": provider,
            "model": model,
            "usage": None,
            "finish_reason": "error",
            "response_time_ms": 0,
            "strategy_id": strategy_id,
            "strategy_name": strategy_name,
            "target_exchanges": target_exchanges or [],
        }
        
        error_request_data = {
            "strategy_id": strategy_id,
            "strategy_name": strategy_name,
            "target_exchanges": target_exchanges or [],
            "symbols": list(all_symbols) if all_symbols else [],
            "error_occurred": True,
        }
        
        try:
            await run_sync(lambda: RedisDataManager.add_ai_history(uid, error_request_data, error_response_data))
        except Exception as save_err:
            logger.warning(f"[{uid}] Failed to save LLM error: {save_err}")
        
        # N25 fix: async version was missing Telegram notification (same as sync version)
        try:
            from notifications.notifier import send_telegram_message_for_user

            error_details = []
            if provider:
                error_details.append(f"Provider: {provider}")
            if model:
                error_details.append(f"Model: {model}")
            if http_status:
                error_details.append(f"HTTP: {http_status}")
            if error_code:
                error_details.append(f"Code: {error_code}")

            details_str = " | ".join(error_details) if error_details else "Unknown error"

            tg_message = (
                f"⚠️ LLM 调用失败\n"
                f"策略: {strategy_name}\n"
                f"错误: {str(e)[:200]}\n"
                f"详情: {details_str}"
            )

            await run_sync(lambda: send_telegram_message_for_user(sync_ctx, tg_message))
            logger.info(f"[{uid}] [async] LLM error notification sent to Telegram")
        except Exception as tg_err:
            logger.warning(f"[{uid}] [async] Failed to send LLM error to Telegram: {tg_err}")
        
        return [], None
