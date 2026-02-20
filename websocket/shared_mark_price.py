# websocket/shared_mark_price.py
"""
多交易所共享 MarkPrice WebSocket 流

设计：
- 每个交易所一个共享流单例（SharedOKXMarkPrice, SharedBitgetMarkPrice, SharedHyperliquidMarkPrice）
- 所有用户共享同一个 WebSocket 连接
- 动态订阅：symbols 变化时自动更新
- 多用户回调：每个用户注册自己的 on_tick 回调

优势：
- 100 个用户只需要每个交易所 1 个 WebSocket 连接
- symbols 自动合并去重
- Redis 写入去重
"""

import asyncio
import json
import logging
import random
import threading
import time
from abc import ABC, abstractmethod
from decimal import Decimal
from typing import Callable, Dict, Optional, Set

logger = logging.getLogger(__name__)


def now_ms() -> int:
    return int(time.time() * 1000)


# ============================================================
# 基类：共享 MarkPrice 流
# ============================================================

class SharedMarkPriceBase(ABC):
    """
    共享 MarkPrice 流基类
    
    子类需要实现：
    - _get_ws_url() - WebSocket URL
    - _build_subscribe_message(symbols) - 订阅消息
    - _parse_tick(msg) - 解析消息，返回 (symbol, price, timestamp) 或 None
    - _symbol_to_redis_key(symbol) - 转换为 Redis key 的 symbol 格式
    """
    
    # 重连配置
    RECONNECT_MIN_DELAY = 1.0
    RECONNECT_MAX_DELAY = 60.0
    RECONNECT_MULTIPLIER = 2.0
    RECONNECT_JITTER = 0.1
    
    # 心跳间隔
    PING_INTERVAL = 25
    
    # 应用层 ping 间隔（秒），None 表示不需要应用层 ping
    APP_PING_INTERVAL: Optional[float] = None
    
    def __init__(self, is_testnet: bool = False):
        if getattr(self, '_initialized', False):
            return
        
        self._is_testnet = is_testnet
        self.exchange_name = self._get_exchange_name()
        
        # 用户订阅管理
        self._user_symbols: Dict[str, Set[str]] = {}  # uid -> symbols
        self._user_callbacks: Dict[str, Callable] = {}  # uid -> on_tick callback
        self._lock = threading.Lock()
        
        # 当前订阅的所有 symbols（合并后）
        self._current_symbols: Set[str] = set()
        
        # Redis 连接（延迟获取）
        self._redis = None
        self._mark_ttl_s = 15
        
        # WebSocket 状态
        self._running = False
        self._stop_event = threading.Event()
        self._symbols_changed = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._reconnect_delay = self.RECONNECT_MIN_DELAY
        
        self._initialized = True
        logger.info(f"[Shared{self.exchange_name}MarkPrice] 初始化完成")
    
    @abstractmethod
    def _get_exchange_name(self) -> str:
        """返回交易所名称"""
        pass
    
    @abstractmethod
    def _get_ws_url(self) -> str:
        """返回 WebSocket URL"""
        pass
    
    @abstractmethod
    def _build_subscribe_message(self, symbols: Set[str]) -> Optional[dict]:
        """构建订阅消息，返回 None 表示不需要发送订阅消息"""
        pass
    
    @abstractmethod
    def _parse_tick(self, msg: dict) -> Optional[tuple]:
        """解析消息，返回 (symbol, price, timestamp) 或 None"""
        pass
    
    @abstractmethod
    def _symbol_to_redis_key(self, symbol: str) -> str:
        """转换 symbol 为统一的 Redis key 格式（如 BTCUSDT）"""
        pass
    
    def _get_redis(self):
        """延迟获取 Redis 连接 - P2 Fix: 使用共享连接池"""
        if self._redis is None:
            from core.database import redis_client
            self._redis = redis_client
        return self._redis
    
    def register_user(
        self,
        uid: str,
        symbols: Set[str],
        on_tick: Optional[Callable[[str, Decimal, int], None]] = None
    ):
        """注册用户订阅"""
        with self._lock:
            self._user_symbols[uid] = set(symbols)
            if on_tick:
                self._user_callbacks[uid] = on_tick
            
            new_symbols = self._compute_all_symbols()
            if new_symbols != self._current_symbols:
                self._current_symbols = new_symbols
                self._symbols_changed.set()
                logger.info(f"[Shared{self.exchange_name}MarkPrice] 用户 {uid} 注册，symbols 更新为 {len(new_symbols)} 个")
        
        self._ensure_running()
    
    def unregister_user(self, uid: str):
        """取消用户订阅"""
        with self._lock:
            self._user_symbols.pop(uid, None)
            self._user_callbacks.pop(uid, None)
            
            new_symbols = self._compute_all_symbols()
            if new_symbols != self._current_symbols:
                self._current_symbols = new_symbols
                self._symbols_changed.set()
                logger.info(f"[Shared{self.exchange_name}MarkPrice] 用户 {uid} 取消注册")
    
    def update_user_symbols(self, uid: str, symbols: Set[str]):
        """更新用户的订阅 symbols"""
        with self._lock:
            if uid not in self._user_symbols:
                return
            
            self._user_symbols[uid] = set(symbols)
            new_symbols = self._compute_all_symbols()
            
            if new_symbols != self._current_symbols:
                self._current_symbols = new_symbols
                self._symbols_changed.set()
    
    def _compute_all_symbols(self) -> Set[str]:
        """计算所有用户需要的 symbols 并集"""
        result: Set[str] = set()
        for syms in self._user_symbols.values():
            result |= syms
        return result
    
    def _ensure_running(self):
        """确保 WebSocket 线程运行"""
        if self._thread and self._thread.is_alive():
            return
        
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run_thread,
            name=f"shared-{self.exchange_name.lower()}-mark-price",
            daemon=True
        )
        self._thread.start()
        logger.info(f"[Shared{self.exchange_name}MarkPrice] WebSocket 线程已启动")
    
    def stop(self):
        """停止 WebSocket"""
        self._stop_event.set()
        self._symbols_changed.set()
        if self._loop:
            try:
                self._loop.call_soon_threadsafe(lambda: None)
            except Exception as e:
                # P3 Fix: 添加日志
                logger.debug(f"[Shared{self.exchange_name}MarkPrice] 停止时调用异常: {e}")
        logger.info(f"[Shared{self.exchange_name}MarkPrice] 已停止")
    
    def _run_thread(self):
        """WebSocket 线程入口"""
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        try:
            self._loop.run_until_complete(self._main_loop())
        except Exception as e:
            logger.error(f"[Shared{self.exchange_name}MarkPrice] 线程异常: {e}")
        finally:
            try:
                pending = asyncio.all_tasks(self._loop)
                for task in pending:
                    task.cancel()
                if pending:
                    self._loop.run_until_complete(
                        asyncio.gather(*pending, return_exceptions=True)
                    )
            except Exception as e:
                # P3 Fix: 添加日志
                logger.debug(f"[Shared{self.exchange_name}MarkPrice] 清理任务异常: {e}")
            finally:
                self._loop.close()
                self._loop = None
    
    async def _main_loop(self):
        """主循环（支持动态订阅，无需断开重连）"""
        import websockets
        
        while not self._stop_event.is_set():
            with self._lock:
                symbols = self._current_symbols.copy()
            
            # 没有 symbols 则等待
            if not symbols:
                await asyncio.get_event_loop().run_in_executor(
                    None,
                    lambda: self._symbols_changed.wait(timeout=5.0)
                )
                self._symbols_changed.clear()
                continue
            
            url = self._get_ws_url()
            logger.info(f"[Shared{self.exchange_name}MarkPrice] 连接 WebSocket...")
            
            try:
                # 如果需要应用层 ping，禁用 websockets 库的协议层 ping
                ws_ping_interval = None if self.APP_PING_INTERVAL else self.PING_INTERVAL
                
                async with websockets.connect(
                    url,
                    open_timeout=30,  # 增加握手超时时间
                    ping_interval=ws_ping_interval,
                    ping_timeout=20,
                    close_timeout=5,
                    max_queue=4096,
                    max_size=10 * 1024 * 1024,
                ) as ws:
                    self._symbols_changed.clear()
                    self._reconnect_delay = self.RECONNECT_MIN_DELAY
                    
                    # 记录当前已订阅的 symbols
                    subscribed_symbols: Set[str] = set()
                    
                    # 初始订阅
                    with self._lock:
                        to_subscribe = self._current_symbols.copy()
                    
                    if to_subscribe:
                        subscribe_msg = self._build_subscribe_message(to_subscribe)
                        if subscribe_msg:
                            await ws.send(json.dumps(subscribe_msg))
                        subscribed_symbols = to_subscribe.copy()
                        logger.info(f"[Shared{self.exchange_name}MarkPrice] 已订阅 {len(subscribed_symbols)} 个 symbols")
                    
                    # 启动应用层 ping 任务（如果需要）
                    ping_task = None
                    if self.APP_PING_INTERVAL:
                        ping_task = asyncio.create_task(self._app_ping_loop(ws))
                    
                    try:
                        while not self._stop_event.is_set():
                            # 检查 symbols 是否变化（动态增减订阅）
                            if self._symbols_changed.is_set():
                                self._symbols_changed.clear()
                                
                                with self._lock:
                                    new_symbols = self._current_symbols.copy()
                                
                                # 计算差异
                                to_unsub = subscribed_symbols - new_symbols
                                to_sub = new_symbols - subscribed_symbols
                                
                                # 取消订阅
                                if to_unsub:
                                    unsub_msg = self._build_unsubscribe_message(to_unsub)
                                    if unsub_msg:
                                        await ws.send(json.dumps(unsub_msg))
                                        logger.info(f"[Shared{self.exchange_name}MarkPrice] 取消订阅 {len(to_unsub)} 个")
                                
                                # 新增订阅
                                if to_sub:
                                    sub_msg = self._build_subscribe_message(to_sub)
                                    if sub_msg:
                                        await ws.send(json.dumps(sub_msg))
                                        logger.info(f"[Shared{self.exchange_name}MarkPrice] 新增订阅 {len(to_sub)} 个")
                                
                                subscribed_symbols = new_symbols.copy()
                                
                                # 如果没有任何订阅了，断开等待
                                if not subscribed_symbols:
                                    logger.info(f"[Shared{self.exchange_name}MarkPrice] 无订阅，断开连接")
                                    break
                            
                            try:
                                raw = await asyncio.wait_for(ws.recv(), timeout=30.0)
                            except asyncio.TimeoutError:
                                continue
                            
                            if not raw:
                                continue
                            
                            # 处理纯文本 pong 响应（Bitget 等交易所）
                            if raw == "pong":
                                continue
                            
                            try:
                                msg = json.loads(raw)
                                tick = self._parse_tick(msg)
                                if tick:
                                    symbol, price, ts = tick
                                    mark_price = Decimal(str(price))
                                    
                                    # 写入 Redis
                                    self._write_to_redis(symbol, price, ts)
                                    
                                    # 分发回调
                                    self._dispatch_tick(symbol, mark_price, ts)
                                    
                            except json.JSONDecodeError:
                                # 非 JSON 消息，忽略
                                pass
                            except Exception as e:
                                logger.debug(f"[Shared{self.exchange_name}MarkPrice] 消息处理异常: {e}")
                    finally:
                        # 取消应用层 ping 任务
                        if ping_task and not ping_task.done():
                            ping_task.cancel()
                            try:
                                await ping_task
                            except asyncio.CancelledError:
                                pass
                            
            except Exception as e:
                logger.warning(f"[Shared{self.exchange_name}MarkPrice] 连接异常: {e}")
                # 指数退避重连
                jitter = random.uniform(-self.RECONNECT_JITTER, self.RECONNECT_JITTER)
                delay = self._reconnect_delay * (1 + jitter)
                await asyncio.sleep(delay)
                self._reconnect_delay = min(
                    self._reconnect_delay * self.RECONNECT_MULTIPLIER,
                    self.RECONNECT_MAX_DELAY
                )
    
    async def _app_ping_loop(self, ws):
        """
        应用层 ping 保活循环
        
        某些交易所（如 Bitget）需要发送应用层的 ping 消息，而不是 WebSocket 协议层的 ping frame。
        子类通过设置 APP_PING_INTERVAL 和重写 _get_app_ping_message() 来启用。
        """
        if not self.APP_PING_INTERVAL:
            return
        
        ping_interval = self.APP_PING_INTERVAL  # 类型收窄
        try:
            while not self._stop_event.is_set():
                await asyncio.sleep(ping_interval)
                if self._stop_event.is_set():
                    break
                try:
                    ping_msg = self._get_app_ping_message()
                    await ws.send(ping_msg)
                    logger.debug(f"[Shared{self.exchange_name}MarkPrice] 应用层 ping 已发送")
                except Exception as e:
                    logger.debug(f"[Shared{self.exchange_name}MarkPrice] 应用层 ping 发送失败: {e}")
                    break
        except asyncio.CancelledError:
            pass
    
    def _get_app_ping_message(self) -> str:
        """获取应用层 ping 消息，子类可覆盖"""
        return "ping"
    
    def _build_unsubscribe_message(self, symbols: Set[str]) -> Optional[dict]:
        """构建取消订阅消息（子类可覆盖）"""
        # 默认返回 None，表示不支持动态取消订阅
        return None
    
    def _write_to_redis(self, symbol: str, mark_price, ts: int):
        """写入 Redis"""
        try:
            rds = self._get_redis()
            redis_symbol = self._symbol_to_redis_key(symbol)
            from core.database import RedisKeys
            key = RedisKeys.market_prices(redis_symbol)
            payload = {"markPrice": str(mark_price), "ts": str(ts)}
            rds.set(key, json.dumps(payload, separators=(",", ":")))
            if self._mark_ttl_s > 0:
                rds.expire(key, self._mark_ttl_s)
        except Exception as e:
            logger.debug(f"[Shared{self.exchange_name}MarkPrice] Redis 写入失败: {e}")
    
    def _dispatch_tick(self, symbol: str, mark_price: Decimal, ts: int):
        """分发 tick 到订阅该 symbol 的用户"""
        with self._lock:
            for uid, syms in self._user_symbols.items():
                if symbol in syms:
                    callback = self._user_callbacks.get(uid)
                    if callback:
                        try:
                            callback(symbol, mark_price, ts)
                        except Exception as e:
                            logger.debug(f"[Shared{self.exchange_name}MarkPrice] 用户 {uid} 回调异常: {e}")
    
    def get_stats(self) -> dict:
        """获取统计信息"""
        with self._lock:
            return {
                "exchange": self.exchange_name,
                "users": len(self._user_symbols),
                "symbols": len(self._current_symbols),
                "running": self._thread.is_alive() if self._thread else False,
            }


# ============================================================
# OKX 共享 MarkPrice 流
# ============================================================

class SharedOKXMarkPrice(SharedMarkPriceBase):
    """OKX 共享标记价格流"""
    
    def __init__(self, is_testnet: bool = False):
        super().__init__(is_testnet)
    
    def _get_exchange_name(self) -> str:
        return "OKX"
    
    def _get_ws_url(self) -> str:
        if self._is_testnet:
            return "wss://wspap.okx.com:8443/ws/v5/public?brokerId=9999"
        return "wss://ws.okx.com:8443/ws/v5/public"
    
    def _build_subscribe_message(self, symbols: Set[str]) -> Optional[dict]:
        """构建 OKX 订阅消息"""
        args = []
        for sym in symbols:
            # 转换为 OKX instId 格式: BTCUSDT -> BTC-USDT-SWAP
            inst_id = self._to_okx_inst_id(sym)
            args.append({"channel": "mark-price", "instId": inst_id})
        
        return {"op": "subscribe", "args": args}
    
    def _build_unsubscribe_message(self, symbols: Set[str]) -> Optional[dict]:
        """构建 OKX 取消订阅消息"""
        args = []
        for sym in symbols:
            inst_id = self._to_okx_inst_id(sym)
            args.append({"channel": "mark-price", "instId": inst_id})
        
        return {"op": "unsubscribe", "args": args}
    
    def _parse_tick(self, msg: dict) -> Optional[tuple]:
        """解析 OKX 消息"""
        # OKX 格式: {"arg": {...}, "data": [{"instId": "BTC-USDT-SWAP", "markPx": "100000", "ts": "..."}]}
        data = msg.get("data")
        if not data or not isinstance(data, list):
            return None
        
        for item in data:
            inst_id = item.get("instId", "")
            mark_px = item.get("markPx")
            ts = item.get("ts")
            
            if inst_id and mark_px:
                symbol = self._from_okx_inst_id(inst_id)
                timestamp = int(ts) if ts else now_ms()
                return (symbol, mark_px, timestamp)
        
        return None
    
    def _symbol_to_redis_key(self, symbol: str) -> str:
        """转换为 Redis key 格式"""
        # 已经是 BTCUSDT 格式
        return symbol.upper()
    
    def _to_okx_inst_id(self, symbol: str) -> str:
        """BTCUSDT -> BTC-USDT-SWAP"""
        symbol = symbol.upper()
        if symbol.endswith("USDT"):
            base = symbol[:-4]
            return f"{base}-USDT-SWAP"
        return symbol
    
    def _from_okx_inst_id(self, inst_id: str) -> str:
        """BTC-USDT-SWAP -> BTCUSDT"""
        parts = inst_id.split("-")
        if len(parts) >= 2:
            return f"{parts[0]}{parts[1]}"
        return inst_id


# ============================================================
# Bitget 共享 MarkPrice 流
# ============================================================

class SharedBitgetMarkPrice(SharedMarkPriceBase):
    """Bitget 共享标记价格流"""
    
    # Bitget 需要应用层 ping，每 25 秒发送一次 "ping" 文本消息
    APP_PING_INTERVAL = 25.0
    
    def __init__(self, is_testnet: bool = False):
        super().__init__(is_testnet)
    
    def _get_exchange_name(self) -> str:
        return "Bitget"
    
    def _get_ws_url(self) -> str:
        if self._is_testnet:
            return "wss://ws.bitget.com/v2/ws/public"
        return "wss://ws.bitget.com/v2/ws/public"
    
    def _build_subscribe_message(self, symbols: Set[str]) -> Optional[dict]:
        """构建 Bitget 订阅消息"""
        args = []
        for sym in symbols:
            args.append({
                "instType": "USDT-FUTURES",
                "channel": "ticker",
                "instId": sym.upper()
            })
        
        return {"op": "subscribe", "args": args}
    
    def _build_unsubscribe_message(self, symbols: Set[str]) -> Optional[dict]:
        """构建 Bitget 取消订阅消息"""
        args = []
        for sym in symbols:
            args.append({
                "instType": "USDT-FUTURES",
                "channel": "ticker",
                "instId": sym.upper()
            })
        
        return {"op": "unsubscribe", "args": args}
    
    def _parse_tick(self, msg: dict) -> Optional[tuple]:
        """解析 Bitget 消息"""
        # Bitget 格式: {"action": "snapshot", "arg": {...}, "data": [{"markPrice": "...", ...}]}
        action = msg.get("action")
        if action not in ("snapshot", "update"):
            return None
        
        data = msg.get("data")
        if not data or not isinstance(data, list):
            return None
        
        for item in data:
            inst_id = item.get("instId", "")
            mark_price = item.get("markPrice")
            ts = item.get("ts")
            
            if inst_id and mark_price:
                timestamp = int(ts) if ts else now_ms()
                return (inst_id.upper(), mark_price, timestamp)
        
        return None
    
    def _symbol_to_redis_key(self, symbol: str) -> str:
        return symbol.upper()


# ============================================================
# Hyperliquid 共享 MarkPrice 流
# ============================================================

class SharedHyperliquidMarkPrice(SharedMarkPriceBase):
    """Hyperliquid 共享标记价格流"""
    
    def __init__(self, is_testnet: bool = False):
        super().__init__(is_testnet)
    
    def _get_exchange_name(self) -> str:
        return "Hyperliquid"
    
    def _get_ws_url(self) -> str:
        if self._is_testnet:
            return "wss://api.hyperliquid-testnet.xyz/ws"
        return "wss://api.hyperliquid.xyz/ws"
    
    def _build_subscribe_message(self, symbols: Set[str]) -> Optional[dict]:
        """Hyperliquid 使用 allMids 订阅所有币种"""
        return {
            "method": "subscribe",
            "subscription": {"type": "allMids"}
        }
    
    def _parse_tick(self, msg: dict) -> Optional[tuple]:
        """解析 Hyperliquid 消息"""
        # Hyperliquid 格式: {"channel": "allMids", "data": {"mids": {"BTC": "100000", "ETH": "3000"}}}
        channel = msg.get("channel")
        if channel != "allMids":
            return None
        
        data = msg.get("data", {})
        mids = data.get("mids", {})
        
        if not mids:
            return None
        
        # Hyperliquid allMids 返回所有币种，我们需要过滤
        ts = now_ms()
        with self._lock:
            for sym in self._current_symbols:
                # 转换 BTCUSDT -> BTC
                hl_sym = self._to_hyperliquid_symbol(sym)
                if hl_sym in mids:
                    price = mids[hl_sym]
                    # 返回第一个匹配的，其他的在下次消息处理
                    return (sym, price, ts)
        
        return None
    
    def _symbol_to_redis_key(self, symbol: str) -> str:
        return symbol.upper()
    
    def _to_hyperliquid_symbol(self, symbol: str) -> str:
        """BTCUSDT -> BTC"""
        symbol = symbol.upper()
        if symbol.endswith("USDT"):
            return symbol[:-4]
        return symbol


# ============================================================
# 获取共享流单例的便捷函数
# ============================================================

_okx_instance: Optional[SharedOKXMarkPrice] = None
_bitget_instance: Optional[SharedBitgetMarkPrice] = None
_hyperliquid_instance: Optional[SharedHyperliquidMarkPrice] = None
_instance_lock = threading.Lock()  # P1 Fix: 添加锁保护单例创建


def get_shared_okx_mark_price(is_testnet: bool = False) -> SharedOKXMarkPrice:
    """获取 OKX 共享 MarkPrice 流"""
    global _okx_instance
    if _okx_instance is None:
        with _instance_lock:
            if _okx_instance is None:
                _okx_instance = SharedOKXMarkPrice(is_testnet)
    return _okx_instance


def get_shared_bitget_mark_price(is_testnet: bool = False) -> SharedBitgetMarkPrice:
    """获取 Bitget 共享 MarkPrice 流"""
    global _bitget_instance
    if _bitget_instance is None:
        with _instance_lock:
            if _bitget_instance is None:
                _bitget_instance = SharedBitgetMarkPrice(is_testnet)
    return _bitget_instance


def get_shared_hyperliquid_mark_price(is_testnet: bool = False) -> SharedHyperliquidMarkPrice:
    """获取 Hyperliquid 共享 MarkPrice 流"""
    global _hyperliquid_instance
    if _hyperliquid_instance is None:
        with _instance_lock:
            if _hyperliquid_instance is None:
                _hyperliquid_instance = SharedHyperliquidMarkPrice(is_testnet)
    return _hyperliquid_instance
