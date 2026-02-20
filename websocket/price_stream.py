# price_stream.py - 标记价格 WebSocket 流（共享版）
"""
共享 MarkPrice WebSocket 流

设计：
- SharedMarkPriceStream: 全局单例，所有用户共享一个 WebSocket 连接
- MarkPriceWS: 兼容层，保持原有接口，内部使用共享流

优势：
- 100 个用户只需要 1 个 WebSocket 连接（而不是 100 个）
- symbols 自动合并去重
- 动态增减订阅，无需全部重连
"""

import asyncio
import json
import logging
import threading
import time
from decimal import Decimal
from typing import Callable, Dict, Optional, Set

import websockets
from core.config import BINANCE_ENVIRONMENT

logger = logging.getLogger(__name__)


def now_ms() -> int:
    return int(time.time() * 1000)


def k_mark(symbol: str) -> str:
    from core.database import RedisKeys
    return RedisKeys.market_prices(symbol.upper())


# ============================================================
# 共享 MarkPrice 流（单例）
# ============================================================

class SharedMarkPriceStream:
    """
    全局共享的标记价格 WebSocket 流
    
    特点：
    - 单例模式，所有用户共享一个 WebSocket 连接
    - 动态订阅：symbols 变化时自动重连
    - 多用户回调：每个用户注册自己的 on_tick 回调
    - Redis 写入：每个 symbol 只写一次（不重复）
    
    使用方式：
        stream = get_shared_mark_price_stream()
        stream.register_user(uid, symbols, on_tick_callback)
        # ... 
        stream.unregister_user(uid)
    """
    
    _instance: Optional['SharedMarkPriceStream'] = None
    _instance_lock = threading.Lock()
    
    def __new__(cls):
        with cls._instance_lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
                cls._instance._initialized = False
            return cls._instance
    
    def __init__(self):
        if getattr(self, '_initialized', False):
            return
        
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
        
        self._initialized = True
        logger.info("[SharedMarkPriceStream] 初始化完成")
    
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
        """
        注册用户订阅
        
        Args:
            uid: 用户 ID
            symbols: 需要订阅的 symbols
            on_tick: 回调函数 (symbol, mark_price, timestamp)
        """
        with self._lock:
            self._user_symbols[uid] = set(symbols)
            if on_tick:
                self._user_callbacks[uid] = on_tick
            
            # 重新计算需要订阅的 symbols
            new_symbols = self._compute_all_symbols()
            if new_symbols != self._current_symbols:
                self._current_symbols = new_symbols
                self._symbols_changed.set()
                logger.info(f"[SharedMarkPriceStream] 用户 {uid} 注册，symbols 更新为 {len(new_symbols)} 个")
            else:
                logger.debug(f"[SharedMarkPriceStream] 用户 {uid} 注册，symbols 无变化")
        
        # 确保 WebSocket 运行
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
                logger.info(f"[SharedMarkPriceStream] 用户 {uid} 取消注册，symbols 更新为 {len(new_symbols)} 个")
    
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
                logger.debug(f"[SharedMarkPriceStream] 用户 {uid} symbols 更新")
    
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
            name="shared-mark-price-ws", 
            daemon=True
        )
        self._thread.start()
        logger.info("[SharedMarkPriceStream] WebSocket 线程已启动")
    
    def stop(self):
        """停止 WebSocket"""
        self._stop_event.set()
        self._symbols_changed.set()  # 唤醒等待
        if self._loop:
            try:
                self._loop.call_soon_threadsafe(lambda: None)
            except Exception as e:
                # P3 Fix: 添加日志
                logger.debug(f"[SharedMarkPriceStream] 停止时调用异常: {e}")
        logger.info("[SharedMarkPriceStream] 已停止")
    
    def _run_thread(self):
        """WebSocket 线程入口"""
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        try:
            self._loop.run_until_complete(self._main_loop())
        except Exception as e:
            logger.error(f"[SharedMarkPriceStream] 线程异常: {e}")
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
                logger.debug(f"[SharedMarkPriceStream] 清理任务异常: {e}")
            finally:
                self._loop.close()
                self._loop = None
    
    async def _main_loop(self):
        """主循环（支持动态订阅，无需断开重连）"""
        while not self._stop_event.is_set():
            # 获取当前需要订阅的 symbols
            with self._lock:
                symbols = self._current_symbols.copy()
            
            # 没有 symbols 则等待
            if not symbols:
                logger.debug("[SharedMarkPriceStream] 无订阅 symbols，等待...")
                await asyncio.get_event_loop().run_in_executor(
                    None, 
                    lambda: self._symbols_changed.wait(timeout=5.0)
                )
                self._symbols_changed.clear()
                continue
            
            # 连接 WebSocket（使用基础 URL，通过消息动态订阅）
            base_url = "wss://fstream.binance.com/ws" if not BINANCE_ENVIRONMENT else "wss://stream.binancefuture.com/ws"
            logger.info(f"[SharedMarkPriceStream] 连接 WebSocket...")
            
            try:
                async with websockets.connect(
                    base_url,
                    open_timeout=30,  # 增加握手超时时间
                    ping_interval=20,
                    ping_timeout=20,
                    close_timeout=5,
                    max_queue=4096,
                    max_size=10 * 1024 * 1024,
                ) as ws:
                    # 记录当前已订阅的 symbols
                    subscribed_symbols: Set[str] = set()
                    request_id = 1
                    
                    # 初始订阅
                    with self._lock:
                        to_subscribe = self._current_symbols.copy()
                    
                    if to_subscribe:
                        await self._send_subscribe(ws, to_subscribe, request_id)
                        subscribed_symbols = to_subscribe.copy()
                        request_id += 1
                        logger.info(f"[SharedMarkPriceStream] 已订阅 {len(subscribed_symbols)} 个 symbols")
                    
                    self._symbols_changed.clear()
                    
                    while not self._stop_event.is_set():
                        # 检查 symbols 是否变化（动态增减订阅，不断开连接）
                        if self._symbols_changed.is_set():
                            self._symbols_changed.clear()
                            
                            with self._lock:
                                new_symbols = self._current_symbols.copy()
                            
                            # 计算差异
                            to_unsub = subscribed_symbols - new_symbols
                            to_sub = new_symbols - subscribed_symbols
                            
                            # 取消订阅
                            if to_unsub:
                                await self._send_unsubscribe(ws, to_unsub, request_id)
                                request_id += 1
                                logger.info(f"[SharedMarkPriceStream] 取消订阅 {len(to_unsub)} 个: {sorted(to_unsub)[:5]}...")
                            
                            # 新增订阅
                            if to_sub:
                                await self._send_subscribe(ws, to_sub, request_id)
                                request_id += 1
                                logger.info(f"[SharedMarkPriceStream] 新增订阅 {len(to_sub)} 个: {sorted(to_sub)[:5]}...")
                            
                            subscribed_symbols = new_symbols.copy()
                            
                            # 如果没有任何订阅了，断开等待
                            if not subscribed_symbols:
                                logger.info("[SharedMarkPriceStream] 无订阅，断开连接")
                                break
                        
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=30.0)
                        except asyncio.TimeoutError:
                            continue
                        
                        if not raw:
                            continue
                        
                        # 解析消息
                        try:
                            msg = json.loads(raw)
                            
                            # 跳过订阅响应消息
                            if "result" in msg or "id" in msg:
                                continue
                            
                            # 处理 markPrice 数据
                            # 格式: {"e":"markPriceUpdate","E":...,"s":"BTCUSDT","p":"50000.00",...}
                            event = msg.get("e")
                            if event != "markPriceUpdate":
                                continue
                            
                            sym = (msg.get("s") or "").upper()
                            mp = msg.get("p")
                            if not sym or mp is None:
                                continue
                            
                            ts = now_ms()
                            mark_price = Decimal(str(mp))
                            
                            # 1) 写入 Redis（全局共享，只写一次）
                            self._write_to_redis(sym, mp, ts)
                            
                            # 2) 分发回调到订阅该 symbol 的用户
                            self._dispatch_tick(sym, mark_price, ts)
                            
                        except Exception as e:
                            logger.debug(f"[SharedMarkPriceStream] 消息处理异常: {e}")
                            
            except websockets.exceptions.ConnectionClosed:
                logger.warning("[SharedMarkPriceStream] 连接关闭，1秒后重连...")
                await asyncio.sleep(1.0)
            except Exception as e:
                logger.error(f"[SharedMarkPriceStream] 连接异常: {e}")
                await asyncio.sleep(2.0)
    
    async def _send_subscribe(self, ws, symbols: Set[str], request_id: int):
        """发送订阅请求"""
        if not symbols:
            return
        streams = [f"{s.lower()}@markPrice@1s" for s in symbols]
        msg = {
            "method": "SUBSCRIBE",
            "params": streams,
            "id": request_id
        }
        await ws.send(json.dumps(msg))
    
    async def _send_unsubscribe(self, ws, symbols: Set[str], request_id: int):
        """发送取消订阅请求"""
        if not symbols:
            return
        streams = [f"{s.lower()}@markPrice@1s" for s in symbols]
        msg = {
            "method": "UNSUBSCRIBE",
            "params": streams,
            "id": request_id
        }
        await ws.send(json.dumps(msg))
    
    def _build_stream_url(self, symbols: Set[str]) -> str:
        """构建 WebSocket URL（备用：URL 方式订阅）"""
        base = "wss://stream.binancefuture.com/stream" if BINANCE_ENVIRONMENT else "wss://fstream.binance.com/stream"
        streams = "/".join([f"{s.lower()}@markPrice@1s" for s in sorted(symbols)])
        return f"{base}?streams={streams}"
    
    def _write_to_redis(self, symbol: str, mark_price: str, ts: int):
        """写入 Redis"""
        try:
            rds = self._get_redis()
            payload = {"markPrice": str(mark_price), "ts": str(ts)}
            key = k_mark(symbol)
            rds.set(key, json.dumps(payload, separators=(",", ":")))
            if self._mark_ttl_s > 0:
                rds.expire(key, self._mark_ttl_s)
        except Exception as e:
            logger.debug(f"[SharedMarkPriceStream] Redis 写入失败: {e}")
    
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
                            # 不要让回调异常影响其他用户
                            logger.debug(f"[SharedMarkPriceStream] 用户 {uid} 回调异常: {e}")
    
    def get_stats(self) -> dict:
        """获取统计信息"""
        with self._lock:
            return {
                "users": len(self._user_symbols),
                "symbols": len(self._current_symbols),
                "running": self._thread.is_alive() if self._thread else False,
                "symbol_list": sorted(self._current_symbols),
            }


# 全局单例获取函数
def get_shared_mark_price_stream() -> SharedMarkPriceStream:
    """获取共享的 MarkPrice 流单例"""
    return SharedMarkPriceStream()


# ============================================================
# 兼容层：MarkPriceWS（保持原有接口）
# ============================================================

class MarkPriceWS:
    """
    标记价格 WebSocket（兼容层）
    
    保持原有接口，内部使用 SharedMarkPriceStream 共享连接。
    
    原有用法不变：
        mp = MarkPriceWS(redis_conn, uid, on_tick=callback)
        mp.start()
        # ...
        mp.stop()
    """
    
    def __init__(
        self,
        redis_conn,
        uid: str,
        *,
        refresh_symbols_interval_s: float = 2.0,
        recv_timeout_s: float = 30.0,
        mark_ttl_s: int = 15,
        on_tick: Optional[Callable[[str, Decimal, int], None]] = None,
    ):
        self.rds = redis_conn
        self.uid = uid
        self.refresh_symbols_interval_s = refresh_symbols_interval_s
        self.recv_timeout_s = recv_timeout_s
        self.mark_ttl_s = mark_ttl_s
        self.on_tick = on_tick
        
        self._running = False
        self._stop_event = threading.Event()
        self._refresh_thread: Optional[threading.Thread] = None
        
        # 共享流
        self._shared_stream = get_shared_mark_price_stream()
    
    def start(self):
        """启动（注册到共享流）"""
        if self._running:
            return
        
        self._running = True
        self._stop_event.clear()
        
        # 初始注册
        symbols = self._load_active_symbols()
        self._shared_stream.register_user(self.uid, symbols, self.on_tick)
        
        # 启动定期刷新 symbols 的线程
        self._refresh_thread = threading.Thread(
            target=self._refresh_symbols_loop,
            name=f"mark-refresh-{self.uid}",
            daemon=True
        )
        self._refresh_thread.start()
        
        logger.debug(f"[MarkPriceWS:{self.uid}] 已启动（使用共享流）")
    
    def stop(self):
        """停止（从共享流取消注册）"""
        self._running = False
        self._stop_event.set()
        
        # 从共享流取消注册
        self._shared_stream.unregister_user(self.uid)
        
        logger.debug(f"[MarkPriceWS:{self.uid}] 已停止")
    
    def _refresh_symbols_loop(self):
        """定期刷新 symbols"""
        last_symbols: Set[str] = set()
        
        while not self._stop_event.is_set():
            try:
                symbols = self._load_active_symbols()
                
                if symbols != last_symbols:
                    self._shared_stream.update_user_symbols(self.uid, symbols)
                    last_symbols = symbols
                
            except Exception as e:
                logger.debug(f"[MarkPriceWS:{self.uid}] 刷新 symbols 异常: {e}")
            
            self._stop_event.wait(timeout=self.refresh_symbols_interval_s)
    
    def _load_active_symbols(self) -> Set[str]:
        """从活跃持仓和挂单中提取需要订阅的 symbols"""
        from core.pf_compatibility import pf_compat
        symbols: Set[str] = set()
        
        # 1. 从活跃持仓获取 symbols
        fields = pf_compat.get_pf_pos_active(self.uid)
        for f in fields:
            if isinstance(f, (bytes, bytearray)):
                f = f.decode()
            f = str(f).strip()
            if not f:
                continue
            
            # 解析格式: "exchange:SYMBOL:SIDE" 或 "SYMBOL:SIDE"
            parts = f.split(":")
            if len(parts) >= 3:
                sym = parts[1].upper()
            elif len(parts) == 2:
                sym = parts[0].upper()
            else:
                sym = parts[0].upper()
            
            if sym and sym not in ("LONG", "SHORT", "BINANCE", "OKX", "BITGET", "HYPERLIQUID"):
                symbols.add(sym)
        
        # 2. 从挂单获取 symbols
        try:
            open_orders = pf_compat.get_pf_open_orders(self.uid, "binance")
            if open_orders and isinstance(open_orders, dict):
                for order_id, order in open_orders.items():
                    if isinstance(order, dict):
                        sym = order.get("symbol", "").upper()
                        if sym:
                            symbols.add(sym)
        except Exception as e:
            logger.debug(f"[MarkPriceWS:{self.uid}] 获取挂单 symbols 失败: {e}")
        
        return symbols
