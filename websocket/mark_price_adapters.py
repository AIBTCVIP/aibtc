# websocket/mark_price_adapters.py
"""
MarkPrice 流适配器

将共享 MarkPrice 流适配为各交易所原有的 MarkPriceStream 接口，
实现零改动迁移到共享流模式。

使用方式：
    # 替换原有导入
    # from exchanges.okx.websocket import OKXMarkPriceStream
    from websocket.mark_price_adapters import OKXMarkPriceStreamAdapter as OKXMarkPriceStream
"""

import logging
import threading
import time
from decimal import Decimal
from typing import Callable, Optional, Set

from websocket.shared_mark_price import (
    get_shared_okx_mark_price,
    get_shared_bitget_mark_price,
    get_shared_hyperliquid_mark_price,
)

logger = logging.getLogger(__name__)


class MarkPriceAdapterBase:
    """MarkPrice 适配器基类"""
    
    def __init__(
        self,
        redis_conn,
        uid: str,
        is_testnet: bool = False,
        on_tick: Optional[Callable[[str, float, int], None]] = None,
        on_state_change: Optional[Callable] = None,
    ):
        self.rds = redis_conn
        self.uid = uid
        self.is_testnet = is_testnet
        self._on_tick = on_tick
        self.on_state_change = on_state_change
        
        self._running = False
        self._stop_event = threading.Event()
        self._refresh_thread: Optional[threading.Thread] = None
        self._refresh_interval = 2.0
        
        # 子类设置
        self._shared_stream = None
        self._exchange_name = "unknown"
    
    def start(self):
        """启动适配器"""
        if self._running:
            return
        
        self._running = True
        self._stop_event.clear()
        
        # 初始注册
        symbols = self._load_active_symbols()
        self._shared_stream.register_user(self.uid, symbols, self._handle_tick)
        
        # 启动 symbols 刷新线程
        self._refresh_thread = threading.Thread(
            target=self._refresh_symbols_loop,
            name=f"{self._exchange_name}-mark-adapter-{self.uid}",
            daemon=True
        )
        self._refresh_thread.start()
        
        logger.debug(f"[{self.uid}][{self._exchange_name}] MarkPrice 适配器已启动（共享模式）")
    
    def stop(self):
        """停止适配器"""
        self._running = False
        self._stop_event.set()
        
        # 从共享流取消注册
        if self._shared_stream:
            self._shared_stream.unregister_user(self.uid)
        
        logger.debug(f"[{self.uid}][{self._exchange_name}] MarkPrice 适配器已停止")
    
    def _handle_tick(self, symbol: str, mark_price: Decimal, ts: int):
        """处理 tick 回调"""
        if self._on_tick:
            try:
                # 转换为原有回调格式
                self._on_tick(symbol, float(mark_price), ts)
            except Exception as e:
                logger.debug(f"[{self.uid}][{self._exchange_name}] tick 回调异常: {e}")
    
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
                logger.debug(f"[{self.uid}][{self._exchange_name}] 刷新 symbols 异常: {e}")
            
            self._stop_event.wait(timeout=self._refresh_interval)
    
    def _load_active_symbols(self) -> Set[str]:
        """加载活跃 symbols（子类实现）"""
        raise NotImplementedError
    
    def add_symbol(self, symbol: str):
        """向后兼容：动态添加 symbol"""
        # 共享流会自动处理，这里只需触发刷新
        pass
    
    @property
    def state(self):
        """返回连接状态（兼容）"""
        from exchanges.okx.websocket import ConnectionState
        if self._shared_stream and self._shared_stream._thread and self._shared_stream._thread.is_alive():
            return ConnectionState.CONNECTED
        return ConnectionState.DISCONNECTED


class OKXMarkPriceStreamAdapter(MarkPriceAdapterBase):
    """OKX MarkPriceStream 适配器"""
    
    def __init__(
        self,
        redis_conn,
        uid: str,
        is_testnet: bool = False,
        on_tick: Optional[Callable[[str, float, int], None]] = None,
        on_state_change: Optional[Callable] = None,
    ):
        super().__init__(redis_conn, uid, is_testnet, on_tick, on_state_change)
        self._exchange_name = "okx"
        self._shared_stream = get_shared_okx_mark_price(is_testnet)
    
    def _load_active_symbols(self) -> Set[str]:
        """从活跃持仓中提取 OKX symbols"""
        from core.pf_compatibility import pf_compat
        
        fields = pf_compat.get_pf_pos_active(self.uid, "okx")
        symbols: Set[str] = set()
        
        for f in fields:
            if isinstance(f, (bytes, bytearray)):
                f = f.decode()
            f = str(f).strip()
            if not f:
                continue
            
            parts = f.split(":")
            if parts:
                sym = parts[0].upper()
                if sym and sym not in ("LONG", "SHORT"):
                    # 保持 BTCUSDT 格式（共享流内部会转换）
                    symbols.add(sym)
        
        return symbols


class BitgetMarkPriceStreamAdapter(MarkPriceAdapterBase):
    """Bitget MarkPriceStream 适配器"""
    
    def __init__(
        self,
        redis_conn,
        uid: str,
        is_testnet: bool = False,
        on_tick: Optional[Callable[[str, float, int], None]] = None,
        on_state_change: Optional[Callable] = None,
    ):
        super().__init__(redis_conn, uid, is_testnet, on_tick, on_state_change)
        self._exchange_name = "bitget"
        self._shared_stream = get_shared_bitget_mark_price(is_testnet)
    
    def _load_active_symbols(self) -> Set[str]:
        """从活跃持仓中提取 Bitget symbols"""
        from core.pf_compatibility import pf_compat
        
        fields = pf_compat.get_pf_pos_active(self.uid, "bitget")
        symbols: Set[str] = set()
        
        for f in fields:
            if isinstance(f, (bytes, bytearray)):
                f = f.decode()
            f = str(f).strip()
            if not f:
                continue
            
            parts = f.split(":")
            if parts:
                sym = parts[0].upper()
                if sym and sym not in ("LONG", "SHORT"):
                    symbols.add(sym)
        
        return symbols


class HyperliquidMarkPriceStreamAdapter(MarkPriceAdapterBase):
    """Hyperliquid MarkPriceStream 适配器"""
    
    def __init__(
        self,
        redis_conn,
        uid: str,
        is_testnet: bool = False,
        on_tick: Optional[Callable[[str, float, int], None]] = None,
        on_state_change: Optional[Callable] = None,
    ):
        super().__init__(redis_conn, uid, is_testnet, on_tick, on_state_change)
        self._exchange_name = "hyperliquid"
        self._shared_stream = get_shared_hyperliquid_mark_price(is_testnet)
    
    def _load_active_symbols(self) -> Set[str]:
        """从活跃持仓中提取 Hyperliquid symbols"""
        from core.pf_compatibility import pf_compat
        
        fields = pf_compat.get_pf_pos_active(self.uid, "hyperliquid")
        symbols: Set[str] = set()
        
        for f in fields:
            if isinstance(f, (bytes, bytearray)):
                f = f.decode()
            f = str(f).strip()
            if not f:
                continue
            
            parts = f.split(":")
            if parts:
                sym = parts[0].upper()
                if sym and sym not in ("LONG", "SHORT"):
                    symbols.add(sym)
        
        return symbols
