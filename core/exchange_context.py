# core/exchange_context.py
"""
交易所上下文模块

每个交易所都有自己的:
- API 客户端
- WebSocket 连接
- 持仓数据存储 (CycleStore)
- 持仓审计器 (PositionAuditor)
- 止损管理器

这样同一用户可以同时在多个交易所交易，数据互不干扰
"""

import logging
import threading
import redis
from typing import Dict, Optional, Any, TYPE_CHECKING

from core.config import REDIS_HOST, REDIS_PORT, REDIS_DB

if TYPE_CHECKING:
    from core.user_context import UserContext

logger = logging.getLogger(__name__)


class ExchangeContext:
    """
    单个交易所的上下文
    
    包含该交易所的所有状态和连接
    """
    
    def __init__(
        self,
        uid: str,
        exchange: str,
        api_key: str,
        api_secret: str,
        passphrase: Optional[str] = None,
        is_testnet: bool = False,
        wallet_address: Optional[str] = None,
        user_context: Optional['UserContext'] = None,
    ):
        self.uid = uid
        self.exchange = exchange
        self.api_key = api_key
        self.api_secret = api_secret
        self.passphrase = passphrase
        self.is_testnet = is_testnet
        self.wallet_address = wallet_address
        self._user_context = user_context
        
        # Redis 连接 - P2 Fix: 使用共享连接池
        from core.database import redis_client
        self._redis = redis_client
        
        # 组件（懒加载）
        self._client = None           # API 客户端
        self._websocket = None        # WebSocket 管理器
        self._position_store = None   # 持仓数据存储
        self._cycle_store = None      # CycleStore (WebSocket 数据处理)
        self._position_auditor = None # 持仓审计器
        self._stop_loss_manager = None  # 止损管理器
        
        # 状态
        self._is_running = False
        self._stop_event = threading.Event()
        
        logger.info(f"[{uid}][{exchange}] ExchangeContext 已创建")
    
    @property
    def client(self):
        """获取 API 客户端（懒加载）"""
        if self._client is None:
            self._client = self._create_client()
        return self._client
    
    @property
    def position_store(self):
        """获取持仓数据存储（懒加载）"""
        if self._position_store is None:
            self._position_store = self._create_position_store()
        return self._position_store
    
    @property
    def cycle_store(self):
        """获取 CycleStore"""
        return self._cycle_store
    
    @property
    def position_auditor(self):
        """获取 PositionAuditor"""
        return self._position_auditor
    
    def _create_client(self):
        """创建 API 客户端"""
        if self.exchange == "binance":
            from exchanges.binance.client import BinanceClient
            return BinanceClient(
                api_key=self.api_key,
                api_secret=self.api_secret,
                is_testnet=self.is_testnet,
            )
        elif self.exchange == "okx":
            from exchanges.okx_exchange import OKXExchange
            return OKXExchange(
                api_key=self.api_key,
                api_secret=self.api_secret,
                passphrase=self.passphrase,
                is_testnet=self.is_testnet,
            )
        elif self.exchange == "bitget":
            from exchanges.bitget_exchange import BitgetExchange
            return BitgetExchange(
                api_key=self.api_key,
                api_secret=self.api_secret,
                passphrase=self.passphrase,
                is_testnet=self.is_testnet,
            )
        elif self.exchange == "hyperliquid":
            from exchanges.hyperliquid_exchange import HyperliquidExchange
            return HyperliquidExchange(
                api_key=self.api_key,
                api_secret=self.api_secret,
                is_testnet=self.is_testnet,
                wallet_address=self.wallet_address,
            )
        else:
            raise ValueError(f"不支持的交易所: {self.exchange}")
    
    def _create_position_store(self):
        """创建持仓数据存储"""
        if self.exchange == "binance":
            from exchanges.binance.position_store import BinancePositionStore
            return BinancePositionStore(
                uid=self.uid,
                user_context=self._user_context,
            )
        elif self.exchange == "okx":
            from exchanges.okx.position_store import OKXPositionStore
            return OKXPositionStore(
                uid=self.uid,
                exchange_client=self.client,
                user_context=self._user_context,
            )
        elif self.exchange == "bitget":
            from exchanges.bitget.position_store import BitgetPositionStore
            return BitgetPositionStore(
                uid=self.uid,
                exchange_client=self.client,
                user_context=self._user_context,
            )
        elif self.exchange == "hyperliquid":
            from exchanges.hyperliquid.position_store import HyperliquidPositionStore
            return HyperliquidPositionStore(
                uid=self.uid,
                exchange_client=self.client,
                user_context=self._user_context,
            )
        else:
            logger.warning(f"[{self.uid}][{self.exchange}] 不支持的交易所，无法创建 PositionStore")
            return None
    
    def _create_websocket(self):
        """创建 WebSocket 管理器"""
        if self.exchange == "binance":
            from exchanges.binance.websocket import BinanceWebSocket
            
            # 创建回调
            callbacks = {
                'on_user_data': self._on_user_data,
                'on_mark_price': self._on_mark_price,
            }
            
            return BinanceWebSocket(
                api_key=self.api_key,
                api_secret=self.api_secret,  # 需要传递 api_secret 用于获取 listen key
                is_testnet=self.is_testnet,
                uid=self.uid,
                callbacks=callbacks,
            )
        # 其他交易所后续实现
        return None
    
    def _on_user_data(self, msg: Dict):
        """处理用户数据流消息"""
        if not self._position_store:
            return
        
        event_type = msg.get("e")
        if event_type == "ACCOUNT_UPDATE":
            self._position_store.on_account_update(msg)
        elif event_type == "ORDER_TRADE_UPDATE":
            self._position_store.on_order_trade_update(msg)
    
    def _on_mark_price(self, symbol: str, mark_price: float, timestamp: int):
        """处理标记价格更新"""
        # 更新到 Redis
        from core.database import redis_client, RedisKeys
        import json
        
        price_key = RedisKeys.market_prices(symbol)
        price_data = {
            "symbol": symbol,
            "markPrice": str(mark_price),
            "ts": timestamp,
        }
        redis_client.set(price_key, json.dumps(price_data))
    
    def start(self):
        """启动交易所服务"""
        if self._is_running:
            return
        
        self._stop_event.clear()
        
        # 根据交易所类型初始化组件
        if self.exchange == "binance":
            self._start_binance_services()
        elif self.exchange == "okx":
            self._start_okx_services()
        elif self.exchange == "bitget":
            self._start_bitget_services()
        elif self.exchange == "hyperliquid":
            self._start_hyperliquid_services()
        else:
            logger.warning(f"[{self.uid}][{self.exchange}] 未知交易所，跳过服务启动")
        
        self._is_running = True
        logger.info(f"[{self.uid}][{self.exchange}] 服务已启动")
    
    def _start_binance_services(self):
        """启动 Binance 特定服务（CycleStore + PositionAuditor）"""
        try:
            # 获取底层 Binance client
            binance_client = self.client.client if hasattr(self.client, 'client') else self.client
            
            # 创建 CycleStore (包含 WebSocket 和止损管理)
            from websocket.cycle_store import BinanceUMHedgeWSOnlyStore
            self._cycle_store = BinanceUMHedgeWSOnlyStore(
                client_sync=binance_client,
                redis_conn=self._redis,
                uid=self.uid,
                user_context=self._user_context,
                exchange=self.exchange,  # 传入交易所名称
                on_auth_failed=self._on_auth_failed,  # 认证失败回调
            )
            self._cycle_store.start()
            logger.info(f"[{self.uid}][{self.exchange}] CycleStore 已启动")
            
            # 创建 PositionAuditor（持仓审计，比对 Redis 与交易所实际持仓）
            # P2 优化：使用全局审计器，减少线程数和 API 调用
            try:
                import os
                if os.environ.get("USE_GLOBAL_AUDITOR", "1") == "1":
                    from cache.global_position_auditor import get_global_position_auditor
                    auditor = get_global_position_auditor()
                    auditor.register_user(self.uid, self.exchange, binance_client, self._on_auth_failed)
                    self._position_auditor = None  # 使用全局审计器，不需要本地实例
                    logger.info(f"[{self.uid}][{self.exchange}] 已注册到全局 PositionAuditor")
                else:
                    from cache.position_auditor import PositionAuditor
                    self._position_auditor = PositionAuditor(
                        client=binance_client,
                        redis_conn=self._redis,
                        uid=self.uid,
                        audit_interval_s=120.0,
                        dry_run=False,
                        on_report=self._on_audit_report,
                        exchange=self.exchange,
                        on_auth_failed=self._on_auth_failed,  # 认证失败回调
                    )
                    self._position_auditor.start()
                    logger.info(f"[{self.uid}][{self.exchange}] PositionAuditor 已启动 (本地模式)")
            except Exception as audit_err:
                logger.warning(f"[{self.uid}][{self.exchange}] PositionAuditor 启动失败: {audit_err}")
            
        except Exception as e:
            logger.error(f"[{self.uid}][{self.exchange}] 启动 Binance 服务失败: {e}")
            logger.exception("启动 Binance 服务失败")
    
    def _on_audit_report(self, report):
        """审计报告回调"""
        if report.has_issues():
            logger.info(f"[{self.uid}][{self.exchange}] 审计报告:\n{report.summary()}")
    
    def _on_auth_failed(self, error_msg: str):
        """
        认证失败回调
        
        当 WebSocket 认证失败时（如 API Key 无效），触发 UserContext 的自动停止逻辑
        """
        if self._user_context:
            self._user_context.auto_stop_exchange_on_auth_failure(self.exchange, error_msg)
        else:
            logger.warning(f"[{self.uid}][{self.exchange}] Auth failed but no UserContext to handle: {error_msg}")
    
    def _start_okx_services(self):
        """
        启动 OKX 服务
        
        使用 CycleStore (WebSocket) 进行完整的周期跟踪，
        同时启动 REST 轮询 PositionStore 作为备份
        """
        try:
            # 尝试启动 CycleStore（WebSocket 完整周期跟踪）
            try:
                from exchanges.okx.cycle_store import OKXCycleStore
                self._cycle_store = OKXCycleStore(
                    api_key=self.api_key,
                    api_secret=self.api_secret,
                    passphrase=self.passphrase,
                    redis_conn=self._redis,
                    uid=self.uid,
                    is_testnet=self.is_testnet,
                    user_context=self._user_context,
                    on_auth_failed=self._on_auth_failed,  # 认证失败回调
                )
                self._cycle_store.start()
                logger.info(f"[{self.uid}][{self.exchange}] CycleStore 已启动 (WebSocket)")
            except Exception as cs_err:
                logger.warning(f"[{self.uid}][{self.exchange}] CycleStore 启动失败: {cs_err}")
                # 回退到 REST 轮询
                self._position_store = self._create_position_store()
                if self._position_store:
                    self._position_store.start()
                    logger.info(f"[{self.uid}][{self.exchange}] PositionStore 已启动 (REST 轮询备份)")
            
            # 启动 OKXPositionAuditor（持仓审计，比对 Redis 与交易所实际持仓）
            try:
                from exchanges.okx.position_auditor import OKXPositionAuditor
                self._position_auditor = OKXPositionAuditor(
                    exchange_client=self.client,  # 传入 OKX client 用于 API 调用
                    redis_conn=self._redis,
                    uid=self.uid,
                    audit_interval_s=120.0,  # 2 分钟检查一次
                    dry_run=False,
                    on_report=self._on_audit_report,
                    on_auth_failed=self._on_auth_failed,  # ✅ 传入认证失败回调
                )
                self._position_auditor.start()
                logger.info(f"[{self.uid}][{self.exchange}] OKXPositionAuditor 已启动 (每 2 分钟审计一次)")
            except Exception as pa_err:
                logger.warning(f"[{self.uid}][{self.exchange}] OKXPositionAuditor 启动失败: {pa_err}")
                
        except Exception as e:
            logger.error(f"[{self.uid}][{self.exchange}] 启动 OKX 服务失败: {e}")
            logger.exception("启动 OKX 服务失败")
    
    def _on_okx_account(self, data: Dict):
        """处理 OKX 账户更新"""
        try:
            from core.pf_compatibility import pf_compat
            from core.utils import now_ms
            
            # OKX 账户数据格式: {"details": [{"ccy": "USDT", "eq": "1000", ...}]}
            details = data.get("details", [])
            for detail in details:
                if detail.get("ccy") == "USDT":
                    account_obj = {
                        "uid": self.uid,
                        "ts": str(now_ms()),
                        "walletBalance": str(detail.get("eq", 0)),
                        "equity": str(detail.get("eq", 0)),
                        "unrealized": str(detail.get("upl", 0)),
                        "source": "OKX_WS",
                        "exchange": self.exchange,
                    }
                    pf_compat.set_pf_account(self.uid, account_obj, self.exchange)
                    break
        except Exception as e:
            logger.warning(f"[{self.uid}][{self.exchange}] 处理账户更新失败: {e}")
    
    def _on_okx_position(self, data: Dict):
        """处理 OKX 持仓更新"""
        try:
            from core.pf_compatibility import pf_compat
            from core.utils import now_ms
            
            # OKX 持仓数据格式
            inst_id = data.get("instId", "")  # e.g., "BTC-USDT-SWAP"
            pos = float(data.get("pos", 0))
            
            # 转换为标准 symbol 格式
            parts = inst_id.split("-")
            if len(parts) >= 2:
                symbol = f"{parts[0]}{parts[1]}"  # BTC-USDT-SWAP -> BTCUSDT
            else:
                symbol = inst_id
            
            # 确定方向
            pos_side = data.get("posSide", "net")
            if pos_side == "net":
                side = "LONG" if pos > 0 else "SHORT"
            else:
                side = pos_side.upper()
            
            field = f"{symbol}:{side}"
            ts = now_ms()
            
            if abs(pos) > 0:
                # 有持仓
                pos_obj = {
                    "symbol": symbol,
                    "side": side,
                    "qty": str(abs(pos)),
                    "entryPrice": str(data.get("avgPx", 0)),
                    "unrealizedPnl": str(data.get("upl", 0)),
                    "marginType": "cross" if data.get("mgnMode") == "cross" else "isolated",
                    "openTimeMs": str(data.get("cTime", ts)),
                    "updatedAt": str(ts),
                    "exchange": self.exchange,
                }
                
                # 更新 Redis
                positions = pf_compat.get_pf_pos(self.uid, self.exchange) or {}
                positions[field] = pos_obj
                pf_compat.set_pf_pos(self.uid, positions, self.exchange)
                
                # 更新活跃列表
                active_list = pf_compat.get_pf_pos_active(self.uid, self.exchange) or []
                if field not in active_list:
                    active_list.append(field)
                    pf_compat.set_pf_pos_active(self.uid, active_list, self.exchange)
            else:
                # 无持仓 - 可能是平仓
                positions = pf_compat.get_pf_pos(self.uid, self.exchange) or {}
                if field in positions:
                    del positions[field]
                    pf_compat.set_pf_pos(self.uid, positions, self.exchange)
                
                active_list = pf_compat.get_pf_pos_active(self.uid, self.exchange) or []
                if field in active_list:
                    active_list.remove(field)
                    pf_compat.set_pf_pos_active(self.uid, active_list, self.exchange)
                    
        except Exception as e:
            logger.warning(f"[{self.uid}][{self.exchange}] 处理持仓更新失败: {e}")
    
    def _on_okx_order(self, data: Dict):
        """处理 OKX 订单更新"""
        # 订单更新可用于跟踪止盈止损触发等
        logger.debug(f"[{self.uid}][{self.exchange}] 订单更新: {data.get('ordId')} {data.get('state')}")
    
    def _start_bitget_services(self):
        """
        启动 Bitget 服务
        
        使用 CycleStore (WebSocket) 进行完整的周期跟踪，
        同时启动 REST 轮询 PositionStore 作为备份
        """
        try:
            # 尝试启动 CycleStore（WebSocket 完整周期跟踪）
            try:
                from exchanges.bitget.cycle_store import BitgetCycleStore
                self._cycle_store = BitgetCycleStore(
                    api_key=self.api_key,
                    api_secret=self.api_secret,
                    passphrase=self.passphrase,
                    redis_conn=self._redis,
                    uid=self.uid,
                    is_testnet=self.is_testnet,
                    user_context=self._user_context,
                    on_auth_failed=self._on_auth_failed,  # 认证失败回调
                )
                self._cycle_store.start()
                logger.info(f"[{self.uid}][{self.exchange}] CycleStore 已启动 (WebSocket)")
            except Exception as cs_err:
                logger.warning(f"[{self.uid}][{self.exchange}] CycleStore 启动失败: {cs_err}")
                # 回退到 REST 轮询
                self._position_store = self._create_position_store()
                if self._position_store:
                    self._position_store.start()
                    logger.info(f"[{self.uid}][{self.exchange}] PositionStore 已启动 (REST 轮询备份)")
            
            # 启动 BitgetPositionAuditor（持仓审计，比对 Redis 与交易所实际持仓）
            try:
                from exchanges.bitget.position_auditor import BitgetPositionAuditor
                self._position_auditor = BitgetPositionAuditor(
                    exchange_client=self.client,  # 传入 Bitget client 用于 API 调用
                    redis_conn=self._redis,
                    uid=self.uid,
                    audit_interval_s=120.0,  # 2 分钟检查一次
                    dry_run=False,
                    on_report=self._on_audit_report,
                    on_auth_failed=self._on_auth_failed,  # 认证失败回调
                )
                self._position_auditor.start()
                logger.info(f"[{self.uid}][{self.exchange}] BitgetPositionAuditor 已启动 (每 2 分钟审计一次)")
            except Exception as pa_err:
                logger.warning(f"[{self.uid}][{self.exchange}] BitgetPositionAuditor 启动失败: {pa_err}")
                
        except Exception as e:
            logger.error(f"[{self.uid}][{self.exchange}] 启动 Bitget 服务失败: {e}")
            logger.exception("启动 Bitget 服务失败")
    
    def _on_bitget_account(self, data: Dict):
        """处理 Bitget 账户更新"""
        try:
            from core.pf_compatibility import pf_compat
            from core.utils import now_ms
            
            # Bitget 账户数据格式
            margin_coin = data.get("marginCoin", "")
            if margin_coin.upper() == "USDT":
                account_obj = {
                    "uid": self.uid,
                    "ts": str(now_ms()),
                    "walletBalance": str(data.get("usdtEquity", 0) or data.get("equity", 0)),
                    "equity": str(data.get("usdtEquity", 0) or data.get("equity", 0)),
                    "unrealized": str(data.get("unrealizedPL", 0)),
                    "source": "BITGET_WS",
                    "exchange": self.exchange,
                }
                pf_compat.set_pf_account(self.uid, account_obj, self.exchange)
        except Exception as e:
            logger.warning(f"[{self.uid}][{self.exchange}] 处理账户更新失败: {e}")
    
    def _on_bitget_position(self, data: Dict):
        """处理 Bitget 持仓更新"""
        try:
            from core.pf_compatibility import pf_compat
            from core.utils import now_ms
            
            symbol = data.get("symbol", "")
            total = float(data.get("total", 0))
            hold_side = data.get("holdSide", "").upper()  # long/short
            
            field = f"{symbol}:{hold_side}"
            ts = now_ms()
            
            if total > 0:
                pos_obj = {
                    "symbol": symbol,
                    "side": hold_side,
                    "qty": str(total),
                    "entryPrice": str(data.get("openPriceAvg", 0)),
                    "unrealizedPnl": str(data.get("unrealizedPL", 0)),
                    "marginType": "cross" if data.get("marginMode") == "crossed" else "isolated",
                    "openTimeMs": str(data.get("cTime", ts)),
                    "updatedAt": str(ts),
                    "exchange": self.exchange,
                }
                
                positions = pf_compat.get_pf_pos(self.uid, self.exchange) or {}
                positions[field] = pos_obj
                pf_compat.set_pf_pos(self.uid, positions, self.exchange)
                
                active_list = pf_compat.get_pf_pos_active(self.uid, self.exchange) or []
                if field not in active_list:
                    active_list.append(field)
                    pf_compat.set_pf_pos_active(self.uid, active_list, self.exchange)
            else:
                positions = pf_compat.get_pf_pos(self.uid, self.exchange) or {}
                if field in positions:
                    del positions[field]
                    pf_compat.set_pf_pos(self.uid, positions, self.exchange)
                
                active_list = pf_compat.get_pf_pos_active(self.uid, self.exchange) or []
                if field in active_list:
                    active_list.remove(field)
                    pf_compat.set_pf_pos_active(self.uid, active_list, self.exchange)
                    
        except Exception as e:
            logger.warning(f"[{self.uid}][{self.exchange}] 处理持仓更新失败: {e}")
    
    def _on_bitget_order(self, data: Dict):
        """处理 Bitget 订单更新"""
        logger.debug(f"[{self.uid}][{self.exchange}] 订单更新: {data.get('orderId')} {data.get('status')}")
    
    def _start_hyperliquid_services(self):
        """
        启动 Hyperliquid 服务
        
        使用 CycleStore (REST 轮询) 进行完整的周期跟踪
        """
        try:
            # 尝试启动 CycleStore（REST 轮询完整周期跟踪）
            try:
                from exchanges.hyperliquid.cycle_store import HyperliquidCycleStore
                self._cycle_store = HyperliquidCycleStore(
                    exchange_client=self.client,
                    redis_conn=self._redis,
                    uid=self.uid,
                    user_context=self._user_context,
                    on_auth_failed=self._on_auth_failed,  # 认证失败回调
                )
                self._cycle_store.start()
                logger.info(f"[{self.uid}][{self.exchange}] CycleStore 已启动 (REST 轮询)")
            except Exception as cs_err:
                logger.warning(f"[{self.uid}][{self.exchange}] CycleStore 启动失败: {cs_err}")
                # 回退到简单的 PositionStore
                self._position_store = self._create_position_store()
                if self._position_store:
                    self._position_store.start()
                    logger.info(f"[{self.uid}][{self.exchange}] PositionStore 已启动 (REST 轮询备份)")
            
            # 启动 HyperliquidPositionAuditor（持仓审计，比对 Redis 与交易所实际持仓）
            try:
                from exchanges.hyperliquid.position_auditor import HyperliquidPositionAuditor
                self._position_auditor = HyperliquidPositionAuditor(
                    exchange_client=self.client,  # 传入 Hyperliquid client 用于 API 调用
                    redis_conn=self._redis,
                    uid=self.uid,
                    audit_interval_s=120.0,  # 2 分钟检查一次
                    dry_run=False,
                    on_report=self._on_audit_report,
                    on_auth_failed=self._on_auth_failed,  # 认证失败回调
                )
                self._position_auditor.start()
                logger.info(f"[{self.uid}][{self.exchange}] HyperliquidPositionAuditor 已启动 (每 2 分钟审计一次)")
            except Exception as pa_err:
                logger.warning(f"[{self.uid}][{self.exchange}] HyperliquidPositionAuditor 启动失败: {pa_err}")
                
        except Exception as e:
            logger.error(f"[{self.uid}][{self.exchange}] 启动 Hyperliquid 服务失败: {e}")
            logger.exception("启动 Hyperliquid 服务失败")
    
    def stop(self):
        """停止交易所服务"""
        if not self._is_running:
            return
        
        self._stop_event.set()
        
        # 停止 CycleStore (Binance/OKX/Bitget)
        if self._cycle_store:
            try:
                self._cycle_store.stop()
            except Exception as e:
                logger.warning(f"[{self.uid}][{self.exchange}] 停止 CycleStore 失败: {e}")
            self._cycle_store = None
        
        # 停止 PositionAuditor
        # P2 优化：如果使用全局审计器，需要注销用户
        import os
        if os.environ.get("USE_GLOBAL_AUDITOR", "1") == "1":
            try:
                from cache.global_position_auditor import get_global_position_auditor
                auditor = get_global_position_auditor()
                auditor.unregister_user(self.uid, self.exchange)
                logger.debug(f"[{self.uid}][{self.exchange}] 已从全局 PositionAuditor 注销")
            except Exception as e:
                logger.warning(f"[{self.uid}][{self.exchange}] 注销全局 PositionAuditor 失败: {e}")
        elif self._position_auditor:
            try:
                self._position_auditor.stop()
            except Exception as e:
                logger.warning(f"[{self.uid}][{self.exchange}] 停止 PositionAuditor 失败: {e}")
            self._position_auditor = None
        
        # 停止 PositionStore (OKX/Bitget/Hyperliquid REST 轮询)
        # 注意: BinancePositionStore 是纯数据存储，没有 stop() 方法
        if self._position_store:
            try:
                if hasattr(self._position_store, 'stop'):
                    self._position_store.stop()
            except Exception as e:
                logger.warning(f"[{self.uid}][{self.exchange}] 停止 PositionStore 失败: {e}")
            self._position_store = None
        
        # 停止 WebSocket
        if self._websocket:
            try:
                self._websocket.stop()
            except Exception as e:
                logger.warning(f"[{self.uid}][{self.exchange}] 停止 WebSocket 失败: {e}")
            self._websocket = None
        
        # 关闭客户端
        if self._client:
            try:
                # 优先使用 close_sync() 方法（避免跨事件循环问题）
                if hasattr(self._client, 'close_sync'):
                    self._client.close_sync()
                elif hasattr(self._client, 'close'):
                    import asyncio
                    import inspect
                    close_method = self._client.close
                    
                    # 检查 close 是否是协程函数
                    if inspect.iscoroutinefunction(close_method):
                        # 尝试获取当前事件循环
                        try:
                            loop = asyncio.get_running_loop()
                            # 在独立线程中执行异步关闭，避免阻塞当前线程
                            import threading
                            def _close_in_thread():
                                new_loop = asyncio.new_event_loop()
                                try:
                                    new_loop.run_until_complete(close_method())
                                except Exception as e:
                                    logger.debug(f"[{self.uid}][{self.exchange}] 异步关闭客户端: {e}")
                                finally:
                                    new_loop.close()
                            
                            close_thread = threading.Thread(target=_close_in_thread, daemon=True)
                            close_thread.start()
                            # 等待一小段时间让关闭完成
                            close_thread.join(timeout=2.0)
                        except RuntimeError:
                            # 没有运行的循环，创建新的
                            loop = asyncio.new_event_loop()
                            try:
                                loop.run_until_complete(close_method())
                            finally:
                                loop.close()
                    else:
                        # 同步方法，直接调用
                        close_method()
            except Exception as e:
                logger.warning(f"[{self.uid}][{self.exchange}] 关闭客户端失败: {e}")
            self._client = None
        
        self._is_running = False
        logger.info(f"[{self.uid}][{self.exchange}] 服务已停止")
    
    @property
    def is_running(self) -> bool:
        return self._is_running


class ExchangeContextManager:
    """
    管理单个用户的所有交易所上下文
    """
    
    def __init__(self, uid: str, user_context: Optional['UserContext'] = None):
        self.uid = uid
        self._user_context = user_context
        self._exchanges: Dict[str, ExchangeContext] = {}
    
    def add_exchange(
        self,
        exchange: str,
        api_key: str,
        api_secret: str,
        passphrase: Optional[str] = None,
        is_testnet: bool = False,
        wallet_address: Optional[str] = None,
    ) -> ExchangeContext:
        """添加交易所"""
        if exchange in self._exchanges:
            logger.warning(f"[{self.uid}] 交易所 {exchange} 已存在，将替换")
            self._exchanges[exchange].stop()
        
        ctx = ExchangeContext(
            uid=self.uid,
            exchange=exchange,
            api_key=api_key,
            api_secret=api_secret,
            passphrase=passphrase,
            is_testnet=is_testnet,
            wallet_address=wallet_address,
            user_context=self._user_context,
        )
        
        self._exchanges[exchange] = ctx
        return ctx
    
    def get_exchange(self, exchange: str) -> Optional[ExchangeContext]:
        """获取交易所上下文"""
        return self._exchanges.get(exchange)
    
    def get_all_exchanges(self) -> Dict[str, ExchangeContext]:
        """获取所有交易所上下文"""
        return self._exchanges
    
    def start_all(self):
        """启动所有交易所服务"""
        for exchange, ctx in self._exchanges.items():
            try:
                ctx.start()
            except Exception as e:
                logger.error(f"[{self.uid}][{exchange}] 启动失败: {e}")
    
    def stop_all(self):
        """停止所有交易所服务"""
        for exchange, ctx in self._exchanges.items():
            try:
                ctx.stop()
            except Exception as e:
                logger.error(f"[{self.uid}][{exchange}] 停止失败: {e}")
        self._exchanges.clear()
    
    def get_running_exchanges(self) -> list:
        """获取运行中的交易所列表"""
        return [ex for ex, ctx in self._exchanges.items() if ctx.is_running]
