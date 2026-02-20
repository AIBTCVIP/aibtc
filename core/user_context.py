# user_context.py
"""
多用户上下文管理模块

核心设计：
1. UserContext: 单用户的所有状态和连接
2. UserContextManager: 管理所有用户上下文的生命周期
3. ExchangeContextManager: 管理单用户的多交易所上下文
4. 支持懒加载、LRU淘汰、优雅停止
"""

import asyncio
import threading
import time
from typing import Dict, Optional, List, Any
from dataclasses import dataclass, field
import logging

logger = logging.getLogger(__name__)


@dataclass
class UserConfig:
    """用户配置（从数据库加载）"""
    uid: str
    
    # 交易配置
    max_positions: int = 30
    position_size_pct: float = 3.0  # 单仓最大倍数 (x倍余额，如 3.0 = 单仓 ≤ 3× balance)
    default_leverage: int = 30
    monitor_symbols: List[str] = field(default_factory=list)  # 用户自定义监控币种
    ai500_enabled: bool = False  # 是否启用 AI500 智能选币
    
    # 执行约束配置 (execution_constraints)
    min_rr_ratio: float = 3.0  # 最小风险回报比
    limit_order_min_distance_pct: float = 3.0  # 限价单最小距离百分比
    
    # AI 配置（用户自定义）
    ai_enabled: bool = False  # 默认关闭
    llm_provider: str = "anthropic"       # anthropic / openai / deepseek / openrouter
    llm_model: str = ""  # 默认为空
    llm_api_key: Optional[str] = None     # 用户自己的 API Key（可选）
    llm_base_url: Optional[str] = None    # 自定义 API 地址
    # system_prompt 已迁移到 user_strategy_configs 表，此字段废弃
    # temperature 和 top_p 现在由 llm_client 根据提供商自动设置，此字段废弃
    max_tokens: int = 65536
    
    # 通知配置
    telegram_bot_token: Optional[str] = None  # 用户自己的 Bot Token
    telegram_chat_id: Optional[str] = None
    telegram_topic_id: Optional[str] = None  # Telegram 群组话题/主题 ID
    telegram_enabled: bool = False
    
    # 用户等级
    tier: str = "free"  # free, basic, pro, vip
    
    # 启用的交易所列表 (从 user_exchange_config 表加载)
    enabled_exchanges: List[str] = field(default_factory=list)


class UserContext:
    """
    单用户上下文 - 包含该用户的所有状态和连接
    
    新架构特点：
    1. 每个交易所有独立的 ExchangeContext
    2. 数据按交易所隔离存储
    3. 支持同时在多个交易所交易
    """
    
    def __init__(self, config: UserConfig, init_exchanges: bool = True):
        """
        初始化用户上下文
        
        Args:
            config: 用户配置
            init_exchanges: 是否自动初始化所有启用的交易所上下文。
                          设为 False 时，只创建空的上下文管理器，
                          交易所上下文需要手动通过 add_single_exchange() 添加。
        """
        self.config = config
        self.uid = config.uid

        # 初始化 Redis 用户数据
        from core.redis_manager import RedisDataManager
        if not RedisDataManager.init_user(self.uid):
            logger.debug(f"[{self.uid}] Redis 用户数据已存在")

        # 多交易所上下文管理器
        from core.exchange_context import ExchangeContextManager
        self._exchange_manager = ExchangeContextManager(self.uid, user_context=self)
        
        # 初始化各交易所上下文（可选）
        if init_exchanges:
            self._init_exchange_contexts()
        
        # 主交易所客户端（用于 K线数据获取，优先 Binance）
        self.client = None
        self._init_primary_client()
        
        # 多交易所交易器（懒加载）
        self._multi_trader = None
        
        # 用户级缓存
        self.account_snapshot: Dict[str, Any] = {
            "balance": 0.0,
            "available": 0.0,
            "total_unrealized": 0.0,
            "positions": [],
            "open_limit_orders": []
        }
        self.tp_sl_cache: Dict[str, Dict] = {}
        self.position_records: set = set()
        self.batch_cache: Dict[str, Dict] = {}
        
        # LLM 客户端（懒加载）
        self._llm_client = None
        
        # 状态
        self.is_running = False
        self.last_active_at = time.time()
        self.error_count = 0
        
        logger.info(f"[{self.uid}] UserContext 已创建，启用交易所: {config.enabled_exchanges}")
    
    def _init_exchange_contexts(self):
        """初始化各交易所上下文"""
        from core.user_db import config_loader
        
        for exchange in self.config.enabled_exchanges:
            try:
                exchange_config = config_loader.get_user_exchange_config(self.uid, exchange)
                if not exchange_config or not exchange_config.get('api_key'):
                    logger.warning(f"[{self.uid}] 交易所 {exchange} 配置不完整，跳过")
                    continue
                
                self._exchange_manager.add_exchange(
                    exchange=exchange,
                    api_key=exchange_config['api_key'],
                    api_secret=exchange_config['api_secret'],
                    passphrase=exchange_config.get('passphrase'),
                    is_testnet=exchange_config.get('is_testnet', False),
                    wallet_address=exchange_config.get('wallet_address'),
                )
                logger.info(f"[{self.uid}] 交易所 {exchange} 上下文已创建")
                
            except Exception as e:
                logger.error(f"[{self.uid}] 初始化交易所 {exchange} 失败: {e}")
    
    def _init_primary_client(self):
        """初始化主交易所客户端（用于 K线数据获取）"""
        # 优先使用 Binance
        binance_ctx = self._exchange_manager.get_exchange('binance')
        if binance_ctx:
            try:
                # 获取 Binance 的底层 client
                self.client = binance_ctx.client.client if hasattr(binance_ctx.client, 'client') else None
                if self.client:
                    logger.info(f"[{self.uid}] 主交易所客户端已初始化 (Binance)")
                    return
            except Exception as e:
                logger.error(f"[{self.uid}] 获取 Binance 客户端失败: {e}")
        
        logger.info(f"[{self.uid}] 未配置 Binance，将使用公共 API 获取 K 线数据")
    
    @property
    def exchange_manager(self):
        """获取交易所管理器"""
        return self._exchange_manager
    
    def get_exchange_context(self, exchange: str):
        """获取指定交易所的上下文"""
        return self._exchange_manager.get_exchange(exchange)
    
    @property
    def multi_trader(self):
        """获取多交易所交易器（懒加载）"""
        if self._multi_trader is None:
            from trading.multi_exchange_trader import MultiExchangeTrader
            self._multi_trader = MultiExchangeTrader(
                self.uid, 
                on_auth_failed=self._on_trader_auth_failed
            )
        return self._multi_trader
    
    def _on_trader_auth_failed(self, exchange_name: str, error_msg: str):
        """
        MultiExchangeTrader 认证失败回调
        
        当交易执行时检测到认证错误，触发停止该交易所的服务
        """
        logger.error(f"[{self.uid}] MultiExchangeTrader 检测到 {exchange_name} 认证失败: {error_msg}")
        self.auto_stop_exchange_on_auth_failure(exchange_name, error_msg)
    
    def has_enabled_exchanges(self) -> bool:
        """检查是否有启用的交易所"""
        return len(self.config.enabled_exchanges) > 0
    
    def start(self):
        """启动用户的所有服务"""
        if self.is_running:
            logger.warning(f"[{self.uid}] 已在运行中")
            return
        
        # 检查是否有启用的交易所
        if not self.has_enabled_exchanges():
            logger.warning(f"[{self.uid}] 未配置任何交易所，跳过启动交易服务")
            return
        
        # 启动所有交易所服务
        self._exchange_manager.start_all()
        
        # 更新主客户端引用
        self._init_primary_client()
        
        self.is_running = True
        logger.info(f"[{self.uid}] 服务已启动，运行中的交易所: {self._exchange_manager.get_running_exchanges()}")
    
    def start_exchange(self, exchange: str) -> bool:
        """
        启动单个交易所的服务
        
        Args:
            exchange: 交易所名称 (binance, okx, bitget, hyperliquid)
        
        Returns:
            是否成功启动
        """
        ctx = self._exchange_manager.get_exchange(exchange)
        if not ctx:
            logger.warning(f"[{self.uid}] 交易所 {exchange} 上下文不存在")
            return False
        
        if ctx.is_running:
            logger.warning(f"[{self.uid}] 交易所 {exchange} 已在运行中")
            return True
        
        try:
            ctx.start()
            
            # 如果有任何交易所在运行，更新用户状态
            if self._exchange_manager.get_running_exchanges():
                self.is_running = True
                # 更新主客户端引用
                self._init_primary_client()
            
            logger.info(f"[{self.uid}] 交易所 {exchange} 已启动")
            return True
        except Exception as e:
            logger.error(f"[{self.uid}] 启动交易所 {exchange} 失败: {e}")
            return False
    
    def stop_exchange(self, exchange: str) -> bool:
        """
        停止单个交易所的服务
        
        Args:
            exchange: 交易所名称 (binance, okx, bitget, hyperliquid)
        
        Returns:
            是否成功停止
        """
        ctx = self._exchange_manager.get_exchange(exchange)
        if not ctx:
            logger.warning(f"[{self.uid}] 交易所 {exchange} 上下文不存在")
            return False
        
        if not ctx.is_running:
            logger.warning(f"[{self.uid}] 交易所 {exchange} 未在运行")
            return True
        
        try:
            ctx.stop()
            
            # 如果所有交易所都停止了，更新用户状态
            if not self._exchange_manager.get_running_exchanges():
                self.is_running = False
            
            logger.info(f"[{self.uid}] 交易所 {exchange} 已停止")
            return True
        except Exception as e:
            logger.error(f"[{self.uid}] 停止交易所 {exchange} 失败: {e}")
            return False
    
    def auto_stop_exchange_on_auth_failure(self, exchange: str, error_msg: str) -> None:
        """
        认证失败时自动停止交易所
        
        当 WebSocket 连接因 API Key 无效、权限不足等原因认证失败时，
        自动停止该交易所并更新数据库状态，同时通知用户。
        
        注意：此方法可能在 WebSocket 线程中被调用，需要在独立线程中执行停止操作，
        避免 "cannot join current thread" 错误。
        
        Args:
            exchange: 交易所名称 (binance, okx, bitget, hyperliquid)
            error_msg: 认证失败的错误信息
        """
        import threading
        from core.user_db import config_loader
        
        logger.warning(f"[{self.uid}][{exchange}] Auth failure detected, auto-stopping exchange: {error_msg}")
        
        # 1. 在独立线程中停止交易所服务（避免在 WebSocket 线程中 join 自己）
        def _stop_in_background():
            try:
                self.stop_exchange(exchange)
            except Exception as e:
                logger.error(f"[{self.uid}][{exchange}] Background stop failed: {e}")
        
        stop_thread = threading.Thread(target=_stop_in_background, name=f"auth-fail-stop-{exchange}", daemon=True)
        stop_thread.start()
        
        # 2. 更新数据库状态（标记为未运行）
        try:
            config_loader.set_exchange_running(self.uid, exchange, False)
            logger.info(f"[{self.uid}][{exchange}] Database updated: running=False")
        except Exception as e:
            logger.error(f"[{self.uid}][{exchange}] Failed to update database: {e}")
        
        # 3. 发送 Telegram 通知
        try:
            from notifications.notifier import send_telegram_message_for_user
            message = (
                f"[Auto-Stop] Exchange {exchange.upper()} stopped due to auth failure:\n"
                f"{error_msg}\n\n"
                f"Please check your API key settings."
            )
            send_telegram_message_for_user(self, message)
            logger.info(f"[{self.uid}][{exchange}] Telegram notification sent")
        except Exception as e:
            logger.warning(f"[{self.uid}][{exchange}] Failed to send Telegram notification: {e}")
    
    def add_single_exchange(self, exchange: str) -> bool:
        """
        添加单个交易所的上下文（不启动）
        
        用于在启动单个交易所时，只创建该交易所的上下文而不影响其他交易所。
        
        Args:
            exchange: 交易所名称 (binance, okx, bitget, hyperliquid)
        
        Returns:
            是否成功添加
        """
        from core.user_db import config_loader
        
        # 检查是否已存在
        if self._exchange_manager.get_exchange(exchange):
            logger.debug(f"[{self.uid}] 交易所 {exchange} 上下文已存在")
            return True
        
        try:
            exchange_config = config_loader.get_user_exchange_config(self.uid, exchange)
            if not exchange_config or not exchange_config.get('api_key'):
                logger.warning(f"[{self.uid}] 交易所 {exchange} 配置不完整")
                return False
            
            self._exchange_manager.add_exchange(
                exchange=exchange,
                api_key=exchange_config['api_key'],
                api_secret=exchange_config['api_secret'],
                passphrase=exchange_config.get('passphrase'),
                is_testnet=exchange_config.get('is_testnet', False),
                wallet_address=exchange_config.get('wallet_address'),
            )
            logger.info(f"[{self.uid}] 交易所 {exchange} 上下文已创建")
            return True
            
        except Exception as e:
            logger.error(f"[{self.uid}] 添加交易所 {exchange} 上下文失败: {e}")
            return False
    
    def get_exchange_status(self, exchange: str) -> Optional[Dict[str, Any]]:
        """
        获取单个交易所的运行状态
        
        Args:
            exchange: 交易所名称
        
        Returns:
            交易所状态信息，包含 is_running 等字段
        """
        ctx = self._exchange_manager.get_exchange(exchange)
        if not ctx:
            return None
        
        return {
            "exchange": exchange,
            "is_running": ctx.is_running,
            "has_cycle_store": ctx.cycle_store is not None,
            "has_position_auditor": ctx.position_auditor is not None,
        }
    
    def get_all_exchange_status(self) -> Dict[str, Dict[str, Any]]:
        """
        获取所有交易所的运行状态
        
        Returns:
            {exchange: status_dict, ...}
        """
        result = {}
        for exchange, ctx in self._exchange_manager.get_all_exchanges().items():
            result[exchange] = {
                "exchange": exchange,
                "is_running": ctx.is_running,
                "has_cycle_store": ctx.cycle_store is not None,
                "has_position_auditor": ctx.position_auditor is not None,
            }
        return result
    
    def stop(self):
        """停止用户的所有服务"""
        if not self.is_running:
            return
        
        # 停止所有交易所服务
        self._exchange_manager.stop_all()
        
        # 关闭 LLM 客户端
        self.reset_llm_client()
        
        # 关闭多交易所交易器
        if self._multi_trader:
            try:
                # 尝试获取当前运行的事件循环
                try:
                    loop = asyncio.get_running_loop()
                    # 已有运行的循环，创建任务但不等待（避免阻塞）
                    # 使用 ensure_future 让清理在后台进行
                    asyncio.ensure_future(self._multi_trader.close(), loop=loop)
                except RuntimeError:
                    # 没有运行的循环，创建新的
                    loop = asyncio.new_event_loop()
                    try:
                        loop.run_until_complete(self._multi_trader.close())
                    except Exception as close_err:
                        logger.debug(f"[{self.uid}] 关闭 multi_trader 时异常: {close_err}")
                    finally:
                        loop.close()
            except Exception as e:
                logger.warning(f"[{self.uid}] 关闭多交易所交易器失败: {e}")
            self._multi_trader = None
        
        self.client = None
        self.is_running = False
        logger.info(f"[{self.uid}] 服务已停止")
    
    def touch(self):
        """更新最后活跃时间"""
        self.last_active_at = time.time()
    
    def is_stale(self, ttl_seconds: int = 3600) -> bool:
        """检查是否过期（默认1小时无活动）"""
        return time.time() - self.last_active_at > ttl_seconds
    
    def get_llm_client(self, strategy_id: str = None):
        """
        获取用户的 LLM 客户端
        
        Args:
            strategy_id: 如果指定策略ID，则使用该策略的配置创建新客户端。
                        调用方需要在使用后关闭客户端。
                        如果不指定，则使用用户全局配置（缓存的客户端）。
        """
        # 如果指定了策略ID，每次创建新客户端
        if strategy_id:
            from core.strategy_cache import get_strategy_cache
            cache = get_strategy_cache()
            
            # 创建新的 LLM 客户端（不缓存，调用方需要关闭）
            # temperature 和 top_p 使用提供商默认值
            return cache.get_llm_client(
                uid=self.uid,
                strategy_id=strategy_id,
                max_tokens=self.config.max_tokens,
            )
        
        # 使用全局配置（保持原有逻辑）
        if self._llm_client is None:
            self._llm_client = self._create_llm_client()
        return self._llm_client
    
    def _get_ai_strategy(self, strategy_id: str) -> Optional[Dict]:
        """根据策略ID获取 AI 策略配置（使用缓存）"""
        from core.strategy_cache import get_strategy_cache
        return get_strategy_cache().get_strategy(self.uid, strategy_id)
    
    def _get_exchange_ai_strategy(self, exchange: str) -> Optional[Dict]:
        """获取交易所的 AI 策略配置"""
        from core.user_db import config_loader
        
        # 获取交易所配置的策略 ID
        strategy_id = config_loader.get_exchange_ai_strategy_id(self.uid, exchange)
        if not strategy_id:
            return None
        
        # 获取策略详情（使用缓存）
        from core.strategy_cache import get_strategy_cache
        return get_strategy_cache().get_strategy(self.uid, strategy_id)

    def reset_llm_client(self):
        """重置 LLM 客户端"""
        if self._llm_client:
            try:
                if hasattr(self._llm_client, 'close_sync'):
                    self._llm_client.close_sync()
                else:
                    try:
                        loop = asyncio.get_running_loop()
                        if loop.is_running():
                            loop.create_task(self._llm_client.close())
                        else:
                            asyncio.run(self._llm_client.close())
                    except RuntimeError:
                        asyncio.run(self._llm_client.close())
            except Exception as e:
                logger.warning(f"[{self.uid}] 关闭 LLM 客户端失败: {e}")
            finally:
                self._llm_client = None
    
    def _create_llm_client(self):
        """根据用户全局配置创建 LLM 客户端"""
        from llm.llm_client import create_llm_client
        # temperature 和 top_p 使用提供商默认值
        return create_llm_client(
            provider=self.config.llm_provider,
            model=self.config.llm_model,
            api_key=self.config.llm_api_key,
            base_url=self.config.llm_base_url,
            max_tokens=self.config.max_tokens,
        )
    
    def _create_llm_client_from_strategy(self, strategy: Dict):
        """根据 AI 策略创建 LLM 客户端"""
        from llm.llm_client import create_llm_client
        
        provider = strategy.get('llm_provider', 'anthropic')
        model = strategy.get('llm_model', '')
        base_url = strategy.get('llm_base_url', '')
        
        # 用户自定义 LLM 参数（None 表示使用提供商默认值）
        temperature = strategy.get('temperature')
        top_p = strategy.get('top_p')
        max_tokens = strategy.get('max_tokens') or self.config.max_tokens
        
        logger.info(f"[{self.uid}] 创建 LLM 客户端: provider={provider}, model={model}, base_url={base_url}, temperature={temperature}, top_p={top_p}, max_tokens={max_tokens}")
        
        return create_llm_client(
            provider=provider,
            model=model,
            api_key=strategy.get('llm_api_key'),
            base_url=base_url,
            temperature=temperature,
            top_p=top_p,
            max_tokens=max_tokens,
        )
    
    def get_system_prompt(self, strategy_id: str | None = None) -> str:
        """
        获取完整的 system prompt
        
        v5.0 新架构：
        - 硬编码部分：OUTPUT_ANCHOR + CORE_DATA_SCHEMA + CORE_ACTIONS + CORE_OUTPUT_FORMAT
        - 用户策略：从 user_ai_strategies 表加载（strategy_preset + strategy_overrides）
        
        Args:
            strategy_id: 策略 ID，如果指定则使用该策略的配置
        """
        from llm.prompt_templates import build_system_prompt
        
        # 加载用户策略（从数据库）
        user_strategy = self._load_user_strategy_sync(strategy_id)
        
        # 构建完整 prompt
        return build_system_prompt(user_strategy)
    
    def _load_user_strategy_sync(self, strategy_id: str | None = None) -> str | None:
        """
        同步加载用户策略
        
        从缓存或数据库加载指定策略的配置，并组装成策略文本
        
        Args:
            strategy_id: 策略 ID
        """
        from core.strategy_cache import get_strategy_cache
        from core.user_db import config_loader
        from llm.prompt_templates import STRATEGY_CATEGORIES, CATEGORY_TITLES
        import json
        
        try:
            # 如果没有指定策略 ID，返回 None（使用默认）
            if not strategy_id:
                return None
            
            # 从缓存获取策略配置（缓存会自动从数据库加载）
            strategy = get_strategy_cache().get_strategy(self.uid, strategy_id)
            
            if not strategy:
                return None
            
            preset_name = strategy.get("strategy_preset")
            strategy_overrides = strategy.get("strategy_overrides")
            
            # 解析 strategy_overrides
            if strategy_overrides and isinstance(strategy_overrides, str):
                try:
                    strategy_overrides = json.loads(strategy_overrides)
                except json.JSONDecodeError:
                    strategy_overrides = None
            
            # 加载预设模板（这个不缓存，因为预设模板很少变化）
            category_contents = {}
            if preset_name:
                templates = config_loader.get_strategy_templates(preset_name)
                for t in templates:
                    category_contents[t["category"]] = t["content"]
            
            # 应用自定义覆盖
            if strategy_overrides:
                for category, content in strategy_overrides.items():
                    if content and content.strip():
                        category_contents[category] = content
            
            # 如果没有任何内容，返回 None
            if not category_contents:
                return None
            
            # 组装策略
            parts = []
            for category in STRATEGY_CATEGORIES:
                content = category_contents.get(category)
                if content and content.strip():
                    title = CATEGORY_TITLES.get(category, f"## {category.replace('_', ' ').title()}")
                    parts.append(f"{title}\n\n{content.strip()}")
            
            return "\n\n".join(parts) if parts else None
            
        except Exception as e:
            logger.warning(f"[{self.uid}] Failed to load user strategy: {e}")
            return None
    
    def has_ai_enabled_for_exchange(self, exchange: str) -> bool:
        """
        检查指定交易所是否启用了 AI
        
        优先检查交易所的 AI 策略配置，如果没有则使用全局 ai_enabled
        """
        from core.user_db import config_loader
        
        # 检查交易所是否配置了 AI 策略
        strategy_id = config_loader.get_exchange_ai_strategy_id(self.uid, exchange)
        if strategy_id:
            # 有策略配置，表示启用了 AI
            return True
        
        # 没有策略配置，使用全局 ai_enabled
        return self.config.ai_enabled
    
    def get_monitor_symbols(self, exchange: str = None) -> List[str]:
        """
        获取用户监控的币种列表
        
        Args:
            exchange: 如果指定，则获取该交易所的监控币种
        
        包含：
        1. 交易所级别配置的监控币种（如果指定了 exchange，或所有启用的交易所）
        2. 用户级别配置的监控币种（全局配置）
        3. 当前持仓的币种（从 Redis 读取，WebSocket 实时更新）
        4. 当前挂单的币种（从 Redis 读取，WebSocket 实时更新）
        5. AI500 智能选币（如果启用）
        """
        symbols = set()
        
        # 1. 从交易所级别配置获取监控币种
        from core.user_db import config_loader
        
        if exchange:
            # 只获取指定交易所的配置
            exchanges_to_check = [exchange]
        else:
            # 获取所有启用的交易所的配置
            exchanges_to_check = self.config.enabled_exchanges or []
        
        for ex in exchanges_to_check:
            exchange_config = config_loader.get_user_exchange_config(self.uid, ex)
            if exchange_config:
                exchange_symbols = exchange_config.get('monitor_symbols', [])
                if exchange_symbols:
                    symbols.update(exchange_symbols)
                
                # 检查交易所级别的 AI500
                if exchange_config.get('ai500_enabled', False):
                    try:
                        from core.database import redis_client
                        ai500_symbols = redis_client.lrange("AI500_SYMBOLS", 0, -1)
                        if ai500_symbols:
                            symbols.update(ai500_symbols)
                    except Exception as e:
                        logger.warning(f"[{self.uid}][{ex}] 获取 AI500 币种失败: {e}")
        
        # 2. 从用户配置获取监控币种
        if not symbols and self.config.monitor_symbols:
            symbols.update(self.config.monitor_symbols)
        
        # 3. 添加持仓币种（从 Redis 缓存读取，WebSocket 实时更新）
        from core.pf_compatibility import pf_compat
        if exchange:
            positions = pf_compat.get_pf_pos(self.uid, exchange)
            for key, pos in positions.items():
                symbol = pos.get("symbol")
                if symbol:
                    symbols.add(symbol)
        else:
            for ex in exchanges_to_check:
                positions = pf_compat.get_pf_pos(self.uid, ex)
                for key, pos in positions.items():
                    symbol = pos.get("symbol")
                    if symbol:
                        symbols.add(symbol)
        
        # 4. 添加挂单币种（从 Redis 缓存读取，WebSocket 实时更新）
        if exchange:
            open_orders = pf_compat.get_pf_open_orders(self.uid, exchange)
            for order_id, order in open_orders.items():
                symbol = order.get("symbol")
                if symbol:
                    symbols.add(symbol)
        else:
            for ex in exchanges_to_check:
                open_orders = pf_compat.get_pf_open_orders(self.uid, ex)
                for order_id, order in open_orders.items():
                    symbol = order.get("symbol")
                    if symbol:
                        symbols.add(symbol)
        
        # 5. 用户级别 AI500 智能选币（只有在交易所没有独立配置时才检查）
        if not exchange and getattr(self.config, 'ai500_enabled', False):
            try:
                from core.database import redis_client
                ai500_symbols = redis_client.lrange("AI500_SYMBOLS", 0, -1)
                if ai500_symbols:
                    symbols.update(ai500_symbols)
            except Exception as e:
                logger.warning(f"[{self.uid}] 获取 AI500 币种失败: {e}")
        
        return list(symbols)
    
    def get_all_positions(self) -> Dict[str, List[Dict]]:
        """
        获取所有交易所的持仓
        
        Returns:
            {
                "binance": [{"symbol": "BTCUSDT", "side": "LONG", ...}, ...],
                "okx": [...],
            }
        """
        result = {}
        for exchange, ctx in self._exchange_manager.get_all_exchanges().items():
            if ctx.position_store:
                positions = ctx.position_store.get_positions()
                result[exchange] = list(positions.values())
        return result
    
    def get_all_closed_trades(self, limit: int = 50) -> Dict[str, List[Dict]]:
        """
        获取所有交易所的已关闭交易
        
        Returns:
            {
                "binance": [{...}, ...],
                "okx": [...],
            }
        """
        result = {}
        for exchange, ctx in self._exchange_manager.get_all_exchanges().items():
            if ctx.position_store:
                closed = ctx.position_store.get_closed_trades()
                # 按时间排序，取最近的
                sorted_trades = sorted(
                    closed.values(),
                    key=lambda x: int(x.get("closeTimeMs", 0) or 0),
                    reverse=True
                )
                result[exchange] = sorted_trades[:limit]
        return result


class UserContextManager:
    """
    用户上下文管理器 - 单例模式
    
    功能：
    1. 按需创建/获取用户上下文
    2. LRU 淘汰长期不活跃的用户
    3. 统一管理所有用户的生命周期
    """
    
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
        
        self._contexts: Dict[str, UserContext] = {}
        self._config_loader = None
        self._max_contexts = 1200  # 支持 1000+ 用户，留 200 缓冲
        self._cleanup_interval = 300
        self._context_ttl = 3600
        
        self._initialized = True
        logger.info("UserContextManager 已初始化")
    
    def set_config_loader(self, loader):
        """设置配置加载器"""
        self._config_loader = loader
    
    def get_context(self, uid: str, auto_start: bool = True, init_exchanges: bool = True) -> Optional[UserContext]:
        """
        获取用户上下文（懒加载）
        
        Args:
            uid: 用户 ID
            auto_start: 创建后是否自动启动所有交易所服务
            init_exchanges: 创建时是否初始化所有启用的交易所上下文。
                          设为 False 时，创建空的上下文，需要手动添加交易所。
        """
        if uid in self._contexts:
            ctx = self._contexts[uid]
            ctx.touch()
            return ctx
        
        config = self._load_user_config(uid)
        if not config:
            logger.warning(f"用户 {uid} 配置不存在")
            return None
        
        if len(self._contexts) >= self._max_contexts:
            self._evict_stale_contexts()
        
        ctx = UserContext(config, init_exchanges=init_exchanges)
        self._contexts[uid] = ctx
        
        if auto_start and init_exchanges:
            ctx.start()
        
        return ctx
    
    def _load_user_config(self, uid: str) -> Optional[UserConfig]:
        """从数据库加载用户配置"""
        if self._config_loader is None:
            logger.error("配置加载器未设置")
            return None
        return self._config_loader.load(uid)
    
    def _evict_stale_contexts(self):
        """淘汰过期的上下文"""
        stale_uids = [
            uid for uid, ctx in self._contexts.items()
            if ctx.is_stale(self._context_ttl)
        ]
        
        for uid in stale_uids:
            self.remove_context(uid)
            logger.info(f"[{uid}] 上下文已淘汰（不活跃）")
        
        if len(self._contexts) >= self._max_contexts:
            sorted_contexts = sorted(
                self._contexts.items(),
                key=lambda x: x[1].last_active_at
            )
            to_evict = len(self._contexts) - self._max_contexts + 10
            for uid, ctx in sorted_contexts[:to_evict]:
                self.remove_context(uid)
                logger.info(f"[{uid}] 上下文已淘汰（超限）")
    
    def remove_context(self, uid: str):
        """移除用户上下文"""
        if uid in self._contexts:
            self._contexts[uid].stop()
            del self._contexts[uid]
    
    def get_all_running_uids(self) -> List[str]:
        """获取所有运行中的用户ID"""
        return [uid for uid, ctx in self._contexts.items() if ctx.is_running]
    
    def stop_all(self):
        """停止所有用户服务"""
        for uid in list(self._contexts.keys()):
            self.remove_context(uid)
        logger.info("所有用户服务已停止")
    
    def get_stats(self) -> Dict:
        """获取管理器统计信息"""
        return {
            "total_contexts": len(self._contexts),
            "running_contexts": sum(1 for c in self._contexts.values() if c.is_running),
            "max_contexts": self._max_contexts,
            "context_ttl": self._context_ttl,
        }


# 全局单例
context_manager = UserContextManager()


def get_user_context(uid: str) -> Optional[UserContext]:
    """便捷函数：获取用户上下文"""
    return context_manager.get_context(uid)
