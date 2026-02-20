# main_async.py
"""
多用户交易系统启动入口 (纯 asyncio 架构)

改造说明：
1. 使用 asyncio.run() 作为唯一入口
2. 所有服务在同一事件循环中运行
3. 通过 lifecycle 统一管理启动和关闭
4. Web 服务器使用 uvicorn 的异步 API
5. 使用 AsyncMultiUserScheduler + TaskGroup 实现用户隔离

用户隔离设计：
- 每个用户有独立的 AsyncUserContext
- 使用 TaskGroup 确保一个用户的错误不影响其他用户
- 每个用户有独立的 batch_cache（不共享全局）

启动方式：
    python main_async.py
"""

import asyncio
import sys
import threading

# P2 优化：减少线程栈大小（默认 1MB -> 512KB）
# 1000 用户场景下可节省约 500MB 内存
try:
    threading.stack_size(512 * 1024)  # 512KB
except Exception:
    pass  # Windows 可能不支持

# 加载 .env 环境变量（必须在其他导入之前）
from dotenv import load_dotenv
load_dotenv()

# 配置日志（使用统一日志配置模块）
from core.logging_config import setup_logging, get_logger
setup_logging()
logger = get_logger(__name__)

# 导入生命周期管理器
from core.lifecycle import lifecycle
from core.config import WEB_HOST, WEB_PORT


# =============================================================================
# 启动处理器
# =============================================================================

@lifecycle.on_startup
async def init_database():
    """初始化数据库"""
    logger.info("初始化数据库...")
    from core.user_db import config_loader
    try:
        # 数据库操作是同步的，使用 to_thread
        await asyncio.to_thread(config_loader.init_tables)
        logger.info("  用户数据库表已就绪")
    except Exception as e:
        logger.warning(f"  数据库初始化失败: {e}")
        logger.info("  继续启动（可能使用已有表）...")


@lifecycle.on_startup
async def init_referral_db():
    """初始化邀请返佣表"""
    logger.info("初始化邀请返佣系统...")
    try:
        from core.referral_db import referral_db
        await asyncio.to_thread(referral_db.init_tables)
        logger.info("  邀请返佣表已就绪")
    except Exception as e:
        logger.warning(f"  邀请返佣表初始化失败: {e}")


@lifecycle.on_startup
async def init_user_context_manager():
    """初始化用户上下文管理器（同步 + 异步）"""
    logger.info("初始化用户上下文管理器...")
    from core.user_context import context_manager
    from core.async_user_context import async_context_manager
    from core.user_db import config_loader
    
    # 配置同步管理器
    context_manager.set_config_loader(config_loader)
    logger.info("  UserContextManager 已配置")
    
    # 配置异步管理器
    async_context_manager.set_config_loader(config_loader)
    async_context_manager.set_sync_manager(context_manager)
    logger.info("  AsyncUserContextManager 已配置")


@lifecycle.on_startup
async def init_http_session():
    """初始化全局 HTTP Session"""
    logger.info("初始化 HTTP Session...")
    from llm.llm_api import init_http_session as _init_http_session
    await _init_http_session()
    logger.info("  HTTP Session 已初始化")


@lifecycle.on_startup
async def init_symbol_availability():
    """初始化交易所符号可用性"""
    logger.info("初始化交易所符号可用性...")
    try:
        from core.symbol_availability import get_symbol_manager
        manager = get_symbol_manager()
        await manager.refresh_all(force=True)
        logger.info("  符号可用性已刷新")
    except Exception as e:
        logger.warning(f"  符号可用性刷新失败: {e}")


@lifecycle.on_startup
async def init_binance_public_cache():
    """
    预热 Binance 公共数据缓存
    
    在用户服务启动前预热，确保：
    1. exchange_info 已缓存（所有用户共享）
    2. 第一个用户请求时不会触发 API 调用
    """
    logger.info("预热 Binance 公共数据缓存...")
    try:
        from core.binance_public_cache import get_binance_public_cache
        cache = get_binance_public_cache()
        # 异步预热 exchange_info
        await cache.get_exchange_info_async()
        stats = cache.get_stats()
        logger.info(f"  Binance 公共缓存已预热: {stats['exchange_info_symbols']} 个交易对")
    except Exception as e:
        logger.warning(f"  Binance 公共缓存预热失败: {e}")


@lifecycle.on_startup
async def init_okx_public_cache():
    """
    预热 OKX 公共数据缓存
    
    在用户服务启动前预热，确保：
    1. instruments 已缓存（所有用户共享，包含合约乘数 ctVal）
    2. 第一个用户请求时不会触发 API 调用
    """
    logger.info("预热 OKX 公共数据缓存...")
    try:
        from core.okx_public_cache import get_okx_public_cache
        cache = get_okx_public_cache()
        # 异步预热 instruments
        await cache.get_instruments_async()
        stats = cache.get_stats()
        logger.info(f"  OKX 公共缓存已预热: {stats['instruments_count']} 个合约")
    except Exception as e:
        logger.warning(f"  OKX 公共缓存预热失败: {e}")


@lifecycle.on_startup
async def init_bitget_public_cache():
    """
    预热 Bitget 公共数据缓存
    
    在用户服务启动前预热，确保：
    1. contracts 已缓存（所有用户共享，包含合约乘数等）
    2. 第一个用户请求时不会触发 API 调用
    """
    logger.info("预热 Bitget 公共数据缓存...")
    try:
        from core.bitget_public_cache import get_bitget_public_cache
        cache = get_bitget_public_cache()
        # 异步预热 contracts
        await cache.get_contracts_async()
        stats = cache.get_stats()
        logger.info(f"  Bitget 公共缓存已预热: {stats['contracts_count']} 个合约")
    except Exception as e:
        logger.warning(f"  Bitget 公共缓存预热失败: {e}")


@lifecycle.on_startup
async def disable_non_admin_testnet():
    """
    启动时检查并关闭非管理员用户的测试网配置
    
    测试网功能仅管理员可用，此检查确保：
    1. 非管理员用户无法使用测试网
    2. 系统重启后自动清理误配置的测试网用户
    """
    logger.info("检查非管理员测试网配置...")
    try:
        from core.user_db import config_loader
        from core.config import ADMIN_USERS
        
        result = config_loader.disable_testnet_for_non_admins(ADMIN_USERS)
        
        if result.get("count", 0) > 0:
            logger.warning(f"  已关闭 {result['count']} 个非管理员用户的测试网配置")
            for u in result.get("affected_users", []):
                logger.info(f"    - [{u['uid']}] {u['username']} / {u['exchange']}")
        else:
            logger.info("  无需处理的测试网配置")
    except Exception as e:
        logger.warning(f"  检查测试网配置失败: {e}")


@lifecycle.on_startup
async def init_strategy_cache():
    """
    预热策略配置缓存
    
    在系统启动时加载所有活跃用户的策略配置到内存，避免：
    1. 首次调度周期的数据库查询风暴
    2. 用户间 LLM 创建时间差异
    """
    logger.info("预热策略配置缓存...")
    try:
        from core.strategy_cache import get_strategy_cache
        
        cache = get_strategy_cache()
        result = cache.preload_all_active_users()
        
        stats = cache.get_stats()
        logger.info(
            f"  策略缓存已预热: {result['total_users']} 用户, {result['total_strategies']} 策略 | "
            f"命中率: {stats['strategy_cache']['hit_rate']}"
        )
    except Exception as e:
        logger.warning(f"  策略缓存预热失败: {e}")


@lifecycle.on_startup
async def init_global_stop_loss_processor():
    """
    初始化全局止损处理器
    
    1000+ 用户规模优化：
    - 单例模式，全局一个处理器
    - 批量 Redis 读取，减少 90% 调用
    - 本地缓存，500ms TTL
    - 队列化下单，限速控制
    
    环境变量：
    - USE_GLOBAL_SLP: 是否使用全局止损处理器（默认 1）
    - USE_EXCHANGE_POOL: 是否使用交易所连接池直接执行订单（默认 0）
    """
    logger.info("初始化全局止损处理器...")
    try:
        import os
        use_global = os.environ.get("USE_GLOBAL_SLP", "1") == "1"
        use_pool = os.environ.get("USE_EXCHANGE_POOL", "0") == "1"
        
        if use_global:
            from trading.global_stop_loss_processor import get_global_stop_loss_processor
            
            # 启动全局止损处理器（可选启用连接池）
            processor = get_global_stop_loss_processor()
            await processor.start(use_pool=use_pool)
            
            if use_pool:
                logger.info("  全局止损处理器已启动 (使用连接池)")
            else:
                logger.info("  全局止损处理器已启动 (使用 multi_exchange_trader)")
        else:
            logger.info("  使用传统止损管理器 (USE_GLOBAL_SLP=0)")
    except Exception as e:
        logger.warning(f"  全局止损处理器初始化失败: {e}")


# =============================================================================
# 关闭处理器
# =============================================================================

@lifecycle.on_shutdown
async def stop_scheduler():
    """停止调度器"""
    logger.info("停止调度器...")
    try:
        # 优先停止异步调度器
        from core.async_multi_user_scheduler import async_scheduler
        async_scheduler.stop()
    except Exception as e:
        logger.debug(f"  停止异步调度器: {e}")
    
    try:
        # 兼容：也停止同步调度器
        from core.multi_user_scheduler import scheduler
        scheduler.stop()
    except Exception as e:
        logger.debug(f"  停止同步调度器: {e}")
    
    # 给调度器一点时间完成当前任务
    await asyncio.sleep(0.5)


@lifecycle.on_shutdown
async def close_http_sessions():
    """关闭 HTTP Sessions"""
    logger.info("关闭 HTTP Sessions...")
    try:
        from llm.llm_api import close_http_session
        await close_http_session()
    except Exception as e:
        logger.debug(f"  关闭 HTTP Session 异常: {e}")
    
    try:
        from analysis.data.volume_stats import close_session as close_volume_stats_session
        await close_volume_stats_session()
    except Exception as e:
        logger.debug(f"  关闭 Volume Stats Session 异常: {e}")
    
    try:
        from core.symbol_availability import get_symbol_manager
        await get_symbol_manager().close()
    except Exception as e:
        logger.debug(f"  关闭 Symbol Availability Session 异常: {e}")
    
    # 关闭 OKX 公共缓存的 HTTP Session
    try:
        from core.okx_public_cache import OKXPublicCache
        okx_cache = OKXPublicCache.get_instance()
        await okx_cache.close()
    except Exception as e:
        logger.debug(f"  关闭 OKX Public Cache Session 异常: {e}")


@lifecycle.on_shutdown
async def stop_all_users():
    """停止所有用户服务"""
    logger.info("停止所有用户服务...")
    
    # 给 SSL transport 时间完成
    await asyncio.sleep(0.3)
    
    # 停止异步用户上下文
    try:
        from core.async_user_context import async_context_manager
        await async_context_manager.stop_all()
    except Exception as e:
        logger.debug(f"  停止异步用户上下文: {e}")
    
    # 停止同步用户上下文
    from core.user_context import context_manager
    context_manager.stop_all()
    
    await asyncio.sleep(0.2)


@lifecycle.on_shutdown
async def stop_commission_service():
    """停止返佣服务"""
    logger.info("停止返佣服务...")
    try:
        from core.commission_service import commission_service
        commission_service.stop()
    except Exception as e:
        logger.debug(f"  停止返佣服务异常: {e}")


@lifecycle.on_shutdown
async def stop_global_stop_loss_processor():
    """停止全局止损处理器"""
    logger.info("停止全局止损处理器...")
    try:
        import os
        use_global = os.environ.get("USE_GLOBAL_SLP", "1") == "1"
        
        if use_global:
            from trading.global_stop_loss_processor import get_global_stop_loss_processor
            
            # 停止全局止损处理器（会自动停止内部的连接池）
            processor = get_global_stop_loss_processor()
            await processor.stop()
            stats = processor.get_stats()
            logger.info(f"  全局止损处理器已停止: {stats}")
    except Exception as e:
        logger.debug(f"  停止全局止损处理器异常: {e}")


@lifecycle.on_shutdown
async def close_async_redis_connection():
    """关闭异步 Redis 连接"""
    logger.info("关闭异步 Redis 连接...")
    try:
        from core.database import close_async_redis
        await close_async_redis()
        logger.debug("  异步 Redis 连接已关闭")
    except Exception as e:
        logger.debug(f"  关闭异步 Redis 连接异常: {e}")


@lifecycle.on_shutdown
async def close_public_caches():
    """关闭公共缓存的线程池"""
    logger.info("关闭公共缓存线程池...")
    
    # Binance 公共缓存
    try:
        from core.binance_public_cache import get_binance_public_cache
        cache = get_binance_public_cache()
        cache.close()
        logger.debug("  Binance 公共缓存已关闭")
    except Exception as e:
        logger.debug(f"  关闭 Binance 公共缓存异常: {e}")
    
    # OKX 公共缓存
    try:
        from core.okx_public_cache import get_okx_public_cache
        cache = get_okx_public_cache()
        cache.close()
        logger.debug("  OKX 公共缓存已关闭")
    except Exception as e:
        logger.debug(f"  关闭 OKX 公共缓存异常: {e}")
    
    # Bitget 公共缓存
    try:
        from core.bitget_public_cache import get_bitget_public_cache
        cache = get_bitget_public_cache()
        cache.close()
        logger.debug("  Bitget 公共缓存已关闭")
    except Exception as e:
        logger.debug(f"  关闭 Bitget 公共缓存异常: {e}")


@lifecycle.on_shutdown
async def close_shared_db_engine():
    """关闭共享数据库引擎"""
    logger.info("关闭共享数据库引擎...")
    try:
        from core.shared_db_engine import close_shared_engine
        close_shared_engine()
        logger.debug("  共享数据库引擎已关闭")
    except Exception as e:
        logger.debug(f"  关闭共享数据库引擎异常: {e}")


# =============================================================================
# 后台任务
# =============================================================================

async def run_scheduler():
    """
    运行调度器（使用 TaskGroup 隔离版本）
    
    选择策略：
    - 优先使用 AsyncMultiUserScheduler（TaskGroup 隔离）
    - 回退到 MultiUserScheduler（asyncio.gather）
    """
    try:
        # 使用新的 TaskGroup 隔离调度器
        from core.async_multi_user_scheduler import async_scheduler
        logger.info("使用 AsyncMultiUserScheduler (TaskGroup 隔离模式)")
        await async_scheduler.start()
    except ImportError:
        # 回退到旧调度器
        logger.warning("AsyncMultiUserScheduler 不可用，使用旧版调度器")
        from core.multi_user_scheduler import scheduler
        await scheduler.start()


async def run_commission_service():
    """运行返佣服务"""
    from core.commission_service import commission_service
    # 如果 commission_service 有 run_async 方法则使用，否则使用线程
    if hasattr(commission_service, 'run_async'):
        await commission_service.run_async()
    else:
        # 兼容旧版本：在线程中运行
        await asyncio.to_thread(commission_service._settlement_loop)


async def run_message_worker():
    """
    运行消息推送服务
    
    优先使用异步版本，回退到同步版本
    """
    try:
        from notifications.notifier import async_message_worker
        logger.info("使用异步消息 worker")
        await async_message_worker()
    except ImportError:
        from notifications.notifier import message_worker
        logger.info("使用同步消息 worker")
        await asyncio.to_thread(message_worker)


async def run_ai500_service():
    """运行 AI500 智能选币服务"""
    from ai500 import update_oi_symbols
    
    # 首次更新
    try:
        await asyncio.to_thread(update_oi_symbols)
    except Exception as e:
        logger.warning(f"AI500 首次更新失败: {e}")
    
    # 定时更新（每3分钟）
    while True:
        try:
            await asyncio.sleep(180)  # 3分钟
            await asyncio.to_thread(update_oi_symbols)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.warning(f"AI500 更新失败: {e}")


async def run_global_market_fetcher():
    """运行全局市场数据获取服务（funding_rate, OI, 24h 等）"""
    from analysis.data.global_market_fetcher import run_global_market_fetcher as fetcher_loop
    await fetcher_loop()


# =============================================================================
# Web 服务器
# =============================================================================

async def run_web_server():
    """运行 Web 服务器"""
    import uvicorn
    from api.web import app
    
    config = uvicorn.Config(
        app,
        host=WEB_HOST,
        port=WEB_PORT,
        log_level="warning",
        access_log=False,
    )
    server = uvicorn.Server(config)
    
    logger.info(f"Web 服务已启动: http://{WEB_HOST}:{WEB_PORT}")
    await server.serve()


# =============================================================================
# 主函数
# =============================================================================

async def main():
    """主入口"""
    print("=" * 60)
    print("TradeV6 多用户交易系统")
    print("纯 asyncio 架构")
    print("=" * 60)
    
    # 注册后台任务
    lifecycle.add_background_task(
        "scheduler",
        run_scheduler,
        restart_on_failure=True,
        restart_delay=10.0
    )
    
    lifecycle.add_background_task(
        "commission",
        run_commission_service,
        restart_on_failure=True,
        restart_delay=30.0
    )
    
    lifecycle.add_background_task(
        "notifier",
        run_message_worker,
        restart_on_failure=True,
        restart_delay=5.0
    )
    
    lifecycle.add_background_task(
        "ai500",
        run_ai500_service,
        restart_on_failure=True,
        restart_delay=60.0
    )
    
    lifecycle.add_background_task(
        "global_market_fetcher",
        run_global_market_fetcher,
        restart_on_failure=True,
        restart_delay=60.0
    )
    
    print("\n系统已启动 | 按 Ctrl+C 停止\n")
    print("=" * 60)
    
    # 运行应用（包含 Web 服务器）
    await lifecycle.run_with_server(run_web_server, "web")
    
    print("\n程序已退出")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n收到 Ctrl+C，正在退出...")
