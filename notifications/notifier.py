"""
通知服务模块

功能：
1. 发送 Telegram 消息（同步/异步）
2. 消息队列处理
3. 多用户支持

改造说明：
- 新增 asyncio.Queue 支持纯 asyncio 模式
- 新增 async_message_worker 异步消息处理
- 保留 threading.Queue 兼容旧代码
"""

import asyncio
import time
import logging
from typing import TYPE_CHECKING, Optional
from queue import Queue

logger = logging.getLogger(__name__)

# 可选导入 httpx（异步 HTTP 客户端）
try:
    import httpx
    HTTPX_AVAILABLE = True
except ImportError:
    HTTPX_AVAILABLE = False
    import requests

from core.config import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, TOPIC_MAP, TELEGRAM_ENABLED, SYSTEM_TELEGRAM_BOT_TOKEN, TELEGRAM_PROXY

if TYPE_CHECKING:
    from core.user_context import UserContext

# 同步队列（threading 模式）
message_queue = Queue()

# 异步队列（asyncio 模式）
# 注意：asyncio.Queue() 的创建是线程安全的，且是轻量级操作
# 即使多个线程同时创建，也只会有一个被保留（通过 global 赋值的原子性）
_async_message_queue: Optional[asyncio.Queue] = None
_async_worker_running = False
_async_worker_lock: Optional[asyncio.Lock] = None  # 延迟初始化，保护 _async_worker_running
import threading
_async_worker_init_lock = threading.Lock()  # 保护 asyncio.Lock 的创建


def send_telegram_message(message: str, topic: str | None = None, chat_id: str | None = None):
    """
    使用全局配置发送 Telegram 消息（旧版兼容）
    
    topic: TOPIC_MAP 里的 key，例如 "Trading-signals"
    chat_id: 指定发送到的聊天 ID（用户特定）
    不传 topic 或 topic 不存在 -> 发到主聊天或指定聊天
    """
    # 检查是否启用 Telegram（如果没有指定 chat_id）
    if not TELEGRAM_ENABLED and not chat_id:
        logger.debug(f"Telegram 已禁用，消息未发送: {message[:50]}...")
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"

    # 使用指定的 chat_id 或全局配置
    target_chat_id = chat_id or TELEGRAM_CHAT_ID

    payload = {
        "chat_id": target_chat_id,
        "text": message,
        "disable_web_page_preview": True,
    }

    # 如果指定了 topic，就把消息发到对应话题
    if topic:
        thread_id = TOPIC_MAP.get(topic)
        if thread_id is None:
            logger.warning(f"未知topic: {topic}，将发送到主聊天")
        else:
            payload["message_thread_id"] = int(thread_id)

    try:
        r = requests.post(url, json=payload, timeout=10)
        r.raise_for_status()
        data = r.json()
        if not data.get("ok"):
            logger.warning(f"TG返回失败: {data}")
    except Exception as e:
        logger.warning(f"TG发送失败: {e}")


def send_telegram_message_for_user(
    ctx: "UserContext",
    message: str,
    topic_id: Optional[str] = None
) -> bool:
    """
    使用系统 Bot Token + 用户 Chat ID 发送 Telegram 消息（多用户支持）
    
    Args:
        ctx: 用户上下文，包含 telegram_chat_id, telegram_topic_id
        message: 消息内容
        topic_id: 可选的话题 ID（覆盖用户配置中的 topic_id）
    
    Returns:
        bool: 是否发送成功
    """
    config = ctx.config
    
    # 检查是否启用 Telegram
    if not config.telegram_enabled:
        logger.debug(f"[{ctx.uid}] Telegram 已禁用，消息未发送")
        return False
    
    # 使用系统级 Bot Token
    bot_token = SYSTEM_TELEGRAM_BOT_TOKEN
    if not bot_token:
        logger.warning(f"[{ctx.uid}] 系统未配置 Telegram Bot Token")
        return False
    
    if not config.telegram_chat_id:
        logger.warning(f"[{ctx.uid}] 未配置 Telegram Chat ID")
        return False
    
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    
    payload = {
        "chat_id": config.telegram_chat_id,
        "text": message,
        "disable_web_page_preview": True,
    }
    
    # 使用传入的 topic_id 或用户配置的 topic_id
    effective_topic_id = topic_id or getattr(config, 'telegram_topic_id', None)
    if effective_topic_id:
        try:
            payload["message_thread_id"] = int(effective_topic_id)
        except (ValueError, TypeError):
            logger.warning(f"[{ctx.uid}] 无效的 topic_id: {effective_topic_id}")
    
    try:
        r = requests.post(url, json=payload, timeout=10)
        r.raise_for_status()
        data = r.json()
        if not data.get("ok"):
            logger.warning(f"[{ctx.uid}] TG返回失败: {data}")
            return False
        return True
    except Exception as e:
        logger.warning(f"[{ctx.uid}] TG发送失败: {e}")
        return False


def queue_message_for_user(
    ctx: "UserContext",
    message: str,
    topic_id: Optional[str] = None
):
    """
    把用户消息入队（多用户支持）
    使用系统级 Bot Token + 用户 Chat ID
    """
    config = ctx.config
    if not config.telegram_enabled:
        return
    
    # 使用系统级 Bot Token
    if not SYSTEM_TELEGRAM_BOT_TOKEN or not config.telegram_chat_id:
        return
    
    message_queue.put({
        "text": message,
        "uid": ctx.uid,
        "bot_token": SYSTEM_TELEGRAM_BOT_TOKEN,  # 使用系统 Bot Token
        "chat_id": config.telegram_chat_id,
        "topic_id": topic_id or getattr(config, 'telegram_topic_id', None),
    })


def queue_message(msg: str, topic: str | None = None, chat_id: str | None = None):
    """
    把消息入队，topic 和 chat_id 可选（旧版兼容）
    """
    # 检查是否启用 Telegram
    if not TELEGRAM_ENABLED and not chat_id:
        return

    message_queue.put({"text": msg, "topic": topic, "chat_id": chat_id})


def message_worker():
    """
    消息发送工作线程
    支持两种消息格式：
    1. 旧版（使用全局配置）: {"text": ..., "topic": ..., "chat_id": ...}
    2. 新版（多用户）: {"text": ..., "uid": ..., "bot_token": ..., "chat_id": ..., "topic_id": ...}
    """
    while True:
        item = message_queue.get()
        try:
            if item:
                # 检查是否是多用户格式
                if "bot_token" in item:
                    # 多用户格式 - 使用用户自己的 bot token
                    _send_user_message(item)
                else:
                    # 旧版格式 - 使用全局配置
                    chat_id = item.get("chat_id")
                    send_telegram_message(item["text"], item.get("topic"), chat_id)
                time.sleep(2)
        finally:
            message_queue.task_done()


def _send_user_message(item: dict):
    """
    发送用户消息（内部函数）
    """
    bot_token = item.get("bot_token")
    chat_id = item.get("chat_id")
    text = item.get("text", "")
    topic_id = item.get("topic_id")
    uid = item.get("uid", "unknown")
    
    if not bot_token or not chat_id:
        logger.warning(f"[{uid}] 缺少必要的 Telegram 配置")
        return
    
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    
    payload = {
        "chat_id": chat_id,
        "text": text,
        "disable_web_page_preview": True,
    }
    
    if topic_id:
        try:
            payload["message_thread_id"] = int(topic_id)
        except (ValueError, TypeError) as e:
            logger.debug(f"[{uid}] 无效的 topic_id: {topic_id}, error: {e}")
    
    try:
        if HTTPX_AVAILABLE:
            import httpx
            with httpx.Client(timeout=10) as client:
                r = client.post(url, json=payload)
                r.raise_for_status()
                data = r.json()
        else:
            import requests
            r = requests.post(url, json=payload, timeout=10)
            r.raise_for_status()
            data = r.json()
        if not data.get("ok"):
            logger.warning(f"[{uid}] TG返回失败: {data}")
    except Exception as e:
        logger.warning(f"[{uid}] TG发送失败: {e}")


# ==================== 纯 Asyncio 模式 ====================

def get_async_queue() -> asyncio.Queue:
    """获取异步消息队列（懒加载）
    
    注意：asyncio.Queue() 创建是轻量级且线程安全的操作。
    即使在极端情况下多个线程同时进入，最多只会创建多个 Queue 实例，
    但最终只有一个会被保留（通过 global 赋值）。这是可接受的，
    因为 Queue 创建成本很低，且这种情况极少发生。
    """
    global _async_message_queue
    if _async_message_queue is None:
        _async_message_queue = asyncio.Queue()
    return _async_message_queue


async def queue_message_async(
    message: str,
    topic: Optional[str] = None,
    chat_id: Optional[str] = None
):
    """
    异步入队消息（旧版兼容格式）
    """
    if not TELEGRAM_ENABLED and not chat_id:
        return
    
    queue = get_async_queue()
    await queue.put({"text": message, "topic": topic, "chat_id": chat_id})


async def queue_message_for_user_async(
    ctx: "UserContext",
    message: str,
    topic_id: Optional[str] = None
):
    """
    异步入队用户消息（多用户支持）
    """
    config = ctx.config
    if not config.telegram_enabled:
        return
    
    if not SYSTEM_TELEGRAM_BOT_TOKEN or not config.telegram_chat_id:
        return
    
    queue = get_async_queue()
    await queue.put({
        "text": message,
        "uid": ctx.uid,
        "bot_token": SYSTEM_TELEGRAM_BOT_TOKEN,
        "chat_id": config.telegram_chat_id,
        "topic_id": topic_id or getattr(config, 'telegram_topic_id', None),
    })


async def send_telegram_message_async(
    message: str,
    topic: Optional[str] = None,
    chat_id: Optional[str] = None
) -> bool:
    """
    异步发送 Telegram 消息（直接发送，不入队）
    """
    if not TELEGRAM_ENABLED and not chat_id:
        logger.debug(f"Telegram 已禁用，消息未发送: {message[:50]}...")
        return False
    
    if not HTTPX_AVAILABLE:
        # 降级到同步模式
        from core.async_helpers import run_sync
        await run_sync(lambda: send_telegram_message(message, topic, chat_id))
        return True
    
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    target_chat_id = chat_id or TELEGRAM_CHAT_ID
    
    payload = {
        "chat_id": target_chat_id,
        "text": message,
        "disable_web_page_preview": True,
    }
    
    if topic:
        thread_id = TOPIC_MAP.get(topic)
        if thread_id is not None:
            payload["message_thread_id"] = int(thread_id)
    
    try:
        # 配置代理（如果有）
        client_kwargs = {"timeout": 10}
        if TELEGRAM_PROXY:
            client_kwargs["proxies"] = {"all://": TELEGRAM_PROXY}
        
        async with httpx.AsyncClient(**client_kwargs) as client:
            r = await client.post(url, json=payload)
            r.raise_for_status()
            data = r.json()
            if not data.get("ok"):
                logger.warning(f"TG返回失败: {data}")
                return False
            return True
    except Exception as e:
        logger.warning(f"TG发送失败: {e}")
        return False


async def send_telegram_message_for_user_async(
    ctx: "UserContext",
    message: str,
    topic_id: Optional[str] = None
) -> bool:
    """
    异步发送用户 Telegram 消息（直接发送，不入队）
    """
    config = ctx.config
    
    if not config.telegram_enabled:
        logger.debug(f"[{ctx.uid}] Telegram 已禁用，消息未发送")
        return False
    
    bot_token = SYSTEM_TELEGRAM_BOT_TOKEN
    if not bot_token:
        logger.warning(f"[{ctx.uid}] 系统未配置 Telegram Bot Token")
        return False
    
    if not config.telegram_chat_id:
        logger.warning(f"[{ctx.uid}] 未配置 Telegram Chat ID")
        return False
    
    if not HTTPX_AVAILABLE:
        # 降级到同步模式
        from core.async_helpers import run_sync
        result = await run_sync(
            lambda: send_telegram_message_for_user(ctx, message, topic_id)
        )
        return result
    
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    
    payload = {
        "chat_id": config.telegram_chat_id,
        "text": message,
        "disable_web_page_preview": True,
    }
    
    effective_topic_id = topic_id or getattr(config, 'telegram_topic_id', None)
    if effective_topic_id:
        try:
            payload["message_thread_id"] = int(effective_topic_id)
        except (ValueError, TypeError):
            logger.warning(f"[{ctx.uid}] 无效的 topic_id: {effective_topic_id}")
    
    try:
        # 配置代理（如果有）
        client_kwargs = {"timeout": 10}
        if TELEGRAM_PROXY:
            client_kwargs["proxies"] = {"all://": TELEGRAM_PROXY}
        
        async with httpx.AsyncClient(**client_kwargs) as client:
            r = await client.post(url, json=payload)
            r.raise_for_status()
            data = r.json()
            if not data.get("ok"):
                logger.warning(f"[{ctx.uid}] TG返回失败: {data}")
                return False
            return True
    except Exception as e:
        logger.warning(f"[{ctx.uid}] TG发送失败: {e}")
        return False


async def async_message_worker():
    """
    异步消息发送 worker
    
    在 main_async.py 中通过 lifecycle 启动：
    lifecycle.add_background_task(async_message_worker, "notifier")
    """
    global _async_worker_running, _async_worker_lock
    
    # 延迟初始化 asyncio.Lock
    if _async_worker_lock is None:
        with _async_worker_init_lock:
            if _async_worker_lock is None:
                _async_worker_lock = asyncio.Lock()
    
    async with _async_worker_lock:
        if _async_worker_running:
            logger.warning("async_message_worker 已在运行")
            return
        _async_worker_running = True
    
    queue = get_async_queue()
    logger.info("异步消息 worker 启动")
    
    try:
        while True:
            try:
                # 等待消息，超时 60 秒检查一次是否应该退出
                try:
                    item = await asyncio.wait_for(queue.get(), timeout=60)
                except asyncio.TimeoutError:
                    continue
                
                if item is None:  # 停止信号
                    break
                
                # 处理消息
                if "bot_token" in item:
                    # 多用户格式
                    await _send_user_message_async(item)
                else:
                    # 旧版格式
                    await send_telegram_message_async(
                        item["text"],
                        item.get("topic"),
                        item.get("chat_id")
                    )
                
                # 限速：每条消息之间间隔 2 秒
                await asyncio.sleep(2)
                
            except asyncio.CancelledError:
                logger.info("异步消息 worker 被取消")
                break
            except Exception as e:
                logger.error(f"异步消息 worker 异常: {e}")
                await asyncio.sleep(1)
    finally:
        _async_worker_running = False
        logger.info("异步消息 worker 停止")


async def _send_user_message_async(item: dict):
    """
    异步发送用户消息（内部函数）
    """
    bot_token = item.get("bot_token")
    chat_id = item.get("chat_id")
    text = item.get("text", "")
    topic_id = item.get("topic_id")
    uid = item.get("uid", "unknown")
    
    if not bot_token or not chat_id:
        logger.warning(f"[{uid}] 缺少必要的 Telegram 配置")
        return
    
    if not HTTPX_AVAILABLE:
        # 降级到同步模式
        from core.async_helpers import run_sync
        await run_sync(lambda: _send_user_message(item))
        return
    
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    
    payload = {
        "chat_id": chat_id,
        "text": text,
        "disable_web_page_preview": True,
    }
    
    if topic_id:
        try:
            payload["message_thread_id"] = int(topic_id)
        except (ValueError, TypeError) as e:
            logger.debug(f"[{uid}] 无效的 topic_id: {topic_id}, error: {e}")
    
    try:
        # 配置代理（如果有）
        client_kwargs = {"timeout": 10}
        if TELEGRAM_PROXY:
            client_kwargs["proxies"] = {"all://": TELEGRAM_PROXY}
        
        async with httpx.AsyncClient(**client_kwargs) as client:
            r = await client.post(url, json=payload)
            r.raise_for_status()
            data = r.json()
            if not data.get("ok"):
                logger.warning(f"[{uid}] TG返回失败: {data}")
            else:
                logger.debug(f"[{uid}] TG发送成功")
    except Exception as e:
        logger.warning(f"[{uid}] TG发送失败: {e}")