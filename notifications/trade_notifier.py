# trade_notifier.py - 交易信号通知器
"""
交易信号通知模块

支持两种模式：
1. 多用户模式: send_tg_trade_signal_for_user(ctx, signals) - 使用用户配置
2. 旧版兼容模式: send_tg_trade_signal(signals, chat_id) - 使用全局配置
"""
from typing import TYPE_CHECKING, List, Dict, Optional
from notifications.notifier import queue_message, queue_message_for_user

if TYPE_CHECKING:
    from core.user_context import UserContext

# 可通知的交易动作
NOTIFIABLE_ACTIONS = {
    "open_long", "open_short",
    "open_long_market", "open_short_market", 
    "open_long_limit", "open_short_limit", 
    "close_long", "close_short", 
    "reverse", 
    "increase_position", "decrease_position", 
    # "stop_orders",
    "update_stop_loss", "update_take_profit",
}


def _format_signal_message(res: Dict) -> Optional[str]:
    """
    格式化单个信号为消息文本
    
    Returns:
        消息文本，如果信号不需要通知则返回 None
    """
    action = res.get("action")
    symbol = res.get("symbol")
    
    if action not in NOTIFIABLE_ACTIONS:
        return None
    
    sym_display = symbol or "（未提供）"
    
    # 根据动作类型选择 emoji
    action_emoji = {
        "open_long": "🟢", "open_long_market": "🟢", "open_long_limit": "🟢",
        "open_short": "🔴", "open_short_market": "🔴", "open_short_limit": "🔴",
        "close_long": "⬜", "close_short": "⬜",
        "reverse": "🔄",
        "increase_position": "➕", "decrease_position": "➖",
        # "stop_orders": "🛡️",
        "update_stop_loss": "🛑", "update_take_profit": "🎯",
    }.get(action, "📌")
    
    msg = (
        f"🚨 AIBTC.VIP 交易信号\n\n"
        f"📌 交易对: {sym_display}\n"
        f"{action_emoji} 动作: {action}\n"
    )
    
    # 显示交易所信息
    exchanges = res.get("_exchanges")
    if exchanges:
        exchanges_str = ", ".join(ex.upper() for ex in exchanges)
        msg += f"🏦 交易所: {exchanges_str}\n"
    
    # 显示模型信息
    model = res.get("_model")
    provider = res.get("_provider")
    if model:
        model_display = f"{provider}/{model}" if provider else model
        msg += f"🤖 模型: {model_display}\n"
    
    if res.get("entry") is not None:
        msg += f"📍 入场价: {res['entry']}\n"
    
    if res.get("stop_loss") is not None:
        msg += f"🛑 止损: {res['stop_loss']}\n"
    
    if res.get("take_profit") is not None:
        msg += f"🎯 止盈: {res['take_profit']}\n"
    
    if res.get("reason"):
        # 截断过长的 reason
        reason = res['reason']
        if len(reason) > 300:
            reason = reason[:300] + "..."
        msg += f"\n🧠 原因:\n{reason}\n"
    
    return msg


def send_tg_trade_signal_for_user(ctx: "UserContext", ai_results: List[Dict]) -> int:
    """
    使用用户配置发送交易信号通知（多用户版本）
    
    Args:
        ctx: 用户上下文
        ai_results: AI 返回的信号列表
    
    Returns:
        成功入队的消息数量
    """
    if not ai_results:
        return 0
    
    if isinstance(ai_results, dict):
        ai_results = [ai_results]
    
    count = 0
    for res in ai_results:
        msg = _format_signal_message(res)
        if msg:
            queue_message_for_user(ctx, msg)
            count += 1
    
    return count


def send_tg_trade_signal(ai_results, chat_id=None):
    """
    发送交易信号到 Telegram（旧版兼容，使用全局配置）
    
    Args:
        ai_results: AI 返回的信号列表
        chat_id: 可选的 chat_id，覆盖全局配置
    """
    if not ai_results:
        print("⚠ AI 返回空，不推送 TG")
        return

    if isinstance(ai_results, dict):
        ai_results = [ai_results]

    for res in ai_results:
        msg = _format_signal_message(res)
        if msg:
            queue_message(msg, topic="Trading-signals", chat_id=chat_id)
