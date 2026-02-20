# response_parser.py
"""
LLM 响应解析模块 (修复版)

修复内容:
1. 正确处理 reasoning_content - 不合并到解析内容中
2. 增加预处理步骤，移除常见的干扰内容
3. 更清晰的解析优先级

负责：
1. 从不同 LLM 提供商的响应中提取内容
2. 处理 reasoning_content（推理模型的思考过程）
3. 解析交易信号（JSON/XML 格式）
4. 标准化信号字段
"""

import re
import json
import logging
from typing import Dict, List, Any, Optional
from dataclasses import dataclass

logger = logging.getLogger(__name__)


# ============================================================
# 响应内容提取结果
# ============================================================
@dataclass
class ParsedContent:
    """解析后的内容"""
    content: str  # 最终内容（用于信号解析）
    reasoning_content: Optional[str] = None  # 推理内容（仅推理模型）
    raw_content: Optional[str] = None  # 原始 content 字段


# ============================================================
# 响应内容提取（不同提供商格式）
# ============================================================
def extract_openai_content(data: Dict[str, Any]) -> ParsedContent:
    """
    从 OpenAI 兼容格式响应中提取内容

    支持：OpenAI, DeepSeek, OpenRouter, Grok 等

    响应格式：
    {
        "choices": [{
            "message": {
                "content": "最终答案",
                "reasoning_content": "思考过程"  // 推理模型特有
            }
        }]
    }

    ⚠️ 重要修复：
    - 推理内容(reasoning_content)仅用于日志/调试
    - 信号解析只使用 raw_content（最终答案）
    - 如果 raw_content 为空但 reasoning_content 有内容，尝试从 reasoning 中提取
    """
    choices = data.get("choices", [])
    if not choices:
        return ParsedContent(content="")

    message = choices[0].get("message", {})

    # 提取 content
    raw_content = message.get("content") or ""

    # 提取 reasoning_content（DeepSeek Reasoner 等推理模型）
    reasoning_content = message.get("reasoning_content") or ""

    # ========== 修复：优先使用 raw_content ==========
    # 推理模型的最终答案应该在 content 字段
    # reasoning_content 只是思考过程，不应该用于解析

    if raw_content:
        # 正常情况：使用 content 字段
        final_content = raw_content
    elif reasoning_content:
        # 异常情况：模型把答案放在了 reasoning_content 里
        # 尝试从中提取 <decision> 块
        logger.warning("raw_content 为空，尝试从 reasoning_content 中提取信号")
        final_content = reasoning_content
    else:
        final_content = ""

    return ParsedContent(
        content=final_content,
        reasoning_content=reasoning_content if reasoning_content else None,
        raw_content=raw_content if raw_content else None,
    )


def extract_anthropic_content(data: Dict[str, Any]) -> ParsedContent:
    """
    从 Anthropic (Claude) 响应中提取内容

    响应格式：
    {
        "content": [
            {"type": "text", "text": "..."},
            {"type": "thinking", "thinking": "..."}  // 可能的思考内容
        ]
    }

    ⚠️ 重要修复：
    - thinking 内容仅用于日志/调试
    - 信号解析只使用 text 块
    """
    content_blocks = data.get("content", [])
    if not content_blocks:
        return ParsedContent(content="")

    text_parts = []
    thinking_parts = []

    for block in content_blocks:
        block_type = block.get("type", "")

        if block_type == "text":
            text = block.get("text", "")
            if text:
                text_parts.append(text)
        elif block_type == "thinking":
            # Claude 的 extended thinking 功能
            thinking = block.get("thinking", "")
            if thinking:
                thinking_parts.append(thinking)

    raw_content = "".join(text_parts)
    reasoning_content = "".join(thinking_parts) if thinking_parts else None

    # ========== 修复：只使用 text 内容 ==========
    if raw_content:
        final_content = raw_content
    elif reasoning_content:
        # 异常情况
        logger.warning("text 内容为空，尝试从 thinking 中提取信号")
        final_content = reasoning_content
    else:
        final_content = ""

    return ParsedContent(
        content=final_content,
        reasoning_content=reasoning_content,
        raw_content=raw_content if raw_content else None,
    )


def extract_streaming_content(chunks: List[Dict[str, Any]], provider: str = "openai") -> ParsedContent:
    """
    从流式响应块中提取完整内容

    Args:
        chunks: 流式响应的所有块
        provider: 提供商名称 (openai/anthropic)

    Returns:
        合并后的内容
    """
    content_parts = []
    reasoning_parts = []

    for chunk in chunks:
        if provider == "anthropic":
            # Anthropic 流式格式
            delta = chunk.get("delta", {})
            if delta.get("type") == "text_delta":
                text = delta.get("text", "")
                if text:
                    content_parts.append(text)
        else:
            # OpenAI 兼容流式格式
            choices = chunk.get("choices", [])
            if choices:
                delta = choices[0].get("delta", {})

                # 标准内容
                content = delta.get("content", "")
                if content:
                    content_parts.append(content)

                # 推理内容（DeepSeek Reasoner 等）
                reasoning = delta.get("reasoning_content", "")
                if reasoning:
                    reasoning_parts.append(reasoning)

    raw_content = "".join(content_parts)
    reasoning_content = "".join(reasoning_parts) if reasoning_parts else None

    # 只使用 raw_content
    if raw_content:
        final_content = raw_content
    elif reasoning_content:
        logger.warning("流式 content 为空，尝试从 reasoning 中提取")
        final_content = reasoning_content
    else:
        final_content = ""

    return ParsedContent(
        content=final_content,
        reasoning_content=reasoning_content,
        raw_content=raw_content if raw_content else None,
    )


# ============================================================
# 内容预处理
# ============================================================
def _preprocess_content(content: str) -> str:
    """
    预处理内容，移除常见的干扰内容

    移除:
    - <think>/<thinking> 块
    - <reasoning>/<analysis> 块
    - Markdown 风格的标题和强调（如果不包含信号）
    """
    if not content:
        return content

    original_length = len(content)

    # 1. 移除 <think>...</think> 或 <thinking>...</thinking> 块
    content = re.sub(
        r'<think(?:ing)?[^>]*>[\s\S]*?</think(?:ing)?>',
        '',
        content,
        flags=re.IGNORECASE
    )

    # 2. 移除 <reasoning>...</reasoning> 块
    content = re.sub(
        r'<reasoning[^>]*>[\s\S]*?</reasoning>',
        '',
        content,
        flags=re.IGNORECASE
    )

    # 3. 移除 <analysis>...</analysis> 块
    content = re.sub(
        r'<analysis[^>]*>[\s\S]*?</analysis>',
        '',
        content,
        flags=re.IGNORECASE
    )

    # 4. 移除 <explanation>...</explanation> 块
    content = re.sub(
        r'<explanation[^>]*>[\s\S]*?</explanation>',
        '',
        content,
        flags=re.IGNORECASE
    )

    content = content.strip()

    if len(content) < original_length:
        removed = original_length - len(content)
        logger.debug(f"预处理移除了 {removed} 字符的干扰内容")

    return content


# ============================================================
# 交易信号解析
# ============================================================
def parse_trading_signals(content: str) -> List[Dict]:
    """
    从 LLM 响应中解析交易信号

    支持多种格式（按优先级）：
    1. <decision>[ JSON数组 ]</decision> - 提示词要求的标准格式（优先）
    2. 纯 JSON 数组
    3. Markdown 代码块包裹的 JSON
    4. 混合文本中的 JSON
    5. XML 子元素格式 (<decision_entry>...</decision_entry>)
    6. 包裹在各种 key 下的 JSON
    """
    if not content:
        return []

    # ========== 预处理：移除干扰内容 ==========
    content = _preprocess_content(content)

    if not content:
        logger.warning("预处理后内容为空")
        return []

    def extract_signals_from_result(result) -> List[Dict]:
        """从解析结果中提取信号列表"""
        if isinstance(result, list):
            return [_normalize_signal(s) for s in result if isinstance(s, dict)]

        if isinstance(result, dict):
            # 检查常见的信号列表 key
            signal_keys = ['signals', 'decisions', 'trades', 'orders',
                           'recommendations', 'actions', 'entries', 'positions',
                           'decision', 'data', 'result', 'output']

            for key in signal_keys:
                if key in result and isinstance(result[key], list):
                    return [_normalize_signal(s) for s in result[key] if isinstance(s, dict)]

            # 如果 dict 本身看起来像一个信号（有 symbol 或 action）
            if result.get('symbol') or result.get('action') or result.get('pair'):
                return [_normalize_signal(result)]

        return []

    # ========== 1. 优先处理 <decision> 标签（提示词要求的标准格式）==========
    decision_tag_pattern = r'<decision>\s*([\s\S]*?)\s*</decision>'
    decision_match = re.search(decision_tag_pattern, content, re.IGNORECASE)
    if decision_match:
        decision_content = decision_match.group(1).strip()

        # 1a. 直接解析为 JSON
        try:
            result = json.loads(decision_content)
            signals = extract_signals_from_result(result)
            if signals:
                logger.debug(f"从 <decision> 标签解析出 {len(signals)} 个信号")
                return signals
        except json.JSONDecodeError:
            pass

        # 1b. 可能有 markdown 代码块
        json_in_decision = re.search(r'```(?:json)?\s*([\s\S]*?)\s*```', decision_content)
        if json_in_decision:
            try:
                result = json.loads(json_in_decision.group(1))
                signals = extract_signals_from_result(result)
                if signals:
                    logger.debug(f"从 <decision> 内的代码块解析出 {len(signals)} 个信号")
                    return signals
            except json.JSONDecodeError:
                pass

        # 1c. 提取 JSON 数组
        array_in_decision = re.search(r'\[\s*\{[\s\S]*\}\s*\]', decision_content)
        if array_in_decision:
            try:
                result = json.loads(array_in_decision.group())
                signals = extract_signals_from_result(result)
                if signals:
                    logger.debug(f"从 <decision> 内的 JSON 数组解析出 {len(signals)} 个信号")
                    return signals
            except json.JSONDecodeError:
                pass

    # ========== 2. 尝试直接解析整个内容为 JSON ==========
    try:
        result = json.loads(content)
        signals = extract_signals_from_result(result)
        if signals:
            logger.debug(f"直接 JSON 解析出 {len(signals)} 个信号")
            return signals
    except json.JSONDecodeError:
        pass

    # ========== 3. 尝试从 markdown 代码块提取 JSON ==========
    patterns = [
        r"```json\s*([\s\S]*?)\s*```",  # ```json ... ```
        r"```\s*([\s\S]*?)\s*```",  # ``` ... ```
        r"<json>\s*([\s\S]*?)\s*</json>",  # <json> ... </json>
        r"<o>\s*([\s\S]*?)\s*</o>",  # <o> ... </o>
    ]

    for pattern in patterns:
        matches = re.findall(pattern, content)
        for match in matches:
            try:
                result = json.loads(match)
                signals = extract_signals_from_result(result)
                if signals:
                    logger.debug(f"从代码块解析出 {len(signals)} 个信号")
                    return signals
            except json.JSONDecodeError:
                continue

    # ========== 4. 尝试找到内容中的 JSON 数组或对象 ==========
    array_match = re.search(r'\[\s*\{[\s\S]*\}\s*\]', content)
    if array_match:
        try:
            result = json.loads(array_match.group())
            signals = extract_signals_from_result(result)
            if signals:
                logger.debug(f"从文本中提取 JSON 数组，解析出 {len(signals)} 个信号")
                return signals
        except json.JSONDecodeError:
            pass

    # 再找单个对象
    object_match = re.search(r'\{[^{}]*"(?:symbol|action|pair)"[^{}]*\}', content)
    if object_match:
        try:
            result = json.loads(object_match.group())
            signals = extract_signals_from_result(result)
            if signals:
                logger.debug(f"从文本中提取单个 JSON 对象")
                return signals
        except json.JSONDecodeError:
            pass

    # ========== 5. 尝试解析 XML 格式 ==========
    # 支持: <decision_entry>, <decision_item>, 嵌套的 <decision><decision>
    if ('<decision_entry>' in content or '<decision_item>' in content or
            '<decision>' in content or '<decisions>' in content):
        xml_signals = _parse_xml_decisions(content)
        if xml_signals:
            logger.debug(f"从 XML 格式解析出 {len(xml_signals)} 个信号")
            return [_normalize_signal(s) for s in xml_signals]

    # ========== 6. 尝试解析其他 XML 风格标签 ==========
    other_xml_tags = ['signal', 'trade', 'order', 'entry']
    for tag in other_xml_tags:
        if f'<{tag}>' in content.lower():
            xml_signals = _parse_generic_xml(content, tag)
            if xml_signals:
                logger.debug(f"从 <{tag}> XML 格式解析出 {len(xml_signals)} 个信号")
                return [_normalize_signal(s) for s in xml_signals]

    # ========== 解析失败 Fallback：记录警告日志 ==========
    # 检测是否可能包含交易信号但解析失败
    content_lower = content.lower()
    signal_indicators = [
        'symbol', 'action', 'buy', 'sell', 'long', 'short',
        'open_long', 'open_short', 'close_long', 'close_short',
        'btc', 'eth', 'usdt', 'stop_loss', 'take_profit'
    ]

    has_signal_indicators = any(indicator in content_lower for indicator in signal_indicators)

    if has_signal_indicators:
        # 内容中有信号相关关键词，但解析失败 - 记录警告
        content_preview = content[:500] + "..." if len(content) > 500 else content
        logger.warning(
            f"信号解析失败: 响应内容包含交易相关关键词但无法解析出有效信号。\n"
            f"内容预览: {content_preview}"
        )
    else:
        # 完全没有信号相关内容
        content_preview = content[:200] + "..." if len(content) > 200 else content
        logger.warning(
            f"信号解析失败: 响应不包含任何交易信号格式。\n"
            f"内容预览: {content_preview}"
        )

    return []


# ============================================================
# XML 格式解析
# ============================================================
def _parse_xml_decisions(content: str) -> List[Dict]:
    """
    解析 XML 格式的决策

    支持多种格式:
    1. <decision><decision_entry>...</decision_entry></decision>
    2. <decision><decision_item>...</decision_item></decision>
    3. <decision><decision>...</decision></decision>  (嵌套 decision)
    4. <decisions><decision>...</decision></decisions>
    5. 直接的 <decision>...<symbol>...</symbol>...</decision>
    """
    signals = []
    entries = []

    # 方法1: 尝试 decision_entry 和 decision_item 标签
    for tag in ['decision_entry', 'decision_item']:
        pattern = rf'<{tag}>([\s\S]*?)</{tag}>'
        found = re.findall(pattern, content, re.IGNORECASE)
        if found:
            entries.extend(found)

    # 方法2: 如果没找到，尝试解析 <decision> 标签
    # 策略：找所有 <decision>...</decision> 块，只保留包含 <symbol> 的（过滤外层包装）
    if not entries:
        all_decisions = re.findall(r'<decision>([\s\S]*?)</decision>', content, re.IGNORECASE)
        # 只保留包含 <symbol> 标签的块（这些是实际的决策条目）
        entries = [d for d in all_decisions if '<symbol>' in d.lower()]

    # 解析每个条目
    for entry in entries:
        signal = {}

        # 提取各个字段
        fields = ['symbol', 'action', 'stop_loss', 'take_profit', 'entry',
                  'position_size', 'quantity', 'leverage', 'reason', 'confidence']

        for field in fields:
            pattern = rf'<{field}>([\s\S]*?)</{field}>'
            match = re.search(pattern, entry, re.IGNORECASE)
            if match:
                value = match.group(1).strip()

                # 转换数值类型
                if field in ('stop_loss', 'take_profit', 'entry', 'position_size',
                             'quantity', 'confidence'):
                    try:
                        value = float(value)
                    except (ValueError, TypeError):
                        pass
                elif field == 'leverage':
                    try:
                        value = int(value)
                    except (ValueError, TypeError):
                        pass

                signal[field] = value

        # 只保留有 symbol 和 action 的信号
        if signal.get('symbol') and signal.get('action'):
            signals.append(signal)

    return signals


def _parse_generic_xml(content: str, tag: str) -> List[Dict]:
    """
    解析通用 XML 格式的信号
    """
    signals = []

    # 大小写不敏感匹配
    pattern = rf'<{tag}>([\s\S]*?)</{tag}>'
    entries = re.findall(pattern, content, re.IGNORECASE)

    for entry in entries:
        signal = {}

        # 提取所有 XML 子标签
        field_pattern = r'<(\w+)>([\s\S]*?)</\1>'
        fields = re.findall(field_pattern, entry, re.IGNORECASE)

        for field_name, field_value in fields:
            signal[field_name.lower()] = field_value.strip()

        if signal:
            signals.append(signal)

    return signals


# ============================================================
# 信号标准化
# ============================================================
def _normalize_signal(signal: Dict) -> Dict:
    """
    标准化信号字段名称和数值类型

    处理不同 LLM 返回的字段名差异，例如:
    - "sl" -> "stop_loss"
    - "tp" -> "take_profit"
    - "size" -> "position_size"
    """
    # 字段别名映射
    field_aliases = {
        # stop_loss 别名
        "sl": "stop_loss",
        "stoploss": "stop_loss",
        "stop": "stop_loss",
        # take_profit 别名
        "tp": "take_profit",
        "takeprofit": "take_profit",
        "target": "take_profit",
        "profit_target": "take_profit",
        # position_size 别名
        "size": "position_size",
        "amount": "position_size",
        "order_value": "position_size",
        "value": "position_size",
        "position_size_usdt": "position_size",  # v4.0 prompt 使用的明确名称
        # entry 别名
        "entry_price": "entry",
        "price": "entry",
        # symbol 别名
        "pair": "symbol",
        "ticker": "symbol",
        # action 别名
        "side": "action",
        "direction": "action",
        "type": "action",
    }

    # action 值标准化
    # 保持提示词中定义的标准 action 不变，只映射非标准别名
    action_mapping = {
        # ========== 开仓 - 市价 ==========
        "open_long_market": "open_long_market",
        "open_short_market": "open_short_market",
        "long": "open_long_market",
        "buy": "open_long_market",
        "open_long": "open_long_market",
        "short": "open_short_market",
        "sell": "open_short_market",
        "open_short": "open_short_market",
        # ========== 开仓 - 限价 ==========
        "open_long_limit": "open_long_limit",
        "open_short_limit": "open_short_limit",
        "limit_long": "open_long_limit",
        "limit_short": "open_short_limit",
        "buy_limit": "open_long_limit",
        "sell_limit": "open_short_limit",
        # ========== 平仓 ==========
        "close_long": "close_long",
        "close_short": "close_short",
        "exit_long": "close_long",
        "sell_long": "close_long",
        "exit_short": "close_short",
        "buy_short": "close_short",
        "cover": "close_short",
        # ========== 仓位管理 ==========
        "increase_position": "increase_position",
        "decrease_position": "decrease_position",
        "add": "increase_position",
        "increase": "increase_position",
        "reduce": "decrease_position",
        "decrease": "decrease_position",
        "partial_close": "decrease_position",
        # ========== 止损止盈管理 ==========
        "stop_orders": "stop_orders",
        "update_stop_loss": "update_stop_loss",
        "update_take_profit": "update_take_profit",
        "set_stops": "stop_orders",
        "update_stops": "stop_orders",
        "modify_sl": "update_stop_loss",
        "modify_tp": "update_take_profit",
        # ========== 取消订单 ==========
        "cancel": "cancel",
        "cancel_order": "cancel",
        "cancel_limit": "cancel",
        # ========== 不操作 ==========
        "hold": "hold",
        "wait": "wait",
        "none": "wait",
        "no_action": "wait",
        "skip": "wait",
        "pass": "wait",
    }

    normalized = {}

    for key, value in signal.items():
        # 转换字段名为小写
        lower_key = key.lower().strip()

        # 使用别名映射
        standard_key = field_aliases.get(lower_key, lower_key)

        # 跳过空值
        if value is None or value == "" or value == "null" or value == "N/A":
            continue

        # 标准化 action 值
        if standard_key == "action" and isinstance(value, str):
            value = action_mapping.get(value.lower().strip(), value.lower().strip())

        # 转换数值类型
        if standard_key in ('stop_loss', 'take_profit', 'entry', 'position_size', 'quantity', 'confidence'):
            if isinstance(value, str):
                # 移除货币符号和逗号
                value = value.replace('$', '').replace(',', '').strip()
                try:
                    value = float(value)
                except (ValueError, TypeError):
                    pass
            elif isinstance(value, (int, float)):
                value = float(value)
        elif standard_key == 'leverage':
            if isinstance(value, str):
                try:
                    value = int(float(value.replace('x', '').replace('X', '').strip()))
                except (ValueError, TypeError):
                    pass

        # symbol 标准化：确保大写，添加 USDT 后缀
        if standard_key == 'symbol' and isinstance(value, str):
            value = value.upper().strip()
            # 如果没有 USDT/USDC 后缀，添加 USDT
            if not value.endswith(('USDT', 'USDC', 'USD', 'BUSD')):
                value = value + 'USDT'

        normalized[standard_key] = value

    return normalized


# ============================================================
# 便捷函数
# ============================================================
def extract_and_parse_signals(
        data: Dict[str, Any],
        provider: str = "openai"
) -> tuple[List[Dict], ParsedContent]:
    """
    一站式提取内容并解析信号

    Args:
        data: API 响应数据
        provider: 提供商名称 (openai/anthropic)

    Returns:
        (信号列表, 解析后的内容)
    """
    # 提取内容
    if provider == "anthropic":
        parsed = extract_anthropic_content(data)
    else:
        parsed = extract_openai_content(data)

    # 解析信号
    signals = parse_trading_signals(parsed.content)

    return signals, parsed
