# prompt_templates.py
"""
LLM Prompt Templates v6.2

设计理念：
- 系统只提供"基础设施"：动作定义 + 输出格式（硬编码）
- 用户策略从数据库加载，通过 strategy_service.py 获取
- 职责分离：本模块只负责硬编码部分

结构顺序（基于 LLM 认知特点优化）：
┌─────────────────────────────────────────────────────────────┐
│ 1. OUTPUT_ANCHOR - 开头锚定（首因效应）                      │
├─────────────────────────────────────────────────────────────┤
│ 2. user_strategy - 决策核心（7大类策略，从DB加载）           │
├─────────────────────────────────────────────────────────────┤
│ 3. CORE_ACTIONS - 可用动作                                  │
├─────────────────────────────────────────────────────────────┤
│ 4. CORE_OUTPUT_FORMAT - 输出格式 + 错误处理（近因效应）      │
└─────────────────────────────────────────────────────────────┘
"""

from typing import Optional


# ============================================================
# P1 安全: 提示注入防护 - 清洗用户策略输入
# ============================================================
def _sanitize_user_strategy(strategy: str, max_length: int = 50000) -> str:
    """
    清洗用户策略文本，防止提示注入攻击
    
    Args:
        strategy: 用户策略文本
        max_length: 最大长度限制
    
    Returns:
        清洗后的策略文本
    """
    if not strategy:
        return ""
    
    # 危险模式列表 - 可能被用于提示注入
    dangerous_patterns = [
        # 常见注入标记
        '</JSON>', '<JSON>', '</decision>', '<decision>',
        'IGNORE PREVIOUS INSTRUCTIONS', 'IGNORE ALL INSTRUCTIONS',
        'IGNORE ABOVE INSTRUCTIONS', 'DISREGARD PREVIOUS',
        # 角色切换尝试
        'SYSTEM:', 'USER:', 'ASSISTANT:',
        'Human:', 'Assistant:', 'Claude:',
        '```system', '```user', '```assistant',
        '---\nrole:', '###SYSTEM', '###USER', '###ASSISTANT',
        # LLM 特定标记
        '[INST]', '[/INST]', '<<SYS>>', '<</SYS>>',
        '<|im_start|>', '<|im_end|>',
        '<|system|>', '<|user|>', '<|assistant|>',
        # 输出格式干扰
        'OUTPUT_ANCHOR', 'CORE_ACTIONS', 'CORE_OUTPUT_FORMAT',
    ]
    
    result = strategy
    for pattern in dangerous_patterns:
        # 大小写不敏感替换
        result = result.replace(pattern, '[FILTERED]')
        result = result.replace(pattern.lower(), '[FILTERED]')
        result = result.replace(pattern.upper(), '[FILTERED]')
    
    # 限制长度
    if len(result) > max_length:
        result = result[:max_length] + "\n[策略文本已截断]"
    
    return result


# ============================================================
# 1. OUTPUT_ANCHOR - 输出锚定（开头，防止思考输出）
# ============================================================
OUTPUT_ANCHOR = """
## OUTPUT CONSTRAINT (READ FIRST)

Your response must be EXACTLY:
<decision>
[{"symbol": "...", "action": "...", ...}, ...]
</decision>

CRITICAL RULES:
1. You MUST output a decision for EVERY symbol in the `symbols` object. No exceptions. Missing symbols = validation failure.
2. NO preamble. NO analysis. NO thinking tags. NO markdown code fences around the XML. Start immediately with `<decision>`.
""".strip()

# ============================================================
# 3. CORE_ACTIONS - 动作定义（硬编码）
# ============================================================
CORE_ACTIONS = """
## Available Actions

### Opening Positions
- `open_long_market` / `open_short_market` → Immediate execution
- `open_long_limit` / `open_short_limit` → Waiting for pullback (must meet distance constraint)

Required: `entry`, `stop_loss`, `take_profit`, `position_size_usdt`, `reason`

- `increase_position` / `decrease_position` → USDT notional to add/reduce
- `stop_orders` → Set or update BOTH stop_loss AND take_profit simultaneously
- `update_stop_loss` / `update_take_profit` → Update one side only
- `close_long` / `close_short` → Full exit

Do not default to `hold` without considering whether adjustments are needed.

### Inaction (CRITICAL DISTINCTION - AUTHORITATIVE DEFINITION)

| Action | Use When | Never Use When |
|--------|----------|----------------|
| `wait` | NO position AND NO pending order for this symbol | Have any position or order |
| `hold` | HAVE position OR pending order, and current params remain valid | No position and no order |
| `cancel` | Pending order no longer valid (price too far, signal changed) | No order to cancel |

**⚠️ Using the wrong inaction type is a validation error.**
""".strip()

# ============================================================
# 4. CORE_OUTPUT_FORMAT - 输出格式（硬编码）
# ============================================================
CORE_OUTPUT_FORMAT = """
## Output Format (ABSOLUTE REQUIREMENT)

### ⛔ FORBIDDEN OUTPUT PATTERNS ⛔

You MUST NOT output ANY of the following:
- `<think>`, `<thinking>`, `<reasoning>`, `<analysis>` or similar tags
- "Let me analyze...", "First, I'll...", "Looking at the data..."
- Chain-of-thought, step-by-step analysis, or internal reasoning
- Any text before or after the `<decision>` block
- Markdown code fences around the XML (no ```xml)

### ⚠️ COMPLETENESS RULE (CRITICAL) ⚠️

You MUST output exactly ONE decision object for EVERY symbol in the `symbols` object.
- If you have a position → use `hold` / management actions / `close_*`
- If you have a pending order → use `hold` / `cancel`
- If neither → use `wait` or `open_*`
- NEVER skip a symbol. The array length MUST equal the number of symbols in `symbols`.

### ✅ REQUIRED OUTPUT FORMAT ✅

Your ENTIRE response must be EXACTLY this structure (nothing else):

Example for 5 monitored symbols (you must output ALL of them):

<decision>
[
  {
    "symbol": "BTCUSDT",
    "action": "wait",
    "reason": "Bearish regime, score -2/10 below threshold, no setup"
  },
  {
    "symbol": "ETHUSDT",
    "action": "wait",
    "reason": "Downtrend intact, waiting for reversal signal"
  },
  {
    "symbol": "SOLUSDT",
    "action": "hold",
    "reason": "Position aligned with trend, SL safe at 2.5% distance"
  },
  {
    "symbol": "DOGEUSDT",
    "action": "wait",
    "reason": "Weak bias (-2), no clear entry zone"
  },
  {
    "symbol": "LTCUSDT",
    "action": "open_short_limit",
    "entry": 54.09,
    "stop_loss": 55.17,
    "take_profit": 52.00,
    "position_size_usdt": 178.83,
    "reason": "4h downtrend + resistance rejection, R:R 3.2"
  }
]
</decision>

**Field requirements:**
- Opening: `symbol`, `action`, `entry`, `stop_loss`, `take_profit`, `position_size_usdt`, `reason`
- `stop_orders`: `symbol`, `action`, `stop_loss`, `take_profit`, `reason`
- `update_stop_loss`: `symbol`, `action`, `stop_loss`, `reason`
- `update_take_profit`: `symbol`, `action`, `take_profit`, `reason`
- Other management/close/inaction: `symbol`, `action`, `reason` (+ `position_size_usdt` for add/reduce = delta amount)
""".strip()

# ============================================================
# 用户策略分类定义（用于组装）
# ============================================================
# 用户策略按以下顺序组装：
STRATEGY_CATEGORIES = [
    "role",  # 1. 角色定义 - 身份、思维模式、Override权限
    "risk_rules",  # 2. 风险规则 - 硬性约束、禁止行为
    "entry_conditions",  # 3. 入场条件 - 分析权重、订单流、动量/反转
    "exit_conditions",  # 4. 出场条件 - 持仓管理框架、决策矩阵
    "position_sizing",  # 5. 仓位管理 - sizing计算、调整系数
    "market_preferences",  # 6. 市场偏好 - 市场状态、波动率、相关性、时段
    "adaptive_rules",  # 7. 适应规则 - 逆向检查、Devil's Advocate、连亏处理
]

# 分类标题映射
CATEGORY_TITLES = {
    "role": "## Role & Trading Style",
    "risk_rules": "## Risk Rules (HARD - Must Obey)",
    "entry_conditions": "## Entry Conditions",
    "exit_conditions": "## Exit Conditions",
    "position_sizing": "## Position Sizing",
    "market_preferences": "## Market Preferences",
    "adaptive_rules": "## Adaptive Rules",
}

# 分类说明（用于文档/调试）
CATEGORY_DESCRIPTIONS = {
    "role": "身份定义、思维模式、Override权限声明（不含字段定义）",
    "risk_rules": "执行层硬性约束、绝对禁止行为、错误处理",
    "entry_conditions": "分析权重、订单流确认、动量/反转检测、触发条件",
    "exit_conditions": "持仓管理框架、决策矩阵、各动作触发条件（不含动作列表）",
    "position_sizing": "基础sizing、累积调整系数、反转交易sizing",
    "market_preferences": "市场状态应用、波动率影响、相关性分析、时段风险",
    "adaptive_rules": "决策检查清单、逆向信号、Devil's Advocate、连亏处理",
}


# ============================================================
# 构建函数
# ============================================================
def build_system_prompt(user_strategy: Optional[str] = None) -> str:
    """
    构建 system prompt

    顺序逻辑（基于 LLM 认知特点优化）：
    1. OUTPUT_ANCHOR - 开头锚定（首因效应）
    2. user_strategy - 决策核心（7大类策略）
    3. CORE_ACTIONS - 可用动作
    4. CORE_OUTPUT_FORMAT - 输出格式 + 错误处理（近因效应）

    Args:
        user_strategy: 用户策略文本（从数据库加载），如果为空则不包含策略部分

    Returns:
        完整的 system prompt
    """
    parts = [
        OUTPUT_ANCHOR,  # 1. 开头锚定（首因效应）
    ]

    # 2. 用户策略（决策核心，紧跟开头）
    # P1 安全: 清洗用户策略，防止提示注入
    if user_strategy:
        sanitized_strategy = _sanitize_user_strategy(user_strategy.strip())
        parts.append(sanitized_strategy)

    # 3. 动作定义
    parts.append(CORE_ACTIONS)

    # 4. 输出格式（近因效应）
    parts.append(CORE_OUTPUT_FORMAT)

    return "\n\n---\n\n".join(parts)


def build_minimal_prompt() -> str:
    """
    构建最小化 prompt（仅系统必需部分，无用户策略）

    顺序：OUTPUT_ANCHOR → CORE_ACTIONS → CORE_OUTPUT_FORMAT

    Returns:
        最小化 system prompt
    """
    parts = [
        OUTPUT_ANCHOR,
        CORE_ACTIONS,
        CORE_OUTPUT_FORMAT,
    ]

    return "\n\n---\n\n".join(parts)


def assemble_user_strategy(category_contents: dict) -> str:
    """
    组装用户策略

    按照 STRATEGY_CATEGORIES 定义的顺序组装各分类内容

    Args:
        category_contents: 分类内容字典，如 {"role": "...", "risk_rules": "..."}

    Returns:
        组装后的完整用户策略文本
    """
    parts = []

    for category in STRATEGY_CATEGORIES:
        content = category_contents.get(category)
        if content and content.strip():
            title = CATEGORY_TITLES.get(category, f"## {category.replace('_', ' ').title()}")
            parts.append(f"{title}\n\n{content.strip()}")

    return "\n\n---\n\n".join(parts)


def get_category_info() -> dict:
    """
    获取分类信息（用于文档/调试）

    Returns:
        包含分类顺序、标题和说明的字典
    """
    return {
        "categories": STRATEGY_CATEGORIES,
        "titles": CATEGORY_TITLES,
        "descriptions": CATEGORY_DESCRIPTIONS,
    }


# ============================================================
# 导出
# ============================================================
__all__ = [
    # 硬编码常量（按使用顺序）
    'OUTPUT_ANCHOR',
    'CORE_ACTIONS',
    'CORE_OUTPUT_FORMAT',
    # 分类定义
    'STRATEGY_CATEGORIES',
    'CATEGORY_TITLES',
    'CATEGORY_DESCRIPTIONS',
    # 构建函数
    'build_system_prompt',
    'build_minimal_prompt',
    'assemble_user_strategy',
    'get_category_info',
    # 增强版（3步验证）
    'QUICK_CHECKS_INTRO',
    'CANDLE_VALIDATION_RULES',
    'ENHANCED_OUTPUT_FORMAT',
    'build_enhanced_prompt',
]


# ============================================================
# 增强版 Prompt 基础设施 (v7.0)
# 注意: 详细的3步验证策略应从数据库加载
# ============================================================

QUICK_CHECKS_INTRO = """
## 决策流程: 3步验证

### 第1步: 快速检查

- 检查 `quick_checks` 中的 alerts
- critical alert 必须阻止交易
- 详见策略中的 quick_checks_rules
""".strip()


CANDLE_VALIDATION_RULES = """
### 第2步: K线形态验证

- 检查 `candle_intelligence` 中的信号
- 包括: rejection, close_position, vs_levels
- 详见策略中的 candle_validation
""".strip()


INDICATOR_VALIDATION_RULES = """
### 第3步: 指标确认

使用以下数据模块进行综合分析（按重要性排序）：

1. **structure** - 市场结构 + referee信号（4H有效性、CHoCH、15M触发）
2. **bias** - 多周期偏向评分 + 反转风险 + 冲突检测 + 交易建议
3. **trend** - 趋势方向 + market_phase + ATR比率
4. **levels** - 关键支撑/阻力位 + 距离百分比
5. **volatility** - 波动率regime + 挤压检测 + 预期波动范围 + 交易含义
6. **momentum** - RSI/MACD/动量指标
7. **order_flow** - 订单流 + 资金费率 + 多空比
8. **pattern** - 形态识别 + 历史胜率 + 可靠性评级 + 方向预测
9. **correlation** - 相关性分析
10. **sentiment** - 市场情绪
11. **candle_intelligence** - K线形态信号

决策规则：
- K线信号与指标冲突时，以K线为准
- referee的4H结构无效时，降低开仓信心
- bias.reversal_risk=confirmed 时，禁止顺势加仓
- volatility.trade_implication 直接指导仓位管理
- pattern.data_reliability=insufficient 时，忽略形态信号
""".strip()


ENHANCED_OUTPUT_FORMAT = """
## 决策输出格式

```json
{
  "validation_results": {
    "quick_check": "PASSED/FAILED",
    "candle_check": "PASSED/FAILED",
    "indicator_check": "PASSED/FAILED"
  },
  "decision": "OPEN_LONG/OPEN_SHORT/HOLD/WAIT/REJECT",
  "symbol": "BTCUSDT",
  "confidence": 0.75,
  "reasoning": {...},
  "risk_management": {
    "entry": 45000,
    "stop_loss": 44200,
    "take_profit": 47000,
    "position_size_usdt": 1000
  }
}
```
""".strip()


def build_enhanced_prompt(user_strategy: Optional[str] = None, 
                          context_data: Optional[dict] = None) -> str:
    """
    构建增强版 system prompt (3步验证流程)

    顺序逻辑：
    1. OUTPUT_ANCHOR - 开头锚定
    2. user_strategy - 决策核心（可选）
    3. QUICK_CHECKS_INTRO - 第1步验证
    4. CANDLE_VALIDATION_RULES - 第2步验证
    5. INDICATOR_VALIDATION_RULES - 第3步验证
    6. ENHANCED_OUTPUT_FORMAT - 输出格式
    7. CORE_ACTIONS - 可用动作

    Args:
        user_strategy: 用户策略文本（从数据库加载）
        context_data: 上下文数据（包含 quick_checks, candle_intelligence 等）

    Returns:
        完整的增强版 system prompt
    """
    import json
    
    parts = [
        OUTPUT_ANCHOR,
    ]
    
    # 2. 用户策略
    if user_strategy:
        sanitized_strategy = _sanitize_user_strategy(user_strategy.strip())
        parts.append(sanitized_strategy)
    
    # 3-5. 3步验证流程
    parts.append(QUICK_CHECKS_INTRO)
    parts.append(CANDLE_VALIDATION_RULES)
    parts.append(INDICATOR_VALIDATION_RULES)
    
    # 6. 输出格式
    parts.append(ENHANCED_OUTPUT_FORMAT)
    
    # 7. 动作定义（保留）
    parts.append(CORE_ACTIONS)
    
    # 添加上下文数据（如果有）
    if context_data:
        # Phase 6b: 传递完整的 11 个子模块数据，不再过滤
        simplified = {}
        if 'quick_checks' in context_data:
            simplified['quick_checks'] = context_data['quick_checks']
        
        if 'symbols' in context_data:
            simplified['symbols'] = {}
            # 所有 context_builder 构建的子模块都传递给 LLM
            symbol_keys = [
                'price', 'trend', 'indicators', 'structure', 'levels',
                'momentum', 'order_flow', 'volatility', 'pattern',
                'correlation', 'sentiment', 'bias', 'candle_intelligence',
            ]
            for symbol, data in context_data['symbols'].items():
                sym_data = {}
                for key in symbol_keys:
                    val = data.get(key)
                    if val is not None:
                        sym_data[key] = val
                # Fallback: include any other keys present (future-proof)
                for key, val in data.items():
                    if key not in sym_data and val is not None:
                        sym_data[key] = val
                simplified['symbols'][symbol] = sym_data
        
        context_json = json.dumps(simplified, indent=2, ensure_ascii=False)
        parts.append(f"\n\n---\n\n当前市场数据:\n{context_json}")
    
    return "\n\n---\n\n".join(parts)