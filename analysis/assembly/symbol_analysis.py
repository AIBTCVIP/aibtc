# symbol_analysis.py
"""
SymbolAnalysis 数据契约 - 上下游统一接口

设计原则：
1. 上游（technical_analyzer）负责所有计算，输出完整的 SymbolAnalysis
2. 下游（context_builder）只做 SymbolAnalysis → LLM dict 的纯映射
3. 零 fallback、零兜底计算

字段规范：
- 所有字段都有明确类型和默认值
- Optional 字段表示"可能无数据"，默认 None
- 非 Optional 字段必须由上游填充
"""

from dataclasses import dataclass, field
from typing import Optional, List, Dict


# ==============================================================
# 子模块 Dataclass 定义
# ==============================================================

@dataclass
class TrendModule:
    """1. 趋势模块 - 多周期趋势方向和强度"""
    direction_4h: str = "unknown"           # up/down/neutral/weakening/recovering
    strength_4h: str = "unknown"            # strong/moderate/choppy/unknown
    quality_4h: str = "unknown"             # good/moderate/poor
    continuation_prob_4h: str = "unknown"   # high/medium/low
    
    direction_1h: str = "unknown"           # up/down/neutral
    structure_1h: str = "unknown"           # impulse_up/pullback_up/impulse_down/pullback_down/range
    
    direction_15m: str = "unknown"          # up/down/neutral
    micro_structure_15m: str = "unknown"    # trending_up/trending_down/ranging/unclear
    
    multi_tf_aligned: Optional[bool] = None # 多周期是否对齐
    conflict: Optional[str] = None          # 冲突描述（如有）
    
    # EMA 细节（供高级分析）
    ema_slope_4h: str = "flat"              # rising/falling/flat
    price_vs_ema_4h: str = "between"        # above/below/between
    
    # 4H 结论层补充
    exhaustion_signal_4h: Optional[str] = None   # none/early/confirmed
    adx_rising_4h: Optional[bool] = None
    
    # 1H 结论层补充
    price_location_1h: Optional[str] = None      # premium/discount/value
    trade_space_1h: Optional[str] = None          # wide/moderate/crowded
    space_up_atr: Optional[float] = None
    space_down_atr: Optional[float] = None
    volatility_state_1h: Optional[str] = None     # expanding/contracting/normal
    consolidation_1h: Optional[bool] = None
    breakout_status_1h: Optional[str] = None      # breakout_up/breakdown/none
    
    # 15M 结论层补充
    volume_confirmation_15m: Optional[str] = None  # strong/moderate/weak
    obv_direction_15m: Optional[str] = None        # confirming/diverging
    rejection_strength_15m: Optional[str] = None   # strong/weak/none
    rejection_direction_15m: Optional[str] = None  # bullish/bearish/doji
    key_level_status_15m: Optional[str] = None     # touching_*/near_*/none


@dataclass
class IndicatorsModule:
    """2. 指标模块 - 关键技术指标值"""
    # RSI
    rsi_4h: Optional[float] = None
    rsi_1h: Optional[float] = None
    rsi_15m: Optional[float] = None
    rsi_zone: Optional[str] = None          # overbought/oversold/bullish/bearish/neutral
    
    # ADX
    adx_4h: Optional[float] = None
    adx_1h: Optional[float] = None
    di_direction_4h: str = "neutral"        # bullish/bearish/neutral
    
    # MACD
    macd_histogram_4h: Optional[float] = None
    macd_histogram_1h: Optional[float] = None
    macd_cross_4h: Optional[str] = None     # bullish_cross/bearish_cross/none
    
    # EMA 距离
    ema_distance_pct_4h: Optional[float] = None  # 价格距 EMA 的百分比
    ema_distance_pct_1h: Optional[float] = None


@dataclass
class StructureModule:
    """3. 市场结构模块 - SMC 结构分析"""
    # 各周期状态
    state_4h: str = "unknown"               # uptrend/downtrend/range/unknown
    state_1h: str = "unknown"
    state_15m: str = "unknown"
    
    # 结构点
    swing_high: Optional[float] = None
    swing_low: Optional[float] = None
    last_hh: Optional[float] = None         # 最近 Higher High
    last_ll: Optional[float] = None         # 最近 Lower Low
    last_hl: Optional[float] = None         # 最近 Higher Low
    last_lh: Optional[float] = None         # 最近 Lower High
    
    # CHoCH / BOS
    choch_status: Optional[str] = None      # detected/none
    choch_direction: Optional[str] = None   # up/down
    bos_status: Optional[str] = None        # detected/none
    bos_direction: Optional[str] = None     # up/down
    last_break_type: Optional[str] = None   # choch_up/choch_down/bos_up/bos_down
    last_break_bars_ago: Optional[int] = None
    last_break_price: Optional[float] = None
    
    # 结构健康度
    health_score: Optional[float] = None    # 0-100
    consistency: Optional[str] = None       # consistent/mixed/weak
    trend_alignment: Optional[str] = None   # aligned/divergent
    
    # 区间信息
    is_range_4h: Optional[bool] = None
    range_location_4h: Optional[str] = None # near_low/near_high/middle
    in_danger_zone_4h: Optional[bool] = None
    structure_valid_4h: Optional[bool] = None
    
    # 入场触发
    has_entry_trigger_15m: Optional[bool] = None
    entry_signal_15m: Optional[str] = None  # long/short/none


@dataclass
class LevelsModule:
    """4. 关键位模块 - 支撑阻力位"""
    # 最近支撑
    support_price: Optional[float] = None
    support_distance_pct: Optional[float] = None
    support_strength: Optional[str] = None  # strong/moderate/weak
    support_touches: Optional[int] = None
    
    # 最近阻力
    resistance_price: Optional[float] = None
    resistance_distance_pct: Optional[float] = None
    resistance_strength: Optional[str] = None
    resistance_touches: Optional[int] = None
    
    # 枢轴点
    pivot_price: Optional[float] = None
    pivot_type: Optional[str] = None        # daily/weekly
    
    # 区间
    range_high: Optional[float] = None
    range_low: Optional[float] = None
    range_mid: Optional[float] = None
    
    # 价格位置
    price_in_range_pct: Optional[float] = None  # 0-100, 0=底部, 100=顶部
    nearest_level_type: Optional[str] = None    # support/resistance
    nearest_level_distance_pct: Optional[float] = None


@dataclass
class MomentumModule:
    """5. 动量模块 - 动量状态和反转风险"""
    state_4h: str = "unknown"               # accelerating/decelerating/neutral/exhausted
    direction: str = "unknown"              # bullish/bearish/neutral
    
    # 反转风险
    reversal_risk: str = "unknown"          # high/medium/low
    reversal_score: Optional[float] = None  # 0-10
    reversal_factors: List[str] = field(default_factory=list)
    
    # 加速度
    acceleration: Optional[str] = None      # accelerating/decelerating/stable
    
    # 背离
    divergence_type: Optional[str] = None   # bullish/bearish/hidden_bullish/hidden_bearish/none
    divergence_strength: Optional[str] = None


@dataclass
class OrderFlowModule:
    """6. 订单流模块 - 买卖压力分析"""
    ratio_5bar: Optional[float] = None      # 5根K线买卖比
    delta_5bar: Optional[float] = None      # 5根K线 delta
    volume_ratio: Optional[float] = None    # 当前成交量 vs 平均
    buy_pressure_pct: Optional[float] = None  # 买压百分比 0-100
    
    imbalance: str = "unknown"              # strong_buying/buying/balanced/selling/strong_selling
    volume_trend: Optional[str] = None      # increasing/decreasing/stable
    
    # 大单
    large_buys_count: Optional[int] = None
    large_sells_count: Optional[int] = None
    
    # 吸收
    absorption_detected: bool = False
    absorption_side: Optional[str] = None   # buy/sell
    
    # 与价格背离
    price_divergence: Optional[str] = None  # bullish/bearish/none


@dataclass
class VolatilityModule:
    """7. 波动率模块 - 波动状态和挤压检测"""
    regime: str = "unknown"                 # high/normal/low/extreme
    trend: Optional[str] = None             # expanding/contracting/stable
    
    # ATR
    atr_pct_4h: Optional[float] = None
    atr_pct_1h: Optional[float] = None
    atr_pct_15m: Optional[float] = None
    atr_ratio_to_ma: Optional[float] = None
    
    # 百分位
    percentile: Optional[float] = None      # 0-100, 当前波动率在历史中的位置
    
    # 布林带
    bb_width: Optional[float] = None
    bb_width_percentile: Optional[float] = None
    bbw_ratio: Optional[float] = None
    
    # 挤压
    squeeze_active: bool = False
    squeeze_status: Optional[str] = None    # squeeze/expansion/normal
    squeeze_bars: Optional[int] = None
    squeeze_intensity: Optional[str] = None # tight/moderate/loose
    
    # 突破概率
    breakout_probability: Optional[float] = None
    
    # 描述（供 LLM 参考）
    description: Optional[str] = None
    trade_implication: Optional[str] = None


@dataclass
class PatternModule:
    """8. 形态模块 - K线形态识别"""
    name: Optional[str] = None              # engulfing/doji/hammer/etc
    pattern_type: Optional[str] = None      # reversal/continuation
    direction: Optional[str] = None         # bullish/bearish
    confidence: Optional[float] = None      # 0-100
    
    # 历史统计
    win_rate: Optional[float] = None        # 历史胜率
    sample_size: Optional[int] = None       # 样本数
    avg_move_pct: Optional[float] = None    # 平均后续涨跌幅


@dataclass
class CorrelationModule:
    """9. 相关性模块 - 与 BTC/ETH 的相关性"""
    btc_correlation: Optional[float] = None     # -1 to 1
    eth_correlation: Optional[float] = None
    
    beta: Optional[float] = None                # 相对 BTC 的 beta
    lead_lag_bars: Optional[int] = None         # 领先/滞后 K 线数
    relative_strength: Optional[float] = None   # 相对强度
    
    # 当前状态
    decoupled: bool = False                     # 是否与大盘脱钩
    outperforming: Optional[bool] = None        # 是否跑赢大盘


@dataclass
class SentimentModule:
    """10. 情绪模块 - 资金费率、持仓量等"""
    # 资金费率
    funding_rate: Optional[float] = None
    funding_bias: Optional[str] = None      # crowded_long/slightly_long/neutral/slightly_short/crowded_short
    
    # 持仓量变化
    oi_change_1h_pct: Optional[float] = None
    oi_change_4h_pct: Optional[float] = None
    oi_change_24h_pct: Optional[float] = None
    oi_price_divergence: Optional[str] = None  # bullish/bearish/confirming_up/confirming_down/neutral
    
    # 多空比
    long_short_ratio: Optional[float] = None
    positioning: Optional[str] = None       # heavily_long/moderately_long/balanced/moderately_short/heavily_short
    
    # 爆仓
    liquidations_long_24h: Optional[float] = None
    liquidations_short_24h: Optional[float] = None
    liquidation_bias: Optional[str] = None  # longs_squeezed/shorts_squeezed/balanced


@dataclass
class GuidanceModule:
    """11. MTF 对齐与行动指导"""
    mtf_conflict: Optional[bool] = None
    mtf_conflict_type: Optional[str] = None       # bullish_divergence/bearish_divergence/weak_*
    mtf_alignment_score: Optional[float] = None   # 0.0-1.0
    mtf_action_bias: Optional[str] = None         # reduce_shorts_watch_longs 等
    reversal_risk_level: Optional[str] = None     # low/medium/high/very_high
    primary_bias: Optional[str] = None            # cautious_bullish/neutral/bearish 等
    for_new_positions: Optional[str] = None       # wait/consider_long/consider_short
    scenarios: Optional[list] = None
    risk_warnings: List[str] = field(default_factory=list)


@dataclass
class BiasModule:
    """12. 综合偏向模块 - 5层乘法模型输出"""
    score: Optional[int] = None             # -10 to +10
    direction: str = "unknown"              # bullish/bearish/neutral
    strength: str = "unknown"               # strong/moderate/weak/none
    
    # 因子分解
    factors: List[str] = field(default_factory=list)
    
    # 反转风险
    reversal_risk: str = "unknown"          # high/medium/low
    reversal_score: Optional[float] = None
    reversal_factors: List[str] = field(default_factory=list)
    
    # 冲突检测
    trend_conflict: bool = False
    
    # 交易建议（可选）
    trade_suggestion: Optional[str] = None
    
    # 层级分数（调试用）
    layer_scores: Dict[str, float] = field(default_factory=dict)
    # 包含: base_score, quality_mult, env_mult, timing_mult, reversal_decay


# ==============================================================
# 主数据结构
# ==============================================================

@dataclass
class SymbolAnalysis:
    """
    单个交易对的完整分析结果
    
    这是上游（technical_analyzer）和下游（context_builder）之间的唯一契约。
    上游负责填充所有字段，下游只做映射。
    """
    # 基础信息
    symbol: str
    price: Optional[float] = None
    change_24h_pct: Optional[float] = None
    high_24h: Optional[float] = None
    low_24h: Optional[float] = None
    volume_24h_usdt: Optional[float] = None
    
    # 12 个子模块
    trend: TrendModule = field(default_factory=TrendModule)
    indicators: IndicatorsModule = field(default_factory=IndicatorsModule)
    structure: StructureModule = field(default_factory=StructureModule)
    levels: LevelsModule = field(default_factory=LevelsModule)
    momentum: MomentumModule = field(default_factory=MomentumModule)
    order_flow: OrderFlowModule = field(default_factory=OrderFlowModule)
    volatility: VolatilityModule = field(default_factory=VolatilityModule)
    pattern: PatternModule = field(default_factory=PatternModule)
    correlation: CorrelationModule = field(default_factory=CorrelationModule)
    sentiment: SentimentModule = field(default_factory=SentimentModule)
    guidance: GuidanceModule = field(default_factory=GuidanceModule)
    bias: BiasModule = field(default_factory=BiasModule)
    
    # 元数据
    analysis_timestamp: Optional[float] = None  # Unix timestamp
    data_freshness_sec: Optional[int] = None    # 数据新鲜度（秒）
    
    def to_dict(self) -> dict:
        """转换为字典，供 context_builder 使用"""
        from dataclasses import asdict
        return asdict(self)
    
    def to_llm_dict(self) -> dict:
        """
        转换为 LLM 友好的字典格式
        - 移除 None 值
        - 移除空列表/字典
        - 移除 unknown 默认值（可选）
        """
        def clean_dict(d: dict) -> dict:
            result = {}
            for k, v in d.items():
                if v is None:
                    continue
                if isinstance(v, dict):
                    cleaned = clean_dict(v)
                    if cleaned:  # 非空才保留
                        result[k] = cleaned
                elif isinstance(v, list):
                    if v:  # 非空才保留
                        result[k] = v
                elif v != "unknown":  # 过滤默认值
                    result[k] = v
            return result
        
        from dataclasses import asdict
        return clean_dict(asdict(self))


# ==============================================================
# 工厂函数（供 technical_analyzer 使用）
# ==============================================================

def create_symbol_analysis(symbol: str) -> SymbolAnalysis:
    """创建一个空的 SymbolAnalysis 实例"""
    return SymbolAnalysis(symbol=symbol)
