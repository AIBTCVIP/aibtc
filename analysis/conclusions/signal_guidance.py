# conclusions/signal_guidance - 多周期结论与行动建议
"""
结论层：MTF 对齐评估、反转风险等级、行动建议与情景规划。

本模块归属：conclusions。
从 indicators 迁出，便于维护与审计；indicators 只保留原始指标与 snapshot。
"""
from typing import Dict, List, Any


def assess_mtf_alignment(tf_states: Dict[str, Dict]) -> Dict:
    """
    多时间框架对齐评估

    检测大小级别趋势/动量的冲突，识别潜在反转信号

    Args:
        tf_states: 各时间框架的状态
            {
                '4h': {'trend': 'down', 'momentum': 'weakening', 'trend_confidence': 0.8},
                '1h': {'trend': 'down', 'momentum': 'reversing', 'trend_confidence': 0.6},
                '15m': {'trend': 'up', 'momentum': 'strong', 'trend_confidence': 0.7}
            }

    Returns:
        {
            'conflict': True/False,
            'conflict_type': 'bullish_divergence' | 'bearish_divergence' | None,
            'alignment_score': 0.0-1.0,  # 1.0 = 完全对齐
            'strength': 0.0-1.0,
            'implication': 'potential_bottom_forming' | 'potential_top_forming' | None,
            'action_bias': 'reduce_shorts_watch_longs' | 'reduce_longs_watch_shorts' | None,
            'details': {...}
        }
    """
    h4 = tf_states.get('4h', {})
    h1 = tf_states.get('1h', {})
    m15 = tf_states.get('15m', {})

    h4_trend = h4.get('trend', 'range')
    h4_momentum = h4.get('momentum', 'strong')
    h4_confidence = h4.get('trend_confidence', 0.5)

    h1_trend = h1.get('trend', 'range')
    h1_momentum = h1.get('momentum', 'strong')
    h1_confidence = h1.get('trend_confidence', 0.5)

    m15_trend = m15.get('trend', 'range')
    m15_momentum = m15.get('momentum', 'strong')
    m15_confidence = m15.get('trend_confidence', 0.5)

    bullish_score = 0.0
    bearish_score = 0.0

    if h4_trend == 'down':
        if h4_momentum == 'reversing':
            bullish_score += 0.4 * h4_confidence
        elif h4_momentum == 'weakening':
            bullish_score += 0.2 * h4_confidence
        else:
            bearish_score += 0.3 * h4_confidence
    elif h4_trend == 'up':
        if h4_momentum == 'reversing':
            bearish_score += 0.4 * h4_confidence
        elif h4_momentum == 'weakening':
            bearish_score += 0.2 * h4_confidence
        else:
            bullish_score += 0.3 * h4_confidence

    if h1_trend == 'up':
        _h1_mult = 0.5 if h1_momentum == 'reversing' else 1.0
        if h4_trend == 'down':
            bullish_score += 0.3 * h1_confidence * _h1_mult
        else:
            bullish_score += 0.15 * h1_confidence * _h1_mult
    elif h1_trend == 'down':
        _h1_mult = 0.5 if h1_momentum == 'reversing' else 1.0
        if h4_trend == 'up':
            bearish_score += 0.3 * h1_confidence * _h1_mult
        else:
            bearish_score += 0.15 * h1_confidence * _h1_mult
    if h1_momentum == 'reversing' and h1_trend not in ('up', 'down'):
        if h4_trend == 'down':
            bullish_score += 0.2 * h1_confidence
        elif h4_trend == 'up':
            bearish_score += 0.2 * h1_confidence

    if m15_trend == 'up' and m15_momentum == 'strong':
        bullish_score += 0.2 * m15_confidence
    elif m15_trend == 'up':
        bullish_score += 0.1 * m15_confidence

    if m15_trend == 'down' and m15_momentum == 'strong':
        bearish_score += 0.2 * m15_confidence
    elif m15_trend == 'down':
        bearish_score += 0.1 * m15_confidence

    result = {
        'conflict': False,
        'conflict_type': None,
        'alignment_score': 1.0,
        'strength': 0.0,
        'implication': None,
        'action_bias': None,
        'details': {
            'bullish_score': round(bullish_score, 3),
            'bearish_score': round(bearish_score, 3),
            'h4': {'trend': h4_trend, 'momentum': h4_momentum},
            'h1': {'trend': h1_trend, 'momentum': h1_momentum},
            '15m': {'trend': m15_trend, 'momentum': m15_momentum}
        }
    }

    if h4_trend == 'down' and bullish_score >= 0.5:
        result['conflict'] = True
        result['conflict_type'] = 'bullish_divergence'
        result['strength'] = round(bullish_score, 3)
        result['implication'] = 'potential_bottom_forming'
        result['action_bias'] = 'reduce_shorts_watch_longs'
        result['alignment_score'] = round(1.0 - bullish_score, 3)

    elif h4_trend == 'up' and bearish_score >= 0.5:
        result['conflict'] = True
        result['conflict_type'] = 'bearish_divergence'
        result['strength'] = round(bearish_score, 3)
        result['implication'] = 'potential_top_forming'
        result['action_bias'] = 'reduce_longs_watch_shorts'
        result['alignment_score'] = round(1.0 - bearish_score, 3)

    elif h4_trend == 'down' and bullish_score >= 0.35:
        result['conflict'] = True
        result['conflict_type'] = 'weak_bullish_divergence'
        result['strength'] = round(bullish_score, 3)
        result['implication'] = 'trend_weakening'
        result['action_bias'] = 'reduce_shorts'
        result['alignment_score'] = round(1.0 - bullish_score * 0.5, 3)

    elif h4_trend == 'up' and bearish_score >= 0.35:
        result['conflict'] = True
        result['conflict_type'] = 'weak_bearish_divergence'
        result['strength'] = round(bearish_score, 3)
        result['implication'] = 'trend_weakening'
        result['action_bias'] = 'reduce_longs'
        result['alignment_score'] = round(1.0 - bearish_score * 0.5, 3)

    else:
        max_score = max(bullish_score, bearish_score)
        result['alignment_score'] = round(1.0 - max_score * 0.3, 3)

    return result


def calculate_reversal_risk(
    trend: str,
    momentum: str,
    reversal_confidence: float,
    mtf_alignment: Dict
) -> str:
    """
    综合计算反转风险等级

    Returns:
        'low' | 'medium' | 'high' | 'very_high'
    """
    score = 0.0

    if momentum == 'reversing':
        score += 0.35
    elif momentum == 'weakening':
        score += 0.15

    score += reversal_confidence * 0.4

    if mtf_alignment.get('conflict'):
        conflict_type = mtf_alignment.get('conflict_type', '')
        if 'divergence' in conflict_type and 'weak' not in conflict_type:
            score += 0.25
        elif 'weak' in conflict_type:
            score += 0.15

    if score >= 0.7:
        return 'very_high'
    elif score >= 0.5:
        return 'high'
    elif score >= 0.3:
        return 'medium'
    else:
        return 'low'


def _get_primary_bias(trend: str, momentum: str, confidence: float) -> str:
    """确定主要交易偏向"""
    if trend == 'down':
        if momentum == 'reversing' and confidence >= 0.6:
            return 'cautious_bullish'
        elif momentum == 'reversing' or momentum == 'weakening':
            return 'neutral'
        else:
            return 'bearish'
    elif trend == 'up':
        if momentum == 'reversing' and confidence >= 0.6:
            return 'cautious_bearish'
        elif momentum == 'reversing' or momentum == 'weakening':
            return 'neutral'
        else:
            return 'bullish'
    else:
        return 'neutral'


def _advice_for_shorts(trend: str, momentum: str, confidence: float) -> Dict:
    """针对空头持仓的建议"""
    if trend != 'down':
        return {'action': 'not_applicable', 'reason': 'trend_not_down'}

    if confidence >= 0.7:
        return {
            'action': 'close_75%',
            'reason': 'high_reversal_probability',
            'urgency': 'high'
        }
    elif confidence >= 0.5:
        return {
            'action': 'close_50%',
            'reason': 'elevated_reversal_risk',
            'urgency': 'medium'
        }
    elif confidence >= 0.35 or momentum == 'weakening':
        return {
            'action': 'tighten_stop_loss',
            'reason': 'weakening_momentum',
            'urgency': 'low'
        }
    else:
        return {
            'action': 'hold',
            'reason': 'trend_intact',
            'urgency': 'none'
        }


def _advice_for_longs(trend: str, momentum: str, confidence: float) -> Dict:
    """针对多头持仓的建议"""
    if trend != 'up':
        return {'action': 'not_applicable', 'reason': 'trend_not_up'}

    if confidence >= 0.7:
        return {
            'action': 'close_75%',
            'reason': 'high_reversal_probability',
            'urgency': 'high'
        }
    elif confidence >= 0.5:
        return {
            'action': 'close_50%',
            'reason': 'elevated_reversal_risk',
            'urgency': 'medium'
        }
    elif confidence >= 0.35 or momentum == 'weakening':
        return {
            'action': 'tighten_stop_loss',
            'reason': 'weakening_momentum',
            'urgency': 'low'
        }
    else:
        return {
            'action': 'hold',
            'reason': 'trend_intact',
            'urgency': 'none'
        }


def _advice_for_new(
    trend: str,
    momentum: str,
    confidence: float,
    signals: List[str]
) -> Dict:
    """针对新开仓的建议"""
    if momentum == 'reversing' and confidence >= 0.7:
        if trend == 'down':
            return {
                'action': 'can_initiate_small_long',
                'size': '50%_of_normal',
                'reason': 'high_reversal_confidence',
                'wait_for': 'breakout_confirmation'
            }
        elif trend == 'up':
            return {
                'action': 'can_initiate_small_short',
                'size': '50%_of_normal',
                'reason': 'high_reversal_confidence',
                'wait_for': 'breakdown_confirmation'
            }

    elif momentum == 'reversing' and confidence >= 0.5:
        return {
            'action': 'wait_for_confirmation',
            'reason': 'reversal_signals_present_but_unconfirmed',
            'watch': signals[:3] if signals else []
        }

    elif momentum == 'weakening':
        return {
            'action': 'avoid_trend_following',
            'reason': 'momentum_weakening',
            'alternative': 'wait_for_clarity'
        }

    else:
        if trend == 'down':
            return {
                'action': 'can_short_with_trend',
                'reason': 'trend_intact',
                'caution': 'use_proper_risk_management'
            }
        elif trend == 'up':
            return {
                'action': 'can_long_with_trend',
                'reason': 'trend_intact',
                'caution': 'use_proper_risk_management'
            }
        else:
            return {
                'action': 'wait',
                'reason': 'no_clear_trend'
            }


def _generate_scenarios(
    trend: str,
    momentum: str,
    current_price: float,
    key_levels: Dict[str, float],
    assessment: Dict
) -> List[Dict]:
    """生成关键价格情景规划"""
    scenarios = []

    breakout_level = (
        key_levels.get('confirm_above') or
        key_levels.get('breakout_level') or
        key_levels.get('swing_high')
    )
    breakdown_level = (
        key_levels.get('invalidation_below') or
        key_levels.get('hl_invalidation') or
        key_levels.get('swing_low')
    )

    if breakout_level and breakout_level > current_price:
        distance_pct = (breakout_level - current_price) / current_price * 100
        priority = _calc_scenario_priority('up', distance_pct, trend, momentum, assessment)

        scenarios.append({
            'trigger': 'price_breaks_above',
            'level': breakout_level,
            'distance_pct': round(distance_pct, 2),
            'interpretation': 'reversal_confirmed' if trend == 'down' else 'trend_continuation',
            'priority': priority,
            'time_relevance': _calc_time_relevance(distance_pct),
            'action': {
                'shorts': 'close_remaining',
                'longs': 'can_add_position' if trend == 'up' else 'initiate_long',
                'new': 'initiate_long',
                'stop_loss': breakdown_level,
                'target_1': round(breakout_level * 1.02, 2),
                'target_2': round(breakout_level * 1.05, 2)
            },
            'new_bias': 'bullish'
        })

    if breakdown_level and breakdown_level < current_price:
        distance_pct = (current_price - breakdown_level) / current_price * 100
        priority = _calc_scenario_priority('down', distance_pct, trend, momentum, assessment)

        scenarios.append({
            'trigger': 'price_breaks_below',
            'level': breakdown_level,
            'distance_pct': round(distance_pct, 2),
            'interpretation': 'reversal_invalidated' if trend == 'down' else 'reversal_confirmed',
            'priority': priority,
            'time_relevance': _calc_time_relevance(distance_pct),
            'action': {
                'shorts': 'can_add_position' if trend == 'down' else 'initiate_short',
                'longs': 'close_all',
                'new': 'initiate_short',
                'stop_loss': breakout_level or round(current_price * 1.02, 2),
                'target_1': round(breakdown_level * 0.98, 2),
                'target_2': round(breakdown_level * 0.95, 2)
            },
            'new_bias': 'bearish'
        })

    if breakout_level and breakdown_level:
        scenarios.append({
            'trigger': 'price_consolidates',
            'range': [breakdown_level, breakout_level],
            'interpretation': 'accumulation_or_distribution',
            'priority': 0.3,
            'time_relevance': 'medium_term',
            'action': {
                'shorts': 'hold_reduced_position' if trend == 'down' else 'wait',
                'longs': 'hold_reduced_position' if trend == 'up' else 'wait',
                'new': 'no_action_wait_for_breakout'
            },
            'watch_for': [
                'volume_increase_on_breakout',
                'time_consolidation_exceeds_24h'
            ]
        })

    confidence = assessment.get('confidence', 0)
    if confidence < 0.7 and momentum in ('weakening', 'reversing'):
        scenarios.append({
            'trigger': 'confidence_increases_above_0.7',
            'conditions': [
                f'Price closes above {breakout_level}' if breakout_level else 'Breakout level TBD',
                'Volume exceeds 20-period average',
                'RSI breaks above 50' if trend == 'down' else 'RSI breaks below 50'
            ],
            'priority': 0.4,
            'time_relevance': 'near_term',
            'action': {
                'new': 'can_initiate_small_position',
                'size': '50%_of_normal'
            }
        })

    scenarios.sort(key=lambda x: x.get('priority', 0), reverse=True)
    return scenarios[:3]


def _calc_scenario_priority(
    direction: str,
    distance_pct: float,
    trend: str,
    momentum: str,
    assessment: Dict
) -> float:
    """计算情景优先级"""
    priority = 0.5

    if distance_pct < 1.0:
        priority += 0.3
    elif distance_pct < 2.0:
        priority += 0.2
    elif distance_pct < 3.0:
        priority += 0.1

    confidence = assessment.get('confidence', 0)
    if direction == 'up' and trend == 'down' and momentum == 'reversing':
        priority += 0.2 * confidence
    elif direction == 'down' and trend == 'up' and momentum == 'reversing':
        priority += 0.2 * confidence

    return min(1.0, round(priority, 2))


def _calc_time_relevance(distance_pct: float) -> str:
    """计算情景的时效性"""
    if distance_pct < 0.5:
        return 'imminent'
    elif distance_pct < 1.5:
        return 'near_term'
    else:
        return 'medium_term'


def _generate_risk_warnings(
    trend: str,
    momentum: str,
    assessment: Dict,
    mtf_alignment: Dict = None
) -> List[Dict]:
    """生成风险提示"""
    warnings = []

    if mtf_alignment and mtf_alignment.get('conflict'):
        warnings.append({
            'type': 'timeframe_divergence',
            'severity': 'medium',
            'message': f"Lower timeframes showing opposite momentum - {mtf_alignment.get('conflict_type', 'divergence')}"
        })

    signals = assessment.get('signals', [])
    if 'forming_HL' in signals or 'forming_LH' in signals:
        warnings.append({
            'type': 'unconfirmed_structure',
            'severity': 'low',
            'message': 'Structure change forming but not confirmed - could be trap'
        })

    confidence = assessment.get('confidence', 0)
    if confidence > 0.5 and confidence < 0.7:
        warnings.append({
            'type': 'moderate_confidence',
            'severity': 'low',
            'message': 'Reversal signals present but confidence not high - wait for confirmation'
        })

    if momentum == 'reversing' and confidence < 0.5:
        warnings.append({
            'type': 'conflicting_signals',
            'severity': 'medium',
            'message': 'Momentum suggests reversal but confidence is low - increased volatility expected'
        })

    return warnings


def generate_action_guidance(
    trend: str,
    momentum: str,
    reversal_assessment: Dict,
    current_price: float,
    key_levels: Dict[str, float],
    mtf_alignment: Dict = None,
    position_context: Dict = None
) -> Dict:
    """
    生成分级行动建议和情景规划

    Returns:
        {
            'primary_bias': 'cautious_bullish' | 'neutral' | 'bearish' | ...,
            'confidence': 0.6,
            'for_existing_shorts': {...},
            'for_existing_longs': {...},
            'for_new_positions': {...},
            'scenarios': [...],
            'risk_warnings': [...]
        }
    """
    confidence = reversal_assessment.get('confidence', 0)
    signals = reversal_assessment.get('signals', [])

    guidance = {
        'primary_bias': _get_primary_bias(trend, momentum, confidence),
        'confidence': confidence,
        'for_existing_shorts': _advice_for_shorts(trend, momentum, confidence),
        'for_existing_longs': _advice_for_longs(trend, momentum, confidence),
        'for_new_positions': _advice_for_new(trend, momentum, confidence, signals),
        'scenarios': _generate_scenarios(
            trend, momentum, current_price, key_levels, reversal_assessment
        ),
        'risk_warnings': _generate_risk_warnings(
            trend, momentum, reversal_assessment, mtf_alignment
        )
    }

    return guidance
