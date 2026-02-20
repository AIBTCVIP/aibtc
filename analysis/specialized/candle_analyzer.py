# candle_analyzer.py
# K线智能解读模块

from typing import Dict, List, Optional
import logging

logger = logging.getLogger(__name__)


class CandleAnalyzer:
    """
    K线智能分析器
    不做新计算,只解读已有K线数据
    预计耗时: 0.15-0.2秒
    """
    
    def __init__(self, config: Optional[Dict] = None):
        self.config = config or {}
        self.timeout_ms = self.config.get('timeout_ms', 200)
        self.enabled = self.config.get('enabled', True)
        self.thresholds_cache = {}
    
    def analyze(self, candle_15m: Dict, candle_1h: Dict, candle_4h: Dict,
                resistance: float, support: float, symbol: str = "",
                historical_data: Optional[List[Dict]] = None,
                correlation_data: Optional[Dict] = None,
                klines_15m: Optional[List[Dict]] = None,
                klines_1h: Optional[List[Dict]] = None,
                klines_4h: Optional[List[Dict]] = None) -> Dict:
        """主分析函数"""
        if not self.enabled:
            return self._empty_result()
        
        try:
            if not self._is_valid_candle(candle_15m):
                return self._empty_result()
            
            prev_15m = self._extract_prev(klines_15m)
            prev_1h = self._extract_prev(klines_1h)
            prev_4h = self._extract_prev(klines_4h)
            
            return {
                "15m": self._analyze_single_candle(candle_15m, resistance, support, "15m", prev_15m),
                "1h": self._analyze_single_candle(candle_1h, resistance, support, "1h", prev_1h) if self._is_valid_candle(candle_1h) else {},
                "4h": self._analyze_single_candle(candle_4h, resistance, support, "4h", prev_4h) if self._is_valid_candle(candle_4h) else {},
                "multi_timeframe": self._check_timeframe_alignment(candle_15m, candle_1h, candle_4h)
            }
        except Exception as e:
            logger.error(f"CandleAnalyzer error for {symbol}: {e}")
            return self._fallback_result()
    
    def _extract_prev(self, klines: Optional[List[Dict]]) -> Optional[Dict]:
        """从 klines 提取前一根 K 线"""
        if not klines or len(klines) < 2:
            return None
        raw = klines[-2]
        prev = {'open': raw.get('o'), 'high': raw.get('h'),
                'low': raw.get('l'), 'close': raw.get('c')}
        return prev if self._is_valid_candle(prev) else None
    
    def _is_valid_candle(self, candle: Dict) -> bool:
        """检查K线数据是否有效"""
        if not candle:
            return False
        required_fields = ['open', 'high', 'low', 'close']
        return all(k in candle and candle[k] is not None for k in required_fields)
    
    def _analyze_single_candle(self, candle: Dict, resistance: float, 
                               support: float, timeframe: str,
                               prev_candle: Optional[Dict] = None) -> Dict:
        """分析K线 - 支持单根+双根形态"""
        
        if not candle or not all(k in candle for k in ['open', 'high', 'low', 'close']):
            return {}
        
        body = abs(candle['close'] - candle['open'])
        upper_wick = candle['high'] - max(candle['close'], candle['open'])
        lower_wick = min(candle['close'], candle['open']) - candle['low']
        candle_range = candle['high'] - candle['low']
        
        if candle_range == 0:
            candle_range = 0.0001
        
        is_bullish = candle['close'] > candle['open']
        
        candle_type = self._identify_type(candle, body, upper_wick, lower_wick, candle_range, is_bullish)
        rejection, rej_strength = self._detect_rejection_compact(body, upper_wick, lower_wick, candle['open'])
        close_zone = self._get_close_zone(candle, candle_range)
        close_pct = round((candle['close'] - candle['low']) / candle_range * 100, 1)
        vs_levels = self._check_vs_levels_compact(candle, resistance, support)
        warnings = self._generate_warnings_compact(candle_type, rejection, vs_levels, close_zone)
        
        result = {
            "type": candle_type,
            "body_pct": round(body / candle['open'] * 100, 2) if candle['open'] > 0 else 0,
            "close_pct": close_pct,
            "close_zone": close_zone
        }
        
        # 双根形态检测
        two_bar = self._detect_two_bar_pattern(candle, prev_candle, body, is_bullish)
        if two_bar:
            result["two_bar_pattern"] = two_bar
        
        if rejection:
            result["rejection"] = rejection
            result["rejection_strength"] = rej_strength
        
        if warnings:
            result["warnings"] = warnings
        
        if vs_levels:
            result.update(vs_levels)
        
        return result
    
    def _identify_type(self, candle: Dict, body: float, upper_wick: float, 
                       lower_wick: float, candle_range: float, is_bullish: bool) -> str:
        """识别K线类型"""
        if candle_range == 0:
            return "doji"
        
        body_ratio = body / candle_range
        
        # doji 变体
        if body_ratio < 0.1:
            if lower_wick > upper_wick * 3:
                return "dragonfly_doji"
            if upper_wick > lower_wick * 3:
                return "gravestone_doji"
            return "doji"
        
        # marubozu（光头光脚，几乎无影线）
        if body_ratio > 0.9:
            return "marubozu_bull" if is_bullish else "marubozu_bear"
        
        # 长影线形态
        if upper_wick > body * 2 and lower_wick < body * 0.5:
            return "shooting_star" if is_bullish else "inverted_hammer"
        
        if lower_wick > body * 2 and upper_wick < body * 0.5:
            return "hammer" if is_bullish else "hanging_man"
        
        # 纺锤线（小实体 + 双影线）
        if body_ratio < 0.3 and upper_wick > body and lower_wick > body:
            return "spinning_top"
        
        # 强势 K 线
        if body_ratio > 0.7:
            return "strong_bullish" if is_bullish else "strong_bearish"
        
        return "bullish" if is_bullish else "bearish"
    
    def _detect_two_bar_pattern(self, candle: Dict, prev: Optional[Dict],
                                body: float, is_bullish: bool) -> Optional[str]:
        """双根K线形态检测"""
        if not prev:
            return None
        prev_body = abs(prev['close'] - prev['open'])
        prev_bullish = prev['close'] > prev['open']
        prev_range = prev['high'] - prev['low']
        if prev_body == 0 or prev_range == 0:
            return None
        
        # 吞没形态：当前实体完全包裹前一根实体，且方向相反
        if is_bullish and not prev_bullish:
            if candle['close'] > prev['open'] and candle['open'] < prev['close']:
                return "bullish_engulfing"
        if not is_bullish and prev_bullish:
            if candle['close'] < prev['open'] and candle['open'] > prev['close']:
                return "bearish_engulfing"
        
        # 孕线：当前实体完全在前一根实体内部
        cur_hi = max(candle['open'], candle['close'])
        cur_lo = min(candle['open'], candle['close'])
        prev_hi = max(prev['open'], prev['close'])
        prev_lo = min(prev['open'], prev['close'])
        if cur_hi < prev_hi and cur_lo > prev_lo and body < prev_body * 0.5:
            return "harami_bull" if is_bullish else "harami_bear"
        
        # 平头顶/底
        price_tol = candle['close'] * 0.001
        if abs(candle['high'] - prev['high']) < price_tol:
            return "tweezer_top"
        if abs(candle['low'] - prev['low']) < price_tol:
            return "tweezer_bottom"
        
        return None
    
    def _detect_rejection_compact(self, body: float, upper_wick: float, 
                                   lower_wick: float, open_price: float) -> tuple:
        """精简版拒绝信号检测 - 返回 (direction_str, strength_str)"""
        upper_rejection = upper_wick > body * 2
        lower_rejection = lower_wick > body * 2
        
        if not (upper_rejection or lower_rejection):
            return ("", "")
        
        rejection_size = max(upper_wick, lower_wick)
        if rejection_size / open_price > 0.03:
            strength = "strong"
        elif rejection_size / open_price > 0.015:
            strength = "moderate"
        else:
            strength = "weak"
        
        if upper_rejection and lower_rejection:
            return (f"both_{strength}", strength)
        elif upper_rejection:
            return (f"upper_{strength}", strength)
        else:
            return (f"lower_{strength}", strength)
    
    def _get_close_zone(self, candle: Dict, candle_range: float) -> str:
        """获取收盘位置区域"""
        if candle_range == 0:
            return "middle"
        
        position_pct = (candle['close'] - candle['low']) / candle_range * 100
        
        if position_pct >= 80:
            return "upper"
        elif position_pct <= 20:
            return "lower"
        else:
            return "middle"
    
    def _check_vs_levels_compact(self, candle: Dict, resistance: float, support: float) -> Dict:
        """精简版关键位检查 - 只返回有意义的字段"""
        result = {}
        tolerance = 0.003
        
        # 检测是否测试了关键位
        tested_resistance = candle['high'] >= resistance * (1 - tolerance) if resistance else False
        closed_above_resistance = candle['close'] > resistance if resistance else False
        tested_support = candle['low'] <= support * (1 + tolerance) if support else False
        closed_below_support = candle['close'] < support if support else False
        
        # 测试标记（即使没有假突破，LLM 也需要知道是否触及了关键位）
        if tested_resistance:
            result["tested_resistance"] = True
        if tested_support:
            result["tested_support"] = True
        
        # 假突破检测
        if tested_resistance and not closed_above_resistance:
            result["false_breakout"] = "upside"
        elif tested_support and not closed_below_support:
            result["false_breakout"] = "downside"
        
        # 只在距离关键位较近时（<2%）添加距离
        if resistance and resistance > 0:
            dist_r = round((candle['close'] - resistance) / resistance * 100, 2)
            if abs(dist_r) < 2:
                result["dist_resistance_pct"] = dist_r
        
        if support and support > 0:
            dist_s = round((candle['close'] - support) / support * 100, 2)
            if abs(dist_s) < 2:
                result["dist_support_pct"] = dist_s
        
        return result
    
    def _generate_warnings_compact(self, candle_type: str, rejection: str, 
                                   vs_levels: Dict, close_zone: str) -> List[str]:
        """精简版警告生成"""
        warnings = []
        
        # 假突破警告
        if vs_levels.get('false_breakout'):
            warnings.append(f"false_breakout_{vs_levels['false_breakout']}")
        
        # 反转形态警告
        if candle_type in ["shooting_star", "hanging_man"]:
            warnings.append("reversal_top")
        elif candle_type in ["hammer", "inverted_hammer"]:
            warnings.append("reversal_bottom")
        
        # 矛盾信号
        if candle_type in ["strong_bullish", "bullish"] and close_zone == "lower":
            warnings.append("bullish_weakness")
        elif candle_type in ["strong_bearish", "bearish"] and close_zone == "upper":
            warnings.append("bearish_weakness")
        
        return warnings
    
    def _check_timeframe_alignment(self, candle_15m: Dict, candle_1h: Dict, 
                                   candle_4h: Dict) -> Dict:
        """Check multi-timeframe alignment - 精简版"""
        
        is_bullish_15m = candle_15m['close'] > candle_15m['open'] if candle_15m else False
        is_bullish_1h = candle_1h['close'] > candle_1h['open'] if candle_1h else False
        is_bullish_4h = candle_4h['close'] > candle_4h['open'] if candle_4h else False
        
        all_bullish = is_bullish_15m and is_bullish_1h and is_bullish_4h
        all_bearish = not is_bullish_15m and not is_bullish_1h and not is_bullish_4h
        
        result = {
            "aligned": all_bullish or all_bearish,
            "direction": "bullish" if all_bullish else ("bearish" if all_bearish else "mixed"),
        }
        
        # 具体背离描述
        if is_bullish_15m != is_bullish_4h:
            result["divergence"] = "15m_up_4h_down" if is_bullish_15m else "15m_down_4h_up"
        if is_bullish_1h != is_bullish_4h:
            result["1h_divergence"] = "1h_up_4h_down" if is_bullish_1h else "1h_down_4h_up"
        
        return result
    
    def _empty_result(self) -> Dict:
        return {
            "15m": {},
            "1h": {},
            "4h": {},
            "multi_timeframe": {"aligned": True}
        }
    
    def _fallback_result(self) -> Dict:
        return {
            "type": "unknown",
            "close_zone": "middle"
        }


# 全局单例
_candle_analyzer = None

def get_candle_analyzer(config: Optional[Dict] = None) -> CandleAnalyzer:
    """获取全局CandleAnalyzer实例"""
    global _candle_analyzer
    if _candle_analyzer is None:
        _candle_analyzer = CandleAnalyzer(config)
    return _candle_analyzer
