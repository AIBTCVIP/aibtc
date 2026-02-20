# market_structure.py
"""
市场结构识别器 v5 - 加密货币优化版 + 动量检测

针对加密货币市场特性优化：
1. 24/7 交易，无开盘跳空 → 更关注连续性
2. 高波动性 → 动态 ATR 适应
3. 山寨币跟随 BTC → 支持波动率分级
4. 插针频繁 → 增强假突破识别
5. 流动性差异大 → 按币种特性自适应

核心改进：
- [v4] 波动率自适应：根据当前波动率动态调整参数
- [v4] 插针过滤：识别并过滤影线假突破
- [v4] 趋势强度量化：输出趋势置信度
- [v4] 结构新鲜度：标记 pivot 的时效性
- [v4] 多级突破确认：区分试探/确认/强势突破
- [v5] 双维度状态机：Trend（确认的）+ Momentum（实时的）
- [v5] 分层反转信号检测：价格行为 + K线形态 + 结构预判
- [v5] 早期反转识别：不等待结构确认，提前发现潜在反转
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple
from enum import Enum

MS_VERSION = "2025-01-29-crypto-v5"


class BreakoutStrength(Enum):
    """突破强度分级"""
    NONE = "none"
    WICK_ONLY = "wick_only"  # 仅影线突破（插针）
    PENDING = "pending"  # 收盘突破但未确认
    CONFIRMED = "confirmed"  # 连续确认
    STRONG = "strong"  # 强势突破（带量/大幅）


class TrendMomentum(Enum):
    """
    趋势动量状态（实时的，与确认趋势正交）
    
    组合使用：
    - DOWN + STRONG = 坚定做空
    - DOWN + WEAKENING = 减少空仓，不追空
    - DOWN + REVERSING = 准备做多，等确认
    """
    STRONG = "strong"           # 趋势强劲，顺势交易
    WEAKENING = "weakening"     # 趋势减弱，减仓/观望
    REVERSING = "reversing"     # 可能反转，准备反向


@dataclass
class Pivot:
    type: str  # "H" or "L"
    index: int
    price: float
    atr_at_pivot: float = 0.0  # pivot 形成时的 ATR（用于判断显著性）


@dataclass
class StructurePoint:
    type: str  # "H" or "L"
    tag: str  # "HH/LH" or "HL/LL"
    index: int
    price: float
    strength: float = 0.0  # pivot 强度（基于 ATR 倍数）
    bars_ago: int = 0  # 距今多少根 K 线


class MarketStructure:
    """
    结构识别（pivot -> HH/HL/LH/LL -> 趋势 -> BOS/CHoCH）

    加密货币优化特性：
    1) 波动率自适应：高波动时放宽阈值，低波动时收紧
    2) 插针识别：区分影线突破和实体突破
    3) 趋势置信度：量化趋势强度，而非简单分类
    4) 结构时效性：最近的 pivot 权重更高
    5) 动态箱体：根据波动率调整箱体边界缓冲
    """

    def __init__(
            self,
            swing_size: int = 10,
            keep_pivots: int = 14,
            trend_vote_lookback: int = 3,

            use_atr_filter: bool = True,
            atr_period: int = 14,
            min_swing_atr_mult: float = 0.8,
            min_swing_pct: float = 0.0,

            range_pivot_count: int = 6,
            range_break_confirm_bars: int = 2,

            # ✅ [v4 新增] 加密货币优化参数
            volatility_adaptive: bool = True,  # 是否启用波动率自适应
            wick_filter_enabled: bool = True,  # 是否过滤插针
            wick_body_ratio_threshold: float = 0.3,  # 实体占比低于此值视为插针
            trend_confidence_enabled: bool = True,  # 是否输出趋势置信度
            freshness_decay_bars: int = 50,  # pivot 新鲜度衰减周期
    ):
        self.swing_size = int(swing_size)
        self.keep_pivots = int(keep_pivots)
        self.trend_vote_lookback = int(trend_vote_lookback)

        self.use_atr_filter = bool(use_atr_filter)
        self.atr_period = int(atr_period)
        self.min_swing_atr_mult = float(min_swing_atr_mult)
        self.min_swing_pct = float(min_swing_pct)

        self.range_pivot_count = int(range_pivot_count)
        self.range_break_confirm_bars = int(range_break_confirm_bars)

        # v4 新增
        self.volatility_adaptive = bool(volatility_adaptive)
        self.wick_filter_enabled = bool(wick_filter_enabled)
        self.wick_body_ratio_threshold = float(wick_body_ratio_threshold)
        self.trend_confidence_enabled = bool(trend_confidence_enabled)
        self.freshness_decay_bars = int(freshness_decay_bars)

    # ==========================================================
    # ATR 计算（Wilder RMA）
    # ==========================================================
    def _compute_atr(self, highs: List[float], lows: List[float], closes: List[float], period: int) -> List[float]:
        n = len(highs)
        if n == 0:
            return []

        tr = [0.0] * n
        for i in range(n):
            if i == 0:
                tr[i] = highs[i] - lows[i]
            else:
                tr[i] = max(
                    highs[i] - lows[i],
                    abs(highs[i] - closes[i - 1]),
                    abs(lows[i] - closes[i - 1]),
                )

        atr = [0.0] * n
        p = max(1, period)
        init_len = min(p, n)
        atr[init_len - 1] = sum(tr[:init_len]) / init_len

        for i in range(init_len, n):
            atr[i] = (atr[i - 1] * (p - 1) + tr[i]) / p

        for i in range(0, init_len - 1):
            atr[i] = atr[init_len - 1]

        return atr

    # ==========================================================
    # [v4 新增] 波动率状态判定
    # ==========================================================
    def _classify_volatility(self, atr: List[float], closes: List[float]) -> Tuple[str, float]:
        """
        判定当前波动率状态
        返回：(状态, 波动率比率)
        状态：low / normal / high / extreme
        """
        if not atr or not closes or len(atr) < 20:
            return "normal", 1.0

        current_atr = atr[-1]
        current_price = closes[-1]
        if current_price <= 0:
            return "normal", 1.0

        # ATR 占价格的比例
        atr_ratio = current_atr / current_price

        # 计算近期 ATR 中位数作为基准
        recent_atr = atr[-50:] if len(atr) >= 50 else atr
        atr_median = sorted(recent_atr)[len(recent_atr) // 2]

        # 当前 ATR 相对中位数的倍数
        vol_multiplier = current_atr / atr_median if atr_median > 0 else 1.0

        # 分级判定
        if vol_multiplier >= 2.0 or atr_ratio >= 0.03:
            return "extreme", vol_multiplier
        elif vol_multiplier >= 1.5 or atr_ratio >= 0.02:
            return "high", vol_multiplier
        elif vol_multiplier <= 0.6 or atr_ratio <= 0.005:
            return "low", vol_multiplier
        else:
            return "normal", vol_multiplier

    # ==========================================================
    # [v4 新增] 动态调整参数
    # ==========================================================
    def _get_adaptive_params(self, vol_state: str, vol_mult: float) -> dict:
        """
        根据波动率状态动态调整参数
        """
        base_atr_mult = self.min_swing_atr_mult
        base_confirm_bars = self.range_break_confirm_bars

        if vol_state == "extreme":
            # 极端波动：大幅放宽阈值，增加确认要求
            return {
                "atr_mult": base_atr_mult * 1.5,
                "confirm_bars": base_confirm_bars + 1,
                "wick_threshold": self.wick_body_ratio_threshold * 0.8,
            }
        elif vol_state == "high":
            # 高波动：适度放宽
            return {
                "atr_mult": base_atr_mult * 1.2,
                "confirm_bars": base_confirm_bars,
                "wick_threshold": self.wick_body_ratio_threshold * 0.9,
            }
        elif vol_state == "low":
            # 低波动：收紧阈值，更敏感
            return {
                "atr_mult": base_atr_mult * 0.7,
                "confirm_bars": max(1, base_confirm_bars - 1),
                "wick_threshold": self.wick_body_ratio_threshold * 1.2,
            }
        else:
            # 正常
            return {
                "atr_mult": base_atr_mult,
                "confirm_bars": base_confirm_bars,
                "wick_threshold": self.wick_body_ratio_threshold,
            }

    # ==========================================================
    # Pivot 检测
    # ==========================================================
    def _pivot_high(self, highs: List[float], idx: int) -> bool:
        s = self.swing_size
        if idx < s or idx + s >= len(highs):
            return False
        window = highs[idx - s: idx + s + 1]
        h = highs[idx]
        m = max(window)
        if h != m:
            return False
        last_pos = max(i for i, v in enumerate(window) if v == m)
        return last_pos == s

    def _pivot_low(self, lows: List[float], idx: int) -> bool:
        s = self.swing_size
        if idx < s or idx + s >= len(lows):
            return False
        window = lows[idx - s: idx + s + 1]
        l = lows[idx]
        m = min(window)
        if l != m:
            return False
        last_pos = max(i for i, v in enumerate(window) if v == m)
        return last_pos == s

    def _raw_pivots(self, highs: List[float], lows: List[float], atr: Optional[List[float]] = None) -> List[Pivot]:
        raw: List[Pivot] = []
        for i in range(len(highs)):
            atr_val = atr[i] if atr and i < len(atr) else 0.0
            if self._pivot_high(highs, i):
                raw.append(Pivot("H", i, highs[i], atr_val))
            if self._pivot_low(lows, i):
                raw.append(Pivot("L", i, lows[i], atr_val))
        raw.sort(key=lambda p: (p.index, 0 if p.type == "L" else 1))
        return raw

    # ==========================================================
    # Pivot 清洗
    # ==========================================================
    def _resolve_same_index_conflict(self, pivots: List[Pivot], closes: List[float]) -> List[Pivot]:
        by_idx: Dict[int, List[Pivot]] = {}
        for p in pivots:
            by_idx.setdefault(p.index, []).append(p)

        out: List[Pivot] = []
        for idx in sorted(by_idx.keys()):
            ps = by_idx[idx]
            if len(ps) == 1:
                out.append(ps[0])
                continue
            c = closes[idx]
            ps_sorted = sorted(ps, key=lambda x: abs(x.price - c), reverse=True)
            out.append(ps_sorted[0])
        return out

    def _enforce_alternation_keep_extreme(self, pivots: List[Pivot]) -> List[Pivot]:
        if not pivots:
            return []

        out: List[Pivot] = [pivots[0]]
        for p in pivots[1:]:
            last = out[-1]
            if p.type != last.type:
                out.append(p)
                continue

            if p.type == "H":
                if p.price > last.price:
                    out[-1] = p
            else:
                if p.price < last.price:
                    out[-1] = p
        return out

    def _min_swing_filter(self, pivots: List[Pivot], atr: Optional[List[float]], atr_mult: float) -> List[Pivot]:
        if not pivots:
            return []

        out: List[Pivot] = [pivots[0]]
        for p in pivots[1:]:
            prev = out[-1]
            if p.type == prev.type:
                continue

            delta = abs(p.price - prev.price)

            ok = True
            if self.use_atr_filter and atr is not None and len(atr) > p.index:
                thr = atr_mult * atr[p.index]
                ok = delta >= thr
            elif self.min_swing_pct > 0:
                base = max(1e-12, abs(prev.price))
                ok = (delta / base) >= self.min_swing_pct

            if ok:
                out.append(p)

        return out

    # ==========================================================
    # 结构标签
    # ==========================================================
    def _tag_structure(self, pivots: List[Pivot], total_bars: int) -> List[StructurePoint]:
        last_high: Optional[float] = None
        last_low: Optional[float] = None

        pts: List[StructurePoint] = []
        for p in pivots:
            if p.type == "H":
                tag = "HH" if (last_high is None or p.price >= last_high) else "LH"
                last_high = p.price
            else:
                tag = "HL" if (last_low is None or p.price >= last_low) else "LL"
                last_low = p.price

            # 计算强度和时效性
            # strength 表示这个 pivot 相对于 ATR 的显著性（价格变动 / ATR）
            # 需要与前一个 pivot 比较才有意义，这里先记录 ATR 倍数供后续使用
            strength = abs(p.price - (pts[-1].price if pts else p.price)) / p.atr_at_pivot if p.atr_at_pivot > 0 else 0
            bars_ago = total_bars - 1 - p.index

            pts.append(StructurePoint(
                type=p.type,
                tag=tag,
                index=p.index,
                price=p.price,
                strength=strength,
                bars_ago=bars_ago,
            ))
        return pts

    # ==========================================================
    # [v4 优化] 趋势判定 + 置信度
    # ==========================================================
    def _classify_trend(self, points: List[StructurePoint]) -> Tuple[str, float]:
        """
        返回：(趋势方向, 置信度 0-1)
        """
        highs = [p for p in points if p.type == "H"]
        lows = [p for p in points if p.type == "L"]

        if len(highs) < 2 or len(lows) < 2:
            return "range", 0.3

        high_prices = [p.price for p in highs]
        low_prices = [p.price for p in lows]

        # =====================================================
        # 计算趋势得分（考虑时效性加权）
        # =====================================================
        up_score = 0.0
        down_score = 0.0
        total_weight = 0.0

        # 高点序列分析
        for i in range(1, len(highs)):
            # 时效性权重：越新的 pivot 权重越高
            freshness = max(0.3, 1.0 - highs[i].bars_ago / self.freshness_decay_bars)
            weight = freshness

            if high_prices[i] > high_prices[i - 1]:
                up_score += weight
            elif high_prices[i] < high_prices[i - 1]:
                down_score += weight
            total_weight += weight

        # 低点序列分析
        for i in range(1, len(lows)):
            freshness = max(0.3, 1.0 - lows[i].bars_ago / self.freshness_decay_bars)
            weight = freshness

            if low_prices[i] > low_prices[i - 1]:
                up_score += weight
            elif low_prices[i] < low_prices[i - 1]:
                down_score += weight
            total_weight += weight

        if total_weight == 0:
            return "range", 0.3

        # 计算净得分
        net_score = (up_score - down_score) / total_weight  # -1 到 1

        # =====================================================
        # 硬规则校验（最近两个点的关系）
        # =====================================================
        latest_high_rising = high_prices[-1] > high_prices[-2]
        latest_high_not_rising = high_prices[-1] <= high_prices[-2]
        latest_low_falling = low_prices[-1] < low_prices[-2]
        latest_low_not_falling = low_prices[-1] >= low_prices[-2]

        # 强趋势确认
        strong_up = False
        strong_down = False

        if len(high_prices) >= 3 and len(low_prices) >= 3:
            strong_up = (high_prices[-1] > high_prices[-2] > high_prices[-3]) and (low_prices[-1] >= low_prices[-2])
            strong_down = (low_prices[-1] < low_prices[-2] < low_prices[-3]) and (high_prices[-1] <= high_prices[-2])

        # =====================================================
        # 综合判定
        # =====================================================
        if strong_up:
            # net_score 为正时增加置信度
            return "up", min(0.95, 0.7 + abs(net_score) * 0.25)
        if strong_down:
            # net_score 为负时增加置信度（用绝对值确保正向加成）
            return "down", min(0.95, 0.7 + abs(net_score) * 0.25)

        if latest_high_rising and latest_low_not_falling:
            # 上升趋势：net_score 正值增加置信度
            confidence = 0.5 + abs(net_score) * 0.3
            return "up", max(0.4, min(0.85, confidence))

        if latest_low_falling and latest_high_not_rising:
            # 下降趋势：net_score 负值（取绝对值）增加置信度
            confidence = 0.5 + abs(net_score) * 0.3
            return "down", max(0.4, min(0.85, confidence))

        # 投票法兜底
        n = self.trend_vote_lookback
        if len(high_prices) >= n + 1 and len(low_prices) >= n + 1:
            up_votes = 0
            down_votes = 0
            for i in range(-n, 0):
                if high_prices[i] > high_prices[i - 1]:
                    up_votes += 1
                elif high_prices[i] < high_prices[i - 1]:
                    down_votes += 1
                if low_prices[i] > low_prices[i - 1]:
                    up_votes += 1
                elif low_prices[i] < low_prices[i - 1]:
                    down_votes += 1

            max_votes = n * 2
            threshold = max(2, int(max_votes * 0.6))

            if up_votes >= threshold and up_votes > down_votes:
                return "up", 0.4 + (up_votes / max_votes) * 0.3
            if down_votes >= threshold and down_votes > up_votes:
                return "down", 0.4 + (down_votes / max_votes) * 0.3

        return "range", max(0.3, 0.5 - abs(net_score) * 0.2)

    def _last_point(self, points: List[StructurePoint], ptype: str, tag: Optional[str] = None) -> Optional[
        StructurePoint]:
        for p in reversed(points):
            if p.type != ptype:
                continue
            if tag is None or p.tag == tag:
                return p
        return None

    # ==========================================================
    # 箱体边界
    # ==========================================================
    def _range_bounds(self, points: List[StructurePoint]) -> Tuple[Optional[float], Optional[float]]:
        if not points:
            return None, None

        n = max(2, self.range_pivot_count)
        recent = points[-n:] if len(points) >= n else points
        range_high = max(p.price for p in recent)
        range_low = min(p.price for p in recent)
        return range_high, range_low

    # ==========================================================
    # [v4 优化] 突破检测（多级确认 + 插针过滤）
    # ==========================================================
    def _analyze_breakout(
            self,
            rows: List[Dict],
            range_high: Optional[float],
            range_low: Optional[float],
            atr: List[float],
            confirm_bars: int,
            wick_threshold: float,
    ) -> Tuple[str, BreakoutStrength, dict]:
        """
        分析突破状态
        返回：(方向, 强度, 详情)
        """
        if range_high is None or range_low is None:
            return "none", BreakoutStrength.NONE, {}

        if len(rows) < 2 or not atr:
            return "none", BreakoutStrength.NONE, {}

        last_row = rows[-1]
        last_close = float(last_row["Close"])
        last_high = float(last_row["High"])
        last_low = float(last_row["Low"])
        last_open = float(last_row["Open"])
        current_atr = atr[-1] if atr else 0

        # 计算实体比例
        total_range = last_high - last_low
        body = abs(last_close - last_open)
        body_ratio = body / total_range if total_range > 0 else 0

        details = {
            "body_ratio": round(body_ratio, 3),
            "atr": round(current_atr, 4) if current_atr else None,
        }

        # =====================================================
        # 向上突破分析
        # =====================================================
        if last_high > range_high:
            # 检查是否仅影线突破（插针）
            if self.wick_filter_enabled and last_close <= range_high and body_ratio < wick_threshold:
                return "up", BreakoutStrength.WICK_ONLY, {**details, "type": "upper_wick_rejection"}

            # 收盘突破
            if last_close > range_high:
                # 检查连续确认
                if len(rows) >= confirm_bars:
                    recent_closes = [float(r["Close"]) for r in rows[-confirm_bars:]]
                    all_above = all(c > range_high for c in recent_closes)

                    if all_above:
                        # 检查突破幅度
                        breakout_dist = last_close - range_high
                        if current_atr > 0 and breakout_dist >= current_atr * 0.5:
                            return "up", BreakoutStrength.STRONG, {**details,
                                                                   "distance_atr": round(breakout_dist / current_atr,
                                                                                         2)}
                        return "up", BreakoutStrength.CONFIRMED, details

                return "up", BreakoutStrength.PENDING, details

        # =====================================================
        # 向下突破分析
        # =====================================================
        if last_low < range_low:
            # 检查是否仅影线突破（插针）
            if self.wick_filter_enabled and last_close >= range_low and body_ratio < wick_threshold:
                return "down", BreakoutStrength.WICK_ONLY, {**details, "type": "lower_wick_rejection"}

            # 收盘突破
            if last_close < range_low:
                # 检查连续确认
                if len(rows) >= confirm_bars:
                    recent_closes = [float(r["Close"]) for r in rows[-confirm_bars:]]
                    all_below = all(c < range_low for c in recent_closes)

                    if all_below:
                        # 检查突破幅度
                        breakout_dist = range_low - last_close
                        if current_atr > 0 and breakout_dist >= current_atr * 0.5:
                            return "down", BreakoutStrength.STRONG, {**details,
                                                                     "distance_atr": round(breakout_dist / current_atr,
                                                                                           2)}
                        return "down", BreakoutStrength.CONFIRMED, details

                return "down", BreakoutStrength.PENDING, details

        return "none", BreakoutStrength.NONE, details

    # ==========================================================
    # [v4 新增] 结构健康度评估
    # ==========================================================
    def _assess_structure_health(self, points: List[StructurePoint], trend: str) -> dict:
        """
        评估结构健康度
        """
        if len(points) < 4:
            return {"healthy": False, "reason": "insufficient_points"}

        highs = [p for p in points if p.type == "H"]
        lows = [p for p in points if p.type == "L"]

        # 检查结构完整性
        if len(highs) < 2 or len(lows) < 2:
            return {"healthy": False, "reason": "incomplete_structure"}

        # 计算 pivot 间距的一致性（使用变异系数 CV）
        if len(points) >= 4:
            intervals = []
            for i in range(1, len(points)):
                intervals.append(points[i].index - points[i - 1].index)

            avg_interval = sum(intervals) / len(intervals)
            if avg_interval > 0:
                # 使用标准差/均值（变异系数）来衡量一致性
                variance = sum((x - avg_interval) ** 2 for x in intervals) / len(intervals)
                std_dev = variance ** 0.5
                cv = std_dev / avg_interval  # 变异系数
                consistency = 1.0 / (1.0 + cv)  # CV 越小，一致性越高
            else:
                consistency = 0.5
        else:
            consistency = 0.5

        # 检查趋势方向与结构是否一致
        if trend == "up":
            # 上升趋势应该有 HH 和 HL
            has_hh = any(p.tag == "HH" for p in highs[-3:])
            has_hl = any(p.tag == "HL" for p in lows[-3:])
            alignment = 1.0 if (has_hh and has_hl) else 0.5 if (has_hh or has_hl) else 0.3
        elif trend == "down":
            # 下降趋势应该有 LH 和 LL
            has_lh = any(p.tag == "LH" for p in highs[-3:])
            has_ll = any(p.tag == "LL" for p in lows[-3:])
            alignment = 1.0 if (has_lh and has_ll) else 0.5 if (has_lh or has_ll) else 0.3
        else:
            alignment = 0.5

        health_score = (consistency * 0.4 + alignment * 0.6)

        return {
            "healthy": health_score >= 0.5,
            "score": round(health_score, 2),
            "consistency": round(consistency, 2),
            "alignment": round(alignment, 2),
        }

    # ==========================================================
    # [v5 新增] 分层反转信号检测
    # ==========================================================
    def detect_reversal_signals(
        self,
        rows: List[Dict],
        trend: str,
        atr: List[float],
        points: List[StructurePoint]
    ) -> Dict:
        """
        分层检测反转信号，避免重复计分
        
        Layer 1: 价格行为（权重 0.4）- 反弹幅度
        Layer 2: K线形态（权重 0.3）- 拒绝形态、连续阳/阴线
        Layer 3: 结构预判（权重 0.3）- 是否在形成 HL/LH
        
        Returns:
            {
                'momentum': 'strong' | 'weakening' | 'reversing',
                'signals': [...],
                'confidence': 0.0-1.0,
                'key_levels': {...},
                'layer_scores': {'price': x, 'pattern': y, 'structure': z}
            }
        """
        if len(rows) < 10 or not atr:
            return {
                'momentum': TrendMomentum.STRONG.value,
                'signals': [],
                'confidence': 0.0,
                'key_levels': {},
                'layer_scores': {},
                'signal_type': None
            }
        
        current = rows[-1]
        current_close = float(current['Close'])
        current_atr = atr[-1] if atr else 0
        
        # 计算近期高低点
        recent_lows = [float(r['Low']) for r in rows[-20:]]
        recent_highs = [float(r['High']) for r in rows[-20:]]
        recent_low = min(recent_lows)
        recent_high = max(recent_highs)
        
        result = {
            'momentum': TrendMomentum.STRONG.value,
            'signals': [],
            'confidence': 0.0,
            'key_levels': {},
            'layer_scores': {},
            'signal_type': None
        }
        
        # 获取结构关键位
        last_swing_high = None
        last_swing_low = None
        last_ll = None
        last_hh = None
        
        for p in reversed(points):
            if p.type == "H" and last_swing_high is None:
                last_swing_high = p.price
            if p.type == "L" and last_swing_low is None:
                last_swing_low = p.price
            if p.tag == "LL" and last_ll is None:
                last_ll = p.price
            if p.tag == "HH" and last_hh is None:
                last_hh = p.price
        
        # ========== 下降趋势中检测底部信号 ==========
        if trend == "down":
            result = self._detect_bottom_signals(
                rows, current_close, current_atr,
                recent_low, recent_high,
                last_swing_high, last_ll, points
            )
        
        # ========== 上升趋势中检测顶部信号 ==========
        elif trend == "up":
            result = self._detect_top_signals(
                rows, current_close, current_atr,
                recent_low, recent_high,
                last_swing_low, last_hh, points
            )
        
        return result
    
    def _detect_bottom_signals(
        self,
        rows: List[Dict],
        current_close: float,
        current_atr: float,
        recent_low: float,
        recent_high: float,
        last_swing_high: Optional[float],
        last_ll: Optional[float],
        points: List[StructurePoint]
    ) -> Dict:
        """
        检测底部反转信号（下降趋势中）
        """
        signals = []
        key_levels = {}
        
        # ========== Layer 1: 价格行为（权重 0.4）==========
        price_score = 0.0
        
        # 1a. 反弹幅度检测（二选一，取最高）
        bounce_pct = (current_close - recent_low) / recent_low if recent_low > 0 else 0
        bounce_atr = (current_close - recent_low) / current_atr if current_atr > 0 else 0
        
        if bounce_pct >= 0.04 or bounce_atr >= 2.5:
            price_score = 1.0
            signals.append('strong_bounce')
        elif bounce_pct >= 0.025 or bounce_atr >= 1.5:
            price_score = 0.6
            signals.append('moderate_bounce')
        elif bounce_pct >= 0.015 or bounce_atr >= 1.0:
            price_score = 0.3
            signals.append('weak_bounce')
        
        # ========== Layer 2: K线形态（权重 0.3）==========
        pattern_score = 0.0
        
        # 2a. 拒绝形态检测（在低点区域）
        rejection = self._detect_rejection_pattern(rows[-10:], recent_low, current_atr)
        if rejection['type'] == 'strong':
            pattern_score = max(pattern_score, 0.8)
            signals.append('strong_rejection')
        elif rejection['type'] == 'moderate':
            pattern_score = max(pattern_score, 0.5)
            signals.append('rejection_wick')
        
        # 2b. 连续阳线检测（与反弹互补，不重复计分）
        bullish_seq = self._count_bullish_sequence(rows[-6:])
        if bullish_seq >= 4 and price_score < 0.8:
            pattern_score = max(pattern_score, 0.6)
            signals.append('bullish_sequence')
        elif bullish_seq >= 3 and price_score < 0.6:
            pattern_score = max(pattern_score, 0.4)
            signals.append('bullish_candles')
        
        # ========== Layer 3: 结构预判（权重 0.3）==========
        structure_score = 0.0
        
        # 3a. 是否在形成 Higher Low
        if last_ll is not None:
            potential_hl = self._is_forming_higher_low(rows, last_ll, current_atr)
            if potential_hl['forming']:
                structure_score = 0.7
                signals.append('forming_HL')
                key_levels['hl_invalidation'] = potential_hl['invalidation_price']
        
        # 3b. 价格接近前高（突破确认位）
        if last_swing_high:
            distance_to_high_pct = (last_swing_high - current_close) / current_close
            if distance_to_high_pct < 0.02:
                structure_score = max(structure_score, 0.8)
                signals.append('near_prev_high')
                key_levels['breakout_level'] = last_swing_high
            elif distance_to_high_pct < 0.04:
                structure_score = max(structure_score, 0.5)
                key_levels['breakout_level'] = last_swing_high
        
        # 3c. 设置关键位
        if last_ll:
            key_levels['invalidation_below'] = last_ll
        if last_swing_high:
            key_levels['confirm_above'] = last_swing_high
        
        # ========== 综合计算 ==========
        return self._calculate_momentum(
            price_score, pattern_score, structure_score,
            signals, key_levels, 'bottom'
        )
    
    def _detect_top_signals(
        self,
        rows: List[Dict],
        current_close: float,
        current_atr: float,
        recent_low: float,
        recent_high: float,
        last_swing_low: Optional[float],
        last_hh: Optional[float],
        points: List[StructurePoint]
    ) -> Dict:
        """
        检测顶部反转信号（上升趋势中）
        """
        signals = []
        key_levels = {}
        
        # ========== Layer 1: 价格行为（权重 0.4）==========
        price_score = 0.0
        
        # 1a. 回落幅度检测
        drop_pct = (recent_high - current_close) / recent_high if recent_high > 0 else 0
        drop_atr = (recent_high - current_close) / current_atr if current_atr > 0 else 0
        
        if drop_pct >= 0.04 or drop_atr >= 2.5:
            price_score = 1.0
            signals.append('strong_drop')
        elif drop_pct >= 0.025 or drop_atr >= 1.5:
            price_score = 0.6
            signals.append('moderate_drop')
        elif drop_pct >= 0.015 or drop_atr >= 1.0:
            price_score = 0.3
            signals.append('weak_drop')
        
        # ========== Layer 2: K线形态（权重 0.3）==========
        pattern_score = 0.0
        
        # 2a. 上影线拒绝检测
        rejection = self._detect_upper_rejection_pattern(rows[-10:], recent_high, current_atr)
        if rejection['type'] == 'strong':
            pattern_score = max(pattern_score, 0.8)
            signals.append('strong_upper_rejection')
        elif rejection['type'] == 'moderate':
            pattern_score = max(pattern_score, 0.5)
            signals.append('upper_rejection_wick')
        
        # 2b. 连续阴线检测
        bearish_seq = self._count_bearish_sequence(rows[-6:])
        if bearish_seq >= 4 and price_score < 0.8:
            pattern_score = max(pattern_score, 0.6)
            signals.append('bearish_sequence')
        elif bearish_seq >= 3 and price_score < 0.6:
            pattern_score = max(pattern_score, 0.4)
            signals.append('bearish_candles')
        
        # ========== Layer 3: 结构预判（权重 0.3）==========
        structure_score = 0.0
        
        # 3a. 是否在形成 Lower High
        if last_hh is not None:
            potential_lh = self._is_forming_lower_high(rows, last_hh, current_atr)
            if potential_lh['forming']:
                structure_score = 0.7
                signals.append('forming_LH')
                key_levels['lh_invalidation'] = potential_lh['invalidation_price']
        
        # 3b. 价格接近前低（跌破确认位）
        if last_swing_low:
            distance_to_low_pct = (current_close - last_swing_low) / current_close
            if distance_to_low_pct < 0.02:
                structure_score = max(structure_score, 0.8)
                signals.append('near_prev_low')
                key_levels['breakdown_level'] = last_swing_low
            elif distance_to_low_pct < 0.04:
                structure_score = max(structure_score, 0.5)
                key_levels['breakdown_level'] = last_swing_low
        
        # 3c. 设置关键位
        if last_hh:
            key_levels['invalidation_above'] = last_hh
        if last_swing_low:
            key_levels['confirm_below'] = last_swing_low
        
        # ========== 综合计算 ==========
        return self._calculate_momentum(
            price_score, pattern_score, structure_score,
            signals, key_levels, 'top'
        )
    
    def _calculate_momentum(
        self,
        price_score: float,
        pattern_score: float,
        structure_score: float,
        signals: List[str],
        key_levels: Dict,
        signal_type: str
    ) -> Dict:
        """
        综合计算动量状态
        """
        weights = [0.4, 0.3, 0.3]
        scores = [price_score, pattern_score, structure_score]
        
        # 至少两个维度有信号才算有效
        active_dimensions = sum(1 for s in scores if s > 0.2)
        
        momentum = TrendMomentum.STRONG.value
        confidence = 0.0
        
        if active_dimensions >= 2:
            weighted_score = sum(w * s for w, s in zip(weights, scores))
            # 多维度确认加成
            weighted_score *= (1 + 0.1 * (active_dimensions - 1))
            confidence = min(0.85, weighted_score)
            
            # 确定动量状态
            if confidence >= 0.6:
                momentum = TrendMomentum.REVERSING.value
            elif confidence >= 0.35:
                momentum = TrendMomentum.WEAKENING.value
        elif active_dimensions == 1 and max(scores) >= 0.6:
            # 单维度但信号强
            confidence = max(scores) * 0.5
            momentum = TrendMomentum.WEAKENING.value
        
        return {
            'momentum': momentum,
            'signals': signals,
            'confidence': round(confidence, 3),
            'key_levels': key_levels,
            'layer_scores': {
                'price': round(price_score, 2),
                'pattern': round(pattern_score, 2),
                'structure': round(structure_score, 2)
            },
            'signal_type': f'potential_{signal_type}'
        }
    
    def _detect_rejection_pattern(
        self,
        klines: List[Dict],
        ref_low: float,
        atr: float
    ) -> Dict:
        """
        检测下影线拒绝形态（锤子线等）
        """
        strong_rejections = 0
        moderate_rejections = 0
        
        for k in klines:
            body = abs(float(k['Close']) - float(k['Open']))
            lower_wick = min(float(k['Open']), float(k['Close'])) - float(k['Low'])
            upper_wick = float(k['High']) - max(float(k['Open']), float(k['Close']))
            
            # 在低点附近（1.5 ATR 内）
            if float(k['Low']) <= ref_low + 1.5 * atr:
                if lower_wick > body * 2 and lower_wick > upper_wick * 2:
                    if float(k['Close']) > float(k['Open']):  # 收阳
                        strong_rejections += 1
                    else:
                        moderate_rejections += 1
        
        if strong_rejections >= 2:
            return {'type': 'strong', 'count': strong_rejections}
        elif strong_rejections >= 1 or moderate_rejections >= 2:
            return {'type': 'moderate', 'count': strong_rejections + moderate_rejections}
        return {'type': 'none', 'count': 0}
    
    def _detect_upper_rejection_pattern(
        self,
        klines: List[Dict],
        ref_high: float,
        atr: float
    ) -> Dict:
        """
        检测上影线拒绝形态（射击之星等）
        """
        strong_rejections = 0
        moderate_rejections = 0
        
        for k in klines:
            body = abs(float(k['Close']) - float(k['Open']))
            upper_wick = float(k['High']) - max(float(k['Open']), float(k['Close']))
            lower_wick = min(float(k['Open']), float(k['Close'])) - float(k['Low'])
            
            # 在高点附近（1.5 ATR 内）
            if float(k['High']) >= ref_high - 1.5 * atr:
                if upper_wick > body * 2 and upper_wick > lower_wick * 2:
                    if float(k['Close']) < float(k['Open']):  # 收阴
                        strong_rejections += 1
                    else:
                        moderate_rejections += 1
        
        if strong_rejections >= 2:
            return {'type': 'strong', 'count': strong_rejections}
        elif strong_rejections >= 1 or moderate_rejections >= 2:
            return {'type': 'moderate', 'count': strong_rejections + moderate_rejections}
        return {'type': 'none', 'count': 0}
    
    def _count_bullish_sequence(self, klines: List[Dict]) -> int:
        """计算连续阳线数量（从最新往前）"""
        count = 0
        for k in reversed(klines):
            if float(k['Close']) > float(k['Open']):
                count += 1
            else:
                break
        return count
    
    def _count_bearish_sequence(self, klines: List[Dict]) -> int:
        """计算连续阴线数量（从最新往前）"""
        count = 0
        for k in reversed(klines):
            if float(k['Close']) < float(k['Open']):
                count += 1
            else:
                break
        return count
    
    def _is_forming_higher_low(
        self,
        rows: List[Dict],
        last_ll: float,
        atr: float
    ) -> Dict:
        """
        检测是否在形成 Higher Low（未确认）
        """
        # 找最近 10 根 K 线中的最低点
        recent_lows = []
        for i in range(-1, -min(11, len(rows)), -1):
            k = rows[i]
            low = float(k['Low'])
            # 简单的局部低点检测：比前后都低
            if i > -len(rows) + 1 and i < -1:
                prev_low = float(rows[i-1]['Low'])
                next_low = float(rows[i+1]['Low'])
                if low < prev_low and low < next_low:
                    recent_lows.append({'price': low, 'index': i})
            elif i == -1:
                # 最新 K 线，只检查是否比前一根低
                if len(rows) >= 2 and low < float(rows[-2]['Low']):
                    recent_lows.append({'price': low, 'index': i})
        
        if recent_lows:
            lowest_recent = min(recent_lows, key=lambda x: x['price'])
            # 如果最近的低点高于上一个 LL，可能在形成 HL
            if lowest_recent['price'] > last_ll:
                return {
                    'forming': True,
                    'potential_hl': lowest_recent['price'],
                    'invalidation_price': last_ll
                }
        
        return {'forming': False}
    
    def _is_forming_lower_high(
        self,
        rows: List[Dict],
        last_hh: float,
        atr: float
    ) -> Dict:
        """
        检测是否在形成 Lower High（未确认）
        """
        # 找最近 10 根 K 线中的最高点
        recent_highs = []
        for i in range(-1, -min(11, len(rows)), -1):
            k = rows[i]
            high = float(k['High'])
            # 简单的局部高点检测
            if i > -len(rows) + 1 and i < -1:
                prev_high = float(rows[i-1]['High'])
                next_high = float(rows[i+1]['High'])
                if high > prev_high and high > next_high:
                    recent_highs.append({'price': high, 'index': i})
            elif i == -1:
                if len(rows) >= 2 and high > float(rows[-2]['High']):
                    recent_highs.append({'price': high, 'index': i})
        
        if recent_highs:
            highest_recent = max(recent_highs, key=lambda x: x['price'])
            # 如果最近的高点低于上一个 HH，可能在形成 LH
            if highest_recent['price'] < last_hh:
                return {
                    'forming': True,
                    'potential_lh': highest_recent['price'],
                    'invalidation_price': last_hh
                }
        
        return {'forming': False}

    # ==========================================================
    # [v5 新增] 信号位置质量评估
    # ==========================================================
    def assess_signal_location(
        self,
        signal_price: float,
        key_levels: Dict[str, float],
        atr: float
    ) -> Dict:
        """
        评估信号出现的位置质量
        
        在关键支撑/阻力位出现的信号 > 随机位置的信号
        多重支撑叠加的位置 > 单一支撑位
        
        Args:
            signal_price: 信号出现时的价格
            key_levels: 关键价位字典，如 {'daily_support': 85000, 'fib_618': 85500, ...}
            atr: 当前 ATR 值
            
        Returns:
            {
                'quality': 0.0-1.0,
                'nearest_level': 'daily_support_85000',
                'distance_atr': 1.2,
                'level_type': 'major' | 'minor',
                'confluence': {...}  # 如果有多重叠加
            }
        """
        if not key_levels or atr <= 0:
            return {
                'quality': 0.4,
                'nearest_level': None,
                'distance_atr': None,  # P4 Fix: 使用 None 替代 float('inf')，避免 JSON 序列化错误
                'level_type': None,
                'confluence': None
            }
        
        # 按重要性排序的关键位类型
        level_importance = {
            'weekly_support': 1.0,
            'weekly_resistance': 1.0,
            'daily_support': 0.9,
            'daily_resistance': 0.9,
            'swing_low': 0.85,
            'swing_high': 0.85,
            'last_ll': 0.85,
            'last_hh': 0.85,
            'fib_618': 0.75,
            'fib_382': 0.7,
            'h4_support': 0.7,
            'h4_resistance': 0.7,
            'round_number': 0.6,
            'hl_invalidation': 0.8,
            'breakout_level': 0.8,
            'confirm_above': 0.75,
            'confirm_below': 0.75,
            'invalidation_below': 0.8,
            'invalidation_above': 0.8,
        }
        
        best_match = {
            'quality': 0.4,
            'nearest_level': None,
            'distance_atr': None,  # P4 Fix: 使用 None 替代 float('inf')
            'level_type': None,
            'level_price': None
        }
        
        # 检测多重支撑叠加
        confluence_threshold = atr * 1.0  # 1 ATR 范围内视为叠加
        confluence_levels = []
        
        for level_name, level_price in key_levels.items():
            if level_price is None or level_price <= 0:
                continue
                
            distance = abs(signal_price - level_price)
            distance_atr = distance / atr
            distance_pct = distance / level_price
            
            # 判断级别类型和重要性
            level_type = 'minor'
            importance = 0.5
            for key, imp in level_importance.items():
                if key in level_name.lower():
                    level_type = 'major' if imp >= 0.7 else 'minor'
                    importance = imp
                    break
            
            # 距离评分（ATR 和百分比结合）
            if distance_atr <= 0.5 or distance_pct <= 0.005:
                distance_score = 1.0  # 精确触及
            elif distance_atr <= 1.0 or distance_pct <= 0.01:
                distance_score = 0.85
            elif distance_atr <= 1.5 or distance_pct <= 0.015:
                distance_score = 0.7
            elif distance_atr <= 2.0 or distance_pct <= 0.02:
                distance_score = 0.55
            else:
                continue  # 太远，不考虑
            
            # 记录叠加的关键位
            if distance <= confluence_threshold:
                confluence_levels.append({
                    'name': level_name,
                    'price': level_price,
                    'importance': importance,
                    'distance_atr': round(distance_atr, 2)
                })
            
            # 综合质量 = 距离评分 × 级别重要性
            quality = distance_score * importance
            
            if quality > best_match['quality']:
                best_match = {
                    'quality': quality,
                    'nearest_level': level_name,
                    'distance_atr': round(distance_atr, 2),
                    'level_type': level_type,
                    'level_price': level_price
                }
        
        # 多重叠加加成
        confluence_info = None
        if len(confluence_levels) >= 2:
            # 叠加的级别越重要，加成越高
            confluence_bonus = sum(l['importance'] for l in confluence_levels) * 0.1
            best_match['quality'] = min(1.0, best_match['quality'] + confluence_bonus)
            confluence_info = {
                'count': len(confluence_levels),
                'levels': [l['name'] for l in confluence_levels],
                'total_importance': round(sum(l['importance'] for l in confluence_levels), 2),
                'bonus_applied': round(confluence_bonus, 2)
            }
        
        best_match['confluence'] = confluence_info
        best_match['quality'] = round(best_match['quality'], 3)
        
        return best_match

    # ==========================================================
    # 主分析函数
    # ==========================================================
    def analyze(self, rows: List[Dict]) -> Dict:
        min_len = self.swing_size * 2 + 5
        if len(rows) < min_len:
            return {"valid": False, "reason": "not_enough_rows", "need": min_len, "have": len(rows)}

        highs = [float(k["High"]) for k in rows]
        lows = [float(k["Low"]) for k in rows]
        closes = [float(k["Close"]) for k in rows]
        opens = [float(k["Open"]) for k in rows]

        # 计算 ATR
        atr: List[float] = []
        if self.use_atr_filter:
            atr = self._compute_atr(highs, lows, closes, self.atr_period)

        # [v4] 波动率自适应
        vol_state, vol_mult = "normal", 1.0
        adaptive_params = {
            "atr_mult": self.min_swing_atr_mult,
            "confirm_bars": self.range_break_confirm_bars,
            "wick_threshold": self.wick_body_ratio_threshold,
        }

        if self.volatility_adaptive and atr:
            vol_state, vol_mult = self._classify_volatility(atr, closes)
            adaptive_params = self._get_adaptive_params(vol_state, vol_mult)

        # Pivot 检测
        raw = self._raw_pivots(highs, lows, atr)
        if len(raw) < 4:
            return {"valid": False, "reason": "not_enough_pivots_raw", "pivots_found": len(raw)}

        pivots = self._resolve_same_index_conflict(raw, closes)
        pivots = self._enforce_alternation_keep_extreme(pivots)
        pivots = self._min_swing_filter(pivots, atr, adaptive_params["atr_mult"])

        if len(pivots) < 4:
            return {"valid": False, "reason": "not_enough_pivots_clean", "pivots_used": len(pivots)}

        # 结构标签
        all_points = self._tag_structure(pivots, len(rows))
        points = all_points[-self.keep_pivots:] if len(all_points) > self.keep_pivots else all_points

        # 趋势判定
        trend, trend_confidence = self._classify_trend(points)
        range_high, range_low = self._range_bounds(points)

        last_close = closes[-1]
        last_break = "none"

        last_swing_high = self._last_point(points, "H")
        last_swing_low = self._last_point(points, "L")

        last_HL = self._last_point(points, "L", "HL")
        last_LH = self._last_point(points, "H", "LH")

        last_HH = self._last_point(points, "H", "HH")
        last_LL = self._last_point(points, "L", "LL")

        # [v4] 突破分析
        breakout_dir, breakout_strength, breakout_details = "none", BreakoutStrength.NONE, {}

        # BOS / CHoCH 判定
        if trend == "up":
            if last_HL and last_close < last_HL.price:
                last_break = "choch_down"
            elif last_swing_high and last_close > last_swing_high.price:
                last_break = "bos_up"
        elif trend == "down":
            if last_LH and last_close > last_LH.price:
                last_break = "choch_up"
            elif last_swing_low and last_close < last_swing_low.price:
                last_break = "bos_down"
        else:
            # Range 制度：使用增强的突破分析
            breakout_dir, breakout_strength, breakout_details = self._analyze_breakout(
                rows, range_high, range_low, atr,
                adaptive_params["confirm_bars"],
                adaptive_params["wick_threshold"],
            )

            if breakout_strength == BreakoutStrength.STRONG:
                last_break = f"bos_{breakout_dir}"
            elif breakout_strength == BreakoutStrength.CONFIRMED:
                last_break = f"bos_{breakout_dir}"
            elif breakout_strength == BreakoutStrength.PENDING:
                last_break = f"pending_break_{breakout_dir}"
            elif breakout_strength == BreakoutStrength.WICK_ONLY:
                last_break = f"wick_reject_{breakout_dir}"

        bias = 1 if trend == "up" else -1 if trend == "down" else 0

        # 区间位置计算
        range_pos: Optional[float] = None
        out_of_range: bool = False
        range_location: str = "unknown"

        if range_high is not None and range_low is not None and range_high > range_low:
            raw_pos = (last_close - range_low) / (range_high - range_low)

            if last_close < range_low:
                range_pos = 0.0
                out_of_range = True
                range_location = "below_range"
            elif last_close > range_high:
                range_pos = 1.0
                out_of_range = True
                range_location = "above_range"
            else:
                out_of_range = False
                range_pos = max(0.0, min(1.0, float(raw_pos)))
                if range_pos <= 0.2:
                    range_location = "near_low"
                elif range_pos >= 0.8:
                    range_location = "near_high"
                else:
                    range_location = "middle"

        # [v4] 结构健康度
        structure_health = self._assess_structure_health(points, trend)

        debug_points = points[-max(6, min(12, len(points))):]

        result = {
            "valid": True,
            "trend": trend,
            "bias": bias,

            "range_high": range_high,
            "range_low": range_low,

            "swing_high": last_swing_high.price if last_swing_high else None,
            "swing_low": last_swing_low.price if last_swing_low else None,

            "last_HL": last_HL.price if last_HL else None,
            "last_LH": last_LH.price if last_LH else None,
            "last_HH": last_HH.price if last_HH else None,
            "last_LL": last_LL.price if last_LL else None,

            "last_break": last_break,

            "range_location": range_location,
            "range_pos": range_pos,
            "out_of_range": out_of_range,

            "structure_points": [
                {"type": p.type, "tag": p.tag, "price": p.price, "index": p.index}
                for p in debug_points
            ],

            "meta": {
                "swing_size": self.swing_size,
                "keep_pivots": self.keep_pivots,
                "trend_vote_lookback": self.trend_vote_lookback,
                "range_pivot_count": self.range_pivot_count,
                "range_break_confirm_bars": self.range_break_confirm_bars,

                "use_atr_filter": self.use_atr_filter,
                "atr_period": self.atr_period,
                "min_swing_atr_mult": self.min_swing_atr_mult,
                "min_swing_pct": self.min_swing_pct,

                "pivots_found": len(raw),
                "pivots_used": len(pivots),
                "rows_used": len(rows),
            },
        }

        # [v4] 新增输出字段
        if self.trend_confidence_enabled:
            result["trend_confidence"] = round(trend_confidence, 2)

        if self.volatility_adaptive:
            result["volatility"] = {
                "state": vol_state,
                "multiplier": round(vol_mult, 2),
            }

        if breakout_strength != BreakoutStrength.NONE:
            result["breakout"] = {
                "direction": breakout_dir,
                "strength": breakout_strength.value,
                "details": breakout_details,
            }

        result["structure_health"] = structure_health

        # [v5] 反转信号检测 + 动量状态
        reversal_signals = self.detect_reversal_signals(rows, trend, atr, points)
        result["momentum"] = reversal_signals['momentum']
        result["reversal_signals"] = {
            "signals": reversal_signals['signals'],
            "confidence": reversal_signals['confidence'],
            "key_levels": reversal_signals['key_levels'],
            "layer_scores": reversal_signals.get('layer_scores', {}),
            "signal_type": reversal_signals.get('signal_type'),
        }

        return result
