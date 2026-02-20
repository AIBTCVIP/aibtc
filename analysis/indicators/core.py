# indicators.py
"""
Indicator Calculation Module v4 - Crypto Optimized

Improvements:
1. Coin tier parameter adjustments
2. Integrated v4 structure analyzer features
3. Enhanced output fields (trend confidence, volatility status, breakout strength, etc.)
"""
import time
import json
import logging
import numpy as np
import talib
from typing import Optional, List, Dict, Any
from core.database import redis_client, RedisKeys
from llm.llm_api import add_to_batch
from core.config import timeframes, EMA_CONFIG, STRUCTURE_PARAMS, get_adjusted_params
from analysis.structure.market_structure import MarketStructure
from analysis.assembly.payload_builder import save_unified_payload

logger = logging.getLogger(__name__)


# ==========================================================
# 区间位置：支持 above_range / below_range
# ==========================================================
def calc_range_location(close: float, range_low: float, range_high: float) -> dict:
    if close is None or range_low is None or range_high is None:
        return {"pos": None, "location": "unknown", "out_of_range": False}
    if range_high <= range_low:
        return {"pos": None, "location": "unknown", "out_of_range": False}

    if close < range_low:
        return {"pos": 0.0, "location": "below_range", "out_of_range": True}
    if close > range_high:
        return {"pos": 1.0, "location": "above_range", "out_of_range": True}

    pos = (close - range_low) / (range_high - range_low)
    pos = max(0.0, min(1.0, float(pos)))

    if pos <= 0.2:
        loc = "near_low"
    elif pos >= 0.8:
        loc = "near_high"
    else:
        loc = "middle"

    return {"pos": pos, "location": loc, "out_of_range": False}


# ==========================================================
# [v4] 结构分析器：动态获取（支持币种分级）
# ==========================================================
# LRU 缓存配置
ANALYZER_CACHE_MAX_SIZE = 100  # 最大缓存条目数

# 使用 OrderedDict 实现 LRU
from collections import OrderedDict
import threading

_structure_analyzer_cache: OrderedDict[str, MarketStructure] = OrderedDict()
_analyzer_cache_lock = threading.Lock()


def get_structure_analyzer(symbol: str, interval: str) -> Optional[MarketStructure]:
    """
    获取针对特定币种和周期的结构分析器（带 LRU 缓存）

    Args:
        symbol: 交易对符号（如 BTCUSDT）
        interval: 时间周期（如 4h, 1h, 15m）

    Returns:
        MarketStructure 实例
    """
    cache_key = f"{symbol}:{interval}"

    # V5-22 fix: 将 check-create-insert 合并到单次锁获取中
    # 旧逻辑：先加锁检查缓存，释放锁，创建 analyzer，再加锁插入
    # 两个线程可能同时 miss 缓存并创建重复实例
    with _analyzer_cache_lock:
        # 检查缓存
        if cache_key in _structure_analyzer_cache:
            # 移到末尾（最近使用）
            _structure_analyzer_cache.move_to_end(cache_key)
            return _structure_analyzer_cache[cache_key]

        base_params = STRUCTURE_PARAMS.get(interval)
        if not base_params:
            return None

        # 根据币种分级调整参数
        adjusted_params = get_adjusted_params(symbol, base_params)

        # 创建分析器
        analyzer = MarketStructure(**adjusted_params)

        # 添加到缓存
        _structure_analyzer_cache[cache_key] = analyzer

        # LRU 淘汰：如果超过最大数量，删除最早的
        while len(_structure_analyzer_cache) > ANALYZER_CACHE_MAX_SIZE:
            _structure_analyzer_cache.popitem(last=False)

    return analyzer


def clear_analyzer_cache():
    """清除分析器缓存（如需重新加载参数时使用）"""
    with _analyzer_cache_lock:
        _structure_analyzer_cache.clear()


def get_analyzer_cache_stats() -> dict:
    """获取缓存统计信息"""
    with _analyzer_cache_lock:
        return {
            "size": len(_structure_analyzer_cache),
            "max_size": ANALYZER_CACHE_MAX_SIZE,
            "keys": list(_structure_analyzer_cache.keys())[-5:],  # 最近 5 个
        }


# ==========================================================
# 将单周期结果快照写入 Redis（供聚合器统一裁判/投喂GPT）
# ==========================================================
def save_signal_snapshot(symbol: str, interval: str, indicators: dict, ttl_sec: int = 600):
    key = RedisKeys.signal_snapshot(symbol, interval)
    redis_client.set(key, json.dumps(indicators, ensure_ascii=False), ex=ttl_sec)


# ==========================================================
# 读取 TF 快照（用于 15m signal 受"制度/位置"约束）
# ==========================================================
def get_tf_snapshot(symbol: str, tf: str) -> Optional[dict]:
    try:
        v = redis_client.get(RedisKeys.signal_snapshot(symbol, tf))
        if not v:
            return None
        if isinstance(v, (bytes, bytearray)):
            v = v.decode("utf-8", errors="ignore")
        return json.loads(v)
    except Exception as e:
        logger.debug(f"Failed to get TF snapshot for {symbol}@{tf}: {e}")
        return None


# ==========================================================
# range_break 分类：假突破 / 真突破（15m 用 4H 箱体边界判断）
# ==========================================================
def classify_range_break_15m(
        rows_15m: List[Dict],
        range_low: float,
        range_high: float,
        atr_15m: Optional[float]
) -> str:
    """
    返回：
      - "none"
      - "fake_break_up" / "fake_break_down"
      - "true_break_up" / "true_break_down"

    规则（轻量版）：
      - 用最近 3 根 close：
        * 上一根出界，当前回到区间内 => fake_break_*
        * 当前出界，且连续两根都出界 => true_break_*
        * 当前出界，且超出距离 >= ATR * 0.35 => true_break_*
        * 其它 => none（等待确认）
    """
    if range_low is None or range_high is None or range_high <= range_low:
        return "none"
    if rows_15m is None or len(rows_15m) < 3:
        return "none"

    closes = [float(r["Close"]) for r in rows_15m]
    c1, c2, c3 = closes[-3], closes[-2], closes[-1]

    def side(c: float) -> str:
        if c > range_high:
            return "up"
        if c < range_low:
            return "down"
        return "in"

    s1, s2, s3 = side(c1), side(c2), side(c3)

    # 上一根出界，当前回到区间 => 假突破
    if s2 in ("up", "down") and s3 == "in":
        return f"fake_break_{s2}"

    # 当前出界 => 判断是否站稳
    if s3 in ("up", "down"):
        # 连续两根出界 => 真突破
        if s2 == s3:
            return f"true_break_{s3}"

        # 单根出界：看是否超出足够距离（用 ATR 尺度）
        if atr_15m is not None and atr_15m > 0:
            dist = (c3 - range_high) if s3 == "up" else (range_low - c3)
            if dist >= atr_15m * 0.35:
                return f"true_break_{s3}"

        return "none"

    return "none"


# ==========================================================
# 15m 触发器：受 4H 制度/位置约束 + 假/真突破分类
# [v4] 支持新的 last_break 状态（wick_reject_*）
# ==========================================================
def calc_15m_signal(
        rows_15m: List[Dict],
        structure_15m: dict,
        out_of_range_15m: bool,
        atr_15m: Optional[float],
        tf4h_snapshot: Optional[dict]
) -> str:
    """
    返回：
      - none
      - fake_break_up/down
      - true_break_up/down
      - break_confirmed   （趋势里 bos_up/bos_down）
      - choch_reversal    （边界处 choch_up/choch_down 提示）
      - wick_rejection    （v4 新增：插针被打回）
    """
    if not structure_15m or not structure_15m.get("valid"):
        return "none"

    lb15 = structure_15m.get("last_break", "none")

    # [v4] 处理插针拒绝信号
    if lb15.startswith("wick_reject_"):
        return "wick_rejection"

    # 无 4H 快照必须 return，避免下面引用 None 崩溃
    if not tf4h_snapshot or not tf4h_snapshot.get("structure") or not tf4h_snapshot["structure"].get("valid"):
        if lb15 in ("bos_up", "bos_down"):
            return "break_confirmed"
        return "none"

    s4 = tf4h_snapshot["structure"]
    trend4 = s4.get("trend", "range")
    loc4 = tf4h_snapshot.get("range_location", "unknown")

    # 4H 区间：必须在边界才允许触发
    if trend4 == "range":
        if loc4 not in ("near_low", "near_high"):
            return "none"

        range_low_4h = s4.get("range_low")
        range_high_4h = s4.get("range_high")

        br = classify_range_break_15m(rows_15m, range_low_4h, range_high_4h, atr_15m)
        if br != "none":
            return br

        if lb15 in ("bos_up", "bos_down"):
            return "break_confirmed"

        if lb15 in ("choch_up", "choch_down"):
            return "choch_reversal"

        return "none"

    # 4H 趋势分支仍允许快触发，但 near_low 做空要更严格
    if lb15 in ("bos_up", "bos_down"):
        if loc4 == "near_low" and lb15 == "bos_down":
            rl4, rh4 = s4.get("range_low"), s4.get("range_high")
            br = classify_range_break_15m(rows_15m, rl4, rh4, atr_15m)
            if br != "true_break_down":
                return "none"
        return "break_confirmed"

    return "none"


def pack_klines(rows: List[Dict], limit: int = 20, include_v: bool = True) -> List[Dict]:
    """
    rows: [{"Timestamp":..., "Open":..., "High":..., "Low":..., "Close":..., "Volume":..., "TakerBuyVolume":..., "TakerSellVolume":...}, ...]
    输出紧凑格式：[{t,o,h,l,c,v,tbv,tsv}, ...]
    """
    if not rows:
        return []

    cut = rows[-limit:] if len(rows) > limit else rows
    out = []
    for r in cut:
        k = {
            "t": int(r["Timestamp"]),
            "o": float(r["Open"]),
            "h": float(r["High"]),
            "l": float(r["Low"]),
            "c": float(r["Close"]),
        }

        if include_v:
            v = r.get("Volume", r.get("Vol", r.get("volume", None)))
            if v is not None:
                k["v"] = float(v)

        # 主动买/卖成交量
        tbv = r.get("TakerBuyVolume")
        tsv = r.get("TakerSellVolume")
        if tbv is not None:
            k["tbv"] = float(tbv)
        if tsv is not None:
            k["tsv"] = float(tsv)

        out.append(k)
    return out


# ==========================================================
# 🔥 计算单周期指标
# ==========================================================
def calculate_signal(symbol: str, interval: str):
    # 从Redis全局K线存储获取数据
    from core.database import redis_client, RedisKeys

    rkey = RedisKeys.market_klines_hot(symbol, interval)
    data = redis_client.hgetall(rkey)

    # 如果热数据为空，返回空（可能是首次启动或数据未下载）
    if not data:
        return

    rows = sorted(data.items(), key=lambda x: int(x[0]))
    rows = [{"Timestamp": int(ts), **json.loads(v)} for ts, v in rows]
    if len(rows) < 5:
        return

    # ------------------------------
    # OHLCV arrays - 一次性提取，避免重复计算
    # ------------------------------
    opens = np.array([float(k["Open"]) for k in rows], dtype=np.float64)
    highs = np.array([float(k["High"]) for k in rows], dtype=np.float64)
    lows = np.array([float(k["Low"]) for k in rows], dtype=np.float64)
    closes = np.array([float(k["Close"]) for k in rows], dtype=np.float64)
    volumes = np.array([
        float(k.get("Volume", k.get("Vol", k.get("volume", 0)))) 
        for k in rows
    ], dtype=np.float64)
    
    # 典型价格 (用于 VWAP 等计算)
    typical_prices = (highs + lows + closes) / 3

    last = rows[-1]
    last_ts = last["Timestamp"]
    last_open = float(opens[-1])
    last_high = float(highs[-1])
    last_low = float(lows[-1])
    last_close = float(closes[-1])
    # DEBUG: 打印最后一条K线完整数据（用于指标计算的输入）
    # print(f"\n===== LAST KLINE (for indicators) =====")
    # print(f"symbol={symbol} tf={interval} ts={last_ts} "
    #       f"readable={time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(last_ts/1000))}")
    # print(json.dumps(last, ensure_ascii=False, indent=2))
    # print("======================================\n")

    # ------------------------------
    # EMA
    # ------------------------------
    ema_periods = EMA_CONFIG.get(interval, [])
    ema_values = {}
    for p in ema_periods:
        ema_series = talib.EMA(closes, timeperiod=p)
        ema_values[f"EMA_{p}"] = float(ema_series[-1]) if np.isfinite(ema_series[-1]) else None
        # 保存上一根的值（用于计算斜率）
        if len(ema_series) >= 2 and np.isfinite(ema_series[-2]):
            ema_values[f"EMA_{p}_prev"] = float(ema_series[-2])
        # M18 fix: 保存 5 根前的值，用于更稳定的 4H 斜率计算（20小时窗口）
        if len(ema_series) >= 6 and np.isfinite(ema_series[-6]):
            ema_values[f"EMA_{p}_prev5"] = float(ema_series[-6])

    # ------------------------------
    # ATR
    # ------------------------------
    atr_series = talib.ATR(highs, lows, closes, timeperiod=14)
    atr_current = float(atr_series[-1]) if np.isfinite(atr_series[-1]) else None

    atr_valid = atr_series[np.isfinite(atr_series)]
    if atr_valid.size >= 20:
        atr_ma20 = float(np.nanmean(atr_valid[-20:]))
    elif atr_valid.size > 0:
        atr_ma20 = float(np.nanmean(atr_valid))
    else:
        atr_ma20 = None

    atr_ratio = None
    if atr_current is not None and last_close > 0:
        atr_ratio = float(atr_current / last_close)

    # ------------------------------
    # ATR SMA50（用于波动状态判断）
    # ------------------------------
    atr_sma50 = None
    if atr_valid.size >= 50:
        atr_sma50 = float(np.nanmean(atr_valid[-50:]))
    elif atr_valid.size > 0:
        atr_sma50 = float(np.nanmean(atr_valid))

    # ------------------------------
    # 4H 专用技术指标
    # ------------------------------
    adx_data = {}
    rsi14 = None
    macd_hist = None
    macd_hist_history = []
    supertrend_dir = None

    if interval == "4h":
        # ADX / DMI
        try:
            adx = talib.ADX(highs, lows, closes, timeperiod=14)
            plus_di = talib.PLUS_DI(highs, lows, closes, timeperiod=14)
            minus_di = talib.MINUS_DI(highs, lows, closes, timeperiod=14)

            adx_data = {
                "adx14": float(adx[-1]) if np.isfinite(adx[-1]) else None,
                "di_plus": float(plus_di[-1]) if np.isfinite(plus_di[-1]) else None,
                "di_minus": float(minus_di[-1]) if np.isfinite(minus_di[-1]) else None,
            }
            # ADX 前一根（用于判断斜率）
            if len(adx) >= 2 and np.isfinite(adx[-2]):
                adx_data["adx_prev"] = float(adx[-2])
        except Exception as e:
            logger.debug(f"ADX calculation failed for {symbol}@{interval}: {e}")

        # RSI
        try:
            rsi = talib.RSI(closes, timeperiod=14)
            rsi14 = float(rsi[-1]) if np.isfinite(rsi[-1]) else None
        except Exception as e:
            logger.debug(f"RSI calculation failed for {symbol}@{interval}: {e}")

        # MACD Histogram
        try:
            macd, macd_signal, macd_histogram = talib.MACD(closes, fastperiod=12, slowperiod=26, signalperiod=9)
            macd_hist = float(macd_histogram[-1]) if np.isfinite(macd_histogram[-1]) else None
            # 最近 6 根的 histogram（用于判断连续走弱）
            valid_hist = macd_histogram[np.isfinite(macd_histogram)]
            if len(valid_hist) >= 6:
                macd_hist_history = [float(x) for x in valid_hist[-6:]]
        except Exception as e:
            logger.debug(f"MACD calculation failed for {symbol}@{interval}: {e}")

        # Supertrend（标准实现：基于 ATR 通道 + 趋势延续）
        # 标准 Supertrend 逻辑：
        # - 上升趋势中：使用下轨作为支撑，跌破下轨才转空
        # - 下降趋势中：使用上轨作为阻力，突破上轨才转多
        # - 趋势延续：在通道内保持前一方向
        try:
            atr_mult = 3.0
            atr_period = 10
            atr_st = talib.ATR(highs, lows, closes, timeperiod=atr_period)

            # H1 fix: 全序列迭代计算 Supertrend（标准实现）
            # 旧实现只用 2 根 K 线，缺乏趋势记忆，导致系统性多头偏差
            # 新实现在完整序列上累积 final_upper/final_lower 和 direction
            n = len(closes)
            if n >= atr_period + 2:
                hl2 = (highs + lows) / 2
                upper_band = hl2 + atr_mult * atr_st
                lower_band = hl2 - atr_mult * atr_st

                # 初始化累积数组
                final_upper = np.full(n, np.nan)
                final_lower = np.full(n, np.nan)
                direction = np.zeros(n, dtype=int)  # +1 = up, -1 = down

                # 找到第一个有效 ATR 的位置
                first_valid = atr_period  # ATR 需要 atr_period 根 K 线
                while first_valid < n and not np.isfinite(atr_st[first_valid]):
                    first_valid += 1

                if first_valid < n:
                    final_upper[first_valid] = upper_band[first_valid]
                    final_lower[first_valid] = lower_band[first_valid]
                    # 初始方向：价格在下轨之上 = up
                    direction[first_valid] = 1 if closes[first_valid] > lower_band[first_valid] else -1

                    for i in range(first_valid + 1, n):
                        if not np.isfinite(atr_st[i]):
                            final_upper[i] = final_upper[i - 1]
                            final_lower[i] = final_lower[i - 1]
                            direction[i] = direction[i - 1]
                            continue

                        # 上轨只降不升（下跌趋势中锁定利润）
                        if upper_band[i] < final_upper[i - 1] or closes[i - 1] > final_upper[i - 1]:
                            final_upper[i] = upper_band[i]
                        else:
                            final_upper[i] = final_upper[i - 1]

                        # 下轨只升不降（上升趋势中锁定利润）
                        if lower_band[i] > final_lower[i - 1] or closes[i - 1] < final_lower[i - 1]:
                            final_lower[i] = lower_band[i]
                        else:
                            final_lower[i] = final_lower[i - 1]

                        # 趋势方向判断
                        if direction[i - 1] == 1:  # 前一根是上升趋势
                            if closes[i] < final_lower[i]:
                                direction[i] = -1  # 跌破下轨，转空
                            else:
                                direction[i] = 1   # 延续上升
                        else:  # 前一根是下降趋势
                            if closes[i] > final_upper[i]:
                                direction[i] = 1   # 突破上轨，转多
                            else:
                                direction[i] = -1  # 延续下降

                    supertrend_dir = "up" if direction[-1] == 1 else "down"
        except Exception as e:
            logger.debug(f"Supertrend calculation failed for {symbol}@{interval}: {e}")

    # ------------------------------
    # 1H 专用技术指标
    # ------------------------------
    vwap_d = None
    donchian_data = {}
    bbw = None
    bbw_median = None
    bbw_percentile = None
    is_squeeze = False
    squeeze_duration = 0

    if interval == "1h":
        # VWAP（日内，按 UTC 交易日重置）
        # 加密货币使用 UTC 00:00 作为日切时间
        try:
            # 使用函数开头已计算的 typical_prices 和 volumes

            # 找到当前 UTC 日的开始（按时间戳分割）
            current_ts = rows[-1]["Timestamp"]
            # 计算当天 UTC 00:00 的时间戳（毫秒）
            day_start_ts = (current_ts // 86400000) * 86400000
            
            # 筛选当天的 K 线
            day_indices = []
            for i, r in enumerate(rows):
                if r["Timestamp"] >= day_start_ts:
                    day_indices.append(i)
            
            if day_indices:
                # 计算当天的 VWAP
                tp_today = typical_prices[day_indices]
                vol_today = volumes[day_indices]
                
                if np.sum(vol_today) > 0:
                    vwap_d = float(np.sum(tp_today * vol_today) / np.sum(vol_today))
            else:
                # 没有当天数据，使用最近 24 根作为兜底
                lookback = min(24, len(typical_prices))
                tp_recent = typical_prices[-lookback:]
                vol_recent = volumes[-lookback:]
                if np.sum(vol_recent) > 0:
                    vwap_d = float(np.sum(tp_recent * vol_recent) / np.sum(vol_recent))
        except Exception as e:
            logger.debug(f"VWAP calculation failed for {symbol}@{interval}: {e}")

        # Donchian Channel（20 周期）
        try:
            dc_period = 20
            if len(highs) >= dc_period:
                donchian_upper = float(np.max(highs[-dc_period:]))
                donchian_lower = float(np.min(lows[-dc_period:]))
                # U13 fix: 计算排除当前K线的 prev 值，用于突破检测
                # 包含当前K线的 upper/lower 作为关键位仍然有效
                # 但 close > upper 永远不成立（close <= high <= upper），所以突破检测需要 prev
                if len(highs) >= dc_period + 1:
                    donchian_upper_prev = float(np.max(highs[-(dc_period + 1):-1]))
                    donchian_lower_prev = float(np.min(lows[-(dc_period + 1):-1]))
                else:
                    donchian_upper_prev = donchian_upper
                    donchian_lower_prev = donchian_lower
                donchian_data = {
                    "upper": donchian_upper,
                    "lower": donchian_lower,
                    "mid": (donchian_upper + donchian_lower) / 2,
                    "upper_prev": donchian_upper_prev,
                    "lower_prev": donchian_lower_prev,
                }
        except Exception as e:
            logger.debug(f"Donchian calculation failed for {symbol}@{interval}: {e}")

        # Bollinger Band Width with percentile and squeeze detection
        bbw_percentile = None
        is_squeeze = False
        squeeze_duration = 0
        try:
            bb_upper, bb_mid, bb_lower = talib.BBANDS(closes, timeperiod=20, nbdevup=2, nbdevdn=2)
            if np.isfinite(bb_upper[-1]) and np.isfinite(bb_lower[-1]) and np.isfinite(bb_mid[-1]):
                bbw = float((bb_upper[-1] - bb_lower[-1]) / bb_mid[-1])

                # BBW history (last 100 bars)
                valid_bbw = []
                for i in range(min(100, len(bb_upper))):
                    idx = -(i + 1)
                    if np.isfinite(bb_upper[idx]) and np.isfinite(bb_lower[idx]) and np.isfinite(bb_mid[idx]) and \
                            bb_mid[idx] != 0:
                        valid_bbw.append((bb_upper[idx] - bb_lower[idx]) / bb_mid[idx])

                if valid_bbw:
                    bbw_median = float(np.median(valid_bbw))
                    # NEW: BBW percentile for squeeze detection
                    bbw_percentile = round(float(np.sum(np.array(valid_bbw) <= bbw) / len(valid_bbw) * 100), 1)
                    
                    # NEW: Squeeze detection (BBW below 20th percentile)
                    squeeze_threshold = float(np.percentile(valid_bbw, 20))
                    is_squeeze = bbw <= squeeze_threshold
                    
                    # NEW: Count consecutive squeeze bars
                    if is_squeeze:
                        for i in range(len(valid_bbw)):
                            if valid_bbw[i] <= squeeze_threshold:
                                squeeze_duration += 1
                            else:
                                break
        except Exception as e:
            logger.debug(f"BBW calculation failed for {symbol}@{interval}: {e}")

    # ------------------------------
    # 15M 专用技术指标
    # ------------------------------
    obv_data = {}
    volume_sma20 = None

    if interval == "15m":
        # OBV - 使用函数开头已计算的 volumes
        try:
            obv = talib.OBV(closes, volumes)

            if np.isfinite(obv[-1]):
                obv_data["obv"] = float(obv[-1])
                # 5 根前的 OBV（用于方向判断）
                if len(obv) >= 6 and np.isfinite(obv[-6]):
                    obv_data["obv_prev"] = float(obv[-6])
        except Exception as e:
            logger.debug(f"OBV calculation failed for {symbol}@{interval}: {e}")

        # 成交量 SMA20 - 使用函数开头已计算的 volumes
        try:
            if len(volumes) >= 20:
                volume_sma20 = float(np.mean(volumes[-20:]))
        except Exception as e:
            logger.debug(f"Volume SMA calculation failed for {symbol}@{interval}: {e}")

    # ------------------------------
    # 市场结构 [v4] 使用动态分析器
    # ------------------------------
    ms = get_structure_analyzer(symbol, interval)
    structure = ms.analyze(rows) if ms else {"valid": False, "reason": "no_analyzer"}

    # ------------------------------
    # 区间位置（用本周期结构的 range_low/range_high）
    # ------------------------------
    range_pos = None
    range_loc = "unknown"
    out_of_range = False

    if structure and structure.get("valid"):
        # 优先复用 MarketStructure 已经算好的口径
        range_pos = structure.get("range_pos")
        range_loc = structure.get("range_location", "unknown")
        out_of_range = bool(structure.get("out_of_range", False))

        # fallback：兼容历史版本
        if range_pos is None or range_loc == "unknown":
            rh = structure.get("range_high")
            rl = structure.get("range_low")
            if rh is not None and rl is not None:
                loc_info = calc_range_location(last_close, rl, rh)
                range_pos = loc_info["pos"]
                range_loc = loc_info["location"]
                out_of_range = loc_info["out_of_range"]

    # ------------------------------
    # 事件型K线（客观可复核，不输出形态结论）
    # ------------------------------
    total = last_high - last_low
    body = abs(last_close - last_open)
    upper = last_high - max(last_open, last_close)
    lower = min(last_open, last_close) - last_low

    candle_stats = {
        "body_ratio": float(body / total) if total > 0 else None,
        "upper_wick_ratio": float(upper / total) if total > 0 else None,
        "lower_wick_ratio": float(lower / total) if total > 0 else None,
    }

    candle_events: Dict[str, Any] = {}

    # ------------------------------
    # 15m signal：假/真突破 + 制度约束
    # ------------------------------
    signal = "none"
    tf4h_snapshot = None
    klines = None

    if interval == "15m":
        tf4h_snapshot = get_tf_snapshot(symbol, "4h")

        signal = calc_15m_signal(
            rows_15m=rows,
            structure_15m=structure,
            out_of_range_15m=out_of_range,
            atr_15m=atr_current,
            tf4h_snapshot=tf4h_snapshot,
        )

        # 15m candle_events
        if structure and structure.get("valid"):
            last_hl = structure.get("last_HL")
            last_lh = structure.get("last_LH")
            if last_hl is not None:
                candle_events["close_above_last_HL"] = bool(last_close > float(last_hl))
            if last_lh is not None:
                candle_events["close_below_last_LH"] = bool(last_close < float(last_lh))

        # 复用真假突破分类（基于 4H 箱体）
        if tf4h_snapshot and tf4h_snapshot.get("structure", {}).get("valid"):
            s4 = tf4h_snapshot["structure"]
            range_low_4h = s4.get("range_low")
            range_high_4h = s4.get("range_high")
            range_break_result = classify_range_break_15m(rows, range_low_4h, range_high_4h, atr_current)
            candle_events["range_break_4h_box"] = range_break_result
        else:
            candle_events["range_break_4h_box"] = "none"

        # U17 fix: 扩展到 20 根，微结构/成交量/OBV 分析受益于更大窗口
        # klines 不进入 LLM feed JSON，只在 technical_analyzer 和 order_flow 中消费
        klines = pack_klines(rows, limit=20, include_v=True)

    # ------------------------------
    # 输出
    # ------------------------------
    indicators = {
        "symbol": symbol,
        "tf": interval,
        "timestamp": last_ts,

        # OHLC data for candle analysis
        "open": last_open,
        "high": last_high,
        "low": last_low,
        "close": last_close,
        "atr_ratio": atr_ratio,
        "atr": atr_current,
        "atr_ma20": atr_ma20,

        "ema": ema_values,

        "candle_stats": candle_stats,
        "candle_events": candle_events if interval == "15m" else {},

        "structure": structure,
        "range_location": range_loc,
        "range_pos": range_pos,
        "out_of_range": out_of_range,

        "signal": signal,
    }

    # ------------------------------
    # [v4] 提取结构分析器的新增字段
    # ------------------------------
    if structure and structure.get("valid"):
        # 趋势置信度
        if structure.get("trend_confidence") is not None:
            indicators["trend_confidence"] = structure["trend_confidence"]

        # 波动率状态
        if structure.get("volatility"):
            indicators["volatility"] = structure["volatility"]

        # 突破分析详情
        if structure.get("breakout"):
            indicators["breakout"] = structure["breakout"]

        # 结构健康度
        if structure.get("structure_health"):
            indicators["structure_health"] = structure["structure_health"]
        
        # [v5] 动量状态
        if structure.get("momentum"):
            indicators["momentum"] = structure["momentum"]
        
        # [v5] 反转信号
        if structure.get("reversal_signals"):
            indicators["reversal_signals"] = structure["reversal_signals"]

    # 4H 专用技术指标
    if interval == "4h":
        indicators["adx"] = adx_data
        indicators["rsi14"] = rsi14
        indicators["macd_hist"] = macd_hist
        indicators["macd_hist_history"] = macd_hist_history
        indicators["supertrend_dir"] = supertrend_dir
        indicators["klines"] = pack_klines(rows, limit=5, include_v=False)

    # 1H 专用技术指标
    if interval == "1h":
        indicators["atr_sma50"] = atr_sma50
        indicators["vwap_d"] = vwap_d
        indicators["donchian"] = donchian_data
        indicators["bbw"] = bbw
        indicators["bbw_median"] = bbw_median
        # NEW: Volatility regime indicators
        indicators["bbw_percentile"] = bbw_percentile
        indicators["is_squeeze"] = is_squeeze
        indicators["squeeze_duration"] = squeeze_duration if is_squeeze else 0
        indicators["klines"] = pack_klines(rows, limit=5, include_v=False)

    # 15M 专用技术指标
    if interval == "15m":
        indicators["klines"] = klines
        indicators["obv"] = obv_data.get("obv")
        indicators["obv_prev"] = obv_data.get("obv_prev")
        indicators["volume_sma20"] = volume_sma20

    # ------------------------------
    # 1) 写快照（4h/1h 用更长 TTL，避免 15m 构建 payload 时已过期导致 symbol_analysis 缺 structure/levels）
    # ------------------------------
    ttl = 600
    if interval == "4h":
        ttl = 4 * 3600 + 600  # 4h + 10min buffer
    elif interval == "1h":
        ttl = 3600 + 600      # 1h + 10min buffer
    save_signal_snapshot(symbol, interval, indicators, ttl_sec=ttl)

    # ------------------------------
    # 2) 投喂逻辑
    # ------------------------------
    if interval == "15m":
        # 构建完整 payload（含 referee + ai_enhancement）
        payload = save_unified_payload(symbol)
        if payload:
            # ref = payload.get("referee", {})
            ai = payload.get("ai_enhancement", {})

            # 构建精简版投喂数据（去除冗余）
            feed_data = {
                # 基础信息
                "symbol": symbol,
                "tf": interval,
                "timestamp": indicators.get("timestamp"),
                "close": indicators.get("close"),

                # 核心指标（精简）
                "atr": indicators.get("atr"),
                "atr_ratio": indicators.get("atr_ratio"),
                "ema": indicators.get("ema"),

                # 结构摘要（去除 structure_points 和 meta）
                "structure_summary": {
                    "trend": indicators.get("structure", {}).get("trend"),
                    "bias": indicators.get("structure", {}).get("bias"),
                    "range_high": indicators.get("structure", {}).get("range_high"),
                    "range_low": indicators.get("structure", {}).get("range_low"),
                    "swing_high": indicators.get("structure", {}).get("swing_high"),
                    "swing_low": indicators.get("structure", {}).get("swing_low"),
                    "last_break": indicators.get("structure", {}).get("last_break"),
                    "range_location": indicators.get("range_location"),
                    "range_pos": indicators.get("range_pos"),
                    # [v4] 新增字段
                    "trend_confidence": indicators.get("trend_confidence"),
                    "volatility": indicators.get("volatility"),
                    "breakout": indicators.get("breakout"),
                    "structure_health": indicators.get("structure_health"),
                    # [v5] 新增字段
                    "momentum": indicators.get("momentum"),
                    "reversal_signals": indicators.get("reversal_signals"),
                },

                # 信号
                "signal": indicators.get("signal"),

                # K线数据
                "klines": indicators.get("klines"),

                # 裁判结论
                # "referee": ref,

                # AI 增强数据
                "ai_enhancement": ai,
            }

            add_to_batch(symbol, interval, feed_data)

    elif interval in ["1h", "4h"]:
        # 1h 和 4h 也需要添加到 batch_cache，供 candle_intelligence 使用
        feed_data = {
            # 基础信息
            "symbol": symbol,
            "tf": interval,
            "timestamp": indicators.get("timestamp"),
            
            # OHLC 数据（candle_intelligence 必需）
            "open": indicators.get("open"),
            "high": indicators.get("high"),
            "low": indicators.get("low"),
            "close": indicators.get("close"),
            
            # 核心指标
            "atr": indicators.get("atr"),
            "atr_ratio": indicators.get("atr_ratio"),
            "ema": indicators.get("ema"),
            
            # 结构摘要
            "structure_summary": {
                "trend": indicators.get("structure", {}).get("trend"),
                "bias": indicators.get("structure", {}).get("bias"),
                "range_high": indicators.get("structure", {}).get("range_high"),
                "range_low": indicators.get("structure", {}).get("range_low"),
                "swing_high": indicators.get("structure", {}).get("swing_high"),
                "swing_low": indicators.get("structure", {}).get("swing_low"),
                "range_location": indicators.get("range_location"),
                "range_pos": indicators.get("range_pos"),
                "trend_confidence": indicators.get("trend_confidence"),
                "volatility": indicators.get("volatility"),
            },
            
            # 信号
            "signal": indicators.get("signal"),
        }
        
        # 4h 专用字段
        if interval == "4h":
            feed_data["adx"] = indicators.get("adx")
            feed_data["rsi14"] = indicators.get("rsi14")
            feed_data["macd_hist"] = indicators.get("macd_hist")
            feed_data["supertrend_dir"] = indicators.get("supertrend_dir")
        
        # 1h 专用字段
        if interval == "1h":
            feed_data["bbw"] = indicators.get("bbw")
            feed_data["bbw_percentile"] = indicators.get("bbw_percentile")
            feed_data["is_squeeze"] = indicators.get("is_squeeze")
            feed_data["squeeze_duration"] = indicators.get("squeeze_duration")
            feed_data["donchian"] = indicators.get("donchian")
        
        add_to_batch(symbol, interval, feed_data)


def calculate_signal_single(symbol: str):
    """计算单个币种的所有周期指标"""
    for tf in timeframes:
        calculate_signal(symbol, tf)

# ==================== 批量处理函数 ====================

async def batch_calculate_indicators(symbols: List[str]):
    """
    批量计算多个币种的指标

    Args:
        symbols: 币种列表
    """
    import asyncio

    if not symbols:
        return

    # 并发计算所有币种的指标
    tasks = [asyncio.create_task(_calculate_symbol_indicators(symbol)) for symbol in symbols]

    # 等待所有计算完成
    await asyncio.gather(*tasks, return_exceptions=True)

async def _calculate_symbol_indicators(symbol: str):
    """
    计算单个币种的所有时间周期指标

    Args:
        symbol: 币种名称
    """
    import asyncio
    for interval in timeframes:
        try:
            # 使用 to_thread 避免阻塞事件循环（calculate_signal 是 CPU 密集型）
            await asyncio.to_thread(calculate_signal, symbol, interval)
        except Exception as e:
            logger.warning(f"计算 {symbol} {interval} 指标失败: {e}")
            continue


# ==========================================================
# [v4 新增] 批量重置分析器缓存
# ==========================================================
def reset_analyzer_for_symbol(symbol: str):
    """
    重置特定币种的分析器缓存
    当需要更新该币种的参数时使用
    """
    # M5 fix: 加锁防止并发修改 OrderedDict 导致 RuntimeError
    global _structure_analyzer_cache
    with _analyzer_cache_lock:
        keys_to_remove = [k for k in _structure_analyzer_cache if k.startswith(f"{symbol}:")]
        for k in keys_to_remove:
            del _structure_analyzer_cache[k]


def get_analyzer_cache_info() -> dict:
    """
    获取分析器缓存信息（调试用）
    """
    # V4 fix: 加锁防止并发读取时 OrderedDict 被修改导致 RuntimeError
    with _analyzer_cache_lock:
        return {
            "cached_count": len(_structure_analyzer_cache),
            "cached_keys": list(_structure_analyzer_cache.keys()),
        }


# ==========================================================
# [v5] 多时间框架状态（读 snapshot）；结论层逻辑见 analysis.signal_guidance
# ==========================================================
def get_mtf_states_for_symbol(symbol: str) -> Dict[str, Dict]:
    """
    获取指定币种的多时间框架状态
    
    从 Redis 读取各时间框架的快照数据
    
    Args:
        symbol: 交易对符号
        
    Returns:
        {
            '4h': {'trend': 'down', 'momentum': 'weakening', ...},
            '1h': {...},
            '15m': {...}
        }
    """
    tf_states = {}
    
    for tf in ['4h', '1h', '15m']:
        snapshot = get_tf_snapshot(symbol, tf)
        if snapshot:
            # V2 fix: trend 和 last_break 在 structure 子字典中，不在顶层
            # trend_confidence 和 range_location 在顶层（由 calculate_signal 提取）
            # momentum 在顶层但是 dict 类型（v5 MarketStructure 输出），
            # 需要提取 .state 字段；assess_mtf_alignment 期望 string
            structure = snapshot.get('structure', {})
            raw_momentum = snapshot.get('momentum')
            if isinstance(raw_momentum, dict):
                momentum_str = raw_momentum.get('state', 'strong')
            elif isinstance(raw_momentum, str):
                momentum_str = raw_momentum
            else:
                momentum_str = 'strong'
            
            tf_states[tf] = {
                'trend': structure.get('trend', 'range') if isinstance(structure, dict) else 'range',
                'momentum': momentum_str,
                'trend_confidence': snapshot.get('trend_confidence', 0.5),
                'last_break': structure.get('last_break', 'none') if isinstance(structure, dict) else 'none',
                'range_location': snapshot.get('range_location', 'unknown'),
            }
        else:
            # V5-13 fix: 默认值补充 last_break 和 range_location 字段
            # 旧逻辑：默认 dict 只有 trend/momentum/trend_confidence，
            # 下游 .get('last_break') 返回 None 而非 'none'，与有数据时的格式不一致
            tf_states[tf] = {
                'trend': 'range',
                'momentum': 'strong',
                'trend_confidence': 0.5,
                'last_break': 'none',
                'range_location': 'unknown',
            }
    
    return tf_states



# 结论层逻辑已迁至 analysis.signal_guidance
from analysis.conclusions.signal_guidance import (
    assess_mtf_alignment,
    calculate_reversal_risk,
    generate_action_guidance,
)
