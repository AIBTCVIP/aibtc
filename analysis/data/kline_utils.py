# kline_utils.py - 公共 K 线工具函数
"""
K线数据工具模块

提供统一的 K 线数据获取和解析功能，避免各分析模块重复实现。

Usage:
    from analysis.data.kline_utils import get_klines_from_redis, extract_ohlcv
"""

import json
import logging
import numpy as np
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from core.database import redis_client, RedisKeys

logger = logging.getLogger(__name__)


# ==========================================================
# 数据结构
# ==========================================================
@dataclass
class OHLCV:
    """OHLCV 数据结构"""
    timestamps: np.ndarray
    opens: np.ndarray
    highs: np.ndarray
    lows: np.ndarray
    closes: np.ndarray
    volumes: np.ndarray
    
    def __len__(self):
        return len(self.closes)
    
    @property
    def typical_prices(self) -> np.ndarray:
        """典型价格 = (H + L + C) / 3"""
        return (self.highs + self.lows + self.closes) / 3


# ==========================================================
# Redis K 线获取
# ==========================================================
def get_klines_from_redis(
    symbol: str,
    tf: str = "15m",
    limit: int = 100
) -> Optional[List[Dict]]:
    """
    从 Redis 热存储获取 K 线数据
    
    K 线存储在 global:market:klines:hot:{symbol}:{interval}
    
    Args:
        symbol: 交易对 (如 "BTCUSDT")
        tf: 时间周期 (如 "15m", "1h", "4h")
        limit: 获取的 K 线数量
    
    Returns:
        K 线列表，每个元素包含 t, o, h, l, c, v 字段
        如果数据不足返回 None
    """
    try:
        key = RedisKeys.market_klines_hot(symbol, tf)
        data = redis_client.hgetall(key)
        
        if not data:
            logger.debug(f"No kline data found for {symbol}:{tf}")
            return None
        
        # 按时间戳排序
        rows = sorted(data.items(), key=lambda x: int(x[0]))
        
        # 取最后 N 根
        if len(rows) > limit:
            rows = rows[-limit:]
        
        # 转换为标准化格式
        klines = []
        for ts, v in rows:
            try:
                k = json.loads(v) if isinstance(v, str) else v
                klines.append({
                    "t": int(ts),
                    "o": float(k.get("Open", k.get("o", 0))),
                    "h": float(k.get("High", k.get("h", 0))),
                    "l": float(k.get("Low", k.get("l", 0))),
                    "c": float(k.get("Close", k.get("c", 0))),
                    "v": float(k.get("Volume", k.get("v", k.get("Vol", 0)))),
                })
            except Exception as e:
                logger.debug(f"Failed to parse kline: {e}")
                continue
        
        return klines if klines else None
    except Exception as e:
        logger.debug(f"Failed to get klines for {symbol}:{tf}: {e}")
        return None


# ==========================================================
# OHLCV 提取
# ==========================================================
def extract_ohlcv(klines: List[Dict], min_bars: int = 10) -> Optional[OHLCV]:
    """
    从 K 线列表提取 OHLCV numpy 数组
    
    支持多种 key 格式:
    - 小写: o, h, l, c, v, t
    - 大写: Open, High, Low, Close, Volume, Timestamp
    
    Args:
        klines: K 线列表
        min_bars: 最小 K 线数量要求
    
    Returns:
        OHLCV 数据结构，如果数据不足返回 None
    """
    if not klines or len(klines) < min_bars:
        return None
    
    try:
        def get_val(k: Dict, keys: List[str], default: float = 0) -> float:
            """从字典获取值，支持多个 key"""
            for key in keys:
                if key in k:
                    return float(k[key])
            return default
        
        timestamps = np.array([
            get_val(k, ["t", "Timestamp", "timestamp", "time"], 0)
            for k in klines
        ], dtype=np.float64)
        
        opens = np.array([
            get_val(k, ["o", "Open", "open"], 0)
            for k in klines
        ], dtype=np.float64)
        
        highs = np.array([
            get_val(k, ["h", "High", "high"], 0)
            for k in klines
        ], dtype=np.float64)
        
        lows = np.array([
            get_val(k, ["l", "Low", "low"], 0)
            for k in klines
        ], dtype=np.float64)
        
        closes = np.array([
            get_val(k, ["c", "Close", "close"], 0)
            for k in klines
        ], dtype=np.float64)
        
        volumes = np.array([
            get_val(k, ["v", "Volume", "volume", "Vol"], 0)
            for k in klines
        ], dtype=np.float64)
        
        return OHLCV(
            timestamps=timestamps,
            opens=opens,
            highs=highs,
            lows=lows,
            closes=closes,
            volumes=volumes
        )
    except Exception as e:
        logger.debug(f"Failed to extract OHLCV: {e}")
        return None


def extract_returns(klines: List[Dict], min_bars: int = 10) -> Optional[np.ndarray]:
    """
    从 K 线列表提取收益率序列
    
    Args:
        klines: K 线列表
        min_bars: 最小 K 线数量要求
    
    Returns:
        收益率 numpy 数组
    """
    ohlcv = extract_ohlcv(klines, min_bars)
    if ohlcv is None:
        return None
    
    try:
        closes = ohlcv.closes
        returns = np.diff(closes) / closes[:-1]
        return returns
    except Exception as e:
        logger.debug(f"Failed to extract returns: {e}")
        return None
