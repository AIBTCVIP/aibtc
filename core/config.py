# config.py
import os

# ==========================================================
# 管理员配置
# ==========================================================
# 管理员用户名列表（支持环境变量配置，多个用逗号分隔）
ADMIN_USERS = [u.strip() for u in os.getenv("ADMIN_USERS", "aibtcvip").split(",") if u.strip()]

# ==========================================================
# 调度器配置
# ==========================================================
# 启动时立即执行一轮（True=立即执行，False=等待下一个15分钟周期）
SCHEDULER_RUN_IMMEDIATELY = False

# 调度器性能配置（根据服务器规格调整）
# 小服务器 (2核4G): batch_size=200, init_concurrent=10
# 中等服务器 (4核8G): batch_size=300, init_concurrent=20
# 大服务器 (8核16G): batch_size=500, init_concurrent=30
# 超大服务器 (12核32G+): batch_size=800, init_concurrent=50
# 注意：init_concurrent 不宜过高，否则会触发交易所 IP 限流
SCHEDULER_BATCH_SIZE = int(os.getenv("SCHEDULER_BATCH_SIZE", "1000"))  # 每批用户数（支持1000用户一次性处理）
SCHEDULER_INIT_CONCURRENT = int(os.getenv("SCHEDULER_INIT_CONCURRENT", "50"))  # 初始化并发数（12核32G可用50）
SCHEDULER_USER_TIMEOUT = float(os.getenv("SCHEDULER_USER_TIMEOUT", "600"))  # 单用户超时（秒）- 增加到 600 秒以适应 LLM 响应

# ==========================================================
# 基础设施配置
# ==========================================================
# Redis 配置（支持环境变量覆盖）
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "10"))

# Web 服务配置
WEB_HOST = os.getenv("WEB_HOST", "0.0.0.0")
WEB_PORT = int(os.getenv("WEB_PORT", "8080"))

# Binance 公共 API（不需要认证）
OI_BASE_URL = "https://fapi.binance.com"

# LLM 配置
LLM_MAX_TOKENS = int(os.getenv("LLM_MAX_TOKENS", "4000"))

# API 限制
KLINE_FETCH_LIMIT = 1500

# ==========================================================
# Telegram 话题配置（系统级，管理员可修改）
# ==========================================================
TOPIC_MAP = {
    "Trading-signals": 58069,      # 交易信号
    "On-chain-monitoring": 58071,  # 链上监控
    "Abnormal-signal": 58065,      # 异常信号
}
DEFAULT_TOPIC = None  # None = 主聊天

# ==========================================================
# 技术分析配置
# ==========================================================

# 时间周期
timeframes = ["4h", "1h", "15m"]

# EMA 参数映射（斐波那契数列）
# 斐波那契周期更贴合市场自然节奏：8, 13, 21, 34, 55, 89, 144
# 配置说明：
#   - 快速 EMA (8/13): 捕捉短期动量变化
#   - 中期 EMA (21/55): 判断趋势方向
#   - 长期 EMA (89/144): 判断大趋势和支撑阻力
EMA_CONFIG = {
    "4h": [21, 55, 89],      # 大周期：中期趋势 + 长期趋势
    "1h": [13, 21, 55],      # 中周期：快速 + 中期 + 长期
    "15m": [8, 21, 55],      # 小周期：超快 + 中期 + 长期
}

# K线数量
KLINE_LIMITS = {
    "15m": 301,
    "1h": 501,
    "4h": 801,
}

# ==========================================================
# 结构计算参数 v4 - 加密货币优化版
# ==========================================================
STRUCTURE_PARAMS = {
    "15m": {
        "swing_size": 3,
        "keep_pivots": 14,
        "trend_vote_lookback": 2,
        "range_pivot_count": 8,
        "min_swing_atr_mult": 0.45,
        "range_break_confirm_bars": 2,
        "volatility_adaptive": True,
        "wick_filter_enabled": True,
        "wick_body_ratio_threshold": 0.25,
        "trend_confidence_enabled": True,
        "freshness_decay_bars": 40,
    },
    "1h": {
        "swing_size": 5,
        "keep_pivots": 14,
        "trend_vote_lookback": 2,
        "range_pivot_count": 8,
        "min_swing_atr_mult": 0.85,
        "range_break_confirm_bars": 2,
        "volatility_adaptive": True,
        "wick_filter_enabled": True,
        "wick_body_ratio_threshold": 0.30,
        "trend_confidence_enabled": True,
        "freshness_decay_bars": 50,
    },
    "4h": {
        "swing_size": 9,
        "keep_pivots": 14,
        "trend_vote_lookback": 3,
        "range_pivot_count": 10,
        "min_swing_atr_mult": 1.1,
        "range_break_confirm_bars": 2,
        "volatility_adaptive": True,
        "wick_filter_enabled": True,
        "wick_body_ratio_threshold": 0.35,
        "trend_confidence_enabled": True,
        "freshness_decay_bars": 60,
    },
}

# ==========================================================
# 币种分级配置
# ==========================================================
COIN_TIER_ADJUSTMENTS = {
    "tier1": {
        "symbols": ["BTCUSDT", "ETHUSDT"],
        "atr_mult_factor": 1.0,
        "wick_threshold_factor": 1.0,
        "confirm_bars_adjust": 0,
    },
    "tier2": {
        "symbols": ["SOLUSDT", "BNBUSDT", "XRPUSDT", "ADAUSDT", "DOGEUSDT",
                    "AVAXUSDT", "DOTUSDT", "LINKUSDT", "MATICUSDT", "LTCUSDT"],
        "atr_mult_factor": 1.1,
        "wick_threshold_factor": 0.9,
        "confirm_bars_adjust": 0,
    },
    "tier3": {
        "symbols": [],
        "atr_mult_factor": 1.2,
        "wick_threshold_factor": 0.8,
        "confirm_bars_adjust": 1,
    },
}


def get_coin_tier(symbol: str) -> str:
    """获取币种分级"""
    for tier, config in COIN_TIER_ADJUSTMENTS.items():
        if symbol in config.get("symbols", []):
            return tier
    return "tier3"


def get_adjusted_params(symbol: str, base_params: dict) -> dict:
    """根据币种分级调整结构参数"""
    tier = get_coin_tier(symbol)
    adjustments = COIN_TIER_ADJUSTMENTS.get(tier, COIN_TIER_ADJUSTMENTS["tier3"])

    adjusted = base_params.copy()

    if "min_swing_atr_mult" in adjusted:
        adjusted["min_swing_atr_mult"] *= adjustments["atr_mult_factor"]

    if "wick_body_ratio_threshold" in adjusted:
        adjusted["wick_body_ratio_threshold"] *= adjustments["wick_threshold_factor"]

    if "range_break_confirm_bars" in adjusted:
        adjusted["range_break_confirm_bars"] += adjustments["confirm_bars_adjust"]

    return adjusted


# ==========================================================
# 波动率阈值配置
# ==========================================================
VOLATILITY_THRESHOLDS = {
    "extreme_ratio": 0.03,
    "high_ratio": 0.02,
    "low_ratio": 0.005,
    "extreme_mult": 2.0,
    "high_mult": 1.5,
    "low_mult": 0.6,
}


# ============================================================
# 多用户专用 - 保留必要的系统配置
# ============================================================

# 交易所环境配置
BINANCE_ENVIRONMENT = False  # False=正式网, True=测试网

# ==========================================================
# Telegram 通知配置
# ==========================================================
# 系统级 Bot Token（所有用户共用，用户只需配置自己的 Chat ID）
# 必须通过环境变量配置: export TELEGRAM_BOT_TOKEN="your_bot_token"
# P0 Fix: 移除硬编码的 Bot Token，必须从环境变量读取
SYSTEM_TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
if not SYSTEM_TELEGRAM_BOT_TOKEN:
    import logging
    logging.getLogger(__name__).warning(
        "TELEGRAM_BOT_TOKEN 未设置，Telegram 通知功能将不可用。"
        "请设置环境变量: export TELEGRAM_BOT_TOKEN='your_bot_token'"
    )

# 以下为旧版兼容配置（管理员/系统通知使用）
TELEGRAM_BOT_TOKEN = SYSTEM_TELEGRAM_BOT_TOKEN  # 兼容旧代码
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
TELEGRAM_ENABLED = bool(SYSTEM_TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID)

# Telegram 代理配置（国内服务器访问 Telegram API 需要代理）
# 支持环境变量配置: export TELEGRAM_PROXY="http://127.0.0.1:7890"
# 留空表示不使用代理
TELEGRAM_PROXY = os.getenv("TELEGRAM_PROXY", "")

# ==========================================================
# Sentiment Analysis Configuration
# ==========================================================

# Funding rate thresholds (decimal values)
FUNDING_RATE_THRESHOLDS = {
    "extreme_long": 0.001,    # 0.1% extreme bullish
    "high_long": 0.0005,      # 0.05% clearly bullish
    "neutral_high": 0.0001,   # 0.01% slightly bullish
    "neutral_low": -0.0001,   # -0.01% slightly bearish
    "high_short": -0.0005,    # -0.05% clearly bearish
    "extreme_short": -0.001,  # -0.1% extreme bearish
}

# OI change thresholds (percentage)
OI_CHANGE_THRESHOLDS = {
    "surge": 10.0,      # 10%+ is surge
    "increase": 5.0,    # 5%+ is increase
    "decrease": -5.0,   # -5% or below is decrease
    "plunge": -10.0,    # -10% or below is plunge
}

# Market regime thresholds (24h price change %)
MARKET_REGIME_THRESHOLDS = {
    "strong_up": 3.0,
    "mild_up": 1.0,
    "mild_down": -1.0,
    "strong_down": -3.0,
}

# Market sentiment ratio thresholds
MARKET_SENTIMENT_THRESHOLDS = {
    "strong_bullish": 0.7,
    "mild_bullish": 0.55,
    "mild_bearish": 0.45,
    "strong_bearish": 0.3,
}

# 全局市场指标币种
# 用途：
# 1. 确保 BTC/ETH 数据始终可用（用于 market_regime 计算）
# 2. 其他币种用于 sentiment 分析（funding rate 聚合等）
# 3. 作为系统基础监控币种，即使用户未订阅也会获取数据
# 注意：market_regime 现在只基于 BTC/ETH 判断，不再使用 9 币种投票机制
GLOBAL_MARKET_SYMBOLS = [
    "BTCUSDT",   # 大盘龙头
    "ETHUSDT",   # 智能合约平台
    "SOLUSDT",   # 新兴公链
    "BNBUSDT",   # 交易所币
    "DOGEUSDT",  # Meme 币代表
    "LTCUSDT",   # 支付币
    "ADAUSDT",   # 老牌公链
    "XRPUSDT",   # 传统加密货币
    "LINKUSDT",  # DeFi/预言机龙头
]

# Sentiment API configuration
SENTIMENT_API_URLS = {
    "fear_greed": "https://api.alternative.me/fng/?limit=1",
    "binance_long_short": "https://fapi.binance.com/futures/data/globalLongShortAccountRatio",
}

# Cache TTL (seconds)
SENTIMENT_CACHE_TTL = {
    "fear_greed": 3600,   # 1 hour
    "long_short": 300,    # 5 minutes
}

# API timeout (seconds)
SENTIMENT_API_TIMEOUT = 5

# ==========================================================
# Time Context Configuration
# ==========================================================

# Trading session definitions (UTC hours)
# V5-01 fix: 与 context_builder._SESSIONS 和 decision_feedback._get_trading_session() 对齐
# 旧定义：asia=0-8, europe=8-16, america=13-22（有重叠且不一致）
# 新定义：asia=0-7, europe=7-13, overlap=13-16, us=16-22, late=22-24
TRADING_SESSIONS = {
    "asia": {
        "name": "Asian Session",
        "start_hour": 0,   # UTC 00:00 = Beijing 08:00
        "end_hour": 7,     # UTC 07:00 = Beijing 15:00
        "characteristics": "Relatively calm, lower volatility, often range-bound",
        "volatility": "low",
        "liquidity": "moderate",
    },
    "europe": {
        "name": "European Session",
        "start_hour": 7,   # UTC 07:00 = London 07:00
        "end_hour": 13,    # UTC 13:00 = London 13:00
        "characteristics": "Increasing liquidity, trend initiation common",
        "volatility": "moderate",
        "liquidity": "high",
    },
    "overlap": {
        "name": "EU-US Overlap",
        "start_hour": 13,  # UTC 13:00
        "end_hour": 16,    # UTC 16:00
        "characteristics": "Peak liquidity, highest volatility of the day",
        "volatility": "very_high",
        "liquidity": "highest",
    },
    "us": {
        "name": "US Session",
        "start_hour": 16,  # UTC 16:00 = New York 11:00
        "end_hour": 22,    # UTC 22:00 = New York 17:00
        "characteristics": "High volatility and volume, major moves common",
        "volatility": "high",
        "liquidity": "highest",
    },
    "late": {
        "name": "Late Session",
        "start_hour": 22,  # UTC 22:00
        "end_hour": 24,    # UTC 24:00 (midnight)
        "characteristics": "Low liquidity, avoid large entries",
        "volatility": "low",
        "liquidity": "low",
    },
}

# Binance perpetual funding settlement hours (UTC)
FUNDING_SETTLEMENT_HOURS = [0, 8, 16]

# Decision feedback configuration
DECISION_FEEDBACK_LOOKBACK_HOURS = 48
DECISION_FEEDBACK_MAX_DECISIONS = 10
