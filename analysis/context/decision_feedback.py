# decision_feedback.py
"""
决策反馈闭环模块（多用户支持）

功能：
1. 从 Redis 读取用户的 AI 历史决策
2. 从 Redis 读取平仓记录 (通过 pf_compat 兼容层)
3. 关联决策与结果
4. 生成 recent_decisions 供投喂给 AI
5. 根据市场状态、波动率、时段等动态生成智能建议

使用方式：
    from decision_feedback import build_decision_feedback
    feedback = build_decision_feedback(uid="user123")
"""

import json
import re
import time
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
from core.database import redis_client
from core.redis_manager import RedisDataManager
from core.pf_compatibility import pf_compat

logger = logging.getLogger(__name__)

# ==========================================================
# 配置
# ==========================================================
LOOKBACK_HOURS = 48
MAX_DECISIONS = 10

# 主流币列表（用于分类建议）
MAJOR_COINS = {"BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT"}
# 中等市值币
MID_CAP_COINS = {"ADAUSDT", "DOGEUSDT", "AVAXUSDT", "DOTUSDT", "MATICUSDT", 
                 "LINKUSDT", "LTCUSDT", "ATOMUSDT", "UNIUSDT", "NEARUSDT"}


def _get_utc_today_start_ms() -> float:
    """Return epoch milliseconds of today's UTC 00:00:00."""
    from datetime import timezone
    now = datetime.now(timezone.utc)
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    return today_start.timestamp() * 1000


# ==========================================================
# 24h 变化数据获取
# ==========================================================
def _get_24h_change(symbol: str) -> Optional[float]:
    """
    从缓存或 API 获取 24h 变化百分比
    
    优先从 volume_stats 的缓存获取，如果没有则返回 None
    """
    try:
        # 尝试从 volume_stats 的缓存获取
        from analysis.data.volume_stats import _cache_get
        cached = _cache_get("24hr", symbol, ttl=300)
        if cached and isinstance(cached, dict):
            return _safe_float(cached.get("priceChangePercent"))
        
        # 如果缓存没有，尝试同步获取（仅在必要时）
        # 注意：这里使用同步方式，可能会有性能影响
        import asyncio
        from analysis.data.volume_stats import get_24hr_change_async
        
        # 检查是否在事件循环中
        try:
            loop = asyncio.get_running_loop()
            # 如果在事件循环中，不能使用 run_until_complete
            # 返回 None，让调用者处理
            return None
        except RuntimeError:
            # 不在事件循环中，可以创建新的
            loop = asyncio.new_event_loop()
            try:
                result = loop.run_until_complete(get_24hr_change_async(symbol))
                if result and isinstance(result, dict):
                    return _safe_float(result.get("priceChangePercent"))
            finally:
                loop.close()
        
        return None
    except Exception as e:
        logger.debug(f"Error getting 24h change for {symbol}: {e}")
        return None


# ==========================================================
# 市场状态分析
# ==========================================================
def _get_market_regime(symbols_data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    分析当前市场状态（牛市/熊市/震荡）
    
    复用 market_context._build_market_regime() 的 BTC/ETH 主导逻辑，
    并转换为 decision_feedback 模块所需的格式。
    
    Returns:
        {
            "regime": "strong_bullish" | "mild_bullish" | "mixed" | "mild_bearish" | "strong_bearish",
            "btc_trend": "up" | "down" | "sideways",
            "eth_trend": "up" | "down" | "sideways",
            "confidence": 0.0-1.0,
            "description": "..."
        }
    """
    try:
        # 获取 symbols_data
        if not symbols_data:
            from analysis.assembly.payload_builder import get_ai_ready_payload
            symbols_data = {}
            btc_data = get_ai_ready_payload("BTCUSDT")
            eth_data = get_ai_ready_payload("ETHUSDT")
            if btc_data:
                symbols_data["BTCUSDT"] = btc_data
            if eth_data:
                symbols_data["ETHUSDT"] = eth_data
        
        # 复用 market_context 的 BTC/ETH 主导逻辑
        from analysis.context.market_context import _build_market_regime
        regime_data = _build_market_regime(symbols_data)
        
        # 转换 market_sentiment -> regime
        market_sentiment = regime_data.get("market_sentiment", "mixed")
        
        # 转换 trend 值：up/down/neutral/recovering/weakening/unknown -> up/down/sideways
        def _simplify_trend(trend: str) -> str:
            if trend in ("up", "recovering"):
                return "up"
            elif trend in ("down", "weakening"):
                return "down"
            else:
                return "sideways"
        
        btc_trend = _simplify_trend(regime_data.get("btc_trend", "unknown"))
        eth_trend = _simplify_trend(regime_data.get("eth_trend", "unknown"))
        
        # 转换 confidence: high/moderate/low -> 0.0-1.0
        confidence_map = {"high": 0.85, "moderate": 0.65, "low": 0.45}
        confidence = confidence_map.get(regime_data.get("confidence", "low"), 0.45)
        
        # 生成描述
        description_map = {
            "strong_bullish": "Strong bullish momentum - BTC and ETH trending up",
            "mild_bullish": "Mild bullish bias - some upward momentum",
            "strong_bearish": "Bearish pressure - BTC and ETH trending down",
            "mild_bearish": "Mild bearish bias - some downward pressure",
            "mixed": "Mixed signals - market in consolidation",
        }
        description = description_map.get(market_sentiment, "Unable to determine market regime")
        
        # 提取 BTC 详细数据用于兼容旧代码
        btc_detail = regime_data.get("btc", {})
        btc_change_24h = btc_detail.get("change_24h_pct")
        
        # 从 momentum_state 估算 RSI（兼容旧代码）
        btc_rsi = 50  # 默认中性
        
        return {
            "regime": market_sentiment,
            "btc_trend": btc_trend,
            "eth_trend": eth_trend,
            "confidence": round(confidence, 2),
            "description": description,
            "btc_rsi": btc_rsi,
            "btc_24h_change": btc_change_24h
        }
        
    except Exception as e:
        logger.debug(f"Error analyzing market regime: {e}")
        return {
            "regime": "unknown",
            "btc_trend": "unknown",
            "eth_trend": "unknown",
            "confidence": 0.3,
            "description": "Unable to determine market regime"
        }


def _get_volatility_level(symbols_data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    分析当前波动率水平
    
    Returns:
        {
            "level": "high" | "medium" | "low",
            "btc_atr_pct": float,
            "recommendation": "..."
        }
    """
    try:
        if not symbols_data:
            from analysis.assembly.payload_builder import get_ai_ready_payload
            btc_data = get_ai_ready_payload("BTCUSDT")
        else:
            btc_data = symbols_data.get("BTCUSDT", {})
        
        # V5-29 fix: 使用 None 哨兵替代 == 2.0 比较
        # 旧逻辑：atr_pct 初始化为 2.0，用 == 2.0 判断是否获取到数据
        # 如果 expected_move_pct 恰好为 2.0，会误判为未获取到数据
        atr_pct = None  # 哨兵值
        
        if btc_data:
            # 尝试从 ai_enhancement.volatility_regime 获取
            ai_enh = btc_data.get("ai_enhancement", {})
            vol_regime = ai_enh.get("volatility_regime", {})
            expected_move = vol_regime.get("expected_move", {})
            
            # 从 1h expected_move 获取 ATR 百分比
            move_1h = expected_move.get("1h", {})
            if move_1h:
                _move_val = _safe_float(move_1h.get("expected_move_pct"))
                if _move_val and _move_val > 0:
                    atr_pct = _move_val
            
            # 如果没有，尝试从 overall 获取
            if atr_pct is None:
                overall = vol_regime.get("overall", {})
                regime = overall.get("regime", "normal")
                if regime == "expansion":
                    atr_pct = 4.0
                elif regime == "squeeze":
                    atr_pct = 1.0
        
        # 使用默认值
        if atr_pct is None:
            atr_pct = 2.0
        
        # 判断波动率水平并生成具体建议
        if atr_pct > 4.0:
            level = "high"
            severity = "WARNING"
            # 高波动：具体的仓位和止损建议
            recommendation = (
                f"[{severity}] BTC ATR {atr_pct:.1f}% (>4%) indicates elevated volatility. "
                f"Reduce position size to 50-70% of normal. "
                f"Set stops at 1.5x ATR (~{atr_pct * 1.5:.1f}%) to avoid noise. "
                f"Avoid scaling into positions."
            )
        elif atr_pct > 2.0:
            level = "medium"
            severity = "INFO"
            recommendation = (
                f"[{severity}] BTC ATR {atr_pct:.1f}% - normal market conditions. "
                f"Standard position sizing applies. "
                f"Stops at 1x ATR (~{atr_pct:.1f}%) recommended."
            )
        else:  # atr_pct <= 2.0
            level = "low"
            severity = "CAUTION"
            # 低波动：警惕突破
            recommendation = (
                f"[{severity}] BTC ATR {atr_pct:.1f}% (<=2%) indicates compression. "
                f"Breakout likely imminent - avoid large positions until direction confirmed. "
                f"Can use wider stops (2x ATR ~{atr_pct * 2:.1f}%) but reduce size."
            )
        
        return {
            "level": level,
            "severity": severity,
            "btc_atr_pct": round(atr_pct, 2),
            "recommendation": recommendation
        }
        
    except Exception as e:
        logger.debug(f"Error analyzing volatility: {e}")
        return {
            "level": "medium",
            "severity": "INFO",
            "btc_atr_pct": 2.0,
            "recommendation": "[INFO] Volatility data unavailable. Use standard risk management: 1-2% position risk, stops at recent swing levels."
        }


def _get_trading_session() -> Dict[str, Any]:
    """
    判断当前交易时段，提供具体的交易建议
    
    Returns:
        {
            "session": "asia" | "europe" | "us" | "overlap" | "late",
            "severity": "INFO" | "CAUTION",
            "characteristics": "...",
            "recommendation": "...",
            "liquidity": "low" | "medium" | "high",
            "volatility_tendency": "range" | "trending" | "volatile"
        }
    """
    from datetime import timezone as _tz
    now = datetime.now(_tz.utc)
    hour = now.hour
    
    # 交易时段定义（UTC）
    # 亚洲: 00:00-07:00 UTC (北京 08:00-15:00)
    # 欧洲: 07:00-13:00 UTC (伦敦 07:00-13:00)
    # EU/US重叠: 13:00-16:00 UTC (最高流动性)
    # 美国: 16:00-22:00 UTC (纽约 11:00-17:00)
    # 深夜: 22:00-00:00 UTC (流动性最低)
    
    if 0 <= hour < 7:
        session = "asia"
        severity = "INFO"
        liquidity = "medium"
        volatility_tendency = "range"
        characteristics = f"Asian session (UTC {hour:02d}:00) - BTC typically ranges within 1-2%"
        recommendation = (
            "[INFO] Lower liquidity, range-bound conditions expected. "
            "Strategy: Mean reversion at support/resistance. "
            "Avoid breakout entries - false breakouts common. "
            "Optimal for limit orders at range extremes."
        )
    elif 7 <= hour < 13:
        session = "europe"
        severity = "INFO"
        liquidity = "high"
        volatility_tendency = "trending"
        characteristics = f"European session (UTC {hour:02d}:00) - London open drives initial direction"
        recommendation = (
            "[INFO] Increasing volume, trend development phase. "
            "Strategy: Follow the first 2-hour direction after 07:00 UTC. "
            "Good for trend entries with momentum confirmation. "
            "Watch for reversals at 10:00-11:00 UTC if initial move overextended."
        )
    elif 13 <= hour < 16:
        session = "overlap"
        severity = "CAUTION"
        liquidity = "high"
        volatility_tendency = "volatile"
        characteristics = f"EU/US overlap (UTC {hour:02d}:00) - peak liquidity, highest volatility"
        recommendation = (
            "[CAUTION] Maximum volatility window - large moves possible. "
            "Strategy: Breakout trades with tight stops, or wait for pullbacks. "
            "Reduce position size by 30% due to whipsaw risk. "
            "Best time for entries if clear direction established."
        )
    elif 16 <= hour < 22:
        session = "us"
        severity = "INFO"
        liquidity = "high"
        volatility_tendency = "trending"
        characteristics = f"US session (UTC {hour:02d}:00) - news-driven, continuation or reversal"
        recommendation = (
            "[INFO] US market dominance - watch for macro news impact. "
            "Strategy: Trade with the established trend, or fade extreme moves. "
            "Key times: 17:30 UTC (US data), 19:00-20:00 UTC (equity close spillover). "
            "Avoid new positions 30min before major announcements."
        )
    else:  # 22-24
        session = "late"
        severity = "CAUTION"
        liquidity = "low"
        volatility_tendency = "range"
        characteristics = f"Late US/Pre-Asia (UTC {hour:02d}:00) - thin liquidity, wide spreads"
        recommendation = (
            "[CAUTION] Lowest liquidity period - avoid large positions. "
            "Strategy: Close or reduce positions, avoid new entries. "
            "Slippage risk elevated. "
            "Prepare watchlist for Asia open (00:00 UTC)."
        )
    
    return {
        "session": session,
        "severity": severity,
        "utc_hour": hour,
        "liquidity": liquidity,
        "volatility_tendency": volatility_tendency,
        "characteristics": characteristics,
        "recommendation": recommendation
    }


def _analyze_coin_performance(closed_positions: List[dict]) -> Dict[str, Any]:
    """
    分析不同类型币种的表现
    
    Returns:
        {
            "major_coins": {"win_rate": x, "avg_pnl": y, "count": z},
            "mid_cap": {...},
            "small_cap": {...},
            "recommendation": "..."
        }
    """
    major_stats = {"wins": 0, "total": 0, "pnl": 0}
    mid_stats = {"wins": 0, "total": 0, "pnl": 0}
    small_stats = {"wins": 0, "total": 0, "pnl": 0}
    
    for pos in closed_positions:
        symbol = pos.get("symbol", "")
        pnl = pos.get("net_pnl", 0)
        
        if symbol in MAJOR_COINS:
            stats = major_stats
        elif symbol in MID_CAP_COINS:
            stats = mid_stats
        else:
            stats = small_stats
        
        stats["total"] += 1
        stats["pnl"] += pnl
        if pnl > 0:
            stats["wins"] += 1
    
    def calc_metrics(stats):
        if stats["total"] == 0:
            # 明确返回 count: 0，而不是 null，让 AI 清楚知道是"没有交易"而非"数据异常"
            # P13 Fix: 添加 win_rate 和 avg_pnl 默认值，避免后续访问时 KeyError
            return {"count": 0, "win_rate": 0, "avg_pnl": 0}
        return {
            "win_rate": round(stats["wins"] / stats["total"] * 100),
            "avg_pnl": round(stats["pnl"] / stats["total"], 2),
            "count": stats["total"]
        }
    
    major = calc_metrics(major_stats)
    mid = calc_metrics(mid_stats)
    small = calc_metrics(small_stats)
    
    # 生成建议
    recommendations = []
    
    # P13 Fix: 检查 count > 0 而不是 truthy，避免空数据时访问 win_rate
    if major["count"] > 0 and mid["count"] > 0:
        if major["win_rate"] > mid["win_rate"] + 10:
            recommendations.append("Major coins outperforming - focus on BTC/ETH/SOL")
        elif mid["win_rate"] > major["win_rate"] + 10:
            recommendations.append("Mid-caps showing strength - consider diversifying")
    
    if small["count"] > 0 and small["win_rate"] < 40:
        recommendations.append("Small caps underperforming - reduce altcoin exposure")
    elif small["count"] > 0 and small["win_rate"] > 55:
        recommendations.append("Small caps performing well - altcoin season signals")
    
    return {
        "major_coins": major,
        "mid_cap": mid,
        "small_cap": small,
        "recommendation": "; ".join(recommendations) if recommendations else "Balanced performance across coin types"
    }


def _analyze_streak(closed_positions: List[dict]) -> Dict[str, Any]:
    """
    分析连续盈亏情况，返回客观数据供 AI 参考
    
    注意：不提供具体的仓位调整建议，避免 AI 学习到"连胜后减仓"等行为模式。
    AI 应根据 prompt_templates.py 中的 Sizing Adjustments 规则自行决定。
    
    Returns:
        {
            "current_streak": int (正数=连胜, 负数=连败),
            "streak_type": "winning" | "losing" | "neutral",
            "streak_pnl": float (连续期间的总盈亏),
            "note": str (客观描述，不含行为指令)
        }
    """
    if not closed_positions:
        return {
            "current_streak": 0,
            "streak_type": "neutral",
            "streak_pnl": 0,
            "note": "No recent trades to analyze"
        }
    
    # 按时间排序（最新的在前）
    sorted_positions = sorted(closed_positions, key=lambda x: x.get("close_time_ms", 0), reverse=True)
    
    # 计算当前连续状态
    streak = 0
    first_result = None
    streak_pnl = 0  # 连续期间的总盈亏
    
    for pos in sorted_positions[:10]:  # 只看最近10笔
        is_win = pos.get("net_pnl", 0) > 0
        
        if first_result is None:
            first_result = is_win
            streak = 1 if is_win else -1
            streak_pnl = pos.get("net_pnl", 0)
        elif is_win == first_result:
            streak += 1 if is_win else -1
            streak_pnl += pos.get("net_pnl", 0)
        else:
            break
    
    # 根据连续状态生成客观描述（不含行为指令）
    if streak >= 2:
        streak_type = "winning"
        note = f"{streak} consecutive wins totaling +${streak_pnl:.2f}"
    elif streak <= -2:
        streak_type = "losing"
        note = f"{abs(streak)} consecutive losses totaling -${abs(streak_pnl):.2f}"
    elif streak == 1:
        streak_type = "neutral"
        note = f"Last trade was profitable (+${streak_pnl:.2f})"
    elif streak == -1:
        streak_type = "neutral"
        note = f"Last trade was a loss (-${abs(streak_pnl):.2f})"
    else:
        streak_type = "neutral"
        note = "Mixed results"
    
    return {
        "current_streak": streak,
        "streak_type": streak_type,
        "streak_pnl": round(streak_pnl, 2),
        "note": note
    }


def _safe_float(value: Any) -> Optional[float]:
    """安全转换为浮点数"""
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


# ==========================================================
# 1. 读取 AI 决策历史（多用户支持）
# ==========================================================
def get_decision_history(
    uid: str, 
    lookback_hours: int = LOOKBACK_HOURS,
    exchange: Optional[str] = None
) -> List[dict]:
    """
    从 MySQL 读取用户的 AI 决策历史
    
    Args:
        uid: 用户 ID
        lookback_hours: 回看小时数
        exchange: 交易所名称（可选），如果指定则只返回该交易所的决策

    Returns:
        List of decisions: [{timestamp, symbol, action, reason, exchange}, ...]
    """
    try:
        from core.ai_decision_db import get_ai_decision_db
        db = get_ai_decision_db()
        ai_history = db.get_recent_decisions(uid, limit=200, lookback_hours=lookback_hours)
        
        if not ai_history:
            return []

        cutoff_ts = time.time() - (lookback_hours * 3600)
        decisions = []

        for record in ai_history:
            # record 格式: {"id": ..., "request": {...}, "response": {...}}
            ai_decision_id = record.get("id")  # AI 决策记录 ID
            response = record.get("response", {})
            
            # 尝试从多种格式中提取时间戳
            ts = response.get("timestamp") or record.get("timestamp") or 0
            if isinstance(ts, str):
                try:
                    ts = float(ts)
                except ValueError:
                    continue

            # 过滤超出回看范围的记录
            if ts < cutoff_ts:
                continue

            # 从 response 中提取信号
            # 支持两种格式：
            # 1. groups 格式: {"groups": [{"response": {"signals": [...]}, "target_exchanges": [...]}]}
            # 2. 直接 signals 格式: {"signals": [...], "target_exchanges": [...]}
            
            signals_with_exchange = []
            
            # 格式 1: groups
            for group in response.get("groups", []):
                group_signals = group.get("response", {}).get("signals", [])
                # 获取该组的目标交易所
                group_exchanges = group.get("target_exchanges", [])
                for sig in group_signals:
                    if isinstance(sig, dict):
                        signals_with_exchange.append({
                            "signal": sig,
                            "exchanges": group_exchanges
                        })
            
            # 格式 2: 直接 signals
            if not signals_with_exchange:
                direct_signals = response.get("signals", [])
                direct_exchanges = response.get("target_exchanges", [])
                for sig in direct_signals:
                    if isinstance(sig, dict):
                        signals_with_exchange.append({
                            "signal": sig,
                            "exchanges": direct_exchanges
                        })
            
            for item in signals_with_exchange:
                signal = item["signal"]
                signal_exchanges = item["exchanges"]
                
                # 如果指定了交易所，只保留该交易所的决策
                if exchange and exchange not in signal_exchanges:
                    continue
                
                decisions.append({
                    "ai_decision_id": ai_decision_id,  # AI 决策记录 ID
                    "timestamp": ts,
                    "symbol": signal.get("symbol"),
                    "action": signal.get("action"),
                    "reason": signal.get("reason", ""),
                    "exchange": signal_exchanges[0] if signal_exchanges else None,
                    "params": {
                        "entry": signal.get("entry"),
                        "stop_loss": signal.get("stop_loss"),
                        "take_profit": signal.get("take_profit"),
                        "position_size": signal.get("position_size"),
                    } if signal.get("entry") else None
                })

        # 按时间倒序排列
        decisions.sort(key=lambda x: x["timestamp"], reverse=True)
        return decisions

    except Exception as e:
        logger.error(f"[{uid}] Error reading decision history: {e}")
        return []


# ==========================================================
# 2. 读取平仓记录
# ==========================================================
def get_closed_positions(
    uid: str, 
    lookback_hours: int = LOOKBACK_HOURS,
    exchange: Optional[str] = None
) -> List[dict]:
    """
    从 MySQL 读取平仓记录

    Args:
        uid: 用户 ID
        lookback_hours: 回看小时数
        exchange: 交易所名称（可选），如果指定则只返回该交易所的平仓记录

    Returns:
        List of closed positions
    """
    try:
        from core.closed_trades_db import get_closed_trades_db
        db = get_closed_trades_db()
        
        # 计算时间范围
        cutoff_ts_ms = int((time.time() - lookback_hours * 3600) * 1000)
        
        # 从 MySQL 获取平仓记录
        records, total = db.get_trades_paginated(
            uid=uid,
            exchange=exchange,
            offset=0,
            limit=200,  # 最多获取 200 条
            start_time_ms=cutoff_ts_ms,
            order_by="close_time_ms",
            order_dir="DESC"
        )
        
        if not records:
            return []
        
        positions = []
        for record in records:
            try:
                # 转换字段名（MySQL 返回的是 camelCase）
                # 注意: record.get("field", 0) 在 field=None 时返回 None，需要用 or 0
                positions.append({
                    "cycle_id": record.get("cycleId"),
                    "symbol": record.get("symbol"),
                    "side": record.get("side"),
                    "exchange": record.get("exchange"),
                    "open_time_ms": float(record.get("openTimeMs") or 0),
                    "close_time_ms": float(record.get("closeTimeMs") or 0),
                    "avg_open_price": float(record.get("avgOpenPrice") or record.get("entryPrice") or 0),
                    "avg_close_price": float(record.get("avgClosePrice") or record.get("exitPrice") or 0),
                    "open_qty": float(record.get("openQty") or record.get("quantity") or 0),
                    "net_pnl": float(record.get("realizedPnl") or 0),
                    "realized_pnl": float(record.get("realizedPnl") or 0),
                    "pnl_pct": float(record.get("pnlPct") or 0),
                    "peak_pnl": float(record.get("peakPnl") or 0),
                    "max_drawdown": float(record.get("maxDrawdown") or 0),
                    "duration_ms": float(record.get("durationMinutes") or 0) * 60000,
                    "fee_total": float(record.get("feeTotal") or 0),
                    "ai_decision_id": record.get("aiDecisionId"),  # AI 决策 ID
                })
            except (TypeError, ValueError) as e:
                logger.debug(f"Error parsing closed position: {e}")
                continue
        
        return positions

    except Exception as e:
        logger.error(f"[{uid}] Error reading closed positions from MySQL: {e}")
        return []


# ==========================================================
# 3. 获取当前持仓（用于评估 hold 决策）
# ==========================================================
def get_current_positions(uid: str, exchange: Optional[str] = None) -> Dict[str, dict]:
    """
    获取当前持仓 (使用 pf_compat 兼容层)

    Args:
        uid: 用户 ID
        exchange: 交易所名称（可选），如果指定则只返回该交易所的持仓

    Returns:
        {"symbol:side": position_info}
    """
    try:
        # 使用 pf_compat 兼容层获取持仓数据（支持按交易所过滤）
        pos_data = pf_compat.get_pf_pos(uid, exchange=exchange, add_exchange_field=True)
        if not pos_data:
            return {}

        # pos_data 格式: {"BTCUSDT:LONG": {...}, "ETHUSDT:SHORT": {...}}
        # 返回相同格式，或者按 symbol 返回
        result = {}
        for key, pos in pos_data.items():
            if isinstance(pos, dict):
                symbol = pos.get("symbol", key.split(":")[0] if ":" in key else key)
                result[key] = pos
        
        return result

    except Exception as e:
        logger.error(f"[{uid}] Error reading current positions: {e}")
        return {}



# L8 fix: match_decision_with_outcome() 已删除（死代码，全项目无调用方）
# 决策-结果关联现在在 build_decision_feedback() 中通过 ai_decision_id 直接完成


def _infer_exit_type(position: dict) -> str:
    """
    推断平仓类型 - 基于阶梯式跟踪止损逻辑

    跟踪止损等级：
    - ≥2.2% → 锁1%
    - ≥3.5% → 锁2%
    - ≥5.5% → 锁4%
    - ≥7.5% → 锁5.5%
    - ≥10%  → 锁8%

    字段：
    - peakPnl: 持仓期间最大浮盈（绝对值 USDT）
    - netPnl: 平仓后净收益（绝对值 USDT）
    - avgOpenPrice: 开仓均价
    - openQty: 开仓数量
    """
    peak = float(position.get("peak_pnl") or 0)
    net = float(position.get("net_pnl") or 0)

    # 计算仓位价值
    open_price = float(position.get("avg_open_price") or 0)
    qty = float(position.get("open_qty") or 0)
    position_value = open_price * qty if open_price and qty else 0

    # 计算百分比
    if position_value > 0:
        peak_pct = peak / position_value * 100
        net_pct = net / position_value * 100
    else:
        # V5-28 fix: position_value == 0 时无法计算百分比
        # 旧逻辑：用绝对值近似百分比（$15 peak 当作 15%），对非小仓位完全错误
        # 新逻辑：返回保守默认值，避免错误推断
        if net > 0:
            return "take_profit"  # 盈利平仓，具体类型未知
        else:
            return "stop_loss"  # 亏损平仓，具体类型未知

    # 跟踪止损触发阈值
    TRAILING_TRIGGER_PCT = 2.2  # 第一级触发阈值

    if net > 0:  # 盈利平仓
        if peak_pct >= TRAILING_TRIGGER_PCT:
            # 曾触发跟踪止损等级
            if net_pct < peak_pct * 0.8:
                return "trailing_stop"  # 跟踪止损锁盈生效
            else:
                return "take_profit"  # 可能是止盈目标触发
        else:
            return "take_profit"  # 未触发跟踪，直接止盈
    else:  # 亏损平仓
        if peak_pct >= TRAILING_TRIGGER_PCT:
            # 曾触发跟踪止损等级，但最终亏损
            # 这种情况比较少见，可能是极端行情跳空
            return "trailing_stop"  # 跟踪止损触发但仍亏损
        else:
            return "stop_loss"  # 从未触发跟踪止损，直接止损


def _generate_system_reason(pos: dict, exit_type: str, pnl_pct: float) -> str:
    """
    当没有 AI 决策记录时，根据 exit_type 生成系统级 reason
    
    用于止盈/止损/跟踪止损自动触发的场景，此时没有对应的 AI 决策记录
    针对 LONG/SHORT 提供差异化的分析
    
    Args:
        pos: 平仓记录，包含 symbol, side, exchange, avg_open_price, avg_close_price,
             open_qty, net_pnl, peak_pnl, duration_ms 等字段
        exit_type: 平仓类型 (take_profit, stop_loss, trailing_stop)
        pnl_pct: 盈亏百分比
    
    Returns:
        系统生成的 reason 字符串
    """
    symbol = pos.get("symbol", "Unknown")
    side = pos.get("side", "").upper()
    duration_ms = pos.get("duration_ms", 0)
    peak_pnl = pos.get("peak_pnl", 0)
    net_pnl = pos.get("net_pnl", 0)
    
    is_long = side == "LONG"
    is_short = side == "SHORT"
    
    # 计算持仓时长，处理 0 或缺失的情况
    if duration_ms and duration_ms > 0:
        duration_hours = duration_ms / 3600000
        if duration_hours >= 24:
            duration_str = f"after {duration_hours / 24:.1f} days"
        elif duration_hours >= 1:
            duration_str = f"after {duration_hours:.1f} hours"
        else:
            duration_str = f"after {duration_ms / 60000:.0f} minutes"
    else:
        duration_str = ""  # 无持仓时长数据时不显示
    
    # 计算峰值盈亏百分比
    position_value = (pos.get("avg_open_price") or 0) * (pos.get("open_qty") or 0)
    peak_pct = (peak_pnl / position_value * 100) if position_value > 0 else 0
    
    # 根据 exit_type 生成不同的 reason
    # 使用 filter(None, [...]) 来避免空字符串导致的多余空格
    if exit_type == "take_profit":
        if pnl_pct >= 5:
            if is_long:
                parts = ["[Auto] Take profit triggered on", f"{symbol} {side}.", "Strong rally captured with", f"+{pnl_pct:.1f}% profit", duration_str + "." if duration_str else "."]
            elif is_short:
                parts = ["[Auto] Take profit triggered on", f"{symbol} {side}.", "Strong selloff captured with", f"+{pnl_pct:.1f}% profit", duration_str + "." if duration_str else "."]
            else:
                parts = ["[Auto] Take profit triggered on", f"{symbol} {side}.", "Strong move captured with", f"+{pnl_pct:.1f}% profit", duration_str + "." if duration_str else "."]
            return " ".join(filter(None, parts)).replace(" .", ".").replace("  ", " ")
        else:
            parts = ["[Auto] Take profit triggered on", f"{symbol} {side}.", "Position closed with", f"+{pnl_pct:.1f}% profit", duration_str + "." if duration_str else "."]
            return " ".join(filter(None, parts)).replace(" .", ".").replace("  ", " ")
    
    elif exit_type == "stop_loss":
        # pnl_pct 是负数，使用 abs() 避免显示 "-2.1% loss" 这种重复表达
        if is_long:
            parts = ["[Auto] Stop loss triggered on", f"{symbol} {side}.", "Closed with", f"-{abs(pnl_pct):.1f}% loss", duration_str, "as price dropped below support."]
        elif is_short:
            parts = ["[Auto] Stop loss triggered on", f"{symbol} {side}.", "Closed with", f"-{abs(pnl_pct):.1f}% loss", duration_str, "as price squeezed above resistance."]
        else:
            parts = ["[Auto] Stop loss triggered on", f"{symbol} {side}.", "Position closed with", f"-{abs(pnl_pct):.1f}% loss", duration_str, "to prevent further drawdown."]
        return " ".join(filter(None, parts)).replace("  ", " ")
    
    elif exit_type == "trailing_stop":
        if net_pnl > 0:
            # 跟踪止损锁盈
            if is_long:
                parts = ["[Auto] Trailing stop triggered on", f"{symbol} {side}.", "Locked in", f"+{pnl_pct:.1f}% profit", duration_str, f"as rally pulled back (peak was +{peak_pct:.1f}%)."]
            elif is_short:
                parts = ["[Auto] Trailing stop triggered on", f"{symbol} {side}.", "Locked in", f"+{pnl_pct:.1f}% profit", duration_str, f"as selloff bounced (peak was +{peak_pct:.1f}%)."]
            else:
                parts = ["[Auto] Trailing stop triggered on", f"{symbol} {side}.", "Locked in", f"+{pnl_pct:.1f}% profit", duration_str, f"(peak was +{peak_pct:.1f}%)."]
            return " ".join(filter(None, parts)).replace("  ", " ")
        else:
            # 跟踪止损但最终亏损（极端行情）
            if is_long:
                parts = ["[Auto] Trailing stop triggered on", f"{symbol} {side}", "during volatile reversal.", "Closed with", f"-{abs(pnl_pct):.1f}% loss", duration_str, f"(peak was +{peak_pct:.1f}%)."]
            elif is_short:
                parts = ["[Auto] Trailing stop triggered on", f"{symbol} {side}", "during short squeeze.", "Closed with", f"-{abs(pnl_pct):.1f}% loss", duration_str, f"(peak was +{peak_pct:.1f}%)."]
            else:
                parts = ["[Auto] Trailing stop triggered on", f"{symbol} {side}", "during volatile move.", "Closed with", f"-{abs(pnl_pct):.1f}% loss", duration_str, f"(peak was +{peak_pct:.1f}%)."]
            return " ".join(filter(None, parts)).replace("  ", " ")
    
    else:
        # 未知类型，生成通用 reason
        if net_pnl > 0:
            parts = ["[Auto] Position", f"{symbol} {side}", "closed automatically with", f"+{pnl_pct:.1f}% profit", duration_str + "." if duration_str else "."]
            return " ".join(filter(None, parts)).replace(" .", ".").replace("  ", " ")
        else:
            parts = ["[Auto] Position", f"{symbol} {side}", "closed automatically with", f"-{abs(pnl_pct):.1f}% loss", duration_str + "." if duration_str else "."]
            return " ".join(filter(None, parts)).replace(" .", ".").replace("  ", " ")


# ==========================================================
# 5. 构建反馈数据
# ==========================================================
def build_decision_feedback(
        uid: str,
        lookback_hours: int = LOOKBACK_HOURS,
        max_decisions: int = MAX_DECISIONS,
        current_prices: Optional[Dict[str, float]] = None,
        exchange: Optional[str] = None
) -> dict:
    """
    构建决策反馈数据，供投喂给 AI
    
    核心逻辑（修正版）：
    1. 从 closed_trades 获取最近 48 小时的平仓记录（最多 10 条）
    2. 通过每条平仓记录的 ai_decision_id 找到对应的决策信息（包含 reason）
    3. 如果 ai_decision_id 为空，说明是 bug（平仓时没有写入决策信息）

    Args:
        uid: 用户 ID
        lookback_hours: 回看小时数
        max_decisions: 最大决策数
        current_prices: 当前价格字典
        exchange: 交易所名称（可选），如果指定则只返回该交易所的反馈

    Returns:
        {
            "recent_decisions": [...],
            "summary": {...}
        }
    """
    # 1. 从 closed_trades 获取最近的平仓记录（这是起点）
    closed_positions = get_closed_positions(uid, lookback_hours, exchange=exchange)
    
    if not closed_positions:
        return {
            "recent_decisions": [],
            "summary": {"total": 0},
            "insights": {"available": False, "reason": "No closed trades"},
            "market_environment": {},
            "lookback_hours": lookback_hours,
            "total_decisions_analyzed": 0,
            "total_closed_positions": 0,
            "today_realized_pnl": 0,
            "today_trades": 0,
            "today_wins": 0,
        }
    
    # 1b. 计算今日已实现 PnL（必须在截断前用全量数据计算）
    today_start_ms = _get_utc_today_start_ms()
    today_pnl = 0.0
    today_trades = 0
    today_wins = 0
    for pos in closed_positions:
        close_ms = pos.get("close_time_ms", 0)
        if close_ms >= today_start_ms:
            today_pnl += pos.get("net_pnl", 0)
            today_trades += 1
            if pos.get("net_pnl", 0) > 0:
                today_wins += 1

    # 限制数量（仅影响 recent_decisions 详情展示，不影响统计）
    # 统计类函数（insights/summary/recommendations）使用全量 closed_positions
    display_positions = closed_positions[:max_decisions]
    
    # 2. 收集所有 ai_decision_id，批量查询决策信息
    decision_ids = []
    for pos in display_positions:
        aid = pos.get("ai_decision_id")
        if aid:
            try:
                decision_ids.append(int(aid))
            except (ValueError, TypeError) as e:
                logger.debug(f"[{uid}] 解析 ai_decision_id 失败: {aid}, error: {e}")
    
    # 3. 批量获取决策信息（传入 uid 确保用户隔离）
    decisions_map = {}
    if decision_ids:
        try:
            from core.ai_decision_db import get_ai_decision_db
            db = get_ai_decision_db()
            decisions_map = db.get_decisions_by_ids(decision_ids, uid=uid)
        except Exception as e:
            logger.error(f"[{uid}] Error fetching decisions by ids: {e}")
    
    # 4. 构建关联后的数据
    matched = []
    missing_decision_count = 0
    
    for pos in display_positions:
        ai_decision_id = pos.get("ai_decision_id")
        
        # 从决策记录中提取 reason
        reason = ""
        action = ""
        decision_timestamp = None
        
        signal_stop_loss = None
        signal_take_profit = None
        
        if ai_decision_id:
            try:
                decision_id = int(ai_decision_id)
                decision_record = decisions_map.get(decision_id)
                if decision_record:
                    # 从 response 中提取 signal 信息
                    response = decision_record.get("response", {})
                    decision_timestamp = decision_record.get("timestamp")
                    
                    # 查找匹配的 signal（通过 symbol + side）
                    # E10 fix: 传入 side 避免同 symbol 多 signal 时匹配错误
                    signal = _find_signal_in_response(response, pos["symbol"], side=pos.get("side"))
                    if signal:
                        reason = signal.get("reason", "")
                        action = signal.get("action", "")
                        signal_stop_loss = signal.get("stop_loss")
                        signal_take_profit = signal.get("take_profit")
            except (ValueError, TypeError) as e:
                logger.debug(f"[{uid}] 解析 ai_decision_id 失败: {ai_decision_id}, error: {e}")
        
        # 计算盈亏百分比
        # V5-18 fix: 使用 .get() 防止 malformed record 导致 KeyError 崩溃
        _avg_price = pos.get("avg_open_price", 0) or 0
        _open_qty = pos.get("open_qty", 0) or 0
        position_value = _avg_price * _open_qty
        pnl_pct = (pos.get("net_pnl", 0) / position_value) * 100 if position_value > 0 else 0
        
        # 推断平仓类型
        exit_type = _infer_exit_type(pos)
        
        # 如果没有 AI 决策记录，根据 exit_type 生成系统级 reason
        if not reason:
            missing_decision_count += 1
            reason = _generate_system_reason(pos, exit_type, pnl_pct)
            action = f"auto_{exit_type}"  # 标记为自动触发
        
        matched.append({
            "symbol": pos.get("symbol", "UNKNOWN"),
            "side": pos.get("side"),
            "exchange": pos.get("exchange"),
            "ai_decision_id": ai_decision_id,
            "action": action or f"open_{pos.get('side', 'unknown').lower()}_market",
            "reason": reason,
            "timestamp": decision_timestamp or ((pos.get("open_time_ms") or 0) / 1000),
            "entry_price": _avg_price if _avg_price > 0 else None,
            "stop_loss": signal_stop_loss,
            "outcome": {
                "status": "closed",
                "result": "win" if (pos.get("net_pnl") or 0) > 0 else "loss",
                "net_pnl": round(pos.get("net_pnl") or 0, 2),
                "pnl_pct": round(pnl_pct, 2),
                "duration_min": round((pos.get("duration_ms") or 0) / 60000, 1),
                "peak_pnl": round(pos.get("peak_pnl") or 0, 2),
                "exit_type": exit_type,
                "cycle_id": pos.get("cycle_id"),
            }
        })
    
    # 5. 格式化输出
    recent_decisions = []
    for d in matched:
        time_ago = _format_time_ago(d["timestamp"])

        entry = {
            "symbol": d["symbol"],
            "side": d.get("side", "").upper(),  # 添加 side 字段
            "time_ago": time_ago,
            "action": d["action"],
            "reason_summary": _summarize_reason(d["reason"]),
            "reason_tags": extract_decision_tags(d.get("reason", ""), d.get("action", "")),
            "outcome": d["outcome"]
        }

        # 计算实际 R:R achieved = pnl_pct / risk_pct
        entry_price = d.get("entry_price")
        stop_loss = d.get("stop_loss")
        pnl_pct = d["outcome"].get("pnl_pct", 0)
        if entry_price and stop_loss and entry_price > 0:
            risk_pct = abs(entry_price - stop_loss) / entry_price * 100
            if risk_pct > 0:
                entry["rr_achieved"] = round(pnl_pct / risk_pct, 2)

        # 添加经验总结
        entry["lesson"] = _generate_lesson(d)

        recent_decisions.append(entry)

    # 6. 生成统计摘要
    # E3 fix: _generate_summary 之前传入 matched（仅 display_positions 的子集，最多 max_decisions 条）
    # 导致 summary 中的 wins/losses/win_rate/total_pnl 基于截断列表，与全量统计冲突。
    # 修复：用全量 closed_positions 直接计算统计，覆盖 _generate_summary 的截断结果。
    summary = _generate_summary(matched)
    all_wins = sum(1 for p in closed_positions if p.get("net_pnl", 0) > 0)
    all_losses = len(closed_positions) - all_wins
    all_pnl = sum(p.get("net_pnl", 0) for p in closed_positions)
    all_wr = round(all_wins / len(closed_positions) * 100) if closed_positions else None
    # E3 fix: 用全量统计覆盖截断统计，消除 LLM 看到的数据冲突
    summary["closed_trades"] = all_wins + all_losses
    summary["wins"] = all_wins
    summary["losses"] = all_losses
    summary["win_rate"] = all_wr
    summary["total_pnl"] = round(all_pnl, 2)
    # 保留 _all 后缀字段以保持向后兼容
    summary["total_closed_all"] = len(closed_positions)
    summary["wins_all"] = all_wins
    summary["losses_all"] = all_losses
    summary["win_rate_all"] = all_wr
    summary["total_pnl_all"] = round(all_pnl, 2)

    # 7. 生成智能洞察
    insights = _generate_insights_from_positions(closed_positions, matched)
    
    # 8. 生成市场环境数据（基于市场状态、波动率、时段等）
    market_environment = _generate_market_environment(closed_positions)
    
    # 9. 添加诊断信息（降级为 DEBUG，因为自动触发平仓是正常业务场景）
    if missing_decision_count > 0:
        # V5-27 fix: 分母使用 len(matched) 而非 len(closed_positions)
        # missing_decision_count 只在 display_positions 循环中累加，分母应匹配
        logger.debug(f"[{uid}] {missing_decision_count}/{len(matched)} displayed trades missing ai_decision_id (auto-triggered or legacy)")

    return {
        "recent_decisions": recent_decisions,
        "summary": summary,
        "insights": insights,
        "market_environment": market_environment,
        "lookback_hours": lookback_hours,
        "total_decisions_analyzed": len(matched),
        "total_closed_positions": len(closed_positions),
        "missing_decision_count": missing_decision_count,  # 诊断信息
        "today_realized_pnl": round(today_pnl, 2),
        "today_trades": today_trades,
        "today_wins": today_wins,
    }


def _find_signal_in_response(response: dict, symbol: str, side: Optional[str] = None) -> Optional[dict]:
    """
    从 AI 响应中查找匹配的 signal
    
    E10 fix: 支持可选的 side 参数，当同一 symbol 有多个 signal 时（如先平后开），
    仅匹配 symbol 会返回错误的 signal。传入 side 后同时匹配 action 方向。
    
    支持两种格式：
    1. groups 格式: {"groups": [{"response": {"signals": [...]}}]}
    2. 直接 signals 格式: {"signals": [...]}
    """
    # E10: 根据 side 判断 action 应包含的方向关键词
    # side="LONG" → action 应包含 "long"（如 open_long_market）
    # side="SHORT" → action 应包含 "short"（如 open_short_market）
    _side_kw = side.lower() if side else None

    def _matches(sig: dict) -> bool:
        if not isinstance(sig, dict):
            return False
        if sig.get("symbol") != symbol:
            return False
        if _side_kw:
            action = (sig.get("action") or "").lower()
            if _side_kw not in action:
                return False
        return True

    # 格式 1: groups
    for group in response.get("groups", []):
        for sig in group.get("response", {}).get("signals", []):
            if _matches(sig):
                return sig
    
    # 格式 2: 直接 signals
    for sig in response.get("signals", []):
        if _matches(sig):
            return sig
    
    # E10 fallback: 如果带 side 匹配失败，退回到仅 symbol 匹配
    # 避免因 action 格式不标准导致完全找不到 signal
    if _side_kw:
        return _find_signal_in_response(response, symbol, side=None)
    
    return None


# L9 fix: _deduplicate_decisions() 已删除（死代码，全项目无调用方）
# 去重逻辑现在在 build_decision_feedback() 中通过 ai_decision_id 直接完成


def _format_time_ago(timestamp: float) -> str:
    """格式化时间差"""
    diff_sec = time.time() - timestamp

    if diff_sec < 3600:
        return f"{int(diff_sec / 60)}m ago"
    elif diff_sec < 86400:
        return f"{int(diff_sec / 3600)}h ago"
    else:
        return f"{int(diff_sec / 86400)}d ago"


def _summarize_reason(reason: str, max_len: int = 500) -> str:
    """
    返回完整的 reason
    
    P4 Fix: 不再截断，返回完整的 reason 供 AI 学习
    max_len 参数保留但默认值设为 500，只在极端情况下截断
    """
    if not reason:
        return ""

    # 直接返回完整 reason，只在超长时截断
    if len(reason) <= max_len:
        return reason

    # 超长时智能截断：尽量在单词边界截断
    truncated = reason[:max_len - 3]
    last_space = truncated.rfind(" ")
    if last_space > max_len * 0.6:
        truncated = truncated[:last_space]
    
    return truncated + "..."


def _generate_lesson(decision: dict) -> str:
    """
    根据决策和结果生成客观的交易总结
    
    注意：只返回客观事实描述，不提供建议或行为指令。
    AI 应根据 prompt_templates.py 中的规则自行决定如何应用这些数据。
    
    包含：结果 + 关键指标（盈亏、持仓时长、峰值、效率）
    """
    action = decision["action"]
    outcome = decision["outcome"]
    status = outcome.get("status")
    net_pnl = outcome.get("net_pnl", 0)
    pnl_pct = outcome.get("pnl_pct", 0)
    duration_min = outcome.get("duration_min", 0)
    peak_pnl = outcome.get("peak_pnl", 0)
    exit_type = outcome.get("exit_type", "")
    symbol = decision.get("symbol", "")
    side = decision.get("side", "").upper()  # LONG or SHORT
    
    # 组合 symbol 和 side，例如 "BTCUSDT LONG"
    symbol_side = f"{symbol} {side}" if side else symbol

    if status == "closed":
        # 计算持仓效率（实际盈亏 vs 峰值盈亏）
        efficiency = (net_pnl / peak_pnl * 100) if peak_pnl > 0 else (100 if net_pnl > 0 else 0)
        
        # 处理 duration_min 为 0 或缺失的情况
        duration_str = f"({duration_min:.0f}min)" if duration_min > 0 else ""
        peak_str = f"+${peak_pnl:.0f}" if peak_pnl > 0 else "N/A"
        
        if net_pnl > 0:
            # ========== 盈利交易（纯客观描述）==========
            if exit_type == "trailing_stop":
                return (
                    f"[WIN] {symbol_side} +{pnl_pct:.1f}% via trailing stop {duration_str}. "
                    f"Captured {efficiency:.0f}% of peak ({peak_str})."
                ).replace(" .", ".").replace("  ", " ")
            elif exit_type == "take_profit":
                return (
                    f"[WIN] {symbol_side} +{pnl_pct:.1f}% hit TP target {duration_str}. "
                    f"Peak was {peak_str}."
                ).replace(" .", ".").replace("  ", " ")
            else:
                return (
                    f"[WIN] {symbol_side} +{pnl_pct:.1f}% {duration_str}. "
                    f"Peak was {peak_str} ({efficiency:.0f}% captured)."
                ).replace(" .", ".").replace("  ", " ")
        else:
            # ========== 亏损交易（纯客观描述）==========
            if exit_type == "stop_loss":
                return (
                    f"[LOSS] {symbol_side} -{abs(pnl_pct):.1f}% stopped out {duration_str}. "
                    f"Peak was {peak_str}."
                ).replace(" .", ".").replace("  ", " ")
            elif exit_type == "trailing_stop" and peak_pnl > 0:
                return (
                    f"[LOSS] {symbol_side} -{abs(pnl_pct):.1f}% after trailing stop {duration_str}. "
                    f"Was profitable (peak {peak_str}) but reversed."
                ).replace(" .", ".").replace("  ", " ")
            else:
                return (
                    f"[LOSS] {symbol_side} -{abs(pnl_pct):.1f}% {duration_str}. "
                    f"Peak was {peak_str}."
                ).replace(" .", ".").replace("  ", " ")

    elif status == "ongoing":
        unrealized = outcome.get("unrealized_pnl", 0)
        return f"[OPEN] {symbol_side} position active. Unrealized: ${unrealized:.0f}."

    elif action == "wait":
        return "[WAIT] No entry taken."

    return ""


# ==========================================================
# 决策模式标签提取与分析
# ==========================================================

def extract_decision_tags(reason: str, action: str = "") -> List[str]:
    """
    从 reason 文本中提取决策模式标签
    
    用于分析不同决策模式的胜率，帮助 LLM 学习哪些策略有效
    
    Args:
        reason: LLM 输出的决策理由
        action: 决策动作 (open_long_market, close_short, etc.)
    
    Returns:
        标签列表，如 ["contrarian_fear", "trend_following", "bounce_play"]
    """
    if not reason:
        return []
    
    tags = []
    reason_lower = reason.lower()
    
    # ========== 1. 逆势交易标签 ==========
    if "extreme fear" in reason_lower or "contrarian" in reason_lower:
        tags.append("contrarian_fear")
    if "extreme greed" in reason_lower:
        tags.append("contrarian_greed")
    if "oversold" in reason_lower and ("long" in action or "buy" in reason_lower):
        tags.append("oversold_bounce")
    if "overbought" in reason_lower and ("short" in action or "sell" in reason_lower):
        tags.append("overbought_fade")
    
    # ========== 2. 趋势跟随标签 ==========
    if "trend" in reason_lower:
        if "downtrend" in reason_lower or "down trend" in reason_lower:
            if "short" in action:
                tags.append("trend_following_short")
            elif "long" in action:
                tags.append("counter_trend_long")  # 逆趋势做多
        elif "uptrend" in reason_lower or "up trend" in reason_lower:
            if "long" in action:
                tags.append("trend_following_long")
            elif "short" in action:
                tags.append("counter_trend_short")  # 逆趋势做空
    
    # ========== 3. 技术形态标签 ==========
    if "bounce" in reason_lower:
        tags.append("bounce_play")
    if "breakout" in reason_lower:
        tags.append("breakout_play")
    if "support" in reason_lower:
        if "long" in action:
            tags.append("support_long")
        elif "short" in action:
            tags.append("support_break_short")
    if "resistance" in reason_lower:
        if "short" in action:
            tags.append("resistance_short")
        elif "long" in action:
            tags.append("resistance_break_long")
    
    # ========== 4. 结构信号标签 ==========
    if "choch" in reason_lower or "change of character" in reason_lower:
        tags.append("structure_choch")
    if "bos" in reason_lower or "break of structure" in reason_lower:
        tags.append("structure_bos")
    if "reversal" in reason_lower:
        tags.append("reversal_signal")
    if "divergence" in reason_lower:
        tags.append("divergence_signal")
    
    # ========== 5. 量价分析标签 ==========
    if "volume" in reason_lower:
        if "massive" in reason_lower or "high" in reason_lower or "spike" in reason_lower:
            tags.append("high_volume")
        if "ratio" in reason_lower:
            tags.append("volume_ratio_signal")
    if "order flow" in reason_lower:
        tags.append("order_flow_signal")
    
    # ========== 6. 相对强度标签 ==========
    if "relative strength" in reason_lower or "relative performance" in reason_lower:
        tags.append("relative_strength")
    if "outperform" in reason_lower:
        tags.append("outperformer")
    if "underperform" in reason_lower:
        tags.append("underperformer")
    
    # ========== 7. 偏向强度标签 ==========
    if "strong bullish" in reason_lower or "bullish bias" in reason_lower:
        # 提取偏向分数（适配 -10~+10 评分范围）
        bias_match = re.search(r'\+(\d+)', reason)
        if bias_match:
            score = int(bias_match.group(1))
            if score >= 7:
                tags.append("strong_bullish_bias")
            else:
                tags.append("moderate_bullish_bias")
        else:
            tags.append("bullish_bias")
    
    if "strong bearish" in reason_lower or "bearish bias" in reason_lower:
        bias_match = re.search(r'-(\d+)', reason)
        if bias_match:
            score = int(bias_match.group(1))
            if score >= 7:
                tags.append("strong_bearish_bias")
            else:
                tags.append("moderate_bearish_bias")
        else:
            tags.append("bearish_bias")
    
    # ========== 8. 入场类型标签（从 action 推断）==========
    if "limit" in action:
        tags.append("limit_entry")
    elif "market" in action:
        tags.append("market_entry")
    
    return tags


def analyze_pattern_performance(decisions: List[dict]) -> dict:
    """
    分析各决策模式的胜率表现（纯统计数据，无建议）
    
    Args:
        decisions: 决策列表，每个决策包含 reason, action, outcome
    
    Returns:
        {
            "pattern_stats": {
                "contrarian_fear": {"trades": 5, "wins": 1, "win_rate": 20, "avg_pnl": -1.5},
                "trend_following_short": {"trades": 8, "wins": 5, "win_rate": 62, "avg_pnl": 2.3},
                ...
            },
            "low_winrate_patterns": ["contrarian_fear", "counter_trend_long"],  # <40% win rate
            "high_winrate_patterns": ["trend_following_short", "support_long"],  # >=50% win rate
        }
    """
    # 只分析已平仓的开仓决策
    closed_opens = [
        d for d in decisions 
        if d.get("outcome", {}).get("status") == "closed" 
        and d.get("action", "").startswith("open_")
    ]
    
    if len(closed_opens) < 3:
        return {"available": False, "reason": "Not enough closed trades for pattern analysis"}
    
    # 统计每个模式的表现
    pattern_stats = {}
    
    for d in closed_opens:
        reason = d.get("reason", "")
        action = d.get("action", "")
        tags = extract_decision_tags(reason, action)
        
        outcome = d.get("outcome", {})
        net_pnl = outcome.get("net_pnl", 0)
        is_win = net_pnl > 0
        
        for tag in tags:
            if tag not in pattern_stats:
                pattern_stats[tag] = {
                    "trades": 0,
                    "wins": 0,
                    "total_pnl": 0
                }
            pattern_stats[tag]["trades"] += 1
            if is_win:
                pattern_stats[tag]["wins"] += 1
            pattern_stats[tag]["total_pnl"] += net_pnl
    
    # 计算胜率和平均盈亏
    for tag, stats in pattern_stats.items():
        trades = stats["trades"]
        if trades > 0:
            stats["win_rate"] = round(stats["wins"] / trades * 100)
            stats["avg_pnl"] = round(stats["total_pnl"] / trades, 2)
        else:
            stats["win_rate"] = 0
            stats["avg_pnl"] = 0
        # 移除 total_pnl（不需要在输出中）
        del stats["total_pnl"]
    
    # 筛选有足够样本的模式（至少 2 笔交易）
    significant_patterns = {k: v for k, v in pattern_stats.items() if v["trades"] >= 2}
    
    if not significant_patterns:
        return {"available": False, "reason": "No patterns with enough samples"}
    
    # 按胜率排序，找出表现最差和最好的模式（纯事实，无建议）
    sorted_by_winrate = sorted(
        significant_patterns.items(), 
        key=lambda x: (x[1]["win_rate"], x[1]["avg_pnl"])
    )
    
    # 低胜率模式（<40%）和高胜率模式（>=50%）- 仅标记，不给建议
    low_winrate_patterns = [p[0] for p in sorted_by_winrate[:3] if p[1]["win_rate"] < 40]
    high_winrate_patterns = [p[0] for p in sorted_by_winrate[-3:] if p[1]["win_rate"] >= 50]
    
    return {
        "available": True,
        "pattern_stats": significant_patterns,
        "low_winrate_patterns": low_winrate_patterns,
        "high_winrate_patterns": high_winrate_patterns,
    }


def _generate_summary(decisions: List[dict]) -> dict:
    """生成统计摘要"""
    total = len(decisions)
    if total == 0:
        return {"total": 0}

    # 统计各类动作
    action_counts = {}
    wins = 0
    losses = 0
    total_pnl = 0

    for d in decisions:
        action = d["action"]
        action_counts[action] = action_counts.get(action, 0) + 1

        outcome = d.get("outcome", {})
        if outcome.get("status") == "closed":
            # P2 Fix: 统一使用 net_pnl > 0 作为胜负判定标准
            # 与 _generate_insights_from_positions() 保持一致
            net_pnl = outcome.get("net_pnl", 0)
            if net_pnl > 0:
                wins += 1
            else:
                losses += 1
            total_pnl += net_pnl

    win_rate = wins / (wins + losses) if (wins + losses) > 0 else None

    return {
        "total_decisions": total,
        "action_breakdown": action_counts,
        "closed_trades": wins + losses,
        "wins": wins,
        "losses": losses,
        # P11 Fix: 统一 win_rate 为百分比格式 (0-100)
        # P2 Fix: 修复 win_rate=0 时被错误判断为 None 的问题
        "win_rate": round(win_rate * 100) if win_rate is not None else None,
        "total_pnl": round(total_pnl, 2)
    }


# L10 fix: _generate_insights() 已删除（死代码，已被 _generate_insights_from_positions() 替代）


def _generate_insights_from_positions(closed_positions: List[dict], deduplicated: Optional[List[dict]] = None) -> dict:
    """
    基于完整的平仓记录生成客观统计洞察
    
    注意：只返回客观数据，不提供行为指令或具体的仓位调整建议。
    AI 应根据 prompt_templates.py 中的规则自行决定如何应用这些数据。

    参数：
    - closed_positions: 直接从 Redis 读取的平仓记录列表
    - deduplicated: 去重后的决策记录，用于分析 entry_type
    """
    if not closed_positions:
        return {"available": False, "reason": "No closed positions to analyze"}

    insights = {"available": True}

    # ========== 0. Entry Type 分析（纯统计数据）==========
    if deduplicated:
        # 筛选已平仓的开仓决策
        closed_decisions = [d for d in deduplicated
                            if d.get("outcome", {}).get("status") == "closed"
                            and d.get("action", "").startswith("open_")]

        limit_trades = [d for d in closed_decisions if "limit" in d.get("action", "")]
        market_trades = [d for d in closed_decisions if "market" in d.get("action", "")]

        def calc_entry_stats(trades):
            if not trades:
                return None
            wins = sum(1 for t in trades if t["outcome"].get("net_pnl", 0) > 0)
            total_pnl = sum(t["outcome"].get("net_pnl", 0) for t in trades)
            avg_pnl = total_pnl / len(trades) if trades else 0
            win_rate = wins / len(trades) if trades else 0
            return {
                "count": len(trades),
                "wins": wins,
                "win_rate_pct": round(win_rate * 100, 1),
                "total_pnl": round(total_pnl, 2),
                "avg_pnl": round(avg_pnl, 2)
            }

        # 只返回统计数据，不给出建议
        insights["entry_type_stats"] = {
            "limit": calc_entry_stats(limit_trades),
            "market": calc_entry_stats(market_trades)
        }

    # ========== 1. 基础统计 ==========
    total = len(closed_positions)
    wins = sum(1 for p in closed_positions if p["net_pnl"] > 0)
    losses = total - wins
    win_rate = wins / total if total > 0 else 0
    total_pnl = sum(p["net_pnl"] for p in closed_positions)
    avg_pnl = total_pnl / total if total > 0 else 0

    insights["performance"] = {
        "total_trades": total,
        "wins": wins,
        "losses": losses,
        "win_rate_pct": round(win_rate * 100, 1),
        "total_pnl": round(total_pnl, 2),
        "avg_pnl": round(avg_pnl, 2)
    }

    # ========== 2. Exit Type 分析（纯统计）==========
    exit_stats = {}
    for pos in closed_positions:
        exit_type = _infer_exit_type(pos)
        if exit_type not in exit_stats:
            exit_stats[exit_type] = {"count": 0, "total_pnl": 0, "wins": 0}
        exit_stats[exit_type]["count"] += 1
        exit_stats[exit_type]["total_pnl"] += pos["net_pnl"]
        if pos["net_pnl"] > 0:
            exit_stats[exit_type]["wins"] += 1

    # 格式化 exit_stats
    formatted_exit_stats = {}
    for exit_type, stats in exit_stats.items():
        avg = stats["total_pnl"] / stats["count"] if stats["count"] > 0 else 0
        wr = stats["wins"] / stats["count"] * 100 if stats["count"] > 0 else 0
        formatted_exit_stats[exit_type] = {
            "count": stats["count"],
            "wins": stats["wins"],
            "win_rate_pct": round(wr, 1),
            "avg_pnl": round(avg, 2)
        }
    
    insights["exit_type_stats"] = formatted_exit_stats

    # ========== 3. 重复亏损 Symbol + Side 检测（纯事实）==========
    symbol_side_losses = {}
    symbol_side_loss_amount = {}
    for pos in closed_positions:
        if pos["net_pnl"] < 0:
            sym = pos["symbol"]
            side = pos.get("side", "").upper()
            key = f"{sym} {side}" if side else sym
            symbol_side_losses[key] = symbol_side_losses.get(key, 0) + 1
            symbol_side_loss_amount[key] = symbol_side_loss_amount.get(key, 0) + pos["net_pnl"]

    repeat_losers = [(key, count, symbol_side_loss_amount[key]) 
                    for key, count in symbol_side_losses.items() if count >= 2]
    repeat_losers.sort(key=lambda x: x[2])  # Sort by loss amount
    
    if repeat_losers:
        # 只返回事实数据，不给出建议
        insights["repeat_losers"] = [
            {"symbol_side": key, "loss_count": count, "total_loss": round(amt, 2)}
            for key, count, amt in repeat_losers[:5]
        ]

    # ========== 4. 利润保护分析（纯事实）==========
    high_peak_low_net = []
    for pos in closed_positions:
        peak = pos.get("peak_pnl", 0)
        net = pos.get("net_pnl", 0)
        if peak > 5 and net < peak * 0.3:
            side = pos.get("side", "").upper()
            high_peak_low_net.append({
                "symbol": pos["symbol"],
                "side": side,
                "peak_pnl": round(peak, 2),
                "net_pnl": round(net, 2),
                "captured_pct": round(net / peak * 100, 0) if peak > 0 else 0
            })

    if high_peak_low_net:
        leaked = sum(h["peak_pnl"] - h["net_pnl"] for h in high_peak_low_net)
        insights["profit_leak"] = {
            "total_leaked": round(leaked, 2),
            "trade_count": len(high_peak_low_net),
            "examples": high_peak_low_net[:3]
        }

    # ========== 5. 决策模式分析（纯统计）==========
    if deduplicated:
        pattern_analysis = analyze_pattern_performance(deduplicated)
        if pattern_analysis.get("available"):
            # 只保留统计数据，不给出建议
            insights["pattern_stats"] = pattern_analysis.get("pattern_stats", {})
            if pattern_analysis.get("low_winrate_patterns"):
                insights["low_winrate_patterns"] = pattern_analysis["low_winrate_patterns"]
            if pattern_analysis.get("high_winrate_patterns"):
                insights["high_winrate_patterns"] = pattern_analysis["high_winrate_patterns"]

    return insights


def _generate_market_environment(
    closed_positions: List[dict],
    symbols_data: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    生成市场环境数据，供 AI 参考。
    
    注意：只返回客观的市场状态数据，不提供行为指令或仓位调整建议。
    AI 应根据 prompt_templates.py 中的规则自行决定如何应用这些数据。
    
    Returns:
        {
            "market_regime": {...},  # 市场状态（牛熊震荡）
            "volatility": {...},     # 波动率水平
            "trading_session": {...}, # 交易时段
            "coin_performance": {...}, # 币种表现统计
            "streak_analysis": {...}   # 连续盈亏统计
        }
    """
    result = {}
    
    # 1. 市场状态分析（客观数据）
    market_regime = _get_market_regime(symbols_data)
    # 只保留客观数据，移除 description（可能包含建议）
    result["market_regime"] = {
        "regime": market_regime.get("regime"),
        "btc_trend": market_regime.get("btc_trend"),
        "eth_trend": market_regime.get("eth_trend"),
        "confidence": market_regime.get("confidence"),
        "btc_rsi": market_regime.get("btc_rsi"),
        "btc_24h_change": market_regime.get("btc_24h_change")
    }
    
    # 2. 波动率分析（客观数据）
    volatility = _get_volatility_level(symbols_data)
    # 只保留客观数据，移除 recommendation
    result["volatility"] = {
        "level": volatility.get("level"),
        "btc_atr_pct": volatility.get("btc_atr_pct")
    }
    
    # 3. 交易时段（客观数据）
    session = _get_trading_session()
    # 只保留客观数据，移除 recommendation 和 characteristics
    result["trading_session"] = {
        "session": session.get("session"),
        "utc_hour": session.get("utc_hour"),
        "liquidity": session.get("liquidity"),
        "volatility_tendency": session.get("volatility_tendency")
    }
    
    # 4. 币种表现分析（客观统计）
    coin_performance = _analyze_coin_performance(closed_positions)
    # 只保留统计数据，移除 recommendation
    result["coin_performance"] = {
        "major_coins": coin_performance.get("major_coins"),
        "mid_cap": coin_performance.get("mid_cap"),
        "small_cap": coin_performance.get("small_cap")
    }
    
    # 5. 连续盈亏分析（客观数据，已在 _analyze_streak 中修复）
    streak = _analyze_streak(closed_positions)
    result["streak_analysis"] = streak
    
    return result


# ==========================================================
# 6. 便捷函数
# ==========================================================
def get_feedback_for_payload(
        uid: str,
        current_prices: Optional[Dict[str, float]] = None,
        exchange: Optional[str] = None
) -> dict:
    """
    获取反馈数据，直接可添加到投喂 payload 中

    Args:
        uid: 用户 ID（必填）
        current_prices: 当前价格字典（可选）
        exchange: 交易所名称（可选），如果指定则只返回该交易所的反馈
                  注意：每个交易所独立投喂，不应混合不同交易所的数据

    使用方式：
        feedback = get_feedback_for_payload(uid="user123", exchange="binance")
        payload["decision_feedback"] = feedback
    """
    return build_decision_feedback(
        uid=uid,
        lookback_hours=LOOKBACK_HOURS,
        max_decisions=MAX_DECISIONS,
        current_prices=current_prices,
        exchange=exchange
    )


# ==========================================================
# 7. Context Memory - Historical Situation Matching
# ==========================================================
def find_similar_situations(
    uid: str,
    current_context: Dict[str, Any],
    exchange: Optional[str] = None,
    lookback_days: int = 30,
    top_n: int = 3,
) -> Optional[Dict[str, Any]]:
    """
    Find historically similar situations from ai_decisions table.

    Compares current market context against past decision contexts
    to find analogous situations and their outcomes. This enables
    the LLM to learn from past experience.

    Similarity dimensions:
    - market_regime (strong_bearish..strong_bullish)
    - fear_greed band (0-20, 20-40, 40-60, 60-80, 80-100)
    - position_count (0, 1-2, 3+)
    - exposure band (0-20%, 20-50%, 50%+)

    Args:
        uid: User ID
        current_context: Current global_context + balance_info snapshot
        exchange: Exchange filter (optional)
        lookback_days: How far back to search
        top_n: How many similar situations to return

    Returns:
        {
            "similar_situations": [{date, context, decision, outcome, lesson}],
            "current_match_pct": int
        }
        or None if not enough data
    """
    try:
        from core.ai_decision_db import get_ai_decision_db
        db = get_ai_decision_db()

        # Get recent decisions with responses
        # Limit to 50 to reduce JSON deserialization overhead
        # (each record contains full feed_json ~10-200KB)
        records = db.get_recent_decisions(
            uid,
            limit=50,
            lookback_hours=lookback_days * 24,
        )

        if not records or len(records) < 5:
            return None

        # Extract current situation fingerprint
        current_fp = _build_situation_fingerprint(current_context)
        if not current_fp:
            return None

        # Score each historical decision for similarity
        scored = []
        high_quality_count = 0  # Track matches with similarity >= 70
        for record in records:
            req = record.get("request", {})
            resp = record.get("response", {})
            ts = record.get("timestamp", 0)

            # Extract historical context from request_data
            hist_context = _extract_context_from_request(req)
            if not hist_context:
                continue

            hist_fp = _build_situation_fingerprint(hist_context)
            if not hist_fp:
                continue

            # Calculate similarity
            similarity = _calc_similarity(current_fp, hist_fp)
            if similarity < 40:
                continue

            # Extract decision and outcome
            signals = resp.get("signals", [])
            if not signals:
                # Try groups format
                for g in resp.get("groups", []):
                    signals.extend(g.get("response", {}).get("signals", []))

            decision_summary = _summarize_signals(signals)

            scored.append({
                "timestamp": ts,
                "similarity": similarity,
                "context_summary": _fp_to_summary(hist_fp),
                "decision": decision_summary,
                "fingerprint": hist_fp,
            })

            if similarity >= 70:
                high_quality_count += 1
            # Early exit: enough high-quality matches found
            if high_quality_count >= top_n * 2:
                break

        if not scored:
            return None

        # Sort by similarity, take top N
        scored.sort(key=lambda x: -x["similarity"])
        top_situations = scored[:top_n]

        # Try to find outcomes for these decisions
        # (match with closed_positions if possible)
        closed = get_closed_positions(uid, lookback_hours=lookback_days * 24, exchange=exchange)

        situations = []
        for sit in top_situations:
            ts = sit["timestamp"]
            ts_ms = ts * 1000

            # Find ALL trades opened around this decision time (within 1h)
            matched_trades = []
            for pos in closed:
                if abs(pos.get("open_time_ms", 0) - ts_ms) < 3600000:
                    matched_trades.append(pos)

            # Build outcome summary from all matched trades
            if matched_trades:
                total_pnl = sum(t.get("net_pnl", 0) for t in matched_trades)
                wins = sum(1 for t in matched_trades if t.get("net_pnl", 0) > 0)
                losses = len(matched_trades) - wins

                trade_details = []
                for t in matched_trades[:3]:  # Show up to 3
                    pnl = t.get("net_pnl", 0)
                    sym = t.get("symbol", "")
                    sd = t.get("side", "")
                    sign = "+" if pnl > 0 else "-"
                    trade_details.append(f"{sym} {sd} {sign}${abs(pnl):.2f}")

                if total_pnl > 0:
                    outcome_str = f"Profitable (${total_pnl:+.2f}, {wins}W/{losses}L) - {', '.join(trade_details)}"
                else:
                    outcome_str = f"Loss (${total_pnl:+.2f}, {wins}W/{losses}L) - {', '.join(trade_details)}"

                # Generate lesson from outcome
                decision = sit.get("decision", "")
                regime = sit.get("fingerprint", {}).get("regime", "")
                lesson_str = _generate_situation_lesson(
                    total_pnl=total_pnl,
                    wins=wins,
                    losses=losses,
                    decision=decision,
                    regime=regime,
                    trades=matched_trades,
                )
            else:
                outcome_str = "unknown"
                lesson_str = ""

            # Skip situations with unknown outcomes - they provide no learning value
            if outcome_str == "unknown":
                continue

            situations.append({
                "date": time.strftime(
                    '%Y-%m-%d %H:%M UTC', time.gmtime(ts)
                ),
                "context": sit["context_summary"],
                "decision": sit["decision"],
                "outcome": outcome_str,
                "lesson": lesson_str,
                "similarity": sit["similarity"],  # Keep for dedup sorting
            })

        if not situations:
            return None

        # W5 fix: deduplicate situations with identical outcomes
        # When multiple decisions within 1h match the same closed trades,
        # they produce identical outcome strings. Keep only the highest
        # similarity match per unique outcome.
        seen_outcomes = {}
        deduped_situations = []
        for sit in situations:
            outcome_key = sit["outcome"]
            if outcome_key not in seen_outcomes:
                seen_outcomes[outcome_key] = sit
                deduped_situations.append(sit)
            elif sit["similarity"] > seen_outcomes[outcome_key]["similarity"]:
                # Replace with higher similarity match
                deduped_situations = [
                    s for s in deduped_situations if s["outcome"] != outcome_key
                ]
                seen_outcomes[outcome_key] = sit
                deduped_situations.append(sit)

        # Remove the temporary similarity field from output
        for sit in deduped_situations:
            sit.pop("similarity", None)

        situations = deduped_situations

        best_match = top_situations[0]["similarity"]

        return {
            "similar_situations": situations,
            "current_match_pct": best_match,
        }

    except Exception as e:
        logger.debug(f"[{uid}] Error finding similar situations: {e}")
        return None


def _build_situation_fingerprint(context: Dict[str, Any]) -> Optional[Dict]:
    """
    Build a comparable fingerprint from a context dict.

    Works with:
    - Current global_context (has market_regime, market_sentiment, account_risk, position_distribution)
    - Historical request_data old format (has global_context, summary, markets)
    - Historical request_data new format (has context_layer/market, positions_layer/positions)
    """
    try:
        # Shortcut: extract context_layer/market if present (layered feed format)
        cl = context.get("market") or context.get("context_layer", {})

        # Try multiple paths for regime
        regime = None
        if "market_regime" in context:
            mr = context["market_regime"]
            regime = mr.get("market_sentiment") or mr.get("state")
        elif cl.get("market_regime"):
            mr = cl["market_regime"]
            regime = mr.get("state") or mr.get("market_sentiment")
        elif "global_context" in context:
            gc = context["global_context"]
            mr = gc.get("market_regime", {})
            regime = mr.get("market_sentiment") or mr.get("state")
        # From summary (old format)
        elif "summary" in context:
            regime = context["summary"].get("market_bias")

        # Fear & greed
        fng = None
        ms = context.get("market_sentiment", {})
        if ms:
            fng_data = ms.get("fear_greed", {})
            fng = fng_data.get("value") if fng_data.get("available") else None
        # Try context_layer path (new format)
        if fng is None and cl.get("sentiment"):
            fng = cl["sentiment"].get("fear_greed")
        # Try global_context path
        if fng is None and "global_context" in context:
            gc = context["global_context"]
            ms2 = gc.get("market_sentiment", {})
            fng_data = ms2.get("fear_greed", {})
            fng = fng_data.get("value") if fng_data.get("available") else None
        # Try summary path (old format)
        if fng is None and "summary" in context:
            fng = context["summary"].get("fear_greed")

        # Position count
        pos_count = 0
        if "positions" in context:
            p = context["positions"]
            pos_count = len(p) if isinstance(p, list) else 0
        elif "positions_layer" in context:
            pl = context["positions_layer"]
            if isinstance(pl, list):
                pos_count = len(pl)
            else:
                pos_count = pl.get("summary", {}).get("count", 0)
        elif "position_distribution" in context:
            pd = context["position_distribution"]
            pos_count = pd.get("long_count", 0) + pd.get("short_count", 0)

        # Exposure
        exposure = 0
        if "account_risk" in context:
            util = context["account_risk"].get("utilization", 0) or 0
            exposure = util * 100
        elif cl.get("account"):
            exposure = cl["account"].get("exposure_pct", 0) or 0
        elif "global_context" in context:
            ar = context["global_context"].get("account_risk", {})
            util = ar.get("utilization", 0) or 0
            exposure = util * 100
        elif "summary" in context:
            exposure = context["summary"].get("exposure_pct", 0) or 0

        if regime is None:
            return None

        # Discretize
        fng_band = _discretize_fng(fng) if fng is not None else "unknown"
        pos_band = "none" if pos_count == 0 else ("few" if pos_count <= 2 else "many")
        exp_band = "low" if exposure < 20 else ("medium" if exposure < 50 else "high")

        return {
            "regime": regime,
            "fng_band": fng_band,
            "fng_raw": fng,
            "pos_band": pos_band,
            "pos_count": pos_count,
            "exp_band": exp_band,
            "exposure": round(exposure, 1),
        }

    except Exception as e:
        logger.debug(f"Error building situation fingerprint: {e}")
        return None


def _discretize_fng(value) -> str:
    """Discretize fear & greed value into bands."""
    if value is None:
        return "unknown"
    try:
        v = float(value)
    except (TypeError, ValueError):
        return "unknown"
    if v <= 20:
        return "extreme_fear"
    elif v <= 40:
        return "fear"
    elif v <= 60:
        return "neutral"
    elif v <= 80:
        return "greed"
    else:
        return "extreme_greed"


def _calc_similarity(fp1: dict, fp2: dict) -> int:
    """Calculate similarity score (0-100) between two fingerprints."""
    score = 0
    total_weight = 0

    # Regime match (weight=40)
    w = 40
    total_weight += w
    r1 = fp1.get("regime", "")
    r2 = fp2.get("regime", "")
    if r1 == r2:
        score += w
    elif _regime_distance(r1, r2) <= 1:
        score += w * 0.5

    # FnG band (weight=25)
    w = 25
    total_weight += w
    if fp1.get("fng_band") == fp2.get("fng_band"):
        score += w
    elif fp1.get("fng_band") != "unknown" and fp2.get("fng_band") != "unknown":
        fng1 = fp1.get("fng_raw")
        fng2 = fp2.get("fng_raw")
        if fng1 is not None and fng2 is not None:
            try:
                diff = abs(float(fng1) - float(fng2))
                if diff <= 10:
                    score += w * 0.7
                elif diff <= 20:
                    score += w * 0.3
            except (TypeError, ValueError):
                pass

    # Position band (weight=20)
    w = 20
    total_weight += w
    if fp1.get("pos_band") == fp2.get("pos_band"):
        score += w
    elif abs(fp1.get("pos_count", 0) - fp2.get("pos_count", 0)) <= 1:
        score += w * 0.5

    # Exposure band (weight=15)
    w = 15
    total_weight += w
    if fp1.get("exp_band") == fp2.get("exp_band"):
        score += w
    else:
        exp_diff = abs(fp1.get("exposure", 0) - fp2.get("exposure", 0))
        if exp_diff <= 10:
            score += w * 0.5

    return round(score / total_weight * 100) if total_weight > 0 else 0


# H4 fix: 补充 market_context.py 产生的 5 状态值（mixed, mild_bullish, mild_bearish）
# 旧列表只有 decision_feedback 的 3 状态词汇，导致 _regime_distance() 对新状态返回 99
_REGIME_ORDER = [
    "strong_bearish", "mild_bearish", "bearish",
    "mixed", "neutral", "ranging",
    "mild_bullish", "bullish", "strong_bullish",
]


def _regime_distance(r1: str, r2: str) -> int:
    """Calculate distance between two regime labels."""
    if r1 == "unknown" or r2 == "unknown":
        return 99  # unknown cannot be compared
    try:
        i1 = _REGIME_ORDER.index(r1)
        i2 = _REGIME_ORDER.index(r2)
        return abs(i1 - i2)
    except ValueError:
        return 99


def _extract_context_from_request(req: Dict) -> Optional[Dict]:
    """Extract context info from a historical request_data record."""
    if not req or not isinstance(req, dict):
        return None
    # The request_data contains the full feed_json
    # Old format: has summary, global_context, markets
    # New format: has market, positions, symbols (data-only feed)
    # Legacy format: has decision_layer, context_layer, etc.
    if "global_context" in req or "summary" in req or "context_layer" in req or "market" in req:
        return req
    return None


def _fp_to_summary(fp: dict) -> str:
    """Convert a fingerprint to a human-readable summary string."""
    parts = []
    regime = fp.get("regime", "unknown")
    parts.append(regime.replace("_", " ").title())

    fng_band = fp.get("fng_band", "unknown")
    if fng_band != "unknown":
        fng_raw = fp.get("fng_raw")
        parts.append(f"FnG={fng_raw}" if fng_raw is not None else fng_band)

    pos_count = fp.get("pos_count", 0)
    parts.append(f"{pos_count} positions")

    exposure = fp.get("exposure", 0)
    parts.append(f"{exposure}% exposure")

    return ", ".join(parts)


def _summarize_signals(signals: List[dict]) -> str:
    """Summarize a list of signals into a brief string."""
    if not signals:
        return "No action taken"

    actions = []
    for sig in signals[:5]:
        if isinstance(sig, dict):
            action = sig.get("action", "")
            symbol = sig.get("symbol", "")
            if action and symbol:
                actions.append(f"{action} {symbol}")

    return "; ".join(actions) if actions else "No action taken"


def _generate_situation_lesson(
    total_pnl: float,
    wins: int,
    losses: int,
    decision: str,
    regime: str,
    trades: List[dict],
) -> str:
    """
    Generate a concise factual summary from a historical outcome.

    Returns only objective facts about what happened - no advice or recommendations.
    The LLM should draw its own conclusions from the data.
    """
    if not trades:
        return ""

    regime_label = regime.replace("_", " ") if regime else "unknown"

    # Analyze the biggest loss/win for context
    worst_trade = min(trades, key=lambda t: t.get("net_pnl", 0))
    best_trade = max(trades, key=lambda t: t.get("net_pnl", 0))

    if total_pnl > 0 and wins > losses:
        # Winning pattern - factual summary
        best_sym = best_trade.get("symbol", "")
        best_side = best_trade.get("side", "")
        best_pnl = best_trade.get("net_pnl", 0)
        return (
            f"{regime_label} market. {decision} → net +${total_pnl:.2f}. "
            f"Best: {best_sym} {best_side} +${best_pnl:.2f}."
        )
    elif total_pnl > 0 and losses > 0:
        # Mixed but net positive - factual summary
        worst_sym = worst_trade.get("symbol", "")
        worst_pnl = worst_trade.get("net_pnl", 0)
        return (
            f"{regime_label} market. Net +${total_pnl:.2f} ({wins}W/{losses}L). "
            f"Worst: {worst_sym} -${abs(worst_pnl):.2f}."
        )
    elif total_pnl < 0:
        # Losing pattern - factual summary with context
        worst_sym = worst_trade.get("symbol", "")
        worst_side = worst_trade.get("side", "")
        worst_pnl = worst_trade.get("net_pnl", 0)

        # Check if it was a counter-trend trade (factual observation)
        is_counter = (
            (worst_side == "LONG" and "bearish" in regime)
            or (worst_side == "SHORT" and "bullish" in regime)
        )
        
        # Check duration and peak for context
        dur_ms = worst_trade.get("duration_ms", 0)
        peak = worst_trade.get("peak_pnl", 0)
        dur_hours = dur_ms / 3600000 if dur_ms else 0
        
        context_parts = [f"{regime_label} market"]
        if is_counter:
            context_parts.append(f"counter-trend {worst_side}")
        if dur_hours > 24:
            context_parts.append(f"held {dur_hours:.0f}h")
        if peak > 0:
            context_parts.append(f"peak was +${peak:.2f}")
        
        return (
            f"{', '.join(context_parts)}. "
            f"{worst_sym} {worst_side} → -${abs(worst_pnl):.2f}. Net: ${total_pnl:.2f}."
        )
    else:
        return ""

