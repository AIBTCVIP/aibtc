# trade_autopsy.py
# Trade Autopsy Module - Phase 4
# Records detailed trade snapshots for failure pattern analysis

import os
import json
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Union
from dataclasses import dataclass

logger = logging.getLogger(__name__)

try:
    from sqlalchemy import create_engine, text
    from sqlalchemy.orm import sessionmaker
    from sqlalchemy.pool import NullPool
    HAS_SQLALCHEMY = True
except ImportError:
    HAS_SQLALCHEMY = False


@dataclass
class TradeSnapshot:
    trade_id: str
    timestamp: str
    symbol: str
    side: str
    entry_price: float
    decision: str
    confidence: float
    reasoning: Dict
    bias_score: int
    bias_direction: str
    candle_type: str
    rejection_signals: Union[str, Dict]  # str (new) or Dict (legacy)
    close_position: Union[str, Dict]     # str (new) or Dict (legacy)
    vs_levels: Dict
    quick_checks_status: str
    had_critical_alert: bool
    had_high_alert: bool
    alerts_triggered: List[Dict]
    btc_state: str
    eth_state: str
    regime: str
    exit_price: Optional[float] = None
    pnl_pct: Optional[float] = None
    result: Optional[str] = None
    hold_duration_minutes: Optional[int] = None
    failure_type: Optional[str] = None
    failure_analysis: Optional[str] = None
    lessons_learned: Optional[str] = None


class TradeAutopsyRecorder:
    _instance = None
    
    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self, db_url: str = None):
        if self._initialized:
            return
        
        if not db_url:
            db_url = os.getenv('DATABASE_URL', 'mysql+pymysql://root:123456@127.0.0.1:3306/tradev6')
        
        self.db_url = db_url
        self.engine = None
        self.Session = None
        
        if HAS_SQLALCHEMY:
            try:
                self.engine = create_engine(db_url, poolclass=NullPool, pool_pre_ping=True)
                self.Session = sessionmaker(bind=self.engine)
                logger.info("TradeAutopsyRecorder initialized - tables managed by user_db.py")
            except Exception as e:
                logger.error(f"Failed to connect to MySQL: {e}")
                self.engine = None
        else:
            logger.warning("SQLAlchemy not available, trade autopsy disabled")
        
        self._initialized = True
    
    def _get_session(self):
        if self.Session:
            return self.Session()
        return None
    
    def record_entry(self, snapshot: TradeSnapshot) -> bool:
        session = self._get_session()
        if not session:
            return False
        
        try:
            session.execute(text("""
                INSERT INTO trade_snapshots (
                    trade_id, timestamp, symbol, side, entry_price,
                    decision, confidence, reasoning_json,
                    bias_score, bias_direction, candle_type,
                    rejection_signals_json, close_position_json, vs_levels_json,
                    quick_checks_status, had_critical_alert, had_high_alert, alerts_triggered_json,
                    btc_state, eth_state, regime
                ) VALUES (
                    :trade_id, :timestamp, :symbol, :side, :entry_price,
                    :decision, :confidence, :reasoning_json,
                    :bias_score, :bias_direction, :candle_type,
                    :rejection_signals_json, :close_position_json, :vs_levels_json,
                    :quick_checks_status, :had_critical_alert, :had_high_alert, :alerts_triggered_json,
                    :btc_state, :eth_state, :regime
                )
            """), {
                'trade_id': snapshot.trade_id,
                'timestamp': snapshot.timestamp,
                'symbol': snapshot.symbol,
                'side': snapshot.side,
                'entry_price': snapshot.entry_price,
                'decision': snapshot.decision,
                'confidence': snapshot.confidence,
                'reasoning_json': json.dumps(snapshot.reasoning),
                'bias_score': snapshot.bias_score,
                'bias_direction': snapshot.bias_direction,
                'candle_type': snapshot.candle_type,
                'rejection_signals_json': json.dumps(snapshot.rejection_signals),
                'close_position_json': json.dumps(snapshot.close_position),
                'vs_levels_json': json.dumps(snapshot.vs_levels),
                'quick_checks_status': snapshot.quick_checks_status,
                'had_critical_alert': int(snapshot.had_critical_alert),
                'had_high_alert': int(snapshot.had_high_alert),
                'alerts_triggered_json': json.dumps(snapshot.alerts_triggered),
                'btc_state': snapshot.btc_state,
                'eth_state': snapshot.eth_state,
                'regime': snapshot.regime,
            })
            session.commit()
            logger.info(f"Recorded trade entry: {snapshot.trade_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to record trade entry: {e}")
            session.rollback()
            return False
        finally:
            session.close()
    
    def record_exit(self, trade_id: str, exit_price: float, hold_duration_minutes: int) -> bool:
        session = self._get_session()
        if not session:
            return False
        
        try:
            result = session.execute(text(
                "SELECT entry_price, side FROM trade_snapshots WHERE trade_id = :trade_id"
            ), {'trade_id': trade_id}).fetchone()
            
            if not result:
                logger.warning(f"Trade not found: {trade_id}")
                return False
            
            entry_price, side = result[0], result[1]
            
            if side == "LONG":
                pnl_pct = ((float(exit_price) - float(entry_price)) / float(entry_price)) * 100
            else:
                pnl_pct = ((float(entry_price) - float(exit_price)) / float(entry_price)) * 100
            
            result_str = "WIN" if pnl_pct > 0.5 else ("LOSS" if pnl_pct < -0.5 else "BREAK_EVEN")
            
            # Auto-analyze failure if loss
            if result_str == "LOSS":
                self._auto_analyze_failure(trade_id)
            
            session.execute(text("""
                UPDATE trade_snapshots SET
                    exit_price = :exit_price, pnl_pct = :pnl_pct,
                    result = :result, hold_duration_minutes = :hold_duration
                WHERE trade_id = :trade_id
            """), {
                'exit_price': exit_price, 'pnl_pct': pnl_pct,
                'result': result_str, 'hold_duration': hold_duration_minutes,
                'trade_id': trade_id
            })
            session.commit()
            logger.info(f"Recorded trade exit: {trade_id}, result={result_str}, pnl_pct={pnl_pct:.2f}%")
            return True
        except Exception as e:
            logger.error(f"Failed to record trade exit: {e}")
            session.rollback()
            return False
        finally:
            session.close()
    
    def _auto_analyze_failure(self, trade_id: str):
        """Auto-analyze failure type based on snapshot data"""
        session = self._get_session()
        if not session:
            return
        
        try:
            row = session.execute(text(
                "SELECT * FROM trade_snapshots WHERE trade_id = :trade_id"
            ), {'trade_id': trade_id}).fetchone()
            
            if not row:
                return
            
            columns = [desc[0] for desc in session.execute(text("DESCRIBE trade_snapshots")).fetchall()]
            snapshot = dict(zip(columns, row))
            
            failure_type = self._detect_failure_type(snapshot)
            failure_analysis = self._generate_failure_analysis(snapshot, failure_type)
            
            session.execute(text("""
                UPDATE trade_snapshots SET
                    failure_type = :failure_type, failure_analysis = :failure_analysis
                WHERE trade_id = :trade_id
            """), {'failure_type': failure_type, 'failure_analysis': failure_analysis, 'trade_id': trade_id})
            session.commit()
            
            self._update_pattern_stats(failure_type)
            logger.info(f"Auto-analyzed failure: {trade_id} -> {failure_type}")
            
        except Exception as e:
            logger.error(f"Failed to auto-analyze failure: {e}")
            session.rollback()
        finally:
            session.close()
    
    def _detect_failure_type(self, snapshot: Dict) -> str:
        """Detect failure type based on snapshot data"""
        reasons = []
        
        # Check 1: False breakout
        vs_levels = json.loads(snapshot.get('vs_levels_json', '{}'))
        if vs_levels.get('false_breakout') or vs_levels.get('false_breakout_suspected'):
            reasons.append('false_breakout')
        
        # Check 2: Candle-indicator conflict
        rejection_raw = json.loads(snapshot.get('rejection_signals_json', '{}'))
        bias_score = snapshot.get('bias_score', 0)
        
        # Support both old dict format and new string format
        if isinstance(rejection_raw, str):
            has_upper = 'upper' in rejection_raw
            has_lower = 'lower' in rejection_raw
        else:
            has_upper = rejection_raw.get('upper_rejection')
            has_lower = rejection_raw.get('lower_rejection')
        
        if bias_score > 5 and has_upper:
            reasons.append('candle_bias_conflict')
        elif bias_score < -5 and has_lower:
            reasons.append('candle_bias_conflict')
        
        # Check 3: BTC environment mismatch
        btc_state = snapshot.get('btc_state', '')
        if btc_state in ['ranging', 'range']:
            reasons.append('btc_environment_mismatch')
        
        # Check 4: Volume weakness
        alerts = json.loads(snapshot.get('alerts_triggered_json', '[]'))
        for alert in alerts:
            if alert.get('type') == 'volume_weakness':
                reasons.append('volume_weakness')
        
        # Check 5: Critical alert ignored
        if snapshot.get('had_critical_alert'):
            reasons.append('alert_ignored')
        
        # Check 6: Low confidence
        confidence = snapshot.get('confidence', 0)
        if confidence and float(confidence) < 0.5:
            reasons.append('low_confidence')
        
        # Return primary failure reason (most important)
        priority = ['false_breakout', 'candle_bias_conflict', 'btc_environment_mismatch', 
                   'volume_weakness', 'alert_ignored', 'low_confidence']
        
        for p in priority:
            if p in reasons:
                return p
        
        return 'unknown'
    
    def _generate_failure_analysis(self, snapshot: Dict, failure_type: str) -> str:
        """Generate failure analysis text"""
        analyses = {
            'false_breakout': "Price broke key level but failed to sustain. Entry was on false breakout.",
            'candle_bias_conflict': "Indicator showed strong direction but candle rejected. Should have followed candle.",
            'btc_environment_mismatch': "BTC was in range/consolidation. Altcoin trade failed due to market environment.",
            'volume_weakness': "Entry made without volume confirmation. Price moved without conviction.",
            'alert_ignored': "Quick checks raised critical alert but trade was executed anyway.",
            'low_confidence': "Trade executed with low confidence (<0.5). Should have skipped.",
            'unknown': "Loss reason could not be automatically determined."
        }
        return analyses.get(failure_type, analyses['unknown'])
    
    def analyze_failure(self, trade_id: str, failure_type: str, 
                       failure_analysis: str, lessons_learned: str) -> bool:
        session = self._get_session()
        if not session:
            return False
        
        try:
            session.execute(text("""
                UPDATE trade_snapshots SET
                    failure_type = :failure_type, failure_analysis = :failure_analysis, lessons_learned = :lessons
                WHERE trade_id = :trade_id
            """), {
                'failure_type': failure_type, 'failure_analysis': failure_analysis,
                'lessons': lessons_learned, 'trade_id': trade_id
            })
            session.commit()
            self._update_pattern_stats(failure_type)
            return True
        except Exception as e:
            logger.error(f"Failed to record failure analysis: {e}")
            session.rollback()
            return False
        finally:
            session.close()
    
    def _update_pattern_stats(self, pattern_type: str):
        session = self._get_session()
        if not session:
            return
        
        try:
            result = session.execute(text("""
                SELECT COUNT(*), SUM(CASE WHEN result = 'WIN' THEN 1 ELSE 0 END), AVG(pnl_pct)
                FROM trade_snapshots WHERE failure_type = :pattern_type
            """), {'pattern_type': pattern_type}).fetchone()
            
            if result and result[0]:
                total, wins, avg_pnl = result[0], result[1], float(result[2] or 0)
                win_rate = (wins / total * 100) if total > 0 else 0
                session.execute(text("""
                    UPDATE failure_patterns SET
                        occurrence_count = :total, win_rate_pct = :win_rate, avg_pnl_pct = :avg_pnl, last_seen = NOW()
                    WHERE pattern_type = :pattern_type
                """), {'total': total, 'win_rate': win_rate, 'avg_pnl': avg_pnl, 'pattern_type': pattern_type})
                session.commit()
        except Exception as e:
            logger.error(f"Failed to update pattern stats: {e}")
            session.rollback()
        finally:
            session.close()
    
    def get_recent_snapshots(self, limit: int = 20, result: str = None) -> List[Dict]:
        session = self._get_session()
        if not session:
            return []
        
        try:
            query = "SELECT * FROM trade_snapshots"
            if result:
                query += " WHERE result = :result"
            query += " ORDER BY timestamp DESC LIMIT :limit"
            
            rows = session.execute(text(query), {'limit': limit, 'result': result}).fetchall()
            columns = [desc[0] for desc in session.execute(text("DESCRIBE trade_snapshots")).fetchall()]
            return [dict(zip(columns, row)) for row in rows]
        except Exception as e:
            logger.error(f"Failed to get recent snapshots: {e}")
            return []
        finally:
            session.close()
    
    def get_failures_by_type(self, pattern_type: str = None) -> List[Dict]:
        session = self._get_session()
        if not session:
            return []
        
        try:
            query = "SELECT * FROM trade_snapshots WHERE result = 'LOSS'"
            if pattern_type:
                query += " AND failure_type = :pattern_type"
            query += " ORDER BY timestamp DESC"
            
            rows = session.execute(text(query), {'pattern_type': pattern_type}).fetchall()
            columns = [desc[0] for desc in session.execute(text("DESCRIBE trade_snapshots")).fetchall()]
            return [dict(zip(columns, row)) for row in rows]
        except Exception as e:
            logger.error(f"Failed to get failures: {e}")
            return []
        finally:
            session.close()
    
    def get_failure_patterns(self) -> List[Dict]:
        session = self._get_session()
        if not session:
            return []
        
        try:
            rows = session.execute(text("""
                SELECT * FROM failure_patterns WHERE is_active = 1 ORDER BY occurrence_count DESC
            """)).fetchall()
            columns = [desc[0] for desc in session.execute(text("DESCRIBE failure_patterns")).fetchall()]
            return [dict(zip(columns, row)) for row in rows]
        except Exception as e:
            logger.error(f"Failed to get failure patterns: {e}")
            return []
        finally:
            session.close()
    
    def add_failure_pattern(self, pattern_type: str, description: str, conditions: Dict) -> bool:
        session = self._get_session()
        if not session:
            return False
        
        try:
            session.execute(text("""
                INSERT INTO failure_patterns (pattern_type, description, conditions_json, first_seen, last_seen)
                VALUES (:pattern_type, :description, :conditions, NOW(), NOW())
            """), {'pattern_type': pattern_type, 'description': description, 'conditions': json.dumps(conditions)})
            session.commit()
            return True
        except Exception as e:
            logger.error(f"Failed to add failure pattern: {e}")
            session.rollback()
            return False
        finally:
            session.close()
    
    def check_context_against_patterns(self, context: Dict) -> List[Dict]:
        patterns = self.get_failure_patterns()
        matched = []
        
        for pattern in patterns:
            conditions = json.loads(pattern.get('conditions_json', '{}'))
            is_match = True
            for key, expected in conditions.items():
                actual = self._get_nested_value(context, key)
                if actual != expected:
                    is_match = False
                    break
            
            if is_match:
                matched.append({
                    'pattern_type': pattern['pattern_type'],
                    'description': pattern['description'],
                    'win_rate_pct': pattern['win_rate_pct'],
                    'avg_pnl_pct': pattern['avg_pnl_pct'],
                    'occurrence_count': pattern['occurrence_count']
                })
        
        return matched
    
    def _get_nested_value(self, data: Dict, path: str) -> Any:
        keys = path.split('.')
        value = data
        for key in keys:
            if isinstance(value, dict):
                value = value.get(key)
            else:
                return None
        return value
    
    def generate_weekly_report(self, week_start: datetime = None) -> Dict:
        session = self._get_session()
        if not session:
            return {}
        
        if not week_start:
            week_start = datetime.now() - timedelta(days=7)
        week_end = week_start + timedelta(days=7)
        
        try:
            result = session.execute(text("""
                SELECT COUNT(*), SUM(CASE WHEN result = 'WIN' THEN 1 ELSE 0 END),
                       SUM(CASE WHEN result = 'LOSS' THEN 1 ELSE 0 END), AVG(pnl_pct)
                FROM trade_snapshots WHERE timestamp >= :week_start AND timestamp < :week_end
            """), {'week_start': week_start, 'week_end': week_end}).fetchone()
            
            if not result or not result[0]:
                return {'week': week_start.isoformat(), 'trades': 0}
            
            total, wins, losses, avg_pnl = result[0], result[1], result[2], float(result[3] or 0)
            win_rate = (wins / total * 100) if total > 0 else 0
            
            failures = self.get_failures_by_type()
            patterns_identified = {}
            for f in failures:
                ft = f.get('failure_type')
                if ft:
                    patterns_identified[ft] = patterns_identified.get(ft, 0) + 1
            
            recommendations = self._generate_recommendations(patterns_identified)
            
            report = {
                'week_start': week_start.isoformat(), 'week_end': week_end.isoformat(),
                'total_trades': total, 'wins': wins, 'losses': losses,
                'win_rate_pct': round(win_rate, 2), 'total_pnl_pct': round(avg_pnl * total, 2),
                'failures_analyzed': len(failures), 'patterns_identified': patterns_identified,
                'recommendations': recommendations
            }
            
            session.execute(text("""
                INSERT INTO weekly_reports (week_start, week_end, total_trades, wins, losses,
                    win_rate_pct, total_pnl_pct, failures_analyzed, patterns_identified_json, recommendations_json)
                VALUES (:week_start, :week_end, :total_trades, :wins, :losses,
                    :win_rate, :total_pnl, :failures, :patterns, :recommendations)
            """), {
                'week_start': week_start.date(), 'week_end': week_end.date(),
                'total_trades': report['total_trades'], 'wins': report['wins'], 'losses': report['losses'],
                'win_rate': report['win_rate_pct'], 'total_pnl': report['total_pnl_pct'],
                'failures': report['failures_analyzed'],
                'patterns': json.dumps(report['patterns_identified']),
                'recommendations': json.dumps(report['recommendations'])
            })
            session.commit()
            return report
        except Exception as e:
            logger.error(f"Failed to generate weekly report: {e}")
            return {}
        finally:
            session.close()
    
    def _generate_recommendations(self, patterns_identified: Dict) -> List[str]:
        recommendations = []
        if not patterns_identified:
            recommendations.append("No significant failure patterns detected.")
            return recommendations
        
        if 'false_breakout' in patterns_identified:
            recommendations.append("False breakout failures detected. Require 2+ candle confirmation.")
        if 'candle_bias_conflict' in patterns_identified:
            recommendations.append("Candle-indicator conflicts detected. Prioritize candle signals.")
        if 'btc_environment_mismatch' in patterns_identified:
            recommendations.append("BTC environment mismatch detected. Avoid altcoin trades during BTC range.")
        if 'volume_weakness' in patterns_identified:
            recommendations.append("Volume weakness failures detected. Require volume confirmation.")
        
        return recommendations
    
    def close(self):
        if self.engine:
            self.engine.dispose()


_autopsy_recorder: Optional[TradeAutopsyRecorder] = None


def get_autopsy_recorder() -> TradeAutopsyRecorder:
    global _autopsy_recorder
    if _autopsy_recorder is None:
        _autopsy_recorder = TradeAutopsyRecorder()
    return _autopsy_recorder
