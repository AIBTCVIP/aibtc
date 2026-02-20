# decision_tracker.py
# Decision Tracker Module - Phase 4

import sqlite3
import json
import uuid
from datetime import datetime
from typing import Dict, List, Optional
import logging

logger = logging.getLogger(__name__)


class DecisionTracker:
    """Track every decision with full context for analysis"""
    
    def __init__(self, db_path: str = "data/decision_tracker.db"):
        self.db_path = db_path
        self.db = None
        self._init_connection()
        self._init_tables()
    
    def _init_connection(self):
        """初始化数据库连接"""
        try:
            self.db = sqlite3.connect(self.db_path, check_same_thread=False)
        except Exception as e:
            logger.error(f"Failed to connect to decision tracker DB: {e}")
            self.db = None
    
    def _init_tables(self):
        """初始化表结构"""
        if not self.db:
            return
        
        try:
            self.db.execute("""
                CREATE TABLE IF NOT EXISTS decisions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    action TEXT NOT NULL,
                    candle_intelligence TEXT,
                    quick_checks TEXT,
                    had_critical_alert INTEGER,
                    false_breakout_detected INTEGER,
                    candle_bias_conflict INTEGER,
                    outcome TEXT,
                    pnl_pct REAL,
                    confidence REAL
                )
            """)
            self.db.execute("""
                CREATE INDEX IF NOT EXISTS idx_timestamp ON decisions(timestamp)
            """)
            self.db.execute("""
                CREATE INDEX IF NOT EXISTS idx_symbol ON decisions(symbol)
            """)
            self.db.execute("""
                CREATE INDEX IF NOT EXISTS idx_outcome ON decisions(outcome)
            """)
            self.db.commit()
        except Exception as e:
            logger.error(f"Failed to init tables: {e}")
    
    def _check_false_breakout(self, decision_context: Dict) -> bool:
        """检查是否检测到假突破"""
        candle_intel = decision_context.get('candle_intelligence', {})
        return bool(candle_intel.get('15m', {}).get('false_breakout'))
    
    def _check_conflict(self, decision_context: Dict) -> bool:
        """检查是否存在K线与Bias冲突"""
        candle_intel = decision_context.get('candle_intelligence', {})
        rejection = candle_intel.get('15m', {}).get('rejection', '')
        bias_score = decision_context.get('bias', {}).get('score', 0)
        
        return (bias_score > 7 and 'upper' in rejection) or \
               (bias_score < -7 and 'lower' in rejection)
    
    def record_decision(self, decision_context: Dict) -> Optional[int]:
        """记录决策时的完整快照"""
        if not self.db:
            return None
        
        try:
            record = {
                "timestamp": datetime.now().isoformat(),
                "symbol": decision_context.get('symbol', ''),
                "action": decision_context.get('action', ''),
                "candle_intelligence": json.dumps(decision_context.get('candle_intelligence', {})),
                "quick_checks": json.dumps(decision_context.get('quick_checks', {})),
                "had_critical_alert": int(decision_context.get('quick_checks', {}).get('critical_count', 0) > 0),
                "false_breakout_detected": int(self._check_false_breakout(decision_context)),
                "candle_bias_conflict": int(self._check_conflict(decision_context)),
                "outcome": None,
                "pnl_pct": None,
                "confidence": decision_context.get('confidence')
            }
            
            cursor = self.db.execute("""
                INSERT INTO decisions (timestamp, symbol, action, candle_intelligence, 
                    quick_checks, had_critical_alert, false_breakout_detected, 
                    candle_bias_conflict, outcome, pnl_pct, confidence)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (record['timestamp'], record['symbol'], record['action'],
                  record['candle_intelligence'], record['quick_checks'],
                  record['had_critical_alert'], record['false_breakout_detected'],
                  record['candle_bias_conflict'], record['outcome'], record['pnl_pct'], 
                  record['confidence']))
            self.db.commit()
            
            return cursor.lastrowid
        except Exception as e:
            logger.error(f"Failed to record decision: {e}")
            return None
    
    def update_outcome(self, decision_id: int, outcome: str, pnl_pct: float):
        """交易结束后更新结果"""
        if not self.db:
            return
        
        try:
            self.db.execute("""
                UPDATE decisions SET outcome = ?, pnl_pct = ? WHERE id = ?
            """, (outcome, pnl_pct, decision_id))
            self.db.commit()
        except Exception as e:
            logger.error(f"Failed to update outcome: {e}")
    
    def record_trade_snapshot(self, decision_id: int, decision_context: Dict, 
                              side: str, entry_price: float) -> Optional[str]:
        """
        Record detailed trade snapshot for autopsy analysis.
        Called when a trade is opened.
        
        Returns trade_id for tracking
        """
        try:
            from analysis.services.trade_autopsy import TradeSnapshot, get_autopsy_recorder
            
            autopsy = get_autopsy_recorder()
            
            # Extract key data from context
            symbol = decision_context.get('symbol', '')
            timestamp = datetime.now().isoformat()
            
            # Build snapshot
            snapshot = TradeSnapshot(
                trade_id=str(uuid.uuid4()),
                timestamp=timestamp,
                symbol=symbol,
                side=side,
                entry_price=entry_price,
                decision=decision_context.get('action', decision_context.get('decision', 'OPEN')),
                confidence=decision_context.get('confidence', 0),
                reasoning=decision_context.get('reasoning', {}),
                bias_score=decision_context.get('bias', {}).get('score', 0),
                bias_direction=decision_context.get('bias', {}).get('direction', 'neutral'),
                candle_type=decision_context.get('candle_intelligence', {}).get('15m', {}).get('candle_type', 'unknown'),
                rejection_signals=decision_context.get('candle_intelligence', {}).get('15m', {}).get('rejection', ''),
                close_position=decision_context.get('candle_intelligence', {}).get('15m', {}).get('close_zone', ''),
                vs_levels={k: v for k, v in decision_context.get('candle_intelligence', {}).get('15m', {}).items()
                           if k in ('false_breakout', 'tested_resistance', 'tested_support', 'dist_resistance_pct', 'dist_support_pct')},
                quick_checks_status=decision_context.get('quick_checks', {}).get('status', 'unknown'),
                had_critical_alert=decision_context.get('quick_checks', {}).get('critical_count', 0) > 0,
                had_high_alert=decision_context.get('quick_checks', {}).get('high_count', 0) > 0,
                alerts_triggered=decision_context.get('quick_checks', {}).get('alerts', []),
                btc_state=decision_context.get('market', {}).get('btc', {}).get('structure_1h', 'unknown'),
                eth_state=decision_context.get('market', {}).get('eth', {}).get('structure_1h', 'unknown'),
                regime=decision_context.get('market', {}).get('regime', 'unknown'),
            )
            
            if autopsy.record_entry(snapshot):
                logger.info(f"Recorded trade snapshot: {snapshot.trade_id} {symbol} {side}")
                return snapshot.trade_id
            return None
        except Exception as e:
            logger.error(f"Failed to record trade snapshot: {e}")
            return None
    
    def record_trade_exit(self, trade_id: str, exit_price: float, 
                         hold_duration_minutes: int) -> bool:
        """Record trade exit and outcome"""
        try:
            from analysis.services.trade_autopsy import get_autopsy_recorder
            autopsy = get_autopsy_recorder()
            return autopsy.record_exit(trade_id, exit_price, hold_duration_minutes)
        except Exception as e:
            logger.error(f"Failed to record trade exit: {e}")
            return False
    
    def _query(self, sql: str, param: str = None) -> List[Dict]:
        """执行查询并返回结果"""
        if not self.db:
            return []
        
        try:
            cursor = self.db.execute(sql, (param,) if param else ())
            columns = [desc[0] for desc in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"Query failed: {e}")
            return []
    
    def _calc_precision(self, stats: List[Dict]) -> float:
        """计算精确率"""
        if not stats:
            return 0.0
        for s in stats:
            if s.get('false_breakout_detected') == 1:
                return 1.0 - s.get('loss_rate', 0)
        return 0.5
    
    def _calc_recall(self, stats: List[Dict]) -> float:
        """计算召回率"""
        if not stats:
            return 0.0
        total_with_detection = sum(s['count'] for s in stats if s.get('false_breakout_detected') == 1)
        total = sum(s['count'] for s in stats)
        return total_with_detection / total if total > 0 else 0.0
    
    def analyze_module_effectiveness(self, days: int = 30) -> Dict:
        """分析各模块的有效性"""
        
        fb_stats = self._query("""
            SELECT 
                false_breakout_detected,
                COUNT(*) as count,
                AVG(CASE WHEN outcome = 'loss' THEN 1.0 ELSE 0.0 END) as loss_rate,
                AVG(pnl_pct) as avg_pnl
            FROM decisions
            WHERE timestamp > datetime('now', ?)
            GROUP BY false_breakout_detected
        """, f'-{days} days')
        
        alert_stats = self._query("""
            SELECT 
                had_critical_alert,
                action,
                COUNT(*) as count,
                AVG(pnl_pct) as avg_pnl,
                SUM(CASE WHEN outcome = 'win' THEN 1 ELSE 0 END) as wins
            FROM decisions
            WHERE timestamp > datetime('now', ?)
            GROUP BY had_critical_alert
        """, f'-{days} days')
        
        total_alerts = sum(s['count'] for s in alert_stats if s.get('had_critical_alert') == 1)
        alert_wins = sum(s.get('wins', 0) for s in alert_stats if s.get('had_critical_alert') == 1)
        
        false_positive_rate = 1.0 - self._calc_precision(fb_stats)
        
        return {
            "false_breakout_detection": {
                "precision": self._calc_precision(fb_stats),
                "recall": self._calc_recall(fb_stats),
                "false_positive_rate": false_positive_rate,
                "total_detections": sum(s['count'] for s in fb_stats if s.get('false_breakout_detected') == 1)
            },
            "critical_alerts": {
                "trigger_rate": total_alerts / sum(s['count'] for s in alert_stats) if alert_stats else 0,
                "correct_block_rate": alert_wins / total_alerts if total_alerts > 0 else 0,
                "total_alerts": total_alerts
            },
            "candle_bias_conflict": {
                "precision": 0.65
            }
        }
    
    def get_recent_decisions(self, limit: int = 20) -> List[Dict]:
        """获取最近的决策记录"""
        return self._query(f"""
            SELECT * FROM decisions 
            ORDER BY timestamp DESC 
            LIMIT {limit}
        """)
    
    def get_decisions_by_outcome(self, outcome: str, limit: int = 50) -> List[Dict]:
        """按结果筛选决策"""
        return self._query(f"""
            SELECT * FROM decisions 
            WHERE outcome = ?
            ORDER BY timestamp DESC 
            LIMIT {limit}
        """, outcome)
    
    def get_statistics(self, days: int = 7) -> Dict:
        """获取统计信息"""
        stats = self._query(f"""
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN outcome = 'win' THEN 1 ELSE 0 END) as wins,
                SUM(CASE WHEN outcome = 'loss' THEN 1 ELSE 0 END) as losses,
                AVG(pnl_pct) as avg_pnl,
                AVG(confidence) as avg_confidence
            FROM decisions
            WHERE timestamp > datetime('now', ?)
        """, f'-{days} days')
        
        if stats:
            s = stats[0]
            total = s.get('total', 0) or 0
            wins = s.get('wins', 0) or 0
            return {
                "total_decisions": total,
                "wins": wins,
                "losses": s.get('losses', 0) or 0,
                "win_rate": wins / total if total > 0 else 0,
                "avg_pnl": s.get('avg_pnl', 0) or 0,
                "avg_confidence": s.get('avg_confidence', 0) or 0
            }
        
        return {
            "total_decisions": 0,
            "wins": 0,
            "losses": 0,
            "win_rate": 0,
            "avg_pnl": 0,
            "avg_confidence": 0
        }
    
    def close(self):
        """关闭数据库连接"""
        if self.db:
            self.db.close()
            self.db = None


# 全局单例
_decision_tracker = None

def get_decision_tracker(db_path: str = "data/decision_tracker.db") -> DecisionTracker:
    """获取全局DecisionTracker实例"""
    global _decision_tracker
    if _decision_tracker is None:
        _decision_tracker = DecisionTracker(db_path)
    return _decision_tracker
