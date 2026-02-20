# quick_checker.py
# 快速异常检测模块 - Phase 2

from typing import List, Dict, Optional
from enum import Enum
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)


class Severity(Enum):
    CRITICAL = "critical"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


class AlertCooldown:
    """Alert冷却机制 - 基于时间而非bar数"""
    
    def __init__(self):
        self.recent_alerts = {}
        self.cooldown_periods = {
            'false_breakout': 30,
            'candle_bias_conflict': 15,
            'btc_environment': 60
        }
    
    def should_trigger(self, symbol: str, alert_type: str) -> bool:
        key = (symbol, alert_type)
        last_time = self.recent_alerts.get(key, datetime.min)
        cooldown_minutes = self.cooldown_periods.get(alert_type, 15)
        
        elapsed = (datetime.now() - last_time).total_seconds()
        if elapsed < cooldown_minutes * 60:
            remaining = int((cooldown_minutes * 60 - elapsed) / 60)
            logger.debug(f"Alert {alert_type} for {symbol} in cooldown, {remaining}min remaining")
            return False
        
        self.recent_alerts[key] = datetime.now()
        return True
    
    def get_cooldown_status(self, symbol: str, alert_type: str) -> Dict:
        key = (symbol, alert_type)
        last_time = self.recent_alerts.get(key)
        if not last_time:
            return {"in_cooldown": False}
        
        cooldown_minutes = self.cooldown_periods.get(alert_type, 15)
        elapsed = (datetime.now() - last_time).total_seconds()
        remaining = max(0, cooldown_minutes * 60 - elapsed)
        
        return {
            "in_cooldown": remaining > 0,
            "remaining_seconds": int(remaining)
        }


class QuickChecker:
    """
    快速异常检测器
    只检测最致命的问题,保证速度
    预计耗时: 0.05-0.1秒
    """
    
    def __init__(self, config: Optional[Dict] = None):
        self.config = config or {}
        self.enabled = self.config.get('enabled', True)
        self.timeout_ms = self.config.get('timeout_ms', 100)
        
        self.rules_config = self.config.get('rules', {
            'candle_bias_conflict': True,
            'false_breakout': True,
            'btc_environment': True
        })
        
        self.cooldown = AlertCooldown()
    
    def check(self, symbols_data: Dict, market_data: Dict) -> Dict:
        """
        执行快速检查
        """
        if not self.enabled:
            return self._empty_result()
        
        alerts = []
        
        for symbol, data in symbols_data.items():
            if not data:
                continue
            
            if self.rules_config.get('candle_bias_conflict', True):
                alerts.extend(self._check_candle_bias_conflict(symbol, data))
            
            if self.rules_config.get('false_breakout', True):
                alerts.extend(self._check_false_breakout(symbol, data))
            
            if self.rules_config.get('btc_environment', True) and symbol not in ['BTCUSDT', 'ETHUSDT']:
                alerts.extend(self._check_btc_environment(symbol, data, market_data))
        
        return self._format_output(alerts)
    
    def _check_candle_bias_conflict(self, symbol: str, data: Dict) -> List[Dict]:
        """Detect conflict between candle and indicator"""
        alerts = []
        
        bias_score = data.get('bias', {}).get('score', 0)
        candle_intel = data.get('candle_intelligence', {})
        c15 = candle_intel.get('15m', {})
        rejection_15m = c15.get('rejection', '')  # now a string like "upper_strong"
        rej_strength = c15.get('rejection_strength', '')
        
        # Rule 1: Strong bullish bias but upper wick rejection
        if bias_score > 7 and 'upper' in rejection_15m:
            if self.cooldown.should_trigger(symbol, 'candle_bias_conflict'):
                alerts.append({
                    "symbol": symbol,
                    "type": "candle_bias_conflict",
                    "severity": Severity.HIGH,
                    "message": f"Strong bullish bias (score={bias_score}) but 15m candle shows upper wick rejection",
                    "evidence": {
                        "bias_score": bias_score,
                        "rejection": rejection_15m,
                        "rejection_strength": rej_strength
                    },
                    "recommendation": "Reduce long confidence by 50%, wait for next candle or skip trade"
                })
        
        # Rule 2: Strong bearish bias but lower wick support
        if bias_score < -7 and 'lower' in rejection_15m:
            if self.cooldown.should_trigger(symbol, 'candle_bias_conflict'):
                alerts.append({
                    "symbol": symbol,
                    "type": "candle_bias_conflict",
                    "severity": Severity.HIGH,
                    "message": f"Strong bearish bias (score={bias_score}) but 15m candle shows lower wick support",
                    "evidence": {
                        "bias_score": bias_score,
                        "rejection": rejection_15m,
                        "rejection_strength": rej_strength
                    },
                    "recommendation": "Reduce short confidence by 50%, wait for next candle or skip trade"
                })
        
        return alerts
    
    def _check_false_breakout(self, symbol: str, data: Dict) -> List[Dict]:
        """Detect false breakout"""
        alerts = []
        
        candle_intel = data.get('candle_intelligence', {})
        candle_15m = candle_intel.get('15m', {})
        
        fb_type = candle_15m.get('false_breakout')
        if fb_type:
            if self.cooldown.should_trigger(symbol, 'false_breakout'):
                alerts.append({
                    "symbol": symbol,
                    "type": "false_breakout",
                    "severity": Severity.CRITICAL,
                    "message": f"False breakout detected: {fb_type}",
                    "evidence": {k: v for k, v in {
                        "tested_resistance": candle_15m.get('tested_resistance'),
                        "tested_support": candle_15m.get('tested_support'),
                        "false_breakout": fb_type
                    }.items() if v is not None},
                    "recommendation": "False breakout historical success rate <15%, strongly recommend waiting for next candle confirmation"
                })
        
        return alerts
    
    def _check_btc_environment(self, symbol: str, data: Dict, market_data: Dict) -> List[Dict]:
        """Check if BTC environment supports altcoin trading"""
        alerts = []
        
        btc_state = market_data.get('btc', {}).get('structure', {}).get('state_4h', '')
        pattern_name = data.get('pattern', {}).get('pattern_name', '')
        
        if btc_state in ['ranging', 'range'] and pattern_name in ['breakout_up', 'breakout_down']:
            if self.cooldown.should_trigger(symbol, 'btc_environment'):
                alerts.append({
                    "symbol": symbol,
                    "type": "btc_environment_mismatch",
                    "severity": Severity.MEDIUM,
                    "message": f"BTC in {btc_state}, {symbol} shows {pattern_name} signal",
                    "evidence": {
                        "btc_state": btc_state,
                        "altcoin_signal": pattern_name,
                        "historical_success_rate": 0.18
                    },
                    "recommendation": "Historical data shows only 18% success rate for altcoin breakout during BTC range, recommend waiting for BTC direction"
                })
        
        return alerts
    
    def _format_output(self, alerts: List[Dict]) -> Dict:
        """格式化输出"""
        if not alerts:
            return {
                "status": "all_clear",
                "message": "No anomalies detected, safe to proceed",
                "critical_count": 0,
                "high_count": 0,
                "medium_count": 0,
                "total_count": 0,
                "alerts": []
            }
        
        for alert in alerts:
            alert['severity'] = alert['severity'].value if isinstance(alert['severity'], Severity) else alert['severity']
        
        severity_order = {'critical': 3, 'high': 2, 'medium': 1, 'low': 0}
        sorted_alerts = sorted(
            alerts, 
            key=lambda x: severity_order.get(x.get('severity', 'low'), 0),
            reverse=True
        )
        
        return {
            "status": "alerts_found",
            "critical_count": sum(1 for a in alerts if a.get('severity') == 'critical'),
            "high_count": sum(1 for a in alerts if a.get('severity') == 'high'),
            "medium_count": sum(1 for a in alerts if a.get('severity') == 'medium'),
            "total_count": len(alerts),
            "alerts": sorted_alerts[:10]
        }
    
    def _empty_result(self) -> Dict:
        return {
            "status": "all_clear",
            "message": "QuickChecker disabled",
            "alerts": [],
            "critical_count": 0,
            "high_count": 0,
            "medium_count": 0,
            "total_count": 0
        }


class AlertManager:
    """Alert管理器 - 处理警告疲劳问题"""
    
    def __init__(self, config: Optional[Dict] = None):
        self.config = config or {}
        self.alert_history = {}
        self.max_alerts_per_type = self.config.get('max_alerts_per_type', 2)
        self.max_total_alerts = self.config.get('max_total_alerts', 5)
        self.min_accuracy_threshold = self.config.get('min_accuracy_threshold', 0.6)
    
    def process_alerts(self, raw_alerts: List[Dict]) -> Dict:
        if not raw_alerts:
            return {
                "alerts": [],
                "suppressed_count": 0,
                "action_required": False
            }
        
        weighted_alerts = self._weight_by_accuracy(raw_alerts)
        deduplicated = self._deduplicate(weighted_alerts, max_per_type=self.max_alerts_per_type)
        top_alerts = sorted(deduplicated, 
                          key=lambda x: x.get('effective_severity', 0), 
                          reverse=True)[:self.max_total_alerts]
        
        for alert in top_alerts:
            if alert.get('historical_accuracy', 1) < self.min_accuracy_threshold:
                if alert.get('severity') == 'critical':
                    alert['severity'] = 'high'
                    alert['note'] = f"降级：历史准确率{alert.get('historical_accuracy', 0):.0%}"
        
        return {
            "alerts": top_alerts,
            "suppressed_count": len(raw_alerts) - len(top_alerts),
            "action_required": any(a.get('severity') == 'critical' for a in top_alerts)
        }
    
    def _weight_by_accuracy(self, alerts: List[Dict]) -> List[Dict]:
        for alert in alerts:
            alert_type = alert.get('type', 'unknown')
            accuracy = self.alert_history.get(alert_type, {}).get('accuracy', 0.5)
            alert['historical_accuracy'] = accuracy
            
            severity_score = {'critical': 3, 'high': 2, 'medium': 1}.get(alert.get('severity', 'low'), 0)
            alert['effective_severity'] = severity_score * accuracy
        
        return alerts
    
    def _deduplicate(self, alerts: List[Dict], max_per_type: int) -> List[Dict]:
        type_counts = {}
        result = []
        
        for alert in alerts:
            alert_type = alert.get('type', 'unknown')
            type_counts[alert_type] = type_counts.get(alert_type, 0) + 1
            
            if type_counts[alert_type] <= max_per_type:
                result.append(alert)
        
        return result
    
    def record_outcome(self, alert_type: str, was_correct: bool):
        """记录alert结果，用于更新准确率"""
        if alert_type not in self.alert_history:
            self.alert_history[alert_type] = {'correct': 0, 'total': 0}
        
        self.alert_history[alert_type]['total'] += 1
        if was_correct:
            self.alert_history[alert_type]['correct'] += 1
        
        h = self.alert_history[alert_type]
        h['accuracy'] = h['correct'] / h['total'] if h['total'] > 0 else 0.5
    
    def get_accuracy(self, alert_type: str) -> float:
        return self.alert_history.get(alert_type, {}).get('accuracy', 0.5)


# Global singleton
_quick_checker = None

def get_quick_checker(config: Optional[Dict] = None) -> QuickChecker:
    """Get global QuickChecker instance"""
    global _quick_checker
    if _quick_checker is None:
        _quick_checker = QuickChecker(config)
    return _quick_checker


_alert_manager = None

def get_alert_manager(config: Optional[Dict] = None) -> AlertManager:
    """Get global AlertManager instance"""
    global _alert_manager
    if _alert_manager is None:
        _alert_manager = AlertManager(config)
    return _alert_manager


# ================== Failure Pattern Matcher ==================

def check_failure_patterns(symbol_data: Dict, market_data: Dict) -> Dict:
    """
    Check current context against known failure patterns.
    Returns matched patterns with risk warnings.
    """
    try:
        from analysis.services.trade_autopsy import get_autopsy_recorder
        
        autopsy = get_autopsy_recorder()
        
        # Build context for pattern matching
        context = {
            'candle_intelligence': symbol_data.get('candle_intelligence', {}),
            'quick_checks': symbol_data.get('quick_checks', {}),
            'bias': symbol_data.get('bias', {}),
            'pattern': symbol_data.get('pattern', {}),
            'market': market_data
        }
        
        # Check against patterns
        matched_patterns = autopsy.check_context_against_patterns(context)
        
        if matched_patterns:
            return {
                'status': 'patterns_matched',
                'matched_count': len(matched_patterns),
                'patterns': matched_patterns[:5],  # Top 5
                'highest_risk': max(matched_patterns, key=lambda x: x.get('win_rate_pct', 100)) if matched_patterns else None
            }
        
        return {
            'status': 'no_match',
            'matched_count': 0,
            'patterns': []
        }
    except Exception as e:
        logger.error(f"Failed to check failure patterns: {e}")
        return {
            'status': 'error',
            'error': str(e)
        }
