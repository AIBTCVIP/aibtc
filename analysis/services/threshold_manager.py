# threshold_manager.py
# 阈值管理器 - 管理各模块的动态阈值

import yaml
from typing import Dict, Optional, Any
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)


class ConfigManager:
    """配置管理器 - 处理自动调参与人工冲突"""
    
    def __init__(self, config_path: str = "config/optimization_config.yaml"):
        self.config_path = config_path
        self.config = self._load_config()
        self.manual_overrides = set()
        self.adjustment_log = []
    
    def _load_config(self) -> Dict:
        """加载配置文件"""
        try:
            with open(self.config_path, 'r') as f:
                return yaml.safe_load(f) or {}
        except FileNotFoundError:
            logger.warning(f"Config file not found: {self.config_path}, using defaults")
            return self._default_config()
        except Exception as e:
            logger.error(f"Failed to load config: {e}")
            return self._default_config()
    
    def _default_config(self) -> Dict:
        """默认配置"""
        return {
            "thresholds": {
                "false_breakout_confidence": 0.7,
                "bias_conflict_score": 7,
                "rejection_threshold_multiplier": 1.5,
                "volume_weak_threshold": 1.2,
                "close_position_threshold": 0.4
            },
            "modules": {
                "candle_intelligence": {"enabled": True, "timeout_ms": 200},
                "quick_checks": {"enabled": True, "timeout_ms": 100}
            },
            "alert_manager": {
                "max_alerts_per_type": 2,
                "max_total_alerts": 5,
                "min_accuracy_threshold": 0.6
            },
            "auto_tune": {
                "enabled": False,
                "target_fp_rate": 0.15,
                "target_recall": 0.75
            }
        }
    
    def get_threshold(self, key: str) -> float:
        """获取阈值"""
        return self.config.get('thresholds', {}).get(key)
    
    def set_threshold(self, key: str, value: float, source: str = "manual"):
        """设置阈值"""
        if 'thresholds' not in self.config:
            self.config['thresholds'] = {}
        
        self.config['thresholds'][key] = value
        
        if source == "manual":
            self.manual_overrides.add(key)
        
        self.adjustment_log.append({
            'key': key,
            'value': value,
            'source': source,
            'timestamp': datetime.now().isoformat()
        })
        
        logger.info(f"Threshold {key} set to {value} (source: {source})")
    
    def save_config(self):
        """保存配置到文件"""
        try:
            with open(self.config_path, 'w') as f:
                yaml.dump(self.config, f, default_flow_style=False)
            logger.info(f"Config saved to {self.config_path}")
        except Exception as e:
            logger.error(f"Failed to save config: {e}")
    
    def get_module_config(self, module_name: str) -> Dict:
        """获取模块配置"""
        return self.config.get('modules', {}).get(module_name, {})
    
    def set_module_enabled(self, module_name: str, enabled: bool):
        """设置模块启用状态"""
        if 'modules' not in self.config:
            self.config['modules'] = {}
        if module_name not in self.config['modules']:
            self.config['modules'][module_name] = {}
        self.config['modules'][module_name]['enabled'] = enabled
    
    def is_module_enabled(self, module_name: str) -> bool:
        """检查模块是否启用"""
        return self.config.get('modules', {}).get(module_name, {}).get('enabled', True)
    
    def get_auto_tune_config(self) -> Dict:
        """获取自动调参配置"""
        return self.config.get('auto_tune', {})
    
    def is_auto_tune_enabled(self) -> bool:
        """检查自动调参是否启用"""
        return self.config.get('auto_tune', {}).get('enabled', False)
    
    def get_adjustment_log(self) -> list:
        """获取调整日志"""
        return self.adjustment_log


class ThresholdManager:
    """阈值管理器 - 缓存和计算动态阈值"""
    
    def __init__(self, config_manager: Optional[ConfigManager] = None):
        self.config = config_manager or ConfigManager()
        self.thresholds_cache = {}
        self.cache_ttl = timedelta(hours=1)
    
    def get_thresholds(self, symbol: str, historical_data: Any = None, 
                      correlation_data: Optional[Dict] = None) -> Dict:
        """获取阈值配置"""
        cache_key = symbol
        
        if cache_key in self.thresholds_cache:
            cache = self.thresholds_cache[cache_key]
            if datetime.now() - cache['updated_at'] < self.cache_ttl:
                return cache['thresholds']
        
        thresholds = {
            'false_breakout_confidence': self.config.get_threshold('false_breakout_confidence') or 0.7,
            'bias_conflict_score': self.config.get_threshold('bias_conflict_score') or 7,
            'rejection_threshold_multiplier': self.config.get_threshold('rejection_threshold_multiplier') or 1.5,
            'volume_weak_threshold': self.config.get_threshold('volume_weak_threshold') or 1.2,
            'close_position_threshold': self.config.get_threshold('close_position_threshold') or 0.4
        }
        
        self.thresholds_cache[cache_key] = {
            'thresholds': thresholds,
            'updated_at': datetime.now()
        }
        
        return thresholds
    
    def get_fallback_thresholds(self) -> Dict:
        """获取默认阈值（用于无历史数据的情况）"""
        return {
            'false_breakout_confidence': 0.75,
            'bias_conflict_score': 8,
            'rejection_threshold_multiplier': 2.0,
            'volume_weak_threshold': 1.2,
            'close_position_threshold': 0.4
        }
    
    def clear_cache(self, symbol: Optional[str] = None):
        """清除缓存"""
        if symbol:
            self.thresholds_cache.pop(symbol, None)
        else:
            self.thresholds_cache.clear()
    
    def get_cache_stats(self) -> Dict:
        """获取缓存统计"""
        return {
            'cached_symbols': len(self.thresholds_cache),
            'cache_ttl_seconds': self.cache_ttl.total_seconds()
        }


# 全局单例
_config_manager = None
_threshold_manager = None

def get_config_manager(config_path: str = "config/optimization_config.yaml") -> ConfigManager:
    """获取全局ConfigManager实例"""
    global _config_manager
    if _config_manager is None:
        _config_manager = ConfigManager(config_path)
    return _config_manager


def get_threshold_manager(config_manager: Optional[ConfigManager] = None) -> ThresholdManager:
    """获取全局ThresholdManager实例"""
    global _threshold_manager
    if _threshold_manager is None:
        _threshold_manager = ThresholdManager(config_manager)
    return _threshold_manager
