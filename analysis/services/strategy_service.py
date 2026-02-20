# analysis/strategy_service.py
"""
LLM 策略服务

负责从数据库加载用户策略，组装成完整的 system prompt

v5.0 架构：
- 策略配置存储在 user_ai_strategies 表中（strategy_preset + strategy_overrides）
- 每个用户可以有多个策略，每个策略独立配置 LLM 和交易策略
- 预设模板存储在 strategy_templates 表中

策略分类（按顺序组装）：
1. role - 角色定义
2. risk_rules - 风险规则
3. entry_conditions - 入场条件
4. exit_conditions - 出场条件
5. position_sizing - 仓位管理
6. market_preferences - 市场偏好
7. adaptive_rules - 适应规则
"""

import json
import logging
import threading
from typing import Optional, Dict, List, Any
from dataclasses import dataclass

from llm.prompt_templates import (
    STRATEGY_CATEGORIES,
    CATEGORY_TITLES,
    assemble_user_strategy,
    build_system_prompt,
)

logger = logging.getLogger(__name__)


@dataclass
class StrategyTemplate:
    """策略模板"""
    preset_name: str
    category: str
    content: str
    display_order: int = 0
    description: str = ""


@dataclass
class AIStrategy:
    """用户 AI 策略配置"""
    uid: str
    strategy_id: str
    name: str
    # LLM 配置
    llm_provider: str = "anthropic"
    llm_model: str = ""
    llm_api_key: Optional[str] = None
    llm_base_url: Optional[str] = None
    # 策略配置
    strategy_preset: Optional[str] = None
    strategy_overrides: Optional[Dict[str, str]] = None


class StrategyService:
    """
    策略服务
    
    负责从数据库加载策略并组装
    """
    
    def __init__(self):
        """初始化策略服务"""
        self._preset_cache: Dict[str, Dict[str, str]] = {}  # {preset_name: {category: content}}
    
    def _get_config_loader(self):
        """获取配置加载器"""
        from core.user_db import config_loader
        return config_loader
    
    def load_preset_templates(self, preset_name: str) -> Dict[str, str]:
        """
        加载预设策略模板
        
        Args:
            preset_name: 预设名称 (conservative/aggressive/trend_following/mean_reversion)
        
        Returns:
            分类内容字典 {category: content}
        """
        # 检查缓存
        if preset_name in self._preset_cache:
            return self._preset_cache[preset_name]
        
        config_loader = self._get_config_loader()
        templates = config_loader.get_strategy_templates(preset_name)
        
        result = {}
        for t in templates:
            result[t["category"]] = t["content"]
        
        # 缓存结果
        self._preset_cache[preset_name] = result
        return result
    
    def load_ai_strategy(self, uid: str, strategy_id: str) -> Optional[AIStrategy]:
        """
        加载用户 AI 策略
        
        Args:
            uid: 用户 ID
            strategy_id: 策略 ID
        
        Returns:
            AI 策略配置
        """
        config_loader = self._get_config_loader()
        data = config_loader.get_ai_strategy_with_preset(uid, strategy_id)
        
        if not data:
            return None
        
        # 解析 strategy_overrides
        overrides = data.get("strategy_overrides")
        if overrides and isinstance(overrides, str):
            try:
                overrides = json.loads(overrides)
            except json.JSONDecodeError:
                overrides = None
        
        return AIStrategy(
            uid=uid,
            strategy_id=data.get("strategy_id", strategy_id),
            name=data.get("name", ""),
            llm_provider=data.get("llm_provider", "anthropic"),
            llm_model=data.get("llm_model", ""),
            llm_api_key=data.get("llm_api_key"),
            llm_base_url=data.get("llm_base_url"),
            strategy_preset=data.get("strategy_preset"),
            strategy_overrides=overrides
        )
    
    def build_user_strategy(self, uid: str, strategy_id: str) -> Optional[str]:
        """
        构建用户策略文本
        
        Args:
            uid: 用户 ID
            strategy_id: 策略 ID
        
        Returns:
            组装后的用户策略文本
        """
        strategy = self.load_ai_strategy(uid, strategy_id)
        
        if not strategy:
            return None
        
        # 加载预设模板
        category_contents = {}
        if strategy.strategy_preset:
            category_contents = self.load_preset_templates(strategy.strategy_preset)
        
        # 应用自定义覆盖
        if strategy.strategy_overrides:
            for category, content in strategy.strategy_overrides.items():
                if content and content.strip():
                    category_contents[category] = content
        
        # 如果没有任何内容，返回 None
        if not category_contents:
            return None
        
        # 组装策略
        return assemble_user_strategy(category_contents)
    
    def build_prompt_for_strategy(self, uid: str, strategy_id: str) -> str:
        """
        为指定策略构建完整的 system prompt
        
        Args:
            uid: 用户 ID
            strategy_id: 策略 ID
        
        Returns:
            完整的 system prompt
        """
        user_strategy = self.build_user_strategy(uid, strategy_id)
        return build_system_prompt(user_strategy)
    
    def list_presets(self) -> List[Dict[str, Any]]:
        """
        列出所有可用的预设策略
        
        Returns:
            预设列表 [{name, description, category_count}]
        """
        config_loader = self._get_config_loader()
        return config_loader.list_strategy_presets()
    
    def get_preset_detail(self, preset_name: str) -> Optional[Dict[str, Any]]:
        """
        获取预设策略详情
        
        Args:
            preset_name: 预设名称
        
        Returns:
            预设详情 {name, categories: {category: content}}
        """
        templates = self.load_preset_templates(preset_name)
        if not templates:
            return None
        
        return {
            "name": preset_name,
            "categories": templates
        }
    
    def clear_cache(self):
        """清除预设缓存"""
        self._preset_cache.clear()


# 全局服务实例
_strategy_service: Optional[StrategyService] = None
_strategy_service_lock = threading.Lock()


def get_strategy_service() -> StrategyService:
    """获取全局策略服务实例（线程安全）"""
    global _strategy_service
    if _strategy_service is None:
        with _strategy_service_lock:
            if _strategy_service is None:  # Double-check locking
                _strategy_service = StrategyService()
    return _strategy_service


def build_user_strategy(uid: str, strategy_id: str) -> Optional[str]:
    """
    便捷函数：构建用户策略
    
    Args:
        uid: 用户 ID
        strategy_id: 策略 ID
    
    Returns:
        用户策略文本
    """
    service = get_strategy_service()
    return service.build_user_strategy(uid, strategy_id)


def build_prompt_for_strategy(uid: str, strategy_id: str) -> str:
    """
    便捷函数：为策略构建完整 prompt
    
    Args:
        uid: 用户 ID
        strategy_id: 策略 ID
    
    Returns:
        完整的 system prompt
    """
    service = get_strategy_service()
    return service.build_prompt_for_strategy(uid, strategy_id)
