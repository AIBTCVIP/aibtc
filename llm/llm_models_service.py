"""
LLM 模型配置服务

提供 LLM 模型配置的 CRUD 操作和缓存管理。
从数据库读取模型参数，替代硬编码的 PROVIDER_PARAMS。

使用方法:
    from llm.llm_models_service import LLMModelsService
    
    service = LLMModelsService()
    
    # 获取模型配置
    model = service.get_model("anthropic", "claude-sonnet-4-20250514")
    
    # 获取提供商的所有模型
    models = service.get_models_by_provider("anthropic")
    
    # 获取模型参数（用于 LLM 客户端）
    params = service.get_model_params("anthropic", "claude-sonnet-4-20250514")
"""

import logging
from typing import Optional, Dict, List, Any
from dataclasses import dataclass
from datetime import date
import threading
import time

logger = logging.getLogger(__name__)


@dataclass
class LLMModel:
    """LLM 模型配置"""
    id: int
    provider: str
    model_id: str
    display_name: str
    description: Optional[str]
    
    # 默认参数
    temperature: float
    top_p: float
    max_tokens: int
    context_window: int
    
    # 能力标签
    supports_vision: bool
    supports_function_call: bool
    supports_streaming: bool
    supports_json_mode: bool
    
    # 定价
    input_price: float  # $/M tokens
    output_price: float  # $/M tokens
    
    # 状态
    is_enabled: bool
    is_recommended: bool
    display_order: int
    
    # 元数据
    release_date: Optional[date]
    deprecated_date: Optional[date]
    notes: Optional[str]
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            "id": self.id,
            "provider": self.provider,
            "model_id": self.model_id,
            "display_name": self.display_name,
            "description": self.description,
            "temperature": float(self.temperature),
            "top_p": float(self.top_p),
            "max_tokens": self.max_tokens,
            "context_window": self.context_window,
            "supports_vision": self.supports_vision,
            "supports_function_call": self.supports_function_call,
            "supports_streaming": self.supports_streaming,
            "supports_json_mode": self.supports_json_mode,
            "input_price": float(self.input_price),
            "output_price": float(self.output_price),
            "is_enabled": self.is_enabled,
            "is_recommended": self.is_recommended,
            "display_order": self.display_order,
            "release_date": self.release_date.isoformat() if self.release_date else None,
            "deprecated_date": self.deprecated_date.isoformat() if self.deprecated_date else None,
            "notes": self.notes,
        }
    
    def get_params(self) -> Dict[str, float]:
        """获取模型参数（用于 LLM 客户端）"""
        return {
            "temperature": float(self.temperature),
            "top_p": float(self.top_p),
        }


class LLMModelsService:
    """
    LLM 模型配置服务
    
    提供模型配置的读取和缓存管理。
    使用内存缓存减少数据库查询。
    """
    
    # 缓存过期时间（秒）
    CACHE_TTL = 300  # 5 分钟
    
    # 单例实例
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        """单例模式"""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
        
        self._cache: Dict[str, LLMModel] = {}  # key: "provider:model_id"
        self._provider_cache: Dict[str, List[LLMModel]] = {}  # key: provider
        self._cache_time: float = 0
        self._cache_lock = threading.Lock()
        self._initialized = True
        
        logger.info("[LLMModelsService] 初始化完成")
    
    def _get_cache_key(self, provider: str, model_id: str) -> str:
        """生成缓存键"""
        return f"{provider.lower()}:{model_id}"
    
    def _is_cache_valid(self) -> bool:
        """检查缓存是否有效"""
        return time.time() - self._cache_time < self.CACHE_TTL
    
    def _load_all_models(self) -> None:
        """从数据库加载所有模型到缓存"""
        from core.user_db import config_loader
        
        try:
            # 使用共享数据库引擎
            if hasattr(config_loader, 'engine') and config_loader.engine:
                from sqlalchemy import text
                with config_loader.engine.connect() as conn:
                    result = conn.execute(text("""
                        SELECT id, provider, model_id, display_name, description,
                               temperature, top_p, max_tokens, context_window,
                               supports_vision, supports_function_call, 
                               supports_streaming, supports_json_mode,
                               input_price, output_price,
                               is_enabled, is_recommended, display_order,
                               release_date, deprecated_date, notes
                        FROM llm_models
                        WHERE is_enabled = 1
                        ORDER BY provider, display_order, model_id
                    """))
                    rows = result.fetchall()
            else:
                # 没有数据库引擎，返回空
                logger.warning("[LLMModelsService] 数据库引擎未初始化")
                rows = []
            
            with self._cache_lock:
                self._cache.clear()
                self._provider_cache.clear()
                
                for row in rows:
                    model = LLMModel(
                        id=row[0],
                        provider=row[1],
                        model_id=row[2],
                        display_name=row[3],
                        description=row[4],
                        temperature=float(row[5]) if row[5] else 0.3,
                        top_p=float(row[6]) if row[6] else 0.9,
                        max_tokens=row[7] or 8192,
                        context_window=row[8] or 128000,
                        supports_vision=bool(row[9]),
                        supports_function_call=bool(row[10]),
                        supports_streaming=bool(row[11]),
                        supports_json_mode=bool(row[12]),
                        input_price=float(row[13]) if row[13] else 0,
                        output_price=float(row[14]) if row[14] else 0,
                        is_enabled=bool(row[15]),
                        is_recommended=bool(row[16]),
                        display_order=row[17] or 100,
                        release_date=row[18],
                        deprecated_date=row[19],
                        notes=row[20],
                    )
                    
                    # 添加到缓存
                    cache_key = self._get_cache_key(model.provider, model.model_id)
                    self._cache[cache_key] = model
                    
                    # 添加到提供商缓存
                    provider_lower = model.provider.lower()
                    if provider_lower not in self._provider_cache:
                        self._provider_cache[provider_lower] = []
                    self._provider_cache[provider_lower].append(model)
                
                self._cache_time = time.time()
                
            logger.debug(f"[LLMModelsService] 加载了 {len(self._cache)} 个模型配置")
            
        except Exception as e:
            logger.warning(f"[LLMModelsService] 加载模型配置失败: {e}，将使用默认参数")
            # 即使加载失败，也设置缓存时间，避免频繁重试
            # 使用较短的 TTL (60秒) 以便稍后重试
            with self._cache_lock:
                self._cache_time = time.time() - self.CACHE_TTL + 60
    
    def _ensure_cache(self) -> None:
        """确保缓存有效"""
        if not self._is_cache_valid():
            self._load_all_models()
    
    def get_model(self, provider: str, model_id: str) -> Optional[LLMModel]:
        """
        获取指定模型的配置
        
        Args:
            provider: 提供商名称
            model_id: 模型 ID
            
        Returns:
            LLMModel 或 None
        """
        self._ensure_cache()
        
        cache_key = self._get_cache_key(provider, model_id)
        return self._cache.get(cache_key)
    
    def get_models_by_provider(self, provider: str) -> List[LLMModel]:
        """
        获取指定提供商的所有模型
        
        Args:
            provider: 提供商名称
            
        Returns:
            模型列表
        """
        self._ensure_cache()
        
        return self._provider_cache.get(provider.lower(), [])
    
    def get_all_models(self) -> List[LLMModel]:
        """获取所有启用的模型"""
        self._ensure_cache()
        
        return list(self._cache.values())
    
    def get_all_providers(self) -> List[str]:
        """获取所有有模型的提供商"""
        self._ensure_cache()
        
        return list(self._provider_cache.keys())
    
    def get_model_params(self, provider: str, model_id: str) -> Dict[str, float]:
        """
        获取模型参数（用于 LLM 客户端）
        
        如果数据库中没有配置，返回默认参数。
        
        Args:
            provider: 提供商名称
            model_id: 模型 ID
            
        Returns:
            包含 temperature 和 top_p 的字典
        """
        model = self.get_model(provider, model_id)
        
        if model:
            return model.get_params()
        
        # 回退到默认参数
        return self._get_default_params(provider, model_id)
    
    def _get_default_params(self, provider: str, model_id: str = "") -> Dict[str, float]:
        """
        获取默认参数（当数据库中没有配置时）
        
        保持与原 llm_client.py 中 PROVIDER_PARAMS 的兼容性
        """
        # 默认参数（与原代码保持一致）
        DEFAULT_PARAMS = {
            "anthropic": {"temperature": 0.1, "top_p": 0.9},
            "openai": {"temperature": 0.3, "top_p": 0.9},
            "deepseek": {"temperature": 0.3, "top_p": 0.95},
            "openrouter": {"temperature": 0.1, "top_p": 0.9},
            "grok": {"temperature": 0.3, "top_p": 0.9},
            "gemini": {"temperature": 0.5, "top_p": 0.9},
            "custom": {"temperature": 0.5, "top_p": 0.9},
        }
        
        DEFAULT = {"temperature": 0.5, "top_p": 0.9}
        
        provider_lower = provider.lower()
        
        # 如果是 custom 且有模型名称，尝试智能推断
        if provider_lower == "custom" and model_id:
            inferred = self._infer_provider_from_model(model_id)
            if inferred != "custom":
                return DEFAULT_PARAMS.get(inferred, DEFAULT).copy()
        
        return DEFAULT_PARAMS.get(provider_lower, DEFAULT).copy()
    
    def _infer_provider_from_model(self, model: str) -> str:
        """根据模型名称推断提供商"""
        model_lower = model.lower()
        
        # 模型前缀映射
        PREFIX_MAP = {
            "claude-": "anthropic",
            "anthropic/claude": "anthropic",
            "gpt-": "openai",
            "o1": "openai",
            "o3": "openai",
            "openai/": "openai",
            "deepseek": "deepseek",
            "gemini": "gemini",
            "google/": "gemini",
            "grok": "grok",
            "x-ai/": "grok",
        }
        
        # 按前缀长度降序排序
        sorted_prefixes = sorted(PREFIX_MAP.keys(), key=len, reverse=True)
        
        for prefix in sorted_prefixes:
            if model_lower.startswith(prefix):
                return PREFIX_MAP[prefix]
        
        return "custom"
    
    def invalidate_cache(self) -> None:
        """使缓存失效（强制下次查询时重新加载）"""
        with self._cache_lock:
            self._cache_time = 0
        logger.debug("[LLMModelsService] 缓存已失效")
    
    # ========================================
    # 管理接口（用于 Admin API）
    # ========================================
    
    def add_model(self, model_data: Dict[str, Any]) -> Optional[int]:
        """
        添加新模型
        
        Args:
            model_data: 模型数据字典
            
        Returns:
            新模型的 ID，失败返回 None
        """
        from core.user_db import config_loader
        
        required_fields = ["provider", "model_id", "display_name"]
        for field in required_fields:
            if field not in model_data:
                logger.error(f"[LLMModelsService] 缺少必填字段: {field}")
                return None
        
        try:
            if hasattr(config_loader, 'engine') and config_loader.engine:
                from sqlalchemy import text
                with config_loader.engine.connect() as conn:
                    result = conn.execute(text("""
                        INSERT INTO llm_models (
                            provider, model_id, display_name, description,
                            temperature, top_p, max_tokens, context_window,
                            supports_vision, supports_function_call,
                            supports_streaming, supports_json_mode,
                            input_price, output_price,
                            is_enabled, is_recommended, display_order,
                            release_date, deprecated_date, notes
                        ) VALUES (
                            :provider, :model_id, :display_name, :description,
                            :temperature, :top_p, :max_tokens, :context_window,
                            :supports_vision, :supports_function_call,
                            :supports_streaming, :supports_json_mode,
                            :input_price, :output_price,
                            :is_enabled, :is_recommended, :display_order,
                            :release_date, :deprecated_date, :notes
                        )
                    """), {
                        "provider": model_data["provider"],
                        "model_id": model_data["model_id"],
                        "display_name": model_data["display_name"],
                        "description": model_data.get("description"),
                        "temperature": model_data.get("temperature", 0.3),
                        "top_p": model_data.get("top_p", 0.9),
                        "max_tokens": model_data.get("max_tokens", 8192),
                        "context_window": model_data.get("context_window", 128000),
                        "supports_vision": model_data.get("supports_vision", False),
                        "supports_function_call": model_data.get("supports_function_call", False),
                        "supports_streaming": model_data.get("supports_streaming", True),
                        "supports_json_mode": model_data.get("supports_json_mode", False),
                        "input_price": model_data.get("input_price", 0),
                        "output_price": model_data.get("output_price", 0),
                        "is_enabled": model_data.get("is_enabled", True),
                        "is_recommended": model_data.get("is_recommended", False),
                        "display_order": model_data.get("display_order", 100),
                        "release_date": model_data.get("release_date"),
                        "deprecated_date": model_data.get("deprecated_date"),
                        "notes": model_data.get("notes"),
                    })
                    conn.commit()
                    
                    # 获取新插入的 ID
                    result = conn.execute(text("SELECT LAST_INSERT_ID()"))
                    new_id = result.scalar()
                    
                    self.invalidate_cache()
                    logger.info(f"[LLMModelsService] 添加模型: {model_data['provider']}/{model_data['model_id']}")
                    return new_id
            else:
                logger.error("[LLMModelsService] 仅支持 MySQL")
                return None
                
        except Exception as e:
            logger.error(f"[LLMModelsService] 添加模型失败: {e}")
            return None
    
    def update_model(self, model_id: int, model_data: Dict[str, Any]) -> bool:
        """
        更新模型配置
        
        Args:
            model_id: 数据库中的模型 ID
            model_data: 要更新的字段
            
        Returns:
            是否成功
        """
        from core.user_db import config_loader
        
        if not model_data:
            return False
        
        try:
            if hasattr(config_loader, 'engine') and config_loader.engine:
                from sqlalchemy import text
                
                # 构建 UPDATE 语句
                set_clauses = []
                params = {"id": model_id}
                
                allowed_fields = [
                    "provider", "model_id", "display_name", "description",
                    "temperature", "top_p", "max_tokens", "context_window",
                    "supports_vision", "supports_function_call",
                    "supports_streaming", "supports_json_mode",
                    "input_price", "output_price",
                    "is_enabled", "is_recommended", "display_order",
                    "release_date", "deprecated_date", "notes"
                ]
                
                for field in allowed_fields:
                    if field in model_data:
                        set_clauses.append(f"{field} = :{field}")
                        params[field] = model_data[field]
                
                if not set_clauses:
                    return False
                
                sql = f"UPDATE llm_models SET {', '.join(set_clauses)} WHERE id = :id"
                
                with config_loader.engine.connect() as conn:
                    conn.execute(text(sql), params)
                    conn.commit()
                
                self.invalidate_cache()
                logger.info(f"[LLMModelsService] 更新模型 ID={model_id}")
                return True
            else:
                logger.error("[LLMModelsService] 仅支持 MySQL")
                return False
                
        except Exception as e:
            logger.error(f"[LLMModelsService] 更新模型失败: {e}")
            return False
    
    def delete_model(self, model_id: int) -> bool:
        """
        删除模型（软删除，设置 is_enabled=0）
        
        Args:
            model_id: 数据库中的模型 ID
            
        Returns:
            是否成功
        """
        return self.update_model(model_id, {"is_enabled": False})
    
    def get_model_by_id(self, model_id: int) -> Optional[Dict[str, Any]]:
        """
        根据数据库 ID 获取模型（包括禁用的）
        
        Args:
            model_id: 数据库中的模型 ID
            
        Returns:
            模型数据字典
        """
        from core.user_db import config_loader
        
        try:
            if hasattr(config_loader, 'engine') and config_loader.engine:
                from sqlalchemy import text
                with config_loader.engine.connect() as conn:
                    result = conn.execute(text("""
                        SELECT id, provider, model_id, display_name, description,
                               temperature, top_p, max_tokens, context_window,
                               supports_vision, supports_function_call, 
                               supports_streaming, supports_json_mode,
                               input_price, output_price,
                               is_enabled, is_recommended, display_order,
                               release_date, deprecated_date, notes
                        FROM llm_models
                        WHERE id = :id
                    """), {"id": model_id})
                    row = result.fetchone()
                    
                    if row:
                        return {
                            "id": row[0],
                            "provider": row[1],
                            "model_id": row[2],
                            "display_name": row[3],
                            "description": row[4],
                            "temperature": float(row[5]) if row[5] else 0.3,
                            "top_p": float(row[6]) if row[6] else 0.9,
                            "max_tokens": row[7] or 8192,
                            "context_window": row[8] or 128000,
                            "supports_vision": bool(row[9]),
                            "supports_function_call": bool(row[10]),
                            "supports_streaming": bool(row[11]),
                            "supports_json_mode": bool(row[12]),
                            "input_price": float(row[13]) if row[13] else 0,
                            "output_price": float(row[14]) if row[14] else 0,
                            "is_enabled": bool(row[15]),
                            "is_recommended": bool(row[16]),
                            "display_order": row[17] or 100,
                            "release_date": row[18].isoformat() if row[18] else None,
                            "deprecated_date": row[19].isoformat() if row[19] else None,
                            "notes": row[20],
                        }
            return None
            
        except Exception as e:
            logger.error(f"[LLMModelsService] 获取模型失败: {e}")
            return None


# 全局单例
llm_models_service = LLMModelsService()
