# llm_client.py
"""
统一 LLM 客户端模块

支持多种大模型提供商：
- Anthropic (Claude)
- OpenAI (GPT)
- DeepSeek
- OpenRouter (聚合)
- 自定义兼容 OpenAI API 的服务

每个用户可以配置自己的：
- 模型提供商
- 具体模型
- API Key
- System Prompt

重试机制：
- 网络错误（Broken pipe, Connection reset 等）自动重试
- 使用指数退避策略
- 默认最多重试 3 次
"""

import os
import asyncio
import aiohttp
from typing import Optional, Dict, Any, List, Tuple, Type
from dataclasses import dataclass
from abc import ABC, abstractmethod
import logging

from llm.response_parser import (
    extract_openai_content,
    extract_anthropic_content,
    parse_trading_signals,
)

logger = logging.getLogger(__name__)


# ============================================================
# 可重试的异常类型
# ============================================================
# 这些异常通常是临时性的网络问题，可以通过重试解决
# 注意：HTTP 响应错误（如 502）会在 chat() 方法中转换为 LLMAPIError，
#       然后通过 RETRYABLE_HTTP_STATUS_CODES 判断是否重试
RETRYABLE_EXCEPTIONS: Tuple[Type[Exception], ...] = (
    aiohttp.ClientOSError,           # 包括 Broken pipe, Connection reset
    aiohttp.ServerDisconnectedError, # 服务器断开连接
    aiohttp.ClientConnectionError,   # 连接错误
    asyncio.TimeoutError,            # 超时
    ConnectionResetError,            # 连接重置
    BrokenPipeError,                 # 管道断开
    ConnectionRefusedError,          # 连接被拒绝
    ConnectionAbortedError,          # 连接被中止
)

# 可重试的 HTTP 状态码（服务器临时错误）
RETRYABLE_HTTP_STATUS_CODES = {
    408,  # Request Timeout
    429,  # Too Many Requests (Rate Limit)
    500,  # Internal Server Error
    502,  # Bad Gateway
    503,  # Service Unavailable
    504,  # Gateway Timeout
}


# ============================================================
# 模型参数配置（按提供商/系列）
# ============================================================
# 每个提供商使用统一的 temperature 和 top_p 参数
# 交易决策场景需要较低的随机性
#
# 注意：这些硬编码参数仅作为后备默认值
# 优先从数据库 llm_models 表读取配置（通过 LLMModelsService）

PROVIDER_PARAMS: Dict[str, Dict[str, float]] = {
    "anthropic": {"temperature": 0.1, "top_p": 0.9},
    "openai": {"temperature": 0.3, "top_p": 0.9},
    "deepseek": {"temperature": 0.3, "top_p": 0.95},
    "openrouter": {"temperature": 0.1, "top_p": 0.9},
    "grok": {"temperature": 0.3, "top_p": 0.9},
    "gemini": {"temperature": 0.5, "top_p": 0.9},
    "custom": {"temperature": 0.5, "top_p": 0.9},
}

# 默认参数
DEFAULT_PROVIDER_PARAMS = {"temperature": 0.5, "top_p": 0.9}

# 延迟导入 LLMModelsService（避免循环导入）
_llm_models_service = None

def _get_llm_models_service():
    """延迟获取 LLMModelsService 单例"""
    global _llm_models_service
    if _llm_models_service is None:
        try:
            from llm.llm_models_service import llm_models_service
            _llm_models_service = llm_models_service
        except Exception as e:
            logger.debug(f"无法加载 LLMModelsService: {e}")
    return _llm_models_service

# 模型名称前缀到提供商的映射（用于 custom 中转时智能识别）
MODEL_PREFIX_TO_PROVIDER: Dict[str, str] = {
    # Anthropic Claude 系列
    "claude-": "anthropic",
    "anthropic/claude": "anthropic",
    
    # OpenAI GPT 系列
    "gpt-": "openai",
    "o1": "openai",
    "o3": "openai",
    "openai/": "openai",
    
    # DeepSeek 系列
    "deepseek": "deepseek",
    
    # Google Gemini 系列
    "gemini": "gemini",
    "google/": "gemini",
    
    # Grok (xAI) 系列
    "grok": "grok",
    "x-ai/": "grok",
    
    # 其他常见中转模型
    "llama": "custom",
    "mistral": "custom",
    "qwen": "custom",
    "yi-": "custom",
}


def infer_provider_from_model(model: str) -> str:
    """
    根据模型名称推断提供商
    
    用于 custom 中转场景，根据模型名称智能选择参数配置
    
    Args:
        model: 模型名称（如 "claude-3-opus", "gpt-4o", "deepseek-chat" 等）
    
    Returns:
        推断的提供商名称，如果无法识别则返回 "custom"
    """
    model_lower = model.lower()
    
    # 按前缀长度降序排序，优先匹配更具体的前缀
    sorted_prefixes = sorted(MODEL_PREFIX_TO_PROVIDER.keys(), key=len, reverse=True)
    
    for prefix in sorted_prefixes:
        if model_lower.startswith(prefix):
            return MODEL_PREFIX_TO_PROVIDER[prefix]
    
    return "custom"


def get_provider_params(provider: str, model: str = "") -> Dict[str, float]:
    """
    获取提供商的推荐参数
    
    优先级：
    1. 数据库 llm_models 表中的模型配置
    2. 硬编码的 PROVIDER_PARAMS 默认值
    
    Args:
        provider: 提供商名称（如 "anthropic", "openai", "deepseek" 等）
        model: 模型名称（可选，用于从数据库查询或 custom 提供商时智能推断）
    
    Returns:
        包含 temperature 和 top_p 的字典
    """
    provider_lower = provider.lower()
    
    # 1. 尝试从数据库获取模型配置
    if model:
        service = _get_llm_models_service()
        if service:
            try:
                params = service.get_model_params(provider_lower, model)
                if params:
                    logger.debug(f"从数据库获取模型参数: {provider}/{model} -> {params}")
                    return params
            except Exception as e:
                logger.debug(f"从数据库获取模型参数失败: {e}")
    
    # 2. 回退到硬编码默认值
    # 如果是 custom 且提供了模型名称，尝试智能推断
    if provider_lower == "custom" and model:
        inferred = infer_provider_from_model(model)
        if inferred != "custom":
            return PROVIDER_PARAMS.get(inferred, DEFAULT_PROVIDER_PARAMS).copy()
    
    return PROVIDER_PARAMS.get(provider_lower, DEFAULT_PROVIDER_PARAMS).copy()


# ============================================================
# 自定义异常类
# ============================================================
class LLMAPIError(Exception):
    """
    LLM API 调用错误

    携带详细的错误信息，便于保存到 Redis 供用户查看
    """

    def __init__(
            self,
            message: str,
            http_status: int = 0,
            error_data: Dict[str, Any] = None,
            provider: str = "",
            model: str = "",
            error_code: str = "",
    ):
        super().__init__(message)
        self.http_status = http_status
        self.error_data = error_data or {}  # API 返回的原始错误 JSON
        self.provider = provider
        self.model = model
        self.error_code = error_code  # API 特定的错误码（如 rate_limit_exceeded）

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典，便于保存到 Redis"""
        return {
            "message": str(self),
            "http_status": self.http_status,
            "error_data": self.error_data,
            "provider": self.provider,
            "model": self.model,
            "error_code": self.error_code,
        }

    @classmethod
    def from_response(
            cls,
            provider: str,
            model: str,
            http_status: int,
            response_data: Dict[str, Any],
    ) -> "LLMAPIError":
        """
        从 API 响应构造错误

        自动解析不同提供商的错误格式
        """
        error_code = ""
        message = f"{provider} API error (HTTP {http_status})"

        # Anthropic/OpenAI 格式: {"error": {"type/code": "...", "message": "..."}}
        # 两者格式相似，统一处理
        if "error" in response_data and isinstance(response_data["error"], dict):
            err = response_data["error"]
            # OpenAI 用 "code" (更具体), Anthropic 用 "type"
            # 优先使用 code，因为它更具体（如 invalid_api_key vs invalid_request_error）
            error_code = err.get("code", "") or err.get("type", "")
            err_msg = err.get("message", "")
            if err_msg:
                message = f"{provider}: {err_msg}"

        # 简单格式: {"message": "..."}
        elif "message" in response_data:
            message = f"{provider}: {response_data['message']}"

        # 回退：直接用原始数据
        elif response_data:
            message = f"{provider} API error: {response_data}"

        return cls(
            message=message,
            http_status=http_status,
            error_data=response_data,
            provider=provider,
            model=model,
            error_code=error_code,
        )


# ============================================================
# 默认 System Prompt（v5.0 - 使用 build_system_prompt）
# 保留此变量是为了向后兼容
# ============================================================
from llm.prompt_templates import build_system_prompt

# 向后兼容：直接导出完整的默认 prompt（不传参数时使用所有默认值）
DEFAULT_SYSTEM_PROMPT = build_system_prompt()


# ============================================================
# LLM 客户端基类
# ============================================================
@dataclass
class LLMResponse:
    """LLM 响应"""
    content: str
    raw_response: Dict[str, Any]
    usage: Dict[str, int]  # prompt_tokens, completion_tokens, total_tokens
    model: str
    finish_reason: str
    response_time_ms: int
    http_status: Optional[int] = None  # HTTP 状态码


class BaseLLMClient(ABC):
    """LLM 客户端基类"""

    def __init__(
            self,
            model: str,
            api_key: str,
            base_url: str,
            provider: str = "",  # 提供商名称，用于获取默认参数
            temperature: float = None,  # None 表示使用提供商默认参数
            top_p: float = None,  # None 表示使用提供商默认参数
            max_tokens: int = 65536,
            timeout: int = 300,  # 超时设置
    ):
        self.model = model
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self.max_tokens = max_tokens
        self.timeout = timeout
        
        # 获取提供商默认参数，允许用户覆盖
        # 对于 custom 提供商，会根据模型名称智能推断
        provider_params = get_provider_params(provider, model) if provider else DEFAULT_PROVIDER_PARAMS
        self.temperature = temperature if temperature is not None else provider_params["temperature"]
        self.top_p = top_p if top_p is not None else provider_params["top_p"]

        self._session: Optional[aiohttp.ClientSession] = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=self.timeout)
            )
        return self._session

    async def close(self):
        """关闭 HTTP session"""
        # 先获取 session 引用并置空，避免重复关闭
        session = self._session
        self._session = None
        
        if session and not session.closed:
            try:
                await session.close()
            except RuntimeError as e:
                # 忽略事件循环不匹配的错误
                if "attached to a different loop" not in str(e):
                    logger.debug(f"Session close error: {e}")
            except Exception as e:
                logger.debug(f"Session close error: {e}")
            
            # 给 SSL transport 一点时间完成清理
            # 即使 close() 失败也要等待，避免 "Unclosed connector" 警告
            try:
                await asyncio.sleep(0.25)
            except Exception:
                pass

    def close_sync(self):
        """
        同步关闭 HTTP session（安全版本，用于跨事件循环场景）

        注意：这不会真正关闭底层连接，而是让 GC 清理。
        这是为了避免 "attached to a different loop" 错误。
        """
        # 直接置空，让 GC 清理连接
        # 不尝试调用 close()，因为它需要 await 且可能在不同的事件循环
        self._session = None

    def __del__(self):
        """析构函数 - 清理未关闭的 session，避免 ResourceWarning"""
        if hasattr(self, '_session') and self._session and not self._session.closed:
            # 在析构时无法 await，直接置空让 GC 清理
            # 这会产生警告，但比不处理要好
            self._session = None

    @abstractmethod
    async def chat(
            self,
            system_prompt: str,
            user_message: str,
    ) -> LLMResponse:
        """发送聊天请求"""
        pass

    @abstractmethod
    def get_provider_name(self) -> str:
        """获取提供商名称"""
        pass

    async def chat_with_retry(
            self,
            system_prompt: str,
            user_message: str,
            max_retries: int = 3,
            base_delay: float = 1.0,
            max_delay: float = 30.0,
            uid: str = None,
    ) -> LLMResponse:
        """
        带重试的聊天请求
        
        使用指数退避策略处理临时性网络错误和服务器临时错误。
        
        可重试的情况：
        - 网络错误（Broken pipe, Connection reset, Timeout 等）
        - HTTP 5xx 错误（502 Bad Gateway, 503 Service Unavailable 等）
        - HTTP 429 Too Many Requests（Rate Limit）
        
        不重试的情况：
        - HTTP 4xx 错误（除 408/429 外，如 401 Unauthorized, 400 Bad Request）
        - API 返回的业务错误
        
        Args:
            system_prompt: 系统提示词
            user_message: 用户消息
            max_retries: 最大重试次数（默认 3 次）
            base_delay: 基础延迟秒数（默认 1 秒）
            max_delay: 最大延迟秒数（默认 30 秒）
            
        Returns:
            LLMResponse: LLM 响应
            
        Raises:
            LLMAPIError: API 错误或重试耗尽后的最后一个错误
        """
        last_error: Optional[Exception] = None
        
        # 打印完整的投喂内容
        # print("\n" + "="*80)
        # print("LLM 投喂内容 - SYSTEM PROMPT")
        # print("="*80)
        # print(system_prompt)
        # print("\n" + "="*80)
        # print("LLM 投喂内容 - USER MESSAGE")
        # print("="*80)
        # print(user_message)
        # print("="*80 + "\n")
        
        for attempt in range(max_retries + 1):
            try:
                return await self.chat(system_prompt, user_message)
                
            except RETRYABLE_EXCEPTIONS as e:
                # 网络层错误，可重试
                last_error = e
                
                if attempt < max_retries:
                    delay = min(base_delay * (2 ** attempt), max_delay)
                    
                    uid_prefix = f"[{uid}] " if uid else ""
                    logger.warning(
                        f"{uid_prefix}[{self.get_provider_name()}] LLM 网络错误 "
                        f"(尝试 {attempt + 1}/{max_retries + 1}): {type(e).__name__}: {e}. "
                        f"将在 {delay:.1f}s 后重试..."
                    )
                    
                    # 重置 session（连接可能已失效）
                    await self.close()
                    await asyncio.sleep(delay)
                    continue
                    
            except LLMAPIError as e:
                # API 层错误，检查是否可重试
                if e.http_status in RETRYABLE_HTTP_STATUS_CODES:
                    last_error = e
                    
                    if attempt < max_retries:
                        # 对于 429 Rate Limit，使用更长的延迟
                        if e.http_status == 429:
                            delay = min(base_delay * (3 ** attempt), max_delay)  # 更激进的退避
                        else:
                            delay = min(base_delay * (2 ** attempt), max_delay)
                        
                        uid_prefix = f"[{uid}] " if uid else ""
                        logger.warning(
                            f"{uid_prefix}[{self.get_provider_name()}] LLM API 错误 HTTP {e.http_status} "
                            f"(尝试 {attempt + 1}/{max_retries + 1}): {e}. "
                            f"将在 {delay:.1f}s 后重试..."
                        )
                        
                        await asyncio.sleep(delay)
                        continue
                else:
                    # 不可重试的 API 错误（如 401, 400 等），直接抛出
                    raise
                    
            except Exception as e:
                # 其他未知错误，记录并抛出（不重试）
                logger.error(f"[{self.get_provider_name()}] LLM 调用遇到未知错误: {type(e).__name__}: {e}")
                raise
        
        # 重试耗尽
        if last_error:
            error_type = type(last_error).__name__
            if isinstance(last_error, LLMAPIError):
                logger.error(
                    f"[{self.get_provider_name()}] LLM 调用失败，重试 {max_retries} 次后仍失败: "
                    f"HTTP {last_error.http_status}: {last_error}"
                )
                # 保留原始 LLMAPIError 的信息，添加重试信息
                raise LLMAPIError(
                    message=f"重试 {max_retries} 次后仍失败: {last_error}",
                    http_status=last_error.http_status,
                    error_data={
                        **last_error.error_data,
                        "retries": max_retries,
                    },
                    provider=self.get_provider_name(),
                    model=self.model,
                    error_code="max_retries_exceeded",
                )
            else:
                logger.error(
                    f"[{self.get_provider_name()}] LLM 调用失败，重试 {max_retries} 次后仍失败: "
                    f"{error_type}: {last_error}"
                )
                raise LLMAPIError(
                    message=f"重试 {max_retries} 次后仍失败: {error_type}: {last_error}",
                    http_status=0,
                    error_data={
                        "original_error": error_type,
                        "original_message": str(last_error),
                        "retries": max_retries,
                    },
                    provider=self.get_provider_name(),
                    model=self.model,
                    error_code="max_retries_exceeded",
                )
        
        # 理论上不会到这里，但为了类型安全
        raise LLMAPIError(
            message="未知错误",
            provider=self.get_provider_name(),
            model=self.model,
            error_code="unknown_error",
        )


# ============================================================
# Anthropic (Claude) 客户端
# ============================================================
class AnthropicClient(BaseLLMClient):
    """Anthropic Claude 客户端"""

    def __init__(
            self,
            model: str = "claude-opus-4-6-20260203",
            api_key: str = None,
            base_url: str = "https://api.anthropic.com",
            **kwargs
    ):
        api_key = api_key or os.getenv("ANTHROPIC_API_KEY", "")
        super().__init__(model=model, api_key=api_key, base_url=base_url, provider="anthropic", **kwargs)

    def get_provider_name(self) -> str:
        return "anthropic"

    async def chat(self, system_prompt: str, user_message: str) -> LLMResponse:
        import time
        start_time = time.time()

        session = await self._get_session()

        headers = {
            "Content-Type": "application/json",
            "x-api-key": self.api_key,
            "anthropic-version": "2023-06-01",
        }

        payload = {
            "model": self.model,
            "max_tokens": self.max_tokens,
            "temperature": self.temperature,
            "top_p": self.top_p,
            "system": system_prompt,
            "messages": [
                {"role": "user", "content": user_message}
            ]
        }

        url = f"{self.base_url}/v1/messages"
        
        logger.info(
            f"[anthropic] 调用模型: {self.model}, "
            f"temperature={self.temperature}, top_p={self.top_p}, max_tokens={self.max_tokens}"
        )

        async with session.post(url, headers=headers, json=payload) as resp:
            # 尝试解析 JSON，处理非 JSON 响应（如 502 错误页面）
            try:
                data = await resp.json()
            except aiohttp.ContentTypeError as e:
                # 服务器返回了非 JSON 响应（通常是 HTML 错误页面）
                text = await resp.text()
                # 截取前 200 字符用于错误信息
                text_preview = text[:200] + "..." if len(text) > 200 else text
                raise LLMAPIError(
                    message=f"API 返回非 JSON 响应 (HTTP {resp.status}): {text_preview}",
                    http_status=resp.status,
                    error_data={"raw_response": text_preview, "content_type": resp.content_type},
                    provider="anthropic",
                    model=self.model,
                    error_code="invalid_response_format",
                )

            if resp.status != 200:
                raise LLMAPIError.from_response(
                    provider="anthropic",
                    model=self.model,
                    http_status=resp.status,
                    response_data=data,
                )

            # 使用 response_parser 提取内容（支持 thinking blocks）
            parsed = extract_anthropic_content(data)
            content = parsed.content
            
            # 如果有思考内容，记录日志
            if parsed.reasoning_content:
                logger.debug(
                    f"[anthropic] 检测到思考内容: "
                    f"{len(parsed.reasoning_content)} 字符"
                )

            usage = data.get("usage", {})

            return LLMResponse(
                content=content,
                raw_response=data,
                usage={
                    "prompt_tokens": usage.get("input_tokens", 0),
                    "completion_tokens": usage.get("output_tokens", 0),
                    "total_tokens": usage.get("input_tokens", 0) + usage.get("output_tokens", 0),
                },
                model=data.get("model", self.model),
                finish_reason=data.get("stop_reason", ""),
                response_time_ms=int((time.time() - start_time) * 1000),
                http_status=resp.status,
            )


# ============================================================
# OpenAI 兼容客户端（GPT / DeepSeek / OpenRouter 等）
# ============================================================
class OpenAICompatibleClient(BaseLLMClient):
    """OpenAI 兼容 API 客户端"""

    def __init__(
            self,
            model: str = "gpt-4o",
            api_key: str = None,
            base_url: str = "https://api.openai.com/v1",
            provider_name: str = "openai",
            **kwargs
    ):
        api_key = api_key or os.getenv("OPENAI_API_KEY", "")
        super().__init__(model=model, api_key=api_key, base_url=base_url, provider=provider_name, **kwargs)
        self._provider_name = provider_name

    def get_provider_name(self) -> str:
        return self._provider_name

    async def chat(self, system_prompt: str, user_message: str) -> LLMResponse:
        import time
        start_time = time.time()

        session = await self._get_session()

        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.api_key}",
        }

        payload = {
            "model": self.model,
            "max_tokens": self.max_tokens,
            "temperature": self.temperature,
            "top_p": self.top_p,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_message}
            ]
        }

        # 智能处理 URL：如果 base_url 已包含 /chat/completions，不再拼接
        if self.base_url.endswith("/chat/completions"):
            url = self.base_url
        else:
            url = f"{self.base_url}/chat/completions"
        
        # 日志显示：如果是 custom 且智能推断了提供商，显示推断结果
        if self._provider_name == "custom":
            inferred = infer_provider_from_model(self.model)
            if inferred != "custom":
                logger.info(
                    f"[{self._provider_name}] 调用模型: {self.model} (参数来自 {inferred}), "
                    f"temperature={self.temperature}, top_p={self.top_p}, max_tokens={self.max_tokens}"
                )
            else:
                logger.info(
                    f"[{self._provider_name}] 调用模型: {self.model}, "
                    f"temperature={self.temperature}, top_p={self.top_p}, max_tokens={self.max_tokens}"
                )
        else:
            logger.info(
                f"[{self._provider_name}] 调用模型: {self.model}, "
                f"temperature={self.temperature}, top_p={self.top_p}, max_tokens={self.max_tokens}"
            )

        async with session.post(url, headers=headers, json=payload) as resp:
            # 尝试解析 JSON，处理非 JSON 响应（如 502 错误页面）
            try:
                data = await resp.json()
            except aiohttp.ContentTypeError as e:
                # 服务器返回了非 JSON 响应（通常是 HTML 错误页面）
                text = await resp.text()
                # 截取前 200 字符用于错误信息
                text_preview = text[:200] + "..." if len(text) > 200 else text
                raise LLMAPIError(
                    message=f"API 返回非 JSON 响应 (HTTP {resp.status}): {text_preview}",
                    http_status=resp.status,
                    error_data={"raw_response": text_preview, "content_type": resp.content_type},
                    provider=self._provider_name,
                    model=self.model,
                    error_code="invalid_response_format",
                )

            if resp.status != 200:
                raise LLMAPIError.from_response(
                    provider=self._provider_name,
                    model=self.model,
                    http_status=resp.status,
                    response_data=data,
                )

            # 使用 response_parser 提取内容（支持 reasoning_content）
            parsed = extract_openai_content(data)
            content = parsed.content
            
            # 如果有推理内容，记录日志
            if parsed.reasoning_content:
                logger.debug(
                    f"[{self._provider_name}] 检测到推理内容: "
                    f"{len(parsed.reasoning_content)} 字符"
                )

            choices = data.get("choices", [])
            usage = data.get("usage", {})

            return LLMResponse(
                content=content,
                raw_response=data,
                usage={
                    "prompt_tokens": usage.get("prompt_tokens", 0),
                    "completion_tokens": usage.get("completion_tokens", 0),
                    "total_tokens": usage.get("total_tokens", 0),
                },
                model=data.get("model", self.model),
                finish_reason=choices[0].get("finish_reason", "") if choices else "",
                response_time_ms=int((time.time() - start_time) * 1000),
                http_status=resp.status,
            )


# ============================================================
# DeepSeek 客户端
# ============================================================
class DeepSeekClient(OpenAICompatibleClient):
    """DeepSeek 客户端"""

    def __init__(
            self,
            model: str = "deepseek-chat",
            api_key: str = None,
            base_url: str = "https://api.deepseek.com/v1",
            **kwargs
    ):
        api_key = api_key or os.getenv("DEEPSEEK_API_KEY", "")
        super().__init__(
            model=model,
            api_key=api_key,
            base_url=base_url,
            provider_name="deepseek",
            **kwargs
        )


# ============================================================
# OpenRouter 客户端（聚合多种模型）
# ============================================================
class OpenRouterClient(OpenAICompatibleClient):
    """OpenRouter 客户端"""

    def __init__(
            self,
            model: str = "anthropic/claude-opus-4-5-20251101",
            api_key: str = None,
            base_url: str = "https://openrouter.ai/api/v1",
            **kwargs
    ):
        api_key = api_key or os.getenv("OPENROUTER_API_KEY", "")
        super().__init__(
            model=model,
            api_key=api_key,
            base_url=base_url,
            provider_name="openrouter",
            **kwargs
        )


# ============================================================
# Grok (xAI) 客户端
# ============================================================
class GrokClient(OpenAICompatibleClient):
    """Grok (xAI) 客户端"""

    def __init__(
            self,
            model: str = "grok-4-1-fast-reasoning",
            api_key: str = None,
            base_url: str = "https://api.x.ai/v1",
            **kwargs
    ):
        api_key = api_key or os.getenv("GROK_API_KEY", "")
        super().__init__(
            model=model,
            api_key=api_key,
            base_url=base_url,
            provider_name="grok",
            **kwargs
        )


# ============================================================
# Google Gemini 客户端
# ============================================================
class GeminiClient(OpenAICompatibleClient):
    """Google Gemini 客户端（使用 OpenAI 兼容 API）"""

    def __init__(
            self,
            model: str = "gemini-3-pro-preview",
            api_key: str = None,
            base_url: str = "https://generativelanguage.googleapis.com/v1beta/openai",
            **kwargs
    ):
        api_key = api_key or os.getenv("GEMINI_API_KEY", "")
        super().__init__(
            model=model,
            api_key=api_key,
            base_url=base_url,
            provider_name="gemini",
            **kwargs
        )


# ============================================================
# 工厂函数
# ============================================================
def create_llm_client(
        provider: str,
        model: str,
        api_key: Optional[str] = None,
        base_url: Optional[str] = None,
        **kwargs
) -> BaseLLMClient:
    """
    创建 LLM 客户端

    Args:
        provider: 提供商 (anthropic / openai / deepseek / openrouter / grok / gemini / custom)
        model: 模型名称
        api_key: API 密钥（可选，默认从环境变量读取）
        base_url: API 地址（可选）
        **kwargs: 其他参数 (temperature, max_tokens 等)

    Returns:
        LLM 客户端实例
    """
    provider = provider.lower()

    if provider == "anthropic":
        return AnthropicClient(
            model=model,
            api_key=api_key,
            base_url=base_url or "https://api.anthropic.com",
            **kwargs
        )

    elif provider == "openai":
        return OpenAICompatibleClient(
            model=model,
            api_key=api_key,
            base_url=base_url or "https://api.openai.com/v1",
            provider_name="openai",
            **kwargs
        )

    elif provider == "deepseek":
        return DeepSeekClient(
            model=model,
            api_key=api_key,
            base_url=base_url or "https://api.deepseek.com/v1",
            **kwargs
        )

    elif provider == "openrouter":
        return OpenRouterClient(
            model=model,
            api_key=api_key,
            base_url=base_url or "https://openrouter.ai/api/v1",
            **kwargs
        )

    elif provider == "grok":
        return GrokClient(
            model=model,
            api_key=api_key,
            base_url=base_url or "https://api.x.ai/v1",
            **kwargs
        )

    elif provider == "gemini":
        return GeminiClient(
            model=model,
            api_key=api_key,
            base_url=base_url or "https://generativelanguage.googleapis.com/v1beta/openai",
            **kwargs
        )

    elif provider == "custom":
        # 自定义 OpenAI 兼容服务
        if not base_url:
            raise ValueError("custom provider requires base_url")
        return OpenAICompatibleClient(
            model=model,
            api_key=api_key or "",
            base_url=base_url,
            provider_name="custom",
            **kwargs
        )

    else:
        raise ValueError(f"Unknown LLM provider: {provider}")


# ============================================================
# 用户级 LLM 调用封装
# ============================================================
async def call_llm_for_user(
        ctx: 'UserContext',
        user_message: str,
        strategy_id: Optional[str] = None,
        max_retries: int = 3,
) -> tuple[List[Dict], LLMResponse]:
    """
    为指定用户调用 LLM（带自动重试）

    Args:
        ctx: 用户上下文
        user_message: 用户消息（投喂数据）
        strategy_id: AI 策略 ID（可选）。如果指定，则使用该策略的配置。
        max_retries: 最大重试次数（默认 3 次），用于处理临时性网络错误

    Returns:
        (解析后的交易信号列表, 原始响应)
        
    Raises:
        LLMAPIError: API 错误或重试耗尽后的错误
    """
    strategy_info = ""
    client = None
    
    try:
        # 获取 LLM 客户端（如果指定了策略，使用策略配置）
        client = ctx.get_llm_client(strategy_id=strategy_id)
        
        system_prompt = ctx.get_system_prompt(strategy_id=strategy_id)

        strategy_info = f" (strategy={strategy_id})" if strategy_id else ""

        # 使用带重试的方法调用 LLM
        response = await client.chat_with_retry(
            system_prompt, 
            user_message,
            max_retries=max_retries,
            uid=ctx.uid,
        )

        signals = parse_trading_signals(response.content)

        # 记录调用结果
        log_msg = (
            f"[{ctx.uid}]{strategy_info} LLM 调用完成: "
            f"provider={client.get_provider_name()}, "
            f"model={response.model}, "
            f"tokens={response.usage.get('total_tokens', 0)}, "
            f"signals={len(signals)}"
        )
        
        if signals:
            logger.info(log_msg)
        elif response.content and len(response.content) > 100:
            # 有较长内容但没解析出信号，可能是解析问题
            logger.info(f"{log_msg} (响应内容 {len(response.content)} 字符，未解析出信号)")
        else:
            logger.info(log_msg)

        return signals, response

    except LLMAPIError:
        # LLMAPIError 已经包含了详细信息（包括重试信息），直接抛出
        logger.error(f"[{ctx.uid}]{strategy_info} LLM 调用失败")
        raise
    except Exception as e:
        # 其他未预期的错误，包装成 LLMAPIError 以携带 provider/model 信息
        logger.error(f"[{ctx.uid}]{strategy_info} LLM 调用遇到未知错误: {type(e).__name__}: {e}")

        # 获取 provider 和 model 信息（如果 client 已创建）
        provider = client.get_provider_name() if client else ""
        model = client.model if client else ""

        # 包装成 LLMAPIError，保留原始错误信息
        wrapped_error = LLMAPIError(
            message=str(e),
            http_status=0,  # 网络错误没有 HTTP 状态码
            error_data={"original_error": type(e).__name__, "original_message": str(e)},
            provider=provider,
            model=model,
            error_code="unknown_error",
        )
        raise wrapped_error from e
    finally:
        # 关闭客户端，释放连接资源
        # 由于不再缓存客户端，每次调用后必须关闭
        if client:
            try:
                await client.close()
            except Exception as e:
                logger.debug(f"关闭 LLM 客户端失败: {e}")
