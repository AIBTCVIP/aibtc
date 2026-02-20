# user_api.py
"""
用户管理 API

提供用户注册、登录、配置管理等功能
支持 Google Authenticator (TOTP) 两步验证

安全特性:
- HttpOnly Cookie 存储 JWT Token (防止 XSS 窃取)
- 同时支持 Bearer Token (用于 API 调用)
"""
import os
import io
import json
import base64
import hashlib
import secrets

# bcrypt for secure password hashing
try:
    import bcrypt
    BCRYPT_AVAILABLE = True
except ImportError:
    BCRYPT_AVAILABLE = False
import time
import asyncio
import logging
import threading
from typing import Optional, List, Dict
from fastapi import APIRouter, HTTPException, Depends, Header, Request, Cookie, Response, Query
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

try:
    import jwt
    JWT_AVAILABLE = True
except ImportError:
    jwt = None
    JWT_AVAILABLE = False
    logger.warning("PyJWT not installed. User authentication will not work.")

# Google Authenticator (TOTP) support
try:
    import pyotp
    import qrcode
    TOTP_AVAILABLE = True
except ImportError as e:
    pyotp = None
    qrcode = None
    TOTP_AVAILABLE = False
    logger.warning(f"pyotp/qrcode not installed. TOTP authentication will not work. Error: {e}")

from core.user_db import config_loader
from core.user_context import context_manager, UserConfig
from api.error_utils import safe_error_detail

# ============================================================
# 用户存在性缓存（避免每个请求都查数据库）
# ============================================================
_user_exists_cache: Dict[str, float] = {}  # {uid: expire_timestamp}
_user_exists_cache_lock = threading.Lock()  # P0 Fix: 添加锁保护
_USER_EXISTS_CACHE_TTL = 60  # 缓存60秒

def _is_user_in_cache(uid: str) -> bool:
    """
    快速检查用户是否在缓存中（不查数据库）
    这个函数可以在主线程调用，非常快
    """
    with _user_exists_cache_lock:
        if uid in _user_exists_cache:
            if _user_exists_cache[uid] > time.time():
                return True
            else:
                # 过期了，删除
                _user_exists_cache.pop(uid, None)
        return False

def _check_user_exists_and_cache(uid: str) -> bool:
    """
    查数据库并更新缓存（在线程池中调用）
    """
    exists = config_loader.user_exists(uid)
    if exists:
        with _user_exists_cache_lock:
            _user_exists_cache[uid] = time.time() + _USER_EXISTS_CACHE_TTL
    return exists

def _invalidate_user_cache(uid: str):
    """使某个用户的缓存失效（用于用户被删除时）"""
    with _user_exists_cache_lock:
        _user_exists_cache.pop(uid, None)

# 检查 SQLAlchemy
try:
    from sqlalchemy import text
    HAS_SQLALCHEMY = True
except ImportError:
    HAS_SQLALCHEMY = False


def _row_to_dict(row):
    """将 SQLAlchemy Row 或 sqlite3.Row 转换为 dict"""
    if row is None:
        return None
    # SQLAlchemy Row 对象
    if hasattr(row, '_mapping'):
        return dict(row._mapping)
    # sqlite3.Row 对象
    if hasattr(row, 'keys'):
        return dict(row)
    return row

# ============================================================
# 配置
# ============================================================
# 安全配置 - 生产环境必须设置环境变量
_jwt_secret = os.getenv("JWT_SECRET")
_is_production = os.getenv("ENVIRONMENT", "development").lower() == "production"

if not _jwt_secret:
    if _is_production:
        # P2 Fix: 生产环境必须设置 JWT_SECRET
        raise RuntimeError(
            "生产环境必须设置 JWT_SECRET 环境变量！"
            "请使用强随机密钥，如: openssl rand -base64 32"
        )
    else:
        import warnings
        warnings.warn(
            "JWT_SECRET 环境变量未设置！使用随机生成的密钥（重启后所有 token 失效）。"
            "生产环境请设置 JWT_SECRET 环境变量。",
            RuntimeWarning
        )
        _jwt_secret = secrets.token_urlsafe(32)

JWT_SECRET = _jwt_secret

JWT_ALGORITHM = "HS256"
JWT_EXPIRE_HOURS = 24 * 7  # 7天

# Cookie 配置
COOKIE_NAME = "auth_token"
COOKIE_MAX_AGE = JWT_EXPIRE_HOURS * 3600  # 与 JWT 过期时间一致
# P2 Fix: 生产环境默认启用 COOKIE_SECURE
COOKIE_SECURE = os.getenv("COOKIE_SECURE", "true" if _is_production else "false").lower() == "true"
COOKIE_SAMESITE = "lax"  # lax 或 strict

# CSRF 配置
CSRF_COOKIE_NAME = "csrf_token"
CSRF_HEADER_NAME = "X-CSRF-Token"

# ============================================================
# P1 安全: 登录限速配置
# ============================================================
# 内存存储登录尝试记录 (生产环境建议使用 Redis)
_login_attempts: Dict[str, tuple] = {}  # {ip:username: (count, first_attempt_time)}
_login_attempts_lock = threading.Lock()

# 限速参数
LOGIN_MAX_ATTEMPTS = 5  # 最大尝试次数
LOGIN_LOCKOUT_SECONDS = 300  # 锁定时间 (5分钟)
LOGIN_CLEANUP_INTERVAL = 600  # 清理间隔 (10分钟)
_last_cleanup_time = 0.0


def _cleanup_login_attempts():
    """清理过期的登录尝试记录"""
    global _last_cleanup_time
    now = time.time()
    
    if now - _last_cleanup_time < LOGIN_CLEANUP_INTERVAL:
        return
    
    with _login_attempts_lock:
        _last_cleanup_time = now
        expired_keys = [
            key for key, (count, ts) in _login_attempts.items()
            if now - ts > LOGIN_LOCKOUT_SECONDS
        ]
        for key in expired_keys:
            del _login_attempts[key]
        
        if expired_keys:
            logger.debug(f"清理了 {len(expired_keys)} 条过期的登录限速记录")


def check_login_rate_limit(ip: str, username: str) -> tuple[bool, int]:
    """
    检查登录限速
    
    Args:
        ip: 客户端 IP
        username: 用户名
    
    Returns:
        (allowed, remaining_seconds): 是否允许登录, 剩余锁定秒数
    """
    _cleanup_login_attempts()
    
    key = f"{ip}:{username}"
    now = time.time()
    
    with _login_attempts_lock:
        if key in _login_attempts:
            count, first_ts = _login_attempts[key]
            elapsed = now - first_ts
            
            if elapsed > LOGIN_LOCKOUT_SECONDS:
                # 锁定期已过，重置计数
                _login_attempts[key] = (1, now)
                return True, 0
            
            if count >= LOGIN_MAX_ATTEMPTS:
                # 已达到最大尝试次数，拒绝
                remaining = int(LOGIN_LOCKOUT_SECONDS - elapsed)
                return False, remaining
            
            # 增加计数
            _login_attempts[key] = (count + 1, first_ts)
        else:
            # 首次尝试
            _login_attempts[key] = (1, now)
    
    return True, 0


def record_login_failure(ip: str, username: str):
    """记录登录失败 (已在 check_login_rate_limit 中计数，此函数用于额外记录)"""
    logger.warning(f"登录失败: ip={ip}, username={username}")


def clear_login_attempts(ip: str, username: str):
    """登录成功后清除尝试记录"""
    key = f"{ip}:{username}"
    with _login_attempts_lock:
        _login_attempts.pop(key, None)

# 从共享配置导入管理员列表
from core.config import ADMIN_USERS

router = APIRouter(prefix="/api/user", tags=["用户管理"])
security = HTTPBearer(auto_error=False)


def generate_csrf_token() -> str:
    """生成 CSRF Token"""
    return secrets.token_urlsafe(32)


def verify_csrf_token(request: Request, csrf_cookie: str = None) -> bool:
    """
    验证 CSRF Token (Double Submit Cookie 模式)
    
    检查 Header 中的 Token 是否与 Cookie 中的一致
    """
    header_token = request.headers.get(CSRF_HEADER_NAME)
    if not header_token or not csrf_cookie:
        return False
    return secrets.compare_digest(header_token, csrf_cookie)


# ============================================================
# 数据模型
# ============================================================

class UserRegisterStep1Request(BaseModel):
    """注册第一步：提供用户名密码，获取 TOTP 二维码"""
    username: str = Field(..., min_length=3, max_length=32)
    password: str = Field(..., min_length=6, max_length=64)
    email: Optional[str] = None
    referral_code: Optional[str] = Field(None, min_length=4, max_length=16, description="邀请码")


class UserRegisterStep1Response(BaseModel):
    """注册第一步响应：返回 TOTP 绑定信息"""
    temp_token: str  # 临时 token，用于第二步验证
    totp_secret: str  # TOTP 密钥（用于手动输入）
    totp_uri: str  # otpauth:// URI（用于生成二维码）
    qr_code_base64: str  # 二维码图片的 base64 编码


class UserRegisterStep2Request(BaseModel):
    """注册第二步：验证 TOTP 完成注册"""
    temp_token: str  # 第一步返回的临时 token
    totp_code: str = Field(..., min_length=6, max_length=6, pattern=r'^\d{6}$')


class UserRegisterRequest(BaseModel):
    """兼容旧版：一步注册（不推荐，仅用于测试）"""
    username: str = Field(..., min_length=3, max_length=32)
    password: str = Field(..., min_length=6, max_length=64)
    email: Optional[str] = None
    referral_code: Optional[str] = Field(None, min_length=4, max_length=16, description="邀请码")


class UserLoginRequest(BaseModel):
    username: str
    password: str
    totp_code: str = Field(..., min_length=6, max_length=6, pattern=r'^\d{6}$', description="Google Authenticator 验证码")


class UserLoginResponse(BaseModel):
    uid: str
    username: str
    token: str
    expires_at: int


class TradingConfigUpdateRequest(BaseModel):
    max_positions: Optional[int] = None
    position_size_pct: Optional[float] = None
    default_leverage: Optional[int] = None
    monitor_symbols: Optional[List[str]] = None
    ai500_enabled: Optional[bool] = None
    # 执行约束配置
    min_rr_ratio: Optional[float] = None  # 最小风险回报比
    limit_order_min_distance_pct: Optional[float] = None  # 限价单最小距离百分比


class LLMConfigUpdateRequest(BaseModel):
    ai_enabled: Optional[bool] = None
    llm_provider: Optional[str] = None  # anthropic / openai / deepseek / openrouter / custom
    llm_model: Optional[str] = None
    llm_api_key: Optional[str] = None
    llm_base_url: Optional[str] = None
    system_prompt: Optional[str] = None
    temperature: Optional[float] = None
    max_tokens: Optional[int] = None


class NotificationConfigUpdateRequest(BaseModel):
    telegram_bot_token: Optional[str] = None
    telegram_chat_id: Optional[str] = None
    telegram_topic_id: Optional[str] = None  # Telegram 群组话题 ID
    telegram_enabled: Optional[bool] = None


class ExchangeConfigRequest(BaseModel):
    """交易所配置请求"""
    # exchange 从 URL 路径获取，不在 body 中
    api_key: Optional[str] = None
    api_secret: Optional[str] = None
    passphrase: Optional[str] = None  # OKX, Bitget 需要
    wallet_address: Optional[str] = None  # Hyperliquid 可选
    is_testnet: bool = False
    is_enabled: bool = True


class UserConfigResponse(BaseModel):
    uid: str
    username: Optional[str] = None  # 用户名（用于前端显示和 sessionStorage 恢复）
    # 交易配置
    max_positions: int
    position_size_pct: float
    default_leverage: int
    monitor_symbols: List[str]
    ai500_enabled: bool = False  # AI500 智能选币
    # 执行约束配置
    min_rr_ratio: float = 3.0  # 最小风险回报比
    limit_order_min_distance_pct: float = 3.0  # 限价单最小距离百分比
    # AI 配置
    ai_enabled: bool
    llm_provider: str
    llm_model: str
    has_custom_llm_key: bool  # 是否有自定义 LLM key
    system_prompt: Optional[str]  # 完整 Prompt
    llm_base_url: Optional[str]
    temperature: Optional[float] = None  # 已废弃，由 llm_client 根据提供商自动设置
    max_tokens: int
    # 通知配置
    telegram_enabled: bool
    has_telegram_bot_token: bool
    has_telegram_chat_id: bool
    has_telegram_topic_id: bool
    # API 配置
    has_api_keys: bool
    is_testnet: bool
    # 状态
    tier: str
    is_running: bool


# ============================================================
# 工具函数
# ============================================================

def hash_password(password: str) -> str:
    """
    密码哈希 - 使用 bcrypt (推荐) 或 SHA256 (后备)
    
    bcrypt 优势:
    - 自动生成随机盐值
    - 可配置的计算成本 (work factor)
    - 抵抗彩虹表和暴力破解攻击
    """
    if BCRYPT_AVAILABLE:
        # 使用 bcrypt - 安全的密码哈希算法
        # work factor=12 表示 2^12 次迭代，平衡安全性和性能
        password_bytes = password.encode('utf-8')
        salt = bcrypt.gensalt(rounds=12)
        hashed = bcrypt.hashpw(password_bytes, salt)
        return hashed.decode('utf-8')
    else:
        # 后备方案：SHA256 + 盐值 (不推荐用于生产环境)
        import warnings
        warnings.warn(
            "bcrypt 未安装，使用 SHA256 哈希（不推荐）。"
            "请安装 bcrypt: pip install bcrypt",
            RuntimeWarning
        )
        salt = os.getenv("PASSWORD_SALT")
        if not salt:
            warnings.warn(
                "PASSWORD_SALT 环境变量未设置！使用默认盐值（不安全）。"
                "生产环境请设置 PASSWORD_SALT 环境变量。",
                RuntimeWarning
            )
            salt = "tradev6-default-salt-change-me"
        return hashlib.sha256(f"{password}{salt}".encode()).hexdigest()


def verify_password(password: str, hashed: str) -> bool:
    """
    验证密码
    
    支持 bcrypt 和 SHA256 两种格式，便于迁移
    """
    if BCRYPT_AVAILABLE and hashed.startswith('$2'):
        # bcrypt 哈希格式以 $2a$, $2b$, $2y$ 开头
        try:
            password_bytes = password.encode('utf-8')
            hashed_bytes = hashed.encode('utf-8')
            return bcrypt.checkpw(password_bytes, hashed_bytes)
        except Exception as e:
            logger.error(f"bcrypt 验证失败: {e}")
            return False
    else:
        # 后备：SHA256 验证 (用于旧密码或 bcrypt 不可用时)
        salt = os.getenv("PASSWORD_SALT")
        if not salt:
            salt = "tradev6-default-salt-change-me"
        sha256_hash = hashlib.sha256(f"{password}{salt}".encode()).hexdigest()
        return sha256_hash == hashed


def generate_uid() -> str:
    """生成用户ID"""
    return secrets.token_hex(8)


def create_jwt_token(uid: str, username: str) -> tuple[str, int]:
    """创建 JWT Token"""
    if not JWT_AVAILABLE:
        raise HTTPException(status_code=500, detail="JWT 模块未安装，请安装 PyJWT")
    
    expires_at = int(time.time()) + JWT_EXPIRE_HOURS * 3600
    payload = {
        "uid": uid,
        "username": username,
        "exp": expires_at,
        "iat": int(time.time()),
    }
    token = jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)
    return token, expires_at


def decode_jwt_token(token: str) -> Optional[Dict]:
    """解码 JWT Token"""
    if not JWT_AVAILABLE:
        return None
    
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
        return payload
    except jwt.ExpiredSignatureError:
        logger.debug("Token expired")
        return None
    except jwt.InvalidTokenError as e:
        logger.debug(f"Invalid token: {e}")
        return None


async def get_current_user(
    request: Request,
    credentials: HTTPAuthorizationCredentials = Depends(security),
    auth_token: Optional[str] = Cookie(None, alias=COOKIE_NAME),
    csrf_token: Optional[str] = Cookie(None, alias=CSRF_COOKIE_NAME)
) -> Dict:
    """
    获取当前用户（依赖注入）
    
    优先级:
    1. Bearer Token (用于 API 调用，不需要 CSRF)
    2. HttpOnly Cookie (用于浏览器页面，需要 CSRF 验证)
    """
    if not JWT_AVAILABLE:
        raise HTTPException(status_code=500, detail="JWT 模块未安装")
    
    token = None
    using_cookie = False
    
    # 优先使用 Bearer Token
    if credentials and credentials.credentials:
        token = credentials.credentials
    # 其次使用 Cookie
    elif auth_token:
        token = auth_token
        using_cookie = True
    
    if not token:
        raise HTTPException(status_code=401, detail="未提供认证信息")
    
    # 如果使用 Cookie 认证，对于修改操作需要验证 CSRF Token
    if using_cookie and request.method in ("POST", "PUT", "DELETE", "PATCH"):
        if not verify_csrf_token(request, csrf_token):
            raise HTTPException(status_code=403, detail="CSRF Token 验证失败")
    
    payload = decode_jwt_token(token)
    
    if not payload:
        raise HTTPException(status_code=401, detail="Token 无效或已过期")
    
    # 注意：不再每次请求都验证用户是否存在于数据库
    # JWT Token 本身就是认证凭证，有过期时间保护
    # 如果用户被删除，token 过期后自然失效
    
    return payload


async def get_optional_user(
    request: Request,
    credentials: HTTPAuthorizationCredentials = Depends(security),
    auth_token: Optional[str] = Cookie(None, alias=COOKIE_NAME)
) -> Optional[Dict]:
    """获取当前用户（可选，不强制）"""
    if not JWT_AVAILABLE:
        return None
    
    token = None
    if credentials and credentials.credentials:
        token = credentials.credentials
    elif auth_token:
        token = auth_token
    
    if not token:
        return None
    
    return decode_jwt_token(token)


# ============================================================
# TOTP 工具函数
# ============================================================

# 临时注册 token 存储
# P2 Note: 当前使用内存存储，适用于单实例部署
# 多实例部署时应迁移到 Redis，使用 key 格式: pending_reg:{token}
# 迁移时需要：1) 设置 TTL  2) 使用 Redis SETNX 防止竞态
_pending_registrations: Dict[str, Dict] = {}
_pending_registrations_lock = threading.Lock()  # P0 Fix: 添加锁保护
PENDING_REGISTRATION_TTL = 600  # 10 分钟有效期


def generate_totp_secret() -> str:
    """生成 TOTP 密钥"""
    if not TOTP_AVAILABLE:
        raise HTTPException(status_code=500, detail="TOTP 模块未安装")
    return pyotp.random_base32()


def get_totp_uri(secret: str, username: str, issuer: str = "AIBTC.VIP") -> str:
    """生成 otpauth:// URI"""
    if not TOTP_AVAILABLE:
        raise HTTPException(status_code=500, detail="TOTP 模块未安装")
    totp = pyotp.TOTP(secret)
    return totp.provisioning_uri(name=username, issuer_name=issuer)


def generate_qr_code_base64(uri: str) -> str:
    """生成二维码的 base64 编码"""
    if not TOTP_AVAILABLE or qrcode is None:
        raise HTTPException(status_code=500, detail="qrcode 模块未安装")
    
    # 生成二维码
    qr = qrcode.QRCode(version=1, box_size=10, border=5)
    qr.add_data(uri)
    qr.make(fit=True)
    img = qr.make_image(fill_color="black", back_color="white")
    
    # 转换为 base64
    buffer = io.BytesIO()
    img.save(buffer, format='PNG')
    buffer.seek(0)
    return base64.b64encode(buffer.getvalue()).decode('utf-8')


def verify_totp(secret: str, code: str) -> bool:
    """验证 TOTP 验证码"""
    if not TOTP_AVAILABLE:
        return False
    totp = pyotp.TOTP(secret)
    return totp.verify(code, valid_window=1)  # 允许前后 30 秒的误差


def create_temp_token(data: Dict) -> str:
    """创建临时 token（用于注册第二步）"""
    token = secrets.token_urlsafe(32)
    with _pending_registrations_lock:
        _pending_registrations[token] = {
            **data,
            "created_at": time.time()
        }
    return token


def get_pending_registration(token: str) -> Optional[Dict]:
    """获取待完成的注册信息"""
    with _pending_registrations_lock:
        data = _pending_registrations.get(token)
        if not data:
            return None
        
        # 检查是否过期
        if time.time() - data["created_at"] > PENDING_REGISTRATION_TTL:
            _pending_registrations.pop(token, None)
            return None
        
        return data


def clear_pending_registration(token: str):
    """清除待完成的注册信息"""
    with _pending_registrations_lock:
        _pending_registrations.pop(token, None)


# ============================================================
# API 端点
# ============================================================

@router.post("/register/step1", response_model=UserRegisterStep1Response)
async def register_step1(req: UserRegisterStep1Request):
    """
    注册第一步：验证用户名，生成 TOTP 二维码
    
    返回临时 token 和二维码，用户需要用 Google Authenticator 扫描二维码，
    然后在第二步中输入验证码完成注册
    """
    # 清理用户名前后空格
    req.username = req.username.strip()
    if req.email:
        req.email = req.email.strip()
    
    # 检查注册是否开放
    if not config_loader.is_registration_enabled():
        raise HTTPException(status_code=403, detail="注册功能已关闭，请联系管理员")
    
    # 禁止使用常用管理员用户名注册
    RESERVED_USERNAMES = {
        "admin", "administrator", "root", "superuser", "sysadmin",
        "system", "operator", "manager", "master", "owner",
        "sudo", "su", "test", "guest", "anonymous", "user",
        "support", "help", "info", "service", "api", "www", "aibtc",
    }
    if req.username.lower() in RESERVED_USERNAMES:
        raise HTTPException(status_code=400, detail="该用户名为系统保留，不允许注册")
    
    # 检查是否需要邀请码
    if config_loader.is_invite_code_required():
        if not req.referral_code:
            raise HTTPException(status_code=400, detail="注册需要邀请码")
        # 验证邀请码是否有效
        from core.referral_db import referral_db
        referrer_uid = referral_db.get_uid_by_referral_code(req.referral_code)
        if not referrer_uid:
            raise HTTPException(status_code=400, detail="邀请码无效")
    
    if not TOTP_AVAILABLE:
        raise HTTPException(status_code=500, detail="TOTP 模块未安装，请联系管理员")
    
    try:
        # 检查用户名是否存在
        with config_loader._get_connection() as conn:
            if hasattr(conn, 'execute'):
                from sqlalchemy import text
                existing = conn.execute(text(
                    "SELECT uid FROM users WHERE username = :username"
                ), {"username": req.username}).fetchone()
            else:
                cursor = conn.execute(
                    "SELECT uid FROM users WHERE username = ?",
                    (req.username,)
                )
                existing = cursor.fetchone()
            
            if existing:
                raise HTTPException(status_code=400, detail="用户名已存在")
        
        # 生成 TOTP 密钥
        totp_secret = generate_totp_secret()
        totp_uri = get_totp_uri(totp_secret, req.username)
        qr_code_base64 = generate_qr_code_base64(totp_uri)
        
        # 创建临时 token，保存注册信息
        temp_token = create_temp_token({
            "username": req.username,
            "password": req.password,  # 临时存储，第二步完成后清除
            "email": req.email,
            "referral_code": req.referral_code,
            "totp_secret": totp_secret,
        })
        
        return UserRegisterStep1Response(
            temp_token=temp_token,
            totp_secret=totp_secret,
            totp_uri=totp_uri,
            qr_code_base64=qr_code_base64,
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"注册第一步失败: {e}")
        raise HTTPException(status_code=500, detail=safe_error_detail(e, "注册"))


@router.post("/register/step2")
async def register_step2(req: UserRegisterStep2Request, response: Response):
    """
    注册第二步：验证 TOTP 验证码，完成注册
    
    用户需要从 Google Authenticator 获取 6 位验证码
    """
    # 检查注册是否开放
    if not config_loader.is_registration_enabled():
        clear_pending_registration(req.temp_token)
        raise HTTPException(status_code=403, detail="注册功能已关闭，请联系管理员")
    
    # 获取待完成的注册信息
    pending = get_pending_registration(req.temp_token)
    if not pending:
        raise HTTPException(status_code=400, detail="注册会话已过期或无效，请重新注册")
    
    # 验证 TOTP 验证码
    if not verify_totp(pending["totp_secret"], req.totp_code):
        raise HTTPException(status_code=400, detail="验证码错误，请检查 Google Authenticator")
    
    # 生成 uid
    uid = generate_uid()
    password_hash = hash_password(pending["password"])
    totp_secret_encrypted = config_loader.crypto.encrypt(pending["totp_secret"])
    
    try:
        with config_loader._get_connection() as conn:
            # 再次检查用户名是否存在（防止并发）
            if hasattr(conn, 'execute'):
                from sqlalchemy import text
                existing = conn.execute(text(
                    "SELECT uid FROM users WHERE username = :username"
                ), {"username": pending["username"]}).fetchone()
                
                if existing:
                    clear_pending_registration(req.temp_token)
                    raise HTTPException(status_code=400, detail="用户名已存在")
                
                # 插入用户（包含 TOTP 密钥）
                conn.execute(text("""
                    INSERT INTO users (uid, username, password_hash, email, totp_secret_encrypted, status, tier)
                    VALUES (:uid, :username, :password_hash, :email, :totp_secret_encrypted, 'active', 'free')
                """), {
                    "uid": uid,
                    "username": pending["username"],
                    "password_hash": password_hash,
                    "email": pending["email"],
                    "totp_secret_encrypted": totp_secret_encrypted,
                })
                
                # 插入默认配置
                conn.execute(text("""
                    INSERT INTO user_trading_config (uid)
                    VALUES (:uid)
                """), {"uid": uid})
            else:
                # SQLite
                cursor = conn.execute(
                    "SELECT uid FROM users WHERE username = ?",
                    (pending["username"],)
                )
                if cursor.fetchone():
                    clear_pending_registration(req.temp_token)
                    raise HTTPException(status_code=400, detail="用户名已存在")
                
                conn.execute("""
                    INSERT INTO users (uid, username, password_hash, email, totp_secret_encrypted, status, tier)
                    VALUES (?, ?, ?, ?, ?, 'active', 'free')
                """, (uid, pending["username"], password_hash, pending["email"], totp_secret_encrypted))
                
                conn.execute("""
                    INSERT INTO user_trading_config (uid) VALUES (?)
                """, (uid,))
        
        # 清除临时注册信息
        clear_pending_registration(req.temp_token)
        
        # 绑定邀请人（如果提供了邀请码）
        if pending.get("referral_code"):
            try:
                from core.referral_db import referral_db
                referral_db.generate_referral_code(uid)
                success, msg = referral_db.bind_referrer(uid, pending["referral_code"])
                if success:
                    logger.info(f"[{uid}] 注册时绑定邀请码: {pending['referral_code']}")
                else:
                    logger.warning(f"[{uid}] 绑定邀请码失败: {msg}")
            except Exception as e:
                logger.warning(f"[{uid}] 绑定邀请码异常: {e}")
        else:
            try:
                from core.referral_db import referral_db
                referral_db.generate_referral_code(uid)
            except Exception as e:
                logger.warning(f"[{uid}] 生成邀请码失败: {e}")
        
        # 生成 token
        token, expires_at = create_jwt_token(uid, pending["username"])
        
        # 设置 HttpOnly Cookie (auth token)
        response.set_cookie(
            key=COOKIE_NAME,
            value=token,
            max_age=COOKIE_MAX_AGE,
            path="/",  # 确保 Cookie 对所有路径有效
            httponly=True,
            secure=COOKIE_SECURE,
            samesite=COOKIE_SAMESITE,
        )
        
        # 设置 CSRF Token Cookie (前端可读，用于发送到 Header)
        csrf_token = generate_csrf_token()
        response.set_cookie(
            key=CSRF_COOKIE_NAME,
            value=csrf_token,
            max_age=COOKIE_MAX_AGE,
            path="/",
            httponly=False,  # 前端需要读取并发送到 Header
            secure=COOKIE_SECURE,
            samesite=COOKIE_SAMESITE,
        )
        
        logger.info(f"用户注册成功: {pending['username']} (uid={uid})")
        
        return {
            "uid": uid,
            "username": pending["username"],
            "token": token,
            "expires_at": expires_at,
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"注册第二步失败: {e}")
        raise HTTPException(status_code=500, detail=safe_error_detail(e, "注册"))


@router.post("/register", response_model=UserLoginResponse, deprecated=True)
async def register(req: UserRegisterRequest):
    """
    用户注册（旧版一步注册，已废弃）
    
    请使用 /register/step1 和 /register/step2 进行两步注册
    """
    raise HTTPException(
        status_code=400, 
        detail="请使用新的两步注册流程: 先调用 /register/step1 获取二维码，然后调用 /register/step2 完成注册"
    )


@router.post("/login")
async def login(req: UserLoginRequest, request: Request, response: Response):
    """
    用户登录
    
    需要提供用户名、密码和 Google Authenticator 验证码
    
    安全特性:
    - 设置 HttpOnly Cookie 存储 token (防止 XSS)
    - 同时返回 token 用于 API 调用（可选使用）
    - P1 安全: 登录限速保护 (防止暴力破解)
    """
    # 清理用户名前后空格
    req.username = req.username.strip()
    
    if not TOTP_AVAILABLE:
        raise HTTPException(status_code=500, detail="TOTP 模块未安装，请联系管理员")
    
    # P1 安全: 登录限速检查
    client_ip = request.client.host if request.client else "unknown"
    allowed, remaining = check_login_rate_limit(client_ip, req.username)
    if not allowed:
        logger.warning(f"登录限速触发: ip={client_ip}, username={req.username}, 剩余锁定={remaining}秒")
        raise HTTPException(
            status_code=429, 
            detail=f"登录尝试次数过多，请在 {remaining} 秒后重试"
        )
    
    try:
        with config_loader._get_connection() as conn:
            if hasattr(conn, 'execute'):
                from sqlalchemy import text
                row = conn.execute(text("""
                    SELECT uid, username, password_hash, totp_secret_encrypted, status
                    FROM users WHERE username = :username
                """), {"username": req.username}).fetchone()
            else:
                cursor = conn.execute("""
                    SELECT uid, username, password_hash, totp_secret_encrypted, status
                    FROM users WHERE username = ?
                """, (req.username,))
                row = cursor.fetchone()
        
        result = _row_to_dict(row)
        
        if not result:
            raise HTTPException(status_code=401, detail="用户名或密码错误")
        
        if result["status"] != "active":
            raise HTTPException(status_code=403, detail="账户已被禁用")
        
        if not verify_password(req.password, result["password_hash"]):
            raise HTTPException(status_code=401, detail="用户名或密码错误")
        
        # P1 安全升级：自动迁移旧 SHA256 密码到 bcrypt
        # 在密码验证成功后，检查是否需要升级哈希算法
        old_hash = result["password_hash"]
        if BCRYPT_AVAILABLE and not old_hash.startswith('$2'):
            try:
                new_hash = hash_password(req.password)
                with config_loader._get_connection() as conn:
                    if hasattr(conn, 'execute'):
                        from sqlalchemy import text
                        conn.execute(text("""
                            UPDATE users SET password_hash = :new_hash WHERE uid = :uid
                        """), {"new_hash": new_hash, "uid": result["uid"]})
                        conn.commit()
                    else:
                        conn.execute("""
                            UPDATE users SET password_hash = ? WHERE uid = ?
                        """, (new_hash, result["uid"]))
                        conn.commit()
                logger.info(f"用户 {result['username']} 密码哈希已从 SHA256 升级到 bcrypt")
            except Exception as e:
                # 升级失败不影响登录，仅记录警告
                logger.warning(f"密码哈希升级失败 (用户 {result['username']}): {e}")
        
        # 验证 TOTP
        totp_secret_encrypted = result.get("totp_secret_encrypted")
        if not totp_secret_encrypted:
            raise HTTPException(status_code=400, detail="账户未绑定 Google Authenticator，请联系管理员")
        
        try:
            totp_secret = config_loader.crypto.decrypt(totp_secret_encrypted)
        except Exception:
            raise HTTPException(status_code=500, detail="TOTP 密钥解密失败")
        
        if not verify_totp(totp_secret, req.totp_code):
            raise HTTPException(status_code=401, detail="Google Authenticator 验证码错误")
        
        token, expires_at = create_jwt_token(result["uid"], result["username"])
        
        # 设置 HttpOnly Cookie (认证用)
        response.set_cookie(
            key=COOKIE_NAME,
            value=token,
            max_age=COOKIE_MAX_AGE,
            path="/",  # 确保 Cookie 对所有路径有效
            httponly=True,  # 防止 JS 访问
            secure=COOKIE_SECURE,  # 生产环境应开启 HTTPS
            samesite=COOKIE_SAMESITE,
        )
        
        # 设置 CSRF Token Cookie (前端可读，用于发送到 Header)
        csrf_token = generate_csrf_token()
        response.set_cookie(
            key=CSRF_COOKIE_NAME,
            value=csrf_token,
            max_age=COOKIE_MAX_AGE,
            path="/",
            httponly=False,  # 前端需要读取并发送到 Header
            secure=COOKIE_SECURE,
            samesite=COOKIE_SAMESITE,
        )
        
        logger.info(f"用户登录成功: {result['username']}")
        
        # P1 安全: 登录成功，清除限速记录
        clear_login_attempts(client_ip, req.username)
        
        # 返回用户信息（token 仍然返回，但前端应优先使用 Cookie）
        return {
            "uid": result["uid"],
            "username": result["username"],
            "token": token,  # 保留用于向后兼容和 API 调用
            "expires_at": expires_at,
        }
        
    except HTTPException as he:
        # P1 安全: 登录失败 (401/403)，记录失败
        if he.status_code in (401, 403):
            record_login_failure(client_ip, req.username)
        raise
    except Exception as e:
        logger.error(f"登录失败: {e}")
        raise HTTPException(status_code=500, detail=safe_error_detail(e, "登录"))


@router.post("/logout")
async def logout(response: Response):
    """
    用户登出
    
    清除认证 Cookie 和 CSRF Cookie
    """
    response.delete_cookie(
        key=COOKIE_NAME,
        path="/",  # 必须与设置时的 path 一致
        httponly=True,
        secure=COOKIE_SECURE,
        samesite=COOKIE_SAMESITE,
    )
    response.delete_cookie(
        key=CSRF_COOKIE_NAME,
        path="/",
        httponly=False,
        secure=COOKIE_SECURE,
        samesite=COOKIE_SAMESITE,
    )
    return {"message": "已登出"}


class ResetPasswordRequest(BaseModel):
    username: str
    totp_code: str
    new_password: str


@router.post("/reset-password")
async def reset_password(req: ResetPasswordRequest):
    """
    通过 TOTP 验证码重置密码
    
    用户忘记密码时，可通过 Google Authenticator 验证码验证身份后重置密码
    """
    # 清理用户名前后空格
    req.username = req.username.strip()
    
    try:
        # 验证新密码长度
        if len(req.new_password) < 6:
            raise HTTPException(status_code=400, detail="密码至少需要6位")
        
        # 查询用户
        with config_loader._get_connection() as conn:
            if hasattr(conn, 'execute'):
                from sqlalchemy import text
                result = conn.execute(text("""
                    SELECT uid, username, totp_secret_encrypted, status
                    FROM users WHERE username = :username
                """), {"username": req.username}).mappings().first()
            else:
                cursor = conn.execute("""
                    SELECT uid, username, totp_secret_encrypted, status
                    FROM users WHERE username = ?
                """, (req.username,))
                row = cursor.fetchone()
                result = dict(zip([d[0] for d in cursor.description], row)) if row else None
        
        if not result:
            raise HTTPException(status_code=404, detail="用户不存在")
        
        if result.get("status") != "active":
            raise HTTPException(status_code=403, detail="账户已被禁用")
        
        # 验证 TOTP
        totp_secret_encrypted = result.get("totp_secret_encrypted")
        if not totp_secret_encrypted:
            raise HTTPException(status_code=400, detail="账户未绑定 Google Authenticator")
        
        try:
            totp_secret = config_loader.crypto.decrypt(totp_secret_encrypted)
        except Exception:
            raise HTTPException(status_code=500, detail="TOTP 密钥解密失败")
        
        if not verify_totp(totp_secret, req.totp_code):
            raise HTTPException(status_code=401, detail="Google Authenticator 验证码错误")
        
        # 更新密码
        new_password_hash = hash_password(req.new_password)
        
        with config_loader._get_connection() as conn:
            if hasattr(conn, 'execute'):
                from sqlalchemy import text
                conn.execute(text("""
                    UPDATE users SET password_hash = :password_hash WHERE uid = :uid
                """), {"password_hash": new_password_hash, "uid": result["uid"]})
            else:
                conn.execute("""
                    UPDATE users SET password_hash = ? WHERE uid = ?
                """, (new_password_hash, result["uid"]))
        
        logger.info(f"用户 {req.username} 通过 TOTP 重置密码成功")
        
        return {"message": "密码重置成功"}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"重置密码失败: {e}")
        raise HTTPException(status_code=500, detail=safe_error_detail(e, "重置密码"))


@router.get("/config", response_model=UserConfigResponse)
async def get_config(user: Dict = Depends(get_current_user)):
    """获取用户配置"""
    uid = user["uid"]
    username = user.get("username")  # 从 JWT payload 获取用户名
    
    config = config_loader.load(uid, use_cache=False)
    if not config:
        raise HTTPException(status_code=404, detail="用户配置不存在")
    
    # 检查是否正在运行（内存状态）
    ctx = context_manager._contexts.get(uid)
    is_running_in_memory = ctx.is_running if ctx else False
    
    # 检查数据库中的持久化状态
    is_trading_enabled = config_loader.is_trading_enabled(uid)
    
    # 系统重启后会自动恢复 trading_enabled=1 的用户
    # 这里返回实际的内存运行状态
    is_running = is_running_in_memory
    
    return UserConfigResponse(
        uid=config.uid,
        username=username,  # 返回用户名，用于前端 sessionStorage 恢复
        max_positions=config.max_positions,
        position_size_pct=config.position_size_pct,
        default_leverage=config.default_leverage,
        monitor_symbols=config.monitor_symbols or [],
        ai500_enabled=getattr(config, 'ai500_enabled', False),
        # 执行约束配置
        min_rr_ratio=getattr(config, 'min_rr_ratio', 3.0),
        limit_order_min_distance_pct=getattr(config, 'limit_order_min_distance_pct', 3.0),
        ai_enabled=config.ai_enabled,
        llm_provider=config.llm_provider,
        llm_model=config.llm_model,
        has_custom_llm_key=bool(config.llm_api_key),
        system_prompt=None,  # v5.0: 已迁移到 user_ai_strategies，使用 /ai-strategies 端点
        llm_base_url=config.llm_base_url,
        temperature=None,  # 已废弃，由 llm_client 根据提供商自动设置
        max_tokens=config.max_tokens,
        telegram_enabled=config.telegram_enabled,
        has_telegram_bot_token=bool(config.telegram_bot_token),
        has_telegram_chat_id=bool(config.telegram_chat_id),
        has_telegram_topic_id=bool(getattr(config, 'telegram_topic_id', None)),
        has_api_keys=len(config.enabled_exchanges) > 0,  # 检查是否有启用的交易所
        is_testnet=False,  # 不再使用全局 testnet 标志
        tier=config.tier,
        is_running=is_running,
    )


async def _init_user_equity(uid: str, api_key: str, api_secret: str, is_testnet: bool):
    """
    获取并保存用户的初始权益（基准资金和账户余额）
    
    在用户首次配置 API Key 后调用
    格式与 cycle_store.py 保持一致
    """
    from binance.client import Client
    from core.pf_compatibility import pf_compat
    from decimal import Decimal
    
    def _d_to_str(x) -> str:
        """Decimal 转字符串，去除尾随零"""
        if x is None:
            return "0"
        d = Decimal(str(x))
        return format(d.normalize(), 'f')
    
    try:
        # 创建临时 client（带限速）
        from core.rate_limiter import get_binance_rate_limiter
        
        client = Client(api_key, api_secret, testnet=is_testnet)
        
        # 获取限速许可
        rate_limiter = get_binance_rate_limiter(api_key)
        rate_limiter.acquire(endpoint="futures_account", timeout=30.0)
        
        # 获取账户信息
        account_data = client.futures_account()
        
        wallet_balance = Decimal(str(account_data.get("totalWalletBalance", 0)))
        unrealized = Decimal(str(account_data.get("totalUnrealizedProfit", 0)))
        equity = wallet_balance + unrealized
        
        ts = int(time.time() * 1000)
        
        # 保存初始权益（格式与 cycle_store.py 一致）
        equity_init = {
            "uid": uid,
            "ts": str(ts),
            "walletBalance": _d_to_str(equity),
            "source": "API_KEY_INIT",
            "exchange": "binance",  # 交易所标识
        }
        pf_compat.set_pf_equity_init(uid, equity_init)
        
        # 保存账户快照（格式与 cycle_store.py 一致）
        account_obj = {
            "uid": uid,
            "ts": str(ts),
            "walletBalance": _d_to_str(wallet_balance),
            "equity": _d_to_str(equity),
            "unrealized": _d_to_str(unrealized),
            "source": "API_KEY_INIT",
            "exchange": "binance",  # 交易所标识
        }
        pf_compat.set_pf_account(uid, account_obj)
        
        logger.info(f"[{uid}] 初始权益已保存: walletBalance={wallet_balance}, equity={equity}")
        
    except Exception as e:
        logger.error(f"[{uid}] 获取初始权益异常: {e}")
        raise


async def _fetch_and_save_account_data(uid: str, exchange: str, exchange_config: Dict):
    """
    主动拉取交易所账户数据并保存到 Redis
    
    用于启动交易所时立即获取账户信息，而不是等待 WebSocket 推送
    """
    from core.pf_compatibility import pf_compat
    from decimal import Decimal
    import time
    
    def _d_to_str(x) -> str:
        if x is None:
            return "0"
        d = Decimal(str(x))
        return format(d.normalize(), 'f')
    
    ts = int(time.time() * 1000)
    
    try:
        if exchange == "binance":
            from binance.client import Client
            from core.rate_limiter import get_binance_rate_limiter
            
            client = Client(
                exchange_config['api_key'],
                exchange_config['api_secret'],
                testnet=exchange_config.get('is_testnet', False)
            )
            
            # 获取限速许可
            rate_limiter = get_binance_rate_limiter(exchange_config['api_key'])
            rate_limiter.acquire(endpoint="futures_account", timeout=30.0)
            
            account_data = client.futures_account()
            wallet = Decimal(str(account_data.get("totalWalletBalance", 0)))
            unrealized = Decimal(str(account_data.get("totalUnrealizedProfit", 0)))
            equity = wallet + unrealized
            
        elif exchange == "okx":
            from exchanges.okx_exchange import OKXExchange
            client = OKXExchange(
                api_key=exchange_config['api_key'],
                api_secret=exchange_config['api_secret'],
                passphrase=exchange_config.get('passphrase'),
                is_testnet=exchange_config.get('is_testnet', False)
            )
            balance = await client.get_balance()
            await client.close()
            wallet = Decimal(str(balance))
            equity = wallet
            unrealized = Decimal("0")
            
        elif exchange == "bitget":
            from exchanges.bitget_exchange import BitgetExchange
            client = BitgetExchange(
                api_key=exchange_config['api_key'],
                api_secret=exchange_config['api_secret'],
                passphrase=exchange_config.get('passphrase'),
                is_testnet=exchange_config.get('is_testnet', False)
            )
            balance = await client.get_balance()
            await client.close()
            wallet = Decimal(str(balance))
            equity = wallet
            unrealized = Decimal("0")
            
        elif exchange == "hyperliquid":
            from exchanges.hyperliquid_exchange import HyperliquidExchange
            client = HyperliquidExchange(
                api_key=exchange_config['api_key'],
                api_secret=exchange_config['api_secret'],
                is_testnet=exchange_config.get('is_testnet', False),
                wallet_address=exchange_config.get('wallet_address')
            )
            balance = await client.get_balance()
            await client.close()
            wallet = Decimal(str(balance))
            equity = wallet
            unrealized = Decimal("0")
        else:
            logger.warning(f"[{uid}] 不支持的交易所: {exchange}")
            return
        
        # 保存账户数据
        account_obj = {
            "uid": uid,
            "ts": str(ts),
            "walletBalance": _d_to_str(wallet),
            "equity": _d_to_str(equity),
            "unrealized": _d_to_str(unrealized),
            "source": f"{exchange.upper()}_START",
            "exchange": exchange,
        }
        pf_compat.set_pf_account(uid, account_obj, exchange)
        
        # 如果没有初始权益，也保存一份
        equity_init = pf_compat.get_pf_equity_init(uid, exchange)
        if not equity_init:
            equity_init_obj = {
                "uid": uid,
                "ts": str(ts),
                "walletBalance": _d_to_str(equity),
                "source": f"{exchange.upper()}_START_INIT",
                "exchange": exchange,
            }
            pf_compat.set_pf_equity_init(uid, equity_init_obj, exchange)
            logger.info(f"[{uid}][{exchange}] 初始权益已保存: {equity}")
        
        logger.info(f"[{uid}][{exchange}] 账户数据已保存: wallet={wallet}, equity={equity}")
        
    except Exception as e:
        logger.error(f"[{uid}][{exchange}] 拉取账户数据失败: {e}")
        raise


@router.put("/trading-config")
async def update_trading_config(
    req: TradingConfigUpdateRequest,
    user: Dict = Depends(get_current_user)
):
    """更新交易配置"""
    uid = user["uid"]
    
    # P2 Fix: SQL 字段白名单
    ALLOWED_TRADING_CONFIG_FIELDS = {
        "max_positions", "position_size_pct", "default_leverage", "monitor_symbols",
        "min_rr_ratio", "limit_order_min_distance_pct"
    }
    
    try:
        updates = {}
        if req.max_positions is not None:
            updates["max_positions"] = req.max_positions
        if req.position_size_pct is not None:
            updates["position_size_pct"] = req.position_size_pct
        if req.default_leverage is not None:
            updates["default_leverage"] = req.default_leverage
        if req.monitor_symbols is not None:
            # 限制最多 20 个币种
            if len(req.monitor_symbols) > 20:
                raise HTTPException(status_code=400, detail="最多只能监控 20 个币种")
            updates["monitor_symbols"] = json.dumps(req.monitor_symbols)
        if req.ai500_enabled is not None:
            updates["ai500_enabled"] = 1 if req.ai500_enabled else 0
        # 执行约束配置
        if req.min_rr_ratio is not None:
            if req.min_rr_ratio < 1 or req.min_rr_ratio > 10:
                raise HTTPException(status_code=400, detail="风险回报比必须在 1-10 之间")
            updates["min_rr_ratio"] = req.min_rr_ratio
        if req.limit_order_min_distance_pct is not None:
            if req.limit_order_min_distance_pct < 0.1 or req.limit_order_min_distance_pct > 20:
                raise HTTPException(status_code=400, detail="限价单距离必须在 0.1%-20% 之间")
            updates["limit_order_min_distance_pct"] = req.limit_order_min_distance_pct
        
        if not updates:
            return {"message": "无更新内容"}
        
        # P2 Fix: 验证所有字段都在白名单中
        for field in updates.keys():
            if field not in ALLOWED_TRADING_CONFIG_FIELDS:
                raise HTTPException(status_code=400, detail=f"不允许更新字段: {field}")
        
        # 构建 SQL
        set_clause = ", ".join([f"{k} = :{k}" for k in updates.keys()])
        updates["uid"] = uid
        
        with config_loader._get_connection() as conn:
            if hasattr(conn, 'execute'):
                from sqlalchemy import text
                conn.execute(text(f"""
                    UPDATE user_trading_config SET {set_clause} WHERE uid = :uid
                """), updates)
            else:
                set_clause_sqlite = ", ".join([f"{k} = ?" for k in updates.keys() if k != "uid"])
                values = [v for k, v in updates.items() if k != "uid"] + [uid]
                conn.execute(f"""
                    UPDATE user_trading_config SET {set_clause_sqlite} WHERE uid = ?
                """, values)
        
        # 清除配置缓存
        config_loader._cache.pop(uid, None)
        
        # 重新加载配置
        new_config = config_loader.load(uid, use_cache=False)
        
        # 刷新 UserContext 中的配置（如果存在）
        ctx = context_manager._contexts.get(uid)
        if ctx and new_config:
            ctx.config = new_config
            logger.info(f"[{uid}] 已刷新 UserContext 交易配置")
        
        # ✅ 同步刷新 AsyncUserContext 的配置
        # AsyncUserContext 是 LLM 调用使用的上下文，必须同步更新
        try:
            from core.async_user_context import async_context_manager
            async_ctx = async_context_manager._contexts.get(uid)
            if async_ctx and new_config:
                async_ctx.config = new_config
                logger.info(f"[{uid}] 已刷新 AsyncUserContext 交易配置")
        except Exception as e:
            logger.warning(f"[{uid}] 刷新 AsyncUserContext 交易配置失败: {e}")
        
        return {"message": "交易配置已更新"}
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=safe_error_detail(e, "更新"))


@router.put("/llm-config")
async def update_llm_config(
    req: LLMConfigUpdateRequest,
    user: Dict = Depends(get_current_user)
):
    """更新 LLM 配置"""
    uid = user["uid"]
    # P2 Fix: SQL 字段白名单
    ALLOWED_LLM_CONFIG_FIELDS = {
        "ai_enabled", "llm_provider", "llm_model", "llm_api_key_encrypted",
        "llm_base_url", "system_prompt", "temperature", "max_tokens"
    }
    
    try:
        updates = {}
        if req.ai_enabled is not None:
            updates["ai_enabled"] = 1 if req.ai_enabled else 0
        if req.llm_provider is not None:
            updates["llm_provider"] = req.llm_provider
        if req.llm_model is not None:
            updates["llm_model"] = req.llm_model
        if req.llm_api_key is not None:
            # 加密存储
            updates["llm_api_key_encrypted"] = config_loader.crypto.encrypt(req.llm_api_key)
        if req.llm_base_url is not None:
            updates["llm_base_url"] = req.llm_base_url
        if req.system_prompt is not None:
            updates["system_prompt"] = req.system_prompt
        if req.temperature is not None:
            updates["temperature"] = req.temperature
        if req.max_tokens is not None:
            updates["max_tokens"] = req.max_tokens
        
        if not updates:
            return {"message": "无更新内容"}
        
        # P2 Fix: 验证所有字段都在白名单中
        for field in updates.keys():
            if field not in ALLOWED_LLM_CONFIG_FIELDS:
                raise HTTPException(status_code=400, detail=f"不允许更新字段: {field}")
        
        set_clause = ", ".join([f"{k} = :{k}" for k in updates.keys()])
        updates["uid"] = uid
        
        with config_loader._get_connection() as conn:
            if hasattr(conn, 'execute'):
                from sqlalchemy import text
                conn.execute(text(f"""
                    UPDATE user_trading_config SET {set_clause} WHERE uid = :uid
                """), updates)
            else:
                set_clause_sqlite = ", ".join([f"{k} = ?" for k in updates.keys() if k != "uid"])
                values = [v for k, v in updates.items() if k != "uid"] + [uid]
                conn.execute(f"""
                    UPDATE user_trading_config SET {set_clause_sqlite} WHERE uid = ?
                """, values)
        
        # 清除缓存
        config_loader._cache.pop(uid, None)
        
        # 更新 UserContext 的配置并重置 LLM 客户端
        new_config = config_loader.load(uid, use_cache=False)
        ctx = context_manager._contexts.get(uid)
        if ctx:
            # 重新加载配置到 ctx.config
            if new_config:
                ctx.config = new_config
            # 重置 LLM 客户端
            ctx.reset_llm_client()
        
        # ✅ 同步更新 AsyncUserContext 的配置和 LLM 客户端
        # AsyncUserContext 有独立的 _llm_client 缓存，需要单独处理
        try:
            from core.async_user_context import async_context_manager
            async_ctx = async_context_manager._contexts.get(uid)
            if async_ctx:
                # 更新配置
                if new_config:
                    async_ctx.config = new_config
                # 重置异步 LLM 客户端
                if async_ctx._llm_client:
                    try:
                        import asyncio
                        # 尝试获取当前事件循环
                        try:
                            loop = asyncio.get_running_loop()
                            # 在运行的循环中创建任务关闭客户端
                            loop.create_task(async_ctx._llm_client.close())
                        except RuntimeError:
                            # 没有运行的循环，创建新的来执行
                            asyncio.run(async_ctx._llm_client.close())
                    except Exception as e:
                        logger.debug(f"[{uid}] 关闭 AsyncUserContext LLM 客户端: {e}")
                    finally:
                        async_ctx._llm_client = None
                logger.info(f"[{uid}] 已刷新 AsyncUserContext LLM 配置")
        except Exception as e:
            logger.warning(f"[{uid}] 刷新 AsyncUserContext 失败: {e}")
        
        return {"message": "LLM 配置已更新"}
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=safe_error_detail(e, "更新"))


@router.put("/notification-config")
async def update_notification_config(
    req: NotificationConfigUpdateRequest,
    user: Dict = Depends(get_current_user)
):
    """更新通知配置"""
    uid = user["uid"]
    
    # P2 安全: 通知配置字段白名单
    ALLOWED_NOTIFICATION_FIELDS = {
        "telegram_bot_token_encrypted",
        "telegram_chat_id",
        "telegram_topic_id",
        "telegram_enabled",
    }
    
    try:
        updates = {}
        # Bot Token 需要加密存储
        if req.telegram_bot_token is not None:
            encrypted_token = config_loader._encrypt(req.telegram_bot_token) if req.telegram_bot_token else None
            updates["telegram_bot_token_encrypted"] = encrypted_token
        if req.telegram_chat_id is not None:
            # 空字符串表示清除，strip() 去除前后空格
            chat_id = req.telegram_chat_id.strip() if req.telegram_chat_id else None
            updates["telegram_chat_id"] = chat_id if chat_id else None
        if req.telegram_topic_id is not None:
            # 空字符串表示清除，strip() 去除前后空格
            topic_id = req.telegram_topic_id.strip() if req.telegram_topic_id else None
            updates["telegram_topic_id"] = topic_id if topic_id else None
        if req.telegram_enabled is not None:
            updates["telegram_enabled"] = 1 if req.telegram_enabled else 0
        
        if not updates:
            return {"message": "无更新内容"}
        
        # P2 安全: 验证所有字段都在白名单中
        for field in updates.keys():
            if field not in ALLOWED_NOTIFICATION_FIELDS:
                logger.warning(f"[{uid}] 尝试更新非法通知配置字段: {field}")
                raise HTTPException(status_code=400, detail=f"不允许更新字段: {field}")
        
        set_clause = ", ".join([f"{k} = :{k}" for k in updates.keys()])
        updates["uid"] = uid
        
        with config_loader._get_connection() as conn:
            if hasattr(conn, 'execute'):
                from sqlalchemy import text
                conn.execute(text(f"""
                    UPDATE user_trading_config SET {set_clause} WHERE uid = :uid
                """), updates)
            else:
                set_clause_sqlite = ", ".join([f"{k} = ?" for k in updates.keys() if k != "uid"])
                values = [v for k, v in updates.items() if k != "uid"] + [uid]
                conn.execute(f"""
                    UPDATE user_trading_config SET {set_clause_sqlite} WHERE uid = ?
                """, values)
        
        # 清除缓存
        config_loader._cache.pop(uid, None)
        
        # ✅ 刷新 UserContext 和 AsyncUserContext 的配置
        # 这样用户更新 Telegram 配置后，通知服务会使用新的配置
        new_config = config_loader.load(uid, use_cache=False)
        
        # 刷新 UserContext
        ctx = context_manager._contexts.get(uid)
        if ctx and new_config:
            ctx.config = new_config
            logger.info(f"[{uid}] 已刷新 UserContext 通知配置")
        
        # 刷新 AsyncUserContext
        try:
            from core.async_user_context import async_context_manager
            async_ctx = async_context_manager._contexts.get(uid)
            if async_ctx and new_config:
                async_ctx.config = new_config
                logger.info(f"[{uid}] 已刷新 AsyncUserContext 通知配置")
        except Exception as e:
            logger.warning(f"[{uid}] 刷新 AsyncUserContext 通知配置失败: {e}")
        
        return {"message": "通知配置已更新"}
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=safe_error_detail(e, "更新"))


@router.get("/status")
async def get_status(user: Dict = Depends(get_current_user)):
    """获取用户交易状态"""
    uid = user["uid"]
    
    ctx = context_manager._contexts.get(uid)
    
    # 如果没有上下文，尝试直接从 API 获取账户信息
    if not ctx:
        # 检查是否有配置
        config = config_loader.load(uid)
        if not config or not config.enabled_exchanges:
            return {
                "is_running": False,
                "has_context": False,
                "account_snapshot": None,
                "error_count": 0,
                "message": "请先配置交易所 API"
            }
        
        # 尝试从 Binance 获取账户信息
        binance_config = config_loader.get_user_exchange_config(uid, 'binance')
        if not binance_config or not binance_config.get('api_key'):
            return {
                "is_running": False,
                "has_context": False,
                "account_snapshot": None,
                "error_count": 0,
                "message": "请先配置 Binance API（用于获取账户状态）"
            }
        
        # 直接调用 API 获取账户信息
        try:
            from binance.client import Client
            from core.rate_limiter import get_binance_rate_limiter
            
            client = Client(
                api_key=binance_config['api_key'],
                api_secret=binance_config['api_secret'],
                testnet=binance_config.get('is_testnet', False)
            )
            
            # 获取限速许可
            rate_limiter = get_binance_rate_limiter(binance_config['api_key'])
            rate_limiter.acquire(endpoint="futures_account", timeout=30.0)
            
            account = client.futures_account()
            
            balance = float(account.get("totalWalletBalance", 0))
            available = float(account.get("availableBalance", 0))
            unrealized = float(account.get("totalUnrealizedProfit", 0))
            
            # 获取持仓
            positions = [
                p for p in account.get("positions", [])
                if float(p.get("positionAmt", 0)) != 0
            ]
            
            return {
                "is_running": False,
                "has_context": False,
                "error_count": 0,
                "account_snapshot": {
                    "balance": balance,
                    "available": available,
                    "total_unrealized": unrealized,
                    "positions_count": len(positions),
                    "orders_count": 0,
                },
                "monitor_symbols": config.monitor_symbols or [],
            }
        except Exception as e:
            logger.warning(f"[{uid}] 获取账户信息失败: {e}")
            return {
                "is_running": False,
                "has_context": False,
                "account_snapshot": None,
                "error_count": 0,
                "message": "获取账户信息失败"
            }
    
    # 总是刷新账户状态以确保数据最新
    try:
        from trading.account_positions import get_account_status_for_user
        snapshot = get_account_status_for_user(ctx)
        if snapshot:
            ctx.account_snapshot = snapshot
    except Exception as e:
        logger.warning(f"[{uid}] 刷新账户状态失败: {e}")
    
    return {
        "is_running": ctx.is_running,
        "has_context": True,
        "last_active_at": ctx.last_active_at,
        "error_count": ctx.error_count,
        "account_snapshot": {
            "balance": ctx.account_snapshot.get("balance", 0),
            "available": ctx.account_snapshot.get("available", 0),
            "total_unrealized": ctx.account_snapshot.get("total_unrealized", 0),
            "positions_count": len(ctx.account_snapshot.get("positions", [])),
            "orders_count": len(ctx.account_snapshot.get("open_limit_orders", [])),
        },
        "monitor_symbols": ctx.get_monitor_symbols(),
    }


@router.post("/start")
async def start_trading(user: Dict = Depends(get_current_user)):
    """启动交易（启动所有已启用的交易所）"""
    uid = user["uid"]
    username = user.get("username", "")
    
    # 检查配置
    config = config_loader.load(uid)
    if not config:
        raise HTTPException(status_code=400, detail="用户配置不存在")
    
    if not config.enabled_exchanges:
        raise HTTPException(status_code=400, detail="请先配置并启用至少一个交易所")
    
    # 检查每个已启用的交易所，如果有测试网配置，非管理员不允许启动
    for exchange in config.enabled_exchanges:
        exchange_config = config_loader.get_user_exchange_config(uid, exchange)
        if exchange_config and exchange_config.get("is_testnet") and username not in ADMIN_USERS:
            from exchanges import EXCHANGE_NAMES
            raise HTTPException(
                status_code=403, 
                detail=f"{EXCHANGE_NAMES.get(exchange, exchange)} 配置了测试网模式，测试网交易仅管理员可用。请关闭测试网模式后再启动交易。"
            )
    
    # 检查是否配置了监控币种或启用了 AI500
    if not config.monitor_symbols and not config.ai500_enabled:
        raise HTTPException(status_code=400, detail="请先配置监控币种或启用 AI500 智能选币")
    
    # 检查是否已有初始权益，如果没有则初始化
    from core.pf_compatibility import pf_compat
    equity_init = pf_compat.get_pf_equity_init(uid)
    if not equity_init:
        # 尝试用 Binance 初始化权益
        binance_config = config_loader.get_user_exchange_config(uid, 'binance')
        if binance_config and binance_config.get('api_key'):
            try:
                await _init_user_equity(uid, binance_config['api_key'], binance_config['api_secret'], binance_config.get('is_testnet', False))
                logger.info(f"[{uid}] 首次启动，已初始化基准权益")
            except Exception as e:
                logger.warning(f"[{uid}] 初始化权益失败: {e}")
    
    # 检查是否已有上下文但未运行，需要先移除再重新创建
    existing_ctx = context_manager._contexts.get(uid)
    if existing_ctx and not existing_ctx.is_running:
        logger.info(f"[{uid}] 发现已停止的上下文，移除后重新创建")
        context_manager.remove_context(uid)
    
    # 获取或创建上下文
    ctx = context_manager.get_context(uid, auto_start=True)
    if not ctx:
        raise HTTPException(status_code=500, detail="创建用户上下文失败")
    
    # 如果上下文存在但未运行，手动启动
    if not ctx.is_running:
        ctx.start()
    
    # 持久化交易启用状态到数据库
    config_loader.set_trading_enabled(uid, True)
    
    # 注册到调度器（使用统一函数，同时注册到新旧调度器）
    try:
        from core.async_multi_user_scheduler import register_user_to_schedulers
        register_user_to_schedulers(uid, config.tier)
    except Exception as e:
        logger.warning(f"注册用户到调度器失败: {e}")
    
    return {"message": "交易已启动", "is_running": ctx.is_running}


@router.post("/stop")
async def stop_trading(user: Dict = Depends(get_current_user)):
    """停止交易"""
    uid = user["uid"]
    
    # 持久化交易停止状态到数据库
    config_loader.set_trading_enabled(uid, False)
    
    # 从调度器移除（使用统一函数）
    try:
        from core.async_multi_user_scheduler import unregister_user_from_schedulers
        unregister_user_from_schedulers(uid)
    except Exception as e:
        logger.warning(f"从调度器移除用户失败: {e}")
    
    ctx = context_manager._contexts.get(uid)
    if ctx:
        if ctx.is_running:
            ctx.stop()
        # 彻底移除上下文，确保下次启动时创建新的
        context_manager.remove_context(uid)
        return {"message": "交易已停止", "is_running": False}
    
    return {"message": "交易未在运行", "is_running": False}


@router.post("/audit-positions")
async def audit_positions(user: Dict = Depends(get_current_user)):
    """手动审计仓位（检查并修复与交易所的数据不一致）"""
    uid = user["uid"]

    ctx = context_manager._contexts.get(uid)
    if not ctx or not ctx.is_running:
        raise HTTPException(status_code=400, detail="交易服务未启动")

    try:
        # 立即执行审计
        report = ctx._position_service.audit_now(dry_run=False)

        # 格式化响应
        result = {
            "timestamp": report.timestamp,
            "duration_ms": report.duration_ms,
            "redis_positions": report.redis_positions,
            "exchange_positions": report.exchange_positions,
            "issues_found": len(report.issues),
            "issues_fixed": report.fixed_count,
            "issues": [
                {
                    "type": issue.issue_type.value,
                    "field": issue.field,
                    "description": issue.description,
                    "fixed": issue.fixed,
                    "auto_fixable": issue.auto_fixable
                }
                for issue in report.issues
            ]
        }

        if report.error:
            result["error"] = report.error

        return result

    except Exception as e:
        raise HTTPException(status_code=500, detail=safe_error_detail(e, "审计"))


@router.get("/prompt-templates")
async def get_prompt_templates():
    """获取 System Prompt 模板列表"""
    # 默认模板（内置）
    default_templates = [
        {
            "id": "default",
            "name": "默认策略",
            "description": "基础的趋势跟踪策略，适合新手",
            "category": "basic",
            "prompt_preview": "你是一个专业的加密货币交易分析师...",
        },
        {
            "id": "aggressive",
            "name": "激进策略",
            "description": "高频交易策略，追求短线收益",
            "category": "advanced",
            "prompt_preview": "你是一个激进的短线交易员...",
        },
        {
            "id": "conservative",
            "name": "保守策略",
            "description": "稳健策略，注重风险控制",
            "category": "basic",
            "prompt_preview": "你是一个保守的投资顾问...",
        },
    ]
    
    try:
        with config_loader._get_connection() as conn:
            if hasattr(conn, 'execute'):
                from sqlalchemy import text
                # 检查表是否存在
                try:
                    results = conn.execute(text("""
                        SELECT id, name, description, category, prompt_content
                        FROM system_prompt_templates
                        WHERE is_public = 1
                        ORDER BY usage_count DESC
                    """)).fetchall()
                    
                    db_templates = [
                        {
                            "id": str(row._mapping["id"]),
                            "name": row._mapping["name"],
                            "description": row._mapping["description"],
                            "category": row._mapping["category"],
                            "prompt_preview": row._mapping["prompt_content"][:200] if row._mapping["prompt_content"] else None,
                        }
                        for row in results
                    ]
                    return {"templates": default_templates + db_templates}
                except Exception as e:
                    # P5 Fix: 记录异步数据库查询失败
                    logger.debug(f"查询模板失败 (async): {e}")
            else:
                try:
                    cursor = conn.execute("""
                        SELECT id, name, description, category, prompt_content
                        FROM system_prompt_templates
                        WHERE is_public = 1
                        ORDER BY usage_count DESC
                    """)
                    results = cursor.fetchall()
                    
                    db_templates = [
                        {
                            "id": str(r["id"]),
                            "name": r["name"],
                            "description": r["description"],
                            "category": r["category"],
                            "prompt_preview": r["prompt_content"][:200] if r["prompt_content"] else None,
                        }
                        for r in results
                    ]
                    return {"templates": default_templates + db_templates}
                except Exception as e:
                    # P5 Fix: 记录同步数据库查询失败
                    logger.debug(f"查询模板失败 (sync): {e}")
        
        return {"templates": default_templates}
    except Exception as e:
        logger.warning(f"获取模板失败: {e}")
        return {"templates": default_templates}


@router.get("/prompt-template/{template_id}")
async def get_prompt_template_detail(template_id: str):
    """获取 System Prompt 模板详情"""
    # 内置模板
    from llm.llm_client import DEFAULT_SYSTEM_PROMPT
    
    builtin_templates = {
        "default": {
            "id": "default",
            "name": "默认策略",
            "description": "基础的趋势跟踪策略，适合新手",
            "category": "basic",
            "prompt_content": DEFAULT_SYSTEM_PROMPT,
        },
        "aggressive": {
            "id": "aggressive",
            "name": "激进策略",
            "description": "高频交易策略，追求短线收益",
            "category": "advanced",
            "prompt_content": """你是一个激进的短线交易员。

你的交易风格：
1. 追求高频交易，快进快出
2. 利用波动获取短期收益
3. 止损严格，单笔亏损不超过1%
4. 同时持有多个仓位，分散风险

输出格式要求：
返回 JSON 数组，每个信号包含：
- symbol: 交易对
- action: 动作类型
- entry: 入场价格
- stop_loss: 止损价格
- take_profit: 止盈价格
- reason: 交易理由
""",
        },
        "conservative": {
            "id": "conservative",
            "name": "保守策略",
            "description": "稳健策略，注重风险控制",
            "category": "basic",
            "prompt_content": """你是一个保守的投资顾问。

你的交易原则：
1. 只在高胜率机会出现时交易
2. 严格控制仓位，单笔不超过总资金的5%
3. 优先考虑风险，其次考虑收益
4. 顺势交易，不抄底摸顶
5. 大多数时候保持观望

输出格式要求：
返回 JSON 数组，每个信号包含：
- symbol: 交易对
- action: 动作类型（大多数情况应该是 hold）
- entry: 入场价格
- stop_loss: 止损价格
- take_profit: 止盈价格
- reason: 交易理由
""",
        },
    }
    
    if template_id in builtin_templates:
        return builtin_templates[template_id]
    
    # 从数据库查询
    try:
        with config_loader._get_connection() as conn:
            if hasattr(conn, 'execute'):
                from sqlalchemy import text
                result = conn.execute(text("""
                    SELECT id, name, description, category, prompt_content
                    FROM system_prompt_templates
                    WHERE id = :id AND is_public = 1
                """), {"id": int(template_id)}).fetchone()
            else:
                cursor = conn.execute("""
                    SELECT id, name, description, category, prompt_content
                    FROM system_prompt_templates
                    WHERE id = ? AND is_public = 1
                """, (int(template_id),))
                result = cursor.fetchone()
            
            if result:
                # SQLAlchemy Row needs ._mapping for dict-style access
                r = result._mapping if hasattr(result, '_mapping') else result
                return {
                    "id": str(r["id"]),
                    "name": r["name"],
                    "description": r["description"],
                    "category": r["category"],
                    "prompt_content": r["prompt_content"],
                }
    except Exception as e:
        logger.warning(f"获取模板详情失败: {e}")
    
    raise HTTPException(status_code=404, detail="模板不存在")


# ============================================================
# 系统监控 API（需要管理员权限）
# ============================================================

async def get_admin_user(user: Dict = Depends(get_current_user)) -> Dict:
    """验证管理员权限"""
    username = user.get("username", "")
    if username not in ADMIN_USERS:
        raise HTTPException(status_code=403, detail="需要管理员权限")
    return user


@router.get("/system/stats")
async def get_system_stats(user: Dict = Depends(get_admin_user)):
    """获取系统统计（管理员）"""
    try:
        from core.async_multi_user_scheduler import get_scheduler_stats
        
        scheduler_stats = get_scheduler_stats()
        context_stats = context_manager.get_stats()
        
        return {
            "scheduler": scheduler_stats,
            "context_manager": context_stats,
            "active_users": [
                {
                    "uid": uid,
                    "is_running": ctx.is_running,
                    "last_active_at": ctx.last_active_at,
                    "error_count": ctx.error_count,
                    "positions_count": len(ctx.account_snapshot.get("positions", [])),
                }
                for uid, ctx in context_manager._contexts.items()
            ],
        }
    except Exception as e:
        logger.error(f"获取系统统计失败: {e}")
        raise HTTPException(status_code=500, detail=safe_error_detail(e, "获取系统统计"))


@router.get("/system/users")
async def list_all_users(user: Dict = Depends(get_admin_user)):
    """获取所有用户列表（管理员）"""
    try:
        with config_loader._get_connection() as conn:
            if hasattr(conn, 'execute'):
                from sqlalchemy import text
                results = conn.execute(text("""
                    SELECT u.uid, u.username, u.email, u.status, u.tier, u.created_at,
                           c.ai_enabled, c.max_positions
                    FROM users u
                    LEFT JOIN user_trading_config c ON u.uid = c.uid
                    ORDER BY u.created_at DESC
                """)).fetchall()
            else:
                cursor = conn.execute("""
                    SELECT u.uid, u.username, u.email, u.status, u.tier, u.created_at,
                           c.ai_enabled, c.max_positions
                    FROM users u
                    LEFT JOIN user_trading_config c ON u.uid = c.uid
                    ORDER BY u.created_at DESC
                """)
                results = cursor.fetchall()
            
            users = []
            for row in results:
                # SQLAlchemy Row needs ._mapping for dict-style access
                r = row._mapping if hasattr(row, '_mapping') else row
                ctx = context_manager._contexts.get(r["uid"])
                users.append({
                    "uid": r["uid"],
                    "username": r["username"],
                    "email": r["email"],
                    "status": r["status"],
                    "tier": r["tier"],
                    "created_at": str(r["created_at"]) if r["created_at"] else None,
                    "ai_enabled": bool(r["ai_enabled"]) if r["ai_enabled"] is not None else True,
                    "max_positions": r["max_positions"] or 5,
                    "is_running": ctx.is_running if ctx else False,
                })
            
            return {"users": users, "total": len(users)}
            
    except Exception as e:
        logger.error(f"获取用户列表失败: {e}")
        raise HTTPException(status_code=500, detail=safe_error_detail(e, "获取用户列表"))


@router.put("/system/user/{uid}/status")
async def update_user_status(
    uid: str,
    status: str,  # active / suspended / banned
    user: Dict = Depends(get_admin_user)
):
    """更新用户状态（管理员）"""
    if status not in ["active", "suspended", "banned"]:
        raise HTTPException(status_code=400, detail="无效的状态值")
    
    try:
        with config_loader._get_connection() as conn:
            if hasattr(conn, 'execute'):
                from sqlalchemy import text
                conn.execute(text("""
                    UPDATE users SET status = :status WHERE uid = :uid
                """), {"status": status, "uid": uid})
            else:
                conn.execute("""
                    UPDATE users SET status = ? WHERE uid = ?
                """, (status, uid))
        
        # 如果禁用用户，停止其服务
        if status in ["suspended", "banned"]:
            ctx = context_manager._contexts.get(uid)
            if ctx:
                ctx.stop()
                context_manager.remove_context(uid)
            
            try:
                from core.async_multi_user_scheduler import unregister_user_from_schedulers
                unregister_user_from_schedulers(uid)
            except Exception as e:
                logger.debug(f"[{uid}] 从调度器注销失败: {e}")
        
        # 清除缓存
        config_loader._cache.pop(uid, None)
        
        return {"message": f"用户 {uid} 状态已更新为 {status}"}
        
    except Exception as e:
        logger.error(f"更新用户状态失败: {e}")
        raise HTTPException(status_code=500, detail=safe_error_detail(e, "更新用户状态"))


@router.put("/system/user/{uid}/tier")
async def update_user_tier(
    uid: str,
    tier: str,  # free / basic / pro / vip
    user: Dict = Depends(get_admin_user)
):
    """更新用户等级（管理员）"""
    if tier not in ["free", "basic", "pro", "vip"]:
        raise HTTPException(status_code=400, detail="无效的等级值")
    
    try:
        with config_loader._get_connection() as conn:
            if hasattr(conn, 'execute'):
                from sqlalchemy import text
                conn.execute(text("""
                    UPDATE users SET tier = :tier WHERE uid = :uid
                """), {"tier": tier, "uid": uid})
            else:
                conn.execute("""
                    UPDATE users SET tier = ? WHERE uid = ?
                """, (tier, uid))
        
        # 更新调度器中的优先级（使用统一函数）
        try:
            from core.async_multi_user_scheduler import update_user_tier_in_schedulers
            update_user_tier_in_schedulers(uid, tier)
        except Exception as e:
            logger.debug(f"[{uid}] 更新调度器等级失败: {e}")
        
        # 清除缓存
        config_loader._cache.pop(uid, None)
        
        return {"message": f"用户 {uid} 等级已更新为 {tier}"}
        
    except Exception as e:
        logger.error(f"更新用户等级失败: {e}")
        raise HTTPException(status_code=500, detail=safe_error_detail(e, "更新用户等级"))


# ============================================================
# 系统设置 API（管理员）
# ============================================================

@router.get("/system/settings")
async def get_system_settings(user: Dict = Depends(get_admin_user)):
    """获取所有系统设置（管理员）"""
    try:
        settings = config_loader.get_all_system_settings()
        return {"settings": settings}
    except Exception as e:
        logger.error(f"获取系统设置失败: {e}")
        raise HTTPException(status_code=500, detail=safe_error_detail(e, "获取系统设置"))


@router.get("/system/settings/{key}")
async def get_system_setting(key: str, user: Dict = Depends(get_admin_user)):
    """获取单个系统设置（管理员）"""
    value = config_loader.get_system_setting(key)
    if value is None:
        raise HTTPException(status_code=404, detail=f"设置项 {key} 不存在")
    return {"key": key, "value": value}


@router.put("/system/settings/{key}")
async def update_system_setting(
    key: str,
    value: str,
    description: str = None,
    user: Dict = Depends(get_admin_user)
):
    """更新系统设置（管理员）"""
    # 只允许更新特定的设置项
    allowed_keys = ["registration_enabled", "require_totp"]
    if key not in allowed_keys:
        raise HTTPException(status_code=400, detail=f"不允许修改设置项: {key}")
    
    # 验证值
    if key == "registration_enabled" and value not in ["0", "1"]:
        raise HTTPException(status_code=400, detail="值必须是 0 或 1")
    if key == "require_totp" and value not in ["0", "1"]:
        raise HTTPException(status_code=400, detail="值必须是 0 或 1")
    
    success = config_loader.set_system_setting(key, value, description)
    if not success:
        raise HTTPException(status_code=500, detail="更新设置失败")
    
    logger.info(f"管理员 {user.get('username')} 更新系统设置: {key} = {value}")
    
    return {"message": f"设置 {key} 已更新为 {value}"}


@router.get("/system/registration-status")
async def get_registration_status():
    """获取注册开放状态（公开接口）"""
    enabled = config_loader.is_registration_enabled()
    return {"registration_enabled": enabled}


# ============================================================
# 多交易所配置 API
# ============================================================

@router.get("/exchanges")
async def get_exchange_configs(user: Dict = Depends(get_current_user)):
    """
    获取用户的所有交易所配置
    
    返回每个交易所的配置状态（不包含密钥）
    """
    uid = user["uid"]
    
    # 获取所有支持的交易所
    from exchanges import EXCHANGE_NAMES, EXCHANGE_REQUIRES_PASSPHRASE, EXCHANGE_HAS_TESTNET
    
    # 获取用户已配置的交易所
    user_configs = config_loader.get_user_exchange_configs(uid)
    user_config_map = {c["exchange"]: c for c in user_configs}
    
    exchanges = []
    for exchange_id, display_name in EXCHANGE_NAMES.items():
        user_cfg = user_config_map.get(exchange_id, {})
        exchanges.append({
            "exchange": exchange_id,
            "display_name": display_name,
            "requires_passphrase": EXCHANGE_REQUIRES_PASSPHRASE.get(exchange_id, False),
            "has_testnet": EXCHANGE_HAS_TESTNET.get(exchange_id, False),
            # 用户配置
            "is_configured": bool(user_cfg.get("has_api_key")),
            "is_enabled": user_cfg.get("is_enabled", False),
            "is_testnet": user_cfg.get("is_testnet", False),
            "wallet_address": user_cfg.get("wallet_address", ""),
            # 监控配置
            "monitor_symbols": user_cfg.get("monitor_symbols", []),
            "ai500_enabled": user_cfg.get("ai500_enabled", False),
            # AI 策略
            "ai_strategy_id": user_cfg.get("ai_strategy_id"),
        })
    
    return {"exchanges": exchanges}


@router.get("/exchanges/enabled")
async def get_enabled_exchanges(user: Dict = Depends(get_current_user)):
    """获取用户已启用的交易所列表（包含连接状态）"""
    uid = user["uid"]
    
    exchanges = config_loader.get_enabled_exchanges(uid)
    
    from exchanges import EXCHANGE_NAMES
    
    # 获取连接状态 - wrap sync function to avoid blocking
    exchange_status = await asyncio.to_thread(_get_exchange_connection_status, uid)
    
    result = {
        "enabled_exchanges": [
            {
                "exchange": ex,
                "display_name": EXCHANGE_NAMES.get(ex, ex),
                "status": exchange_status.get(ex, {}).get("status", "unknown"),
                "last_update": exchange_status.get(ex, {}).get("last_update"),
                "error": exchange_status.get(ex, {}).get("error"),
            }
            for ex in exchanges
        ]
    }
    return result


def _get_exchange_connection_status(uid: str) -> Dict[str, Dict]:
    """
    获取用户所有交易所的连接状态
    
    优先级:
    0. 检查交易所是否实际在运行（内存状态）
    1. 检查 WebSocket 状态 (pf:{uid}:{exchange}:ws_status)
    2. 检查账户数据时间戳
    3. 检查持仓数据时间戳
    """
    from core.pf_compatibility import pf_compat
    from core.database import redis_client
    import time
    
    status = {}
    exchanges = ["binance", "okx", "bitget", "hyperliquid"]
    now_ms = int(time.time() * 1000)
    stale_threshold_ms = 60 * 1000  # 60秒无更新视为过期
    disconnect_threshold_ms = 5 * 60 * 1000  # 5分钟无更新视为断开
    
    rds = redis_client
    
    # 0. 先获取用户上下文，检查各交易所的实际运行状态
    ctx = context_manager._contexts.get(uid)
    running_exchanges = set()
    if ctx:
        for ex in exchanges:
            ex_ctx = ctx.get_exchange_context(ex)
            if ex_ctx and ex_ctx.is_running:
                running_exchanges.add(ex)
    
    # 同时检查数据库中的 is_running 状态（持久化状态）
    db_running_exchanges = set(config_loader.get_running_exchanges(uid) or [])
    
    for exchange in exchanges:
        try:
            # 1. 首先检查 WebSocket 状态 (优先级最高，因为反映实际连接)
            ws_status_key = f"pf:{uid}:{exchange}:ws_status"
            ws_status_raw = rds.get(ws_status_key)
            if ws_status_raw:
                ws_status = json.loads(ws_status_raw)
                ws_state = ws_status.get("state", "")
                ws_ts = int(ws_status.get("ts", 0) or 0)
                ws_error = ws_status.get("error")
                ws_age_ms = now_ms - ws_ts if ws_ts else 0
                
                # WebSocket 状态有效（60秒内）
                if ws_ts > 0 and ws_age_ms < stale_threshold_ms:
                    if ws_state == "connected":
                        status[exchange] = {
                            "status": "connected",
                            "last_update": ws_ts,
                            "age_ms": ws_age_ms,
                        }
                        continue
                    elif ws_state in ("connecting", "reconnecting"):
                        status[exchange] = {
                            "status": "connecting",
                            "last_update": ws_ts,
                            "error": ws_error,
                        }
                        continue
                    elif ws_state == "auth_failed":
                        status[exchange] = {
                            "status": "auth_failed",
                            "last_update": ws_ts,
                            "error": ws_error,
                        }
                        continue
                    elif ws_state == "error":
                        status[exchange] = {
                            "status": "error",
                            "last_update": ws_ts,
                            "error": ws_error,
                        }
                        continue
            
            # 2. 如果交易所没有在运行（内存和数据库都没有），标记为 stopped
            if exchange not in running_exchanges and exchange not in db_running_exchanges:
                # 检查是否有历史数据（用于显示 "已停止但有数据"）
                account = pf_compat.get_pf_account(uid, exchange)
                if account:
                    last_ts = int(account.get("ts", 0) or account.get("updatedAt", 0) or 0)
                    status[exchange] = {
                        "status": "stopped",
                        "last_update": last_ts if last_ts > 0 else None,
                        "has_data": True,
                    }
                else:
                    status[exchange] = {
                        "status": "stopped",
                        "last_update": None,
                        "has_data": False,
                    }
                continue
            
            # 3. 检查账户数据（作为备选）
            account = pf_compat.get_pf_account(uid, exchange)
            if account:
                # 尝试多种时间戳字段
                last_ts = int(account.get("ts", 0) or account.get("updatedAt", 0) or account.get("updateTime", 0) or 0)
                if last_ts > 0:
                    age_ms = now_ms - last_ts
                    
                    if age_ms < stale_threshold_ms:
                        status[exchange] = {
                            "status": "connected",
                            "last_update": last_ts,
                            "age_ms": age_ms,
                        }
                        continue
                    elif age_ms < disconnect_threshold_ms:
                        status[exchange] = {
                            "status": "stale",
                            "last_update": last_ts,
                            "age_ms": age_ms,
                        }
                        continue
            
            # 3. 检查持仓数据
            pos_data = pf_compat.get_pf_pos(uid, exchange)
            if pos_data:
                max_ts = 0
                for field, pos in pos_data.items():
                    ts = int(pos.get("updatedAt", 0) or pos.get("ts", 0) or 0)
                    if ts > max_ts:
                        max_ts = ts
                
                if max_ts > 0:
                    age_ms = now_ms - max_ts
                    if age_ms < stale_threshold_ms:
                        status[exchange] = {
                            "status": "connected",
                            "last_update": max_ts,
                            "age_ms": age_ms,
                        }
                        continue
                    elif age_ms < disconnect_threshold_ms:
                        status[exchange] = {
                            "status": "stale",
                            "last_update": max_ts,
                            "age_ms": age_ms,
                        }
                        continue
            
            # 4. 如果有账户或持仓数据但时间戳为空/过旧，标记为 unknown
            if account or pos_data:
                status[exchange] = {"status": "unknown", "last_update": None}
            else:
                # 没有任何数据
                status[exchange] = {"status": "disconnected", "last_update": None}
                
        except Exception as e:
            logger.debug(f"[{uid}] Error getting {exchange} status: {e}")
            status[exchange] = {
                "status": "error",
                "last_update": None,
            }
    
    return status


@router.get("/exchanges/status")
async def get_exchanges_status(user: Dict = Depends(get_current_user)):
    """
    获取所有交易所的连接状态
    
    返回:
    - status: connected | stale | disconnected | error | unknown
    - last_update: 最后更新时间戳 (毫秒)
    - age_ms: 距离最后更新的时间 (毫秒)
    """
    uid = user["uid"]
    
    from exchanges import EXCHANGE_NAMES
    
    status = await asyncio.to_thread(_get_exchange_connection_status, uid)
    enabled = config_loader.get_enabled_exchanges(uid)
    
    result = {
        "exchanges": {
            ex: {
                **status.get(ex, {"status": "unknown"}),
                "display_name": EXCHANGE_NAMES.get(ex, ex),
                "is_enabled": ex in enabled,
            }
            for ex in ["binance", "okx", "bitget", "hyperliquid"]
        }
    }
    
    return result


@router.get("/exchanges/alerts")
async def get_exchange_alerts(
    user: Dict = Depends(get_current_user),
    limit: int = 20
):
    """
    获取交易所连接告警历史
    
    返回最近的连接状态变化事件
    """
    uid = user["uid"]
    
    try:
        from core.exchange_monitor import get_exchange_monitor
        
        monitor_mgr = get_exchange_monitor()
        monitor = monitor_mgr.get_or_create_monitor(uid)
        
        alerts = monitor.get_alerts(limit=limit)
        status = monitor.get_status()
        
        return {
            "alerts": alerts,
            "current_status": status,
        }
    except Exception as e:
        logger.warning(f"[{uid}] 获取告警失败: {e}")
        return {
            "alerts": [],
            "current_status": {},
        }


@router.get("/exchanges/trading-status")
async def get_all_exchanges_trading_status(user: Dict = Depends(get_current_user)):
    """
    获取所有交易所的交易运行状态
    
    返回每个已配置交易所的运行状态。
    """
    uid = user["uid"]
    
    from exchanges import EXCHANGE_NAMES, SUPPORTED_EXCHANGES
    
    # 获取用户上下文
    ctx = context_manager._contexts.get(uid)
    
    # 一次性获取所有交易所配置（避免多次数据库查询）
    all_configs = config_loader.get_user_exchange_configs(uid)
    config_map = {c["exchange"]: c for c in all_configs}
    
    result = {"exchanges": []}
    
    for exchange in SUPPORTED_EXCHANGES:
        exchange_config = config_map.get(exchange, {})
        
        status = {
            "exchange": exchange,
            "display_name": EXCHANGE_NAMES.get(exchange, exchange),
            "is_configured": bool(exchange_config.get("has_api_key")),
            "is_enabled": exchange_config.get("is_enabled", False),
            "is_running": False,
            "has_context": False,
        }
        
        if ctx:
            exchange_status = ctx.get_exchange_status(exchange)
            if exchange_status:
                status["is_running"] = exchange_status.get("is_running", False)
                status["has_context"] = True
                status["has_cycle_store"] = exchange_status.get("has_cycle_store", False)
                status["has_position_auditor"] = exchange_status.get("has_position_auditor", False)
        
        result["exchanges"].append(status)
    
    # 添加整体状态
    result["user_is_running"] = ctx.is_running if ctx else False
    
    return result


@router.get("/exchanges/{exchange}")
async def get_exchange_config(
    exchange: str,
    user: Dict = Depends(get_current_user)
):
    """获取指定交易所的详细配置（敏感信息部分隐藏）"""
    uid = user["uid"]
    
    config = config_loader.get_user_exchange_config(uid, exchange)
    if not config:
        return {
            "exchange": exchange,
            "is_configured": False,
            "is_enabled": False,
            "is_testnet": False,
            "has_api_key": False,
            "has_api_secret": False,
            "has_passphrase": False,
            "wallet_address": "",
        }
    
    return {
        "exchange": exchange,
        "is_configured": bool(config.get("api_key")),
        "is_enabled": config.get("is_enabled", False),
        "is_testnet": config.get("is_testnet", False),
        "has_api_key": bool(config.get("api_key")),
        "has_api_secret": bool(config.get("api_secret")),
        "has_passphrase": bool(config.get("passphrase")),
        "wallet_address": config.get("wallet_address", ""),
        # 部分显示密钥（用于确认）
        "api_key_preview": config.get("api_key", "")[:8] + "..." if config.get("api_key") else "",
    }


@router.put("/exchanges/{exchange}")
async def update_exchange_config(
    exchange: str,
    req: ExchangeConfigRequest,
    user: Dict = Depends(get_current_user)
):
    """
    更新交易所配置
    
    支持的交易所: binance, okx, hyperliquid
    
    注意：测试网功能仅管理员可用
    """
    uid = user["uid"]
    username = user.get("username", "")
    
    # 验证交易所
    from exchanges import EXCHANGE_NAMES, EXCHANGE_REQUIRES_PASSPHRASE
    
    if exchange not in EXCHANGE_NAMES:
        raise HTTPException(status_code=400, detail=f"不支持的交易所: {exchange}")
    
    # 测试网功能仅管理员可用
    if req.is_testnet and username not in ADMIN_USERS:
        raise HTTPException(status_code=403, detail="测试网功能仅管理员可用")
    
    # OKX 和 Bitget 需要 passphrase
    if EXCHANGE_REQUIRES_PASSPHRASE.get(exchange, False) and req.is_enabled and req.api_key and not req.passphrase:
        # 检查是否已有 passphrase
        existing = config_loader.get_user_exchange_config(uid, exchange)
        if not existing or not existing.get("passphrase"):
            raise HTTPException(status_code=400, detail=f"{EXCHANGE_NAMES[exchange]} 需要 API Passphrase")
    
    # 保存配置
    success = config_loader.save_user_exchange_config(
        uid=uid,
        exchange=exchange,
        api_key=req.api_key,
        api_secret=req.api_secret,
        passphrase=req.passphrase,
        wallet_address=req.wallet_address,
        is_testnet=req.is_testnet,
        is_enabled=req.is_enabled
    )
    
    if not success:
        raise HTTPException(status_code=500, detail="保存配置失败")
    
    logger.info(f"[{uid}] 交易所配置已更新: {exchange}")
    
    # ✅ 刷新 UserContext 中的 ExchangeContext（如果存在且正在运行）
    # 这样用户更新 API Key 后，交易服务会使用新的凭证
    ctx = context_manager._contexts.get(uid)
    if ctx:
        exchange_ctx = ctx.get_exchange_context(exchange)
        if exchange_ctx:
            was_running = exchange_ctx.is_running
            
            # 停止旧的交易所服务
            if was_running:
                logger.info(f"[{uid}] 停止交易所 {exchange} 服务以刷新配置...")
                ctx.stop_exchange(exchange)
            
            # 从管理器中移除旧的 ExchangeContext
            if exchange in ctx._exchange_manager._exchanges:
                del ctx._exchange_manager._exchanges[exchange]
            
            # 使用新配置重新创建 ExchangeContext
            new_config = config_loader.get_user_exchange_config(uid, exchange)
            if new_config and new_config.get('api_key'):
                ctx._exchange_manager.add_exchange(
                    exchange=exchange,
                    api_key=new_config['api_key'],
                    api_secret=new_config.get('api_secret', ''),
                    passphrase=new_config.get('passphrase'),
                    is_testnet=new_config.get('is_testnet', False),
                    wallet_address=new_config.get('wallet_address'),
                )
                logger.info(f"[{uid}] 交易所 {exchange} ExchangeContext 已重新创建")
                
                # 如果之前在运行，重新启动
                if was_running:
                    logger.info(f"[{uid}] 重新启动交易所 {exchange} 服务...")
                    ctx.start_exchange(exchange)
        
        # ✅ 重置 UserContext 中的 MultiExchangeTrader（如果存在）
        # MultiExchangeTrader 也缓存了交易所客户端
        if ctx._multi_trader:
            try:
                import asyncio
                try:
                    loop = asyncio.get_running_loop()
                    loop.create_task(ctx._multi_trader.close())
                except RuntimeError:
                    asyncio.run(ctx._multi_trader.close())
            except Exception as e:
                logger.debug(f"[{uid}] 关闭 UserContext MultiExchangeTrader: {e}")
            finally:
                ctx._multi_trader = None
            logger.info(f"[{uid}] 已重置 UserContext MultiExchangeTrader")
    
    # ✅ 刷新 AsyncUserContext 中的 MultiExchangeTrader（如果存在）
    # AsyncUserContext 有独立的 _multi_trader 缓存
    try:
        from core.async_user_context import async_context_manager
        async_ctx = async_context_manager._contexts.get(uid)
        if async_ctx and async_ctx._multi_trader:
            try:
                import asyncio
                try:
                    loop = asyncio.get_running_loop()
                    loop.create_task(async_ctx._multi_trader.close())
                except RuntimeError:
                    asyncio.run(async_ctx._multi_trader.close())
            except Exception as e:
                logger.debug(f"[{uid}] 关闭 AsyncUserContext MultiExchangeTrader: {e}")
            finally:
                async_ctx._multi_trader = None
            logger.info(f"[{uid}] 已重置 AsyncUserContext MultiExchangeTrader")
    except Exception as e:
        logger.warning(f"[{uid}] 刷新 AsyncUserContext MultiExchangeTrader 失败: {e}")
    
    return {"message": f"{EXCHANGE_NAMES[exchange]} 配置已更新"}


@router.put("/exchanges/{exchange}/enable")
async def enable_exchange(
    exchange: str,
    enabled: bool,
    user: Dict = Depends(get_current_user)
):
    """启用/禁用交易所"""
    uid = user["uid"]
    
    from exchanges import EXCHANGE_NAMES
    if exchange not in EXCHANGE_NAMES:
        raise HTTPException(status_code=400, detail=f"不支持的交易所: {exchange}")
    
    # 检查是否已配置
    config = config_loader.get_user_exchange_config(uid, exchange)
    if enabled and (not config or not config.get("api_key")):
        raise HTTPException(status_code=400, detail="请先配置 API 密钥")
    
    success = config_loader.set_exchange_enabled(uid, exchange, enabled)
    if not success:
        raise HTTPException(status_code=500, detail="更新失败")
    
    # ✅ 如果禁用交易所，停止正在运行的服务（包括审计器）
    if not enabled:
        ctx = context_manager._contexts.get(uid)
        if ctx:
            exchange_ctx = ctx.get_exchange_context(exchange)
            if exchange_ctx and exchange_ctx.is_running:
                logger.info(f"[{uid}] 禁用交易所 {exchange}，停止服务...")
                ctx.stop_exchange(exchange)
                logger.info(f"[{uid}] 交易所 {exchange} 服务已停止")
    
    status = "已启用" if enabled else "已禁用"
    return {"message": f"{EXCHANGE_NAMES[exchange]} {status}"}


class ExchangeMonitorConfigRequest(BaseModel):
    """交易所监控配置请求"""
    monitor_symbols: Optional[List[str]] = None
    ai500_enabled: Optional[bool] = None


@router.put("/exchanges/{exchange}/monitor-config")
async def update_exchange_monitor_config(
    exchange: str,
    req: ExchangeMonitorConfigRequest,
    user: Dict = Depends(get_current_user)
):
    """
    更新交易所的监控配置（监控币种和 AI500）
    
    每个交易所可以独立配置要监控的币种和是否启用 AI500。
    会自动校验币种是否在该交易所可用，无效币种会被自动过滤并返回警告。
    """
    uid = user["uid"]
    
    from exchanges import EXCHANGE_NAMES
    if exchange not in EXCHANGE_NAMES:
        raise HTTPException(status_code=400, detail=f"不支持的交易所: {exchange}")
    
    # 检查是否已配置
    exchange_config = config_loader.get_user_exchange_config(uid, exchange)
    if not exchange_config or not exchange_config.get("api_key"):
        raise HTTPException(status_code=400, detail=f"请先配置 {EXCHANGE_NAMES[exchange]} API 密钥")
    
    # 验证币种数量
    if req.monitor_symbols is not None and len(req.monitor_symbols) > 20:
        raise HTTPException(status_code=400, detail="每个交易所最多只能监控 20 个币种")
    
    # ✅ 校验币种是否在该交易所可用
    valid_symbols = req.monitor_symbols
    invalid_symbols = []
    if req.monitor_symbols:
        from core.symbol_availability import get_symbol_manager
        manager = get_symbol_manager()
        valid_symbols = []
        seen = set()  # 用于去重
        for symbol in req.monitor_symbols:
            symbol_upper = symbol.upper()
            # 跳过重复的币种
            if symbol_upper in seen:
                continue
            seen.add(symbol_upper)
            
            if manager.is_symbol_available(exchange, symbol_upper):
                valid_symbols.append(symbol_upper)
            else:
                invalid_symbols.append(symbol_upper)
        
        if invalid_symbols:
            logger.info(f"[{uid}] {exchange} 监控配置: 过滤了无效币种 {invalid_symbols}")
    
    # 更新配置（使用过滤后的有效币种）
    success = config_loader.set_exchange_monitor_config(
        uid=uid,
        exchange=exchange,
        monitor_symbols=valid_symbols,
        ai500_enabled=req.ai500_enabled
    )
    
    if not success:
        raise HTTPException(status_code=500, detail="更新配置失败")
    
    # 构建响应
    response = {
        "message": f"{EXCHANGE_NAMES[exchange]} 监控配置已更新",
        "exchange": exchange,
        "monitor_symbols": valid_symbols,
        "ai500_enabled": req.ai500_enabled
    }
    
    # 如果有无效币种，添加警告信息
    if invalid_symbols:
        response["warning"] = f"以下币种在 {EXCHANGE_NAMES[exchange]} 不可用，已自动移除: {', '.join(invalid_symbols)}"
        response["invalid_symbols"] = invalid_symbols
    
    return response


@router.get("/exchanges/{exchange}/monitor-config")
async def get_exchange_monitor_config(
    exchange: str,
    user: Dict = Depends(get_current_user)
):
    """
    获取交易所的监控配置
    """
    uid = user["uid"]
    
    from exchanges import EXCHANGE_NAMES
    if exchange not in EXCHANGE_NAMES:
        raise HTTPException(status_code=400, detail=f"不支持的交易所: {exchange}")
    
    exchange_config = config_loader.get_user_exchange_config(uid, exchange)
    
    if not exchange_config:
        return {
            "exchange": exchange,
            "display_name": EXCHANGE_NAMES[exchange],
            "is_configured": False,
            "monitor_symbols": [],
            "ai500_enabled": False
        }
    
    return {
        "exchange": exchange,
        "display_name": EXCHANGE_NAMES[exchange],
        "is_configured": bool(exchange_config.get("api_key")),
        "monitor_symbols": exchange_config.get("monitor_symbols", []),
        "ai500_enabled": exchange_config.get("ai500_enabled", False)
    }


class ExchangeAIConfigRequest(BaseModel):
    """交易所 AI 配置请求"""
    ai_enabled: Optional[bool] = None
    llm_provider: Optional[str] = None
    llm_model: Optional[str] = None
    llm_api_key: Optional[str] = None
    llm_base_url: Optional[str] = None
    system_prompt: Optional[str] = None


class AIStrategyRequest(BaseModel):
    """AI 策略请求 (v5.0)"""
    name: str = Field(..., min_length=1, max_length=64)
    llm_provider: str = Field(default="anthropic", description="LLM 提供商")
    llm_model: str = Field(default="", description="模型名称")
    llm_api_key: Optional[str] = Field(default=None, description="API Key")
    llm_base_url: str = Field(default="", description="Base URL (自定义模型必填)")
    # LLM 参数
    temperature: Optional[float] = Field(default=None, ge=0, le=2, description="Temperature (0-2)")
    top_p: Optional[float] = Field(default=None, ge=0, le=1, description="Top P (0-1)")
    max_tokens: Optional[int] = Field(default=None, ge=1000, le=200000, description="Max Tokens")
    # v5.0: 策略配置
    strategy_preset: Optional[str] = Field(default="default", description="预设策略 (conservative/aggressive/trend_following/mean_reversion/default)")
    strategy_overrides: Optional[Dict[str, str]] = Field(default=None, description="自定义覆盖 {category: content}")


class ExchangeAIStrategyRequest(BaseModel):
    """交易所 AI 策略设置请求"""
    strategy_id: Optional[str] = None  # None 表示不使用 AI


@router.put("/exchanges/{exchange}/ai-config")
async def update_exchange_ai_config(
    exchange: str,
    req: ExchangeAIConfigRequest,
    user: Dict = Depends(get_current_user)
):
    """
    更新交易所的 AI 配置（每个交易所独立 AI 设置）
    
    每个交易所可以配置独立的 AI 模型、API Key 和策略提示词。
    """
    uid = user["uid"]
    
    from exchanges import EXCHANGE_NAMES
    if exchange not in EXCHANGE_NAMES:
        raise HTTPException(status_code=400, detail=f"不支持的交易所: {exchange}")
    
    # 检查是否已配置
    exchange_config = config_loader.get_user_exchange_config(uid, exchange)
    if not exchange_config or not exchange_config.get("api_key"):
        raise HTTPException(status_code=400, detail=f"请先配置 {EXCHANGE_NAMES[exchange]} API 密钥")
    
    # 更新配置
    success = config_loader.set_exchange_ai_config(
        uid=uid,
        exchange=exchange,
        ai_enabled=req.ai_enabled,
        llm_provider=req.llm_provider,
        llm_model=req.llm_model,
        llm_api_key=req.llm_api_key,
        llm_base_url=req.llm_base_url,
        system_prompt=req.system_prompt
    )
    
    if not success:
        raise HTTPException(status_code=500, detail="更新 AI 配置失败")
    
    return {
        "message": f"{EXCHANGE_NAMES[exchange]} AI 配置已更新",
        "exchange": exchange
    }


@router.get("/exchanges/{exchange}/ai-config")
async def get_exchange_ai_config(
    exchange: str,
    user: Dict = Depends(get_current_user)
):
    """
    获取交易所的 AI 配置
    """
    uid = user["uid"]
    
    from exchanges import EXCHANGE_NAMES
    if exchange not in EXCHANGE_NAMES:
        raise HTTPException(status_code=400, detail=f"不支持的交易所: {exchange}")
    
    exchange_config = config_loader.get_user_exchange_config(uid, exchange)
    
    if not exchange_config:
        return {
            "exchange": exchange,
            "display_name": EXCHANGE_NAMES[exchange],
            "is_configured": False,
            "ai_enabled": False,
            "llm_provider": "anthropic",
            "llm_model": "",
            "has_llm_api_key": False,
            "llm_base_url": "",
            "system_prompt": ""
        }
    
    return {
        "exchange": exchange,
        "display_name": EXCHANGE_NAMES[exchange],
        "is_configured": bool(exchange_config.get("api_key")),
        "ai_enabled": exchange_config.get("ai_enabled", False),
        "llm_provider": exchange_config.get("llm_provider", "anthropic"),
        "llm_model": exchange_config.get("llm_model", ""),
        "has_llm_api_key": bool(exchange_config.get("llm_api_key")),
        "llm_base_url": exchange_config.get("llm_base_url", ""),
        "system_prompt": exchange_config.get("system_prompt", "")
    }


# ============================================================
# AI 策略池 API
# ============================================================

@router.get("/llm-models")
async def get_available_llm_models(
    user: Dict = Depends(get_current_user),
    provider: Optional[str] = Query(None, description="按提供商筛选"),
):
    """
    获取可用的 LLM 模型列表（用户级）
    
    从数据库 llm_models 表获取启用的模型列表，供前端设置页面使用。
    
    Returns:
        {
            "providers": ["anthropic", "openai", ...],
            "models": [
                {
                    "provider": "anthropic",
                    "model_id": "claude-sonnet-4-20250514",
                    "display_name": "Claude Sonnet 4",
                    "description": "...",
                    "is_recommended": true,
                    "context_window": 200000,
                    "supports_vision": true,
                },
                ...
            ]
        }
    """
    try:
        from llm.llm_models_service import llm_models_service
        
        # 获取所有启用的模型
        if provider:
            models = llm_models_service.get_models_by_provider(provider.lower())
        else:
            models = llm_models_service.get_all_models()
        
        # 获取所有提供商
        providers = llm_models_service.get_all_providers()
        
        # 转换为前端需要的格式（只返回必要字段）
        model_list = []
        for m in models:
            model_list.append({
                "provider": m.provider,
                "model_id": m.model_id,
                "display_name": m.display_name,
                "description": m.description,
                "is_recommended": m.is_recommended,
                "context_window": m.context_window,
                "supports_vision": m.supports_vision,
                "supports_function_call": m.supports_function_call,
                # v5.1: 添加模型默认参数，用于前端预填
                "temperature": m.temperature,
                "top_p": m.top_p,
                "max_tokens": m.max_tokens,
            })
        
        return {
            "providers": sorted(providers),
            "models": model_list,
        }
        
    except Exception as e:
        logger.warning(f"获取 LLM 模型列表失败: {e}，返回空列表")
        # 返回空列表而不是报错，让前端可以回退到手动输入
        return {
            "providers": ["anthropic", "openai", "deepseek", "openrouter", "grok", "gemini", "custom"],
            "models": [],
        }


@router.get("/ai-strategies")
async def get_ai_strategies(user: Dict = Depends(get_current_user)):
    """
    获取用户的所有 AI 策略
    """
    uid = user["uid"]
    strategies = config_loader.get_user_ai_strategies(uid)
    return {"strategies": strategies}


@router.get("/ai-strategies/presets")
async def list_strategy_presets(user: Dict = Depends(get_current_user)):
    """
    获取可用的预设策略列表 (v5.0)
    
    合并系统预设和用户自定义预设
    
    Returns:
        {
            "presets": [
                {"name": "default", "description": "...", "is_user_preset": false},
                {"name": "my_custom", "description": "...", "is_user_preset": true},
                ...
            ],
            "categories": [...]
        }
    """
    from llm.prompt_templates import STRATEGY_CATEGORIES, CATEGORY_TITLES
    
    uid = user["uid"]
    
    # 获取系统预设（只返回启用的）
    system_presets = config_loader.list_strategy_presets()
    for p in system_presets:
        p["is_user_preset"] = False
    
    # 获取用户自定义预设
    user_presets = config_loader.list_user_strategy_presets(uid)
    # user_presets 已经有 is_user_preset: True
    
    # 合并，用户预设在前
    presets = user_presets + system_presets
    
    categories = [
        {
            "name": cat,
            "title": CATEGORY_TITLES.get(cat, cat),
            "description": {
                "role": "交易风格、性格定义",
                "risk_rules": "硬性风险约束（必须遵守）",
                "entry_conditions": "开仓条件",
                "exit_conditions": "平仓条件",
                "position_sizing": "仓位大小规则",
                "market_preferences": "偏好/避免的币种、时段",
                "adaptive_rules": "连亏/连胜/高波动时的调整",
            }.get(cat, "")
        }
        for cat in STRATEGY_CATEGORIES
    ]
    
    return {
        "presets": presets,
        "categories": categories
    }


@router.get("/ai-strategies/presets/{preset_name}")
async def get_preset_templates(preset_name: str, user: Dict = Depends(get_current_user)):
    """
    获取指定预设的所有分类内容 (v5.0)
    
    优先查找用户自定义预设，如果不存在则查找系统预设。
    
    Returns:
        {
            "preset_name": "conservative",
            "templates": {
                "role": "...",
                "risk_rules": "...",
                ...
            },
            "is_user_preset": false
        }
    """
    uid = user["uid"]
    
    # 先查找用户自定义预设
    user_templates = config_loader.get_user_strategy_templates(uid, preset_name)
    if user_templates:
        template_dict = {t["category"]: t["content"] for t in user_templates}
        # 获取 description（所有分类共享同一个 description）
        description = user_templates[0].get("description", "") if user_templates else ""
        return {
            "preset_name": preset_name,
            "templates": template_dict,
            "description": description,
            "is_user_preset": True
        }
    
    # 再查找系统预设
    templates = config_loader.get_strategy_templates(preset_name)
    if not templates:
        raise HTTPException(status_code=404, detail=f"预设 '{preset_name}' 不存在")
    
    # 转换为 {category: content} 格式
    template_dict = {t["category"]: t["content"] for t in templates}
    
    return {
        "preset_name": preset_name,
        "templates": template_dict,
        "description": "",  # 系统预设没有用户自定义描述
        "is_user_preset": False
    }


# ============================================================
# 用户自定义策略预设 API
# ============================================================

class UserPresetRequest(BaseModel):
    preset_name: str
    description: str = ""
    categories: Dict[str, str]  # {category: content}


@router.post("/ai-strategies/user-presets")
async def create_user_preset(
    req: UserPresetRequest,
    user: Dict = Depends(get_current_user)
):
    """
    创建用户自定义策略预设
    
    用户可以保存当前配置为自己的预设模板，供以后复用。
    """
    uid = user["uid"]
    
    # 验证预设名称
    if not req.preset_name or not req.preset_name.strip():
        raise HTTPException(status_code=400, detail="预设名称不能为空")
    
    if len(req.preset_name) > 64:
        raise HTTPException(status_code=400, detail="预设名称不能超过 64 个字符")
    
    # 检查预设数量限制（每用户最多 5 个自定义预设）
    existing = config_loader.list_user_strategy_presets(uid)
    if len(existing) >= 5:
        raise HTTPException(status_code=400, detail="最多只能创建 5 个自定义预设")
    
    # 检查名称是否与系统预设冲突
    system_presets = config_loader.list_strategy_presets()
    system_names = [p["name"] for p in system_presets]
    if req.preset_name in system_names:
        raise HTTPException(status_code=400, detail=f"预设名称 '{req.preset_name}' 与系统预设冲突")
    
    # 检查名称是否已存在
    for p in existing:
        if p["name"] == req.preset_name:
            raise HTTPException(status_code=400, detail=f"预设名称 '{req.preset_name}' 已存在")
    
    # 验证分类
    valid_categories = ["role", "risk_rules", "entry_conditions", "exit_conditions",
                       "position_sizing", "market_preferences", "adaptive_rules"]
    for cat in req.categories.keys():
        if cat not in valid_categories:
            raise HTTPException(status_code=400, detail=f"无效的分类: {cat}")
    
    success = config_loader.save_user_strategy_preset(
        uid=uid,
        preset_name=req.preset_name,
        description=req.description,
        categories=req.categories
    )
    
    if not success:
        raise HTTPException(status_code=500, detail="创建预设失败")
    
    return {"message": f"预设 '{req.preset_name}' 已创建", "preset_name": req.preset_name}


@router.put("/ai-strategies/user-presets/{preset_name}")
async def update_user_preset(
    preset_name: str,
    req: UserPresetRequest,
    user: Dict = Depends(get_current_user)
):
    """
    更新用户自定义策略预设
    """
    uid = user["uid"]
    
    # 检查预设是否存在
    existing = config_loader.list_user_strategy_presets(uid)
    found = False
    for p in existing:
        if p["name"] == preset_name:
            found = True
            break
    
    if not found:
        raise HTTPException(status_code=404, detail=f"预设 '{preset_name}' 不存在")
    
    # 验证分类
    valid_categories = ["role", "risk_rules", "entry_conditions", "exit_conditions",
                       "position_sizing", "market_preferences", "adaptive_rules"]
    for cat in req.categories.keys():
        if cat not in valid_categories:
            raise HTTPException(status_code=400, detail=f"无效的分类: {cat}")
    
    # 如果改名，检查新名称是否冲突
    is_renaming = req.preset_name != preset_name
    if is_renaming:
        system_presets = config_loader.list_strategy_presets()
        system_names = [p["name"] for p in system_presets]
        if req.preset_name in system_names:
            raise HTTPException(status_code=400, detail=f"预设名称 '{req.preset_name}' 与系统预设冲突")
        
        for p in existing:
            if p["name"] == req.preset_name:
                raise HTTPException(status_code=400, detail=f"预设名称 '{req.preset_name}' 已存在")
    
    # 先保存新的预设（避免数据丢失）
    success = config_loader.save_user_strategy_preset(
        uid=uid,
        preset_name=req.preset_name,
        description=req.description,
        categories=req.categories
    )
    
    if not success:
        raise HTTPException(status_code=500, detail="更新预设失败")
    
    # 如果是重命名，保存成功后再删除旧的
    if is_renaming:
        config_loader.delete_user_strategy_preset(uid, preset_name)
    
    return {"message": f"预设 '{req.preset_name}' 已更新"}


@router.delete("/ai-strategies/user-presets/{preset_name}")
async def delete_user_preset(
    preset_name: str,
    user: Dict = Depends(get_current_user)
):
    """
    删除用户自定义策略预设
    """
    uid = user["uid"]
    
    success = config_loader.delete_user_strategy_preset(uid, preset_name)
    
    if not success:
        raise HTTPException(status_code=404, detail=f"预设 '{preset_name}' 不存在")
    
    return {"message": f"预设 '{preset_name}' 已删除"}


@router.post("/ai-strategies")
async def create_ai_strategy(
    req: AIStrategyRequest,
    user: Dict = Depends(get_current_user)
):
    """
    创建新的 AI 策略 (v5.0)
    
    策略配置使用 strategy_preset + strategy_overrides 模式：
    - strategy_preset: 选择一个预设作为基础
    - strategy_overrides: 可选，覆盖特定分类的内容
    """
    uid = user["uid"]
    
    # Strip 所有字符串字段，防止用户误输入空格
    req.name = req.name.strip() if req.name else ""
    req.llm_model = req.llm_model.strip() if req.llm_model else ""
    req.llm_api_key = req.llm_api_key.strip() if req.llm_api_key else None
    req.llm_base_url = req.llm_base_url.strip() if req.llm_base_url else ""
    
    # 检查策略数量限制（每用户最多 10 个策略）
    existing = config_loader.get_user_ai_strategies(uid)
    if len(existing) >= 10:
        raise HTTPException(status_code=400, detail="最多只能创建 10 个 AI 策略")
    
    # 检查名称是否重复
    for s in existing:
        if s["name"] == req.name:
            raise HTTPException(status_code=400, detail=f"策略名称 '{req.name}' 已存在")
    
    # 验证预设是否存在（检查系统预设和用户自定义预设）
    if req.strategy_preset:
        system_presets = config_loader.list_strategy_presets()
        user_presets = config_loader.list_user_strategy_presets(uid)
        all_preset_names = [p["name"] for p in system_presets] + [p["name"] for p in user_presets]
        if req.strategy_preset not in all_preset_names:
            raise HTTPException(status_code=400, detail=f"预设 '{req.strategy_preset}' 不存在")
    
    # 序列化 strategy_overrides
    overrides_json = None
    if req.strategy_overrides:
        overrides_json = json.dumps(req.strategy_overrides, ensure_ascii=False)
    
    # 生成策略 ID
    import secrets
    strategy_id = secrets.token_hex(8)
    
    success = config_loader.save_ai_strategy(
        uid=uid,
        strategy_id=strategy_id,
        name=req.name,
        llm_provider=req.llm_provider,
        llm_model=req.llm_model,
        llm_api_key=req.llm_api_key,
        llm_base_url=req.llm_base_url,
        temperature=req.temperature,
        top_p=req.top_p,
        max_tokens=req.max_tokens,
        strategy_preset=req.strategy_preset or "default",
        strategy_overrides=overrides_json,
    )
    
    if not success:
        raise HTTPException(status_code=500, detail="创建策略失败")
    
    return {
        "message": f"策略 '{req.name}' 已创建",
        "strategy_id": strategy_id
    }


@router.get("/ai-strategies/{strategy_id}")
async def get_ai_strategy(
    strategy_id: str,
    user: Dict = Depends(get_current_user)
):
    """
    获取单个 AI 策略的详细信息 (v5.0)
    """
    uid = user["uid"]
    
    # 使用新的 v5.0 方法
    strategy = config_loader.get_ai_strategy_with_preset(uid, strategy_id)
    if not strategy:
        raise HTTPException(status_code=404, detail="策略不存在")
    
    # 解析 strategy_overrides
    overrides = None
    if strategy.get("strategy_overrides"):
        try:
            overrides = json.loads(strategy["strategy_overrides"]) if isinstance(strategy["strategy_overrides"], str) else strategy["strategy_overrides"]
        except (json.JSONDecodeError, ValueError):
            overrides = None
    
    # 不返回完整的 API Key
    return {
        "id": strategy["strategy_id"],
        "name": strategy["name"],
        "llm_provider": strategy["llm_provider"],
        "llm_model": strategy["llm_model"],
        "has_api_key": bool(strategy.get("llm_api_key")),
        "llm_base_url": strategy.get("llm_base_url", ""),
        # LLM 参数
        "temperature": strategy.get("temperature"),
        "top_p": strategy.get("top_p"),
        "max_tokens": strategy.get("max_tokens"),
        # v5.0: 策略配置
        "strategy_preset": strategy.get("strategy_preset", "default"),
        "strategy_overrides": overrides,
    }


@router.put("/ai-strategies/{strategy_id}")
async def update_ai_strategy(
    strategy_id: str,
    req: AIStrategyRequest,
    user: Dict = Depends(get_current_user)
):
    """
    更新 AI 策略 (v5.0)
    """
    uid = user["uid"]
    
    # Strip 所有字符串字段，防止用户误输入空格
    req.name = req.name.strip() if req.name else ""
    req.llm_model = req.llm_model.strip() if req.llm_model else ""
    req.llm_api_key = req.llm_api_key.strip() if req.llm_api_key else None
    req.llm_base_url = req.llm_base_url.strip() if req.llm_base_url else ""
    
    # 检查策略是否存在
    existing = config_loader.get_ai_strategy_with_preset(uid, strategy_id)
    if not existing:
        raise HTTPException(status_code=404, detail="策略不存在")
    
    # 检查名称是否与其他策略重复
    all_strategies = config_loader.get_user_ai_strategies(uid)
    for s in all_strategies:
        if s["id"] != strategy_id and s["name"] == req.name:
            raise HTTPException(status_code=400, detail=f"策略名称 '{req.name}' 已存在")
    
    # 验证预设是否存在（检查系统预设和用户自定义预设）
    if req.strategy_preset:
        system_presets = config_loader.list_strategy_presets()
        user_presets = config_loader.list_user_strategy_presets(uid)
        all_preset_names = [p["name"] for p in system_presets] + [p["name"] for p in user_presets]
        if req.strategy_preset not in all_preset_names:
            raise HTTPException(status_code=400, detail=f"预设 '{req.strategy_preset}' 不存在")
    
    # 序列化 strategy_overrides
    overrides_json = None
    if req.strategy_overrides:
        overrides_json = json.dumps(req.strategy_overrides, ensure_ascii=False)
    
    # 处理 API Key：如果没有提供新的，保留原有的
    api_key = req.llm_api_key if req.llm_api_key else existing.get("llm_api_key")
    
    success = config_loader.save_ai_strategy(
        uid=uid,
        strategy_id=strategy_id,
        name=req.name,
        llm_provider=req.llm_provider,
        llm_model=req.llm_model,
        llm_api_key=api_key,
        llm_base_url=req.llm_base_url,
        temperature=req.temperature,
        top_p=req.top_p,
        max_tokens=req.max_tokens,
        strategy_preset=req.strategy_preset or "default",
        strategy_overrides=overrides_json,
    )
    
    if not success:
        raise HTTPException(status_code=500, detail="更新策略失败")
    
    # 失效缓存
    from core.strategy_cache import get_strategy_cache
    get_strategy_cache().invalidate(uid, strategy_id)
    
    return {"message": f"策略 '{req.name}' 已更新"}


@router.delete("/ai-strategies/{strategy_id}")
async def delete_ai_strategy(
    strategy_id: str,
    user: Dict = Depends(get_current_user)
):
    """
    删除 AI 策略 (v5.0)
    
    如果有交易所正在使用该策略，会自动清除关联
    """
    uid = user["uid"]
    
    # 检查策略是否存在
    existing = config_loader.get_ai_strategy_with_preset(uid, strategy_id)
    if not existing:
        raise HTTPException(status_code=404, detail="策略不存在")
    
    success = config_loader.delete_user_ai_strategy(uid, strategy_id)
    if not success:
        raise HTTPException(status_code=500, detail="删除策略失败")
    
    # 失效缓存
    from core.strategy_cache import get_strategy_cache
    get_strategy_cache().invalidate(uid, strategy_id)
    
    return {"message": f"策略 '{existing['name']}' 已删除"}


@router.post("/ai-strategies/{strategy_id}/reload")
async def reload_ai_strategy(
    strategy_id: str,
    user: Dict = Depends(get_current_user)
):
    """
    重载 AI 策略缓存
    
    当用户修改 Prompt 模板后，调用此接口使策略立即生效。
    
    重载流程：
    1. 获取策略当前使用的预设名称 (strategy_preset)
    2. 从预设表重新加载最新的模板内容
    3. 更新 strategy_overrides 字段
    4. 失效缓存
    """
    uid = user["uid"]
    
    try:
        # 检查策略是否存在
        existing = config_loader.get_ai_strategy_with_preset(uid, strategy_id)
        if not existing:
            raise HTTPException(status_code=404, detail="策略不存在")
        
        # 获取策略使用的预设名称
        preset_name = existing.get("strategy_preset") or "default"
        
        # 从预设表重新加载最新的模板内容
        # 先查找用户自定义预设
        user_templates = config_loader.get_user_strategy_templates(uid, preset_name)
        if user_templates:
            template_dict = {t["category"]: t["content"] for t in user_templates}
        else:
            # 再查找系统预设
            templates = config_loader.get_strategy_templates(preset_name)
            if templates:
                template_dict = {t["category"]: t["content"] for t in templates}
            else:
                template_dict = {}
        
        # 更新 strategy_overrides 字段
        import json
        new_overrides = json.dumps(template_dict, ensure_ascii=False) if template_dict else None
        
        # 保存更新后的策略（只更新 strategy_overrides）
        config_loader.update_strategy_overrides(uid, strategy_id, new_overrides)
        
        # 失效缓存，下次使用时会重新加载
        from core.strategy_cache import get_strategy_cache
        get_strategy_cache().invalidate(uid, strategy_id)
        
        logger.info(f"[{uid}] 策略 {strategy_id} ({existing['name']}) 已重载，预设: {preset_name}")
        
        return {
            "message": f"策略 '{existing['name']}' 已重载", 
            "strategy_id": strategy_id,
            "preset_name": preset_name,
            "categories_updated": list(template_dict.keys()) if template_dict else []
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[{uid}] 重载策略 {strategy_id} 失败: {e}")
        raise HTTPException(status_code=500, detail=f"重载策略失败: {str(e)}")


class TestLLMRequest(BaseModel):
    """测试 LLM 连接请求"""
    llm_provider: str
    llm_model: str
    llm_api_key: str
    llm_base_url: Optional[str] = None


@router.post("/ai-strategies/test-connection")
async def test_llm_connection(
    req: TestLLMRequest,
    user: Dict = Depends(get_current_user)
):
    """
    测试 LLM API 连接是否成功
    
    发送一个简单的测试请求到 LLM API，验证配置是否正确
    """
    try:
        from llm.llm_client import (
            AnthropicClient,
            OpenAICompatibleClient,
            DeepSeekClient,
            OpenRouterClient,
            GrokClient,
            GeminiClient,
        )
        
        # 根据提供商创建客户端
        provider = req.llm_provider.lower()
        model = req.llm_model.strip()
        api_key = req.llm_api_key.strip() if req.llm_api_key else ""
        base_url = req.llm_base_url.strip() if req.llm_base_url else ""
        client = None
        
        if provider == "anthropic":
            client = AnthropicClient(model=model, api_key=api_key)
        elif provider == "openai":
            # OpenAI 使用兼容客户端
            client = OpenAICompatibleClient(model=model, api_key=api_key)
        elif provider == "deepseek":
            client = DeepSeekClient(model=model, api_key=api_key)
        elif provider == "openrouter":
            client = OpenRouterClient(model=model, api_key=api_key)
        elif provider == "grok":
            client = GrokClient(model=model, api_key=api_key)
        elif provider == "gemini":
            client = GeminiClient(model=model, api_key=api_key)
        elif provider == "custom":
            if not base_url:
                raise HTTPException(status_code=400, detail="自定义提供商需要填写 Base URL")
            # 自定义提供商使用兼容客户端 + 自定义 base_url
            client = OpenAICompatibleClient(model=model, api_key=api_key, base_url=base_url)
        else:
            raise HTTPException(status_code=400, detail=f"不支持的 LLM 提供商: {provider}")
        
        # 发送测试消息
        test_system = "You are a helpful assistant."
        test_message = "Say 'OK' if you can receive this message."
        
        # 调用异步 chat 方法
        response = await client.chat(
            system_prompt=test_system,
            user_message=test_message,
        )
        
        # 关闭客户端
        await client.close()
        
        if response and response.content:
            return {
                "success": True,
                "message": "连接成功！",
                "response": response.content[:100]  # 只返回前100个字符
            }
        else:
            error_msg = response.error if response else "响应为空"
            return {
                "success": False,
                "message": f"连接失败: {error_msg}",
                "response": None
            }
            
    except HTTPException:
        raise
    except Exception as e:
        # P2 Fix: 记录完整错误到日志，返回安全的错误消息
        logger.error(f"LLM 连接测试失败: {type(e).__name__}: {e}")
        error_msg = str(e)
        # 简化错误信息（不暴露内部细节）
        if "401" in error_msg or "Unauthorized" in error_msg.lower():
            error_msg = "API Key 无效或已过期"
        elif "404" in error_msg or "not found" in error_msg.lower():
            error_msg = f"模型 '{req.llm_model}' 不存在或无权访问"
        elif "429" in error_msg or "rate limit" in error_msg.lower():
            error_msg = "请求频率超限，请稍后再试"
        elif "connection" in error_msg.lower() or "timeout" in error_msg.lower():
            error_msg = "连接超时，请检查网络或 Base URL"
        else:
            error_msg = "连接测试失败"
        
        return {
            "success": False,
            "message": error_msg,
            "response": None
        }


@router.put("/exchanges/{exchange}/ai-strategy")
async def set_exchange_ai_strategy(
    exchange: str,
    req: ExchangeAIStrategyRequest,
    user: Dict = Depends(get_current_user)
):
    """
    设置交易所使用的 AI 策略
    
    strategy_id 为空或 null 时，清除策略关联（不使用 AI）
    如果停用 AI 策略且交易所正在运行，将自动停止该交易所的交易。
    """
    uid = user["uid"]
    
    from exchanges import EXCHANGE_NAMES
    if exchange not in EXCHANGE_NAMES:
        raise HTTPException(status_code=400, detail=f"不支持的交易所: {exchange}")
    
    # 检查交易所是否已配置
    exchange_config = config_loader.get_user_exchange_config(uid, exchange)
    if not exchange_config or not exchange_config.get("api_key"):
        raise HTTPException(status_code=400, detail=f"请先配置 {EXCHANGE_NAMES[exchange]} API 密钥")
    
    # 如果指定了策略，检查策略是否存在
    if req.strategy_id:
        strategy = config_loader.get_user_ai_strategy(uid, req.strategy_id)
        if not strategy:
            raise HTTPException(status_code=404, detail="策略不存在")
    
    success = config_loader.set_exchange_ai_strategy(uid, exchange, req.strategy_id)
    if not success:
        raise HTTPException(status_code=500, detail="设置 AI 策略失败")
    
    # 如果停用 AI 策略（strategy_id 为空），检查是否需要停止交易
    trading_stopped = False
    if not req.strategy_id:
        # 获取用户上下文，检查交易所是否正在运行
        ctx = context_manager._contexts.get(uid)
        if ctx:
            exchange_ctx = ctx.get_exchange_context(exchange)
            if exchange_ctx and getattr(exchange_ctx, 'is_running', False):
                # 交易所正在运行，停止交易
                logger.info(f"[{uid}] 停用 AI 策略，自动停止 {exchange} 交易")
                ctx.stop_exchange(exchange)
                config_loader.set_exchange_running(uid, exchange, False)
                trading_stopped = True
                
                # 如果所有交易所都停止了，从调度器移除
                if not ctx.is_running:
                    try:
                        from core.async_multi_user_scheduler import unregister_user_from_schedulers
                        unregister_user_from_schedulers(uid)
                    except Exception as e:
                        logger.warning(f"从调度器移除用户失败: {e}")
                    config_loader.set_trading_enabled(uid, False)
    
    if req.strategy_id:
        strategy = config_loader.get_user_ai_strategy(uid, req.strategy_id)
        return {
            "message": f"{EXCHANGE_NAMES[exchange]} 已设置使用策略 '{strategy['name']}'",
            "exchange": exchange,
            "strategy_id": req.strategy_id
        }
    else:
        message = f"{EXCHANGE_NAMES[exchange]} 已停用 AI"
        if trading_stopped:
            message += "，交易已自动停止"
        return {
            "message": message,
            "exchange": exchange,
            "strategy_id": None,
            "trading_stopped": trading_stopped
        }


@router.get("/exchanges/{exchange}/ai-strategy")
async def get_exchange_ai_strategy(
    exchange: str,
    user: Dict = Depends(get_current_user)
):
    """
    获取交易所当前使用的 AI 策略
    """
    uid = user["uid"]
    
    from exchanges import EXCHANGE_NAMES
    if exchange not in EXCHANGE_NAMES:
        raise HTTPException(status_code=400, detail=f"不支持的交易所: {exchange}")
    
    strategy_id = config_loader.get_exchange_ai_strategy_id(uid, exchange)
    
    if not strategy_id:
        return {
            "exchange": exchange,
            "strategy_id": None,
            "strategy": None
        }
    
    strategy = config_loader.get_user_ai_strategy(uid, strategy_id)
    
    return {
        "exchange": exchange,
        "strategy_id": strategy_id,
        "strategy": {
            "id": strategy["id"],
            "name": strategy["name"],
            "llm_provider": strategy["llm_provider"],
            "llm_model": strategy["llm_model"],
            "has_api_key": bool(strategy.get("llm_api_key"))
        } if strategy else None
    }


@router.post("/exchanges/{exchange}/start")
async def start_exchange_trading(
    exchange: str,
    user: Dict = Depends(get_current_user)
):
    """
    启动单个交易所的交易服务
    
    只启动指定交易所的 WebSocket 连接和数据同步服务，
    不影响其他交易所的运行状态。
    
    注意：如果用户配置使用测试网，只有管理员可以启动
    """
    uid = user["uid"]
    username = user.get("username", "")
    
    from exchanges import EXCHANGE_NAMES
    if exchange not in EXCHANGE_NAMES:
        raise HTTPException(status_code=400, detail=f"不支持的交易所: {exchange}")
    
    # 检查是否已配置
    exchange_config = config_loader.get_user_exchange_config(uid, exchange)
    if not exchange_config or not exchange_config.get("api_key"):
        raise HTTPException(status_code=400, detail=f"请先配置 {EXCHANGE_NAMES[exchange]} API 密钥")
    
    # 测试网检查：只有管理员可以启动测试网交易
    if exchange_config.get("is_testnet") and username not in ADMIN_USERS:
        raise HTTPException(
            status_code=403, 
            detail=f"测试网交易仅管理员可用。请关闭测试网模式后再启动交易。"
        )
    
    # 检查是否已启用
    if not exchange_config.get("is_enabled"):
        raise HTTPException(status_code=400, detail=f"请先启用 {EXCHANGE_NAMES[exchange]} 交易所")
    
    # 检查交易所是否配置了监控币种或 AI500（优先检查交易所级别配置）
    exchange_monitor_symbols = exchange_config.get("monitor_symbols", [])
    exchange_ai500_enabled = exchange_config.get("ai500_enabled", False)
    
    # 如果交易所没有独立配置，检查用户级别的全局配置
    user_config = config_loader.load(uid)
    if not user_config:
        raise HTTPException(status_code=400, detail="用户配置不存在")
    
    has_monitor_config = (
        bool(exchange_monitor_symbols) or 
        exchange_ai500_enabled or
        bool(user_config.monitor_symbols) or 
        user_config.ai500_enabled
    )
    
    if not has_monitor_config:
        raise HTTPException(
            status_code=400, 
            detail=f"请先在 {EXCHANGE_NAMES[exchange]} 配置中设置监控币种或启用 AI500"
        )
    
    # 获取或创建用户上下文
    ctx = context_manager._contexts.get(uid)
    if not ctx:
        # 创建用户上下文，但不初始化所有交易所上下文（只创建空壳）
        # 这样启动单个交易所时不会创建其他交易所的上下文
        ctx = context_manager.get_context(uid, auto_start=False, init_exchanges=False)
        if not ctx:
            raise HTTPException(status_code=500, detail="创建用户上下文失败")
    
    # 检查交易所上下文是否存在，如果不存在则只添加这一个交易所
    exchange_ctx = ctx.get_exchange_context(exchange)
    if not exchange_ctx:
        # 使用新的 add_single_exchange 方法只添加指定的交易所
        success = ctx.add_single_exchange(exchange)
        if not success:
            raise HTTPException(status_code=500, detail=f"创建交易所 {EXCHANGE_NAMES[exchange]} 上下文失败")
    
    # 启动单个交易所
    success = ctx.start_exchange(exchange)
    if not success:
        raise HTTPException(status_code=500, detail=f"启动 {EXCHANGE_NAMES[exchange]} 失败")
    
    # 确保交易所是启用状态
    config_loader.set_exchange_enabled(uid, exchange, True)
    
    # 设置交易所运行状态（持久化，用于重启恢复）
    config_loader.set_exchange_running(uid, exchange, True)
    
    # 设置全局交易启用状态（用于重启后恢复）
    config_loader.set_trading_enabled(uid, True)
    
    # 主动拉取一次账户数据并写入 Redis
    try:
        await _fetch_and_save_account_data(uid, exchange, exchange_config)
    except Exception as e:
        logger.warning(f"[{uid}] 拉取 {exchange} 账户数据失败（不影响启动）: {e}")
    
    # 注册到调度器（如果用户整体未注册，使用统一函数）
    try:
        from core.async_multi_user_scheduler import register_user_to_schedulers, is_user_registered
        if not is_user_registered(uid):
            register_user_to_schedulers(uid, user_config.tier)
    except Exception as e:
        logger.warning(f"注册用户到调度器失败: {e}")
    
    return {
        "message": f"{EXCHANGE_NAMES[exchange]} 交易已启动",
        "exchange": exchange,
        "is_running": True
    }


@router.post("/exchanges/{exchange}/stop")
async def stop_exchange_trading(
    exchange: str,
    user: Dict = Depends(get_current_user)
):
    """
    停止单个交易所的交易服务
    
    只停止指定交易所的 WebSocket 连接和数据同步服务，
    不影响其他交易所的运行状态。
    """
    uid = user["uid"]
    
    from exchanges import EXCHANGE_NAMES
    if exchange not in EXCHANGE_NAMES:
        raise HTTPException(status_code=400, detail=f"不支持的交易所: {exchange}")
    
    # 获取用户上下文
    ctx = context_manager._contexts.get(uid)
    if not ctx:
        # 没有上下文，可能本来就没有运行
        return {
            "message": f"{EXCHANGE_NAMES[exchange]} 交易未在运行",
            "exchange": exchange,
            "is_running": False
        }
    
    # 停止单个交易所
    success = ctx.stop_exchange(exchange)
    
    # 更新交易所运行状态（持久化）
    config_loader.set_exchange_running(uid, exchange, False)
    
    # 如果所有交易所都停止了，从调度器移除并更新持久化状态
    if not ctx.is_running:
        try:
            from core.async_multi_user_scheduler import unregister_user_from_schedulers
            unregister_user_from_schedulers(uid)
        except Exception as e:
            logger.warning(f"从调度器移除用户失败: {e}")
        
        # 所有交易所都停止了，设置 trading_enabled=False
        config_loader.set_trading_enabled(uid, False)
    
    return {
        "message": f"{EXCHANGE_NAMES[exchange]} 交易已停止",
        "exchange": exchange,
        "is_running": False
    }


@router.get("/exchanges/{exchange}/trading-status")
async def get_exchange_trading_status(
    exchange: str,
    user: Dict = Depends(get_current_user)
):
    """
    获取单个交易所的交易运行状态
    
    返回交易所的详细运行状态，包括 WebSocket 连接状态等。
    """
    uid = user["uid"]
    
    from exchanges import EXCHANGE_NAMES
    if exchange not in EXCHANGE_NAMES:
        raise HTTPException(status_code=400, detail=f"不支持的交易所: {exchange}")
    
    # 检查配置
    exchange_config = config_loader.get_user_exchange_config(uid, exchange)
    if not exchange_config or not exchange_config.get("api_key"):
        return {
            "exchange": exchange,
            "display_name": EXCHANGE_NAMES[exchange],
            "is_configured": False,
            "is_enabled": False,
            "is_running": False,
            "has_context": False,
        }
    
    # 获取用户上下文
    ctx = context_manager._contexts.get(uid)
    
    if not ctx:
        return {
            "exchange": exchange,
            "display_name": EXCHANGE_NAMES[exchange],
            "is_configured": True,
            "is_enabled": exchange_config.get("is_enabled", False),
            "is_running": False,
            "has_context": False,
        }
    
    # 获取交易所状态
    exchange_status = ctx.get_exchange_status(exchange)
    
    if not exchange_status:
        return {
            "exchange": exchange,
            "display_name": EXCHANGE_NAMES[exchange],
            "is_configured": True,
            "is_enabled": exchange_config.get("is_enabled", False),
            "is_running": False,
            "has_context": False,
        }
    
    return {
        "exchange": exchange,
        "display_name": EXCHANGE_NAMES[exchange],
        "is_configured": True,
        "is_enabled": exchange_config.get("is_enabled", False),
        "is_running": exchange_status.get("is_running", False),
        "has_context": True,
        "has_cycle_store": exchange_status.get("has_cycle_store", False),
        "has_position_auditor": exchange_status.get("has_position_auditor", False),
    }


@router.delete("/exchanges/{exchange}")
async def delete_exchange_config(
    exchange: str,
    user: Dict = Depends(get_current_user)
):
    """删除交易所配置"""
    uid = user["uid"]
    
    success = config_loader.delete_user_exchange_config(uid, exchange)
    if not success:
        raise HTTPException(status_code=500, detail="删除失败")
    
    return {"message": "配置已删除"}


@router.post("/exchanges/{exchange}/test")
async def test_exchange_connection(
    exchange: str,
    user: Dict = Depends(get_current_user)
):
    """
    测试交易所连接
    
    验证 API 密钥是否有效，返回账户余额信息
    """
    uid = user["uid"]
    
    from exchanges import create_exchange, EXCHANGE_NAMES
    
    if exchange not in EXCHANGE_NAMES:
        raise HTTPException(status_code=400, detail=f"不支持的交易所: {exchange}")
    
    # 获取配置
    config = config_loader.get_user_exchange_config(uid, exchange)
    if not config or not config.get("api_key"):
        raise HTTPException(status_code=400, detail="请先配置 API 密钥")
    
    # 检查需要 passphrase 的交易所
    from exchanges import EXCHANGE_REQUIRES_PASSPHRASE
    if EXCHANGE_REQUIRES_PASSPHRASE.get(exchange, False) and not config.get("passphrase"):
        raise HTTPException(status_code=400, detail=f"{EXCHANGE_NAMES[exchange]} 需要配置 API Passphrase")
    
    exchange_client = None
    try:
        # 创建交易所实例
        exchange_client = create_exchange(
            exchange_type=exchange,
            api_key=config["api_key"],
            api_secret=config.get("api_secret", ""),
            passphrase=config.get("passphrase"),
            is_testnet=config.get("is_testnet", False),
            wallet_address=config.get("wallet_address")
        )
        
        # 测试获取余额
        balance = await exchange_client.get_balance()
        
        return {
            "success": True,
            "exchange": exchange,
            "balance": balance,
            "message": f"连接成功，可用余额: {balance:.2f} USDT"
        }
        
    except Exception as e:
        logger.error(f"[{uid}] 测试 {exchange} 连接失败: {e}")
        return {
            "success": False,
            "exchange": exchange,
            "message": "连接失败"
        }
    finally:
        # 确保关闭连接
        if exchange_client:
            try:
                await exchange_client.close()
            except Exception as e:
                logger.debug(f"关闭交易所连接失败: {e}")


# ============================================================
# 多交易所 Dashboard API
# ============================================================

@router.get("/exchange-dashboard/{exchange}")
async def get_exchange_dashboard(
    exchange: str,
    user: Dict = Depends(get_current_user),
    limit: int = 100,
    closed_limit: int = 50,
    offset: int = 0
):
    """
    获取指定交易所的仪表盘数据（返回格式和 /api/dashboard 完全一致）
    
    从 Redis 读取数据，按 exchange 字段筛选
    """
    uid = user["uid"]
    
    # 验证交易所是否有效
    from exchanges import SUPPORTED_EXCHANGES, EXCHANGE_NAMES
    if exchange not in SUPPORTED_EXCHANGES:
        raise HTTPException(status_code=400, detail=f"不支持的交易所: {exchange}")
    
    # 检查用户是否配置了该交易所
    config = config_loader.get_user_exchange_config(uid, exchange)
    if not config or not config.get("api_key"):
        raise HTTPException(
            status_code=400, 
            detail=f"未配置 {EXCHANGE_NAMES.get(exchange, exchange)} 交易所"
        )
    
    try:
        from core.pf_compatibility import pf_compat
        from core.database import redis_client
        from api.web import (
            Position, ClosedTrade,
            live_pnl_if_stale, calculate_statistics, calculate_statistics_async,
            build_equity_curve, build_equity_curve_async, calc_max_drawdown_peak
        )
        
        # ========== 获取持仓数据（筛选指定交易所）==========
        active_fields = pf_compat.get_pf_pos_active(uid)
        pos_data = pf_compat.get_pf_pos(uid)
        cycle_data = pf_compat.get_pf_cycle(uid)
        
        # 筛选指定交易所的持仓，创建 Position 对象列表
        positions_raw = []
        for field in (active_fields or []):
            if field not in pos_data:
                continue
            pos_dict = pos_data[field].copy()
            
            # 筛选指定交易所
            if pos_dict.get("exchange") != exchange:
                continue
            
            # 从 cycle 数据中补充信息
            if field in cycle_data:
                cycle = cycle_data[field]
                if not pos_dict.get("stopLossPrice"):
                    pos_dict["stopLossPrice"] = cycle.get("stopLossPrice") or cycle.get("stopLoss")
                if not pos_dict.get("takeProfitPrice"):
                    pos_dict["takeProfitPrice"] = cycle.get("takeProfitPrice") or cycle.get("takeProfit")
                # 补充 openOrderType
                if not pos_dict.get("openOrderType"):
                    pos_dict["openOrderType"] = cycle.get("openOrderType")
            
            try:
                positions_raw.append(Position(**pos_dict))
            except Exception as e:
                logger.debug(f"[{uid}] 创建 Position 对象失败: {e}")
        
        # 获取账户信息
        account = pf_compat.get_pf_account(uid) or {}
        
        # ---------- positions：做"读时兜底"（和 /api/dashboard 一样）----------
        positions = []
        for p in positions_raw:
            base = p.dict()
            live = live_pnl_if_stale(redis_client, base, stale_ms=3000)

            # ✅ 保留官方口径（对账/调试用）
            live["officialUnrealizedPnl"] = base.get("unrealizedPnl", "0")

            # ✅ 兼容前端：它只读 unrealizedPnl，所以这里让 unrealizedPnl 变成"展示口径"
            live["unrealizedPnl"] = live.get("liveUnrealizedPnl", base.get("unrealizedPnl", "0"))

            positions.append(live)
        
        # 获取初始权益
        from api.web import get_initial_equity
        initial_equity = get_initial_equity(uid)
        
        # ========== 获取已平仓交易（筛选指定交易所）==========
        closed_trades_raw = pf_compat.get_pf_closed_h(uid)
        
        # 筛选指定交易所的交易
        filtered_closed = {
            k: v for k, v in (closed_trades_raw or {}).items()
            if v.get("exchange") == exchange
        }
        
        # 按关闭时间排序（最新在前）
        sorted_trades = sorted(
            filtered_closed.items(),
            key=lambda x: int(x[1].get('closeTimeMs', '0')),
            reverse=True
        )
        
        # ========== 统计/曲线：最近 N 笔 or 全部 ==========
        closed_total = len(filtered_closed)
        
        if limit == -1:
            trades_for_stats_raw = [t[1] for t in sorted_trades]
            stats_limit_value = -1
        else:
            trades_for_stats_raw = [t[1] for t in sorted_trades[:limit]]
            stats_limit_value = limit
        
        # 转换为 ClosedTrade 对象
        trades_for_stats = []
        for t in trades_for_stats_raw:
            try:
                trades_for_stats.append(ClosedTrade(**t))
            except Exception as e:
                logger.debug(f"[{uid}] 创建 ClosedTrade 对象失败: {e}")
        
        # 计算统计数据（和 /api/dashboard 一样）
        # 使用异步版本，避免阻塞事件循环
        stats, equity_curve = await asyncio.gather(
            calculate_statistics_async(positions_raw, trades_for_stats, initial_equity),
            build_equity_curve_async(trades_for_stats, initial_equity),
        )
        mdd_peak = calc_max_drawdown_peak(equity_curve)
        
        # ✅ 覆盖"未实现收益"为展示口径（让顶部卡片和表格一致）
        try:
            total_live_unreal = sum(float(x.get("unrealizedPnl") or 0) for x in positions)
        except Exception as e:
            logger.debug(f"[{uid}] 计算未实现收益失败: {e}")
            total_live_unreal = 0.0
        stats.totalUnrealizedPnl = round(total_live_unreal, 2)
        
        # ✅ 列表分页（不受统计口径影响）
        paginated_trades = sorted_trades[offset:offset + closed_limit]
        closed_trades_page = [t[1] for t in paginated_trades]
        
        # ========== 返回格式和 /api/dashboard 完全一致 ==========
        return {
            "statistics": stats,
            "statsLimit": stats_limit_value,
            "equityCurve": equity_curve,

            # ✅ 给前端备用展示（比如"回撤% 基于初始资金 XXX"）
            "initialEquity": round(initial_equity, 2) if (initial_equity is not None) else None,

            # ✅ 当前账户（实时）
            "account": account,

            # 🔥 直接从 account 取，不使用任何额外变量
            "walletBalance": float(account.get("walletBalance")) if account and account.get(
                "walletBalance") is not None else None,

            "equity": float(account.get("equity")) if account and account.get("equity") is not None else None,

            "unrealized": float(account.get("unrealized")) if account and account.get("unrealized") is not None else None,

            # ✅ 标准最大回撤（从历史峰值到后续谷底）
            "maxDrawdownPeakPct": mdd_peak["maxDrawdownPeakPct"],
            "maxDrawdownPeakAmount": mdd_peak["maxDrawdownPeakAmount"],
            "maxDrawdownPeakFrom": mdd_peak["maxDrawdownPeakFrom"],
            "maxDrawdownPeakTo": mdd_peak["maxDrawdownPeakTo"],
            "maxDrawdownPeakEquity": mdd_peak["maxDrawdownPeakEquity"],
            "maxDrawdownTroughEquity": mdd_peak["maxDrawdownTroughEquity"],

            # ✅ positions 现在是 dict 列表（包含 liveSource/markPrice 等）
            "positions": positions,
            "closedTrades": closed_trades_page,
            "closedTotal": closed_total,
            "closedLimit": closed_limit,
            "closedOffset": offset
        }
        
    except Exception as e:
        logger.error(f"[{uid}] 获取 {exchange} dashboard 失败: {e}")
        import traceback
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=safe_error_detail(e, "获取数据"))


@router.get("/all-exchanges-summary")
async def get_all_exchanges_summary(user: Dict = Depends(get_current_user)):
    """
    获取用户所有已启用交易所的汇总数据
    
    返回每个交易所的余额和持仓数量概览
    """
    uid = user["uid"]
    
    exchanges = config_loader.get_enabled_exchanges(uid)
    if not exchanges:
        return {"exchanges": [], "total": {"balance": 0, "positions": 0}}
    
    from exchanges import create_exchange, EXCHANGE_NAMES
    
    results = []
    total_balance = 0.0
    total_positions = 0
    
    for exchange in exchanges:
        config = config_loader.get_user_exchange_config(uid, exchange)
        if not config or not config.get("api_key"):
            results.append({
                "exchange": exchange,
                "exchange_name": EXCHANGE_NAMES.get(exchange, exchange),
                "status": "not_configured",
                "balance": 0,
                "positions_count": 0,
            })
            continue
        
        exchange_client = None
        try:
            exchange_client = create_exchange(
                exchange_type=exchange,
                api_key=config["api_key"],
                api_secret=config["api_secret"],
                passphrase=config.get("passphrase"),
                is_testnet=config.get("is_testnet", False),
                wallet_address=config.get("wallet_address")
            )
            
            account = await exchange_client.get_account()
            positions = await exchange_client.get_positions()
            
            balance = account.total_balance or 0
            pos_count = len(positions)
            
            total_balance += balance
            total_positions += pos_count
            
            results.append({
                "exchange": exchange,
                "exchange_name": EXCHANGE_NAMES.get(exchange, exchange),
                "status": "connected",
                "balance": round(balance, 2),
                "available": round(account.available_balance, 2),
                "unrealized_pnl": round(account.unrealized_pnl, 2),
                "positions_count": pos_count,
                "is_testnet": config.get("is_testnet", False),
            })
            
        except Exception as e:
            logger.warning(f"[{uid}] 获取 {exchange} 汇总失败: {e}")
            results.append({
                "exchange": exchange,
                "exchange_name": EXCHANGE_NAMES.get(exchange, exchange),
                "status": "error",
                "balance": 0,
                "positions_count": 0,
            })
        finally:
            if exchange_client:
                try:
                    await exchange_client.close()
                except Exception:
                    pass
    
    return {
        "exchanges": results,
        "total": {
            "balance": round(total_balance, 2),
            "positions": total_positions,
        },
        "timestamp": int(time.time() * 1000),
    }


# ============================================================
# Redis 数据管理 API
# ============================================================

class ResetRedisDataRequest(BaseModel):
    """重置 Redis 数据请求"""
    exchange: str = "all"  # "all" | "binance" | "okx" | "bitget" | "hyperliquid"
    data_types: List[str] = []  # ["positions", "closed_trades", "ai_history", "notifications", "cache", "decision_feedback"]


@router.post("/reset-redis-data")
async def reset_redis_data(
    request: ResetRedisDataRequest = None,
    user: Dict = Depends(get_current_user)
):
    """
    重置用户的 Redis 数据（支持选择性重置）
    
    Args:
        exchange: 交易所选择
            - "all": 所有交易所
            - "binance", "okx", "bitget", "hyperliquid": 指定交易所
        data_types: 要重置的数据类型列表
            - "positions": 持仓记录
            - "closed_trades": 已平仓交易
            - "ai_history": AI 历史
            - "notifications": 通知记录
            - "cache": 缓存数据
            - "decision_feedback": 决策反馈
    
    保留:
    - 用户偏好设置 (preferences)
    - 元数据 (metadata)
    - API 密钥配置
    """
    uid = user["uid"]
    
    # 兼容旧版调用（无 body 时重置全部）
    if request is None:
        request = ResetRedisDataRequest(
            exchange="all",
            data_types=["positions", "closed_trades", "ai_history", "notifications", "cache", "decision_feedback"]
        )
    
    try:
        from core.redis_manager import RedisDataManager
        from core.database import RedisKeys
        from exchanges import SUPPORTED_EXCHANGES
        
        exchange = request.exchange
        data_types = request.data_types
        
        if not data_types:
            raise HTTPException(status_code=400, detail="请至少选择一项要重置的数据类型")
        
        # 确定要处理的交易所列表
        if exchange == "all":
            target_exchanges = list(SUPPORTED_EXCHANGES)
        elif exchange in SUPPORTED_EXCHANGES:
            target_exchanges = [exchange]
        else:
            raise HTTPException(status_code=400, detail=f"不支持的交易所: {exchange}")
        
        reset_fields = {}
        reset_summary = []
        
        # 根据选择的数据类型构建重置字段
        for data_type in data_types:
            if data_type == "positions":
                # 持仓记录（包括 cycles 和 open_orders）
                if exchange == "all":
                    reset_fields[RedisKeys.field_positions()] = {}
                    reset_fields[RedisKeys.field_positions_active()] = []
                for ex in target_exchanges:
                    reset_fields[RedisKeys.exchange_positions(ex)] = {}
                    reset_fields[RedisKeys.exchange_positions_active(ex)] = []
                    reset_fields[RedisKeys.exchange_cycles(ex)] = {}  # 同时重置周期数据
                    reset_fields[RedisKeys.exchange_open_orders(ex)] = {}  # 同时重置挂单缓存
                reset_summary.append("持仓记录")
                
            elif data_type == "closed_trades":
                # 已平仓交易 - 主要存储在 MySQL，Redis 只有兼容性数据
                
                # 1. 删除 MySQL 中的 closed_trades（主存储）
                try:
                    from core.closed_trades_db import get_closed_trades_db
                    db = get_closed_trades_db()
                    if exchange == "all":
                        deleted_count = db.delete_user_trades(uid)
                    else:
                        deleted_count = db.delete_user_trades(uid, exchange=exchange)
                    logger.info(f"Deleted {deleted_count} closed trades from MySQL for {uid}" + 
                               (f" ({exchange})" if exchange != "all" else ""))
                except Exception as e:
                    logger.warning(f"Failed to delete MySQL closed_trades for {uid}: {e}")
                
                # 2. 处理排行榜统计（保持数据一致性）
                try:
                    from core.referral_db import referral_db
                    if exchange == "all":
                        # 全部交易所：直接删除排行榜统计
                        deleted_stats = referral_db.delete_user_profit_stats(uid)
                        if deleted_stats > 0:
                            logger.info(f"Deleted {deleted_stats} leaderboard stats for {uid}")
                    else:
                        # 单交易所：触发对账，从剩余交易重新计算排行榜
                        result = referral_db.reconcile_user_profit_stats(uid)
                        logger.info(f"Reconciled leaderboard stats for {uid} after {exchange} reset: {result}")
                except Exception as e:
                    logger.warning(f"Failed to update leaderboard stats for {uid}: {e}")
                
                # 3. 清理 Redis 中的兼容性数据
                if exchange == "all":
                    reset_fields[RedisKeys.field_trades()] = []
                    reset_fields[RedisKeys.field_trades_closed()] = {}
                    # 删除旧的 pf:closed:* 键（v1 兼容性键）
                    from core.database import get_async_redis
                    redis = await get_async_redis()
                    await redis.delete(f"pf:closed:h:{uid}")
                    await redis.delete(f"pf:closed:z:{uid}")
                for ex in target_exchanges:
                    reset_fields[RedisKeys.exchange_closed_trades(ex)] = {}
                reset_summary.append("已平仓交易")
                
            elif data_type == "ai_history":
                # AI 历史（全局数据，不分交易所，只在选择"全部交易所"时重置）
                if exchange == "all":
                    # 删除 MySQL 中的 AI 决策历史（主存储）
                    try:
                        from core.ai_decision_db import get_ai_decision_db
                        db = get_ai_decision_db()
                        deleted_count = db.delete_user_decisions(uid)
                        logger.info(f"Deleted {deleted_count} AI decisions from MySQL for {uid}")
                    except Exception as e:
                        logger.warning(f"Failed to delete MySQL ai_decisions for {uid}: {e}")
                    
                    # 清理 Redis 中可能残留的旧 ai_history 数据（兼容性清理）
                    reset_fields[RedisKeys.field_ai_history()] = {}
                    # 删除旧的 ZSET/HASH 格式数据
                    from core.database import get_async_redis
                    redis = await get_async_redis()
                    await redis.delete(RedisKeys.ai_history_zset(uid))
                    await redis.delete(RedisKeys.ai_history_data(uid))
                    
                    reset_summary.append("AI 历史")
                    
            elif data_type == "notifications":
                # 通知记录（全局数据，不分交易所，只在选择"全部交易所"时重置）
                # 主要存储在 MySQL，Redis 只有兼容性数据
                if exchange == "all":
                    # 1. 删除 MySQL 中的 notifications（主存储）
                    try:
                        from core.notifications_db import get_notifications_db
                        db = get_notifications_db()
                        deleted_count = db.delete_notifications(uid)
                        logger.info(f"Deleted {deleted_count} notifications from MySQL for {uid}")
                    except Exception as e:
                        logger.warning(f"Failed to delete MySQL notifications for {uid}: {e}")
                    
                    # 2. 清理 Redis 中的兼容性数据
                    reset_fields[RedisKeys.field_notifications()] = []
                    reset_summary.append("通知记录")
                    
            elif data_type == "cache":
                # 缓存数据
                if exchange == "all":
                    reset_fields[RedisKeys.field_cache()] = {}
                for ex in target_exchanges:
                    reset_fields[RedisKeys.exchange_account(ex)] = {}
                    reset_fields[RedisKeys.exchange_equity_init(ex)] = {}
                reset_summary.append("缓存数据")
                
            elif data_type == "decision_feedback":
                # 决策反馈（全局数据，不分交易所，只在选择"全部交易所"时重置）
                if exchange == "all":
                    reset_fields[RedisKeys.field_decision_feedback()] = {}
                    reset_summary.append("决策反馈")
        
        # 执行重置
        for field, value in reset_fields.items():
            RedisDataManager.set_user_field(uid, field, value)
        
        # 更新元数据
        metadata = RedisDataManager.get_user_field(uid, RedisKeys.field_metadata()) or {}
        metadata["last_reset"] = time.time()
        metadata["last_reset_exchange"] = exchange
        metadata["last_reset_types"] = data_types
        RedisDataManager.set_user_field(uid, RedisKeys.field_metadata(), metadata)
        
        exchange_text = "全部交易所" if exchange == "all" else exchange.upper()
        logger.info(f"[{uid}] Redis 数据已重置: {exchange_text} - {', '.join(reset_summary)}")
        
        return {
            "success": True,
            "message": f"已重置: {exchange_text} - {', '.join(reset_summary)}",
            "exchange": exchange,
            "data_types": data_types,
            "reset_fields_count": len(reset_fields),
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[{uid}] 重置 Redis 数据失败: {e}")
        raise HTTPException(status_code=500, detail=safe_error_detail(e, "重置"))


@router.get("/redis-stats")
async def get_redis_stats(
    user: Dict = Depends(get_current_user)
):
    """
    获取用户 Redis 数据统计
    """
    import asyncio
    uid = user["uid"]
    
    try:
        from core.redis_manager import RedisDataManager
        
        # 使用 to_thread 避免阻塞事件循环（get_user_stats 有大量同步 Redis 调用）
        stats = await asyncio.to_thread(RedisDataManager.get_user_stats, uid)
        
        return {
            "success": True,
            "stats": stats,
        }
        
    except Exception as e:
        logger.error(f"[{uid}] 获取 Redis 统计失败: {e}")
        raise HTTPException(status_code=500, detail=safe_error_detail(e, "获取 Redis 统计"))


# ============================================================
# 公开仪表盘设置 API
# ============================================================

@router.get("/public-dashboard/settings")
async def get_public_dashboard_settings(
    user: Dict = Depends(get_current_user)
):
    """获取公开仪表盘设置"""
    uid = user["uid"]
    
    from core.referral_db import referral_db
    
    settings = referral_db.get_public_dashboard_settings(uid)
    
    # 构建公开链接
    public_link = None
    if settings.get("public_dashboard") and settings.get("public_dashboard_token"):
        public_link = f"/dashboard/{settings['public_dashboard_token']}"
    
    return {
        "enabled": settings.get("public_dashboard", False),
        "token": settings.get("public_dashboard_token"),
        "display_name": settings.get("display_name"),
        "public_link": public_link,
    }


@router.put("/public-dashboard/settings")
async def update_public_dashboard_settings(
    enabled: bool = Query(..., description="是否开启公开仪表盘"),
    user: Dict = Depends(get_current_user)
):
    """更新公开仪表盘设置"""
    uid = user["uid"]
    
    from core.referral_db import referral_db
    
    result = referral_db.update_public_dashboard_settings(uid, enabled)
    
    if not result.get("success"):
        raise HTTPException(status_code=500, detail=result.get("error", "更新失败"))
    
    # 构建公开链接
    public_link = None
    if enabled and result.get("token"):
        public_link = f"/dashboard/{result['token']}"
    
    return {
        "message": "公开仪表盘已开启" if enabled else "公开仪表盘已关闭",
        "enabled": enabled,
        "token": result.get("token"),
        "public_link": public_link,
    }


# ============================================================
# 公开仪表盘数据 API（无需登录）
# ============================================================

public_router = APIRouter(prefix="/api/public", tags=["公开仪表盘"])


@public_router.get("/dashboard/{token}")
async def get_public_dashboard_data(
    token: str,
    request: Request,
    limit: int = Query(default=-1, description="统计计算使用的交易数量（-1 表示全部）"),
    closed_limit: int = Query(default=10, ge=1, le=200, description="已平仓交易每页数量"),
    offset: int = Query(default=0, ge=0, description="已平仓交易偏移量"),
    exchange: str = Query(default=None, description="交易所筛选（可选）"),
):
    """
    获取公开仪表盘数据
    
    通过 token 访问用户的公开仪表盘数据，不需要登录。
    返回与 /api/dashboard 相同的数据结构，支持分页和交易所筛选。
    """
    from core.referral_db import referral_db
    from core.pf_compatibility import pf_compat
    from exchanges import EXCHANGE_NAMES
    
    # 限流检查（基于 IP）
    client_ip = request.client.host if request.client else "unknown"
    rate_limit_key = f"public_dashboard_rate:{client_ip}"
    
    # 每分钟最多 30 次请求
    current_count = await _check_rate_limit(rate_limit_key, max_requests=30, window_seconds=60)
    if current_count > 30:
        raise HTTPException(status_code=429, detail="请求过于频繁，请稍后再试")
    
    # 通过 token 获取用户 ID
    uid = referral_db.get_user_by_dashboard_token(token)
    
    if not uid:
        raise HTTPException(status_code=404, detail="仪表盘不存在或已关闭")
    
    # 获取用户设置（显示名称等）
    settings = referral_db.get_public_dashboard_settings(uid)
    display_name = settings.get("display_name") or f"{uid[:4]}****{uid[-4:]}"
    
    # 导入 web.py 中的辅助函数
    from api.web import (
        get_active_positions, get_account, get_initial_equity,
        calculate_statistics, calculate_statistics_async,
        build_equity_curve, build_equity_curve_async, calc_max_drawdown_peak,
        live_pnl_batch, ClosedTrade, redis_client
    )
    
    # 获取持仓数据
    positions_raw = get_active_positions(uid, exchange)
    account = get_account(uid, exchange) or {}
    
    # 计算实时盈亏
    positions_dict_list = [p.dict() for p in positions_raw]
    positions_with_live = live_pnl_batch(redis_client, positions_dict_list, stale_ms=3000)
    
    positions = []
    for live in positions_with_live:
        live["officialUnrealizedPnl"] = live.get("unrealizedPnl", "0")
        live["unrealizedPnl"] = live.get("liveUnrealizedPnl", live.get("unrealizedPnl", "0"))
        positions.append(live)
    
    # 获取初始权益
    initial_equity = get_initial_equity(uid, exchange)
    
    # 获取所有已关闭交易
    all_closed_trades = pf_compat.get_pf_closed_h(uid, exchange, add_exchange_field=True)
    closed_total = len(all_closed_trades) if all_closed_trades else 0
    
    # 排序（最新的在前面）
    sorted_trades = sorted(
        all_closed_trades.items(),
        key=lambda x: int(x[1].get('closeTimeMs', '0') if isinstance(x[1], dict) else '0'),
        reverse=True
    ) if all_closed_trades else []
    
    # 统计/曲线：最近 N 笔 or 全部
    if limit == -1:
        stats_trades_data = sorted_trades
        stats_limit_value = -1
    else:
        stats_trades_data = sorted_trades[:limit]
        stats_limit_value = limit
    
    # 转换为 ClosedTrade 对象
    trades_for_stats = []
    for trade_id, trade_data in stats_trades_data:
        try:
            trades_for_stats.append(ClosedTrade(**trade_data))
        except Exception:
            continue
    
    # 计算统计数据 (async to avoid blocking event loop)
    stats, equity_curve = await asyncio.gather(
        calculate_statistics_async(positions_raw, trades_for_stats, initial_equity),
        build_equity_curve_async(trades_for_stats, initial_equity),
    )
    mdd_peak = calc_max_drawdown_peak(equity_curve)
    
    # 覆盖"未实现收益"为展示口径
    try:
        total_live_unreal = sum(float(x.get("unrealizedPnl") or 0) for x in positions)
    except Exception:
        total_live_unreal = 0.0
    stats.totalUnrealizedPnl = round(total_live_unreal, 2)
    
    # 列表分页
    closed_trades_page_data = sorted_trades[offset:offset + closed_limit]
    closed_trades_page = []
    for trade_id, trade_data in closed_trades_page_data:
        try:
            closed_trades_page.append(ClosedTrade(**trade_data))
        except Exception:
            continue
    
    # 获取启用的交易所列表
    enabled_exchanges = config_loader.get_enabled_exchanges(uid)
    exchanges_data = []
    for ex in enabled_exchanges:
        try:
            ex_account = pf_compat.get_pf_account(uid, ex)
            ex_balance = float(ex_account.get("walletBalance", 0) or 0) if ex_account else 0
            ex_positions = pf_compat.get_pf_pos(uid, ex)
            ex_pos_count = len([p for p in (ex_positions or {}).values() if float(p.get("qty", 0) or 0) > 0])
            
            exchanges_data.append({
                "exchange": ex,
                "display_name": EXCHANGE_NAMES.get(ex, ex),
                "balance": round(ex_balance, 2),
                "positions_count": ex_pos_count,
                "status": "connected",  # 公开模式不显示实际连接状态
            })
        except Exception:
            continue
    
    # ✅ 获取所有涉及币种的精度信息
    symbols_with_exchange = []
    for pos in positions:
        symbols_with_exchange.append({
            "symbol": pos.get("symbol"),
            "exchange": pos.get("exchange") or exchange or "binance"
        })
    for trade_id, trade_data in closed_trades_page_data:
        symbols_with_exchange.append({
            "symbol": trade_data.get("symbol"),
            "exchange": trade_data.get("exchange") or exchange or "binance"
        })
    
    # 批量获取精度
    from core.symbol_precision import get_symbols_precision_batch_async
    symbol_precisions = await get_symbols_precision_batch_async(symbols_with_exchange)
    
    return {
        "display_name": display_name,
        "statistics": stats,
        "statsLimit": stats_limit_value,
        "equityCurve": equity_curve,
        "initialEquity": round(initial_equity, 2) if initial_equity is not None else None,
        "account": account,
        "walletBalance": float(account.get("walletBalance")) if account and account.get("walletBalance") is not None else None,
        "equity": float(account.get("equity")) if account and account.get("equity") is not None else None,
        "unrealized": float(account.get("unrealized")) if account and account.get("unrealized") is not None else None,
        "maxDrawdownPeakPct": mdd_peak["maxDrawdownPeakPct"],
        "maxDrawdownPeakAmount": mdd_peak["maxDrawdownPeakAmount"],
        "maxDrawdownPeakFrom": mdd_peak["maxDrawdownPeakFrom"],
        "maxDrawdownPeakTo": mdd_peak["maxDrawdownPeakTo"],
        "maxDrawdownPeakEquity": mdd_peak["maxDrawdownPeakEquity"],
        "maxDrawdownTroughEquity": mdd_peak["maxDrawdownTroughEquity"],
        "positions": positions,
        "closedTrades": closed_trades_page,
        "closedTotal": closed_total,
        "closedLimit": closed_limit,
        "closedOffset": offset,
        "enabledExchanges": exchanges_data,
        # ✅ 币种精度信息（用于前端格式化显示）
        "symbolPrecisions": symbol_precisions,
    }


async def _check_rate_limit(key: str, max_requests: int, window_seconds: int) -> int:
    """
    简单的限流检查
    
    Returns:
        当前窗口内的请求次数
    """
    try:
        from core.database import get_async_redis
        
        redis = await get_async_redis()
        current_time = int(time.time())
        window_key = f"{key}:{current_time // window_seconds}"
        
        # 增加计数 - native async Redis
        count = await redis.incr(window_key)
        
        # 设置过期时间（只在第一次设置）
        if count == 1:
            await redis.expire(window_key, window_seconds + 1)
        
        return count
    except Exception:
        # 如果 Redis 出错，允许请求通过
        return 0


@public_router.get("/analysis-history/{token}")
async def get_public_analysis_history(
    token: str,
    request: Request,
    limit: int = Query(default=50, ge=1, le=100, description="每页数量"),
    offset: int = Query(default=0, ge=0, description="偏移量"),
):
    """
    获取公开仪表盘的 AI 决策历史（旧版，返回完整数据）
    
    通过 token 访问用户的 AI 决策历史，不需要登录。
    已废弃，请使用 /analysis-history-v2/{token}
    """
    from core.referral_db import referral_db
    from core.async_redis import get_ai_history_paginated_async
    
    # 限流检查（基于 IP）
    client_ip = request.client.host if request.client else "unknown"
    rate_limit_key = f"public_analysis_rate:{client_ip}"
    
    # 每分钟最多 20 次请求
    current_count = await _check_rate_limit(rate_limit_key, max_requests=20, window_seconds=60)
    if current_count > 20:
        raise HTTPException(status_code=429, detail="请求过于频繁，请稍后再试")
    
    # 通过 token 获取用户 ID
    uid = referral_db.get_user_by_dashboard_token(token)
    
    if not uid:
        raise HTTPException(status_code=404, detail="仪表盘不存在或已关闭")
    
    # 导入 web.py 中的辅助函数
    from api.web import _normalize_analysis_item
    
    # 获取 AI 决策历史（从 MySQL）
    history_items, total = await get_ai_history_paginated_async(uid, offset, limit)
    
    if not history_items:
        return {"items": [], "total": total, "limit": limit, "offset": offset}
    
    # 转换数据格式以兼容前端期望
    items = []
    for history_item in history_items:
        request_data = history_item.get("request", {})
        response_data = history_item.get("response", {})
        item = _normalize_analysis_item(request_data, response_data)
        items.append(item)
    
    return {"items": items, "total": total, "limit": limit, "offset": offset}


@public_router.get("/analysis-history-v2/{token}")
async def get_public_analysis_history_v2(
    token: str,
    request: Request,
    limit: int = Query(default=50, ge=1, le=100, description="每页数量"),
    offset: int = Query(default=0, ge=0, description="偏移量"),
):
    """
    获取公开仪表盘的 AI 决策历史摘要（V2 优化版）
    
    相比旧版：
    - 不返回完整的 request/response JSON
    - 只返回摘要字段，数据量减少 90%+
    - 详情通过 /analysis-history-v2/{token}/{id} 按需加载
    """
    from core.referral_db import referral_db
    from core.async_redis import get_ai_history_summary_paginated_async
    
    # 限流检查（基于 IP）
    client_ip = request.client.host if request.client else "unknown"
    rate_limit_key = f"public_analysis_rate:{client_ip}"
    
    # 每分钟最多 30 次请求（V2 API 请求更频繁，因为需要单独加载详情）
    current_count = await _check_rate_limit(rate_limit_key, max_requests=30, window_seconds=60)
    if current_count > 30:
        raise HTTPException(status_code=429, detail="请求过于频繁，请稍后再试")
    
    # 通过 token 获取用户 ID
    uid = referral_db.get_user_by_dashboard_token(token)
    
    if not uid:
        raise HTTPException(status_code=404, detail="仪表盘不存在或已关闭")
    
    # 获取摘要列表
    summaries, total = await get_ai_history_summary_paginated_async(uid, offset, limit)
    
    return {"items": summaries, "total": total, "limit": limit, "offset": offset}


@public_router.get("/analysis-history-v2/{token}/{decision_id}")
async def get_public_analysis_history_detail_v2(
    token: str,
    decision_id: int,
    request: Request,
):
    """
    获取公开仪表盘的单条 AI 决策详情（V2）
    
    用于列表点击后按需加载详情
    """
    from core.referral_db import referral_db
    from core.async_redis import get_ai_decision_detail_async
    from api.web import _normalize_analysis_item
    
    # 限流检查（基于 IP）
    client_ip = request.client.host if request.client else "unknown"
    rate_limit_key = f"public_analysis_detail_rate:{client_ip}"
    
    # 每分钟最多 60 次详情请求
    current_count = await _check_rate_limit(rate_limit_key, max_requests=60, window_seconds=60)
    if current_count > 60:
        raise HTTPException(status_code=429, detail="请求过于频繁，请稍后再试")
    
    # 通过 token 获取用户 ID
    uid = referral_db.get_user_by_dashboard_token(token)
    
    if not uid:
        raise HTTPException(status_code=404, detail="仪表盘不存在或已关闭")
    
    # 获取详情
    detail = await get_ai_decision_detail_async(uid, decision_id)
    
    if not detail:
        raise HTTPException(status_code=404, detail="决策记录不存在")
    
    # 格式化为前端期望的格式
    request_data = detail.get("request", {})
    response_data = detail.get("response", {})
    
    item = _normalize_analysis_item(request_data, response_data)
    item["id"] = decision_id
    item["timestamp"] = detail.get("timestamp")
    
    return item


# ============================================================
# 交易统计 API（MySQL 数据源）
# ============================================================

@router.get("/trades/list")
async def get_trades_list(
    user: Dict = Depends(get_current_user),
    exchange: str = Query(default=None, description="交易所筛选"),
    symbol: str = Query(default=None, description="币种筛选"),
    side: str = Query(default=None, description="方向筛选 (LONG/SHORT)"),
    pnl_filter: str = Query(default=None, description="盈亏筛选 (profit/loss/all)"),
    start_time: int = Query(default=None, description="开始时间戳(ms)"),
    end_time: int = Query(default=None, description="结束时间戳(ms)"),
    order_by: str = Query(default="close_time_ms", description="排序字段"),
    order_dir: str = Query(default="DESC", description="排序方向"),
    limit: int = Query(default=50, ge=1, le=200, description="每页数量"),
    offset: int = Query(default=0, ge=0, description="偏移量"),
):
    """
    获取已平仓交易列表（支持筛选和分页）
    
    数据源: MySQL closed_trades 表
    """
    uid = user["uid"]
    
    try:
        from core.closed_trades_db import get_closed_trades_db
        db = get_closed_trades_db()
        
        trades, total = db.get_trades_paginated(
            uid=uid,
            exchange=exchange,
            offset=offset,
            limit=limit,
            symbol=symbol,
            side=side,
            pnl_filter=pnl_filter,
            start_time_ms=start_time,
            end_time_ms=end_time,
            order_by=order_by,
            order_dir=order_dir
        )
        
        return {
            "success": True,
            "data": trades,
            "total": total,
            "offset": offset,
            "limit": limit,
            "filters": {
                "exchange": exchange,
                "symbol": symbol,
                "side": side,
                "pnl_filter": pnl_filter,
            }
        }
    except Exception as e:
        logger.error(f"Failed to get trades list for {uid}: {e}")
        raise HTTPException(status_code=500, detail=safe_error_detail(e, "获取交易列表"))


@router.get("/trades/statistics")
async def get_trades_statistics(
    user: Dict = Depends(get_current_user),
    exchange: str = Query(default=None, description="交易所筛选"),
    symbol: str = Query(default=None, description="币种筛选"),
    days: int = Query(default=30, ge=1, le=365, description="统计天数"),
):
    """
    获取交易统计数据
    
    返回:
    - 总交易数、胜率、总盈亏
    - 平均盈亏、最大盈利、最大亏损
    - 盈亏比、平均持仓时长
    - 总手续费、总资金费
    """
    uid = user["uid"]
    
    try:
        from core.closed_trades_db import get_closed_trades_db
        db = get_closed_trades_db()
        
        stats = db.get_statistics(
            uid=uid,
            exchange=exchange,
            days=days,
            symbol=symbol
        )
        
        return {
            "success": True,
            "data": stats
        }
    except Exception as e:
        logger.error(f"Failed to get trade statistics for {uid}: {e}")
        raise HTTPException(status_code=500, detail=safe_error_detail(e, "获取交易统计"))


@router.get("/trades/symbol-breakdown")
async def get_trades_symbol_breakdown(
    user: Dict = Depends(get_current_user),
    exchange: str = Query(default=None, description="交易所筛选"),
    days: int = Query(default=30, ge=1, le=365, description="统计天数"),
):
    """
    获取按币种分组的交易统计
    
    返回每个币种的:
    - 交易次数、胜率、总盈亏、平均盈亏
    """
    uid = user["uid"]
    
    try:
        from core.closed_trades_db import get_closed_trades_db
        db = get_closed_trades_db()
        
        breakdown = db.get_symbol_breakdown(
            uid=uid,
            exchange=exchange,
            days=days
        )
        
        return {
            "success": True,
            "data": breakdown
        }
    except Exception as e:
        logger.error(f"Failed to get symbol breakdown for {uid}: {e}")
        raise HTTPException(status_code=500, detail=safe_error_detail(e, "获取币种统计"))


@router.get("/trades/daily-pnl")
async def get_trades_daily_pnl(
    user: Dict = Depends(get_current_user),
    exchange: str = Query(default=None, description="交易所筛选"),
    days: int = Query(default=30, ge=1, le=365, description="统计天数"),
):
    """
    获取每日盈亏统计
    
    返回每天的:
    - 日期、交易次数、胜率、当日盈亏
    """
    uid = user["uid"]
    
    try:
        from core.closed_trades_db import get_closed_trades_db
        db = get_closed_trades_db()
        
        daily_pnl = db.get_daily_pnl(
            uid=uid,
            exchange=exchange,
            days=days
        )
        
        return {
            "success": True,
            "data": daily_pnl
        }
    except Exception as e:
        logger.error(f"Failed to get daily PnL for {uid}: {e}")
        raise HTTPException(status_code=500, detail=safe_error_detail(e, "获取每日盈亏"))


@router.get("/trades/{cycle_id}")
async def get_trade_detail(
    cycle_id: str,
    user: Dict = Depends(get_current_user),
    exchange: str = Query(..., description="交易所"),
):
    """
    获取单条交易详情（含原始数据）
    """
    uid = user["uid"]
    
    try:
        from core.closed_trades_db import get_closed_trades_db
        db = get_closed_trades_db()
        
        trade = db.get_trade_by_cycle_id(uid, exchange, cycle_id)
        
        if not trade:
            raise HTTPException(status_code=404, detail="交易记录不存在")
        
        return {
            "success": True,
            "data": trade
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get trade detail for {uid}: {e}")
        raise HTTPException(status_code=500, detail=safe_error_detail(e, "获取交易详情"))


# ============================================================
# 通知 API（MySQL 数据源）
# ============================================================

@router.get("/notifications/list")
async def get_notifications_list(
    user: Dict = Depends(get_current_user),
    notification_type: str = Query(default=None, description="通知类型筛选"),
    unread_only: bool = Query(default=False, description="只获取未读"),
    limit: int = Query(default=50, ge=1, le=200, description="每页数量"),
    offset: int = Query(default=0, ge=0, description="偏移量"),
):
    """
    获取通知列表（支持筛选和分页）
    """
    uid = user["uid"]
    
    try:
        from core.notifications_db import get_notifications_db
        db = get_notifications_db()
        
        notifications, total = db.get_notifications_paginated(
            uid=uid,
            offset=offset,
            limit=limit,
            notification_type=notification_type,
            unread_only=unread_only
        )
        
        return {
            "success": True,
            "data": notifications,
            "total": total,
            "offset": offset,
            "limit": limit,
        }
    except Exception as e:
        logger.error(f"Failed to get notifications for {uid}: {e}")
        raise HTTPException(status_code=500, detail=safe_error_detail(e, "获取通知列表"))


@router.post("/notifications/mark-read")
async def mark_notifications_read(
    user: Dict = Depends(get_current_user),
    notification_ids: List[int] = None,
):
    """
    标记通知为已读
    
    Args:
        notification_ids: 通知 ID 列表，None 表示全部标记
    """
    uid = user["uid"]
    
    try:
        from core.notifications_db import get_notifications_db
        db = get_notifications_db()
        
        updated = db.mark_as_read(uid, notification_ids)
        
        return {
            "success": True,
            "updated": updated
        }
    except Exception as e:
        logger.error(f"Failed to mark notifications as read for {uid}: {e}")
        raise HTTPException(status_code=500, detail=safe_error_detail(e, "标记通知已读"))


@router.get("/notifications/unread-count")
async def get_notifications_unread_count(
    user: Dict = Depends(get_current_user),
):
    """
    获取未读通知数量
    """
    uid = user["uid"]
    
    try:
        from core.notifications_db import get_notifications_db
        db = get_notifications_db()
        
        count = db.get_unread_count(uid)
        
        return {
            "success": True,
            "unread_count": count
        }
    except Exception as e:
        logger.error(f"Failed to get unread count for {uid}: {e}")
        raise HTTPException(status_code=500, detail=safe_error_detail(e, "获取未读数量"))


@router.delete("/notifications")
async def delete_notifications(
    user: Dict = Depends(get_current_user),
    notification_ids: List[int] = Query(default=None, description="要删除的通知 ID"),
    older_than_days: int = Query(default=None, description="删除 N 天前的通知"),
):
    """
    删除通知
    """
    uid = user["uid"]
    
    try:
        from core.notifications_db import get_notifications_db
        db = get_notifications_db()
        
        deleted = db.delete_notifications(
            uid=uid,
            notification_ids=notification_ids,
            older_than_days=older_than_days
        )
        
        return {
            "success": True,
            "deleted": deleted
        }
    except Exception as e:
        logger.error(f"Failed to delete notifications for {uid}: {e}")
        raise HTTPException(status_code=500, detail=safe_error_detail(e, "删除通知"))

