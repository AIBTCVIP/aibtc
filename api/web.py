# web.py
"""
Web API 模块 - 支持多用户

改造说明：
1. 保留原有的 /{uid} 路由格式
2. 新增用户管理 API（通过 user_api router）
3. 支持 JWT 认证
4. 支持 lifespan 管理（纯 asyncio 架构）

安全特性:
- HttpOnly Cookie 认证
- 安全响应头 (CSP, HSTS, X-Frame-Options 等)
"""
from contextlib import asynccontextmanager
from fastapi import FastAPI, Query, Depends, Request, HTTPException
from fastapi.responses import HTMLResponse, FileResponse, Response, JSONResponse

# 尝试使用 FastJSONResponse（更快），如果不可用则回退到 JSONResponse
try:
    import orjson
    from fastapi.responses import FastJSONResponse
    FastJSONResponse = FastJSONResponse
except ImportError:
    FastJSONResponse = JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from starlette.middleware.base import BaseHTTPMiddleware
from pydantic import BaseModel
from typing import List, Dict, Optional, Any
import os
import json
import redis
import time
import threading
import uvicorn
import logging
import asyncio

# 静态资源版本号（服务启动时生成，用于缓存控制）
STATIC_VERSION = str(int(time.time()))
from core.config import REDIS_HOST, REDIS_PORT, REDIS_DB, WEB_HOST, WEB_PORT
from core.redis_manager import RedisDataManager
from core.constants import decimal_safe as D
from api.user_api import get_current_user
from api.error_utils import safe_error_detail

logger = logging.getLogger(__name__)


# =============================================================================
# Lifespan 管理（纯 asyncio 架构支持）
# =============================================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    FastAPI 生命周期管理
    
    在纯 asyncio 架构中，此 lifespan 用于：
    - 启动时：记录日志，可执行初始化
    - 关闭时：清理资源
    
    注意：主要的初始化工作由 lifecycle.py 管理
    """
    # 启动
    logger.info("FastAPI 应用启动")
    yield
    # 关闭
    logger.info("FastAPI 应用关闭")


app = FastAPI(
    title="AIBTC.VIP 多用户交易系统",
    lifespan=lifespan
)


# ==================== 安全中间件 ====================

class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    """
    安全响应头中间件
    
    添加以下安全头:
    - X-Content-Type-Options: 防止 MIME 类型嗅探
    - X-Frame-Options: 防止点击劫持
    - X-XSS-Protection: XSS 过滤（旧浏览器）
    - Referrer-Policy: 控制 Referer 头
    - Permissions-Policy: 限制浏览器功能
    - Content-Security-Policy: 内容安全策略（渐进实施）
    """
    
    async def dispatch(self, request: Request, call_next):
        response: Response = await call_next(request)
        
        # 基础安全头
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["X-XSS-Protection"] = "1; mode=block"
        response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
        
        # 权限策略 - 限制敏感功能
        response.headers["Permissions-Policy"] = (
            "geolocation=(), microphone=(), camera=(), "
            "payment=(), usb=(), magnetometer=(), gyroscope=()"
        )
        
        # CSP - 渐进式实施（report-only 模式可用于调试）
        # 注意：由于使用内联脚本和外部 CDN，暂时需要 'unsafe-inline' 和 CDN 域名
        # 后续迁移到本地文件后可收紧策略
        csp_directives = [
            "default-src 'self'",
            # 脚本来源：允许本站、内联脚本、CDN
            "script-src 'self' 'unsafe-inline' 'unsafe-eval' https://cdn.tailwindcss.com https://unpkg.com https://cdn.jsdelivr.net",
            # 样式来源：Tailwind 需要内联样式
            "style-src 'self' 'unsafe-inline' https://cdn.tailwindcss.com https://fonts.googleapis.com",
            "img-src 'self' data: blob:",
            "font-src 'self' https://fonts.gstatic.com",
            "connect-src 'self' wss: ws: https://cdn.jsdelivr.net",  # WebSocket 连接 + CDN source maps
            "frame-ancestors 'none'",
            "base-uri 'self'",
            "form-action 'self'",
        ]
        response.headers["Content-Security-Policy"] = "; ".join(csp_directives)
        
        # 生产环境应启用 HSTS
        # response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
        
        return response


# 请求耗时监控中间件（只记录慢请求）
@app.middleware("http")
async def request_timing_middleware(request: Request, call_next):
    import time
    if request.url.path.startswith("/api/"):
        start = time.perf_counter()
        response = await call_next(request)
        elapsed = (time.perf_counter() - start) * 1000
        if elapsed > 500:  # 只记录超过 500ms 的请求
            logger.warning(f"[SLOW REQUEST] {request.method} {request.url.path} took {elapsed:.0f}ms")
        return response
    return await call_next(request)

# 添加安全中间件
app.add_middleware(SecurityHeadersMiddleware)

# ==================== P2 安全: 全局 API 限速中间件 ====================

# 限速配置
API_RATE_LIMIT_REQUESTS = 100  # 每窗口最大请求数
API_RATE_LIMIT_WINDOW = 60  # 窗口时间（秒）
_api_rate_limits: Dict[str, tuple] = {}  # {ip: (count, window_start)}
# P1 Fix: 延迟初始化 asyncio.Lock，避免模块加载时创建
_api_rate_limits_lock: Optional[asyncio.Lock] = None
_api_rate_limits_init_lock = threading.Lock()


def _get_api_rate_limits_lock() -> asyncio.Lock:
    """获取 API 限速锁（延迟初始化）"""
    global _api_rate_limits_lock
    if _api_rate_limits_lock is None:
        with _api_rate_limits_init_lock:
            if _api_rate_limits_lock is None:
                _api_rate_limits_lock = asyncio.Lock()
    return _api_rate_limits_lock


async def _cleanup_rate_limits():
    """清理过期的限速记录"""
    now = time.time()
    async with _get_api_rate_limits_lock():
        expired = [ip for ip, (_, ts) in _api_rate_limits.items() if now - ts > API_RATE_LIMIT_WINDOW * 2]
        for ip in expired:
            del _api_rate_limits[ip]


@app.middleware("http")
async def api_rate_limit_middleware(request: Request, call_next):
    """
    全局 API 限速中间件
    
    对 /api/ 路径的请求进行限速，防止 DoS 攻击
    限制: 每 IP 每分钟最多 100 次请求
    """
    # 只对 API 路径限速
    if not request.url.path.startswith("/api/"):
        return await call_next(request)
    
    # 排除公开端点（已有独立限速）
    if request.url.path.startswith("/api/public/"):
        return await call_next(request)
    
    client_ip = request.client.host if request.client else "unknown"
    now = time.time()
    
    async with _get_api_rate_limits_lock():
        if client_ip in _api_rate_limits:
            count, window_start = _api_rate_limits[client_ip]
            
            if now - window_start > API_RATE_LIMIT_WINDOW:
                # 窗口已过期，重置
                _api_rate_limits[client_ip] = (1, now)
            elif count >= API_RATE_LIMIT_REQUESTS:
                # 超过限制
                remaining = int(API_RATE_LIMIT_WINDOW - (now - window_start))
                logger.warning(f"API 限速触发: ip={client_ip}, path={request.url.path}")
                return FastJSONResponse(
                    status_code=429,
                    content={"detail": f"请求过于频繁，请在 {remaining} 秒后重试"}
                )
            else:
                # 增加计数
                _api_rate_limits[client_ip] = (count + 1, window_start)
        else:
            # 首次请求
            _api_rate_limits[client_ip] = (1, now)
    
    # 定期清理（每 100 次请求清理一次）
    if len(_api_rate_limits) > 1000:
        asyncio.create_task(_cleanup_rate_limits())
    
    return await call_next(request)

# GZip 压缩中间件（对大于 1KB 的响应启用压缩，可减少 70% 传输体积）
app.add_middleware(GZipMiddleware, minimum_size=1000)

# CORS 配置（支持前端跨域）
# 从环境变量读取允许的域名，生产环境必须设置
_cors_origins = os.getenv("CORS_ORIGINS", "")
_is_production = os.getenv("ENVIRONMENT", "development").lower() == "production"

if _cors_origins:
    # 支持逗号分隔的多个域名
    CORS_ALLOW_ORIGINS = [origin.strip() for origin in _cors_origins.split(",") if origin.strip()]
elif _is_production:
    # P1 Fix: 生产环境必须设置 CORS_ORIGINS
    raise RuntimeError(
        "生产环境必须设置 CORS_ORIGINS 环境变量！"
        "示例: CORS_ORIGINS=https://example.com,https://app.example.com"
    )
else:
    # 开发环境默认允许本地域名
    import warnings
    warnings.warn(
        "CORS_ORIGINS 环境变量未设置！使用开发环境默认配置。"
        "生产环境请设置 CORS_ORIGINS 环境变量。",
        RuntimeWarning
    )
    CORS_ALLOW_ORIGINS = ["http://localhost:3000", "http://localhost:8080", "http://127.0.0.1:3000"]

# P1 Fix: 限制允许的方法和头部
app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ALLOW_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["Authorization", "Content-Type", "X-CSRF-Token", "X-Requested-With"],
)

# 注册用户管理 API
from api.user_api import router as user_router
app.include_router(user_router)

# 注册管理员 API
from api.admin_api import router as admin_router
app.include_router(admin_router)

# 注册邀请返佣和排行榜 API
from api.referral_api import router as referral_router, admin_router as referral_admin_router
app.include_router(referral_router)
app.include_router(referral_admin_router)

# 注册公开仪表盘 API（无需登录）
from api.user_api import public_router
app.include_router(public_router)

# ==================== Redis 连接 ====================
# P2 Fix: 使用共享连接池，避免连接碎片化
from core.database import redis_client

# ==================== 异步 Redis 数据获取（用于 API 层，不阻塞事件循环）====================

async def get_active_positions_async(uid: str, exchange: str = None) -> List["Position"]:
    """
    异步获取当前活跃持仓（带 exchange 字段）
    
    使用异步 Redis 客户端，不阻塞事件循环
    """
    try:
        from core.async_redis import async_pf_compat
        
        is_global = exchange is None
        
        # 并行获取三个数据
        active_fields, pos_data, cycle_data = await asyncio.gather(
            async_pf_compat.get_pf_pos_active_async(uid, exchange),
            async_pf_compat.get_pf_pos_async(uid, exchange, add_exchange_field=is_global),
            async_pf_compat.get_pf_cycle_async(uid, exchange, add_exchange_prefix=is_global),
        )
        
        if not active_fields:
            return []
        
        positions: List[Position] = []
        for field in active_fields:
            if field in pos_data:
                pos_dict = pos_data[field].copy()
                
                # 确保必需字段有默认值
                if "unrealizedPnl" not in pos_dict:
                    pos_dict["unrealizedPnl"] = "0"
                
                # 从 cycle 数据中补充信息
                if field in cycle_data:
                    cycle = cycle_data[field]
                    if not pos_dict.get("stopLossPrice"):
                        pos_dict["stopLossPrice"] = cycle.get("stopLossPrice") or cycle.get("stopLoss")
                    if not pos_dict.get("takeProfitPrice"):
                        pos_dict["takeProfitPrice"] = cycle.get("takeProfitPrice") or cycle.get("takeProfit")
                    if not pos_dict.get("openOrderType"):
                        pos_dict["openOrderType"] = cycle.get("openOrderType")
                
                positions.append(Position(**pos_dict))
        return positions
    except Exception as e:
        logger.error(f"[Async] 获取持仓数据错误: {e}")
        return []


async def get_account_async(uid: str, exchange: str = None) -> Optional[Dict]:
    """异步获取当前账户余额/权益"""
    try:
        from core.async_redis import async_pf_compat
        return await async_pf_compat.get_pf_account_async(uid, exchange)
    except Exception as e:
        logger.error(f"[Async] 读取 account 错误: {e}")
        return None


async def get_initial_equity_async(uid: str, exchange: str = None) -> Optional[float]:
    """异步获取初始资金快照"""
    try:
        from core.async_redis import async_pf_compat
        equity_data = await async_pf_compat.get_pf_equity_init_async(uid, exchange)
        if not equity_data:
            return None
        
        if isinstance(equity_data, dict):
            return float(equity_data.get("walletBalance") or equity_data.get("initialEquity", 0))
        else:
            raw = str(equity_data).strip()
            try:
                return float(raw)
            except Exception as e:
                logger.debug(f"解析 initial_equity 失败: raw={raw}, error={e}")
        return None
    except Exception as e:
        logger.error(f"[Async] 读取 initial_equity 错误: {e}")
        return None


async def get_closed_trades_async(uid: str, exchange: str = None, add_exchange_field: bool = True) -> Dict[str, Any]:
    """异步获取已关闭交易"""
    try:
        from core.async_redis import async_pf_compat
        return await async_pf_compat.get_pf_closed_h_async(uid, exchange, add_exchange_field=add_exchange_field)
    except Exception as e:
        logger.error(f"[Async] 获取已关闭交易错误: {e}")
        return {}


async def live_pnl_batch_async(positions: List[dict], *, stale_ms: int = 3000) -> List[dict]:
    """
    异步批量计算持仓的实时盈亏
    
    使用异步 Redis 客户端批量获取 mark prices
    """
    if not positions:
        return []
    
    now = int(time.time() * 1000)
    
    # 第一步：找出需要获取 mark price 的 symbols
    symbols_to_fetch = set()
    for pos in positions:
        try:
            updated_at = int(pos.get("updatedAt") or "0")
        except Exception:
            updated_at = 0
        
        if now - updated_at <= stale_ms:
            continue
        
        symbol = pos.get("symbol")
        qty = D(pos.get("qty") or "0")
        entry = D(pos.get("entryPrice") or "0")
        
        if symbol and qty > 0 and entry > 0:
            symbols_to_fetch.add(symbol)
    
    # 第二步：异步批量获取 mark prices
    mark_prices = {}
    if symbols_to_fetch:
        from core.async_redis import get_mark_prices_batch_async
        symbols_list = list(symbols_to_fetch)
        
        # 获取原始价格数据
        from core.async_redis import get_async_redis
        from core.database import RedisKeys
        
        async_redis = await get_async_redis()
        keys = [RedisKeys.market_prices(s) for s in symbols_list]
        raw_results = await async_redis.mget(keys)
        
        for i, symbol in enumerate(symbols_list):
            raw = raw_results[i]
            if raw:
                try:
                    mk = json.loads(raw)
                    mark = D(mk.get("markPrice") or "0")
                    if mark > 0:
                        mark_prices[symbol] = {
                            "mark": mark,
                            "ts": mk.get("ts")
                        }
                except Exception as e:
                    logger.debug(f"解析 mark_price 失败: symbol={symbol}, error={e}")
    
    # 第三步：处理每个持仓
    results = []
    for pos in positions:
        out = pos.copy()
        
        try:
            updated_at = int(pos.get("updatedAt") or "0")
        except Exception:
            updated_at = 0
        
        # 如果数据不过期，直接使用原始数据
        if now - updated_at <= stale_ms:
            out["liveUnrealizedPnl"] = out.get("unrealizedPnl", "0")
            out["liveSource"] = "FRESH_DATA"
            results.append(out)
            continue
        
        symbol = pos.get("symbol")
        side = (pos.get("side") or "").upper()
        qty = D(pos.get("qty") or "0")
        entry = D(pos.get("entryPrice") or "0")
        
        if not symbol or qty <= 0 or entry <= 0:
            out["liveUnrealizedPnl"] = out.get("unrealizedPnl", "0")
            out["liveSource"] = "FALLBACK_NO_DATA"
            results.append(out)
            continue
        
        if symbol not in mark_prices:
            out["liveUnrealizedPnl"] = out.get("unrealizedPnl", "0")
            out["liveSource"] = "FALLBACK_NO_MARK"
            results.append(out)
            continue
        
        mk_data = mark_prices[symbol]
        mark = mk_data["mark"]
        
        pnl = (mark - entry) * qty if side == "LONG" else (entry - mark) * qty
        out["liveUnrealizedPnl"] = str(pnl)
        out["liveSource"] = "MARK_PRICE_EST"
        out["markPrice"] = str(mark)
        out["markTs"] = mk_data.get("ts")
        results.append(out)
    
    return results

# ==================== 静态文件（带缓存优化）====================

class CachedStaticFiles(StaticFiles):
    """
    带缓存头的静态文件服务
    
    缓存策略：
    - JS/CSS/HTML：不缓存，确保用户获取最新版本
    - 字体：长期缓存（很少变化）
    - 图片：中期缓存
    """
    async def get_response(self, path: str, scope):
        response = await super().get_response(path, scope)
        
        # 对静态资源设置缓存头
        if path.endswith(('.js', '.css', '.html')):
            # JS/CSS/HTML：不缓存，确保用户获取最新版本
            response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
            response.headers['Pragma'] = 'no-cache'
            response.headers['Expires'] = '0'
        elif path.endswith(('.woff', '.woff2', '.ttf', '.eot')):
            # 字体：缓存 1 年（很少变化）
            response.headers['Cache-Control'] = 'public, max-age=31536000, immutable'
        elif path.endswith(('.png', '.jpg', '.jpeg', '.gif', '.ico', '.svg', '.webp')):
            # 图片：缓存 1 周
            response.headers['Cache-Control'] = 'public, max-age=604800'
        
        return response

app.mount("/static", CachedStaticFiles(directory="static"), name="static")


# ==================== HTML 页面路由（带版本号注入）====================

def _serve_html_with_version(file_path: str) -> HTMLResponse:
    """
    读取 HTML 文件并替换版本号占位符
    
    将 HTML 中的 ?v=20260129 替换为动态版本号（服务启动时间戳）
    这样每次重启服务，所有 JS/CSS 文件都会重新加载
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        # 替换固定版本号为动态版本号
        content = content.replace('?v=20260129', f'?v={STATIC_VERSION}')
        return HTMLResponse(content=content)
    except Exception as e:
        logger.error(f"读取 HTML 文件失败: {file_path}, {e}")
        return HTMLResponse(content="Page not found", status_code=404)


@app.get("/", response_class=HTMLResponse)
async def read_root():
    return _serve_html_with_version("static/index.html")


@app.get("/index.html", response_class=HTMLResponse)
async def index_page():
    return _serve_html_with_version("static/index.html")


@app.get("/login.html", response_class=HTMLResponse)
async def login_page():
    return _serve_html_with_version("static/login.html")


@app.get("/register", response_class=HTMLResponse)
async def register_page():
    """邀请注册页面 - 重定向到 login.html（前端会自动切换到注册模式）"""
    return _serve_html_with_version("static/login.html")


@app.get("/settings.html", response_class=HTMLResponse)
async def settings_page():
    return _serve_html_with_version("static/settings.html")


@app.get("/admin.html", response_class=HTMLResponse)
async def admin_page():
    return _serve_html_with_version("static/admin.html")


@app.get("/referral-admin.html", response_class=HTMLResponse)
async def referral_admin_page():
    return FileResponse("static/referral-admin.html")


@app.get("/referral.html", response_class=HTMLResponse)
async def referral_page():
    return _serve_html_with_version("static/referral.html")


@app.get("/leaderboard.html", response_class=HTMLResponse)
async def leaderboard_page():
    return _serve_html_with_version("static/leaderboard.html")

@app.get("/exchange.html", response_class=HTMLResponse)
async def exchange_page():
    # Exchange detail page - same as index but locked to a specific exchange
    return _serve_html_with_version("static/exchange.html")

@app.get("/statistics.html", response_class=HTMLResponse)
async def statistics_page():
    """财务统计页面"""
    return _serve_html_with_version("static/statistics.html")


@app.get("/dashboard/{token}", response_class=HTMLResponse)
async def public_dashboard_page(token: str):
    """公开仪表盘页面"""
    return _serve_html_with_version("static/public.html")


# ==================== 数据模型 ====================

class Position(BaseModel):
    symbol: str
    side: str
    qty: str
    entryPrice: str
    breakEvenPrice: Optional[str] = None  # Bitget 可能不返回此字段
    unrealizedPnl: str
    marginType: Optional[str] = None  # 某些交易所可能不返回
    isolatedMargin: Optional[str] = None  # 某些交易所可能不返回
    updatedAt: str
    openTimeMs: Optional[str] = None
    stopLossPrice: Optional[str] = None
    takeProfitPrice: Optional[str] = None
    openOrderType: Optional[str] = None  # MARKET / LIMIT
    liveNetPnl: Optional[str] = None
    exchange: Optional[str] = None  # 交易所标识: binance, okx, bitget, hyperliquid


class ClosedTrade(BaseModel):
    cycleId: str
    symbol: str
    side: str
    openTimeMs: str
    closeTimeMs: str
    durationMs: str
    avgOpenPrice: str
    avgClosePrice: str
    openQty: str
    closeQty: str
    feeTotal: str
    fundingTotal: str
    realizedPnlEst: str
    netPnl: str
    # ✅ 峰值收益（过程维护）
    peakPnl: str = "0"
    # ✅ B2：峰值 → 平仓回撤（一次性在 close_cycle 计算）
    drawdownToClose: str = "0"
    # （可选：如果你已经彻底不用 B1，可以直接删掉）
    maxDrawdown: str = "0"
    maxAbsQty: str
    closeTradeCount: str = "0"
    openOrderType: Optional[str] = None  # MARKET / LIMIT
    exchange: Optional[str] = None  # 交易所标识: binance, okx, bitget, hyperliquid

class Statistics(BaseModel):
    totalPnl: float
    winRate: float
    totalTrades: int
    avgDuration: int
    longWins: int
    shortWins: int
    totalUnrealizedPnl: float
    activePositions: int

    totalFee: float
    totalFunding: float
    maxDrawdown: float
    maxDrawdownPct: float

    # ✅ 新增：初始资金（来自交易所快照）
    initialEquity: Optional[float] = None

    # ✅ 新增：胜 / 负 / 平
    winCount: int = 0
    lossCount: int = 0
    breakevenCount: int = 0

    # ✅ 新增：用于 PF / Expectancy
    grossProfit: float = 0.0   # 所有盈利单净收益之和
    grossLoss: float = 0.0     # 所有亏损单净收益绝对值之和
    longLosses: int = 0
    shortLosses: int = 0

# ==================== Redis 数据获取 ====================

def get_active_positions(uid: str, exchange: str = None) -> List[Position]:
    """
    获取当前活跃持仓（带 exchange 字段）
    
    Args:
        uid: 用户 ID
        exchange: 交易所名称（可选）。如果指定，只返回该交易所的持仓
    """
    try:
        from core.pf_compatibility import pf_compat

        # 全局模式下，active_fields 会带交易所前缀 (如 "binance:ETHUSDT")
        # 单交易所模式下，active_fields 是原始 symbol (如 "ETHUSDT")
        active_fields = pf_compat.get_pf_pos_active(uid, exchange)
        if not active_fields:
            return []

        # 全局模式下使用 add_exchange_field=True，key 会带前缀
        # 单交易所模式下不加前缀
        is_global = exchange is None
        pos_data = pf_compat.get_pf_pos(uid, exchange, add_exchange_field=is_global)
        cycle_data = pf_compat.get_pf_cycle(uid, exchange, add_exchange_prefix=is_global)
        
        positions: List[Position] = []
        for field in active_fields:
            if field in pos_data:
                pos_dict = pos_data[field].copy()
                
                # 确保必需字段有默认值
                if "unrealizedPnl" not in pos_dict:
                    pos_dict["unrealizedPnl"] = "0"
                
                # 从 cycle 数据中补充信息
                if field in cycle_data:
                    cycle = cycle_data[field]
                    # 兼容新旧字段名
                    if not pos_dict.get("stopLossPrice"):
                        pos_dict["stopLossPrice"] = cycle.get("stopLossPrice") or cycle.get("stopLoss")
                    if not pos_dict.get("takeProfitPrice"):
                        pos_dict["takeProfitPrice"] = cycle.get("takeProfitPrice") or cycle.get("takeProfit")
                    # 补充 openOrderType
                    if not pos_dict.get("openOrderType"):
                        pos_dict["openOrderType"] = cycle.get("openOrderType")
                
                positions.append(Position(**pos_dict))
        return positions
    except Exception as e:
        logger.error(f"获取持仓数据错误: {e}")
        return []


def get_closed_trades(uid: str, limit: int = 50, offset: int = 0, exchange: str = None) -> List[ClosedTrade]:
    """
    获取已完成交易（分页，按时间倒序，带 exchange 字段）
    
    Args:
        uid: 用户 ID
        limit: 每页数量
        offset: 偏移量
        exchange: 交易所名称（可选）。如果指定，只返回该交易所的交易
    """
    try:
        from core.pf_compatibility import pf_compat

        if limit <= 0:
            return []

        # 使用 add_exchange_field=True 在合并时添加 exchange 字段
        closed_trades = pf_compat.get_pf_closed_h(uid, exchange, add_exchange_field=True)
        if not closed_trades:
            return []

        # 按关闭时间排序（最新的在前面）
        sorted_trades = sorted(closed_trades.items(),
                              key=lambda x: int(x[1].get('closeTimeMs', '0') if isinstance(x[1], dict) else '0'),
                              reverse=True)
        paginated_trades = sorted_trades[offset:offset + limit]

        trades: List[ClosedTrade] = []
        for trade_id, trade_data in paginated_trades:
            try:
                trades.append(ClosedTrade(**trade_data))
            except Exception as e:
                # 跳过无效的交易记录，但记录错误
                logger.warning(f"跳过无效交易记录 {trade_id}: {e}")
                continue

        return trades
    except Exception as e:
        logger.error(f"获取已完成交易错误: {e}")
        return []


def get_closed_total(uid: str, exchange: str = None) -> int:
    """
    已完成交易总数（用于分页展示/禁用下一页）
    
    Args:
        uid: 用户 ID
        exchange: 交易所名称（可选）。如果指定，只返回该交易所的交易总数
    """
    try:
        from core.pf_compatibility import pf_compat
        closed_trades = pf_compat.get_pf_closed_h(uid, exchange)
        return len(closed_trades) if closed_trades else 0
    except Exception as e:
        logger.error(f"获取交易总数错误: {e}")
        return 0

def get_account(uid: str, exchange: str = None) -> Optional[Dict]:
    """
    当前账户余额/权益（实时）
    
    Args:
        uid: 用户 ID
        exchange: 交易所名称（可选）。如果指定，只返回该交易所的账户数据
    """
    try:
        from core.pf_compatibility import pf_compat
        return pf_compat.get_pf_account(uid, exchange)
    except Exception as e:
        logger.error(f"读取 account 错误: {e}")
        return None

# ✅ 新增：读取“初始资金快照”（由 trade_cycle_store_um_onekey.py SETNX 写入）
def get_initial_equity(uid: str, exchange: str = None) -> Optional[float]:
    """
    Redis key: user:{uid} (通过兼容层访问)
    value 兼容两种格式：
      1) 纯数字字符串: "4855.37"
      2) JSON: {"walletBalance":"4855.37"} 或 {"initialEquity":"4855.37"}
    
    Args:
        uid: 用户 ID
        exchange: 交易所名称（可选）。如果指定，只返回该交易所的初始资金
    """
    try:
        from core.pf_compatibility import pf_compat
        equity_data = pf_compat.get_pf_equity_init(uid, exchange)
        if not equity_data:
            return None

        # 处理不同的数据格式
        if isinstance(equity_data, dict):
            # JSON格式
            return float(equity_data.get("walletBalance") or equity_data.get("initialEquity", 0))
        else:
            # 可能是字符串或数字
            raw = str(equity_data).strip()
            try:
                return float(raw)
            except Exception:
                pass

        # ✅ 2) 兼容 JSON 格式
        obj = json.loads(raw)
        if isinstance(obj, dict):
            wb = obj.get("walletBalance")
            if wb is None:
                wb = obj.get("initialEquity")
            if wb is None:
                return None
            return float(wb)

        return None
    except Exception as e:
        logger.error(f"读取初始资金错误: {e}")
        return None

def calc_max_drawdown_peak(equity_curve):
    """
    equity_curve: list[dict], 每个点至少包含:
      - "equity": number
      - "ts" 或 "time" (可选，用于返回区间)
    返回: dict
    """
    if not equity_curve:
        return {
            "maxDrawdownPeakPct": None,
            "maxDrawdownPeakAmount": None,
            "maxDrawdownPeakFrom": None,
            "maxDrawdownPeakTo": None,
            "maxDrawdownPeakEquity": None,
            "maxDrawdownTroughEquity": None,
        }

    peak = None
    peak_ts = None

    max_dd_pct = 0.0
    max_dd_amount = 0.0
    dd_from_ts = None
    dd_to_ts = None
    dd_peak_equity = None
    dd_trough_equity = None

    for p in equity_curve:
        e = p.get("equity")
        if e is None:
            continue
        try:
            e = float(e)
        except Exception:
            continue

        ts = p.get("ts") or p.get("time")

        # 初始化 / 更新峰值
        if peak is None or e > peak:
            peak = e
            peak_ts = ts
            continue

        # 计算回撤（避免 peak=0）
        if peak > 0:
            dd_amount = peak - e
            dd_pct = dd_amount / peak

            if dd_pct > max_dd_pct:
                max_dd_pct = dd_pct
                max_dd_amount = dd_amount
                dd_from_ts = peak_ts
                dd_to_ts = ts
                dd_peak_equity = peak
                dd_trough_equity = e

    # 如果从未回撤过（一路新高），返回 0 而不是 None
    return {
        "maxDrawdownPeakPct": round(max_dd_pct * 100, 4),      # 百分比（例如 15.4321）
        "maxDrawdownPeakAmount": round(max_dd_amount, 8),      # 金额
        "maxDrawdownPeakFrom": dd_from_ts,
        "maxDrawdownPeakTo": dd_to_ts,
        "maxDrawdownPeakEquity": dd_peak_equity,
        "maxDrawdownTroughEquity": dd_trough_equity,
    }

def live_pnl_if_stale(rds, pos_obj: dict, *, stale_ms: int = 3000) -> dict:
    """
    不修改 pos_obj['unrealizedPnl']（账务口径）
    只额外返回 liveUnrealizedPnl（展示口径）
    """
    out = dict(pos_obj)
    try:
        updated_at = int(pos_obj.get("updatedAt") or "0")
    except Exception:
        updated_at = 0

    now = int(time.time() * 1000)
    if now - updated_at <= stale_ms:
        out["liveUnrealizedPnl"] = out.get("unrealizedPnl", "0")
        out["liveSource"] = "ACCOUNT_UPDATE"
        return out

    symbol = pos_obj.get("symbol")
    side = (pos_obj.get("side") or "").upper()
    qty = D(pos_obj.get("qty") or "0")
    entry = D(pos_obj.get("entryPrice") or "0")
    if not symbol or qty <= 0 or entry <= 0:
        out["liveUnrealizedPnl"] = out.get("unrealizedPnl", "0")
        out["liveSource"] = "FALLBACK_NO_DATA"
        return out

    from core.database import RedisKeys
    raw = rds.get(RedisKeys.market_prices(symbol))
    if not raw:
        out["liveUnrealizedPnl"] = out.get("unrealizedPnl", "0")
        out["liveSource"] = "FALLBACK_NO_MARK"
        return out

    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode()
    mk = json.loads(raw)
    mark = D(mk.get("markPrice") or "0")
    if mark <= 0:
        out["liveUnrealizedPnl"] = out.get("unrealizedPnl", "0")
        out["liveSource"] = "FALLBACK_BAD_MARK"
        return out

    pnl = (mark - entry) * qty if side == "LONG" else (entry - mark) * qty
    out["liveUnrealizedPnl"] = str(pnl)
    out["liveSource"] = "MARK_PRICE_EST"
    out["markPrice"] = str(mark)
    out["markTs"] = mk.get("ts")
    return out


def live_pnl_batch(rds, positions: List[dict], *, stale_ms: int = 3000) -> List[dict]:
    """
    批量计算持仓的实时盈亏（优化版本，减少 Redis 调用）
    
    Args:
        rds: Redis 客户端
        positions: 持仓列表（每个元素是 dict）
        stale_ms: 数据过期阈值（毫秒）
        
    Returns:
        处理后的持仓列表，每个持仓增加 liveUnrealizedPnl, liveSource 等字段
    """
    if not positions:
        return []
    
    now = int(time.time() * 1000)
    results = []
    
    # 第一步：找出需要获取 mark price 的 symbols
    symbols_to_fetch = set()
    for pos in positions:
        try:
            updated_at = int(pos.get("updatedAt") or "0")
        except Exception:
            updated_at = 0
        
        # 如果数据不过期，不需要获取 mark price
        if now - updated_at <= stale_ms:
            continue
        
        symbol = pos.get("symbol")
        qty = D(pos.get("qty") or "0")
        entry = D(pos.get("entryPrice") or "0")
        
        if symbol and qty > 0 and entry > 0:
            symbols_to_fetch.add(symbol)
    
    # 第二步：批量获取 mark prices（使用 Pipeline）
    mark_prices = {}
    if symbols_to_fetch:
        from core.database import RedisKeys
        pipe = rds.pipeline()
        symbols_list = list(symbols_to_fetch)
        for symbol in symbols_list:
            pipe.get(RedisKeys.market_prices(symbol))
        
        raw_results = pipe.execute()
        
        for i, symbol in enumerate(symbols_list):
            raw = raw_results[i]
            if raw:
                if isinstance(raw, (bytes, bytearray)):
                    raw = raw.decode()
                try:
                    mk = json.loads(raw)
                    mark = D(mk.get("markPrice") or "0")
                    if mark > 0:
                        mark_prices[symbol] = {
                            "mark": mark,
                            "ts": mk.get("ts")
                        }
                except Exception:
                    pass
    
    # 第三步：处理每个持仓
    for pos in positions:
        out = dict(pos)
        
        try:
            updated_at = int(pos.get("updatedAt") or "0")
        except Exception:
            updated_at = 0
        
        # 数据不过期，使用原始值
        if now - updated_at <= stale_ms:
            out["liveUnrealizedPnl"] = out.get("unrealizedPnl", "0")
            out["liveSource"] = "ACCOUNT_UPDATE"
            results.append(out)
            continue
        
        symbol = pos.get("symbol")
        side = (pos.get("side") or "").upper()
        qty = D(pos.get("qty") or "0")
        entry = D(pos.get("entryPrice") or "0")
        
        # 数据不完整
        if not symbol or qty <= 0 or entry <= 0:
            out["liveUnrealizedPnl"] = out.get("unrealizedPnl", "0")
            out["liveSource"] = "FALLBACK_NO_DATA"
            results.append(out)
            continue
        
        # 获取 mark price
        mk_data = mark_prices.get(symbol)
        if not mk_data:
            out["liveUnrealizedPnl"] = out.get("unrealizedPnl", "0")
            out["liveSource"] = "FALLBACK_NO_MARK"
            results.append(out)
            continue
        
        mark = mk_data["mark"]
        pnl = (mark - entry) * qty if side == "LONG" else (entry - mark) * qty
        out["liveUnrealizedPnl"] = str(pnl)
        out["liveSource"] = "MARK_PRICE_EST"
        out["markPrice"] = str(mark)
        out["markTs"] = mk_data.get("ts")
        results.append(out)
    
    return results

# ==================== 统计/曲线计算 ====================

def calculate_statistics(
    positions: List[Position],
    trades: List[ClosedTrade],
    initial_equity: Optional[float]
) -> Statistics:
    """计算统计数据（胜率/手续费/资金费/最大回撤% 基于初始资金）- 同步版本"""

    total_pnl = sum(float(t.netPnl) for t in trades)

    winning_trades = [t for t in trades if float(t.netPnl) > 0]
    losing_trades = [t for t in trades if float(t.netPnl) < 0]
    breakeven_trades = [t for t in trades if float(t.netPnl) == 0]
    win_rate = (len(winning_trades) / len(trades) * 100) if trades else 0.0
    gross_profit = sum(float(t.netPnl) for t in winning_trades)  # >0
    gross_loss_abs = sum(abs(float(t.netPnl)) for t in losing_trades)  # >0

    long_wins = sum(1 for t in winning_trades if t.side == "LONG")
    short_wins = sum(1 for t in winning_trades if t.side == "SHORT")
    long_losses = sum(1 for t in losing_trades if t.side == "LONG")
    short_losses = sum(1 for t in losing_trades if t.side == "SHORT")

    total_duration_ms = sum(int(float(t.durationMs)) for t in trades)
    avg_duration_min = (total_duration_ms / len(trades) / 60000) if trades else 0.0

    total_unrealized = sum(float(p.unrealizedPnl) for p in positions)

    total_fee = sum(float(getattr(t, "feeTotal", "0") or 0) for t in trades)
    total_funding = sum(float(getattr(t, "fundingTotal", "0") or 0) for t in trades)

    # ✅ 最大回撤：用“权益曲线”计算（起点=initial_equity，若不存在则退化为0起点）
    sorted_trades = sorted(trades, key=lambda x: int(x.closeTimeMs))

    start_equity = float(initial_equity) if (initial_equity is not None and initial_equity > 0) else 0.0
    equity = start_equity
    peak = start_equity
    max_dd = 0.0

    for t in sorted_trades:
        equity += float(t.netPnl)
        if equity > peak:
            peak = equity
        dd = peak - equity
        if dd > max_dd:
            max_dd = dd

    # ✅ 最大回撤百分比：以 peak 为分母（经典定义）
    # 如果 peak<=0（例如初始资金不存在 或 极端情况），则返回 0
    max_dd_pct = (max_dd / peak * 100) if peak > 0 else 0.0

    return Statistics(
        totalPnl=round(total_pnl, 2),
        winRate=round(win_rate, 1),
        totalTrades=len(trades),
        avgDuration=int(avg_duration_min),
        longWins=long_wins,
        shortWins=short_wins,
        longLosses=long_losses,
        shortLosses=short_losses,
        totalUnrealizedPnl=round(total_unrealized, 2),
        activePositions=len(positions),

        totalFee=round(total_fee, 2),
        totalFunding=round(total_funding, 2),
        maxDrawdown=round(max_dd, 2),
        maxDrawdownPct=round(max_dd_pct, 2),

        initialEquity=round(float(initial_equity), 2) if (initial_equity is not None) else None,
        winCount=len(winning_trades),
        lossCount = len(losing_trades),
        breakevenCount = len(breakeven_trades),
        grossProfit = round(gross_profit, 2),
        grossLoss = round(gross_loss_abs, 2),
    )

def build_equity_curve(
    trades: List[ClosedTrade], 
    initial_equity: Optional[float],
    max_points: int = 500
) -> List[Dict]:
    """
    构建权益曲线
    
    Args:
        trades: 已平仓交易列表
        initial_equity: 初始权益
        max_points: 最大返回点数（默认 500），超过时进行智能采样
                   设为 -1 表示不限制
    
    Returns:
        权益曲线数据点列表
    """
    sorted_trades = sorted(trades, key=lambda x: int(x.closeTimeMs))

    start_equity = float(initial_equity) if (initial_equity is not None and initial_equity > 0) else 0.0
    equity = start_equity
    curve: List[Dict] = []

    # 取一个"参考时间"
    if sorted_trades:
        t0 = int(sorted_trades[0].closeTimeMs)
    else:
        t0 = int(time.time() * 1000)

    # ✅ 永远先塞两个"基准点"（保证能画出水平线 + 轴范围）
    curve.append({"time": max(0, t0 - 1), "equity": round(equity, 4), "pnl": 0.0})
    curve.append({"time": t0, "equity": round(equity, 4), "pnl": 0.0})

    # 有成交才继续累加
    for t in sorted_trades:
        pnl = float(t.netPnl)
        equity += pnl
        curve.append({
            "time": int(t.closeTimeMs),
            "equity": round(equity, 4),
            "pnl": round(pnl, 4),
        })

    # ✅ 智能采样：如果点数超过 max_points，进行降采样
    # 保留关键点：第一个点、最后一个点、极值点（最高/最低权益）
    if max_points > 0 and len(curve) > max_points:
        curve = _downsample_equity_curve(curve, max_points)
    
    return curve


def _downsample_equity_curve(curve: List[Dict], max_points: int) -> List[Dict]:
    """
    智能降采样权益曲线，保留关键点
    
    策略：
    1. 始终保留第一个和最后一个点
    2. 保留权益最高点和最低点（极值点）
    3. 其余点均匀采样
    """
    if len(curve) <= max_points:
        return curve
    
    n = len(curve)
    
    # 找出极值点索引
    max_equity_idx = 0
    min_equity_idx = 0
    max_equity = curve[0]["equity"]
    min_equity = curve[0]["equity"]
    
    for i, point in enumerate(curve):
        if point["equity"] > max_equity:
            max_equity = point["equity"]
            max_equity_idx = i
        if point["equity"] < min_equity:
            min_equity = point["equity"]
            min_equity_idx = i
    
    # 必须保留的索引集合
    must_keep = {0, n - 1, max_equity_idx, min_equity_idx}
    
    # 计算需要额外采样的点数
    remaining_points = max_points - len(must_keep)
    
    if remaining_points <= 0:
        # 只返回必须保留的点
        result = [curve[i] for i in sorted(must_keep)]
        return result
    
    # 均匀采样其余点
    # 排除必须保留的索引，从剩余索引中均匀选取
    available_indices = [i for i in range(n) if i not in must_keep]
    
    if len(available_indices) <= remaining_points:
        # 可用点不够，全部保留
        return curve
    
    # 均匀采样
    step = len(available_indices) / remaining_points
    sampled_indices = set()
    for i in range(remaining_points):
        idx = int(i * step)
        if idx < len(available_indices):
            sampled_indices.add(available_indices[idx])
    
    # 合并所有需要保留的索引
    all_keep = must_keep | sampled_indices
    
    # 按时间顺序返回
    result = [curve[i] for i in sorted(all_keep)]
    return result


async def calculate_statistics_async(
    positions: List[Position],
    trades: List[ClosedTrade],
    initial_equity: Optional[float]
) -> Statistics:
    """
    计算统计数据（异步版本）
    
    将 CPU 密集型计算放入线程池，避免阻塞事件循环
    """
    from core.async_helpers import run_cpu_bound
    return await run_cpu_bound(calculate_statistics, positions, trades, initial_equity)


async def build_equity_curve_async(
    trades: List[ClosedTrade],
    initial_equity: Optional[float],
    max_points: int = 500
) -> List[Dict]:
    """
    构建权益曲线（异步版本）
    
    将 CPU 密集型计算放入线程池，避免阻塞事件循环
    
    Args:
        trades: 已平仓交易列表
        initial_equity: 初始权益
        max_points: 最大返回点数（默认 500）
    """
    from core.async_helpers import run_cpu_bound
    return await run_cpu_bound(build_equity_curve, trades, initial_equity, max_points)


# ==================== 推理内容 ====================
def _safe_json_load(s: str):
    try:
        return json.loads(s) if s else None
    except Exception:
        return {"_raw": s}


def _normalize_analysis_item(req_raw, resp_raw):
    """
    兼容新旧两种 Redis 格式，统一转为前端期望的格式：
    {
        "request": { ... 投喂内容 ... },
        "response": { "signals": [...], "content": "...", ... }
    }
    """
    req_obj = _safe_json_load(req_raw) if isinstance(req_raw, str) else req_raw
    resp_obj = _safe_json_load(resp_raw) if isinstance(resp_raw, str) else resp_raw

    # === 新格式（分组）===
    # req: {"timestamp": ..., "groups": [{"group": 1, "symbols": [...], "request": {...}}, ...]}
    # resp: {"timestamp": ..., "groups": [{"group": 1, "symbols": [...], "response": {...}}, ...]}
    if isinstance(resp_obj, dict) and "groups" in resp_obj:
        # 合并所有分组的 signals
        merged_signals = []
        merged_content_parts = []
        http_status = None
        finish_reason = None
        response_time_total = 0
        total_prompt_tokens = 0
        total_completion_tokens = 0
        total_total_tokens = 0
        merged_model = None

        for g in resp_obj.get("groups", []):
            group_resp = g.get("response", {})
            signals = group_resp.get("signals", [])
            merged_signals.extend(signals)

            content = group_resp.get("content", "")
            if content:
                merged_content_parts.append(content)

            if http_status is None:
                http_status = group_resp.get("http_status")
            if finish_reason is None:
                finish_reason = group_resp.get("finish_reason")
            response_time_total += group_resp.get("response_time_ms", 0)
            usage = group_resp.get("usage") or {}
            total_prompt_tokens += int(usage.get("prompt_tokens") or 0)
            total_completion_tokens += int(usage.get("completion_tokens") or 0)
            total_total_tokens += int(usage.get("total_tokens") or 0)
            # ✅ 新增：取第一个非空的 model
            if merged_model is None:
                merged_model = usage.get("model")

        # 合并所有分组的 request
        merged_request = {}
        if isinstance(req_obj, dict) and "groups" in req_obj:
            # 取第一组的 request 作为基础，合并 markets
            all_markets = {}
            for g in req_obj.get("groups", []):
                group_req = g.get("request", {})
                if not merged_request:
                    merged_request = group_req.copy()
                    merged_request["markets"] = {}
                markets = group_req.get("markets", {})
                all_markets.update(markets)
            if merged_request:
                merged_request["markets"] = all_markets
        else:
            merged_request = req_obj

        return {
            "request": merged_request,
            "response": {
                "signals": merged_signals,
                "content": "\n\n".join(merged_content_parts),
                "http_status": http_status,
                "finish_reason": finish_reason,
                "response_time_ms": response_time_total,
                "usage": {
                    "model": merged_model,
                    "prompt_tokens": total_prompt_tokens,
                    "completion_tokens": total_completion_tokens,
                    "total_tokens": total_total_tokens
                },
                "timestamp": resp_obj.get("timestamp"),  # ✅ 前端期望 timestamp 不是 ts
                "ts": resp_obj.get("timestamp"),
                "_grouped": True,
                "_group_count": len(resp_obj.get("groups", []))
            }
        }

    # === 旧格式（单次投喂）===
    # req: {"timestamp": ..., "request": "..."}
    # resp: {"signals": [...], "content": "...", ...}
    # 处理旧数据没有 content 的情况
    if isinstance(resp_obj, dict) and "content" not in resp_obj:
        # 如果没有 content，尝试从其他字段构造
        signals = resp_obj.get("signals", [])
        if signals:
            resp_obj["content"] = f"Generated {len(signals)} trading signals"
        else:
            resp_obj["content"] = "No content available (legacy data)"

    return {
        "request": req_obj,
        "response": resp_obj
    }


def _normalize_history_items_sync(history_items: list) -> list:
    """
    CPU密集型操作：批量转换历史记录格式
    在线程池中执行，避免阻塞事件循环
    """
    items = []
    for history_item in history_items:
        request_data = history_item.get("request", {})
        response_data = history_item.get("response", {})
        item = _normalize_analysis_item(request_data, response_data)
        items.append(item)
    return items


@app.get("/api/analysis-history/")
async def api_analysis_history_global(
    user: Dict = Depends(get_current_user),
    limit: int = Query(50, ge=1, le=100),  # 限制最大100条，避免大量JSON传输
    offset: int = Query(0, ge=0),
):
    import time
    import asyncio
    t0 = time.perf_counter()
    
    uid = user["uid"]

    # ✅ 使用异步版本读取 AI 历史记录 (JSON解析和排序已在线程池中)
    from core.async_redis import get_ai_history_paginated_async
    history_items, total = await get_ai_history_paginated_async(uid, offset, limit)
    
    t1 = time.perf_counter()
    redis_ms = (t1 - t0) * 1000

    if not history_items:
        return {"items": [], "total": total, "limit": limit, "offset": offset}

    # ✅ 数据格式转换放到线程池，避免阻塞事件循环
    items = await asyncio.to_thread(_normalize_history_items_sync, history_items)

    t2 = time.perf_counter()
    normalize_ms = (t2 - t1) * 1000
    total_ms = (t2 - t0) * 1000
    
    if total_ms > 200:
        logger.warning(f"[ANALYSIS-HISTORY] redis={redis_ms:.0f}ms, normalize={normalize_ms:.0f}ms, total={total_ms:.0f}ms, items={len(items)}, total_records={total}")

    # 使用 FastJSONResponse 加速大 JSON 序列化（比标准 json 快 10 倍）
    return FastJSONResponse({"items": items, "total": total, "limit": limit, "offset": offset})


@app.get("/api/analysis-history-v2/")
async def api_analysis_history_summary(
    user: Dict = Depends(get_current_user),
    limit: int = Query(50, ge=1, le=100),
    offset: int = Query(0, ge=0),
):
    """
    获取 AI 决策历史摘要列表（V2 优化版）
    
    相比 /api/analysis-history/：
    - 不返回完整的 request/response JSON
    - 只返回摘要字段，数据量减少 90%+
    - 适合列表展示，详情通过 /api/analysis-history-v2/{id} 按需加载
    """
    import time
    t0 = time.perf_counter()
    
    uid = user["uid"]
    
    from core.async_redis import get_ai_history_summary_paginated_async
    summaries, total = await get_ai_history_summary_paginated_async(uid, offset, limit)
    
    t1 = time.perf_counter()
    total_ms = (t1 - t0) * 1000
    
    if total_ms > 100:
        logger.info(f"[ANALYSIS-HISTORY-V2] query={total_ms:.0f}ms, items={len(summaries)}, total={total}")
    
    return FastJSONResponse({"items": summaries, "total": total, "limit": limit, "offset": offset})


@app.get("/api/analysis-history-v2/{decision_id}")
async def api_analysis_history_detail(
    decision_id: int,
    user: Dict = Depends(get_current_user),
):
    """
    获取单条 AI 决策详情（含完整 request/response）
    
    用于列表点击后按需加载详情
    """
    import time
    t0 = time.perf_counter()
    
    uid = user["uid"]
    
    from core.async_redis import get_ai_decision_detail_async
    detail = await get_ai_decision_detail_async(uid, decision_id)
    
    if not detail:
        raise HTTPException(status_code=404, detail="决策记录不存在")
    
    # 格式化为前端期望的格式
    request_data = detail.get("request", {})
    response_data = detail.get("response", {})
    
    item = _normalize_analysis_item(request_data, response_data)
    item["id"] = decision_id
    item["timestamp"] = detail.get("timestamp")
    
    t1 = time.perf_counter()
    total_ms = (t1 - t0) * 1000
    
    if total_ms > 100:
        logger.info(f"[ANALYSIS-HISTORY-V2-DETAIL] query={total_ms:.0f}ms, id={decision_id}")
    
    return FastJSONResponse(item)

@app.get("/logic", response_class=HTMLResponse)
async def logic_page():
    return FileResponse("static/index.html")
# ==================== API 端点 ====================

@app.get("/api/positions/{uid}")
async def api_positions(uid: str, user: Dict = Depends(get_current_user)):
    # 安全检查：只能访问自己的数据
    if user["uid"] != uid:
        raise HTTPException(status_code=403, detail="无权访问其他用户的数据")
    
    # ✅ 使用异步版本
    positions_raw = await get_active_positions_async(uid)
    positions_dict_list = [p.dict() for p in positions_raw]
    positions_with_live = await live_pnl_batch_async(positions_dict_list, stale_ms=3000)
    
    positions = []
    for live in positions_with_live:
        live["officialUnrealizedPnl"] = live.get("unrealizedPnl", "0")
        live["unrealizedPnl"] = live.get("liveUnrealizedPnl", live.get("unrealizedPnl", "0"))
        positions.append(live)
    return {"positions": positions, "count": len(positions)}

@app.get("/api/closed-trades/{uid}")
async def api_closed(uid: str, user: Dict = Depends(get_current_user), limit: int = 50, offset: int = 0):
    # 安全检查：只能访问自己的数据
    if user["uid"] != uid:
        raise HTTPException(status_code=403, detail="无权访问其他用户的数据")
    
    # ✅ 使用异步版本
    all_closed_trades = await get_closed_trades_async(uid, add_exchange_field=True)
    total = len(all_closed_trades) if all_closed_trades else 0
    
    # 排序并分页
    sorted_trades = sorted(
        all_closed_trades.items(),
        key=lambda x: int(x[1].get('closeTimeMs', '0') if isinstance(x[1], dict) else '0'),
        reverse=True
    ) if all_closed_trades else []
    
    paginated_trades = sorted_trades[offset:offset + limit]
    trades = []
    for trade_id, trade_data in paginated_trades:
        try:
            trades.append(ClosedTrade(**trade_data))
        except Exception:
            continue
    
    return {
        "trades": trades,
        "count": len(trades),
        "total": total,
        "limit": limit,
        "offset": offset
    }

@app.get("/api/statistics/{uid}")
async def api_statistics(uid: str, user: Dict = Depends(get_current_user), limit: int = 100):
    # 安全检查：只能访问自己的数据
    if user["uid"] != uid:
        raise HTTPException(status_code=403, detail="无权访问其他用户的数据")
    
    # ✅ 使用异步版本并行获取数据
    positions, initial_equity, all_closed_trades = await asyncio.gather(
        get_active_positions_async(uid),
        get_initial_equity_async(uid),
        get_closed_trades_async(uid, add_exchange_field=True),
    )

    # 排序并取前 limit 条
    sorted_trades = sorted(
        all_closed_trades.items(),
        key=lambda x: int(x[1].get('closeTimeMs', '0') if isinstance(x[1], dict) else '0'),
        reverse=True
    ) if all_closed_trades else []
    
    trades_for_stats = []
    for trade_id, trade_data in sorted_trades[:limit]:
        try:
            trades_for_stats.append(ClosedTrade(**trade_data))
        except Exception:
            continue

    # 使用异步版本，避免阻塞事件循环
    stats = await calculate_statistics_async(positions, trades_for_stats, initial_equity)
    return stats


@app.get("/api/dashboard")
async def api_dashboard(
    user: Dict = Depends(get_current_user),
    limit: int = 100,          # 最近 N 笔；传 -1 代表全部
    closed_limit: int = 50,
    offset: int = 0,
    exchange: str = None       # 交易所筛选（可选），如 binance, okx, bitget
):
    """
    获取仪表盘数据（异步版本，不阻塞事件循环）
    
    Args:
        limit: 统计计算使用的交易数量（-1 表示全部）
        closed_limit: 交易列表每页数量
        offset: 交易列表偏移量
        exchange: 交易所筛选（可选）。如果指定，只返回该交易所的数据
    """
    uid = user["uid"]
    
    # ✅ 使用异步版本并行获取数据，不阻塞事件循环
    positions_raw, account, initial_equity, all_closed_trades = await asyncio.gather(
        get_active_positions_async(uid, exchange),
        get_account_async(uid, exchange),
        get_initial_equity_async(uid, exchange),
        get_closed_trades_async(uid, exchange, add_exchange_field=True),
    )
    
    account = account or {}
    
    # ✅ 使用异步版本批量获取 mark prices
    positions_dict_list = [p.dict() for p in positions_raw]
    positions_with_live = await live_pnl_batch_async(positions_dict_list, stale_ms=3000)
    
    positions = []
    for live in positions_with_live:
        # ✅ 保留官方口径（对账/调试用）
        live["officialUnrealizedPnl"] = live.get("unrealizedPnl", "0")

        # ✅ 兼容你现有前端：它只读 unrealizedPnl，所以这里让 unrealizedPnl 变成"展示口径"
        live["unrealizedPnl"] = live.get("liveUnrealizedPnl", live.get("unrealizedPnl", "0"))

        positions.append(live)

    closed_total = len(all_closed_trades) if all_closed_trades else 0
    
    # 排序（最新的在前面）
    sorted_trades = sorted(
        all_closed_trades.items(),
        key=lambda x: int(x[1].get('closeTimeMs', '0') if isinstance(x[1], dict) else '0'),
        reverse=True
    ) if all_closed_trades else []
    
    # ✅ 统计/曲线：最近 N 笔 or 全部
    if limit == -1:
        stats_trades_data = sorted_trades
        stats_limit_value = -1
    else:
        stats_trades_data = sorted_trades[:limit]
        stats_limit_value = limit
    
    # 转换为 ClosedTrade 对象
    trades_for_stats: List[ClosedTrade] = []
    for trade_id, trade_data in stats_trades_data:
        try:
            trades_for_stats.append(ClosedTrade(**trade_data))
        except Exception:
            continue

    # stats 仍按原逻辑（账务/历史）计算
    # 注意：这里 positions_raw 是 Position 对象列表，满足 calculate_statistics 的签名
    # 使用异步版本，避免阻塞事件循环
    stats, equity_curve = await asyncio.gather(
        calculate_statistics_async(positions_raw, trades_for_stats, initial_equity),
        build_equity_curve_async(trades_for_stats, initial_equity),
    )
    mdd_peak = calc_max_drawdown_peak(equity_curve)

    # ✅ 覆盖"未实现收益"为展示口径（让顶部卡片和表格一致）
    try:
        total_live_unreal = sum(float(x.get("unrealizedPnl") or 0) for x in positions)
    except Exception:
        total_live_unreal = 0.0
    stats.totalUnrealizedPnl = round(total_live_unreal, 2)

    # ✅ 列表分页（从已排序的数据中切片，不再重复查询）
    closed_trades_page_data = sorted_trades[offset:offset + closed_limit]
    closed_trades_page: List[ClosedTrade] = []
    for trade_id, trade_data in closed_trades_page_data:
        try:
            closed_trades_page.append(ClosedTrade(**trade_data))
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
        "closedOffset": offset,
        
        # ✅ 币种精度信息（用于前端格式化显示）
        "symbolPrecisions": symbol_precisions
    }

# ==================== 多交易所 API ====================

@app.get("/api/dashboard/exchanges")
async def api_dashboard_by_exchange(
    user: Dict = Depends(get_current_user),
    closed_limit: int = 20,
):
    """
    获取按交易所分组的仪表板数据（异步版本）
    """
    uid = user["uid"]
    
    try:
        # ✅ 使用异步版本获取所有交易所数据
        from core.async_redis import get_all_exchanges_data_async
        all_data = await get_all_exchanges_data_async(uid)
        
        exchanges_result = {}
        total_balance = 0.0
        total_equity = 0.0
        total_unrealized = 0.0
        total_positions = 0
        
        # 收集所有持仓，批量获取 mark prices
        all_positions_by_exchange = {}
        
        for exchange, data in all_data.items():
            positions = data.get("positions", {})
            if positions:
                positions_list = []
                for field, pos in positions.items():
                    pos_dict = pos.copy()
                    pos_dict["exchange"] = exchange
                    positions_list.append(pos_dict)
                all_positions_by_exchange[exchange] = positions_list
        
        # 批量处理所有交易所的持仓
        all_positions_flat = []
        exchange_indices = {}
        idx = 0
        for exchange, positions_list in all_positions_by_exchange.items():
            exchange_indices[exchange] = (idx, idx + len(positions_list))
            all_positions_flat.extend(positions_list)
            idx += len(positions_list)
        
        # ✅ 使用异步版本批量获取 live pnl
        all_positions_with_live = await live_pnl_batch_async(all_positions_flat, stale_ms=3000) if all_positions_flat else []
        
        for exchange, data in all_data.items():
            exchange_result = {}
            
            # 账户
            account = data.get("account", {})
            if account:
                exchange_result["account"] = account
                total_balance += float(account.get("walletBalance") or 0)
                total_equity += float(account.get("equity") or 0)
                total_unrealized += float(account.get("unrealized") or 0)
            
            # 持仓（从批量处理结果中获取）
            if exchange in exchange_indices:
                start_idx, end_idx = exchange_indices[exchange]
                positions_list = []
                for live in all_positions_with_live[start_idx:end_idx]:
                    live["officialUnrealizedPnl"] = live.get("unrealizedPnl", "0")
                    live["unrealizedPnl"] = live.get("liveUnrealizedPnl", live.get("unrealizedPnl", "0"))
                    positions_list.append(live)
                
                if positions_list:
                    exchange_result["positions"] = positions_list
                    total_positions += len(positions_list)
            
            # 已关闭交易
            closed_trades = data.get("closed_trades", {})
            if closed_trades:
                sorted_trades = sorted(
                    closed_trades.items(),
                    key=lambda x: int(x[1].get("closeTimeMs", "0") or "0"),
                    reverse=True
                )
                exchange_result["closedTrades"] = [t[1] for t in sorted_trades[:closed_limit]]
                exchange_result["closedTotal"] = len(closed_trades)
            
            # 初始权益
            equity_init = data.get("equity_init", {})
            if equity_init:
                exchange_result["initialEquity"] = float(equity_init.get("walletBalance") or 0)
            
            if exchange_result:
                exchanges_result[exchange] = exchange_result
        
        return {
            "exchanges": exchanges_result,
            "summary": {
                "totalBalance": round(total_balance, 2),
                "totalEquity": round(total_equity, 2),
                "totalUnrealized": round(total_unrealized, 2),
                "totalPositions": total_positions,
                "exchangeCount": len(exchanges_result),
            }
        }
    except Exception as e:
        logger.error(f"获取多交易所数据失败: {e}")
        return {"exchanges": {}, "summary": {}, "error": "获取数据失败"}


@app.get("/api/positions/exchanges")
async def api_positions_by_exchange(
    user: Dict = Depends(get_current_user),
):
    """
    获取按交易所分组的持仓数据（异步版本）
    """
    uid = user["uid"]
    
    try:
        # ✅ 使用异步版本
        from core.async_redis import get_positions_by_exchange_async
        positions_by_exchange = await get_positions_by_exchange_async(uid)
        
        # 收集所有持仓，批量获取 mark prices
        all_positions_flat = []
        exchange_indices = {}
        idx = 0
        
        for exchange, positions in positions_by_exchange.items():
            positions_list = []
            for field, pos in positions.items():
                pos_dict = pos.copy()
                pos_dict["exchange"] = exchange
                positions_list.append(pos_dict)
            exchange_indices[exchange] = (idx, idx + len(positions_list))
            all_positions_flat.extend(positions_list)
            idx += len(positions_list)
        
        # ✅ 使用异步版本批量获取 live pnl
        all_positions_with_live = await live_pnl_batch_async(all_positions_flat, stale_ms=3000) if all_positions_flat else []
        
        result = {}
        for exchange in positions_by_exchange.keys():
            if exchange in exchange_indices:
                start_idx, end_idx = exchange_indices[exchange]
                positions_list = []
                for live in all_positions_with_live[start_idx:end_idx]:
                    live["officialUnrealizedPnl"] = live.get("unrealizedPnl", "0")
                    live["unrealizedPnl"] = live.get("liveUnrealizedPnl", live.get("unrealizedPnl", "0"))
                    positions_list.append(live)
                result[exchange] = positions_list
        
        return {"exchanges": result, "count": sum(len(p) for p in result.values())}
    except Exception as e:
        logger.error(f"获取多交易所持仓失败: {e}")
        return {"exchanges": {}, "count": 0, "error": "获取持仓失败"}


@app.get("/api/closed-trades/exchanges")
async def api_closed_trades_by_exchange(
    user: Dict = Depends(get_current_user),
    limit: int = 50,
):
    """
    获取按交易所分组的已关闭交易（异步版本）
    """
    uid = user["uid"]
    
    try:
        # ✅ 使用异步版本
        from core.async_redis import get_closed_trades_by_exchange_async
        closed_by_exchange = await get_closed_trades_by_exchange_async(uid)
        
        result = {}
        for exchange, trades in closed_by_exchange.items():
            sorted_trades = sorted(
                trades.items(),
                key=lambda x: int(x[1].get("closeTimeMs", "0") or "0"),
                reverse=True
            )
            result[exchange] = [t[1] for t in sorted_trades[:limit]]
        
        return {
            "exchanges": result,
            "count": sum(len(t) for t in result.values()),
        }
    except Exception as e:
        logger.error(f"获取多交易所已关闭交易失败: {e}")
        return {"exchanges": {}, "count": 0, "error": "获取交易记录失败"}


# ==================== 财务统计 API ====================

@app.get("/api/statistics")
async def api_statistics(
    user: Dict = Depends(get_current_user),
    start_date: str = None,
    end_date: str = None,
    exchanges: str = None,  # 逗号分隔
    symbols: str = None,    # 逗号分隔
    pnl_filter: str = Query(default="all", pattern="^(all|profit|loss)$"),
    side_filter: str = Query(default="all", pattern="^(all|long|short)$"),
    page: int = Query(default=1, ge=1, le=1000),
    page_size: int = Query(default=50, ge=1, le=200)
):
    """
    高级财务统计 API
    
    支持复杂筛选和聚合统计
    
    Args:
        start_date: 开始日期 (YYYY-MM-DD)
        end_date: 结束日期 (YYYY-MM-DD)
        exchanges: 交易所列表（逗号分隔）
        symbols: 币种列表（逗号分隔）
        pnl_filter: 盈亏筛选 (all/profit/loss)
        side_filter: 方向筛选 (all/long/short)
        page: 页码
        page_size: 每页数量
    """
    uid = user["uid"]
    
    try:
        from core.closed_trades_db import get_closed_trades_db
        
        # 解析列表参数
        exchange_list = [e.strip() for e in exchanges.split(",")] if exchanges else None
        symbol_list = [s.strip() for s in symbols.split(",")] if symbols else None
        
        db = get_closed_trades_db()
        result = await asyncio.to_thread(
            db.get_advanced_statistics,
            uid=uid,
            start_date=start_date,
            end_date=end_date,
            exchanges=exchange_list,
            symbols=symbol_list,
            pnl_filter=pnl_filter,
            side_filter=side_filter,
            page=page,
            page_size=page_size
        )
        
        if "error" in result:
            raise HTTPException(status_code=500, detail=result["error"])
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取统计数据失败: {e}")
        raise HTTPException(status_code=500, detail=safe_error_detail(e, "获取统计数据"))


if __name__ == "__main__":
    filename = os.path.basename(__file__).replace(".py", "")
    uvicorn.run(
        f"{filename}:app",
        host=WEB_HOST,
        port=WEB_PORT,
        reload=True
    )

@app.get("/api/debug/redis-data")
async def api_debug_redis_data(
    user: Dict = Depends(get_current_user),
):
    """
    调试 API：检查 Redis 中各交易所的数据情况（异步版本）
    
    用于排查数据不显示的问题
    
    P2 Fix: 添加管理员权限检查
    """
    # P2 Fix: 只允许管理员访问调试端点
    from core.config import ADMIN_USERS
    username = user.get("username", "")
    if username not in ADMIN_USERS:
        raise HTTPException(status_code=403, detail="需要管理员权限")
    
    uid = user["uid"]
    
    try:
        from core.async_redis import AsyncRedisDataManager
        from core.database import RedisKeys
        
        result = {
            "uid": uid,
            "exchanges": {},
            "raw_user_data": None,
        }
        
        # ✅ 使用异步版本检查 user:{uid} 超级键的原始数据
        raw_data = await AsyncRedisDataManager.get_user_all(uid)
        if raw_data:
            # 只返回键列表和数据类型，不返回完整数据
            result["raw_user_data"] = {
                "fields": list(raw_data.keys()),
                "field_types": {k: type(v).__name__ for k, v in raw_data.items()},
            }
        
        # ✅ 批量获取所有交易所的数据
        exchanges = ["binance", "okx", "bitget", "hyperliquid"]
        fields = []
        field_map = {}
        
        for exchange in exchanges:
            account_field = RedisKeys.exchange_account(exchange)
            positions_field = RedisKeys.exchange_positions(exchange)
            equity_init_field = RedisKeys.exchange_equity_init(exchange)
            
            fields.extend([account_field, positions_field, equity_init_field])
            field_map[account_field] = (exchange, "account")
            field_map[positions_field] = (exchange, "positions")
            field_map[equity_init_field] = (exchange, "equity_init")
        
        # 批量读取
        batch_data = await AsyncRedisDataManager.get_user_fields_batch(uid, fields)
        
        # 组织结果
        for exchange in exchanges:
            exchange_data = {}
            
            account_field = RedisKeys.exchange_account(exchange)
            account = batch_data.get(account_field)
            exchange_data["account"] = {
                "field": account_field,
                "has_data": account is not None,
                "data_preview": str(account)[:200] if account else None,
            }
            
            positions_field = RedisKeys.exchange_positions(exchange)
            positions = batch_data.get(positions_field)
            exchange_data["positions"] = {
                "field": positions_field,
                "has_data": positions is not None,
                "count": len(positions) if isinstance(positions, dict) else 0,
            }
            
            equity_init_field = RedisKeys.exchange_equity_init(exchange)
            equity_init = batch_data.get(equity_init_field)
            exchange_data["equity_init"] = {
                "field": equity_init_field,
                "has_data": equity_init is not None,
            }
            
            result["exchanges"][exchange] = exchange_data
        
        return result
    except Exception as e:
        logger.error(f"调试数据获取失败: {e}")
        logger.exception("调试数据获取失败")  # 只在服务器日志中打印，不返回给客户端
        return {"error": "内部错误，请查看服务器日志"}


def run_api_server():
    uvicorn.run(
        app,
        host=WEB_HOST,
        port=WEB_PORT,
        reload=False,
        access_log=False,
        log_level="warning"
    )
