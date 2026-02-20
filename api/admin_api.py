# admin_api.py
"""
管理员 API

提供系统管理、用户管理、系统设置等功能
"""
import logging
from typing import Dict, List, Optional
from fastapi import APIRouter, HTTPException, Depends, Query
from pydantic import BaseModel

from api.user_api import get_current_user
from api.error_utils import safe_error_detail
from core.user_db import config_loader
from core.user_context import context_manager
from core.config import ADMIN_USERS

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/admin", tags=["管理员"])


# ============================================================
# 权限验证
# ============================================================

async def get_admin_user(user: Dict = Depends(get_current_user)) -> Dict:
    """验证管理员权限"""
    username = user.get("username", "")
    if username not in ADMIN_USERS:
        raise HTTPException(status_code=403, detail="需要管理员权限")
    return user


# ============================================================
# 数据模型
# ============================================================

class SettingsUpdateRequest(BaseModel):
    registration_enabled: Optional[bool] = None
    require_invite_code: Optional[bool] = None


class UserStatusUpdateRequest(BaseModel):
    status: str  # active / suspended / banned


class UserTierUpdateRequest(BaseModel):
    tier: str  # free / basic / pro / vip


# ============================================================
# 系统设置 API
# ============================================================

@router.get("/verify")
async def verify_admin(user: Dict = Depends(get_admin_user)):
    """验证管理员权限（用于前端页面访问控制）"""
    return {"is_admin": True, "username": user.get("username")}


@router.get("/settings")
async def get_settings(user: Dict = Depends(get_admin_user)):
    """获取系统设置"""
    try:
        settings = config_loader.get_all_system_settings()
        # 默认值：registration_enabled=True, require_invite_code=False
        reg_value = settings.get("registration_enabled", {}).get("value")
        invite_value = settings.get("require_invite_code", {}).get("value")
        return {
            "registration_enabled": reg_value == "1" if reg_value is not None else True,
            "require_invite_code": invite_value == "1" if invite_value is not None else False,
        }
    except Exception as e:
        logger.error(f"获取系统设置失败: {e}")
        raise HTTPException(status_code=500, detail=safe_error_detail(e, "获取系统设置"))


@router.put("/settings")
async def update_settings(req: SettingsUpdateRequest, user: Dict = Depends(get_admin_user)):
    """更新系统设置"""
    try:
        if req.registration_enabled is not None:
            value = "1" if req.registration_enabled else "0"
            config_loader.set_system_setting("registration_enabled", value)
            logger.info(f"管理员 {user.get('username')} 更新注册设置: {req.registration_enabled}")
        
        if req.require_invite_code is not None:
            value = "1" if req.require_invite_code else "0"
            config_loader.set_system_setting("require_invite_code", value)
            logger.info(f"管理员 {user.get('username')} 更新邀请码设置: {req.require_invite_code}")
        
        return {"message": "设置已更新"}
    except Exception as e:
        logger.error(f"更新系统设置失败: {e}")
        raise HTTPException(status_code=500, detail=safe_error_detail(e, "更新系统设置"))


# ============================================================
# 系统统计 API
# ============================================================

@router.get("/stats")
async def get_stats(user: Dict = Depends(get_admin_user)):
    """获取系统统计"""
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
                    "positions_count": len(ctx.account_snapshot.get("positions", [])) if ctx.account_snapshot else 0,
                }
                for uid, ctx in context_manager._contexts.items()
            ],
        }
    except Exception as e:
        logger.error(f"获取系统统计失败: {e}")
        raise HTTPException(status_code=500, detail=safe_error_detail(e, "获取系统统计"))


# ============================================================
# 用户管理 API
# ============================================================

def _row_to_dict(row):
    """将 SQLAlchemy Row 或 sqlite3.Row 转换为 dict"""
    if row is None:
        return None
    if hasattr(row, '_mapping'):
        return dict(row._mapping)
    if hasattr(row, 'keys'):
        return dict(row)
    return row


@router.get("/users")
async def list_users(
    user: Dict = Depends(get_admin_user),
    page: int = Query(1, ge=1, description="页码"),
    page_size: int = Query(20, ge=1, le=100, description="每页数量"),
    search: str = Query("", description="搜索关键词（UID、用户名或邮箱）"),
):
    """获取用户列表（支持分页和搜索）"""
    try:
        with config_loader._get_connection() as conn:
            # P1 Fix: 使用固定 SQL 模板，避免动态拼接
            # 搜索条件通过 CASE/COALESCE 或条件参数实现
            params = {}
            offset = (page - 1) * page_size
            
            if hasattr(conn, 'execute'):
                from sqlalchemy import text
                
                if search:
                    search_pattern = f"%{search}%"
                    params = {
                        "search": search_pattern,
                        "limit": page_size,
                        "offset": offset
                    }
                    # 带搜索条件的 SQL（固定模板）- 支持 UID、用户名、邮箱搜索
                    count_sql = text("""
                        SELECT COUNT(*) FROM users u 
                        WHERE u.uid LIKE :search OR u.username LIKE :search OR u.email LIKE :search
                    """)
                    query_sql = text("""
                        SELECT u.uid, u.username, u.email, u.status, u.tier, u.created_at,
                               c.ai_enabled, c.max_positions
                        FROM users u
                        LEFT JOIN user_trading_config c ON u.uid = c.uid
                        WHERE u.uid LIKE :search OR u.username LIKE :search OR u.email LIKE :search
                        ORDER BY u.created_at DESC
                        LIMIT :limit OFFSET :offset
                    """)
                else:
                    params = {"limit": page_size, "offset": offset}
                    # 无搜索条件的 SQL（固定模板）
                    count_sql = text("SELECT COUNT(*) FROM users u")
                    query_sql = text("""
                        SELECT u.uid, u.username, u.email, u.status, u.tier, u.created_at,
                               c.ai_enabled, c.max_positions
                        FROM users u
                        LEFT JOIN user_trading_config c ON u.uid = c.uid
                        ORDER BY u.created_at DESC
                        LIMIT :limit OFFSET :offset
                    """)
                
                total = conn.execute(count_sql, params).scalar() or 0
                results = conn.execute(query_sql, params).fetchall()
            else:
                # SQLite 原生连接
                if search:
                    search_pattern = f"%{search}%"
                    # 带搜索条件的 SQL（固定模板）- 支持 UID、用户名、邮箱搜索
                    cursor = conn.execute(
                        "SELECT COUNT(*) FROM users u WHERE u.uid LIKE ? OR u.username LIKE ? OR u.email LIKE ?",
                        (search_pattern, search_pattern, search_pattern)
                    )
                    total = cursor.fetchone()[0] or 0
                    
                    cursor = conn.execute("""
                        SELECT u.uid, u.username, u.email, u.status, u.tier, u.created_at,
                               c.ai_enabled, c.max_positions
                        FROM users u
                        LEFT JOIN user_trading_config c ON u.uid = c.uid
                        WHERE u.uid LIKE ? OR u.username LIKE ? OR u.email LIKE ?
                        ORDER BY u.created_at DESC
                        LIMIT ? OFFSET ?
                    """, (search_pattern, search_pattern, search_pattern, page_size, offset))
                else:
                    # 无搜索条件的 SQL（固定模板）
                    cursor = conn.execute("SELECT COUNT(*) FROM users u")
                    total = cursor.fetchone()[0] or 0
                    
                    cursor = conn.execute("""
                        SELECT u.uid, u.username, u.email, u.status, u.tier, u.created_at,
                               c.ai_enabled, c.max_positions
                        FROM users u
                        LEFT JOIN user_trading_config c ON u.uid = c.uid
                        ORDER BY u.created_at DESC
                        LIMIT ? OFFSET ?
                    """, (page_size, offset))
                
                results = cursor.fetchall()
            
            users = []
            for row in results:
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
            
            total_pages = (total + page_size - 1) // page_size if total > 0 else 1
            
            return {
                "users": users,
                "total": total,
                "page": page,
                "page_size": page_size,
                "total_pages": total_pages,
            }
            
    except Exception as e:
        logger.error(f"获取用户列表失败: {e}")
        raise HTTPException(status_code=500, detail=safe_error_detail(e, "获取用户列表"))


@router.put("/users/{uid}/status")
async def update_user_status(
    uid: str,
    req: UserStatusUpdateRequest,
    user: Dict = Depends(get_admin_user)
):
    """更新用户状态"""
    if req.status not in ["active", "suspended", "banned"]:
        raise HTTPException(status_code=400, detail="无效的状态值")
    
    try:
        with config_loader._get_connection() as conn:
            if hasattr(conn, 'execute'):
                from sqlalchemy import text
                conn.execute(text("""
                    UPDATE users SET status = :status WHERE uid = :uid
                """), {"status": req.status, "uid": uid})
            else:
                conn.execute("""
                    UPDATE users SET status = ? WHERE uid = ?
                """, (req.status, uid))
        
        # 如果禁用用户，停止其服务
        if req.status in ["suspended", "banned"]:
            ctx = context_manager._contexts.get(uid)
            if ctx:
                ctx.stop()
                context_manager.remove_context(uid)
            
            try:
                from core.async_multi_user_scheduler import unregister_user_from_schedulers
                unregister_user_from_schedulers(uid)
            except Exception as e:
                # P2 Fix: 添加日志，调度器注销失败不影响状态更新
                logger.warning(f"从调度器注销用户 {uid} 失败: {e}")
        
        config_loader._cache.pop(uid, None)
        
        logger.info(f"管理员 {user.get('username')} 更新用户 {uid} 状态为 {req.status}")
        return {"message": f"用户 {uid} 状态已更新为 {req.status}"}
        
    except Exception as e:
        logger.error(f"更新用户状态失败: {e}")
        raise HTTPException(status_code=500, detail=safe_error_detail(e, "更新用户状态"))


@router.put("/users/{uid}/tier")
async def update_user_tier(
    uid: str,
    req: UserTierUpdateRequest,
    user: Dict = Depends(get_admin_user)
):
    """更新用户等级"""
    if req.tier not in ["free", "basic", "pro", "vip"]:
        raise HTTPException(status_code=400, detail="无效的等级值")
    
    try:
        with config_loader._get_connection() as conn:
            if hasattr(conn, 'execute'):
                from sqlalchemy import text
                conn.execute(text("""
                    UPDATE users SET tier = :tier WHERE uid = :uid
                """), {"tier": req.tier, "uid": uid})
            else:
                conn.execute("""
                    UPDATE users SET tier = ? WHERE uid = ?
                """, (req.tier, uid))
        
        # 更新调度器中的优先级（使用统一函数）
        try:
            from core.async_multi_user_scheduler import update_user_tier_in_schedulers
            update_user_tier_in_schedulers(uid, req.tier)
        except Exception as e:
            # P2 Fix: 添加日志
            logger.warning(f"更新调度器中用户 {uid} 等级失败: {e}")
        
        config_loader._cache.pop(uid, None)
        
        logger.info(f"管理员 {user.get('username')} 更新用户 {uid} 等级为 {req.tier}")
        return {"message": f"用户 {uid} 等级已更新为 {req.tier}"}
        
    except Exception as e:
        logger.error(f"更新用户等级失败: {e}")
        raise HTTPException(status_code=500, detail=safe_error_detail(e, "更新用户等级"))


@router.get("/users/{uid}/referral")
async def get_user_referral(uid: str, user: Dict = Depends(get_admin_user)):
    """获取用户邀请详情"""
    try:
        from core.referral_db import referral_db
        
        user_info = referral_db.get_user_referral_info(uid)
        team_stats = referral_db.get_team_stats(uid)
        balance = referral_db.get_user_commission_balance(uid)
        direct_referrals = referral_db.get_direct_referrals(uid, limit=50)
        
        return {
            "user_info": user_info,
            "team_stats": team_stats,
            "balance": balance,
            "direct_referrals": direct_referrals,
        }
    except Exception as e:
        logger.error(f"获取用户邀请详情失败: {e}")
        raise HTTPException(status_code=500, detail=safe_error_detail(e, "获取用户邀请详情"))


# ============================================================
# 排行榜对账 API
# ============================================================

@router.post("/leaderboard/reconcile")
async def reconcile_leaderboard(user: Dict = Depends(get_admin_user)):
    """
    手动触发排行榜对账
    
    从已平仓交易记录重新计算所有用户的收益统计，确保排行榜数据准确。
    """
    try:
        from core.commission_service import commission_service
        
        result = commission_service.reconcile_now()
        
        if result["success"]:
            return {
                "message": f"对账完成，处理 {result['users']} 个用户",
                "users_processed": result["users"]
            }
        else:
            raise HTTPException(status_code=500, detail=result.get("error", "对账失败"))
    except Exception as e:
        logger.error(f"手动对账失败: {e}")
        raise HTTPException(status_code=500, detail=safe_error_detail(e, "手动对账"))


@router.get("/leaderboard/reconcile-status")
async def get_reconcile_status(user: Dict = Depends(get_admin_user)):
    """获取对账服务状态"""
    try:
        from core.commission_service import commission_service
        
        return commission_service.get_reconcile_status()
    except Exception as e:
        logger.error(f"获取对账状态失败: {e}")
        raise HTTPException(status_code=500, detail=safe_error_detail(e, "获取对账状态"))


@router.post("/leaderboard/reconcile/{uid}")
async def reconcile_user_leaderboard(uid: str, user: Dict = Depends(get_admin_user)):
    """对账单个用户的排行榜数据"""
    try:
        from core.referral_db import referral_db
        
        result = referral_db.reconcile_user_profit_stats(uid)
        
        return {
            "message": f"用户 {uid} 对账完成",
            "uid": uid,
            "periods": result
        }
    except Exception as e:
        logger.error(f"用户对账失败: {e}")
        raise HTTPException(status_code=500, detail=safe_error_detail(e, "用户对账"))


# ============================================================
# 测试网管理 API
# ============================================================

@router.get("/testnet/users")
async def get_testnet_users(user: Dict = Depends(get_admin_user)):
    """
    获取所有使用测试网的非管理员用户
    
    测试网功能仅管理员可用，此接口用于查看是否有非管理员误开启了测试网
    """
    try:
        testnet_users = config_loader.get_testnet_users(ADMIN_USERS)
        
        # 统计运行中的
        running_count = sum(1 for u in testnet_users if u.get("is_running"))
        
        return {
            "testnet_users": testnet_users,
            "total_count": len(testnet_users),
            "running_count": running_count,
            "message": f"共 {len(testnet_users)} 个非管理员用户配置了测试网，{running_count} 个正在运行"
        }
    except Exception as e:
        logger.error(f"获取测试网用户失败: {e}")
        raise HTTPException(status_code=500, detail=safe_error_detail(e, "获取测试网用户"))


@router.post("/testnet/disable-all")
async def disable_testnet_for_all_non_admins(user: Dict = Depends(get_admin_user)):
    """
    批量关闭所有非管理员用户的测试网配置
    
    此操作会：
    1. 将所有非管理员用户的 is_testnet 设置为 0
    2. 将他们的 is_running 设置为 0（停止交易）
    3. 返回受影响的用户列表
    
    注意：这不会停止正在运行的上下文，需要用户重新启动才会生效，
    或者服务重启后这些用户将无法再启动测试网交易。
    """
    try:
        result = config_loader.disable_testnet_for_non_admins(ADMIN_USERS)
        
        # 尝试停止正在运行的上下文
        stopped_contexts = []
        for u in result.get("affected_users", []):
            if u.get("is_running"):
                try:
                    from core.user_context import context_manager
                    ctx = context_manager._contexts.get(u["uid"])
                    if ctx:
                        exchange = u["exchange"]
                        ctx.stop_exchange(exchange)
                        stopped_contexts.append(f"{u['username']}/{exchange}")
                        logger.info(f"已停止测试网用户上下文: [{u['uid']}] {u['username']} / {exchange}")
                except Exception as e:
                    logger.warning(f"停止用户上下文失败 [{u['uid']}]: {e}")
        
        return {
            "message": f"已关闭 {result['count']} 个非管理员用户的测试网配置",
            "affected_users": result.get("affected_users", []),
            "count": result.get("count", 0),
            "stopped_contexts": stopped_contexts
        }
    except Exception as e:
        logger.error(f"批量关闭测试网配置失败: {e}")
        raise HTTPException(status_code=500, detail=safe_error_detail(e, "批量关闭测试网配置"))


# ============================================================
# 系统监控面板 API
# ============================================================

@router.get("/monitor")
async def get_monitor_data(user: Dict = Depends(get_admin_user)):
    """
    获取系统监控数据
    
    返回：
    - 系统资源使用情况（CPU、内存、线程）
    - 调度器状态和统计
    - 数据库连接池状态
    - Redis 连接状态
    - LLM 调用统计
    - 交易执行统计
    """
    import time
    import psutil
    import threading
    
    try:
        # 1. 系统资源
        process = psutil.Process()
        memory_info = process.memory_info()
        
        system_resources = {
            "cpu_percent": process.cpu_percent(interval=0.1),
            "memory_mb": round(memory_info.rss / 1024 / 1024, 1),
            "memory_percent": round(process.memory_percent(), 1),
            "threads": threading.active_count(),
            "open_files": len(process.open_files()) if hasattr(process, 'open_files') else 0,
        }
        
        # 2. 调度器状态
        from core.async_multi_user_scheduler import get_scheduler_stats
        scheduler_stats = get_scheduler_stats()
        
        # 3. 用户上下文统计
        context_stats = context_manager.get_stats()
        
        # 4. 数据库连接池状态
        db_pool_stats = {"status": "unknown"}
        try:
            from core.shared_db_engine import get_engine_stats
            db_pool_stats = get_engine_stats()
        except Exception as e:
            logger.warning(f"获取数据库连接池状态失败: {e}")
            db_pool_stats = {"status": "error"}
        
        # 5. Redis 连接状态
        redis_stats = {"status": "unknown"}
        try:
            from core.database import redis_client
            redis_info = redis_client.info("clients")
            redis_stats = {
                "status": "connected",
                "connected_clients": redis_info.get("connected_clients", 0),
            }
        except Exception as e:
            logger.warning(f"获取 Redis 连接状态失败: {e}")
            redis_stats = {"status": "error"}
        
        # 6. LLM 调用统计（从调度器获取）
        llm_stats = {
            "total_calls": scheduler_stats.get("total_runs", 0),
            "success_calls": scheduler_stats.get("success_runs", 0),
            "failed_calls": scheduler_stats.get("failed_runs", 0),
            "last_cycle_duration": scheduler_stats.get("last_cycle_duration", 0),
        }
        
        # 7. 活跃用户详情
        active_users = []
        for uid, ctx in list(context_manager._contexts.items())[:50]:  # 限制50个
            try:
                active_users.append({
                    "uid": uid[:12] + "...",
                    "is_running": ctx.is_running,
                    "error_count": ctx.error_count,
                    "last_active": ctx.last_active_at,
                    "exchanges": list(ctx.exchange_manager._contexts.keys()) if ctx.exchange_manager else [],
                })
            except Exception as e:
                # P2 Fix: 添加日志
                logger.debug(f"获取用户 {uid[:8]} 上下文信息失败: {e}")
        
        return {
            "timestamp": time.time(),
            "system": system_resources,
            "scheduler": scheduler_stats,
            "contexts": context_stats,
            "db_pool": db_pool_stats,
            "redis": redis_stats,
            "llm": llm_stats,
            "active_users": active_users,
        }
        
    except Exception as e:
        logger.error(f"获取监控数据失败: {e}")
        raise HTTPException(status_code=500, detail=safe_error_detail(e, "获取监控数据"))


@router.get("/monitor/history")
async def get_monitor_history(
    user: Dict = Depends(get_admin_user),
    minutes: int = Query(60, ge=1, le=1440, description="历史分钟数")
):
    """
    获取监控历史数据（用于图表展示）
    
    注意：此功能需要后台定期采集数据，当前返回空数据
    
    计划实现：
    - 使用 Redis TimeSeries 或 InfluxDB 存储历史数据
    - 后台任务每分钟采集一次监控指标
    - 支持自定义时间范围查询
    """
    # 功能待实现：历史数据采集和存储
    # 优先级：P3（水平扩展阶段）
    return {
        "message": "历史数据功能开发中",
        "data": []
    }


# ============================================================
# 策略模板管理 API
# ============================================================

class StrategyTemplateUpdateRequest(BaseModel):
    content: str
    description: Optional[str] = None
    is_enabled: Optional[bool] = True


class StrategyPresetCreateRequest(BaseModel):
    preset_name: str
    description: str
    categories: Dict[str, str]  # category -> content


@router.get("/strategy-templates")
async def list_strategy_presets(user: Dict = Depends(get_admin_user)):
    """
    获取所有策略预设列表
    
    返回每个预设的名称、描述、分类数量等概要信息
    """
    from sqlalchemy import text
    
    try:
        with config_loader._get_connection() as conn:
            results = conn.execute(text("""
                SELECT preset_name, MAX(description) as description, COUNT(*) as category_count,
                       MIN(created_at) as created_at, MAX(updated_at) as updated_at
                FROM strategy_templates
                GROUP BY preset_name
                ORDER BY preset_name
            """)).fetchall()
            
            presets = []
            for row in results:
                presets.append({
                    "preset_name": row.preset_name,
                    "description": row.description,
                    "category_count": row.category_count,
                    "created_at": str(row.created_at) if row.created_at else None,
                    "updated_at": str(row.updated_at) if row.updated_at else None,
                })
            
            return {"presets": presets}
            
    except Exception as e:
        logger.error(f"获取策略预设列表失败: {e}")
        raise HTTPException(status_code=500, detail=safe_error_detail(e, "获取策略预设列表"))


@router.get("/strategy-templates/{preset_name}")
async def get_strategy_preset(preset_name: str, user: Dict = Depends(get_admin_user)):
    """
    获取指定预设的所有分类模板
    """
    from sqlalchemy import text
    
    try:
        with config_loader._get_connection() as conn:
            results = conn.execute(text("""
                SELECT id, preset_name, category, content, display_order, 
                       is_enabled, description, created_at, updated_at
                FROM strategy_templates
                WHERE preset_name = :preset_name
                ORDER BY display_order, category
            """), {"preset_name": preset_name}).fetchall()
            
            if not results:
                raise HTTPException(status_code=404, detail=f"预设 {preset_name} 不存在")
            
            templates = []
            description = None
            for row in results:
                if description is None:
                    description = row.description
                templates.append({
                    "id": row.id,
                    "category": row.category,
                    "content": row.content,
                    "display_order": row.display_order,
                    "is_enabled": bool(row.is_enabled),
                    "created_at": str(row.created_at) if row.created_at else None,
                    "updated_at": str(row.updated_at) if row.updated_at else None,
                })
            
            return {
                "preset_name": preset_name,
                "description": description,
                "templates": templates
            }
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取策略预设详情失败: {e}")
        raise HTTPException(status_code=500, detail=safe_error_detail(e, "获取策略预设详情"))


@router.put("/strategy-templates/{preset_name}/{category}")
async def update_strategy_template(
    preset_name: str,
    category: str,
    req: StrategyTemplateUpdateRequest,
    user: Dict = Depends(get_admin_user)
):
    """
    更新指定预设的指定分类模板
    """
    from sqlalchemy import text
    
    valid_categories = [
        "role", "risk_rules", "entry_conditions", "exit_conditions",
        "position_sizing", "market_preferences", "adaptive_rules"
    ]
    if category not in valid_categories:
        raise HTTPException(status_code=400, detail=f"无效的分类: {category}")
    
    try:
        with config_loader._get_connection() as conn:
            # 检查是否存在
            exists = conn.execute(text("""
                SELECT id FROM strategy_templates 
                WHERE preset_name = :preset_name AND category = :category
            """), {"preset_name": preset_name, "category": category}).fetchone()
            
            if not exists:
                raise HTTPException(status_code=404, detail=f"模板 {preset_name}/{category} 不存在")
            
            # 更新
            conn.execute(text("""
                UPDATE strategy_templates 
                SET content = :content, 
                    description = COALESCE(:description, description),
                    is_enabled = :is_enabled,
                    updated_at = CURRENT_TIMESTAMP
                WHERE preset_name = :preset_name AND category = :category
            """), {
                "content": req.content,
                "description": req.description,
                "is_enabled": 1 if req.is_enabled else 0,
                "preset_name": preset_name,
                "category": category
            })
            conn.commit()
        
        # 清除策略服务缓存，让修改立即生效
        from analysis.services.strategy_service import get_strategy_service
        get_strategy_service().clear_cache()
        
        logger.info(f"管理员 {user.get('username')} 更新策略模板 {preset_name}/{category}")
        return {"message": f"模板 {preset_name}/{category} 已更新"}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"更新策略模板失败: {e}")
        raise HTTPException(status_code=500, detail=safe_error_detail(e, "更新策略模板"))


@router.post("/strategy-templates")
async def create_strategy_preset(
    req: StrategyPresetCreateRequest,
    user: Dict = Depends(get_admin_user)
):
    """
    创建新的策略预设
    """
    from sqlalchemy import text
    
    valid_categories = [
        "role", "risk_rules", "entry_conditions", "exit_conditions",
        "position_sizing", "market_preferences", "adaptive_rules"
    ]
    
    # 验证分类
    for cat in req.categories.keys():
        if cat not in valid_categories:
            raise HTTPException(status_code=400, detail=f"无效的分类: {cat}")
    
    category_order = {
        "role": 1, "risk_rules": 2, "entry_conditions": 3, "exit_conditions": 4,
        "position_sizing": 5, "market_preferences": 6, "adaptive_rules": 7
    }
    
    try:
        with config_loader._get_connection() as conn:
            # 检查是否已存在
            exists = conn.execute(text("""
                SELECT preset_name FROM strategy_templates 
                WHERE preset_name = :preset_name LIMIT 1
            """), {"preset_name": req.preset_name}).fetchone()
            
            if exists:
                raise HTTPException(status_code=400, detail=f"预设 {req.preset_name} 已存在")
            
            # 插入所有分类
            for category, content in req.categories.items():
                conn.execute(text("""
                    INSERT INTO strategy_templates 
                    (preset_name, category, content, display_order, description)
                    VALUES (:preset_name, :category, :content, :display_order, :description)
                """), {
                    "preset_name": req.preset_name,
                    "category": category,
                    "content": content,
                    "display_order": category_order.get(category, 99),
                    "description": req.description
                })
            conn.commit()
        
        # 清除策略服务缓存
        from analysis.services.strategy_service import get_strategy_service
        get_strategy_service().clear_cache()
        
        logger.info(f"管理员 {user.get('username')} 创建策略预设 {req.preset_name}")
        return {"message": f"预设 {req.preset_name} 已创建", "preset_name": req.preset_name}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"创建策略预设失败: {e}")
        raise HTTPException(status_code=500, detail=safe_error_detail(e, "创建策略预设"))


@router.delete("/strategy-templates/{preset_name}")
async def delete_strategy_preset(preset_name: str, user: Dict = Depends(get_admin_user)):
    """
    删除策略预设
    
    注意：这会删除该预设的所有分类模板
    """
    from sqlalchemy import text
    
    # 保护默认预设不被删除
    protected_presets = ["default", "conservative", "aggressive", "trend_following", "mean_reversion"]
    if preset_name in protected_presets:
        raise HTTPException(status_code=400, detail=f"系统预设 {preset_name} 不能删除")
    
    try:
        with config_loader._get_connection() as conn:
            result = conn.execute(text("""
                DELETE FROM strategy_templates WHERE preset_name = :preset_name
            """), {"preset_name": preset_name})
            deleted = result.rowcount
            conn.commit()
        
        if deleted == 0:
            raise HTTPException(status_code=404, detail=f"预设 {preset_name} 不存在")
        
        # 清除策略服务缓存
        from analysis.services.strategy_service import get_strategy_service
        get_strategy_service().clear_cache()
        
        logger.info(f"管理员 {user.get('username')} 删除策略预设 {preset_name}")
        return {"message": f"预设 {preset_name} 已删除", "deleted_count": deleted}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"删除策略预设失败: {e}")
        raise HTTPException(status_code=500, detail=safe_error_detail(e, "删除策略预设"))


@router.post("/strategy-templates/{preset_name}/sync-to-users")
async def sync_preset_to_users(
    preset_name: str,
    user: Dict = Depends(get_admin_user)
):
    """
    将系统预设模板同步到所有使用该预设的用户
    
    这会更新所有 strategy_preset = preset_name 且 strategy_overrides 为空或等于旧模板的用户策略。
    
    注意：
    - 只更新没有自定义修改的用户（strategy_overrides 为空或与系统模板完全一致）
    - 有自定义修改的用户不会被更新，需要用户手动选择是否更新
    
    返回：
    - updated_count: 更新的策略数量
    - skipped_count: 跳过的策略数量（有自定义修改）
    - user_list: 更新的用户列表
    """
    from sqlalchemy import text
    import json
    
    try:
        # 1. 获取系统预设模板内容
        with config_loader._get_connection() as conn:
            templates = conn.execute(text("""
                SELECT category, content 
                FROM strategy_templates 
                WHERE preset_name = :preset_name
                ORDER BY display_order
            """), {"preset_name": preset_name}).fetchall()
            
            if not templates:
                raise HTTPException(status_code=404, detail=f"预设 {preset_name} 不存在")
            
            # 构建新的 overrides JSON
            new_overrides = {row.category: row.content for row in templates}
            new_overrides_json = json.dumps(new_overrides, ensure_ascii=False)
            
            # 2. 查找所有使用该预设的用户策略
            strategies = conn.execute(text("""
                SELECT id, uid, strategy_id, name, strategy_overrides
                FROM user_ai_strategies
                WHERE strategy_preset = :preset_name
            """), {"preset_name": preset_name}).fetchall()
            
            updated_count = 0
            skipped_count = 0
            updated_users = []
            skipped_users = []
            
            for s in strategies:
                # 检查用户是否有自定义修改
                has_custom = False
                if s.strategy_overrides:
                    try:
                        user_overrides = json.loads(s.strategy_overrides) if isinstance(s.strategy_overrides, str) else s.strategy_overrides
                        # 比较用户的 overrides 是否与系统模板不同
                        # 如果用户有额外的分类或内容不同，则认为有自定义
                        for cat, content in user_overrides.items():
                            if cat not in new_overrides or content != new_overrides.get(cat):
                                has_custom = True
                                break
                    except json.JSONDecodeError:
                        has_custom = True
                
                if has_custom:
                    skipped_count += 1
                    skipped_users.append({
                        "uid": s.uid,
                        "strategy_id": s.strategy_id,
                        "name": s.name,
                        "reason": "有自定义修改"
                    })
                else:
                    # 更新用户策略
                    conn.execute(text("""
                        UPDATE user_ai_strategies
                        SET strategy_overrides = :overrides,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE id = :id
                    """), {"overrides": new_overrides_json, "id": s.id})
                    
                    updated_count += 1
                    updated_users.append({
                        "uid": s.uid,
                        "strategy_id": s.strategy_id,
                        "name": s.name
                    })
            
            conn.commit()
        
        # 清除策略缓存
        from core.strategy_cache import get_strategy_cache
        cache = get_strategy_cache()
        for u in updated_users:
            cache.invalidate(u["uid"], u["strategy_id"])
        
        logger.info(
            f"管理员 {user.get('username')} 同步预设 {preset_name} 到用户: "
            f"更新 {updated_count}, 跳过 {skipped_count}"
        )
        
        return {
            "message": f"预设 {preset_name} 已同步",
            "updated_count": updated_count,
            "skipped_count": skipped_count,
            "updated_users": updated_users,
            "skipped_users": skipped_users
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"同步预设到用户失败: {e}")
        raise HTTPException(status_code=500, detail=safe_error_detail(e, "同步预设到用户"))


@router.post("/strategy-templates/{preset_name}/force-sync-to-users")
async def force_sync_preset_to_users(
    preset_name: str,
    user: Dict = Depends(get_admin_user)
):
    """
    强制将系统预设模板同步到所有使用该预设的用户
    
    这会更新所有 strategy_preset = preset_name 的用户策略，
    无论用户是否有自定义修改。
    
    警告：这会覆盖用户的自定义修改！
    
    返回：
    - updated_count: 更新的策略数量
    - user_list: 更新的用户列表
    """
    from sqlalchemy import text
    import json
    
    try:
        with config_loader._get_connection() as conn:
            # 1. 获取系统预设模板内容
            templates = conn.execute(text("""
                SELECT category, content 
                FROM strategy_templates 
                WHERE preset_name = :preset_name
                ORDER BY display_order
            """), {"preset_name": preset_name}).fetchall()
            
            if not templates:
                raise HTTPException(status_code=404, detail=f"预设 {preset_name} 不存在")
            
            # 构建新的 overrides JSON
            new_overrides = {row.category: row.content for row in templates}
            new_overrides_json = json.dumps(new_overrides, ensure_ascii=False)
            
            # 2. 查找所有使用该预设的用户策略
            strategies = conn.execute(text("""
                SELECT id, uid, strategy_id, name
                FROM user_ai_strategies
                WHERE strategy_preset = :preset_name
            """), {"preset_name": preset_name}).fetchall()
            
            # 3. 批量更新
            result = conn.execute(text("""
                UPDATE user_ai_strategies
                SET strategy_overrides = :overrides,
                    updated_at = CURRENT_TIMESTAMP
                WHERE strategy_preset = :preset_name
            """), {"overrides": new_overrides_json, "preset_name": preset_name})
            
            updated_count = result.rowcount
            conn.commit()
            
            updated_users = [
                {"uid": s.uid, "strategy_id": s.strategy_id, "name": s.name}
                for s in strategies
            ]
        
        # 清除策略缓存
        from core.strategy_cache import get_strategy_cache
        cache = get_strategy_cache()
        for u in updated_users:
            cache.invalidate(u["uid"], u["strategy_id"])
        
        logger.info(
            f"管理员 {user.get('username')} 强制同步预设 {preset_name} 到 {updated_count} 个用户策略"
        )
        
        return {
            "message": f"预设 {preset_name} 已强制同步到所有用户",
            "updated_count": updated_count,
            "updated_users": updated_users
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"强制同步预设到用户失败: {e}")
        raise HTTPException(status_code=500, detail=safe_error_detail(e, "强制同步预设到用户"))


@router.get("/strategy-templates/{preset_name}/usage")
async def get_preset_usage(
    preset_name: str,
    user: Dict = Depends(get_admin_user)
):
    """
    获取预设模板的使用情况
    
    返回有多少用户/策略正在使用该预设
    """
    from sqlalchemy import text
    
    try:
        with config_loader._get_connection() as conn:
            # 统计使用该预设的策略数量
            result = conn.execute(text("""
                SELECT 
                    COUNT(*) as total_strategies,
                    COUNT(DISTINCT uid) as total_users
                FROM user_ai_strategies
                WHERE strategy_preset = :preset_name
            """), {"preset_name": preset_name}).fetchone()
            
            # 获取详细列表
            strategies = conn.execute(text("""
                SELECT uid, strategy_id, name, 
                       CASE WHEN strategy_overrides IS NOT NULL AND strategy_overrides != '' 
                            THEN 1 ELSE 0 END as has_overrides,
                       updated_at
                FROM user_ai_strategies
                WHERE strategy_preset = :preset_name
                ORDER BY updated_at DESC
                LIMIT 100
            """), {"preset_name": preset_name}).fetchall()
            
            return {
                "preset_name": preset_name,
                "total_strategies": result.total_strategies,
                "total_users": result.total_users,
                "strategies": [
                    {
                        "uid": s.uid,
                        "strategy_id": s.strategy_id,
                        "name": s.name,
                        "has_overrides": bool(s.has_overrides),
                        "updated_at": str(s.updated_at) if s.updated_at else None
                    }
                    for s in strategies
                ]
            }
            
    except Exception as e:
        logger.error(f"获取预设使用情况失败: {e}")
        raise HTTPException(status_code=500, detail=safe_error_detail(e, "获取预设使用情况"))


# ============================================================
# 数据修复 API
# ============================================================

class DataRepairRequest(BaseModel):
    uid: Optional[str] = None  # 指定用户，None 表示所有用户
    dry_run: bool = True  # 是否只预览不实际修复


# 默认持仓时长（分钟），当 openTimeMs=0 时使用
DEFAULT_DURATION_MINUTES = 30


def _update_raw_data_with_fixes(raw_data_str: str, updates: dict) -> str:
    """
    更新 raw_data JSON 中的对应字段
    
    数据库字段 -> raw_data 字段映射:
    - open_time_ms -> openTimeMs
    - entry_price -> avgOpenPrice, entryPrice
    - exit_price -> avgClosePrice
    - quantity -> maxAbsQty (只更新 maxAbsQty，不改变 closeQty/openQty)
    - realized_pnl -> netPnl
    - fee_total -> feeTotal
    - funding_total -> fundingTotal
    - duration_minutes -> durationMs (转换为毫秒)
    - pnl_pct -> pnlPct
    
    Args:
        raw_data_str: 原始 raw_data JSON 字符串
        updates: 要更新的字段 {db_field: value}
    
    Returns:
        更新后的 raw_data JSON 字符串
    """
    import json
    
    if not raw_data_str:
        return raw_data_str
    
    try:
        raw = json.loads(raw_data_str) if isinstance(raw_data_str, str) else raw_data_str
        
        # 字段映射 - 注意：所有值都需要转换为字符串，以符合 ClosedTrade 模型要求
        if "open_time_ms" in updates:
            raw["openTimeMs"] = str(updates["open_time_ms"])
        
        if "entry_price" in updates:
            raw["avgOpenPrice"] = str(updates["entry_price"])
            raw["entryPrice"] = str(updates["entry_price"])
        
        if "exit_price" in updates:
            raw["avgClosePrice"] = str(updates["exit_price"])
        
        if "quantity" in updates:
            qty_str = str(updates["quantity"])
            # 更新 maxAbsQty
            raw["maxAbsQty"] = qty_str
        
        if "realized_pnl" in updates:
            raw["netPnl"] = str(updates["realized_pnl"])
        
        if "fee_total" in updates:
            raw["feeTotal"] = str(updates["fee_total"])
        
        if "funding_total" in updates:
            raw["fundingTotal"] = str(updates["funding_total"])
        
        if "duration_minutes" in updates:
            # 转换为毫秒，并转为字符串
            raw["durationMs"] = str(int(updates["duration_minutes"]) * 60 * 1000)
        
        if "pnl_pct" in updates:
            raw["pnlPct"] = str(updates["pnl_pct"])
        
        return json.dumps(raw, ensure_ascii=False)
        
    except Exception:
        return raw_data_str


def _extract_values_from_raw_data(raw_data_str: str, close_time_ms: int = None) -> dict:
    """
    从 raw_data JSON 提取可修复的值
    
    当 openTimeMs=0 且有 close_time_ms 时，使用 close_time_ms - 30分钟 作为默认开仓时间
    
    返回字段映射: {db_field: value}
    """
    import json
    
    values = {}
    
    if not raw_data_str:
        # 即使没有 raw_data，也可以用 close_time_ms 推算 open_time_ms
        if close_time_ms and close_time_ms > 1000000000000:
            values["open_time_ms"] = close_time_ms - (DEFAULT_DURATION_MINUTES * 60 * 1000)
            values["open_time_ms_estimated"] = True  # 标记为估算值
        return values
    
    try:
        raw = json.loads(raw_data_str) if isinstance(raw_data_str, str) else raw_data_str
        
        # open_time_ms - 优先从 raw_data 获取，否则从 close_time_ms 推算
        open_time = raw.get("openTimeMs")
        if open_time:
            open_time = int(open_time)
            if open_time > 1000000000000:  # 合理的毫秒时间戳
                values["open_time_ms"] = open_time
        
        # 如果 openTimeMs 无效，使用 close_time_ms 往前推 30 分钟
        if "open_time_ms" not in values and close_time_ms and close_time_ms > 1000000000000:
            values["open_time_ms"] = close_time_ms - (DEFAULT_DURATION_MINUTES * 60 * 1000)
            values["open_time_ms_estimated"] = True  # 标记为估算值
        
        # entry_price - 从 avgOpenPrice 或 entryPrice 获取
        entry = raw.get("avgOpenPrice") or raw.get("entryPrice")
        if entry:
            val = float(entry)
            if val > 0:
                values["entry_price"] = val
        
        # exit_price - 从 avgClosePrice 获取，或从 closeQuote / closeQty 计算
        exit_p = raw.get("avgClosePrice")
        if exit_p:
            val = float(exit_p)
            if val > 0:
                values["exit_price"] = val
        
        if "exit_price" not in values:
            # 尝试从 closeQuote / closeQty 计算
            close_quote = raw.get("closeQuote")
            close_qty = raw.get("closeQty")
            if close_quote and close_qty:
                try:
                    cq = float(close_quote)
                    cqty = float(close_qty)
                    if cqty > 0:
                        exit_p = cq / cqty
                        if exit_p > 0:
                            values["exit_price"] = exit_p
                except (ValueError, ZeroDivisionError):
                    pass
        
        # quantity - 优先从 closeQty 获取，其次 openQty，最后 maxAbsQty
        qty = None
        for qty_field in ["closeQty", "openQty", "maxAbsQty"]:
            qty_val = raw.get(qty_field)
            if qty_val:
                try:
                    val = float(qty_val)
                    if val > 0:
                        qty = val
                        break
                except ValueError:
                    pass
        if qty:
            values["quantity"] = qty
        
        # realized_pnl - 优先使用 netPnl（包含手续费）
        pnl = raw.get("netPnl")
        if pnl is not None:
            try:
                values["realized_pnl"] = float(pnl)
            except ValueError:
                pass
        
        # fee_total
        fee = raw.get("feeTotal")
        if fee is not None:
            try:
                values["fee_total"] = float(fee)
            except ValueError:
                pass
        
        # funding_total
        funding = raw.get("fundingTotal")
        if funding is not None:
            try:
                values["funding_total"] = float(funding)
            except ValueError:
                pass
        
    except Exception:
        pass
    
    return values


def _check_raw_data_issues(raw_data_str: str, db_values: dict) -> tuple:
    """
    检查 raw_data 与数据库字段的不一致问题
    
    Args:
        raw_data_str: raw_data JSON 字符串
        db_values: 数据库字段值 dict
    
    Returns:
        (issues_list, needs_update, fixable_values)
    """
    import json
    
    issues = []
    needs_update = False
    fixable_values = {}
    
    if not raw_data_str:
        return issues, needs_update, fixable_values
    
    try:
        raw = json.loads(raw_data_str) if isinstance(raw_data_str, str) else raw_data_str
        
        # 1. 检查 openTimeMs
        db_open_time = db_values.get("open_time_ms")
        if db_open_time and db_open_time > 1000000000000:
            raw_open_time = raw.get("openTimeMs")
            try:
                raw_open_time_int = int(raw_open_time or 0)
            except (ValueError, TypeError):
                raw_open_time_int = 0
            
            if raw_open_time_int != db_open_time:
                issues.append(f"openTimeMs: raw={raw_open_time} vs db={db_open_time}")
                needs_update = True
                fixable_values["open_time_ms"] = db_open_time
        
        # 2. 检查 durationMs - 这是关键修复点
        db_duration_min = db_values.get("duration_minutes")
        if db_duration_min is not None and db_duration_min >= 0:
            expected_duration_ms = db_duration_min * 60 * 1000
            raw_duration_ms = raw.get("durationMs")
            try:
                raw_duration_ms_int = int(raw_duration_ms or 0)
            except (ValueError, TypeError):
                raw_duration_ms_int = 0
            
            # 检查 durationMs 是否异常（可能错误存储了 closeTimeMs）
            # 如果 raw_duration_ms 大于 1000000000000，说明存储了时间戳而不是持续时间
            if raw_duration_ms_int > 1000000000000 or abs(raw_duration_ms_int - expected_duration_ms) > 60000:
                issues.append(f"durationMs: raw={raw_duration_ms} vs expected={expected_duration_ms}")
                needs_update = True
                fixable_values["duration_minutes"] = db_duration_min
        
        # 3. 检查 avgOpenPrice
        db_entry = db_values.get("entry_price") or 0
        if db_entry > 0:
            raw_entry = raw.get("avgOpenPrice") or raw.get("entryPrice")
            try:
                raw_entry_val = float(raw_entry or 0)
            except (ValueError, TypeError):
                raw_entry_val = 0
            
            if abs(raw_entry_val - db_entry) > 0.0001 * db_entry:
                issues.append(f"avgOpenPrice: raw={raw_entry} vs db={db_entry}")
                needs_update = True
                fixable_values["entry_price"] = db_entry
        
        # 4. 检查 avgClosePrice
        db_exit = db_values.get("exit_price") or 0
        if db_exit > 0:
            raw_exit = raw.get("avgClosePrice")
            try:
                raw_exit_val = float(raw_exit or 0)
            except (ValueError, TypeError):
                raw_exit_val = 0
            
            if abs(raw_exit_val - db_exit) > 0.0001 * db_exit:
                issues.append(f"avgClosePrice: raw={raw_exit} vs db={db_exit}")
                needs_update = True
                fixable_values["exit_price"] = db_exit
        
        # 5. 检查 maxAbsQty - 只有当 maxAbsQty=0 但 closeQty>0 时才需要修复
        try:
            raw_close_qty = float(raw.get("closeQty") or 0)
            raw_open_qty = float(raw.get("openQty") or 0)
            raw_max_qty = float(raw.get("maxAbsQty") or 0)
        except (ValueError, TypeError):
            raw_close_qty = raw_open_qty = raw_max_qty = 0
        
        correct_qty = raw_close_qty or raw_open_qty
        if correct_qty > 0 and raw_max_qty == 0:
            issues.append(f"maxAbsQty=0, should be {correct_qty}")
            needs_update = True
            fixable_values["quantity"] = correct_qty
        
    except (json.JSONDecodeError, TypeError, ValueError):
        pass
    
    return issues, needs_update, fixable_values


@router.get("/data-repair/scan")
async def scan_abnormal_trades(
    user: Dict = Depends(get_admin_user),
    uid: str = Query(None, description="指定用户 UID，不填则扫描所有"),
    limit: int = Query(100, ge=1, le=1000, description="返回数量限制"),
    include_raw_data_mismatch: bool = Query(True, description="是否包含 raw_data 与数据库字段不一致的记录")
):
    """
    扫描异常的 closed_trades 数据
    
    检测以下异常：
    1. open_time_ms = 0 或 NULL 或不合理
    2. duration_minutes 异常（负数或超大值）
    3. entry_price 或 exit_price 为 0
    4. quantity 为 0
    5. raw_data 中的字段与数据库字段不一致（新增）
    
    返回异常记录列表和统计
    """
    from sqlalchemy import text
    import json
    
    try:
        from core.closed_trades_db import get_closed_trades_db
        db = get_closed_trades_db()
        
        with db._get_connection() as conn:
            # 构建查询条件 - 扩展检测范围
            conditions = []
            params = {"limit": limit}
            
            # 异常条件（数据库字段异常）
            conditions.append("(open_time_ms IS NULL OR open_time_ms = 0 OR open_time_ms < 1000000000000)")
            conditions.append("(duration_minutes IS NULL OR duration_minutes < 0 OR duration_minutes > 525600)")
            conditions.append("(entry_price IS NULL OR entry_price = 0)")
            conditions.append("(exit_price IS NULL OR exit_price = 0)")
            conditions.append("(quantity IS NULL OR quantity = 0)")
            # 添加：检查 raw_data 中 openTimeMs=0 的记录（使用 JSON 函数精确匹配）
            conditions.append("(JSON_EXTRACT(raw_data, '$.openTimeMs') = '0' OR JSON_EXTRACT(raw_data, '$.openTimeMs') = '\"0\"')")
            # 添加：检查 raw_data 中 durationMs 是时间戳的记录（大于 1000000000000）
            conditions.append("(CAST(JSON_UNQUOTE(JSON_EXTRACT(raw_data, '$.durationMs')) AS UNSIGNED) > 1000000000000)")
            
            where_clause = " OR ".join(conditions)
            
            if uid:
                where_clause = f"uid = :uid AND ({where_clause})"
                params["uid"] = uid
            
            # 统计异常数量（数据库字段异常）
            count_sql = text(f"""
                SELECT COUNT(*) FROM closed_trades WHERE {where_clause}
            """)
            total_db_abnormal = conn.execute(count_sql, params).scalar() or 0
            
            # 获取数据库字段异常的记录
            if uid:
                query_sql = text(f"""
                    SELECT id, uid, exchange, cycle_id, symbol, side, 
                           entry_price, exit_price, quantity, realized_pnl, pnl_pct,
                           fee_total, funding_total, leverage,
                           open_time_ms, close_time_ms, duration_minutes,
                           raw_data, created_at
                    FROM closed_trades 
                    WHERE uid = :uid AND ({where_clause})
                    ORDER BY id DESC
                    LIMIT :limit
                """)
            else:
                query_sql = text(f"""
                    SELECT id, uid, exchange, cycle_id, symbol, side, 
                           entry_price, exit_price, quantity, realized_pnl, pnl_pct,
                           fee_total, funding_total, leverage,
                           open_time_ms, close_time_ms, duration_minutes,
                           raw_data, created_at
                    FROM closed_trades 
                    WHERE {where_clause}
                    ORDER BY id DESC
                    LIMIT :limit
                """)
            
            all_results = conn.execute(query_sql, params).fetchall()
            
            abnormal_records = []
            raw_data_mismatch_count = 0
            
            for row in all_results:
                r = row._mapping if hasattr(row, '_mapping') else row
                
                # 分析异常原因
                issues = []           # 数据库字段问题
                unfixable = []        # 无法修复的问题
                raw_data_issues = []  # raw_data 不一致问题
                
                # 提取可修复的值（传入 close_time_ms 用于估算 open_time_ms）
                extracted = _extract_values_from_raw_data(r["raw_data"], r["close_time_ms"])
                
                # ========== 检查数据库字段异常 ==========
                
                # 1. open_time_ms
                if r["open_time_ms"] is None or r["open_time_ms"] == 0 or r["open_time_ms"] < 1000000000000:
                    if "open_time_ms" in extracted:
                        issues.append("open_time_ms=0")
                    else:
                        unfixable.append("open_time_ms=0 (无法修复)")
                
                # 2. duration_minutes
                if r["duration_minutes"] is None or r["duration_minutes"] < 0 or r["duration_minutes"] > 525600:
                    # 如果有 open_time_ms 和 close_time_ms，可以计算
                    open_time_for_calc = extracted.get("open_time_ms") or r["open_time_ms"]
                    if open_time_for_calc and open_time_for_calc > 1000000000000 and r["close_time_ms"]:
                        issues.append("duration异常")
                    else:
                        unfixable.append("duration异常 (无法修复)")
                
                # 3. entry_price
                if r["entry_price"] is None or float(r["entry_price"] or 0) == 0:
                    if "entry_price" in extracted:
                        issues.append("entry_price=0")
                    else:
                        unfixable.append("entry_price=0 (无法修复)")
                
                # 4. exit_price
                if r["exit_price"] is None or float(r["exit_price"] or 0) == 0:
                    if "exit_price" in extracted:
                        issues.append("exit_price=0")
                    else:
                        unfixable.append("exit_price=0 (无法修复)")
                
                # 5. quantity
                if r["quantity"] is None or float(r["quantity"] or 0) == 0:
                    if "quantity" in extracted:
                        issues.append("quantity=0")
                    else:
                        unfixable.append("quantity=0 (无法修复)")
                
                # ========== 检查 raw_data 与数据库字段是否一致 ==========
                
                raw_data_needs_update = False
                raw_data_fixable_values = {}
                
                if include_raw_data_mismatch and r["raw_data"]:
                    # 构建数据库值字典
                    db_values = {
                        "open_time_ms": r["open_time_ms"],
                        "duration_minutes": r["duration_minutes"],
                        "entry_price": float(r["entry_price"]) if r["entry_price"] else 0,
                        "exit_price": float(r["exit_price"]) if r["exit_price"] else 0,
                        "quantity": float(r["quantity"]) if r["quantity"] else 0,
                    }
                    
                    raw_data_issues, raw_data_needs_update, raw_data_fixable_values = _check_raw_data_issues(
                        r["raw_data"], db_values
                    )
                
                if raw_data_needs_update:
                    raw_data_mismatch_count += 1
                    issues.append("raw_data不一致")
                
                # 只有有问题的记录才加入结果
                if not issues and not unfixable and not raw_data_issues:
                    continue
                
                # ========== 计算可修复的字段 ==========
                
                correct_values = {}
                is_estimated = extracted.get("open_time_ms_estimated", False)
                
                # open_time_ms - 从 raw_data 获取或从 close_time_ms 估算
                if r["open_time_ms"] is None or r["open_time_ms"] == 0 or r["open_time_ms"] < 1000000000000:
                    if "open_time_ms" in extracted:
                        correct_values["open_time_ms"] = extracted["open_time_ms"]
                        if is_estimated:
                            correct_values["_open_time_estimated"] = True
                
                # duration_minutes - 需要 open_time_ms 和 close_time_ms
                open_time_for_calc = correct_values.get("open_time_ms") or r["open_time_ms"]
                if open_time_for_calc and open_time_for_calc > 1000000000000 and r["close_time_ms"]:
                    if r["duration_minutes"] is None or r["duration_minutes"] < 0 or r["duration_minutes"] > 525600:
                        duration = int((r["close_time_ms"] - open_time_for_calc) / 60000)
                        if duration >= 0:
                            correct_values["duration_minutes"] = duration
                
                # 其他字段
                for field in ["entry_price", "exit_price", "quantity", "realized_pnl", 
                              "fee_total", "funding_total"]:
                    if field in extracted:
                        db_val = r[field] if field in ["entry_price", "exit_price", "quantity", "realized_pnl"] else r.get(field)
                        new_val = extracted[field]
                        # 只有当数据库值为空或为0时才修复
                        if db_val is None or float(db_val or 0) == 0:
                            correct_values[field] = new_val
                
                # 计算 pnl_pct
                pnl = correct_values.get("realized_pnl") or (float(r["realized_pnl"]) if r["realized_pnl"] else 0)
                entry = correct_values.get("entry_price") or (float(r["entry_price"]) if r["entry_price"] else 0)
                qty = correct_values.get("quantity") or (float(r["quantity"]) if r["quantity"] else 0)
                if entry > 0 and qty > 0:
                    cost = entry * abs(qty)
                    pnl_pct = round(pnl / cost * 100, 4)
                    if r["pnl_pct"] is None or abs(float(r["pnl_pct"] or 0) - pnl_pct) > 0.01:
                        correct_values["pnl_pct"] = pnl_pct
                
                # 判断是否可修复：有可修复的问题（issues）或有需要更新的 raw_data
                # 如果只有 unfixable 问题，则不可修复
                can_repair = (bool(issues) or bool(correct_values) or raw_data_needs_update) and not (not issues and unfixable)
                
                abnormal_records.append({
                    "id": r["id"],
                    "uid": r["uid"],
                    "exchange": r["exchange"],
                    "cycle_id": r["cycle_id"],
                    "symbol": r["symbol"],
                    "side": r["side"],
                    "current_values": {
                        "open_time_ms": r["open_time_ms"],
                        "close_time_ms": r["close_time_ms"],
                        "duration_minutes": r["duration_minutes"],
                        "entry_price": float(r["entry_price"]) if r["entry_price"] else None,
                        "exit_price": float(r["exit_price"]) if r["exit_price"] else None,
                        "quantity": float(r["quantity"]) if r["quantity"] else None,
                        "realized_pnl": float(r["realized_pnl"]) if r["realized_pnl"] else None,
                    },
                    "correct_values": correct_values,
                    "issues": issues,
                    "unfixable": unfixable,
                    "raw_data_issues": raw_data_issues,
                    "can_repair": can_repair,
                    "raw_data_needs_update": raw_data_needs_update,
                    "created_at": str(r["created_at"]) if r["created_at"] else None,
                })
                
                # 限制返回数量
                if len(abnormal_records) >= limit:
                    break
            
            # 统计可修复数量
            repairable_count = sum(1 for r in abnormal_records if r["can_repair"])
            
            return {
                "total_db_abnormal": total_db_abnormal,
                "raw_data_mismatch_count": raw_data_mismatch_count,
                "total_abnormal": len(abnormal_records),
                "returned_count": len(abnormal_records),
                "repairable_count": repairable_count,
                "records": abnormal_records,
            }
            
    except Exception as e:
        logger.error(f"扫描异常数据失败: {e}")
        raise HTTPException(status_code=500, detail=safe_error_detail(e, "扫描异常数据"))


@router.post("/data-repair/fix")
async def repair_abnormal_trades(
    req: DataRepairRequest,
    user: Dict = Depends(get_admin_user)
):
    """
    修复异常的 closed_trades 数据
    
    从 raw_data JSON 中提取正确的值并更新数据库记录。
    
    注意：cycle_id 中的时间戳是 close_time，不能用于修复 open_time！
    如果 raw_data.openTimeMs = 0，则 open_time_ms 和 duration_minutes 无法修复。
    
    修复内容：
    1. open_time_ms - 从 raw_data.openTimeMs 提取（如果有效）
    2. duration_minutes - 重新计算 (close_time_ms - open_time_ms) / 60000
    3. entry_price, exit_price - 从 raw_data 提取
    4. quantity - 从 raw_data.closeQty/openQty 提取
    5. realized_pnl - 从 raw_data.netPnl 提取
    6. fee_total, funding_total - 从 raw_data 提取
    7. pnl_pct - 重新计算
    
    参数：
    - uid: 指定用户，不填则修复所有用户
    - dry_run: 是否只预览不实际修复（默认 True）
    """
    from sqlalchemy import text
    import json
    
    try:
        from core.closed_trades_db import get_closed_trades_db
        db = get_closed_trades_db()
        
        with db._get_connection() as conn:
            # 查询需要修复的记录
            # 包括：数据库字段异常 OR raw_data 需要同步
            if req.uid:
                # 指定用户：获取该用户所有记录来检查
                query_sql = text("""
                    SELECT id, uid, cycle_id, symbol, side,
                           open_time_ms, close_time_ms, duration_minutes,
                           entry_price, exit_price, quantity, realized_pnl, pnl_pct,
                           fee_total, funding_total, leverage,
                           raw_data
                    FROM closed_trades 
                    WHERE uid = :uid
                """)
                params = {"uid": req.uid}
            else:
                # 所有用户：获取数据库字段异常 或 raw_data 可能不一致的记录
                # 注意：raw_data 不一致需要在代码中检查，这里先获取所有可能有问题的记录
                conditions = []
                conditions.append("(open_time_ms IS NULL OR open_time_ms = 0 OR open_time_ms < 1000000000000)")
                conditions.append("(duration_minutes IS NULL OR duration_minutes < 0 OR duration_minutes > 525600)")
                conditions.append("(entry_price IS NULL OR entry_price = 0)")
                conditions.append("(exit_price IS NULL OR exit_price = 0)")
                conditions.append("(quantity IS NULL OR quantity = 0)")
                # 添加：检查 raw_data 中 openTimeMs=0 的记录（使用 JSON 函数精确匹配）
                conditions.append("(JSON_EXTRACT(raw_data, '$.openTimeMs') = '0' OR JSON_EXTRACT(raw_data, '$.openTimeMs') = '\"0\"')")
                # 添加：检查 raw_data 中 durationMs 是时间戳的记录（大于 1000000000000）
                conditions.append("(CAST(JSON_UNQUOTE(JSON_EXTRACT(raw_data, '$.durationMs')) AS UNSIGNED) > 1000000000000)")
                where_clause = " OR ".join(conditions)
                
                query_sql = text(f"""
                    SELECT id, uid, cycle_id, symbol, side,
                           open_time_ms, close_time_ms, duration_minutes,
                           entry_price, exit_price, quantity, realized_pnl, pnl_pct,
                           fee_total, funding_total, leverage,
                           raw_data
                    FROM closed_trades 
                    WHERE {where_clause}
                """)
                params = {}
            
            results = conn.execute(query_sql, params).fetchall()
            
            repaired = []
            skipped = []
            errors = []
            
            for row in results:
                r = row._mapping if hasattr(row, '_mapping') else row
                record_id = r["id"]
                
                try:
                    # 提取可修复的值（传入 close_time_ms 用于估算 open_time_ms）
                    extracted = _extract_values_from_raw_data(r["raw_data"], r["close_time_ms"])
                    
                    updates = {}
                    
                    # ========== 第一步：修复数据库字段 ==========
                    
                    # 修复 open_time_ms - 从 raw_data 获取或从 close_time_ms 估算
                    if r["open_time_ms"] is None or r["open_time_ms"] == 0 or r["open_time_ms"] < 1000000000000:
                        if "open_time_ms" in extracted:
                            updates["open_time_ms"] = extracted["open_time_ms"]
                    
                    # 修复 duration_minutes
                    open_time_for_calc = updates.get("open_time_ms") or r["open_time_ms"]
                    if open_time_for_calc and open_time_for_calc > 1000000000000 and r["close_time_ms"]:
                        if r["duration_minutes"] is None or r["duration_minutes"] < 0 or r["duration_minutes"] > 525600:
                            duration = int((r["close_time_ms"] - open_time_for_calc) / 60000)
                            if duration >= 0:
                                updates["duration_minutes"] = duration
                    
                    # 修复其他字段
                    field_mapping = {
                        "entry_price": "entry_price",
                        "exit_price": "exit_price",
                        "quantity": "quantity",
                        "realized_pnl": "realized_pnl",
                        "fee_total": "fee_total",
                        "funding_total": "funding_total",
                    }
                    
                    for db_field, extract_field in field_mapping.items():
                        db_val = r[db_field]
                        if db_val is None or float(db_val or 0) == 0:
                            if extract_field in extracted:
                                updates[db_field] = extracted[extract_field]
                    
                    # 计算 pnl_pct
                    pnl = updates.get("realized_pnl") or (float(r["realized_pnl"]) if r["realized_pnl"] else None)
                    entry = updates.get("entry_price") or (float(r["entry_price"]) if r["entry_price"] else None)
                    qty = updates.get("quantity") or (float(r["quantity"]) if r["quantity"] else None)
                    
                    if pnl is not None and entry and entry > 0 and qty and qty != 0:
                        cost = entry * abs(qty)
                        new_pnl_pct = round(pnl / cost * 100, 4)
                        current_pnl_pct = float(r["pnl_pct"]) if r["pnl_pct"] else None
                        if current_pnl_pct is None or abs(current_pnl_pct - new_pnl_pct) > 0.01:
                            updates["pnl_pct"] = new_pnl_pct
                    
                    # ========== 第二步：检查并修复 raw_data ==========
                    
                    raw_data_sync_values = {}
                    
                    if r["raw_data"]:
                        try:
                            raw = json.loads(r["raw_data"]) if isinstance(r["raw_data"], str) else r["raw_data"]
                            
                            # 检查 openTimeMs
                            db_open_time = updates.get("open_time_ms") or r["open_time_ms"]
                            if db_open_time and db_open_time > 1000000000000:
                                raw_open_time = raw.get("openTimeMs")
                                if raw_open_time is None or int(raw_open_time or 0) != db_open_time:
                                    raw_data_sync_values["open_time_ms"] = db_open_time
                            
                            # 检查 avgOpenPrice
                            db_entry = updates.get("entry_price") or (float(r["entry_price"]) if r["entry_price"] else 0)
                            if db_entry > 0:
                                raw_entry = float(raw.get("avgOpenPrice") or raw.get("entryPrice") or 0)
                                if abs(raw_entry - db_entry) > 0.0001 * db_entry:
                                    raw_data_sync_values["entry_price"] = db_entry
                            
                            # 检查 avgClosePrice
                            db_exit = updates.get("exit_price") or (float(r["exit_price"]) if r["exit_price"] else 0)
                            if db_exit > 0:
                                raw_exit = float(raw.get("avgClosePrice") or 0)
                                if abs(raw_exit - db_exit) > 0.0001 * db_exit:
                                    raw_data_sync_values["exit_price"] = db_exit
                            
                            # 检查 durationMs - 关键修复点
                            # 检测 durationMs 是否错误存储了时间戳（大于 1000000000000）
                            db_duration = updates.get("duration_minutes") if updates.get("duration_minutes") is not None else r["duration_minutes"]
                            if db_duration is not None and db_duration >= 0:
                                expected_duration_ms = db_duration * 60 * 1000
                                raw_duration_ms = raw.get("durationMs")
                                try:
                                    raw_duration_ms_int = int(raw_duration_ms or 0)
                                except (ValueError, TypeError):
                                    raw_duration_ms_int = 0
                                
                                # 如果 raw_duration_ms 大于 1000000000000，说明存储了时间戳而不是持续时间
                                # 或者与预期值差异超过 1 分钟
                                if raw_duration_ms_int > 1000000000000 or abs(raw_duration_ms_int - expected_duration_ms) > 60000:
                                    raw_data_sync_values["duration_minutes"] = db_duration
                            
                            # 检查 maxAbsQty - 只有当 maxAbsQty=0 时才需要修复
                            raw_close_qty = float(raw.get("closeQty") or 0)
                            raw_max_qty = float(raw.get("maxAbsQty") or 0)
                            if raw_close_qty > 0 and raw_max_qty == 0:
                                raw_data_sync_values["quantity"] = raw_close_qty
                                
                        except (json.JSONDecodeError, TypeError, ValueError):
                            pass
                    
                    # ========== 第三步：生成最终的 raw_data ==========
                    
                    if raw_data_sync_values or updates:
                        # 合并所有需要同步到 raw_data 的值
                        all_raw_updates = {**updates, **raw_data_sync_values}
                        updated_raw_data = _update_raw_data_with_fixes(r["raw_data"], all_raw_updates)
                        if updated_raw_data != r["raw_data"]:
                            updates["raw_data"] = updated_raw_data
                    
                    # ========== 第四步：执行更新 ==========
                    
                    if not updates:
                        skipped.append({
                            "id": record_id,
                            "uid": r["uid"],
                            "cycle_id": r["cycle_id"],
                            "reason": "无需修复"
                        })
                        continue
                    
                    # 执行更新
                    if not req.dry_run:
                        set_clauses = ", ".join([f"{k} = :{k}" for k in updates.keys()])
                        update_sql = text(f"UPDATE closed_trades SET {set_clauses} WHERE id = :id")
                        updates["id"] = record_id
                        conn.execute(update_sql, updates)
                    
                    repaired.append({
                        "id": record_id,
                        "uid": r["uid"],
                        "cycle_id": r["cycle_id"],
                        "symbol": r["symbol"],
                        "updates": {k: v for k, v in updates.items() if k != "id" and k != "raw_data"},
                        "raw_data_updated": "raw_data" in updates,
                    })
                    
                except Exception as e:
                    errors.append({
                        "id": record_id,
                        "uid": r["uid"],
                        "cycle_id": r["cycle_id"],
                        "error": str(e)
                    })
            
            if not req.dry_run:
                conn.commit()
        
        action = "预览" if req.dry_run else "修复"
        logger.info(
            f"管理员 {user.get('username')} {action}数据: "
            f"修复 {len(repaired)}, 跳过 {len(skipped)}, 错误 {len(errors)}"
        )
        
        return {
            "dry_run": req.dry_run,
            "message": f"{'预览完成' if req.dry_run else '修复完成'}",
            "repaired_count": len(repaired),
            "skipped_count": len(skipped),
            "error_count": len(errors),
            "repaired": repaired[:50],
            "skipped": skipped[:20],
            "errors": errors[:20],
        }
        
    except Exception as e:
        logger.error(f"修复异常数据失败: {e}")
        raise HTTPException(status_code=500, detail=safe_error_detail(e, "修复异常数据"))


@router.post("/data-repair/fix-single/{record_id}")
async def repair_single_trade(
    record_id: int,
    user: Dict = Depends(get_admin_user)
):
    """
    修复单条 closed_trades 记录
    
    从 raw_data 和 cycle_id 中提取正确值并更新
    """
    from sqlalchemy import text
    
    try:
        from core.closed_trades_db import get_closed_trades_db
        db = get_closed_trades_db()
        
        with db._get_connection() as conn:
            # 查询记录
            result = conn.execute(text("""
                SELECT id, uid, cycle_id, symbol, side,
                       open_time_ms, close_time_ms, duration_minutes,
                       entry_price, exit_price, quantity, realized_pnl, pnl_pct,
                       fee_total, funding_total, leverage,
                       raw_data
                FROM closed_trades WHERE id = :id
            """), {"id": record_id}).fetchone()
            
            if not result:
                raise HTTPException(status_code=404, detail=f"记录 {record_id} 不存在")
            
            r = result._mapping if hasattr(result, '_mapping') else result
            
            # 提取可修复的值（传入 close_time_ms 用于估算 open_time_ms）
            extracted = _extract_values_from_raw_data(r["raw_data"], r["close_time_ms"])
            
            if not extracted:
                raise HTTPException(status_code=400, detail="无法提取正确值")
            
            updates = {}
            
            # 修复 open_time_ms - 从 raw_data 获取或从 close_time_ms 估算
            if r["open_time_ms"] is None or r["open_time_ms"] == 0 or r["open_time_ms"] < 1000000000000:
                if "open_time_ms" in extracted:
                    updates["open_time_ms"] = extracted["open_time_ms"]
            
            # 修复 duration_minutes
            open_time_for_calc = updates.get("open_time_ms") or r["open_time_ms"]
            if open_time_for_calc and open_time_for_calc > 1000000000000 and r["close_time_ms"]:
                if r["duration_minutes"] is None or r["duration_minutes"] < 0 or r["duration_minutes"] > 525600:
                    duration = int((r["close_time_ms"] - open_time_for_calc) / 60000)
                    if duration >= 0:
                        updates["duration_minutes"] = duration
            
            # 修复其他字段
            for field in ["entry_price", "exit_price", "quantity", "realized_pnl", 
                          "fee_total", "funding_total"]:
                db_val = r[field]
                if db_val is None or float(db_val or 0) == 0:
                    if field in extracted:
                        updates[field] = extracted[field]
            
            # 计算 pnl_pct
            pnl = updates.get("realized_pnl") or (float(r["realized_pnl"]) if r["realized_pnl"] else None)
            entry = updates.get("entry_price") or (float(r["entry_price"]) if r["entry_price"] else None)
            qty = updates.get("quantity") or (float(r["quantity"]) if r["quantity"] else None)
            
            if pnl is not None and entry and entry > 0 and qty and qty != 0:
                cost = entry * abs(qty)
                new_pnl_pct = round(pnl / cost * 100, 4)
                current_pnl_pct = float(r["pnl_pct"]) if r["pnl_pct"] else None
                if current_pnl_pct is None or abs(current_pnl_pct - new_pnl_pct) > 0.01:
                    updates["pnl_pct"] = new_pnl_pct
            
            if not updates:
                return {
                    "message": "无需修复，数据已正确",
                    "record_id": record_id,
                    "updates": {}
                }
            
            # 同时更新 raw_data JSON（前端展示用）
            updated_raw_data = _update_raw_data_with_fixes(r["raw_data"], updates)
            if updated_raw_data != r["raw_data"]:
                updates["raw_data"] = updated_raw_data
            
            # 执行更新
            set_clauses = ", ".join([f"{k} = :{k}" for k in updates.keys()])
            update_sql = text(f"UPDATE closed_trades SET {set_clauses} WHERE id = :id")
            updates["id"] = record_id
            conn.execute(update_sql, updates)
            conn.commit()
            
            logger.info(f"管理员 {user.get('username')} 修复记录 {record_id}: {list(updates.keys())}")
            
            return {
                "message": "修复成功",
                "record_id": record_id,
                "uid": r["uid"],
                "cycle_id": r["cycle_id"],
                "symbol": r["symbol"],
                "updates": {k: v for k, v in updates.items() if k != "id" and k != "raw_data"},
                "raw_data_updated": "raw_data" in updates,
            }
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"修复单条记录失败: {e}")
        raise HTTPException(status_code=500, detail=safe_error_detail(e, "修复单条记录"))


@router.post("/data-repair/fix-raw-data-types")
async def fix_raw_data_types(
    req: DataRepairRequest,
    user: Dict = Depends(get_admin_user)
):
    """
    修复 raw_data 中的数据类型问题
    
    之前的修复可能将 openTimeMs、durationMs 等字段设置为整数类型，
    但 ClosedTrade 模型要求这些字段是字符串类型，导致前端无法显示。
    
    此接口扫描并修复这些数据类型问题。
    
    参数：
    - uid: 指定用户，不填则修复所有用户
    - dry_run: 是否只预览不实际修复（默认 True）
    """
    from sqlalchemy import text
    import json
    
    # 需要确保是字符串类型的字段
    STRING_FIELDS = [
        "openTimeMs", "closeTimeMs", "durationMs",
        "avgOpenPrice", "avgClosePrice", "entryPrice",
        "openQty", "closeQty", "maxAbsQty",
        "feeTotal", "fundingTotal", "realizedPnlEst", "netPnl",
        "peakPnl", "drawdownToClose", "maxDrawdown",
        "closeTradeCount", "pnlPct"
    ]
    
    try:
        from core.closed_trades_db import get_closed_trades_db
        db = get_closed_trades_db()
        
        with db._get_connection() as conn:
            # 查询记录
            if req.uid:
                query_sql = text("""
                    SELECT id, uid, cycle_id, symbol, raw_data
                    FROM closed_trades 
                    WHERE uid = :uid
                """)
                params = {"uid": req.uid}
            else:
                query_sql = text("""
                    SELECT id, uid, cycle_id, symbol, raw_data
                    FROM closed_trades
                """)
                params = {}
            
            results = conn.execute(query_sql, params).fetchall()
            
            fixed = []
            skipped = []
            errors = []
            
            for row in results:
                r = row._mapping if hasattr(row, '_mapping') else row
                record_id = r["id"]
                
                try:
                    if not r["raw_data"]:
                        skipped.append({
                            "id": record_id,
                            "uid": r["uid"],
                            "reason": "raw_data 为空"
                        })
                        continue
                    
                    raw = json.loads(r["raw_data"]) if isinstance(r["raw_data"], str) else r["raw_data"]
                    
                    # 检查并修复数据类型
                    needs_fix = False
                    fixed_fields = []
                    
                    for field in STRING_FIELDS:
                        if field in raw and raw[field] is not None:
                            if not isinstance(raw[field], str):
                                raw[field] = str(raw[field])
                                needs_fix = True
                                fixed_fields.append(field)
                    
                    if not needs_fix:
                        skipped.append({
                            "id": record_id,
                            "uid": r["uid"],
                            "reason": "数据类型正确，无需修复"
                        })
                        continue
                    
                    # 执行更新
                    if not req.dry_run:
                        new_raw_data = json.dumps(raw, ensure_ascii=False)
                        update_sql = text("UPDATE closed_trades SET raw_data = :raw_data WHERE id = :id")
                        conn.execute(update_sql, {"raw_data": new_raw_data, "id": record_id})
                    
                    fixed.append({
                        "id": record_id,
                        "uid": r["uid"],
                        "cycle_id": r["cycle_id"],
                        "symbol": r["symbol"],
                        "fixed_fields": fixed_fields,
                    })
                    
                except Exception as e:
                    errors.append({
                        "id": record_id,
                        "uid": r["uid"],
                        "error": str(e)
                    })
            
            if not req.dry_run:
                conn.commit()
        
        action = "预览" if req.dry_run else "修复"
        logger.info(
            f"管理员 {user.get('username')} {action} raw_data 数据类型: "
            f"修复 {len(fixed)}, 跳过 {len(skipped)}, 错误 {len(errors)}"
        )
        
        return {
            "dry_run": req.dry_run,
            "message": f"{'预览完成' if req.dry_run else '修复完成'}",
            "fixed_count": len(fixed),
            "skipped_count": len(skipped),
            "error_count": len(errors),
            "fixed": fixed[:100],
            "skipped": skipped[:20] if req.dry_run else [],
            "errors": errors[:20],
        }
        
    except Exception as e:
        logger.error(f"修复 raw_data 数据类型失败: {e}")
        raise HTTPException(status_code=500, detail=safe_error_detail(e, "修复 raw_data 数据类型"))


# ============================================================
# LLM 模型管理 API
# ============================================================

class LLMModelCreateRequest(BaseModel):
    """创建 LLM 模型请求"""
    provider: str
    model_id: str
    display_name: str
    description: Optional[str] = None
    temperature: float = 0.3
    top_p: float = 0.9
    max_tokens: int = 8192
    context_window: int = 128000
    supports_vision: bool = False
    supports_function_call: bool = False
    supports_streaming: bool = True
    supports_json_mode: bool = False
    input_price: float = 0
    output_price: float = 0
    is_enabled: bool = True
    is_recommended: bool = False
    display_order: int = 100
    release_date: Optional[str] = None
    deprecated_date: Optional[str] = None
    notes: Optional[str] = None


class LLMModelUpdateRequest(BaseModel):
    """更新 LLM 模型请求"""
    display_name: Optional[str] = None
    description: Optional[str] = None
    temperature: Optional[float] = None
    top_p: Optional[float] = None
    max_tokens: Optional[int] = None
    context_window: Optional[int] = None
    supports_vision: Optional[bool] = None
    supports_function_call: Optional[bool] = None
    supports_streaming: Optional[bool] = None
    supports_json_mode: Optional[bool] = None
    input_price: Optional[float] = None
    output_price: Optional[float] = None
    is_enabled: Optional[bool] = None
    is_recommended: Optional[bool] = None
    display_order: Optional[int] = None
    release_date: Optional[str] = None
    deprecated_date: Optional[str] = None
    notes: Optional[str] = None


@router.get("/llm-models")
async def list_llm_models(
    user: Dict = Depends(get_admin_user),
    provider: Optional[str] = Query(None, description="按提供商筛选"),
    include_disabled: bool = Query(False, description="是否包含禁用的模型"),
):
    """
    获取所有 LLM 模型配置
    
    返回数据库中配置的所有模型，包括参数、能力、定价等信息。
    """
    from sqlalchemy import text
    
    try:
        with config_loader._get_connection() as conn:
            # 构建查询
            params = {}
            where_clauses = []
            
            if provider:
                where_clauses.append("provider = :provider")
                params["provider"] = provider.lower()
            
            if not include_disabled:
                where_clauses.append("is_enabled = 1")
            
            where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
            
            result = conn.execute(text(f"""
                SELECT id, provider, model_id, display_name, description,
                       temperature, top_p, max_tokens, context_window,
                       supports_vision, supports_function_call, 
                       supports_streaming, supports_json_mode,
                       input_price, output_price,
                       is_enabled, is_recommended, display_order,
                       release_date, deprecated_date, notes,
                       created_at, updated_at
                FROM llm_models
                {where_sql}
                ORDER BY provider, display_order, model_id
            """), params).fetchall()
            
            models = []
            providers_set = set()
            
            for row in result:
                r = row._mapping if hasattr(row, '_mapping') else row
                providers_set.add(r["provider"])
                models.append({
                    "id": r["id"],
                    "provider": r["provider"],
                    "model_id": r["model_id"],
                    "display_name": r["display_name"],
                    "description": r["description"],
                    "temperature": float(r["temperature"]) if r["temperature"] else 0.3,
                    "top_p": float(r["top_p"]) if r["top_p"] else 0.9,
                    "max_tokens": r["max_tokens"] or 8192,
                    "context_window": r["context_window"] or 128000,
                    "supports_vision": bool(r["supports_vision"]),
                    "supports_function_call": bool(r["supports_function_call"]),
                    "supports_streaming": bool(r["supports_streaming"]),
                    "supports_json_mode": bool(r["supports_json_mode"]),
                    "input_price": float(r["input_price"]) if r["input_price"] else 0,
                    "output_price": float(r["output_price"]) if r["output_price"] else 0,
                    "is_enabled": bool(r["is_enabled"]),
                    "is_recommended": bool(r["is_recommended"]),
                    "display_order": r["display_order"] or 100,
                    "release_date": str(r["release_date"]) if r["release_date"] else None,
                    "deprecated_date": str(r["deprecated_date"]) if r["deprecated_date"] else None,
                    "notes": r["notes"],
                    "created_at": str(r["created_at"]) if r["created_at"] else None,
                    "updated_at": str(r["updated_at"]) if r["updated_at"] else None,
                })
            
            return {
                "total": len(models),
                "providers": sorted(list(providers_set)),
                "models": models,
            }
            
    except Exception as e:
        logger.error(f"获取 LLM 模型列表失败: {e}")
        raise HTTPException(status_code=500, detail=safe_error_detail(e, "获取 LLM 模型列表"))


@router.get("/llm-models/providers")
async def get_llm_providers(
    user: Dict = Depends(get_admin_user),
):
    """
    获取所有 LLM 提供商及其模型数量
    """
    from sqlalchemy import text
    
    try:
        with config_loader._get_connection() as conn:
            result = conn.execute(text("""
                SELECT provider, 
                       COUNT(*) as total_count,
                       SUM(CASE WHEN is_enabled = 1 THEN 1 ELSE 0 END) as enabled_count,
                       SUM(CASE WHEN is_recommended = 1 THEN 1 ELSE 0 END) as recommended_count
                FROM llm_models
                GROUP BY provider
                ORDER BY provider
            """)).fetchall()
            
            providers = []
            for row in result:
                r = row._mapping if hasattr(row, '_mapping') else row
                providers.append({
                    "provider": r["provider"],
                    "total_count": r["total_count"],
                    "enabled_count": r["enabled_count"],
                    "recommended_count": r["recommended_count"],
                })
            
            return {
                "providers": providers,
            }
            
    except Exception as e:
        logger.error(f"获取 LLM 提供商列表失败: {e}")
        raise HTTPException(status_code=500, detail=safe_error_detail(e, "获取 LLM 提供商列表"))


@router.post("/llm-models/refresh-cache")
async def refresh_llm_models_cache(
    user: Dict = Depends(get_admin_user),
):
    """
    刷新 LLM 模型缓存
    
    当直接修改数据库后，可以调用此接口强制刷新缓存。
    """
    try:
        from llm.llm_models_service import llm_models_service
        
        llm_models_service.invalidate_cache()
        
        logger.info(f"管理员 {user.get('username')} 刷新 LLM 模型缓存")
        
        return {
            "message": "LLM 模型缓存已刷新",
        }
        
    except Exception as e:
        logger.error(f"刷新 LLM 模型缓存失败: {e}")
        raise HTTPException(status_code=500, detail=safe_error_detail(e, "刷新 LLM 模型缓存"))


@router.get("/llm-models/{model_db_id}")
async def get_llm_model(
    model_db_id: int,
    user: Dict = Depends(get_admin_user),
):
    """
    获取单个 LLM 模型配置
    """
    try:
        from llm.llm_models_service import llm_models_service
        
        model = llm_models_service.get_model_by_id(model_db_id)
        
        if not model:
            raise HTTPException(status_code=404, detail=f"模型 ID {model_db_id} 不存在")
        
        return model
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取 LLM 模型失败: {e}")
        raise HTTPException(status_code=500, detail=safe_error_detail(e, "获取 LLM 模型"))


@router.post("/llm-models")
async def create_llm_model(
    req: LLMModelCreateRequest,
    user: Dict = Depends(get_admin_user),
):
    """
    创建新的 LLM 模型配置
    """
    # 验证 provider
    valid_providers = ["anthropic", "openai", "deepseek", "openrouter", "grok", "gemini", "custom"]
    if req.provider.lower() not in valid_providers:
        raise HTTPException(
            status_code=400, 
            detail=f"无效的提供商: {req.provider}，有效值: {', '.join(valid_providers)}"
        )
    
    # 验证 temperature 和 top_p 范围
    if not (0 <= req.temperature <= 2):
        raise HTTPException(status_code=400, detail="temperature 必须在 0-2 之间")
    if not (0 <= req.top_p <= 1):
        raise HTTPException(status_code=400, detail="top_p 必须在 0-1 之间")
    
    try:
        from llm.llm_models_service import llm_models_service
        
        model_data = req.dict()
        model_data["provider"] = req.provider.lower()  # 统一小写
        new_id = llm_models_service.add_model(model_data)
        
        if not new_id:
            raise HTTPException(status_code=500, detail="创建模型失败")
        
        logger.info(f"管理员 {user.get('username')} 创建 LLM 模型: {req.provider}/{req.model_id}")
        
        return {
            "message": f"模型 {req.provider}/{req.model_id} 已创建",
            "id": new_id,
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"创建 LLM 模型失败: {e}")
        raise HTTPException(status_code=500, detail=safe_error_detail(e, "创建 LLM 模型"))


@router.put("/llm-models/{model_db_id}")
async def update_llm_model(
    model_db_id: int,
    req: LLMModelUpdateRequest,
    user: Dict = Depends(get_admin_user),
):
    """
    更新 LLM 模型配置
    """
    # 验证 temperature 和 top_p 范围（如果提供）
    if req.temperature is not None and not (0 <= req.temperature <= 2):
        raise HTTPException(status_code=400, detail="temperature 必须在 0-2 之间")
    if req.top_p is not None and not (0 <= req.top_p <= 1):
        raise HTTPException(status_code=400, detail="top_p 必须在 0-1 之间")
    
    try:
        from llm.llm_models_service import llm_models_service
        
        # 只更新非 None 的字段
        update_data = {k: v for k, v in req.dict().items() if v is not None}
        
        if not update_data:
            raise HTTPException(status_code=400, detail="没有要更新的字段")
        
        success = llm_models_service.update_model(model_db_id, update_data)
        
        if not success:
            raise HTTPException(status_code=500, detail="更新模型失败")
        
        logger.info(f"管理员 {user.get('username')} 更新 LLM 模型 ID={model_db_id}: {list(update_data.keys())}")
        
        return {
            "message": f"模型 ID {model_db_id} 已更新",
            "updated_fields": list(update_data.keys()),
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"更新 LLM 模型失败: {e}")
        raise HTTPException(status_code=500, detail=safe_error_detail(e, "更新 LLM 模型"))


@router.delete("/llm-models/{model_db_id}")
async def delete_llm_model(
    model_db_id: int,
    user: Dict = Depends(get_admin_user),
):
    """
    删除 LLM 模型（软删除，设置 is_enabled=0）
    """
    try:
        from llm.llm_models_service import llm_models_service
        
        success = llm_models_service.delete_model(model_db_id)
        
        if not success:
            raise HTTPException(status_code=500, detail="删除模型失败")
        
        logger.info(f"管理员 {user.get('username')} 删除 LLM 模型 ID={model_db_id}")
        
        return {
            "message": f"模型 ID {model_db_id} 已禁用",
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"删除 LLM 模型失败: {e}")
        raise HTTPException(status_code=500, detail=safe_error_detail(e, "删除 LLM 模型"))
