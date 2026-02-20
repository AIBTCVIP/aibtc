# referral_api.py
"""
邀请返佣 API

功能：
1. 用户邀请码管理
2. 绑定邀请关系
3. 返佣查询
4. 排行榜
5. 管理员配置
"""

import logging
from typing import Optional, List, Dict
from fastapi import APIRouter, HTTPException, Depends, Query
from pydantic import BaseModel, Field

from api.user_api import get_current_user
from api.admin_api import get_admin_user
from core.referral_db import referral_db, CommissionSourceType, LeaderboardPeriod

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/referral", tags=["邀请返佣"])
admin_router = APIRouter(prefix="/api/admin/referral", tags=["管理员-邀请返佣"])


# ============================================================
# 数据模型
# ============================================================

class BindReferrerRequest(BaseModel):
    referral_code: str = Field(..., min_length=4, max_length=16)


class ReferralCodeResponse(BaseModel):
    referral_code: str
    referral_link: str


class ReferralStatsResponse(BaseModel):
    referral_code: str
    referrer_uid: Optional[str]
    total_team: int
    direct_count: int
    level_counts: Dict[int, int]
    total_earned: float
    available_balance: float


class CommissionRateItem(BaseModel):
    level: int = Field(..., ge=1, le=15)
    rate: float = Field(..., ge=0, le=1)
    min_amount: float = Field(default=0, ge=0)
    is_enabled: bool = True


class BatchUpdateRatesRequest(BaseModel):
    rates: List[CommissionRateItem]


class LeaderboardSettingsRequest(BaseModel):
    show_on_leaderboard: Optional[bool] = None
    display_name: Optional[str] = Field(None, max_length=64)
    hide_profit_amount: Optional[bool] = None


# ============================================================
# 用户 API
# ============================================================

@router.get("/code", response_model=ReferralCodeResponse)
async def get_referral_code(user: Dict = Depends(get_current_user)):
    """获取我的邀请码"""
    uid = user["uid"]
    
    code = referral_db.generate_referral_code(uid)
    
    # 构建邀请链接（根据实际域名配置）
    base_url = "https://aibtc.vip"  # 可从配置读取
    referral_link = f"{base_url}/register?ref={code}"
    
    return ReferralCodeResponse(
        referral_code=code,
        referral_link=referral_link
    )


@router.post("/bind")
async def bind_referrer(
    req: BindReferrerRequest,
    user: Dict = Depends(get_current_user)
):
    """绑定邀请人"""
    uid = user["uid"]
    
    success, message = referral_db.bind_referrer(uid, req.referral_code)
    
    if not success:
        raise HTTPException(status_code=400, detail=message)
    
    return {"message": message}


@router.get("/stats", response_model=ReferralStatsResponse)
async def get_referral_stats(user: Dict = Depends(get_current_user)):
    """获取我的邀请统计"""
    uid = user["uid"]
    
    # 获取邀请信息
    info = referral_db.get_user_referral_info(uid)
    code = info.get("referral_code") if info else referral_db.generate_referral_code(uid)
    
    # 获取团队统计
    team_stats = referral_db.get_team_stats(uid)
    
    # 获取返佣余额
    balance = referral_db.get_user_commission_balance(uid)
    
    return ReferralStatsResponse(
        referral_code=code,
        referrer_uid=info.get("referrer_uid") if info else None,
        total_team=team_stats["total_team"],
        direct_count=team_stats["direct_count"],
        level_counts=team_stats["level_counts"],
        total_earned=balance.get("total_earned", 0),
        available_balance=balance.get("available_balance", 0)
    )


@router.get("/direct-referrals")
async def get_direct_referrals(
    user: Dict = Depends(get_current_user),
    limit: int = Query(default=50, ge=1, le=100),
    offset: int = Query(default=0, ge=0)
):
    """获取直接邀请的用户列表"""
    uid = user["uid"]
    
    referrals = referral_db.get_direct_referrals(uid, limit, offset)
    
    return {
        "referrals": referrals,
        "limit": limit,
        "offset": offset
    }


@router.get("/team-tree")
async def get_team_tree(
    user: Dict = Depends(get_current_user),
    max_depth: int = Query(default=3, ge=1, le=5)
):
    """
    获取团队树形结构（最多5层，避免数据量过大）
    """
    uid = user["uid"]
    
    def build_tree(parent_uid: str, depth: int) -> List[Dict]:
        if depth > max_depth:
            return []
        
        children = referral_db.get_direct_referrals(parent_uid, limit=100)
        result = []
        
        for child in children:
            child_uid = child["uid"]
            node = {
                "uid": child_uid,
                "username": child.get("username"),
                "referred_at": str(child.get("referred_at")),
                "level": depth,
                "children": build_tree(child_uid, depth + 1) if depth < max_depth else []
            }
            result.append(node)
        
        return result
    
    tree = build_tree(uid, 1)
    
    return {
        "root_uid": uid,
        "tree": tree,
        "max_depth": max_depth
    }


@router.get("/commissions")
async def get_commission_history(
    user: Dict = Depends(get_current_user),
    status: Optional[str] = Query(default=None),
    limit: int = Query(default=50, ge=1, le=100),
    offset: int = Query(default=0, ge=0)
):
    """获取返佣记录"""
    uid = user["uid"]
    
    commissions = referral_db.get_commission_history(uid, status, limit, offset)
    
    return {
        "commissions": commissions,
        "limit": limit,
        "offset": offset
    }


@router.get("/balance")
async def get_commission_balance(user: Dict = Depends(get_current_user)):
    """获取返佣余额"""
    uid = user["uid"]
    
    balance = referral_db.get_user_commission_balance(uid)
    
    return balance


# ============================================================
# 排行榜 API
# ============================================================

@router.get("/leaderboard")
async def get_leaderboard(
    period: str = Query(default="daily", pattern="^(daily|weekly|monthly|all_time)$"),
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    min_trades: int = Query(default=1, ge=0)
):
    """获取收益排行榜"""
    leaderboard = referral_db.get_leaderboard(
        period_type=period,
        limit=limit,
        offset=offset,
        min_trades=min_trades
    )
    
    return {
        "period": period,
        "leaderboard": leaderboard,
        "limit": limit,
        "offset": offset
    }


@router.get("/leaderboard/my-rank")
async def get_my_rank(
    user: Dict = Depends(get_current_user),
    period: str = Query(default="daily", pattern="^(daily|weekly|monthly|all_time)$")
):
    """获取我的排名"""
    uid = user["uid"]
    
    rank_info = referral_db.get_user_rank(uid, period)
    
    if not rank_info:
        return {
            "period": period,
            "rank": None,
            "message": "暂无排名数据"
        }
    
    return {
        "period": period,
        **rank_info
    }


@router.put("/leaderboard/settings")
async def update_leaderboard_settings(
    req: LeaderboardSettingsRequest,
    user: Dict = Depends(get_current_user)
):
    """更新排行榜显示设置"""
    uid = user["uid"]
    
    success = referral_db.update_leaderboard_settings(
        uid=uid,
        show_on_leaderboard=req.show_on_leaderboard,
        display_name=req.display_name,
        hide_profit_amount=req.hide_profit_amount
    )
    
    if not success:
        raise HTTPException(status_code=500, detail="更新设置失败")
    
    return {"message": "设置已更新"}


# ============================================================
# 管理员 API
# ============================================================

@admin_router.get("/rates")
async def get_commission_rates(user: Dict = Depends(get_admin_user)):
    """获取所有层级的返佣比例配置"""
    rates = referral_db.get_commission_rates()
    
    return {
        "rates": rates,
        "max_levels": referral_db.MAX_REFERRAL_LEVELS
    }


@admin_router.put("/rates/{level}")
async def update_commission_rate(
    level: int,
    rate: float = Query(..., ge=0, le=1),
    min_amount: float = Query(default=0, ge=0),
    is_enabled: bool = Query(default=True),
    user: Dict = Depends(get_admin_user)
):
    """更新指定层级的返佣比例"""
    if level < 1 or level > referral_db.MAX_REFERRAL_LEVELS:
        raise HTTPException(status_code=400, detail=f"层级必须在 1-{referral_db.MAX_REFERRAL_LEVELS} 之间")
    
    success = referral_db.update_commission_rate(
        level=level,
        rate=rate,
        min_amount=min_amount,
        is_enabled=is_enabled,
        updated_by=user["uid"]
    )
    
    if not success:
        raise HTTPException(status_code=500, detail="更新失败")
    
    return {
        "message": f"第 {level} 层返佣比例已更新为 {rate*100}%",
        "level": level,
        "rate": rate,
        "min_amount": min_amount,
        "is_enabled": is_enabled
    }


@admin_router.put("/rates")
async def batch_update_commission_rates(
    req: BatchUpdateRatesRequest,
    user: Dict = Depends(get_admin_user)
):
    """批量更新返佣比例"""
    rates = [
        {
            "level": item.level,
            "rate": item.rate,
            "min_amount": item.min_amount,
            "is_enabled": item.is_enabled
        }
        for item in req.rates
    ]
    
    success = referral_db.batch_update_commission_rates(rates, updated_by=user["uid"])
    
    if not success:
        raise HTTPException(status_code=500, detail="批量更新失败")
    
    return {
        "message": f"已更新 {len(rates)} 个层级的返佣比例",
        "updated_levels": [item.level for item in req.rates]
    }


@admin_router.get("/stats")
async def get_referral_system_stats(user: Dict = Depends(get_admin_user)):
    """获取返佣系统统计"""
    from core.referral_db import referral_db
    
    with referral_db._get_connection() as conn:
        try:
            from sqlalchemy import text
            HAS_SQLALCHEMY = True
        except ImportError:
            HAS_SQLALCHEMY = False
        
        if HAS_SQLALCHEMY:
            # 总用户数
            total_users = conn.execute(text(
                "SELECT COUNT(*) FROM user_referrals"
            )).fetchone()[0]
            
            # 有邀请人的用户数
            referred_users = conn.execute(text(
                "SELECT COUNT(*) FROM user_referrals WHERE referrer_uid IS NOT NULL"
            )).fetchone()[0]
            
            # 总返佣金额
            total_commission = conn.execute(text(
                "SELECT COALESCE(SUM(commission_amount), 0) FROM referral_commissions"
            )).fetchone()[0]
            
            # 待结算返佣
            pending_commission = conn.execute(text(
                "SELECT COALESCE(SUM(commission_amount), 0) FROM referral_commissions WHERE status = 'pending'"
            )).fetchone()[0]
            
            # 已结算返佣
            settled_commission = conn.execute(text(
                "SELECT COALESCE(SUM(commission_amount), 0) FROM referral_commissions WHERE status = 'settled'"
            )).fetchone()[0]
        else:
            cursor = conn.execute("SELECT COUNT(*) FROM user_referrals")
            total_users = cursor.fetchone()[0]
            
            cursor = conn.execute("SELECT COUNT(*) FROM user_referrals WHERE referrer_uid IS NOT NULL")
            referred_users = cursor.fetchone()[0]
            
            cursor = conn.execute("SELECT COALESCE(SUM(commission_amount), 0) FROM referral_commissions")
            total_commission = cursor.fetchone()[0]
            
            cursor = conn.execute("SELECT COALESCE(SUM(commission_amount), 0) FROM referral_commissions WHERE status = 'pending'")
            pending_commission = cursor.fetchone()[0]
            
            cursor = conn.execute("SELECT COALESCE(SUM(commission_amount), 0) FROM referral_commissions WHERE status = 'settled'")
            settled_commission = cursor.fetchone()[0]
    
    return {
        "total_users": total_users,
        "referred_users": referred_users,
        "referral_rate": referred_users / total_users if total_users > 0 else 0,
        "total_commission": float(total_commission or 0),
        "pending_commission": float(pending_commission or 0),
        "settled_commission": float(settled_commission or 0)
    }


@admin_router.get("/commissions")
async def get_all_commissions(
    user: Dict = Depends(get_admin_user),
    status: Optional[str] = Query(default=None),
    uid: Optional[str] = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0)
):
    """获取所有返佣记录"""
    from core.referral_db import referral_db
    
    with referral_db._get_connection() as conn:
        try:
            from sqlalchemy import text
            HAS_SQLALCHEMY = True
        except ImportError:
            HAS_SQLALCHEMY = False
        
        conditions = []
        params = {"limit": limit, "offset": offset}
        
        if status:
            conditions.append("status = :status")
            params["status"] = status
        if uid:
            conditions.append("(uid = :uid OR from_uid = :uid)")
            params["uid"] = uid
        
        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        
        if HAS_SQLALCHEMY:
            results = conn.execute(text(f"""
                SELECT * FROM referral_commissions
                {where_clause}
                ORDER BY created_at DESC
                LIMIT :limit OFFSET :offset
            """), params).fetchall()
            
            commissions = [dict(r._mapping) for r in results]
        else:
            # SQLite 版本需要调整参数占位符
            sql = f"""
                SELECT * FROM referral_commissions
                {where_clause.replace(':status', '?').replace(':uid', '?').replace(':limit', '?').replace(':offset', '?')}
                ORDER BY created_at DESC
                LIMIT ? OFFSET ?
            """
            cursor_params = []
            if status:
                cursor_params.append(status)
            if uid:
                cursor_params.extend([uid, uid])
            cursor_params.extend([limit, offset])
            
            cursor = conn.execute(sql, tuple(cursor_params))
            commissions = [dict(r) for r in cursor.fetchall()]
    
    return {
        "commissions": commissions,
        "limit": limit,
        "offset": offset
    }


@admin_router.post("/settle")
async def settle_commissions(
    user: Dict = Depends(get_admin_user),
    uid: Optional[str] = Query(default=None, description="指定用户ID，为空则结算所有")
):
    """手动结算返佣"""
    count = referral_db.settle_commissions(uid)
    
    return {
        "message": f"已结算 {count} 条返佣记录",
        "settled_count": count
    }


@admin_router.get("/user/{uid}")
async def get_user_referral_detail(
    uid: str,
    user: Dict = Depends(get_admin_user)
):
    """获取指定用户的邀请详情"""
    # 基本信息
    info = referral_db.get_user_referral_info(uid)
    if not info:
        raise HTTPException(status_code=404, detail="用户不存在")
    
    # 团队统计
    team_stats = referral_db.get_team_stats(uid)
    
    # 返佣余额
    balance = referral_db.get_user_commission_balance(uid)
    
    # 最近返佣记录
    recent_commissions = referral_db.get_commission_history(uid, limit=20)
    
    # 直接邀请
    direct_referrals = referral_db.get_direct_referrals(uid, limit=20)
    
    return {
        "user_info": info,
        "team_stats": team_stats,
        "balance": balance,
        "recent_commissions": recent_commissions,
        "direct_referrals": direct_referrals
    }


# ============================================================
# 排行榜管理员 API
# ============================================================

@admin_router.get("/leaderboard/stats")
async def get_leaderboard_stats(user: Dict = Depends(get_admin_user)):
    """获取排行榜统计"""
    from core.referral_db import referral_db
    
    stats = {}
    
    for period in ["daily", "weekly", "monthly", "all_time"]:
        leaderboard = referral_db.get_leaderboard(period_type=period, limit=10)
        stats[period] = {
            "top_10": leaderboard,
            "total_participants": len(leaderboard)  # 简化，实际应查询总数
        }
    
    return stats
