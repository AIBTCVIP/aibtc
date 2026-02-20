# ai500.py
import logging
import requests
from threading import Timer
from datetime import datetime
from core.database import redis_client

logger = logging.getLogger(__name__)

# 配置
INTERVAL = 180  # 每180秒（3分钟）执行一次
REDIS_KEY = "AI500_SYMBOLS"

EXCLUDE_SYMBOLS = {"PAXGUSDT"}
# LATEST_URL = "http://127.0.0.1:8900/latest"
LATEST_URL = "https://token.aibtc.vip/latest"

def _fetch_symbols():
    """
    获取所有符合条件的币种列表
    只从最新接口获取，排除 EXCLUDE_SYMBOLS
    """
    symbols_set = set()

    # --- 最新接口 ---
    try:
        resp = requests.get(LATEST_URL, timeout=5)
        coins = resp.json().get("data", {}).get("coins", [])
        for c in coins:
            pair = c.get("pair")
            if pair and pair not in EXCLUDE_SYMBOLS:
                symbols_set.add(pair)
    except Exception as e:
        logger.warning(f"[AI500] latest 获取失败: {e}")

    merged_list = sorted(symbols_set)
    return merged_list

def _schedule_next():
    """
    启动下一次 Timer（守护线程）
    """
    t = Timer(INTERVAL, update_oi_symbols)
    t.daemon = True
    t.start()

def update_oi_symbols():
    """
    主函数：获取币种并更新 Redis
    """
    now = datetime.now()

    # 跳过整 1 小时节点（HH:00）
    if now.minute == 0:
        logger.info(f"[AI500] {now.strftime('%H:%M')} 是整点，跳过执行")
    else:
        symbols = _fetch_symbols()
        if symbols:
            redis_client.delete(REDIS_KEY)
            redis_client.rpush(REDIS_KEY, *symbols)
            logger.debug(f"[AI500] 更新 Redis 成功: {len(symbols)} 个币种")
        else:
            logger.warning("[AI500] 获取为空，Redis 不更新")

    # 调度下一次执行
    _schedule_next()
