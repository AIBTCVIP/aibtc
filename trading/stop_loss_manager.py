# stop_loss_manager.py
"""
止盈止损管理模块

职责:
1. 初始止盈止损设置 (开仓后自动挂单)
2. 移动止损 (Trailing Stop) 触发与更新
3. 止损订单确认 (pending -> confirmed)

与 ws_cycle_store_um.py 解耦:
- 本模块只负责止损策略逻辑和订单下发
- 数据存储/读取通过回调或直接 Redis 操作
"""

from __future__ import annotations

import asyncio
import logging
import threading
import traceback
from decimal import Decimal
from typing import Any, Callable, Optional, TYPE_CHECKING, Set
from core.pf_compatibility import pf_compat
from core.utils import D, now_ms, d_to_str

if TYPE_CHECKING:
    from core.user_context import UserContext

logger = logging.getLogger(__name__)


class StopLossManager:
    """
    止盈止损管理器

    配置参数:
    - SL_PCT: 初始止损百分比 (默认 1.5%)
    - TP_PCT: 初始止盈百分比 (默认 5%)
    - TRAIL_STAGES: 移动止损阶段配置

    使用方式:
    1. 初始化时传入 redis_conn, uid, execute_trade_func
    2. 开仓成交后调用 set_initial_stops()
    3. Mark 价格更新时调用 on_mark_tick()
    4. 收到止损订单回报时调用 confirm_trailing_from_order()
    """

    # 初始止损/止盈百分比
    SL_PCT = Decimal("0.015")  # 1.5%
    TP_PCT = Decimal("0.05")  # 5%

    # 移动止损阶段: (触发盈利%, 锁定盈利%)
    # LONG: entry * (1 + lock)
    # SHORT: entry * (1 - lock)
    TRAIL_STAGES = [
        (Decimal("2.2"), Decimal("0.01")),  # >=2.2% -> 锁盈1%
        (Decimal("3.5"), Decimal("0.02")),  # >=3.5% -> 锁盈2%
        (Decimal("5.5"), Decimal("0.04")),  # +5.5% → 锁4%
        (Decimal("7.5"), Decimal("0.055")),  # +7.5% → 锁5.5%
        (Decimal("10.0"), Decimal("0.08")),  # +10.0% → 锁8.0%
    ]
    
    # 类级别的任务跟踪（用于优雅关闭）
    _pending_tasks: Set[asyncio.Task] = set()
    _tasks_lock = threading.Lock()

    def __init__(
            self,
            redis_conn,
            uid: str,
            execute_trade_func: Optional[Callable[..., Any]] = None,
            user_context: Optional["UserContext"] = None,
    ):
        """
        Args:
            redis_conn: Redis 连接
            uid: 用户标识
            execute_trade_func: (已废弃，保留用于兼容) 异步交易执行函数
            user_context: (已废弃，保留用于兼容) 用户上下文
        
        Note:
            现在统一使用 multi_exchange_trader 进行交易执行，
            execute_trade_func 和 user_context 参数仅为向后兼容保留。
        """
        self.rds = redis_conn
        self.uid = uid
        # 以下两个参数已废弃，保留仅为向后兼容
        self._execute_trade_func = execute_trade_func
        self._user_context = user_context

    # ========== 工具方法 ==========

    def _profit_pct(self, *, side: str, entry: Decimal, mark: Decimal) -> Decimal:
        """
        计算盈利百分比

        Returns:
            盈利百分比，例如 2.3 表示 +2.3%
        """
        if entry <= 0 or mark <= 0:
            return D("0")
        side = (side or "").upper()
        if side == "LONG":
            return (mark - entry) / entry * Decimal("100")
        return (entry - mark) / entry * Decimal("100")  # SHORT

    def _trail_stage_for_pct(self, pct: Decimal) -> int:
        """
        根据盈利百分比确定移动止损阶段

        Returns:
            0=不触发; 1..N=阶段号（N=len(TRAIL_STAGES)）
        """
        # 从最高触发档位往下找，保证拿到“最高 stage”
        for i in range(len(self.TRAIL_STAGES) - 1, -1, -1):
            trigger_pct = self.TRAIL_STAGES[i][0]
            if pct >= trigger_pct:
                return i + 1
        return 0

    def _trail_stop_price(self, *, side: str, entry: Decimal, stage: int) -> Decimal:
        """
        根据阶段计算移动止损价格

        Args:
            side: LONG 或 SHORT
            entry: 开仓均价
            stage: 阶段号 (1-4)

        Returns:
            止损价格
        """
        if entry <= 0 or stage <= 0:
            return D("0")
        lock = self.TRAIL_STAGES[stage - 1][1]
        side = (side or "").upper()
        if side == "LONG":
            return entry * (Decimal("1") + lock)
        return entry * (Decimal("1") - lock)  # SHORT

    def _calc_sl_tp(self, entry: Decimal, side: str) -> tuple[str, str]:
        """
        计算初始止损/止盈价格

        Args:
            entry: 开仓均价
            side: LONG 或 SHORT

        Returns:
            (stop_loss, take_profit) 字符串元组
        """
        if entry <= 0:
            return "0", "0"
        if (side or "").upper() == "LONG":
            sl = entry * (Decimal("1") - self.SL_PCT)
            tp = entry * (Decimal("1") + self.TP_PCT)
        else:
            sl = entry * (Decimal("1") + self.SL_PCT)
            tp = entry * (Decimal("1") - self.TP_PCT)
        return d_to_str(sl), d_to_str(tp)

    # ========== 订单下发 ==========

    def _dispatch_execute_trade(self, **kwargs) -> None:
        """
        通过 multi_exchange_trader 下发交易指令
        
        支持异步/同步环境，带任务跟踪
        """
        try:
            from trading.multi_exchange_trader import execute_multi_exchange_trade
            
            symbol = str(kwargs.get('symbol', ''))
            action = str(kwargs.get('action', ''))
            stop_loss = kwargs.get('stop_loss')
            take_profit = kwargs.get('take_profit')
            exchange = kwargs.get('exchange')  # 指定交易所（可选）
            
            # 确保必要参数不为空
            if not symbol or not action:
                logger.error(f"Missing symbol or action in kwargs: {kwargs}")
                return
            
            # 创建协程任务
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                loop = None
            
            async def _execute():
                result = await execute_multi_exchange_trade(
                    uid=self.uid,
                    symbol=symbol,
                    action=action,
                    stop_loss=stop_loss,
                    take_profit=take_profit,
                    exchange=exchange  # 传递指定的交易所
                )
                if result.failure_count > 0:
                    logger.warning(f"[{self.uid}] 部分交易所执行失败: {result.to_dict()}")
                return result
            
            if loop and loop.is_running():
                task = loop.create_task(_execute())
                
                # 跟踪任务
                with self._tasks_lock:
                    self._pending_tasks.add(task)
                
                def _done(t: asyncio.Task):
                    # 从跟踪集合中移除
                    with self._tasks_lock:
                        self._pending_tasks.discard(t)
                    try:
                        exc = t.exception()
                        if exc:
                            logger.error(f"Task error uid={self.uid} kwargs={kwargs} err={exc}")
                    except asyncio.CancelledError:
                        pass
                    except Exception as e:
                        logger.error(f"Task callback error: {e}")
                
                task.add_done_callback(_done)
            else:
                # 没有运行的事件循环，使用线程池执行
                import concurrent.futures
                with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                    future = executor.submit(asyncio.run, _execute())
                    try:
                        future.result(timeout=30)  # 30秒超时
                    except Exception as e:
                        logger.error(f"Sync execute error: {e}")
                
        except Exception as e:
            logger.error(f"Dispatch error kwargs={kwargs} err={e}")
            logger.exception("止损任务分发失败")
    
    @classmethod
    def get_pending_tasks_count(cls) -> int:
        """获取待处理任务数量"""
        with cls._tasks_lock:
            return len(cls._pending_tasks)
    
    @classmethod
    async def wait_pending_tasks(cls, timeout: float = 10.0) -> int:
        """
        等待所有待处理任务完成
        
        Args:
            timeout: 最大等待时间（秒）
        
        Returns:
            未完成的任务数量
        """
        with cls._tasks_lock:
            tasks = list(cls._pending_tasks)
        
        if not tasks:
            return 0
        
        logger.info(f"[StopLossManager] 等待 {len(tasks)} 个待处理任务...")
        
        try:
            done, pending = await asyncio.wait(tasks, timeout=timeout)
            if pending:
                logger.warning(f"[StopLossManager] {len(pending)} 个任务超时未完成")
                for task in pending:
                    task.cancel()
            return len(pending)
        except Exception as e:
            logger.error(f"[StopLossManager] 等待任务失败: {e}")
            return len(tasks)

    # ========== 初始止盈止损 ==========

    def set_initial_stops(
            self,
            symbol: str,
            side: str,
            entry: Decimal,
            order_type: Optional[str] = None,
            exchange: Optional[str] = None,
    ) -> tuple[str, str]:
        """
        设置初始止盈止损订单

        Args:
            symbol: 交易对
            side: LONG 或 SHORT
            entry: 开仓均价
            order_type: 订单类型 (MARKET/LIMIT/...)，市价单不设置止盈止损
            exchange: 指定交易所（可选）

        Returns:
            (stop_loss, take_profit) 实际下单的价格，市价单返回 ("0", "0")
        """
        # 只有明确的市价单才跳过止盈止损设置
        # 如果 order_type 为空或不确定，默认设置止盈止损
        skip_stop_orders = False

        if order_type:
            order_type_upper = str(order_type).strip().upper()
            if order_type_upper in ("MARKET", "STOP_MARKET"):
                skip_stop_orders = True
                logger.info(f"[设置止盈止损] SKIP symbol={symbol} side={side} reason=MARKET_ORDER({order_type})")
            else:
                logger.info(f"[设置止盈止损] SET symbol={symbol} side={side} order_type={order_type_upper}")
        else:
            logger.info(f"[设置止盈止损] SET symbol={symbol} side={side} order_type=None -> 默认设置")

        if skip_stop_orders:
            return "0", "0"

        stop_loss, take_profit = self._calc_sl_tp(entry, side)

        logger.debug(f"[设置止盈止损] CALCULATED symbol={symbol} side={side} entry={entry} sl={stop_loss} tp={take_profit}")

        self._dispatch_execute_trade(
            symbol=symbol,
            action="stop_orders",
            stop_loss=stop_loss,
            take_profit=take_profit,
            exchange=exchange,  # 指定交易所
        )
        logger.info(f"[设置止盈止损] SUBMITTED symbol={symbol} side={side} sl={stop_loss} tp={take_profit} exchange={exchange}")

        return stop_loss, take_profit

    # ========== 移动止损 (Trailing) ==========

    def on_mark_tick(self, sym: str, mark: Decimal, ts: int, exchange: str = None) -> None:
        """
        Mark 价格更新回调 - 检查并触发移动止损

        流程:
        1. 检查是否有活跃仓位
        2. 计算当前盈利百分比
        3. 确定目标阶段
        4. 如果需要升级，写 pending 并下发更新订单

        Args:
            sym: 交易对符号
            mark: 当前标记价格
            ts: 时间戳 (毫秒)
            exchange: 指定交易所（必须指定，用于正确读写 Redis 数据）
        """
        try:
            # 必须指定交易所，否则无法正确读写按交易所隔离的 Redis 数据
            if not exchange:
                logger.warning(f"[{self.uid}] on_mark_tick 未指定 exchange，跳过")
                return
            
            # 使用兼容层获取指定交易所的数据
            pos_data = pf_compat.get_pf_pos(self.uid, exchange=exchange)
            cycle_data = pf_compat.get_pf_cycle(self.uid, exchange=exchange)

            fields = [f"{sym}:LONG", f"{sym}:SHORT"]

            for field in fields:
                pos = pos_data.get(field, {})
                cyc = cycle_data.get(field, {})

                if not pos or not cyc:
                    continue
                side = (pos.get("side") or "").upper()
                if side not in ("LONG", "SHORT"):
                    continue

                qty = D(pos.get("qty", "0"))
                if qty <= 0:
                    continue

                # 优先用 pos.entryPrice，其次 cyc.avgOpenPrice
                entry = D(pos.get("entryPrice", "0"))
                if entry <= 0:
                    entry = D(cyc.get("avgOpenPrice", "0"))
                if entry <= 0 or mark <= 0:
                    continue

                pct = self._profit_pct(side=side, entry=entry, mark=mark)
                target_stage = self._trail_stage_for_pct(pct)
                if target_stage <= 0:
                    continue

                cur_stage = int(cyc.get("slTrailStage", "0") or "0")
                pending_stage = int(cyc.get("slTrailPendingStage", "0") or "0")

                # 只执行最高 stage（忽略中间 stage）
                effective_stage = max(cur_stage, pending_stage)
                if target_stage <= effective_stage:
                    continue

                new_sl = self._trail_stop_price(side=side, entry=entry, stage=target_stage)
                if new_sl <= 0:
                    continue

                # 防止"立刻触发/被拒"：留缓冲，避免 tickSize normalize 后贴到 mark
                eps = entry * Decimal("0.00005")  # 0.005%
                if side == "LONG" and new_sl >= (mark - eps):
                    continue
                if side == "SHORT" and new_sl <= (mark + eps):
                    continue

                # 检查 new_sl 是否比当前止损更优（只能朝有利方向移动）
                current_sl_str = pos.get("stopLossPrice") or "0"
                try:
                    current_sl = D(current_sl_str) if current_sl_str and current_sl_str != "0" else None
                except (ValueError, TypeError, Exception):
                    current_sl = None
                
                if current_sl is not None and current_sl > 0:
                    if side == "LONG":
                        # 多单：新止损必须严格更高（更接近当前价）
                        if new_sl <= current_sl:
                            continue
                    else:  # SHORT
                        # 空单：新止损必须严格更低（更接近当前价）
                        if new_sl >= current_sl:
                            continue

                # 防抖：同一 exchange+field+stage 10 秒内只发一次
                guard_key = f"pf:trail_sl:sent:{self.uid}:{exchange}:{field}:{target_stage}"
                if not self.rds.set(guard_key, "1", nx=True, ex=10):
                    continue

                # cycle 短锁（增加到 500ms，给异步操作更多时间）
                lock_key = f"pf:lock:cycle:{self.uid}:{exchange}:{field}"
                if not self.rds.set(lock_key, "1", nx=True, px=500):
                    try:
                        self.rds.delete(guard_key)
                    except Exception as e:
                        logger.debug(f"[{self.uid}] 删除 guard_key 失败 (非关键): {e}")
                    continue

                try:
                    # 再次获取最新数据（降低覆盖概率）
                    latest_cycle_data = pf_compat.get_pf_cycle(self.uid, exchange=exchange)
                    cyc2 = latest_cycle_data.get(field, {})
                    if not cyc2:
                        try:
                            self.rds.delete(guard_key)
                        except Exception as e:
                            logger.debug(f"[{self.uid}] 删除 guard_key 失败 (非关键): {e}")
                        continue

                    cur_stage2 = int(cyc2.get("slTrailStage", "0") or "0")
                    pending_stage2 = int(cyc2.get("slTrailPendingStage", "0") or "0")
                    if target_stage <= cur_stage2 or target_stage <= pending_stage2:
                        continue

                    sl_str = d_to_str(new_sl)

                    # 先写 pending（不直接推进 stage）
                    cyc2["slTrailPendingStage"] = str(target_stage)
                    cyc2["slTrailPendingStopLoss"] = sl_str
                    cyc2["slTrailPendingTs"] = str(ts)
                    cyc2["slTrailPendingMark"] = d_to_str(D(mark))
                    cyc2["slTrailPendingPct"] = d_to_str(pct)

                    # 使用兼容层更新 cycle 数据（指定交易所）
                    latest_cycle_data[field] = cyc2
                    pf_compat.set_pf_cycle(self.uid, latest_cycle_data, exchange=exchange)

                    # 同步到 pos（前端展示 pending）
                    try:
                        latest_pos_data = pf_compat.get_pf_pos(self.uid, exchange=exchange)
                        pos2 = latest_pos_data.get(field, {})
                        if pos2:
                            pos2["slTrailPendingStage"] = str(target_stage)
                            pos2["slTrailPendingStopLoss"] = sl_str
                            pos2["slTrailPendingTs"] = str(ts)
                            pos2["updatedAt"] = str(ts)
                            latest_pos_data[field] = pos2
                            pf_compat.set_pf_pos(self.uid, latest_pos_data, exchange=exchange)
                    except Exception as e:
                        logger.debug(f"[{self.uid}] 同步 pos 展示数据失败 (非关键): {e}")

                    # 下发更新止损（异步）- 指定交易所
                    self._dispatch_execute_trade(
                        symbol=sym,
                        action="update_stop_loss",
                        stop_loss=sl_str,
                        exchange=exchange,
                    )
                    logger.info(
                        f"[调整移动止损] symbol={sym} side={side} stage={target_stage} "
                        f"entry={d_to_str(entry)} mark={d_to_str(mark)} new_sl={sl_str} "
                        f"exchange={exchange}"
                    )

                except Exception as e:
                    # P0 Fix: 记录异常而非静默吞掉
                    logger.error(
                        f"[{self.uid}] 移动止损更新失败: symbol={sym} exchange={exchange} "
                        f"target_stage={target_stage} error={e}"
                    )
                    # 失败释放 guard：允许快速重试
                    try:
                        self.rds.delete(guard_key)
                    except Exception as e:
                        logger.debug(f"[{self.uid}] 删除 guard_key 失败 (非关键): {e}")
                finally:
                    try:
                        self.rds.delete(lock_key)
                    except Exception as e:
                        logger.debug(f"[{self.uid}] 删除 lock_key 失败 (非关键): {e}")

        except Exception as e:
            # 记录异常但不影响 mark ws
            logger.debug(f"[{self.uid}] on_mark_tick 异常: {e}")

    # ========== 加仓后重置移动止损阶段 ==========
    def reset_trailing_on_add_position(self, symbol: str, side: str, exchange: str = None) -> None:
        """加仓后重置移动止损阶段，让系统基于新均价重新判断
        
        Args:
            symbol: 交易对
            side: 持仓方向 (LONG/SHORT)
            exchange: 交易所名称（多交易所环境必须指定）
        """
        # 必须指定交易所
        if not exchange:
            logger.warning(f"[{self.uid}] reset_trailing_on_add_position 未指定 exchange，跳过")
            return
        
        field = f"{symbol}:{side.upper()}"
        # 使用兼容层获取数据（指定 exchange 以正确分离各交易所数据）
        cycle_data = pf_compat.get_pf_cycle(self.uid, exchange=exchange)
        pos_data = pf_compat.get_pf_pos(self.uid, exchange=exchange)

        lock_key = f"pf:lock:cycle:{self.uid}:{exchange}:{field}"
        if not self.rds.set(lock_key, "1", nx=True, px=300):
            return

        try:
            cyc = cycle_data.get(field, {})
            if not cyc:
                return

            old_stage = cyc.get("slTrailStage", "0")
            old_pending = cyc.get("slTrailPendingStage", "0")

            # 只重置 stage，止损价等让系统重新计算
            cyc["slTrailStage"] = "0"
            cyc["slTrailPendingStage"] = "0"
            cyc["updatedAt"] = str(now_ms())

            # 更新到兼容层（指定 exchange）
            cycle_data[field] = cyc
            pf_compat.set_pf_cycle(self.uid, cycle_data, exchange=exchange)

            # 同步 pos
            pos = pos_data.get(field, {})
            if pos:
                pos["slTrailStage"] = "0"
                pos["slTrailPendingStage"] = "0"
                pos["updatedAt"] = str(now_ms())
                # 更新到兼容层（指定 exchange）
                pos_data[field] = pos
                pf_compat.set_pf_pos(self.uid, pos_data, exchange=exchange)

            if old_stage != "0" or old_pending != "0":
                logger.info(f"[移动止损重置] 加仓触发 symbol={symbol} side={side} "
                            f"old_stage={old_stage} old_pending={old_pending} exchange={exchange}")
        finally:
            try:
                self.rds.delete(lock_key)
            except Exception as e:
                logger.debug(f"[{self.uid}] 删除 lock_key 失败 (非关键): {e}")

    # ========== Trailing 确认 ==========

    def confirm_trailing_from_order(self, o: dict, exchange: str = None) -> None:
        """
        从订单回报确认移动止损 (pending -> confirmed)

        当 WS 回报显示 stop 类订单创建/成交时，
        把 pending 提升为正式 slTrailStage

        Args:
            o: ORDER_TRADE_UPDATE 中的 'o' 字段
            exchange: 交易所名称（多交易所环境必须指定）
        """
        try:
            # 必须指定交易所
            if not exchange:
                logger.warning(f"[{self.uid}] confirm_trailing_from_order 未指定 exchange，跳过")
                return
            
            exec_type = (o.get("x") or "").upper()  # NEW / TRADE / ...
            if exec_type not in ("NEW", "TRADE"):
                return

            symbol = o.get("s")
            ps = (o.get("ps") or "").upper()
            if not symbol or ps not in ("LONG", "SHORT"):
                return

            order_type = (o.get("o") or "").upper()

            # 宽松判断：STOP/STOP_MARKET/STOP_LOSS... 都算
            is_stop_like = any(k in order_type for k in ("STOP", "STOP_MARKET", "STOP_LOSS"))
            if not is_stop_like:
                return

            field = f"{symbol}:{ps}"
            # 使用兼容层获取数据（指定 exchange）
            cycle_data = pf_compat.get_pf_cycle(self.uid, exchange=exchange)

            lock_key = f"pf:lock:cycle:{self.uid}:{exchange}:{field}"
            if not self.rds.set(lock_key, "1", nx=True, px=300):
                return

            try:
                cyc = cycle_data.get(field, {})
                if not cyc:
                    return

                p_stage = int(cyc.get("slTrailPendingStage", "0") or "0")
                if p_stage <= 0:
                    return

                # pending -> 正式
                cyc["slTrailStage"] = str(p_stage)
                cyc["slTrailStopLoss"] = str(cyc.get("slTrailPendingStopLoss", "0") or "0")
                cyc["slTrailLastTs"] = str(cyc.get("slTrailPendingTs", "0") or "0")

                # 清空 pending
                cyc["slTrailPendingStage"] = "0"
                cyc["slTrailPendingStopLoss"] = "0"
                cyc["slTrailPendingTs"] = "0"
                cyc["slTrailPendingMark"] = "0"
                cyc["slTrailPendingPct"] = "0"

                # 更新到兼容层（指定 exchange）
                cycle_data[field] = cyc
                pf_compat.set_pf_cycle(self.uid, cycle_data, exchange=exchange)

                # 同步 pos（展示用，指定 exchange）
                pos_data = pf_compat.get_pf_pos(self.uid, exchange=exchange)
                pos = pos_data.get(field, {})
                if pos:
                    pos["slTrailStage"] = cyc["slTrailStage"]
                    pos["slTrailStopLoss"] = cyc["slTrailStopLoss"]
                    pos["slTrailLastTs"] = cyc["slTrailLastTs"]
                    pos["slTrailPendingStage"] = "0"
                    pos["slTrailPendingStopLoss"] = "0"
                    pos["slTrailPendingTs"] = "0"
                    pos["updatedAt"] = str(now_ms())
                    # 更新到兼容层（指定 exchange）
                    pos_data[field] = pos
                    pf_compat.set_pf_pos(self.uid, pos_data, exchange=exchange)

                logger.info(
                    f"[移动止损确认] symbol={symbol} side={ps} stage={p_stage} "
                    f"sl={cyc['slTrailStopLoss']} exchange={exchange}"
                )

            finally:
                try:
                    self.rds.delete(lock_key)
                except Exception as e:
                    logger.debug(f"[{self.uid}] 删除 lock_key 失败 (非关键): {e}")
        except Exception as e:
            logger.debug(f"[{self.uid}] confirm_trailing_from_order 异常: {e}")

    # ========== 配置更新 ==========

    def update_config(
            self,
            *,
            sl_pct: Optional[Decimal] = None,
            tp_pct: Optional[Decimal] = None,
            trail_stages: Optional[list] = None,
    ) -> None:
        """
        动态更新止损配置

        Args:
            sl_pct: 初始止损百分比
            tp_pct: 初始止盈百分比
            trail_stages: 移动止损阶段配置 [(trigger_pct, lock_pct), ...]
        """
        if sl_pct is not None:
            self.SL_PCT = D(sl_pct)
        if tp_pct is not None:
            self.TP_PCT = D(tp_pct)
        if trail_stages is not None:
            self.TRAIL_STAGES = [
                (D(t[0]), D(t[1])) for t in trail_stages
            ]
        logger.info(
            f"Config updated: SL={self.SL_PCT}, TP={self.TP_PCT}, "
            f"TRAIL_STAGES={len(self.TRAIL_STAGES)} stages"
        )


# 测试函数（开发时使用）
