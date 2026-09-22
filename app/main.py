"""并行任务协调收尾屏障 API。

路由：
  POST   /barriers                          创建屏障（参与者名单、达标人数、截止时间）
  GET    /barriers/{bid}                    查询当前状态（不阻塞）
  POST   /barriers/{bid}/checkin            参与者幂等报到
  POST   /barriers/{bid}/batch-checkins     参与者批量幂等报到（幂等键 + 首次结果回放）
  POST   /barriers/{bid}/cancel             主动取消（与达标报到原子竞争，先发生者为唯一终态）
  PATCH  /barriers/{bid}/deadline           运行中延期（仅 waiting 且新时间更晚时原子生效）
  POST   /barriers/{bid}/replace            运行中替补（未报到成员原子换为名单外新标识并继承分组）
  GET    /barriers/{bid}/wait               阻塞等待达标 / 取消 / 截止
  GET    /barriers/{bid}/events             审计事件（报到 + 替补，cursor/limit 分页）

交互文档：GET /docs（Swagger UI）
"""

from __future__ import annotations

import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from redis.asyncio import Redis
from redis.exceptions import RedisError

from .barrier_store import (
    AlreadyCheckedIn,
    BarrierCancelled,
    BarrierDeadlineConflict,
    BarrierExpired,
    BarrierNotFound,
    BarrierReached,
    BarrierStore,
    BarrierSubstituteConflict,
    BatchNotParticipant,
    IdempotencyConflict,
    NotAParticipant,
    ParticipantNotInRoster,
    ReplacementInRoster,
)
from .config import load_settings
from .models import (
    BarrierEventsPage,
    BarrierView,
    BatchCheckinRequest,
    BatchCheckinResponse,
    BatchParticipantResult,
    CancelRequest,
    CancelResponse,
    CheckinEvent,
    CheckinRequest,
    CheckinResponse,
    CreateBarrierRequest,
    ExtendDeadlineRequest,
    ReplaceRequest,
    ReplaceResponse,
)
from .service import _dt, build_view, build_view_from_snapshot, wait_for_settle

settings = load_settings()


@asynccontextmanager
async def lifespan(app: FastAPI):
    redis = Redis.from_url(settings.redis_url, decode_responses=True)
    app.state.store = BarrierStore(redis)
    yield
    await app.state.store.aclose()


app = FastAPI(
    title="任务收尾屏障 API",
    description=(
        "并行任务协调收尾：创建屏障、幂等报到、批量幂等报到、阻塞等待达标、"
        "取消或截止、运行中延期与运行中参与者替补。"
    ),
    version="1.5.0",
    lifespan=lifespan,
)


def store(request: Request) -> BarrierStore:
    return request.app.state.store


@app.exception_handler(RedisError)
async def redis_error_handler(request: Request, exc: RedisError) -> JSONResponse:
    return JSONResponse(
        status_code=503, content={"detail": f"Redis 暂不可用: {exc}"}
    )


@app.get("/", tags=["meta"])
async def root() -> dict[str, object]:
    return {
        "service": "task-barrier",
        "docs": "/docs",
        "health": "/health",
        "endpoints": {
            "create": "POST /barriers",
            "get": "GET /barriers/{barrier_id}",
            "checkin": "POST /barriers/{barrier_id}/checkin",
            "batch_checkin": "POST /barriers/{barrier_id}/batch-checkins",
            "cancel": "POST /barriers/{barrier_id}/cancel",
            "extend": "PATCH /barriers/{barrier_id}/deadline",
            "replace": "POST /barriers/{barrier_id}/replace",
            "wait": "GET /barriers/{barrier_id}/wait",
            "events": "GET /barriers/{barrier_id}/events",
        },
    }


@app.get("/health", tags=["meta"])
async def health(request: Request) -> dict[str, str]:
    await store(request).ping()
    return {"status": "ok"}


@app.post("/barriers", response_model=BarrierView, status_code=201, tags=["barriers"])
async def create_barrier(payload: CreateBarrierRequest, request: Request) -> BarrierView:
    """创建屏障。截止时间过后 BARRIER_TTL_BUFFER_MS 毫秒内数据可查询，之后自动回收。

    可选传入 participant_groups（每个参与者恰属一组）与 group_thresholds
    启用分组配额：只有总报到数达到 threshold 且各组报到数均达到对应配额才达标。
    两者必须同时提供或同时省略，分组/配额矛盾（未知成员、漏分组、非正配额、
    配额超过组人数、配额之和超过总 threshold）返回 422。
    """
    created_at = time.time()
    deadline_ts = payload.deadline_ts()

    if deadline_ts <= created_at:
        raise HTTPException(status_code=422, detail="deadline 必须是未来时间")

    threshold = (
        payload.threshold
        if payload.threshold is not None
        else len(payload.participants)
    )
    ttl_ms = int((deadline_ts - created_at) * 1000) + settings.ttl_buffer_ms

    meta = await store(request).create(
        participants=payload.participants,
        threshold=threshold,
        deadline_ts=deadline_ts,
        created_at=created_at,
        ttl_ms=ttl_ms,
        creator_id=payload.creator_id,
        participant_groups=payload.participant_groups,
        group_thresholds=payload.group_thresholds,
    )
    return await build_view(store(request), meta, created_at)


async def _load_meta(request: Request, barrier_id: str):
    meta = await store(request).get_meta(barrier_id)
    if meta is None:
        raise HTTPException(status_code=404, detail="屏障不存在或已过期回收")
    return meta


@app.get("/barriers/{barrier_id}", response_model=BarrierView, tags=["barriers"])
async def get_barrier(barrier_id: str, request: Request) -> BarrierView:
    """非阻塞查询屏障当前状态。"""
    meta = await _load_meta(request, barrier_id)
    return await build_view(store(request), meta)


@app.post(
    "/barriers/{barrier_id}/checkin",
    response_model=CheckinResponse,
    tags=["barriers"],
)
async def checkin(
    barrier_id: str, payload: CheckinRequest, request: Request
) -> CheckinResponse:
    """参与者幂等报到。

    - 重复报到返回 already_checked_in / late，不改变任何结果；
    - 并发报到首次同时满足「总达标人数 + 各组配额」时，达标成员与分组计数在
      Redis Lua 脚本内被原子冻结；跨组并发报到也只有一个请求 triggered=true；
    - 冻结之后的报到只会记录为迟到，quorum、frozen_at 与 group_progress 永不改写；
    - 屏障被取消后报到返回 409，取消终态不受任何影响。
    """
    meta = await _load_meta(request, barrier_id)
    svc = store(request)
    try:
        result = await svc.checkin(barrier_id, payload.participant_id)
    except NotAParticipant:
        raise HTTPException(
            status_code=403,
            detail=f"参与者 {payload.participant_id!r} 不在该屏障名单内",
        )
    except BarrierExpired:
        raise HTTPException(status_code=409, detail="屏障已过截止时间，报到被拒绝")
    except BarrierCancelled:
        raise HTTPException(status_code=409, detail="屏障已被取消，报到被拒绝")
    except BarrierNotFound:
        raise HTTPException(status_code=404, detail="屏障不存在或已过期回收")

    view = await build_view(svc, meta)

    return CheckinResponse(
        barrier_id=barrier_id,
        status=result.status,
        triggered=result.triggered,
        quorum=result.quorum,
        frozen_at=_dt(result.frozen_at) if result.frozen_at is not None else None,
        view=view,
    )


@app.post(
    "/barriers/{barrier_id}/batch-checkins",
    response_model=BatchCheckinResponse,
    tags=["barriers"],
)
async def batch_checkin(
    barrier_id: str, payload: BatchCheckinRequest, request: Request
) -> BatchCheckinResponse:
    """参与者批量幂等报到（名单校验、截止/取消校验、整批写入均在同一段 Redis
    Lua 脚本内一次原子完成）。

    - **幂等**：同一 ``idempotency_key`` 的重试不再次执行报到，逐字段回放
      **首次响应**（含首次执行时刻的 ``view`` 快照、``triggered`` /
      ``quorum`` / ``frozen_at`` 与逐成员结果，``replayed=true``）；同键但
      参与者集合不同返回 ``409``；
    - **整批原子**：参与者列表中任一成员不在（替补后的）名单内、或屏障已过
      截止时间、或已被取消时，**整批不产生任何报到副作用**——不写报到集合、
      不推进分组计数、不追加审计事件，也不占用幂等键，随后可用同一幂等键
      携带修正后的名单安全重试；
    - **达标冻结**：本批首次同时满足「总达标人数 + 各组配额」时，quorum 与
      分组计数在 Lua 脚本内被原子冻结；批量接口与单人报到接口共用同一冻结
      键，跨接口并发也**只有一个请求** ``triggered=true``，冻结成功后发布
      Pub/Sub 唤醒全部 /wait 等待者；
    - 已达标冻结后的批量报到全部记为迟到（``status=late``），快照永不改写；
      已取消后批量报到返回 ``409``。
    """
    await _load_meta(request, barrier_id)
    svc = store(request)
    try:
        result, replayed = await svc.batch_checkin(
            barrier_id,
            participants=payload.participants,
            idempotency_key=payload.idempotency_key,
        )
    except BatchNotParticipant as exc:
        raise HTTPException(
            status_code=403,
            detail=(
                f"参与者 {exc.args[0]!r} 不在该屏障名单内，整批报到被拒绝"
                "（未产生任何报到）"
            ),
        )
    except BarrierExpired:
        raise HTTPException(status_code=409, detail="屏障已过截止时间，整批报到被拒绝")
    except BarrierCancelled:
        raise HTTPException(status_code=409, detail="屏障已被取消，整批报到被拒绝")
    except IdempotencyConflict:
        raise HTTPException(
            status_code=409,
            detail="该幂等键已用于另一份参与者列表，不能改写首次结果",
        )
    except BarrierNotFound:
        raise HTTPException(status_code=404, detail="屏障不存在或已过期回收")

    # 重试回放使用首次执行时冻结的视图快照：即使之后又有报到 / 取消 / 截止，
    # 同幂等键也必须返回首次响应。首次执行则快照 == 执行后的实时状态。
    view = build_view_from_snapshot(barrier_id, result.snapshot)

    return BatchCheckinResponse(
        barrier_id=barrier_id,
        status=result.status,
        triggered=result.triggered,
        replayed=replayed,
        idempotency_key=result.idempotency_key,
        accepted_count=len(result.accepted),
        accepted=[
            BatchParticipantResult(
                participant_id=item.participant_id, result=item.result
            )
            for item in result.accepted
        ],
        quorum=result.quorum,
        frozen_at=_dt(result.frozen_at) if result.frozen_at is not None else None,
        view=view,
    )


@app.post(
    "/barriers/{barrier_id}/cancel",
    response_model=CancelResponse,
    tags=["barriers"],
)
async def cancel_barrier(
    barrier_id: str, payload: CancelRequest, request: Request
) -> CancelResponse:
    """主动取消屏障。

    - 取消与达标报到在 Redis 中原子竞争：先发生者成为唯一终态；
    - 取消成功后状态为 cancelled 并保存 reason / cancelled_at，此后报到返回 409；
    - 已达标或已截止后取消返回 409，任何路径都不得改写既有终态；
    - 重复取消幂等：返回首次保存的同一份 reason 与 cancelled_at；
    - 取消成功会唤醒全部已在 /wait 挂起的等待者。
    """
    meta = await _load_meta(request, barrier_id)
    svc = store(request)
    try:
        result = await svc.cancel(barrier_id, payload.reason)
    except BarrierReached:
        raise HTTPException(status_code=409, detail="屏障已达标，终态不可取消")
    except BarrierExpired:
        raise HTTPException(status_code=409, detail="屏障已过截止时间，取消被拒绝")
    except BarrierNotFound:
        raise HTTPException(status_code=404, detail="屏障不存在或已过期回收")

    view = await build_view(svc, meta)
    return CancelResponse(
        barrier_id=barrier_id,
        status=result.status,
        reason=result.reason,
        cancelled_at=_dt(result.cancelled_at),
        view=view,
    )


@app.patch(
    "/barriers/{barrier_id}/deadline",
    response_model=BarrierView,
    tags=["barriers"],
)
async def extend_deadline(
    barrier_id: str, payload: ExtendDeadlineRequest, request: Request
) -> BarrierView:
    """运行中延期：把截止时间更新为更晚的绝对时间。

    - 仅当屏障仍处于 waiting 且新截止时间**严格晚于**当前 deadline 时生效；
    - reached / cancelled / expired 均为终态，延期返回 409，终态绝不复活；
    - 终态校验、deadline 比较、deadline 写回与全部相关键 TTL 刷新在同一段
      Redis Lua 脚本内原子完成，与并发报到、取消、其他延期只有唯一一致结果；
    - 多个并发延期以 Redis 实际生效顺序为准：先写入更大 deadline 者成功，
      其余请求（新时间更早或相同，即使只是不晚于被抢先更新后的值）返回 409；
    - 成功后发布 deadline_extended 通知，已在 /wait 挂起的等待者立即回读
      最新 deadline 继续等待，不会按旧截止时间误报 expired；
    - 响应返回更新后的完整 BarrierView。
    """
    meta = await _load_meta(request, barrier_id)
    svc = store(request)

    new_deadline_ts = payload.deadline_ts()
    if new_deadline_ts <= time.time():
        raise HTTPException(status_code=422, detail="新 deadline 必须是未来时间")

    try:
        await svc.extend_deadline(
            barrier_id,
            new_deadline_ts=new_deadline_ts,
            ttl_buffer_ms=settings.ttl_buffer_ms,
        )
    except BarrierDeadlineConflict as exc:
        # Lua 内原子区分冲突原因；终态与“时间不够晚”对外同为 409 但文案不同。
        reason = exc.args[0] if exc.args else "conflict"
        detail = {
            "reached": "屏障已达标，终态不可延期",
            "cancelled": "屏障已被取消，终态不可延期",
            "expired": "屏障已过截止时间，延期被拒绝",
            "conflict": "新 deadline 必须严格晚于当前 deadline",
        }.get(reason, "延期冲突")
        raise HTTPException(status_code=409, detail=detail)
    except BarrierNotFound:
        raise HTTPException(status_code=404, detail="屏障不存在或已过期回收")

    # 重新读取 meta：deadline / ttl 已在 Lua 内写回，视图必须反映最新值。
    latest = await svc.get_meta(barrier_id)
    return await build_view(svc, latest if latest is not None else meta)


@app.post(
    "/barriers/{barrier_id}/replace",
    response_model=ReplaceResponse,
    tags=["barriers"],
)
async def replace_participant(
    barrier_id: str, payload: ReplaceRequest, request: Request
) -> ReplaceResponse:
    """运行中参与者替补：把尚未报到的原成员原子替换为名单外的新标识。

    - 仅当屏障仍处于 waiting、原参与者**在名单内且尚未报到**、新标识**不在
      当前名单内**时生效；替换在同一段 Redis Lua 脚本内完成名单改写、
      分组继承（新成员继承原成员的分组）与审计事件追加；
    - 达标 / 取消 / 截止均为终态，替补返回 409，终态绝不复活；原成员已先
      报到或已被先发生的替补换掉、新标识已在名单内也一律拒绝（409/404）；
    - 替补与报到、取消、延期及其他替补并发时，全部由 Redis 单线程串行化：
      以实际生效顺序为准，原成员的去留与新标识的加入只有唯一一致结果；
    - 成功后发布 participant_replaced 通知，已在 /wait 挂起的等待者立即
      回读替换后的名单与分组继续等待，并在同一原子操作中追加一条
      ``replaced`` 审计事件（可在 /events 中按 seq 分页查询）。
    """
    meta = await _load_meta(request, barrier_id)
    svc = store(request)
    try:
        result = await svc.substitute(
            barrier_id,
            original_participant_id=payload.participant_id,
            replacement_id=payload.replacement_id,
        )
    except ParticipantNotInRoster:
        raise HTTPException(
            status_code=404,
            detail=(
                f"参与者 {payload.participant_id!r} 不在该屏障当前名单内"
                "（可能已被替补换下）"
            ),
        )
    except AlreadyCheckedIn:
        raise HTTPException(
            status_code=409,
            detail=f"参与者 {payload.participant_id!r} 已报到，不能被替补",
        )
    except ReplacementInRoster:
        raise HTTPException(
            status_code=409,
            detail=f"替补标识 {payload.replacement_id!r} 已在名单内，必须使用名单外的新标识",
        )
    except BarrierSubstituteConflict as exc:
        reason = exc.args[0] if exc.args else "conflict"
        detail = {
            "reached": "屏障已达标，终态不可替补",
            "cancelled": "屏障已被取消，替补被拒绝",
            "expired": "屏障已过截止时间，替补被拒绝",
        }.get(reason, "替补冲突")
        raise HTTPException(status_code=409, detail=detail)
    except BarrierNotFound:
        raise HTTPException(status_code=404, detail="屏障不存在或已过期回收")

    # 名单与分组映射已在 Lua 内改写：必须重新读取 meta，视图才能反映换人结果。
    latest = await svc.get_meta(barrier_id)
    view = await build_view(svc, latest if latest is not None else meta)
    return ReplaceResponse(
        barrier_id=barrier_id,
        status="replaced",
        participant_id=result.original_participant_id,
        replacement_id=result.replacement_id,
        group=result.group,
        replaced_at=_dt(result.replaced_at),
        view=view,
    )


@app.get("/barriers/{barrier_id}/wait", response_model=BarrierView, tags=["barriers"])
async def wait_barrier(barrier_id: str, request: Request) -> BarrierView:
    """阻塞等待直到屏障达标、被取消（立即返回终态快照）或截止（返回报到成员与缺席名单）。

    多个等待者同时挂起：达标、取消、运行中延期或运行中替补时通过 Redis
    Pub/Sub 一并唤醒，轮询为兜底。收到延期通知后回读最新 deadline 继续等待，
    不会按旧截止时间误报 expired；收到替补通知后回读替换后的名单继续等待。
    最长挂起时间受 BARRIER_MAX_WAIT_S 限制。
    """
    meta = await _load_meta(request, barrier_id)
    return await wait_for_settle(
        store(request),
        meta,
        poll_interval_s=settings.poll_interval_s,
        max_wait_s=settings.max_wait_s,
    )


@app.get(
    "/barriers/{barrier_id}/events",
    response_model=BarrierEventsPage,
    tags=["barriers"],
)
async def list_events(
    barrier_id: str,
    request: Request,
    cursor: int = Query(
        default=0, ge=0, description="已读到的最大事件序号；0 表示从头开始"
    ),
    limit: int = Query(
        default=100, ge=1, le=1000, description="单页最大事件数（1-1000）"
    ),
) -> BarrierEventsPage:
    """查询审计事件（按 seq 升序，cursor/limit 分页）。

    每次名单成员报到（首次、重复、达标、迟到；含批量报到中的逐成员结果，
    每批至多一条 reached）都会在与状态变更同一 Redis 原子操作中追加一条
    事件；运行中参与者替补成功时也在换人脚本内追加一条 ``replaced`` 事件
    （``participant_id`` 为换上的新标识，``original_participant_id`` 为被
    换下的原成员）。非名单成员、截止后报到、被整批拒绝的批量报到、被拒绝
    的替补等错误请求不产生事件；批量报到的幂等重试只回放首次结果，不重复
    追加事件。事件只增不改，游标分页在并发写入下稳定不重不漏；屏障回收后
    事件一并消失。
    """
    await _load_meta(request, barrier_id)
    events, next_cursor = await store(request).list_events(barrier_id, cursor, limit)
    return BarrierEventsPage(
        barrier_id=barrier_id,
        events=[
            CheckinEvent(
                seq=e["seq"],
                participant_id=e["participant_id"],
                result=e["result"],
                ts=_dt(e["ts"]),
                original_participant_id=e.get("original_participant_id"),
            )
            for e in events
        ],
        next_cursor=next_cursor,
    )
