"""Redis 持久层：屏障元数据、原子报到（Lua）、冻结快照、报到审计事件与事件通知。

每个屏障使用如下键（``bid`` 为屏障 id）：

``barrier:{bid}``           hash  元数据（threshold、deadline、ttl_ms 等）
``barrier:{bid}:roster``    set   当前有效参与者名单（替补原子换人后为替换后的名单）
``barrier:{bid}:checked``   set   所有有效报到（含达标后的迟到报到）
``barrier:{bid}:frozen``    hash  达标冻结快照，仅在达标后存在
                            （quorum/frozen_at/group_counts）
``barrier:{bid}:cancel``    hash  主动取消终态，仅在取消后存在（reason/cancelled_at）
``barrier:{bid}:events``    list  审计事件（JSON，seq 从 1 递增，只追加；
                            含报到事件与运行中替补事件）
``barrier:{bid}:gmap``      hash  参与者 -> 分组名（仅启用分组配额时存在）
``barrier:{bid}:gcheck``    hash  各分组去重后的报到人数（仅启用分组配额时存在）

``frozen`` 与 ``cancel`` 互斥：达标报到与主动取消由各自的 Lua 脚本原子竞争，
Redis 单线程串行执行脚本，先发生者成为唯一终态，任何路径都不得改写。
运行中延期（deadline 延长）与运行中替补（未报到成员换人）由另外两段 Lua
脚本与前两者同样地原子竞争：终态键（frozen/cancel）或已过截止的屏障一律
拒绝延期与替补，绝不复活终态。

频道 ``barrier:{bid}:events`` 在达标冻结后发布 ``reached`` 消息、
在主动取消后发布 ``cancelled`` 消息、在成功延期后发布 ``deadline_extended``
消息（通知挂起中的 /wait 回读最新 deadline 继续等待）、在成功替补后发布
``participant_replaced`` 消息（通知挂起中的 /wait 回读替换后的名单）。
"""

from __future__ import annotations

import json
import time
import uuid
from dataclasses import dataclass
from typing import Any

from redis.asyncio import Redis

# 报到脚本。返回数组：{状态, 是否新加入(0/1), quorum JSON, frozen_at 字符串}
#
# 状态取值：
#   unknown             屏障不存在
#   not_participant     报到者不在名单内
#   late                屏障达标后报到，快照不变
#   cancelled           屏障已被主动取消（与取消脚本原子竞争的败者）
#   expired             已过截止时间
#   reached             本次报到使屏障达标，已原子写入冻结快照
#   checked_in          报到成功，屏障仍在等待
#   already_checked_in  幂等重复报到（未达标前）
#
# 启用分组配额时，SADD 成功（去重后的新报到）才会同步 HINCRBY 对应分组计数；
# 重复 / 迟到报到都不重复累计。达标条件为「总报到数 >= threshold 且
# 每个分组计数 >= 该组配额」，满足时在同一脚本内冻结 quorum 与分组计数快照。
#
# 四种成功状态（reached/checked_in/already_checked_in/late）都会在同一脚本内
# 向 events 列表追加一条审计事件（seq 递增）；错误路径不产生任何事件。
CHECKIN_SCRIPT = """
local meta_key    = KEYS[1]
local roster_key  = KEYS[2]
local checked_key = KEYS[3]
local frozen_key  = KEYS[4]
local events_key  = KEYS[5]
local cancel_key  = KEYS[6]
local gmap_key    = KEYS[7]
local gcheck_key  = KEYS[8]
local pid         = ARGV[1]
local now         = tonumber(ARGV[2])

local function refresh_ttl()
    local ttl_ms = tonumber(redis.call('HGET', meta_key, 'ttl_ms'))
    if ttl_ms then
        redis.call('PEXPIRE', meta_key, ttl_ms)
        redis.call('PEXPIRE', roster_key, ttl_ms)
        redis.call('PEXPIRE', checked_key, ttl_ms)
        redis.call('PEXPIRE', frozen_key, ttl_ms)
        redis.call('PEXPIRE', events_key, ttl_ms)
        redis.call('PEXPIRE', cancel_key, ttl_ms)
        redis.call('PEXPIRE', gmap_key, ttl_ms)
        redis.call('PEXPIRE', gcheck_key, ttl_ms)
    end
end

-- 审计事件：与状态变更处于同一原子脚本，seq 自 1 递增（下标 = seq - 1）。
local function record_event(result)
    local seq = redis.call('LLEN', events_key) + 1
    redis.call('RPUSH', events_key, cjson.encode({
        seq = seq,
        participant_id = pid,
        result = result,
        ts = now
    }))
end

-- 分组配额是否全部满足：gcheck 中的每个分组计数均不低于 meta 里登记的配额。
-- 配额表由创建校验保证不大于组人数，因此只有真正报到才能把计数推过配额。
local function group_quotas_satisfied()
    local raw = redis.call('HGET', meta_key, 'group_thresholds')
    if not raw or raw == '' then
        return true
    end
    local thresholds = cjson.decode(raw)
    for group, need in pairs(thresholds) do
        if tonumber(redis.call('HGET', gcheck_key, group) or '0') < tonumber(need) then
            return false
        end
    end
    return true
end

-- 冻结快照：quorum 成员名单 + 冻结瞬间的分组计数，一次原子写定，永不改写。
local function freeze_quorum(now_ts)
    local members = redis.call('SMEMBERS', checked_key)
    table.sort(members)
    local counts = redis.call('HGETALL', gcheck_key)
    local counts_map = {}
    for i = 1, #counts, 2 do
        counts_map[counts[i]] = tonumber(counts[i + 1])
    end
    redis.call('HSET', frozen_key,
        'quorum', cjson.encode(members),
        'frozen_at', tostring(now_ts),
        'group_counts', cjson.encode(counts_map))
    return cjson.encode(members)
end

if redis.call('HEXISTS', meta_key, 'threshold') == 0 then
    return {'unknown', '0', '', '0'}
end

-- 名单校验优先于一切状态：非名单成员永远无法报到，也不能获得冻结快照。
if redis.call('SISMEMBER', roster_key, pid) == 0 then
    return {'not_participant', '0', '', '0'}
end

-- 达标冻结之后：冻结键已存在，任何后续报到都不得改写快照。
local quorum = redis.call('HGET', frozen_key, 'quorum')
if quorum then
    -- 仅记录迟到事实，总数 / 分组计数 / quorum / frozen_at 均保持不变。
    redis.call('SADD', checked_key, pid)
    record_event('late')
    refresh_ttl()
    return {'late', '0', quorum, redis.call('HGET', frozen_key, 'frozen_at')}
end

-- 主动取消之后：取消键已存在，报到不得再写入任何状态（连 checked 也不追加）。
-- cancelled 与 reached 同为终态：若能走到这里说明取消脚本先于本脚本提交。
if redis.call('HEXISTS', cancel_key, 'cancelled_at') == 1 then
    return {'cancelled', '0', '', '0'}
end

local deadline = tonumber(redis.call('HGET', meta_key, 'deadline'))
if now >= deadline then
    return {'expired', '0', '', '0'}
end

-- 去重后的唯一报到才累计总数与分组计数；重复报到（newly_added == 0）
-- 只追加审计事件，任何计数都不变。
local newly_added = redis.call('SADD', checked_key, pid)
if newly_added == 1 then
    local group = redis.call('HGET', gmap_key, pid)
    if group then
        redis.call('HINCRBY', gcheck_key, group, 1)
    end
end
local count = redis.call('SCARD', checked_key)
local threshold = tonumber(redis.call('HGET', meta_key, 'threshold'))

-- 首次同时满足「总数达标 + 各组配额达标」：在同一脚本内冻结成员名单与
-- 分组计数，杜绝并发覆盖（跨组并发报到也只有一个请求进入此分支）。
if count >= threshold and group_quotas_satisfied() then
    local payload = freeze_quorum(now)
    record_event('reached')
    refresh_ttl()
    return {'reached', tostring(newly_added), payload, tostring(now)}
end

if newly_added == 1 then
    record_event('checked_in')
    refresh_ttl()
    return {'checked_in', '0', '', '0'}
end
record_event('already_checked_in')
refresh_ttl()
return {'already_checked_in', '0', '', '0'}
"""


# 取消脚本。返回数组：
#   {'unknown'}                          屏障不存在
#   {'reached', quorum, frozen_at}       已达标，终态不可改写，取消被拒绝
#   {'expired'}                          已过截止时间，取消被拒绝
#   {'cancelled', reason, cancelled_at}  本次请求完成取消（唯一一次写入）
#   {'already_cancelled', reason, ts}    重复取消，幂等返回既有取消结果
#
# 与报到脚本的竞争完全由 Redis 单线程串行化保证：本脚本检查 frozen/cancel 与
# 写入 cancel 键之间不可能插入达标报到；反之报到脚本也会先看到 cancel 键。
CANCEL_SCRIPT = """
local meta_key   = KEYS[1]
local frozen_key = KEYS[4]
local cancel_key = KEYS[6]
local reason     = ARGV[1]
local now        = tonumber(ARGV[2])

if redis.call('HEXISTS', meta_key, 'threshold') == 0 then
    return {'unknown'}
end

-- 既有终态一律不得改写：已取消则幂等返回原结果；已达标则拒绝。
local cancelled_at = redis.call('HGET', cancel_key, 'cancelled_at')
if cancelled_at then
    return {'already_cancelled', redis.call('HGET', cancel_key, 'reason'), cancelled_at}
end

local quorum = redis.call('HGET', frozen_key, 'quorum')
if quorum then
    return {'reached', quorum, redis.call('HGET', frozen_key, 'frozen_at')}
end

-- 已截止也是终态（expired），取消不得覆盖。
local deadline = tonumber(redis.call('HGET', meta_key, 'deadline'))
if now >= deadline then
    return {'expired'}
end

local now_str = tostring(now)
redis.call('HSET', cancel_key, 'reason', reason, 'cancelled_at', now_str)

local ttl_ms = tonumber(redis.call('HGET', meta_key, 'ttl_ms'))
if ttl_ms then
    redis.call('PEXPIRE', cancel_key, ttl_ms)
end
return {'cancelled', reason, now_str}
"""


# 运行中延期脚本。返回数组：
#   {'unknown'}                                        屏障不存在
#   {'reached'}                                        已达标，终态不可延期
#   {'cancelled'}                                      已取消，终态不可延期
#   {'expired'}                                        已过截止时间，不可延期/复活
#   {'conflict', old_deadline}                         新截止时间不晚于当前值，
#                                                      含并发延期已先写入更大值
#   {'extended', old_deadline, new_deadline, ttl_ms}   本次延期成功
#
# 与报到 / 取消脚本一样在 Redis 单线程内原子执行：frozen/cancel 存在性检查、
# 截止检查、deadline 大小比较与写入之间不可能插入任何其他脚本，因此延期与
# 并发报到、取消、延期之间只有唯一一致结果。成功时在同一脚本内将新的
# deadline 写回 meta，并重算 ttl_ms、刷新所有相关键的存活期：新 ttl 锚定
# 「当前时刻 + (新 deadline - now) + 缓冲」，保证任何键都不会在新 deadline
# 之前被回收，且与报到脚本 refresh_ttl 的再锚定语义保持一致。
EXTEND_SCRIPT = """
local meta_key    = KEYS[1]
local roster_key  = KEYS[2]
local checked_key = KEYS[3]
local frozen_key  = KEYS[4]
local events_key  = KEYS[5]
local cancel_key  = KEYS[6]
local gmap_key    = KEYS[7]
local gcheck_key  = KEYS[8]
local new_deadline = tonumber(ARGV[1])
local now          = tonumber(ARGV[2])
local buffer_ms    = tonumber(ARGV[3])

if redis.call('HEXISTS', meta_key, 'threshold') == 0 then
    return {'unknown'}
end

-- 终态一：达标冻结快照已存在，延期不得触碰任何终态。
if redis.call('HEXISTS', frozen_key, 'quorum') == 1 then
    return {'reached'}
end

-- 终态二：取消快照已存在，终态不可复活。
if redis.call('HEXISTS', cancel_key, 'cancelled_at') == 1 then
    return {'cancelled'}
end

local deadline = tonumber(redis.call('HGET', meta_key, 'deadline'))

-- 终态三：已过当前截止时间（expired 不落键，靠时间判定），同样不得复活。
if now >= deadline then
    return {'expired'}
end

-- 只允许向后延期；相等或更早一律冲突。并发延期时 Redis 串行执行：
-- 先写入更大 deadline 的请求成功，其余请求读到的已是更新后的 deadline。
if new_deadline <= deadline then
    return {'conflict', tostring(deadline)}
end

-- 重算存活期：保证新 deadline + buffer 之前所有键都存活。ttl_ms 同步写回
-- meta，使报到脚本之后的 refresh_ttl 也锚定到同一条绝对过期时间线。
local ttl_ms = math.max(0, math.ceil((new_deadline - now) * 1000)) + buffer_ms
redis.call('HSET', meta_key, 'deadline', tostring(new_deadline), 'ttl_ms', ttl_ms)

redis.call('PEXPIRE', meta_key, ttl_ms)
redis.call('PEXPIRE', roster_key, ttl_ms)
redis.call('PEXPIRE', checked_key, ttl_ms)
redis.call('PEXPIRE', frozen_key, ttl_ms)
redis.call('PEXPIRE', events_key, ttl_ms)
redis.call('PEXPIRE', cancel_key, ttl_ms)
redis.call('PEXPIRE', gmap_key, ttl_ms)
redis.call('PEXPIRE', gcheck_key, ttl_ms)

return {'extended', tostring(deadline), tostring(new_deadline), tostring(ttl_ms)}
"""


# 运行中参与者替补脚本。返回数组：
#   {'unknown'}                                   屏障不存在
#   {'replaced_before', original}                 原成员已不在名单
#                                                 （已被先执行的替补换掉）
#   {'already_checked_in', original}              原成员已报到，不可被替换
#   {'replacement_in_roster', replacement}        新标识已在（替换后的）名单内
#   {'reached'}                                   已达标，终态不可替补
#   {'cancelled'}                                 已取消，终态不可替补
#   {'expired'}                                   已过截止时间，替补被拒绝
#   {'replaced', original, replacement, group, ts} 本次替补成功（唯一一次换人；
#                                                 group 为空串表示未启用分组）
#
# 与报到 / 取消 / 延期脚本一样在 Redis 单线程内原子执行：原成员「在名单内且
# 尚未报到」的校验、新标识「名单外」的校验、终态（frozen/cancel/截止）校验、
# roster SREM + SADD、gmap 分组继承（HDEL 原成员 + HSET 新成员）与审计事件
# 追加之间不可能插入任何其他脚本。因此替补与并发报到、取消、延期、其他替补
# 以 Redis 实际生效顺序为准，必有且仅有一个赢家：原成员在替补生效前先报到，
# 或另一个替补先把同一原成员换掉，本次替补都会被原子拒绝。
SUBSTITUTE_SCRIPT = """
local meta_key      = KEYS[1]
local roster_key    = KEYS[2]
local checked_key   = KEYS[3]
local frozen_key    = KEYS[4]
local events_key    = KEYS[5]
local cancel_key    = KEYS[6]
local gmap_key      = KEYS[7]
local gcheck_key    = KEYS[8]
local original      = ARGV[1]
local replacement   = ARGV[2]
local now           = tonumber(ARGV[3])

if redis.call('HEXISTS', meta_key, 'threshold') == 0 then
    return {'unknown'}
end

-- 原成员必须仍在（替补后已被更新的）名单内：名单校验优先于一切状态，与
-- 报到脚本一致。两个并发替补竞争同一原成员时，先执行者把其 SREM，
-- 后执行者在此被原子拒绝。
if redis.call('SISMEMBER', roster_key, original) == 0 then
    return {'replaced_before', original}
end

-- 原成员尚未报到才允许替补：与并发报到竞争时，先报到者 SADD checked，
-- 替补在此被原子拒绝；反之替补先 SREM roster，报到脚本的名单校验随即失败。
if redis.call('SISMEMBER', checked_key, original) == 1 then
    return {'already_checked_in', original}
end

-- 新标识必须是名单外标识（也不允许等于另一名现有成员）：并发替补/报到下，
-- roster 是唯一的事实来源，Redis 串行执行保证该校验不会过期。
if redis.call('SISMEMBER', roster_key, replacement) == 1 then
    return {'replacement_in_roster', replacement}
end

-- 终态一：达标冻结快照已存在，替补不得触碰终态（quorum 永不改写）。
if redis.call('HEXISTS', frozen_key, 'quorum') == 1 then
    return {'reached'}
end

-- 终态二：取消快照已存在，终态不可复活。
if redis.call('HEXISTS', cancel_key, 'cancelled_at') == 1 then
    return {'cancelled'}
end

-- 终态三：已过当前截止时间（expired 不落键，靠时间判定），同样不得替补。
local deadline = tonumber(redis.call('HGET', meta_key, 'deadline'))
if now >= deadline then
    return {'expired'}
end

-- 继承原分组：gmap 中原成员 -> 分组改写到新标识；未启用分组配额时 gmap 不
-- 存在，HGET/HDEL/HSET 均为空操作。gcheck 是报到计数，原成员未报到，计数
-- 不因换人改变，无需触碰。
local group = redis.call('HGET', gmap_key, original)
redis.call('SREM', roster_key, original)
redis.call('SADD', roster_key, replacement)
if group then
    redis.call('HDEL', gmap_key, original)
    redis.call('HSET', gmap_key, replacement, group)
end

-- 审计事件与换人处于同一原子脚本，seq 自 1 递增（下标 = seq - 1）。
-- 仅记录成功的替补；冲突路径不产生任何事件。
local seq = redis.call('LLEN', events_key) + 1
redis.call('RPUSH', events_key, cjson.encode({
    seq = seq,
    participant_id = replacement,
    original_participant_id = original,
    result = 'replaced',
    ts = now
}))

-- 与报到脚本一致地把全部相关键的存活期再锚定到 meta 中的 ttl_ms。
local ttl_ms = tonumber(redis.call('HGET', meta_key, 'ttl_ms'))
if ttl_ms then
    redis.call('PEXPIRE', meta_key, ttl_ms)
    redis.call('PEXPIRE', roster_key, ttl_ms)
    redis.call('PEXPIRE', checked_key, ttl_ms)
    redis.call('PEXPIRE', frozen_key, ttl_ms)
    redis.call('PEXPIRE', events_key, ttl_ms)
    redis.call('PEXPIRE', cancel_key, ttl_ms)
    redis.call('PEXPIRE', gmap_key, ttl_ms)
    redis.call('PEXPIRE', gcheck_key, ttl_ms)
end

return {'replaced', original, replacement, tostring(group or ''), tostring(now)}
"""


class BarrierNotFound(Exception):
    """屏障不存在或已过期清理。"""


class NotAParticipant(Exception):
    """报到者不在屏障参与者名单内。"""


class BarrierExpired(Exception):
    """屏障已过截止时间且未达标。"""


class BarrierReached(Exception):
    """屏障已达标冻结，终态不可改写（取消被拒绝）。"""


class BarrierCancelled(Exception):
    """屏障已被主动取消，后续报到被拒绝。"""


class BarrierDeadlineConflict(Exception):
    """延期被拒绝：屏障已进入终态，或新截止时间不晚于当前截止时间。"""


class ParticipantNotInRoster(Exception):
    """替补的原成员不在名单内（屏障从未声明过，或已被先发生的替补换掉）。"""


class AlreadyCheckedIn(Exception):
    """替补被拒绝：原成员已经报到，不可再被替换。"""


class ReplacementInRoster(Exception):
    """替补被拒绝：新标识已在名单内，必须是名单外的新标识。"""


class BarrierSubstituteConflict(Exception):
    """替补被拒绝：屏障已进入终态（reached/cancelled/expired）。"""


@dataclass(frozen=True)
class BarrierMeta:
    barrier_id: str
    participants: list[str]
    threshold: int
    deadline: float  # Unix 秒
    created_at: float  # Unix 秒
    ttl_ms: int
    creator_id: str | None
    # 参与者 -> 分组名；空字典 / None 表示未启用分组配额
    participant_groups: dict[str, str]
    # 分组名 -> 配额；空字典表示未启用分组配额
    group_thresholds: dict[str, int]

    @property
    def groups_enabled(self) -> bool:
        return bool(self.group_thresholds)


@dataclass(frozen=True)
class CheckinResult:
    status: str
    newly_added: bool
    quorum: list[str] | None
    frozen_at: float | None

    @property
    def triggered(self) -> bool:
        return self.status == "reached"

    @property
    def reached(self) -> bool:
        """屏障是否处于已达标状态（含本次触发与达标后报到）。"""
        return self.status in ("reached", "late")


@dataclass(frozen=True)
class CancelResult:
    # cancelled（本次完成取消）| already_cancelled（重复取消，幂等）
    status: str
    reason: str
    cancelled_at: float

    @property
    def idempotent(self) -> bool:
        return self.status == "already_cancelled"


@dataclass(frozen=True)
class ExtendResult:
    old_deadline: float  # 延期前的截止时间（Unix 秒）
    new_deadline: float  # 本次写入的截止时间（Unix 秒）
    ttl_ms: int


@dataclass(frozen=True)
class SubstituteResult:
    original_participant_id: str  # 被换下的原名单成员
    replacement_id: str  # 换上的名单外新标识（换人后即为现名单成员）
    group: str | None  # 继承自原成员的分组；未启用分组配额时为 None
    replaced_at: float


class BarrierStore:
    def __init__(self, redis: Redis) -> None:
        self.redis = redis
        self._checkin = redis.register_script(CHECKIN_SCRIPT)
        self._cancel = redis.register_script(CANCEL_SCRIPT)
        self._extend = redis.register_script(EXTEND_SCRIPT)
        self._substitute = redis.register_script(SUBSTITUTE_SCRIPT)

    @staticmethod
    def channel(barrier_id: str) -> str:
        return f"barrier:{barrier_id}:events"

    def _keys(self, barrier_id: str) -> tuple[str, ...]:
        return (
            f"barrier:{barrier_id}",
            f"barrier:{barrier_id}:roster",
            f"barrier:{barrier_id}:checked",
            f"barrier:{barrier_id}:frozen",
            f"barrier:{barrier_id}:events",
            f"barrier:{barrier_id}:cancel",
            f"barrier:{barrier_id}:gmap",
            f"barrier:{barrier_id}:gcheck",
        )

    async def create(
        self,
        participants: list[str],
        threshold: int,
        deadline_ts: float,
        created_at: float,
        ttl_ms: int,
        creator_id: str | None = None,
        participant_groups: dict[str, str] | None = None,
        group_thresholds: dict[str, int] | None = None,
    ) -> BarrierMeta:
        barrier_id = uuid.uuid4().hex
        (
            meta_key,
            roster_key,
            _,
            _,
            _,
            _,
            gmap_key,
            gcheck_key,
        ) = self._keys(barrier_id)
        groups = participant_groups or {}
        quotas = group_thresholds or {}
        mapping: dict[str, Any] = {
            "threshold": threshold,
            "deadline": f"{deadline_ts:.3f}",
            "created_at": f"{created_at:.3f}",
            "ttl_ms": ttl_ms,
        }
        if creator_id:
            mapping["creator_id"] = creator_id
        if quotas:
            mapping["group_thresholds"] = json.dumps(quotas, ensure_ascii=False)

        async with self.redis.pipeline(transaction=True) as pipe:
            pipe.hset(meta_key, mapping=mapping)
            pipe.sadd(roster_key, *participants)
            pipe.pexpire(meta_key, ttl_ms)
            pipe.pexpire(roster_key, ttl_ms)
            if groups:
                pipe.hset(gmap_key, mapping=groups)
                pipe.pexpire(gmap_key, ttl_ms)
                # gcheck 计数键提前建立并设置 TTL：各分组从 0 开始，由报到脚本
                # HINCRBY 累加；即使全程无人报到也会随屏障一起被回收。
                pipe.hset(gcheck_key, mapping={g: 0 for g in quotas})
                pipe.pexpire(gcheck_key, ttl_ms)
            await pipe.execute()

        return BarrierMeta(
            barrier_id=barrier_id,
            participants=sorted(participants),
            threshold=threshold,
            deadline=deadline_ts,
            created_at=created_at,
            ttl_ms=ttl_ms,
            creator_id=creator_id,
            participant_groups=dict(groups),
            group_thresholds=dict(quotas),
        )

    async def get_meta(self, barrier_id: str) -> BarrierMeta | None:
        meta_key, roster_key, _, _, _, _, gmap_key, _ = self._keys(barrier_id)
        data = await self.redis.hgetall(meta_key)
        if not data:
            return None
        participants = sorted(await self.redis.smembers(roster_key))
        group_of: dict[str, str] = {}
        if "group_thresholds" in data:
            group_of = await self.redis.hgetall(gmap_key)
        return BarrierMeta(
            barrier_id=barrier_id,
            participants=participants,
            threshold=int(data["threshold"]),
            deadline=float(data["deadline"]),
            created_at=float(data["created_at"]),
            ttl_ms=int(data["ttl_ms"]),
            creator_id=data.get("creator_id"),
            participant_groups=group_of,
            group_thresholds=(
                json.loads(data["group_thresholds"])
                if "group_thresholds" in data
                else {}
            ),
        )

    async def checkin(self, barrier_id: str, participant_id: str) -> CheckinResult:
        """参与者幂等报到；达标判定（含分组配额）与冻结在 Lua 中原子完成。"""
        keys = self._keys(barrier_id)
        raw = await self._checkin(
            keys=list(keys), args=[participant_id, f"{time.time():.3f}"]
        )
        status, newly, quorum_json, frozen_at = raw

        if status == "unknown":
            raise BarrierNotFound(barrier_id)
        if status == "not_participant":
            raise NotAParticipant(participant_id)
        if status == "expired":
            raise BarrierExpired(barrier_id)
        if status == "cancelled":
            raise BarrierCancelled(barrier_id)

        result = CheckinResult(
            status=status,
            newly_added=newly == "1",
            quorum=json.loads(quorum_json) if quorum_json else None,
            frozen_at=float(frozen_at) if frozen_at not in ("", "0") else None,
        )

        # 冻结发生在 Lua 内部；脚本返回后立刻发布唤醒事件。
        # 即使发布丢失，等待方也有轮询兜底，不会睡死。
        if status == "reached":
            await self.redis.publish(self.channel(barrier_id), "reached")

        return result

    async def cancel(self, barrier_id: str, reason: str) -> CancelResult:
        """主动取消；与达标报到原子竞争，先发生者成为唯一终态。

        已取消时幂等返回同一取消结果；已达标 / 已截止抛出终态冲突异常。
        """
        keys = self._keys(barrier_id)
        raw = await self._cancel(
            keys=list(keys), args=[reason, f"{time.time():.3f}"]
        )
        status = raw[0]

        if status == "unknown":
            raise BarrierNotFound(barrier_id)
        if status == "reached":
            raise BarrierReached(barrier_id)
        if status == "expired":
            raise BarrierExpired(barrier_id)

        result = CancelResult(
            status=status,
            reason=raw[1],
            cancelled_at=float(raw[2]),
        )

        # 仅本次真正完成取消时发布唤醒；重复取消不重复广播。
        if status == "cancelled":
            await self.redis.publish(self.channel(barrier_id), "cancelled")

        return result

    async def extend_deadline(
        self,
        barrier_id: str,
        new_deadline_ts: float,
        ttl_buffer_ms: int,
        now: float | None = None,
    ) -> ExtendResult:
        """运行中延期：原子校验终态与时间先后，原子更新 deadline 与全部键 TTL。

        只允许 waiting 且新截止时间严格晚于当前 deadline 的请求成功；
        reached / cancelled / expired 与「不晚于当前值」（含并发延期已抢先
        写入更大值）均抛出 :class:`BarrierDeadlineConflict`；屏障不存在则
        抛出 :class:`BarrierNotFound`。成功后发布 ``deadline_extended``，
        唤醒 /wait 等待者回读最新 deadline，终态绝不会被复活。
        """
        keys = self._keys(barrier_id)
        raw = await self._extend(
            keys=list(keys),
            args=[
                f"{new_deadline_ts:.3f}",
                f"{(now if now is not None else time.time()):.3f}",
                int(ttl_buffer_ms),
            ],
        )
        status = raw[0]

        if status == "unknown":
            raise BarrierNotFound(barrier_id)
        if status in ("reached", "cancelled", "expired", "conflict"):
            raise BarrierDeadlineConflict(status)

        result = ExtendResult(
            old_deadline=float(raw[1]),
            new_deadline=float(raw[2]),
            ttl_ms=int(raw[3]),
        )

        # 仅真正完成延期时广播；等待者收到后回读 meta 中的新 deadline。
        # 发布丢失也无碍：等待者的轮询兜底同样会回读最新 deadline。
        await self.redis.publish(self.channel(barrier_id), "deadline_extended")
        return result

    async def substitute(
        self,
        barrier_id: str,
        original_participant_id: str,
        replacement_id: str,
        now: float | None = None,
    ) -> SubstituteResult:
        """运行中参与者替补：原子地把尚未报到的原成员换成名单外的新标识。

        仅当屏障仍处于 waiting、原成员在名单内且尚未报到、新标识不在名单内时
        成功；换人在同一段 Lua 脚本内完成 roster 原子改写、gmap 分组继承与
        审计事件追加。与并发报到、取消、延期、其他替补的竞争完全由 Redis
        单线程串行化决定，以实际生效顺序为唯一结果：

        - 原成员已不在名单（已被先发生的替补换掉）→ :class:`ParticipantNotInRoster`；
        - 原成员已先报到（或其报到恰好先于本脚本生效）→ :class:`AlreadyCheckedIn`；
        - 新标识已在名单内（含被其他替补先换入）→ :class:`ReplacementInRoster`；
        - reached / cancelled / expired → :class:`BarrierSubstituteConflict`；
        - 屏障不存在 → :class:`BarrierNotFound`。

        成功后发布 ``participant_replaced`` 唤醒 /wait 等待者回读替换后的名单。
        """
        keys = self._keys(barrier_id)
        raw = await self._substitute(
            keys=list(keys),
            args=[
                original_participant_id,
                replacement_id,
                f"{(now if now is not None else time.time()):.3f}",
            ],
        )
        status = raw[0]

        if status == "unknown":
            raise BarrierNotFound(barrier_id)
        if status == "replaced_before":
            raise ParticipantNotInRoster(original_participant_id)
        if status == "already_checked_in":
            raise AlreadyCheckedIn(original_participant_id)
        if status == "replacement_in_roster":
            raise ReplacementInRoster(replacement_id)
        if status in ("reached", "cancelled", "expired"):
            raise BarrierSubstituteConflict(status)

        result = SubstituteResult(
            original_participant_id=raw[1],
            replacement_id=raw[2],
            group=raw[3] or None,
            replaced_at=float(raw[4]),
        )

        # 仅真正完成换人时广播；等待者收到后回读最新 roster 与分组映射。
        # 发布丢失也无碍：等待者的轮询兜底同样会回读最新状态。
        await self.redis.publish(self.channel(barrier_id), "participant_replaced")
        return result

    async def get_frozen(self, barrier_id: str) -> dict[str, Any] | None:
        _, _, _, frozen_key, _, _, _, _ = self._keys(barrier_id)
        data = await self.redis.hgetall(frozen_key)
        if not data:
            return None
        frozen: dict[str, Any] = {
            "quorum": json.loads(data["quorum"]),
            "frozen_at": float(data["frozen_at"]),
        }
        # 旧快照或未启用分组配额时为空映射；视图据此不再补算分组进度。
        raw_counts = data.get("group_counts")
        frozen["group_counts"] = json.loads(raw_counts) if raw_counts else {}
        return frozen

    async def get_cancelled(self, barrier_id: str) -> dict[str, Any] | None:
        _, _, _, _, _, cancel_key, _, _ = self._keys(barrier_id)
        data = await self.redis.hgetall(cancel_key)
        if not data:
            return None
        return {
            "reason": data["reason"],
            "cancelled_at": float(data["cancelled_at"]),
        }

    async def checked_members(self, barrier_id: str) -> set[str]:
        _, _, checked_key, _, _, _, _, _ = self._keys(barrier_id)
        return await self.redis.smembers(checked_key)

    async def group_check_counts(self, barrier_id: str) -> dict[str, int]:
        """读取各分组当前去重报到人数；未启用分组配额时为空字典。"""
        _, _, _, _, _, _, _, gcheck_key = self._keys(barrier_id)
        raw = await self.redis.hgetall(gcheck_key)
        return {group: int(count) for group, count in raw.items()}

    async def list_events(
        self, barrier_id: str, cursor: int, limit: int
    ) -> tuple[list[dict[str, Any]], int]:
        """按游标读取报到审计事件：返回 seq > cursor 的至多 limit 条及新游标。

        事件只追加且 seq 与列表下标一一对应（下标 = seq - 1），并发报到
        只会让游标右侧变长，已读区间稳定，因此游标分页不重不漏。
        """
        _, _, _, _, events_key, _, _, _ = self._keys(barrier_id)
        raw = await self.redis.lrange(events_key, cursor, cursor + limit - 1)
        events = [json.loads(item) for item in raw]
        next_cursor = events[-1]["seq"] if events else cursor
        return events, next_cursor

    async def ping(self) -> None:
        await self.redis.ping()

    async def aclose(self) -> None:
        await self.redis.aclose()
