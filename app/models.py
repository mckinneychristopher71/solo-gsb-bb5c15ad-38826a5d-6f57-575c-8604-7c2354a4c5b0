"""Pydantic 请求 / 响应模型。"""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone

from pydantic import BaseModel, Field, model_validator


class CreateBarrierRequest(BaseModel):
    participants: list[str] = Field(
        ..., min_length=1, description="屏障参与者标识名单（如主机名、任务 id）"
    )
    threshold: int | None = Field(
        default=None,
        ge=1,
        description="达标所需报到人数；缺省为全体参与者人数",
    )
    deadline: datetime = Field(
        ...,
        description="截止时间，ISO-8601；不带时区偏移时按 UTC 处理",
    )
    creator_id: str | None = Field(default=None, description="创建者标识，可选")
    participant_groups: dict[str, str] | None = Field(
        default=None,
        description=(
            "参与者 -> 分组名的映射，每个参与者恰属一组；"
            "必须与 group_thresholds 同时提供或同时省略"
        ),
    )
    group_thresholds: dict[str, int] | None = Field(
        default=None,
        description=(
            "各分组的达标配额；启用分组配额后，只有总报到数达到 threshold 且"
            "每个分组报到数均达到对应配额才进入 reached"
        ),
    )

    @model_validator(mode="after")
    def _validate(self) -> "CreateBarrierRequest":
        # 名单去重并保持顺序；空标识无意义
        cleaned: list[str] = []
        for p in self.participants:
            name = p.strip()
            if not name:
                raise ValueError("participants 不得包含空字符串")
            if name not in cleaned:
                cleaned.append(name)
        if not cleaned:
            raise ValueError("participants 至少需要一个成员")
        self.participants = cleaned

        if self.threshold is not None and self.threshold > len(cleaned):
            raise ValueError("threshold 不能大于参与者人数")

        groups_given = self.participant_groups is not None
        quotas_given = self.group_thresholds is not None
        if groups_given != quotas_given:
            raise ValueError(
                "participant_groups 与 group_thresholds 必须同时提供或同时省略"
            )
        if not groups_given:
            return self

        # 每个参与者恰属一组：dict 天然保证一人一组，这里校验覆盖完整、无未知成员。
        roster = set(cleaned)
        group_of: dict[str, str] = {}
        for pid, raw_group in self.participant_groups.items():
            if pid not in roster:
                raise ValueError(f"participant_groups 包含名单外的参与者 {pid!r}")
            group = raw_group.strip()
            if not group:
                raise ValueError(f"参与者 {pid!r} 的分组名为空")
            group_of[pid] = group
        for pid in cleaned:
            if pid not in group_of:
                raise ValueError(f"participant_groups 缺少参与者 {pid!r} 的分组")

        # 配额清洗（分组名去空白；去重后重名视为冲突）
        cleaned_quotas: dict[str, int] = {}
        for raw_group, quota in self.group_thresholds.items():
            group = raw_group.strip()
            if not group:
                raise ValueError("group_thresholds 不得包含空分组名")
            if group in cleaned_quotas:
                raise ValueError(f"分组 {group!r} 在 group_thresholds 中重复")
            if not isinstance(quota, int) or isinstance(quota, bool) or quota <= 0:
                raise ValueError(f"分组 {group!r} 的配额必须为正整数")
            cleaned_quotas[group] = quota

        group_sizes = Counter(group_of.values())
        for group in cleaned_quotas:
            if group not in group_sizes:
                raise ValueError(
                    f"group_thresholds 包含 participant_groups 中不存在的分组 {group!r}"
                )
        for group, size in group_sizes.items():
            if group not in cleaned_quotas:
                raise ValueError(f"group_thresholds 缺少分组 {group!r} 的配额")
            quota = cleaned_quotas[group]
            if quota > size:
                raise ValueError(
                    f"分组 {group!r} 的配额 {quota} 超过该组人数 {size}"
                )

        effective_threshold = (
            self.threshold if self.threshold is not None else len(cleaned)
        )
        quota_sum = sum(cleaned_quotas.values())
        if quota_sum > effective_threshold:
            raise ValueError(
                f"各组配额之和 {quota_sum} 不能大于总达标人数 {effective_threshold}"
            )

        self.participant_groups = group_of
        self.group_thresholds = cleaned_quotas
        return self

    def deadline_ts(self) -> float:
        d = self.deadline
        if d.tzinfo is None:
            d = d.replace(tzinfo=timezone.utc)
        return d.astimezone(timezone.utc).timestamp()


class ExtendDeadlineRequest(BaseModel):
    deadline: datetime = Field(
        ...,
        description=(
            "新的绝对截止时间，ISO-8601；不带时区偏移时按 UTC 处理。"
            "必须严格晚于屏障当前 deadline，否则返回 409"
        ),
    )

    def deadline_ts(self) -> float:
        d = self.deadline
        if d.tzinfo is None:
            d = d.replace(tzinfo=timezone.utc)
        return d.astimezone(timezone.utc).timestamp()


class GroupProgress(BaseModel):
    group: str = Field(description="分组名")
    threshold: int = Field(description="该组达标配额")
    checked_count: int = Field(
        description="该组去重后的报到人数；reached 状态为达标冻结瞬间的快照"
    )
    satisfied: bool = Field(description="该组报到数是否已达到配额")


class BarrierView(BaseModel):
    barrier_id: str
    status: str = Field(description="waiting | reached | expired | cancelled")
    participants: list[str]
    threshold: int
    deadline: datetime
    created_at: datetime
    checked_in: list[str]
    # waiting 阶段的当前报到人数；其余状态为快照人数
    checked_count: int
    # 达标后冻结的成员名单（仅 reached）
    quorum: list[str] | None = None
    frozen_at: datetime | None = None
    # 截止时仍未报到的成员（仅 expired）
    missing: list[str] | None = None
    # 达标冻结之后才报到的成员（reached 状态的补充信息，不改写结果）
    late: list[str] | None = None
    # 主动取消的原因与时间（仅 cancelled）
    cancel_reason: str | None = None
    cancelled_at: datetime | None = None
    creator_id: str | None = None
    # 创建时声明的参与者 -> 分组映射（仅启用分组配额时出现）
    participant_groups: dict[str, str] | None = None
    # 分组配额进度（仅启用分组配额时出现）；未启用时为 null，行为与旧版一致
    group_progress: list[GroupProgress] | None = None


class CheckinRequest(BaseModel):
    participant_id: str = Field(..., min_length=1, description="参与者标识")


class CheckinResponse(BaseModel):
    barrier_id: str
    status: str = Field(
        description=(
            "checked_in（已报到，继续等待）| already_checked_in（幂等重复报到）"
            "| reached（本次报到触发达标）| late（达标后报到，快照未变）"
        )
    )
    triggered: bool
    quorum: list[str] | None = None
    frozen_at: datetime | None = None
    view: BarrierView


class CancelRequest(BaseModel):
    reason: str = Field(
        ...,
        min_length=1,
        description="取消原因，非空；去除首尾空白后不得为空字符串",
    )

    @model_validator(mode="after")
    def _validate(self) -> "CancelRequest":
        reason = self.reason.strip()
        if not reason:
            raise ValueError("reason 不得为空白字符串")
        self.reason = reason
        return self


class CancelResponse(BaseModel):
    barrier_id: str
    status: str = Field(
        description="cancelled（本次完成取消）| already_cancelled（重复取消，幂等返回同一结果）"
    )
    reason: str = Field(description="取消原因；重复取消时返回首次保存的原因")
    cancelled_at: datetime
    view: BarrierView


class ReplaceRequest(BaseModel):
    participant_id: str = Field(
        ..., min_length=1, description="被替换的原参与者标识（必须仍在名单内且尚未报到）"
    )
    replacement_id: str = Field(
        ..., min_length=1, description="换上的新参与者标识（必须是当前名单外的新标识）"
    )

    @model_validator(mode="after")
    def _validate(self) -> "ReplaceRequest":
        # 与创建名单一致地去除首尾空白；两个标识清洗后都不得为空、不得相同。
        original = self.participant_id.strip()
        replacement = self.replacement_id.strip()
        if not original:
            raise ValueError("participant_id 不得为空白字符串")
        if not replacement:
            raise ValueError("replacement_id 不得为空白字符串")
        if original == replacement:
            raise ValueError("replacement_id 必须是名单外的新标识，不能与原参与者相同")
        self.participant_id = original
        self.replacement_id = replacement
        return self


class ReplaceResponse(BaseModel):
    barrier_id: str
    status: str = Field(description="replaced（本次替补原子生效）")
    participant_id: str = Field(description="被换下的原参与者标识")
    replacement_id: str = Field(description="换上的新参与者标识（换人后即为现名单成员）")
    group: str | None = Field(
        default=None,
        description="新成员继承自原成员的分组名；未启用分组配额时为 null",
    )
    replaced_at: datetime = Field(description="替补生效时间（UTC）")
    view: BarrierView


class CheckinEvent(BaseModel):
    seq: int = Field(description="事件序号，从 1 开始单调递增，只追加不回改")
    participant_id: str = Field(
        description=(
            "事件相关的参与者标识：报到事件为报到者；替补事件为换上的新标识"
        )
    )
    result: str = Field(
        description=(
            "checked_in | already_checked_in | reached | late | replaced"
            "（replaced 为运行中参与者替补事件）"
        )
    )
    ts: datetime = Field(description="事件发生时间（UTC）")
    # 仅替补事件（result=replaced）出现：被换下的原参与者标识；
    # 替补事件中 participant_id 为换上的新标识。其余事件不返回该字段。
    original_participant_id: str | None = Field(
        default=None,
        description="仅替补事件出现：被换下的原参与者标识",
    )


class BarrierEventsPage(BaseModel):
    barrier_id: str
    events: list[CheckinEvent]
    next_cursor: int = Field(
        description="下一页游标（本页最大 seq）；以其为 cursor 继续查询，无新事件时与入参 cursor 相同"
    )
