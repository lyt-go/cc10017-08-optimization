"""Schema演进业务规则：状态流转、字段校验与筛选口径都收在这里。

增强：基于任务恢复索引数据表核对候选检查点，校验检查点对应的字段版本，
确认恢复入口不再从最早检查点扫描全部状态。
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from app.store import store

MODULE = "schema_evolve"
LIST_FIELDS = ["规则编号", "关联主题", "新增字段", "删除字段", "类型变更", "默认值填充", "兼容模式", "生效版本"]
REQUIRED_FIELDS = ["规则编号", "关联主题", "新增字段"]
STATUS_ORDER = ["草稿", "待校验", "已生效", "已回滚"]
ACTION_RULES = {"提交校验": "待校验", "标记生效": "已生效", "执行回滚": "已回滚"}
NEGATIVE_ACTIONS = []


class SchemaEvolveService:
    def list_entries(
        self,
        *,
        filters: dict[str, str] | None = None,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        rows = store.rows(MODULE)
        for field, expected in (filters or {}).items():
            rows = [row for row in rows if expected.casefold() in str(row.get(field, "")).casefold()]
        if keyword:
            rows = [row for row in rows if keyword.casefold() in str(row.get("规则编号", "")).casefold()]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        total = len(rows)
        start = (page - 1) * size
        return rows[start:start + size], total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        return store.find(MODULE, entry_id)

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [field for field in REQUIRED_FIELDS if not str(values.get(field) or "").strip()]
        if missing:
            return None, missing
        rows = store.rows(MODULE)
        entry = {"id": max((int(row.get("id", 0)) for row in rows), default=0) + 1}
        entry.update({field: values.get(field) for field in LIST_FIELDS})
        entry["status"] = STATUS_ORDER[0]
        entry["pending"] = True
        entry["abnormal"] = False
        rows.append(entry)
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"演进规则 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于Schema演进可执行范围"
        target = ACTION_RULES[action]
        if target not in STATUS_ORDER:
            return None, f"目标状态「{target}」不在允许的状态序列里"
        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        return entry, f"演进规则已{action}"

    def verify_checkpoint_candidates(self, entry_id: int) -> tuple[dict[str, Any] | None, str]:
        """核对候选检查点：跨模块接收任务恢复索引数据表，校验检查点字段版本。

        确认恢复入口按索引的下一检查点启动，不再扫描最早状态；核对结果写入
        schema_checkpoint_verifications 产物表。
        """
        rule = store.find(MODULE, entry_id)
        if rule is None:
            return None, f"演进规则 {entry_id} 不存在或已归档"
        topic_code = rule.get("关联主题")
        job = next((r for r in store.rows("flink_job") if r.get("消费主题") == topic_code), None)
        if job is None:
            return None, f"关联主题 {topic_code} 未配置流式任务，无法核对候选检查点"
        job_code = str(job.get("任务编号"))

        # 跨模块接收任务恢复索引数据表（来源版本由流式计算保留）
        indexes = [r for r in store.rows("recovery_index") if str(r.get("任务编号")) == job_code]
        if not indexes:
            return None, f"任务 {job_code} 尚未生成恢复索引，请先在流式计算页生成"

        checkpoints = [c for c in store.rows("flink_checkpoints")
                       if str(c.get("任务编号")) == job_code]
        earliest = min(checkpoints, key=lambda c: str(c.get("检查点时间", "")), default=None)
        target_version = str(rule.get("生效版本", ""))

        checks: list[dict[str, Any]] = []
        for idx in indexes:
            next_ckpt = str(idx.get("下一检查点", ""))
            ckpt = next((c for c in checkpoints if str(c.get("检查点编号")) == next_ckpt), None)
            field_version = str(ckpt.get("结构版本")) if ckpt else "-"
            version_ok = ckpt is not None and field_version == target_version \
                and str(idx.get("任务版本")) == target_version
            no_earliest_scan = earliest is not None and next_ckpt != str(earliest.get("检查点编号"))
            checks.append({
                "索引编号": idx.get("索引编号"),
                "任务编号": job_code,
                "任务版本": idx.get("任务版本"),
                "下一检查点": next_ckpt,
                "检查点字段版本": field_version,
                "规则生效版本": target_version,
                "字段版本校验": "通过" if version_ok else "不通过",
                "不再扫描最早状态": "是" if no_earliest_scan else "否",
            })

        passed = bool(checks) and all(
            c["字段版本校验"] == "通过" and c["不再扫描最早状态"] == "是" for c in checks)
        verification = {
            "规则编号": rule.get("规则编号"),
            "关联主题": topic_code,
            "任务编号": job_code,
            "生效版本": target_version,
            "最早检查点": earliest.get("检查点编号") if earliest else "-",
            "核对时间": datetime.now().isoformat(timespec="seconds"),
            "核对结果": checks,
            "结论": "通过" if passed else "需处理",
        }
        store.rows("schema_checkpoint_verifications").append(verification)
        return verification, "候选检查点核对完成"
