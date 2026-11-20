"""任务恢复索引业务规则：按任务版本定位可恢复检查点，维护任务恢复索引数据表。

优化点：
- 旧恢复入口按时间顺序从最早检查点扫描全部状态，任务越多启动越慢；
- 新做法按任务版本定位唯一可启动检查点，写入任务恢复索引数据表；
- 写入中断后从断点续写：只定位未完成检查点，保留中断前已完成的恢复项；
- 索引行保留任务版本与下一检查点，供Schema演进跨模块校验检查点字段版本。
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from app.store import store

MODULE = "recovery_index"
CHECKPOINT_TABLE = "flink_checkpoints"
WRITE_LOG_TABLE = "recovery_write_log"
LIST_FIELDS = ["索引编号", "任务编号", "任务版本", "输出表名", "下一检查点", "检查点时间", "候选检查点数", "来源模块"]
REQUIRED_FIELDS = ["索引编号", "任务编号", "任务版本", "下一检查点"]
STATUS_ORDER = ["待续写", "可恢复", "已恢复", "已作废"]
ACTION_RULES = {"断点续写": "可恢复", "标记已恢复": "已恢复", "作废索引": "已作废"}
NEGATIVE_ACTIONS = ["作废索引"]
SORTABLE_FIELDS = {"索引编号", "任务编号", "任务版本", "输出表名", "下一检查点", "检查点时间"}


class RecoveryIndexService:
    # ───────────────────────────────
    # 列表 / 明细（读取时实时关联业务记录，刷新页面即同步）
    # ───────────────────────────────

    def list_entries(
        self,
        *,
        filters: dict[str, str] | None = None,
        keyword: str | None = None,
        status: str | None = None,
        sort_by: str | None = None,
        sort_order: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        rows = store.rows(MODULE)
        for field, expected in (filters or {}).items():
            rows = [row for row in rows if expected.casefold() in str(row.get(field, "")).casefold()]
        if keyword:
            rows = [row for row in rows if keyword.casefold() in str(row.get("索引编号", "")).casefold()]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        if sort_by and sort_by in SORTABLE_FIELDS:
            rows = sorted(rows, key=lambda row: str(row.get(sort_by, "")),
                          reverse=(sort_order or "asc").lower() == "desc")
        total = len(rows)
        start = (page - 1) * size
        return [self._enrich(row) for row in rows[start:start + size]], total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        row = store.find(MODULE, entry_id)
        return self._enrich(row) if row else None

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [field for field in REQUIRED_FIELDS if not str(values.get(field) or "").strip()]
        if missing:
            return None, missing
        job = self._find_job(str(values.get("任务编号")))
        if job is None:
            return None, [f"任务编号 {values.get('任务编号')} 未在流式计算模块登记"]
        rows = store.rows(MODULE)
        entry = {"id": max((int(row.get("id", 0)) for row in rows), default=0) + 1}
        entry.update({field: values.get(field) for field in LIST_FIELDS})
        entry["来源模块"] = entry.get("来源模块") or "流式计算"
        entry.setdefault("已跳过检查点", "-")
        entry.setdefault("恢复状态说明", "人工登记")
        entry["status"] = "可恢复"
        entry["pending"] = False
        entry["abnormal"] = False
        rows.append(entry)
        return entry, []

    # ───────────────────────────────
    # 状态动作
    # ───────────────────────────────

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"恢复索引 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于任务恢复索引可执行范围"
        if action == "断点续写":
            result = self.generate_for_job(str(entry.get("任务编号")), str(entry.get("任务版本")))
            if not result.get("ok"):
                return None, str(result.get("error", "断点续写失败"))
            kept = result["kept_completed"]
            written = result["written"]
            if result["remaining"] > 0:
                return result["entry"], f"断点续写写入{written}项，保留已完成{kept}项，剩余{result['remaining']}项"
            return result["entry"], f"断点续写完成：本次写入{written}项，保留已完成{kept}项，索引已可恢复"
        target = ACTION_RULES[action]
        if target not in STATUS_ORDER:
            return None, f"目标状态「{target}」不在允许的状态序列里"
        entry["status"] = target
        entry["pending"] = target == "待续写"
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        return entry, f"恢复索引已{action}"

    # ───────────────────────────────
    # 候选检查点预览（冲突判定前置）
    # ───────────────────────────────

    def checkpoint_preview(self, job_code: str, task_version: str | None = None) -> dict[str, Any]:
        """列出任务的候选检查点，并给出按时间/按版本两种检索的候选集合差异。"""
        all_ckpts = self._checkpoints_for(job_code)
        done_ids = self._completed_checkpoint_ids(job_code, task_version)
        version_based = [c for c in all_ckpts
                         if not task_version or str(c.get("任务版本")) == task_version]
        checkpoints = []
        for ckpt in version_based:
            item = dict(ckpt)
            item["已写入索引"] = "是" if str(ckpt.get("检查点编号")) in done_ids else "否"
            checkpoints.append(item)
        return {
            "任务编号": job_code,
            "任务版本": task_version or "",
            "checkpoints": checkpoints,
            "time_based_count": len(all_ckpts),
            "version_based_count": len(version_based),
            "conflict": len(all_ckpts) != len(version_based),
            "structure_versions": sorted({str(c.get("结构版本", "")) for c in all_ckpts}),
            "completed_checkpoint_ids": sorted(done_ids),
        }

    # ───────────────────────────────
    # 恢复索引生成：冲突判定 + 断点续写
    # ───────────────────────────────

    def generate_for_job(
        self,
        job_code: str,
        task_version: str,
        fail_after: int | None = None,
    ) -> dict[str, Any]:
        """按任务版本定位可恢复检查点并写入任务恢复索引数据表。

        fail_after 用于模拟写入中断：本次调用写入 N 项后即中断，剩余检查点
        留待下次断点续写；中断前已完成的恢复项保留在写入日志里，不会重复恢复。
        """
        job = self._find_job(job_code)
        if job is None:
            return {"ok": False, "error": f"流式任务 {job_code} 不存在"}
        task_version = (task_version or "").strip()
        if not task_version:
            return {"ok": False, "error": "缺少任务版本，无法定位可恢复检查点"}

        all_ckpts = self._checkpoints_for(job_code)
        if not all_ckpts:
            return {"ok": False, "error": f"任务 {job_code} 暂无检查点数据"}

        # 冲突判定：按检查点时间检索（旧做法，从最早扫描全部状态）与按任务版本
        # 检索会留下不同的候选集合；索引只能给每个任务一个可启动检查点，按任务版本定位。
        time_based = all_ckpts
        version_based = [c for c in all_ckpts if str(c.get("任务版本")) == task_version]
        conflict = len(time_based) != len(version_based)
        structure_versions = sorted({str(c.get("结构版本", "")) for c in all_ckpts})
        if not version_based:
            return {
                "ok": False,
                "error": f"任务版本 {task_version} 下没有可恢复检查点（结构版本：{'、'.join(structure_versions)}）",
                "conflict": conflict,
                "time_based_count": len(time_based),
                "version_based_count": 0,
            }

        # 断点续写：只定位未完成检查点，保留已完成索引，避免重复恢复。
        done_ids = self._completed_checkpoint_ids(job_code, task_version)
        kept_completed = sum(1 for c in version_based if str(c.get("检查点编号")) in done_ids)
        todo = [c for c in version_based if str(c.get("检查点编号")) not in done_ids]

        entry = self._find_index(job_code, task_version) or self._new_index(job, task_version)

        written = 0
        interrupted = False
        for ckpt in todo:
            if fail_after is not None and written >= fail_after:
                interrupted = True
                break
            self._write_index_entry(entry, ckpt, version_based, time_based)
            written += 1

        remaining = len(todo) - written
        total_done = kept_completed + written
        if remaining > 0:
            entry["status"] = "待续写"
            entry["pending"] = True
            entry["恢复状态说明"] = f"写入中断，已完成{total_done}项，剩余{remaining}项待断点续写"
        else:
            entry["status"] = "可恢复"
            entry["pending"] = False
            entry["恢复状态说明"] = "按任务版本定位可恢复检查点，不再扫描最早状态"

        return {
            "ok": True,
            "entry": self._enrich(entry),
            "conflict": conflict,
            "resolution": "按任务版本定位" if conflict else "候选集合一致，无需判定",
            "structure_versions": structure_versions,
            "time_based_count": len(time_based),
            "version_based_count": len(version_based),
            "kept_completed": kept_completed,
            "written": written,
            "remaining": remaining,
            "interrupted": interrupted,
            "next_checkpoint": entry.get("下一检查点"),
        }

    # ───────────────────────────────
    # 内部辅助
    # ───────────────────────────────

    def _enrich(self, row: dict[str, Any]) -> dict[str, Any]:
        """读取时实时关联流式任务与Schema演进规则，刷新页面即同步更新。"""
        item = dict(row)
        job = self._find_job(str(row.get("任务编号", "")))
        item["关联任务状态"] = str(job.get("status")) if job else "未关联"
        rule = None
        if job is not None:
            topic_code = job.get("消费主题")
            candidates = [r for r in store.rows("schema_evolve") if r.get("关联主题") == topic_code]
            rule = next((r for r in candidates if r.get("status") == "已生效"), None) or (
                candidates[0] if candidates else None)
        item["关联规则版本"] = str(rule.get("生效版本")) if rule else "-"
        item["版本一致"] = "是" if rule and str(rule.get("生效版本")) == str(row.get("任务版本")) else "否"
        return item

    def _find_job(self, job_code: str) -> dict[str, Any] | None:
        for row in store.rows("flink_job"):
            if str(row.get("任务编号")) == job_code:
                return row
        return None

    def _find_index(self, job_code: str, task_version: str) -> dict[str, Any] | None:
        for row in store.rows(MODULE):
            if str(row.get("任务编号")) == job_code and str(row.get("任务版本")) == task_version:
                return row
        return None

    def _new_index(self, job: dict[str, Any], task_version: str) -> dict[str, Any]:
        rows = store.rows(MODULE)
        seq = max((int(row.get("id", 0)) for row in rows), default=0) + 1
        entry = {
            "id": seq,
            "索引编号": f"RECO-{seq:04d}",
            "任务编号": job.get("任务编号"),
            "任务版本": task_version,
            "输出表名": job.get("输出表名", ""),
            "下一检查点": "-",
            "检查点时间": "-",
            "候选检查点数": "0",
            "已跳过检查点": "-",
            "来源模块": "流式计算",
            "恢复状态说明": "待写入",
            "status": "待续写",
            "pending": True,
            "abnormal": False,
        }
        rows.append(entry)
        return entry

    def _checkpoints_for(self, job_code: str) -> list[dict[str, Any]]:
        rows = [c for c in store.rows(CHECKPOINT_TABLE) if str(c.get("任务编号")) == job_code]
        return sorted(rows, key=lambda c: str(c.get("检查点时间", "")))

    def _completed_checkpoint_ids(self, job_code: str, task_version: str | None) -> set[str]:
        return {
            str(log.get("检查点编号"))
            for log in store.rows(WRITE_LOG_TABLE)
            if str(log.get("任务编号")) == job_code
            and (task_version is None or str(log.get("任务版本")) == task_version)
            and log.get("写入状态") == "已完成"
        }

    def _write_index_entry(
        self,
        entry: dict[str, Any],
        ckpt: dict[str, Any],
        version_based: list[dict[str, Any]],
        time_based: list[dict[str, Any]],
    ) -> None:
        """把单个恢复项写入任务恢复索引数据表，并记录写入日志作为断点。"""
        entry["下一检查点"] = ckpt.get("检查点编号")
        entry["检查点时间"] = ckpt.get("检查点时间")
        entry["输出表名"] = ckpt.get("输出表名", entry.get("输出表名"))
        entry["候选检查点数"] = str(len(version_based))
        skipped = [str(c.get("检查点编号")) for c in time_based
                   if str(c.get("任务版本")) != str(entry.get("任务版本"))]
        entry["已跳过检查点"] = ",".join(skipped) if skipped else "-"
        entry["来源模块"] = "流式计算"
        logs = store.rows(WRITE_LOG_TABLE)
        logs.append({
            "id": max((int(log.get("id", 0)) for log in logs), default=0) + 1,
            "索引编号": entry.get("索引编号"),
            "任务编号": entry.get("任务编号"),
            "任务版本": entry.get("任务版本"),
            "检查点编号": ckpt.get("检查点编号"),
            "写入状态": "已完成",
            "写入时间": datetime.now().isoformat(timespec="seconds"),
        })
