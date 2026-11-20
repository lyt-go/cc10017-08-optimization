"""流式计算业务规则：作业管理、链路消费与状态推进。

增强：Flink作业启动时校验上游主题状态，运行中消费主题数据生成计算产物。
"""
from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any

from app.store import store

MODULE = "flink_job"
REQUIRED_FIELDS = ["任务编号", "消费主题", "转换逻辑"]
STATUS_ORDER = ["草稿", "待校验", "运行中", "已停止"]
ACTION_RULES = {"提交校验": "待校验", "启动作业": "运行中", "停止作业": "已停止"}
NEGATIVE_ACTIONS = []


class FlinkJobService:
    def get_timeline(self) -> list[dict[str, Any]]:
        rows = store.rows(MODULE)
        today = date.today()
        items: list[dict] = []
        for i, row in enumerate(rows):
            start = (today - timedelta(days=7 - i * 2)).isoformat()
            end = (today + timedelta(days=3 - i)).isoformat()
            items.append({
                "id": int(row.get("id", i + 1)),
                "name": str(row.get(list(row.keys())[1] if len(row) > 1 else "name", "")),
                "start": start,
                "end": end,
                "status": str(row.get("status", STATUS_ORDER[i % len(STATUS_ORDER)])),
                "logs": f"[{start}] 作业启动\n[{start}] 正在处理...\n[{end}] 任务{'完成' if row.get('status') == STATUS_ORDER[-1] else '进行中'}",
            })
        return items

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        return store.find(MODULE, entry_id)

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [f for f in REQUIRED_FIELDS if not str(values.get(f) or "").strip()]
        if missing:
            return None, missing
        # 校验消费主题是否存在
        topic_code = values.get("消费主题")
        if topic_code:
            topic = self._find_topic(topic_code)
            if not topic:
                return None, [f"消费主题 {topic_code} 未注册"]

        rows = store.rows(MODULE)
        entry = {"id": max((int(r.get("id", 0)) for r in rows), default=0) + 1}
        entry.update({f: values.get(f) for f in REQUIRED_FIELDS})
        # 补充可选字段
        for f in ["输出表名", "并行度", "检查点间隔", "状态后端", "水位策略"]:
            entry[f] = values.get(f, "")
        entry["status"] = STATUS_ORDER[0]
        entry["pending"] = True
        entry["abnormal"] = False
        rows.append(entry)
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"流式任务 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于流式计算可执行范围"
        target = ACTION_RULES[action]

        # 启动作业时校验上游主题必须已上线
        if action == "启动作业":
            topic_code = entry.get("消费主题")
            topic = self._find_topic(topic_code) if topic_code else None
            if not topic:
                return None, f"消费主题 {topic_code} 未注册"
            if topic.get("status") != "已上线":
                return None, f"消费主题 {topic_code} 状态为「{topic.get('status')}」，须先上线"
            # 生成计算产物
            self._generate_compute_artifact(entry, topic)

        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        return entry, f"流式任务已{action}"

    def _find_topic(self, topic_code: str) -> dict[str, Any] | None:
        for row in store.rows("topic_registry"):
            if row.get("主题编号") == topic_code:
                return row
        return None

    def _generate_compute_artifact(self, entry: dict[str, Any], topic: dict[str, Any]) -> None:
        """Flink作业运行时生成计算产物记录。"""
        output_table = entry.get("输出表名", "unknown")
        artifact = {
            "job_id": entry.get("id"),
            "job_code": entry.get("任务编号"),
            "topic_code": topic.get("主题编号"),
            "topic_name": topic.get("主题名称"),
            "output_table": output_table,
            "transform": entry.get("转换逻辑"),
            "parallelism": entry.get("并行度"),
            "computed_at": datetime.now().isoformat(),
            "status": "running",
        }
        self._ensure_artifact_table("flink_compute_artifacts")
        store.rows("flink_compute_artifacts").append(artifact)
        entry["_last_artifact"] = artifact

    def get_compute_artifacts(self, entry_id: int) -> list[dict[str, Any]]:
        self._ensure_artifact_table("flink_compute_artifacts")
        return [a for a in store.rows("flink_compute_artifacts") if a.get("job_id") == entry_id]

    def list_checkpoint_jobs(self) -> list[dict[str, Any]]:
        """列出有检查点库存的流式任务，供恢复索引生成时筛选任务版本。"""
        jobs: list[dict[str, Any]] = []
        for row in store.rows(MODULE):
            ckpts = [c for c in store.rows("flink_checkpoints")
                     if str(c.get("任务编号")) == str(row.get("任务编号"))]
            if not ckpts:
                continue
            jobs.append({
                "id": row.get("id"),
                "任务编号": row.get("任务编号"),
                "输出表名": row.get("输出表名"),
                "status": row.get("status"),
                "检查点数": len(ckpts),
                "任务版本列表": sorted({str(c.get("任务版本", "")) for c in ckpts}),
            })
        return jobs

    def _ensure_artifact_table(self, name: str) -> None:
        store.rows(name)
