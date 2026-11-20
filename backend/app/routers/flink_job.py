"""流式计算 时间线接口：返回作业时间序列结构。"""
from __future__ import annotations

import csv
import io
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import StreamingResponse

from app.schemas import ActionPayload, ActionResult, EntryPayload
from app.services.flink_job import FlinkJobService
from app.services.recovery_index import RecoveryIndexService

router = APIRouter(prefix="/api/flink_job", tags=["流式计算"])
service = FlinkJobService()
recovery_service = RecoveryIndexService()
LIST_FIELDS = ["任务编号", "消费主题", "转换逻辑", "输出表名", "并行度", "检查点间隔", "状态后端", "水位策略"]
STATUSES = ["草稿", "待校验", "运行中", "已停止"]


def csv_cell(value: Any) -> str:
    text = str(value) if value is not None else ""
    if text and text[0] in "=+-@":
        text = "'" + text
    return '"' + text.replace('"', '""') + '"'


@router.get("")
def list_entries() -> dict[str, Any]:
    items = service.get_timeline()
    return {"items": items}


@router.get("/export")
def export_entries() -> StreamingResponse:
    items = service.get_timeline()
    buf = io.StringIO(newline="")
    writer = csv.writer(buf)
    all_keys = ["id"]
    if items:
        all_keys = list(items[0].keys())
    writer.writerow(all_keys)
    for item in items:
        writer.writerow([csv_cell(item.get(k, "")) for k in all_keys])
    buf.seek(0)
    return StreamingResponse(iter([buf.getvalue()]), media_type="text/csv; charset=utf-8",
                             headers={"Content-Disposition": "attachment; filename=flink_job.csv"})


@router.get("/recovery/jobs")
def list_checkpoint_jobs() -> dict[str, Any]:
    """列出有检查点库存的流式任务；必须声明在 /{entry_id} 之前。"""
    return {"items": service.list_checkpoint_jobs()}


@router.get("/{entry_id}")
def get_entry(entry_id: int) -> dict:
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"流式任务 {entry_id} 不存在")
    return entry


@router.get("/{entry_id}/checkpoints")
def get_checkpoint_candidates(
    entry_id: int,
    任务版本: str | None = Query(default=None, description="按任务版本筛选候选检查点"),
) -> dict[str, Any]:
    """预览任务的候选检查点：按时间与按版本两种检索的候选集合差异。"""
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"流式任务 {entry_id} 不存在")
    return recovery_service.checkpoint_preview(str(entry.get("任务编号")), 任务版本)


@router.post("/{entry_id}/recovery-index")
def generate_recovery_index(entry_id: int, payload: dict[str, Any]) -> ActionResult:
    """筛选任务版本，定位可恢复检查点并生成任务恢复索引数据表。

    写入中断后再次调用即从断点续写：只定位未完成检查点，保留已完成索引。
    """
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"流式任务 {entry_id} 不存在")
    task_version = str(payload.get("任务版本") or "").strip()
    fail_after_raw = payload.get("fail_after")
    try:
        fail_after = int(fail_after_raw) if fail_after_raw is not None else None
    except (TypeError, ValueError):
        return ActionResult(ok=False, message="fail_after 必须是整数")
    result = recovery_service.generate_for_job(
        str(entry.get("任务编号")), task_version, fail_after=fail_after)
    if not result.get("ok"):
        return ActionResult(ok=False, message=str(result.get("error", "恢复索引生成失败")), entry=result)
    if result["remaining"] > 0:
        message = (f"写入中断：本次写入{result['written']}项，保留已完成{result['kept_completed']}项，"
                   f"剩余{result['remaining']}项待断点续写")
    else:
        message = (f"恢复索引已生成：按任务版本 {task_version} 定位下一检查点 "
                   f"{result['next_checkpoint']}，不再扫描最早检查点")
    return ActionResult(ok=True, message=message, entry=result)


@router.post("")
def create_entry(payload: EntryPayload) -> ActionResult:
    entry_or_none, missing = service.create_entry(payload.values)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message="流式任务已登记", entry=entry_or_none)


@router.post("/{entry_id}/actions")
def run_action(entry_id: int, payload: ActionPayload) -> ActionResult:
    entry_or_none, message = service.run_action(entry_id, payload.action)
    if entry_or_none is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry_or_none)
