"""任务恢复索引测试：冲突判定、断点续写、Schema演进核对与列表检索。"""
from __future__ import annotations

import unittest

from app.services.flink_job import FlinkJobService
from app.services.recovery_index import RecoveryIndexService
from app.services.schema_evolve import SchemaEvolveService
from app.store import store


class TestRecoveryIndexGeneration(unittest.TestCase):
    """按任务版本定位可恢复检查点并生成任务恢复索引数据表。"""

    def setUp(self) -> None:
        self.svc = RecoveryIndexService()

    def test_conflict_resolution_by_task_version(self) -> None:
        """订单聚合任务跨两次输出表结构变更：按时间与按版本检索候选集合不同，按任务版本定位。"""
        result = self.svc.generate_for_job("FLIN-0001", "v2")
        self.assertTrue(result["ok"])
        # 冲突判定：时间检索 6 个候选，版本检索只剩 v2 的 2 个
        self.assertTrue(result["conflict"])
        self.assertEqual(result["time_based_count"], 6)
        self.assertEqual(result["version_based_count"], 2)
        self.assertEqual(result["resolution"], "按任务版本定位")
        self.assertEqual(result["structure_versions"], ["v1", "v2", "v3"])
        # 索引只给每个任务一个可启动检查点：v2 下最新的 chk-0004
        entry = result["entry"]
        self.assertEqual(entry["任务版本"], "v2")
        self.assertEqual(entry["下一检查点"], "chk-0004")
        self.assertEqual(entry["status"], "可恢复")
        self.assertEqual(entry["候选检查点数"], "2")
        # 跳过的 v1 检查点记录在索引里，不再从最早检查点扫描
        self.assertIn("chk-0001", entry["已跳过检查点"])
        self.assertIn("chk-0005", entry["已跳过检查点"])
        self.assertEqual(entry["来源模块"], "流式计算")

    def test_generate_requires_existing_job_and_version(self) -> None:
        """任务不存在或缺少任务版本时给出可读错误。"""
        missing = self.svc.generate_for_job("FLIN-9999", "v1")
        self.assertFalse(missing["ok"])
        self.assertIn("不存在", missing["error"])
        no_version = self.svc.generate_for_job("FLIN-0001", "")
        self.assertFalse(no_version["ok"])
        self.assertIn("任务版本", no_version["error"])
        bad_version = self.svc.generate_for_job("FLIN-0001", "v9")
        self.assertFalse(bad_version["ok"])
        self.assertIn("没有可恢复检查点", bad_version["error"])

    def test_generate_is_idempotent_when_completed(self) -> None:
        """已完成的索引再次生成不重复写入恢复项。"""
        result = self.svc.generate_for_job("FLIN-0002", "v2")
        self.assertTrue(result["ok"])
        self.assertEqual(result["written"], 0)
        self.assertEqual(result["kept_completed"], 2)
        self.assertEqual(result["remaining"], 0)
        logs = [log for log in store.rows("recovery_write_log")
                if log.get("任务编号") == "FLIN-0002"]
        self.assertEqual(len(logs), 2)


class TestBreakpointResume(unittest.TestCase):
    """写入中断后从断点恢复：只定位未完成检查点，保留已完成索引。"""

    def setUp(self) -> None:
        self.svc = RecoveryIndexService()
        self.schema_svc = SchemaEvolveService()

    def test_interrupt_then_resume_keeps_completed_items(self) -> None:
        """RECO-0003 种子处于写入中断：chk-0201 已完成，chk-0202/chk-0203 未完成。"""
        # 中断状态下 Schema 演进核对不通过：下一检查点仍是最早检查点
        before, _ = self.schema_svc.verify_checkpoint_candidates(3)
        self.assertEqual(before["结论"], "需处理")
        self.assertEqual(before["核对结果"][0]["不再扫描最早状态"], "否")

        # 模拟再次写入中断：本次只写 1 项
        first = self.svc.generate_for_job("FLIN-0003", "v1", fail_after=1)
        self.assertTrue(first["ok"])
        self.assertTrue(first["interrupted"])
        self.assertEqual(first["written"], 1)
        self.assertEqual(first["kept_completed"], 1)  # 保留中断前已完成的恢复项
        self.assertEqual(first["remaining"], 1)
        self.assertEqual(first["entry"]["status"], "待续写")
        self.assertEqual(first["entry"]["下一检查点"], "chk-0202")

        # 断点恢复：只定位未完成检查点
        second = self.svc.generate_for_job("FLIN-0003", "v1")
        self.assertEqual(second["written"], 1)
        self.assertEqual(second["kept_completed"], 2)
        self.assertEqual(second["remaining"], 0)
        self.assertEqual(second["entry"]["status"], "可恢复")
        self.assertEqual(second["entry"]["下一检查点"], "chk-0203")

        # 写入日志保留完整断点链，已完成的 chk-0201 未重复恢复
        logs = [log for log in store.rows("recovery_write_log")
                if log.get("任务编号") == "FLIN-0003"]
        self.assertEqual([log["检查点编号"] for log in logs],
                         ["chk-0201", "chk-0202", "chk-0203"])

        # 续写完成后 Schema 演进核对通过：字段版本匹配且不再扫描最早状态
        after, _ = self.schema_svc.verify_checkpoint_candidates(3)
        self.assertEqual(after["结论"], "通过")
        self.assertEqual(after["核对结果"][0]["字段版本校验"], "通过")
        self.assertEqual(after["核对结果"][0]["不再扫描最早状态"], "是")

    def test_resume_action_on_entry(self) -> None:
        """索引行上的断点续写动作驱动生成流程。"""
        entry = next(r for r in store.rows("recovery_index") if r.get("索引编号") == "RECO-0003")
        updated, message = self.svc.run_action(entry["id"], "断点续写")
        self.assertIsNotNone(updated)
        self.assertIn("断点续写", message)
        self.assertEqual(updated["status"], "可恢复")


class TestSchemaEvolveVerification(unittest.TestCase):
    """Schema演进基于任务恢复索引数据表校验检查点字段版本。"""

    def setUp(self) -> None:
        self.schema_svc = SchemaEvolveService()

    def test_verify_order_agg_checkpoints(self) -> None:
        """订单聚合任务：索引指向 v3 最新检查点，不再扫描最早状态。"""
        result, message = self.schema_svc.verify_checkpoint_candidates(1)
        self.assertIsNotNone(result)
        self.assertEqual(result["任务编号"], "FLIN-0001")
        self.assertEqual(result["生效版本"], "v3")
        self.assertEqual(result["最早检查点"], "chk-0001")
        check = next(c for c in result["核对结果"] if c["索引编号"] == "RECO-0001")
        self.assertEqual(check["下一检查点"], "chk-0006")
        self.assertEqual(check["检查点字段版本"], "v3")
        self.assertEqual(check["字段版本校验"], "通过")
        self.assertEqual(check["不再扫描最早状态"], "是")
        # 核对结果写入产物表
        verifications = store.rows("schema_checkpoint_verifications")
        self.assertTrue(any(v.get("规则编号") == "SCHE-0001" for v in verifications))

    def test_verify_without_index_returns_message(self) -> None:
        """未生成恢复索引时核对给出可读说明。"""
        job = {"id": 900, "status": "运行中", "pending": False, "abnormal": False,
               "任务编号": "FLIN-9000", "消费主题": "TOPI-9000", "转换逻辑": "测试",
               "输出表名": "nowhere", "并行度": "1", "检查点间隔": "60000",
               "状态后端": "Heap", "水位策略": "event-time"}
        store.rows("flink_job").append(job)
        rule = {"id": 900, "status": "已生效", "pending": False, "abnormal": False,
                "规则编号": "SCHE-9000", "关联主题": "TOPI-9000", "新增字段": "x",
                "删除字段": "-", "类型变更": "-", "默认值填充": "-",
                "兼容模式": "FULL", "生效版本": "v1"}
        store.rows("schema_evolve").append(rule)
        result, message = self.schema_svc.verify_checkpoint_candidates(900)
        self.assertIsNone(result)
        self.assertIn("尚未生成恢复索引", message)


class TestRecoveryIndexList(unittest.TestCase):
    """列表筛选、排序、分页与关联业务记录同步。"""

    def setUp(self) -> None:
        self.svc = RecoveryIndexService()

    def test_filter_by_version_and_status(self) -> None:
        items, total = self.svc.list_entries(filters={"任务版本": "v3"})
        self.assertGreaterEqual(total, 1)
        self.assertTrue(all(item["任务版本"] == "v3" for item in items))
        items, total = self.svc.list_entries(status="可恢复")
        self.assertTrue(all(item["status"] == "可恢复" for item in items))
        items, _ = self.svc.list_entries(keyword="RECO-0001")
        self.assertEqual(len(items), 1)

    def test_sort_by_checkpoint_time_desc(self) -> None:
        items, _ = self.svc.list_entries(sort_by="检查点时间", sort_order="desc", size=50)
        times = [item["检查点时间"] for item in items]
        self.assertEqual(times, sorted(times, reverse=True))

    def test_pagination(self) -> None:
        first, total = self.svc.list_entries(page=1, size=2)
        self.assertEqual(len(first), 2)
        second, _ = self.svc.list_entries(page=2, size=2)
        self.assertGreaterEqual(total, 3)
        self.assertGreaterEqual(len(second), 1)
        self.assertNotEqual(first[0]["id"], second[0]["id"])

    def test_enrichment_syncs_with_business_records(self) -> None:
        """刷新列表时关联任务状态与规则版本实时同步。"""
        flink_svc = FlinkJobService()
        job = next(r for r in store.rows("flink_job") if r.get("任务编号") == "FLIN-0002")
        original = job["status"]
        try:
            flink_svc.run_action(job["id"], "停止作业")
            items, _ = self.svc.list_entries(filters={"任务编号": "FLIN-0002"})
            self.assertEqual(items[0]["关联任务状态"], "已停止")
            self.assertEqual(items[0]["关联规则版本"], "v2")
            self.assertEqual(items[0]["版本一致"], "是")
        finally:
            job["status"] = original


if __name__ == "__main__":
    unittest.main()
