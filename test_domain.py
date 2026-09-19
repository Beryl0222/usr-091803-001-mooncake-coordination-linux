"""产销协同领域核心的行为测试，对应排产会上的每条诉求。"""

import os
import tempfile
import unittest

from domain import Domain, DomainError
from eventstore import EventStore


def build_domain():
    domain = Domain(EventStore(), seed=True)
    domain.define_shift({"shift_id": "SHIFT-A", "date": "2026-09-19", "name": "早班", "start": "06:00", "end": "14:00"})
    domain.define_shift({"shift_id": "SHIFT-B", "date": "2026-09-19", "name": "中班", "start": "14:00", "end": "22:00"})
    return domain


def receive_materials(domain):
    domain.receive_material({"batch_id": "MB-CHA", "material": "黑猪叉烧", "qty": 500, "unit": "kg", "supplier": "屯昌黑猪合作社"})
    domain.receive_material({"batch_id": "MB-NUT", "material": "果仁", "qty": 200, "allergens": ["坚果"]})
    domain.receive_material({"batch_id": "MB-FLR", "material": "面粉", "qty": 1000})


def create_order(domain, order_id="SO-0001"):
    return domain.create_order({
        "order_id": order_id,
        "channel": "商超补货",
        "store": "海口家乐福",
        "items": [{"product": "叉烧五仁月饼", "qty": 600}],
        "due": "2026-09-25",
    })


def produce(domain, order_id="SO-0001"):
    """面团拆开加工、两批成品合箱的完整生产链。"""
    domain.consume_material({
        "event_key": "scan-flour", "batch_id": "MB-FLR", "qty": 100,
        "lot_id": "LOT-DOUGH", "product": "面团",
        "step_id": "ST-02", "shift_id": "SHIFT-A", "worker_id": "W-001", "order_id": order_id,
    })
    domain.transform_lots({
        "inputs": [{"lot_id": "LOT-DOUGH", "qty": 100}],
        "outputs": [
            {"lot_id": "LOT-DOUGH-A", "product": "面团", "qty": 60},
            {"lot_id": "LOT-DOUGH-B", "product": "面团", "qty": 40},
        ],
        "step_id": "ST-04", "shift_id": "SHIFT-A", "worker_id": "W-002",
    })
    domain.consume_material({
        "event_key": "scan-pork", "batch_id": "MB-CHA", "qty": 30,
        "lot_id": "LOT-FILL", "product": "叉烧馅",
        "step_id": "ST-03", "shift_id": "SHIFT-A", "worker_id": "W-003", "order_id": order_id,
    })
    domain.transform_lots({
        "inputs": [{"lot_id": "LOT-DOUGH-A", "qty": 60}, {"lot_id": "LOT-FILL", "qty": 30}],
        "outputs": [{"lot_id": "LOT-CAKE-1", "product": "叉烧月饼", "qty": 90}],
        "step_id": "ST-05", "shift_id": "SHIFT-B", "worker_id": "W-001", "equipment_id": "EQ-OVEN-01",
    })
    domain.transform_lots({
        "inputs": [{"lot_id": "LOT-DOUGH-B", "qty": 40}],
        "outputs": [{"lot_id": "LOT-CAKE-2", "product": "叉烧月饼", "qty": 40}],
        "step_id": "ST-05", "shift_id": "SHIFT-B", "worker_id": "W-002", "equipment_id": "EQ-OVEN-01",
    })
    domain.pack_box({
        "box_code": "BOX-001",
        "items": [{"lot_id": "LOT-CAKE-1", "qty": 50}, {"lot_id": "LOT-CAKE-2", "qty": 30}],
        "order_id": order_id, "worker_id": "W-001",
    })


class CommitTest(unittest.TestCase):
    """订单承诺要落到原料批次、设备、工序和班次。"""

    def setUp(self):
        self.d = build_domain()
        receive_materials(self.d)
        create_order(self.d)

    def commit(self, **overrides):
        payload = {
            "materials": [{"batch_id": "MB-CHA", "qty": 120}, {"batch_id": "MB-FLR", "qty": 300}],
            "equipment": [{"equip_id": "EQ-OVEN-01", "step_id": "ST-05", "shift_id": "SHIFT-A", "planned_qty": 600}],
            "route": ["ST-01", "ST-02", "ST-03", "ST-04", "ST-05", "ST-08"],
            "shifts": ["SHIFT-A", "SHIFT-B"],
            "decided_by": "生产经理",
            "note": "商超补货优先",
        }
        payload.update(overrides)
        return self.d.commit_order("SO-0001", payload)

    def test_commit_records_batch_equipment_step_shift(self):
        result = self.commit()
        order = result["order"]
        self.assertEqual(order["status"], "已承诺")
        self.assertEqual(order["committed_materials"][0], {"batch_id": "MB-CHA", "qty": 120})
        self.assertEqual(order["committed_equipment"][0]["equip_id"], "EQ-OVEN-01")
        self.assertEqual(order["committed_shifts"], ["SHIFT-A", "SHIFT-B"])
        self.assertEqual([step["name"] for step in order["route_detail"]][:2], ["配料", "和面"])
        # 原料被预留，可用量随之减少
        self.assertEqual(self.d.material_view("MB-CHA")["reserved"], 120)
        self.assertEqual(self.d.material_view("MB-CHA")["available"], 380)
        # 决定带着当时的快照，供日后重放
        decision = result["decision"]
        self.assertEqual(decision["context"]["available_materials"]["MB-CHA"], 500)
        self.assertEqual(decision["context"]["equipment_load"]["EQ-OVEN-01@SHIFT-A"], 0)

    def test_commit_rejects_insufficient_material(self):
        self.commit()
        create_order(self.d, "SO-0002")
        with self.assertRaises(DomainError) as ctx:
            self.d.commit_order("SO-0002", {"materials": [{"batch_id": "MB-CHA", "qty": 400}]})
        self.assertEqual(ctx.exception.status, 409)

    def test_commit_rejects_equipment_overload(self):
        self.commit()
        create_order(self.d, "SO-0002")
        with self.assertRaises(DomainError) as ctx:
            self.d.commit_order("SO-0002", {
                "equipment": [{"equip_id": "EQ-OVEN-01", "shift_id": "SHIFT-A", "planned_qty": 700}],
            })
        self.assertEqual(ctx.exception.status, 409)
        self.assertIn("产能", str(ctx.exception))

    def test_commit_unknown_order_is_404(self):
        with self.assertRaises(DomainError) as ctx:
            self.d.commit_order("SO-404", {"materials": []})
        self.assertEqual(ctx.exception.status, 404)

    def test_recommit_releases_previous_reservation(self):
        first = self.commit()
        second = self.commit(materials=[{"batch_id": "MB-CHA", "qty": 100}])
        self.assertEqual(self.d.material_view("MB-CHA")["reserved"], 100)
        self.assertEqual(second["decision"]["supersedes"], first["decision"]["decision_id"])
        self.assertEqual(len(self.d.orders["SO-0001"]["decisions"]), 2)

    def test_consume_releases_reservation_once(self):
        self.commit()
        self.d.consume_material({
            "event_key": "k1", "batch_id": "MB-CHA", "qty": 30, "lot_id": "LOT-X",
            "product": "叉烧馅", "step_id": "ST-03", "shift_id": "SHIFT-A",
            "worker_id": "W-001", "order_id": "SO-0001",
        })
        view = self.d.material_view("MB-CHA")
        self.assertEqual(view["consumed"], 30)
        self.assertEqual(view["reserved"], 90)
        self.assertEqual(self.d.orders["SO-0001"]["status"], "生产中")


class TraceabilityTest(unittest.TestCase):
    """一批面团拆开加工、多批成品合箱后仍能反查来源。"""

    def setUp(self):
        self.d = build_domain()
        receive_materials(self.d)
        create_order(self.d)
        produce(self.d)

    def test_box_trace_back_to_material_batches(self):
        trace = self.d.trace_box("BOX-001")
        sources = {entry["batch_id"] for entry in trace["sources"]["material_batches"]}
        self.assertEqual(sources, {"MB-FLR", "MB-CHA"})
        self.assertIn("LOT-DOUGH-A", trace["sources"]["lots"])
        self.assertIn("LOT-DOUGH-B", trace["sources"]["lots"])
        self.assertIn("LOT-FILL", trace["sources"]["lots"])

    def test_split_lot_forward_trace(self):
        trace = self.d.trace_lot("LOT-DOUGH")
        self.assertEqual(
            set(trace["destinations"]["lots"]), {"LOT-DOUGH-A", "LOT-DOUGH-B", "LOT-CAKE-1", "LOT-CAKE-2"}
        )
        self.assertEqual(trace["destinations"]["boxes"], ["BOX-001"])

    def test_merged_lot_keeps_both_parents(self):
        trace = self.d.trace_lot("LOT-CAKE-1")
        # 直接上游是面团 A 和馅料，面团 A 又能反查到拆分前的面团批次
        self.assertEqual(set(trace["sources"]["lots"]), {"LOT-DOUGH-A", "LOT-FILL", "LOT-DOUGH"})
        batches = {entry["batch_id"] for entry in trace["sources"]["material_batches"]}
        self.assertEqual(batches, {"MB-FLR", "MB-CHA"})

    def test_handlers_show_who_took_over(self):
        handlers = self.d.trace_lot("LOT-DOUGH-A")["handlers"]
        self.assertTrue(any(h["worker_id"] == "W-002" and h["action"] == "加工产出" for h in handlers))
        cake_handlers = self.d.trace_lot("LOT-CAKE-1")["handlers"]
        self.assertTrue(any(h["worker"] == "王阿香" for h in cake_handlers))


class IdempotencyTest(unittest.TestCase):
    """扫描枪断网补传不能重复扣料。"""

    def setUp(self):
        self.d = build_domain()
        receive_materials(self.d)

    def scan(self):
        return self.d.consume_material({
            "event_key": "gun-07-offline-1", "batch_id": "MB-CHA", "qty": 10,
            "lot_id": "LOT-SCAN", "product": "叉烧馅",
            "step_id": "ST-03", "shift_id": "SHIFT-A", "worker_id": "W-001",
        })

    def test_same_event_key_deducts_once(self):
        first = self.scan()
        second = self.scan()
        self.assertFalse(first["deduplicated"])
        self.assertTrue(second["deduplicated"])
        self.assertEqual(self.d.materials["MB-CHA"]["consumed"], 10)
        self.assertEqual(self.d.lots["LOT-SCAN"]["produced"], 10)

    def test_transform_and_pack_dedup(self):
        self.scan()
        transform = {
            "event_key": "tr-1",
            "inputs": [{"lot_id": "LOT-SCAN", "qty": 10}],
            "outputs": [{"lot_id": "LOT-SCAN-OUT", "product": "叉烧月饼", "qty": 10}],
            "step_id": "ST-05", "worker_id": "W-002",
        }
        self.d.transform_lots(transform)
        again = self.d.transform_lots(transform)
        self.assertTrue(again["deduplicated"])
        self.assertEqual(self.d.lots["LOT-SCAN-OUT"]["produced"], 10)
        pack = {"event_key": "pk-1", "box_code": "BOX-900", "items": [{"lot_id": "LOT-SCAN-OUT", "qty": 10}]}
        self.d.pack_box(pack)
        self.assertTrue(self.d.pack_box(pack)["deduplicated"])
        self.assertEqual(self.d.lots["LOT-SCAN-OUT"]["remaining"], 0)

    def test_time_entry_dedup(self):
        entry = {"event_key": "te-1", "worker_id": "W-001", "shift_id": "SHIFT-A", "step_id": "ST-02", "hours": 8, "output_qty": 500}
        self.d.record_time_entry(entry)
        self.assertTrue(self.d.record_time_entry(entry)["deduplicated"])
        self.assertEqual(len(self.d.time_entries), 1)


class FreezeAndRecallTest(unittest.TestCase):
    """过敏原或检验异常出现时立即冻结相关箱码并找出已发往哪些门店。"""

    def setUp(self):
        self.d = build_domain()
        receive_materials(self.d)
        create_order(self.d)
        produce(self.d)
        self.d.pack_box({"box_code": "BOX-002", "items": [{"lot_id": "LOT-CAKE-1", "qty": 40}]})
        self.d.ship({"box_codes": ["BOX-001"], "store": "海口家乐福", "order_id": "SO-0001"})

    def test_qc_fail_freezes_boxes_and_lists_stores(self):
        result = self.d.record_check({
            "target_type": "lot", "target_id": "LOT-FILL",
            "item": "微生物", "result": "fail", "checked_by": "质检员小周",
        })
        impact = result["impact"]
        self.assertEqual(set(impact["frozen_boxes"]), {"BOX-001", "BOX-002"})
        self.assertEqual(impact["stores"], ["海口家乐福"])
        self.assertEqual(impact["shipments"][0]["store"], "海口家乐福")
        self.assertTrue(self.d.box_view("BOX-001")["frozen"])
        self.assertIn("LOT-CAKE-1", impact["frozen_lots"])

    def test_frozen_box_cannot_ship_until_release(self):
        self.d.record_check({"target_type": "lot", "target_id": "LOT-FILL", "item": "微生物", "result": "fail"})
        with self.assertRaises(DomainError) as ctx:
            self.d.ship({"box_codes": ["BOX-002"], "store": "三亚旺豪"})
        self.assertEqual(ctx.exception.status, 409)
        hold_id = next(h["hold_id"] for h in self.d.holds.values() if h["target_id"] == "BOX-002")
        self.d.release_hold(hold_id, {"released_by": "质量负责人"})
        self.d.ship({"box_codes": ["BOX-002"], "store": "三亚旺豪"})
        self.assertEqual(self.d.boxes["BOX-002"]["status"], "已出库")

    def test_allergen_on_material_freezes_downstream(self):
        self.d.consume_material({
            "event_key": "nut-1", "batch_id": "MB-NUT", "qty": 20,
            "lot_id": "LOT-NUT", "product": "果仁馅",
            "step_id": "ST-03", "shift_id": "SHIFT-A", "worker_id": "W-002",
        })
        self.d.transform_lots({
            "inputs": [{"lot_id": "LOT-NUT", "qty": 20}],
            "outputs": [{"lot_id": "LOT-TART", "product": "五仁月饼", "qty": 20}],
            "step_id": "ST-05", "worker_id": "W-002",
        })
        self.d.pack_box({"box_code": "BOX-100", "items": [{"lot_id": "LOT-TART", "qty": 20}]})
        result = self.d.record_check({
            "target_type": "material", "target_id": "MB-NUT",
            "item": "过敏原", "result": "fail", "allergen": "坚果",
        })
        self.assertEqual(result["impact"]["frozen_boxes"], ["BOX-100"])
        self.assertIn("过敏原异常:坚果", result["impact"]["reason"])

    def test_recall_is_read_only(self):
        before = len(self.d.holds)
        recall = self.d.recall("lot", "LOT-FILL")
        self.assertEqual(set(recall["affected_boxes"]), {"BOX-001", "BOX-002"})
        self.assertEqual(recall["stores"], ["海口家乐福"])
        self.assertEqual(len(self.d.holds), before)

    def test_frozen_material_cannot_be_consumed(self):
        self.d.record_check({"target_type": "material", "target_id": "MB-CHA", "item": "兽药残留", "result": "fail"})
        with self.assertRaises(DomainError) as ctx:
            self.d.consume_material({
                "event_key": "late-scan", "batch_id": "MB-CHA", "qty": 1, "lot_id": "LOT-LATE",
                "product": "叉烧馅", "step_id": "ST-03", "shift_id": "SHIFT-A", "worker_id": "W-001",
            })
        self.assertEqual(ctx.exception.status, 409)


class AppendOnlyTest(unittest.TestCase):
    """质检、返工、出库、退货及报告补录都不得覆盖旧记录。"""

    def setUp(self):
        self.d = build_domain()
        receive_materials(self.d)
        create_order(self.d)
        produce(self.d)

    def test_repeated_checks_keep_full_history(self):
        self.d.record_check({"target_type": "lot", "target_id": "LOT-CAKE-1", "item": "净含量", "result": "pass"})
        self.d.record_check({"target_type": "lot", "target_id": "LOT-CAKE-1", "item": "净含量", "result": "fail", "detail": "复测偏差"})
        checks = self.d.list_checks("lot", "LOT-CAKE-1")
        self.assertEqual([c["result"] for c in checks], ["pass", "fail"])

    def test_report_backfill_supersedes_without_overwriting(self):
        first = self.d.file_report({
            "ref_type": "order", "ref_id": "SO-0001", "kind": "生产日报",
            "content": "当日产出 80 箱", "reported_by": "班长",
        })
        second = self.d.file_report({
            "ref_type": "order", "ref_id": "SO-0001", "kind": "生产日报",
            "content": "更正：当日产出 90 箱", "reported_by": "班长",
            "supersedes": first["report"]["report_id"],
        })
        reports = self.d.list_reports("order", "SO-0001")
        self.assertEqual(len(reports), 2)
        self.assertEqual(reports[0]["content"], "当日产出 80 箱")
        self.assertEqual(reports[1]["supersedes"], first["report"]["report_id"])
        with self.assertRaises(DomainError):
            self.d.file_report({
                "ref_type": "order", "ref_id": "SO-0001", "kind": "生产日报",
                "content": "引用了不存在的报告", "supersedes": "RPT-9999",
            })

    def test_return_does_not_touch_shipment_record(self):
        self.d.ship({"shipment_id": "SHP-0001", "box_codes": ["BOX-001"], "store": "海口家乐福"})
        self.d.record_return({"box_codes": ["BOX-001"], "from_store": "海口家乐福", "reason": "临近保质期"})
        self.assertEqual(self.d.shipments["SHP-0001"]["store"], "海口家乐福")
        self.assertEqual(self.d.boxes["BOX-001"]["status"], "已退货")
        self.assertEqual(len(self.d.returns), 1)

    def test_return_validations(self):
        with self.assertRaises(DomainError) as ctx:
            self.d.record_return({"box_codes": ["BOX-001"], "from_store": "海口家乐福"})
        self.assertEqual(ctx.exception.status, 409)  # 未出库不能退货
        self.d.ship({"box_codes": ["BOX-001"], "store": "海口家乐福"})
        with self.assertRaises(DomainError) as ctx:
            self.d.record_return({"box_codes": ["BOX-001"], "from_store": "三亚旺豪"})
        self.assertEqual(ctx.exception.status, 409)  # 退货门店必须匹配

    def test_rework_records_accumulate(self):
        self.d.record_rework({"lot_id": "LOT-CAKE-2", "qty": 5, "from_step": "ST-05", "to_step": "ST-04", "reason": "饼皮开裂"})
        self.d.record_rework({"lot_id": "LOT-CAKE-2", "qty": 3, "from_step": "ST-05", "to_step": "ST-04", "reason": "露馅"})
        self.assertEqual(len(self.d.lots["LOT-CAKE-2"]["reworks"]), 2)
        self.assertEqual(self.d.lot_view("LOT-CAKE-2")["rework_count"], 2)

    def test_event_log_is_append_only(self):
        before = [e["seq"] for e in self.d.list_events()]
        self.d.record_check({"target_type": "lot", "target_id": "LOT-CAKE-1", "item": "净含量", "result": "pass"})
        after = self.d.list_events()
        self.assertEqual([e["seq"] for e in after[: len(before)]], before)


class ReplayTest(unittest.TestCase):
    """负责人应能重放某个订单当时的排产决定。"""

    def test_replay_decisions_and_timeline(self):
        d = Domain(EventStore(), seed=True)
        d.define_shift({"shift_id": "SHIFT-A", "date": "2026-09-19", "name": "早班", "at": "2026-09-19T06:00:00+00:00"})
        d.receive_material({"batch_id": "MB-CHA", "material": "黑猪叉烧", "qty": 500, "at": "2026-09-19T07:00:00+00:00"})
        d.create_order({
            "order_id": "SO-0001", "channel": "商超补货", "store": "海口家乐福",
            "items": [{"product": "叉烧五仁月饼", "qty": 600}], "due": "2026-09-25",
            "at": "2026-09-19T07:30:00+00:00",
        })
        d.commit_order("SO-0001", {
            "materials": [{"batch_id": "MB-CHA", "qty": 120}],
            "equipment": [{"equip_id": "EQ-OVEN-01", "shift_id": "SHIFT-A", "planned_qty": 600}],
            "route": ["ST-01", "ST-05"], "shifts": ["SHIFT-A"],
            "decided_by": "生产经理", "note": "先保商超",
            "at": "2026-09-19T08:00:00+00:00",
        })
        d.consume_material({
            "event_key": "r1", "batch_id": "MB-CHA", "qty": 30, "lot_id": "LOT-R",
            "product": "叉烧馅", "step_id": "ST-03", "shift_id": "SHIFT-A",
            "worker_id": "W-001", "order_id": "SO-0001",
            "at": "2026-09-19T10:00:00+00:00",
        })
        d.commit_order("SO-0001", {
            "materials": [{"batch_id": "MB-CHA", "qty": 150}],
            "decided_by": "生产经理", "note": "游客团购加单，调整预留",
            "at": "2026-09-19T12:00:00+00:00",
        })
        # 重放到两次决定之间：只能看到第一次决定和当时的订单状态
        replay = d.replay_order("SO-0001", at="2026-09-19T11:00:00+00:00")
        self.assertEqual(len(replay["decisions"]), 1)
        self.assertEqual(replay["decisions"][0]["note"], "先保商超")
        self.assertEqual(replay["decisions"][0]["context"]["available_materials"]["MB-CHA"], 500)
        self.assertEqual(replay["state"]["status"], "生产中")
        self.assertFalse(any(e["type"] == "Shipped" for e in replay["timeline"]))
        # 完整重放：两次决定都在，时间线按序展开
        full = d.replay_order("SO-0001")
        self.assertEqual(len(full["decisions"]), 2)
        self.assertEqual(full["decisions"][1]["supersedes"], full["decisions"][0]["decision_id"])
        seqs = [e["seq"] for e in full["timeline"]]
        self.assertEqual(seqs, sorted(seqs))


class WageTest(unittest.TestCase):
    """按村民工时核对产量和应付报酬。"""

    def test_wages_from_time_entries(self):
        d = build_domain()
        d.record_time_entry({
            "worker_id": "W-001", "shift_id": "SHIFT-A", "step_id": "ST-02",
            "hours": 8, "output_qty": 500, "at": "2026-09-19T14:00:00+00:00",
        })
        d.record_time_entry({
            "worker_id": "W-001", "shift_id": "SHIFT-B", "step_id": "ST-05",
            "hours": 4, "output_qty": 300, "at": "2026-09-19T22:00:00+00:00",
        })
        d.record_time_entry({
            "worker_id": "W-002", "shift_id": "SHIFT-A", "step_id": "ST-04",
            "hours": 6, "output_qty": 400, "at": "2026-09-19T14:00:00+00:00",
        })
        wages = d.wages(worker_id="W-001")["wages"][0]
        self.assertEqual(wages["total_hours"], 12)
        self.assertEqual(wages["total_output"], 800)
        self.assertEqual(wages["amount_due"], 12 * 22)
        self.assertEqual(wages["entries"][0]["shift"], "早班")
        everyone = d.wages(date_from="2026-09-19", date_to="2026-09-19")["wages"]
        self.assertEqual({w["worker_id"] for w in everyone if w["entries"]}, {"W-001", "W-002"})
        empty = d.wages(worker_id="W-001", date_from="2026-09-20")["wages"][0]
        self.assertEqual(empty["amount_due"], 0)


class PersistenceTest(unittest.TestCase):
    """事件落盘，重启后投影完整恢复。"""

    def test_rebuild_from_event_log(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "events.jsonl")
            d = Domain(EventStore(path), seed=True)
            d.define_shift({"shift_id": "SHIFT-A", "date": "2026-09-19", "name": "早班"})
            d.define_shift({"shift_id": "SHIFT-B", "date": "2026-09-19", "name": "中班"})
            receive_materials(d)
            create_order(d)
            produce(d)
            d.record_check({"target_type": "lot", "target_id": "LOT-FILL", "item": "微生物", "result": "fail"})

            rebuilt = Domain(EventStore(path), seed=True)
            self.assertEqual(rebuilt.materials["MB-CHA"]["consumed"], 30)
            self.assertEqual(rebuilt.lots["LOT-CAKE-1"]["remaining"], 40)
            self.assertTrue(rebuilt.box_view("BOX-001")["frozen"])
            trace = rebuilt.trace_box("BOX-001")
            self.assertEqual(
                {e["batch_id"] for e in trace["sources"]["material_batches"]}, {"MB-FLR", "MB-CHA"}
            )
            # 发号器延续，新箱码不会与已持久化的箱码撞号
            packed = rebuilt.pack_box({"items": [{"lot_id": "LOT-CAKE-2", "qty": 10}]})
            self.assertEqual(packed["box"]["box_code"], "BOX-0002")


if __name__ == "__main__":
    unittest.main()
