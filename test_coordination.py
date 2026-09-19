"""产销协同核心场景的端到端测试。

在内存事件账本上串起中秋排产会的完整链路：

1. 订单承诺落到原料批次/设备/工序/班次；
2. 一批面团拆开加工、多批成品合箱后仍能双向反查；
3. 扫描枪断网补传（同一幂等键）不重复扣料；
4. 过敏原/检验异常立即冻结相关箱码并定位已发门店；
5. 质检/返工/出库/退货/报告补录均为只增记录，旧记录不被覆盖；
6. 可重放某订单当时的排产决定；
7. 按村民工时核对产量与应付报酬。
"""

import json
import sqlite3
import tempfile
import unittest

from commands import App
from domain import Conflict, NotFound, ValidationFailed, replay
from eventstore import EventStore


def t(hour, minute=0):
    return f"2026-09-19T{hour:02d}:{minute:02d}:00Z"


class FlowTestBase(unittest.TestCase):
    def setUp(self):
        self.app = App(EventStore(":memory:"))

    def emit(self, fn, **kw):
        event, duplicated = fn(kw)
        self.assertFalse(duplicated)
        return event

    # -- 场景准备 -----------------------------------------------------------

    def seed(self):
        """屯昌两家工坊收到补货单与游客团购单的起始数据。"""
        self.emit(
            self.app.register_material,
            lot_code="M-PORK-01", material="黑猪叉烧", qty=100, unit="kg",
            supplier="屯昌黑猪合作社", allergen_flags=["大豆(酱油)"],
        )
        self.emit(
            self.app.register_material,
            lot_code="M-NUTS-01", material="果仁", qty=60, unit="kg",
            supplier="山间果仁档", allergen_flags=["坚果"],
        )
        self.emit(
            self.app.register_material,
            lot_code="M-DOUGH-01", material="面团", qty=200, unit="kg",
            supplier="自拌",
        )
        self.emit(
            self.app.register_material,
            lot_code="M-BOX-01", material="中秋礼盒", qty=500, unit="个",
            supplier="包装厂",
        )
        for code, name, rate in [
            ("W-ALIN", "阿玲", 20),
            ("W-AMEI", "阿梅", 20),
            ("W-LAOX", "老邢", 25),
        ]:
            self.emit(self.app.register_worker, worker_code=code, name=name, hourly_rate=rate)
        self.emit(
            self.app.receive_order,
            order_id="O-SUPER-01", customer="商超", store="商场总店",
            product="黑猪叉烧月饼", qty=1000, unit="个",
            due_at="2026-09-24T10:00:00Z",
        )
        self.emit(
            self.app.receive_order,
            order_id="O-TOUR-02", customer="游客团购", store="游客中心店",
            product="果仁月饼", qty=500, unit="个",
            due_at="2026-09-23T10:00:00Z", allergen_flags=["坚果"],
        )


class TestScheduling(FlowTestBase):
    def test_order_promise_falls_on_lot_equipment_process_shift(self):
        self.seed()
        event = self.emit(
            self.app.schedule_production,
            idem_key="sched-O-SUPER-01-v1",
            order_id="O-SUPER-01",
            equipment="1号炉",
            processes=[
                {"process": "搅拌", "equipment": "1号搅拌机", "shift_id": "S-MORNING"},
                {"process": "包馅", "equipment": "1号包馅机", "shift_id": "S-MORNING"},
                {"process": "烘烤", "equipment": "1号炉", "shift_id": "S-AFTERNOON"},
            ],
            allocations=[
                {"lot_code": "M-PORK-01", "qty": 40},
                {"lot_code": "M-DOUGH-01", "qty": 80},
            ],
            requirements=[
                {"material": "黑猪叉烧", "qty": 35, "unit": "kg"},
                {"material": "面团", "qty": 80, "unit": "kg"},
            ],
            workers=["W-ALIN", "W-LAOX"],
        )
        plan = event["payload"]["plan"]
        self.assertEqual(plan["processes"][1],
                         {"process": "包馅", "equipment": "1号包馅机", "shift_id": "S-MORNING"})
        self.assertEqual({a["lot_code"] for a in plan["allocations"]},
                         {"M-PORK-01", "M-DOUGH-01"})
        state = self.app.state()
        self.assertEqual(state.orders["O-SUPER-01"].status, "scheduled")
        # 排产只承诺不扣料：40kg 叉烧仍全部可用。
        self.assertEqual(state.lots["M-PORK-01"].available, 100)

    def test_schedule_rejects_uncovered_requirement(self):
        self.seed()
        with self.assertRaises(Conflict):
            self.app.schedule_production({
                "order_id": "O-SUPER-01",
                "processes": [{"process": "烘烤", "equipment": "1号炉", "shift_id": "S-NIGHT"}],
                "allocations": [{"lot_code": "M-PORK-01", "qty": 10}],
                "requirements": [{"material": "黑猪叉烧", "qty": 50, "unit": "kg"}],
            })

    def test_schedule_rejects_unknown_lot(self):
        self.seed()
        with self.assertRaises(NotFound):
            self.app.schedule_production({
                "order_id": "O-SUPER-01",
                "processes": [{"process": "烘烤", "equipment": "1号炉", "shift_id": "S-NIGHT"}],
                "allocations": [{"lot_code": "M-NOPE", "qty": 1}],
            })

    def test_schedule_cannot_promise_same_lot_to_two_orders_beyond_stock(self):
        self.seed()
        # 第一单承诺 180kg 面团（总量 200kg）。
        self.emit(
            self.app.schedule_production,
            order_id="O-SUPER-01",
            processes=[{"process": "烘烤", "equipment": "1号炉", "shift_id": "S-AFTERNOON"}],
            allocations=[{"lot_code": "M-DOUGH-01", "qty": 180}],
        )
        # 第二单再承诺 30kg：超出可用量，拒绝。
        with self.assertRaises(Conflict):
            self.app.schedule_production({
                "order_id": "O-TOUR-02",
                "processes": [{"process": "烘烤", "equipment": "2号炉", "shift_id": "S-NIGHT"}],
                "allocations": [{"lot_code": "M-DOUGH-01", "qty": 30}],
            })
        # 第一单实际消耗 30kg 后，原承诺未兑现部分降到 150kg，第二单再承诺 20kg 可行。
        self.emit(
            self.app.consume_material,
            lot_code="M-DOUGH-01", order_id="O-SUPER-01", qty=30,
            process="搅拌", equipment="1号搅拌机", shift_id="S-MORNING",
        )
        self.emit(
            self.app.schedule_production,
            order_id="O-TOUR-02",
            processes=[{"process": "搅拌", "equipment": "2号搅拌机", "shift_id": "S-NIGHT"}],
            allocations=[{"lot_code": "M-DOUGH-01", "qty": 20}],
        )
        lot = self.app.state().lots["M-DOUGH-01"]
        self.assertEqual(lot.available, 170)

    def test_replay_decision_matches_plan_and_material_state_then(self):
        self.seed()
        self.emit(
            self.app.schedule_production,
            order_id="O-SUPER-01", equipment="1号炉",
            processes=[{"process": "烘烤", "equipment": "1号炉", "shift_id": "S-AFTERNOON"}],
            allocations=[{"lot_code": "M-PORK-01", "qty": 40}],
        )
        sched_seq = self.app.state().orders["O-SUPER-01"].planned_seq
        # 之后才发生的扣料不应影响"当时"的重放结果。
        self.emit(
            self.app.consume_material,
            lot_code="M-PORK-01", order_id="O-SUPER-01", qty=40,
            process="包馅", equipment="1号包馅机", shift_id="S-MORNING",
        )
        replayed = self.app.replay_order_decision("O-SUPER-01")
        self.assertEqual(replayed["replayed_at_seq"], sched_seq)
        self.assertEqual(replayed["material_lots_then"][0]["available_then"], 100)
        self.assertEqual(replayed["order_status_then"], "scheduled")
        # 事件流继续，当前态反映扣料。
        self.assertEqual(self.app.state().lots["M-PORK-01"].available, 60)

    def test_schedule_history_keeps_every_version(self):
        self.seed()
        self.emit(
            self.app.schedule_production,
            order_id="O-TOUR-02", equipment="2号炉",
            processes=[{"process": "烘烤", "equipment": "2号炉", "shift_id": "S-NIGHT"}],
            allocations=[{"lot_code": "M-NUTS-01", "qty": 20}],
        )
        self.emit(
            self.app.schedule_production,
            order_id="O-TOUR-02", equipment="2号炉",
            processes=[{"process": "烘烤", "equipment": "2号炉", "shift_id": "S-MORNING"}],
            allocations=[{"lot_code": "M-NUTS-01", "qty": 25}],
        )
        order = self.app.state().orders["O-TOUR-02"]
        self.assertEqual(len(order.schedule_history), 2)
        first = self.app.replay_order_decision("O-TOUR-02", version=0)
        self.assertEqual(first["plan"]["processes"][0]["shift_id"], "S-NIGHT")
        self.assertEqual(first["plan"]["allocations"][0]["qty"], 20)
        latest = self.app.replay_order_decision("O-TOUR-02")
        self.assertEqual(latest["plan"]["processes"][0]["shift_id"], "S-MORNING")


class TestProductionAndGenealogy(FlowTestBase):
    def _schedule_super(self):
        self.emit(
            self.app.schedule_production,
            order_id="O-SUPER-01", equipment="1号炉",
            processes=[
                {"process": "搅拌", "equipment": "1号搅拌机", "shift_id": "S-MORNING"},
                {"process": "包馅", "equipment": "1号包馅机", "shift_id": "S-MORNING"},
                {"process": "烘烤", "equipment": "1号炉", "shift_id": "S-AFTERNOON"},
                {"process": "包装", "equipment": "包装台", "shift_id": "S-EVENING"},
            ],
            allocations=[
                {"lot_code": "M-PORK-01", "qty": 40},
                {"lot_code": "M-DOUGH-01", "qty": 80},
            ],
        )

    def test_split_dough_traceable_in_both_directions(self):
        self.seed()
        self._schedule_super()
        # 一批面团拆开：白班阿玲 50kg、晚班阿梅 30kg 分开加工。
        self.emit(self.app.split_lot,
                  parent_lot="M-DOUGH-01", child_lot="D-A", qty=50,
                  order_id="O-SUPER-01", process="分切", equipment="1号分切台",
                  shift_id="S-MORNING", worker_code="W-ALIN")
        self.emit(self.app.split_lot,
                  parent_lot="M-DOUGH-01", child_lot="D-B", qty=30,
                  order_id="O-SUPER-01", process="分切", equipment="1号分切台",
                  shift_id="S-EVENING", worker_code="W-AMEI")
        trace = self.app.trace("M-DOUGH-01")
        self.assertEqual(set(trace["downstream"] and [e["child"] for e in trace["downstream"]]),
                         {"D-A", "D-B"})
        # 半成品批次也能反查到原料。
        back = self.app.trace("D-B")
        self.assertEqual([e["parent"] for e in back["upstream"]], ["M-DOUGH-01"])

    def test_timeline_shows_handover_after_villager_shift_change(self):
        self.seed()
        self._schedule_super()
        self.emit(self.app.split_lot,
                  parent_lot="M-DOUGH-01", child_lot="D-A", qty=50,
                  order_id="O-SUPER-01", process="分切", equipment="1号分切台",
                  shift_id="S-MORNING", worker_code="W-ALIN",
                  business_time=t(8))
        self.emit(self.app.produce_lot,
                  lot_code="F-A", order_id="O-SUPER-01", product="黑猪叉烧月饼",
                  qty=400, unit="个", kind="semi",
                  inputs=[{"lot_code": "D-A", "qty": 20}, {"lot_code": "M-PORK-01", "qty": 16}],
                  process="包馅", equipment="1号包馅机", shift_id="S-MORNING",
                  worker_code="W-ALIN", piece_rate=0.1, business_time=t(10))
        self.emit(self.app.produce_lot,
                  lot_code="F-A", order_id="O-SUPER-01", product="黑猪叉烧月饼",
                  qty=200, unit="个", kind="semi",
                  inputs=[{"lot_code": "D-A", "qty": 12}, {"lot_code": "M-PORK-01", "qty": 10}],
                  process="烘烤", equipment="1号炉", shift_id="S-AFTERNOON",
                  worker_code="W-LAOX", piece_rate=0.1, business_time=t(14))
        tl = self.app.timeline("D-A")
        self.assertEqual([r["worker_code"] for r in tl["events"]], ["W-ALIN", "W-ALIN", "W-LAOX"])
        shifts = [(r["process"], r["shift_id"]) for r in tl["events"]]
        self.assertIn(("分切", "S-MORNING"), shifts)
        self.assertIn(("烘烤", "S-AFTERNOON"),
                      [(r["process"], r["shift_id"]) for r in tl["events"]])

    def test_over_consumption_is_rejected(self):
        self.seed()
        self._schedule_super()
        self.app.consume_material({
            "lot_code": "M-PORK-01", "order_id": "O-SUPER-01", "qty": 90,
            "process": "包馅", "equipment": "1号包馅机", "shift_id": "S-MORNING",
        })
        with self.assertRaises(Conflict):
            self.app.consume_material({
                "lot_code": "M-PORK-01", "order_id": "O-SUPER-01", "qty": 20,
                "process": "包馅", "equipment": "1号包馅机", "shift_id": "S-MORNING",
            })

    def test_scanner_retries_are_idempotent(self):
        self.seed()
        self._schedule_super()
        scan = {
            "idem_key": "scan-7788",
            "lot_code": "M-PORK-01", "order_id": "O-SUPER-01", "qty": 20,
            "process": "包馅", "equipment": "1号包馅机", "shift_id": "S-MORNING",
            "scanner_id": "SCAN-02",
        }
        first, dup1 = self.app.consume_material(scan)
        self.assertFalse(dup1)
        second, dup2 = self.app.consume_material(dict(scan, business_time=t(12)))
        self.assertTrue(dup2)
        self.assertEqual(second["seq"], first["seq"])
        lot = self.app.state().lots["M-PORK-01"]
        self.assertEqual(lot.qty_consumed, 20)
        self.assertEqual(len(lot.consumption), 1)
        # 账本层面对"同键不同事件类型"的补传同样只保留首条，绝不产生第二条。
        count_after = len(self.app.store.events())
        _, dup_store = self.app.store.append(
            "material_consumed", {"forged": True},
            event_id="scan-7788", business_time=t(13), recorded_at=t(13),
        )
        self.assertTrue(dup_store)
        self.assertEqual(len(self.app.store.events()), count_after)

    def test_retry_returns_first_event_even_after_stock_exhausted(self):
        """补传期间库存已被其他单据占用：重试仍必须返回首条成功事件。"""
        self.seed()
        self.emit(
            self.app.schedule_production,
            order_id="O-SUPER-01", equipment="1号炉",
            processes=[{"process": "包馅", "equipment": "1号包馅机", "shift_id": "S-MORNING"}],
            allocations=[{"lot_code": "M-PORK-01", "qty": 100}],
        )
        scan = {
            "idem_key": "scan-full",
            "lot_code": "M-PORK-01", "order_id": "O-SUPER-01", "qty": 100,
            "process": "包馅", "equipment": "1号包馅机", "shift_id": "S-MORNING",
        }
        first, dup = self.app.consume_material(scan)
        self.assertFalse(dup)
        self.assertEqual(self.app.state().lots["M-PORK-01"].available, 0)
        # 不带幂等键的新扣料因库存不足被拒。
        with self.assertRaises(Conflict):
            self.app.consume_material(dict(scan, idem_key="other"))
        # 断网补传同一把枪的同一条扫描：不报错、不二次扣料。
        retry, dup2 = self.app.consume_material(scan)
        self.assertTrue(dup2)
        self.assertEqual(retry["seq"], first["seq"])
        self.assertEqual(self.app.state().lots["M-PORK-01"].qty_consumed, 100)

    def test_split_merge_pack_full_chain_and_trace(self):
        self.seed()
        self._schedule_super()
        # 叉烧订单：面团拆两批，分别包馅产出 F-A / F-B。
        self.emit(self.app.split_lot, parent_lot="M-DOUGH-01", child_lot="D-A", qty=40,
                  order_id="O-SUPER-01", process="分切", equipment="1号分切台",
                  shift_id="S-MORNING", worker_code="W-ALIN")
        self.emit(self.app.split_lot, parent_lot="M-DOUGH-01", child_lot="D-B", qty=40,
                  order_id="O-SUPER-01", process="分切", equipment="1号分切台",
                  shift_id="S-EVENING", worker_code="W-AMEI")
        self.emit(self.app.produce_lot, lot_code="F-A", order_id="O-SUPER-01",
                  product="黑猪叉烧月饼", qty=500, unit="个", kind="semi",
                  inputs=[{"lot_code": "D-A", "qty": 40}, {"lot_code": "M-PORK-01", "qty": 20}],
                  process="包馅", equipment="1号包馅机", shift_id="S-MORNING",
                  worker_code="W-ALIN", piece_rate=0.1)
        self.emit(self.app.produce_lot, lot_code="F-B", order_id="O-SUPER-01",
                  product="黑猪叉烧月饼", qty=500, unit="个", kind="semi",
                  inputs=[{"lot_code": "D-B", "qty": 40}, {"lot_code": "M-PORK-01", "qty": 20}],
                  process="包馅", equipment="2号包馅机", shift_id="S-EVENING",
                  worker_code="W-AMEI", piece_rate=0.1)
        self.emit(self.app.finish_lot, lot_code="F-A", kind="finished")
        self.emit(self.app.finish_lot, lot_code="F-B", kind="finished")
        # 多批成品合箱，同时核销包装材料批次。
        self.emit(self.app.pack_box,
                  box_code="BX-001", order_id="O-SUPER-01",
                  items=[{"lot_code": "F-A", "qty": 300}, {"lot_code": "F-B", "qty": 200}],
                  packaging={"lot_code": "M-BOX-01", "qty": 50, "unit": "个"},
                  shift_id="S-EVENING", worker_code="W-AMEI")
        box = self.app.state().boxes["BX-001"]
        self.assertEqual(box.source_lots, ["F-A", "F-B"])
        self.assertEqual(self.app.state().lots["M-BOX-01"].qty_consumed, 50)
        # 从箱码反查到两条原料分支：叉烧 + 面团。
        traced = self.app.trace("BX-001")
        self.assertIn("M-PORK-01", traced["related_lots"] )
        self.assertIn("M-DOUGH-01", traced["related_lots"])
        # 从原料批次正查到箱码。
        forward = self.app.trace("M-PORK-01")
        self.assertIn("BX-001", forward["boxes"])
        self.assertIn("F-B", [e["child"] for e in forward["downstream"]])

    def test_cannot_pack_unfinished_lot(self):
        self.seed()
        self._schedule_super()
        self.emit(self.app.produce_lot, lot_code="F-A", order_id="O-SUPER-01",
                  product="黑猪叉烧月饼", qty=500, unit="个", kind="semi",
                  inputs=[{"lot_code": "M-DOUGH-01", "qty": 40},
                          {"lot_code": "M-PORK-01", "qty": 20}],
                  process="包馅", equipment="1号包馅机", shift_id="S-MORNING")
        with self.assertRaises(Conflict):
            self.app.pack_box({
                "box_code": "BX-BAD", "order_id": "O-SUPER-01",
                "lot_code": "F-A", "qty": 100,
            })

    def test_merge_different_orders_lots_traceable(self):
        self.seed()
        # 游客果仁单：坚果批次 split 后，一个子批与叉烧面团合批（混搭礼盒场景）。
        self.emit(self.app.produce_lot, lot_code="FN-1", order_id="O-TOUR-02",
                  product="果仁月饼", qty=200, unit="个", kind="semi",
                  inputs=[{"lot_code": "M-NUTS-01", "qty": 10}],
                  process="包馅", equipment="2号包馅机", shift_id="S-NIGHT",
                  worker_code="W-LAOX", piece_rate=0.12)
        self.emit(self.app.produce_lot, lot_code="FP-1", order_id="O-SUPER-01",
                  product="黑猪叉烧月饼", qty=200, unit="个", kind="semi",
                  inputs=[{"lot_code": "M-DOUGH-01", "qty": 10},
                          {"lot_code": "M-PORK-01", "qty": 8}],
                  process="包馅", equipment="1号包馅机", shift_id="S-MORNING",
                  worker_code="W-ALIN", piece_rate=0.1)
        self.emit(self.app.merge_lots, child_lot="FMIX-1", order_id="O-TOUR-02",
                  parents=[{"lot_code": "FN-1", "qty": 100}, {"lot_code": "FP-1", "qty": 100}],
                  product="双拼月饼",
                  process="拼配", equipment="拼配台", shift_id="S-EVENING",
                  worker_code="W-AMEI")
        self.emit(self.app.finish_lot, lot_code="FMIX-1", kind="finished",
                  product="双拼月饼")
        self.emit(self.app.pack_box, box_code="BX-MIX", order_id="O-TOUR-02",
                  lot_code="FMIX-1", qty=200)
        # 坚果过敏原经合批传导到箱码。
        traced = self.app.trace("M-NUTS-01")
        self.assertIn("BX-MIX", traced["boxes"])
        self.assertEqual(self.app.state().boxes["BX-MIX"].source_lots, ["FMIX-1"])

    def test_genealogy_edge_count_stable_on_full_replay(self):
        self.test_split_merge_pack_full_chain_and_trace()
        # 状态是纯函数：拿全部事件重新 fold，结果不变。
        fresh = replay(self.app.store.events())
        self.assertEqual(len(fresh.edges), 6)  # 2 split + 2*2 投入边
        self.assertEqual(set(fresh.boxes), {"BX-001"})


class TestFreezeAndTrace(FlowTestBase):
    def _build_shipped_chain(self):
        self.seed()
        self.emit(
            self.app.schedule_production,
            order_id="O-SUPER-01", equipment="1号炉",
            processes=[{"process": "烘烤", "equipment": "1号炉", "shift_id": "S-AFTERNOON"}],
            allocations=[{"lot_code": "M-PORK-01", "qty": 20},
                         {"lot_code": "M-DOUGH-01", "qty": 40}],
        )
        self.emit(self.app.produce_lot, lot_code="F-A", order_id="O-SUPER-01",
                  product="黑猪叉烧月饼", qty=500, unit="个", kind="semi",
                  inputs=[{"lot_code": "M-DOUGH-01", "qty": 40},
                          {"lot_code": "M-PORK-01", "qty": 20}],
                  process="包馅", equipment="1号包馅机", shift_id="S-MORNING",
                  worker_code="W-ALIN", piece_rate=0.1)
        self.emit(self.app.finish_lot, lot_code="F-A", kind="finished")
        self.emit(self.app.pack_box, box_code="BX-001", order_id="O-SUPER-01",
                  lot_code="F-A", qty=300, worker_code="W-ALIN")
        self.emit(self.app.pack_box, box_code="BX-002", order_id="O-SUPER-01",
                  lot_code="F-A", qty=200, worker_code="W-ALIN")
        self.emit(self.app.ship, shipment_id="SH-01", order_id="O-SUPER-01",
                  store="商场总店", boxes=["BX-001"], operator="老邢",
                  business_time=t(18))
        # BX-002 尚未发运，留厂内。

    def test_allergen_alert_freezes_related_boxes_and_finds_stores(self):
        self._build_shipped_chain()
        event = self.emit(
            self.app.allergen_alert,
            scope=[{"kind": "lot", "ref": "M-PORK-01"}],
            allergen="大豆(酱油)",
            note="叉烧酱料过敏原标识与外箱不符",
            business_time=t(20),
        )
        p = event["payload"]
        self.assertEqual(set(p["affected_boxes"]), {"BX-001", "BX-002"})
        self.assertEqual(len(p["stores"]), 1)
        self.assertEqual(p["stores"][0]["store"], "商场总店")
        self.assertEqual(p["stores"][0]["boxes"], ["BX-001"])
        self.assertEqual(p["stores"][0]["remaining_qty"], 300)
        # 立即冻结：已发的箱码状态 frozen，留厂的不能出库。
        self.assertTrue(self.app.state().boxes["BX-001"].frozen)
        with self.assertRaises(Conflict):
            self.app.ship({
                "shipment_id": "SH-02", "order_id": "O-SUPER-01",
                "store": "商场二店", "boxes": ["BX-002"],
            })

    def test_quality_failed_freezes_immediately(self):
        self._build_shipped_chain()
        ev = self.emit(
            self.app.quality_inspection,
            scope=[{"kind": "box", "ref": "BX-002"}],
            status="failed", inspector="品控阿强", note="菌落总数超标",
        )
        self.assertIn("BX-002", ev["payload"]["affected_boxes"])
        # 箱码异常沿谱系向上冻结同源成品批次和已发的 BX-001。
        self.assertIn("BX-001", ev["payload"]["affected_boxes"])
        self.assertIn("F-A", ev["payload"]["affected_lots"])
        self.assertEqual(ev["payload"]["stores"][0]["store"], "商场总店")
        # passed 不触发冻结。
        ok = self.emit(
            self.app.quality_inspection,
            scope=[{"kind": "box", "ref": "BX-002"}], status="passed",
            inspector="品控阿强", note="复检合格",
        )
        self.assertEqual(ok["payload"]["affected_boxes"], [])

    def test_frozen_lot_blocks_consumption_and_packing(self):
        self.seed()
        self.emit(
            self.app.allergen_alert,
            scope=[{"kind": "lot", "ref": "M-NUTS-01"}],
            allergen="坚果",
        )
        with self.assertRaises(Conflict):
            self.app.consume_material({
                "lot_code": "M-NUTS-01", "order_id": "O-TOUR-02", "qty": 1,
                "process": "包馅", "equipment": "2号包馅机", "shift_id": "S-NIGHT",
            })

    def test_release_restores_box_state_respecting_returns(self):
        self._build_shipped_chain()
        self.emit(self.app.allergen_alert,
                  scope=[{"kind": "lot", "ref": "M-PORK-01"}], allergen="大豆(酱油)")
        self.emit(self.app.receive_return,
                  shipment_id="SH-01",
                  items=[{"box_code": "BX-001", "qty": 300}],
                  reason="召回", business_time=t(22))
        self.assertEqual(self.app.state().boxes["BX-001"].state, "frozen")
        self.emit(self.app.release_freeze, refs=["BX-001", "BX-002", "F-A", "M-PORK-01"],
                  reason="复检合格，召回流程结束")
        box = self.app.state().boxes["BX-001"]
        self.assertFalse(box.frozen)
        self.assertEqual(box.state, "returned")  # 不因解冻覆盖退货状态
        self.assertFalse(self.app.state().lots["F-A"].frozen)

    def test_return_over_shipment_rejected(self):
        self._build_shipped_chain()
        with self.assertRaises(Conflict):
            self.app.receive_return({
                "shipment_id": "SH-01",
                "items": [{"box_code": "BX-001", "qty": 301}],
            })

    def test_partial_release_keeps_other_boxes_frozen(self):
        """同案冻结多个箱码时，只解冻其中一个不能放出其他箱码。"""
        self._build_shipped_chain()
        self.emit(self.app.allergen_alert,
                  scope=[{"kind": "lot", "ref": "M-PORK-01"}], allergen="大豆(酱油)")
        self.assertTrue(self.app.state().boxes["BX-001"].frozen)
        self.assertTrue(self.app.state().boxes["BX-002"].frozen)
        self.emit(self.app.release_freeze, refs=["BX-002"], reason="该箱复检合格")
        state = self.app.state()
        self.assertFalse(state.boxes["BX-002"].frozen)
        self.assertEqual(state.boxes["BX-002"].state, "packed")
        self.assertTrue(state.boxes["BX-001"].frozen)
        # 冻结记录仍 active；留厂箱码解冻后可以出库，已发问题箱码仍拦截。
        self.emit(self.app.ship, shipment_id="SH-02", order_id="O-SUPER-01",
                  store="商场二店", boxes=["BX-002"])
        self.assertEqual(self.app.state().boxes["BX-002"].state, "shipped")
        with self.assertRaises(Conflict):
            self.app.ship({
                "shipment_id": "SH-03", "order_id": "O-SUPER-01",
                "store": "商场总店", "boxes": ["BX-001"],
            })

    def test_unknown_ref_in_scope_404(self):
        self.seed()
        with self.assertRaises(NotFound):
            self.app.allergen_alert({
                "scope": [{"kind": "lot", "ref": "M-GHOST"}],
                "allergen": "坚果",
            })


class TestAppendOnlyAndHistory(FlowTestBase):
    def test_events_cannot_be_updated_or_deleted(self):
        self.seed()
        conn = self.app.store._conn
        with self.assertRaises(sqlite3.IntegrityError):
            conn.execute("UPDATE events SET event_type='hacked' WHERE seq=1")
        with self.assertRaises(sqlite3.IntegrityError):
            conn.execute("DELETE FROM events WHERE seq=1")

    def test_backfilled_report_does_not_cover_old_record(self):
        self.seed()
        first = self.emit(self.app.add_report,
                          entity_kind="order", entity_id="O-SUPER-01",
                          report_type="排产备注", content="首批安排1号炉",
                          author="生产经理", business_time=t(9))
        second = self.emit(self.app.add_report,
                           entity_kind="order", entity_id="O-SUPER-01",
                           report_type="排产备注", content="设备改为2号炉",
                           author="生产经理", business_time=t(11))
        order = self.app.state().orders["O-SUPER-01"]
        self.assertEqual([r["content"] for r in order.reports],
                         ["首批安排1号炉", "设备改为2号炉"])
        self.assertNotEqual(first["seq"], second["seq"])
        # 允许补录"过去"的报告，记录顺序仍按追加 seq 排列。
        self.emit(self.app.add_report,
                  entity_kind="order", entity_id="O-SUPER-01",
                  report_type="巡检补录", content="晨间温控记录补录",
                  author="品控阿强", business_time="2026-09-18T07:30:00Z")
        contents = [r["content"] for r in self.app.state().orders["O-SUPER-01"].reports]
        self.assertEqual(contents, ["首批安排1号炉", "设备改为2号炉", "晨间温控记录补录"])
        # recorded_at 与 business_time 分开保存。
        backfill = self.app.state().orders["O-SUPER-01"].reports[-1]
        self.assertEqual(backfill["business_time"], "2026-09-18T07:30:00Z")
        self.assertNotEqual(backfill["recorded_at"], backfill["business_time"])

    def test_rework_completions_append_only(self):
        self.seed()
        self.emit(self.app.produce_lot, lot_code="F-A", order_id="O-SUPER-01",
                  product="黑猪叉烧月饼", qty=100, unit="个", kind="semi",
                  inputs=[{"lot_code": "M-DOUGH-01", "qty": 8},
                          {"lot_code": "M-PORK-01", "qty": 4}],
                  process="包馅", equipment="1号包馅机", shift_id="S-MORNING")
        self.emit(self.app.finish_lot, lot_code="F-A", kind="finished")
        self.emit(self.app.pack_box, box_code="BX-RW", order_id="O-SUPER-01",
                  lot_code="F-A", qty=100)
        self.emit(self.app.start_rework,
                  rework_id="RW-01", order_id="O-SUPER-01",
                  ref_kind="box", ref="BX-RW",
                  reason="封口不严", process="返工封口", equipment="封口机",
                  shift_id="S-EVENING", worker_code="W-AMEI")
        self.emit(self.app.complete_rework,
                  rework_id="RW-01", qty=90, unit="个", note="90个重新封口，10个报废")
        with self.assertRaises(Conflict):
            self.app.complete_rework({"rework_id": "RW-01", "qty": 95, "note": "想改结果"})
        rw = self.app.state().reworks["RW-01"]
        self.assertEqual(rw.state, "done")
        self.assertEqual(len(rw.completions), 1)

    def test_cancel_then_schedule_rejected(self):
        self.seed()
        self.emit(self.app.cancel_order, order_id="O-SUPER-01", reason="商超撤单")
        with self.assertRaises(Conflict):
            self.app.schedule_production({
                "order_id": "O-SUPER-01",
                "processes": [{"process": "烘烤", "equipment": "1号炉", "shift_id": "S-NIGHT"}],
                "allocations": [{"lot_code": "M-PORK-01", "qty": 1}],
            })

    def test_return_appends_and_sets_partial_then_full(self):
        self._partial_setup()
        # 部分退。
        self.emit(self.app.receive_return, shipment_id="SH-01",
                  items=[{"box_code": "BX-001", "qty": 100}], reason="压损")
        ship = self.app.state().shipments["SH-01"]
        self.assertEqual(ship.state, "partially_returned")
        order = self.app.state().orders["O-SUPER-01"]
        self.assertEqual(len(order.returns), 1)
        # 再退剩余：追加第二条记录，出库与退货历史都保留。
        self.emit(self.app.receive_return, shipment_id="SH-01",
                  items=[{"box_code": "BX-001", "qty": 200}], reason="召回")
        ship = self.app.state().shipments["SH-01"]
        self.assertEqual(ship.state, "returned")
        order = self.app.state().orders["O-SUPER-01"]
        self.assertEqual(len(order.returns), 2)
        self.assertEqual(order.shipments[0]["state"], "returned")

    def _partial_setup(self):
        self.seed()
        self.emit(self.app.produce_lot, lot_code="F-A", order_id="O-SUPER-01",
                  product="黑猪叉烧月饼", qty=300, unit="个", kind="semi",
                  inputs=[{"lot_code": "M-DOUGH-01", "qty": 24},
                          {"lot_code": "M-PORK-01", "qty": 12}],
                  process="包馅", equipment="1号包馅机", shift_id="S-MORNING")
        self.emit(self.app.finish_lot, lot_code="F-A", kind="finished")
        self.emit(self.app.pack_box, box_code="BX-001", order_id="O-SUPER-01",
                  lot_code="F-A", qty=300)
        self.emit(self.app.ship, shipment_id="SH-01", order_id="O-SUPER-01",
                  store="商场总店", boxes=["BX-001"])

    def test_persistence_across_reopen(self):
        with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
            store = EventStore(tmp.name)
            app = App(store)
            app.register_material({
                "lot_code": "M1", "material": "果仁", "qty": 10, "unit": "kg",
            })
            store.close()
            store2 = EventStore(tmp.name)
            app2 = App(store2)
            self.assertIn("M1", app2.state().lots)
            store2.close()


class TestWorkerPayroll(FlowTestBase):
    def test_payroll_and_reconciliation_by_shift_hours(self):
        self.seed()
        # 白班阿玲：8 小时 + 500 个计件；午后老邢顶班 4 小时 + 300 个。
        self.emit(self.app.record_effort,
                  worker_code="W-ALIN", shift_id="S-MORNING", order_id="O-SUPER-01",
                  hours=8, business_time=t(12))
        self.emit(self.app.record_effort,
                  worker_code="W-LAOX", shift_id="S-AFTERNOON", order_id="O-SUPER-01",
                  hours=4, business_time=t(18))
        self.emit(self.app.produce_lot, lot_code="F-A", order_id="O-SUPER-01",
                  product="黑猪叉烧月饼", qty=500, unit="个", kind="semi",
                  inputs=[{"lot_code": "M-DOUGH-01", "qty": 40},
                          {"lot_code": "M-PORK-01", "qty": 20}],
                  process="包馅", equipment="1号包馅机", shift_id="S-MORNING",
                  worker_code="W-ALIN", piece_rate=0.1)
        self.emit(self.app.produce_lot, lot_code="F-A", order_id="O-SUPER-01",
                  product="黑猪叉烧月饼", qty=300, unit="个", kind="semi",
                  inputs=[{"lot_code": "M-DOUGH-01", "qty": 24},
                          {"lot_code": "M-PORK-01", "qty": 12}],
                  process="烘烤", equipment="1号炉", shift_id="S-AFTERNOON",
                  worker_code="W-LAOX", piece_rate=0.1)
        payroll = self.app.payroll("W-ALIN")
        self.assertEqual(payroll["total_hours"], 8)
        self.assertEqual(payroll["hourly_pay"], 160.0)
        self.assertEqual(payroll["piece_pay"], 50.0)
        self.assertEqual(payroll["total_pay"], 210.0)
        rows = self.app.reconciliation("O-SUPER-01")
        by_worker = {r["worker_code"]: r for r in rows}
        self.assertEqual(by_worker["W-ALIN"]["output_qty"], 500)
        self.assertTrue(by_worker["W-ALIN"]["has_shift_record"])
        self.assertEqual(by_worker["W-LAOX"]["shift_hours"], 4)
        # 阿梅没登记工时却有产出时，核对表应标记缺工时。
        self.emit(self.app.produce_lot, lot_code="F-B", order_id="O-SUPER-01",
                  product="黑猪叉烧月饼", qty=50, unit="个", kind="semi",
                  inputs=[{"lot_code": "M-DOUGH-01", "qty": 4},
                          {"lot_code": "M-PORK-01", "qty": 2}],
                  process="包馅", equipment="2号包馅机", shift_id="S-EVENING",
                  worker_code="W-AMEI", piece_rate=0.1)
        rows = self.app.reconciliation("O-SUPER-01")
        amei = next(r for r in rows if r["worker_code"] == "W-AMEI")
        self.assertFalse(amei["has_shift_record"])
        self.assertEqual(amei["shift_hours"], 0)

    def test_effort_backfill_allowed_and_kept(self):
        self.seed()
        self.emit(self.app.record_effort,
                  worker_code="W-AMEI", shift_id="S-EVENING", order_id="O-SUPER-01",
                  hours=6, note="补录节前夜值班",
                  business_time="2026-09-18T20:00:00Z")
        worker = self.app.state().workers["W-AMEI"]
        self.assertEqual(len(worker.efforts), 1)
        self.assertEqual(worker.efforts[0]["hours"], 6)


class TestHttpLayer(unittest.TestCase):
    """通过真实 HTTP 服务器验证路由、状态码与幂等响应。"""

    @classmethod
    def setUpClass(cls):
        import service as svc
        from http.server import ThreadingHTTPServer
        cls.svc = svc
        cls.app = App(EventStore(":memory:"))
        svc.DB_PATH = ":memory:"
        svc.reset_app(cls.app)
        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), svc.Handler)
        cls.thread = __import__("threading").Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()
        cls.base_url = f"http://127.0.0.1:{cls.server.server_port}"

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join(timeout=2)

    def _req(self, method, path, body=None):
        import urllib.request
        import urllib.error
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(
            f"{self.base_url}{path}", data=data, method=method,
            headers={"Content-Type": "application/json"},
        )
        try:
            with urllib.request.urlopen(req, timeout=3) as resp:
                return resp.status, json.loads(resp.read())
        except urllib.error.HTTPError as exc:
            return exc.code, json.loads(exc.read())

    def test_health_unchanged(self):
        status, payload = self._req("GET", "/health")
        self.assertEqual(status, 200)
        self.assertEqual(payload, {"status": "ok", "service": "mooncake-coordination",
                                   "name": "月饼工坊产销协同"})

    def test_unknown_route_404_and_bad_json_400(self):
        import urllib.request
        import urllib.error
        with self.assertRaises(urllib.error.HTTPError) as ctx:
            urllib.request.urlopen(f"{self.base_url}/unknown", timeout=3)
        self.assertEqual(ctx.exception.code, 404)
        ctx.exception.close()
        req = urllib.request.Request(f"{self.base_url}/v1/materials",
                                     data=b"{bad", method="POST",
                                     headers={"Content-Type": "application/json"})
        with self.assertRaises(urllib.error.HTTPError) as ctx:
            urllib.request.urlopen(req, timeout=3)
        self.assertEqual(ctx.exception.code, 400)

    def test_full_flow_over_http_and_idempotent_retry(self):
        status, body = self._req("POST", "/v1/materials", {
            "lot_code": "M-PORK", "material": "黑猪叉烧", "qty": 50, "unit": "kg",
        })
        self.assertEqual(status, 201)
        status, body = self._req("POST", "/v1/orders", {
            "order_id": "O1", "product": "叉烧月饼", "qty": 500, "unit": "个",
        })
        self.assertEqual(status, 201)
        status, body = self._req("POST", "/v1/schedules", {
            "order_id": "O1",
            "processes": [{"process": "烘烤", "equipment": "1号炉", "shift_id": "S1"}],
            "allocations": [{"lot_code": "M-PORK", "qty": 20}],
        })
        self.assertEqual(status, 201)
        scan = {
            "idem_key": "scan-http-1",
            "lot_code": "M-PORK", "order_id": "O1", "qty": 20,
            "process": "包馅", "equipment": "1号包馅机", "shift_id": "S1",
        }
        s1, b1 = self._req("POST", "/v1/consumptions", scan)
        s2, b2 = self._req("POST", "/v1/consumptions", scan)
        self.assertEqual((s1, b1["duplicated"]), (201, False))
        self.assertEqual((s2, b2["duplicated"]), (200, True))
        self.assertEqual(b1["event"]["seq"], b2["event"]["seq"])
        status, body = self._req("GET", "/v1/lots/M-PORK")
        self.assertEqual(status, 200)
        self.assertEqual(body["qty_consumed"], 20)
        # 冻结后扣料被拒。
        s, b = self._req("POST", "/v1/allergens", {
            "scope": [{"kind": "lot", "ref": "M-PORK"}], "allergen": "大豆",
        })
        self.assertEqual(s, 201)
        s, b = self._req("POST", "/v1/consumptions", {
            "idem_key": "scan-http-2",
            "lot_code": "M-PORK", "order_id": "O1", "qty": 1,
            "process": "包馅", "equipment": "1号包馅机", "shift_id": "S1",
        })
        self.assertEqual(s, 409)
        # trace / freezes / replay 查询可用。
        s, b = self._req("GET", "/v1/trace?code=M-PORK")
        self.assertEqual(s, 200)
        s, b = self._req("GET", "/v1/freezes?active=1")
        self.assertEqual(s, 200)
        self.assertTrue(all(f["active"] for f in b["freezes"]))
        s, b = self._req("GET", "/v1/orders/O1/replay-decision")
        self.assertEqual(s, 200)
        self.assertEqual(b["order_id"], "O1")
        # 事件全量查询。
        s, b = self._req("GET", "/v1/events")
        self.assertEqual(s, 200)
        self.assertGreaterEqual(b["count"], 4)


if __name__ == "__main__":
    unittest.main()
