"""命令应用层：校验请求 → 追加事件；以及查询投影。

命令只做两件事：基于重放出的当前状态做业务校验，然后把事件追加到账本。
所有命令都接受 ``idem_key``：扫描枪断网补传时携带同一个键，重复请求
返回首条事件且 ``duplicated=true``，不会第二次扣料。

``business_time`` 可以显式传入（报告/工时补录历史），缺省取当前 UTC 时间；
``recorded_at`` 始终是实际落库时间，二者分别保留。
"""

import functools
import threading

from domain import (
    Conflict,
    EPS,
    NotFound,
    ValidationFailed,
    expand_freeze_scope,
    is_frozen,
    parse_time,
    production_reconciliation,
    replay,
    require_number,
    require_text,
    trace_code,
    utcnow_iso,
    worker_payroll,
)
from eventstore import EventStore

REF_KINDS = ("lot", "box")
ENTITY_KINDS = ("order", "lot", "box", "shipment", "worker")


def idempotent(fn):
    """所有写命令共用：业务校验前先按 idem_key 查首次结果。

    扫描枪断网补传时，即使首传之后库存状态已变化导致重试"本不该再通过"，
    也必须返回首条事件而不是再扣一次或报错——这是补传不重复扣料的关键。
    """

    @functools.wraps(fn)
    def wrapper(self, data):
        # 整个"查重 → 状态校验 → 追加"串行执行：并发扫描请求不会同时
        # 通过库存检查而导致超扣（ThreadingHTTPServer 每请求一线程）。
        with self._write_lock:
            if isinstance(data, dict) and data.get("idem_key"):
                key = require_text(data["idem_key"], "idem_key")
                existing = self.store.find_event(key)
                if existing is not None:
                    return existing, True
            return fn(self, data)

    return wrapper


class App:
    def __init__(self, store: EventStore):
        self.store = store
        self._write_lock = threading.RLock()

    # -- 基础 ---------------------------------------------------------------

    def state(self, up_to_seq=None):
        return replay(self.store.events(up_to_seq))

    def _emit(self, event_type, payload, data):
        event_id = data.get("idem_key")
        if event_id is not None:
            require_text(event_id, "idem_key")
        business_time = data.get("business_time") or utcnow_iso()
        parse_time(business_time, "business_time")
        event, duplicated = self.store.append(
            event_type,
            payload,
            event_id=event_id,
            business_time=business_time,
            recorded_at=utcnow_iso(),
        )
        return event, duplicated

    def _require_lot(self, state, code):
        lot = state.lots.get(code)
        if lot is None:
            raise NotFound(f"批次不存在：{code}")
        return lot

    def _require_box(self, state, code):
        box = state.boxes.get(code)
        if box is None:
            raise NotFound(f"箱码不存在：{code}")
        return box

    def _require_order(self, state, code):
        order = state.orders.get(code)
        if order is None:
            raise NotFound(f"订单不存在：{code}")
        return order

    def _require_worker(self, state, code):
        worker = state.workers.get(code)
        if worker is None:
            raise NotFound(f"村民/工人不存在：{code}")
        return worker

    def _assert_lot_usable(self, state, lot, qty, *, committed=0.0):
        if getattr(lot, "frozen", False) or is_frozen(state, lot.code):
            raise Conflict(f"批次 {lot.code} 已被冻结，禁止领用或装箱")
        if lot.available + EPS < qty:
            extra = f"（含其他订单已承诺未消耗 {round(committed, 3)}{lot.unit}）" if committed else ""
            raise Conflict(
                f"批次 {lot.code} 可用量不足：需要 {qty}{lot.unit}{extra}，"
                f"仅剩 {round(lot.available, 3)}{lot.unit}"
            )

    # -- 注册类 -------------------------------------------------------------

    @idempotent
    def register_material(self, data):
        code = require_text(data.get("lot_code"), "原料批次号")
        material = require_text(data.get("material"), "原料名称")
        qty = require_number(data.get("qty"), "数量")
        unit = require_text(data.get("unit"), "单位")
        flags = _clean_flags(data.get("allergen_flags"))
        state = self.state()
        if code in state.lots:
            raise Conflict(f"批次已存在：{code}")
        payload = {
            "lot_code": code,
            "material": material,
            "qty": qty,
            "unit": unit,
            "supplier": data.get("supplier", ""),
            "allergen_flags": flags,
        }
        return self._emit("material_registered", payload, data)

    @idempotent
    def register_worker(self, data):
        code = require_text(data.get("worker_code"), "工人编号")
        name = require_text(data.get("name"), "姓名")
        rate = require_number(data.get("hourly_rate", 0), "时薪", positive=False)
        state = self.state()
        if code in state.workers:
            raise Conflict(f"工人已存在：{code}")
        payload = {"worker_code": code, "name": name, "hourly_rate": rate}
        return self._emit("worker_registered", payload, data)

    @idempotent
    def receive_order(self, data):
        order_id = require_text(data.get("order_id"), "订单号")
        product = require_text(data.get("product"), "产品")
        qty = require_number(data.get("qty"), "订单数量")
        unit = require_text(data.get("unit"), "单位")
        if data.get("due_at"):
            parse_time(data["due_at"], "交货时间")
        state = self.state()
        if order_id in state.orders:
            raise Conflict(f"订单已存在：{order_id}")
        payload = {
            "order_id": order_id,
            "customer": data.get("customer", ""),
            "store": data.get("store", ""),
            "product": product,
            "qty": qty,
            "unit": unit,
            "due_at": data.get("due_at", ""),
            "allergen_flags": _clean_flags(data.get("allergen_flags")),
        }
        return self._emit("order_received", payload, data)

    @idempotent
    def cancel_order(self, data):
        order_id = require_text(data.get("order_id"), "订单号")
        state = self.state()
        order = self._require_order(state, order_id)
        if order.status == "cancelled":
            raise Conflict("订单已取消")
        payload = {"order_id": order_id, "reason": data.get("reason", "")}
        return self._emit("order_cancelled", payload, data)

    # -- 排产承诺 -----------------------------------------------------------

    @idempotent
    def schedule_production(self, data):
        """把订单承诺落到原料批次、设备、工序和班次。

        plan 结构：
          processes:    [{process, equipment, shift_id}] 工序顺序与班次
          allocations:  [{lot_code, qty, unit}] 占用哪些原料批次
          requirements: [{material, qty, unit}] 可选，用于核对投料是否足额
          equipment:    主设备（可选）；workers: 预排工人（可选）
        """
        order_id = require_text(data.get("order_id"), "订单号")
        state = self.state()
        order = self._require_order(state, order_id)
        if order.status == "cancelled":
            raise Conflict("订单已取消，不能排产")

        processes = data.get("processes")
        if not isinstance(processes, list) or not processes:
            raise ValidationFailed("排产必须包含至少一道工序 processes")
        norm_processes = []
        for item in processes:
            if not isinstance(item, dict):
                raise ValidationFailed("工序定义必须是对象")
            norm_processes.append(
                {
                    "process": require_text(item.get("process"), "工序名称"),
                    "equipment": require_text(item.get("equipment"), "设备"),
                    "shift_id": require_text(item.get("shift_id"), "班次"),
                }
            )
        if len({p["process"] for p in norm_processes}) != len(norm_processes):
            raise ValidationFailed("同一排产中工序名称不能重复")

        allocations = data.get("allocations", [])
        if not isinstance(allocations, list) or not allocations:
            raise ValidationFailed("排产必须承诺至少一个原料批次 allocations")
        norm_alloc = []
        alloc_by_material = {}
        grouped = {}
        ordered_codes = []
        for item in allocations:
            if not isinstance(item, dict):
                raise ValidationFailed("原料分配必须是对象")
            lot = self._require_lot(state, require_text(item.get("lot_code"), "原料批次号"))
            qty = require_number(item.get("qty"), "分配数量")
            unit = require_text(item.get("unit", lot.unit), "单位")
            if unit != lot.unit:
                raise ValidationFailed(
                    f"批次 {lot.code} 单位为 {lot.unit}，分配写成 {unit}"
                )
            if lot.code in grouped:
                if grouped[lot.code]["unit"] != unit:
                    raise ValidationFailed(f"批次 {lot.code} 出现冲突单位")
                grouped[lot.code]["qty"] += qty
            else:
                grouped[lot.code] = {"lot_code": lot.code, "qty": qty, "unit": unit,
                                     "material": lot.material}
                ordered_codes.append(lot.code)
        for code in ordered_codes:
            entry = grouped[code]
            lot = state.lots[code]
            # 可用量还要扣掉其他订单（及本单旧计划）已承诺但尚未消耗的部分。
            committed = 0.0
            for other in state.orders.values():
                if not other.plan or other.status == "cancelled":
                    continue
                if other.order_id == order_id:
                    continue  # 本单重排，旧承诺由本次提交整体替换
                for alloc in other.plan["allocations"]:
                    if alloc["lot_code"] != code:
                        continue
                    consumed = sum(
                        c["qty"]
                        for c in lot.consumption
                        if c.get("order_id") == other.order_id
                    )
                    committed += max(alloc["qty"] - consumed, 0.0)
            self._assert_lot_usable(
                state, lot, entry["qty"] + committed, committed=committed
            )
            norm_alloc.append(entry)
            bucket = alloc_by_material.setdefault(
                entry["material"], {"qty": 0.0, "unit": entry["unit"]}
            )
            bucket["qty"] += entry["qty"]

        requirements = data.get("requirements", [])
        if requirements:
            if not isinstance(requirements, list):
                raise ValidationFailed("requirements 必须是列表")
            for req in requirements:
                material = require_text(req.get("material"), "需求原料")
                need = require_number(req.get("qty"), "需求数量")
                unit = require_text(req.get("unit"), "需求单位")
                got = alloc_by_material.get(material)
                if got is None or got["qty"] + EPS < need or got["unit"] != unit:
                    raise Conflict(
                        f"原料 {material} 承诺不足：需要 {need}{unit}，"
                        f"已分配 {0 if got is None else round(got['qty'], 3)}"
                        f"{'' if got is None else got['unit']}"
                    )

        workers = data.get("workers", [])
        if workers and not isinstance(workers, list):
            raise ValidationFailed("workers 必须是工人编号列表")

        plan = {
            "order_id": order_id,
            "equipment": require_text(data.get("equipment", norm_processes[0]["equipment"]),
                                      "主设备"),
            "processes": norm_processes,
            "allocations": norm_alloc,
            "requirements": requirements or [],
            "workers": [require_text(w, "工人编号") for w in workers],
        }
        payload = {"order_id": order_id, "plan": plan}
        return self._emit("production_scheduled", payload, data)

    def replay_order_decision(self, order_id, version=-1):
        """重放某订单当时的排产决定：把账本截到该版排产事件处重新归约。

        version 为 schedule_history 的 0 基下标，默认最新一版；
        早期版本仍可逐版重放（订单详情的 schedule_history 里保留全部版本）。
        """
        state = self.state()
        order = self._require_order(state, order_id)
        if order.planned_seq is None:
            raise NotFound(f"订单 {order_id} 尚未排产")
        history = order.schedule_history
        try:
            chosen = history[version]
        except IndexError:
            raise NotFound(f"订单 {order_id} 不存在该排产版本：{version}")
        seq = chosen["seq"]
        as_of = self.state(up_to_seq=seq)
        historical_order = as_of.orders[order_id]
        plan = chosen["plan"]
        lots_view = []
        for alloc in plan["allocations"]:
            lot = as_of.lots.get(alloc["lot_code"])
            lots_view.append(
                {
                    "lot_code": alloc["lot_code"],
                    "material": alloc["material"],
                    "allocated_qty": alloc["qty"],
                    "unit": alloc["unit"],
                    "available_then": round(lot.available, 3) if lot else None,
                    "allergen_flags": lot.allergen_flags if lot else [],
                }
            )
        return {
            "order_id": order_id,
            "replayed_at_seq": order.planned_seq,
            "business_time": historical_order.schedule_history[-1]["business_time"],
            "order_status_then": historical_order.status,
            "plan": plan,
            "material_lots_then": lots_view,
        }

    # -- 生产执行 -----------------------------------------------------------

    @idempotent
    def consume_material(self, data):
        lot_code = require_text(data.get("lot_code"), "原料批次号")
        order_id = require_text(data.get("order_id"), "订单号")
        qty = require_number(data.get("qty"), "扣料数量")
        state = self.state()
        lot = self._require_lot(state, lot_code)
        self._require_order(state, order_id)
        unit = require_text(data.get("unit", lot.unit), "单位")
        process = require_text(data.get("process"), "工序")
        equipment = require_text(data.get("equipment"), "设备")
        shift_id = require_text(data.get("shift_id"), "班次")
        if unit != lot.unit:
            raise ValidationFailed(f"批次 {lot.code} 单位为 {lot.unit}，扣料写成 {unit}")
        self._assert_lot_usable(state, lot, qty)
        payload = {
            "lot_code": lot.code,
            "order_id": order_id,
            "qty": qty,
            "unit": unit,
            "process": process,
            "equipment": equipment,
            "shift_id": shift_id,
            "scanner_id": data.get("scanner_id", ""),
        }
        return self._emit("material_consumed", payload, data)

    def _normalize_inputs(self, state, inputs):
        if not isinstance(inputs, list) or not inputs:
            raise ValidationFailed("至少需要一个投入批次 inputs")
        grouped = {}
        ordered_codes = []
        for item in inputs:
            code = require_text(item.get("lot_code"), "投入批次号")
            lot = self._require_lot(state, code)
            qty = require_number(item.get("qty"), "投入数量")
            unit = require_text(item.get("unit", lot.unit), "单位")
            if unit != lot.unit:
                raise ValidationFailed(f"批次 {code} 单位为 {lot.unit}，投入写成 {unit}")
            if code in grouped:
                if grouped[code]["unit"] != unit:
                    raise ValidationFailed(f"批次 {code} 出现冲突单位")
                grouped[code]["qty"] += qty
            else:
                grouped[code] = {"lot_code": code, "qty": qty, "unit": unit}
                ordered_codes.append(code)
        # 同一快照下对同批次的多条投入要合并计数后再验可用量。
        for code in ordered_codes:
            self._assert_lot_usable(state, state.lots[code], grouped[code]["qty"])
        return [grouped[code] for code in ordered_codes]

    def _process_context(self, data):
        return {
            "process": require_text(data.get("process"), "工序"),
            "equipment": require_text(data.get("equipment"), "设备"),
            "shift_id": require_text(data.get("shift_id"), "班次"),
            "worker_code": data.get("worker_code"),
        }

    @idempotent
    def split_lot(self, data):
        """一批面团拆开加工：从父批次划出数量到新批次，留下谱系边。"""
        parent_code = require_text(data.get("parent_lot"), "父批次号")
        child_code = require_text(data.get("child_lot"), "拆出批次号")
        qty = require_number(data.get("qty"), "拆分数量")
        order_id = require_text(data.get("order_id"), "订单号")
        state = self.state()
        parent = self._require_lot(state, parent_code)
        self._require_order(state, order_id)
        if child_code in state.lots:
            raise Conflict(f"拆出批次已存在：{child_code}")
        unit = require_text(data.get("unit", parent.unit), "单位")
        if unit != parent.unit:
            raise ValidationFailed(f"批次 {parent.code} 单位为 {parent.unit}")
        self._assert_lot_usable(state, parent, qty)
        ctx = self._process_context(data)
        if ctx["worker_code"]:
            self._require_worker(state, ctx["worker_code"])
        payload = {
            "parent_lot": parent.code,
            "child_lot": child_code,
            "order_id": order_id,
            "qty": qty,
            "unit": unit,
            **ctx,
        }
        return self._emit("lot_split", payload, data)

    @idempotent
    def merge_lots(self, data):
        """多个批次合成一个新批次（合箱前的合批同样走这里）。"""
        child_code = require_text(data.get("child_lot"), "合批批次号")
        order_id = require_text(data.get("order_id"), "订单号")
        state = self.state()
        self._require_order(state, order_id)
        if child_code in state.lots:
            raise Conflict(f"合批批次已存在：{child_code}")
        inputs = self._normalize_inputs(state, data.get("parents"))
        ctx = self._process_context(data)
        if ctx["worker_code"]:
            self._require_worker(state, ctx["worker_code"])
        payload = {
            "child_lot": child_code,
            "order_id": order_id,
            "parents": inputs,
            "product": data.get("product", ""),
            **ctx,
        }
        return self._emit("lots_merged", payload, data)

    @idempotent
    def produce_lot(self, data):
        """记录某道工序的产出（搅拌、包馅、烘烤等），可同时核销投入批次。"""
        order_id = require_text(data.get("order_id"), "订单号")
        product = require_text(data.get("product"), "产出产品")
        qty = require_number(data.get("qty"), "产出数量")
        unit = require_text(data.get("unit"), "单位")
        state = self.state()
        self._require_order(state, order_id)
        lot_code = data.get("lot_code")
        if lot_code:
            lot_code = require_text(lot_code, "产出批次号")
            if lot_code in state.lots and state.lots[lot_code].qty_produced > 0:
                # 允许向已存在批次追加产出（同一批次跨班加工），但不允许换产品。
                if state.lots[lot_code].product and state.lots[lot_code].product != product:
                    raise Conflict(f"批次 {lot_code} 产品与登记不一致")
        else:
            raise ValidationFailed("产出批次号 lot_code 不能为空")
        inputs = self._normalize_inputs(state, data.get("inputs", []))
        ctx = self._process_context(data)
        worker_code = None
        piece_rate = 0.0
        if ctx["worker_code"]:
            worker = self._require_worker(state, ctx["worker_code"])
            worker_code = worker.code
            piece_rate = require_number(data.get("piece_rate", 0), "计件单价", positive=False)
        payload = {
            "lot_code": lot_code,
            "order_id": order_id,
            "product": product,
            "qty": qty,
            "unit": unit,
            "inputs": inputs,
            "kind": data.get("kind", "semi"),
            "worker_code": worker_code,
            "piece_rate": piece_rate,
            "process": ctx["process"],
            "equipment": ctx["equipment"],
            "shift_id": ctx["shift_id"],
        }
        return self._emit("lot_produced", payload, data)

    @idempotent
    def finish_lot(self, data):
        code = require_text(data.get("lot_code"), "批次号")
        state = self.state()
        lot = self._require_lot(state, code)
        if lot.state == "finished":
            raise Conflict(f"批次 {code} 已标记完工")
        if lot.qty_produced <= 0 and lot.qty_received <= 0:
            raise Conflict(f"批次 {code} 尚无产出，不能完工")
        payload = {"lot_code": code, "kind": data.get("kind"), "product": data.get("product")}
        return self._emit("lot_finished", payload, data)

    # -- 装箱 ---------------------------------------------------------------

    @idempotent
    def pack_box(self, data):
        """成品装箱，支持把多个成品批次合入同一个箱码。"""
        box_code = require_text(data.get("box_code"), "箱码")
        order_id = require_text(data.get("order_id"), "订单号")
        state = self.state()
        self._require_order(state, order_id)
        if box_code in state.boxes:
            raise Conflict(f"箱码已存在：{box_code}")

        items = data.get("items")
        if not items:
            lot_code = require_text(data.get("lot_code"), "成品批次号")
            qty = require_number(data.get("qty"), "装箱数量")
            unit = require_text(data.get("unit", state.lots.get(lot_code).unit if lot_code in state.lots else ""), "单位")
            items = [{"lot_code": lot_code, "qty": qty, "unit": unit}]
        normalized = []
        units = set()
        total = 0.0
        grouped = {}
        ordered_codes = []
        for item in items:
            lot = self._require_lot(
                state, require_text(item.get("lot_code"), "成品批次号")
            )
            qty = require_number(item.get("qty"), "装箱数量")
            unit = require_text(item.get("unit", lot.unit), "单位")
            if lot.kind != "finished":
                raise Conflict(f"批次 {lot.code} 尚未标记为成品，不能装箱")
            if unit != lot.unit:
                raise ValidationFailed(f"批次 {lot.code} 单位为 {lot.unit}")
            if lot.code in grouped:
                grouped[lot.code]["qty"] += qty
            else:
                grouped[lot.code] = {"lot_code": lot.code, "qty": qty, "unit": unit}
                ordered_codes.append(lot.code)
        for code in ordered_codes:
            entry = grouped[code]
            self._assert_lot_usable(state, state.lots[code], entry["qty"])
            normalized.append(entry)
            units.add(entry["unit"])
            total += entry["qty"]
        if len(units) != 1:
            raise ValidationFailed("合箱各批次单位必须一致")
        box_qty = data.get("qty", total)
        require_number(box_qty, "箱码数量")
        if abs(float(box_qty) - total) > EPS:
            raise ValidationFailed(f"箱码数量 {box_qty} 与各批次合计 {total} 不一致")
        worker_code = data.get("worker_code")
        if worker_code:
            self._require_worker(state, worker_code)
        packaging = None
        pkg = data.get("packaging")
        if pkg:
            if not isinstance(pkg, dict):
                raise ValidationFailed("packaging 必须为包装材料核销对象")
            pkg_lot = self._require_lot(
                state, require_text(pkg.get("lot_code"), "包装材料批次")
            )
            pkg_qty = require_number(pkg.get("qty"), "包装材料数量")
            pkg_unit = require_text(pkg.get("unit", pkg_lot.unit), "包装单位")
            if pkg_unit != pkg_lot.unit:
                raise ValidationFailed(f"包装批次 {pkg_lot.code} 单位为 {pkg_lot.unit}")
            self._assert_lot_usable(state, pkg_lot, pkg_qty)
            packaging = {"lot_code": pkg_lot.code, "qty": pkg_qty, "unit": pkg_unit}
        payload = {
            "box_code": box_code,
            "order_id": order_id,
            "qty": float(box_qty),
            "unit": next(iter(units)),
            "items": normalized,
            "packaging": packaging,
            "packaging_shift_id": data.get("packaging_shift_id", data.get("shift_id", ""))
            if packaging
            else "",
            "package_material": data.get("package_material", ""),
            "worker_code": worker_code,
        }
        return self._emit("box_packed", payload, data)

    # -- 质检 / 冻结 --------------------------------------------------------

    def _parse_scope(self, state, data):
        scope = data.get("scope")
        if not isinstance(scope, list) or not scope:
            raise ValidationFailed("scope 必须为 [{\"kind\": \"lot|box\", \"ref\": ...}]")
        refs = []
        for item in scope:
            if not isinstance(item, dict):
                raise ValidationFailed("scope 条目必须是对象")
            kind = require_text(item.get("kind"), "scope.kind")
            ref = require_text(item.get("ref"), "scope.ref")
            if kind not in REF_KINDS:
                raise ValidationFailed("scope.kind 只能是 lot 或 box")
            if kind == "lot":
                self._require_lot(state, ref)
            else:
                self._require_box(state, ref)
            refs.append((kind, ref))
        return refs

    @idempotent
    def quality_inspection(self, data):
        """登记质检结果；不合格/异常立即级联冻结并列出已发门店。"""
        status = require_text(data.get("status"), "质检结论")
        if status not in ("passed", "failed", "anomaly"):
            raise ValidationFailed("质检结论只能是 passed/failed/anomaly")
        state = self.state()
        scope_refs = self._parse_scope(state, data)
        affected_lots, affected_boxes, stores = [], [], []
        if status in ("failed", "anomaly"):
            affected_lots, affected_boxes, stores = expand_freeze_scope(state, scope_refs)
        payload = {
            "scope_refs": scope_refs,
            "status": status,
            "note": data.get("note", ""),
            "inspector": data.get("inspector", ""),
            "affected_lots": affected_lots,
            "affected_boxes": affected_boxes,
            "stores": stores,
        }
        return self._emit("quality_inspection", payload, data)

    @idempotent
    def allergen_alert(self, data):
        state = self.state()
        scope_refs = self._parse_scope(state, data)
        allergen = require_text(data.get("allergen"), "过敏原名称")
        affected_lots, affected_boxes, stores = expand_freeze_scope(state, scope_refs)
        payload = {
            "scope_refs": scope_refs,
            "allergen": allergen,
            "reason": data.get("note", f"过敏原告警：{allergen}"),
            "note": data.get("note", ""),
            "operator": data.get("operator", ""),
            "affected_lots": affected_lots,
            "affected_boxes": affected_boxes,
            "stores": stores,
            "status": "anomaly",
        }
        return self._emit("allergen_alert", payload, data)

    @idempotent
    def manual_freeze(self, data):
        state = self.state()
        scope_refs = self._parse_scope(state, data)
        reason = require_text(data.get("reason"), "冻结原因")
        affected_lots, affected_boxes, stores = expand_freeze_scope(state, scope_refs)
        payload = {
            "scope_refs": scope_refs,
            "reason": reason,
            "operator": data.get("operator", ""),
            "affected_lots": affected_lots,
            "affected_boxes": affected_boxes,
            "stores": stores,
            "status": "anomaly",
        }
        return self._emit("freeze_manual", payload, data)

    @idempotent
    def release_freeze(self, data):
        refs = data.get("refs")
        if not isinstance(refs, list) or not refs:
            raise ValidationFailed("refs 必须为要解冻的批次/箱码列表")
        state = self.state()
        norm = []
        for ref in refs:
            ref = require_text(ref, "解冻对象")
            if ref not in state.lots and ref not in state.boxes:
                raise NotFound(f"批次或箱码不存在：{ref}")
            norm.append(ref)
        payload = {
            "refs": norm,
            "reason": require_text(data.get("reason"), "解冻原因"),
            "operator": data.get("operator", ""),
        }
        return self._emit("freeze_released", payload, data)

    # -- 返工 ---------------------------------------------------------------

    @idempotent
    def start_rework(self, data):
        rework_id = require_text(data.get("rework_id"), "返工单号")
        order_id = require_text(data.get("order_id"), "订单号")
        ref_kind = require_text(data.get("ref_kind"), "返工对象类型")
        ref = require_text(data.get("ref"), "返工对象")
        if ref_kind not in REF_KINDS:
            raise ValidationFailed("ref_kind 只能是 lot 或 box")
        state = self.state()
        self._require_order(state, order_id)
        if rework_id in state.reworks:
            raise Conflict(f"返工单已存在：{rework_id}")
        if ref_kind == "lot":
            self._require_lot(state, ref)
        else:
            self._require_box(state, ref)
        ctx = self._process_context(data)
        payload = {
            "rework_id": rework_id,
            "order_id": order_id,
            "ref_kind": ref_kind,
            "ref": ref,
            "reason": data.get("reason", ""),
            **ctx,
        }
        return self._emit("rework_started", payload, data)

    @idempotent
    def complete_rework(self, data):
        rework_id = require_text(data.get("rework_id"), "返工单号")
        state = self.state()
        rw = state.reworks.get(rework_id)
        if rw is None:
            raise NotFound(f"返工单不存在：{rework_id}")
        if rw.state == "done":
            raise Conflict(f"返工单 {rework_id} 已完成，完成记录只可追加不可覆盖")
        result_lot = data.get("result_lot")
        qty = data.get("qty")
        unit = data.get("unit", "")
        if result_lot:
            self._require_lot(state, result_lot)
        if qty is not None:
            qty = require_number(qty, "返工后数量")
        payload = {
            "rework_id": rework_id,
            "result_lot": result_lot,
            "qty": qty,
            "unit": unit,
            "note": data.get("note", ""),
            "worker_code": data.get("worker_code", rw.worker_code),
        }
        return self._emit("rework_completed", payload, data)

    # -- 出库 / 退货 --------------------------------------------------------

    @idempotent
    def ship(self, data):
        shipment_id = require_text(data.get("shipment_id"), "出库单号")
        order_id = require_text(data.get("order_id"), "订单号")
        store = require_text(data.get("store"), "门店")
        box_codes = data.get("boxes")
        if not isinstance(box_codes, list) or not box_codes:
            raise ValidationFailed("boxes 必须为箱码列表")
        state = self.state()
        self._require_order(state, order_id)
        if shipment_id in state.shipments:
            raise Conflict(f"出库单已存在：{shipment_id}")
        items = []
        for raw in box_codes:
            code = require_text(raw, "箱码")
            box = self._require_box(state, code)
            if box.frozen or is_frozen(state, code):
                raise Conflict(f"箱码 {code} 处于冻结状态，禁止出库")
            if box.shipment_id is not None:
                raise Conflict(f"箱码 {code} 已随 {box.shipment_id} 出库")
            if box.order_id != order_id:
                raise Conflict(f"箱码 {code} 不属于订单 {order_id}")
            items.append({"box_code": code, "qty": box.qty, "unit": box.unit})
        payload = {
            "shipment_id": shipment_id,
            "order_id": order_id,
            "store": store,
            "items": items,
            "carrier": data.get("carrier", ""),
            "operator": data.get("operator", ""),
        }
        return self._emit("outbound_shipped", payload, data)

    @idempotent
    def receive_return(self, data):
        shipment_id = require_text(data.get("shipment_id"), "出库单号")
        state = self.state()
        ship = state.shipments.get(shipment_id)
        if ship is None:
            raise NotFound(f"出库单不存在：{shipment_id}")
        raw_items = data.get("items")
        if not isinstance(raw_items, list) or not raw_items:
            raise ValidationFailed("items 必须为退货明细")
        items = []
        for item in raw_items:
            code = require_text(item.get("box_code"), "箱码")
            qty = require_number(item.get("qty"), "退货数量")
            box = self._require_box(state, code)
            if box.shipment_id != shipment_id:
                raise Conflict(f"箱码 {code} 不属于出库单 {shipment_id}")
            shipped_qty = next(it["qty"] for it in ship.items if it["box_code"] == code)
            already = ship.returned.get(code, 0)
            if already + qty > shipped_qty + EPS:
                raise Conflict(
                    f"箱码 {code} 退货超额：出库 {shipped_qty}{box.unit}，"
                    f"已退 {already}，本次再退 {qty}"
                )
            items.append({"box_code": code, "qty": qty, "unit": box.unit})
        payload = {
            "shipment_id": shipment_id,
            "order_id": ship.order_id,
            "store": data.get("store", ship.store),
            "items": items,
            "reason": data.get("reason", ""),
            "operator": data.get("operator", ""),
        }
        return self._emit("return_received", payload, data)

    # -- 报告补录 / 工时 ----------------------------------------------------

    @idempotent
    def add_report(self, data):
        """质检/生产等报告补录：始终追加新事件，不改动或覆盖既有报告。"""
        entity_kind = require_text(data.get("entity_kind"), "实体类型")
        entity_id = require_text(data.get("entity_id"), "实体编号")
        if entity_kind not in ENTITY_KINDS:
            raise ValidationFailed(f"entity_kind 只能是 {'/'.join(ENTITY_KINDS)}")
        report_type = require_text(data.get("report_type"), "报告类型")
        content = data.get("content")
        if content is None or (isinstance(content, str) and not content.strip()):
            raise ValidationFailed("报告内容 content 不能为空")
        state = self.state()
        collection = {
            "order": state.orders,
            "lot": state.lots,
            "box": state.boxes,
            "shipment": state.shipments,
            "worker": state.workers,
        }[entity_kind]
        if entity_id not in collection:
            raise NotFound(f"{entity_kind} 不存在：{entity_id}")
        documents = data.get("documents", [])
        if documents and not isinstance(documents, list):
            raise ValidationFailed("documents 必须是附件标识列表")
        payload = {
            "entity_kind": entity_kind,
            "entity_id": entity_id,
            "report_type": report_type,
            "content": content,
            "author": data.get("author", ""),
            "documents": documents,
        }
        return self._emit("report_added", payload, data)

    @idempotent
    def record_effort(self, data):
        code = require_text(data.get("worker_code"), "工人编号")
        shift_id = require_text(data.get("shift_id"), "班次")
        hours = require_number(data.get("hours"), "工时（小时）")
        state = self.state()
        worker = self._require_worker(state, code)
        rate = require_number(data.get("hourly_rate", worker.hourly_rate), "时薪",
                              positive=False)
        if data.get("order_id"):
            self._require_order(state, data["order_id"])
        payload = {
            "worker_code": code,
            "shift_id": shift_id,
            "hours": hours,
            "hourly_rate": rate,
            "order_id": data.get("order_id"),
            "note": data.get("note", ""),
        }
        return self._emit("worker_effort", payload, data)

    # -- 查询 ---------------------------------------------------------------

    def payroll(self, worker_code):
        return worker_payroll(self.state(), worker_code)

    def reconciliation(self, order_id=None):
        return production_reconciliation(self.state(), order_id)

    def trace(self, code):
        state = self.state()
        if code not in state.lots and code not in state.boxes:
            raise NotFound(f"批次或箱码不存在：{code}")
        if code in state.boxes:
            box = state.boxes[code]
            result = {
                "code": code,
                "kind": "box",
                "box": box_view(box),
                "source_traces": [trace_code(state, sl) for sl in box.source_lots or [box.lot_code]],
            }
            lots = set()
            for t in result["source_traces"]:
                lots.update(t["related_lots"])
            result["related_lots"] = sorted(lots)
            return result
        result = trace_code(state, code)
        result["kind"] = "lot"
        return result

    def timeline(self, code):
        """某批次/箱码经历过的全部工序与交接记录（村民换班后据此查接手人）。"""
        state = self.state()
        if code not in state.lots and code not in state.boxes:
            raise NotFound(f"批次或箱码不存在：{code}")
        rows = []
        for event in state.events:
            if not _event_references(event["payload"], code):
                continue
            p = event["payload"]
            rows.append(
                {
                    "seq": event["seq"],
                    "event_type": event["type"],
                    "business_time": event["business_time"],
                    "process": p.get("process")
                    or ("包装" if event["type"] == "box_packed" else ""),
                    "equipment": p.get("equipment", ""),
                    "shift_id": p.get("shift_id")
                    or p.get("packaging_shift_id", ""),
                    "worker_code": p.get("worker_code", ""),
                    "order_id": p.get("order_id", ""),
                    "qty": p.get("qty"),
                    "note": p.get("note", p.get("reason", "")),
                }
            )
        return {"code": code, "events": rows}


def _event_references(payload, code):
    """判断事件是否与某批次/箱码直接相关，用于工序时间线。"""
    keys = ("lot_code", "parent_lot", "child_lot", "box_code")
    if any(payload.get(k) == code for k in keys):
        return True
    for item in payload.get("inputs", []) + payload.get("parents", []) + payload.get(
        "items", []
    ):
        if isinstance(item, dict) and item.get("lot_code") == code:
            return True
        if isinstance(item, dict) and item.get("box_code") == code:
            return True
    for kind, ref in payload.get("scope_refs", []):
        if ref == code:
            return True
    packaging = payload.get("packaging")
    if isinstance(packaging, dict) and packaging.get("lot_code") == code:
        return True
    return False


def _clean_flags(value):
    if value is None:
        return []
    if not isinstance(value, list):
        raise ValidationFailed("allergen_flags 必须是字符串列表")
    flags = sorted({require_text(v, "过敏原标识") for v in value})
    return flags


# ---------------------------------------------------------------------------
# 只读投影
# ---------------------------------------------------------------------------


def lot_view(lot, state=None):
    committed = 0.0
    if state is not None:
        for order in state.orders.values():
            if not order.plan or order.status == "cancelled":
                continue
            for alloc in order.plan["allocations"]:
                if alloc["lot_code"] != lot.code:
                    continue
                consumed = sum(
                    c["qty"]
                    for c in lot.consumption
                    if c.get("order_id") == order.order_id
                )
                committed += max(alloc["qty"] - consumed, 0.0)
    return {
        "lot_code": lot.code,
        "kind": lot.kind,
        "material": lot.material,
        "product": lot.product,
        "unit": lot.unit,
        "supplier": lot.supplier,
        "allergen_flags": lot.allergen_flags,
        "state": lot.state,
        "frozen": lot.frozen,
        "order_id": lot.order_id,
        "qty_received": round(lot.qty_received, 3),
        "qty_produced": round(lot.qty_produced, 3),
        "qty_consumed": round(lot.qty_consumed, 3),
        "qty_packed": round(lot.qty_packed, 3),
        "qty_available": round(lot.available, 3),
        "qty_committed_unconsumed": round(committed, 3),
        "qty_available_to_promise": round(max(lot.available - committed, 0.0), 3),
        "created_at": lot.created_at,
        "consumption_records": lot.consumption,
        "quality_records": lot.quality,
        "reports": lot.reports,
    }


def box_view(box):
    return {
        "box_code": box.code,
        "order_id": box.order_id,
        "lot_code": box.lot_code,
        "source_lots": box.source_lots,
        "source_items": box.source_items,
        "qty": box.qty,
        "unit": box.unit,
        "state": box.state,
        "frozen": box.frozen,
        "packed_at": box.packed_at,
        "shipment_id": box.shipment_id,
        "store": box.store,
        "package_material": box.package_material,
        "worker_code": box.worker_code,
        "quality_records": box.quality,
        "reports": box.reports,
    }


def order_view(order, *, detailed=False):
    data = {
        "order_id": order.order_id,
        "customer": order.customer,
        "store": order.store,
        "product": order.product,
        "qty": order.qty,
        "unit": order.unit,
        "due_at": order.due_at,
        "allergen_flags": order.allergen_flags,
        "status": order.status,
        "created_at": order.created_at,
        "scheduled": order.plan is not None,
        "plan": order.plan,
    }
    if detailed:
        data.update(
            {
                "schedule_history": order.schedule_history,
                "shipments": order.shipments,
                "returns": order.returns,
                "reworks": order.reworks,
                "quality_records": order.quality,
                "reports": order.reports,
            }
        )
    return data


def freeze_view(fr, event):
    return {
        "freeze_id": fr.id,
        "source": fr.source,
        "reason": fr.reason,
        "active": fr.active,
        "business_time": fr.business_time,
        "operator": fr.operator,
        "scope_refs": fr.refs,
        "affected_lots": event["payload"].get("affected_lots", []),
        "affected_boxes": event["payload"].get("affected_boxes", []),
        "stores": event["payload"].get("stores", []),
        "released_refs": sorted(fr.released_refs),
        "releases": fr.releases,
    }


def worker_view(worker):
    return {
        "worker_code": worker.code,
        "name": worker.name,
        "hourly_rate": worker.hourly_rate,
        "efforts": worker.efforts,
        "outputs": worker.outputs,
        "reports": worker.reports,
    }


def shipment_view(ship):
    return {
        "shipment_id": ship.shipment_id,
        "order_id": ship.order_id,
        "store": ship.store,
        "carrier": ship.carrier,
        "operator": ship.operator,
        "business_time": ship.business_time,
        "state": ship.state,
        "items": ship.items,
        "returned": ship.returned,
    }


def all_views(state):
    return {
        "lots": [lot_view(l, state) for l in sorted(state.lots.values(), key=lambda x: x.code)],
        "boxes": [box_view(b) for b in sorted(state.boxes.values(), key=lambda x: x.code)],
        "orders": [order_view(o) for o in sorted(state.orders.values(), key=lambda x: x.order_id)],
        "workers": [worker_view(w) for w in sorted(state.workers.values(), key=lambda x: x.code)],
        "shipments": [
            shipment_view(s) for s in sorted(state.shipments.values(), key=lambda x: x.shipment_id)
        ],
    }
