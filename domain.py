"""产销协同领域层。

围绕"订单 → 原料批次 → 工序/设备/班次 → 半成品/成品批次 → 箱码 → 门店"
这条链维护当前状态。状态完全由 ``eventstore`` 中的事件重放得到：
任何时候清空内存重新 fold 一遍，结果一致。

关键约束：

* 批次支持拆分（一批面团分开加工）与合批（多批成品合箱），谱系边只增；
* 过敏原/检验异常沿谱系双向级联冻结相关批次与箱码，并给出已发门店；
* 质检、返工、出库、退货、报告都是追加记录，状态字段只反映最新结论，
  历史明细永远保留在对应实体的事件列表里。
"""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal

EPS = 1e-6


class DomainError(Exception):
    """业务规则冲突（HTTP 层映射为 4xx）。"""


class NotFound(DomainError):
    """引用的实体不存在。"""


class Conflict(DomainError):
    """状态冲突，例如冻结中出库、库存不足、重复扣料等。"""


class ValidationFailed(DomainError):
    """请求内容不合法。"""


def utcnow_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_time(value, field_name="时间"):
    """宽松校验 ISO 时间字符串，失败时给出中文错误。"""
    if not isinstance(value, str) or not value:
        raise ValidationFailed(f"{field_name}必须是 ISO 时间字符串")
    text = value.replace("Z", "+00:00")
    try:
        datetime.fromisoformat(text)
    except ValueError as exc:
        raise ValidationFailed(f"{field_name}格式无法识别：{value}") from exc
    return value


def require_number(value, name, *, positive=True):
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValidationFailed(f"{name}必须是数字")
    if positive and value <= 0:
        raise ValidationFailed(f"{name}必须大于 0")
    return float(value)


def require_text(value, name):
    if not isinstance(value, str) or not value.strip():
        raise ValidationFailed(f"{name}不能为空")
    return value.strip()


# ---------------------------------------------------------------------------
# 状态结构
# ---------------------------------------------------------------------------


@dataclass
class Lot:
    code: str
    kind: str  # material 原料 | semi 半成品 | finished 成品
    material: str = ""
    product: str = ""
    qty_received: float = 0.0  # 原料入库量
    qty_produced: float = 0.0  # 产出量（半成品/成品）
    qty_consumed: float = 0.0  # 作为投入被消耗
    qty_packed: float = 0.0  # 已装箱（成品）
    unit: str = ""
    supplier: str = ""
    allergen_flags: list = field(default_factory=list)
    state: str = "registered"  # registered/in_progress/finished/consumed
    order_id: str | None = None
    created_at: str = ""
    frozen: bool = False
    consumption: list = field(default_factory=list)  # 只增扣料明细
    reports: list = field(default_factory=list)
    quality: list = field(default_factory=list)

    @property
    def available(self):
        """可继续投入或装箱的数量。"""
        stock = self.qty_received + self.qty_produced
        return stock - self.qty_consumed - self.qty_packed


@dataclass
class Box:
    code: str
    lot_code: str
    order_id: str
    qty: float
    unit: str
    packed_at: str
    worker_code: str | None = None
    package_material: str = ""
    state: str = "packed"  # packed/frozen/released/shipped/returned
    frozen: bool = False
    shipment_id: str | None = None
    store: str | None = None
    source_lots: list = field(default_factory=list)  # 合箱时可能来自多个成品批次
    source_items: list = field(default_factory=list)  # [{lot_code,qty,unit}]
    quality: list = field(default_factory=list)
    reports: list = field(default_factory=list)


@dataclass
class FreezeRecord:
    id: int
    refs: list
    source: str  # quality | allergen | manual
    reason: str
    active: bool
    business_time: str
    operator: str = ""
    releases: list = field(default_factory=list)  # 只增解冻记录
    released_refs: set = field(default_factory=set)  # 已逐个解冻的引用


@dataclass
class Order:
    order_id: str
    customer: str = ""
    store: str = ""
    product: str = ""
    qty: float = 0.0
    unit: str = ""
    due_at: str = ""
    allergen_flags: list = field(default_factory=list)
    status: str = "open"  # open/scheduled/in_production/ready/shipped/closed/cancelled
    created_at: str = ""
    plan: dict | None = None
    planned_seq: int | None = None
    schedule_history: list = field(default_factory=list)
    shipments: list = field(default_factory=list)
    returns: list = field(default_factory=list)
    reports: list = field(default_factory=list)
    reworks: list = field(default_factory=list)
    quality: list = field(default_factory=list)


@dataclass
class Worker:
    code: str
    name: str = ""
    hourly_rate: float = 0.0
    efforts: list = field(default_factory=list)  # 工时记录，只增
    outputs: list = field(default_factory=list)  # 计件产量，只增
    reports: list = field(default_factory=list)


@dataclass
class Shipment:
    shipment_id: str
    order_id: str
    store: str
    business_time: str
    carrier: str = ""
    operator: str = ""
    items: list = field(default_factory=list)  # {box_code,qty,unit}
    returned: dict = field(default_factory=dict)  # box_code -> 已退数量
    state: str = "shipped"  # shipped/partially_returned/returned


@dataclass
class Rework:
    rework_id: str
    ref_kind: str
    ref: str
    order_id: str
    process: str
    equipment: str
    shift_id: str
    worker_code: str
    reason: str
    started_at: str
    state: str = "open"  # open/done
    completions: list = field(default_factory=list)


@dataclass
class State:
    lots: dict = field(default_factory=dict)
    boxes: dict = field(default_factory=dict)
    orders: dict = field(default_factory=dict)
    workers: dict = field(default_factory=dict)
    shipments: dict = field(default_factory=dict)
    reworks: dict = field(default_factory=dict)
    edges: list = field(default_factory=list)  # {parent,child,qty,unit,reason,seq,time}
    freezes: list = field(default_factory=list)
    events: list = field(default_factory=list)
    max_seq: int = 0


# ---------------------------------------------------------------------------
# 事件归约
# ---------------------------------------------------------------------------


def replay(events) -> State:
    """把事件序列按 seq 归约成当前状态。传入截断的事件序列即得到历史快照。"""
    state = State()
    for event in events:
        apply_event(state, event)
    return state


def apply_event(state: State, event):
    state.max_seq = event["seq"]
    state.events.append(event)
    kind = event["type"]
    p = event["payload"]
    handler = _APPLIERS.get(kind)
    if handler:
        handler(state, event)


def _lot_order_id(state, lot_code):
    lot = state.lots.get(lot_code)
    return lot.order_id if lot else None


def _apply_lot_registered(state, event):
    p = event["payload"]
    if p["lot_code"] in state.lots:
        return
    state.lots[p["lot_code"]] = Lot(
        code=p["lot_code"],
        kind=p["kind"],
        material=p.get("material", ""),
        product=p.get("product", ""),
        unit=p.get("unit", ""),
        supplier=p.get("supplier", ""),
        allergen_flags=list(p.get("allergen_flags", [])),
        order_id=p.get("order_id"),
        created_at=event["business_time"],
    )


def _apply_material_registered(state, event):
    # 原料登记是批次登记的语义化别名，保留独立事件类型便于检索。
    p = event["payload"]
    state.lots[p["lot_code"]] = Lot(
        code=p["lot_code"],
        kind="material",
        material=p["material"],
        qty_received=p["qty"],
        unit=p["unit"],
        supplier=p.get("supplier", ""),
        allergen_flags=list(p.get("allergen_flags", [])),
        created_at=event["business_time"],
    )


def _apply_worker_registered(state, event):
    p = event["payload"]
    state.workers[p["worker_code"]] = Worker(
        code=p["worker_code"], name=p["name"], hourly_rate=float(p.get("hourly_rate", 0))
    )


def _apply_order_received(state, event):
    p = event["payload"]
    state.orders[p["order_id"]] = Order(
        order_id=p["order_id"],
        customer=p.get("customer", ""),
        store=p.get("store", ""),
        product=p["product"],
        qty=float(p["qty"]),
        unit=p["unit"],
        due_at=p.get("due_at", ""),
        allergen_flags=list(p.get("allergen_flags", [])),
        created_at=event["business_time"],
    )


def _apply_order_cancelled(state, event):
    order = state.orders[event["payload"]["order_id"]]
    order.status = "cancelled"


def _apply_production_scheduled(state, event):
    p = event["payload"]
    order = state.orders[p["order_id"]]
    order.plan = p["plan"]
    order.planned_seq = event["seq"]
    order.schedule_history.append(
        {"seq": event["seq"], "business_time": event["business_time"], "plan": p["plan"]}
    )
    order.status = "scheduled"


def _consume_input(state, lot_code, qty, record):
    lot = state.lots[lot_code]
    lot.qty_consumed += qty
    lot.consumption.append(record)
    if lot.state == "registered":
        lot.state = "in_progress"
    if lot.available <= EPS and lot.qty_received + lot.qty_produced > 0:
        lot.state = "consumed"


def _apply_material_consumed(state, event):
    p = event["payload"]
    lot = state.lots[p["lot_code"]]
    if lot.order_id is None:
        lot.order_id = p["order_id"]
    _consume_input(
        state,
        p["lot_code"],
        p["qty"],
        {
            "order_id": p["order_id"],
            "process": p["process"],
            "equipment": p["equipment"],
            "shift_id": p["shift_id"],
            "qty": p["qty"],
            "unit": p["unit"],
            "scanner_id": p.get("scanner_id"),
            "business_time": event["business_time"],
            "seq": event["seq"],
        },
    )
    order = state.orders[p["order_id"]]
    if order.status == "scheduled":
        order.status = "in_production"


def _add_edge(state, event, parent, child, qty, unit, reason):
    state.edges.append(
        {
            "parent": parent,
            "child": child,
            "qty": qty,
            "unit": unit,
            "reason": reason,
            "seq": event["seq"],
            "business_time": event["business_time"],
        }
    )


def _apply_lot_split(state, event):
    p = event["payload"]
    parent = state.lots[p["parent_lot"]]
    child = state.lots.get(p["child_lot"])
    if child is None:
        child = Lot(
            code=p["child_lot"],
            kind=parent.kind,
            material=parent.material,
            product=parent.product,
            unit=parent.unit,
            allergen_flags=list(parent.allergen_flags),
            order_id=p["order_id"],
            created_at=event["business_time"],
        )
        state.lots[p["child_lot"]] = child
    _consume_input(
        state,
        parent.code,
        p["qty"],
        {
            "order_id": p["order_id"],
            "process": p["process"],
            "equipment": p["equipment"],
            "shift_id": p["shift_id"],
            "qty": p["qty"],
            "unit": p["unit"],
            "worker_code": p.get("worker_code"),
            "business_time": event["business_time"],
            "seq": event["seq"],
            "into_lot": child.code,
        },
    )
    child.qty_produced += p["qty"]
    child.state = child.state if child.state == "finished" else "in_progress"
    child.allergen_flags = sorted(set(child.allergen_flags) | set(parent.allergen_flags))
    if child.order_id is None:
        child.order_id = p["order_id"]
    _add_edge(state, event, parent.code, child.code, p["qty"], p["unit"], "split")
    order = state.orders[p["order_id"]]
    if order.status == "scheduled":
        order.status = "in_production"


def _apply_lots_merged(state, event):
    p = event["payload"]
    child = state.lots.get(p["child_lot"])
    if child is None:
        first = state.lots.get(p["parents"][0]["lot_code"])
        child = Lot(
            code=p["child_lot"],
            kind="semi",
            product=p.get("product", first.product if first else ""),
            material=first.material if first else "",
            unit=p["parents"][0]["unit"],
            order_id=p["order_id"],
            created_at=event["business_time"],
        )
        state.lots[p["child_lot"]] = child
    total = 0.0
    unit = ""
    for item in p["parents"]:
        parent = state.lots[item["lot_code"]]
        _consume_input(
            state,
            parent.code,
            item["qty"],
            {
                "order_id": p["order_id"],
                "process": p["process"],
                "equipment": p["equipment"],
                "shift_id": p["shift_id"],
                "qty": item["qty"],
                "unit": item["unit"],
                "worker_code": p.get("worker_code"),
                "business_time": event["business_time"],
                "seq": event["seq"],
                "into_lot": child.code,
            },
        )
        child.allergen_flags = sorted(set(child.allergen_flags) | set(parent.allergen_flags))
        _add_edge(state, event, parent.code, child.code, item["qty"], item["unit"], "merge")
        total += item["qty"]
        unit = item["unit"]
    child.qty_produced += total
    child.state = child.state if child.state == "finished" else "in_progress"
    if not child.unit:
        child.unit = unit
    if child.order_id is None:
        child.order_id = p["order_id"]
    order = state.orders[p["order_id"]]
    if order.status == "scheduled":
        order.status = "in_production"


def _apply_lot_produced(state, event):
    p = event["payload"]
    lot = state.lots.get(p["lot_code"])
    if lot is None:
        lot = Lot(
            code=p["lot_code"],
            kind="semi",
            product=p.get("product", ""),
            unit=p["unit"],
            order_id=p["order_id"],
            created_at=event["business_time"],
        )
        state.lots[p["lot_code"]] = lot
    lot.product = p.get("product", lot.product)
    lot.qty_produced += p["qty"]
    lot.unit = p["unit"]
    lot.kind = p.get("kind", "semi")
    lot.state = "in_progress"
    if lot.order_id is None:
        lot.order_id = p["order_id"]
    for item in p.get("inputs", []):
        parent = state.lots[item["lot_code"]]
        _consume_input(
            state,
            parent.code,
            item["qty"],
            {
                "order_id": p["order_id"],
                "process": p["process"],
                "equipment": p["equipment"],
                "shift_id": p["shift_id"],
                "qty": item["qty"],
                "unit": item["unit"],
                "worker_code": p.get("worker_code"),
                "business_time": event["business_time"],
                "seq": event["seq"],
                "into_lot": lot.code,
            },
        )
        lot.allergen_flags = sorted(set(lot.allergen_flags) | set(parent.allergen_flags))
        _add_edge(state, event, parent.code, lot.code, item["qty"], item["unit"], "process")
    if p.get("worker_code"):
        worker = state.workers[p["worker_code"]]
        worker.outputs.append(
            {
                "lot_code": lot.code,
                "product": p.get("product", ""),
                "qty": p["qty"],
                "unit": p["unit"],
                "process": p["process"],
                "shift_id": p["shift_id"],
                "order_id": p["order_id"],
                "piece_rate": p.get("piece_rate", 0),
                "business_time": event["business_time"],
                "seq": event["seq"],
            }
        )
    order = state.orders[p["order_id"]]
    if order.status == "scheduled":
        order.status = "in_production"


def _apply_lot_finished(state, event):
    p = event["payload"]
    lot = state.lots[p["lot_code"]]
    lot.state = "finished"
    if p.get("kind"):
        lot.kind = p["kind"]
    if p.get("product"):
        lot.product = p["product"]


def _apply_box_packed(state, event):
    p = event["payload"]
    # items 支持多批成品合箱；未提供时退化为单批次装箱。
    items = p.get("items")
    if not items:
        items = [{"lot_code": p["lot_code"], "qty": p["qty"], "unit": p["unit"]}]
    source_lots = []
    for item in items:
        lot = state.lots[item["lot_code"]]
        lot.qty_packed += item["qty"]
        source_lots.append(item["lot_code"])
    packaging = p.get("packaging")
    if packaging:
        pkg_lot = state.lots[packaging["lot_code"]]
        _consume_input(
            state,
            pkg_lot.code,
            packaging["qty"],
            {
                "order_id": p["order_id"],
                "process": "包装",
                "equipment": "设备:包装台",
                "shift_id": p.get("packaging_shift_id", ""),
                "qty": packaging["qty"],
                "unit": packaging["unit"],
                "into_box": p["box_code"],
                "business_time": event["business_time"],
                "seq": event["seq"],
            },
        )
    primary = items[0]
    box = Box(
        code=p["box_code"],
        lot_code=primary["lot_code"],
        order_id=p["order_id"],
        qty=p["qty"],
        unit=primary.get("unit", p.get("unit", "")),
        packed_at=event["business_time"],
        worker_code=p.get("worker_code"),
        package_material=p.get("package_material", ""),
        source_lots=source_lots,
        source_items=[dict(it) for it in items],
    )
    state.boxes[p["box_code"]] = box
    order = state.orders[p["order_id"]]
    if order.status in ("scheduled", "in_production"):
        order.status = "in_production"


def _mark_ref_quality(state, ref_kind, ref, record):
    if ref_kind == "lot":
        lot = state.lots.get(ref)
        if lot:
            lot.quality.append(record)
    else:
        box = state.boxes.get(ref)
        if box:
            box.quality.append(record)
            order = state.orders.get(box.order_id)
            if order is not None:
                order.quality.append(record)


def _apply_freeze_record(state, event, *, source):
    p = event["payload"]
    record = FreezeRecord(
        id=event["seq"],
        refs=list(p["scope_refs"]),
        source=source,
        reason=p.get("reason", ""),
        active=True,
        business_time=event["business_time"],
        operator=p.get("operator", ""),
    )
    state.freezes.append(record)
    for ref in p.get("affected_boxes", []):
        box = state.boxes.get(ref)
        if box:
            box.frozen = True
            box.state = "frozen"
    for ref in p.get("affected_lots", []):
        lot = state.lots.get(ref)
        if lot:
            lot.frozen = True
    quality_record = {
        "seq": event["seq"],
        "business_time": event["business_time"],
        "source": source,
        "status": p.get("status", "anomaly"),
        "inspector": p.get("inspector", p.get("operator", "")),
        "allergen": p.get("allergen"),
        "note": p.get("note", p.get("reason", "")),
        "affected_lots": list(p.get("affected_lots", [])),
        "affected_boxes": list(p.get("affected_boxes", [])),
        "stores": list(p.get("stores", [])),
        "freeze_id": event["seq"],
    }
    for ref_kind, ref in p.get("scope_refs", []):
        _mark_ref_quality(state, ref_kind, ref, quality_record)


def _apply_quality_inspection(state, event):
    _apply_freeze_record(state, event, source="quality")


def _apply_allergen_alert(state, event):
    _apply_freeze_record(state, event, source="allergen")


def _apply_freeze_manual(state, event):
    _apply_freeze_record(state, event, source="manual")


def _active_freeze_ids(state, ref):
    """ref（批次或箱码）当前命中的有效冻结 id，含级联影响集合；已逐个解冻的除外。"""
    ids = set()
    for ev in state.events:
        if ev["type"] not in ("quality_inspection", "allergen_alert", "freeze_manual"):
            continue
        freeze = next((f for f in state.freezes if f.id == ev["seq"]), None)
        if freeze is None:
            continue
        touched = {r for _, r in freeze.refs}
        touched.update(ev["payload"].get("affected_lots", []))
        touched.update(ev["payload"].get("affected_boxes", []))
        if ref in touched and ref not in freeze.released_refs:
            ids.add(ev["seq"])
    return ids


def _apply_freeze_released(state, event):
    p = event["payload"]
    targets = set(p["refs"])
    for fr in state.freezes:
        src_event = next(e for e in state.events if e["seq"] == fr.id)
        touched = {r for _, r in fr.refs}
        touched.update(src_event["payload"].get("affected_lots", []))
        touched.update(src_event["payload"].get("affected_boxes", []))
        hit = targets & touched - fr.released_refs
        if not hit:
            continue
        fr.released_refs.update(hit)
        fr.releases.append(
            {
                "seq": event["seq"],
                "business_time": event["business_time"],
                "reason": p.get("reason", ""),
                "operator": p.get("operator", ""),
                "refs": sorted(hit),
            }
        )
        # 所有受影响对象都逐个解冻后，整条冻结记录才归档。
        if touched <= fr.released_refs:
            fr.active = False
    for ref in targets:
        if ref in state.boxes:
            box = state.boxes[ref]
            if not _active_freeze_ids(state, ref):
                box.frozen = False
                if box.state == "frozen":
                    if box.shipment_id:
                        ship = state.shipments[box.shipment_id]
                        shipped = next(
                            it["qty"] for it in ship.items if it["box_code"] == box.code
                        )
                        box.state = (
                            "returned"
                            if ship.returned.get(box.code, 0) + EPS >= shipped
                            else "shipped"
                        )
                    else:
                        box.state = "packed"
        elif ref in state.lots:
            lot = state.lots[ref]
            if not _active_freeze_ids(state, ref):
                lot.frozen = False


def _apply_rework_started(state, event):
    p = event["payload"]
    rw = Rework(
        rework_id=p["rework_id"],
        ref_kind=p["ref_kind"],
        ref=p["ref"],
        order_id=p["order_id"],
        process=p["process"],
        equipment=p["equipment"],
        shift_id=p["shift_id"],
        worker_code=p.get("worker_code", ""),
        reason=p.get("reason", ""),
        started_at=event["business_time"],
    )
    state.reworks[p["rework_id"]] = rw
    order = state.orders[p["order_id"]]
    order.reworks.append(
        {"rework_id": p["rework_id"], "ref_kind": p["ref_kind"], "ref": p["ref"],
         "reason": p.get("reason", ""), "business_time": event["business_time"],
         "state": "open", "completions": []}
    )


def _apply_rework_completed(state, event):
    p = event["payload"]
    rw = state.reworks[p["rework_id"]]
    rw.state = "done"
    completion = {
        "seq": event["seq"],
        "business_time": event["business_time"],
        "result_lot": p.get("result_lot"),
        "qty": p.get("qty"),
        "unit": p.get("unit", ""),
        "note": p.get("note", ""),
        "worker_code": p.get("worker_code", rw.worker_code),
    }
    rw.completions.append(completion)
    order = state.orders[rw.order_id]
    view = next(x for x in order.reworks if x["rework_id"] == rw.rework_id)
    view["state"] = "done"
    view["completions"].append(completion)


def _apply_outbound_shipped(state, event):
    p = event["payload"]
    ship = Shipment(
        shipment_id=p["shipment_id"],
        order_id=p["order_id"],
        store=p["store"],
        business_time=event["business_time"],
        carrier=p.get("carrier", ""),
        operator=p.get("operator", ""),
        items=[dict(it) for it in p["items"]],
    )
    state.shipments[p["shipment_id"]] = ship
    order = state.orders[p["order_id"]]
    order.shipments.append(
        {
            "shipment_id": p["shipment_id"],
            "store": p["store"],
            "carrier": p.get("carrier", ""),
            "operator": p.get("operator", ""),
            "business_time": event["business_time"],
            "items": [dict(it) for it in p["items"]],
            "state": "shipped",
        }
    )
    for item in p["items"]:
        box = state.boxes[item["box_code"]]
        box.state = "shipped"
        box.shipment_id = p["shipment_id"]
        box.store = p["store"]
    if order.status not in ("cancelled", "closed"):
        order.status = "shipped"


def _apply_return_received(state, event):
    p = event["payload"]
    ship = state.shipments[p["shipment_id"]]
    for item in p["items"]:
        ship.returned[item["box_code"]] = ship.returned.get(item["box_code"], 0) + item["qty"]
        box = state.boxes[item["box_code"]]
        shipped_qty = next(it["qty"] for it in ship.items if it["box_code"] == box.code)
        # 冻结中的箱码保留 frozen：退货数量照样登记，解冻时再落到 returned。
        if ship.returned[box.code] + EPS >= shipped_qty and not (box.frozen):
            box.state = "returned"
    total_shipped = sum(it["qty"] for it in ship.items)
    total_returned = sum(ship.returned.values())
    ship.state = "returned" if total_returned + EPS >= total_shipped else "partially_returned"
    order = state.orders[p["order_id"]]
    record = {
        "seq": event["seq"],
        "shipment_id": p["shipment_id"],
        "store": p.get("store", ship.store),
        "reason": p.get("reason", ""),
        "operator": p.get("operator", ""),
        "business_time": event["business_time"],
        "items": [dict(it) for it in p["items"]],
    }
    order.returns.append(record)
    view = next(x for x in order.shipments if x["shipment_id"] == ship.shipment_id)
    view["state"] = ship.state
    view.setdefault("returns", []).append(record)


def _apply_report_added(state, event):
    p = event["payload"]
    record = {
        "seq": event["seq"],
        "report_type": p["report_type"],
        "content": p["content"],
        "author": p.get("author", ""),
        "documents": list(p.get("documents", [])),
        "business_time": event["business_time"],
        "recorded_at": event["recorded_at"],
    }
    kind, eid = p["entity_kind"], p["entity_id"]
    entity = {
        "order": state.orders.get(eid),
        "lot": state.lots.get(eid),
        "box": state.boxes.get(eid),
        "shipment": state.shipments.get(eid),
        "worker": state.workers.get(eid),
    }.get(kind)
    if entity is not None:
        entity.reports.append(record)


def _apply_worker_effort(state, event):
    p = event["payload"]
    worker = state.workers[p["worker_code"]]
    worker.efforts.append(
        {
            "seq": event["seq"],
            "shift_id": p["shift_id"],
            "order_id": p.get("order_id"),
            "hours": p["hours"],
            "hourly_rate": p.get("hourly_rate", worker.hourly_rate),
            "note": p.get("note", ""),
            "business_time": event["business_time"],
        }
    )


_APPLIERS = {
    "material_registered": _apply_material_registered,
    "lot_registered": _apply_lot_registered,
    "worker_registered": _apply_worker_registered,
    "order_received": _apply_order_received,
    "order_cancelled": _apply_order_cancelled,
    "production_scheduled": _apply_production_scheduled,
    "material_consumed": _apply_material_consumed,
    "lot_split": _apply_lot_split,
    "lots_merged": _apply_lots_merged,
    "lot_produced": _apply_lot_produced,
    "lot_finished": _apply_lot_finished,
    "box_packed": _apply_box_packed,
    "quality_inspection": _apply_quality_inspection,
    "allergen_alert": _apply_allergen_alert,
    "freeze_manual": _apply_freeze_manual,
    "freeze_released": _apply_freeze_released,
    "rework_started": _apply_rework_started,
    "rework_completed": _apply_rework_completed,
    "outbound_shipped": _apply_outbound_shipped,
    "return_received": _apply_return_received,
    "report_added": _apply_report_added,
    "worker_effort": _apply_worker_effort,
}


# ---------------------------------------------------------------------------
# 谱系与冻结级联
# ---------------------------------------------------------------------------


def genealogy_neighbors(state, code):
    """返回 {parents, children} 邻接索引（只增边归约而来）。"""
    parents, children = {}, {}
    for edge in state.edges:
        if edge["child"] == code:
            parents[edge["parent"]] = parents.get(edge["parent"], 0) + edge["qty"]
        if edge["parent"] == code:
            children[edge["child"]] = children.get(edge["child"], 0) + edge["qty"]
    return {"parents": parents, "children": children}


def trace_code(state, code):
    """沿谱系双向展开：上游给出原料来源，下游给出半成品/成品/箱码去向。"""
    seen_lots = {code} if code in state.lots else set()
    stack = [code]
    upstream, downstream = [], []
    while stack:
        current = stack.pop()
        for edge in state.edges:
            if edge["child"] == current and edge["parent"] not in seen_lots:
                seen_lots.add(edge["parent"])
                upstream.append(edge)
                stack.append(edge["parent"])
    stack = [code]
    seen_down = {code}
    while stack:
        current = stack.pop()
        for edge in state.edges:
            if edge["parent"] == current and edge["child"] not in seen_down:
                seen_down.add(edge["child"])
                downstream.append(edge)
                stack.append(edge["child"])
    related = seen_lots | seen_down
    boxes = [
        b.code
        for b in state.boxes.values()
        if b.lot_code in related or any(sl in related for sl in b.source_lots)
    ]
    return {
        "code": code,
        "upstream": sorted(upstream, key=lambda e: e["seq"]),
        "downstream": sorted(downstream, key=lambda e: e["seq"]),
        "related_lots": sorted(related),
        "boxes": boxes,
    }


def _shipment_location(state, box_code):
    """箱码当前去向：仍在厂内为 None，否则给出门店与剩余在途数量。"""
    box = state.boxes.get(box_code)
    if box is None or not box.shipment_id:
        return None
    ship = state.shipments[box.shipment_id]
    shipped = next(it["qty"] for it in ship.items if it["box_code"] == box_code)
    remaining = shipped - ship.returned.get(box_code, 0)
    if remaining <= EPS:
        return None
    return {
        "shipment_id": ship.shipment_id,
        "store": ship.store,
        "shipped_qty": shipped,
        "remaining_qty": remaining,
        "shipped_at": ship.business_time,
    }


def expand_freeze_scope(state, scope_refs):
    """把质检/过敏原命中的引用沿谱系展开成受影响批次、箱码和门店清单。"""
    affected_lots, affected_boxes = set(), set()
    for ref_kind, ref in scope_refs:
        if ref_kind == "box":
            if ref not in state.boxes:
                raise NotFound(f"箱码不存在：{ref}")
            affected_boxes.add(ref)
            box = state.boxes[ref]
            for source_lot in box.source_lots or [box.lot_code]:
                trace = trace_code(state, source_lot)
                affected_lots.update(trace["related_lots"])
                # 与该箱共享来源批次的其他箱码同样要立即冻结。
                affected_boxes.update(trace["boxes"])
        else:
            if ref not in state.lots:
                raise NotFound(f"批次不存在：{ref}")
            trace = trace_code(state, ref)
            affected_lots.update(trace["related_lots"])
            affected_boxes.update(trace["boxes"])
    stores = {}
    for box_code in sorted(affected_boxes):
        loc = _shipment_location(state, box_code)
        if loc:
            entry = stores.setdefault(
                loc["store"],
                {"store": loc["store"], "shipment_ids": set(), "boxes": set(), "qty": 0.0},
            )
            entry["shipment_ids"].add(loc["shipment_id"])
            entry["boxes"].add(box_code)
            entry["qty"] += loc["remaining_qty"]
    store_list = [
        {
            "store": s["store"],
            "shipment_ids": sorted(s["shipment_ids"]),
            "boxes": sorted(s["boxes"]),
            "remaining_qty": round(s["qty"], 3),
        }
        for s in sorted(stores.values(), key=lambda x: x["store"])
    ]
    return sorted(affected_lots), sorted(affected_boxes), store_list


def is_frozen(state, code):
    return bool(_active_freeze_ids(state, code))


# ---------------------------------------------------------------------------
# 工时与报酬
# ---------------------------------------------------------------------------


def worker_payroll(state, worker_code):
    worker = state.workers.get(worker_code)
    if worker is None:
        raise NotFound(f"村民/工人不存在：{worker_code}")
    hourly = Decimal("0")
    shift_lines = []
    for effort in worker.efforts:
        amount = Decimal(str(effort["hours"])) * Decimal(str(effort["hourly_rate"]))
        hourly += amount
        shift_lines.append({**effort, "pay": float(amount.quantize(Decimal("0.01")))})
    piece = Decimal("0")
    piece_lines = []
    for out in worker.outputs:
        amount = Decimal(str(out["qty"])) * Decimal(str(out.get("piece_rate", 0)))
        piece += amount
        piece_lines.append({**out, "pay": float(amount.quantize(Decimal("0.01")))})
    total = (hourly + piece).quantize(Decimal("0.01"))
    return {
        "worker_code": worker.code,
        "name": worker.name,
        "hourly_pay": float(hourly.quantize(Decimal("0.01"))),
        "piece_pay": float(piece.quantize(Decimal("0.01"))),
        "total_pay": float(total),
        "shift_lines": shift_lines,
        "piece_lines": piece_lines,
        "total_hours": round(sum(e["hours"] for e in worker.efforts), 3),
        "total_output_qty": round(sum(o["qty"] for o in worker.outputs), 3),
    }


def production_reconciliation(state, order_id=None):
    """按班次工时核对产量：列出每个产出记录对应工人的工时覆盖情况。"""
    rows = []
    for worker in state.workers.values():
        for out in worker.outputs:
            if order_id and out["order_id"] != order_id:
                continue
            shift_hours = sum(
                e["hours"]
                for e in worker.efforts
                if e["shift_id"] == out["shift_id"]
                and (not order_id or e.get("order_id") in (None, order_id))
            )
            rows.append(
                {
                    "worker_code": worker.code,
                    "worker_name": worker.name,
                    "order_id": out["order_id"],
                    "shift_id": out["shift_id"],
                    "process": out["process"],
                    "lot_code": out["lot_code"],
                    "output_qty": out["qty"],
                    "unit": out["unit"],
                    "shift_hours": round(shift_hours, 3),
                    "business_time": out["business_time"],
                    "has_shift_record": shift_hours > 0,
                }
            )
    return sorted(rows, key=lambda r: (r["order_id"], r["shift_id"], r["worker_code"]))
