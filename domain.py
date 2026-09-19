"""产销协同领域核心：命令处理、投影维护与批次追溯。

设计要点：
- 所有业务变化都先追加到事件存储，再折叠成内存投影，
  质检、返工、出库、退货和报告补录因此永远不会覆盖旧记录；
- 扫码投料、工时上报等离线可能重复提交的命令用 event_key 幂等去重，
  扫描枪断网补传不会重复扣料；
- 批次图谱记录 原料批次 -> 半成品批次 -> 箱码 的拆分与合并关系，
  一批面团拆开加工、多批成品合箱之后仍能反查来源；
- 每次排产承诺都把当时可用的原料、设备负荷快照进事件，
  负责人可以随时重放某个订单当时的排产决定。
"""

from collections import defaultdict

from eventstore import EventStore


class DomainError(Exception):
    """业务规则校验失败，message 面向调用方，status 是建议的 HTTP 状态码。"""

    def __init__(self, message, status=400):
        super().__init__(message)
        self.status = status


# 订单状态
ORDER_NEW = "已接收"
ORDER_COMMITTED = "已承诺"
ORDER_PRODUCING = "生产中"
ORDER_SHIPPED = "已出库"

# 箱码状态
BOX_IN_STOCK = "在库"
BOX_SHIPPED = "已出库"
BOX_RETURNED = "已退货"

# 冻结状态
HOLD_ACTIVE = "生效中"
HOLD_RELEASED = "已解除"

CHECK_PASS = "pass"
CHECK_FAIL = "fail"

TARGET_TYPES = ("material", "lot", "box")

DEFAULT_STEPS = ["配料", "和面", "制馅", "包制", "烘烤", "冷却", "内包", "装箱"]

DEFAULT_EQUIPMENT = [
    {"equip_id": "EQ-MIX-01", "name": "一号搅拌机", "kind": "和面", "capacity_per_shift": 800},
    {"equip_id": "EQ-OVEN-01", "name": "一号烤炉", "kind": "烘烤", "capacity_per_shift": 1200},
    {"equip_id": "EQ-PACK-01", "name": "一号包装线", "kind": "包装", "capacity_per_shift": 2000},
]

DEFAULT_WORKERS = [
    {"worker_id": "W-001", "name": "王阿香", "hourly_rate": 22.0},
    {"worker_id": "W-002", "name": "陈阿强", "hourly_rate": 20.0},
    {"worker_id": "W-003", "name": "李阿珍", "hourly_rate": 18.0},
]


def _mentions(obj, needle):
    """递归判断事件负载里是否出现某个标识，用于订单时间线检索。"""
    if obj == needle:
        return True
    if isinstance(obj, dict):
        return any(_mentions(value, needle) for value in obj.values())
    if isinstance(obj, (list, tuple)):
        return any(_mentions(value, needle) for value in obj)
    return False


class Domain:
    """命令入口与内存投影。投影只是事件的折叠结果，可随时重建。"""

    def __init__(self, store=None, seed=True):
        self.store = store or EventStore()
        self.steps = {}
        self.equipment = {}
        self.shifts = {}
        self.workers = {}
        self.materials = {}
        self.orders = {}
        self.lots = {}
        self.boxes = {}
        self.shipments = {}
        self.returns = {}
        self.checks = []
        self.holds = {}
        self.reports = []
        self.time_entries = []
        # 追溯索引
        self.batch_to_lots = defaultdict(list)   # 原料批次 -> 投料去向
        self.lot_boxes = defaultdict(list)       # 批次 -> 装入的箱码
        self.box_shipments = defaultdict(list)   # 箱码 -> 出库单
        self.lot_handlers = defaultdict(list)    # 批次 -> 经手记录（谁接手）
        self.equip_alloc = defaultdict(dict)     # (设备, 班次) -> {订单: 计划量}
        self._counters = defaultdict(int)
        existing = self.store.all()
        for record in existing:
            self._apply(record)
        if seed and not existing:
            self.seed_defaults()

    # ------------------------------------------------------------------
    # 基础设施
    # ------------------------------------------------------------------

    def seed_defaults(self):
        """写入默认工序、设备和村民档案，让服务开箱即可联调。"""
        for index, name in enumerate(DEFAULT_STEPS, start=1):
            self.define_step({"step_id": f"ST-{index:02d}", "name": name, "seq": index})
        for equipment in DEFAULT_EQUIPMENT:
            self.register_equipment(equipment)
        for worker in DEFAULT_WORKERS:
            self.register_worker(worker)

    def _next_id(self, prefix):
        self._counters[prefix] += 1
        return f"{prefix}-{self._counters[prefix]:04d}"

    def _track_id(self, prefix, value):
        """回放事件时恢复发号器，避免重启后撞号。"""
        if isinstance(value, str) and value.startswith(prefix + "-"):
            try:
                number = int(value.rsplit("-", 1)[1])
            except ValueError:
                return
            self._counters[prefix] = max(self._counters[prefix], number)

    def _emit(self, type_, payload, at=None):
        record = self.store.append(type_, payload, at=at)
        self._apply(record)
        return record

    def _apply(self, record):
        handler = getattr(self, f"_on_{record['type']}", None)
        if handler is not None:
            handler(record["payload"], record)

    def _dedup(self, data):
        """离线补传去重：同一 event_key 直接返回首次处理结果。"""
        key = data.get("event_key")
        if not key:
            return None
        prior = self.store.find_by_key(key)
        if prior is None:
            return None
        return {"deduplicated": True, "event": prior, "payload": prior["payload"]}

    @staticmethod
    def _require(mapping, key, label):
        if not key or key not in mapping:
            raise DomainError(f"{label}不存在: {key}", 404)
        return mapping[key]

    @staticmethod
    def _positive(data, field, label):
        try:
            value = float(data.get(field) or 0)
        except (TypeError, ValueError):
            raise DomainError(f"{label}必须是数字")
        if value <= 0:
            raise DomainError(f"{label}必须为正数")
        return value

    # ------------------------------------------------------------------
    # 主数据：工序 / 设备 / 班次 / 村民
    # ------------------------------------------------------------------

    def define_step(self, data):
        step_id = data.get("step_id") or self._next_id("ST")
        if step_id in self.steps:
            raise DomainError(f"工序已存在: {step_id}", 409)
        name = data.get("name")
        if not name:
            raise DomainError("工序缺少名称")
        payload = {"step_id": step_id, "name": name, "seq": int(data.get("seq") or len(self.steps) + 1)}
        self._emit("StepDefined", payload, at=data.get("at"))
        return {"step": self.steps[step_id]}

    def register_equipment(self, data):
        equip_id = data.get("equip_id") or self._next_id("EQ")
        if equip_id in self.equipment:
            raise DomainError(f"设备已存在: {equip_id}", 409)
        if not data.get("name"):
            raise DomainError("设备缺少名称")
        capacity = data.get("capacity_per_shift")
        payload = {
            "equip_id": equip_id,
            "name": data["name"],
            "kind": data.get("kind", ""),
            "capacity_per_shift": float(capacity) if capacity is not None else None,
        }
        self._emit("EquipmentRegistered", payload, at=data.get("at"))
        return {"equipment": self.equipment[equip_id]}

    def define_shift(self, data):
        shift_id = data.get("shift_id") or self._next_id("SHIFT")
        if shift_id in self.shifts:
            raise DomainError(f"班次已存在: {shift_id}", 409)
        for field in ("date", "name"):
            if not data.get(field):
                raise DomainError(f"班次缺少{field}")
        payload = {
            "shift_id": shift_id,
            "date": data["date"],
            "name": data["name"],
            "start": data.get("start", ""),
            "end": data.get("end", ""),
        }
        self._emit("ShiftDefined", payload, at=data.get("at"))
        return {"shift": self.shifts[shift_id]}

    def register_worker(self, data):
        worker_id = data.get("worker_id") or self._next_id("W")
        if worker_id in self.workers:
            raise DomainError(f"村民已登记: {worker_id}", 409)
        if not data.get("name"):
            raise DomainError("村民缺少姓名")
        rate = data.get("hourly_rate")
        if rate is None or float(rate) < 0:
            raise DomainError("村民缺少有效时薪")
        payload = {"worker_id": worker_id, "name": data["name"], "hourly_rate": float(rate)}
        self._emit("WorkerRegistered", payload, at=data.get("at"))
        return {"worker": self.workers[worker_id]}

    def _on_StepDefined(self, payload, record):
        self.steps[payload["step_id"]] = dict(payload)
        self._track_id("ST", payload["step_id"])

    def _on_EquipmentRegistered(self, payload, record):
        self.equipment[payload["equip_id"]] = dict(payload)
        self._track_id("EQ", payload["equip_id"])

    def _on_ShiftDefined(self, payload, record):
        self.shifts[payload["shift_id"]] = dict(payload)
        self._track_id("SHIFT", payload["shift_id"])

    def _on_WorkerRegistered(self, payload, record):
        self.workers[payload["worker_id"]] = dict(payload)
        self._track_id("W", payload["worker_id"])

    # ------------------------------------------------------------------
    # 原料批次
    # ------------------------------------------------------------------

    def receive_material(self, data):
        dup = self._dedup(data)
        if dup:
            return dup
        material = data.get("material")
        if not material:
            raise DomainError("缺少原料名称")
        qty = self._positive(data, "qty", "收货数量")
        batch_id = data.get("batch_id") or self._next_id("MB")
        if batch_id in self.materials:
            raise DomainError(f"原料批次已存在: {batch_id}", 409)
        payload = {
            "batch_id": batch_id,
            "material": material,
            "qty": qty,
            "unit": data.get("unit", "kg"),
            "allergens": list(data.get("allergens") or []),
            "supplier": data.get("supplier", ""),
        }
        if data.get("event_key"):
            payload["event_key"] = data["event_key"]
        self._emit("MaterialReceived", payload, at=data.get("at"))
        return {"deduplicated": False, "batch": self.material_view(batch_id)}

    def _on_MaterialReceived(self, payload, record):
        self.materials[payload["batch_id"]] = {
            "batch_id": payload["batch_id"],
            "material": payload["material"],
            "unit": payload.get("unit", "kg"),
            "allergens": list(payload.get("allergens") or []),
            "supplier": payload.get("supplier", ""),
            "received": float(payload["qty"]),
            "consumed": 0.0,
            "reserved_by": {},
            "holds": [],
            "received_at": record["at"],
        }
        self._track_id("MB", payload["batch_id"])

    @staticmethod
    def _available(batch, exclude_order=None):
        reserved = sum(qty for order, qty in batch["reserved_by"].items() if order != exclude_order)
        return batch["received"] - batch["consumed"] - reserved

    def material_view(self, batch_id):
        batch = self._require(self.materials, batch_id, "原料批次")
        view = {key: value for key, value in batch.items() if key != "reserved_by"}
        view["reserved"] = sum(batch["reserved_by"].values())
        view["available"] = self._available(batch)
        view["frozen"] = self._is_frozen("material", batch_id)
        return view

    def list_materials(self):
        return [self.material_view(batch_id) for batch_id in sorted(self.materials)]

    # ------------------------------------------------------------------
    # 订单与排产承诺
    # ------------------------------------------------------------------

    def create_order(self, data):
        dup = self._dedup(data)
        if dup:
            return dup
        items = data.get("items") or []
        if not items:
            raise DomainError("订单缺少明细")
        for item in items:
            if not item.get("product") or float(item.get("qty") or 0) <= 0:
                raise DomainError("订单明细需要产品名和正数数量")
        order_id = data.get("order_id") or self._next_id("SO")
        if order_id in self.orders:
            raise DomainError(f"订单已存在: {order_id}", 409)
        payload = {
            "order_id": order_id,
            "channel": data.get("channel", ""),
            "store": data.get("store", ""),
            "items": items,
            "due": data.get("due", ""),
        }
        if data.get("event_key"):
            payload["event_key"] = data["event_key"]
        self._emit("OrderReceived", payload, at=data.get("at"))
        return {"deduplicated": False, "order": self.order_view(order_id)}

    def _on_OrderReceived(self, payload, record):
        self.orders[payload["order_id"]] = {
            "order_id": payload["order_id"],
            "channel": payload.get("channel", ""),
            "store": payload.get("store", ""),
            "items": payload.get("items", []),
            "due": payload.get("due", ""),
            "status": ORDER_NEW,
            "decisions": [],
            "committed_materials": [],
            "committed_equipment": [],
            "route": [],
            "committed_shifts": [],
            "received_at": record["at"],
        }
        self._track_id("SO", payload["order_id"])

    def commit_order(self, order_id, data):
        """排产承诺：把订单落到原料批次、设备、工序和班次。

        校验通过后写入 ScheduleDecided 事件，事件里带着当时的原料可用量
        和设备负荷快照，之后可以用 replay_order 重放这次决定。
        """
        order = self._require(self.orders, order_id, "订单")
        materials = data.get("materials") or []
        equipment = data.get("equipment") or []
        route = data.get("route") or []
        shifts = data.get("shifts") or []
        if not materials and not equipment:
            raise DomainError("排产承诺至少需要原料预留或设备安排")
        for step_id in route:
            self._require(self.steps, step_id, "工序")
        for shift_id in shifts:
            self._require(self.shifts, shift_id, "班次")

        material_lines = []
        for line in materials:
            batch = self._require(self.materials, line.get("batch_id"), "原料批次")
            if self._is_frozen("material", batch["batch_id"]):
                raise DomainError(f"原料批次 {batch['batch_id']} 已冻结，不能承诺", 409)
            qty = self._positive(line, "qty", "预留数量")
            available = self._available(batch, exclude_order=order_id)
            if available < qty:
                raise DomainError(
                    f"原料批次 {batch['batch_id']} 可用量 {available} 不足，无法预留 {qty}", 409
                )
            material_lines.append({"batch_id": batch["batch_id"], "qty": qty})

        equipment_lines = []
        for line in equipment:
            equip = self._require(self.equipment, line.get("equip_id"), "设备")
            shift_id = line.get("shift_id")
            self._require(self.shifts, shift_id, "班次")
            if line.get("step_id"):
                self._require(self.steps, line["step_id"], "工序")
            planned = self._positive(line, "planned_qty", "设备计划量")
            capacity = equip.get("capacity_per_shift")
            if capacity is not None:
                load = self._equip_load(equip["equip_id"], shift_id, exclude_order=order_id)
                if load + planned > capacity:
                    raise DomainError(
                        f"设备 {equip['name']} 在班次 {shift_id} 剩余产能 {capacity - load}，"
                        f"排不下 {planned}",
                        409,
                    )
            equipment_lines.append(
                {
                    "equip_id": equip["equip_id"],
                    "step_id": line.get("step_id", ""),
                    "shift_id": shift_id,
                    "planned_qty": planned,
                }
            )

        decision_id = self._next_id("DEC")
        payload = {
            "decision_id": decision_id,
            "order_id": order_id,
            "materials": material_lines,
            "equipment": equipment_lines,
            "route": list(route),
            "shifts": list(shifts),
            "decided_by": data.get("decided_by", ""),
            "note": data.get("note", ""),
            "supersedes": order["decisions"][-1] if order["decisions"] else None,
            "context": {
                "order_items": order["items"],
                "available_materials": {
                    line["batch_id"]: self._available(
                        self.materials[line["batch_id"]], exclude_order=order_id
                    )
                    for line in material_lines
                },
                "equipment_load": {
                    f"{line['equip_id']}@{line['shift_id']}": self._equip_load(
                        line["equip_id"], line["shift_id"], exclude_order=order_id
                    )
                    for line in equipment_lines
                },
            },
        }
        self._emit("ScheduleDecided", payload, at=data.get("at"))
        return {"decision": payload, "order": self.order_view(order_id)}

    def _equip_load(self, equip_id, shift_id, exclude_order=None):
        allocs = self.equip_alloc.get((equip_id, shift_id), {})
        return sum(qty for order, qty in allocs.items() if order != exclude_order)

    def _on_ScheduleDecided(self, payload, record):
        order = self.orders[payload["order_id"]]
        order_id = payload["order_id"]
        # 释放上一次决定的占用，再应用新决定；旧决定仍完整保留在事件里
        for batch in self.materials.values():
            batch["reserved_by"].pop(order_id, None)
        for allocs in self.equip_alloc.values():
            allocs.pop(order_id, None)
        for line in payload["materials"]:
            batch = self.materials[line["batch_id"]]
            batch["reserved_by"][order_id] = batch["reserved_by"].get(order_id, 0.0) + line["qty"]
        for line in payload["equipment"]:
            key = (line["equip_id"], line["shift_id"])
            self.equip_alloc[key][order_id] = self.equip_alloc[key].get(order_id, 0.0) + line[
                "planned_qty"
            ]
        order["status"] = ORDER_COMMITTED
        order["decisions"].append(payload["decision_id"])
        order["committed_materials"] = payload["materials"]
        order["committed_equipment"] = payload["equipment"]
        order["route"] = payload["route"]
        order["committed_shifts"] = payload["shifts"]
        self._track_id("DEC", payload["decision_id"])

    def order_view(self, order_id):
        order = self._require(self.orders, order_id, "订单")
        view = dict(order)
        view["route_detail"] = [
            {"step_id": step_id, "name": self.steps.get(step_id, {}).get("name", step_id)}
            for step_id in order["route"]
        ]
        return view

    def list_orders(self):
        return [self.order_view(order_id) for order_id in sorted(self.orders)]

    # ------------------------------------------------------------------
    # 扫码投料（幂等扣料）
    # ------------------------------------------------------------------

    def consume_material(self, data):
        """扫描枪投料：原料批次扣减并记入半成品批次。

        event_key 是扫描枪本地生成的唯一键，断网补传时同一 key 只会
        处理一次，不会重复扣料。
        """
        dup = self._dedup(data)
        if dup:
            return dup
        batch = self._require(self.materials, data.get("batch_id"), "原料批次")
        qty = self._positive(data, "qty", "投料数量")
        order_id = data.get("order_id")
        if order_id:
            self._require(self.orders, order_id, "订单")
        step = self._require(self.steps, data.get("step_id"), "工序")
        shift = self._require(self.shifts, data.get("shift_id"), "班次")
        worker = self._require(self.workers, data.get("worker_id"), "村民")
        if self._is_frozen("material", batch["batch_id"]):
            raise DomainError(f"原料批次 {batch['batch_id']} 已冻结，不能投料", 409)
        available = self._available(batch, exclude_order=order_id)
        if available < qty:
            raise DomainError(
                f"原料批次 {batch['batch_id']} 可用量 {available} 不足，无法投料 {qty}", 409
            )
        lot_id = data.get("lot_id") or self._next_id("LOT")
        lot = self.lots.get(lot_id)
        if lot is None and not data.get("product"):
            raise DomainError("新批次缺少产品名称")
        if lot is not None and self._is_frozen("lot", lot_id):
            raise DomainError(f"批次 {lot_id} 已冻结，不能继续投料", 409)
        payload = {
            "batch_id": batch["batch_id"],
            "qty": qty,
            "lot_id": lot_id,
            "product": data.get("product") or (lot or {}).get("product", ""),
            "step_id": step["step_id"],
            "shift_id": shift["shift_id"],
            "worker_id": worker["worker_id"],
            "order_id": order_id,
        }
        if data.get("event_key"):
            payload["event_key"] = data["event_key"]
        self._emit("MaterialConsumed", payload, at=data.get("at"))
        return {
            "deduplicated": False,
            "batch": self.material_view(batch["batch_id"]),
            "lot": self.lot_view(lot_id),
        }

    def _on_MaterialConsumed(self, payload, record):
        batch = self.materials[payload["batch_id"]]
        qty = float(payload["qty"])
        batch["consumed"] += qty
        order_id = payload.get("order_id")
        if order_id:
            reserved = batch["reserved_by"].get(order_id, 0.0)
            release = min(reserved, qty)
            if release:
                remaining = reserved - release
                if remaining > 0:
                    batch["reserved_by"][order_id] = remaining
                else:
                    batch["reserved_by"].pop(order_id, None)
        lot = self.lots.get(payload["lot_id"])
        if lot is None:
            lot = self._new_lot(payload["lot_id"], payload.get("product", ""), order_id, record["at"])
        lot["produced"] += qty
        lot["remaining"] += qty
        lot["parents"].append({"type": "material", "id": payload["batch_id"], "qty": qty})
        self.batch_to_lots[payload["batch_id"]].append({"lot_id": lot["lot_id"], "qty": qty})
        self._record_handler(lot["lot_id"], "投料", payload, record["at"])
        if order_id and self.orders[order_id]["status"] == ORDER_COMMITTED:
            self.orders[order_id]["status"] = ORDER_PRODUCING
        self._track_id("LOT", payload["lot_id"])

    def _new_lot(self, lot_id, product, order_id, at):
        lot = {
            "lot_id": lot_id,
            "product": product,
            "produced": 0.0,
            "remaining": 0.0,
            "order_id": order_id,
            "parents": [],
            "children": [],
            "holds": [],
            "reworks": [],
            "created_at": at,
        }
        self.lots[lot_id] = lot
        return lot

    def _record_handler(self, lot_id, action, payload, at):
        self.lot_handlers[lot_id].append(
            {
                "at": at,
                "action": action,
                "step_id": payload.get("step_id", ""),
                "step": self.steps.get(payload.get("step_id"), {}).get("name", ""),
                "worker_id": payload.get("worker_id", ""),
                "worker": self.workers.get(payload.get("worker_id"), {}).get("name", ""),
                "shift_id": payload.get("shift_id", ""),
            }
        )

    # ------------------------------------------------------------------
    # 工序加工：拆分与合并
    # ------------------------------------------------------------------

    def transform_lots(self, data):
        """工序过站：一个或多个输入批次加工成一个或多个输出批次。

        一批面团拆成几份是 1->N，多批半成品合并是 N->1，
        两种情况的父子关系都写进批次图谱，保证可以反查来源。
        """
        dup = self._dedup(data)
        if dup:
            return dup
        inputs = data.get("inputs") or []
        outputs = data.get("outputs") or []
        if not inputs or not outputs:
            raise DomainError("加工需要至少一个输入批次和一个输出批次")
        step = self._require(self.steps, data.get("step_id"), "工序")
        worker = self._require(self.workers, data.get("worker_id"), "村民")
        shift_id = data.get("shift_id")
        if shift_id:
            self._require(self.shifts, shift_id, "班次")
        equipment_id = data.get("equipment_id")
        if equipment_id:
            self._require(self.equipment, equipment_id, "设备")
        order_id = data.get("order_id")
        if order_id:
            self._require(self.orders, order_id, "订单")

        input_lines = []
        for line in inputs:
            lot = self._require(self.lots, line.get("lot_id"), "批次")
            qty = self._positive(line, "qty", "投入数量")
            if self._is_frozen("lot", lot["lot_id"]):
                raise DomainError(f"批次 {lot['lot_id']} 已冻结，不能加工", 409)
            if lot["remaining"] < qty:
                raise DomainError(
                    f"批次 {lot['lot_id']} 剩余 {lot['remaining']}，不足投入 {qty}", 409
                )
            input_lines.append({"lot_id": lot["lot_id"], "qty": qty})

        output_lines = []
        for line in outputs:
            lot_id = line.get("lot_id") or self._next_id("LOT")
            existing = self.lots.get(lot_id)
            if existing is None and not line.get("product"):
                raise DomainError("新批次缺少产品名称")
            if existing is not None and self._is_frozen("lot", lot_id):
                raise DomainError(f"批次 {lot_id} 已冻结，不能并入", 409)
            qty = self._positive(line, "qty", "产出数量")
            output_lines.append(
                {
                    "lot_id": lot_id,
                    "product": line.get("product") or (existing or {}).get("product", ""),
                    "qty": qty,
                }
            )

        payload = {
            "transform_id": self._next_id("TR"),
            "inputs": input_lines,
            "outputs": output_lines,
            "step_id": step["step_id"],
            "worker_id": worker["worker_id"],
            "shift_id": shift_id or "",
            "equipment_id": equipment_id or "",
            "order_id": order_id,
        }
        if data.get("event_key"):
            payload["event_key"] = data["event_key"]
        self._emit("LotTransformed", payload, at=data.get("at"))
        return {
            "deduplicated": False,
            "transform_id": payload["transform_id"],
            "inputs": [self.lot_view(line["lot_id"]) for line in input_lines],
            "outputs": [self.lot_view(line["lot_id"]) for line in output_lines],
        }

    def _on_LotTransformed(self, payload, record):
        for line in payload["inputs"]:
            lot = self.lots[line["lot_id"]]
            lot["remaining"] -= line["qty"]
            self._record_handler(lot["lot_id"], "加工消耗", payload, record["at"])
        for out in payload["outputs"]:
            lot = self.lots.get(out["lot_id"])
            if lot is None:
                lot = self._new_lot(out["lot_id"], out.get("product", ""), payload.get("order_id"), record["at"])
            lot["produced"] += out["qty"]
            lot["remaining"] += out["qty"]
            for line in payload["inputs"]:
                lot["parents"].append({"type": "lot", "id": line["lot_id"], "qty": line["qty"]})
                self.lots[line["lot_id"]]["children"].append(
                    {"type": "lot", "id": out["lot_id"], "qty": out["qty"]}
                )
            self._record_handler(lot["lot_id"], "加工产出", payload, record["at"])
            self._track_id("LOT", out["lot_id"])
        order_id = payload.get("order_id")
        if order_id and self.orders[order_id]["status"] == ORDER_COMMITTED:
            self.orders[order_id]["status"] = ORDER_PRODUCING
        self._track_id("TR", payload["transform_id"])

    def lot_view(self, lot_id):
        lot = self._require(self.lots, lot_id, "批次")
        view = {key: value for key, value in lot.items() if key not in ("parents", "children", "holds", "reworks")}
        view["frozen"] = self._is_frozen("lot", lot_id)
        view["rework_count"] = len(lot["reworks"])
        return view

    # ------------------------------------------------------------------
    # 装箱（多批成品合箱）
    # ------------------------------------------------------------------

    def pack_box(self, data):
        """装箱：一个箱码可以装多个成品批次，箱码与批次的关系入图谱。"""
        dup = self._dedup(data)
        if dup:
            return dup
        box_code = data.get("box_code") or self._next_id("BOX")
        if box_code in self.boxes:
            raise DomainError(f"箱码已存在: {box_code}", 409)
        items = data.get("items") or []
        if not items:
            raise DomainError("装箱缺少内容明细")
        order_id = data.get("order_id")
        if order_id:
            self._require(self.orders, order_id, "订单")
        worker_id = data.get("worker_id")
        if worker_id:
            self._require(self.workers, worker_id, "村民")
        content_lines = []
        for line in items:
            lot = self._require(self.lots, line.get("lot_id"), "批次")
            qty = self._positive(line, "qty", "装箱数量")
            if self._is_frozen("lot", lot["lot_id"]):
                raise DomainError(f"批次 {lot['lot_id']} 已冻结，不能装箱", 409)
            if lot["remaining"] < qty:
                raise DomainError(
                    f"批次 {lot['lot_id']} 剩余 {lot['remaining']}，不足装箱 {qty}", 409
                )
            content_lines.append({"lot_id": lot["lot_id"], "qty": qty})
        payload = {
            "box_code": box_code,
            "items": content_lines,
            "order_id": order_id,
            "worker_id": worker_id or "",
        }
        if data.get("event_key"):
            payload["event_key"] = data["event_key"]
        self._emit("BoxPacked", payload, at=data.get("at"))
        return {"deduplicated": False, "box": self.box_view(box_code)}

    def _on_BoxPacked(self, payload, record):
        self.boxes[payload["box_code"]] = {
            "box_code": payload["box_code"],
            "contents": [dict(line) for line in payload["items"]],
            "status": BOX_IN_STOCK,
            "order_id": payload.get("order_id"),
            "holds": [],
            "packed_at": record["at"],
        }
        for line in payload["items"]:
            lot = self.lots[line["lot_id"]]
            lot["remaining"] -= line["qty"]
            lot["children"].append({"type": "box", "id": payload["box_code"], "qty": line["qty"]})
            self.lot_boxes[line["lot_id"]].append(payload["box_code"])
            self._record_handler(line["lot_id"], "装箱", payload, record["at"])
        order_id = payload.get("order_id")
        if order_id and self.orders[order_id]["status"] == ORDER_COMMITTED:
            self.orders[order_id]["status"] = ORDER_PRODUCING
        self._track_id("BOX", payload["box_code"])

    def box_view(self, box_code):
        box = self._require(self.boxes, box_code, "箱码")
        view = {key: value for key, value in box.items() if key != "holds"}
        view["frozen"] = self._is_frozen("box", box_code)
        view["holds"] = [self.holds[hold_id] for hold_id in box["holds"]]
        return view

    # ------------------------------------------------------------------
    # 质检、冻结与召回
    # ------------------------------------------------------------------

    def record_check(self, data):
        """记录质检结果；不合格时立即冻结相关批次和箱码，并算出门店影响面。"""
        target_type = data.get("target_type")
        if target_type not in TARGET_TYPES:
            raise DomainError(f"质检对象类型必须是 {TARGET_TYPES}")
        target_id = data.get("target_id")
        self._target_of(target_type, target_id)
        result = data.get("result")
        if result not in (CHECK_PASS, CHECK_FAIL):
            raise DomainError("质检结果必须是 pass 或 fail")
        check_id = data.get("check_id") or self._next_id("QC")
        payload = {
            "check_id": check_id,
            "target_type": target_type,
            "target_id": target_id,
            "item": data.get("item", ""),
            "result": result,
            "allergen": data.get("allergen", ""),
            "detail": data.get("detail", ""),
            "checked_by": data.get("checked_by", ""),
        }
        self._emit("QualityChecked", payload, at=data.get("at"))
        impact = None
        if result == CHECK_FAIL:
            reason = f"检验异常:{payload['item']}"
            if payload["allergen"]:
                reason = f"过敏原异常:{payload['allergen']}"
            impact = self._freeze(target_type, target_id, reason, check_id, at=data.get("at"))
        return {"check": payload, "impact": impact}

    def _on_QualityChecked(self, payload, record):
        self.checks.append({**payload, "at": record["at"]})
        self._track_id("QC", payload["check_id"])

    def list_checks(self, target_type=None, target_id=None):
        return [
            check
            for check in self.checks
            if (not target_type or check["target_type"] == target_type)
            and (not target_id or check["target_id"] == target_id)
        ]

    def _target_of(self, target_type, target_id):
        if target_type == "material":
            return self._require(self.materials, target_id, "原料批次")
        if target_type == "lot":
            return self._require(self.lots, target_id, "批次")
        return self._require(self.boxes, target_id, "箱码")

    def _impact(self, target_type, target_id):
        """从任意节点出发，算出受影响的批次与箱码集合。"""
        lots, boxes = set(), set()
        if target_type == "lot":
            lots.add(target_id)
        elif target_type == "material":
            lots.update(entry["lot_id"] for entry in self.batch_to_lots.get(target_id, []))
        else:
            boxes.add(target_id)
        stack = list(lots)
        while stack:
            current = stack.pop()
            for child in self.lots[current]["children"]:
                if child["type"] == "box":
                    boxes.add(child["id"])
                elif child["id"] not in lots:
                    lots.add(child["id"])
                    stack.append(child["id"])
        for lot_id in lots:
            boxes.update(self.lot_boxes.get(lot_id, []))
        return lots, boxes

    def _freeze(self, target_type, target_id, reason, source_check_id=None, at=None):
        """冻结受影响的所有批次和箱码，返回影响面（含已发往的门店）。"""
        lots, boxes = self._impact(target_type, target_id)
        frozen_materials, frozen_lots, frozen_boxes = [], [], []
        # 原料批次出问题时，批次本身也要冻结，防止继续投料
        if target_type == "material" and not self._is_frozen("material", target_id):
            self._emit(
                "HoldPlaced",
                {
                    "hold_id": self._next_id("HOLD"),
                    "target_type": "material",
                    "target_id": target_id,
                    "reason": reason,
                    "source_check_id": source_check_id,
                },
                at=at,
            )
            frozen_materials.append(target_id)
        for lot_id in sorted(lots):
            if self._is_frozen("lot", lot_id):
                continue
            hold_id = self._next_id("HOLD")
            self._emit(
                "HoldPlaced",
                {
                    "hold_id": hold_id,
                    "target_type": "lot",
                    "target_id": lot_id,
                    "reason": reason,
                    "source_check_id": source_check_id,
                },
                at=at,
            )
            frozen_lots.append(lot_id)
        for box_code in sorted(boxes):
            if self._is_frozen("box", box_code):
                continue
            hold_id = self._next_id("HOLD")
            self._emit(
                "HoldPlaced",
                {
                    "hold_id": hold_id,
                    "target_type": "box",
                    "target_id": box_code,
                    "reason": reason,
                    "source_check_id": source_check_id,
                },
                at=at,
            )
            frozen_boxes.append(box_code)
        shipments, stores = self._shipments_of(boxes)
        return {
            "reason": reason,
            "frozen_materials": frozen_materials,
            "frozen_lots": frozen_lots,
            "frozen_boxes": frozen_boxes,
            "shipments": shipments,
            "stores": stores,
        }

    def _shipments_of(self, box_codes):
        shipments, stores = [], set()
        for box_code in sorted(box_codes):
            for shipment_id in self.box_shipments.get(box_code, []):
                shipment = self.shipments[shipment_id]
                shipments.append(dict(shipment))
                stores.add(shipment["store"])
        return shipments, sorted(stores)

    def _on_HoldPlaced(self, payload, record):
        self.holds[payload["hold_id"]] = {
            **payload,
            "status": HOLD_ACTIVE,
            "placed_at": record["at"],
        }
        self._target_of(payload["target_type"], payload["target_id"])["holds"].append(
            payload["hold_id"]
        )
        self._track_id("HOLD", payload["hold_id"])

    def _on_HoldReleased(self, payload, record):
        hold = self.holds[payload["hold_id"]]
        hold["status"] = HOLD_RELEASED
        hold["released_at"] = record["at"]
        hold["released_by"] = payload.get("released_by", "")

    def _is_frozen(self, target_type, target_id):
        target = self._target_of(target_type, target_id)
        return any(
            self.holds[hold_id]["status"] == HOLD_ACTIVE for hold_id in target.get("holds", [])
        )

    def place_hold(self, data):
        """负责人手动冻结，同样立即算出门店影响面。"""
        target_type = data.get("target_type")
        if target_type not in TARGET_TYPES:
            raise DomainError(f"冻结对象类型必须是 {TARGET_TYPES}")
        target_id = data.get("target_id")
        self._target_of(target_type, target_id)
        reason = data.get("reason") or "人工冻结"
        return self._freeze(target_type, target_id, reason, at=data.get("at"))

    def release_hold(self, hold_id, data):
        hold = self._require(self.holds, hold_id, "冻结记录")
        if hold["status"] != HOLD_ACTIVE:
            raise DomainError(f"冻结记录 {hold_id} 已解除", 409)
        self._emit(
            "HoldReleased",
            {"hold_id": hold_id, "released_by": data.get("released_by", ""), "note": data.get("note", "")},
            at=data.get("at"),
        )
        return {"hold": self.holds[hold_id]}

    def recall(self, target_type, target_id):
        """只读召回分析：受影响批次、箱码、出库单和门店，不改变任何状态。"""
        if target_type not in TARGET_TYPES:
            raise DomainError(f"召回对象类型必须是 {TARGET_TYPES}")
        self._target_of(target_type, target_id)
        lots, boxes = self._impact(target_type, target_id)
        shipments, stores = self._shipments_of(boxes)
        active_holds = [
            hold
            for hold in self.holds.values()
            if hold["status"] == HOLD_ACTIVE
            and (
                hold["target_id"] in lots
                or hold["target_id"] in boxes
                or (hold["target_type"] == target_type and hold["target_id"] == target_id)
            )
        ]
        return {
            "target": {"target_type": target_type, "target_id": target_id},
            "affected_lots": sorted(lots),
            "affected_boxes": sorted(boxes),
            "shipments": shipments,
            "stores": stores,
            "active_holds": active_holds,
        }

    # ------------------------------------------------------------------
    # 出库与退货
    # ------------------------------------------------------------------

    def ship(self, data):
        box_codes = data.get("box_codes") or []
        if not box_codes:
            raise DomainError("出库缺少箱码")
        store = data.get("store")
        if not store:
            raise DomainError("出库缺少门店")
        order_id = data.get("order_id")
        if order_id:
            self._require(self.orders, order_id, "订单")
        for box_code in box_codes:
            box = self._require(self.boxes, box_code, "箱码")
            if self._is_frozen("box", box_code):
                raise DomainError(f"箱码 {box_code} 已冻结，不能出库", 409)
            if box["status"] != BOX_IN_STOCK:
                raise DomainError(
                    f"箱码 {box_code} 当前状态 {box['status']}，不能出库", 409
                )
        shipment_id = data.get("shipment_id") or self._next_id("SHP")
        payload = {
            "shipment_id": shipment_id,
            "box_codes": list(box_codes),
            "store": store,
            "order_id": order_id,
        }
        self._emit("Shipped", payload, at=data.get("at"))
        return {"shipment": self.shipments[shipment_id]}

    def _on_Shipped(self, payload, record):
        self.shipments[payload["shipment_id"]] = {
            **payload,
            "shipped_at": record["at"],
        }
        for box_code in payload["box_codes"]:
            self.boxes[box_code]["status"] = BOX_SHIPPED
            self.box_shipments[box_code].append(payload["shipment_id"])
        order_id = payload.get("order_id")
        if order_id:
            self.orders[order_id]["status"] = ORDER_SHIPPED
        self._track_id("SHP", payload["shipment_id"])

    def record_return(self, data):
        box_codes = data.get("box_codes") or []
        if not box_codes:
            raise DomainError("退货缺少箱码")
        from_store = data.get("from_store")
        if not from_store:
            raise DomainError("退货缺少来源门店")
        for box_code in box_codes:
            box = self._require(self.boxes, box_code, "箱码")
            if box["status"] != BOX_SHIPPED:
                raise DomainError(
                    f"箱码 {box_code} 当前状态 {box['status']}，不能退货", 409
                )
            shipped_to = {
                self.shipments[shipment_id]["store"]
                for shipment_id in self.box_shipments.get(box_code, [])
            }
            if shipped_to and from_store not in shipped_to:
                raise DomainError(
                    f"箱码 {box_code} 发往的门店是 {sorted(shipped_to)}，与退货门店不符", 409
                )
        return_id = data.get("return_id") or self._next_id("RET")
        payload = {
            "return_id": return_id,
            "box_codes": list(box_codes),
            "from_store": from_store,
            "reason": data.get("reason", ""),
        }
        self._emit("Returned", payload, at=data.get("at"))
        return {"return": self.returns[return_id]}

    def _on_Returned(self, payload, record):
        self.returns[payload["return_id"]] = {**payload, "returned_at": record["at"]}
        for box_code in payload["box_codes"]:
            self.boxes[box_code]["status"] = BOX_RETURNED
        self._track_id("RET", payload["return_id"])

    # ------------------------------------------------------------------
    # 返工与报告补录（只追加，不覆盖）
    # ------------------------------------------------------------------

    def record_rework(self, data):
        lot = self._require(self.lots, data.get("lot_id"), "批次")
        qty = self._positive(data, "qty", "返工数量")
        if qty > lot["remaining"]:
            raise DomainError(f"批次 {lot['lot_id']} 剩余 {lot['remaining']}，不足返工 {qty}", 409)
        from_step = self._require(self.steps, data.get("from_step"), "工序")
        to_step = self._require(self.steps, data.get("to_step"), "工序")
        rework_id = data.get("rework_id") or self._next_id("RW")
        payload = {
            "rework_id": rework_id,
            "lot_id": lot["lot_id"],
            "qty": qty,
            "from_step": from_step["step_id"],
            "to_step": to_step["step_id"],
            "reason": data.get("reason", ""),
            "worker_id": data.get("worker_id", ""),
        }
        self._emit("ReworkRecorded", payload, at=data.get("at"))
        return {"rework": payload, "lot": self.lot_view(lot["lot_id"])}

    def _on_ReworkRecorded(self, payload, record):
        self.lots[payload["lot_id"]]["reworks"].append({**payload, "at": record["at"]})
        self._track_id("RW", payload["rework_id"])

    def file_report(self, data):
        """报告补录：新报告引用旧报告（supersedes），旧记录保持原样。"""
        for field in ("ref_type", "ref_id", "kind", "content"):
            if not data.get(field):
                raise DomainError(f"报告缺少{field}")
        supersedes = data.get("supersedes")
        if supersedes and supersedes not in {report["report_id"] for report in self.reports}:
            raise DomainError(f"被更正的报告不存在: {supersedes}", 404)
        report_id = data.get("report_id") or self._next_id("RPT")
        payload = {
            "report_id": report_id,
            "ref_type": data["ref_type"],
            "ref_id": data["ref_id"],
            "kind": data["kind"],
            "content": data["content"],
            "supersedes": supersedes,
            "reported_by": data.get("reported_by", ""),
        }
        self._emit("ReportFiled", payload, at=data.get("at"))
        return {"report": self.reports[-1]}

    def _on_ReportFiled(self, payload, record):
        self.reports.append({**payload, "at": record["at"]})
        self._track_id("RPT", payload["report_id"])

    def list_reports(self, ref_type=None, ref_id=None):
        return [
            report
            for report in self.reports
            if (not ref_type or report["ref_type"] == ref_type)
            and (not ref_id or report["ref_id"] == ref_id)
        ]

    # ------------------------------------------------------------------
    # 工时与报酬
    # ------------------------------------------------------------------

    def record_time_entry(self, data):
        """村民工时上报，event_key 幂等，换班重复提交不会重复计酬。"""
        dup = self._dedup(data)
        if dup:
            return dup
        worker = self._require(self.workers, data.get("worker_id"), "村民")
        shift = self._require(self.shifts, data.get("shift_id"), "班次")
        step = self._require(self.steps, data.get("step_id"), "工序")
        hours = self._positive(data, "hours", "工时")
        try:
            output_qty = float(data.get("output_qty") or 0)
        except (TypeError, ValueError):
            raise DomainError("产量必须是数字")
        if output_qty < 0:
            raise DomainError("产量不能为负")
        entry_id = data.get("entry_id") or self._next_id("TE")
        payload = {
            "entry_id": entry_id,
            "worker_id": worker["worker_id"],
            "shift_id": shift["shift_id"],
            "step_id": step["step_id"],
            "hours": hours,
            "output_qty": output_qty,
            "lot_id": data.get("lot_id", ""),
        }
        if data.get("event_key"):
            payload["event_key"] = data["event_key"]
        self._emit("TimeEntryRecorded", payload, at=data.get("at"))
        return {"deduplicated": False, "entry": self.time_entries[-1]}

    def _on_TimeEntryRecorded(self, payload, record):
        self.time_entries.append({**payload, "at": record["at"]})
        self._track_id("TE", payload["entry_id"])

    def wages(self, worker_id=None, date_from=None, date_to=None):
        """按村民工时核对产量和应付报酬。"""
        if worker_id:
            self._require(self.workers, worker_id, "村民")
            worker_ids = [worker_id]
        else:
            worker_ids = sorted(self.workers)
        result = []
        for wid in worker_ids:
            worker = self.workers[wid]
            entries = [
                entry
                for entry in self.time_entries
                if entry["worker_id"] == wid
                and (not date_from or entry["at"][:10] >= date_from)
                and (not date_to or entry["at"][:10] <= date_to)
            ]
            total_hours = sum(entry["hours"] for entry in entries)
            total_output = sum(entry["output_qty"] for entry in entries)
            result.append(
                {
                    "worker_id": wid,
                    "name": worker["name"],
                    "hourly_rate": worker["hourly_rate"],
                    "entries": [
                        {
                            **entry,
                            "shift": self.shifts.get(entry["shift_id"], {}).get("name", ""),
                            "step": self.steps.get(entry["step_id"], {}).get("name", ""),
                        }
                        for entry in entries
                    ],
                    "total_hours": total_hours,
                    "total_output": total_output,
                    "amount_due": round(total_hours * worker["hourly_rate"], 2),
                }
            )
        return {"wages": result}

    # ------------------------------------------------------------------
    # 追溯与重放
    # ------------------------------------------------------------------

    def _ancestors(self, lot_id):
        """反查来源：返回上游批次集合和原料批次用量。"""
        seen, batches = set(), defaultdict(float)
        stack = [lot_id]
        while stack:
            current = stack.pop()
            for parent in self.lots[current]["parents"]:
                if parent["type"] == "material":
                    batches[parent["id"]] += parent["qty"]
                elif parent["id"] not in seen:
                    seen.add(parent["id"])
                    stack.append(parent["id"])
        seen.discard(lot_id)
        return seen, dict(batches)

    def _descendants(self, lot_id):
        """正向追踪：返回下游批次和箱码集合。"""
        lots, boxes = set(), set()
        stack = [lot_id]
        while stack:
            current = stack.pop()
            for child in self.lots[current]["children"]:
                if child["type"] == "box":
                    boxes.add(child["id"])
                elif child["id"] not in lots:
                    lots.add(child["id"])
                    stack.append(child["id"])
        lots.discard(lot_id)
        return lots, boxes

    def trace_lot(self, lot_id):
        self._require(self.lots, lot_id, "批次")
        ancestor_lots, batches = self._ancestors(lot_id)
        descendant_lots, boxes = self._descendants(lot_id)
        shipments, stores = self._shipments_of(boxes)
        return {
            "lot": self.lot_view(lot_id),
            "sources": {
                "material_batches": [
                    {"batch_id": batch_id, "material": self.materials[batch_id]["material"], "qty": qty}
                    for batch_id, qty in sorted(batches.items())
                ],
                "lots": sorted(ancestor_lots),
            },
            "destinations": {
                "lots": sorted(descendant_lots),
                "boxes": sorted(boxes),
                "shipments": shipments,
                "stores": stores,
            },
            "handlers": list(self.lot_handlers.get(lot_id, [])),
        }

    def trace_box(self, box_code):
        box = self._require(self.boxes, box_code, "箱码")
        batches = defaultdict(float)
        source_lots = set()
        for item in box["contents"]:
            source_lots.add(item["lot_id"])
            lots, lot_batches = self._ancestors(item["lot_id"])
            source_lots.update(lots)
            for batch_id, qty in lot_batches.items():
                batches[batch_id] += qty
        shipments, stores = self._shipments_of({box_code})
        return {
            "box": self.box_view(box_code),
            "sources": {
                "material_batches": [
                    {"batch_id": batch_id, "material": self.materials[batch_id]["material"], "qty": qty}
                    for batch_id, qty in sorted(batches.items())
                ],
                "lots": sorted(source_lots),
            },
            "destinations": {"shipments": shipments, "stores": stores},
            "holds": [self.holds[hold_id] for hold_id in box["holds"]],
        }

    def replay_order(self, order_id, at=None):
        """重放订单：当时的排产决定（含快照）和完整时间线。

        at 参数可以把时间线截断到某个时刻，看到"当时为止"的决定过程；
        state 字段是把 at 之前的事件重新折叠出的订单状态。
        """
        self._require(self.orders, order_id, "订单")
        timeline = [
            record
            for record in self.store.all()
            if _mentions(record["payload"], order_id) and (at is None or record["at"] <= at)
        ]
        decisions = [record for record in timeline if record["type"] == "ScheduleDecided"]
        state = None
        if at is not None:
            ghost = Domain(EventStore(), seed=False)
            for record in self.store.all():
                if record["at"] <= at:
                    ghost._apply(record)
            if order_id in ghost.orders:
                state = ghost.order_view(order_id)
        return {
            "order_id": order_id,
            "as_of": at,
            "decisions": [record["payload"] for record in decisions],
            "timeline": timeline,
            "state": state,
        }

    def list_events(self, type_=None, limit=None):
        events = self.store.all()
        if type_:
            events = [event for event in events if event["type"] == type_]
        if limit:
            events = events[-int(limit):]
        return events
