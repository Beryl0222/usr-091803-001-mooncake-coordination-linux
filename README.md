# 月饼工坊产销协同

本项目服务于屯昌特色月饼生产与村民就业协同。原料批次、工序、订单和出库需要在季节性高峰中保持一致的责任链。系统以只追加事件为底座：质检、返工、出库、退货和报告补录都作为新记录追加，永不覆盖旧记录，负责人可以随时重放当时的排产决定。

## 运行

```bash
python3 service.py --check          # 基础自检
python3 service.py --port 8000      # 启动服务（事件落盘到 data/mooncake-events.jsonl）
python3 service.py --port 8000 --db /tmp/events.jsonl   # 指定事件日志位置
npm test                            # 运行全部测试（契约 + 领域 + HTTP）
```

`GET /health` 返回稳定的服务身份，便于本地联调和运维巡检。业务接口都在 `/api/*` 下，启动时自动写入默认工序（配料→装箱）、三台设备和三名村民的档案。

## 核心能力

- **排产承诺**：订单承诺落到原料批次（预留）、设备（按班次产能校验）、工序路线和班次，决定事件带着当时的可用量快照。
- **批次追溯**：面团批次可以拆开加工（1→N），多批成品可以合箱（N→1），批次图谱支持从任意箱码反查原料批次，也能从原料批次正查到门店。
- **扫码幂等**：投料、加工、装箱、工时上报都接受 `event_key`，扫描枪断网补传同一 key 只处理一次，不会重复扣料。
- **冻结与召回**：质检不合格或过敏原异常会立即冻结相关批次和箱码，并列出已发往的门店；冻结的箱码不能出库，解除后才能放行。
- **工时报酬**：村民工时按班次和工序上报，按工时 × 时薪核对应付报酬和产量。

## 接口一览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/health` | 健康检查 |
| POST | `/api/materials/receive` | 原料批次收货（含过敏原、供应商） |
| GET | `/api/materials`、`/api/materials/{id}` | 批次库存（可用量 = 收货 − 已扣 − 已预留） |
| POST | `/api/orders` | 接单（商超补货 / 游客团购） |
| POST | `/api/orders/{id}/commit` | 排产承诺：原料批次 + 设备 + 工序 + 班次 |
| GET | `/api/orders/{id}/replay?at=` | 重放订单的排产决定和时间线 |
| POST | `/api/scan/consume` | 扫码投料（`event_key` 幂等扣料） |
| POST | `/api/lots/transform` | 工序过站：批次拆分 / 合并 |
| GET | `/api/lots/{id}/trace` | 批次追溯（来源、去向、经手人） |
| POST | `/api/boxes/pack` | 装箱（多批成品合箱） |
| GET | `/api/boxes/{code}/trace` | 箱码反查原料批次与发往门店 |
| POST | `/api/quality/checks` | 质检记录；fail 立即冻结并返回门店影响面 |
| POST | `/api/holds`、`/api/holds/{id}/release` | 人工冻结 / 解除 |
| GET | `/api/recall?target_type=&target_id=` | 只读召回分析 |
| POST | `/api/shipments`、`/api/returns` | 出库到门店 / 门店退货 |
| POST | `/api/rework` | 返工记录（只追加） |
| POST | `/api/reports` | 报告补录（`supersedes` 引用旧报告，不覆盖） |
| POST | `/api/time-entries` | 村民工时上报（`event_key` 幂等） |
| GET | `/api/wages?worker_id=&from=&to=` | 按工时核对产量与应付报酬 |
| GET | `/api/events?type=&limit=` | 事件审计 |
| GET/POST | `/api/steps`、`/api/equipment`、`/api/shifts`、`/api/workers` | 主数据 |

## 示例：从承诺到召回

```bash
# 排产承诺：预留黑猪叉烧批次，烤炉排在早班
curl -X POST localhost:8000/api/orders/SO-0001/commit -d '{
  "materials": [{"batch_id": "MB-CHA", "qty": 120}],
  "equipment": [{"equip_id": "EQ-OVEN-01", "step_id": "ST-05", "shift_id": "SHIFT-A", "planned_qty": 600}],
  "route": ["ST-01","ST-02","ST-03","ST-04","ST-05","ST-08"],
  "shifts": ["SHIFT-A"], "decided_by": "生产经理"}'

# 扫码投料（断网补传同一 event_key 不会重复扣料）
curl -X POST localhost:8000/api/scan/consume -d '{
  "event_key": "gun-07-0001", "batch_id": "MB-CHA", "qty": 30,
  "lot_id": "LOT-FILL", "product": "叉烧馅",
  "step_id": "ST-03", "shift_id": "SHIFT-A", "worker_id": "W-001", "order_id": "SO-0001"}'

# 检验异常：立即冻结相关箱码，并列出已发往的门店
curl -X POST localhost:8000/api/quality/checks -d '{
  "target_type": "lot", "target_id": "LOT-FILL", "item": "微生物", "result": "fail"}'
```

## 结构

- `eventstore.py` — 只追加事件存储（JSONL 落盘，重启回放）
- `domain.py` — 领域核心：命令、投影、批次图谱、冻结召回、工时报酬
- `api.py` — HTTP 路由与错误格式
- `service.py` — 运行入口，保留 `/health` 健康检查
- `service_contract.py` / `test_domain.py` / `test_api.py` — 测试
