# 月饼工坊产销协同

服务于屯昌月饼旺季的产销协同后端：商超补货单与游客团购单进来后，排产承诺要落到
**原料批次、设备、工序、班次**；一批面团拆开加工、多批成品合箱后仍能双向反查来源；
扫描枪断网补传不能重复扣料；过敏原或检验异常出现时立即冻结相关箱码并找出已发往的门店；
质检、返工、出库、退货、报告补录全部只增不改；负责人能重放某订单当时的排产决定，
并按村民工时核对产量与应付报酬。

## 运行

仅依赖 Python 3 标准库：

```bash
python3 service.py --check                 # 基础检查
python3 service.py --port 8000 --db mooncake.db
curl http://127.0.0.1:8000/health          # 健康检查（原有接口保持不变）
```

## 测试

```bash
npm test
# 等价于 python3 -m unittest -v service_contract test_coordination
```

健康检查契约（`service_contract.py`）保持原行为；`test_coordination.py` 覆盖
排产、拆分/合批/合箱反查、幂等补传、冻结级联与门店定位、只增记录、决定重放、
工时报酬共 30+ 个端到端用例。

## 架构

```
HTTP (service.py, 标准库 ThreadingHTTPServer)
        │
commands.py   命令层：业务校验 → 追加事件（App 写锁串行化，整链"查重→校验→落库"）
        │
domain.py     事件归约：当前状态、批次谱系图、冻结级联、工时报酬（纯函数，可重放）
        │
eventstore.py SQLite 只增事件账本：触发器拒绝 UPDATE/DELETE，event_id 幂等去重
```

事件同时记录 `business_time`（业务发生时间，允许补录历史）与 `recorded_at`
（实际落库时间）。状态是事件流的纯函数：任何时候重新 fold 全部事件结果一致。

## 需求到能力的对应

| 需求 | 实现 |
| --- | --- |
| 订单承诺落到批次/设备/工序/班次 | `POST /v1/schedules`，plan 含 processes（工序+设备+班次）与 allocations（原料批次+数量），可带 requirements 校验投料是否足额 |
| 一批面团拆开加工仍可反查 | `POST /v1/lots/split` 记录父子谱系边；`GET /v1/trace?code=` 双向追溯 |
| 多批成品合箱仍可反查 | `POST /v1/boxes` 的 `items` 支持多批次合箱；箱码保存全部来源批次 |
| 断网补传不重复扣料 | 每条写命令带 `idem_key`（如扫描记录号），**业务校验前先查重**；重复提交返回首条事件与 `duplicated:true`，HTTP 200 |
| 过敏原/检验异常立即冻结 | `POST /v1/allergens`、`POST /v1/quality-inspections`（status=failed/anomaly）沿谱系双向级联冻结批次与箱码，冻结中禁止扣料/装箱/出库 |
| 找出已发往哪些门店 | 冻结响应直接给 `stores`：门店、出库单、箱码、仍在途数量；`GET /v1/freezes` 可复查 |
| 质检/返工/出库/退货/报告不覆盖旧记录 | 全部只增事件；SQLite 触发器物理禁止 UPDATE/DELETE；返工完成、退货、报告均追加明细。报告可用历史 `business_time` 补录 |
| 重放某订单当时的排产决定 | `GET /v1/orders/{id}/replay-decision`（`?version=0` 可回看更早版本），把账本截到排产事件序号重新归约，返回当时的计划与原料库存 |
| 村民工时核对产量与报酬 | `POST /v1/efforts` 记工时、产出事件记件；`GET /v1/workers/{code}/payroll` 算工时+计件报酬；`GET /v1/reconciliation?order_id=` 标出有产量无工时的异常行 |
| 换班后半成品谁接手 | `GET /v1/timeline?code=` 返回批次/箱码经历的每道工序、班次、设备、工人与时间 |

## 主要接口

写接口均为 `POST` + JSON，除业务字段外可带：
`idem_key`（幂等键，扫描枪必带）、`business_time`（ISO 时间，缺省当前 UTC）。

### 基础资料与订单
- `POST /v1/materials` 登记原料批次（含 `allergen_flags`）
- `POST /v1/workers` 登记村民/工人（时薪）
- `POST /v1/orders` 接单；`POST /v1/orders/{id}/cancel`

### 排产与生产
- `POST /v1/schedules` 排产承诺
- `POST /v1/consumptions` 扫描扣料（幂等键防重复）
- `POST /v1/lots/split` 拆分批次；`POST /v1/lots/merge` 合批
- `POST /v1/lots/produce` 工序产出（可同时核销投入批次、登记计件工人）
- `POST /v1/lots/finish` 标记成品完工
- `POST /v1/boxes` 装箱（`items` 多批合箱；`packaging` 核销包装材料批次）

### 质量与处置
- `POST /v1/quality-inspections` 质检 passed/failed/anomaly
- `POST /v1/allergens` 过敏原告警
- `POST /v1/freezes` 人工冻结；`POST /v1/freezes/release` 解冻（解冻不覆盖退货状态）
- `POST /v1/reworks` 返工单；`POST /v1/reworks/{id}/complete`
- `POST /v1/shipments` 出库（冻结箱、已出库箱自动拦截）
- `POST /v1/shipments/{id}/returns` 退货（超额退货拒绝；支持分批退）
- `POST /v1/reports` 报告补录（order/lot/box/shipment/worker）

### 工时
- `POST /v1/efforts` 班次工时（可补录历史）

### 查询
- `GET /v1/state` 当前全量投影；`GET /v1/events?up_to_seq=N` 原始事件流
- `GET /v1/lots`、`GET /v1/lots/{code}`、`GET /v1/boxes`、`GET /v1/boxes/{code}`
- `GET /v1/orders`、`GET /v1/orders/{id}`（含排产历史、出库/退货/返工/质检/报告明细）
- `GET /v1/trace?code=`、`GET /v1/timeline?code=`
- `GET /v1/freezes?active=1`、`GET /v1/shipments`、`GET /v1/shipments/{id}`
- `GET /v1/workers`、`GET /v1/workers/{code}`、`GET /v1/workers/{code}/payroll`
- `GET /v1/reconciliation`、`GET /v1/orders/{id}/replay-decision`

### 错误约定
- `400` 请求不合法；`404` 实体不存在；`409` 状态冲突（冻结、超量、重复等）
- 写成功返回 `201 {"event": ..., "duplicated": false}`；幂等命中返回 `200` 且 `duplicated: true`

## 典型链路示例

```bash
# 1. 原料 / 订单
curl -s -XPOST localhost:8000/v1/materials -d '{"lot_code":"M-PORK-01","material":"黑猪叉烧","qty":100,"unit":"kg","allergen_flags":["大豆(酱油)"]}'
curl -s -XPOST localhost:8000/v1/orders   -d '{"order_id":"O-1","product":"黑猪叉烧月饼","qty":1000,"unit":"个","store":"商场总店"}'

# 2. 排产承诺：批次 + 工序 + 设备 + 班次
curl -s -XPOST localhost:8000/v1/schedules -d '{
  "order_id":"O-1",
  "processes":[{"process":"烘烤","equipment":"1号炉","shift_id":"S-AFTERNOON"}],
  "allocations":[{"lot_code":"M-PORK-01","qty":40}]
}'

# 3. 扫描枪扣料（断网重发时原样重放本条即可）
curl -s -XPOST localhost:8000/v1/consumptions -H 'Content-Type: application/json' -d '{
  "idem_key":"scan-7788","lot_code":"M-PORK-01","order_id":"O-1","qty":20,
  "process":"包馅","equipment":"1号包馅机","shift_id":"S-MORNING"}'

# 4. 异常：冻结并立即知道已发门店
curl -s -XPOST localhost:8000/v1/allergens -d '{
  "scope":[{"kind":"lot","ref":"M-PORK-01"}],"allergen":"大豆(酱油)"}'
```

数据库默认落在 `mooncake_coordination.db`，可用 `--db` 指定；重启后事件与状态自动恢复。
