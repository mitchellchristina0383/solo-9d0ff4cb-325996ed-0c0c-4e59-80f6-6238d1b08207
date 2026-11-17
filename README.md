# 写入切流反向代理（FastAPI + SQLite）

值班工程师更新服务节点时，用本系统把写入流量从旧节点切到备用节点。核心保证：

- **不中断已送达的写入**：每个写入在投递前快照现役节点并持久化绑定，切流只改路由表，
  不取消在途请求；回执仍从**原送达节点**读取，连续多次切流也不打断它。
- **不重复投递**：代理对任何写入只发一次。连接失败标 `FAILED`、读超时标
  `TIMEOUT_UNCERTAIN`，都**不会**自动改投另一节点；后端两侧写入 ID 集合永不相交。
- **探测失败保持旧路由**：切流前先探目标节点 `/healthz`，失败返回 409，路由表不变。
- **超时显式不确定**：写入已在后端落库但回执超时（读超时）时返回 504 +
  `TIMEOUT_UNCERTAIN`，页面醒目提示，可事后点"向原节点查询"核实，由原节点就地更正状态。
- **变更记录可按持久游标续看**：成功切流、写入受理、写入结论改变时，把节点、写入 ID
  （如适用）与新状态记入变更记录。事件与对应的路由/写入状态在**同一 SQLite 事务**中提交：
  事务失败则状态和事件一起回滚，不会留下虚假事件；已生效的变化也一定有据可查。
  事件 ID 是持久、单调递增的游标，`GET /api/changes?after=<id>` 按提交顺序翻页，
  浏览器断线或代理重启后从上次游标继续，不重不漏。核实未改变结论、no-op 切流、
  被探测拦截的切流都不会产生变更事件（只留在原始日志里）。

## 目录结构

```
backend/            模拟写入后端（两个实例，各自 SQLite 保存收到的写入）
  app/main.py       慢回执 delay、健康故障注入 /control/fault、/healthz、/records
  Dockerfile
proxy/              FastAPI 反向代理 + 切流控制台
  app/main.py       代理、切流、探测、在途/排空跟踪、故障注入透传
  app/db.py         SQLite 持久层（nodes / route / writes / events），事务化状态+事件
  app/dashboard.html 切流网页（现役节点、排空状态、写入记录、变更续看、事件日志）
  Dockerfile
docker-compose.yml  一键启动 proxy + backend-a + backend-b（含健康检查与数据卷）
scripts/selftest.py 纯标准库端到端自测（30 项断言，原有投递/切流语义）
scripts/selftest_changes.py 变更记录/持久游标专项自测（72 项断言，含重启、交错与事务回滚）
```

## 用 Docker Compose 启动

```bash
docker compose up --build
# 控制台： http://localhost:8080/
```

- 后端各自把写入保存在卷 `backend-a-data` / `backend-b-data` 的 `/data/writes.db`；
  代理状态在卷 `proxy-data` 的 `/data/proxy.db`。
- 想演练回执超时，可把 compose 里 `WRITER_READ_TIMEOUT` 调小（如 `2`）后重建。

## 不用 Docker 的本地运行（开发/自测）

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r proxy/requirements.txt        # 后端依赖相同
mkdir -p .run/data
NODE_NAME=A BACKEND_DB=$PWD/.run/data/a.db \
  python -m uvicorn app.main:app --app-dir backend --port 8001 &
NODE_NAME=B BACKEND_DB=$PWD/.run/data/b.db \
  python -m uvicorn app.main:app --app-dir backend --port 8002 &
NODE_A_NAME=A NODE_B_NAME=B \
BACKEND_A_URL=http://127.0.0.1:8001 BACKEND_B_URL=http://127.0.0.1:8002 \
PROXY_DB=$PWD/.run/data/proxy.db \
  python -m uvicorn app.main:app --app-dir proxy --port 8080
```

## 网页操作演练

1. 慢回执延迟填 `10` 秒，点"投递写入"，在请求返回前立刻点另一节点的"切换流量"；
2. 观察：现役节点立即变更；旧节点卡片显示 **DRAINING 排空中** 与在途写入 ID；
   慢请求结束后仍由旧节点回执，写入只存在于旧节点；
3. 点某节点"注入健康故障"后再向它切流：收到 409 拦截提示，现役节点不变；
4. "拉取 A/B 的记录"可对比两个后端各自 SQLite 中保存的写入；
5. 超时写入显示红色 **TIMEOUT_UNCERTAIN**，点"向原节点查询"可核实真实落库情况；
6. "变更记录"卡片可按游标续看：把上次看到的游标 ID 填入 `after` 点"从游标续看"，
   浏览器会记住最后追平的游标（localStorage），断线/刷新/代理重启后自动接着看；
   勾选"每 3 秒自动续看"可实时追加新变更。若 events 卷被重建、服务端游标比本地小，
   页面会提示并自动从头追看，不会卡在失效游标上。

## HTTP 接口（代理 8080）

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/writes?delay=秒` | 投递写入（代理只投一次，绑定当前现役节点）；重复 ID 返回 409 且不投递 |
| POST | `/api/switch` | body `{"target":"B"}`；探测失败 409 并保持旧路由 |
| GET  | `/api/status` | 现役节点、健康、排空/在途、全部写入、最近事件、最新变更游标 |
| GET  | `/api/changes?after=<id>&limit=N` | 按持久游标续看变更（`id > after`，升序，limit 1..500） |
| GET  | `/api/writes/{id}/refresh` | 向**原送达节点**查询回执并就地更新状态 |
| GET  | `/api/nodes/{name}/records` | 透传该后端自己保存的写入记录 |
| POST | `/api/nodes/{name}/fault` | 注入/解除健康故障 `{"on": true|false}` |

`/api/changes` 响应示例：

```json
{
  "after": 41, "limit": 100, "latest": 45,
  "has_more": false, "next_after": 45, "count": 4,
  "events": [
    {"id": 42, "ts": 1794948670.1, "kind": "SWITCH_OK",
     "node": "B", "write_id": null, "state": "B", "message": "A -> B ok; ..."},
    {"id": 43, "ts": 1794948670.2, "kind": "WRITE_ACCEPTED",
     "node": "B", "write_id": "…", "state": "PENDING", "message": "…"},
    {"id": 44, "ts": 1794948670.3, "kind": "WRITE_ACKED",
     "node": "B", "write_id": "…", "state": "ACKED", "message": "…"},
    {"id": 45, "ts": 1794948672.0, "kind": "WRITE_RESOLVED",
     "node": "A", "write_id": "…", "state": "ACKED", "message": "…"}
  ]
}
```

变更类型：`SWITCH_OK`（成功切流，`node`/`state` 为目标节点）、`WRITE_ACCEPTED`
（受理，`state=PENDING`）、`WRITE_ACKED`、`WRITE_TIMEOUT_UNCERTAIN`、`WRITE_FAILED`、
`WRITE_RESOLVED`（事后核实改变结论）。`BOOT`、`SWITCH_BLOCKED`、`FAULT_INJECTION`
等只是原始日志，不出现在 `/api/changes` 中。

## 自测

```bash
python -I scripts/selftest.py            # 自动拉起 3 个进程，跑完即清理
python -I scripts/selftest_changes.py    # 变更记录/游标专项自测
```

`selftest.py` 覆盖：普通写入无重复、慢回执在途切流、探测失败保旧路由、连续再次切流不打断
在途写入、读超时 504 不确定且不补投（事后向原节点核实可转 ACKED）、两侧后端记录不相交且与
代理一一对应。

`selftest_changes.py` 覆盖：受理/回执变更字段、核实同一结论不记事件、重复写入 ID 拒绝且
无虚假事件、no-op/被拦截切流不记事件、并发写入+切流交错下小页游标翻页不漏不重、
翻页同时持续写入/核实时的流仍升序且与事后全量一致、超时核实无变化不记而改变结论恰好记一次、
代理重启后从旧游标续看不重不漏、用受控慢回执后端制造"核实先确认 ACKED、迟到读超时后到"
的交错（结论不回退、无虚假超时事件）、用 SQLite 触发器注入事件写入失败以验证状态与事件
同事务回滚。页面行为另用 jsdom 模拟验证（自动续看、刷新后按 localStorage 游标续看、
手动游标重载、切流/写入后即时追加）。

## 关键环境变量

| 变量 | 默认 | 含义 |
| --- | --- | --- |
| `WRITER_READ_TIMEOUT` | 30 | 等回执读超时秒数，超时即结果不确定 |
| `CONNECT_TIMEOUT` | 3 | 连接超时，连接失败视为未送达 `FAILED` |
| `PROBE_TIMEOUT` | 2.5 | 切流健康探测超时 |
| `BACKEND_A_URL` / `BACKEND_B_URL` | 本地 8001/8002 | 两个后端地址 |
