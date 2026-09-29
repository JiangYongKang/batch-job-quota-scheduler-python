# 本地批处理任务配额调度服务（Batch Job Quota Scheduler）

一个**只依赖本地环境**的小型批处理调度服务：通过 HTTP 提交 / 查询 / 取消任务，
在多个任务竞争有限计算资源时，给出**稳定、可解释**的调度结果，并在服务重启后
恢复未完成任务、不丢失也不重复计算资源占用。

- 语言/运行时：Python 3.14（`requires-python >= 3.14.4`）
- 依赖：仅 `fastapi[standard]`（仓库 `uv.lock` 已锁定），无外部服务、无真实凭据
- 持久化：本地单个 JSON 快照（原子写：临时文件 + `os.replace`）
- 任务执行：本地确定性模拟（按声明的时长等待 + 可声明失败次数 / 超时），不需要任何 worker 集群

---

## 1. 快速开始

```bash
uv sync                       # 安装已锁定依赖
uv run python main.py         # 启动服务，默认监听 127.0.0.1:8000
```

另开一个终端：

```bash
curl -s localhost:8000/status
curl -s -X POST localhost:8000/jobs -H 'Content-Type: application/json' \
  -d '{"name":"demo","owner":"alice","resource":2,"duration":0.5}'
```

提交后可从返回体拿到 `job_id`，再查询：

```bash
curl -s localhost:8000/jobs/<job_id>
curl -s -X POST localhost:8000/jobs/<job_id>/cancel
```

交互式文档：启动后访问 <http://127.0.0.1:8000/docs>。

---

## 2. 资源模型

- 集群有一个整数容量 `cluster_capacity`（默认 **10** 个资源单位）。
- 每个任务提交时声明 `resource`（**正整数**资源单位）。
- **同一时刻所有 `RUNNING` 任务的 `resource` 之和永远不超过集群容量。**
- 资源占用完全由任务状态派生：只有 `RUNNING` 占资源。任务进入成功 / 失败 /
  取消 / 退避 / 过期等状态即释放资源，没有独立计数器，因此不存在泄漏或重复计算。

### 每个调用方的配额

- 配额按 `owner`（提交方身份）维度生效，默认每个 owner 最多占用 **6** 单位。
- 配额计入该 owner **所有非终态任务**（排队中、运行中、退避中）占用的资源总和，
  而不只是运行中的任务。这样可以防止某个调用方用“大量排队任务”淹没队列。
- 任务到达终态后立即释放配额。

---

## 3. 调度规则

派发顺序（全局）：

1. **优先级高的先派发**（`priority` 数值越大越优先，默认 0）。
2. **同优先级严格按提交先后**（全局单调递增的 `seq`），**不允许插队**。
3. 逐优先级扫描：如果某优先级的队头任务当前剩余容量放不下，则
   **该优先级后续任务以及所有更低优先级任务本轮都不再派发**——既保证同优先级不插队，
   也避免低优先级任务抢占高优先级队头正在等待的资源。容量一释放立即重新评估。

重试与退避：

- 任务执行失败或超时可重试，**可重试失败次数上限**为 `max_attempts`（默认 3），
  即累计出现 1…`max_attempts` 次 FAILED/TIMEOUT 后终结为 `FAILED`。
- 一次尝试失败后**先释放全部资源**，任务进入 `BACKOFF`，退避结束后**带着原来的
  `seq` 重新排队**，因此重试任务不会因为重试而“后来居上”。
- 退避期间任务不占任何资源、不阻塞其他任务。
- **尝试计数与重试预算是两件事**：`attempt_count` / `attempts` 记录所有“已开始”的
  尝试（含重启导致的 `INTERRUPTED`）；但只有 `FAILED/TIMEOUT` 会计入重试预算，
  `INTERRUPTED`（进程重启/关闭打断）**不消耗重试次数**。

排队超时：

- 任务可声明 `queue_timeout`。在队列中等待超过该时长仍未被派发，则以
  `EXPIRED / QUEUE_TIMEOUT` 终结，便于识别“排队过久”的任务。查询任务时会返回
  `waited_seconds`，列表接口也能看到排队中的任务。

### 任务状态机

```
WAITING ──派发──► RUNNING ┬─成功──────────────► SUCCEEDED   （终态）
   ▲                     ├─取消信号──────────► CANCELED    （终态）
   │                     ├─失败/超时且次数耗尽► FAILED      （终态，带 fail_reason）
   │                     └─失败/超时但还能重试─► BACKOFF ──退避结束──► WAITING
   └─重启恢复（RUNNING 的在途尝试记为 INTERRUPTED，不消耗重试次数）
WAITING └─排队超时────────────────────────────► EXPIRED     （终态）
WAITING/BACKOFF └─取消─────────────────────────► CANCELED    （终态）
```

---

## 4. HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `POST` | `/jobs` | 提交任务（201）。非法 / 超容量 / 超配额返回 **400** 且原因可区分 |
| `GET` | `/jobs` | 列出任务，支持 `?owner=`、`?status=` 过滤 |
| `GET` | `/jobs/{job_id}` | 查询单个任务（404 = 不存在） |
| `POST` | `/jobs/{job_id}/cancel` | 取消任务 |
| `GET` | `/status` | 集群容量 / 已用 / 各 owner 配额用量 / 各状态任务数 |

### 提交参数

| 字段 | 类型 | 默认 | 含义 |
| --- | --- | --- | --- |
| `name` | string | `"job"` | 任务名 |
| `owner` | string | 必填 | 调用方身份，配额主体 |
| `resource` | int | 必填 | 所需资源单位（正整数） |
| `priority` | int | `0` | 越大越优先 |
| `duration` | float | `0.1` | 模拟执行时长（秒） |
| `fail_times` | int | `0` | 前 N 次尝试模拟失败，之后成功 |
| `timeout` | float | 30 | 单次尝试超时（秒）；`duration > timeout` 即超时 |
| `queue_timeout` | float | 无 | 排队最长等待时间（秒） |
| `max_attempts` | int | `3` | 总尝试次数上限（含首次） |
| `job_id` | string | 自动生成 | 客户端幂等键；重复提交同一 `job_id` 返回原任务 |

### 取消结果（`result` 字段，确定且可区分）

- `CANCELED`：任务在 `WAITING/BACKOFF`（不占资源），已直接终结。
- `CANCELING`：任务在 `RUNNING`，已发出取消信号，由执行链路收尾后变 `CANCELED`。
- `ALREADY_TERMINAL`：任务已是终态（成功/失败/取消/过期），取消是无操作，结果幂等。

---

## 5. 拒绝原因与失败原因（可区分）

### 提交被拒绝：HTTP 400，`error = SUBMIT_REJECTED`

| `reason` | 含义 |
| --- | --- |
| `INVALID_RESOURCE` | `resource` 不是正整数（缺失、0、负数、非整数、布尔值等） |
| `EXCEEDS_CAPACITY` | 单个请求超过集群总容量，**任何时候都不可能被调度**，直接拒绝 |
| `EXCEEDS_QUOTA` | 该 owner 现有非终态占用 + 本次请求会超过其配额，拒绝 |

其余错误：`404 NOT_FOUND`（任务 id 不存在）。

> 说明：在本实现中，超过容量 / 超过配额 / 非法请求都是**确定性拒绝（400）**，
> 因为对一个永远无法满足的请求无限排队没有意义；容量暂时不足但请求合法
> （`resource ≤ 容量` 且 `owner 用量 + resource ≤ 配额`）时才**排队等待**。

### 任务终态失败：`status = FAILED` 或 `EXPIRED`，`fail_reason` 可区分

| `fail_reason` | 含义 |
| --- | --- |
| `ATTEMPT_FAILED` | 尝试返回失败（模拟异常），且 `max_attempts` 已耗尽 |
| `ATTEMPT_TIMEOUT` | 最后一次尝试执行超过 `timeout`，次数耗尽 |
| `QUEUE_TIMEOUT` | 排队等待超过 `queue_timeout` 仍未派发（`status=EXPIRED`） |

### 重复提交

- 相同 `job_id`（客户端幂等键）重复提交：返回 **200/201 + `"duplicate": true`**
  以及已存在的任务，不会创建第二个任务，也不会重复计费。

每个任务的 `attempts` 数组完整记录每次尝试的序号、起止时间、结果
（`SUCCESS/FAILED/TIMEOUT/CANCELED/INTERRUPTED`）与说明，作为审计依据。

---

## 6. 重启恢复

- 每次状态变更都把全部任务原子写入 `SCHED_STORE_PATH`（默认 `data/scheduler.json`）。
- 进程收到 SIGTERM/SIGINT 时会先停调度循环、把所有在途尝试记为 `INTERRUPTED` 并
  重新排队后再落盘退出，因此**正常停止时快照里不会出现 RUNNING**。
- 即使进程被 `SIGKILL` 强杀（来不及收尾），启动加载时也会把未闭合的尝试补记为
  `INTERRUPTED` 再恢复：
  - 重启前 `RUNNING` 的任务：在途尝试标记为 **`INTERRUPTED`**（不消耗重试次数），
    任务重新进入 `WAITING`，按原顺序重新派发；
  - `WAITING/BACKOFF` 的任务：重新排队（退避计时清零，直接可派发）；
  - 已终态任务：原样保留。
- 加载完成的瞬间没有任何 `RUNNING` 任务，因此**已用容量必然为 0**，资源占用
  不会凭空消失（任务还在），也不会被重复计算（没有残留的“幽灵”占用）。

---

## 7. 关键判定日志

所有关键决策都在日志里打印**输入与依据**（logger 名 `scheduler`，输出到 stderr）：

- `submit accepted ... (basis: capacity=.. used=.. owner_usage=.. quota=..)`
- `submit duplicate idempotency-key=.. -> existing job=.. status=..`
- `dispatch: job=.. resource=.. priority=.. seq=.. attempt=.. (basis: capacity=.. used_after=..)`
- `hold: job=.. resource=.. does not fit available=..; lower/same-priority dispatch paused`
- `retry: ... resource released, backoff=..s then re-queue at seq=..`
- `finalize: ... -> SUCCEEDED / FAILED reason=.. / CANCELED; resource released`
- `expire: ... waited=..s >= queue_timeout=.. -> EXPIRED(QUEUE_TIMEOUT)`
- `recovery: ... re-queued` / `recovery: N unfinished job(s) re-queued`

---

## 8. 本地验证

### 运行自动化测试（无需任何外部服务）

```bash
uv run python -m unittest discover -s tests -v
```

覆盖四类场景，共 30 个用例：

- **并发派发**（`tests/test_dispatch.py`）：容量总和上限、同优先级 FIFO 不插队、
  高优先级超越被阻塞的低优先级队头、容量边界恰好放得下；
- **配额与容量边界**（`tests/test_quota_capacity.py`）：三类拒绝原因可区分、
  配额按 owner 隔离、终态释放配额、容量与配额拒绝互不混淆；
- **失败与取消**（`tests/test_failure_cancel.py`）：重试成功 / 退避期间资源释放 /
  次数耗尽 `ATTEMPT_FAILED` / 超时 `ATTEMPT_TIMEOUT` / 等待中取消 / 运行中取消 /
  重复取消的确定结果 / 幂等重复提交 / 排队超时；
- **重启恢复**（`tests/test_recovery.py`）：运行中任务重启后重新排队并成功、
  `INTERRUPTED` 不占重试、终态不变、重启瞬间零幽灵占用、重启后取消。

### 手工验证（示例）

```bash
uv run python main.py
# 终端 A：占住 6 单位
curl -s -X POST localhost:8000/jobs -H 'Content-Type: application/json' \
  -d '{"name":"long","owner":"a","resource":6,"duration":30}'
# 终端 B：观察排队与三类拒绝
curl -s -X POST localhost:8000/jobs -d '{"name":"x","owner":"b","resource":0}' \
  -H 'Content-Type: application/json'            # INVALID_RESOURCE
curl -s -X POST localhost:8000/jobs -d '{"name":"x","owner":"b","resource":11}' \
  -H 'Content-Type: application/json'            # EXCEEDS_CAPACITY
curl -s -X POST localhost:8000/jobs -d '{"name":"ok","owner":"b","resource":6}' \
  -H 'Content-Type: application/json'            # accepted（合法，排队等待）
curl -s localhost:8000/status                    # used=6, 队列中可见 ok
```

按 `Ctrl+C` 停止后重新 `uv run python main.py`：之前在跑的任务会被记为
`INTERRUPTED` 并重新排队，`/status` 中不会出现残留占用。

---

## 9. 配置项（环境变量）

| 变量 | 默认 | 含义 |
| --- | --- | --- |
| `SCHED_CLUSTER_CAPACITY` | `10` | 集群总资源单位 |
| `SCHED_QUOTA_PER_OWNER` | `6` | 每个 owner 配额 |
| `SCHED_DEFAULT_ATTEMPTS` | `3` | 默认尝试次数上限 |
| `SCHED_ATTEMPT_TIMEOUT` | `30` | 默认单次尝试超时（秒） |
| `SCHED_QUEUE_TIMEOUT` | 空（不限） | 默认排队超时（秒） |
| `SCHED_RETRY_BACKOFF` | `0.2` | 重试退避（秒） |
| `SCHED_TICK` | `0.02` | 调度循环周期（秒） |
| `SCHED_STORE_PATH` | `data/scheduler.json` | 持久化快照路径 |

---

## 10. 代码结构

```
main.py                     # 入口：组装配置 / 存储 / 调度器 / HTTP 应用
scheduler/
  models.py                 # 状态、拒绝/失败原因枚举，Job/Attempt 数据模型
  config.py                 # 容量、配额、超时等配置（支持环境变量）
  errors.py                 # SubmitError（携带可区分 reason）/ NotFound
  logging_setup.py          # 决策日志
  quota.py                  # 资源校验（三类拒绝）与容量/配额核算
  store.py                  # JSON 原子快照持久化
  executor.py               # 本地确定性执行模拟（成功/失败/超时/取消）
  scheduler.py              # 调度核心：派发循环、重试、取消、恢复
  api.py                    # FastAPI HTTP 接口与生命周期
tests/                      # unittest 测试（TestClient，基于临时文件存储）
```
