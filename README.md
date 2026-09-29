# 本地批处理任务配额调度服务（batch-job-quota-scheduler）

一个只依赖本地环境的小型批处理任务调度服务：通过 HTTP 提交 / 查询 / 取消任务；
在多个任务竞争有限计算资源时，按**优先级 + 提交先后（严格 FIFO）**派发；
强制执行**集群总容量**与**调用方配额**两类约束；所有状态变更持久化到本地
JSON 快照，服务重启后未完成任务不会丢失、资源占用不会凭空产生或被重复计算。

- 运行时：Python 3.14 + FastAPI / uvicorn（uvicorn 只监听 127.0.0.1）
- 存储：单个本地 JSON 文件（原子写：临时文件 + `fsync` + rename）
- 任务负载：**确定性模拟**（succeed / sleep / fail），不访问任何外部服务或凭据

## 1. 快速开始

```bash
uv sync                                  # 安装锁文件里的依赖
uv run python main.py                    # 启动服务 http://127.0.0.1:8000
# 交互式文档：http://127.0.0.1:8000/docs
```

带配置启动（全部环境变量均可选）：

```bash
SCHEDULER_CLUSTER_CAPACITY=6 \
SCHEDULER_QUOTAS='{"alice": 4}' \
SCHEDULER_STATE_FILE=data/scheduler-state.json \
uv run python main.py
```

| 环境变量 | 默认值 | 含义 |
| --- | --- | --- |
| `SCHEDULER_CLUSTER_CAPACITY` | `8` | 集群总资源槽（正整数） |
| `SCHEDULER_DEFAULT_QUOTA` | 等于总容量 | 未显式配置的调用方可并发占有的资源上限 |
| `SCHEDULER_QUOTAS` | `{}` | JSON 对象，调用方 -> 配额，如 `{"alice": 4}` |
| `SCHEDULER_DEFAULT_TIMEOUT` | `30` | 未指定超时时的任务超时秒数 |
| `SCHEDULER_MAX_TIMEOUT` | `300` | 单个任务允许申请的超时上限，超过按非法请求拒绝 |
| `SCHEDULER_STALE_QUEUE_SECONDS` | `60` | 排队超过该秒数后，查询结果中 `stale=true` |
| `SCHEDULER_STATE_FILE` | `data/scheduler-state.json` | 快照文件路径 |
| `SCHEDULER_TICK_INTERVAL` | `0.1` | 调度循环定时间隔（秒），退避到期也靠它兜底 |
| `SCHEDULER_LOG_LEVEL` | `INFO` | 日志级别 |

## 2. HTTP 接口

调用方身份通过请求头 `X-Caller` 传递（缺省为 `anonymous`）。

### 2.1 提交任务 `POST /jobs`

```bash
curl -s -X POST http://127.0.0.1:8000/jobs \
  -H 'Content-Type: application/json' -H 'X-Caller: alice' \
  -d '{
    "job_id": "job-001",
    "resource": 2,
    "priority": 10,
    "max_retries": 1,
    "timeout_seconds": 5.0,
    "workload": {"type": "sleep", "duration_seconds": 0.5}
  }'
```

请求字段：

| 字段 | 必填 | 约束 | 说明 |
| --- | --- | --- | --- |
| `job_id` | 是 | 1–128 字符 | 客户端指定的幂等 ID；重复提交会被拒绝 |
| `resource` | 是 | 1–10000 的整数 | 任务需要的资源槽 |
| `priority` | 否 | 0–100，默认 10 | 越大越优先 |
| `max_retries` | 否 | 0–10，默认 0 | 失败/超时后的额外尝试次数（总尝试 = `max_retries+1`） |
| `timeout_seconds` | 否 | >0 且 ≤ 3600 | 单次尝试超时，缺省用服务默认值，且不得超过服务上限 |
| `workload.type` | 是 | `succeed` / `sleep` / `fail` | 模拟负载类型 |
| `workload.duration_seconds` | 否 | 0–3600，默认 0 | sleep 时长，也是 succeed/fail 前的等待 |

成功返回 `201`，响应中 `accepted=true, queued=true`（提交只负责入队，
是否立即运行由调度循环决定）。任务可能立即处于 `running`，也可能 `queued`。

### 2.2 查询

- `GET /jobs/{job_id}`：单个任务详情（含 `status` / `attempts` /
  `wait_seconds` / `stale` / `fail_reason` / `fail_detail` 等）。
- `GET /jobs?status=queued&limit=100`：列表，可带 `X-Caller` 只看自己的任务。
- `GET /stats`：集群总容量、已用/可用资源、各状态计数、运行中与排队中的
  任务 ID（排队按调度顺序返回），以及每个调用方当前占有的资源。

### 2.3 取消 `POST /jobs/{job_id}/cancel`

- 任务在排队：立即变为 `canceled`，返回 `canceled=true`；
- 任务在运行：发出协作式取消（并中断底层协程），请求立即返回
  `canceled=false`，任务随后异步落到 `canceled`（轮询 GET 确认）；
- 任务已结束：返回 `409`，`reason=already_terminal`，与其他拒绝原因区分。

## 3. 调度规则

1. **排序**：`priority` 降序，相同优先级按 `enqueue_seq` 升序（提交顺序）。
2. **派发循环**：按上面的顺序从头到尾扫描，对队首任务依次检查：
   1. 是否处于重试退避期（`not_before`）；
   2. 集群剩余容量是否够：`used + resource <= cluster_capacity`；
   3. 该调用方配额是否够：`caller_used + resource <= quota(caller)`。
   三者都满足才派发。
3. **严格 FIFO，不允许插队**：扫描到**第一个**当前不能派发的任务时，
   本轮派发立即停止——即使后面更小的任务（甚至别的调用方的任务）恰好放得
   下也不会越过它。下一次提交 / 完成 / 取消 / 退避到期 / 定时 tick 都会
   重新触发调度。这是一个有意的、稳定且可解释的取舍：队首不被任何后到
   任务超越。
4. **配额语义**：配额只限制该调用方**运行中**任务占有的资源总量；排队中的
   任务不占有资源、不消耗配额。已经入队的任务不会因为配额变紧而被拒绝，
   它只会等待。
5. **并发**：所有派发都在一个 asyncio 事件循环里、在同一把锁下完成；
   负载协程并发执行，但引擎状态的读写都经过该锁。
6. **失败重试**：负载异常（`fail`）或超时（`timeout`）后，只要
   `attempts <= max_retries` 就带指数退避（0.05s 起，上限 2s）重新入队；
   重试期间资源已经在失败瞬间释放，**不会多占**；超过上限则 `failed`。
7. **排队老化**：排队中的任务在 GET 响应里持续给出 `wait_seconds`，
   超过 `SCHEDULER_STALE_QUEUE_SECONDS` 时 `stale=true`，排队太久一眼可见。

## 4. 拒绝原因（提交/取消时 HTTP 4xx，`detail.reason`）

| reason | HTTP | 触发条件 |
| --- | --- | --- |
| `invalid_request` | 400/422 | 参数非法：超时超过上限、状态过滤值未知等；schema 级非法（resource≤0、priority 越界、未知 workload 类型等）由 pydantic 直接返回 422 |
| `capacity_exceeded` | 422 | 单个任务的资源请求 **> 集群总容量**（这种任务永远不可能运行） |
| `quota_exceeded` | 422 | 单个任务的资源请求 **> 该调用方配额**（即使集群放得下） |
| `duplicate_job` | 409 | `job_id` 已存在（无论该任务是否已结束）；响应正文带当前状态，可据此改查 GET |
| `not_found` | 404 | 任务不存在（带 `X-Caller` 查询/取消他人任务时同样按不存在处理） |
| `already_terminal` | 409 | 取消已经 `succeeded` / `failed` / `canceled` 的任务 |

判定顺序保证原因可区分：先查重复 → 容量 → 配额。因此"既超容量又超配额"
时报 `capacity_exceeded`；"集群放得下但个人配额放不下"时报 `quota_exceeded`。
注意：**容量/配额当前不够（但请求本身合法）不会被拒绝**，而是正常排队
（返回 201、`queued=true`）；只有"永远无法满足"的请求才直接拒绝。

## 5. 失败原因（任务记录上的 `fail_reason`）

| fail_reason | 含义 |
| --- | --- |
| `workload_failed` | 模拟负载自身失败（`workload.type=fail` 或执行异常） |
| `timeout` | 单次尝试超过 `timeout_seconds` 被中断 |
| `canceled` | 排队时被取消，或运行中收到取消并完成收尾 |
| `retries_exhausted` | （说明性文案）重试预算用尽；最终记录仍保留最后一次的具体原因（`workload_failed` 或 `timeout`），`fail_detail` 中含 "retries exhausted after N attempt(s)" |
| `restart_interrupted` | 重启时发现该任务上次在运行中，被中断后重新入队；若随后成功，该标记会清除，若最终失败则被具体失败原因覆盖 |

## 6. 资源回收与重启恢复

- **资源只在派发时增加一次**（`_admit`），**只在收尾时释放一次**
  （`_finalize` / 停机处理）。成功、失败、超时、取消、重试全部走同一条
  收尾路径，`used` 与每个调用方的占用一起更新，并有非负保护。
- **每次状态变更都全量持久化**（原子 rename，崩溃也不会写坏文件）。
- **启动恢复**：
  - 快照中 `queued` / 已结束的任务原样恢复；
  - 快照中 `running` 的任务（说明上次进程没跑完就退出，无论优雅停机还是
    崩溃）一律**重新置为排队**，标记 `restart_interrupted`，随后由调度器
    重新派发，`attempts` 保留（不重置、不累加额外次数）；
  - 资源占用**不信任磁盘上的计数字段，而是按恢复后的任务状态重算**：
    重排队瞬间没有任何运行中任务，所以 `used=0`，既不会凭空占着资源，
    也不会重复计数。
- **优雅停机**：已收到取消请求的运行中任务按 `canceled` 落终态；其余运行
  中任务重新入队，保证"未完成的任务不会凭空消失"。

## 7. 本地验证

### 7.1 自动化测试（无需外部服务）

```bash
uv run python -m unittest tests.test_scheduler -v
```

29 个用例，覆盖：

- **并发派发**：容量边界（如容量 6、4 个各需 2 的任务至多 3 个并发，
  第 4 个排队；全部结束后 `used=0`）；
- **优先级/FIFO**：高优先级先于低优先级；同优先级下"放得下的小任务"也
  不能越过队首；
- **配额与容量边界**：非法参数、超容量、超配额、重复提交四类拒绝的状态码
  与 reason 互不相同；配额只约束运行中占有；
- **取消链路**：取消排队任务、取消运行中任务、重复取消/取消已结束任务
  返回可区分的确定结果；取消一个任务不影响另一个任务的资源；
- **失败/重试/超时**：尝试次数上限、退避后重试、超时原因区分、每种终态后
  资源都归还；
- **排队老化**：等待超过阈值时 `stale=true` 且有 `wait_seconds`；
- **重启恢复**：优雅重启后已结束/排队任务存活、运行中任务重新入队；
  模拟**硬崩溃**（不执行停机收尾）后新进程重算资源（无幻影占用）并把任务
  跑完；取消请求跨重启仍然生效。

### 7.2 手工 curl 验证

```bash
uv run python main.py &

# 提交一个大任务占住资源，再提交一个放不进的任务观察 queued 与排队顺序
curl -s -X POST localhost:8000/jobs -H 'Content-Type: application/json' \
  -d '{"job_id":"a","resource":6,"workload":{"type":"sleep","duration_seconds":5}}'
curl -s -X POST localhost:8000/jobs -H 'Content-Type: application/json' \
  -d '{"job_id":"b","resource":4,"workload":{"type":"succeed"}}'
curl -s localhost:8000/stats                       # running=[a], queued=[b]

# 三种典型拒绝
curl -s -X POST localhost:8000/jobs -H 'Content-Type: application/json' \
  -d '{"job_id":"x","resource":99,"workload":{"type":"succeed"}}'   # capacity_exceeded
curl -s -X POST localhost:8000/jobs -H 'Content-Type: application/json' -H 'X-Caller: alice' \
  -d '{"job_id":"y","resource":9,"workload":{"type":"succeed"}}'    # 422 schema
curl -s -X POST localhost:8000/jobs/a/cancel -X POST                 # 取消 a，资源立即归还

# 观察 data/scheduler-state.json，然后 kill 进程再启动：未完成任务自动恢复
```

服务端日志对每个关键判定都会打印**输入与依据**，例如：

```
... scheduler.engine | submit accepted job_id=b caller=anonymous resource=4 priority=10 ... (capacity=8 used=6 caller_quota=8 caller_used=0)
... scheduler.engine | dispatch stop: head job_id=b needs=4 capacity_left=2 (used=6/8); later jobs must not overtake it
... scheduler.engine | job a CANCELED, resource 6 released, used=0/8
... scheduler.engine | recovery complete jobs=2 requeued_running=1 recomputed_used=0 (persisted counter was 6; ignored, recomputed from job states)
```

## 8. 项目结构

```
main.py                      # 入口：配置 + 日志 + uvicorn
scheduler/
  models.py                  # 状态机、拒绝/失败原因枚举、Job 模型与序列化
  config.py                  # 环境变量配置
  schemas.py                 # HTTP 请求/响应 pydantic 模型
  persistence.py             # 原子 JSON 快照
  workload.py                # 确定性模拟负载（succeed/sleep/fail）
  engine.py                  # 准入、调度循环、执行、重试、取消、恢复
  logging_setup.py           # 日志格式
  app.py                     # FastAPI 路由与错误码映射
tests/test_scheduler.py      # 29 个 HTTP 级集成测试（stdlib unittest + httpx）
```
