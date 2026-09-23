# 本地 API 网关（Python / FastAPI）

一个可完全本地运行的 API 网关层：按规则路由到多个上游、可配置且可解释的负载均衡、
被动故障摘除 + 主动探测恢复、按客户端的限流与并发控制、超时/失败熔断，以及一组
**相互可区分**的错误码。所有外部依赖均可选——默认演示拓扑使用进程内 stub 上游，
无需启动任何真实后端即可运行与验证。

## 快速开始

```bash
uv sync                       # 安装依赖（fastapi / httpx / pytest）
uv run uvicorn main:app --port 8000
```

另开终端：

```bash
curl -s localhost:8000/v1/echo/ping                 # 轮询 echo-a / echo-b
curl -s localhost:8000/v1/load/x | head -c 200      # least-inflight 路由
curl -s localhost:8000/v1/flaky/x                   # 恒失败上游（观察摘除）
curl -s localhost:8000/v1/slow/x                   # 恒超时上游（观察 504 + 熔断）
curl -s localhost:8000/_gateway/health             # 上游健康/限流/LB 内部状态
curl -i localhost:8000/no-such-route               # 结构化 404
```

运行测试：

```bash
uv run pytest -q                # 全部 36 个用例
uv run pytest -s                # 同时打印每个请求的决策日志
```

## 架构与请求管线

```
Request
  └─ Router            前缀 + 方法匹配，第一条命中规则生效；未命中 -> ROUTE_NOT_FOUND
      └─ ClientLimiter 按客户端原子检查 固定窗口速率 与 并发占用
          └─ HealthRegistry.healthy_candidates   只返回未摘除（且半开名额未满）的实例
              └─ LoadBalancer.acquire            在候选中按策略选一个并占 in-flight 槽
                  └─ Transport.send(timeout)     超时由传输层强制
                      └─ 结果回写 Health（连续失败摘除 / 成功复位）
              └─ 必要时按确定性规则重试（见下）
          └─ limiter.release（任何路径都释放并发槽）
  └─ 统一错误映射（见“错误语义”）
```

主动健康探测运行在独立后台线程，探测请求由网关直接通过 transport 发出，
**不经过 Router、ClientLimiter、LoadBalancer**，因此探测与正常业务流量的计数、
槽位、熔断互不干扰（`tests/test_recovery_integration.py` 对此有专门断言）。

## 路由与负载均衡规则

### 路由（`gateway/config.py: RouteRule`）

* `prefix`：路径前缀匹配，按配置顺序评估，**第一条命中生效**。
* 段边界匹配：`/v1` 命中 `/v1` 与 `/v1/x`，但**不**命中 `/v10/x`。
* `methods`：可选方法白名单；为空表示接受任意方法。
* `upstream_ids`：该路由可转发的一个或多个上游。
* 未命中任何规则时返回 `404 ROUTE_NOT_FOUND`，响应体 `details.tried` 逐条列出
  每条规则的前缀/方法判定依据——**绝不会静默转发到错误目标**。

### 负载均衡策略（`lb_policy`，按路由配置，选择依据全部入日志）

| 策略 | 选择规则 | 可解释字段 |
| --- | --- | --- |
| `round_robin`（默认） | 路由内单调计数器对候选数取模，确定性轮转 | `counter`、`position` |
| `random` | 对候选均匀随机 | `roll`（记录抽中下标，可复核） |
| `least_inflight` | 选当前在飞请求最少者，**并列时按配置顺序** | 各候选 `inflight` |

已摘除上游不出现在候选列表；半开（PROBING）上游仅在试探名额内出现；
同一请求重试时已尝试过的上游通过 `exclude` 排除，不会重复选择。

## 健康与熔断参数（每条路由）

| 参数 | 默认 | 含义 |
| --- | --- | --- |
| `failure_threshold` | 3 | 连续失败（含连接错误/超时/可计失败状态）达到该值即摘除 |
| `open_state_seconds` | 5.0 | 摘除后冷却时长，期间不接收任何业务流量 |
| `half_open_max_probe_requests` | 1 | 半开状态下探测/试探的**同时在飞**上限 |
| `half_open_success_threshold` | 1 | 半开后连续成功达到该值才恢复 HEALTHY |
| `probe_path` | `/health` | 主动探测路径（与业务路径分离） |
| `probe_interval_seconds` | 1.0 | 半开期间两次探测的最小间隔 |
| `probe_timeout_seconds` | 1.0 | 探测请求自身的超时 |
| `timeout_seconds` | 2.0 | 业务请求上游超时 |

状态机（每个「路由 × 上游」独立）：

```
HEALTHY ── 连续失败 >= failure_threshold ──▶ EJECTED
EJECTED ── open_state_seconds 到期（下一个探测 tick）──▶ PROBING
PROBING ── 连续成功 >= half_open_success_threshold ──▶ HEALTHY
PROBING ── 任一次探测/试探失败 ──▶ EJECTED（冷却重新计时）
```

半开试探总预算确定性地等于 `half_open_max_probe_requests +
half_open_success_threshold - 1`（默认配置下恰好只有 1 次试探机会）。
业务试探请求与主动探测共享在飞上限；探测在飞时业务流量不得占用该槽位。

## 限流与并发（`ClientRateRule`，按客户端）

客户端维度取值顺序：`X-Client-Id` 请求头 → `X-Forwarded-For` 首跳 → `anonymous`。

| 参数 | 含义 |
| --- | --- |
| `requests_per_window` | 每个固定窗口允许的请求数（0 关闭） |
| `window_seconds` | 固定窗口长度；窗口从该客户端首个请求起确定性滚动 |
| `max_concurrency` | 同一客户端同时在飞请求上限（0 关闭） |

速率与并发在**同一把锁内一次原子判定**，先判速率再判并发；
被拒请求不消耗任何计数/槽位，因此拒绝永远不会产生上游副作用。
限流命中返回 `429`（带 `Retry-After`），并发命中返回 `503 CONCURRENCY_LIMITED`。

### 重试

* 最多 `1 + max_retries` 次尝试；仅对幂等方法（GET/HEAD/OPTIONS/PUT/DELETE）重试；
* 触发条件为**最近一次**尝试是连接错误、超时或状态码 ∈ `retry_on_statuses`；
  不可重试的 4xx（如 404/403）立即终止；
* 每次重试换到另一个未尝试、未摘除的上游；非幂等请求（如 POST）绝不重试，
  因此同一请求不会在上游造成额外副作用。

## 错误语义（各类失败原因严格可区分）

所有失败均返回统一 JSON 结构：

```json
{"error": {"code": "<CODE>", "message": "...", "details": {...}}}
```

| code | HTTP | 触发条件 |
| --- | --- | --- |
| `ROUTE_NOT_FOUND` | 404 | 无任何路由规则匹配（含方法不匹配） |
| `RATE_LIMITED` | 429 | 客户端超出窗口配额（附 `Retry-After`） |
| `CONCURRENCY_LIMITED` | 503 | 客户端超出并发上限 |
| `UPSTREAM_CIRCUIT_OPEN` | 503 | 发送前该路由所有上游断路器均处于 EJECTED（熔断真正打开） |
| `ALL_UPSTREAMS_UNAVAILABLE` | 503 | 发送前无可用候选：路由无配置上游，或上游均处半开且试探名额已满（`details.reason` 区分） |
| `UPSTREAM_TIMEOUT` | 504 | 尝试真实发出但上游在 `timeout_seconds` 内未响应 |
| `UPSTREAM_ERROR` | 502 | 连接错误或上游返回非 2xx/3xx（响应体附 `upstream_status`） |

关键边界：把断路器推过阈值的**那一次请求**已真实到达上游，返回它自己的结果
（504/502）；只有其后的请求在“发送前”就无候选可用时才得到
`ALL_UPSTREAMS_UNAVAILABLE`，从而“熔断/摘除”和“普通上游错误”不会混淆。

## 决策日志

网关对每个请求打印单行 `key=value` 决策日志到 stderr：

* `#request`：方法、路径、客户端、命中规则、LB 策略与上游列表、限流判定快照；
* `#attempt`：第几次尝试、选中上游、策略依据（counter/roll/inflight）、候选列表、
  结果（success/timeout/connect_error/upstream_error）、状态码、耗时与**全量上游健康快照**；
* `#retry_decision`：是否重试及判定依据（幂等性、剩余次数、上次原因）；
* `#probe`：探测路由/上游/路径、成败、原因、耗时与健康快照；
* `#gateway_error` / `#no_upstream` / `#rejected`：最终错误码与限流原因。

用 `uv run pytest -s` 可在测试输出中直接看到这些日志。

## 本地验证方法

```bash
uv run pytest tests/test_ratelimit.py            # 并发限流：2000 线程下计数一致性
uv run pytest tests/test_health.py               # 连续失败摘除/确定性探测恢复/探测隔离
uv run pytest tests/test_forwarder_gateway.py    # 超时熔断、重试副作用、错误码区分
uv run pytest tests/test_recovery_integration.py # 摘除->探测恢复->重新接入闭环
uv run pytest tests/test_http_app.py             # FastAPI 入口的状态码/响应体映射
uv run pytest -q                                 # 全量回归
```

也可以手动观察摘除与恢复：演示中的 `flaky`/`slow` 上游的 `/health` 恒为健康，
持续请求 `/v1/flaky/x` 可先看到 502、随后 503（摘除），约 2 秒后探测成功自动恢复。

## 代码地图

| 文件 | 职责 |
| --- | --- |
| `gateway/config.py` | 配置模型（路由/上游/限流）与演示拓扑 |
| `gateway/router.py` | 前缀+方法路由及匹配解释 |
| `gateway/loadbalancer.py` | 三种 LB 策略与在飞计数（线程安全） |
| `gateway/health.py` | 熔断状态机、候选准入、探测调度与回写 |
| `gateway/ratelimiter.py` | 每客户端固定窗口限流 + 并发槽 |
| `gateway/transport.py` | `HttpTransport`（httpx）与 `StubTransport`（进程内） |
| `gateway/forwarder.py` | 选实例、发送、重试、错误映射 |
| `gateway/gateway.py` | 门面编排与主动探测后台循环 |
| `gateway/app.py` | FastAPI 适配（catch-all + 内部状态端点） |
| `gateway/demo.py` / `main.py` | 本地演示装配与启动入口 |
