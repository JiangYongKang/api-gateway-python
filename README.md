# 本地 API 网关（Python / FastAPI）

一个可完全本地运行的 API 网关层：按规则路由到多个上游、可配置且可解释的负载均衡、
被动故障摘除 + 主动探测恢复、按客户端的限流与并发控制、超时/失败熔断、
**按权重的灰度分流与客户端粘性**（可在线调权、可观察），以及一组
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

# 灰度演示（/v1/gray：echo-a 90% / echo-v2 10%，客户端粘性 30s）
for i in $(seq 1 20); do
  curl -s -H "X-Client-Id: user-$i" localhost:8000/v1/gray/x | grep -o '"upstream":"[^"]*"'
done | sort | uniq -c                               # 观察 ~9:1 分流
curl -s localhost:8000/_gateway/weights            # 权重 / 粘性 / 实际流量分布
curl -s -X PUT localhost:8000/_gateway/weights/gray-demo \
  -H 'content-type: application/json' \
  -d '{"echo-a": 1, "echo-v2": 1}'                 # 不重启调到 50:50，新请求立即生效
```

运行测试：

```bash
uv run pytest -q                # 全部 74 个用例
uv run pytest -s                # 同时打印每个请求的决策日志
```

## 架构与请求管线

```
Request
  └─ Router            前缀 + 方法匹配，第一条命中规则生效；未命中 -> ROUTE_NOT_FOUND
      └─ ClientLimiter 按客户端原子检查 固定窗口速率 与 并发占用
          └─ HealthRegistry.healthy_candidates   只返回未摘除（且半开名额未满）的实例
              └─ LoadBalancer.acquire            在候选中按策略选一个并占 in-flight 槽
                                                 （配了权重的路由：粘性命中或加权哈希分桶）
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

## 灰度发布：按权重分流 + 客户端粘性

同一条路由可以为每个实例配置权重（`RouteRule.weights`），把新老版本按比例放量；
`sticky_seconds > 0` 时同一客户端在 TTL 内稳定命中同一实例，避免灰度期间
用户在两个版本间来回跳。**没配 `weights` 的路由完全走原有 LB 策略路径，
行为与灰度功能引入前逐字节一致。**

### 权重规则（`gateway/weights.py: WeightManager`）

* 路由一旦配置权重，选择切换为**确定性哈希分桶**：
  `bucket = sha256(route_id::client_id) mod 总权重`，按配置顺序把
  `[0, 总权重)` 切成各实例的累计区间，bucket 落在谁的区间就选谁。
  同一客户端在权重与候选集不变时**永远**落到同一实例，可复现、可手工复核
  （日志记录 `hash`/`bucket`/`ranges`/各实例权重与版本号）。
* 权重为 0（或权重表中缺失）的实例**不承接新业务流量**，但健康维护
  （失败摘除、主动探测、半开恢复）照常运行——临时下线和逐级放量都靠它。
* 所有实例权重均为 0 时按无候选处理（`ALL_UPSTREAMS_UNAVAILABLE`），
  绝不把请求发给权重为 0 的实例。
* 权重只接受非负整数；非法值返回 `400 INVALID_WEIGHTS`。

### 粘性规则（`gateway/weights.py: StickyAssigner`）

* 首次（或重新分配）时按上面的加权哈希选定实例，之后**滑动 TTL** 内
  同一客户端复用该实例；活跃用户的粘性随访问续期。
* 出现以下任一情况时按确定规则**重新分配**（仍走加权哈希）：
  粘性到期、目标实例被摘除、目标权重被调为 0。
  重新分配只在健康且权重 > 0 的候选中进行，**绝不落到已摘除实例**。
* 目标实例恢复后**不会**把客户端立刻切回去：客户端继续留在当前实例直到
  粘性自然到期，避免恢复瞬间的流量回切抖动。
* 查找 + 校验 + 重新分配在**同一把锁**内原子完成：同一客户端并发打进来的
  多个请求只会得到一个分配结果，不会同时被分到两个实例。

### 在线调权与可观察性

| 端点 | 说明 |
| --- | --- |
| `GET /_gateway/weights` | 每条路由当前权重（含版本号）、存活粘性分配、各实例实际承接的请求数 |
| `PUT /_gateway/weights/{route_id}` | 不重启调整权重，body 如 `{"echo-a": 1, "echo-v2": 1}` |

调权语义：新到的请求立即按新权重走；**在处理中的请求**已完成实例选择，
不受影响；**已粘住的客户端**在其粘性仍有效（未到期、目标健康且权重 > 0）
时保持不动，失效时按新权重确定性地重新分配——切换瞬间同一客户端
不会被分到多个实例。路由不存在返回 `404 ROUTE_NOT_FOUND`，权重里出现
路由外的上游或非法值返回 `400 INVALID_WEIGHTS`，写错名字不会静默生效。

兼容范围：调权只接受该路由 `upstream_ids` 内的实例；权重表是路由级的，
运行时调整只影响本进程内存（重启后回到静态配置）；未在权重表中列出的
实例按权重 0 处理（显式 opt-in）。

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
| `INVALID_WEIGHTS` | 400 | 灰度调权请求非法：权重为负/非整数、上游不属于该路由、body 非 JSON |

关键边界：把断路器推过阈值的**那一次请求**已真实到达上游，返回它自己的结果
（504/502）；只有其后的请求在“发送前”就无候选可用时才得到
`ALL_UPSTREAMS_UNAVAILABLE`，从而“熔断/摘除”和“普通上游错误”不会混淆。

## 决策日志

网关对每个请求打印单行 `key=value` 决策日志到 stderr：

* `#request`：方法、路径、客户端、命中规则、LB 策略与上游列表、限流判定快照；
* `#attempt`：第几次尝试、选中上游、策略依据（counter/roll/inflight；
  灰度路由额外带 `bucket`/`hash`/`ranges`/权重版本与 `sticky=hit|new|expired|target_ineligible`）、
  候选列表、结果（success/timeout/connect_error/upstream_error）、状态码、耗时与**全量上游健康快照**；
* `#retry_decision`：是否重试及判定依据（幂等性、剩余次数、上次原因）；
* `#probe`：探测路由/上游/路径、成败、原因、耗时与健康快照；
* `#weights_updated`：在线调权前后的权重表、总权重与版本号；
* `#gateway_error` / `#no_upstream` / `#rejected`：最终错误码与限流原因。

用 `uv run pytest -s` 可在测试输出中直接看到这些日志。

## 本地验证方法

```bash
uv run pytest tests/test_ratelimit.py            # 并发限流：2000 线程下计数一致性
uv run pytest tests/test_health.py               # 连续失败摘除/确定性探测恢复/探测隔离
uv run pytest tests/test_forwarder_gateway.py    # 超时熔断、重试副作用、错误码区分
uv run pytest tests/test_recovery_integration.py # 摘除->探测恢复->重新接入闭环
uv run pytest tests/test_http_app.py             # FastAPI 入口的状态码/响应体映射
uv run pytest tests/test_weights.py              # 权重分桶确定性/比例/零权重 + 粘性 TTL/并发原子性
uv run pytest tests/test_grayrelease.py          # 灰度分流、摘除/调零重分配、恢复不回切（伪时钟）
uv run pytest tests/test_gray_admin.py           # 在线查/调权重端点与非法请求的错误码
uv run pytest tests/test_gray_acceptance.py -s   # 真起 uvicorn + 并发打流，观察实际分流比与粘性
uv run pytest -q                                 # 全量回归
```

灰度能力的端到端观察（`tests/test_gray_acceptance.py` 会把每个场景的判定依据、
权重与上游健康状态打进日志，`-s` 可见）：真实 uvicorn 服务下，400 个并发客户端
的实际分流比落在权重 ±7% 内；同一热客户端 16 线程并发 64 请求全部命中同一实例；
`PUT /_gateway/weights/gray` 调权后新流量立即按新权重走且权重 0 的实例
健康跟踪不中断。

也可以手动观察摘除与恢复：演示中的 `flaky`/`slow` 上游的 `/health` 恒为健康，
持续请求 `/v1/flaky/x` 可先看到 502、随后 503（摘除），约 2 秒后探测成功自动恢复。

## 代码地图

| 文件 | 职责 |
| --- | --- |
| `gateway/config.py` | 配置模型（路由/上游/限流/灰度权重与粘性）与演示拓扑 |
| `gateway/router.py` | 前缀+方法路由及匹配解释 |
| `gateway/loadbalancer.py` | 三种 LB 策略、灰度加权/粘性选择、在飞与流量计数（线程安全） |
| `gateway/weights.py` | 灰度权重表（确定性哈希分桶、在线调权）与粘性分配器（滑动 TTL、原子分配） |
| `gateway/health.py` | 熔断状态机、候选准入、探测调度与回写 |
| `gateway/ratelimiter.py` | 每客户端固定窗口限流 + 并发槽 |
| `gateway/transport.py` | `HttpTransport`（httpx）与 `StubTransport`（进程内） |
| `gateway/forwarder.py` | 选实例、发送、重试、错误映射 |
| `gateway/gateway.py` | 门面编排与主动探测后台循环 |
| `gateway/app.py` | FastAPI 适配（catch-all + 内部状态端点） |
| `gateway/demo.py` / `main.py` | 本地演示装配与启动入口 |
