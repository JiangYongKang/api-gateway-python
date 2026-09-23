# 本地 API 网关（Python / FastAPI）

一个可在本地直接运行的 API 网关层，提供**路由转发、可解释负载均衡、上游健康摘除与
探测恢复、按客户端限流与并发控制、超时熔断、可区分的错误语义与判定日志**。

所有决策都以结构化日志打印“请求 → 判定依据 → 上游状态”，便于排障与验证。

---

## 1. 架构与模块

| 模块 | 职责 |
| --- | --- |
| `gateway/router.py` | 最长前缀路由匹配，无匹配返回明确错误，绝不静默转发 |
| `gateway/loadbalancer.py` | 在“当前健康实例集合”上按策略选实例，并给出判定依据 |
| `gateway/health.py` | 连续失败摘除（HEALTHY/UNHEALTHY/SUSPECT）+ 定时探测恢复 |
| `gateway/circuit.py` | 每实例熔断器（CLOSED/OPEN/HALF_OPEN） |
| `gateway/ratelimit.py` | 按客户端的令牌桶限流 + 并发计数（asyncio 锁保证一致） |
| `gateway/upstream.py` | httpx 上游调用；故障分类 connect/timeout/bad_status |
| `gateway/proxy.py` | 编排：路由→限流→选实例(健康/熔断)→转发→重试/状态更新 |
| `gateway/app.py` | FastAPI 入口与错误码 → HTTP 响应映射 |
| `gateway/config_loader.py` | 从 JSON/dict 装载配置 |
| `demo/` | 本地演示：假上游、网关配置与启动脚本 |

请求处理顺序：

```
路由匹配 → 客户端限流/并发检查 → 选健康且熔断允许的实例
       → 转发上游 → 成功透传 / 失败更新健康与熔断 → (满足条件才)换实例重试
```

---

## 2. 路由与负载均衡规则

### 路由

- **最长前缀匹配**：多个前缀同时命中时取最长者（如 `/svc/v2` 优先于 `/svc`）。
- 无任何前缀命中 → `route_not_found`（404），不会转发到任何上游。
- 方法透传，路径与查询串原样转发到选中实例。

### 负载均衡策略（`lb_strategy`）

| 策略 | 行为 | 可解释性 |
| --- | --- | --- |
| `round_robin` | 在**当前健康集合**（按 id 排序）上轮询，游标单调递增 | 日志输出游标、下标、健康集合 |
| `weighted_round_robin` | nginx 风格平滑加权轮询，权重越大占比越高且分布均匀 | 日志输出权重与各实例当前权重 |
| `random` | 伪随机，支持种子（默认固定种子，测试可复现） | 日志输出下标与集合 |
| `least_connections` | 选在途连接最少者，并列时按 id 字典序（结果确定） | 日志输出每实例在途数 |

- 负载均衡**只在健康实例中选择**：被摘除实例与熔断打开实例都不会收到业务请求。
- 首选实例熔断打开时，网关会在其余健康实例中确定性地（id 升序）找一个熔断允许者；
  全部不可用时才报错，**不会发往错误目标**。

---

## 3. 健康管理与熔断参数

### 健康摘除（每路由独立）

| 参数 | 含义 |
| --- | --- |
| `failure_threshold` | 连续失败达到该值即摘除为 `unhealthy` |
| `probe_interval` | 摘除后的冷却秒数；到期进入 `suspect`，只允许**一次探测** |
| `probe_path` | 健康探测路径（如 `/healthz`），与业务路径隔离 |

状态机：

```
HEALTHY ──连续失败>=threshold──▶ UNHEALTHY ──冷却 probe_interval──▶ SUSPECT
   ▲                                                                  │
   └──────────────────────── 探测成功 ◀──────────── 探测失败→重新回到 UNHEALTHY
```

- 一次成功即清零连续失败计数；摘除期间业务流量绝不发往该实例。
- **探测与业务流量互不干扰**：探测是独立的 GET 请求、走独立路径、不占用业务并发名额、
  不扣限流令牌；每个冷却周期至多探测一次。
- 探测成功恢复为 `HEALTHY`，并同时重置该实例的熔断计数。

### 熔断（每实例独立）

| 参数 | 含义 |
| --- | --- |
| `circuit_threshold` | 连续失败（含超时）达到该值即 OPEN |
| `circuit_open_seconds` | OPEN 冷却秒数；到期放**一个**试探请求进入 HALF_OPEN |

- OPEN 期间直接快速失败，不访问上游。
- HALF_OPEN 试探成功 → CLOSED；试探失败 → 重新 OPEN 并重新计时。
- 熔断与健康摘除是两层保护：熔断阈值通常更小、反应更快；健康摘除用于持续故障隔离。

### 上游调用与重试

| 参数 | 含义 |
| --- | --- |
| `timeout_seconds` | 单次上游调用超时 |
| `max_retries` | 最大**重试**次数（总尝试次数 = max_retries + 1） |
| `retry_on_status` | 哪些上游 5xx 状态码可重试 |

重试规则（保证无额外副作用）：

- 仅幂等方法（GET/HEAD/OPTIONS/PUT/DELETE）重试；POST/PATCH 等不重试。
- 重试**换一个未尝试过的健康实例**，绝不重复打同一实例。
- connect/timeout 总是可重试；5xx 仅当其状态码在 `retry_on_status` 中。
- 限流令牌与并发名额在**请求入口一次性获取**，重试不重复计数；请求结束释放并发。

---

## 4. 限流与并发

| 参数 | 含义 |
| --- | --- |
| `rate_limit.requests_per_second` | 每客户端每秒令牌数（令牌桶容量等于该值，`0` 不限） |
| `rate_limit.max_concurrent` | 每客户端同时在途请求上限（`0` 不限） |
| `client_id_header` | 客户端标识请求头（默认 `X-Client-Id`，缺省为 `anonymous`） |

- 计数按客户端维度隔离，不同客户端互不影响。
- 基于 `asyncio.Lock` 保证“检查 + 扣减”原子，并发下计数严格一致。
- 两种超限分别返回 `rate_limited`（429）与 `concurrency_limited`（429），可区分。

---

## 5. 错误语义（失败原因可区分）

响应体统一为：

```json
{"error": {"code": "<错误码>", "message": "<说明>", "details": {...}}}
```

| code | HTTP | 触发条件 |
| --- | --- | --- |
| `route_not_found` | 404 | 无匹配路由前缀 |
| `no_available_upstream` | 503 | 匹配到路由但无健康实例（已摘除/无实例） |
| `rate_limited` | 429 | 客户端每秒令牌不足 |
| `concurrency_limited` | 429 | 客户端在途并发超限 |
| `circuit_open` | 503 | 健康实例的熔断器全部打开 |
| `upstream_timeout` | 504 | 上游调用超时 |
| `upstream_error` | 502 | 上游连接失败（connect） |
| `upstream_bad_status` | 502 | 上游返回 5xx |
| `internal_error` | 500 | 网关自身错误（如配置错误） |

成功响应附带 `X-Upstream-Id`（实际服务实例）与 `X-Request-Id`；4xx 视为客户端问题，
**原样透传**，不计入健康/熔断失败。

健康端点：`GET /_gateway/health` 可查看每条路由各实例的健康与熔断实时状态。

---

## 6. 判定日志

每个请求按阶段打印结构化 JSON 日志：

```
route_match → rate_limit → lb_select → upstream_call → health_update → [retry] → response/error
```

日志字段包含：请求方法/路径、`request_id`、客户端 id、限流令牌与在途数、
负载均衡选择依据、上游返回/故障类型、健康与熔断快照、耗时。可用 `X-Request-Id` 串联。

---

## 7. 本地验证

### 运行测试

```bash
uv sync            # 安装依赖（含 pytest）
uv run pytest      # 全部 40+ 用例，-o log_cli=true 可实时看判定日志
```

测试覆盖：并发限流计数一致性、并发上限端到端、连续失败摘除、探测恢复与重新摘除、
超时熔断与半开恢复、重试副作用（幂等/换实例/不重复扣令牌）、各类错误码 HTTP 映射、
以及判定日志是否包含请求/依据/上游状态。

### 端到端演示（三个终端）

```bash
# 终端 1、2：两个假上游
UP_NAME=users-1 uv run uvicorn demo.upstream_server:app --port 18081
UP_NAME=users-2 uv run uvicorn demo.upstream_server:app --port 18082

# 终端 3：网关（默认 18080，可用 GW_PORT 改端口）
GW_PORT=18090 uv run python demo/run_gateway.py demo/gateway_config.json
```

验证命令：

```bash
# 轮询（响应头 X-Upstream-Id 显示实例）
curl -s -D - -H 'X-Client-Id: d' http://127.0.0.1:18090/users/42 -o /dev/null | grep -i x-upstream

# 无匹配路由 -> 404
curl -i http://127.0.0.1:18090/nope

# 限流：同客户端突发 -> 前若干个 200，随后 429
for i in $(seq 1 8); do curl -s -o /dev/null -w '%{http_code} ' \
  -H 'X-Client-Id: burst' http://127.0.0.1:18090/users/p; done; echo

# 让 users-1 持续失败 -> 观察摘除；流量只去 users-2
curl -X POST http://127.0.0.1:18081/_control/fail
# 恢复 -> 后台探测在下一个 probe_interval 自动把它接回
curl -X POST http://127.0.0.1:18081/_control/ok

# 让 users-1 挂起 -> 504 超时，连续失败后熔断 -> 503 circuit_open
curl -X POST http://127.0.0.1:18081/_control/hang

# 查看实时健康/熔断状态
curl -s http://127.0.0.1:18090/_gateway/health | python -m json.tool
```

假上游控制接口：`POST /_control/{ok|fail|hang}`，探测端点 `GET /healthz`。

### 作为库嵌入

```python
from gateway.app import create_app
from gateway.config_loader import load_config

config = load_config({
    "rate_limit": {"requests_per_second": 100, "max_concurrent": 20},
    "routes": [{
        "id": "users", "path_prefix": "/users",
        "lb_strategy": "least_connections",
        "failure_threshold": 3, "probe_interval": 1.0, "probe_path": "/healthz",
        "circuit_threshold": 5, "circuit_open_seconds": 2.0,
        "timeout_seconds": 1.0, "max_retries": 1,
        "upstreams": [
            {"id": "u1", "base_url": "http://127.0.0.1:18081", "weight": 1},
            {"id": "u2", "base_url": "http://127.0.0.1:18082", "weight": 2},
        ],
    }],
})
app = create_app(config)
```

也可直接使用 `main.py`（`uv run uvicorn main:app`），它加载 `default_demo_config()`。
