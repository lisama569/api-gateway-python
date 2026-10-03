# 本地反向代理网关（Python / FastAPI）

一个完全在本地可复现的反向代理网关：用**本地假上游**模拟多个后端实例，
与仓库现有 HTTP 服务入口（`main.py` / FastAPI）集成，不依赖任何外部服务或真实凭据。

## 能力总览

| 能力 | 说明 |
| --- | --- |
| 路由分发 | 按路径**最长前缀**匹配到路由；路由下有多个本地上游实例 |
| 选择策略 | `round_robin`（轮询）/ `least_conn`（最少连接），可按路由配置并在运行期热切换 |
| 热更新 | `POST /admin/reload` **原子替换**配置快照；在途请求持有旧快照继续执行，不被中断 |
| 重试与故障转移 | 安全方法失败时按配置自动重试，优先换到同路由下其它健康实例；次数/单次耗时/整体耗时均有上限 |
| 回源合并 | 同一缓存键的并发未命中只回源一次，其余请求等待并共享结果（single-flight） |
| 半开恢复 | 熔断冷却后只放小流量探测，探测成功才重新参与常规分流 |
| 限流 | 令牌桶，支持**全局 / 每客户端 / 每路由 / 每客户端在该路由**四个维度 |
| 访问控制 | 来源白名单、黑名单（支持 IP 与 CIDR） |
| 缓存 | 可缓存响应按 **TTL** 缓存；支持 `If-None-Match` / `If-Modified-Since` 条件请求；缓存命中不回源 |
| 透传 | 流式路由与不可缓存方法**原样透传**，不读不写缓存 |
| 可区分失败 | 无路由 / 黑名单 / 白名单 / 限流 / 上游连接失败 / 超时 / 无健康实例，状态码与 JSON `error` 码均不同 |

## 目录结构

```
main.py                  HTTP 服务入口（uvicorn 启动 main:app）
run_local.py             一键本地启动：假上游 :9101 + 网关 :8000
gateway/
  config.py              配置数据模型（路由/上游/策略/限流/缓存 TTL/重试/合并等待）
  router.py              最长前缀匹配 + 原子热更新快照
  upstream.py            轮询/最少连接选择器 + 健康熔断 + 半开探测恢复
  coalesce.py            回源合并（single-flight）：同键并发未命中只回源一次
  ratelimit.py           线程安全令牌桶与多维限流器
  access.py              白名单/黑名单判定
  cache.py               TTL 响应缓存、新鲜度、Vary 键
  proxy.py               代理主链路、重试循环与可区分错误码
  app.py                 构造 FastAPI、挂载代理与 /admin 管理接口
  local_config.py        指向本地假上游的演示配置
fake_upstream/
  server.py              本地假上游（多实例 a/b/c、延迟、故障开关、SSE 流）
tests/                   67 个测试（单测 + 真实本地 uvicorn 端到端）
```

## 本地运行

需要 Python 3.14 与 [uv](https://docs.astral.sh/uv/)。

```bash
uv sync                       # 安装依赖（含 pytest）
uv run python run_local.py    # 启动 假上游 :9101 与 网关 :8000
```

另开终端：

```bash
# 轮询（users 路由两个实例 a/b）
curl -s http://127.0.0.1:8000/api/users/1
curl -s http://127.0.0.1:8000/api/users/2

# 缓存：首次 MISS，后续 HIT（命中不再访问上游）
curl -si http://127.0.0.1:8000/api/users/9 | grep -i x-cache
curl -si http://127.0.0.1:8000/api/users/9 | grep -i x-cache

# 条件请求：304 Not Modified（网关本地校验，不回源）
ETAG=$(curl -si http://127.0.0.1:8000/api/users/9 | grep -i etag | awk '{print $2}' | tr -d '\r')
curl -i -H "If-None-Match: $ETAG" http://127.0.0.1:8000/api/users/9

# 流式透传（SSE，逐帧到达）
curl -N http://127.0.0.1:8000/stream

# 无匹配路由
curl -i http://127.0.0.1:8000/nope

# 运行期热更新策略（不重启、在途请求不中断）
curl -X POST http://127.0.0.1:8000/admin/reload \
  -H 'content-type: application/json' \
  -d '{"route_id":"users","strategy":"least_conn"}'

# 假上游控制：让某实例变慢（毫秒）/ 返回故障
curl -X POST "http://127.0.0.1:9101/a/state/slow?ms=6000"   # 触发网关 504
curl -X POST "http://127.0.0.1:9101/a/state/fail?on=1"       # 上游返回 5xx
curl -X POST "http://127.0.0.1:9101/a/state/slow?ms=0"

# 重试与故障转移（users 路由已配 retry：a 故障时自动换到 b）
curl -X POST "http://127.0.0.1:9101/a/state/fail?on=1"
curl -s http://127.0.0.1:8000/api/users/1        # 200，instance=b，响应头带 x-gateway-attempts
curl -s http://127.0.0.1:8000/api/users/2        # 观察日志：probe ... decision=recovered（半开恢复）
curl -X POST "http://127.0.0.1:9101/a/state/fail?on=0"

# 回源合并：缓存过期瞬间并发打同一个键，上游只回源一次
curl -X POST "http://127.0.0.1:9101/a/state/slow?ms=500"
for i in $(seq 1 8); do curl -s http://127.0.0.1:8000/api/users/9 & done; wait
curl -s http://127.0.0.1:9101/a/state            # hits 只增加 1；跟随者响应带 x-coalesced: shared
curl -X POST "http://127.0.0.1:9101/a/state/slow?ms=0"
```

## 路由与选择策略

- **匹配**：对请求路径做最长前缀匹配，例如同时存在 `/api` 与 `/api/v2` 时，
  `/api/v2/x` 命中后者；无任何前缀命中时返回 `404 no_route`。
- **轮询 `round_robin`**：仅在健康实例间按配置顺序轮转。
- **最少连接 `least_conn`**：选择活动连接数（已 `acquire` 未 `release`）最少的健康实例，
  平局时按配置顺序，保证确定性。
- **热更新**：配置以不可变 `GatewayConfig` 快照保存，`router.update()` 单次原子替换引用。
  在途请求在处理开始时拿到快照引用，整个请求周期使用旧配置；新请求立即看到新配置。
  选择器按 `route id` 维护连接计数，热更新时保留在途计数并对实例集合做增删适配。

## 限流语义

- 算法为**惰性补充的令牌桶**：容量 `capacity`，每秒补充 `rate` 个令牌；
  补充与取令牌在同一把锁内完成，**并发压力下不会多放行**（有 200 线程竞 10 令牌的测试）。
- 判定顺序（任一维度耗尽即拒绝）：
  1. `global` 全局聚合（使用全局规则）
  2. `client` 每客户端（使用全局规则）
  3. `route` 路由聚合（使用路由规则）
  4. `client_route` 每客户端在该路由（使用路由规则）
- 拒绝返回 `429`，响应体含 `"error":"rate_limited"` 与命中的 `scope`，
  并带 `Retry-After` 头与 `retry_after`（秒）字段；补充速率为 0 时给出固定上限值。

## 白名单 / 黑名单

- 命中黑名单 → `403 ip_blocked`。
- 白名单非空且来源不在白名单（支持 CIDR）→ `403 ip_not_allowed`。
- 两者错误码不同，可与 `no_route`、限流、上游失败区分。

## 缓存语义

- **可缓存条件**：方法为 GET/HEAD、路由非流式、`cache_ttl > 0`、上游响应为 200，
  且响应 `Cache-Control` 不含 `no-store` / `private`。
- **键**：`route id | method | path | query | Vary 头`。
- **新鲜度**：`stored_at + ttl`；过期条目惰性删除并重新回源（不返回陈旧内容）。
- **命中**：直接由网关返回，附 `x-cache: HIT`，**绝不访问上游**；回源写入附 `x-cache: MISS`。
- **条件请求**：新鲜命中时在网关本地比较
  `If-None-Match`（支持 `W/`、`*`、多 ETag）或 `If-Modified-Since`，
  通过则返回 `304`（仅带 ETag/Last-Modified 校验头，无响应体），同样不回源。
- **透传**：`route.streaming=True` 以及 POST/PUT/PATCH/DELETE 等方法既不读也不写缓存；
  流式响应通过 `aiter_raw` 逐块转发，保持 SSE 等场景的分块边界与到达节奏。

## 重试与故障转移

按路由配置 `retry=RetryConfig(...)` 后启用（缺省 `None`，不重试、行为与之前完全一致）：

| 字段 | 缺省 | 说明 |
| --- | --- | --- |
| `max_attempts` | 2 | 总尝试次数上限（含首次）；1 表示不重试 |
| `per_attempt_timeout` | `None` | 单次尝试超时（秒）；`None` 时沿用 `route.timeout` |
| `budget` | 10.0 | 整个重试过程的整体耗时上限（秒），到期不再发起新尝试 |
| `retry_on_statuses` | `(500, 502, 503, 504)` | 上游返回这些状态码时视为可重试失败 |

- **哪些请求会重试**：仅安全方法 `GET/HEAD/OPTIONS` 且非流式路由。
  **写操作（POST/PUT/PATCH/DELETE）与流式请求一律不重试**，按原有单次失败结果返回，
  同一个业务动作不会在上游被执行多次。
- **故障转移**：每次尝试重新 `acquire` 实例并**排除本请求已试过的实例**，
  优先换到同一条路由下其它健康实例；没有更多健康实例即停止，不会无限重试。
- **干净等价的重放**：每次尝试用同一份请求头与请求体重新构造请求、重新确定目标实例，
  不携带上一跳的残留状态。
- **失败可区分**：全部尝试失败后，502/504 的 JSON 错误体带 `attempts` 明细
  （试过哪些实例、各自结局：`timeout` / `transport_error` / `status_5xx`）；
  若最后一次失败是上游 5xx 透传，则明细放在响应头 `x-gateway-attempts`。
  从未试过任何实例（全部熔断）仍是 `503 no_upstream`，与重试耗尽可区分。
- 可重试的 5xx 会熔断对应实例（配合半开恢复）；未配置 `retry` 的路由维持原语义，
  上游 5xx 仅透传、不熔断。

## 回源合并（single-flight）

- **生效范围**：仅可缓存请求（GET/HEAD、非流式、`cache_ttl > 0`）在**缓存未命中或刚好过期**时。
  不可缓存方法、流式请求完全不参与；不同键之间绝不共享。
- **键**：路由 + 方法 + 路径 + 查询 + 会影响结果的请求头（已知 Vary 字段的取值）。
- **行为**：第一个到达的请求（领导者）回源，其余同键请求（跟随者）等待并共享这一份结果；
  上游命中计数只有 1。跟随者响应带 `x-coalesced: shared`。
- **变体安全**：领导者写缓存后，跟随者按**自己的请求头**重新查缓存（Vary 变体各自命中，
  条件请求 304 行为不变）；未缓存的结果（如 `no-store`）仅在 Vary 取值一致时共享，
  不一致则自行回源。TTL 与条件请求语义完全不变。
- **失败传播**：领导者回源失败（502/504）时，每个跟随者各自拿到同样的可区分失败结果；
  航班随即关闭，后续请求可重新发起，不会一直挂住。
- **等待上限**：`route.coalesce_wait`（秒，缺省 10）。等待超时后跟随者**自行回源**
  （成为新的领导者），保证有明确结果。

## 半开恢复

- 实例熔断后进入 `fail_timeout` 冷却；**冷却结束后不直接恢复**，而是进入半开窗口：
  只允许至多 `half_open_probes` 个并发探测请求打到该实例。
- **探测成功**（非 5xx 且未超时/未连接失败）：实例恢复为健康，重新参与常规分流。
- **探测失败**：重新进入冷却，之后可再次探测。探测在途期间槽位占满，
  其余请求走健康实例或拿到 `503 no_upstream`，不会整批压回刚半开的实例。
- 网关级配置（`GatewayConfig`）：`fail_timeout`（冷却秒数，缺省 5.0）、
  `half_open_probes`（并发探测上限，缺省 1）。
- **与热更新配合**：选择器按 route id 维护状态，热更新保留在途连接计数与半开状态、
  仅适配实例集合；已从路由移除的实例，其迟到的 release/report 都是 no-op，
  不会留下残留状态，也不会把旧快照的实例集合换回去。

## 故障判定与降级规则

| 场景 | HTTP 状态 | JSON `error` | 处理 |
| --- | --- | --- | --- |
| 无路由匹配 | 404 | `no_route` | 直接拒绝 |
| 命中黑名单 | 403 | `ip_blocked` | 直接拒绝 |
| 不在白名单 | 403 | `ip_not_allowed` | 直接拒绝 |
| 触发限流 | 429 | `rate_limited` | 带 `scope` 与 `Retry-After` |
| 路由存在但所有实例都不健康 | 503 | `no_upstream` | 不再尝试连接 |
| 上游连接失败 | 502 | `upstream_error` | 标记该实例熔断；重试耗尽后带 `attempts` 明细 |
| 上游读/写/连接超时 | 504 | `upstream_timeout` | 标记该实例熔断；重试耗尽后带 `attempts` 明细 |
| 上游自身返回 4xx/5xx | 原样透传 | — | 不缓存、不改写状态码；配置重试后 5xx 明细见 `x-gateway-attempts` 头 |

- **熔断与恢复**：某实例连接失败/超时后被标记为不健康并进入 `fail_timeout`（默认 5 秒）冷却；
  冷却期内选择器跳过它；冷却结束后进入**半开窗口**，仅放行 `half_open_probes` 个并发探测，
  探测成功才重新参与常规分流（见上文"半开恢复"）。
- 多实例场景会优先把流量交给健康实例；全部不健康时才返回 503，
  因此 502/504（尝试过但失败）与 503（无实例可试）语义可区分。
- 连接计数在转发前递增，在响应完成/异常/流式关闭的 finally 路径中递减，避免计数泄漏。

## 本地假上游

`fake_upstream/server.py` 用一个 FastAPI 应用以路径前缀 `/{instance}/...` 模拟 a/b/c 三个实例：

- `GET /{instance}/api/{...}` 与 `GET /{instance}/{...}`：回显实例名、路径、命中计数（带 ETag/Last-Modified）。
- `GET /{instance}/stream`：5 帧 SSE 流。
- `GET /{instance}/slow/{ms}`：睡眠指定毫秒，用于复现超时（任意路由前缀下也识别 `slow/毫秒`）。
- `POST /{instance}/state/fail?on=1`：让该实例返回 500。
- `POST /{instance}/state/slow?ms=N`：为该实例所有普通响应加延迟。
- `GET /{instance}/state`：查看命中数等状态。

## 测试

```bash
uv run pytest -q
```

测试全部走**真实本地 TCP**（uvicorn 起真服务，httpx 发真请求），覆盖：

- 并发限流：200 线程竞 10 令牌恰好放行 10 个；端到端 30 并发 / 容量 5 恰好放行 5 个。
- 路由热更新：原子快照、match 与 reload 并发无损坏、在途慢请求跨越 reload 正常完成。
- 缓存：MISS/HIT、TTL 过期回源、命中不再访问上游（断言上游命中计数）、ETag 与 IMS 的 304、
  不可缓存方法不写缓存。
- 重试与故障转移：5xx/超时自动换实例、次数与整体预算上限、写操作与流式不重试、
  重试耗尽后 `attempts` 明细与 `no_upstream` 可区分。
- 回源合并：8 并发同键只回源 1 次、不同键互不合并、共享失败各自可判断且可重新发起、
  等待超时自行回源、Vary 变体不串味、不可缓存与流式不参与。
- 半开恢复：探测槽位限流、探测成功恢复/失败重新冷却、热更新保留在途计数且
  清除已移除实例的残留状态。
- 上游失败：连接拒绝 502、读超时 504、全部熔断后 503、无路由 404，断言错误码彼此可区分。
- 流式透传：断言 SSE 5 帧原样到达。
- 策略：轮询序列、最少连接选择、冷却恢复、热更新保留在途计数。

测试运行日志（stderr）打印判定依据，例如：

```
... INFO [gateway] route route=svc upstream=a reason=strategy=round_robin streaming=False attempt=1 target=...
... INFO [gateway] retry route=svc decision=retry attempt=1/2 upstream=a reason=status_500
... INFO [gateway] route route=svc upstream=b reason=strategy=round_robin streaming=False attempt=2 target=...
... INFO [gateway.coalesce] coalesce key=c|GET|/c/hot| decision=shared
... INFO [gateway.upstream] probe route=svc upstream=a decision=recovered reason=probe_ok
... INFO [gateway] rl scope=route key=users decision=deny remaining=0.00 capacity=3
... INFO [gateway] cache key=users|GET|/api/users/9| decision=HIT reason=fresh ttl=3
```

## 管理接口

| 接口 | 方法 | 说明 |
| --- | --- | --- |
| `/admin/health` | GET | 存活与当前配置版本号 |
| `/admin/config` | GET | 当前快照的路由、策略、上游、TTL、流式标记 |
| `/admin/reload` | POST | `{"route_id","strategy"}` 切策略；或 `{"base": "http://..."}` 按新上游基址重建本地配置 |

## 说明与边界

- 面向本地学习/验证：限流器、缓存与回源合并均为单进程内存实现，不做多实例共享持久化。
- 缓存不做 stale-while-revalidate / 仅在 TTL 内保证新鲜；鉴权以直连客户端 IP 为准。
- 重试只对安全方法重放已缓冲的请求；`budget` 是墙钟上限，到期即停止发起新尝试。
- 回源合并的跟随者在领导者返回 `no-store` 等不可缓存结果时，仅在同键同变体下共享
  这一次的结果，不会写入缓存。
- 半开探测是"并发槽位"限流：探测在途期间其余流量走健康实例或拿到 503，
  探测成功后实例立即恢复常规分流。
