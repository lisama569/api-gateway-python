# 本地反向代理网关（Python / FastAPI）

一个完全在本地可复现的反向代理网关：用**本地假上游**模拟多个后端实例，
与仓库现有 HTTP 服务入口（`main.py` / FastAPI）集成，不依赖任何外部服务或真实凭据。

## 能力总览

| 能力 | 说明 |
| --- | --- |
| 路由分发 | 按路径**最长前缀**匹配到路由；路由下有多个本地上游实例 |
| 选择策略 | `round_robin`（轮询）/ `least_conn`（最少连接），可按路由配置并在运行期热切换 |
| 热更新 | `POST /admin/reload` **原子替换**配置快照；在途请求持有旧快照继续执行，不被中断 |
| 限流 | 令牌桶，支持**全局 / 每客户端 / 每路由 / 每客户端在该路由**四个维度 |
| 访问控制 | 来源白名单、黑名单（支持 IP 与 CIDR） |
| 缓存 | 可缓存响应按 **TTL** 缓存；支持 `If-None-Match` / `If-Modified-Since` 条件请求；缓存命中不回源 |
| 重试/故障转移 | 安全方法（GET/HEAD/OPTIONS）可按配置重试，优先换到同路由下其它健康实例；次数/单次/整体耗时均有上限 |
| 回源合并 | 缓存未命中时同键并发请求只放行一个回源，其余等待并共享结果（single-flight） |
| 半开恢复 | 熔断冷却后用小流量探测逐步恢复，探测成功才重新参与常规分流 |
| 透传 | 流式路由与不可缓存方法**原样透传**，不读不写缓存，不重试、不合并 |
| 可区分失败 | 无路由 / 黑名单 / 白名单 / 限流 / 上游连接失败 / 超时 / 无健康实例，状态码与 JSON `error` 码均不同 |

## 目录结构

```
main.py                  HTTP 服务入口（uvicorn 启动 main:app）
run_local.py             一键本地启动：假上游 :9101 + 网关 :8000
gateway/
  config.py              配置数据模型（路由/上游/策略/限流/缓存 TTL/重试/半开）
  router.py              最长前缀匹配 + 原子热更新快照
  upstream.py            轮询/最少连接选择器 + 熔断/半开恢复状态机
  ratelimit.py           线程安全令牌桶与多维限流器
  access.py              白名单/黑名单判定
  cache.py               TTL 响应缓存、新鲜度、Vary 键、合并回源键
  coalesce.py            同键请求合并回源（single-flight）
  proxy.py               代理主链路、重试/故障转移与可区分错误码
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
curl -X POST "http://127.0.0.1:9101/a/state/slow?ms=6000"   # 实例 a 变慢
curl -X POST "http://127.0.0.1:9101/a/state/fail?on=1"       # 实例 a 返回 5xx
curl -X POST "http://127.0.0.1:9101/a/state/slow?ms=0"

# 重试/故障转移（users 路由已配 retry：max_attempts=2, per_try_timeout=2s, budget=5s）：
# a 变慢后，请求先在 a 超时（2s），自动换到 b 成功返回，响应头 x-gateway-tried: a,b
curl -X POST "http://127.0.0.1:9101/a/state/slow?ms=6000"
curl -si http://127.0.0.1:8000/api/users/1 | grep -i 'x-gateway-tried\|instance'
curl -X POST "http://127.0.0.1:9101/a/state/slow?ms=0"

# 回源合并：缓存过期瞬间并发 6 个相同请求，上游只被打 1 次
curl -s http://127.0.0.1:9101/a/state   # 记下 hits
for i in $(seq 6); do curl -s http://127.0.0.1:8000/api/users/9 & done; wait
curl -s http://127.0.0.1:9101/a/state   # hits 只 +1（或经另一实例 +1）

# 半开恢复：a 熔断后，冷却（默认 5s）到点不会整批放回，
# 只允许 1 个探测请求进 a；探测成功才恢复分流，日志出现 half_open_to_up
curl -X POST "http://127.0.0.1:9101/a/state/slow?ms=6000"
curl -s http://127.0.0.1:8000/api/users/1   # a 超时熔断
sleep 6 && curl -s http://127.0.0.1:8000/api/users/1   # 探测 a 成功，恢复
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

- **配置**（`RouteConfig.retry = RetryConfig(...)`，默认 `None` 即不重试，保持原单次行为）：
  - `max_attempts`：总尝试次数上限（含首次），>1 才启用重试；
  - `per_try_timeout`：单次尝试超时（缺省用路由 `timeout`）；
  - `budget`：整个请求（含所有重试）的整体耗时上限，超出即停止；
  - `retry_on_5xx`：上游返回 5xx 时是否换实例重试（连接错误/超时总是可重试）。
- **适用范围**：仅幂等安全方法 `GET/HEAD/OPTIONS` 且非流式路由。
  **写操作（POST/PUT/PATCH/DELETE）与流式请求一律不重试**，按原有单次失败结果返回，
  不会因重放让同一业务动作在上游执行多次。
- **故障转移**：每次尝试重新走选择器，并**排除本请求已试过的实例**，
  因此会优先落到同路由下其它健康实例；实例因超时/连接错误被熔断后也不再被选中。
- **干净等价**：每次尝试重新构造请求（请求头逐跳重建、请求体为同一份字节、
  目标实例重新选择），上一跳的连接/状态不会残留到下一跳。
- **失败语义**：全部尝试失败后，状态码与 `error` 码沿用最后一跳的类型
  （`504 upstream_timeout` / `502 upstream_error`），并附加
  `attempts`、`tried_upstreams`、`route` 字段；若最后一跳是上游 5xx 响应，
  则**原样透传**该响应并附 `x-gateway-tried` 头标明实际试过的实例。
  这些结果与 `no_route`（404）、`rate_limited`（429）、`no_upstream`（503）均可区分。
- 响应头 `x-gateway-tried: a,b` 仅在发生重试时出现，可用于观测故障转移路径。

## 回源合并（single-flight）

- **适用条件**：仅可缓存请求（GET/HEAD、非流式、`cache_ttl > 0`）在**缓存未命中或刚过期**时生效；
  其余请求（不可缓存方法、流式、不同键）完全不参与合并，互不影响。
- **键**：`route id | method | path | query | 该资源已知 Vary 字段对应的请求头值`，
  即“路由、方法、路径、查询以及会影响结果的请求头都相同”的请求才共享一次回源。
- **行为**：第一个到达的请求成为 leader 回源；其余同键请求等待并共享其结果
  （响应头 `x-coalesce: leader/shared` 可观测）。leader 返回失败响应（如 502/504/上游 5xx）
  时同样共享，等待方各自拿到可判断的失败；leader 异常退出时等待方自己重新发起，
  不会一直挂住。
- **等待上限**：`RouteConfig.coalesce_wait`（默认 10s，0 表示不合并）。
  超时返回 `504 coalesce_wait_timeout`，是明确结果而非静默回源。
- **不改变既有语义**：TTL、条件请求（304）、Vary 变体隔离都不受影响；
  不同键、可缓存与不可缓存、普通与流式请求之间绝不复用结果。
- **边界**：缓存完全冷启动、某资源尚未缓存过任何变体时，合并键不含 Vary 头
  （网关还不知道哪些头影响结果），此时并发不同变体请求可能共享同一次回源；
  首个响应缓存后，后续合并键即按 Vary 精确区分。

## 半开恢复与热更新

- **状态机**：`UP →（连接/超时失败）→ DOWN（冷却 fail_timeout，默认 5s）
  →（冷却到期）→ HALF_OPEN（仅放行 `RouteConfig.half_open_probes` 个并发探测，默认 1）
  →（探测成功）→ UP；（探测失败）→ DOWN 重新冷却`。
- **恢复语义**：冷却到点后**不整批放回**，只有探测成功的实例才重新参与常规分流；
  探测中的实例不接收常规流量（探测名额已满即跳过），避免刚恢复就被流量打满。
- **探测判定**：探测请求返回 <500 视为成功（实例恢复）；连接/超时失败或 5xx 视为失败
  （回到 DOWN，重新冷却）。对非探测实例，5xx 不改变其健康状态（保持既有语义）。
- **热更新配合**：`GatewayConfig` 快照原子替换；同一 `route id` 下
  **在途连接计数、半开状态、探测计数全部保留**；已从路由移除的实例状态被清理，
  其后续 `release` 为 no-op，不会产生残留或计数错乱。

## 故障判定与降级规则

| 场景 | HTTP 状态 | JSON `error` | 处理 |
| --- | --- | --- | --- |
| 无路由匹配 | 404 | `no_route` | 直接拒绝 |
| 命中黑名单 | 403 | `ip_blocked` | 直接拒绝 |
| 不在白名单 | 403 | `ip_not_allowed` | 直接拒绝 |
| 触发限流 | 429 | `rate_limited` | 带 `scope` 与 `Retry-After` |
| 路由存在但所有实例都不健康 | 503 | `no_upstream` | 不再尝试连接 |
| 上游连接失败 | 502 | `upstream_error` | 标记该实例熔断；可重试时换实例重试 |
| 上游读/写/连接超时 | 504 | `upstream_timeout` | 标记该实例熔断；可重试时换实例重试 |
| 回源合并等待超时 | 504 | `coalesce_wait_timeout` | 明确失败，可重新发起 |
| 上游自身返回 4xx/5xx | 原样透传 | — | 不缓存、不改写状态码；可重试时换实例重试 |

- **熔断与恢复**：某实例连接失败/超时后被标记为 DOWN 并进入 `fail_timeout`（默认 5 秒）冷却；
  冷却期内选择器跳过它；冷却结束后进入**半开**（HALF_OPEN），仅放行有限探测，
  探测成功才恢复为 UP（见“半开恢复与热更新”）。
- 多实例场景会优先把流量交给健康实例；全部不健康时才返回 503，
  因此 502/504（尝试过但失败）与 503（无实例可试）语义可区分。
- 重试耗尽时，502/504 响应体附加 `attempts` 与 `tried_upstreams`，
  能看出实际试过哪些实例、最终因什么失败。
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
- 重试/故障转移：超时与 5xx 换实例重试、重试耗尽（超时/5xx 透传）、写操作不重试、
  整体预算截断、无重试配置时保持原单次行为。
- 回源合并：同键并发只回源一次、不同键不共享、Vary 头区分合并键、
  共享失败与恢复、等待超时给出 504、不可缓存方法不合并。
- 半开恢复：冷却到期进入半开、探测名额限制、探测成功恢复/失败再熔断、
  热更新保留在途计数与半开状态、移除实例无残留。
- 上游失败：连接拒绝 502、读超时 504、全部熔断后 503、无路由 404，断言错误码彼此可区分。
- 流式透传：断言 SSE 5 帧原样到达。
- 策略：轮询序列、最少连接选择、冷却恢复、热更新保留在途计数。

测试运行日志（stderr）打印路由/限流/缓存/重试/合并/恢复的判定依据，例如：

```
... INFO [gateway] route route=svc upstream=a reason=strategy=round_robin ... attempt=1/2 probe=False target=...
... INFO [gateway] retry route=svc upstream=a attempt=1 decision=retry reason=timeout was_probe=False
... INFO [gateway] retry route=svc decision=exhausted attempts=2 tried=a,b last=timeout
... INFO [gateway] coalesce key=svc|GET|/svc/x| role=waiter decision=shared status=200
... INFO [gateway] recovery route=svc upstream=a decision=half_open_to_up
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

- 面向本地学习/验证：限流器、缓存与合并回源均为单进程内存实现，不做多实例共享持久化。
- 缓存不做 stale-while-revalidate / 仅在 TTL 内保证新鲜；鉴权以直连客户端 IP 为准。
- 重试仅针对幂等安全方法（GET/HEAD/OPTIONS）且非流式路由；写操作与流式请求
  不重试、不合并，语义与无重试配置时完全一致。
- 回源合并是“同键并发去重”，不是缓存：合并只发生在回源途中，结果共享后即失效，
  后续请求仍按 TTL 正常读缓存。
- 合并键里的 Vary 头来自该资源已缓存变体；冷启动（无任何已缓存变体）时
  不同变体的并发请求可能共享同一次回源，缓存建立后即按 Vary 精确区分。
- 半开探测名额按路由配置（`half_open_probes`，默认 1）；探测期间的实例
  不接收常规流量，恢复后才重新参与轮询/最少连接分流。
