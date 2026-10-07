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
| 回源合并 | 缓存未命中时同键并发只放行一个回源；按影响结果的请求头区分，冷启动不同变体不串味，回包后再按上游 `Vary` 权威校验，不一致者自行回源（single-flight） |
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
tests/                   72 个测试（单测 + 真实本地 uvicorn 端到端）
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
- **合并键**：`route id | method | path | query | 影响结果的请求头值`。
  - 该资源已缓存过变体时，取这些变体响应里 `Vary` 声明字段的并集（精确）；
  - **冷启动**（刚启动、或该资源从未缓存过任何变体，上游 `Vary` 尚未知）时，
    先按标准内容协商头 `accept / accept-encoding / accept-language` 做**保守预分流**，
    因此带 `accept-encoding: gzip` 和不带压缩的并发请求从一开始就落在不同航班，各自回源。
- **回包后的权威校验（防止变体串味）**：leader 回源返回后，每个 waiter 还要拿
  **上游响应自己声明的 `Vary`** 对一遍请求头：
  - 所有 `Vary` 字段的请求头值都与 leader 相同 → 共享该结果（`x-coalesce: shared`）；
  - 任一 `Vary` 字段不一致，或响应是 `Vary: *` → **绝不套用别人的变体**，
    waiter 立即**自己回源**，拿到与自己请求头对得上的响应（`x-coalesce: self`）。
    这条防线不依赖预分流是否覆盖了某个头：即便上游按自定义头（如 `Vary: x-tenant`）
    区分内容、冷启动预分流里没有它，waiter 也只会在 leader 回包后被校验拦住并自行回源，
    任何请求都不可能拿到与自己请求头不符的响应。
  - `Vary` 之外的请求头（如 `user-agent`）不同不影响共享。
- **回源失败**：leader 拿到的是明确失败响应（如上游 5xx）时，同航班 waiter 共享这份失败，
  各自都能凭状态码/响应体判断失败；航班随即结束，**之后再发会重新发起回源**，
  上游恢复后即可正常成功，不会一直失败或卡住。leader 发生连接级异常退出时，
  waiter 拿到 `aborted` 信号后也各自自行回源。
- **等待超时**：等待有上限 `RouteConfig.coalesce_wait`（默认 10s，`0` 表示不合并）。
  waiter 超时拿到的是**明确的失败响应** `504 coalesce_wait_timeout`（JSON `error` 码可判断），
  而不是静默套用别人的结果；航班本身不受影响，leader 成功后会正常写缓存，
  稍后重试即可命中，不会一直卡在那里。
- **不改变既有语义**：TTL、条件请求（304）、Vary 变体隔离都不受影响；
  不同键、可缓存与不可缓存、普通与流式请求之间绝不复用结果。
  回源合并只是“同键在途去重”，不是缓存：结果共享后航班即结束，后续仍按 TTL 正常读缓存。
- **可观测标记**：响应头 `x-coalesce` 取值
  `leader`（本次回源者）/ `shared`（共享了 leader 结果）/ `self`（因请求头不匹配
  或 leader 异常而自行回源）；日志打印每个请求的
  `coalesce key=... role=leader/waiter decision=fetch/shared/self_fetch/timeout`。

### 本地复现并发合并（冷启动 / 请求头不一致 / 失败 / 超时）

一键起服务后（`uv run python run_local.py`），假上游的 `/a/vary` 按 `Accept-Encoding`
返回 `gzip` / `identity` 两个变体并声明 `Vary: accept-encoding`；
`/a/vary-custom` 按自定义头 `X-Tenant` 区分并声明 `Vary: x-tenant`。
用并发请求即可复现，配合假上游 `state` 里的 `hits` 判断实际回源次数：

```bash
# ① 冷启动 + 请求头不一致：压缩/不压缩各自回源，各拿各的变体（hits +2）
for h in "gzip" "identity"; do
  curl -s -H "accept-encoding: $h" http://127.0.0.1:8000/api/users/vary &
done; wait
curl -s http://127.0.0.1:9101/a/state     # 两个变体各回源一次，不会串味

# ② 冷启动 + 自定义 Vary 头（不在预分流里）：回包后二次校验，不一致者自己回源
for t in 1 2; do
  curl -s -H "x-tenant: $t" http://127.0.0.1:8000/api/users/vary-custom &
done; wait
# 响应头里一个是 x-coalesce: leader，另一个是 x-coalesce: self

# ③ 回源失败共享、随后恢复：4 个并发都拿到 500，但只回源一次；恢复后再发即 200
curl -X POST "http://127.0.0.1:9101/a/state/fail?on=1"
curl -X POST "http://127.0.0.1:9101/a/state/slow?ms=300"
for i in $(seq 4); do curl -si http://127.0.0.1:8000/api/users/9 & done; wait
curl -X POST "http://127.0.0.1:9101/a/state/fail?on=0"
curl -X POST "http://127.0.0.1:9101/a/state/slow?ms=0"
curl -si http://127.0.0.1:8000/api/users/9   # 重新回源成功，x-cache: MISS

# ④ 等待超时：leader 回源超过 coalesce_wait，waiter 拿到明确 504，稍后再发成功
curl -X POST "http://127.0.0.1:9101/a/state/slow?ms=2000"
for i in 1 2; do curl -si http://127.0.0.1:8000/api/users/9 & done; wait
# 一个 200（leader），一个 504 {"error":"coalesce_wait_timeout"}（默认等待 10s，
# 自动化用例里把 coalesce_wait 调到 0.2s 稳定复现）
```

> 网关 stderr 日志会同时打印每个请求选中的实例（`route route=... upstream=...`）、
> 缓存判定（`cache key=... decision=MISS/STORE/HIT`）以及合并角色
> （`coalesce ... role=leader/waiter decision=...`），可直接对照出
> “哪些请求被合并、哪些各自回源、各自打到了哪个实例”。

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
- `GET /{instance}/vary`（任意网关前缀下 `/vary` 末段均可）：按 `Accept-Encoding` 返回
  `gzip`/`identity` 变体并声明 `Vary: accept-encoding`，用于复现冷启动请求头不一致的合并场景。
- `GET /{instance}/vary-custom`（任意前缀下 `/vary-custom` 末段均可）：按自定义头
  `X-Tenant` 返回变体并声明 `Vary: x-tenant`，用于复现“预分流未覆盖、回包后二次校验自回源”。
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
- 回源合并：同键并发只回源一次、不同键不共享、**冷启动压缩/不压缩并发各自回源不串味**、
  同变体并发仍合并（1 leader + N shared）、**自定义 Vary 头在回包后被二次校验、不一致者自回源**、
  自回源后两个变体各自缓存命中、共享失败与恢复、等待超时给出 504、不可缓存方法不合并。
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
... INFO [gateway] coalesce key=svc|GET|/svc/vary-custom||accept=*/*|accept-encoding=gzip, deflate|accept-language= role=waiter decision=self_fetch reason=vary_mismatch:x-tenant
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
- 回源合并是“同键在途去重”，不是缓存：合并只发生在回源途中，结果共享后航班即结束，
  后续请求仍按 TTL 正常读缓存。
- 合并键对“影响结果的请求头”的区分分两层：冷启动先用标准内容协商头
  （`accept/accept-encoding/accept-language`）保守预分流；无论是否预分流命中，
  leader 回包后都会以上游响应的 `Vary` 做权威二次校验，不一致（或 `Vary: *`）的
  waiter 一律自己回源。因此自定义 `Vary` 头、冷启动不同变体都绝不会串味。
- 半开探测名额按路由配置（`half_open_probes`，默认 1）；探测期间的实例
  不接收常规流量，恢复后才重新参与轮询/最少连接分流。
