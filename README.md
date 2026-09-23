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
| 透传 | 流式路由与不可缓存方法**原样透传**，不读不写缓存 |
| 可区分失败 | 无路由 / 黑名单 / 白名单 / 限流 / 上游连接失败 / 超时 / 无健康实例，状态码与 JSON `error` 码均不同 |

## 目录结构

```
main.py                  HTTP 服务入口（uvicorn 启动 main:app）
run_local.py             一键本地启动：假上游 :9101 + 网关 :8000
gateway/
  config.py              配置数据模型（路由/上游/策略/限流/缓存 TTL）
  router.py              最长前缀匹配 + 原子热更新快照
  upstream.py            轮询/最少连接选择器 + 健康熔断与冷却恢复
  ratelimit.py           线程安全令牌桶与多维限流器
  access.py              白名单/黑名单判定
  cache.py               TTL 响应缓存、新鲜度、Vary 键
  proxy.py               代理主链路与可区分错误码
  app.py                 构造 FastAPI、挂载代理与 /admin 管理接口
  local_config.py        指向本地假上游的演示配置
fake_upstream/
  server.py              本地假上游（多实例 a/b/c、延迟、故障开关、SSE 流）
tests/                   43 个测试（单测 + 真实本地 uvicorn 端到端）
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

## 故障判定与降级规则

| 场景 | HTTP 状态 | JSON `error` | 处理 |
| --- | --- | --- | --- |
| 无路由匹配 | 404 | `no_route` | 直接拒绝 |
| 命中黑名单 | 403 | `ip_blocked` | 直接拒绝 |
| 不在白名单 | 403 | `ip_not_allowed` | 直接拒绝 |
| 触发限流 | 429 | `rate_limited` | 带 `scope` 与 `Retry-After` |
| 路由存在但所有实例都不健康 | 503 | `no_upstream` | 不再尝试连接 |
| 上游连接失败 | 502 | `upstream_error` | 标记该实例熔断 |
| 上游读/写/连接超时 | 504 | `upstream_timeout` | 标记该实例熔断 |
| 上游自身返回 4xx/5xx | 原样透传 | — | 不缓存、不改写状态码 |

- **熔断与恢复**：某实例连接失败/超时后被标记为不健康并进入 `fail_timeout`（默认 5 秒）冷却；
  冷却期内选择器跳过它，冷却结束后自动重新参与选择（半开恢复）。
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
- 上游失败：连接拒绝 502、读超时 504、全部熔断后 503、无路由 404，断言错误码彼此可区分。
- 流式透传：断言 SSE 5 帧原样到达。
- 策略：轮询序列、最少连接选择、冷却恢复、热更新保留在途计数。

测试运行日志（stderr）打印三类判定依据，例如：

```
... INFO [gateway] route route=orders upstream=a reason=strategy=least_conn streaming=False target=...
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

- 面向本地学习/验证：限流器与缓存为单进程内存实现，不做多实例共享持久化。
- 缓存不做 stale-while-revalidate / 仅在 TTL 内保证新鲜；鉴权以直连客户端 IP 为准。
