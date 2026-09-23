# 本地反向代理网关（Python / FastAPI）

一套**完全本地可复现**的反向代理网关：用多个本地假上游实例模拟后端集群，
实现按请求特征的路由分发、轮询/最少连接选择、按客户端+路由维度限流、
来源白/黑名单、带 TTL 的响应缓存与条件请求、流式透传，以及可区分的故障原因。

不依赖任何外部服务或真实凭据。

---

## 1. 快速开始

```bash
uv sync                       # 安装依赖（含 pytest）
uv run python run_local.py    # 启动 3 个假上游(9001/9002/9003) + 网关(8080)
```

另开终端：

```bash
# 轮询：连续调用会在 u0/u1/u2 间循环
curl -s localhost:8080/gateway/rr/echo
# 最少连接
curl -s localhost:8080/gateway/lc/echo
# 缓存（TTL 2 秒）
curl -s -D - localhost:8080/gateway/cache/cacheable
# 限流（5 次 / 2 秒）
for i in $(seq 1 7); do curl -s -o /dev/null -w "%{http_code} " \
  localhost:8080/gateway/rl/echo; done; echo
# 流式（SSE 原样分块透传）
curl -N "localhost:8080/gateway/rr/stream?chunks=5&delay_ms=100"
# 运行期计数（路由选择次数、在途连接、缓存命中/未命中）
curl -s localhost:8080/admin/stats | python -m json.tool
```

运行测试（29 个，全部基于真实本地假上游）：

```bash
uv run pytest          # 摘要
uv run pytest -s       # 打印路由选择 / 限流计数 / 缓存判定日志
```

---

## 2. 模块结构

| 文件 | 职责 |
| --- | --- |
| `main.py` | HTTP 服务入口；默认本地配置、生命周期、代理路由与 admin 路由挂载 |
| `run_local.py` | 一键在本机多端口启动 3 个假上游 + 网关 |
| `fake_upstream.py` | 假上游：`/echo` `/count` `/slow` `/fail` `/work` `/stream` `/cacheable(-lm)` |
| `gateway/router.py` | 最长前缀路由匹配；轮询 / 最少连接选择；原子热更新；选择计数 |
| `gateway/ratelimit.py` | 固定窗口限流（单锁，绝不多放行）；白/黑名单（支持 CIDR） |
| `gateway/cache.py` | 带 TTL 的缓存、新鲜度判定、`If-None-Match` / `If-Modified-Since`、304 续期 |
| `gateway/upstream.py` | httpx 上游客户端；区分超时/连接失败；SSE 流式透传 |
| `gateway/proxy.py` | 主流程串联：ACL → 路由 → 限流 → 流式/缓存/转发 |
| `gateway/admin.py` | 热更新配置、切换策略、清缓存、重置换行窗口、查看统计 |
| `gateway/errors.py` | 统一错误类型与可区分响应（状态码 + `error` code + `retryable`） |

---

## 3. 路由与选择策略

- **匹配**：按最长路径前缀匹配（`/gateway/rr` 命中 `rr` 路由）；无匹配返回
  `404 {"error":"route_not_found"}`。
- **上游路径**：默认剥掉网关挂载点 `/gateway` 与路由前缀，并保留 query string。
- **策略**：
  - `round_robin`：路由级轮询下标；
  - `least_connections`：选择在途活跃连接数最少的实例，同数时按固定顺序打破平局。
  - 在途计数在选中时 +1，请求结束（含异常、流结束）时 -1。
- **热更新**：
  - `PUT /admin/config`：原子替换整份配置快照；
  - `POST /admin/routes/{id}/strategy`：单路由切换策略。
  - 在途请求已持有的上游配置与计数不受影响，**热更新不中断在途请求**
    （`test_config_hot_reload_does_not_break_inflight` 并发在途期间反复换配置验证）。
- **路由配置示例**：

```jsonc
{
  "id": "cache",
  "path_prefix": "/gateway/cache",
  "upstream_ids": ["u0", "u1"],
  "strategy": "round_robin",
  "cache_ttl": 2.0,
  "rate_limit": {"requests": 5, "window_seconds": 2.0}
}
```

---

## 4. 限流与白/黑名单语义

- **维度**：限流键为 `(客户端 IP, 路由 id, 上限, 窗口)`，不同客户端、不同路由互不影响。
  优先使用路由级配置，其次使用全局 `default_rate_limit`。
- **算法**：固定窗口计数。计数与窗口判断在同一把锁内完成，
  **并发压力下严格只放行 N 个**（`test_concurrent_rate_limit_no_over_admit`：
  20 并发、上限 5，恰好 5 个 200、15 个 429）。
- **拒绝响应**：`429`，带 `Retry-After`（距离窗口重置秒数），body：

```json
{"error":"rate_limited","message":"...","retryable":true}
```

- **ACL**：黑名单优先拒绝；白名单非空时仅放行名单来源。支持单 IP 与 CIDR。
  - 命中黑名单：`403 ip_blacklisted`
  - 不在白名单：`403 ip_not_allowed`（原因与黑名单可区分）
- 客户端 IP 取 `X-Forwarded-For` 首段（本地验证方便模拟来源），否则取 TCP 对端。
- 管理接口：`POST /admin/ratelimit/reset` 清空计数；ACL 随 `PUT /admin/config` 热更新。

---

## 5. 缓存语义

- **缓存对象**：仅 `GET/HEAD` 且路由 `cache_ttl > 0`；状态码属于可缓存集合
  （200/203/204/301/308/404…）；上游 `Cache-Control: no-store|private` 不缓存。
- **缓存键**：`METHOD 上游路径?query`（query 不同键不同）。
- **新鲜命中**：TTL 内直接返回缓存体，响应带 `Age` 与 `X-Cache: HIT`，
  **不再访问上游**（测试用假上游命中计数冻结来断言）。
- **条件请求**：
  - 缓存新鲜且 `If-None-Match` 命中 / `If-Modified-Since` 晚于 `Last-Modified`：
    网关直接回 `304`（`X-Cache: HIT-304`），不访问上游；
  - 缓存过期：网关以缓存的 `ETag`/`Last-Modified` 发条件请求回源；
    上游回 `304` 时复用旧响应体并刷新 TTL（revalidate）；上游回 `200` 时替换缓存。
- **未命中回源**：响应带 `X-Cache: MISS`。
- **不缓存的请求**：非 GET/HEAD（如 POST）原样转发，响应不带缓存标记。
- `POST /admin/cache/clear` 清缓存；`GET /admin/stats` 查看每键 hit/miss。

---

## 6. 流式透传

- 上游路径以 `/stream` 开头（或请求头 `X-Gateway-Stream: 1`）时，
  使用 httpx 流式请求，**chunk 到达即转发**（首块延迟远小于总耗时，非缓冲）。
- 上游响应头（`Content-Type`、`X-Upstream-Instance`、`Cache-Control: no-store` 等）原样透传。
- 流式响应**绝不进入缓存**；流关闭后在途连接计数立即释放。
- 打开流阶段的超时/连接失败同样区分为 504/502。

---

## 7. 故障判定与降级规则

| 场景 | HTTP 状态 | `error` code | `retryable` | 说明 |
| --- | --- | --- | --- | --- |
| 无匹配路由 / 路由无有效上游 | 404 | `route_not_found` | false | 客户端路径错误，不重试 |
| 命中黑名单 | 403 | `ip_blacklisted` | false | |
| 不在白名单 | 403 | `ip_not_allowed` | false | 与黑名单原因可区分 |
| 超出限流 | 429 | `rate_limited` | true | 带 `Retry-After` |
| 上游 TCP 连接失败 | 502 | `upstream_unavailable` | true | 端口未监听 / 连接重置等 |
| 上游响应/开流超时 | 504 | `upstream_timeout` | true | 带 `Retry-After`=超时配置 |
| 上游自身 4xx/5xx | 原样透传 | 上游 body | — | 网关不改写状态码（如假上游 `/fail` 的 500） |

所有错误响应统一为：

```json
{"error": "<可区分 code>", "message": "<细节>", "retryable": <bool>}
```

降级原则：网关自身只对「连不上 / 超时 / 被治理规则拒绝」负责并给出可重试提示；
上游已产出的状态码与响应体（含错误 body、SSE 分块）原样透传，不吞错、不伪造。

---

## 8. 观测日志（判定依据）

运行期与测试（`pytest -s`）会打印每一步判定：

```
[acl]      ip=... ALLOW/REJECT ...
[router]   MATCH HIT/MISS  method=... path=... -> route=...
[router]   SELECT route=... strategy=... upstream=... active=... active_map=...
[router]   RELEASE upstream=... active=...
[ratelimit] ip=... route=... count=3/5 decision=ALLOW/REJECT
[cache]    key=... HIT/MISS/STALE/STORE/REVALIDATED ... hit_count=...
[cache]    CONDITIONAL ... If-None-Match=... -> 304/200
[upstream] OK/TIMEOUT/UNAVAILABLE/STREAM OPEN/STREAM CLOSE ...
```

`GET /admin/stats` 返回路由选择次数、各上游在途数、缓存每键 hit/miss 计数。

---

## 9. 本地验证清单（对应需求）

| 能力 | 测试 |
| --- | --- |
| 轮询分发 | `test_round_robin_distribution` |
| 最少连接（并发均衡） | `test_least_connections_prefers_idle` / `test_concurrent_least_connections_balances` |
| 热更新不中断在途 | `test_config_hot_reload_does_not_break_inflight` |
| 策略热切换 | `test_strategy_hot_switch` |
| 无路由/坏上游可区分 | `test_no_route_distinguishable` / `test_misconfigured_upstream` |
| 并发限流不多放行 | `test_concurrent_rate_limit_no_over_admit` |
| 限流窗口恢复、429+Retry-After | `test_rate_limit_window_reopens` / `..._sequential_distinguishable` |
| 白/黑名单/CIDR | `test_blacklist_and_whitelist` / `test_acl_hot_reload_via_admin` |
| 缓存命中不访问上游 | `test_cache_hit_skips_upstream` |
| 条件请求 304 | `test_conditional_request_on_fresh_cache_returns_304` / `test_if_modified_since_304` |
| TTL 过期再校验 | `test_ttl_expiry_then_revalidate` |
| 非 GET / query 分键 | `test_non_get_not_cached` / `test_cache_key_includes_query` |
| 超时 504 / 失败 502 / 5xx 透传 | `tests/test_upstream_failures.py` |
| 失败原因可区分 | `test_error_codes_are_distinguishable`（4 种 code 互不相同） |
| 流式原样透传/计数释放/不缓存 | `tests/test_streaming.py` |
