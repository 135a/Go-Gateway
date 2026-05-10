# FlowGate — 高性能分布式 API 网关

> 基于 Go + Gin + Redis + MySQL 构建的高性能 API 网关，支持动态路由、限流熔断、安全防护与分布式热更新。

[![Go Version](https://img.shields.io/badge/Go-1.21+-blue)](https://go.dev/)
[![License](https://img.shields.io/badge/License-MIT-green)](LICENSE)

---

## 📊 性能指标（JMeter 全场景压测）

| 指标 | 数值 |
|------|------|
| 总请求数 | 71,362 次 |
| 峰值吞吐量 | **5,777 req/s** |
| P50 响应延迟 | **27ms** |
| P90 响应延迟 | **< 70ms** |
| 路由匹配耗时 | **~15ns**（Go Benchmark 实测） |
| 限流拦截准确率 | **100%**，无漏放 |
| Admin 管理接口错误率 | **0%** |
| 配置热更新延迟 | **< 1s** |

---

## 🏗️ 系统架构

```
                          ┌─────────────────────────────────────────────────┐
                          │                   FlowGate                       │
                          │                                                   │
Client Request ──────────►│  Metrics → Blacklist → RateLimit → CircuitBreaker│──► Backend Services
                          │                    ↓                              │
                          │             Dynamic Proxy                         │
                          │          (Lock-free Radix Tree)                   │
                          └────────────────┬────────────────────────────────-┘
                                           │
                          ┌────────────────┴───────────────┐
                          │                                 │
                     ┌────▼────┐                     ┌──────▼──────┐
                     │  MySQL  │                     │    Redis    │
                     │ 持久存储 │                     │ 缓存+Pub/Sub │
                     │路由/黑名单│                    │ 分布式同步   │
                     └─────────┘                     └─────────────┘
                                                            │
                                              ┌─────────────┴─────────────┐
                                              │     Admin API 写入变更      │
                                              │  → 发布到 Redis Channel    │
                                              │  → 所有节点实时同步内存状态  │
                                              └───────────────────────────┘
```

### 请求处理中间件链

```
HTTP Request
    │
    ▼
[1] Metrics 中间件       ← 统计 QPS / 错误率 / 延迟，异步写入 Block 日志
    │
    ▼
[2] Blacklist 中间件     ← IP 黑名单 & Path 前缀黑名单，命中直接返回 403
    │
    ▼
[3] RateLimit 中间件     ← 滑动窗口限流（Redis Lua 原子脚本）
    │                      超频触发自动拉黑（Redis SETEX）
    ▼
[4] CircuitBreaker 中间件 ← 三态熔断器（Closed/Open/Half-Open）
    │                        连续失败自动熔断，冷却后自愈
    ▼
[5] Dynamic Proxy        ← 无锁 Radix Tree 路由匹配（~15ns）
    │                      Round-Robin 负载均衡，原子操作无锁
    ▼
Backend Service
```

---

## ⚙️ 核心技术实现

### 1. 无锁 Radix Tree 路由引擎（性能核心）

**设计目标**：在高并发下实现纳秒级路由匹配，同时支持零停机热更新。

**实现方案**：

- 基于 `atomic.Pointer[RouteTree]` 实现完全无锁的路由表，读操作无需任何锁，天然支持高并发读取。
- 路由树支持三种匹配模式：
  - **精确匹配**：`/api/user/login`
  - **参数路由**：`/api/user/:id`（`:` 前缀通配）
  - **通配符路由**：`/static/*filepath`
- 热更新策略：Admin API 收到路由变更后，**构建全新路由树**，通过 `atomic.Pointer.Store()` 原子替换，读写完全无锁，实现零停机动态上线路由。

```go
// 核心无锁读取
tree := r.tree.Load()  // atomic.Pointer，无锁

// 热更新：构建新树 → 原子替换
newTree := buildNewTree(routes)
r.tree.Store(newTree)  // 其他 goroutine 不受影响
```

**性能实测**（Go Benchmark，`go test -bench=. ./internal/engine/...`）：
```
BenchmarkRouteMatch-8    ~15ns/op    0 allocs/op
```

---

### 2. 双阈值限流 + 自动拉黑（安全防护）

**设计目标**：精准识别异常流量，自动封禁恶意 IP，全程无人工干预。

**实现方案**：

#### 第一阈值：滑动窗口限流（Redis Lua 原子脚本）

使用 Lua 脚本保证 `INCR + EXPIRE` 的原子性，避免计数器竞态问题：

```lua
local key    = KEYS[1]
local limit  = tonumber(ARGV[1])
local window = tonumber(ARGV[2])

local current = redis.call("INCR", key)
if current == 1 then
    redis.call("EXPIRE", key, window)
end
if current > limit then
    return 0  -- 超限，返回 429
end
return 1      -- 放行
```

#### 第二阈值：自动拉黑（Token Bucket 双重防护）

当单 IP 在窗口期内触发频率**超过拉黑阈值**时（如 60s 内请求 > 500 次），自动执行：

```go
// 写入 Redis 黑名单，TTL = 封禁时长
redis.SetEX(ctx, "blacklist:ip:"+ip, "1", banDuration)
```

- 后续请求在 **Blacklist 中间件**层直接拦截，返回 403，不再进入限流逻辑
- 压测场景中拦截准确率 **100%**，无漏放

---

### 3. 三态熔断器（高可用保障）

**设计目标**：后端服务故障时自动熔断，防止级联雪崩；故障恢复后自动自愈。

**状态机实现**：

```
           连续失败 >= 阈值
Closed ─────────────────────► Open
  ▲                              │
  │                              │ 冷却期结束
  │                              ▼
  │    探测请求成功          Half-Open
  └──────────────────────────────┘
         探测请求失败 → 重回 Open
```

| 状态 | 行为 |
|------|------|
| **Closed（正常）** | 正常转发，统计失败次数 |
| **Open（熔断）** | 直接返回 503，阻断级联雪崩 |
| **Half-Open（探测）** | 放行一个探测请求，成功则恢复，失败则继续熔断 |

所有状态转换基于 `atomic.Int32` 实现，无锁线程安全。

配合 **Round-Robin 负载均衡器**（`atomic.Uint64` 计数器）：多后端实例流量均匀分发，单节点故障自动剔除。

---

### 4. Redis Pub/Sub 分布式热更新（零停机运维）

**设计目标**：多网关实例部署时，Admin API 的任何配置变更能在 1s 内同步到所有节点，无需重启。

**实现流程**：

```
Admin API 请求
    │
    ▼
写入 MySQL（持久化）
    │
    ▼
发布到 Redis Channel（"gateway:config:update"）
    │
    ├──► 节点 A 订阅收到消息 → 重新加载路由/黑名单到内存
    ├──► 节点 B 订阅收到消息 → 重新加载路由/黑名单到内存
    └──► 节点 C 订阅收到消息 → 重新加载路由/黑名单到内存
              ↑
         延迟 < 1s
```

**优势**：
- 配置变更实时生效，不影响正在处理的请求
- 新节点启动时从 MySQL 全量加载，从 Redis 增量同步
- 支持水平扩展，节点数量不影响同步延迟

---

### 5. 可观测性体系

**Metrics 中间件**（异步统计，不影响主链路延迟）：

- 实时统计每个路由的 QPS、错误率、平均延迟
- 拦截事件（限流/黑名单/熔断）异步写入 MySQL Block 日志

**暴露接口**：

```bash
GET /api/metrics
# 返回：总请求数 / 成功率 / 各路由 QPS / 拦截次数

GET /api/logs
# 返回：最近的限流/熔断/黑名单拦截记录
```

---

## 🚀 快速启动

### 方式一：Docker Compose（推荐）

```bash
git clone https://github.com/135a/FlowGate-API-Gateway.git
cd FlowGate-API-Gateway

docker-compose up -d --build
```

服务启动后：
- 网关入口：`http://localhost:8080`
- Admin API：`http://localhost:8080/api`

### 方式二：本地运行

**环境要求**：Go 1.21+、MySQL 8.0、Redis 7.0

```bash
# 1. 初始化数据库
mysql -u root -p < scripts/mysql/init.sql

# 2. 配置环境变量
export MYSQL_DSN="root:password@tcp(127.0.0.1:3306)/flowgate"
export REDIS_ADDR="127.0.0.1:6379"

# 3. 启动网关
go run cmd/main.go
```

---

## 🛠️ Admin API 接口文档

### 路由管理

```bash
# 查询所有路由
GET /api/routes

# 新增路由
POST /api/routes
{
  "path": "/api/user/:id",
  "target": "http://user-service:8081",
  "method": "GET"
}

# 更新路由
PUT /api/routes/:id

# 删除路由
DELETE /api/routes/:id
```

### 黑名单管理

```bash
# 查询黑名单
GET /api/blacklist

# 新增黑名单规则
POST /api/blacklist
{
  "type": "IP",           # IP | PATH_PREFIX
  "value": "192.168.1.1"
}

# 删除规则
DELETE /api/blacklist/:id
```

### 监控与日志

```bash
# 实时流量指标
GET /api/metrics

# 拦截日志
GET /api/logs
```

---

## 📈 压测说明

项目内置完整的压测套件，位于 `/benchmark` 目录。

### Go Benchmark（单元级性能验证）

```bash
# 路由引擎基准测试
go test -bench=. -benchmem ./internal/engine/...

# 预期结果
BenchmarkRouteMatch-8    ~15ns/op    0 allocs/op
```

### JMeter 全场景压测

压测计划文件：`benchmark/FlowGate_Benchmark.jmx`

| 场景 | 描述 | 关键指标 |
|------|------|---------|
| 基础路由 | 300 线程持续压测正常请求 | 峰值 5,777 req/s，P90 < 70ms |
| 限流验证 | 超过阈值的请求批量触发 | 拦截准确率 100% |
| 熔断降级 | 模拟后端服务宕机 | 自动触发熔断，503 快速返回 |
| 极限压力 | 短时高并发冲击 | 系统稳定，无内存泄漏 |

详细执行步骤见 [`benchmark/STRESS_TEST_GUIDE.md`](benchmark/STRESS_TEST_GUIDE.md)。

---

## 📁 项目结构

```
FlowGate-API-Gateway/
├── cmd/
│   └── main.go              # 程序入口
├── internal/
│   ├── engine/              # 核心路由引擎（Radix Tree）
│   ├── middleware/          # 中间件链（限流/熔断/黑名单/Metrics）
│   ├── admin/               # Admin API 处理器
│   ├── proxy/               # 动态反向代理
│   └── pubsub/              # Redis Pub/Sub 热更新订阅
├── scripts/
│   └── mysql/               # 数据库初始化脚本
├── benchmark/               # 压测工具与报告
├── docker-compose.yml
└── Dockerfile
```

---

## 📄 License

MIT License — 欢迎 Star & Fork 🌟
