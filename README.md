# communicate — Go 分布式实时聊天室

> 一个真实可用的 Go 后端 + 网络编程项目：从单机 TCP 聊天室起步，演进到多节点分布式架构，目标是逐步做到**千万级在线**（作为北极星方向）。

用 Go 从零实现了聊天室的完整链路——自定义 TCP 协议、会话管理、JWT 鉴权、消息持久化、在线状态、跨节点消息路由、负载均衡、压测与可观测，并把每一层都抽象成可替换的接口（ports & adapters）。

## 功能特性

- **公聊广播**：一条消息广播给所有在线用户
- **私聊**：指定昵称定向投递，跨节点可达
- **改昵称**：全局唯一，重名拒绝（并发安全）
- **在线列表**：全局视角（分布式下跨节点）
- **历史消息**：持久化落盘，重启不丢
- **Web 前端**：浏览器经 WebSocket 进聊天室（登录 / 公聊 / 在线列表），与 TCP 客户端同房间互通
- **JWT 鉴权**：注册 / 登录 / 凭 token 免密重连（滑动过期）
- **心跳 + 断线检测**：超时判定离线并广播
- **优雅关闭**：SIGTERM 下 flush 持久化队列，不丢消息
- **水平扩展**：多 gateway 节点 + L4 负载均衡 + NATS 消息总线
- **可观测**：zap 结构化日志 + Prometheus 指标 + pprof 性能分析

## 架构

### 部署拓扑

```
                        ┌────────────────┐
   浏览器 WebSocket ───▶│                │
                        │       LB       │   L4 轮询（cmd/lb）
   终端 TCP ──────────▶│    :9000       │
                        │                │
   压测 bench ────────▶│                │
                        └───────┬────────┘
                                │ TCP round-robin
            ┌───────────────────┼───────────────────┐
            ▼                   ▼                   ▼
       ┌─────────┐        ┌─────────┐        ┌─────────┐
       │ gateway │        │ gateway │   …    │ gateway │  接入层：无状态
       │ node-0  │        │ node-1  │        │ node-N  │  可水平扩展
       │ :8888   │        │ :8889   │        │         │  （internal/gateway）
       └────┬────┘        └────┬────┘        └────┬────┘
            │                  │                  │
            └──────────────────┼──────────────────┘
                               │
        ┌──────────────────────┼──────────────────────┐
        ▼                      ▼                      ▼
   ┌─────────┐          ┌─────────────┐         ┌─────────────┐
   │  NATS   │          │    Redis    │         │ PostgreSQL  │
   │ 消息总线 │          │  在线状态   │         │   持久化    │
   └─────────┘          └─────────────┘         └─────────────┘

可观测：每个 gateway 暴露 /metrics + /debug/pprof（Prometheus 拉取），zap 结构化日志。
```

### 单节点内部数据流

```
 客户端连接
    │
    ├─ TCP 字节流 ──▶ tcpConn ─┐
    │                           ├── conn 接口（ReadMessage / WriteMessage / …）
    └─ WS 文本帧 ──▶ wsConn ───┘
                              │
                              ▼
                    handle(conn) goroutine        ← 每连接一个协程
                              │
                    ┌─────────┴─────────┐
                    │   authenticate    │  注册 / 登录 / 免密重连（JWT）
                    └─────────┬─────────┘
                              │ addSession → presence.Join（昵称全局唯一）
                              ▼
                       读循环 route(msg)
                              │
        ┌──────────┬─────────┼─────────┬──────────────┐
        ▼          ▼         ▼         ▼              ▼
      chat      private    rename     who       history / heartbeat
        │          │         │         │
        ▼          ▼         ▼         ▼
   broadcast   private   presence   presence.Online
        │      (定向投递) .Rename
        ▼
   bus.Publish ──▶ NATS / memory ──▶ subscribeBus 回调 ──▶ 各本地 Session.send
                                                              │
                                                   sendCh(64) ┐
                                                   sysCh(8)   ├─▶ writeLoop ──▶ conn.WriteMessage ──▶ 客户端
                                                              │
        │
        └─ enqueue ──▶ saveCh(1024) ──▶ saveLoop ──▶ store.SaveMessage（异步落盘）
```

要点：

- **`conn` 是传输层的 port**：`tcpConn`（TCP 长度前缀帧）和 `wsConn`（WebSocket 文本帧）是同一个接口的两个 adapter。`handle`/`Session`/`route` 只面向「消息」，不关心底层字节流还是 WS 帧——所以浏览器和终端客户端能进同一个房间、互发消息，业务代码零改动。
- **写路径解耦**：广播先经 `bus`（内存直发或 NATS 跨节点），回调把消息投进每个 `Session` 的 `sendCh`（业务队列，满则丢并计数）与 `sysCh`（系统消息队列，尽力而为），由 `writeLoop` 单独写回连接——慢客户端不拖垮广播。
- **持久化异步**：广播同时 `enqueue` 到 `saveCh`，`saveLoop` 后台落盘，磁盘 I/O 不阻塞广播延迟。

### ports & adapters 分层

核心思想：业务逻辑只依赖接口，不关心底层实现。单机模式和分布式模式**业务代码零改动**，只换后端的适配器实现。

| 接口 | 默认实现 | 可选实现 | 职责 |
|------|---------|---------|------|
| `conn`（传输层） | `tcpConn` | `wsConn` | 面向「消息」的连接：TCP 长度前缀帧 / WebSocket 文本帧 |
| `bus.Bus` | `memory` | `nats` | 消息发布/订阅（广播扇出、私聊定向） |
| `presence.Presence` | `memory` | `redis` | 在线登记（昵称全局唯一、跨节点定位） |
| `store.Store` | `sqlite` | `postgres` | 用户 + 消息持久化 |
| `auth.Manager` | JWT | — | 无状态鉴权（签发/校验） |

## 目录结构

```
communicate/
├── cmd/
│   ├── gateway/       # 聊天接入节点（含 web/ 前端，//go:embed 打包）
│   ├── client/        # 命令行聊天客户端
│   ├── lb/            # L4 TCP 负载均衡器（round-robin）
│   └── bench/         # 压测客户端（load generator）
├── internal/
│   ├── auth/          # JWT 签发/校验
│   ├── bus/           # 消息总线（memory / NATS）
│   ├── gateway/       # 接入层核心：会话管理、消息路由、广播
│   ├── presence/      # 在线状态（memory / Redis）
│   ├── protocol/      # 自定义 TCP 协议（长度前缀 + JSON 帧）
│   └── store/         # 存储（SQLite / PostgreSQL）
├── scripts/
│   └── run-cluster.sh # 一键起多节点集群（N gateway + 1 LB）
├── deploy/            # 生产部署：systemd + nginx/Caddy + 部署手册
├── docs/              # 需求 / 设计 / 架构 / 复盘 / 问题记录
├── docker-compose.yml # 开发依赖：PostgreSQL + NATS + Redis
└── go.mod
```

## 技术栈

- **语言/运行时**：Go（goroutine / channel / context）
- **协议**：自定义 TCP 协议（长度前缀 + JSON 帧）/ WebSocket（JSON 文本帧，gorilla/websocket）
- **存储**：SQLite（单机）/ PostgreSQL（分布式）
- **在线状态**：内存 / Redis（`HSetNX` 保证昵称全局唯一）
- **消息总线**：内存 / NATS（core pub-sub）
- **鉴权**：JWT（HMAC-SHA256）
- **可观测**：zap + Prometheus + pprof
- **编排**：Docker + docker-compose

## 快速开始

> 环境：Go 1.21+，Docker（分布式模式需要）。本仓库在 WSL 内开发，构建/测试用 WSL 的 Linux Go（Windows 版 Go 在 9P 文件系统上会报 RLock 错误，详见 `docs/问题与解决方案.md`）。

### 单机模式（最快跑通，零外部依赖）

```bash
# 终端 1：启动服务端（默认 SQLite + 内存在线状态 + 内存总线）
go build -o chat-gateway ./cmd/gateway
./chat-gateway -addr 127.0.0.1:8888

# 终端 2/3：启动客户端（可开多个，互发消息）
go run ./cmd/client -addr 127.0.0.1:8888

# 浏览器：打开 http://127.0.0.1:8890（WebSocket 前端，默认 -ws-addr 端口）
```

客户端登录后是菜单驱动：`1` 公聊 / `2` 私聊 / `3` 改昵称 / `4` 在线列表 / `5` 历史消息 / `0` 退出。浏览器端是登录框 + 聊天窗，与终端客户端进的是同一个房间。

### 分布式模式（多节点集群）

```bash
# 1. 启动外部依赖（PostgreSQL + NATS + Redis）
docker compose up -d

# 2. 一键起 N 个 gateway + 1 个 LB（默认 2 节点）
scripts/run-cluster.sh 2

# 3. 客户端连 LB（LB 在 127.0.0.1:9000，round-robin 分到各节点）
go run ./cmd/client -addr 127.0.0.1:9000
```

`scripts/run-cluster.sh` 会自动构建 gateway/lb，启动 N 个节点（监听 `127.0.0.1:8888` 起递增，metrics 端口 `9091` 起递增），再起 LB 把连接轮询分发给各节点。Ctrl-C 优雅停掉所有进程。

### gateway 参数

```
-addr            监听地址（默认 127.0.0.1:8888）
-ws-addr          WebSocket 监听地址（默认 127.0.0.1:8890，浏览器前端接入）
-allowed-origins  WebSocket Origin 白名单（逗号分隔）；空 = 放行所有（开发）
-node             节点身份 ID（多节点部署须全局唯一；默认回落 -addr）
-store            存储后端：sqlite | postgres
-dsn              PostgreSQL DSN（-store postgres 时；可读环境变量 COMMUNICATE_DSN）
-presence         在线状态：memory | redis
-bus              消息总线：memory | nats
-metrics          /metrics 与 pprof 监听地址（默认 :9090）
-jwt-secret       JWT 签名密钥（生产务必覆盖；可读环境变量 COMMUNICATE_JWT_SECRET）
```

## 部署（公网）

上线最小可用的部署方案：gateway 跑在 systemd 服务里，nginx（或 Caddy）做 TLS 终止 + WebSocket 反向代理，浏览器经 `https`/`wss` 接入。含密钥环境变量注入、systemd 单元、nginx/Caddy 配置与上线检查清单，详见 [`deploy/README.md`](deploy/README.md)。

## 压测

`cmd/bench` 模拟 N 个客户端登录并保持在线，由其中一个连发广播，统计**登录吞吐、广播延迟（avg/P50/P99）、丢消息数**：

```bash
go build -o chat-bench ./cmd/bench

# 单机：1000 客户端、10 条广播
./chat-bench -addr 127.0.0.1:8888 -n 1000 -msg 10 -settle 8s

# 分布式：连 LB，压多节点集群
./chat-bench -addr 127.0.0.1:9000 -n 1000 -msg 10 -settle 8s
```

基准数据（V3.4，1000 客户端 / 10 广播）：单机 avg 15.8ms；2 节点（经 NATS）avg 14.8ms、在线分布 500/500、丢 0。

## 测试

```bash
go test -race ./...   # 全部单元/集成测试 + 数据竞态检测
```

覆盖：协议帧编解码、存储、JWT、presence（memory + Redis 同一套契约用例）、bus（memory + NATS）、gateway 会话管理，以及两个跨边界集成测试——`TestWebSocketClientChatsWithTCPClient`（浏览器 WS 与终端 TCP 同房间互通）和 `TestTwoNodesCrossNode`（双节点跨节点，Redis/NATS 不可用时自动 skip）。

## 里程碑与路线图

| 里程碑 | 主题 | 状态 |
|--------|------|------|
| V1 | 正确性（修 bug + 鉴权 + 持久化 + 测试） | ✅ 完成 |
| V2 | 性能（压测调优 + 异步持久化 + 背压 + 可观测） | ✅ 完成 |
| V3 | 分布式（NATS + Redis + PG + LB 水平扩展） | ✅ 完成 |
| V4 | 上线最小可用（WebSocket 前端 + 公网部署） | 🚧 进行中 |
| V5 | 中等规模（分区扇出 + 分片） | 📋 规划 |
| V6 | 大规模（多区域 + 分库分表） | 📋 规划 |
| 🎯 | 千万级在线 | 北极星方向 |

## 项目文档

- `docs/需求.md` — 需求（功能/非功能/验收）
- `docs/设计.md` — 架构、协议、数据结构设计
- `docs/V3架构.md` — V3 分布式架构讨论稿
- `docs/项目要点记录.md` — 决策记录 + 当前状态 + 下一步
- `docs/问题与解决方案.md` — 问题根因 / 方案（P1-1 ~ P3-3）
- `docs/复盘.md` — 阶段复盘
