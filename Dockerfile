# communicate gateway 多阶段构建
# 构建：docker build -t communicate-gateway .
# 说明：第一阶段编译出静态二进制，第二阶段用最小镜像跑，最终镜像不含 Go 工具链。

# ---- 构建阶段 ----
FROM golang:1.27-alpine AS build
WORKDIR /src

# 先只拷依赖清单，利用 Docker 层缓存：go.mod/go.sum 不变就不重新 go mod download
COPY go.mod go.sum ./
RUN go mod download

# 再拷源码（含 cmd/gateway/web 的 //go:embed 前端）
COPY . .

# 静态编译：CGO_ENABLED=0 产出无 cgo 依赖的静态二进制（modernc.org/sqlite 是纯 Go）
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/chat-gateway ./cmd/gateway

# ---- 运行阶段 ----
FROM alpine:3.20

# 非 root 用户（对应裸机 systemd 的 User=communicate）
RUN addgroup -S communicate && adduser -S -G communicate communicate
USER communicate

COPY --from=build /out/chat-gateway /usr/local/bin/chat-gateway

# 8888 TCP 客户端 / 8890 WebSocket+前端 / 9100 Prometheus 指标
EXPOSE 8888 8890 9100

ENTRYPOINT ["/usr/local/bin/chat-gateway"]
