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
# 同时建好数据目录并 chown：-file-dir 默认是相对路径 ./files，靠下面的 WORKDIR /data
# 落到 /data/files。目录必须在切 USER 之前就建好且属于 communicate，否则非 root 启动时
# storage.NewDisk 的 MkdirAll 会 permission denied，进程 Fatal 退出（容器无限重启）。
RUN addgroup -S communicate && adduser -S -G communicate communicate \
 && mkdir -p /data/files \
 && chown -R communicate:communicate /data

COPY --from=build /out/chat-gateway /usr/local/bin/chat-gateway

# 工作目录设成数据目录，让默认的 ./files 落在持久化的 /data 下
WORKDIR /data

# 声明 /data 为数据卷：裸 docker run 不给 -v 时也不至于把上传的文件写进容器可写层，
# 容器一删文件就没了。docker compose 里由具名卷 gateway_files 显式挂载。
VOLUME ["/data"]

USER communicate

# 8888 TCP 客户端 / 8890 WebSocket+前端 / 9100 Prometheus 指标
EXPOSE 8888 8890 9100

ENTRYPOINT ["/usr/local/bin/chat-gateway"]
