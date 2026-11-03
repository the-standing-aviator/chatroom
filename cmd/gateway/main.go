// 聊天室接入节点（gateway）入口：初始化日志与存储，启动接入节点。
package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof" // 仅副作用：init 里把 /debug/pprof/* 注册到 DefaultServeMux
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"

	"communicate/internal/auth"
	"communicate/internal/bus"
	"communicate/internal/gateway"
	"communicate/internal/presence"
	"communicate/internal/store"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:8888", "监听地址")
	wsAddr := flag.String("ws-addr", "127.0.0.1:8890", "WebSocket 监听地址（浏览器前端接入）")
	allowedOrigins := flag.String("allowed-origins", "", "WebSocket 允许的 Origin 白名单（逗号分隔）；空 = 放行所有（开发）")
	node := flag.String("node", "", "节点身份 ID（默认取 -addr 值；多节点部署时须全局唯一）")
	dbPath := flag.String("db", "chat.db", "SQLite 数据库文件路径（-store sqlite 时使用）")
	storeType := flag.String("store", "sqlite", "存储后端：sqlite | postgres")
	dsn := flag.String("dsn", "postgres://chat:chat@localhost:5432/chat?sslmode=disable", "PostgreSQL DSN（-store postgres 时使用）")
	metricsAddr := flag.String("metrics", ":9090", "Prometheus /metrics 监听地址")
	presenceType := flag.String("presence", "memory", "在线状态后端：memory | redis")
	redisAddr := flag.String("redis-addr", "localhost:6379", "Redis 地址（-presence redis 时使用）")
	jwtSecret := flag.String("jwt-secret", "dev-secret-change-me", "JWT 签名密钥（生产务必覆盖）")
	jwtTTL := flag.Duration("jwt-ttl", 24*time.Hour, "JWT 有效期")
	busType := flag.String("bus", "memory", "消息总线后端：memory | nats")
	natsURL := flag.String("nats-url", "nats://localhost:4222", "NATS 地址（-bus nats 时使用）")
	flag.Parse()

	logger, err := zap.NewProduction()
	if err != nil {
		log.Fatalf("init logger: %v", err)
	}
	defer logger.Sync()

	st, err := openStore(*storeType, *dbPath, *dsn)
	if err != nil {
		logger.Fatal("open store failed", zap.Error(err))
	}
	defer st.Close()

	p, err := openPresence(*presenceType, *redisAddr)
	if err != nil {
		logger.Fatal("open presence failed", zap.Error(err))
	}
	defer p.Close()

	am := auth.New(*jwtSecret, *jwtTTL)

	b, err := openBus(*busType, *natsURL)
	if err != nil {
		logger.Fatal("open bus failed", zap.Error(err))
	}
	defer b.Close()

	// 节点身份默认回落监听地址：单机/单进程无需指定；多节点部署时每个节点 -node 唯一。
	nodeID := *node
	if nodeID == "" {
		nodeID = *addr
	}

	gw := gateway.NewGateway(nodeID, *addr, logger, st, p, am, b)

	// WebSocket Origin 白名单：生产设真实域名（如 https://chat.example.com），
	// 杜绝别的网站用你的服务做跨站 WebSocket；空 = 放行所有（开发/本地）。
	if *allowedOrigins != "" {
		var origins []string
		for _, o := range strings.Split(*allowedOrigins, ",") {
			if o = strings.TrimSpace(o); o != "" {
				origins = append(origins, o)
			}
		}
		gw.SetAllowedOrigins(origins)
	}

	// 可观测：注册指标并起独立 HTTP 端口，暴露 /metrics（Prometheus 拉取）与
	// /debug/pprof/*（性能分析）。两者都挂在 DefaultServeMux，同一端口统一管理。
	gateway.RegisterMetrics(prometheus.DefaultRegisterer, gw)
	http.Handle("/metrics", gateway.MetricsHandler())
	go func() {
		logger.Info("metrics server started", zap.String("addr", *metricsAddr))
		if err := http.ListenAndServe(*metricsAddr, nil); err != nil { // nil → DefaultServeMux
			logger.Error("metrics server failed", zap.Error(err))
		}
	}()

	// WebSocket 入口：浏览器通过 HTTP 升级到 WS，与 TCP 客户端共用同一套 gateway。
	// 独立 ServeMux（不用 DefaultServeMux），避免与 metrics/pprof 的注册混在一起。
	wsMux := http.NewServeMux()
	wsMux.Handle("/", webHandler())             // 静态前端：/ 出 index.html
	wsMux.HandleFunc("/ws", gw.HandleWebSocket) // /ws 出 WebSocket 升级（比 / 更精确，优先匹配）
	wsServer := &http.Server{Addr: *wsAddr, Handler: wsMux}
	go func() {
		logger.Info("websocket server started", zap.String("addr", *wsAddr))
		if err := wsServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("websocket server failed", zap.Error(err))
		}
	}()

	// 监听退出信号，触发优雅关闭：flush 持久化队列后退出，正常关闭不丢消息。
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	done := make(chan struct{})
	go func() {
		sig := <-sigCh
		logger.Info("shutdown signal received", zap.String("signal", sig.String()))
		// 先停 WebSocket 监听（拒绝新升级），再 Shutdown 现有会话，
		// 否则 Shutdown 等 handle 退出期间仍可能有新 WS 连进来导致永远等不到。
		_ = wsServer.Close()
		gw.Shutdown()
		close(done)
	}()

	if err := gw.Start(); err != nil {
		logger.Fatal("gateway failed", zap.Error(err))
	}
	<-done // 等 Shutdown flush 完成
	logger.Info("shutdown complete")
}

// openStore 按 -store 选择存储后端，返回 Store 接口实现。
// 服务端只依赖接口，不关心底层是 SQLite 还是 PostgreSQL。
func openStore(kind, dbPath, dsn string) (store.Store, error) {
	switch kind {
	case "sqlite":
		return store.Open(dbPath)
	case "postgres":
		return store.OpenPostgres(dsn)
	default:
		return nil, fmt.Errorf("unknown store type %q (want sqlite | postgres)", kind)
	}
}

// openPresence 按 -presence 选择在线状态后端，返回 Presence 接口实现。
// 服务端只依赖接口，不关心底层是内存还是 Redis（ports & adapters）。
func openPresence(kind, addr string) (presence.Presence, error) {
	switch kind {
	case "memory":
		return presence.NewMemory(), nil
	case "redis":
		return presence.NewRedis(addr), nil
	default:
		return nil, fmt.Errorf("unknown presence type %q (want memory | redis)", kind)
	}
}

// openBus 按 -bus 选择消息总线后端，返回 Bus 接口实现。
// 服务端只依赖接口，不关心底层是内存直发还是 NATS 跨节点投递（ports & adapters）。
func openBus(kind, url string) (bus.Bus, error) {
	switch kind {
	case "memory":
		return bus.NewMemory(), nil
	case "nats":
		return bus.NewNATS(url)
	default:
		return nil, fmt.Errorf("unknown bus type %q (want memory | nats)", kind)
	}
}
