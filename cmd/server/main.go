// 聊天室服务端入口：初始化日志与存储，启动服务器。
package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof" // 仅副作用：init 里把 /debug/pprof/* 注册到 DefaultServeMux
	"os"
	"os/signal"
	"syscall"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"

	"communicate/internal/presence"
	"communicate/internal/server"
	"communicate/internal/store"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:8888", "监听地址")
	dbPath := flag.String("db", "chat.db", "SQLite 数据库文件路径（-store sqlite 时使用）")
	storeType := flag.String("store", "sqlite", "存储后端：sqlite | postgres")
	dsn := flag.String("dsn", "postgres://chat:chat@localhost:5432/chat?sslmode=disable", "PostgreSQL DSN（-store postgres 时使用）")
	metricsAddr := flag.String("metrics", ":9090", "Prometheus /metrics 监听地址")
	presenceType := flag.String("presence", "memory", "在线状态后端：memory | redis")
	redisAddr := flag.String("redis-addr", "localhost:6379", "Redis 地址（-presence redis 时使用）")
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

	srv := server.NewServer(*addr, logger, st, p)

	// 可观测：注册指标并起独立 HTTP 端口，暴露 /metrics（Prometheus 拉取）与
	// /debug/pprof/*（性能分析）。两者都挂在 DefaultServeMux，同一端口统一管理。
	server.RegisterMetrics(prometheus.DefaultRegisterer, srv)
	http.Handle("/metrics", server.MetricsHandler())
	go func() {
		logger.Info("metrics server started", zap.String("addr", *metricsAddr))
		if err := http.ListenAndServe(*metricsAddr, nil); err != nil { // nil → DefaultServeMux
			logger.Error("metrics server failed", zap.Error(err))
		}
	}()

	// 监听退出信号，触发优雅关闭：flush 持久化队列后退出，正常关闭不丢消息。
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	done := make(chan struct{})
	go func() {
		sig := <-sigCh
		logger.Info("shutdown signal received", zap.String("signal", sig.String()))
		srv.Shutdown()
		close(done)
	}()

	if err := srv.Start(); err != nil {
		logger.Fatal("server failed", zap.Error(err))
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
