// 聊天室服务端入口：初始化日志与存储，启动服务器。
package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"go.uber.org/zap"

	"communicate/internal/server"
	"communicate/internal/store"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:8888", "监听地址")
	dbPath := flag.String("db", "chat.db", "SQLite 数据库文件路径")
	flag.Parse()

	logger, err := zap.NewProduction()
	if err != nil {
		log.Fatalf("init logger: %v", err)
	}
	defer logger.Sync()

	st, err := store.Open(*dbPath)
	if err != nil {
		logger.Fatal("open store failed", zap.Error(err))
	}
	defer st.Close()

	srv := server.NewServer(*addr, logger, st)

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
