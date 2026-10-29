// 聊天室服务端入口：初始化日志与存储，启动服务器。
package main

import (
	"flag"
	"log"

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
	if err := srv.Start(); err != nil {
		logger.Fatal("server failed", zap.Error(err))
	}
}
