// 负载均衡器（LB）：L4 TCP 反向代理，把每个进来的 TCP 连接轮流（round-robin）
// 分发给后端网关（gateway）节点。V3.4 用它在压测时把 bench 的连接均匀分到多节点。
//
// 为什么自写而不是用 HAProxy/nginx：本项目主线是练 Go 网络编程，LB 的本质
// 就是「accept 连接 → 挑后端 → 双向搬运字节」，自己实现一遍才能讲清原理；
// 生产等价物是 HAProxy 的 balance roundrobin，接口（bench 连的地址）不变。
package main

import (
	"flag"
	"io"
	"log"
	"net"
	"strings"
	"sync/atomic"

	"go.uber.org/zap"
)

// roundRobin 是后端选择器：next() 依次返回下一个后端地址，转完一圈从头再来。
// 用原子计数器保证并发连接的选择线程安全且无锁。
type roundRobin struct {
	backends []string
	counter  atomic.Uint64
}

// newRoundRobin 构造选择器。backends 必须非空，否则 next() 会除零。
func newRoundRobin(backends []string) *roundRobin {
	return &roundRobin{backends: backends}
}

// next 返回下一个后端地址（先自增再取模，i 从 0 开始）。
func (r *roundRobin) next() string {
	i := r.counter.Add(1) - 1
	return r.backends[i%uint64(len(r.backends))]
}

// proxy 双向桥接 client（客户端↔LB）与 backend（LB↔网关）两条连接：
// 一个 goroutine 把 client→backend 的字节搬过去，主 goroutine 把 backend→client 的搬回来。
// 任一侧读完（连接关闭/出错）就关闭另一侧，另一侧 io.Copy 随即因关闭而返回，
// 两个方向都不会泄漏 goroutine。这是 net 库经典的双向转发写法。
func proxy(client, backend net.Conn) {
	go func() {
		_, _ = io.Copy(backend, client) // client → backend
		_ = backend.Close()             // 客户端发完了：关掉对网关这一侧
	}()
	_, _ = io.Copy(client, backend) // backend → client
	_ = client.Close()              // 网关读完了：关掉对客户端这一侧
}

// run 是 LB 的 accept 主循环：接一条连接就开一个 goroutine 路由并转发。
// 单独抽出来（而非塞在 main 里）是为了能用临时 listener 做测试。
func run(ln net.Listener, rr *roundRobin, logger *zap.Logger) {
	for {
		conn, err := ln.Accept()
		if err != nil {
			logger.Error("accept failed", zap.Error(err))
			continue
		}
		go handleConn(conn, rr, logger)
	}
}

// handleConn 处理一条客户端连接：轮询挑一个后端，拨号过去，然后双向转发。
func handleConn(client net.Conn, rr *roundRobin, logger *zap.Logger) {
	defer client.Close()

	backend := rr.next()
	upstream, err := net.Dial("tcp", backend)
	if err != nil {
		logger.Warn("dial backend failed", zap.String("backend", backend), zap.Error(err))
		return // 后端不可达：直接断开这条客户端连接
	}
	// 每条连接的归属用 Debug 级别记录：压测上万连接时 Info 会刷屏，生产默认 Info 不显示。
	logger.Debug("routed", zap.String("client", client.RemoteAddr().String()), zap.String("backend", backend))
	proxy(client, upstream)
}

// splitBackends 把逗号分隔的地址串拆成去空白的列表。
func splitBackends(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if t := strings.TrimSpace(p); t != "" {
			out = append(out, t)
		}
	}
	return out
}

func main() {
	listenAddr := flag.String("listen", "127.0.0.1:9000", "LB 监听地址（bench 连这里）")
	backendsFlag := flag.String("backends", "127.0.0.1:8888,127.0.0.1:8889", "后端网关地址列表，逗号分隔")
	flag.Parse()

	backends := splitBackends(*backendsFlag)
	if len(backends) == 0 {
		log.Fatalf("no backends: -backends 至少给一个后端地址，如 127.0.0.1:8888,127.0.0.1:8889")
	}

	logger, err := zap.NewProduction()
	if err != nil {
		log.Fatalf("init logger: %v", err)
	}
	defer logger.Sync()

	rr := newRoundRobin(backends)
	ln, err := net.Listen("tcp", *listenAddr)
	if err != nil {
		logger.Fatal("listen failed", zap.Error(err))
	}
	logger.Info("lb started", zap.String("listen", *listenAddr), zap.Strings("backends", backends))

	run(ln, rr, logger)
}
