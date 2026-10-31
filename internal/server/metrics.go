package server

import (
	"net/http"
	"runtime"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// RegisterMetrics 把聊天室指标注册进默认 registry。
//
// 每个进程只能调用一次：重复注册同名指标会 panic，所以放在 main 里、
// NewServer 之外（否则每个测试各建一个 Server 会二次注册）。
//
// 用 GaugeFunc/CounterFunc 而不是普通 Gauge/Counter：前者在每次 scrape 时
// 才调用闭包读取实时值，因此直接复用 Server 已有的状态（sessions 表、dropped
// 计数、runtime.NumGoroutine），无需在业务路径上额外埋点。
func RegisterMetrics(srv *Server) {
	prometheus.MustRegister(
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "chat_online",
			Help: "当前在线客户端数量",
		}, func() float64 {
			srv.mu.RLock()
			defer srv.mu.RUnlock()
			return float64(len(srv.sessions))
		}),
		prometheus.NewCounterFunc(prometheus.CounterOpts{
			Name: "chat_dropped_messages_total",
			Help: "因发送队列满被丢弃的消息总数（背压触发信号）",
		}, func() float64 {
			return float64(srv.dropped.Load())
		}),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "chat_goroutines",
			Help: "当前进程 goroutine 数量（协程泄漏探测器）",
		}, func() float64 {
			return float64(runtime.NumGoroutine())
		}),
	)
}

// MetricsHandler 返回暴露默认 registry 的 /metrics 处理器。
// 独立于聊天 TCP 端口，避免拉指标时占用业务连接。
func MetricsHandler() http.Handler {
	return promhttp.Handler()
}
