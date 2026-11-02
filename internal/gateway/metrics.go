package gateway

import (
	"net/http"
	"runtime"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// RegisterMetrics 把聊天室指标注册进给定的 registerer。
//
// 接受 prometheus.Registerer 而非写死默认 registry：生产传 prometheus.DefaultRegisterer，
// 测试传独立的 NewRegistry，避免重复注册同名指标 panic（每个测试各建一个 Gateway）。
//
// 用 GaugeFunc/CounterFunc 而不是普通 Gauge/Counter：前者在每次 scrape 时
// 才调用闭包读取实时值，因此直接复用 Gateway 已有的状态（sessions 表、dropped
// 计数、runtime.NumGoroutine），无需在业务路径上额外埋点。
func RegisterMetrics(reg prometheus.Registerer, gw *Gateway) {
	reg.MustRegister(
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "chat_online",
			Help: "当前在线客户端数量",
		}, func() float64 {
			gw.mu.RLock()
			defer gw.mu.RUnlock()
			return float64(len(gw.sessions))
		}),
		prometheus.NewCounterFunc(prometheus.CounterOpts{
			Name: "chat_dropped_messages_total",
			Help: "因发送队列满被丢弃的消息总数（背压触发信号）",
		}, func() float64 {
			return float64(gw.dropped.Load())
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
