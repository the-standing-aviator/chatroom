// 压测客户端（load generator）：模拟 N 个客户端登录并保持在线，
// 由其中一个发公聊广播，统计广播延迟与丢消息数，作为 V2 性能基线。
//
// 与 cmd/client 一样复用 internal/protocol 的帧编解码，保证压测流量
// 走的是真实协议路径，而不是一套简化版假协议。
package main

import (
	"flag"
	"fmt"
	"math"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"communicate/internal/protocol"
)

// msgPrefix 是压测广播 content 的唯一前缀，后面跟序号，
// 用于让接收方把「第几条广播」与发送方打点对上（协议里广播没有消息 ID）。
const msgPrefix = "bench-msg-"

var (
	addr   = flag.String("addr", "127.0.0.1:8888", "服务器地址")
	n      = flag.Int("n", 100, "模拟客户端数量")
	msg    = flag.Int("msg", 10, "广播消息条数")
	settle = flag.Duration("settle", 0, "登录后到发消息前的静置时间，等上下线广播消化完")
)

// benchClient 是一个模拟客户端：一条 TCP 连接 + 一个昵称。
type benchClient struct {
	name string
	conn net.Conn
}

// login 发送 auth 帧并校验结果，成功返回 nil。
func (c *benchClient) login() error {
	if err := protocol.WriteFrame(c.conn, protocol.Message{
		Type:     protocol.TypeAuth,
		Name:     c.name,
		Password: "bench-pass",
	}); err != nil {
		return fmt.Errorf("write auth: %w", err)
	}
	resp, err := protocol.ReadFrame(c.conn)
	if err != nil {
		return fmt.Errorf("read auth resp: %w", err)
	}
	if resp.Type == protocol.TypeError {
		return fmt.Errorf("auth rejected: %s", resp.Error)
	}
	return nil
}

// receive 持续读取服务端推送，匹配到压测广播就记录端到端延迟。
// 一直阻塞读，直到连接被关闭（收尾时 close conn 打断）或对端断开。
// 不能像早期版本那样用 200ms 读超时 + 空转检查 done——那会让广播数据
// 到达后躺在 socket 里 ~100ms 才被读走，虚高涨到压测延迟（测量 artifact）。
func (c *benchClient) receive(col *collector) {
	for {
		m, err := protocol.ReadFrame(c.conn)
		if err != nil {
			return // 连接关闭或断开
		}
		if m.Type == protocol.TypeBroadcast && strings.HasPrefix(m.Content, msgPrefix) {
			if seq, err := strconv.Atoi(strings.TrimPrefix(m.Content, msgPrefix)); err == nil {
				col.markReceived(seq)
			}
		}
	}
}

// heartbeat 每 10s 发一次心跳，保持连接活跃（服务端 readTimeout=30s，
// 不发心跳会被当作断线踢掉）。
func (c *benchClient) heartbeat(done <-chan struct{}) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			_ = protocol.WriteFrame(c.conn, protocol.Message{Type: protocol.TypeHeartbeat})
		}
	}
}

// collector 汇总延迟样本与丢消息计数，线程安全。
type collector struct {
	mu        sync.Mutex
	sendTime  map[int]time.Time // 序号 -> 发送时刻（发送方写入）
	latencies []time.Duration   // 所有接收样本的端到端延迟
	received  int               // 实际收到的广播条数
}

func newCollector() *collector {
	return &collector{sendTime: make(map[int]time.Time)}
}

// markSent 由发送方在写帧前调用，记录该序号的发送时刻。
func (c *collector) markSent(seq int) {
	c.mu.Lock()
	c.sendTime[seq] = time.Now()
	c.mu.Unlock()
}

// markReceived 由接收方收到广播时调用，计算并记录端到端延迟。
func (c *collector) markReceived(seq int) {
	c.mu.Lock()
	if t0, ok := c.sendTime[seq]; ok {
		c.latencies = append(c.latencies, time.Since(t0))
		c.received++
	}
	c.mu.Unlock()
}

// percentile 返回排序后延迟样本的第 p 分位数。
func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(math.Ceil(p/100*float64(len(sorted)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func main() {
	flag.Parse()

	// 阶段 1：并发登录 N 个客户端。clients 各下标由不同 goroutine 写入，
	// 互不重叠，无需额外加锁。
	clients := make([]*benchClient, *n)
	var wg sync.WaitGroup
	start := time.Now()
	for i := 0; i < *n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			conn, err := net.Dial("tcp", *addr)
			if err != nil {
				fmt.Printf("bench-%d dial: %v\n", i, err)
				return
			}
			c := &benchClient{name: fmt.Sprintf("bench-%d", i), conn: conn}
			if err := c.login(); err != nil {
				fmt.Printf("bench-%d login: %v\n", i, err)
				_ = conn.Close()
				return
			}
			clients[i] = c
		}(i)
	}
	wg.Wait()
	loginDur := time.Since(start)

	// 过滤出登录成功的客户端。
	success := make([]*benchClient, 0, *n)
	for _, c := range clients {
		if c != nil {
			success = append(success, c)
		}
	}
	if len(success) == 0 {
		fmt.Println("没有任何客户端登录成功，无法继续")
		return
	}

	// 阶段 2：每个客户端启动接收循环；除发送方外都发心跳。
	col := newCollector()
	done := make(chan struct{})
	for i, c := range success {
		go c.receive(col)
		if i != 0 {
			go c.heartbeat(done)
		}
	}

	// 静置：等登录产生的上下线广播被各 writeLoop 消化完，再发消息，
	// 避免 chat 排在上下线广播后面被拖慢（这里测的是纯广播延迟）。
	if *settle > 0 {
		time.Sleep(*settle)
	}

	// 阶段 3：第一个客户端作为发送方，连发 msg 条公聊广播。
	sender := success[0]
	sent := 0
	for seq := 0; seq < *msg; seq++ {
		col.markSent(seq)
		err := protocol.WriteFrame(sender.conn, protocol.Message{
			Type:    protocol.TypeChat,
			Content: fmt.Sprintf("%s%d", msgPrefix, seq),
		})
		if err != nil {
			fmt.Printf("send msg-%d: %v\n", seq, err)
			break
		}
		sent++
	}

	// 阶段 4：给广播留出传播时间，然后关闭连接打断 receive、close(done) 打断 heartbeat。
	time.Sleep(2 * time.Second)
	for _, c := range success {
		_ = c.conn.Close()
	}
	close(done)

	col.mu.Lock()
	latencies := append([]time.Duration(nil), col.latencies...)
	received := col.received
	col.mu.Unlock()

	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })

	var avg time.Duration
	for _, d := range latencies {
		avg += d
	}
	if len(latencies) > 0 {
		avg /= time.Duration(len(latencies))
	}

	expected := len(success) * sent // 每条广播发给所有在线用户（含发送方自己）
	lost := expected - received

	fmt.Println()
	fmt.Println("===== 压测结果 =====")
	fmt.Printf("登录: 成功 %d/%d，耗时 %s（%.1f 个/秒）\n",
		len(success), *n, loginDur.Round(time.Millisecond), float64(len(success))/loginDur.Seconds())
	fmt.Printf("广播: 发送 %d 条，期望接收 %d，实际 %d，丢 %d\n",
		sent, expected, received, lost)
	fmt.Printf("延迟: 平均 %s，P50 %s，P99 %s（样本 %d）\n",
		avg.Round(time.Microsecond),
		percentile(latencies, 50).Round(time.Microsecond),
		percentile(latencies, 99).Round(time.Microsecond),
		len(latencies))
}
