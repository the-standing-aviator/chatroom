package main

import (
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/zap"
)

// TestRoundRobinNext 验证选择器按顺序循环返回后端地址。
func TestRoundRobinNext(t *testing.T) {
	rr := newRoundRobin([]string{"a", "b", "c"})
	got := []string{rr.next(), rr.next(), rr.next(), rr.next()}
	want := []string{"a", "b", "c", "a"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("第 %d 次 next() = %q, want %q", i, got[i], want[i])
		}
	}
}

// startEcho 起一个 echo 后端，返回监听地址；每接一条连接就给 hits +1，
// 用于事后断言该后端被命中了几次。
func startEcho(t *testing.T, hits *atomic.Int64) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return // listener 关闭
			}
			hits.Add(1)
			go func(c net.Conn) {
				defer c.Close()
				_, _ = io.Copy(c, c) // echo：收到的字节原样写回
			}(c)
		}
	}()
	return ln.Addr().String()
}

// TestRunDistributesRoundRobinAndEchoes 起两个 echo 后端 + 一个 LB，
// 通过 LB 建 4 条连接，验证：① 连接被轮流分到两个后端（各 2 次）；
// ② 字节能经 LB 双向透传（echo 回来内容一致）。
func TestRunDistributesRoundRobinAndEchoes(t *testing.T) {
	var h1, h2 atomic.Int64
	b1 := startEcho(t, &h1)
	b2 := startEcho(t, &h2)

	lbLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { lbLn.Close() })

	go run(lbLn, newRoundRobin([]string{b1, b2}), zap.NewNop())

	for i := 0; i < 4; i++ {
		c, err := net.Dial("tcp", lbLn.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		msg := []byte("ping")
		if _, err := c.Write(msg); err != nil {
			t.Fatal(err)
		}
		buf := make([]byte, len(msg))
		if _, err := io.ReadFull(c, buf); err != nil {
			t.Fatal(err)
		}
		if string(buf) != string(msg) {
			t.Fatalf("第 %d 条 echo = %q, want %q", i, buf, msg)
		}
		_ = c.Close()
	}

	// hits 由后端 goroutine 异步自增，稍等它落地再断言分布。
	time.Sleep(20 * time.Millisecond)
	if h1.Load() != 2 || h2.Load() != 2 {
		t.Fatalf("命中分布 = b1:%d b2:%d, want 各 2", h1.Load(), h2.Load())
	}
}
