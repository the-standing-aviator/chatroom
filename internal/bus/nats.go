package bus

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/nats-io/nats.go"

	"communicate/internal/protocol"
)

// NATS 是 Bus 的 NATS 实现（分布式）。subject 直接映射到 NATS 主题；
// 消息体用 JSON 编码 protocol.Message——NATS 消息自带长度分帧，无需再套
// protocol 的「长度前缀」帧头（那是给裸 TCP 字节流用的）。
type NATS struct {
	nc *nats.Conn
}

// NewNATS 连接指定 URL 的 NATS 并返回总线实现。连接失败返回 error。
func NewNATS(url string) (*NATS, error) {
	nc, err := nats.Connect(url)
	if err != nil {
		return nil, fmt.Errorf("connect nats %q: %w", url, err)
	}
	return &NATS{nc: nc}, nil
}

// Publish 把消息 JSON 编码后发布到 subject。
func (n *NATS) Publish(subject string, m protocol.Message) error {
	data, err := json.Marshal(m)
	if err != nil {
		return fmt.Errorf("marshal message: %w", err)
	}
	return n.nc.Publish(subject, data)
}

// Subscribe 订阅 subject，收到消息时解码为 protocol.Message 并调用 handler。
// 注意：handler 由 NATS 的 dispatch 协程并发调用，须自行保证 handler 线程安全
// （区别于内存实现的同步调用）。
func (n *NATS) Subscribe(subject string, handler func(protocol.Message)) (Subscription, error) {
	sub, err := n.nc.Subscribe(subject, func(msg *nats.Msg) {
		var m protocol.Message
		if err := json.Unmarshal(msg.Data, &m); err != nil {
			return // 非本协议消息，静默丢弃（解码失败不崩 dispatch 协程）
		}
		handler(m)
	})
	if err != nil {
		return nil, fmt.Errorf("subscribe %q: %w", subject, err)
	}
	return &natsSub{sub: sub}, nil
}

// Close 关闭 NATS 连接（连带取消所有订阅）。
func (n *NATS) Close() error {
	n.nc.Close()
	return nil
}

// natsSub 把 *nats.Subscription 包成总线 Subscription，隐藏底层类型。
type natsSub struct {
	sub *nats.Subscription
}

// Unsubscribe 取消订阅。幂等：NATS 原生重复取消会返回 ErrBadSubscription，
// 这里吞掉，保证与内存实现「重复取消不报错」的契约一致。
func (s *natsSub) Unsubscribe() error {
	if err := s.sub.Unsubscribe(); err != nil && !errors.Is(err, nats.ErrBadSubscription) {
		return err
	}
	return nil
}
