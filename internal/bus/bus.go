// Package bus 抽象「消息投递」：公聊扇出、私聊定向，都通过它发布/订阅。
//
// 单机用内存实现（memory.go），分布式用 NATS 实现（nats.go，V3.3 后续接入）。
// 与 presence/store 同一套 ports & adapters 思路：接入层只依赖 Bus 接口，
// 不关心底下是进程内直发还是 NATS 跨节点投递。
//
// 为什么直接传 protocol.Message 而不是 []byte：bus 的职责就是投递聊天消息，
// protocol.Message 是各进程共享的领域词汇（protocol 包本来就被 server/client/bench
// 共用），直接带领域类型更简单、更类型安全；「为了通用传 bytes」属于过度抽象。
package bus

import "communicate/internal/protocol"

// Subscription 表示一个订阅，可取消。取消后不再收到该主题的消息。
type Subscription interface {
	// Unsubscribe 取消订阅。幂等：重复取消不报错。
	Unsubscribe() error
}

// Bus 是消息总线接口。subject 是主题名（频道），发布者不知道订阅者是谁、有几个，
// 这正是解耦的关键：发布方只管发，订阅方只管收。
type Bus interface {
	// Publish 把一条消息发布到 subject，投递给当前所有订阅该主题的处理函数。
	Publish(subject string, m protocol.Message) error

	// Subscribe 订阅 subject，之后该主题每收到一条消息就调用一次 handler。
	Subscribe(subject string, handler func(protocol.Message)) (Subscription, error)

	// Close 释放底层资源（连接、订阅等）。调用后不应再 Publish/Subscribe。
	Close() error
}
