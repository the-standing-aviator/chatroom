package gateway

import (
	"errors"
	"fmt"
	"net"
	"runtime/debug"
	"time"

	"go.uber.org/zap"

	"communicate/internal/presence"
	"communicate/internal/protocol"
	"communicate/internal/store"
)

// handle 处理一个客户端连接：鉴权 → 加入在线表 → 读循环路由消息。
func (gw *Gateway) handle(c conn) {
	// recover 兜底：单连接处理 panic 只影响该连接，不崩整个进程（修复 P1-1）。
	defer func() {
		if r := recover(); r != nil {
			gw.log.Error("panic in handler",
				zap.Any("panic", r),
				zap.String("stack", string(debug.Stack())),
			)
		}
		// 连接由 writeLoop 在 flush 完发送队列后统一关闭（优雅关闭），这里不再直接关。
	}()

	sess := newSession(c, gw)
	defer sess.close()

	// 鉴权
	if !gw.authenticate(sess) {
		return
	}

	// 加入在线表（昵称唯一性由 presence.Join 保证）
	if err := gw.addSession(sess); err != nil {
		if errors.Is(err, presence.ErrConflict) {
			sess.send(protocol.Message{Type: protocol.TypeError, Error: "该用户已在线"})
		} else {
			gw.log.Error("add session failed", zap.Error(err))
			sess.send(protocol.Message{Type: protocol.TypeError, Error: "上线失败，请稍后重试"})
		}
		sess.setName("") // 复位，避免 close 时误广播"已下线"
		return
	}
	gw.log.Info("user online", zap.String("name", sess.Name()), zap.String("addr", c.RemoteAddr()))
	gw.broadcastSystem(fmt.Sprintf("%s 已上线", sess.Name()))

	// 读循环：每轮刷新读超时，心跳会不断重置它；超时即判定断线。
	for {
		_ = c.SetReadDeadline(time.Now().Add(readTimeout))
		m, err := c.ReadMessage()
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				sess.send(protocol.Message{Type: protocol.TypeError, Error: "长时间未活跃，已被断开"})
			}
			break
		}
		// 消息限流：除心跳外，客户端所有消息过一遍令牌桶。超限则回 error 并丢弃，
		// 而不是断连——刷屏是行为问题，警告即可；断连反而可能误伤偶尔手快的正常用户。
		// 心跳必须豁免，否则慢速限流会把保活心跳拒掉，客户端被误判断线。
		if m.Type != protocol.TypeHeartbeat && !sess.limiter.Allow() {
			sess.send(protocol.Message{Type: protocol.TypeError, Error: "发送太频繁，请稍后再试"})
			continue
		}
		gw.route(sess, m)
	}
}

// route 按消息类型分发处理。
func (gw *Gateway) route(s *Session, m protocol.Message) {
	switch m.Type {
	case protocol.TypeChat:
		if m.Content == "" {
			s.send(protocol.Message{Type: protocol.TypeError, Error: "消息内容不能为空"})
			return
		}
		gw.broadcast(s.Name(), m.Content)

	case protocol.TypeFile:
		if m.FileID == "" || m.FileName == "" {
			s.send(protocol.Message{Type: protocol.TypeError, Error: "文件消息缺少 file_id 或 file_name"})
			return
		}
		// 带 To 的是私聊文件：只投递给目标用户，不进公聊广播。
		if m.To != "" {
			if m.To == s.Name() {
				s.send(protocol.Message{Type: protocol.TypeError, Error: "不能给自己发私聊文件"})
				return
			}
			if !gw.privateFile(s.Name(), m.To, m.FileID, m.FileName, m.FileSize) {
				s.send(protocol.Message{Type: protocol.TypeError, Error: "用户 " + m.To + " 不存在或不在线"})
			}
			return
		}
		gw.broadcastFile(s.Name(), m.FileID, m.FileName, m.FileSize)

	case protocol.TypePrivate:
		if m.To == "" || m.Content == "" {
			s.send(protocol.Message{Type: protocol.TypeError, Error: "私聊需要指定对象和内容"})
			return
		}
		if !gw.private(s.Name(), m.To, m.Content) {
			s.send(protocol.Message{Type: protocol.TypeError, Error: "用户 " + m.To + " 不存在或不在线"})
		}

	case protocol.TypeRename:
		newName := trimName(m.NewName)
		if newName == "" {
			s.send(protocol.Message{Type: protocol.TypeError, Error: "昵称不能为空"})
			return
		}
		old := s.Name()
		if err := gw.rename(s, newName); err != nil {
			s.send(protocol.Message{Type: protocol.TypeError, Error: err.Error()})
			return
		}
		// 改名后重签 token（sub = 新名），否则前端重连时用旧 token 会被打回旧名。
		s.send(protocol.Message{Type: protocol.TypeSystem, Content: "已更新昵称为 " + newName, Token: gw.issueToken(newName), TS: time.Now().Unix()})
		gw.broadcastSystem(fmt.Sprintf("%s 改名为 %s", old, newName))

	case protocol.TypeWho:
		s.send(protocol.Message{Type: protocol.TypeWhoList, Users: gw.who(), TS: time.Now().Unix()})

	case protocol.TypeHistory:
		// 按 s.Name() 过滤：只返回公聊 + 与自己相关的私聊，防止私聊内容泄漏给第三方。
		// BeforeID > 0 表示「上滑加载更早」的分页请求：按指定会话（Peer）取更早消息。
		var msgs []store.Message
		var err error
		if m.BeforeID > 0 {
			msgs, err = gw.store.ListMessagesBefore(s.Name(), m.Peer, m.BeforeID, m.Limit)
		} else {
			msgs, err = gw.store.ListMessages(s.Name(), m.Limit)
		}
		if err != nil {
			gw.log.Error("list messages failed", zap.Error(err))
			s.send(protocol.Message{Type: protocol.TypeError, Error: "查询历史失败"})
			return
		}
		// 回显 Peer/BeforeID，让前端能分辨这是「初始全量」还是「某会话上滑加载更早」，
		// 并把结果归位到正确的会话（否则多会话并发分页时无法对应）。
		s.send(protocol.Message{Type: protocol.TypeHistory, Peer: m.Peer, BeforeID: m.BeforeID, Messages: storeToProto(msgs), TS: time.Now().Unix()})

	case protocol.TypeHeartbeat:
		s.send(protocol.Message{Type: protocol.TypePong})

	default:
		s.send(protocol.Message{Type: protocol.TypeError, Error: "未知消息类型: " + m.Type})
	}
}

// storeToProto 把持久化消息转换为协议消息，用于 history 响应。
func storeToProto(msgs []store.Message) []*protocol.Message {
	out := make([]*protocol.Message, 0, len(msgs))
	for _, m := range msgs {
		out = append(out, &protocol.Message{
			ID:       m.ID,
			Type:     m.Type,
			From:     m.From,
			To:       m.To,
			Content:  m.Content,
			FileID:   m.FileID,
			FileName: m.FileName,
			FileSize: m.FileSize,
			TS:       m.TS.Unix(),
		})
	}
	return out
}
