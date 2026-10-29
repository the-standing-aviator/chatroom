package server

import (
	"fmt"
	"net"
	"runtime/debug"
	"time"

	"go.uber.org/zap"

	"communicate/internal/protocol"
	"communicate/internal/store"
)

// handle 处理一个客户端连接：鉴权 → 加入在线表 → 读循环路由消息。
func (srv *Server) handle(conn net.Conn) {
	// recover 兜底：单连接处理 panic 只影响该连接，不崩整个进程（修复 P1-1）。
	defer func() {
		if r := recover(); r != nil {
			srv.log.Error("panic in handler",
				zap.Any("panic", r),
				zap.String("stack", string(debug.Stack())),
			)
		}
		_ = conn.Close()
	}()

	sess := newSession(conn, srv)
	defer sess.close()

	// 鉴权
	if !srv.authenticate(sess) {
		return
	}

	// 加入在线表（昵称唯一性在此保证）
	if !srv.addSession(sess) {
		sess.send(protocol.Message{Type: protocol.TypeError, Error: "该用户已在线"})
		sess.setName("") // 复位，避免 close 时误广播"已下线"
		return
	}
	srv.log.Info("user online", zap.String("name", sess.Name()), zap.String("addr", conn.RemoteAddr().String()))
	srv.broadcastSystem(fmt.Sprintf("%s 已上线", sess.Name()))

	// 读循环：每轮刷新读超时，心跳会不断重置它；超时即判定断线。
	for {
		_ = conn.SetReadDeadline(time.Now().Add(readTimeout))
		m, err := protocol.ReadFrame(conn)
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				sess.send(protocol.Message{Type: protocol.TypeError, Error: "长时间未活跃，已被断开"})
			}
			break
		}
		srv.route(sess, m)
	}
}

// route 按消息类型分发处理。
func (srv *Server) route(s *Session, m protocol.Message) {
	switch m.Type {
	case protocol.TypeChat:
		if m.Content == "" {
			s.send(protocol.Message{Type: protocol.TypeError, Error: "消息内容不能为空"})
			return
		}
		srv.broadcast(s.Name(), m.Content)

	case protocol.TypePrivate:
		if m.To == "" || m.Content == "" {
			s.send(protocol.Message{Type: protocol.TypeError, Error: "私聊需要指定对象和内容"})
			return
		}
		if !srv.private(s.Name(), m.To, m.Content) {
			s.send(protocol.Message{Type: protocol.TypeError, Error: "用户 " + m.To + " 不存在或不在线"})
		}

	case protocol.TypeRename:
		newName := trimName(m.NewName)
		if newName == "" {
			s.send(protocol.Message{Type: protocol.TypeError, Error: "昵称不能为空"})
			return
		}
		old := s.Name()
		if err := srv.rename(s, newName); err != nil {
			s.send(protocol.Message{Type: protocol.TypeError, Error: err.Error()})
			return
		}
		s.send(protocol.Message{Type: protocol.TypeSystem, Content: "已更新昵称为 " + newName, TS: time.Now().Unix()})
		srv.broadcastSystem(fmt.Sprintf("%s 改名为 %s", old, newName))

	case protocol.TypeWho:
		s.send(protocol.Message{Type: protocol.TypeWhoList, Users: srv.who(), TS: time.Now().Unix()})

	case protocol.TypeHistory:
		msgs, err := srv.store.ListMessages(m.Limit)
		if err != nil {
			srv.log.Error("list messages failed", zap.Error(err))
			s.send(protocol.Message{Type: protocol.TypeError, Error: "查询历史失败"})
			return
		}
		s.send(protocol.Message{Type: protocol.TypeHistory, Messages: storeToProto(msgs), TS: time.Now().Unix()})

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
			Type:    m.Type,
			From:    m.From,
			To:      m.To,
			Content: m.Content,
			TS:      m.TS.Unix(),
		})
	}
	return out
}
