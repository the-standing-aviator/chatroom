package server

import (
	"errors"
	"strings"
	"time"

	"go.uber.org/zap"
	"golang.org/x/crypto/bcrypt"

	"communicate/internal/protocol"
	"communicate/internal/store"
)

// authenticate 处理登录/注册：昵称不存在则注册，存在则校验密码。
// 成功则设置会话昵称并返回 true。
func (srv *Server) authenticate(s *Session) bool {
	_ = s.conn.SetReadDeadline(time.Now().Add(authTimeout))
	m, err := protocol.ReadFrame(s.conn)
	if err != nil {
		return false
	}
	if m.Type != protocol.TypeAuth {
		s.send(protocol.Message{Type: protocol.TypeError, Error: "请先登录（auth）"})
		return false
	}

	name := trimName(m.Name)
	if name == "" || m.Password == "" {
		s.send(protocol.Message{Type: protocol.TypeError, Error: "昵称和密码不能为空"})
		return false
	}

	u, err := srv.store.GetUser(name)
	switch {
	case errors.Is(err, store.ErrNotFound):
		// 新用户：注册
		hash, err := bcrypt.GenerateFromPassword([]byte(m.Password), bcrypt.DefaultCost)
		if err != nil {
			srv.log.Error("hash password failed", zap.Error(err))
			return false
		}
		if err := srv.store.CreateUser(name, string(hash)); err != nil {
			if errors.Is(err, store.ErrConflict) {
				s.send(protocol.Message{Type: protocol.TypeError, Error: "该昵称已被注册，请换一个"})
			} else {
				srv.log.Error("create user failed", zap.Error(err))
			}
			return false
		}
		s.setName(name)
		s.send(protocol.Message{Type: protocol.TypeSystem, Content: "注册成功，欢迎 " + name, Token: srv.issueToken(name), TS: time.Now().Unix()})
		return true

	case err != nil:
		srv.log.Error("get user failed", zap.Error(err))
		return false

	default:
		// 已有用户：校验密码
		if bcrypt.CompareHashAndPassword([]byte(u.PasswordHash), []byte(m.Password)) != nil {
			s.send(protocol.Message{Type: protocol.TypeError, Error: "密码错误"})
			return false
		}
		s.setName(name)
		s.send(protocol.Message{Type: protocol.TypeSystem, Content: "登录成功，欢迎 " + name, Token: srv.issueToken(name), TS: time.Now().Unix()})
		return true
	}
}

// issueToken 为已通过密码校验的用户签发 JWT。失败时记录日志并返回空串，
// 不让 token 签发失败阻断登录（V3.3 之前 token 尚不参与鉴权校验）。
func (srv *Server) issueToken(name string) string {
	token, err := srv.auth.Issue(name)
	if err != nil {
		srv.log.Error("issue token failed", zap.Error(err))
		return ""
	}
	return token
}

// trimName 规范化昵称：去掉首尾空白并限制长度。
func trimName(name string) string {
	name = strings.TrimSpace(name)
	if r := []rune(name); len(r) > 32 {
		name = string(r[:32])
	}
	return name
}
