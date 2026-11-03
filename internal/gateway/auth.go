package gateway

import (
	"errors"
	"strings"
	"time"

	"go.uber.org/zap"
	"golang.org/x/crypto/bcrypt"

	"communicate/internal/auth"
	"communicate/internal/protocol"
	"communicate/internal/store"
)

// authenticate 处理登录/注册：昵称不存在则注册，存在则校验密码。
// 成功则设置会话昵称并返回 true。
func (gw *Gateway) authenticate(s *Session) bool {
	_ = s.conn.SetReadDeadline(time.Now().Add(authTimeout))
	m, err := s.conn.ReadMessage()
	if err != nil {
		return false
	}
	// 免密重连：auth_token 帧走 token 校验路径，身份以 token 的 sub 为准。
	if m.Type == protocol.TypeAuthToken {
		return gw.authenticateToken(s, m)
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

	u, err := gw.store.GetUser(name)
	switch {
	case errors.Is(err, store.ErrNotFound):
		// 新用户：注册
		hash, err := bcrypt.GenerateFromPassword([]byte(m.Password), bcrypt.DefaultCost)
		if err != nil {
			gw.log.Error("hash password failed", zap.Error(err))
			return false
		}
		if err := gw.store.CreateUser(name, string(hash)); err != nil {
			if errors.Is(err, store.ErrConflict) {
				s.send(protocol.Message{Type: protocol.TypeError, Error: "该昵称已被注册，请换一个"})
			} else {
				gw.log.Error("create user failed", zap.Error(err))
			}
			return false
		}
		s.setName(name)
		s.send(protocol.Message{Type: protocol.TypeSystem, Content: "注册成功，欢迎 " + name, Token: gw.issueToken(name), TS: time.Now().Unix()})
		return true

	case err != nil:
		gw.log.Error("get user failed", zap.Error(err))
		return false

	default:
		// 已有用户：校验密码
		if bcrypt.CompareHashAndPassword([]byte(u.PasswordHash), []byte(m.Password)) != nil {
			s.send(protocol.Message{Type: protocol.TypeError, Error: "密码错误"})
			return false
		}
		s.setName(name)
		s.send(protocol.Message{Type: protocol.TypeSystem, Content: "登录成功，欢迎 " + name, Token: gw.issueToken(name), TS: time.Now().Unix()})
		return true
	}
}

// authenticateToken 处理免密重连：校验 JWT，用 token 里的 sub 作为昵称，随后复用
// handle 的 addSession 上线流程。不查 store、不做 bcrypt——签名与有效期即身份证明
// （无状态鉴权）。成功后 re-issue 一个新 token，实现滑动过期。
func (gw *Gateway) authenticateToken(s *Session, m protocol.Message) bool {
	if m.Token == "" {
		s.send(protocol.Message{Type: protocol.TypeError, Error: "缺少 token"})
		return false
	}
	name, err := gw.auth.Verify(m.Token)
	if err != nil {
		if errors.Is(err, auth.ErrExpired) {
			s.send(protocol.Message{Type: protocol.TypeError, Error: "token 已过期，请重新登录"})
		} else {
			s.send(protocol.Message{Type: protocol.TypeError, Error: "token 无效，请重新登录"})
		}
		return false
	}
	s.setName(name)
	s.send(protocol.Message{Type: protocol.TypeSystem, Content: "已重连，欢迎 " + name, Token: gw.issueToken(name), TS: time.Now().Unix()})
	return true
}

// issueToken 为已通过密码校验的用户签发 JWT。失败时记录日志并返回空串，
// 不让 token 签发失败阻断登录（免密重连签不出 token 时仍可退回密码登录）。
func (gw *Gateway) issueToken(name string) string {
	token, err := gw.auth.Issue(name)
	if err != nil {
		gw.log.Error("issue token failed", zap.Error(err))
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
