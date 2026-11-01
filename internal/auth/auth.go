// Package auth 负责 JWT（无状态 token）的签发与校验。
//
// 为什么引入 JWT：单机版鉴权靠「登录时 bcrypt 校验 + 之后连接一直在线」，
// 隐含「身份 = 这条连接」的假设。分布式下接入节点无状态，客户端重连可能
// 落到任意网关，需要一个不查库、不共享会话就能验身份的凭证——JWT 就是它：
// 登录时签发，之后凭签名验身份，无需每次查库。
//
// 注意 JWT 不替代 bcrypt：bcrypt 仍负责密码哈希（登录时校验密码），
// JWT 是登录成功后签发的会话凭证，两者职责不同、各司其职。
package auth

import (
	"errors"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// 哨兵错误。
var (
	ErrInvalid = errors.New("auth: invalid token")
	ErrExpired = errors.New("auth: token expired")
)

// Manager 用 HMAC-SHA256（对称签名）签发/校验 JWT。
// 对称签名适合单进程签发 + 多节点共享同一密钥的分布式接入层场景；
// 若要支持第三方验签/跨系统，再换非对称（RS256）。
type Manager struct {
	secret []byte
	ttl    time.Duration
}

// New 创建一个 JWT 管理器。secret 是签名密钥，ttl 是 token 有效期。
func New(secret string, ttl time.Duration) *Manager {
	return &Manager{secret: []byte(secret), ttl: ttl}
}

// Issue 签发一个把 name 写入 sub 声明的 JWT，有效期 ttl。
func (m *Manager) Issue(name string) (string, error) {
	now := time.Now()
	claims := jwt.RegisteredClaims{
		Subject:   name,
		IssuedAt:  jwt.NewNumericDate(now),
		ExpiresAt: jwt.NewNumericDate(now.Add(m.ttl)),
	}
	return jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString(m.secret)
}

// Verify 校验 token 签名与有效期，返回其中的 name。
// 签名不符/结构错返回 ErrInvalid，过期返回 ErrExpired。
func (m *Manager) Verify(tokenStr string) (string, error) {
	token, err := jwt.ParseWithClaims(tokenStr, &jwt.RegisteredClaims{}, func(*jwt.Token) (any, error) {
		return m.secret, nil
	})
	if err != nil {
		// jwt/v5 在 ParseWithClaims 里会顺带校验 exp/nbf/iat（RegisteredClaims.Valid），
		// 过期返回 jwt.ErrTokenExpired，这里映射成我们自己的哨兵错误。
		if errors.Is(err, jwt.ErrTokenExpired) {
			return "", ErrExpired
		}
		return "", ErrInvalid
	}
	claims, ok := token.Claims.(*jwt.RegisteredClaims)
	if !ok || !token.Valid {
		return "", ErrInvalid
	}
	return claims.Subject, nil
}
