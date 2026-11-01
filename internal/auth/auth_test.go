package auth

import (
	"errors"
	"testing"
	"time"
)

// TestIssueAndVerify 验证签发 → 校验的往返：拿回原 name。
func TestIssueAndVerify(t *testing.T) {
	m := New("test-secret", time.Hour)
	token, err := m.Issue("alice")
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}
	name, err := m.Verify(token)
	if err != nil || name != "alice" {
		t.Fatalf("Verify = %q, %v; want (alice, nil)", name, err)
	}
}

// TestVerifyTampered 验证篡改签名后被拒绝（改末位字符，保证内容确实变了）。
func TestVerifyTampered(t *testing.T) {
	m := New("test-secret", time.Hour)
	token, _ := m.Issue("alice")

	last := token[len(token)-1]
	c := byte('x')
	if last == c {
		c = 'y'
	}
	tampered := token[:len(token)-1] + string(c)

	if _, err := m.Verify(tampered); !errors.Is(err, ErrInvalid) {
		t.Fatalf("Verify(tampered) = %v, want ErrInvalid", err)
	}
}

// TestVerifyWrongSecret 验证用错误密钥验签被拒绝（对称签名的关键性质）。
func TestVerifyWrongSecret(t *testing.T) {
	m1 := New("secret-1", time.Hour)
	m2 := New("secret-2", time.Hour)
	token, _ := m1.Issue("alice")
	if _, err := m2.Verify(token); !errors.Is(err, ErrInvalid) {
		t.Fatalf("Verify(wrong secret) = %v, want ErrInvalid", err)
	}
}

// TestVerifyExpired 验证过期 token 被拒绝并返回 ErrExpired。
func TestVerifyExpired(t *testing.T) {
	m := New("test-secret", -time.Second) // 负 ttl → 签发即过期
	token, err := m.Issue("alice")
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}
	if _, err := m.Verify(token); !errors.Is(err, ErrExpired) {
		t.Fatalf("Verify(expired) = %v, want ErrExpired", err)
	}
}

// TestVerifyGarbage 验证非 JWT 字符串被拒绝。
func TestVerifyGarbage(t *testing.T) {
	m := New("test-secret", time.Hour)
	if _, err := m.Verify("not-a-jwt"); !errors.Is(err, ErrInvalid) {
		t.Fatalf("Verify(garbage) = %v, want ErrInvalid", err)
	}
}
