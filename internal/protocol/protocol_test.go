package protocol

import (
	"bytes"
	"encoding/binary"
	"io"
	"reflect"
	"strings"
	"testing"
)

// frame 构造一个「长度前缀 + JSON 体」的原始字节序列，用于构造非法/边界输入。
func frame(body []byte) []byte {
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(body)))
	return append(hdr[:], body...)
}

// TestWriteReadFrameRoundTrip 验证正常消息编码后解码能还原（核心正确性）。
func TestWriteReadFrameRoundTrip(t *testing.T) {
	cases := []struct {
		name string
		in   Message
	}{
		{name: "chat", in: Message{Type: TypeChat, From: "alice", Content: "你好，世界", TS: 1234567890}},
		{name: "private", in: Message{Type: TypePrivate, From: "alice", To: "bob", Content: "hello", TS: 42}},
		{name: "auth", in: Message{Type: TypeAuth, Name: "alice", Password: "secret"}},
		{
			name: "history_with_messages",
			in: Message{
				Type:  TypeHistory,
				Limit: 50,
				Messages: []*Message{
					{Type: TypeBroadcast, From: "x", Content: "y", TS: 1},
					{Type: TypePrivate, From: "x", To: "z", Content: "w", TS: 2},
				},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			if err := WriteFrame(&buf, tc.in); err != nil {
				t.Fatalf("WriteFrame: %v", err)
			}
			got, err := ReadFrame(&buf)
			if err != nil {
				t.Fatalf("ReadFrame: %v", err)
			}
			if !reflect.DeepEqual(got, tc.in) {
				t.Fatalf("round-trip mismatch:\n got: %+v\nwant: %+v", got, tc.in)
			}
		})
	}
}

// TestReadFrameInvalidSize 验证非法帧大小（0 或超过上限）会被拒绝。
func TestReadFrameInvalidSize(t *testing.T) {
	cases := []struct {
		name string
		size uint32
	}{
		{name: "zero", size: 0},
		{name: "too_large", size: MaxFrameSize + 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var hdr [4]byte
			binary.BigEndian.PutUint32(hdr[:], tc.size)
			if _, err := ReadFrame(bytes.NewReader(hdr[:])); err == nil {
				t.Fatalf("expected error for size %d, got nil", tc.size)
			}
		})
	}
}

// TestReadFrameMissingType 验证消息体缺 type 字段会被拒绝。
func TestReadFrameMissingType(t *testing.T) {
	if _, err := ReadFrame(bytes.NewReader(frame([]byte(`{"content":"hi"}`)))); err == nil {
		t.Fatal("expected error for missing type, got nil")
	}
}

// TestReadFrameShortBody 验证「帧头声明长度 > 实际 body 长度」（半包）会报错。
func TestReadFrameShortBody(t *testing.T) {
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], 10) // 声称 10 字节
	partial := append(hdr[:], []byte(`{"a":`)...)
	if _, err := ReadFrame(bytes.NewReader(partial)); err != io.ErrUnexpectedEOF {
		t.Fatalf("expected io.ErrUnexpectedEOF, got %v", err)
	}
}

// TestReadFrameMultipleFrames 验证连续两帧（粘包）能被正确拆开各读一条。
func TestReadFrameMultipleFrames(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteFrame(&buf, Message{Type: TypeChat, Content: "first", TS: 1}); err != nil {
		t.Fatal(err)
	}
	if err := WriteFrame(&buf, Message{Type: TypeChat, Content: "second", TS: 2}); err != nil {
		t.Fatal(err)
	}

	first, err := ReadFrame(&buf)
	if err != nil {
		t.Fatalf("read first: %v", err)
	}
	if first.Content != "first" {
		t.Fatalf("first.Content = %q, want %q", first.Content, "first")
	}

	second, err := ReadFrame(&buf)
	if err != nil {
		t.Fatalf("read second: %v", err)
	}
	if second.Content != "second" {
		t.Fatalf("second.Content = %q, want %q", second.Content, "second")
	}
}

// TestWriteFrameTooLarge 验证超上限消息会被 WriteFrame 拒绝。
func TestWriteFrameTooLarge(t *testing.T) {
	m := Message{Type: TypeChat, Content: strings.Repeat("a", MaxFrameSize+1)}
	if err := WriteFrame(io.Discard, m); err == nil {
		t.Fatal("expected error for oversized message, got nil")
	}
}
