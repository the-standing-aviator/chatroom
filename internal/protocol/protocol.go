// Package protocol 定义聊天室的消息帧格式与消息类型。
//
// TCP 是字节流，没有消息边界，因此必须靠帧头来分帧。帧格式：
//
//	[4 字节大端 uint32 = 消息体长度 N][N 字节 JSON 消息体]
//
// 消息体是 JSON，用 type 字段区分消息类型，其余字段按类型按需使用。
package protocol

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

// MaxFrameSize 单帧消息体（JSON）的最大字节数，防止恶意客户端发送超长数据撑爆内存。
const MaxFrameSize = 64 * 1024

// Message 是客户端与服务端之间交换的统一消息结构。
type Message struct {
	Type string `json:"type"`

	// 鉴权（auth）
	Name     string `json:"name,omitempty"`
	Password string `json:"password,omitempty"`
	Token    string `json:"token,omitempty"` // 登录成功后服务端签发的 JWT（V3.3 重连用）

	// 聊天（chat / private / broadcast）
	From    string `json:"from,omitempty"`
	To      string `json:"to,omitempty"`
	Content string `json:"content,omitempty"`

	// 文件消息（file）：只传元数据（file_id/file_name/file_size），
	// 文件字节走独立 HTTP 通道（/upload、/download），不进聊天帧（帧上限 MaxFrameSize=64KB）。
	FileID   string `json:"file_id,omitempty"`
	FileName string `json:"file_name,omitempty"`
	FileSize int64  `json:"file_size,omitempty"`

	// 时间戳，Unix 秒
	TS int64 `json:"ts,omitempty"`

	// 消息 id（仅 history 响应携带，用于前端「上滑加载更早」的分页游标）
	ID int64 `json:"id,omitempty"`

	// 改名（rename）
	NewName string `json:"new_name,omitempty"`

	// 在线列表（who_list）
	Users []string `json:"users,omitempty"`

	// 历史消息（history）
	Limit    int        `json:"limit,omitempty"`
	Peer     string     `json:"peer,omitempty"`      // 指定会话：空=公聊，否则=私聊对象昵称
	BeforeID int64      `json:"before_id,omitempty"` // 分页游标：0=取最新，>0=取 id 更早的消息
	Messages []*Message `json:"messages,omitempty"`

	// 错误提示（error）
	Error string `json:"error,omitempty"`
}

// 客户端 → 服务端的消息类型。
const (
	TypeAuth      = "auth"       // 登录/注册：Name + Password
	TypeAuthToken = "auth_token" // 免密重连：Token（身份以 token 的 sub 为准）
	TypeChat      = "chat"       // 公聊：Content
	TypeFile      = "file"       // 文件消息：FileID + FileName + FileSize（字节走 HTTP）
	TypePrivate   = "private"    // 私聊：To + Content
	TypeRename    = "rename"     // 改名：NewName
	TypeWho       = "who"        // 查看在线列表
	TypeHistory   = "history"    // 查看历史消息：Limit
	TypeHeartbeat = "heartbeat"  // 心跳保活
)

// 服务端 → 客户端的消息类型。
const (
	TypeBroadcast = "broadcast" // 公聊广播：From + Content + TS
	TypeSystem    = "system"    // 系统提示（上下线等）：Content + TS
	TypeError     = "error"     // 错误提示：Error
	TypePong      = "pong"      // 心跳应答
	TypeWhoList   = "who_list"  // 在线列表：Users
)

// WriteFrame 把一条消息编码为「长度前缀 + JSON」的帧写入 w。
func WriteFrame(w io.Writer, m Message) error {
	body, err := Marshal(m)
	if err != nil {
		return err
	}

	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(body)))
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	if _, err := w.Write(body); err != nil {
		return err
	}
	return nil
}

// ReadFrame 从 r 读出一个完整帧并解析为 Message。
func ReadFrame(r io.Reader) (Message, error) {
	var hdr [4]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return Message{}, err
	}
	n := binary.BigEndian.Uint32(hdr[:])
	if n == 0 || n > MaxFrameSize {
		return Message{}, fmt.Errorf("invalid frame size: %d", n)
	}

	body := make([]byte, n)
	if _, err := io.ReadFull(r, body); err != nil {
		return Message{}, err
	}
	return Unmarshal(body)
}

// Marshal 把消息编码为 JSON 字节，超出 MaxFrameSize 返回错误。
// TCP 帧复用它（再加长度前缀），WebSocket 文本帧也直接拿它当 payload——
// 两种传输共用同一份编解码与大小/type 校验，逻辑不重复。
func Marshal(m Message) ([]byte, error) {
	body, err := json.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("marshal message: %w", err)
	}
	if len(body) > MaxFrameSize {
		return nil, errors.New("message too large")
	}
	return body, nil
}

// Unmarshal 从 JSON 字节解析出消息，校验 type 非空。
func Unmarshal(data []byte) (Message, error) {
	var m Message
	if err := json.Unmarshal(data, &m); err != nil {
		return Message{}, fmt.Errorf("unmarshal message: %w", err)
	}
	if m.Type == "" {
		return Message{}, errors.New("message missing type")
	}
	return m, nil
}
