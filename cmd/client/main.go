// 聊天室客户端入口：连接服务器、登录，进入菜单驱动的主循环。
package main

import (
	"bufio"
	"flag"
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	"communicate/internal/protocol"
)

var serverAddr string

func init() {
	flag.StringVar(&serverAddr, "addr", "127.0.0.1:8888", "服务器地址（默认 127.0.0.1:8888）")
}

// Client 是聊天室客户端。
type Client struct {
	conn net.Conn
	done chan struct{} // 主动退出信号：receive 用它区分主动退出与异常断开
}

func main() {
	flag.Parse()

	conn, err := net.Dial("tcp", serverAddr)
	if err != nil {
		fmt.Println("连接服务器失败:", err)
		return
	}
	defer conn.Close()

	c := &Client{conn: conn}
	c.run()
}

func (c *Client) run() {
	reader := bufio.NewReader(os.Stdin)

	// 登录
	fmt.Print("昵称: ")
	name := readLine(reader)
	fmt.Print("密码: ")
	// 注：V1 用明文读行（回显可见）；可改用 golang.org/x/term 实现密码不回显。
	password := readLine(reader)

	if err := protocol.WriteFrame(c.conn, protocol.Message{Type: protocol.TypeAuth, Name: name, Password: password}); err != nil {
		fmt.Println("发送登录失败:", err)
		return
	}
	resp, err := protocol.ReadFrame(c.conn)
	if err != nil {
		fmt.Println("登录失败:", err)
		return
	}
	if resp.Type == protocol.TypeError {
		fmt.Println("登录失败:", resp.Error)
		return
	}
	fmt.Println(resp.Content)

	c.done = make(chan struct{})

	// 心跳保活 + 接收服务器消息
	go c.heartbeat()
	go c.receive()

	// 主循环：菜单
	for {
		showMenu()
		switch readLine(reader) {
		case "1":
			c.publicChat(reader)
		case "2":
			c.privateChat(reader)
		case "3":
			c.rename(reader)
		case "4":
			c.who()
		case "5":
			c.history()
		case "0":
			fmt.Println("已退出")
			close(c.done) // 通知 receive 这是主动退出
			return
		default:
			fmt.Println("无效选项，请输入 0-5")
		}
	}
}

func showMenu() {
	fmt.Println()
	fmt.Println("===== 菜单 =====")
	fmt.Println("1. 公聊")
	fmt.Println("2. 私聊")
	fmt.Println("3. 改昵称")
	fmt.Println("4. 在线列表")
	fmt.Println("5. 历史消息")
	fmt.Println("0. 退出")
	fmt.Print("请选择: ")
}

// heartbeat 每 10 秒发一次心跳，保持连接活跃并探测服务器存活。
func (c *Client) heartbeat() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		if err := protocol.WriteFrame(c.conn, protocol.Message{Type: protocol.TypeHeartbeat}); err != nil {
			return
		}
	}
}

// receive 持续读取并打印服务器推送的消息。
func (c *Client) receive() {
	for {
		m, err := protocol.ReadFrame(c.conn)
		if err != nil {
			select {
			case <-c.done:
				// 主动退出：静默返回，不打印「连接已断开」
				return
			default:
				fmt.Printf("\n[连接已断开] %v\n", err)
				os.Exit(0)
			}
		}
		c.printMessage(m)
	}
}

func (c *Client) printMessage(m protocol.Message) {
	ts := time.Unix(m.TS, 0).Format("15:04:05")
	switch m.Type {
	case protocol.TypeBroadcast:
		fmt.Printf("[%s] %s: %s\n", ts, m.From, m.Content)
	case protocol.TypePrivate:
		fmt.Printf("[%s] %s 对你说: %s\n", ts, m.From, m.Content)
	case protocol.TypeSystem:
		fmt.Printf("[%s] * %s\n", ts, m.Content)
	case protocol.TypeError:
		fmt.Printf("! 错误: %s\n", m.Error)
	case protocol.TypeWhoList:
		fmt.Printf("[%s] 在线用户(%d): %s\n", ts, len(m.Users), strings.Join(m.Users, ", "))
	case protocol.TypeHistory:
		fmt.Println("===== 历史消息 =====")
		for _, hm := range m.Messages {
			hts := time.Unix(hm.TS, 0).Format("15:04:05")
			if hm.Type == "private" {
				fmt.Printf("[%s] %s -> %s: %s\n", hts, hm.From, hm.To, hm.Content)
			} else {
				fmt.Printf("[%s] %s: %s\n", hts, hm.From, hm.Content)
			}
		}
		fmt.Println("===================")
	case protocol.TypePong:
		// 心跳应答，静默
	default:
		fmt.Printf("[未知消息] %+v\n", m)
	}
}

func (c *Client) publicChat(r *bufio.Reader) {
	fmt.Println("进入公聊，输入内容发送，输入 exit 返回菜单")
	for {
		line := readLine(r)
		if line == "exit" {
			return
		}
		if line == "" {
			continue
		}
		if err := protocol.WriteFrame(c.conn, protocol.Message{Type: protocol.TypeChat, Content: line}); err != nil {
			fmt.Println("发送失败:", err)
			return
		}
	}
}

func (c *Client) privateChat(r *bufio.Reader) {
	fmt.Print("私聊对象昵称: ")
	to := readLine(r)
	if to == "" || to == "exit" {
		return
	}
	fmt.Printf("给 %s 发消息，输入 exit 返回菜单\n", to)
	for {
		line := readLine(r)
		if line == "exit" {
			return
		}
		if line == "" {
			continue
		}
		if err := protocol.WriteFrame(c.conn, protocol.Message{Type: protocol.TypePrivate, To: to, Content: line}); err != nil {
			fmt.Println("发送失败:", err)
			return
		}
	}
}

func (c *Client) rename(r *bufio.Reader) {
	fmt.Print("新昵称: ")
	newName := readLine(r)
	if newName == "" {
		return
	}
	if err := protocol.WriteFrame(c.conn, protocol.Message{Type: protocol.TypeRename, NewName: newName}); err != nil {
		fmt.Println("发送失败:", err)
	}
}

func (c *Client) who() {
	_ = protocol.WriteFrame(c.conn, protocol.Message{Type: protocol.TypeWho})
}

func (c *Client) history() {
	_ = protocol.WriteFrame(c.conn, protocol.Message{Type: protocol.TypeHistory, Limit: 50})
}

// readLine 从标准输入读取一行并去掉行尾换行符。
func readLine(r *bufio.Reader) string {
	line, err := r.ReadString('\n')
	if err != nil {
		return ""
	}
	return strings.TrimRight(line, "\r\n")
}
