# 部署手册（公网上线最小可用）

把 communicate 部署到一台公网服务器：gateway 作为 systemd 服务跑在本机回环，
nginx（或 Caddy）做 **TLS 终止 + WebSocket 反向代理**，浏览器经 `https` / `wss` 接入。

```
 浏览器 ──wss://──▶ nginx/Caddy (443, TLS) ──▶ gateway -ws-addr 127.0.0.1:8890
                                                └─ TCP 终端客户端 -addr 127.0.0.1:8888（默认不对外）
```

## 前置

- 一台 Linux 服务器（示例用 Ubuntu 24.04），有公网 IP，域名 `chat.example.com` 的 A 记录指向它。
- 服务器装好 Go（构建用）与 nginx（或 Caddy）。

## 1. 构建并安装二进制

```bash
cd /home/never/go-projects/communicate
/usr/local/go/bin/go build -o chat-gateway ./cmd/gateway
sudo install -m 0755 chat-gateway /opt/communicate/chat-gateway
```

## 2. 建运行用户与目录

```bash
sudo useradd --system --no-create-home --shell /usr/sbin/nologin communicate
sudo mkdir -p /opt/communicate /etc/communicate
sudo chown communicate:communicate /opt/communicate
```

## 3. 配置密钥（环境变量，不进命令行）

```bash
# 生成一个长随机 JWT 密钥
JWT=$(openssl rand -hex 32)
sudo tee /etc/communicate/gateway.env >/dev/null <<EOF
COMMUNICATE_JWT_SECRET=$JWT
EOF
sudo chmod 600 /etc/communicate/gateway.env
```

> 分布式（`-store postgres`）才需要再追加 `COMMUNICATE_DSN=postgres://chat:密码@127.0.0.1:5432/chat?sslmode=disable`。

## 4. 安装 systemd 服务

```bash
sudo cp deploy/gateway.service /etc/systemd/system/communicate-gateway.service
sudo systemctl daemon-reload
sudo systemctl enable --now communicate-gateway
sudo systemctl status communicate-gateway   # 确认 active (running)
```

看日志：`journalctl -u communicate-gateway -f`。

## 5. 反向代理 + TLS

**nginx**：

```bash
# 先把 deploy/nginx.conf 里的 server_name 换成你的域名
sudo cp deploy/nginx.conf /etc/nginx/sites-available/communicate
sudo ln -s /etc/nginx/sites-available/communicate /etc/nginx/sites-enabled/
sudo certbot --nginx -d chat.example.com    # 自动签发并配置证书，含自动续期
sudo systemctl reload nginx
```

**Caddy**（更省心，自动 HTTPS）：

```bash
sudo caddy run --config deploy/Caddyfile
```

## 6. 验证

```bash
# 浏览器打开 https://chat.example.com —— 登录、发消息、看在线列表
# 健康检查（本机）：
curl -fsS http://127.0.0.1:9090/metrics | grep chat_online
```

## 上线前检查清单

- [ ] `COMMUNICATE_JWT_SECRET` 已改成随机长串（不是默认 `dev-secret-change-me`）
- [ ] `-allowed-origins` 已改成真实域名 `https://chat.example.com`
- [ ] 证书已签发且自动续期（`certbot renew` 或 Caddy 自动）
- [ ] 防火墙只开 80/443；`-metrics`(9090) 与 TCP `-addr`(8888) 不对外
- [ ] systemd 已 enable，重启机器后自动拉起

## 说明

- **为什么 gateway 监听回环、由反代对外**：TLS 证书管理与续期交给成熟的 nginx/Caddy，
  服务本身保持纯 HTTP/WS；后面加多节点时，反代还能做负载分发。
- **为什么密钥走环境变量**：命令行参数会被 `ps aux` 看见；`EnvironmentFile` 注入后
  只有 `communicate` 用户能读到该文件。
- **TCP 终端端口（8888）默认不对外**：公网主客户端是浏览器（wss）；若要对外开终端
  TCP 客户端，再在前面加一层 TCP LB（`cmd/lb`）或直接映射端口（不推荐，先上 LB）。
