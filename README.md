# fsrobot - 飞书群聊机器人接入 Agent Pi

基于 Go 语言开发，通过**飞书 WebSocket 长连接**接入飞书群聊事件，将群成员发送的对话请求路由给本地 **Agent Pi**（`pi` 命令行 Agent）执行，并将回复实时返回群聊。

---

## 特性亮点

1. **长连接免公网 IP**：基于飞书官方 `oapi-sdk-go` WebSocket 模式，不需要公网域名、公网 IP 或内网穿透工具。
2. **多轮对话上下文（Session）**：默认按群聊 ID (`chat_id`) 或私聊用户 ID (`open_id`) 隔离并维护 Pi Agent 的独立会话，支持连续追问。
3. **消息去重机制**：内置 TTL 消息防抖去重，避免飞书网络重推导致的重复调用。
4. **异步非阻塞**：消息接收与 Agent 执行解耦，不阻塞飞书长连接的心跳与事件收取。
5. **支持模型与思考配置**：支持透传 `--model`、`--thinking`、`--append-system-prompt` 等参数。
6. **Token 用量可见**：AGP Agent 在最终回复卡片底部附带本轮 `输入 / 缓存命中 / 输出` 与合计 token，便于成本核对。

---

## 快速开始

### 1. 飞书开放平台配置

1. 登录 [飞书开放平台](https://open.feishu.cn/) 创建企业自建应用。
2. 进入应用，在 **「添加应用能力」** 中添加 **「机器人」**。
3. 在 **「开发配置」 -> 「权限管理」** 申请以下权限：
   - `以应用的身份发消息` (`im:message:send_as_bot`)
   - `获取群聊中所有@机器人的消息` (`im:message.group_at_msg:readonly`)
   - `获取用户发给机器人的单聊消息` (`im:message.p2p_msg:readonly`)
4. 在 **「开发配置」 -> 「事件与回调」**：
   - 接收事件方式选择 **「使用长连接接收事件 (WebSocket)」**
   - 添加事件：**「接收消息 (im.message.receive_v1)」**
5. 在 **「版本管理与发布」** 创建并发布一个版本。
6. 将机器人拉入飞书群聊中。

### 2. 本地配置

复制配置文件模板：
```bash
cp config.example.json config.json
```

编辑 `config.json`：
```json
{
  "app_id": "cli_xxxxxxxxxxxx",
  "app_secret": "yyyyyyyyyyyyyyyyyyyyyyyy",
  "pi_bin": "/opt/homebrew/bin/pi",
  "pi_model": "",
  "pi_thinking": "",
  "pi_work_dir": ".",
  "enable_session": true,
  "session_dir": "./sessions"
}
```

也可以直接通过环境变量注入：
```bash
export FEISHU_APP_ID="cli_xxxxxxxxxxxx"
export FEISHU_APP_SECRET="yyyyyyyyyyyyyyyyyyyyyyyy"
# 可选:
export PI_BIN="/opt/homebrew/bin/pi"
export PI_MODEL="anthropic/claude-3-7-sonnet"
```

### 3. 编译并运行

```bash
# 编译并启动服务
make run
```

控制台看到如下输出即说明启动成功：
```text
Initializing Feishu Robot with Agent Pi...
Starting Feishu Bot (WebSocket mode)... Session enabled: true (dir: ./sessions)
Using Agent Pi binary: /opt/homebrew/bin/pi, Model: ...
```

在飞书群里 `@机器人 你的问题`，机器人就会自动调用 Agent Pi 并给出回复。

---

## 目录结构

```
fsrobot/
├── cmd/
│   └── bot/
│       └── main.go              # 程序入口与生命周期管理
├── internal/
│   ├── agent/
│   │   └── pi.go                # Agent Pi CLI 调用封装（Session/超时/清洗）
│   ├── config/
│   │   └── config.go            # 配置管理（JSON文件与环境变量支持）
│   └── handler/
│       ├── dedupe.go            # 消息防抖去重器
│       └── message.go           # 飞书消息解析与异步回复处理
├── config.example.json          # 配置示例
├── Makefile                     # 构建脚本
└── README.md
```
