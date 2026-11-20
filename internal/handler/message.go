package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"regexp"
	"strconv"
	"strings"
	"time"

	lark "github.com/larksuite/oapi-sdk-go/v3"
	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"

	"fsrobot/internal/registry"
)

var mentionRegexp = regexp.MustCompile(`@_user_\d+`)

// MessageHandler handles incoming Feishu message events and routes them via AgentRegistry.
type MessageHandler struct {
	larkClient *lark.Client
	registry   *registry.Registry
	sessions   *SessionManager
	dedupe     *Deduplicator
	botOpenID  string
	requireAt  bool
}

// NewMessageHandler creates a new MessageHandler backed by an AgentRegistry.
func NewMessageHandler(client *lark.Client, reg *registry.Registry, botOpenID string, requireAt bool) *MessageHandler {
	return &MessageHandler{
		larkClient: client,
		registry:   reg,
		sessions:   NewSessionManager(),
		dedupe:     NewDeduplicator(10 * time.Minute),
		botOpenID:  botOpenID,
		requireAt:  requireAt,
	}
}

type textContent struct {
	Text string `json:"text"`
}

// HandleMessageReceive processes im.message.receive_v1 events.
func (h *MessageHandler) HandleMessageReceive(ctx context.Context, event *larkim.P2MessageReceiveV1) error {
	if event == nil || event.Event == nil || event.Event.Message == nil {
		return nil
	}

	msg := event.Event.Message
	msgID := ""
	if msg.MessageId != nil {
		msgID = *msg.MessageId
	}
	if msgID == "" {
		return nil
	}

	if h.dedupe.CheckAndSet(msgID) {
		log.Printf("[Handler] Duplicate message ignored: %s", msgID)
		return nil
	}

	chatType := ""
	if msg.ChatType != nil {
		chatType = *msg.ChatType
	}

	// In group chats, filter by @mention only when require_at is enabled.
	if chatType == "group" && h.requireAt && !h.isBotMentioned(msg) {
		return nil
	}

	msgType := ""
	if msg.MessageType != nil {
		msgType = *msg.MessageType
	}

	chatID := ""
	if msg.ChatId != nil {
		chatID = *msg.ChatId
	}
	senderID := ""
	if event.Event.Sender != nil && event.Event.Sender.SenderId != nil && event.Event.Sender.SenderId.OpenId != nil {
		senderID = *event.Event.Sender.SenderId.OpenId
	}

	sessionID := chatID
	if sessionID == "" {
		sessionID = senderID
	}

	rawContent := ""
	if msg.Content != nil {
		rawContent = *msg.Content
	}

	switch msgType {
	case "text":
		var tc textContent
		if err := json.Unmarshal([]byte(rawContent), &tc); err != nil {
			log.Printf("[Handler] Failed to parse message content JSON: %v", err)
			return nil
		}
		prompt := strings.TrimSpace(mentionRegexp.ReplaceAllString(tc.Text, ""))
		log.Printf("[Handler] Received text message chat=%s(%s) sender=%s: %q", chatID, chatType, senderID, prompt)
		go h.processAndReply(context.Background(), msgID, sessionID, chatID, senderID, prompt)

	case "image":
		imgKey, err := parseImageContent(rawContent)
		if err != nil {
			log.Printf("[Handler] Failed to parse image content: %v", err)
			return nil
		}
		log.Printf("[Handler] Received image message chat=%s(%s) sender=%s key=%s", chatID, chatType, senderID, imgKey)
		go h.processMediaAndReply(context.Background(), msgID, sessionID, chatID, senderID, "", []string{imgKey})

	case "post":
		text, imgKeys, err := parsePostContent(rawContent)
		if err != nil {
			log.Printf("[Handler] Failed to parse post content: %v", err)
			return nil
		}
		log.Printf("[Handler] Received post message chat=%s(%s) sender=%s text=%q images=%d", chatID, chatType, senderID, text, len(imgKeys))
		go h.processMediaAndReply(context.Background(), msgID, sessionID, chatID, senderID, text, imgKeys)

	default:
		log.Printf("[Handler] Unsupported message type=%s, ignoring", msgType)
	}
	return nil
}

func (h *MessageHandler) processMediaAndReply(ctx context.Context, msgID, sessionID, chatID, senderID, text string, imgKeys []string) {
	if len(imgKeys) == 0 {
		h.processAndReply(ctx, msgID, sessionID, chatID, senderID, text)
		return
	}

	var savedPaths []string
	for _, key := range imgKeys {
		path, err := downloadFeishuImage(ctx, h.larkClient, msgID, key)
		if err != nil {
			log.Printf("[Handler] Failed to download image %s: %v", key, err)
			continue
		}
		savedPaths = append(savedPaths, path)
	}

	if len(savedPaths) == 0 {
		if text == "" {
			h.replyCard(ctx, msgID, "❌ 图片下载失败", "red", "无法从飞书拉取该图片，请检查应用是否具备图片资源读取权限（`im:resource` 或 `im:message`）。")
			return
		}
		h.processAndReply(ctx, msgID, sessionID, chatID, senderID, text)
		return
	}

	var sb strings.Builder
	if text != "" {
		sb.WriteString(text)
		sb.WriteString("\n\n")
	} else {
		sb.WriteString("请查看并分析用户发送的以下图片内容，结合图片信息回复用户：\n")
	}
	sb.WriteString("[附带图片（请使用 read 工具读取以下本地路径查看图片）]\n")
	for _, p := range savedPaths {
		sb.WriteString("- ")
		sb.WriteString(p)
		sb.WriteString("\n")
	}

	h.processAndReply(ctx, msgID, sessionID, chatID, senderID, strings.TrimSpace(sb.String()))
}

func (h *MessageHandler) isBotMentioned(msg *larkim.EventMessage) bool {
	if len(msg.Mentions) == 0 {
		return false
	}
	if h.botOpenID == "" {
		// Fallback if bot OpenID wasn't fetched at startup
		return true
	}
	for _, m := range msg.Mentions {
		if m != nil && m.Id != nil && m.Id.OpenId != nil && *m.Id.OpenId == h.botOpenID {
			return true
		}
	}
	return false
}

func (h *MessageHandler) resolveProvider(chatID, prompt string) (registry.AgentProvider, string, bool) {
	// Support one-shot routing: "#gp 你的问题"
	if strings.HasPrefix(prompt, "#") {
		fields := strings.SplitN(prompt[1:], " ", 2)
		candidateID := strings.TrimSpace(fields[0])
		if p, ok := h.registry.Get(candidateID); ok {
			rest := ""
			if len(fields) > 1 {
				rest = strings.TrimSpace(fields[1])
			}
			return p, rest, true
		}
	}

	// Check chat-bound agent
	if boundID := h.sessions.GetAgentID(chatID); boundID != "" {
		if p, ok := h.registry.Get(boundID); ok {
			return p, prompt, false
		}
	}

	// Fallback to default
	p, ok := h.registry.Default()
	if !ok {
		return nil, prompt, false
	}
	return p, prompt, false
}

func (h *MessageHandler) processAndReply(ctx context.Context, msgID, sessionID, chatID, senderID, prompt string) {
	if prompt == "" {
		h.replyCard(ctx, msgID, "✨ 飞书智能助手", "blue", "你好！我是飞书智能助手，已接入 **Agent 注册中心**。\n发送 `/help` 可查看可用指令与已注册的 Agent。")
		return
	}

	// 1. Handle built-in slash commands (executed immediately without waiting in queue)
	if strings.HasPrefix(prompt, "/") {
		if handled := h.handleCommand(ctx, msgID, sessionID, chatID, prompt); handled {
			return
		}
	}

	// 2. Resolve target AgentProvider
	provider, cleanPrompt, _ := h.resolveProvider(chatID, prompt)
	if provider == nil {
		h.replyCard(ctx, msgID, "❌ 提示", "red", "当前没有可用的已注册 Agent，请检查服务配置。")
		return
	}
	if cleanPrompt == "" {
		h.replyCard(ctx, msgID, "💡 提示", "blue", fmt.Sprintf("请在 `#%s` 后输入具体问题。", provider.Meta().ID))
		return
	}

	meta := provider.Meta()

	// React to the user's message immediately so they know it was received and is being handled.
	go h.addReaction(ctx, msgID, "OnIt")

	// 3. Use primary session if idle; otherwise spawn an isolated parallel sub-session immediately
	lockKey := fmt.Sprintf("%s:%s", meta.ID, sessionID)
	lk := h.sessions.SessionLock(lockKey)
	effSessionID := sessionID
	isParallel := false
	statusTitle := fmt.Sprintf("✨ %s (%s)", meta.Name, meta.ID)

	if lk.TryLock() {
		defer lk.Unlock()
	} else {
		isParallel = true
		effSessionID = fmt.Sprintf("%s:sub:%s", sessionID, shortSuffix(msgID, 8))
		statusTitle = fmt.Sprintf("⚡ 并行子任务 · %s (%s)", meta.Name, meta.ID)
	}

	runCtx, cancel := context.WithCancel(ctx)
	h.sessions.StartRun(lockKey, msgID, effSessionID, cleanPrompt, isParallel, cancel)
	defer func() {
		cancel()
		h.sessions.EndRun(lockKey, msgID)
	}()

	log.Printf("[Handler] Dispatching to agent=%s session=%s (parallel=%v)", meta.ID, effSessionID, isParallel)

	req := registry.RunRequest{
		SessionID: effSessionID,
		Prompt:    cleanPrompt,
		Metadata: map[string]string{
			"chat_id":   chatID,
			"sender_id": senderID,
			"msg_id":    msgID,
		},
	}

	evCh, err := provider.Execute(runCtx, req)
	if err != nil {
		h.replyCard(ctx, msgID, fmt.Sprintf("❌ %s 启动失败", meta.ID), "red", fmt.Sprintf("```text\n%v\n```", err))
		return
	}

	statusCardID := ""
	if meta.Streaming {
		initText := "⏳ 正在思考并处理中..."
		if isParallel {
			initText = "⚡ 主任务仍在执行，已为您开启**并行子任务**处理中..."
		}
		statusCardID = h.replyCard(ctx, msgID, statusTitle, "wathet", initText)
	}

	var deltaBuf strings.Builder
	var finalReply string
	var execErr error
	var runUsage *registry.Usage
	var activeSubagents []string
	lastPatchAt := time.Now()

	updateStatusCard := func(force bool, statusLine string) {
		if statusCardID == "" {
			return
		}
		if !force && time.Since(lastPatchAt) < 1500*time.Millisecond {
			return
		}
		lastPatchAt = time.Now()
		var body strings.Builder
		if statusLine != "" {
			body.WriteString(statusLine)
			body.WriteString("\n\n")
		}
		if len(activeSubagents) > 0 {
			body.WriteString("**子 Agent 动态**：\n")
			for _, s := range activeSubagents {
				body.WriteString("- ")
				body.WriteString(s)
				body.WriteString("\n")
			}
			body.WriteString("\n")
		}
		if deltaBuf.Len() > 0 {
			body.WriteString(deltaBuf.String())
		} else if statusLine == "" && len(activeSubagents) == 0 {
			body.WriteString("⏳ 正在思考并处理中...")
		}
		h.patchCard(ctx, statusCardID, statusTitle, "wathet", strings.TrimSpace(body.String()))
	}

	for ev := range evCh {
		if ev.Usage != nil {
			u := *ev.Usage
			runUsage = &u
		}
		switch ev.Type {
		case registry.EventToolStart:
			phase := fmt.Sprintf("🔧 正在调用工具：`%s`", ev.Tool)
			h.sessions.UpdatePhase(lockKey, msgID, phase)
			updateStatusCard(false, phase)
		case registry.EventSubagentStart:
			phase := fmt.Sprintf("🚀 子 Agent `%s` 运行中...", ev.Agent)
			h.sessions.UpdatePhase(lockKey, msgID, phase)
			activeSubagents = append(activeSubagents, fmt.Sprintf("🚀 `%s` 运行中...", ev.Agent))
			updateStatusCard(true, fmt.Sprintf("🚀 已派发子 Agent：`%s`", ev.Agent))
		case registry.EventSubagentEnd:
			// Update status summary
			for i, s := range activeSubagents {
				if strings.Contains(s, ev.Agent) {
					if ev.Err != nil {
						activeSubagents[i] = fmt.Sprintf("❌ `%s` 失败 (%s)", ev.Agent, ev.Summary)
					} else {
						activeSubagents[i] = fmt.Sprintf("✅ `%s` 完成 (%s)", ev.Agent, ev.Summary)
					}
					break
				}
			}
			h.sessions.UpdatePhase(lockKey, msgID, fmt.Sprintf("✅ 子 Agent `%s` 已完成，继续调度中...", ev.Agent))
			// Send each subagent's output as a dedicated card to Feishu immediately!
			subTitle := fmt.Sprintf("⚡ 子 Agent：%s", ev.Agent)
			if ev.Summary != "" {
				subTitle = fmt.Sprintf("⚡ 子 Agent：%s (%s)", ev.Agent, ev.Summary)
			}
			if ev.Err != nil {
				h.replyCard(ctx, msgID, subTitle, "red", fmt.Sprintf("```text\n%v\n```", ev.Err))
			} else if strings.TrimSpace(ev.Content) != "" {
				cleanedSub, subImgs := prepareCardImages(ctx, h.larkClient, meta.WorkDir, ev.Content)
				h.replyCardWithImages(ctx, msgID, subTitle, "turquoise", cleanedSub, subImgs)
			}
			updateStatusCard(true, "")
		case registry.EventDelta:
			if deltaBuf.Len() == 0 {
				h.sessions.UpdatePhase(lockKey, msgID, "✍️ 正在生成回复...")
			}
			deltaBuf.WriteString(ev.Content)
			updateStatusCard(false, "")
		case registry.EventFinal:
			finalReply = ev.Content
		case registry.EventError:
			execErr = ev.Err
		}
	}

	if execErr != nil {
		if runCtx.Err() == context.Canceled {
			if statusCardID != "" {
				h.patchCard(ctx, statusCardID, statusTitle, "grey", fmt.Sprintf("⏹️ **[%s]** 当前任务已被中止。", meta.ID))
			} else {
				h.replyCard(ctx, msgID, "", "", fmt.Sprintf("⏹️ **[%s]** 当前任务已被中止。", meta.ID))
			}
			return
		}
		log.Printf("[Handler] Agent %s failed for msgID=%s: %v", meta.ID, msgID, execErr)
		errText := fmt.Sprintf("```text\n%v\n```", execErr)
		if partial := strings.TrimSpace(deltaBuf.String()); partial != "" {
			errText = fmt.Sprintf("%s\n\n---\n⚠️ **任务中断（已保留以上部分输出）**：`%v`", partial, execErr)
		}
		if statusCardID != "" {
			h.patchCard(ctx, statusCardID, fmt.Sprintf("❌ %s 处理出错", meta.ID), "red", errText)
		} else {
			h.replyCard(ctx, msgID, fmt.Sprintf("❌ %s 处理出错", meta.ID), "red", errText)
		}
		return
	}

	if strings.TrimSpace(finalReply) == "" {
		finalReply = deltaBuf.String()
	}
	if strings.TrimSpace(finalReply) == "" {
		finalReply = fmt.Sprintf("（**[%s]** 返回了空内容）", meta.ID)
	}

	cleanedReply, imgKeys := prepareCardImages(ctx, h.larkClient, meta.WorkDir, finalReply)
	if cleanedReply == "" && len(imgKeys) > 0 {
		cleanedReply = "🖼️ 已生成图片如下："
	}
	cleanedReply = appendUsageFooter(cleanedReply, runUsage)

	if statusCardID != "" && len(activeSubagents) > 0 {
		var summary strings.Builder
		summary.WriteString("✅ **所有子 Agent 执行完毕**（总结见下方卡片）：\n")
		for _, s := range activeSubagents {
			summary.WriteString("- ")
			summary.WriteString(s)
			summary.WriteString("\n")
		}
		h.patchCard(ctx, statusCardID, statusTitle, "green", strings.TrimSpace(summary.String()))
		h.replyCardWithImages(ctx, msgID, statusTitle, "blue", cleanedReply, imgKeys)
	} else if statusCardID != "" {
		h.patchCardWithImages(ctx, statusCardID, statusTitle, "blue", cleanedReply, imgKeys)
	} else {
		h.replyCardWithImages(ctx, msgID, statusTitle, "blue", cleanedReply, imgKeys)
	}
}

func (h *MessageHandler) handleCommand(ctx context.Context, msgID, sessionID, chatID, cmdLine string) bool {
	parts := strings.Fields(cmdLine)
	if len(parts) == 0 {
		return false
	}
	cmd := strings.ToLower(parts[0])

	switch cmd {
	case "/help":
		h.replyCard(ctx, msgID, "📖 可用指令", "wathet", strings.Join([]string{
			"- `/agents` - 查看所有已注册的 Agent 及当前群绑定",
			"- `/use <agent_id>` - 切换当前群默认使用的 Agent",
			"- `/status` - 查看当前群使用的 Agent 及运行状态",
			"- `/stop` - 中止当前群正在执行的 Agent 任务",
			"- `/new` - 重置当前群与当前 Agent 的对话上下文",
			"- `#<agent_id> <问题>` - 单次临时指定某个 Agent 回答",
		}, "\n"))
		return true

	case "/agents":
		list := h.registry.List()
		if len(list) == 0 {
			h.replyCard(ctx, msgID, "✨ Agent 列表", "grey", "当前没有已注册的 Agent。")
			return true
		}
		activeID := h.sessions.GetAgentID(chatID)
		if activeID == "" {
			if def, ok := h.registry.Default(); ok {
				activeID = def.Meta().ID
			}
		}

		var sb strings.Builder
		for _, m := range list {
			mark := "⚪"
			if m.ID == activeID {
				mark = "✅"
			}
			streamTag := ""
			if m.Streaming {
				streamTag = " `[流式]`"
			}
			sb.WriteString(fmt.Sprintf("%s **`%s`** (%s)%s\n> %s\n\n", mark, m.ID, m.Name, streamTag, m.Description))
		}
		sb.WriteString("使用 `/use <agent_id>` 切换当前群默认 Agent。")
		h.replyCard(ctx, msgID, "✨ 已注册的 Agent 列表", "blue", sb.String())
		return true

	case "/status":
		provider, _, _ := h.resolveProvider(chatID, "")
		if provider == nil {
			h.replyCard(ctx, msgID, "📊 运行状态", "grey", "当前没有可用的 Agent。")
			return true
		}
		meta := provider.Meta()
		lockKey := fmt.Sprintf("%s:%s", meta.ID, sessionID)
		active := h.sessions.ListActive(lockKey)
		if len(active) == 0 {
			h.replyCard(ctx, msgID, "📊 当前状态", "green", fmt.Sprintf("**当前 Agent**：`%s` (%s)\n**运行状态**：✅ 空闲中", meta.ID, meta.Name))
			return true
		}

		var sb strings.Builder
		sb.WriteString(fmt.Sprintf("**当前 Agent**：`%s` (%s)\n**运行中任务数**：`%d`\n\n", meta.ID, meta.Name, len(active)))
		for i, st := range active {
			elapsed := time.Since(st.StartedAt).Round(time.Second)
			kind := "主会话"
			if st.IsParallel {
				kind = "⚡ 并行子任务"
			}
			summary := truncateRunes(strings.ReplaceAll(st.Prompt, "\n", " "), 80)
			sb.WriteString(fmt.Sprintf("%d. **[%s]**（已耗时 `%s`）\n   - 阶段：%s\n   - 摘要：%s\n\n", i+1, kind, elapsed, st.Phase, summary))
		}
		sb.WriteString("发送 `/stop` 可中止所有正在执行的任务。")
		h.replyCard(ctx, msgID, "📊 当前状态", "orange", strings.TrimSpace(sb.String()))
		return true

	case "/stop":
		provider, _, _ := h.resolveProvider(chatID, "")
		if provider == nil {
			h.replyCard(ctx, msgID, "⏹️ 中止任务", "grey", "当前没有可用的 Agent。")
			return true
		}
		lockKey := fmt.Sprintf("%s:%s", provider.Meta().ID, sessionID)
		if count := h.sessions.CancelAll(lockKey); count > 0 {
			h.replyCard(ctx, msgID, "", "", fmt.Sprintf("正在中止 **[%s]** 的 %d 个运行中任务...", provider.Meta().ID, count))
		} else {
			h.replyCard(ctx, msgID, "", "", fmt.Sprintf("**[%s]** 当前没有正在执行的任务。", provider.Meta().ID))
		}
		return true

	case "/use":
		if len(parts) < 2 {
			h.replyCard(ctx, msgID, "", "", "请指定要切换的 Agent ID，例如：`/use gp` 或 `/use pi`")
			return true
		}
		targetID := parts[1]
		p, ok := h.registry.Get(targetID)
		if !ok {
			h.replyCard(ctx, msgID, "", "", fmt.Sprintf("未找到名为 `%s` 的 Agent，发送 `/agents` 查看可用列表。", targetID))
			return true
		}
		h.sessions.SetAgentID(chatID, targetID)
		h.replyCard(ctx, msgID, "✅ 已切换 Agent", "green", fmt.Sprintf("已将当前会话切换至 **`%s`** (%s)。", p.Meta().ID, p.Meta().Name))
		return true

	case "/new":
		provider, _, _ := h.resolveProvider(chatID, "")
		if provider == nil {
			h.replyCard(ctx, msgID, "", "", "当前没有可用的 Agent。")
			return true
		}
		if err := provider.ResetSession(ctx, sessionID); err != nil {
			h.replyCard(ctx, msgID, "❌ 重置失败", "red", fmt.Sprintf("重置 **[%s]** 会话失败：%v", provider.Meta().ID, err))
			return true
		}
		h.replyCard(ctx, msgID, "🔄 会话已重置", "green", fmt.Sprintf("已重置 **[%s]** 的当前会话上下文，可以开始新话题了。", provider.Meta().ID))
		return true

	default:
		return false
	}
}

func buildCardJSON(title, template, markdownContent string, imageKeys []string) (string, error) {
	chunks := splitMarkdownChunks(markdownContent, 12000)
	elements := make([]map[string]any, 0, len(chunks)+len(imageKeys))
	for _, chunk := range chunks {
		if strings.TrimSpace(chunk) == "" {
			continue
		}
		elements = append(elements, map[string]any{
			"tag":     "markdown",
			"content": chunk,
		})
	}
	for _, key := range imageKeys {
		elements = append(elements, map[string]any{
			"tag":     "img",
			"img_key": key,
			"alt": map[string]any{
				"tag":     "plain_text",
				"content": "图片",
			},
		})
	}

	card := map[string]any{
		"schema": "2.0",
		"config": map[string]any{
			"update_multi": true,
		},
		"body": map[string]any{
			"elements": elements,
		},
	}

	if title != "" {
		if template == "" {
			template = "blue"
		}
		card["header"] = map[string]any{
			"template": template,
			"title": map[string]any{
				"tag":     "plain_text",
				"content": title,
			},
		}
	}

	cardBytes, err := json.Marshal(card)
	if err != nil {
		return "", err
	}
	return string(cardBytes), nil
}

func (h *MessageHandler) replyCard(ctx context.Context, msgID, title, template, markdownContent string) string {
	return h.replyCardWithImages(ctx, msgID, title, template, markdownContent, nil)
}

func (h *MessageHandler) replyCardWithImages(ctx context.Context, msgID, title, template, markdownContent string, imageKeys []string) string {
	cardStr, err := buildCardJSON(title, template, markdownContent, imageKeys)
	if err != nil {
		log.Printf("[Handler] Failed to marshal card JSON: %v", err)
		h.replyText(ctx, msgID, markdownContent)
		return ""
	}

	req := larkim.NewReplyMessageReqBuilder().
		MessageId(msgID).
		Body(larkim.NewReplyMessageReqBodyBuilder().
			MsgType(larkim.MsgTypeInteractive).
			Content(cardStr).
			Build()).
		Build()

	resp, err := h.larkClient.Im.Message.Reply(ctx, req)
	if err != nil {
		log.Printf("[Handler] Failed to reply card to message %s: %v (falling back to text)", msgID, err)
		h.replyText(ctx, msgID, markdownContent)
		return ""
	}

	if !resp.Success() {
		log.Printf("[Handler] Feishu card reply error code=%d msg=%s reqId=%s (falling back to text)", resp.Code, resp.Msg, resp.RequestId())
		h.replyText(ctx, msgID, markdownContent)
		return ""
	}

	log.Printf("[Handler] Successfully replied card to message %s", msgID)
	if resp.Data != nil && resp.Data.MessageId != nil {
		return *resp.Data.MessageId
	}
	return ""
}

func (h *MessageHandler) patchCard(ctx context.Context, cardMsgID, title, template, markdownContent string) {
	h.patchCardWithImages(ctx, cardMsgID, title, template, markdownContent, nil)
}

func (h *MessageHandler) patchCardWithImages(ctx context.Context, cardMsgID, title, template, markdownContent string, imageKeys []string) {
	if cardMsgID == "" {
		return
	}
	cardStr, err := buildCardJSON(title, template, markdownContent, imageKeys)
	if err != nil {
		log.Printf("[Handler] Failed to marshal patch card JSON: %v", err)
		return
	}

	req := larkim.NewPatchMessageReqBuilder().
		MessageId(cardMsgID).
		Body(larkim.NewPatchMessageReqBodyBuilder().
			Content(cardStr).
			Build()).
		Build()

	resp, err := h.larkClient.Im.Message.Patch(ctx, req)
	if err != nil {
		log.Printf("[Handler] Failed to patch card %s: %v", cardMsgID, err)
		return
	}
	if !resp.Success() {
		log.Printf("[Handler] Feishu card patch error code=%d msg=%s reqId=%s", resp.Code, resp.Msg, resp.RequestId())
		return
	}
	log.Printf("[Handler] Successfully patched card %s", cardMsgID)
}

func splitMarkdownChunks(s string, maxLen int) []string {
	if len(s) <= maxLen {
		return []string{s}
	}
	var chunks []string
	runes := []rune(s)
	for len(runes) > 0 {
		n := maxLen
		if n > len(runes) {
			n = len(runes)
		}
		chunks = append(chunks, string(runes[:n]))
		runes = runes[n:]
	}
	return chunks
}

// appendUsageFooter 在回复正文末尾追加一行 token 用量（无数据则原样返回）。
func appendUsageFooter(content string, u *registry.Usage) string {
	if u == nil || (u.InputTokens == 0 && u.OutputTokens == 0) {
		return content
	}
	parts := []string{fmt.Sprintf("输入 `%s`", formatTokenCount(u.InputTokens))}
	if u.CacheReadTokens > 0 {
		parts = append(parts, fmt.Sprintf("缓存命中 `%s`", formatTokenCount(u.CacheReadTokens)))
	}
	parts = append(parts, fmt.Sprintf("输出 `%s`", formatTokenCount(u.OutputTokens)))
	return fmt.Sprintf("%s\n\n---\n💡 **Token 用量**：%s · 合计 `%s`",
		strings.TrimRight(content, "\n"), strings.Join(parts, " · "), formatTokenCount(u.Total()))
}

func formatTokenCount(n int) string {
	switch {
	case n < 1000:
		return strconv.Itoa(n)
	case n < 1_000_000:
		return fmt.Sprintf("%.1fk", float64(n)/1000)
	default:
		return fmt.Sprintf("%.2fM", float64(n)/1_000_000)
	}
}

func truncateRunes(s string, maxRunes int) string {
	runes := []rune(strings.TrimSpace(s))
	if len(runes) <= maxRunes {
		return string(runes)
	}
	return string(runes[:maxRunes]) + "..."
}

func shortSuffix(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[len(s)-n:]
}

func (h *MessageHandler) addReaction(ctx context.Context, msgID, emojiType string) {
	if msgID == "" || emojiType == "" {
		return
	}
	req := larkim.NewCreateMessageReactionReqBuilder().
		MessageId(msgID).
		Body(larkim.NewCreateMessageReactionReqBodyBuilder().
			ReactionType(larkim.NewEmojiBuilder().
				EmojiType(emojiType).
				Build()).
			Build()).
		Build()

	resp, err := h.larkClient.Im.MessageReaction.Create(ctx, req)
	if err != nil {
		log.Printf("[Handler] Failed to add reaction %s to message %s: %v", emojiType, msgID, err)
		return
	}
	if !resp.Success() {
		log.Printf("[Handler] Feishu reaction error code=%d msg=%s reqId=%s", resp.Code, resp.Msg, resp.RequestId())
	}
}

func (h *MessageHandler) replyText(ctx context.Context, msgID, text string) {
	contentBytes, err := json.Marshal(map[string]string{
		"text": text,
	})
	if err != nil {
		log.Printf("[Handler] Failed to marshal reply content: %v", err)
		return
	}

	contentStr := string(contentBytes)
	req := larkim.NewReplyMessageReqBuilder().
		MessageId(msgID).
		Body(larkim.NewReplyMessageReqBodyBuilder().
			MsgType(larkim.MsgTypeText).
			Content(contentStr).
			Build()).
		Build()

	resp, err := h.larkClient.Im.Message.Reply(ctx, req)
	if err != nil {
		log.Printf("[Handler] Failed to reply to message %s: %v", msgID, err)
		return
	}

	if !resp.Success() {
		log.Printf("[Handler] Feishu reply error code=%d msg=%s reqId=%s", resp.Code, resp.Msg, resp.RequestId())
		return
	}

	log.Printf("[Handler] Successfully replied to message %s", msgID)
}
