package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	lark "github.com/larksuite/oapi-sdk-go/v3"
	"github.com/larksuite/oapi-sdk-go/v3/event/dispatcher/callback"
	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"

	"fsrobot/internal/registry"
	"fsrobot/internal/roles"
)

var mentionRegexp = regexp.MustCompile(`@_user_\d+`)

// MessageHandler handles incoming Feishu message events and routes them via AgentRegistry.
type MessageHandler struct {
	larkClient *lark.Client
	registry   *registry.Registry
	sessions   *SessionManager
	dedupe     *Deduplicator
	users      *UserResolver
	approvals  *ApprovalManager
	questions  *QuestionManager
	roles      roles.Provider
	botOpenID  string
	requireAt  bool
}

// NewMessageHandler creates a new MessageHandler backed by an AgentRegistry and roles.Provider.
func NewMessageHandler(client *lark.Client, reg *registry.Registry, botOpenID string, requireAt bool, adminOpenIDs []string, roleProvider roles.Provider) *MessageHandler {
	if roleProvider == nil {
		roleProvider = roles.NewDefaultCatalog()
	}
	return &MessageHandler{
		larkClient: client,
		registry:   reg,
		sessions:   NewSessionManager(),
		dedupe:     NewDeduplicator(10 * time.Minute),
		users:      NewUserResolver(client),
		approvals:  NewApprovalManager(adminOpenIDs),
		questions:  NewQuestionManager(),
		roles:      roleProvider,
		botOpenID:  botOpenID,
		requireAt:  requireAt,
	}
}

// EnableRolePersistence enables loading and saving per-chat role selections to path across restarts.
func (h *MessageHandler) EnableRolePersistence(path string) {
	h.sessions.EnableRolePersistence(path)
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
	parentID := ""
	if msg.ParentId != nil {
		parentID = *msg.ParentId
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
		prompt := resolveMentions(tc.Text, msg.Mentions, h.botOpenID, h.users)
		log.Printf("[Handler] Received text message chat=%s(%s) sender=%s: %q", chatID, chatType, senderID, prompt)
		if matched, auth, approved, apprID, err := h.approvals.TryHandle(sessionID, senderID, prompt); matched {
			go h.replyApprovalFeedback(context.Background(), msgID, auth, approved, apprID, err)
			return nil
		}
		go h.processAndReply(context.Background(), msgID, parentID, sessionID, chatID, senderID, prompt)

	case "image":
		imgKey, err := parseImageContent(rawContent)
		if err != nil {
			log.Printf("[Handler] Failed to parse image content: %v", err)
			return nil
		}
		log.Printf("[Handler] Received image message chat=%s(%s) sender=%s key=%s", chatID, chatType, senderID, imgKey)
		go h.processMediaAndReply(context.Background(), msgID, parentID, sessionID, chatID, senderID, "", []string{imgKey})

	case "post":
		text, imgKeys, err := parsePostContent(rawContent)
		if err != nil {
			log.Printf("[Handler] Failed to parse post content: %v", err)
			return nil
		}
		log.Printf("[Handler] Received post message chat=%s(%s) sender=%s text=%q images=%d", chatID, chatType, senderID, text, len(imgKeys))
		go h.processMediaAndReply(context.Background(), msgID, parentID, sessionID, chatID, senderID, text, imgKeys)

	default:
		log.Printf("[Handler] Unsupported message type=%s, ignoring", msgType)
	}
	return nil
}

func (h *MessageHandler) processMediaAndReply(ctx context.Context, msgID, parentID, sessionID, chatID, senderID, text string, imgKeys []string) {
	if len(imgKeys) == 0 {
		h.processAndReply(ctx, msgID, parentID, sessionID, chatID, senderID, text)
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
		h.processAndReply(ctx, msgID, parentID, sessionID, chatID, senderID, text)
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

	h.processAndReply(ctx, msgID, parentID, sessionID, chatID, senderID, strings.TrimSpace(sb.String()))
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

func (h *MessageHandler) resolveProvider(chatID, senderID, prompt string) (registry.AgentProvider, string, bool) {
	// Support one-shot routing: "#gp 你的问题" (only for admins when admin_open_ids is configured)
	if strings.HasPrefix(prompt, "#") && (!h.approvals.HasAdmins() || h.approvals.IsAdmin(senderID)) {
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

func (h *MessageHandler) processAndReply(ctx context.Context, msgID, parentID, sessionID, chatID, senderID, prompt string) {
	if prompt == "" {
		h.replyCard(ctx, msgID, "✨ 飞书智能助手", "blue", "你好！我是飞书智能助手，已接入 **Agent 注册中心**。\n发送 `/help` 可查看可用指令与已注册的 Agent。")
		return
	}

	// 1. Handle built-in slash commands (executed immediately without waiting in queue)
	if strings.HasPrefix(prompt, "/") {
		if handled := h.handleCommand(ctx, msgID, sessionID, chatID, senderID, prompt); handled {
			return
		}
	}

	// 1.5 If the user explicitly quotes/replies to a pending ask_user_question card,
	// treat this message as a free-form text answer instead of spawning a parallel task.
	isAdmin := !h.approvals.HasAdmins() || h.approvals.IsAdmin(senderID)
	if matched, qItem, qErr := h.questions.ResolveByCardText(sessionID, parentID, senderID, isAdmin, prompt); matched {
		if qErr != nil {
			h.replyCard(ctx, msgID, "❌ 回复失败", "red", fmt.Sprintf("回传问题答案失败：`%v`", qErr))
			return
		}
		if qItem != nil && qItem.CardID != "" {
			senderLabel := h.users.FormatSender(ctx, chatID, senderID)
			answeredBody := fmt.Sprintf("**问题**：%s\n\n✅ **%s 已通过文本回复**：\n> %s", qItem.Question, senderLabel, prompt)
			h.patchCard(context.Background(), qItem.CardID, fmt.Sprintf("💬 已回复 (%s)", qItem.ID), "green", answeredBody)
		}
		go h.addReaction(ctx, msgID, "DONE")
		return
	}

	// 2. Resolve target AgentProvider
	provider, cleanPrompt, _ := h.resolveProvider(chatID, senderID, prompt)
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
	h.sessions.StartRun(lockKey, msgID, effSessionID, senderID, cleanPrompt, isParallel, cancel)
	defer func() {
		cancel()
		h.sessions.EndRun(lockKey, msgID)
	}()

	senderLabel := h.users.FormatSender(runCtx, chatID, senderID)
	rawRole := h.sessions.GetRole(chatID)
	activeRole := h.roles.ResolvePromptOrDefault(rawRole)
	log.Printf("[Handler] Dispatching to agent=%s session=%s role=%s workDir=%s tools=%v (parallel=%v)", meta.ID, effSessionID, activeRole.Name, activeRole.Policy.WorkDir, activeRole.Policy.Tools, isParallel)
	rolePrompt := h.roles.FormatPrompt(rawRole)
	thinkingText := h.roles.ThinkingText(rawRole)
	agentPrompt := fmt.Sprintf("[角色人设]\n%s\n\n[飞书上下文: chat_id=%s | 发送者: %s | 重要说明: 你的最终输出会自动渲染为飞书卡片回复到当前群聊，日常群内回复请直接输出内容即可，禁止调用 feishu_send_message 往当前群重复发消息（仅当明确需要给个人发私聊或跨群发消息时才调用）；若生成了本地图片(.png/.jpg等)，请在回复中直接附带图片完整绝对路径或 ![说明](完整路径)，不要把目录和文件名拆开写，网关会自动上传并展示到飞书卡片]\n%s", rolePrompt, chatID, senderLabel, cleanPrompt)

	req := registry.RunRequest{
		SessionID: effSessionID,
		Prompt:    agentPrompt,
		WorkDir:   activeRole.Policy.WorkDir,
		Policy:    activeRole.Policy,
		Caller: registry.CallerContext{
			SenderID:      senderID,
			IsBoss:        h.approvals.IsAdmin(senderID),
			AllowApproval: h.approvals.HasAdmins(),
		},
		Metadata: map[string]string{
			"chat_id":    chatID,
			"channel_id": "feishu:" + chatID,
			"sender_id":  senderID,
			"msg_id":     msgID,
		},
	}

	evCh, err := provider.Execute(runCtx, req)
	if err != nil {
		h.replyCard(ctx, msgID, fmt.Sprintf("❌ %s 启动失败", meta.ID), "red", fmt.Sprintf("```text\n%v\n```", err))
		return
	}

	statusCardID := ""
	if meta.Streaming {
		initText := thinkingText
		if isParallel {
			initText = "⚡ 主任务仍在执行，已为您开启**并行子任务**处理中..."
		}
		statusCardID = h.replyCard(ctx, msgID, statusTitle, "wathet", initText)
	}

	var cardMu sync.Mutex
	var deltaBuf strings.Builder
	var finalReply string
	var execErr error
	var runUsage *registry.Usage
	var activeSubagents []string
	var recentSteps []string
	var pendingApprovalIDs []string
	pendingApprovalCards := make(map[string][]string)
	var pendingQuestionIDs []string
	var alertCardIDs []string
	var approvalHistory []string
	var auditCardID string
	var auditLogs []string
	startedAt := time.Now()
	lastPatchAt := time.Time{}
	lastActivityAt := startedAt
	statusTpl := "wathet"
	currentPhase := "思考中..."
	latestProgress := ""
	lastStalled := false
	alertedCurrentPhase := false

	defer func() {
		cardMu.Lock()
		apprIDsCopy := append([]string(nil), pendingApprovalIDs...)
		apprCardsSnap := make(map[string][]string, len(pendingApprovalCards))
		for k, v := range pendingApprovalCards {
			apprCardsSnap[k] = append([]string(nil), v...)
		}
		qIDsCopy := append([]string(nil), pendingQuestionIDs...)
		cardMu.Unlock()

		for _, id := range apprIDsCopy {
			if h.approvals.Remove(sessionID, id) {
				for _, cid := range apprCardsSnap[id] {
					if cid != "" {
						h.patchCard(context.Background(), cid, fmt.Sprintf("⏹️ 审批已失效 (%s)", id), "grey", "⏹️ **关联任务已结束或被中止，该审批已自动失效。**")
					}
				}
			}
		}
		for _, id := range qIDsCopy {
			h.questions.Remove(sessionID, id)
		}
	}()

	addStepLocked := func(step string) {
		elapsed := time.Since(startedAt).Round(time.Second)
		entry := fmt.Sprintf("`%s` %s", elapsed, step)
		recentSteps = append(recentSteps, entry)
		if len(recentSteps) > 6 {
			recentSteps = recentSteps[len(recentSteps)-6:]
		}
	}

	updateStatusCard := func(force bool) {
		if statusCardID == "" {
			return
		}
		cardMu.Lock()
		if !force && time.Since(lastPatchAt) < 1500*time.Millisecond {
			cardMu.Unlock()
			return
		}
		lastPatchAt = time.Now()
		tpl := statusTpl
		curPhase := currentPhase
		progressNote := latestProgress
		stalled := lastStalled
		var body strings.Builder
		if len(recentSteps) > 0 {
			body.WriteString(fmt.Sprintf("**执行进展**（已耗时 `%s`）：\n", time.Since(startedAt).Round(time.Second)))
			for _, st := range recentSteps {
				body.WriteString("- ")
				body.WriteString(st)
				body.WriteString("\n")
			}
			body.WriteString("\n")
		}
		if progressNote != "" {
			if stalled {
				body.WriteString(fmt.Sprintf("⚠️ **当前卡顿提示**：%s\n\n", progressNote))
			} else {
				body.WriteString(fmt.Sprintf("📡 **实时输出**：`%s`\n\n", progressNote))
			}
		} else if time.Since(lastActivityAt) >= 30*time.Second {
			body.WriteString(fmt.Sprintf("⏳ **当前状态**：%s（已持续 `%s`）\n\n", curPhase, time.Since(lastActivityAt).Round(time.Second)))
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
		} else if len(recentSteps) == 0 && len(activeSubagents) == 0 && progressNote == "" {
			body.WriteString(thinkingText)
		}
		content := strings.TrimSpace(body.String())
		cardMu.Unlock()

		h.patchCard(ctx, statusCardID, statusTitle, tpl, content)
	}

	stopWatchdog := make(chan struct{})
	if statusCardID != "" {
		go func() {
			ticker := time.NewTicker(15 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-stopWatchdog:
					return
				case <-runCtx.Done():
					return
				case <-ticker.C:
					cardMu.Lock()
					silentFor := time.Since(lastActivityAt)
					shouldPatch := silentFor >= 30*time.Second
					shouldAlert := (silentFor >= 3*time.Minute || lastStalled) && !alertedCurrentPhase
					var phaseSnap, progressSnap string
					var stepsSnap []string
					var stalledSnap bool
					if shouldAlert {
						alertedCurrentPhase = true
						statusTpl = "orange"
						phaseSnap = currentPhase
						progressSnap = latestProgress
						stalledSnap = lastStalled
						stepsSnap = append([]string(nil), recentSteps...)
					}
					cardMu.Unlock()

					if shouldPatch || shouldAlert {
						updateStatusCard(true)
					}
					if shouldAlert {
						if aid := h.sendWatchdogAlertCard(ctx, msgID, lockKey, chatID, senderID, meta.ID, phaseSnap, progressSnap, time.Since(startedAt).Round(time.Second), stepsSnap, stalledSnap); aid != "" {
							cardMu.Lock()
							alertCardIDs = append(alertCardIDs, aid)
							cardMu.Unlock()
						}
					}
				}
			}
		}()
	}

	for ev := range evCh {
		if ev.Usage != nil {
			u := *ev.Usage
			runUsage = &u
		}
		switch ev.Type {
		case registry.EventToolStart:
			phase := fmt.Sprintf("🔧 调用工具：`%s`", ev.Tool)
			h.sessions.UpdatePhase(lockKey, msgID, phase)
			cardMu.Lock()
			currentPhase = phase
			latestProgress = ""
			lastStalled = false
			alertedCurrentPhase = false
			statusTpl = "wathet"
			lastActivityAt = time.Now()
			addStepLocked(phase)
			shouldForce := time.Since(lastPatchAt) >= 600*time.Millisecond
			cardMu.Unlock()
			updateStatusCard(shouldForce)
		case registry.EventToolProgress:
			phase := fmt.Sprintf("🔧 `%s`：%s", ev.Tool, ev.Content)
			h.sessions.UpdatePhase(lockKey, msgID, phase)
			cardMu.Lock()
			currentPhase = fmt.Sprintf("🔧 `%s` 运行中", ev.Tool)
			latestProgress = ev.Content
			lastStalled = ev.Stalled
			if !ev.Stalled {
				lastActivityAt = time.Now()
				statusTpl = "wathet"
			} else {
				statusTpl = "orange"
			}
			shouldAlertNow := ev.Stalled && !alertedCurrentPhase
			var stepsSnap []string
			if shouldAlertNow {
				alertedCurrentPhase = true
				stepsSnap = append([]string(nil), recentSteps...)
			}
			cardMu.Unlock()
			updateStatusCard(ev.Stalled)
			if shouldAlertNow {
				if aid := h.sendWatchdogAlertCard(ctx, msgID, lockKey, chatID, senderID, meta.ID, currentPhase, ev.Content, time.Since(startedAt).Round(time.Second), stepsSnap, true); aid != "" {
					cardMu.Lock()
					alertCardIDs = append(alertCardIDs, aid)
					cardMu.Unlock()
				}
			}
		case registry.EventSubagentStart:
			isRiskGuard := strings.Contains(strings.ToLower(ev.Agent), "risk_guard")
			phase := fmt.Sprintf("🚀 子 Agent `%s` 运行中...", ev.Agent)
			h.sessions.UpdatePhase(lockKey, msgID, phase)
			cardMu.Lock()
			currentPhase = phase
			lastStalled = false
			alertedCurrentPhase = false
			statusTpl = "wathet"
			lastActivityAt = time.Now()
			addStepLocked(fmt.Sprintf("🚀 派发子 Agent：`%s`", ev.Agent))
			updatedExisting := false
			if isRiskGuard {
				for i, s := range activeSubagents {
					if strings.Contains(strings.ToLower(s), "risk_guard") {
						activeSubagents[i] = fmt.Sprintf("🚀 `%s` 运行中...", ev.Agent)
						updatedExisting = true
						break
					}
				}
			}
			if !updatedExisting {
				activeSubagents = append(activeSubagents, fmt.Sprintf("🚀 `%s` 运行中...", ev.Agent))
			}
			cardMu.Unlock()
			updateStatusCard(true)
		case registry.EventSubagentEnd:
			isRiskGuard := strings.Contains(strings.ToLower(ev.Agent), "risk_guard")
			cardMu.Lock()
			for i, s := range activeSubagents {
				if strings.Contains(s, ev.Agent) && (isRiskGuard || strings.HasPrefix(s, "🚀")) {
					if ev.Err != nil {
						activeSubagents[i] = fmt.Sprintf("❌ `%s` 失败 (%s)", ev.Agent, ev.Summary)
					} else {
						activeSubagents[i] = fmt.Sprintf("✅ `%s` 完成 (%s)", ev.Agent, ev.Summary)
					}
					break
				}
			}
			addStepLocked(fmt.Sprintf("✅ 子 Agent `%s` 完成", ev.Agent))
			phase := fmt.Sprintf("✅ 子 Agent `%s` 已完成，继续调度中...", ev.Agent)
			currentPhase = phase
			lastActivityAt = time.Now()
			cardMu.Unlock()
			h.sessions.UpdatePhase(lockKey, msgID, phase)

			if isRiskGuard {
				var entry string
				auditTpl := "turquoise"
				if ev.Err != nil {
					auditTpl = "red"
					entry = fmt.Sprintf("❌ **审计异常** (%s)\n```text\n%v\n```", ev.Summary, ev.Err)
				} else if trimmed := strings.TrimSpace(ev.Content); trimmed != "" {
					if strings.Contains(trimmed, "高风险操作已拦截") {
						auditTpl = "red"
					} else if strings.Contains(trimmed, "需管理员确认") {
						auditTpl = "orange"
					}
					durTag := ""
					if ev.Summary != "" {
						durTag = fmt.Sprintf(" (%s)", ev.Summary)
					}
					entry = fmt.Sprintf("%s%s", trimmed, durTag)
				}
				if entry != "" {
					cardMu.Lock()
					auditLogs = append(auditLogs, fmt.Sprintf("**#%d** %s", len(auditLogs)+1, entry))
					totalAudits := len(auditLogs)
					visibleLogs := auditLogs
					if len(visibleLogs) > 12 {
						visibleLogs = visibleLogs[len(visibleLogs)-12:]
					}
					body := strings.Join(visibleLogs, "\n\n---\n\n")
					if totalAudits > len(visibleLogs) {
						body = fmt.Sprintf("*(已折叠更早的 %d 条审计记录)*\n\n---\n\n%s", totalAudits-len(visibleLogs), body)
					}
					curAuditCardID := auditCardID
					cardMu.Unlock()

					auditTitle := fmt.Sprintf("🛡️ 安全审计记录（共 %d 次）", totalAudits)
					if curAuditCardID == "" {
						if newID := h.replyCard(ctx, msgID, auditTitle, auditTpl, body); newID != "" {
							cardMu.Lock()
							auditCardID = newID
							cardMu.Unlock()
						}
					} else {
						h.patchCard(ctx, curAuditCardID, auditTitle, auditTpl, body)
					}
				}
			} else {
				// Send each non-audit subagent's output as a dedicated card to Feishu immediately!
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
			}
			updateStatusCard(true)
		case registry.EventApprovalRequest:
			cardMu.Lock()
			pendingApprovalIDs = append(pendingApprovalIDs, ev.ApprovalID)
			addStepLocked(fmt.Sprintf("🔐 等待审批：`%s` (%s)", ev.Tool, ev.ApprovalID))
			currentPhase = fmt.Sprintf("🔐 等待管理员确认：`%s` (%s)", ev.Tool, ev.ApprovalID)
			lastActivityAt = time.Now()
			cardMu.Unlock()
			h.sessions.UpdatePhase(lockKey, msgID, fmt.Sprintf("🔐 等待管理员确认：`%s` (%s)", ev.Tool, ev.ApprovalID))
			updateStatusCard(true)
			argsJSON := "{}"
			if len(ev.Args) > 0 {
				if b, mErr := json.MarshalIndent(ev.Args, "", "  "); mErr == nil {
					argsJSON = string(b)
				}
			}
			cardMu.Lock()
			histCopy := append([]string(nil), approvalHistory...)
			approvalHistory = append(approvalHistory, fmt.Sprintf("`%s` (%s)", ev.ApprovalID, ev.Tool))
			cardMu.Unlock()

			apprCardIDs := h.sendApprovalCard(ctx, msgID, sessionID, senderLabel, ev.ApprovalID, ev.Tool, ev.Content, argsJSON, histCopy)
			if len(apprCardIDs) > 0 {
				cardMu.Lock()
				pendingApprovalCards[ev.ApprovalID] = apprCardIDs
				cardMu.Unlock()
			}
			apprID := ev.ApprovalID
			apprTool := ev.Tool
			apprReason := ev.Content
			h.approvals.AddWithTimeout(sessionID, apprID, apprTool, apprReason, ev.ReplyApproval, func() {
				timeoutBody := fmt.Sprintf("⏰ **审批已超时（60s），已自动拒绝执行**\n- **待执行工具**：`%s`\n- **风险原因**：%s", apprTool, apprReason)
				for _, cid := range apprCardIDs {
					if cid != "" {
						h.patchCard(context.Background(), cid, fmt.Sprintf("⏰ 审批已超时 (%s)", apprID), "grey", timeoutBody)
					}
				}
			})
		case registry.EventQuestionRequest:
			cardMu.Lock()
			pendingQuestionIDs = append(pendingQuestionIDs, ev.QuestionID)
			addStepLocked(fmt.Sprintf("💬 等待用户回复 (%s)", ev.QuestionID))
			currentPhase = fmt.Sprintf("💬 等待用户选择或回复 (%s)", ev.QuestionID)
			lastActivityAt = time.Now()
			cardMu.Unlock()
			h.sessions.UpdatePhase(lockKey, msgID, fmt.Sprintf("💬 等待用户选择或回复 (%s)", ev.QuestionID))
			updateStatusCard(true)

			qCardID := h.sendQuestionCard(ctx, msgID, sessionID, ev.QuestionID, senderID, ev.Content, ev.Options)
			qID := ev.QuestionID
			qText := ev.Content
			h.questions.AddWithTimeout(sessionID, qID, senderID, qText, ev.Options, qCardID, ev.ReplyQuestion, func() {
				if qCardID != "" {
					timeoutBody := fmt.Sprintf("**问题**：%s\n\n⏰ **提问已超时（90s），已交由助手自行决定继续执行。**", qText)
					h.patchCard(context.Background(), qCardID, fmt.Sprintf("⏰ 提问已超时 (%s)", qID), "grey", timeoutBody)
				}
			})
		case registry.EventDelta:
			cardMu.Lock()
			isFirstDelta := deltaBuf.Len() == 0
			deltaBuf.WriteString(ev.Content)
			currentPhase = "✍️ 正在生成回复 / 推理中..."
			lastStalled = false
			statusTpl = "wathet"
			lastActivityAt = time.Now()
			cardMu.Unlock()
			if isFirstDelta {
				h.sessions.UpdatePhase(lockKey, msgID, "✍️ 正在生成回复...")
			}
			updateStatusCard(false)
		case registry.EventFinal:
			finalReply = ev.Content
		case registry.EventError:
			execErr = ev.Err
		}
	}
	close(stopWatchdog)
	cardMu.Lock()
	alertsToClose := append([]string(nil), alertCardIDs...)
	cardMu.Unlock()
	for _, aid := range alertsToClose {
		h.patchCard(context.Background(), aid, fmt.Sprintf("✅ 监控已结束 (%s)", meta.ID), "grey", "✅ **该任务已执行结束，监控已自动关闭。**")
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

func (h *MessageHandler) handleCommand(ctx context.Context, msgID, sessionID, chatID, senderID, cmdLine string) bool {
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
			"- `/role [预设名|自定义人设|重置]`（或 `/角色`）- 查看或切换当前群角色人设",
			"- `/status` - 查看当前群使用的 Agent 及运行状态",
			"- `/stop` - 中止当前群正在执行的 Agent 任务",
			"- `/new` - 重置当前群与当前 Agent 的对话上下文",
			"- `/sessions`（或 `/会话`）- 查看当前群历史会话列表",
			"- `/resume <序号|session_id>`（或 `/恢复`）- 恢复指定历史会话",
			"- `/delsession <序号|session_id>`（或 `/del`、`/删会话`）- 删除指定历史会话",
			"- `/ctx-stat` - 查看当前会话 ContextLedger、Epoch 掩码与缓存命中率指标（只读）",
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
		provider, _, _ := h.resolveProvider(chatID, senderID, "")
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
		provider, _, _ := h.resolveProvider(chatID, senderID, "")
		if provider == nil {
			h.replyCard(ctx, msgID, "⏹️ 中止任务", "grey", "当前没有可用的 Agent。")
			return true
		}
		lockKey := fmt.Sprintf("%s:%s", provider.Meta().ID, sessionID)
		allowAll := !h.approvals.HasAdmins() || h.approvals.IsAdmin(senderID)
		count, total := h.sessions.CancelAllowed(lockKey, senderID, allowAll)
		if count > 0 {
			h.replyCard(ctx, msgID, "", "", fmt.Sprintf("正在中止 **[%s]** 的 %d 个运行中任务...", provider.Meta().ID, count))
		} else if total > 0 {
			h.replyCard(ctx, msgID, "⛔ 权限不足", "red", "仅管理员或任务发起人本人可中止运行中的任务。")
		} else {
			h.replyCard(ctx, msgID, "", "", fmt.Sprintf("**[%s]** 当前没有正在执行的任务。", provider.Meta().ID))
		}
		return true

	case "/use":
		if !h.approvals.IsAdmin(senderID) {
			h.replyCard(ctx, msgID, "⛔ 权限不足", "red", "仅管理员可切换当前群默认 Agent。")
			return true
		}
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
		if h.approvals.HasAdmins() && !h.approvals.IsAdmin(senderID) {
			h.replyCard(ctx, msgID, "⛔ 权限不足", "red", "仅管理员可重置当前群会话。")
			return true
		}
		provider, _, _ := h.resolveProvider(chatID, senderID, "")
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

	case "/role", "/角色":
		arg := strings.TrimSpace(strings.TrimPrefix(cmdLine, parts[0]))
		defRole := h.roles.Default()
		if arg == "" {
			cur := h.sessions.GetRole(chatID)
			curName := fmt.Sprintf("%s（默认）", defRole.Name)
			if cur != "" {
				curName = "自定义 / 已切换"
				if matched, ok := h.roles.Resolve(cur); ok {
					curName = matched.Name
					cur = matched.Prompt
				} else if matched, ok := h.roles.MatchByPrompt(cur); ok {
					curName = matched.Name
				}
			} else {
				cur = defRole.Prompt
			}

			var presetLabels []string
			for _, p := range h.roles.Presets() {
				label := fmt.Sprintf("`%s`", p.Name)
				if p.Name == defRole.Name {
					label += "（默认）"
				} else if len(p.Aliases) > 0 {
					label += fmt.Sprintf("（别名 `%s`）", strings.Join(p.Aliases, "`/`"))
				}
				presetLabels = append(presetLabels, label)
			}

			body := fmt.Sprintf(
				"**当前角色**：`%s`\n**可用预设**：%s\n\n**当前人设**：\n> %s\n\n用法：\n- `/角色 <预设名>` 或 `/role <别名>` 切换预设\n- `/角色 <任意自定义人设描述>` 自定义角色\n- `/角色 重置` 恢复默认人设",
				curName, strings.Join(presetLabels, "、"), truncateRunes(strings.ReplaceAll(cur, "\n", " "), 160),
			)
			h.replyCard(ctx, msgID, "🎭 群聊角色设定", "wathet", body)
			return true
		}

		if !h.approvals.IsAdmin(senderID) {
			h.replyCard(ctx, msgID, "⛔ 权限不足", "red", "仅管理员可修改当前群角色设定。")
			return true
		}

		switch strings.ToLower(arg) {
		case "重置", "默认", "reset", "default":
			h.sessions.SetRole(chatID, "")
			h.replyCard(ctx, msgID, "🎭 已恢复默认角色", "green", fmt.Sprintf("当前群已恢复默认角色：**`%s`** 😏", defRole.Name))
			return true
		}

		if resolved, ok := h.roles.Resolve(arg); ok {
			h.sessions.SetRole(chatID, resolved.Name)
			h.replyCard(ctx, msgID, "🎭 已切换角色", "green", fmt.Sprintf("当前群角色已切换为：**`%s`**", resolved.Name))
			return true
		}

		h.sessions.SetRole(chatID, arg)
		h.replyCard(ctx, msgID, "🎭 已更新自定义角色", "green", fmt.Sprintf("当前群已应用新角色设定：\n> %s", truncateRunes(arg, 160)))
		return true

	case "/sessions", "/会话":
		provider, _, _ := h.resolveProvider(chatID, senderID, "")
		if provider == nil {
			h.replyCard(ctx, msgID, "📚 会话列表", "grey", "当前没有可用的 Agent。")
			return true
		}
		smp, ok := provider.(registry.SessionManagerProvider)
		if !ok {
			h.replyCard(ctx, msgID, "📚 会话列表", "grey", fmt.Sprintf("**[%s]** 暂不支持查询历史会话列表。", provider.Meta().ID))
			return true
		}
		curID, list, err := smp.ListSessions(ctx, sessionID)
		if err != nil {
			h.replyCard(ctx, msgID, "❌ 查询会话失败", "red", fmt.Sprintf("```text\n%v\n```", err))
			return true
		}
		if len(list) == 0 {
			h.replyCard(ctx, msgID, "📚 历史会话列表", "grey", "当前群暂无已持久化的历史会话。")
			return true
		}
		lockKey := fmt.Sprintf("%s:%s", provider.Meta().ID, sessionID)
		ids := make([]string, 0, len(list))
		var sb strings.Builder
		for i, s := range list {
			ids = append(ids, s.SessionID)
			mark := "⚪"
			curTag := ""
			if s.SessionID == curID {
				mark = "✅"
				curTag = " **`[当前]`**"
			}
			preview := strings.TrimSpace(strings.ReplaceAll(s.FirstPrompt, "\n", " "))
			if preview == "" {
				preview = "（无首条提问摘要）"
			} else {
				preview = truncateRunes(preview, 36)
			}
			sb.WriteString(fmt.Sprintf("%s **%d.** `%s`%s\n> %s · %d 条记录 · %s\n\n",
				mark, i+1, s.SessionID, curTag, preview, s.MessageCount, s.UpdatedAt))
		}
		h.sessions.SetListedSessions(lockKey, ids)
		sb.WriteString("用法：`/resume <序号>` 恢复会话，`/del <序号>` 删除会话（仅限本群）。")
		h.replyCard(ctx, msgID, fmt.Sprintf("📚 历史会话列表 (%s)", provider.Meta().ID), "wathet", sb.String())
		return true

	case "/resume", "/恢复":
		if h.approvals.HasAdmins() && !h.approvals.IsAdmin(senderID) {
			h.replyCard(ctx, msgID, "⛔ 权限不足", "red", "仅管理员可恢复历史会话。")
			return true
		}
		if len(parts) < 2 {
			h.replyCard(ctx, msgID, "💡 用法提示", "blue", "请指定要恢复的序号或会话 ID，例如：`/resume 2`（先用 `/sessions` 查看序号）。")
			return true
		}
		provider, _, _ := h.resolveProvider(chatID, senderID, "")
		if provider == nil {
			h.replyCard(ctx, msgID, "🔄 恢复会话", "grey", "当前没有可用的 Agent。")
			return true
		}
		smp, ok := provider.(registry.SessionManagerProvider)
		if !ok {
			h.replyCard(ctx, msgID, "🔄 恢复会话", "grey", fmt.Sprintf("**[%s]** 暂不支持恢复历史会话。", provider.Meta().ID))
			return true
		}
		lockKey := fmt.Sprintf("%s:%s", provider.Meta().ID, sessionID)
		targetID := h.sessions.ResolveListedSession(lockKey, parts[1])
		if err := smp.ResumeSession(ctx, sessionID, targetID); err != nil {
			h.replyCard(ctx, msgID, "❌ 恢复会话失败", "red", fmt.Sprintf("`%v`", err))
			return true
		}
		h.replyCard(ctx, msgID, "🔄 已恢复历史会话", "green", fmt.Sprintf("当前群已切换回 **[%s]** 会话：`%s`", provider.Meta().ID, targetID))
		return true

	case "/delsession", "/del", "/删会话":
		if h.approvals.HasAdmins() && !h.approvals.IsAdmin(senderID) {
			h.replyCard(ctx, msgID, "⛔ 权限不足", "red", "仅管理员可删除历史会话。")
			return true
		}
		if len(parts) < 2 {
			h.replyCard(ctx, msgID, "💡 用法提示", "blue", "请指定要删除的序号或会话 ID，例如：`/del 2`（先用 `/sessions` 查看序号）。")
			return true
		}
		provider, _, _ := h.resolveProvider(chatID, senderID, "")
		if provider == nil {
			h.replyCard(ctx, msgID, "🗑️ 删除会话", "grey", "当前没有可用的 Agent。")
			return true
		}
		smp, ok := provider.(registry.SessionManagerProvider)
		if !ok {
			h.replyCard(ctx, msgID, "🗑️ 删除会话", "grey", fmt.Sprintf("**[%s]** 暂不支持删除历史会话。", provider.Meta().ID))
			return true
		}
		lockKey := fmt.Sprintf("%s:%s", provider.Meta().ID, sessionID)
		targetID := h.sessions.ResolveListedSession(lockKey, parts[1])
		if err := smp.DeleteSession(ctx, sessionID, targetID); err != nil {
			h.replyCard(ctx, msgID, "❌ 删除会话失败", "red", fmt.Sprintf("`%v`", err))
			return true
		}
		h.replyCard(ctx, msgID, "🗑️ 已删除历史会话", "green", fmt.Sprintf("已从 **[%s]** 清理会话：`%s`", provider.Meta().ID, targetID))
		return true

	case "/ctx-stat":
		provider, _, _ := h.resolveProvider(chatID, senderID, "")
		if provider == nil {
			h.replyCard(ctx, msgID, "📊 上下文与缓存状态", "grey", "当前没有可用的 Agent。")
			return true
		}
		csp, ok := provider.(registry.ContextStatsProvider)
		if !ok {
			h.replyCard(ctx, msgID, "📊 上下文与缓存状态", "grey", fmt.Sprintf("**[%s]** 暂不支持查询上下文与缓存指标。", provider.Meta().ID))
			return true
		}
		stats, err := csp.GetContextStats(ctx, sessionID)
		if err != nil {
			h.replyCard(ctx, msgID, "❌ 查询上下文指标失败", "red", fmt.Sprintf("```text\n%v\n```", err))
			return true
		}
		h.replyCard(ctx, msgID, fmt.Sprintf("📊 上下文与缓存指标 (%s)", provider.Meta().ID), "wathet", formatContextStatsCard(provider.Meta().ID, stats))
		return true

	default:
		return false
	}
}

func formatContextStatsCard(agentID string, stats *registry.ContextStats) string {
	if stats == nil {
		return "暂无上下文指标数据。"
	}
	epochStride := stats.EpochStride
	if epochStride <= 0 {
		epochStride = 16
	}
	ledgerHash := stats.LedgerHash
	if ledgerHash == "" {
		ledgerHash = "-"
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("- **Agent / Session**：`%s` / `%s`\n", agentID, stats.SessionID))
	sb.WriteString(fmt.Sprintf("- **会话消息 / 累计记录轮次**：%d 条消息 · %d 轮\n\n", stats.MessageCount, stats.TotalTurns))

	sb.WriteString(fmt.Sprintf("**📦 Epoch 冻结掩码进度**\n- Epoch `#%d` | 步长 `%d` | 窗口 `%d` | Epoch 偏移 `%d/%d` | 冻结边界 `msg[%d]` | 已折叠 `%d` 条\n\n",
		stats.EpochID, epochStride, stats.MaskWindow, stats.EpochPosition, epochStride, stats.FreezeBoundary, stats.MaskedCount))

	sb.WriteString(fmt.Sprintf("**📒 ContextLedger 账本** (`v%d` · hash `%s`)\n- Active `%d` · Superseded `%d` · Resolved `%d`\n",
		stats.LedgerVersion, ledgerHash, stats.LedgerActiveCnt, stats.LedgerSuperseded, stats.LedgerResolvedCnt))
	if len(stats.ActiveConstraints) == 0 {
		sb.WriteString("- 活跃约束：暂无\n")
	} else {
		sb.WriteString("- 活跃约束：\n")
		for _, c := range stats.ActiveConstraints {
			prov := "-"
			if len(c.Provenance) > 0 {
				prov = strings.Join(c.Provenance, ", ")
			}
			sb.WriteString(fmt.Sprintf("  - `[%s]` `%s`: %s _(prov: %s)_\n", c.Category, c.ID, truncateRunes(c.Statement, 80), prov))
		}
	}
	if len(stats.SupersededConstraints) > 0 {
		sb.WriteString("- 已废弃/替代约束：\n")
		maxSup := len(stats.SupersededConstraints)
		if maxSup > 5 {
			maxSup = 5
		}
		for _, c := range stats.SupersededConstraints[:maxSup] {
			target := c.SupersededBy
			if target == "" {
				target = "-"
			}
			sb.WriteString(fmt.Sprintf("  - ~~`%s`~~ (→ `%s`): %s\n", c.ID, target, truncateRunes(c.Statement, 60)))
		}
	}
	sb.WriteString("\n")

	sb.WriteString(fmt.Sprintf("**🔍 无损回查与检索累计**\n- `recall_entries`: **%d** 次 | `restore_observation`: **%d** 次 | `search_context`: **%d** 次\n\n",
		stats.TotalRecallCalls, stats.TotalRestoreCalls, stats.TotalSearchCalls))

	if len(stats.RecentTurns) > 0 {
		latest := stats.RecentTurns[0]
		strategy := latest.StrategyMode
		if strategy == "" {
			strategy = "-"
		}
		var wsSummary string
		switch {
		case latest.WorkingSetTokens == 0 && latest.WorkingSetEntries == 0:
			wsSummary = "未触发 (`0 tok`)"
		case latest.WorkingSetTokens == 0:
			wsSummary = fmt.Sprintf("`%d` 条 / `0 tok` (未触发)", latest.WorkingSetEntries)
		default:
			wsSummary = fmt.Sprintf("`%d` 条 / `%d` tok", latest.WorkingSetEntries, latest.WorkingSetTokens)
		}
		sb.WriteString(fmt.Sprintf("**🧩 最新策略与 WorkingSet 状态**\n- 策略: `%s` | WorkingSet: %s (命中实体 `%d`, 检索 `%.1fms`)\n\n",
			strategy, wsSummary, latest.GraphMatchedEntities, latest.RetrievalBuildMs))
	}

	sb.WriteString("**📈 最近 5 轮 Input / CacheRead / Hit% 走势**\n")
	if len(stats.RecentTurns) == 0 {
		sb.WriteString("- 暂无已记录的轮次指标。\n")
	} else {
		for i := len(stats.RecentTurns) - 1; i >= 0; i-- {
			t := stats.RecentTurns[i]
			denom := t.InputTokens
			if t.CacheReadTokens > denom {
				denom = t.CacheReadTokens
			}
			hitPct := 0.0
			if denom > 0 {
				hitPct = float64(t.CacheReadTokens) * 100.0 / float64(denom)
			}
			tHash := t.LedgerHash
			if tHash == "" {
				tHash = "-"
			}
			metaParts := []string{
				fmt.Sprintf("`%s`", t.CreatedAt),
				fmt.Sprintf("ep#%d", t.EpochID),
			}
			if t.StrategyMode != "" {
				metaParts = append(metaParts, t.StrategyMode)
			}
			if t.StrategyMode != "" || t.WorkingSetEntries > 0 || t.WorkingSetTokens > 0 {
				metaParts = append(metaParts, fmt.Sprintf("ws %d条/%dtok", t.WorkingSetEntries, t.WorkingSetTokens))
			}
			metaParts = append(metaParts, fmt.Sprintf("ledger v%d/%s", t.LedgerVersion, tHash))

			timing := fmt.Sprintf("%.1fms", t.ContextBuildMs)
			if t.StrategyMode != "" || t.RetrievalBuildMs > 0 {
				timing = fmt.Sprintf("%.1fms (ret %.1fms)", t.ContextBuildMs, t.RetrievalBuildMs)
			}

			var callNotes []string
			if t.SearchCalls > 0 {
				callNotes = append(callNotes, fmt.Sprintf("search×%d", t.SearchCalls))
			}
			if t.RecallCalls > 0 {
				callNotes = append(callNotes, fmt.Sprintf("recall×%d", t.RecallCalls))
			}
			if t.RestoreCalls > 0 {
				callNotes = append(callNotes, fmt.Sprintf("restore×%d", t.RestoreCalls))
			}
			callSuffix := ""
			if len(callNotes) > 0 {
				callSuffix = fmt.Sprintf(" · [%s]", strings.Join(callNotes, ", "))
			}

			sb.WriteString(fmt.Sprintf("- **T%d** (%s): Input `%d` · CacheRead `%d` · CacheWrite `%d` · Hit **`%.1f%%`** · %s%s\n",
				t.TurnIndex, strings.Join(metaParts, ", "),
				t.InputTokens, t.CacheReadTokens, t.CacheWriteTokens, hitPct, timing, callSuffix))
		}
	}
	return strings.TrimRight(sb.String(), "\n")
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

const contextWarnThreshold = 80_000

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
	warn := ""
	if u.InputTokens >= contextWarnThreshold {
		warn = " · ⚠️ 上下文较长，建议发送 `/new` 开启新会话"
	}
	return fmt.Sprintf("%s\n\n---\n💡 **Token 用量**：%s · 合计 `%s`%s",
		strings.TrimRight(content, "\n"), strings.Join(parts, " · "), formatTokenCount(u.Total()), warn)
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

func buildApprovalCardJSON(adminIDs []string, sessionID, senderLabel, approvalID, tool, reason, argsJSON string, history []string, isDM bool) (string, error) {
	actionHint := "请在 **60 秒内**点击下方按钮（或直接回复 `确认` / `拒绝`）："
	if !isDM {
		var adminMentions []string
		for _, id := range adminIDs {
			adminMentions = append(adminMentions, fmt.Sprintf("<at id=%s></at>", id))
		}
		mentionLine := "管理员"
		if len(adminMentions) > 0 {
			mentionLine = strings.Join(adminMentions, " ")
		}
		actionHint = fmt.Sprintf("请 %s 在 **60 秒内**点击下方按钮（或直接回复 `确认` / `拒绝`）：", mentionLine)
	}

	historyLine := ""
	if len(history) > 0 {
		historyLine = fmt.Sprintf("📋 **本轮已处理审批**：%s\n\n", strings.Join(history, "、"))
	}

	senderLine := ""
	if strings.TrimSpace(senderLabel) != "" {
		senderLine = fmt.Sprintf("- **触发人**：%s\n", senderLabel)
	}

	bodyText := fmt.Sprintf(
		"%s⚠️ **检测到高风险操作，需要管理员确认**\n%s- **审批编号**：`%s`\n- **待执行工具**：`%s`\n- **风险原因**：%s\n- **操作参数**：\n```json\n%s\n```\n\n%s",
		historyLine, senderLine, approvalID, tool, reason, argsJSON, actionHint,
	)

	card := map[string]any{
		"schema": "2.0",
		"config": map[string]any{
			"update_multi": true,
		},
		"header": map[string]any{
			"template": "orange",
			"title": map[string]any{
				"tag":     "plain_text",
				"content": fmt.Sprintf("🔐 高风险操作审批 (%s)", approvalID),
			},
		},
		"body": map[string]any{
			"elements": []map[string]any{
				{
					"tag":     "markdown",
					"content": bodyText,
				},
				{
					"tag":              "column_set",
					"flex_mode":        "none",
					"horizontal_align": "left",
					"columns": []map[string]any{
						{
							"tag":   "column",
							"width": "auto",
							"elements": []map[string]any{
								{
									"tag":  "button",
									"type": "primary",
									"text": map[string]any{
										"tag":     "plain_text",
										"content": "✅ 确认执行",
									},
									"behaviors": []map[string]any{
										{
											"type": "callback",
											"value": map[string]string{
												"approval_id": approvalID,
												"chat_id":     sessionID,
												"decision":    "approve",
											},
										},
									},
								},
							},
						},
						{
							"tag":   "column",
							"width": "auto",
							"elements": []map[string]any{
								{
									"tag":  "button",
									"type": "danger",
									"text": map[string]any{
										"tag":     "plain_text",
										"content": "🚫 拒绝",
									},
									"behaviors": []map[string]any{
										{
											"type": "callback",
											"value": map[string]string{
												"approval_id": approvalID,
												"chat_id":     sessionID,
												"decision":    "reject",
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	cardBytes, err := json.Marshal(card)
	if err != nil {
		return "", err
	}
	return string(cardBytes), nil
}

func (h *MessageHandler) sendApprovalCard(ctx context.Context, msgID, sessionID, senderLabel, approvalID, tool, reason, argsJSON string, history []string) []string {
	adminIDs := h.approvals.AdminIDs()

	// Prefer direct private message to configured approver(s) so group chats stay clean.
	if len(adminIDs) > 0 {
		dmCardStr, err := buildApprovalCardJSON(adminIDs, sessionID, senderLabel, approvalID, tool, reason, argsJSON, history, true)
		if err != nil {
			log.Printf("[Handler] Failed to marshal DM approval card JSON: %v", err)
		} else {
			var sentIDs []string
			for _, adminID := range adminIDs {
				req := larkim.NewCreateMessageReqBuilder().
					ReceiveIdType(larkim.ReceiveIdTypeOpenId).
					Body(larkim.NewCreateMessageReqBodyBuilder().
						ReceiveId(adminID).
						MsgType(larkim.MsgTypeInteractive).
						Content(dmCardStr).
						Build()).
					Build()
				resp, err := h.larkClient.Im.Message.Create(ctx, req)
				if err != nil {
					log.Printf("[Handler] Failed to send DM approval card to %s: %v", adminID, err)
					continue
				}
				if !resp.Success() {
					log.Printf("[Handler] Feishu DM approval card error admin=%s code=%d msg=%s reqId=%s", adminID, resp.Code, resp.Msg, resp.RequestId())
					continue
				}
				if resp.Data != nil && resp.Data.MessageId != nil && *resp.Data.MessageId != "" {
					sentIDs = append(sentIDs, *resp.Data.MessageId)
				}
			}
			if len(sentIDs) > 0 {
				return sentIDs
			}
		}
	}

	// Fallback: reply in the original chat if no admins are configured or all DM sends failed.
	cardStr, err := buildApprovalCardJSON(adminIDs, sessionID, senderLabel, approvalID, tool, reason, argsJSON, history, false)
	if err != nil {
		log.Printf("[Handler] Failed to marshal fallback approval card JSON: %v", err)
		return nil
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
		log.Printf("[Handler] Failed to send fallback approval card: %v", err)
		return nil
	}
	if !resp.Success() {
		log.Printf("[Handler] Feishu fallback approval card error code=%d msg=%s reqId=%s", resp.Code, resp.Msg, resp.RequestId())
		return nil
	}
	if resp.Data != nil && resp.Data.MessageId != nil && *resp.Data.MessageId != "" {
		return []string{*resp.Data.MessageId}
	}
	return nil
}

func (h *MessageHandler) sendWatchdogAlertCard(ctx context.Context, replyToMsgID, lockKey, chatID, initiatorID, agentID, phase, progressNote string, elapsed time.Duration, steps []string, stalled bool) string {
	var body strings.Builder
	if stalled {
		body.WriteString(fmt.Sprintf("⚠️ **任务疑似卡住**（总耗时 `%s`）\n", elapsed))
	} else {
		body.WriteString(fmt.Sprintf("⏳ **长任务进度提醒**（已耗时 `%s`）\n", elapsed))
	}
	body.WriteString(fmt.Sprintf("- **当前阶段**：%s\n", phase))
	if progressNote != "" {
		body.WriteString(fmt.Sprintf("- **最新采样**：`%s`\n", progressNote))
	}
	if len(steps) > 0 {
		lastStep := steps[len(steps)-1]
		body.WriteString(fmt.Sprintf("- **最近步骤**：%s\n", lastStep))
	}
	body.WriteString("\n如需中止可点击下方按钮，或发送 `/stop`。")

	card := map[string]any{
		"schema": "2.0",
		"config": map[string]any{
			"wide_screen_mode": true,
			"update_multi":     true,
		},
		"header": map[string]any{
			"template": "orange",
			"title": map[string]any{
				"tag":     "plain_text",
				"content": fmt.Sprintf("⏳ 长任务监控 (%s)", agentID),
			},
		},
		"body": map[string]any{
			"elements": []any{
				map[string]any{
					"tag":     "markdown",
					"content": body.String(),
				},
				map[string]any{
					"tag":     "column_set",
					"columns": []any{
						map[string]any{
							"tag":   "column",
							"width": "auto",
							"elements": []any{
								map[string]any{
									"tag":  "button",
									"type": "danger",
									"text": map[string]any{
										"tag":     "plain_text",
										"content": "🛑 中断任务",
									},
									"value": map[string]any{
										"action":       "abort_chat",
										"chat_id":      chatID,
										"lock_key":     lockKey,
										"run_id":       replyToMsgID,
										"initiator_id": initiatorID,
									},
								},
							},
						},
					},
				},
			},
		},
	}

	raw, err := json.Marshal(card)
	if err != nil {
		return ""
	}
	req := larkim.NewReplyMessageReqBuilder().
		MessageId(replyToMsgID).
		Body(larkim.NewReplyMessageReqBodyBuilder().
			MsgType("interactive").
			Content(string(raw)).
			Build()).
		Build()
	resp, err := h.larkClient.Im.Message.Reply(ctx, req)
	if err != nil || !resp.Success() {
		log.Printf("[Handler] sendWatchdogAlertCard error: %v", err)
		return ""
	}
	if resp.Data != nil && resp.Data.MessageId != nil {
		return *resp.Data.MessageId
	}
	return ""
}

func (h *MessageHandler) sendQuestionCard(ctx context.Context, msgID, sessionID, questionID, initiatorID, question string, options []string) string {
	var body strings.Builder
	body.WriteString(fmt.Sprintf("💬 **%s**\n\n", question))
	if initiatorID != "" {
		body.WriteString(fmt.Sprintf("请 <at id=%s></at> 点击下方选项，或**引用/回复本卡片发送文字**；90 秒未回复将自动跳过：", initiatorID))
	} else {
		body.WriteString("请点击下方选项，或**引用/回复本卡片发送文字**；90 秒未回复将自动跳过：")
	}

	elements := []any{
		map[string]any{
			"tag":     "markdown",
			"content": body.String(),
		},
	}
	for i, opt := range options {
		if i >= 5 {
			break
		}
		btnType := "default"
		if i == 0 {
			btnType = "primary"
		}
		elements = append(elements, map[string]any{
			"tag":  "button",
			"type": btnType,
			"text": map[string]any{
				"tag":     "plain_text",
				"content": opt,
			},
			"value": map[string]any{
				"action":      "answer_question",
				"question_id": questionID,
				"chat_id":     sessionID,
				"answer":      opt,
				"skipped":     "false",
			},
		})
	}
	elements = append(elements, map[string]any{
		"tag":  "button",
		"type": "default",
		"text": map[string]any{
			"tag":     "plain_text",
			"content": "⏭️ 跳过 / 由你决定",
		},
		"value": map[string]any{
			"action":      "answer_question",
			"question_id": questionID,
			"chat_id":     sessionID,
			"answer":      "",
			"skipped":     "true",
		},
	})

	card := map[string]any{
		"schema": "2.0",
		"config": map[string]any{
			"wide_screen_mode": true,
			"update_multi":     true,
		},
		"header": map[string]any{
			"template": "blue",
			"title": map[string]any{
				"tag":     "plain_text",
				"content": fmt.Sprintf("💬 需要您的确认或选择 (%s)", questionID),
			},
		},
		"body": map[string]any{
			"elements": elements,
		},
	}

	raw, err := json.Marshal(card)
	if err != nil {
		return ""
	}
	req := larkim.NewReplyMessageReqBuilder().
		MessageId(msgID).
		Body(larkim.NewReplyMessageReqBodyBuilder().
			MsgType("interactive").
			Content(string(raw)).
			Build()).
		Build()
	resp, err := h.larkClient.Im.Message.Reply(ctx, req)
	if err != nil || !resp.Success() {
		log.Printf("[Handler] sendQuestionCard error: %v", err)
		return ""
	}
	if resp.Data != nil && resp.Data.MessageId != nil {
		return *resp.Data.MessageId
	}
	return ""
}

// HandleCardAction handles Feishu card button callbacks (card.action.trigger).
func (h *MessageHandler) HandleCardAction(ctx context.Context, event *callback.CardActionTriggerEvent) (*callback.CardActionTriggerResponse, error) {
	if event == nil || event.Event == nil || event.Event.Action == nil {
		return nil, nil
	}

	val := event.Event.Action.Value
	operatorOpenID := ""
	if event.Event.Operator != nil {
		operatorOpenID = event.Event.Operator.OpenID
	}

	if action, _ := val["action"].(string); action == "answer_question" {
		questionID, _ := val["question_id"].(string)
		chatID, _ := val["chat_id"].(string)
		answer, _ := val["answer"].(string)
		skippedStr, _ := val["skipped"].(string)
		skipped := skippedStr == "true"
		isAdmin := !h.approvals.HasAdmins() || h.approvals.IsAdmin(operatorOpenID)
		found, authorized, qItem, err := h.questions.ResolveByID(chatID, questionID, operatorOpenID, isAdmin, answer, skipped)
		if !found {
			return &callback.CardActionTriggerResponse{
				Toast: &callback.Toast{
					Type:    "warning",
					Content: "该提问已回答或已超时",
				},
			}, nil
		}
		if !authorized {
			return &callback.CardActionTriggerResponse{
				Toast: &callback.Toast{
					Type:    "error",
					Content: "⛔ 仅管理员或任务发起人可回答此提问",
				},
			}, nil
		}
		if err != nil {
			return &callback.CardActionTriggerResponse{
				Toast: &callback.Toast{
					Type:    "error",
					Content: fmt.Sprintf("回传答案失败：%v", err),
				},
			}, nil
		}
		operatorLabel := h.users.FormatSender(ctx, chatID, operatorOpenID)
		tpl := "green"
		title := fmt.Sprintf("💬 已选择 (%s)", questionID)
		statusLine := fmt.Sprintf("✅ **%s 已选择**：`%s`", operatorLabel, answer)
		toastMsg := "已提交选择"
		if skipped {
			tpl = "grey"
			title = fmt.Sprintf("⏭️ 已跳过 (%s)", questionID)
			statusLine = fmt.Sprintf("⏭️ **%s 选择跳过，交由助手自行决定**", operatorLabel)
			toastMsg = "已跳过，继续执行"
		}
		updatedCard := map[string]any{
			"schema": "2.0",
			"config": map[string]any{"update_multi": true},
			"header": map[string]any{
				"template": tpl,
				"title": map[string]any{
					"tag":     "plain_text",
					"content": title,
				},
			},
			"body": map[string]any{
				"elements": []any{
					map[string]any{
						"tag":     "markdown",
						"content": fmt.Sprintf("**问题**：%s\n\n%s", qItem.Question, statusLine),
					},
				},
			},
		}
		return &callback.CardActionTriggerResponse{
			Toast: &callback.Toast{
				Type:    "success",
				Content: toastMsg,
			},
			Card: &callback.Card{
				Type: "raw",
				Data: updatedCard,
			},
		}, nil
	}

	if action, _ := val["action"].(string); action == "abort_chat" {
		chatID, _ := val["chat_id"].(string)
		lockKey, _ := val["lock_key"].(string)
		runID, _ := val["run_id"].(string)
		initiatorID, _ := val["initiator_id"].(string)
		if lockKey == "" {
			lockKey = chatID
		}
		isAdmin := !h.approvals.HasAdmins() || h.approvals.IsAdmin(operatorOpenID)
		isInitiator := initiatorID != "" && operatorOpenID == initiatorID
		if !isAdmin && !isInitiator {
			return &callback.CardActionTriggerResponse{
				Toast: &callback.Toast{
					Type:    "error",
					Content: "⛔ 仅管理员或任务发起人可中断此任务",
				},
			}, nil
		}
		var canceled int
		if runID != "" {
			if ok, _ := h.sessions.CancelRun(lockKey, runID, operatorOpenID, isAdmin); ok {
				canceled = 1
			} else if lockKey != chatID {
				if ok, _ := h.sessions.CancelRun(chatID, runID, operatorOpenID, isAdmin); ok {
					canceled = 1
				}
			}
		} else {
			canceled, _ = h.sessions.CancelAllowed(lockKey, operatorOpenID, isAdmin)
			if canceled == 0 && lockKey != chatID {
				canceled, _ = h.sessions.CancelAllowed(chatID, operatorOpenID, isAdmin)
			}
		}
		statusText := "⏹️ **任务已手动中断**"
		if canceled == 0 {
			statusText = "ℹ️ **当前已无正在执行的任务**"
		}
		updatedCard := map[string]any{
			"schema": "2.0",
			"config": map[string]any{"update_multi": true},
			"header": map[string]any{
				"template": "grey",
				"title": map[string]any{
					"tag":     "plain_text",
					"content": "⏹️ 长任务已中断",
				},
			},
			"body": map[string]any{
				"elements": []any{
					map[string]any{
						"tag":     "markdown",
						"content": statusText,
					},
				},
			},
		}
		return &callback.CardActionTriggerResponse{
			Toast: &callback.Toast{
				Type:    "info",
				Content: fmt.Sprintf("已发送中断信号（%d 个任务）", canceled),
			},
			Card: &callback.Card{
				Type: "raw",
				Data: updatedCard,
			},
		}, nil
	}

	approvalID, _ := val["approval_id"].(string)
	chatID, _ := val["chat_id"].(string)
	decision, _ := val["decision"].(string)
	if approvalID == "" || chatID == "" {
		return nil, nil
	}

	wantApprove := decision == "approve"
	found, authorized, item, err := h.approvals.ResolveByID(chatID, approvalID, operatorOpenID, wantApprove)
	if !found {
		return &callback.CardActionTriggerResponse{
			Toast: &callback.Toast{
				Type:    "warning",
				Content: "该审批已处理或已超时",
			},
		}, nil
	}
	if !authorized {
		return &callback.CardActionTriggerResponse{
			Toast: &callback.Toast{
				Type:    "error",
				Content: "⛔ 仅管理员可审批此操作",
			},
		}, nil
	}
	if err != nil {
		return &callback.CardActionTriggerResponse{
			Toast: &callback.Toast{
				Type:    "error",
				Content: fmt.Sprintf("回传审批结果失败：%v", err),
			},
		}, nil
	}

	operatorLabel := h.users.FormatSender(ctx, chatID, operatorOpenID)
	tpl := "grey"
	title := fmt.Sprintf("🚫 已拒绝 (%s)", approvalID)
	statusLine := fmt.Sprintf("🚫 **管理员 %s 已拒绝执行**", operatorLabel)
	toastType := "info"
	toastMsg := "已拒绝执行该操作"
	if wantApprove {
		tpl = "green"
		title = fmt.Sprintf("✅ 已批准 (%s)", approvalID)
		statusLine = fmt.Sprintf("✅ **管理员 %s 已确认放行**，正在继续执行...", operatorLabel)
		toastType = "success"
		toastMsg = "已确认放行"
	}

	updatedBody := fmt.Sprintf("%s\n- **待执行工具**：`%s`\n- **风险原因**：%s", statusLine, item.Tool, item.Reason)
	updatedCard := map[string]any{
		"schema": "2.0",
		"config": map[string]any{
			"update_multi": true,
		},
		"header": map[string]any{
			"template": tpl,
			"title": map[string]any{
				"tag":     "plain_text",
				"content": title,
			},
		},
		"body": map[string]any{
			"elements": []map[string]any{
				{
					"tag":     "markdown",
					"content": updatedBody,
				},
			},
		},
	}

	return &callback.CardActionTriggerResponse{
		Toast: &callback.Toast{
			Type:    toastType,
			Content: toastMsg,
		},
		Card: &callback.Card{
			Type: "raw",
			Data: updatedCard,
		},
	}, nil
}

func (h *MessageHandler) replyApprovalFeedback(ctx context.Context, msgID string, authorized, approved bool, approvalID string, err error) {
	if !authorized {
		h.replyCard(ctx, msgID, "⛔ 无审批权限", "red", fmt.Sprintf("仅管理员可审批操作 `%s`，请由管理员回复确认。", approvalID))
		return
	}
	if err != nil {
		h.replyCard(ctx, msgID, "❌ 审批回传失败", "red", fmt.Sprintf("审批 `%s` 回传给 Agent 时出错：%v", approvalID, err))
		return
	}
	if approved {
		h.replyCard(ctx, msgID, "✅ 已批准执行", "green", fmt.Sprintf("管理员已确认放行 `%s`，Agent 正在继续执行...", approvalID))
	} else {
		h.replyCard(ctx, msgID, "🚫 已拒绝执行", "grey", fmt.Sprintf("管理员已拒绝操作 `%s`，已拦截执行。", approvalID))
	}
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
