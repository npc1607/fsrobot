package agent

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"fsrobot/internal/config"
	"fsrobot/internal/protocol"
	"fsrobot/internal/registry"
)

// AGPProvider implements registry.AgentProvider by speaking the custom
// Agent Gateway Protocol (JSONL) over unix://, tcp://, or stdio://.
type AGPProvider struct {
	meta        registry.AgentMeta
	endpoint    string
	workDir     string
	model       string
	tools       []string
	idleTimeout time.Duration
	reqSeq      atomic.Uint64
}

// NewAGPProvider creates an AGPProvider from AgentConfig.
func NewAGPProvider(ac config.AgentConfig, defaults *config.Config) (*AGPProvider, error) {
	if ac.Endpoint == "" {
		return nil, fmt.Errorf("agent %q (type=agp) requires non-empty endpoint", ac.ID)
	}

	id := ac.ID
	if id == "" {
		id = "gp"
	}
	name := ac.Name
	if name == "" {
		name = "赛博包工头"
	}
	desc := ac.Description
	if desc == "" {
		desc = fmt.Sprintf("基于自定义 AGP 协议接入 (%s)", ac.Endpoint)
	}

	var idleTimeout time.Duration
	switch {
	case ac.TimeoutSec < 0:
		// 显式配置为负数时禁用静默超时
		idleTimeout = 0
	case ac.TimeoutSec > 0:
		idleTimeout = time.Duration(ac.TimeoutSec) * time.Second
	default:
		idleTimeout = defaults.PiTimeout
		if idleTimeout <= 0 {
			idleTimeout = 10 * time.Minute
		}
	}

	workDir := ac.WorkDir
	if workDir == "" {
		workDir = defaults.PiWorkDir
	}

	return &AGPProvider{
		meta: registry.AgentMeta{
			ID:          id,
			Name:        name,
			Description: desc,
			Streaming:   true,
			WorkDir:     workDir,
		},
		endpoint:    ac.Endpoint,
		workDir:     workDir,
		model:       ac.Model,
		tools:       ac.Tools,
		idleTimeout: idleTimeout,
	}, nil
}

func (p *AGPProvider) Meta() registry.AgentMeta {
	return p.meta
}

func (p *AGPProvider) nextID() string {
	n := p.reqSeq.Add(1)
	return fmt.Sprintf("req-%d-%d", time.Now().Unix(), n)
}

func (p *AGPProvider) Execute(ctx context.Context, req registry.RunRequest) (<-chan registry.StreamEvent, error) {
	ch := make(chan registry.StreamEvent, 16)

	go func() {
		defer close(ch)

		workDir := req.Policy.WorkDir
		if workDir == "" {
			workDir = req.WorkDir
		}
		if workDir == "" {
			workDir = p.workDir
		}
		toolsAllowlist := req.Policy.Tools
		if len(toolsAllowlist) == 0 {
			toolsAllowlist = p.tools
		}
		var caps []protocol.CapabilityRule
		for _, c := range req.Policy.Capabilities {
			caps = append(caps, protocol.CapabilityRule{
				Kind:  c.Kind,
				Name:  c.Name,
				Allow: c.Allow,
				Paths: c.Paths,
			})
		}
		policy := &protocol.ExecutionPolicy{
			WorkDir:      workDir,
			Tools:        toolsAllowlist,
			AllowedDirs:  req.Policy.AllowedDirs,
			DeniedPaths:  req.Policy.DeniedPaths,
			AllowBash:    req.Policy.AllowBash,
			AllowWrite:   req.Policy.AllowWrite,
			BashMode:     req.Policy.BashMode,
			Capabilities: caps,
		}
		caller := &protocol.CallerContext{
			SenderID:      req.Caller.SenderID,
			IsBoss:        req.Caller.IsBoss,
			AllowApproval: req.Caller.AllowApproval,
		}

		agpReq := protocol.Request{
			ID:        p.nextID(),
			Op:        protocol.OpRun,
			Client:    "fsrobot/1.0",
			SessionID: req.SessionID,
			Prompt:    req.Prompt,
			Metadata:  req.Metadata,
			Options: &protocol.RunOptions{
				WorkDir: workDir,
				Model:   p.model,
				Tools:   toolsAllowlist,
				// 不再限制单次任务的总墙上时长，由客户端静默超时（idleTimeout）与服务端细粒度超时控制
				TimeoutSec: 0,
				Policy:     policy,
				Caller:     caller,
			},
		}

		if err := p.exchangeWithIdleTimeout(ctx, agpReq, ch, p.idleTimeout); err != nil {
			ch <- registry.StreamEvent{Type: registry.EventError, Err: err}
		}
	}()

	return ch, nil
}

func (p *AGPProvider) ResetSession(ctx context.Context, sessionID string) error {
	execCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	agpReq := protocol.Request{
		ID:        p.nextID(),
		Op:        protocol.OpResetSession,
		Client:    "fsrobot/1.0",
		SessionID: sessionID,
	}

	ch := make(chan registry.StreamEvent, 4)
	go func() {
		for range ch {
		}
	}()
	defer close(ch)

	return p.exchange(execCtx, agpReq, ch)
}

func (p *AGPProvider) exchangeControl(ctx context.Context, agpReq protocol.Request) (*protocol.Event, error) {
	rwc, cleanup, err := p.openTransport(ctx)
	if err != nil {
		return nil, err
	}
	defer cleanup()

	go func() {
		<-ctx.Done()
		_ = rwc.Close()
	}()

	b, err := json.Marshal(agpReq)
	if err != nil {
		return nil, fmt.Errorf("marshal AGP request: %w", err)
	}
	b = append(b, '\n')
	if _, err := rwc.Write(b); err != nil {
		return nil, fmt.Errorf("write AGP request to %s: %w", p.endpoint, err)
	}

	scanner := bufio.NewScanner(rwc)
	scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var ev protocol.Event
		if err := json.Unmarshal([]byte(line), &ev); err != nil {
			continue
		}
		if ev.ID != "" && ev.ID != agpReq.ID {
			continue
		}
		if ev.Event == protocol.EventError {
			return nil, fmt.Errorf("%s: %s", ev.Code, ev.Message)
		}
		return &ev, nil
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return nil, fmt.Errorf("AGP connection closed without response")
}

func (p *AGPProvider) ListSessions(ctx context.Context, sessionID string) (string, []registry.SessionInfo, error) {
	execCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	ev, err := p.exchangeControl(execCtx, protocol.Request{
		ID:        p.nextID(),
		Op:        protocol.OpListSessions,
		Client:    "fsrobot/1.0",
		SessionID: sessionID,
	})
	if err != nil {
		return "", nil, err
	}
	out := make([]registry.SessionInfo, 0, len(ev.Sessions))
	for _, s := range ev.Sessions {
		out = append(out, registry.SessionInfo{
			SessionID:    s.SessionID,
			MessageCount: s.MessageCount,
			FirstPrompt:  s.FirstPrompt,
			UpdatedAt:    s.UpdatedAt,
		})
	}
	return ev.SessionID, out, nil
}

func (p *AGPProvider) ResumeSession(ctx context.Context, sessionID, targetID string) error {
	execCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	_, err := p.exchangeControl(execCtx, protocol.Request{
		ID:        p.nextID(),
		Op:        protocol.OpResumeSession,
		Client:    "fsrobot/1.0",
		SessionID: sessionID,
		TargetID:  targetID,
	})
	return err
}

func (p *AGPProvider) DeleteSession(ctx context.Context, sessionID, targetID string) error {
	execCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	_, err := p.exchangeControl(execCtx, protocol.Request{
		ID:        p.nextID(),
		Op:        protocol.OpDeleteSession,
		Client:    "fsrobot/1.0",
		SessionID: sessionID,
		TargetID:  targetID,
	})
	return err
}

func (p *AGPProvider) GetContextStats(ctx context.Context, sessionID string) (*registry.ContextStats, error) {
	execCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	ev, err := p.exchangeControl(execCtx, protocol.Request{
		ID:        p.nextID(),
		Op:        protocol.OpContextStats,
		Client:    "fsrobot/1.0",
		SessionID: sessionID,
	})
	if err != nil {
		return nil, err
	}
	if ev.ContextStats == nil {
		return &registry.ContextStats{SessionID: ev.SessionID}, nil
	}
	src := ev.ContextStats
	out := &registry.ContextStats{
		SessionID:             src.SessionID,
		MessageCount:          src.MessageCount,
		EpochID:               src.EpochID,
		EpochStride:           src.EpochStride,
		EpochPosition:         src.EpochPosition,
		FreezeBoundary:        src.FreezeBoundary,
		MaskedCount:           src.MaskedCount,
		MaskWindow:            src.MaskWindow,
		LedgerVersion:         src.LedgerVersion,
		LedgerHash:            src.LedgerHash,
		LedgerActiveCnt:       src.LedgerActiveCnt,
		LedgerSuperseded:      src.LedgerSuperseded,
		LedgerResolvedCnt:     src.LedgerResolvedCnt,
		ActiveConstraints:     toRegistryConstraints(src.ActiveConstraints),
		SupersededConstraints: toRegistryConstraints(src.SupersededConstraints),
		RecentTurns:           make([]registry.ContextTurnMetric, 0, len(src.RecentTurns)),
		TotalRecallCalls:      src.TotalRecallCalls,
		TotalRestoreCalls:     src.TotalRestoreCalls,
		TotalTurns:            src.TotalTurns,
	}
	for _, t := range src.RecentTurns {
		out.RecentTurns = append(out.RecentTurns, registry.ContextTurnMetric{
			TurnIndex:        t.TurnIndex,
			StrategyMode:     t.StrategyMode,
			EpochID:          t.EpochID,
			EpochMaskedCount: t.EpochMaskedCount,
			LedgerVersion:    t.LedgerVersion,
			LedgerActiveCnt:  t.LedgerActiveCnt,
			LedgerSuperseded: t.LedgerSuperseded,
			LedgerHash:       t.LedgerHash,
			ContextBuildMs:   t.ContextBuildMs,
			InputTokens:      t.InputTokens,
			CacheReadTokens:  t.CacheReadTokens,
			CacheWriteTokens: t.CacheWriteTokens,
			OutputTokens:     t.OutputTokens,
			RecallCalls:      t.RecallCalls,
			RestoreCalls:     t.RestoreCalls,
			CreatedAt:        t.CreatedAt,
		})
	}
	return out, nil
}

func toRegistryConstraints(in []protocol.ContextConstraintSummary) []registry.ContextConstraintSummary {
	if len(in) == 0 {
		return nil
	}
	out := make([]registry.ContextConstraintSummary, 0, len(in))
	for _, c := range in {
		out = append(out, registry.ContextConstraintSummary{
			ID:           c.ID,
			Category:     c.Category,
			Statement:    c.Statement,
			Status:       c.Status,
			SupersededBy: c.SupersededBy,
			Provenance:   append([]string(nil), c.Provenance...),
		})
	}
	return out
}

func (p *AGPProvider) Close() error {
	return nil
}

func (p *AGPProvider) exchange(ctx context.Context, req protocol.Request, out chan<- registry.StreamEvent) error {
	return p.exchangeWithIdleTimeout(ctx, req, out, 0)
}

func (p *AGPProvider) exchangeWithIdleTimeout(ctx context.Context, req protocol.Request, out chan<- registry.StreamEvent, idleTimeout time.Duration) error {
	rwc, cleanup, err := p.openTransport(ctx)
	if err != nil {
		return err
	}
	defer cleanup()
	return p.exchangeStreamWithIdleTimeout(ctx, rwc, req, out, idleTimeout)
}

func (p *AGPProvider) exchangeStream(ctx context.Context, rwc io.ReadWriteCloser, req protocol.Request, out chan<- registry.StreamEvent) error {
	return p.exchangeStreamWithIdleTimeout(ctx, rwc, req, out, 0)
}

func (p *AGPProvider) exchangeStreamWithIdleTimeout(ctx context.Context, rwc io.ReadWriteCloser, req protocol.Request, out chan<- registry.StreamEvent, idleTimeout time.Duration) error {
	streamCtx, cancelStream := context.WithCancel(ctx)
	defer cancelStream()

	var (
		idleTimedOut atomic.Bool
		timerMu      sync.Mutex
		timerStopped bool
		idleTimer    *time.Timer
	)
	if idleTimeout > 0 {
		idleTimer = time.AfterFunc(idleTimeout, func() {
			idleTimedOut.Store(true)
			cancelStream()
		})
		defer func() {
			timerMu.Lock()
			timerStopped = true
			idleTimer.Stop()
			timerMu.Unlock()
		}()
	}
	resetIdle := func() {
		if idleTimer == nil {
			return
		}
		timerMu.Lock()
		if !timerStopped {
			idleTimer.Reset(idleTimeout)
		}
		timerMu.Unlock()
	}

	// Cancel transport on context cancellation or idle timeout
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-streamCtx.Done():
			_ = rwc.Close()
		case <-done:
		}
	}()

	var writeMu sync.Mutex
	writeFrame := func(r protocol.Request) error {
		b, err := json.Marshal(r)
		if err != nil {
			return fmt.Errorf("marshal AGP request: %w", err)
		}
		b = append(b, '\n')
		writeMu.Lock()
		_, err = rwc.Write(b)
		writeMu.Unlock()
		return err
	}

	if err := writeFrame(req); err != nil {
		return fmt.Errorf("write AGP request to %s: %w", p.endpoint, err)
	}

	scanner := bufio.NewScanner(rwc)
	// Allow up to 4MB per JSONL event line
	scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)

	var accumulated strings.Builder
	gotFinal := false

	var (
		accUsage registry.Usage
		hasUsage bool
	)
	addUsage := func(u *protocol.Usage) {
		if u == nil {
			return
		}
		hasUsage = true
		accUsage.InputTokens += u.InputTokens
		accUsage.OutputTokens += u.OutputTokens
		accUsage.CacheReadTokens += u.CacheReadTokens
	}
	usageSnapshot := func() *registry.Usage {
		if !hasUsage {
			return nil
		}
		u := accUsage
		return &u
	}

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		// 只要服务端持续有事件输出，就重置静默超时计时器
		resetIdle()

		var ev protocol.Event
		if err := json.Unmarshal([]byte(line), &ev); err != nil {
			log.Printf("[Agent:%s] Invalid JSONL from server: %s (%v)", p.meta.ID, line, err)
			continue
		}

		// Filter by request ID if provided
		if ev.ID != "" && ev.ID != req.ID {
			continue
		}

		// Any event may carry token usage; accumulate it for this run.
		addUsage(ev.Usage)

		switch ev.Event {
		case protocol.EventUsage:
			// Dedicated usage frame: consume silently, usage already accumulated.
			continue
		case protocol.EventStarted, protocol.EventThinking:
			out <- registry.StreamEvent{Type: registry.EventThinking, Content: ev.Text}
		case protocol.EventToolStart:
			out <- registry.StreamEvent{Type: registry.EventToolStart, Tool: ev.Tool}
		case protocol.EventToolProgress:
			out <- registry.StreamEvent{Type: registry.EventToolProgress, Tool: ev.Tool, Content: ev.Text, Stalled: ev.Stalled}
		case protocol.EventToolEnd:
			out <- registry.StreamEvent{Type: registry.EventToolEnd, Tool: ev.Tool, Content: ev.Summary}
		case protocol.EventSubagentStart:
			out <- registry.StreamEvent{Type: registry.EventSubagentStart, Agent: ev.AgentName, Content: ev.Text}
		case protocol.EventSubagentEnd:
			var subErr error
			if ev.Message != "" {
				subErr = fmt.Errorf("%s", ev.Message)
			}
			out <- registry.StreamEvent{
				Type:    registry.EventSubagentEnd,
				Agent:   ev.AgentName,
				Content: ev.Text,
				Summary: ev.Summary,
				Err:     subErr,
			}
		case protocol.EventApprovalRequest:
			approvalID := ev.ApprovalID
			out <- registry.StreamEvent{
				Type:       registry.EventApprovalRequest,
				ApprovalID: approvalID,
				Tool:       ev.Tool,
				Content:    ev.Text,
				Args:       ev.Args,
				ReplyApproval: func(approved bool) error {
					resetIdle()
					return writeFrame(protocol.Request{
						ID:         req.ID,
						Op:         protocol.OpApprovalReply,
						ApprovalID: approvalID,
						Approved:   approved,
					})
				},
			}
		case protocol.EventQuestionRequest:
			questionID := ev.QuestionID
			out <- registry.StreamEvent{
				Type:       registry.EventQuestionRequest,
				QuestionID: questionID,
				Content:    ev.Question,
				Options:    ev.Options,
				ReplyQuestion: func(answer string, skipped bool) error {
					resetIdle()
					return writeFrame(protocol.Request{
						ID:         req.ID,
						Op:         protocol.OpQuestionReply,
						QuestionID: questionID,
						Answer:     answer,
						Skipped:    skipped,
					})
				},
			}
		case protocol.EventDelta:
			accumulated.WriteString(ev.Text)
			out <- registry.StreamEvent{Type: registry.EventDelta, Content: ev.Text}
		case protocol.EventFinal:
			gotFinal = true
			finalText := ev.Text
			if finalText == "" {
				finalText = accumulated.String()
			}
			out <- registry.StreamEvent{Type: registry.EventFinal, Content: finalText, Usage: usageSnapshot()}
			return nil
		case protocol.EventResetAck, protocol.EventHelloAck, protocol.EventPong, protocol.EventAbortAck:
			return nil
		case protocol.EventError:
			msg := ev.Message
			if msg == "" {
				msg = ev.Code
			}
			return fmt.Errorf("agent error: %s", msg)
		}
	}

	if idleTimedOut.Load() {
		return fmt.Errorf("agent %s idle timed out after %s (no events received)", p.meta.ID, idleTimeout)
	}

	if err := scanner.Err(); err != nil {
		if ctx.Err() != nil {
			return fmt.Errorf("agent %s canceled/timed out: %w", p.meta.ID, ctx.Err())
		}
		return fmt.Errorf("read AGP stream: %w", err)
	}

	if !gotFinal && accumulated.Len() > 0 {
		out <- registry.StreamEvent{Type: registry.EventFinal, Content: accumulated.String(), Usage: usageSnapshot()}
	}
	return nil
}

func (p *AGPProvider) openTransport(ctx context.Context) (io.ReadWriteCloser, func(), error) {
	switch {
	case strings.HasPrefix(p.endpoint, "unix://"):
		sockPath := strings.TrimPrefix(p.endpoint, "unix://")
		dialCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
		defer cancel()
		var d net.Dialer
		conn, err := d.DialContext(dialCtx, "unix", sockPath)
		if err != nil {
			return nil, func() {}, fmt.Errorf("dial unix socket %s: %w", sockPath, err)
		}
		return conn, func() { _ = conn.Close() }, nil

	case strings.HasPrefix(p.endpoint, "tcp://"):
		addr := strings.TrimPrefix(p.endpoint, "tcp://")
		dialCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
		defer cancel()
		var d net.Dialer
		conn, err := d.DialContext(dialCtx, "tcp", addr)
		if err != nil {
			return nil, func() {}, fmt.Errorf("dial tcp %s: %w", addr, err)
		}
		return conn, func() { _ = conn.Close() }, nil

	case strings.HasPrefix(p.endpoint, "stdio://"):
		cmdLine := strings.TrimSpace(strings.TrimPrefix(p.endpoint, "stdio://"))
		parts := strings.Fields(cmdLine)
		if len(parts) == 0 {
			return nil, func() {}, fmt.Errorf("empty stdio command in endpoint %q", p.endpoint)
		}
		cmd := exec.CommandContext(ctx, parts[0], parts[1:]...)
		if p.workDir != "" {
			cmd.Dir = p.workDir
		}
		stdin, err := cmd.StdinPipe()
		if err != nil {
			return nil, func() {}, err
		}
		stdout, err := cmd.StdoutPipe()
		if err != nil {
			return nil, func() {}, err
		}
		if err := cmd.Start(); err != nil {
			return nil, func() {}, fmt.Errorf("start stdio agent %q: %w", cmdLine, err)
		}
		rwc := &stdioRWC{r: stdout, w: stdin}
		cleanup := func() {
			_ = stdin.Close()
			_ = stdout.Close()
			_ = cmd.Wait()
		}
		return rwc, cleanup, nil

	default:
		return nil, func() {}, fmt.Errorf("unsupported AGP endpoint scheme: %q (use unix://, tcp://, or stdio://)", p.endpoint)
	}
}

type stdioRWC struct {
	r io.ReadCloser
	w io.WriteCloser
}

func (s *stdioRWC) Read(p []byte) (int, error)  { return s.r.Read(p) }
func (s *stdioRWC) Write(p []byte) (int, error) { return s.w.Write(p) }
func (s *stdioRWC) Close() error {
	err1 := s.w.Close()
	err2 := s.r.Close()
	if err1 != nil {
		return err1
	}
	return err2
}
