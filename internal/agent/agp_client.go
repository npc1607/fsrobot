package agent

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
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

const (
	agpClientName        = "fsrobot/1.0"
	agpHelloTimeout      = 5 * time.Second
	agpAbortTimeout      = 2 * time.Second
	agpAbortSettleWait   = 300 * time.Millisecond
	agpControlTimeout    = 15 * time.Second
	agpScannerMaxBytes   = 4 * 1024 * 1024
	agpStreamBufferSize  = 64
	agpPipeAbortDeadline = 30 * time.Millisecond
)

var (
	errSessionClosed  = errors.New("agp session closed")
	errProviderClosed = errors.New("agp provider closed")
)

// AGPProvider implements registry.AgentProvider by speaking the custom
// Agent Gateway Protocol (JSONL) over unix://, tcp://, or stdio:// using
// a persistent, OpHello-handshaked, multiplexed connection.
type AGPProvider struct {
	meta        registry.AgentMeta
	endpoint    string
	workDir     string
	model       string
	tools       []string
	idleTimeout time.Duration
	reqSeq      atomic.Uint64

	procCtx    context.Context
	procCancel context.CancelFunc

	connMu  sync.Mutex
	session *agpSession
	closed  bool
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

	procCtx, procCancel := context.WithCancel(context.Background())

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
		procCtx:     procCtx,
		procCancel:  procCancel,
	}, nil
}

func (p *AGPProvider) Meta() registry.AgentMeta {
	return p.meta
}

func (p *AGPProvider) nextID() string {
	n := p.reqSeq.Add(1)
	return fmt.Sprintf("req-%d-%d", time.Now().Unix(), n)
}

func (p *AGPProvider) getOrConnect(ctx context.Context) (*agpSession, error) {
	p.connMu.Lock()
	defer p.connMu.Unlock()

	if p.closed {
		return nil, errProviderClosed
	}
	if p.session != nil && !p.session.isClosed() {
		return p.session, nil
	}

	rwc, cleanup, err := p.openTransport(ctx)
	if err != nil {
		return nil, err
	}

	sess := newAGPSession(p.meta.ID, rwc, cleanup, p.nextID)
	helloCtx, cancel := context.WithTimeout(ctx, agpHelloTimeout)
	defer cancel()
	if err := sess.hello(helloCtx); err != nil {
		_ = sess.Close()
		return nil, fmt.Errorf("AGP handshake failed: %w", err)
	}

	p.session = sess
	return sess, nil
}

func (p *AGPProvider) dropSession(sess *agpSession) {
	if sess == nil {
		return
	}
	p.connMu.Lock()
	if p.session == sess {
		p.session = nil
	}
	p.connMu.Unlock()
	_ = sess.Close()
}

func (p *AGPProvider) buildRunRequest(req registry.RunRequest) protocol.Request {
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

	return protocol.Request{
		ID:        p.nextID(),
		Op:        protocol.OpRun,
		Client:    agpClientName,
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
}

func (p *AGPProvider) Execute(ctx context.Context, req registry.RunRequest) (<-chan registry.StreamEvent, error) {
	ch := make(chan registry.StreamEvent, 16)

	go func() {
		defer close(ch)

		agpReq := p.buildRunRequest(req)
		if err := p.exchangeWithIdleTimeout(ctx, agpReq, ch, p.idleTimeout); err != nil {
			ch <- registry.StreamEvent{Type: registry.EventError, Err: err}
		}
	}()

	return ch, nil
}

func (p *AGPProvider) ResetSession(ctx context.Context, sessionID string) error {
	execCtx, cancel := context.WithTimeout(ctx, agpControlTimeout)
	defer cancel()

	_, err := p.exchangeControl(execCtx, protocol.Request{
		ID:        p.nextID(),
		Op:        protocol.OpResetSession,
		Client:    agpClientName,
		SessionID: sessionID,
	})
	return err
}

func (p *AGPProvider) exchangeControl(ctx context.Context, agpReq protocol.Request) (*protocol.Event, error) {
	for attempt := 0; attempt < 2; attempt++ {
		sess, err := p.getOrConnect(ctx)
		if err != nil {
			return nil, err
		}
		ev, err := sess.request(ctx, agpReq)
		if err != nil {
			if attempt == 0 && errors.Is(err, errSessionClosed) && ctx.Err() == nil {
				p.dropSession(sess)
				continue
			}
			return nil, err
		}
		if ev.Event == protocol.EventError {
			return nil, fmt.Errorf("%s: %s", ev.Code, ev.Message)
		}
		return ev, nil
	}
	return nil, errSessionClosed
}

func (p *AGPProvider) ListSessions(ctx context.Context, sessionID string) (string, []registry.SessionInfo, error) {
	execCtx, cancel := context.WithTimeout(ctx, agpControlTimeout)
	defer cancel()
	ev, err := p.exchangeControl(execCtx, protocol.Request{
		ID:        p.nextID(),
		Op:        protocol.OpListSessions,
		Client:    agpClientName,
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
	execCtx, cancel := context.WithTimeout(ctx, agpControlTimeout)
	defer cancel()
	_, err := p.exchangeControl(execCtx, protocol.Request{
		ID:        p.nextID(),
		Op:        protocol.OpResumeSession,
		Client:    agpClientName,
		SessionID: sessionID,
		TargetID:  targetID,
	})
	return err
}

func (p *AGPProvider) DeleteSession(ctx context.Context, sessionID, targetID string) error {
	execCtx, cancel := context.WithTimeout(ctx, agpControlTimeout)
	defer cancel()
	_, err := p.exchangeControl(execCtx, protocol.Request{
		ID:        p.nextID(),
		Op:        protocol.OpDeleteSession,
		Client:    agpClientName,
		SessionID: sessionID,
		TargetID:  targetID,
	})
	return err
}

func (p *AGPProvider) GetContextStats(ctx context.Context, sessionID string) (*registry.ContextStats, error) {
	execCtx, cancel := context.WithTimeout(ctx, agpControlTimeout)
	defer cancel()
	ev, err := p.exchangeControl(execCtx, protocol.Request{
		ID:        p.nextID(),
		Op:        protocol.OpContextStats,
		Client:    agpClientName,
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

// Close shuts down any active persistent AGP connection and associated stdio subprocess.
func (p *AGPProvider) Close() error {
	p.connMu.Lock()
	p.closed = true
	sess := p.session
	p.session = nil
	p.connMu.Unlock()

	if p.procCancel != nil {
		p.procCancel()
	}
	if sess != nil {
		return sess.Close()
	}
	return nil
}

func (p *AGPProvider) exchange(ctx context.Context, req protocol.Request, out chan<- registry.StreamEvent) error {
	return p.exchangeWithIdleTimeout(ctx, req, out, 0)
}

func (p *AGPProvider) exchangeWithIdleTimeout(ctx context.Context, req protocol.Request, out chan<- registry.StreamEvent, idleTimeout time.Duration) error {
	for attempt := 0; attempt < 2; attempt++ {
		sess, err := p.getOrConnect(ctx)
		if err != nil {
			return err
		}
		receivedAny, err := sess.run(ctx, req, out, idleTimeout)
		if err != nil && !receivedAny && attempt == 0 && errors.Is(err, errSessionClosed) && ctx.Err() == nil {
			p.dropSession(sess)
			continue
		}
		return err
	}
	return errSessionClosed
}

// exchangeStream executes one AGP request on a caller-provided transport (used in tests).
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

	// On cancellation or idle timeout, send an explicit OpAbort frame before closing the transport.
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-streamCtx.Done():
			if req.Op == protocol.OpRun {
				if dl, ok := rwc.(interface{ SetWriteDeadline(time.Time) error }); ok {
					_ = dl.SetWriteDeadline(time.Now().Add(agpPipeAbortDeadline))
				}
				_ = writeFrame(protocol.Request{
					ID:       p.nextID(),
					Op:       protocol.OpAbort,
					Client:   agpClientName,
					TargetID: req.ID,
				})
				if dl, ok := rwc.(interface{ SetWriteDeadline(time.Time) error }); ok {
					_ = dl.SetWriteDeadline(time.Time{})
				}
			}
			_ = rwc.Close()
		case <-done:
		}
	}()

	if err := writeFrame(req); err != nil {
		return fmt.Errorf("write AGP request to %s: %w", p.endpoint, err)
	}

	scanner := bufio.NewScanner(rwc)
	scanner.Buffer(make([]byte, 64*1024), agpScannerMaxBytes)

	state := newStreamAccumulator(p.meta.ID, req.ID, out, writeFrame, resetIdle)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		resetIdle()

		var ev protocol.Event
		if err := json.Unmarshal([]byte(line), &ev); err != nil {
			log.Printf("[Agent:%s] Invalid JSONL from server: %s (%v)", p.meta.ID, line, err)
			continue
		}
		if ev.ID != "" && ev.ID != req.ID {
			continue
		}

		finished, err := state.handle(ev)
		if err != nil {
			return err
		}
		if finished {
			return nil
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
	state.flushAccumulatedOnEOF()
	return nil
}

// agpStream holds a single multiplexed OpRun event stream on top of agpSession.
type agpStream struct {
	mu     sync.Mutex
	ch     chan protocol.Event
	done   chan struct{}
	closed bool
}

func newAGPStream() *agpStream {
	return &agpStream{
		ch:   make(chan protocol.Event, agpStreamBufferSize),
		done: make(chan struct{}),
	}
}

func (s *agpStream) push(ev protocol.Event) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.ch <- ev
}

func (s *agpStream) close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.closed = true
	close(s.ch)
	close(s.done)
}

// agpSession multiplexes control waiters and concurrent OpRun streams over a single
// long-lived AGP transport initialized with an OpHello handshake.
type agpSession struct {
	agentID string
	nextID  func() string
	rwc     io.ReadWriteCloser
	cleanup func()

	writeMu   sync.Mutex
	closeOnce sync.Once

	mu      sync.Mutex
	streams map[string]*agpStream
	waiters map[string]chan protocol.Event
	closed  bool
	err     error
}

func newAGPSession(agentID string, rwc io.ReadWriteCloser, cleanup func(), nextID func() string) *agpSession {
	s := &agpSession{
		agentID: agentID,
		nextID:  nextID,
		rwc:     rwc,
		cleanup: cleanup,
		streams: make(map[string]*agpStream),
		waiters: make(map[string]chan protocol.Event),
	}
	go s.readLoop()
	return s
}

func (s *agpSession) isClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

func (s *agpSession) Close() error {
	var err error
	s.closeOnce.Do(func() {
		s.fail(errSessionClosed)
		if s.cleanup != nil {
			s.cleanup()
		} else if s.rwc != nil {
			err = s.rwc.Close()
		}
	})
	return err
}

func (s *agpSession) fail(err error) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	s.err = err
	streams := s.streams
	waiters := s.waiters
	s.streams = make(map[string]*agpStream)
	s.waiters = make(map[string]chan protocol.Event)
	s.mu.Unlock()

	for _, st := range streams {
		st.close()
	}
	for _, ch := range waiters {
		close(ch)
	}
}

func (s *agpSession) readLoop() {
	scanner := bufio.NewScanner(s.rwc)
	scanner.Buffer(make([]byte, 64*1024), agpScannerMaxBytes)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var ev protocol.Event
		if err := json.Unmarshal([]byte(line), &ev); err != nil {
			log.Printf("[Agent:%s] Invalid JSONL from server: %s (%v)", s.agentID, line, err)
			continue
		}
		s.route(ev)
	}

	err := scanner.Err()
	if err == nil {
		err = errSessionClosed
	} else {
		err = fmt.Errorf("%w: %v", errSessionClosed, err)
	}
	s.fail(err)
}

func (s *agpSession) route(ev protocol.Event) {
	s.mu.Lock()
	st := s.streams[ev.ID]
	w := s.waiters[ev.ID]
	if w != nil {
		delete(s.waiters, ev.ID)
	}
	isTerm := isTerminalStreamEvent(ev.Event)
	if st != nil && isTerm {
		delete(s.streams, ev.ID)
	}
	s.mu.Unlock()

	if st != nil {
		st.push(ev)
		if isTerm {
			st.close()
		}
		return
	}
	if w != nil {
		w <- ev
	}
}

func (s *agpSession) send(req protocol.Request) error {
	b, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshal AGP request: %w", err)
	}
	b = append(b, '\n')

	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	if s.isClosed() {
		return errSessionClosed
	}
	if _, err := s.rwc.Write(b); err != nil {
		return fmt.Errorf("%w: write AGP request: %v", errSessionClosed, err)
	}
	return nil
}

func (s *agpSession) request(ctx context.Context, req protocol.Request) (*protocol.Event, error) {
	ch := make(chan protocol.Event, 1)

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, errSessionClosed
	}
	s.waiters[req.ID] = ch
	s.mu.Unlock()

	if err := s.send(req); err != nil {
		s.mu.Lock()
		delete(s.waiters, req.ID)
		s.mu.Unlock()
		return nil, err
	}

	select {
	case ev, ok := <-ch:
		if !ok {
			return nil, errSessionClosed
		}
		return &ev, nil
	case <-ctx.Done():
		s.mu.Lock()
		delete(s.waiters, req.ID)
		s.mu.Unlock()
		return nil, ctx.Err()
	}
}

func (s *agpSession) hello(ctx context.Context) error {
	ev, err := s.request(ctx, protocol.Request{
		ID:     s.nextID(),
		Op:     protocol.OpHello,
		Client: agpClientName,
	})
	if err != nil {
		return err
	}
	if ev.Event == protocol.EventError {
		return fmt.Errorf("%s: %s", ev.Code, ev.Message)
	}
	if ev.Event != protocol.EventHelloAck {
		return fmt.Errorf("unexpected handshake event %q", ev.Event)
	}
	return nil
}

func (s *agpSession) abort(ctx context.Context, targetID string) error {
	if targetID == "" {
		return nil
	}
	ev, err := s.request(ctx, protocol.Request{
		ID:       s.nextID(),
		Op:       protocol.OpAbort,
		Client:   agpClientName,
		TargetID: targetID,
	})
	if err != nil {
		return err
	}
	if ev.Event == protocol.EventError {
		return fmt.Errorf("%s: %s", ev.Code, ev.Message)
	}
	return nil
}

func (s *agpSession) removeStream(id string) {
	s.mu.Lock()
	st := s.streams[id]
	delete(s.streams, id)
	s.mu.Unlock()
	if st != nil {
		st.close()
	}
}

func (s *agpSession) run(ctx context.Context, req protocol.Request, out chan<- registry.StreamEvent, idleTimeout time.Duration) (bool, error) {
	st := newAGPStream()

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return false, errSessionClosed
	}
	s.streams[req.ID] = st
	s.mu.Unlock()

	if err := s.send(req); err != nil {
		s.removeStream(req.ID)
		return false, err
	}

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

	state := newStreamAccumulator(s.agentID, req.ID, out, s.send, resetIdle)
	receivedAny := false

	for {
		select {
		case ev, ok := <-st.ch:
			if !ok {
				if idleTimedOut.Load() {
					return receivedAny, fmt.Errorf("agent %s idle timed out after %s (no events received)", s.agentID, idleTimeout)
				}
				if ctx.Err() != nil {
					return receivedAny, fmt.Errorf("agent %s canceled/timed out: %w", s.agentID, ctx.Err())
				}
				if state.flushAccumulatedOnEOF() {
					return receivedAny, nil
				}
				return receivedAny, errSessionClosed
			}
			receivedAny = true
			resetIdle()
			finished, err := state.handle(ev)
			if err != nil {
				s.removeStream(req.ID)
				return receivedAny, err
			}
			if finished {
				s.removeStream(req.ID)
				return receivedAny, nil
			}

		case <-streamCtx.Done():
			abortCtx, cancelAbort := context.WithTimeout(context.Background(), agpAbortTimeout)
			_ = s.abort(abortCtx, req.ID)
			cancelAbort()

			// Give the server a brief settle window to wrap up the aborted run cleanly.
			settle := time.NewTimer(agpAbortSettleWait)
			select {
			case <-st.done:
			case <-settle.C:
			}
			settle.Stop()
			s.removeStream(req.ID)

			if idleTimedOut.Load() {
				return receivedAny, fmt.Errorf("agent %s idle timed out after %s (no events received)", s.agentID, idleTimeout)
			}
			return receivedAny, fmt.Errorf("agent %s canceled/timed out: %w", s.agentID, ctx.Err())
		}
	}
}

func isTerminalStreamEvent(eventType string) bool {
	switch eventType {
	case protocol.EventFinal, protocol.EventError,
		protocol.EventResetAck, protocol.EventHelloAck,
		protocol.EventPong, protocol.EventAbortAck:
		return true
	default:
		return false
	}
}

// streamAccumulator handles per-run AGP event translation, delta buffering, usage accumulation,
// and approval/question reply callbacks across both persistent and single-shot streams.
type streamAccumulator struct {
	agentID     string
	reqID       string
	out         chan<- registry.StreamEvent
	writeFrame  func(protocol.Request) error
	resetIdle   func()
	accumulated strings.Builder
	gotFinal    bool
	accUsage    registry.Usage
	hasUsage    bool
}

func newStreamAccumulator(
	agentID, reqID string,
	out chan<- registry.StreamEvent,
	writeFrame func(protocol.Request) error,
	resetIdle func(),
) *streamAccumulator {
	return &streamAccumulator{
		agentID:    agentID,
		reqID:      reqID,
		out:        out,
		writeFrame: writeFrame,
		resetIdle:  resetIdle,
	}
}

func (a *streamAccumulator) addUsage(u *protocol.Usage) {
	if u == nil {
		return
	}
	a.hasUsage = true
	a.accUsage.InputTokens += u.InputTokens
	a.accUsage.OutputTokens += u.OutputTokens
	a.accUsage.CacheReadTokens += u.CacheReadTokens
}

func (a *streamAccumulator) usageSnapshot() *registry.Usage {
	if !a.hasUsage {
		return nil
	}
	u := a.accUsage
	return &u
}

func (a *streamAccumulator) handle(ev protocol.Event) (bool, error) {
	a.addUsage(ev.Usage)

	switch ev.Event {
	case protocol.EventUsage:
		return false, nil
	case protocol.EventStarted, protocol.EventThinking:
		a.out <- registry.StreamEvent{Type: registry.EventThinking, Content: ev.Text}
	case protocol.EventToolStart:
		a.out <- registry.StreamEvent{Type: registry.EventToolStart, Tool: ev.Tool}
	case protocol.EventToolProgress:
		a.out <- registry.StreamEvent{Type: registry.EventToolProgress, Tool: ev.Tool, Content: ev.Text, Stalled: ev.Stalled}
	case protocol.EventToolEnd:
		a.out <- registry.StreamEvent{Type: registry.EventToolEnd, Tool: ev.Tool, Content: ev.Summary}
	case protocol.EventSubagentStart:
		a.out <- registry.StreamEvent{Type: registry.EventSubagentStart, Agent: ev.AgentName, Content: ev.Text}
	case protocol.EventSubagentEnd:
		var subErr error
		if ev.Message != "" {
			subErr = fmt.Errorf("%s", ev.Message)
		}
		a.out <- registry.StreamEvent{
			Type:    registry.EventSubagentEnd,
			Agent:   ev.AgentName,
			Content: ev.Text,
			Summary: ev.Summary,
			Err:     subErr,
		}
	case protocol.EventApprovalRequest:
		approvalID := ev.ApprovalID
		a.out <- registry.StreamEvent{
			Type:       registry.EventApprovalRequest,
			ApprovalID: approvalID,
			Tool:       ev.Tool,
			Content:    ev.Text,
			Args:       ev.Args,
			ReplyApproval: func(approved bool) error {
				a.resetIdle()
				return a.writeFrame(protocol.Request{
					ID:         a.reqID,
					Op:         protocol.OpApprovalReply,
					ApprovalID: approvalID,
					Approved:   approved,
				})
			},
		}
	case protocol.EventQuestionRequest:
		questionID := ev.QuestionID
		a.out <- registry.StreamEvent{
			Type:       registry.EventQuestionRequest,
			QuestionID: questionID,
			Content:    ev.Question,
			Options:    ev.Options,
			ReplyQuestion: func(answer string, skipped bool) error {
				a.resetIdle()
				return a.writeFrame(protocol.Request{
					ID:         a.reqID,
					Op:         protocol.OpQuestionReply,
					QuestionID: questionID,
					Answer:     answer,
					Skipped:    skipped,
				})
			},
		}
	case protocol.EventDelta:
		a.accumulated.WriteString(ev.Text)
		a.out <- registry.StreamEvent{Type: registry.EventDelta, Content: ev.Text}
	case protocol.EventFinal:
		a.gotFinal = true
		finalText := ev.Text
		if finalText == "" {
			finalText = a.accumulated.String()
		}
		a.out <- registry.StreamEvent{Type: registry.EventFinal, Content: finalText, Usage: a.usageSnapshot()}
		return true, nil
	case protocol.EventResetAck, protocol.EventHelloAck, protocol.EventPong, protocol.EventAbortAck:
		return true, nil
	case protocol.EventError:
		msg := ev.Message
		if msg == "" {
			msg = ev.Code
		}
		return true, fmt.Errorf("agent error: %s", msg)
	}
	return false, nil
}

func (a *streamAccumulator) flushAccumulatedOnEOF() bool {
	if !a.gotFinal && a.accumulated.Len() > 0 {
		a.out <- registry.StreamEvent{Type: registry.EventFinal, Content: a.accumulated.String(), Usage: a.usageSnapshot()}
		return true
	}
	return false
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
		procCtx := p.procCtx
		if procCtx == nil {
			procCtx = ctx
		}
		cmd := exec.CommandContext(procCtx, parts[0], parts[1:]...)
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
