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
	"sync/atomic"
	"time"

	"fsrobot/internal/config"
	"fsrobot/internal/protocol"
	"fsrobot/internal/registry"
)

// AGPProvider implements registry.AgentProvider by speaking the custom
// Agent Gateway Protocol (JSONL) over unix://, tcp://, or stdio://.
type AGPProvider struct {
	meta     registry.AgentMeta
	endpoint string
	workDir  string
	model    string
	tools    []string
	timeout  time.Duration
	reqSeq   atomic.Uint64
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
		name = "GP Agent"
	}
	desc := ac.Description
	if desc == "" {
		desc = fmt.Sprintf("基于自定义 AGP 协议接入 (%s)", ac.Endpoint)
	}

	timeout := defaults.PiTimeout
	if ac.TimeoutSec > 0 {
		timeout = time.Duration(ac.TimeoutSec) * time.Second
	}
	if timeout <= 0 {
		timeout = 5 * time.Minute
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
		endpoint: ac.Endpoint,
		workDir:  workDir,
		model:    ac.Model,
		tools:    ac.Tools,
		timeout:  timeout,
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

		execCtx, cancel := context.WithTimeout(ctx, p.timeout)
		defer cancel()

		workDir := req.WorkDir
		if workDir == "" {
			workDir = p.workDir
		}

		agpReq := protocol.Request{
			ID:        p.nextID(),
			Op:        protocol.OpRun,
			Client:    "fsrobot/1.0",
			SessionID: req.SessionID,
			Prompt:    req.Prompt,
			Metadata:  req.Metadata,
			Options: &protocol.RunOptions{
				WorkDir:    workDir,
				Model:      p.model,
				Tools:      p.tools,
				TimeoutSec: int(p.timeout.Seconds()),
			},
		}

		if err := p.exchange(execCtx, agpReq, ch); err != nil {
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

func (p *AGPProvider) Close() error {
	return nil
}

func (p *AGPProvider) exchange(ctx context.Context, req protocol.Request, out chan<- registry.StreamEvent) error {
	rwc, cleanup, err := p.openTransport(ctx)
	if err != nil {
		return err
	}
	defer cleanup()
	return p.exchangeStream(ctx, rwc, req, out)
}

func (p *AGPProvider) exchangeStream(ctx context.Context, rwc io.ReadWriteCloser, req protocol.Request, out chan<- registry.StreamEvent) error {

	// Cancel transport on context cancellation
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			_ = rwc.Close()
		case <-done:
		}
	}()

	payload, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshal AGP request: %w", err)
	}
	payload = append(payload, '\n')

	if _, err := rwc.Write(payload); err != nil {
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
		var d net.Dialer
		conn, err := d.DialContext(ctx, "unix", sockPath)
		if err != nil {
			return nil, func() {}, fmt.Errorf("dial unix socket %s: %w", sockPath, err)
		}
		return conn, func() { _ = conn.Close() }, nil

	case strings.HasPrefix(p.endpoint, "tcp://"):
		addr := strings.TrimPrefix(p.endpoint, "tcp://")
		var d net.Dialer
		conn, err := d.DialContext(ctx, "tcp", addr)
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
