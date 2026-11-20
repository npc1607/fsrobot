package agent

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"sync"
	"time"

	"fsrobot/internal/config"
	"fsrobot/internal/registry"
)

var (
	ansiRegexp             = regexp.MustCompile(`\x1b\[[0-9;]*[a-zA-Z]`)
	piSessionInvalidRegexp = regexp.MustCompile(`[^a-zA-Z0-9._-]+`)
)

// PiProvider implements registry.AgentProvider by invoking the pi coding agent CLI in JSON streaming mode.
type PiProvider struct {
	meta          registry.AgentMeta
	binPath       string
	model         string
	thinking      string
	workDir       string
	tools         []string
	systemPrompt  string
	timeout       time.Duration
	enableSession bool
	sessionDir    string

	mu         sync.Mutex
	sessionGen map[string]int // increments on ResetSession so pi starts a fresh session
}

// NewPiProvider creates a PiProvider from AgentConfig and global defaults.
func NewPiProvider(ac config.AgentConfig, defaults *config.Config) (*PiProvider, error) {
	binPath := ac.Bin
	if binPath == "" {
		binPath = defaults.PiBin
	}
	if binPath == "" {
		binPath = "pi"
	}
	if _, err := exec.LookPath(binPath); err != nil {
		return nil, fmt.Errorf("pi executable %q not found in PATH: %w", binPath, err)
	}

	id := ac.ID
	if id == "" {
		id = "pi"
	}
	name := ac.Name
	if name == "" {
		name = "Pi Agent"
	}
	desc := ac.Description
	if desc == "" {
		desc = "本地 Pi CLI Agent（支持流式输出、工具调用与多轮会话）"
	}

	timeout := defaults.PiTimeout
	if ac.TimeoutSec > 0 {
		timeout = time.Duration(ac.TimeoutSec) * time.Second
	}
	if timeout <= 0 {
		timeout = 30 * time.Minute
	}

	enableSession := defaults.EnableSession
	if ac.EnableSession != nil {
		enableSession = *ac.EnableSession
	}

	sessionDir := ac.SessionDir
	if sessionDir == "" {
		sessionDir = defaults.SessionDir
	}
	if enableSession && sessionDir != "" {
		_ = os.MkdirAll(sessionDir, 0755)
	}

	workDir := ac.WorkDir
	if workDir == "" {
		workDir = defaults.PiWorkDir
	}
	model := ac.Model
	if model == "" {
		model = defaults.PiModel
	}
	thinking := ac.Thinking
	if thinking == "" {
		thinking = defaults.PiThinking
	}
	sysPrompt := ac.SystemPrompt
	if sysPrompt == "" {
		sysPrompt = defaults.AppendSystemPrompt
	}

	return &PiProvider{
		meta: registry.AgentMeta{
			ID:          id,
			Name:        name,
			Description: desc,
			Streaming:   true,
			WorkDir:     workDir,
		},
		binPath:       binPath,
		model:         model,
		thinking:      thinking,
		workDir:       workDir,
		tools:         ac.Tools,
		systemPrompt:  sysPrompt,
		timeout:       timeout,
		enableSession: enableSession,
		sessionDir:    sessionDir,
		sessionGen:    make(map[string]int),
	}, nil
}

func (p *PiProvider) Meta() registry.AgentMeta {
	return p.meta
}

func (p *PiProvider) effectiveSessionID(sessionID string) string {
	clean := sanitizePiSessionID(sessionID)
	p.mu.Lock()
	defer p.mu.Unlock()
	gen := p.sessionGen[clean]
	if gen == 0 {
		return clean
	}
	return fmt.Sprintf("%s_g%d", clean, gen)
}

func sanitizePiSessionID(raw string) string {
	s := piSessionInvalidRegexp.ReplaceAllString(strings.TrimSpace(raw), "_")
	s = strings.Trim(s, "._-")
	if s == "" {
		return "default"
	}
	return s
}

func (p *PiProvider) ResetSession(_ context.Context, sessionID string) error {
	clean := sanitizePiSessionID(sessionID)
	p.mu.Lock()
	defer p.mu.Unlock()
	p.sessionGen[clean]++
	return nil
}

func (p *PiProvider) Close() error {
	return nil
}

// Execute runs pi in `--mode json -p` streaming mode and forwards events onto the returned channel.
func (p *PiProvider) Execute(ctx context.Context, req registry.RunRequest) (<-chan registry.StreamEvent, error) {
	ch := make(chan registry.StreamEvent, 16)

	go func() {
		defer close(ch)
		if err := p.runStreaming(ctx, req, ch); err != nil {
			ch <- registry.StreamEvent{Type: registry.EventError, Err: err}
		}
	}()

	return ch, nil
}

func (p *PiProvider) runStreaming(ctx context.Context, req registry.RunRequest, out chan<- registry.StreamEvent) error {
	execCtx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()

	args := []string{"--mode", "json", "-p"}

	if p.enableSession && req.SessionID != "" {
		effID := p.effectiveSessionID(req.SessionID)
		args = append(args, "--session-id", effID, "--session-dir", p.sessionDir)
	} else {
		args = append(args, "--no-session")
	}

	if p.model != "" {
		args = append(args, "--model", p.model)
	}
	if p.thinking != "" {
		args = append(args, "--thinking", p.thinking)
	}
	if len(p.tools) > 0 {
		args = append(args, "--tools", strings.Join(p.tools, ","))
	}
	if p.systemPrompt != "" {
		args = append(args, "--append-system-prompt", p.systemPrompt)
	}

	args = append(args, "--", req.Prompt)

	cmd := exec.CommandContext(execCtx, p.binPath, args...)
	workDir := req.WorkDir
	if workDir == "" {
		workDir = p.workDir
	}
	if workDir != "" {
		cmd.Dir = workDir
	}

	cmd.Env = append(os.Environ(), "PI_ORCHESTRATOR_SUBAGENT=1")

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("pi stdout pipe: %w", err)
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	log.Printf("[Agent:%s] Running %s --mode json -p (session=%s promptLen=%d)", p.meta.ID, p.binPath, req.SessionID, len(req.Prompt))
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start pi: %w", err)
	}

	lastText, lastErrMsg, lastUsage := parsePiJSONStream(stdout, out)

	waitErr := cmd.Wait()
	errStr := cleanOutput(stderr.String())

	if waitErr != nil {
		if execCtx.Err() == context.DeadlineExceeded {
			return fmt.Errorf("agent %s timed out after %s", p.meta.ID, p.timeout)
		}
		if execCtx.Err() == context.Canceled {
			return execCtx.Err()
		}
		if strings.TrimSpace(lastText) != "" {
			log.Printf("[Agent:%s] Command exited with %v but produced output", p.meta.ID, waitErr)
			out <- registry.StreamEvent{Type: registry.EventFinal, Content: lastText, Usage: lastUsage}
			return nil
		}
		if lastErrMsg != "" {
			return fmt.Errorf("agent %s error: %s", p.meta.ID, lastErrMsg)
		}
		return fmt.Errorf("agent %s failed (%w): %s", p.meta.ID, waitErr, errStr)
	}

	if lastErrMsg != "" && strings.TrimSpace(lastText) == "" {
		return fmt.Errorf("agent %s error: %s", p.meta.ID, lastErrMsg)
	}

	out <- registry.StreamEvent{Type: registry.EventFinal, Content: lastText, Usage: lastUsage}
	return nil
}

type piJSONUsage struct {
	Input     int `json:"input,omitempty"`
	Output    int `json:"output,omitempty"`
	CacheRead int `json:"cacheRead,omitempty"`
}

type piJSONEvent struct {
	Type                  string       `json:"type"`
	ToolName              string       `json:"toolName,omitempty"`
	ToolCallID            string       `json:"toolCallId,omitempty"`
	IsError               bool         `json:"isError,omitempty"`
	DurationMs            int64        `json:"durationMs,omitempty"`
	Usage                 *piJSONUsage `json:"usage,omitempty"`
	AssistantMessageEvent *struct {
		Type  string `json:"type"`
		Delta string `json:"delta,omitempty"`
	} `json:"assistantMessageEvent,omitempty"`
	Message *struct {
		Role         string       `json:"role"`
		StopReason   string       `json:"stopReason,omitempty"`
		ErrorMessage string       `json:"errorMessage,omitempty"`
		Usage        *piJSONUsage `json:"usage,omitempty"`
		Content      []struct {
			Type string `json:"type"`
			Text string `json:"text,omitempty"`
		} `json:"content,omitempty"`
	} `json:"message,omitempty"`
	Result *struct {
		Content []struct {
			Type string `json:"type"`
			Text string `json:"text,omitempty"`
		} `json:"content,omitempty"`
	} `json:"result,omitempty"`
}

func isPiSubagentTool(name string) bool {
	lower := strings.ToLower(strings.TrimSpace(name))
	return lower == "subagent" || lower == "orchestrate" || lower == "agent" || strings.Contains(lower, "subagent")
}

func parsePiJSONStream(r io.Reader, out chan<- registry.StreamEvent) (lastText string, lastErr string, lastUsage *registry.Usage) {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)

	var deltaAcc strings.Builder

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var ev piJSONEvent
		if err := json.Unmarshal([]byte(line), &ev); err != nil {
			continue
		}

		if ev.Usage != nil && (ev.Usage.Input > 0 || ev.Usage.Output > 0) {
			lastUsage = &registry.Usage{
				InputTokens:     ev.Usage.Input,
				OutputTokens:    ev.Usage.Output,
				CacheReadTokens: ev.Usage.CacheRead,
			}
		}

		switch ev.Type {
		case "message_update":
			if ev.AssistantMessageEvent != nil && ev.AssistantMessageEvent.Type == "text_delta" && ev.AssistantMessageEvent.Delta != "" {
				deltaAcc.WriteString(ev.AssistantMessageEvent.Delta)
				out <- registry.StreamEvent{Type: registry.EventDelta, Content: ev.AssistantMessageEvent.Delta}
			}
		case "tool_execution_start":
			if isPiSubagentTool(ev.ToolName) {
				out <- registry.StreamEvent{
					Type:  registry.EventSubagentStart,
					Agent: ev.ToolName,
				}
			} else if ev.ToolName != "" {
				out <- registry.StreamEvent{
					Type: registry.EventToolStart,
					Tool: ev.ToolName,
				}
			}
		case "tool_execution_end":
			var resultBuf strings.Builder
			if ev.Result != nil {
				for _, c := range ev.Result.Content {
					if c.Type == "text" && c.Text != "" {
						resultBuf.WriteString(c.Text)
					}
				}
			}
			dur := ""
			if ev.DurationMs > 0 {
				dur = (time.Duration(ev.DurationMs) * time.Millisecond).Round(time.Millisecond).String()
			}
			if isPiSubagentTool(ev.ToolName) {
				subEv := registry.StreamEvent{
					Type:    registry.EventSubagentEnd,
					Agent:   ev.ToolName,
					Content: strings.TrimSpace(resultBuf.String()),
					Summary: dur,
				}
				if ev.IsError {
					subEv.Err = fmt.Errorf("%s", subEv.Content)
				}
				out <- subEv
			} else if ev.ToolName != "" {
				out <- registry.StreamEvent{
					Type:    registry.EventToolEnd,
					Tool:    ev.ToolName,
					Content: strings.TrimSpace(resultBuf.String()),
				}
			}
		case "message_end":
			if ev.Message != nil && ev.Message.Role == "assistant" {
				var msgBuf strings.Builder
				for _, c := range ev.Message.Content {
					if c.Type == "text" && c.Text != "" {
						msgBuf.WriteString(c.Text)
					}
				}
				if msgBuf.Len() > 0 {
					lastText = strings.TrimSpace(msgBuf.String())
				}
				if ev.Message.ErrorMessage != "" {
					lastErr = ev.Message.ErrorMessage
				}
				if ev.Message.Usage != nil && (ev.Message.Usage.Input > 0 || ev.Message.Usage.Output > 0) {
					lastUsage = &registry.Usage{
						InputTokens:     ev.Message.Usage.Input,
						OutputTokens:    ev.Message.Usage.Output,
						CacheReadTokens: ev.Message.Usage.CacheRead,
					}
				}
			}
		}
	}

	if lastText == "" {
		lastText = strings.TrimSpace(deltaAcc.String())
	}
	return lastText, lastErr, lastUsage
}

func cleanOutput(s string) string {
	cleaned := ansiRegexp.ReplaceAllString(s, "")
	return strings.TrimSpace(cleaned)
}
