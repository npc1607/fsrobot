package registry

import (
	"context"
	"fmt"
	"sort"
	"sync"
)

// AgentMeta describes a registered agent's identity and capabilities.
type AgentMeta struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Description string `json:"description"`
	Streaming   bool   `json:"streaming"`
	WorkDir     string `json:"work_dir,omitempty"`
}

// RunRequest represents a normalized request sent to any registered agent.
type RunRequest struct {
	SessionID string
	Prompt    string
	WorkDir   string
	Metadata  map[string]string
}

// StreamEventType classifies streaming events emitted by an AgentProvider.
type StreamEventType string

const (
	EventThinking      StreamEventType = "thinking"
	EventToolStart     StreamEventType = "tool_start"
	EventToolEnd       StreamEventType = "tool_end"
	EventSubagentStart StreamEventType = "subagent_start"
	EventSubagentEnd   StreamEventType = "subagent_end"
	EventDelta         StreamEventType = "delta"
	EventFinal         StreamEventType = "final"
	EventError         StreamEventType = "error"
)

// StreamEvent represents an incremental or final output from an agent run.
type StreamEvent struct {
	Type    StreamEventType
	Content string
	Tool    string
	Agent   string
	Summary string
	Err     error
	Usage   *Usage
}

// Usage 报告一次运行消耗的 token 数量（用于成本展示）。
type Usage struct {
	InputTokens     int
	OutputTokens    int
	CacheReadTokens int
}

// Total 返回输入与输出之和。
func (u *Usage) Total() int {
	if u == nil {
		return 0
	}
	return u.InputTokens + u.OutputTokens
}

// AgentProvider is the top-level abstraction implemented by all agent backends
// (e.g. Pi CLI, gp custom protocol, remote HTTP/RPC agents).
type AgentProvider interface {
	Meta() AgentMeta
	Execute(ctx context.Context, req RunRequest) (<-chan StreamEvent, error)
	ResetSession(ctx context.Context, sessionID string) error
	Close() error
}

// Registry manages all registered AgentProviders and default routing.
type Registry struct {
	mu        sync.RWMutex
	providers map[string]AgentProvider
	defaultID string
}

// NewRegistry creates an empty AgentRegistry.
func NewRegistry(defaultAgentID string) *Registry {
	return &Registry{
		providers: make(map[string]AgentProvider),
		defaultID: defaultAgentID,
	}
}

// Register adds or replaces an AgentProvider in the registry.
func (r *Registry) Register(p AgentProvider) error {
	if p == nil {
		return fmt.Errorf("cannot register nil provider")
	}
	meta := p.Meta()
	if meta.ID == "" {
		return fmt.Errorf("agent provider must have a non-empty ID")
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	r.providers[meta.ID] = p
	if r.defaultID == "" {
		r.defaultID = meta.ID
	}
	return nil
}

// Get returns a registered provider by ID.
func (r *Registry) Get(id string) (AgentProvider, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	p, ok := r.providers[id]
	return p, ok
}

// Default returns the default registered provider.
func (r *Registry) Default() (AgentProvider, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.defaultID != "" {
		if p, ok := r.providers[r.defaultID]; ok {
			return p, true
		}
	}
	// Fallback to first available in sorted order for determinism
	if len(r.providers) == 0 {
		return nil, false
	}
	keys := make([]string, 0, len(r.providers))
	for k := range r.providers {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return r.providers[keys[0]], true
}

// DefaultID returns the configured default agent ID.
func (r *Registry) DefaultID() string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.defaultID
}

// List returns metadata for all registered agents sorted by ID.
func (r *Registry) List() []AgentMeta {
	r.mu.RLock()
	defer r.mu.RUnlock()
	list := make([]AgentMeta, 0, len(r.providers))
	for _, p := range r.providers {
		list = append(list, p.Meta())
	}
	sort.Slice(list, func(i, j int) bool {
		return list[i].ID < list[j].ID
	})
	return list
}

// CloseAll closes all registered providers.
func (r *Registry) CloseAll() {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, p := range r.providers {
		_ = p.Close()
	}
}
