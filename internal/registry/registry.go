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

// CapabilityRule defines an allowlisted external capability (skill, mcp, js, cli, etc.)
// with its allowed actions/command prefixes and path boundaries.
type CapabilityRule struct {
	Kind  string   `json:"kind" yaml:"kind"`
	Name  string   `json:"name" yaml:"name"`
	Allow []string `json:"allow,omitempty" yaml:"allow,omitempty"`
	Paths []string `json:"paths,omitempty" yaml:"paths,omitempty"`
}

// ExecutionPolicy defines per-invocation workspace, tool, capability, and path boundaries.
type ExecutionPolicy struct {
	WorkDir      string           `json:"work_dir,omitempty" yaml:"work_dir,omitempty"`
	Tools        []string         `json:"allowed_tools,omitempty" yaml:"allowed_tools,omitempty"`
	AllowedDirs  []string         `json:"allowed_dirs,omitempty" yaml:"allowed_dirs,omitempty"`
	DeniedPaths  []string         `json:"denied_paths,omitempty" yaml:"denied_paths,omitempty"`
	AllowBash    *bool            `json:"allow_bash,omitempty" yaml:"allow_bash,omitempty"`
	AllowWrite   *bool            `json:"allow_write,omitempty" yaml:"allow_write,omitempty"`
	BashMode     string           `json:"bash_mode,omitempty" yaml:"bash_mode,omitempty"`
	Capabilities []CapabilityRule `json:"capabilities,omitempty" yaml:"capabilities,omitempty"`
}

// CallerContext identifies the human initiator and approval capability for a run.
type CallerContext struct {
	SenderID      string `json:"sender_id,omitempty"`
	IsBoss        bool   `json:"is_boss,omitempty"`
	AllowApproval bool   `json:"allow_approval,omitempty"`
}

// RunRequest represents a normalized request sent to any registered agent.
type RunRequest struct {
	SessionID string
	Prompt    string
	WorkDir   string
	Policy    ExecutionPolicy
	Caller    CallerContext
	Metadata  map[string]string
}

// StreamEventType classifies streaming events emitted by an AgentProvider.
type StreamEventType string

const (
	EventThinking        StreamEventType = "thinking"
	EventToolStart       StreamEventType = "tool_start"
	EventToolProgress    StreamEventType = "tool_progress"
	EventToolEnd         StreamEventType = "tool_end"
	EventSubagentStart   StreamEventType = "subagent_start"
	EventSubagentEnd     StreamEventType = "subagent_end"
	EventApprovalRequest StreamEventType = "approval_request"
	EventQuestionRequest StreamEventType = "question_request"
	EventDelta           StreamEventType = "delta"
	EventFinal           StreamEventType = "final"
	EventError           StreamEventType = "error"
)

// StreamEvent represents an incremental or final output from an agent run.
type StreamEvent struct {
	Type          StreamEventType
	Content       string
	Tool          string
	Agent         string
	Summary       string
	ApprovalID    string
	QuestionID    string
	Options       []string
	Args          map[string]any
	Stalled       bool
	ReplyApproval func(approved bool) error
	ReplyQuestion func(answer string, skipped bool) error
	Err           error
	Usage         *Usage
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

// SessionInfo describes a persisted historical session for listing/resuming/deleting.
type SessionInfo struct {
	SessionID    string
	MessageCount int
	FirstPrompt  string
	UpdatedAt    string
}

// SessionManagerProvider is an optional interface implemented by AgentProviders
// that support listing, resuming, and deleting persisted historical sessions.
type SessionManagerProvider interface {
	ListSessions(ctx context.Context, sessionID string) (currentID string, list []SessionInfo, err error)
	ResumeSession(ctx context.Context, sessionID, targetID string) error
	DeleteSession(ctx context.Context, sessionID, targetID string) error
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
