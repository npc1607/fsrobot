package protocol

// AGP (Agent Gateway Protocol) defines the JSONL framing protocol
// used between fsrobot and external agents (such as gp).

const (
	OpHello         = "hello"
	OpRun           = "run"
	OpResetSession  = "reset_session"
	OpAbort         = "abort"
	OpPing          = "ping"
	OpApprovalReply = "approval_reply"
	OpQuestionReply = "question_reply"
	OpListSessions  = "list_sessions"
	OpResumeSession = "resume_session"
	OpDeleteSession = "delete_session"
)

const (
	EventHelloAck        = "hello_ack"
	EventStarted         = "started"
	EventThinking        = "thinking"
	EventToolStart       = "tool_start"
	EventToolProgress    = "tool_progress"
	EventToolEnd         = "tool_end"
	EventSubagentStart   = "subagent_start"
	EventSubagentEnd     = "subagent_end"
	EventApprovalRequest = "approval_request"
	EventQuestionRequest = "question_request"
	EventDelta           = "delta"
	EventFinal           = "final"
	EventUsage           = "usage"
	EventResetAck        = "reset_ack"
	EventAbortAck        = "abort_ack"
	EventSessionsList    = "sessions_list"
	EventResumeAck       = "resume_ack"
	EventDeleteAck       = "delete_ack"
	EventPong            = "pong"
	EventError           = "error"
)

// CapabilityRule defines an allowlisted external capability (skill, mcp, js, cli, etc.).
type CapabilityRule struct {
	Kind  string   `json:"kind"`
	Name  string   `json:"name"`
	Allow []string `json:"allow,omitempty"`
	Paths []string `json:"paths,omitempty"`
}

// ExecutionPolicy carries structured workspace, tool, capability, and path boundaries for an agent run.
type ExecutionPolicy struct {
	WorkDir      string           `json:"work_dir,omitempty"`
	Tools        []string         `json:"tools,omitempty"`
	AllowedDirs  []string         `json:"allowed_dirs,omitempty"`
	DeniedPaths  []string         `json:"denied_paths,omitempty"`
	AllowBash    *bool            `json:"allow_bash,omitempty"`
	AllowWrite   *bool            `json:"allow_write,omitempty"`
	BashMode     string           `json:"bash_mode,omitempty"`
	Capabilities []CapabilityRule `json:"capabilities,omitempty"`
}

// CallerContext carries the human initiator identity and approval capability.
type CallerContext struct {
	SenderID      string `json:"sender_id,omitempty"`
	IsBoss        bool   `json:"is_boss,omitempty"`
	AllowApproval bool   `json:"allow_approval,omitempty"`
}

// RunOptions carries per-invocation overrides for an agent run.
type RunOptions struct {
	WorkDir    string           `json:"work_dir,omitempty"`
	Model      string           `json:"model,omitempty"`
	Tools      []string         `json:"tools,omitempty"`
	TimeoutSec int              `json:"timeout_sec,omitempty"`
	Policy     *ExecutionPolicy `json:"policy,omitempty"`
	Caller     *CallerContext   `json:"caller,omitempty"`
}

// Request represents a JSONL command frame sent to an agent server.
type Request struct {
	ID         string            `json:"id"`
	Op         string            `json:"op"`
	Client     string            `json:"client,omitempty"`
	SessionID  string            `json:"session_id,omitempty"`
	Prompt     string            `json:"prompt,omitempty"`
	TargetID   string            `json:"target_id,omitempty"`
	ApprovalID string            `json:"approval_id,omitempty"`
	Approved   bool              `json:"approved,omitempty"`
	QuestionID string            `json:"question_id,omitempty"`
	Answer     string            `json:"answer,omitempty"`
	Skipped    bool              `json:"skipped,omitempty"`
	Options    *RunOptions       `json:"options,omitempty"`
	Metadata   map[string]string `json:"metadata,omitempty"`
}

// Usage reports token usage for a completed turn.
type Usage struct {
	InputTokens     int `json:"input_tokens,omitempty"`
	OutputTokens    int `json:"output_tokens,omitempty"`
	CacheReadTokens int `json:"cache_read_tokens,omitempty"`
}

// SessionSummary describes a persisted session returned by list_sessions.
type SessionSummary struct {
	SessionID    string `json:"session_id"`
	MessageCount int    `json:"message_count"`
	FirstPrompt  string `json:"first_prompt,omitempty"`
	UpdatedAt    string `json:"updated_at"`
}

// Event represents a JSONL event frame emitted by an agent server.
type Event struct {
	ID           string           `json:"id"`
	Event        string           `json:"event"`
	SessionID    string           `json:"session_id,omitempty"`
	Sessions     []SessionSummary `json:"sessions,omitempty"`
	ApprovalID   string         `json:"approval_id,omitempty"`
	QuestionID   string         `json:"question_id,omitempty"`
	Question     string         `json:"question,omitempty"`
	Options      []string       `json:"options,omitempty"`
	Text         string         `json:"text,omitempty"`
	Tool         string         `json:"tool,omitempty"`
	Args         map[string]any `json:"args,omitempty"`
	Summary      string         `json:"summary,omitempty"`
	Code         string         `json:"code,omitempty"`
	Message      string         `json:"message,omitempty"`
	AgentName    string         `json:"agent_name,omitempty"`
	AgentVersion string         `json:"agent_version,omitempty"`
	Stalled      bool           `json:"stalled,omitempty"`
	Usage        *Usage         `json:"usage,omitempty"`
}
