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
)

const (
	EventHelloAck        = "hello_ack"
	EventStarted         = "started"
	EventThinking        = "thinking"
	EventToolStart       = "tool_start"
	EventToolEnd         = "tool_end"
	EventSubagentStart   = "subagent_start"
	EventSubagentEnd     = "subagent_end"
	EventApprovalRequest = "approval_request"
	EventDelta           = "delta"
	EventFinal           = "final"
	EventUsage           = "usage"
	EventResetAck        = "reset_ack"
	EventAbortAck        = "abort_ack"
	EventPong            = "pong"
	EventError           = "error"
)

// RunOptions carries per-invocation overrides for an agent run.
type RunOptions struct {
	WorkDir       string   `json:"work_dir,omitempty"`
	Model         string   `json:"model,omitempty"`
	Tools         []string `json:"tools,omitempty"`
	TimeoutSec    int      `json:"timeout_sec,omitempty"`
	AllowApproval bool     `json:"allow_approval,omitempty"`
	SenderID      string   `json:"sender_id,omitempty"`
	IsBoss        bool     `json:"is_boss,omitempty"`
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
	Options    *RunOptions       `json:"options,omitempty"`
	Metadata   map[string]string `json:"metadata,omitempty"`
}

// Usage reports token usage for a completed turn.
type Usage struct {
	InputTokens     int `json:"input_tokens,omitempty"`
	OutputTokens    int `json:"output_tokens,omitempty"`
	CacheReadTokens int `json:"cache_read_tokens,omitempty"`
}

// Event represents a JSONL event frame emitted by an agent server.
type Event struct {
	ID           string         `json:"id"`
	Event        string         `json:"event"`
	SessionID    string         `json:"session_id,omitempty"`
	ApprovalID   string         `json:"approval_id,omitempty"`
	Text         string         `json:"text,omitempty"`
	Tool         string         `json:"tool,omitempty"`
	Args         map[string]any `json:"args,omitempty"`
	Summary      string         `json:"summary,omitempty"`
	Code         string         `json:"code,omitempty"`
	Message      string         `json:"message,omitempty"`
	AgentName    string         `json:"agent_name,omitempty"`
	AgentVersion string         `json:"agent_version,omitempty"`
	Usage        *Usage         `json:"usage,omitempty"`
}
