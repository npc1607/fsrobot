package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"time"
)

// AgentConfig defines a single registered agent backend.
type AgentConfig struct {
	ID          string   `json:"id"`
	Type        string   `json:"type"`                  // "pi" or "agp" (Agent Gateway Protocol, e.g. gp)
	Name        string   `json:"name,omitempty"`        // Display name
	Description string   `json:"description,omitempty"` // Description shown in /agents
	Enabled     *bool    `json:"enabled,omitempty"`     // Default true if omitted
	Endpoint    string   `json:"endpoint,omitempty"`    // For agp: "unix:///tmp/gp.sock", "tcp://127.0.0.1:9527", or "stdio:///path/to/gp --mode rpc"
	Bin         string   `json:"bin,omitempty"`         // For pi: path to pi executable
	Model       string   `json:"model,omitempty"`
	Thinking    string   `json:"thinking,omitempty"`
	WorkDir     string   `json:"work_dir,omitempty"`
	Tools       []string `json:"tools,omitempty"`
	SystemPrompt string  `json:"append_system_prompt,omitempty"`
	TimeoutSec  int      `json:"timeout_sec,omitempty"`
	EnableSession *bool  `json:"enable_session,omitempty"`
	SessionDir  string   `json:"session_dir,omitempty"`
}

// IsEnabled returns whether this agent entry is enabled.
func (a AgentConfig) IsEnabled() bool {
	if a.Enabled == nil {
		return true
	}
	return *a.Enabled
}

// Config represents the application configuration.
type Config struct {
	// Feishu App credentials
	AppID        string   `json:"app_id"`
	AppSecret    string   `json:"app_secret"`
	RequireAt    bool     `json:"require_at"`     // Group chat requires @mention of the bot (default: false)
	AdminOpenIDs []string `json:"admin_open_ids"` // Allowed admin open_ids for high-risk approval (empty = deny all)

	// Routing
	DefaultAgent string        `json:"default_agent"`
	Agents       []AgentConfig `json:"agents"`

	// Legacy / shorthand Pi options (used when Agents is empty or as defaults)
	PiBin              string        `json:"pi_bin"`
	PiModel            string        `json:"pi_model"`
	PiThinking         string        `json:"pi_thinking"`
	PiWorkDir          string        `json:"pi_work_dir"`
	PiTimeout          time.Duration `json:"pi_timeout"`
	AppendSystemPrompt string        `json:"append_system_prompt"`
	EnableSession      bool          `json:"enable_session"`
	SessionDir         string        `json:"session_dir"`

	// Shorthand GP endpoint (via env GP_ENDPOINT)
	GPEndpoint string `json:"gp_endpoint"`
}

// DefaultConfig returns default configuration values.
func DefaultConfig() *Config {
	return &Config{
		DefaultAgent:  "pi",
		PiBin:         "pi",
		PiWorkDir:     ".",
		PiTimeout:     15 * time.Minute,
		EnableSession: true,
		SessionDir:    "./sessions",
	}
}

// Load loads configuration from config.json (if present) and overrides with environment variables.
func Load() (*Config, error) {
	cfg := DefaultConfig()

	configPath := os.Getenv("CONFIG_PATH")
	if configPath == "" {
		configPath = "config.json"
	}

	if data, err := os.ReadFile(configPath); err == nil {
		if err := json.Unmarshal(data, cfg); err != nil {
			return nil, err
		}
	}

	// Environment variable overrides
	if v := os.Getenv("FEISHU_APP_ID"); v != "" {
		cfg.AppID = v
	}
	if v := os.Getenv("FEISHU_APP_SECRET"); v != "" {
		cfg.AppSecret = v
	}
	if v := os.Getenv("REQUIRE_AT"); v != "" {
		cfg.RequireAt = v == "true" || v == "1"
	}
	if v := os.Getenv("ADMIN_OPEN_IDS"); v != "" {
		var ids []string
		for _, part := range splitAndTrim(v) {
			ids = append(ids, part)
		}
		cfg.AdminOpenIDs = ids
	}
	if v := os.Getenv("DEFAULT_AGENT"); v != "" {
		cfg.DefaultAgent = v
	}
	if v := os.Getenv("GP_ENDPOINT"); v != "" {
		cfg.GPEndpoint = v
	}
	if v := os.Getenv("PI_BIN"); v != "" {
		cfg.PiBin = v
	}
	if v := os.Getenv("PI_MODEL"); v != "" {
		cfg.PiModel = v
	}
	if v := os.Getenv("PI_THINKING"); v != "" {
		cfg.PiThinking = v
	}
	if v := os.Getenv("PI_WORK_DIR"); v != "" {
		cfg.PiWorkDir = v
	}
	if v := os.Getenv("PI_SESSION_DIR"); v != "" {
		cfg.SessionDir = v
	}
	if v := os.Getenv("PI_APPEND_SYSTEM_PROMPT"); v != "" {
		cfg.AppendSystemPrompt = v
	}

	// Ensure session directory exists if sessions are enabled
	if cfg.EnableSession && cfg.SessionDir != "" {
		if absDir, err := filepath.Abs(cfg.SessionDir); err == nil {
			_ = os.MkdirAll(absDir, 0755)
		}
	}

	return cfg, nil
}

func splitAndTrim(s string) []string {
	var out []string
	start := 0
	for i := 0; i <= len(s); i++ {
		if i == len(s) || s[i] == ',' {
			part := s[start:i]
			// trim spaces
			for len(part) > 0 && (part[0] == ' ' || part[0] == '\t') {
				part = part[1:]
			}
			for len(part) > 0 && (part[len(part)-1] == ' ' || part[len(part)-1] == '\t') {
				part = part[:len(part)-1]
			}
			if part != "" {
				out = append(out, part)
			}
			start = i + 1
		}
	}
	return out
}
