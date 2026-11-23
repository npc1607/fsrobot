package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
)

// AgentConfig defines a single registered agent backend.
type AgentConfig struct {
	ID            string   `toml:"id" json:"id"`
	Type          string   `toml:"type" json:"type"`                                     // "pi" or "agp"
	Name          string   `toml:"name" json:"name,omitempty"`                           // Display name
	Description   string   `toml:"description" json:"description,omitempty"`             // Description shown in /agents
	Enabled       *bool    `toml:"enabled" json:"enabled,omitempty"`                     // Default true if omitted
	Endpoint      string   `toml:"endpoint" json:"endpoint,omitempty"`                   // For agp: "unix://...", "tcp://...", or "stdio://..."
	Bin           string   `toml:"bin" json:"bin,omitempty"`                             // For pi: path to pi executable
	Model         string   `toml:"model" json:"model,omitempty"`
	Thinking      string   `toml:"thinking" json:"thinking,omitempty"`
	WorkDir       string   `toml:"work_dir" json:"work_dir,omitempty"`
	Tools         []string `toml:"tools" json:"tools,omitempty"`
	SystemPrompt  string   `toml:"append_system_prompt" json:"append_system_prompt,omitempty"`
	TimeoutSec    int      `toml:"timeout_sec" json:"timeout_sec,omitempty"`
	EnableSession *bool    `toml:"enable_session" json:"enable_session,omitempty"`
	SessionDir    string   `toml:"session_dir" json:"session_dir,omitempty"`
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
	// Feishu App credentials & permissions
	AppID     string   `toml:"app_id" json:"app_id"`
	AppSecret string   `toml:"app_secret" json:"app_secret"`
	RequireAt bool     `toml:"require_at" json:"require_at"` // Group chat requires @mention of the bot (default: false)
	Bosses    []string `toml:"bosses" json:"bosses"`         // Allowed boss/admin open_ids (empty = deny high-risk actions)

	// Routing
	DefaultAgent string        `toml:"default_agent" json:"default_agent"`
	RolesFile    string        `toml:"roles_file" json:"roles_file,omitempty"`
	Agents       []AgentConfig `toml:"agents" json:"agents"`

	// Legacy / shorthand Pi options (used when Agents is empty or as defaults)
	PiBin              string        `toml:"pi_bin" json:"pi_bin"`
	PiModel            string        `toml:"pi_model" json:"pi_model"`
	PiThinking         string        `toml:"pi_thinking" json:"pi_thinking"`
	PiWorkDir          string        `toml:"pi_work_dir" json:"pi_work_dir"`
	PiTimeout          time.Duration `toml:"pi_timeout" json:"pi_timeout"`
	AppendSystemPrompt string        `toml:"append_system_prompt" json:"append_system_prompt"`
	EnableSession      bool          `toml:"enable_session" json:"enable_session"`
	SessionDir         string        `toml:"session_dir" json:"session_dir"`

	// Shorthand GP endpoint (via env GP_ENDPOINT)
	GPEndpoint string `toml:"gp_endpoint" json:"gp_endpoint"`
}

// DefaultConfig returns default configuration values.
func DefaultConfig() *Config {
	return &Config{
		DefaultAgent:  "pi",
		RolesFile:     "roles.yaml",
		PiBin:         "pi",
		PiWorkDir:     ".",
		PiTimeout:     30 * time.Minute,
		EnableSession: true,
		SessionDir:    "./sessions",
	}
}

// Load loads configuration from config.toml (or config.json fallback) and overrides with environment variables.
func Load() (*Config, error) {
	cfg := DefaultConfig()

	configPath := strings.TrimSpace(os.Getenv("CONFIG_PATH"))
	if configPath != "" {
		if err := decodeConfigFile(configPath, cfg); err != nil {
			return nil, err
		}
	} else if _, err := os.Stat("config.toml"); err == nil {
		if err := decodeConfigFile("config.toml", cfg); err != nil {
			return nil, err
		}
	} else if _, err := os.Stat("config.json"); err == nil {
		if err := decodeConfigFile("config.json", cfg); err != nil {
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
	if v := os.Getenv("BOSSES"); v != "" {
		cfg.Bosses = splitAndTrim(v)
	} else if v := os.Getenv("ADMIN_OPEN_IDS"); v != "" {
		cfg.Bosses = splitAndTrim(v)
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

	if cfg.EnableSession && cfg.SessionDir != "" {
		if absDir, err := filepath.Abs(cfg.SessionDir); err == nil {
			_ = os.MkdirAll(absDir, 0755)
		}
	}

	return cfg, nil
}

func decodeConfigFile(path string, cfg *Config) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if strings.HasSuffix(strings.ToLower(path), ".json") {
		return json.Unmarshal(data, cfg)
	}
	return toml.Unmarshal(data, cfg)
}

func splitAndTrim(s string) []string {
	var out []string
	for _, part := range strings.Split(s, ",") {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}
