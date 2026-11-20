package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadTOML(t *testing.T) {
	dir := t.TempDir()
	tomlPath := filepath.Join(dir, "config.toml")
	content := `
app_id = "cli_test"
app_secret = "sec_test"
require_at = true
bosses = ["ou_boss_1", "ou_boss_2"]
default_agent = "gp"

[[agents]]
id = "gp"
type = "agp"
name = "赛博包工头"
endpoint = "unix:///tmp/gp.sock"
timeout_sec = 600
`
	if err := os.WriteFile(tomlPath, []byte(content), 0o644); err != nil {
		t.Fatalf("write toml: %v", err)
	}

	t.Setenv("CONFIG_PATH", tomlPath)
	t.Setenv("FEISHU_APP_ID", "")
	t.Setenv("FEISHU_APP_SECRET", "")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() failed: %v", err)
	}
	if cfg.AppID != "cli_test" || !cfg.RequireAt || cfg.DefaultAgent != "gp" {
		t.Fatalf("unexpected cfg: %+v", cfg)
	}
	if len(cfg.Bosses) != 2 || cfg.Bosses[0] != "ou_boss_1" {
		t.Fatalf("unexpected bosses: %+v", cfg.Bosses)
	}
	if len(cfg.Agents) != 1 || cfg.Agents[0].Name != "赛博包工头" || cfg.Agents[0].TimeoutSec != 600 {
		t.Fatalf("unexpected agents: %+v", cfg.Agents)
	}
}
