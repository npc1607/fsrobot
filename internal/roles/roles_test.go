package roles

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestDefaultCatalogResolveAndThinkingText(t *testing.T) {
	c := NewDefaultCatalog()

	if def := c.Default(); def.Name != "贱兮兮" {
		t.Fatalf("expected default role 贱兮兮, got %q", def.Name)
	}

	// Empty raw prompt -> uses default role's thinking text, or neutral fallback if none configured
	if got := c.ThinkingText(""); !strings.Contains(got, "本大爷") {
		t.Fatalf("expected default role thinking text, got: %q", got)
	}
	neutral := NewCatalog("plain", []Role{{Name: "plain", Prompt: "plain"}})
	if got := neutral.ThinkingText(""); got != "⏳ 正在思考并处理中..." {
		t.Fatalf("expected neutral fallback when role has no ThinkingText, got: %q", got)
	}

	// Resolve by alias
	r, ok := c.Resolve("2077")
	if !ok || r.Name != "夜之城" {
		t.Fatalf("expected alias 2077 to resolve to 夜之城, got %+v (ok=%v)", r, ok)
	}
	if got := c.ThinkingText(r.Prompt); !strings.Contains(got, "斯坦") {
		t.Fatalf("expected 夜之城 thinking text to mention 斯坦, got %q", got)
	}

	// Custom prompt -> fallback formatted thinking text
	if got := c.ThinkingText("你是一个海盗船长"); !strings.Contains(got, "海盗船长") {
		t.Fatalf("expected custom prompt in thinking text, got %q", got)
	}
}

func TestTimeGreeting(t *testing.T) {
	cases := []struct {
		hour int
		want string
	}{
		{8, "早上好"},
		{12, "中午好"},
		{16, "下午好"},
		{22, "晚上好"},
		{2, "晚上好"},
	}
	for _, tc := range cases {
		ts := time.Date(2026, 11, 23, tc.hour, 0, 0, 0, time.Local)
		if got := TimeGreeting(ts); got != tc.want {
			t.Fatalf("hour=%d: got %q, want %q", tc.hour, got, tc.want)
		}
	}
}

func TestYAMLIncompleteRoleRejectedAndAliasCollisionGuarded(t *testing.T) {
	yamlPath := filepath.Join(t.TempDir(), "roles.yaml")
	content := `
default: 完整角色
timezone: Asia/Shanghai
roles:
  - name: 完整角色
    aliases: ["dj", "main"]
    prompt: "我是完整角色"
    thinking_text: "完整角色思考中..."
  - name: 缺思考文案
    prompt: "没有 thinking_text，不应暴露"
  - name: 缺提示词
    thinking_text: "没有 prompt，不应暴露"
  - name: 第二角色
    aliases: ["dj", "second"]
    prompt: "试图抢占 dj 别名"
    thinking_text: "第二角色思考中..."
`
	if err := os.WriteFile(yamlPath, []byte(content), 0o600); err != nil {
		t.Fatalf("write yaml: %v", err)
	}

	cat, err := LoadCatalogFromYAML(yamlPath)
	if err != nil {
		t.Fatalf("LoadCatalogFromYAML failed: %v", err)
	}

	// Only 2 complete roles should be registered
	presets := cat.Presets()
	if len(presets) != 2 {
		t.Fatalf("expected 2 complete roles, got %d: %+v", len(presets), presets)
	}
	if _, ok := cat.Resolve("缺思考文案"); ok {
		t.Fatal("incomplete role without thinking_text must not be exposed")
	}
	if _, ok := cat.Resolve("缺提示词"); ok {
		t.Fatal("incomplete role without prompt must not be exposed")
	}

	// Alias "dj" must remain owned by 完整角色, not hijacked by 第二角色
	r, ok := cat.Resolve("dj")
	if !ok || r.Name != "完整角色" {
		t.Fatalf("expected alias dj to belong to 完整角色, got %+v (ok=%v)", r, ok)
	}
	r2, ok := cat.Resolve("second")
	if !ok || r2.Name != "第二角色" {
		t.Fatalf("expected non-conflicting alias second to resolve to 第二角色, got %+v (ok=%v)", r2, ok)
	}
}

func TestHotReloadCatalogUpdatesAndPreservesLastGood(t *testing.T) {
	yamlPath := filepath.Join(t.TempDir(), "roles.yaml")
	v1 := `
default: alpha
timezone: Asia/Shanghai
roles:
  - name: alpha
    prompt: "alpha v1"
    thinking_text: "alpha thinking"
`
	if err := os.WriteFile(yamlPath, []byte(v1), 0o600); err != nil {
		t.Fatalf("write v1: %v", err)
	}

	hc := NewHotReloadCatalog(yamlPath)
	if got := hc.Default().Prompt; got != "alpha v1" {
		t.Fatalf("expected alpha v1, got %q", got)
	}

	// Force next access to check stat immediately
	hc.mu.Lock()
	hc.lastCheck = time.Now().Add(-2 * time.Second)
	hc.mu.Unlock()

	// Write broken YAML -> must keep alpha v1
	if err := os.WriteFile(yamlPath, []byte("roles: [broken yaml"), 0o600); err != nil {
		t.Fatalf("write broken: %v", err)
	}
	if got := hc.Default().Prompt; got != "alpha v1" {
		t.Fatalf("expected last known good alpha v1 on broken yaml, got %q", got)
	}

	// Write valid v2 with new role beta
	hc.mu.Lock()
	hc.lastCheck = time.Now().Add(-2 * time.Second)
	hc.mu.Unlock()
	v2 := `
default: alpha
timezone: Asia/Shanghai
roles:
  - name: alpha
    prompt: "alpha v2"
    thinking_text: "alpha thinking v2"
  - name: beta
    aliases: ["b"]
    prompt: "beta prompt"
    thinking_text: "beta thinking"
`
	if err := os.WriteFile(yamlPath, []byte(v2), 0o600); err != nil {
		t.Fatalf("write v2: %v", err)
	}
	if got := hc.Default().Prompt; got != "alpha v2" {
		t.Fatalf("expected hot-reloaded alpha v2, got %q", got)
	}
	if b, ok := hc.Resolve("b"); !ok || b.Name != "beta" {
		t.Fatalf("expected hot-reloaded beta via alias b, got %+v (ok=%v)", b, ok)
	}
}

func TestYAMLCapabilitiesAndBashModeScoped(t *testing.T) {
	yamlPath := filepath.Join(t.TempDir(), "roles.yaml")
	content := `
default: 客服画图
timezone: Asia/Shanghai
roles:
  - name: 客服画图
    prompt: "只读查代码并用画图 skill 出图"
    thinking_text: "画图中..."
    policy:
      bash_mode: scoped
      allowed_tools: ["read", "bash"]
      capabilities:
        - kind: skill
          name: mermaid-diagram
          allow: ["mmdc", "scripts/render.sh"]
          paths: ["/opt/skills/mermaid"]
        - kind: mcp
          name: feishu-mcp
          allow: ["mcp__feishu__read_doc"]
`
	if err := os.WriteFile(yamlPath, []byte(content), 0o600); err != nil {
		t.Fatalf("write yaml: %v", err)
	}
	cat, err := LoadCatalogFromYAML(yamlPath)
	if err != nil {
		t.Fatalf("LoadCatalogFromYAML failed: %v", err)
	}
	r, ok := cat.Resolve("客服画图")
	if !ok {
		t.Fatal("expected 客服画图 to resolve")
	}
	if r.Policy.BashMode != "scoped" || len(r.Policy.Capabilities) != 2 {
		t.Fatalf("unexpected policy parsed: %+v", r.Policy)
	}
	if r.Policy.Capabilities[0].Kind != "skill" || len(r.Policy.Capabilities[0].Allow) != 2 {
		t.Fatalf("unexpected first capability: %+v", r.Policy.Capabilities[0])
	}
}
