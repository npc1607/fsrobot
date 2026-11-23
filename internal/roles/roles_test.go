package roles

import (
	"strings"
	"testing"
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
