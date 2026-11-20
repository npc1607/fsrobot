package agent

import (
	"strings"
	"testing"

	"fsrobot/internal/registry"
)

func TestParsePiJSONStream(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"session","version":3,"id":"abc","timestamp":"2026-11-19T00:00:00Z","cwd":"."}`,
		`{"type":"tool_execution_start","toolCallId":"c1","toolName":"bash","args":{"command":"ls"}}`,
		`{"type":"tool_execution_end","toolCallId":"c1","toolName":"bash","result":{"content":[{"type":"text","text":"ok"}]},"isError":false,"durationMs":120}`,
		`{"type":"tool_execution_start","toolCallId":"c2","toolName":"subagent","args":{}}`,
		`{"type":"tool_execution_end","toolCallId":"c2","toolName":"subagent","result":{"content":[{"type":"text","text":"subagent done"}]},"isError":false,"durationMs":850}`,
		`{"type":"message_update","assistantMessageEvent":{"type":"text_delta","contentIndex":0,"delta":"你好"}}`,
		`{"type":"message_update","assistantMessageEvent":{"type":"text_delta","contentIndex":0,"delta":"世界"}}`,
		`{"type":"message_end","message":{"role":"assistant","content":[{"type":"text","text":"你好世界"}]}}`,
	}, "\n")

	out := make(chan registry.StreamEvent, 16)
	lastText, lastErr, _ := parsePiJSONStream(strings.NewReader(input), out)
	close(out)

	if lastErr != "" {
		t.Fatalf("unexpected lastErr: %q", lastErr)
	}
	if lastText != "你好世界" {
		t.Fatalf("expected lastText '你好世界', got %q", lastText)
	}

	var gotToolStart, gotSubStart, gotSubEnd, gotDelta int
	for ev := range out {
		switch ev.Type {
		case registry.EventToolStart:
			gotToolStart++
		case registry.EventSubagentStart:
			gotSubStart++
		case registry.EventSubagentEnd:
			gotSubEnd++
			if ev.Content != "subagent done" || ev.Summary != "850ms" {
				t.Fatalf("unexpected subagent end: %+v", ev)
			}
		case registry.EventDelta:
			gotDelta++
		}
	}

	if gotToolStart != 1 || gotSubStart != 1 || gotSubEnd != 1 || gotDelta != 2 {
		t.Fatalf("unexpected event counts: tool=%d subStart=%d subEnd=%d delta=%d",
			gotToolStart, gotSubStart, gotSubEnd, gotDelta)
	}
}

func TestSanitizePiSessionID(t *testing.T) {
	got := sanitizePiSessionID("oc_1ec2a28c:sub:11e19b3")
	if got != "oc_1ec2a28c_sub_11e19b3" {
		t.Fatalf("unexpected sanitized id: %q", got)
	}
}
