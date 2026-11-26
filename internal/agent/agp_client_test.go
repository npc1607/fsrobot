package agent

import (
	"bufio"
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"fsrobot/internal/config"
	"fsrobot/internal/protocol"
	"fsrobot/internal/registry"
)

func TestAGPExchangeStream(t *testing.T) {
	p, err := NewAGPProvider(config.AgentConfig{
		ID:       "gp",
		Type:     "agp",
		Endpoint: "unix:///tmp/unused.sock",
	}, config.DefaultConfig())
	if err != nil {
		t.Fatalf("NewAGPProvider failed: %v", err)
	}

	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	// Mock AGP server responding on serverConn
	go func() {
		scanner := bufio.NewScanner(serverConn)
		if !scanner.Scan() {
			return
		}
		var req protocol.Request
		_ = json.Unmarshal(scanner.Bytes(), &req)

		writeEv := func(ev protocol.Event) {
			b, _ := json.Marshal(ev)
			b = append(b, '\n')
			_, _ = serverConn.Write(b)
		}

		writeEv(protocol.Event{ID: req.ID, Event: protocol.EventStarted, SessionID: req.SessionID})
		writeEv(protocol.Event{ID: req.ID, Event: protocol.EventToolStart, Tool: "grep"})
		writeEv(protocol.Event{ID: req.ID, Event: protocol.EventDelta, Text: "部分内容"})
		writeEv(protocol.Event{ID: req.ID, Event: protocol.EventFinal, Text: "最终完整回答"})
	}()

	out := make(chan registry.StreamEvent, 8)
	req := protocol.Request{
		ID:        "req-1",
		Op:        protocol.OpRun,
		SessionID: "chat_123",
		Prompt:    "test",
	}

	if err := p.exchangeStream(context.Background(), clientConn, req, out); err != nil {
		t.Fatalf("exchangeStream failed: %v", err)
	}
	close(out)

	var gotFinal string
	var gotTool string
	for ev := range out {
		switch ev.Type {
		case registry.EventToolStart:
			gotTool = ev.Tool
		case registry.EventFinal:
			gotFinal = ev.Content
		}
	}

	if gotTool != "grep" {
		t.Fatalf("expected tool_start grep, got %q", gotTool)
	}
	if gotFinal != "最终完整回答" {
		t.Fatalf("expected final text, got %q", gotFinal)
	}
}

func TestAGPExchangeStreamUsage(t *testing.T) {
	p, err := NewAGPProvider(config.AgentConfig{
		ID:       "gp",
		Type:     "agp",
		Endpoint: "unix:///tmp/unused.sock",
	}, config.DefaultConfig())
	if err != nil {
		t.Fatalf("NewAGPProvider failed: %v", err)
	}

	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	go func() {
		scanner := bufio.NewScanner(serverConn)
		if !scanner.Scan() {
			return
		}
		var req protocol.Request
		_ = json.Unmarshal(scanner.Bytes(), &req)

		writeEv := func(ev protocol.Event) {
			b, _ := json.Marshal(ev)
			b = append(b, '\n')
			_, _ = serverConn.Write(b)
		}

		writeEv(protocol.Event{ID: req.ID, Event: protocol.EventStarted, SessionID: req.SessionID})
		writeEv(protocol.Event{ID: req.ID, Event: protocol.EventDelta, Text: "回答"})
		// 独立的 usage 帧（部分实现会单独上报）与 final 帧自带的 usage 都应被累加。
		writeEv(protocol.Event{ID: req.ID, Event: protocol.EventUsage, Usage: &protocol.Usage{InputTokens: 1200, OutputTokens: 30, CacheReadTokens: 800}})
		writeEv(protocol.Event{ID: req.ID, Event: protocol.EventFinal, Text: "最终回答", Usage: &protocol.Usage{InputTokens: 300, OutputTokens: 70}})
	}()

	out := make(chan registry.StreamEvent, 8)
	req := protocol.Request{
		ID:        "req-usage",
		Op:        protocol.OpRun,
		SessionID: "chat_123",
		Prompt:    "test",
	}

	if err := p.exchangeStream(context.Background(), clientConn, req, out); err != nil {
		t.Fatalf("exchangeStream failed: %v", err)
	}
	close(out)

	var usage *registry.Usage
	for ev := range out {
		if ev.Type == registry.EventFinal {
			usage = ev.Usage
		}
	}

	if usage == nil {
		t.Fatal("expected usage on final event, got nil")
	}
	if usage.InputTokens != 1500 || usage.OutputTokens != 100 || usage.CacheReadTokens != 800 {
		t.Fatalf("unexpected usage: %+v", *usage)
	}
	if usage.Total() != 1600 {
		t.Fatalf("expected total 1600, got %d", usage.Total())
	}
}

func TestAGPExchangeStreamIdleTimeoutReset(t *testing.T) {
	p, err := NewAGPProvider(config.AgentConfig{
		ID:       "gp",
		Type:     "agp",
		Endpoint: "unix:///tmp/unused.sock",
	}, config.DefaultConfig())
	if err != nil {
		t.Fatalf("NewAGPProvider failed: %v", err)
	}

	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	// 总耗时 ~200ms，远超 80ms 的静默超时，但每 35ms 持续推送事件，不应超时
	go func() {
		scanner := bufio.NewScanner(serverConn)
		if !scanner.Scan() {
			return
		}
		var req protocol.Request
		_ = json.Unmarshal(scanner.Bytes(), &req)

		writeEv := func(ev protocol.Event) {
			b, _ := json.Marshal(ev)
			b = append(b, '\n')
			_, _ = serverConn.Write(b)
		}

		writeEv(protocol.Event{ID: req.ID, Event: protocol.EventStarted, SessionID: req.SessionID})
		for i := 0; i < 5; i++ {
			time.Sleep(35 * time.Millisecond)
			writeEv(protocol.Event{ID: req.ID, Event: protocol.EventDelta, Text: "chunk"})
		}
		time.Sleep(35 * time.Millisecond)
		writeEv(protocol.Event{ID: req.ID, Event: protocol.EventFinal, Text: "done"})
	}()

	out := make(chan registry.StreamEvent, 16)
	req := protocol.Request{
		ID:        "req-idle-ok",
		Op:        protocol.OpRun,
		SessionID: "chat_123",
		Prompt:    "test",
	}

	if err := p.exchangeStreamWithIdleTimeout(context.Background(), clientConn, req, out, 80*time.Millisecond); err != nil {
		t.Fatalf("expected stream to succeed with periodic events, got error: %v", err)
	}
	close(out)

	var gotFinal string
	for ev := range out {
		if ev.Type == registry.EventFinal {
			gotFinal = ev.Content
		}
	}
	if gotFinal != "done" {
		t.Fatalf("expected final 'done', got %q", gotFinal)
	}
}

func TestAGPExchangeStreamIdleTimeoutTriggered(t *testing.T) {
	p, err := NewAGPProvider(config.AgentConfig{
		ID:       "gp",
		Type:     "agp",
		Endpoint: "unix:///tmp/unused.sock",
	}, config.DefaultConfig())
	if err != nil {
		t.Fatalf("NewAGPProvider failed: %v", err)
	}

	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	// 服务端发完 started 后卡死不发任何数据
	go func() {
		scanner := bufio.NewScanner(serverConn)
		if !scanner.Scan() {
			return
		}
		var req protocol.Request
		_ = json.Unmarshal(scanner.Bytes(), &req)

		b, _ := json.Marshal(protocol.Event{ID: req.ID, Event: protocol.EventStarted, SessionID: req.SessionID})
		b = append(b, '\n')
		_, _ = serverConn.Write(b)
		// 保持连接打开但不发送后续数据
		time.Sleep(2 * time.Second)
	}()

	out := make(chan registry.StreamEvent, 8)
	req := protocol.Request{
		ID:        "req-idle-hang",
		Op:        protocol.OpRun,
		SessionID: "chat_123",
		Prompt:    "test",
	}

	err = p.exchangeStreamWithIdleTimeout(context.Background(), clientConn, req, out, 80*time.Millisecond)
	if err == nil {
		t.Fatal("expected idle timeout error, got nil")
	}
	if !strings.Contains(err.Error(), "idle timed out") {
		t.Fatalf("expected idle timed out error, got: %v", err)
	}
}

func TestAGPProvider_GetContextStats_RoundTrip(t *testing.T) {
	// Keep unix socket path well below macOS 104-byte sun_path limit.
	sockDir, err := os.MkdirTemp("/tmp", "agp-stat-")
	if err != nil {
		t.Fatalf("os.MkdirTemp failed: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(sockDir) })
	sockPath := filepath.Join(sockDir, "s.sock")
	ln, err := net.Listen("unix", sockPath)
	if err != nil {
		t.Fatalf("net.Listen unix failed: %v", err)
	}
	defer ln.Close()

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		scanner := bufio.NewScanner(conn)
		if !scanner.Scan() {
			return
		}
		var req protocol.Request
		_ = json.Unmarshal(scanner.Bytes(), &req)
		if req.Op != protocol.OpContextStats {
			b, _ := json.Marshal(protocol.Event{ID: req.ID, Event: protocol.EventError, Code: "BAD_OP", Message: req.Op})
			_, _ = conn.Write(append(b, '\n'))
			return
		}
		resp := protocol.Event{
			ID:        req.ID,
			Event:     protocol.EventContextStats,
			SessionID: "chat_123_eff",
			ContextStats: &protocol.ContextStats{
				SessionID:        "chat_123_eff",
				MessageCount:     18,
				EpochID:          1,
				EpochStride:      16,
				EpochPosition:    2,
				FreezeBoundary:   16,
				MaskedCount:      3,
				MaskWindow:       5,
				LedgerVersion:    2,
				LedgerHash:       "cafe1234",
				LedgerActiveCnt:  1,
				LedgerSuperseded: 1,
				ActiveConstraints: []protocol.ContextConstraintSummary{
					{ID: "c1", Category: "constraint", Statement: "Keep WAL enabled", Status: "active", Provenance: []string{"e1"}},
				},
				TotalRecallCalls:  2,
				TotalRestoreCalls: 1,
				TotalTurns:        3,
				RecentTurns: []protocol.ContextTurnMetric{
					{TurnIndex: 3, InputTokens: 1000, CacheReadTokens: 800, LedgerHash: "cafe1234"},
				},
			},
		}
		b, _ := json.Marshal(resp)
		_, _ = conn.Write(append(b, '\n'))
	}()

	p, err := NewAGPProvider(config.AgentConfig{
		ID:       "gp",
		Type:     "agp",
		Endpoint: "unix://" + sockPath,
	}, config.DefaultConfig())
	if err != nil {
		t.Fatalf("NewAGPProvider failed: %v", err)
	}

	stats, err := p.GetContextStats(context.Background(), "chat_123")
	if err != nil {
		t.Fatalf("GetContextStats failed: %v", err)
	}
	if stats.SessionID != "chat_123_eff" || stats.LedgerHash != "cafe1234" || stats.EpochID != 1 {
		t.Fatalf("unexpected stats: %+v", stats)
	}
	if stats.TotalRecallCalls != 2 || stats.TotalRestoreCalls != 1 || len(stats.RecentTurns) != 1 {
		t.Fatalf("unexpected counts/turns: %+v", stats)
	}
	if len(stats.ActiveConstraints) != 1 || stats.ActiveConstraints[0].ID != "c1" {
		t.Fatalf("unexpected constraints: %+v", stats.ActiveConstraints)
	}
}
