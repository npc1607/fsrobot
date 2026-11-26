package agent

import (
	"bufio"
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
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

func startMockUnixAGP(t *testing.T, onConn func(net.Conn)) string {
	t.Helper()
	sockDir, err := os.MkdirTemp("/tmp", "agp-t-")
	if err != nil {
		t.Fatalf("os.MkdirTemp failed: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(sockDir) })
	sockPath := filepath.Join(sockDir, "s.sock")
	ln, err := net.Listen("unix", sockPath)
	if err != nil {
		t.Fatalf("net.Listen unix failed: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				onConn(c)
			}(conn)
		}
	}()
	return "unix://" + sockPath
}

func TestAGPProvider_GetContextStats_RoundTrip(t *testing.T) {
	endpoint := startMockUnixAGP(t, func(conn net.Conn) {
		scanner := bufio.NewScanner(conn)
		for scanner.Scan() {
			var req protocol.Request
			_ = json.Unmarshal(scanner.Bytes(), &req)
			var resp protocol.Event
			switch req.Op {
			case protocol.OpHello:
				resp = protocol.Event{ID: req.ID, Event: protocol.EventHelloAck, AgentName: "gp", AgentVersion: "test"}
			case protocol.OpContextStats:
				resp = protocol.Event{
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
						TotalSearchCalls:  4,
						TotalTurns:        3,
						RecentTurns: []protocol.ContextTurnMetric{
							{
								TurnIndex:            3,
								StrategyMode:         "e4a_pr2",
								InputTokens:          1000,
								CacheReadTokens:      800,
								LedgerHash:           "cafe1234",
								ContextBuildMs:       1.5,
								WorkingSetTokens:     3746,
								WorkingSetEntries:    13,
								RetrievalBuildMs:     12.4,
								GraphMatchedEntities: 5,
								RecallCalls:          1,
								SearchCalls:          2,
							},
						},
					},
				}
			default:
				resp = protocol.Event{ID: req.ID, Event: protocol.EventError, Code: "BAD_OP", Message: req.Op}
			}
			b, _ := json.Marshal(resp)
			_, _ = conn.Write(append(b, '\n'))
		}
	})

	p, err := NewAGPProvider(config.AgentConfig{
		ID:       "gp",
		Type:     "agp",
		Endpoint: endpoint,
	}, config.DefaultConfig())
	if err != nil {
		t.Fatalf("NewAGPProvider failed: %v", err)
	}
	defer func() { _ = p.Close() }()

	stats, err := p.GetContextStats(context.Background(), "chat_123")
	if err != nil {
		t.Fatalf("GetContextStats failed: %v", err)
	}
	if stats.SessionID != "chat_123_eff" || stats.LedgerHash != "cafe1234" || stats.EpochID != 1 {
		t.Fatalf("unexpected stats: %+v", stats)
	}
	if stats.TotalRecallCalls != 2 || stats.TotalRestoreCalls != 1 || stats.TotalSearchCalls != 4 || len(stats.RecentTurns) != 1 {
		t.Fatalf("unexpected counts/turns: %+v", stats)
	}
	gotTurn := stats.RecentTurns[0]
	if gotTurn.StrategyMode != "e4a_pr2" ||
		gotTurn.WorkingSetTokens != 3746 ||
		gotTurn.WorkingSetEntries != 13 ||
		gotTurn.RetrievalBuildMs != 12.4 ||
		gotTurn.GraphMatchedEntities != 5 ||
		gotTurn.RecallCalls != 1 ||
		gotTurn.SearchCalls != 2 {
		t.Fatalf("unexpected PR#2 turn metrics: %+v", gotTurn)
	}
	if len(stats.ActiveConstraints) != 1 || stats.ActiveConstraints[0].ID != "c1" {
		t.Fatalf("unexpected constraints: %+v", stats.ActiveConstraints)
	}
}

func TestProtocolContextStats_UnmarshalPR2AndLegacyJSON(t *testing.T) {
	pr2JSON := `{
		"id": "req_1",
		"event": "ctx_stat",
		"session_id": "chat_pr2",
		"context_stats": {
			"session_id": "chat_pr2",
			"total_recall_calls": 2,
			"total_restore_calls": 1,
			"total_search_calls": 5,
			"total_turns": 148,
			"recent_turns": [
				{
					"turn_index": 148,
					"strategy_mode": "e4a_pr2",
					"epoch_id": 7,
					"ledger_version": 3,
					"ledger_hash": "21afc8a9",
					"context_build_ms": 1.5,
					"working_set_tokens": 3746,
					"working_set_entries": 13,
					"retrieval_build_ms": 8.25,
					"graph_matched_entities": 4,
					"input_tokens": 77475,
					"cache_read_tokens": 77184,
					"cache_write_tokens": 0,
					"recall_calls": 1,
					"restore_calls": 0,
					"search_calls": 3,
					"created_at": "11:53:37"
				}
			]
		}
	}`
	var ev protocol.Event
	if err := json.Unmarshal([]byte(pr2JSON), &ev); err != nil {
		t.Fatalf("unmarshal PR2 event failed: %v", err)
	}
	if ev.ContextStats == nil || ev.ContextStats.TotalSearchCalls != 5 || len(ev.ContextStats.RecentTurns) != 1 {
		t.Fatalf("unexpected PR2 ContextStats: %+v", ev.ContextStats)
	}
	tm := ev.ContextStats.RecentTurns[0]
	if tm.StrategyMode != "e4a_pr2" || tm.WorkingSetTokens != 3746 || tm.WorkingSetEntries != 13 ||
		tm.RetrievalBuildMs != 8.25 || tm.GraphMatchedEntities != 4 || tm.SearchCalls != 3 {
		t.Fatalf("unexpected PR2 turn metric: %+v", tm)
	}

	legacyJSON := `{
		"id": "req_2",
		"event": "ctx_stat",
		"session_id": "chat_legacy",
		"context_stats": {
			"session_id": "chat_legacy",
			"total_recall_calls": 1,
			"total_restore_calls": 2,
			"total_turns": 3,
			"recent_turns": [
				{
					"turn_index": 3,
					"epoch_id": 1,
					"ledger_version": 1,
					"ledger_hash": "abcd1234",
					"context_build_ms": 2.0,
					"input_tokens": 1000,
					"cache_read_tokens": 800
				}
			]
		}
	}`
	var legacyEv protocol.Event
	if err := json.Unmarshal([]byte(legacyJSON), &legacyEv); err != nil {
		t.Fatalf("unmarshal legacy event failed: %v", err)
	}
	if legacyEv.ContextStats == nil || legacyEv.ContextStats.TotalSearchCalls != 0 || len(legacyEv.ContextStats.RecentTurns) != 1 {
		t.Fatalf("unexpected legacy ContextStats: %+v", legacyEv.ContextStats)
	}
	ltm := legacyEv.ContextStats.RecentTurns[0]
	if ltm.StrategyMode != "" || ltm.WorkingSetTokens != 0 || ltm.WorkingSetEntries != 0 ||
		ltm.RetrievalBuildMs != 0 || ltm.GraphMatchedEntities != 0 || ltm.SearchCalls != 0 {
		t.Fatalf("expected zero defaults for missing PR2 fields, got: %+v", ltm)
	}
}

func TestAGPProvider_PersistentConnectionHandshakeAndReuse(t *testing.T) {
	var connCount atomic.Int32
	var helloCount atomic.Int32
	var ops []string
	var opsMu sync.Mutex

	endpoint := startMockUnixAGP(t, func(conn net.Conn) {
		connCount.Add(1)
		scanner := bufio.NewScanner(conn)
		for scanner.Scan() {
			var req protocol.Request
			_ = json.Unmarshal(scanner.Bytes(), &req)
			opsMu.Lock()
			ops = append(ops, req.Op)
			opsMu.Unlock()

			writeEv := func(ev protocol.Event) {
				b, _ := json.Marshal(ev)
				_, _ = conn.Write(append(b, '\n'))
			}
			switch req.Op {
			case protocol.OpHello:
				helloCount.Add(1)
				writeEv(protocol.Event{ID: req.ID, Event: protocol.EventHelloAck, AgentName: "gp"})
			case protocol.OpRun:
				writeEv(protocol.Event{ID: req.ID, Event: protocol.EventStarted, SessionID: req.SessionID})
				writeEv(protocol.Event{ID: req.ID, Event: protocol.EventDelta, Text: "hi"})
				writeEv(protocol.Event{ID: req.ID, Event: protocol.EventFinal, Text: "hi"})
			case protocol.OpResetSession:
				writeEv(protocol.Event{ID: req.ID, Event: protocol.EventResetAck, SessionID: req.SessionID})
			case protocol.OpListSessions:
				writeEv(protocol.Event{ID: req.ID, Event: protocol.EventSessionsList, SessionID: req.SessionID})
			case protocol.OpContextStats:
				writeEv(protocol.Event{ID: req.ID, Event: protocol.EventContextStats, SessionID: req.SessionID, ContextStats: &protocol.ContextStats{SessionID: req.SessionID}})
			}
		}
	})

	p, err := NewAGPProvider(config.AgentConfig{
		ID:       "gp",
		Type:     "agp",
		Endpoint: endpoint,
	}, config.DefaultConfig())
	if err != nil {
		t.Fatalf("NewAGPProvider failed: %v", err)
	}

	evCh, err := p.Execute(context.Background(), registry.RunRequest{SessionID: "chat_1", Prompt: "ping"})
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	for range evCh {
	}
	if _, err := p.GetContextStats(context.Background(), "chat_1"); err != nil {
		t.Fatalf("GetContextStats failed: %v", err)
	}
	if err := p.ResetSession(context.Background(), "chat_1"); err != nil {
		t.Fatalf("ResetSession failed: %v", err)
	}
	if _, _, err := p.ListSessions(context.Background(), "chat_1"); err != nil {
		t.Fatalf("ListSessions failed: %v", err)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	if got := connCount.Load(); got != 1 {
		t.Fatalf("expected exactly 1 persistent connection across 4 calls, got %d", got)
	}
	if got := helloCount.Load(); got != 1 {
		t.Fatalf("expected exactly 1 OpHello handshake, got %d", got)
	}
	opsMu.Lock()
	defer opsMu.Unlock()
	if len(ops) < 5 || ops[0] != protocol.OpHello || ops[1] != protocol.OpRun {
		t.Fatalf("expected OpHello first before OpRun, got ops=%v", ops)
	}
}

func TestAGPProvider_SendsAbortFrameOnCancelAndIdleTimeout(t *testing.T) {
	var abortTargets []string
	var abortMu sync.Mutex
	abortCh := make(chan string, 4)

	endpoint := startMockUnixAGP(t, func(conn net.Conn) {
		scanner := bufio.NewScanner(conn)
		var writeMu sync.Mutex
		writeEv := func(ev protocol.Event) {
			b, _ := json.Marshal(ev)
			writeMu.Lock()
			_, _ = conn.Write(append(b, '\n'))
			writeMu.Unlock()
		}
		for scanner.Scan() {
			var req protocol.Request
			_ = json.Unmarshal(scanner.Bytes(), &req)
			switch req.Op {
			case protocol.OpHello:
				writeEv(protocol.Event{ID: req.ID, Event: protocol.EventHelloAck, AgentName: "gp"})
			case protocol.OpRun:
				writeEv(protocol.Event{ID: req.ID, Event: protocol.EventStarted, SessionID: req.SessionID})
			case protocol.OpAbort:
				abortMu.Lock()
				abortTargets = append(abortTargets, req.TargetID)
				abortMu.Unlock()
				writeEv(protocol.Event{ID: req.ID, Event: protocol.EventAbortAck})
				writeEv(protocol.Event{ID: req.TargetID, Event: protocol.EventError, Code: "ABORTED", Message: "aborted by client"})
				abortCh <- req.TargetID
			case protocol.OpContextStats:
				writeEv(protocol.Event{ID: req.ID, Event: protocol.EventContextStats, SessionID: req.SessionID, ContextStats: &protocol.ContextStats{SessionID: req.SessionID}})
			}
		}
	})

	// 1. Context cancellation sends explicit OpAbort and keeps persistent connection healthy
	p, err := NewAGPProvider(config.AgentConfig{
		ID:       "gp",
		Type:     "agp",
		Endpoint: endpoint,
	}, config.DefaultConfig())
	if err != nil {
		t.Fatalf("NewAGPProvider failed: %v", err)
	}
	defer func() { _ = p.Close() }()

	runCtx, cancelRun := context.WithCancel(context.Background())
	evCh, err := p.Execute(runCtx, registry.RunRequest{SessionID: "chat_abort", Prompt: "long task"})
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	<-evCh // receive EventThinking (started)
	cancelRun()
	for range evCh {
	}

	select {
	case target := <-abortCh:
		if target == "" {
			t.Fatal("expected non-empty TargetID in OpAbort")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for OpAbort frame on cancel")
	}

	// Persistent connection remains reusable after aborting a run
	if _, err := p.GetContextStats(context.Background(), "chat_abort"); err != nil {
		t.Fatalf("expected persistent connection to stay healthy after abort, got: %v", err)
	}

	// 2. Idle timeout also sends explicit OpAbort
	sess, err := p.getOrConnect(context.Background())
	if err != nil {
		t.Fatalf("getOrConnect failed: %v", err)
	}
	out2 := make(chan registry.StreamEvent, 8)
	_, err = sess.run(context.Background(), protocol.Request{
		ID:        "run-idle-abort",
		Op:        protocol.OpRun,
		SessionID: "chat_idle",
		Prompt:    "hang",
	}, out2, 60*time.Millisecond)
	if err == nil || !strings.Contains(err.Error(), "idle timed out") {
		t.Fatalf("expected idle timed out error, got: %v", err)
	}
	select {
	case target := <-abortCh:
		if target != "run-idle-abort" {
			t.Fatalf("expected OpAbort TargetID='run-idle-abort', got %q", target)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for OpAbort frame on idle timeout")
	}
}

func TestAGPProvider_ReconnectsAndRehandshakesOnBrokenConn(t *testing.T) {
	var connNum atomic.Int32
	var helloNum atomic.Int32

	endpoint := startMockUnixAGP(t, func(conn net.Conn) {
		cIdx := connNum.Add(1)
		scanner := bufio.NewScanner(conn)
		for scanner.Scan() {
			var req protocol.Request
			_ = json.Unmarshal(scanner.Bytes(), &req)
			writeEv := func(ev protocol.Event) {
				b, _ := json.Marshal(ev)
				_, _ = conn.Write(append(b, '\n'))
			}
			switch req.Op {
			case protocol.OpHello:
				helloNum.Add(1)
				writeEv(protocol.Event{ID: req.ID, Event: protocol.EventHelloAck, AgentName: "gp"})
			case protocol.OpContextStats:
				if cIdx == 1 {
					// Simulate server restarting / dropping connection cleanly before replying
					return
				}
				writeEv(protocol.Event{ID: req.ID, Event: protocol.EventContextStats, SessionID: req.SessionID, ContextStats: &protocol.ContextStats{SessionID: req.SessionID}})
			}
		}
	})

	p, err := NewAGPProvider(config.AgentConfig{
		ID:       "gp",
		Type:     "agp",
		Endpoint: endpoint,
	}, config.DefaultConfig())
	if err != nil {
		t.Fatalf("NewAGPProvider failed: %v", err)
	}
	defer func() { _ = p.Close() }()

	stats, err := p.GetContextStats(context.Background(), "chat_reconnect")
	if err != nil {
		t.Fatalf("expected transparent reconnect & rehandshake, got: %v", err)
	}
	if stats.SessionID != "chat_reconnect" {
		t.Fatalf("unexpected SessionID: %+v", stats)
	}
	if connNum.Load() != 2 || helloNum.Load() != 2 {
		t.Fatalf("expected 2 connections & 2 hellos after reconnect, got conn=%d hello=%d", connNum.Load(), helloNum.Load())
	}
}
