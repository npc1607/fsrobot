package handler

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	lark "github.com/larksuite/oapi-sdk-go/v3"

	"fsrobot/internal/registry"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

type stubCtxStatProvider struct {
	meta          registry.AgentMeta
	stats         *registry.ContextStats
	executeCalled bool
}

func (s *stubCtxStatProvider) Meta() registry.AgentMeta { return s.meta }
func (s *stubCtxStatProvider) Execute(_ context.Context, _ registry.RunRequest) (<-chan registry.StreamEvent, error) {
	s.executeCalled = true
	panic("Execute MUST NOT be called by /ctx-stat")
}
func (s *stubCtxStatProvider) ResetSession(_ context.Context, _ string) error { return nil }
func (s *stubCtxStatProvider) Close() error                                   { return nil }
func (s *stubCtxStatProvider) GetContextStats(_ context.Context, _ string) (*registry.ContextStats, error) {
	return s.stats, nil
}

func TestFormatContextStatsCard(t *testing.T) {
	stats := &registry.ContextStats{
		SessionID:         "chat_oc_1",
		MessageCount:      24,
		EpochID:           1,
		EpochStride:       16,
		EpochPosition:     6,
		FreezeBoundary:    16,
		MaskedCount:       5,
		MaskWindow:        5,
		LedgerVersion:     4,
		LedgerHash:        "a1b2c3d4",
		LedgerActiveCnt:   2,
		LedgerSuperseded:  1,
		LedgerResolvedCnt: 1,
		ActiveConstraints: []registry.ContextConstraintSummary{
			{ID: "cst_vpc", Category: "environment", Statement: "Offline VPC only", Status: "active", Provenance: []string{"ent_1", "ent_2"}},
			{ID: "cst_db", Category: "decision", Statement: "Use SQLite WAL", Status: "active", Provenance: []string{"ent_5"}},
		},
		SupersededConstraints: []registry.ContextConstraintSummary{
			{ID: "cst_old", Category: "decision", Statement: "Use JSON file", Status: "superseded", SupersededBy: "cst_db"},
		},
		TotalRecallCalls:  3,
		TotalRestoreCalls: 2,
		TotalSearchCalls:  5,
		TotalTurns:        7,
		// Newest-first order as returned by sqlite ListContextMetrics
		RecentTurns: []registry.ContextTurnMetric{
			{
				TurnIndex:            7,
				StrategyMode:         "e4a_pr2",
				EpochID:              1,
				LedgerVersion:        4,
				LedgerHash:           "a1b2c3d4",
				InputTokens:          2000,
				CacheReadTokens:      1800,
				CacheWriteTokens:     0,
				ContextBuildMs:       2.5,
				WorkingSetEntries:    13,
				WorkingSetTokens:     3746,
				RetrievalBuildMs:     0.0,
				GraphMatchedEntities: 6,
				SearchCalls:          2,
				RecallCalls:          1,
				CreatedAt:            "16:10:07",
			},
			{
				TurnIndex:        6,
				StrategyMode:     "e4a_pr1",
				EpochID:          1,
				LedgerVersion:    4,
				LedgerHash:       "a1b2c3d4",
				InputTokens:      1900,
				CacheReadTokens:  1520,
				CacheWriteTokens: 100,
				ContextBuildMs:   2.1,
				RetrievalBuildMs: 4.3,
				RestoreCalls:     1,
				CreatedAt:        "16:10:06",
			},
		},
	}

	out := formatContextStatsCard("gp", stats)
	checks := []string{
		"chat_oc_1",
		"Epoch `#1`",
		"Epoch 偏移 `6/16`",
		"冻结边界 `msg[16]`",
		"已折叠 `5` 条",
		"a1b2c3d4",
		"cst_vpc",
		"Offline VPC only",
		"~~`cst_old`~~ (→ `cst_db`)",
		"`recall_entries`: **3** 次 | `restore_observation`: **2** 次 | `search_context`: **5** 次",
		"策略: `e4a_pr2` | WorkingSet: `13` 条 / `3746` tok (命中实体 `6`, 检索 `0.0ms`)",
		"**T6** (`16:10:06`, ep#1, e4a_pr1, ws 0条/0tok, ledger v4/a1b2c3d4): Input `1900` · CacheRead `1520` · CacheWrite `100` · Hit **`80.0%`** · 2.1ms (ret 4.3ms) · [restore×1]",
		"**T7** (`16:10:07`, ep#1, e4a_pr2, ws 13条/3746tok, ledger v4/a1b2c3d4): Input `2000` · CacheRead `1800` · CacheWrite `0` · Hit **`90.0%`** · 2.5ms (ret 0.0ms) · [search×2, recall×1]",
	}
	for _, want := range checks {
		if !strings.Contains(out, want) {
			t.Fatalf("formatted card missing %q\nfull output:\n%s", want, out)
		}
	}
	if strings.Index(out, "**T6**") > strings.Index(out, "**T7**") {
		t.Fatalf("expected chronological turn order T6 before T7, got:\n%s", out)
	}
}

func TestFormatContextStatsCard_ZeroWorkingSetAndLegacyTurn(t *testing.T) {
	stats := &registry.ContextStats{
		SessionID: "chat_zero_ws",
		RecentTurns: []registry.ContextTurnMetric{
			{
				TurnIndex:       148,
				StrategyMode:    "e4a_pr2",
				EpochID:         7,
				LedgerVersion:   3,
				LedgerHash:      "21afc8a9",
				InputTokens:     77475,
				CacheReadTokens: 77184,
				ContextBuildMs:  1.5,
				CreatedAt:       "11:53:37",
			},
			{
				TurnIndex:       147,
				EpochID:         7,
				LedgerVersion:   3,
				LedgerHash:      "21afc8a9",
				InputTokens:     70000,
				CacheReadTokens: 63000,
				ContextBuildMs:  1.2,
				CreatedAt:       "11:52:00",
			},
		},
	}

	out := formatContextStatsCard("gp", stats)
	for _, want := range []string{
		"`recall_entries`: **0** 次 | `restore_observation`: **0** 次 | `search_context`: **0** 次",
		"策略: `e4a_pr2` | WorkingSet: 未触发 (`0 tok`) (命中实体 `0`, 检索 `0.0ms`)",
		"**T147** (`11:52:00`, ep#7, ledger v3/21afc8a9): Input `70000` · CacheRead `63000` · CacheWrite `0` · Hit **`90.0%`** · 1.2ms",
		"**T148** (`11:53:37`, ep#7, e4a_pr2, ws 0条/0tok, ledger v3/21afc8a9): Input `77475` · CacheRead `77184` · CacheWrite `0` · Hit **`99.6%`** · 1.5ms (ret 0.0ms)",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("formatted card missing %q\nfull output:\n%s", want, out)
		}
	}
}

func TestHandleCommand_CtxStat_DoesNotInvokeLLMAndRepliesCard(t *testing.T) {
	var sentCardBody string
	mockHTTP := &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			if req.Body != nil {
				b, _ := io.ReadAll(req.Body)
				sentCardBody = string(b)
			}
			respPayload := `{"code":0,"msg":"ok","data":{"message_id":"om_card_1"}}`
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       io.NopCloser(strings.NewReader(respPayload)),
			}, nil
		}),
	}
	client := lark.NewClient("cli_test", "sec_test", lark.WithHttpClient(mockHTTP))

	prov := &stubCtxStatProvider{
		meta: registry.AgentMeta{ID: "gp", Name: "GP"},
		stats: &registry.ContextStats{
			SessionID:       "chat_test",
			EpochID:         0,
			EpochStride:     16,
			MaskWindow:      5,
			LedgerVersion:   1,
			LedgerHash:      "11223344",
			LedgerActiveCnt: 1,
			ActiveConstraints: []registry.ContextConstraintSummary{
				{ID: "c_rule", Category: "constraint", Statement: "No LLM on ctx-stat"},
			},
			TotalRecallCalls:  1,
			TotalRestoreCalls: 4,
			TotalTurns:        1,
			RecentTurns: []registry.ContextTurnMetric{
				{TurnIndex: 1, InputTokens: 1000, CacheReadTokens: 750, CreatedAt: "16:00:00"},
			},
		},
	}
	reg := registry.NewRegistry("gp")
	reg.Register(prov)

	h := NewMessageHandler(client, reg, "bot_1", false, nil, nil)
	handled := h.handleCommand(context.Background(), "chat_test", "ou_user", "msg_1", "chat_test", "/ctx-stat")
	if !handled {
		t.Fatal("expected /ctx-stat to be handled as command")
	}
	if prov.executeCalled {
		t.Fatal("/ctx-stat MUST NOT call provider.Execute")
	}

	var envelope struct {
		MsgType string `json:"msg_type"`
		Content string `json:"content"`
	}
	if err := json.Unmarshal([]byte(sentCardBody), &envelope); err != nil {
		t.Fatalf("failed to decode Feishu reply body %q: %v", sentCardBody, err)
	}
	if envelope.MsgType != "interactive" || !strings.Contains(envelope.Content, "11223344") || !strings.Contains(envelope.Content, "75.0%") {
		t.Fatalf("unexpected reply card payload: %+v", envelope)
	}
}
