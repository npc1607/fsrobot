package handler

import (
	"testing"
	"time"
)

func TestQuestionManagerButtonAndTextReply(t *testing.T) {
	qm := NewQuestionManager()
	qm.timeout = 5 * time.Second

	var gotAnswer string
	var gotSkipped bool
	qm.AddWithTimeout("chat_1", "ask_1", "ou_user", "请选择方案", []string{"方案A", "方案B"}, "card_1", func(ans string, sk bool) error {
		gotAnswer = ans
		gotSkipped = sk
		return nil
	}, nil)

	// Unauthorized user cannot click or reply
	found, auth, _, _ := qm.ResolveByID("chat_1", "ask_1", "ou_stranger", false, "方案A", false)
	if !found || auth {
		t.Fatalf("expected stranger button click to be rejected (found=%v auth=%v)", found, auth)
	}
	if matched, _, _ := qm.ResolveByCardText("chat_1", "card_1", "ou_stranger", false, "方案C"); matched {
		t.Fatal("expected stranger text reply NOT to match pending question")
	}

	// Unrelated message (empty parentID or quoting a different card) must NOT hijack card_1
	if matched, _, _ := qm.ResolveByCardText("chat_1", "", "ou_user", false, "随便发句话"); matched {
		t.Fatal("expected non-reply message NOT to hijack pending question")
	}
	if matched, _, _ := qm.ResolveByCardText("chat_1", "other_card", "ou_user", false, "回复别的消息"); matched {
		t.Fatal("expected reply to other card NOT to hijack card_1")
	}

	// Initiator replies directly to card_1 with free-form text
	matched, item, err := qm.ResolveByCardText("chat_1", "card_1", "ou_user", false, "都不选，按方案 C 做")
	if err != nil || !matched || item == nil || item.CardID != "card_1" {
		t.Fatalf("expected text reply to resolve question, matched=%v item=%+v err=%v", matched, item, err)
	}
	if gotAnswer != "都不选，按方案 C 做" || gotSkipped {
		t.Fatalf("unexpected reply payload: answer=%q skipped=%v", gotAnswer, gotSkipped)
	}

	// Second question: admin clicks skip
	qm.AddWithTimeout("chat_1", "ask_2", "ou_user", "选环境", []string{"test", "prod"}, "card_2", func(ans string, sk bool) error {
		gotAnswer = ans
		gotSkipped = sk
		return nil
	}, nil)
	found, auth, _, err = qm.ResolveByID("chat_1", "ask_2", "ou_boss", true, "", true)
	if err != nil || !found || !auth || !gotSkipped {
		t.Fatalf("expected admin skip to succeed, found=%v auth=%v skipped=%v err=%v", found, auth, gotSkipped, err)
	}
}
