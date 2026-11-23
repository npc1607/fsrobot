package handler

import (
	"context"
	"path/filepath"
	"testing"
)

func TestSessionManagerCancelAllowed(t *testing.T) {
	sm := NewSessionManager()

	_, cancelUserA := context.WithCancel(context.Background())
	_, cancelUserB := context.WithCancel(context.Background())

	sm.StartRun("gp:chat_1", "run_a", "chat_1", "ou_user_a", "task A", false, cancelUserA)
	sm.StartRun("gp:chat_1", "run_b", "chat_1:sub:1", "ou_user_b", "task B", true, cancelUserB)

	// Non-admin user C cannot cancel anyone's task
	if canceled, total := sm.CancelAllowed("gp:chat_1", "ou_user_c", false); canceled != 0 || total != 2 {
		t.Fatalf("expected user C to cancel 0 of 2, got canceled=%d total=%d", canceled, total)
	}

	// Non-admin initiator user A can only cancel their own task (1 of 2)
	if canceled, total := sm.CancelAllowed("gp:chat_1", "ou_user_a", false); canceled != 1 || total != 2 {
		t.Fatalf("expected user A to cancel 1 of 2, got canceled=%d total=%d", canceled, total)
	}

	// Admin (allowAll=true) can cancel all remaining tasks
	if canceled, total := sm.CancelAllowed("gp:chat_1", "ou_boss", true); canceled != 2 || total != 2 {
		t.Fatalf("expected admin to cancel all 2, got canceled=%d total=%d", canceled, total)
	}
}

func TestSessionManagerCancelRunTargetsSingleTask(t *testing.T) {
	sm := NewSessionManager()
	var mainCanceled, subCanceled bool
	sm.StartRun("gp:chat_1", "run_main", "chat_1", "ou_user_a", "main", false, func() { mainCanceled = true })
	sm.StartRun("gp:chat_1", "run_sub", "chat_1:sub:1", "ou_user_a", "sub", true, func() { subCanceled = true })

	// Unauthorized user cannot cancel run_main
	if canceled, exists := sm.CancelRun("gp:chat_1", "run_main", "ou_other", false); canceled || !exists {
		t.Fatalf("expected unauthorized CancelRun to fail while exists=true, got canceled=%v exists=%v", canceled, exists)
	}
	if mainCanceled || subCanceled {
		t.Fatal("neither task should be canceled")
	}

	// Initiator cancels only run_main; run_sub stays untouched
	if canceled, exists := sm.CancelRun("gp:chat_1", "run_main", "ou_user_a", false); !canceled || !exists {
		t.Fatalf("expected CancelRun to succeed, got canceled=%v exists=%v", canceled, exists)
	}
	if !mainCanceled || subCanceled {
		t.Fatalf("expected only mainCanceled=true and subCanceled=false, got main=%v sub=%v", mainCanceled, subCanceled)
	}
}

func TestSessionManagerRolePersistenceAcrossRestart(t *testing.T) {
	storePath := filepath.Join(t.TempDir(), "chat_roles.json")

	sm1 := NewSessionManager()
	sm1.EnableRolePersistence(storePath)
	sm1.SetRole("chat_abc", "night_city_prompt")

	// Simulate restart with a brand new SessionManager loading from the same storePath
	sm2 := NewSessionManager()
	sm2.EnableRolePersistence(storePath)
	if got := sm2.GetRole("chat_abc"); got != "night_city_prompt" {
		t.Fatalf("expected persisted role after restart, got %q", got)
	}

	// Resetting role removes it from disk too
	sm2.SetRole("chat_abc", "")
	sm3 := NewSessionManager()
	sm3.EnableRolePersistence(storePath)
	if got := sm3.GetRole("chat_abc"); got != "" {
		t.Fatalf("expected empty role after reset and restart, got %q", got)
	}
}
