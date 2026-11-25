package handler

import (
	"testing"
	"time"
)

func TestApprovalManager(t *testing.T) {
	emptyMgr := NewApprovalManager(nil)
	if emptyMgr.HasAdmins() {
		t.Fatal("expected HasAdmins() == false when admin list is empty")
	}

	mgr := NewApprovalManager([]string{"ou_admin_1"})
	if !mgr.HasAdmins() {
		t.Fatal("expected HasAdmins() == true")
	}

	var gotApproved *bool
	mgr.Add("chat_1", "apr_1", "bash", "rm test", func(approved bool) error {
		gotApproved = &approved
		return nil
	})

	// 1. Non-admin cannot approve
	matched, auth, _, _, _ := mgr.TryHandle("chat_1", "ou_other", "确认")
	if !matched || auth || gotApproved != nil {
		t.Fatalf("expected non-admin rejection, matched=%v auth=%v gotApproved=%v", matched, auth, gotApproved)
	}

	// 2. Normal message does not match approval
	matched, _, _, _, _ = mgr.TryHandle("chat_1", "ou_admin_1", "顺便帮我看下日志")
	if matched {
		t.Fatal("expected normal message not to match approval")
	}

	// 3. Admin approves
	matched, auth, approved, id, err := mgr.TryHandle("chat_1", "ou_admin_1", "确认")
	if err != nil || !matched || !auth || !approved || id != "apr_1" {
		t.Fatalf("expected admin approval success, matched=%v auth=%v approved=%v id=%q err=%v", matched, auth, approved, id, err)
	}
	if gotApproved == nil || !*gotApproved {
		t.Fatalf("expected Reply(true) to be called")
	}
}

func TestApprovalManagerTimeout(t *testing.T) {
	mgr := NewApprovalManager([]string{"ou_admin_1"})
	mgr.timeout = 30 * time.Millisecond

	replied := make(chan bool, 1)
	timedOut := make(chan struct{}, 1)
	mgr.AddWithTimeout("chat_1", "apr_t1", "bash", "rm test", func(approved bool) error {
		replied <- approved
		return nil
	}, func() {
		close(timedOut)
	})

	select {
	case approved := <-replied:
		if approved {
			t.Fatal("expected timeout to auto-reject (approved=false)")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for auto-rejection callback")
	}

	select {
	case <-timedOut:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for onTimeout hook")
	}
}

func TestApprovalManagerCrossChatDMReply(t *testing.T) {
	mgr := NewApprovalManager([]string{"ou_admin_1"})

	var gotApproved *bool
	mgr.Add("oc_group_1", "apr_dm_1", "bash", "access outside workdir", func(approved bool) error {
		gotApproved = &approved
		return nil
	})

	// Non-admin replying in another chat should not match or resolve
	matched, _, _, _, _ := mgr.TryHandle("oc_dm_other", "ou_non_admin", "确认")
	if matched || gotApproved != nil {
		t.Fatalf("expected non-admin DM not to match group approval, matched=%v got=%v", matched, gotApproved)
	}

	// Admin replying in DM chat resolves the group's pending approval
	matched, auth, approved, id, err := mgr.TryHandle("oc_dm_admin", "ou_admin_1", "确认")
	if err != nil || !matched || !auth || !approved || id != "apr_dm_1" {
		t.Fatalf("expected admin DM approval success, matched=%v auth=%v approved=%v id=%q err=%v", matched, auth, approved, id, err)
	}
	if gotApproved == nil || !*gotApproved {
		t.Fatal("expected Reply(true) to be called")
	}
}
