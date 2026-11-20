package handler

import (
	"strings"
	"sync"
	"time"
)

// PendingApproval represents an in-flight high-risk action awaiting admin confirmation.
type PendingApproval struct {
	ID        string
	ChatID    string
	Tool      string
	Reason    string
	Reply     func(approved bool) error
	CreatedAt time.Time
}

// ApprovalManager manages admin allowlists and pending high-risk confirmations per chat.
type ApprovalManager struct {
	mu       sync.Mutex
	admins   map[string]struct{}
	pending  map[string][]*PendingApproval
}

// NewApprovalManager creates an ApprovalManager with the configured admin open_ids.
func NewApprovalManager(adminOpenIDs []string) *ApprovalManager {
	admins := make(map[string]struct{}, len(adminOpenIDs))
	for _, id := range adminOpenIDs {
		id = strings.TrimSpace(id)
		if id != "" {
			admins[id] = struct{}{}
		}
	}
	return &ApprovalManager{
		admins:  admins,
		pending: make(map[string][]*PendingApproval),
	}
}

// HasAdmins reports whether at least one admin open_id is configured.
func (m *ApprovalManager) HasAdmins() bool {
	return len(m.admins) > 0
}

// IsAdmin reports whether openID belongs to the configured admin list.
func (m *ApprovalManager) IsAdmin(openID string) bool {
	_, ok := m.admins[strings.TrimSpace(openID)]
	return ok
}

// Add registers a pending approval for chatID.
func (m *ApprovalManager) Add(chatID, approvalID, tool, reason string, replyFn func(bool) error) {
	if chatID == "" || approvalID == "" || replyFn == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.pending[chatID] = append(m.pending[chatID], &PendingApproval{
		ID:        approvalID,
		ChatID:    chatID,
		Tool:      tool,
		Reason:    reason,
		Reply:     replyFn,
		CreatedAt: time.Now(),
	})
}

// Remove deletes a pending approval (e.g. on timeout or cancellation).
func (m *ApprovalManager) Remove(chatID, approvalID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	list := m.pending[chatID]
	for i, item := range list {
		if item.ID == approvalID {
			m.pending[chatID] = append(list[:i], list[i+1:]...)
			if len(m.pending[chatID]) == 0 {
				delete(m.pending, chatID)
			}
			return
		}
	}
}

// TryHandle checks if text is an approval/rejection reply for a pending action in chatID.
func (m *ApprovalManager) TryHandle(chatID, senderOpenID, text string) (matched, authorized, approved bool, approvalID string, err error) {
	fields := strings.Fields(strings.TrimSpace(text))
	if len(fields) == 0 || len(fields) > 2 {
		return false, false, false, "", nil
	}

	var wantApprove bool
	switch strings.ToLower(fields[0]) {
	case "确认", "同意", "允许", "放行", "yes", "ok", "y":
		wantApprove = true
	case "拒绝", "取消", "不同意", "拦截", "no", "n":
		wantApprove = false
	default:
		return false, false, false, "", nil
	}

	targetID := ""
	if len(fields) == 2 {
		targetID = strings.TrimPrefix(fields[1], "#")
	}

	m.mu.Lock()
	list := m.pending[chatID]
	if len(list) == 0 {
		m.mu.Unlock()
		return false, false, false, "", nil
	}

	idx := len(list) - 1 // default to most recent pending approval in this chat
	if targetID != "" {
		idx = -1
		for i, item := range list {
			if item.ID == targetID {
				idx = i
				break
			}
		}
		if idx == -1 {
			m.mu.Unlock()
			return false, false, false, "", nil
		}
	}

	if !m.IsAdmin(senderOpenID) {
		id := list[idx].ID
		m.mu.Unlock()
		return true, false, false, id, nil
	}

	chosen := list[idx]
	m.pending[chatID] = append(list[:idx], list[idx+1:]...)
	if len(m.pending[chatID]) == 0 {
		delete(m.pending, chatID)
	}
	m.mu.Unlock()

	if chosen.Reply != nil {
		err = chosen.Reply(wantApprove)
	}
	return true, true, wantApprove, chosen.ID, err
}

// ResolveByID resolves a specific pending approval by ID (used by card button callbacks).
func (m *ApprovalManager) ResolveByID(chatID, approvalID, senderOpenID string, wantApprove bool) (found, authorized bool, item *PendingApproval, err error) {
	m.mu.Lock()
	list := m.pending[chatID]
	idx := -1
	for i, it := range list {
		if it.ID == approvalID {
			idx = i
			break
		}
	}
	if idx == -1 {
		m.mu.Unlock()
		return false, false, nil, nil
	}

	if !m.IsAdmin(senderOpenID) {
		it := *list[idx]
		m.mu.Unlock()
		return true, false, &it, nil
	}

	chosen := list[idx]
	m.pending[chatID] = append(list[:idx], list[idx+1:]...)
	if len(m.pending[chatID]) == 0 {
		delete(m.pending, chatID)
	}
	m.mu.Unlock()

	if chosen.Reply != nil {
		err = chosen.Reply(wantApprove)
	}
	return true, true, chosen, err
}
