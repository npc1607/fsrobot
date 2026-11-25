package handler

import (
	"strings"
	"sync"
	"time"
)

const defaultApprovalTimeout = 60 * time.Second

// PendingApproval represents an in-flight high-risk action awaiting admin confirmation.
type PendingApproval struct {
	ID        string
	ChatID    string
	Tool      string
	Reason    string
	Reply     func(approved bool) error
	CreatedAt time.Time
	timer     *time.Timer
}

// ApprovalManager manages admin allowlists and pending high-risk confirmations per chat.
type ApprovalManager struct {
	mu        sync.Mutex
	admins    map[string]struct{}
	adminList []string
	pending   map[string][]*PendingApproval
	timeout   time.Duration
}

// NewApprovalManager creates an ApprovalManager with the configured admin open_ids.
func NewApprovalManager(adminOpenIDs []string) *ApprovalManager {
	admins := make(map[string]struct{}, len(adminOpenIDs))
	var adminList []string
	for _, id := range adminOpenIDs {
		id = strings.TrimSpace(id)
		if id != "" {
			if _, exists := admins[id]; !exists {
				admins[id] = struct{}{}
				adminList = append(adminList, id)
			}
		}
	}
	return &ApprovalManager{
		admins:    admins,
		adminList: adminList,
		pending:   make(map[string][]*PendingApproval),
		timeout:   defaultApprovalTimeout,
	}
}

// AdminIDs returns a copy of the configured admin open_ids in order.
func (m *ApprovalManager) AdminIDs() []string {
	out := make([]string, len(m.adminList))
	copy(out, m.adminList)
	return out
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

// Add registers a pending approval with an automatic 15s timeout rejection.
func (m *ApprovalManager) Add(chatID, approvalID, tool, reason string, replyFn func(bool) error) {
	m.AddWithTimeout(chatID, approvalID, tool, reason, replyFn, nil)
}

// AddWithTimeout registers a pending approval and triggers onTimeout if not resolved within m.timeout.
func (m *ApprovalManager) AddWithTimeout(chatID, approvalID, tool, reason string, replyFn func(bool) error, onTimeout func()) {
	if chatID == "" || approvalID == "" || replyFn == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	item := &PendingApproval{
		ID:        approvalID,
		ChatID:    chatID,
		Tool:      tool,
		Reason:    reason,
		Reply:     replyFn,
		CreatedAt: time.Now(),
	}

	if m.timeout > 0 {
		item.timer = time.AfterFunc(m.timeout, func() {
			if !m.expire(chatID, approvalID) {
				return
			}
			_ = replyFn(false)
			if onTimeout != nil {
				onTimeout()
			}
		})
	}

	m.pending[chatID] = append(m.pending[chatID], item)
}

func (m *ApprovalManager) expire(chatID, approvalID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	list := m.pending[chatID]
	for i, item := range list {
		if item.ID == approvalID {
			m.pending[chatID] = append(list[:i], list[i+1:]...)
			if len(m.pending[chatID]) == 0 {
				delete(m.pending, chatID)
			}
			return true
		}
	}
	return false
}

// Remove deletes a pending approval (e.g. on cancellation or completion), stops its timer,
// and reports whether an unresolved approval was actually removed.
func (m *ApprovalManager) Remove(chatID, approvalID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	list := m.pending[chatID]
	for i, item := range list {
		if item.ID == approvalID {
			if item.timer != nil {
				item.timer.Stop()
			}
			m.pending[chatID] = append(list[:i], list[i+1:]...)
			if len(m.pending[chatID]) == 0 {
				delete(m.pending, chatID)
			}
			return true
		}
	}
	return false
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
	resolvedChatID := chatID
	list := m.pending[resolvedChatID]
	idx := -1

	if len(list) > 0 {
		if targetID != "" {
			for i, item := range list {
				if item.ID == targetID {
					idx = i
					break
				}
			}
		} else {
			idx = len(list) - 1
		}
	} else if m.IsAdmin(senderOpenID) {
		// Allow an admin replying from a DM chat to resolve a pending approval from another chat.
		var latestAt time.Time
		for cid, items := range m.pending {
			for i, item := range items {
				if targetID != "" {
					if item.ID == targetID {
						resolvedChatID = cid
						list = items
						idx = i
						break
					}
				} else if idx == -1 || item.CreatedAt.After(latestAt) {
					resolvedChatID = cid
					list = items
					idx = i
					latestAt = item.CreatedAt
				}
			}
			if targetID != "" && idx != -1 {
				break
			}
		}
	}

	if idx == -1 || len(list) == 0 {
		m.mu.Unlock()
		return false, false, false, "", nil
	}

	if !m.IsAdmin(senderOpenID) {
		id := list[idx].ID
		m.mu.Unlock()
		return true, false, false, id, nil
	}

	chosen := list[idx]
	if chosen.timer != nil {
		chosen.timer.Stop()
	}
	m.pending[resolvedChatID] = append(list[:idx], list[idx+1:]...)
	if len(m.pending[resolvedChatID]) == 0 {
		delete(m.pending, resolvedChatID)
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
	if chosen.timer != nil {
		chosen.timer.Stop()
	}
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
