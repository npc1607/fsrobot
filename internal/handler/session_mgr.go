package handler

import (
	"context"
	"sort"
	"sync"
	"time"
)

// RunStatus tracks an active task state for a chat/session.
type RunStatus struct {
	RunID      string
	SessionID  string
	IsParallel bool
	Prompt     string
	Phase      string
	StartedAt  time.Time
	Cancel     context.CancelFunc
}

// SessionManager manages per-chat active agent bindings, main-session locks, and active run states.
type SessionManager struct {
	mu           sync.Mutex
	chatAgents   map[string]string
	sessionLocks map[string]*sync.Mutex
	runs         map[string]map[string]*RunStatus // chatKey -> runID -> RunStatus
}

// NewSessionManager creates a new SessionManager.
func NewSessionManager() *SessionManager {
	return &SessionManager{
		chatAgents:   make(map[string]string),
		sessionLocks: make(map[string]*sync.Mutex),
		runs:         make(map[string]map[string]*RunStatus),
	}
}

// GetAgentID returns the bound agent ID for a chat, or empty string if using default.
func (m *SessionManager) GetAgentID(chatID string) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.chatAgents[chatID]
}

// SetAgentID binds a specific agent ID to a chat.
func (m *SessionManager) SetAgentID(chatID, agentID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.chatAgents[chatID] = agentID
}

// SessionLock returns the mutex protecting the primary persistent session for chatKey.
func (m *SessionManager) SessionLock(chatKey string) *sync.Mutex {
	m.mu.Lock()
	defer m.mu.Unlock()
	lk, ok := m.sessionLocks[chatKey]
	if !ok {
		lk = &sync.Mutex{}
		m.sessionLocks[chatKey] = lk
	}
	return lk
}

// StartRun records an active task run under chatKey.
func (m *SessionManager) StartRun(chatKey, runID, effSessionID, prompt string, isParallel bool, cancel context.CancelFunc) {
	m.mu.Lock()
	defer m.mu.Unlock()
	byRun, ok := m.runs[chatKey]
	if !ok {
		byRun = make(map[string]*RunStatus)
		m.runs[chatKey] = byRun
	}
	byRun[runID] = &RunStatus{
		RunID:      runID,
		SessionID:  effSessionID,
		IsParallel: isParallel,
		Prompt:     prompt,
		Phase:      "思考中...",
		StartedAt:  time.Now(),
		Cancel:     cancel,
	}
}

// UpdatePhase updates the execution phase description for an active run.
func (m *SessionManager) UpdatePhase(chatKey, runID, phase string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if byRun, ok := m.runs[chatKey]; ok {
		if r, ok := byRun[runID]; ok {
			r.Phase = phase
		}
	}
}

// EndRun removes the active task record for runID.
func (m *SessionManager) EndRun(chatKey, runID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if byRun, ok := m.runs[chatKey]; ok {
		delete(byRun, runID)
		if len(byRun) == 0 {
			delete(m.runs, chatKey)
		}
	}
}

// CancelAll aborts all currently running tasks for chatKey and returns the count canceled.
func (m *SessionManager) CancelAll(chatKey string) int {
	m.mu.Lock()
	byRun := m.runs[chatKey]
	cancels := make([]context.CancelFunc, 0, len(byRun))
	for _, r := range byRun {
		if r.Cancel != nil {
			cancels = append(cancels, r.Cancel)
		}
	}
	m.mu.Unlock()

	for _, c := range cancels {
		c()
	}
	return len(cancels)
}

// ListActive returns a sorted snapshot of all active runs for chatKey.
func (m *SessionManager) ListActive(chatKey string) []RunStatus {
	m.mu.Lock()
	defer m.mu.Unlock()
	byRun := m.runs[chatKey]
	if len(byRun) == 0 {
		return nil
	}
	out := make([]RunStatus, 0, len(byRun))
	for _, r := range byRun {
		out = append(out, *r)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].StartedAt.Before(out[j].StartedAt)
	})
	return out
}
