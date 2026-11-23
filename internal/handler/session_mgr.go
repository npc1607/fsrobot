package handler

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// RunStatus tracks an active task state for a chat/session.
type RunStatus struct {
	RunID       string
	SessionID   string
	InitiatorID string
	IsParallel  bool
	Prompt      string
	Phase       string
	StartedAt   time.Time
	UpdatedAt   time.Time
	Cancel      context.CancelFunc
}

// SessionManager manages per-chat active agent bindings, role personas, main-session locks, and active run states.
type SessionManager struct {
	mu            sync.Mutex
	chatAgents    map[string]string
	chatRoles     map[string]string
	roleStorePath string
	sessionLocks  map[string]*sync.Mutex
	runs          map[string]map[string]*RunStatus // chatKey -> runID -> RunStatus
	listedCache   map[string][]string              // chatKey -> ordered sessionIDs from last /sessions
}

// NewSessionManager creates a new SessionManager.
func NewSessionManager() *SessionManager {
	return &SessionManager{
		chatAgents:   make(map[string]string),
		chatRoles:    make(map[string]string),
		sessionLocks: make(map[string]*sync.Mutex),
		runs:         make(map[string]map[string]*RunStatus),
		listedCache:  make(map[string][]string),
	}
}

// SetListedSessions caches the ordered session IDs from the most recent /sessions list.
func (m *SessionManager) SetListedSessions(chatKey string, ids []string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make([]string, len(ids))
	copy(cp, ids)
	m.listedCache[chatKey] = cp
}

// ResolveListedSession resolves a 1-based index (e.g. "2") or raw sessionID against the cached list.
func (m *SessionManager) ResolveListedSession(chatKey, arg string) string {
	trimmed := strings.TrimSpace(arg)
	if idx, err := strconv.Atoi(trimmed); err == nil && idx >= 1 {
		m.mu.Lock()
		defer m.mu.Unlock()
		list := m.listedCache[chatKey]
		if idx <= len(list) {
			return list[idx-1]
		}
	}
	return trimmed
}

// EnableRolePersistence binds a JSON file path to load/save per-chat role overrides across restarts.
func (m *SessionManager) EnableRolePersistence(path string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.roleStorePath = path
	if path == "" {
		return
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		if !os.IsNotExist(err) {
			log.Printf("[SessionManager] failed to read role store %s: %v", path, err)
		}
		return
	}
	var loaded map[string]string
	if err := json.Unmarshal(raw, &loaded); err != nil {
		log.Printf("[SessionManager] failed to parse role store %s: %v", path, err)
		return
	}
	for k, v := range loaded {
		if k != "" && v != "" {
			m.chatRoles[k] = v
		}
	}
}

// GetRole returns the bound role prompt for a chat, or empty string if using default.
func (m *SessionManager) GetRole(chatID string) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.chatRoles[chatID]
}

// SetRole sets a custom role prompt for a chat and persists it if role persistence is enabled.
func (m *SessionManager) SetRole(chatID, rolePrompt string) {
	m.mu.Lock()
	if rolePrompt == "" {
		delete(m.chatRoles, chatID)
	} else {
		m.chatRoles[chatID] = rolePrompt
	}
	storePath := m.roleStorePath
	var snapshot map[string]string
	if storePath != "" {
		snapshot = make(map[string]string, len(m.chatRoles))
		for k, v := range m.chatRoles {
			snapshot[k] = v
		}
	}
	m.mu.Unlock()

	if storePath != "" {
		if dir := filepath.Dir(storePath); dir != "" && dir != "." {
			_ = os.MkdirAll(dir, 0o755)
		}
		if raw, err := json.MarshalIndent(snapshot, "", "  "); err == nil {
			if err := os.WriteFile(storePath, raw, 0o600); err != nil {
				log.Printf("[SessionManager] failed to persist role store %s: %v", storePath, err)
			}
		}
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
func (m *SessionManager) StartRun(chatKey, runID, effSessionID, initiatorID, prompt string, isParallel bool, cancel context.CancelFunc) {
	m.mu.Lock()
	defer m.mu.Unlock()
	byRun, ok := m.runs[chatKey]
	if !ok {
		byRun = make(map[string]*RunStatus)
		m.runs[chatKey] = byRun
	}
	now := time.Now()
	byRun[runID] = &RunStatus{
		RunID:       runID,
		SessionID:   effSessionID,
		InitiatorID: initiatorID,
		IsParallel:  isParallel,
		Prompt:      prompt,
		Phase:       "思考中...",
		StartedAt:   now,
		UpdatedAt:   now,
		Cancel:      cancel,
	}
}

// UpdatePhase updates the execution phase description for an active run.
func (m *SessionManager) UpdatePhase(chatKey, runID, phase string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if byRun, ok := m.runs[chatKey]; ok {
		if r, ok := byRun[runID]; ok {
			r.Phase = phase
			r.UpdatedAt = time.Now()
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

// CancelAllowed aborts active tasks under chatKey that operatorID is permitted to stop.
// When allowAll is true (e.g., admin), all active tasks under chatKey are canceled;
// otherwise only tasks whose InitiatorID matches operatorID are canceled.
func (m *SessionManager) CancelAllowed(chatKey, operatorID string, allowAll bool) (canceled int, total int) {
	m.mu.Lock()
	byRun := m.runs[chatKey]
	total = len(byRun)
	cancels := make([]context.CancelFunc, 0, total)
	for _, r := range byRun {
		if !allowAll && (operatorID == "" || r.InitiatorID != operatorID) {
			continue
		}
		if r.Cancel != nil {
			cancels = append(cancels, r.Cancel)
		}
	}
	m.mu.Unlock()

	for _, c := range cancels {
		c()
	}
	return len(cancels), total
}

// CancelRun aborts a single specific active task runID under chatKey if operatorID is permitted.
func (m *SessionManager) CancelRun(chatKey, runID, operatorID string, allowAll bool) (canceled bool, exists bool) {
	m.mu.Lock()
	var cancel context.CancelFunc
	if byRun, ok := m.runs[chatKey]; ok {
		if r, ok := byRun[runID]; ok {
			exists = true
			if allowAll || (operatorID != "" && r.InitiatorID == operatorID) {
				cancel = r.Cancel
			}
		}
	}
	m.mu.Unlock()

	if cancel != nil {
		cancel()
		return true, true
	}
	return false, exists
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
