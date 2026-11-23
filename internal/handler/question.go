package handler

import (
	"strings"
	"sync"
	"time"
)

const defaultQuestionTimeout = 90 * time.Second

// PendingQuestion represents an in-flight ask_user_question awaiting user button click or text reply.
type PendingQuestion struct {
	ID          string
	ChatID      string
	InitiatorID string
	Question    string
	Options     []string
	CardID      string
	Reply       func(answer string, skipped bool) error
	CreatedAt   time.Time
	timer       *time.Timer
}

// QuestionManager manages pending interactive questions per chat, supporting both
// button callbacks and direct text replies.
type QuestionManager struct {
	mu      sync.Mutex
	pending map[string][]*PendingQuestion
	timeout time.Duration
}

// NewQuestionManager creates a QuestionManager with the default 90s timeout.
func NewQuestionManager() *QuestionManager {
	return &QuestionManager{
		pending: make(map[string][]*PendingQuestion),
		timeout: defaultQuestionTimeout,
	}
}

// AddWithTimeout registers a pending question and invokes onTimeout if unanswered within m.timeout.
func (m *QuestionManager) AddWithTimeout(chatID, qID, initiatorID, question string, options []string, cardID string, replyFn func(string, bool) error, onTimeout func()) {
	if chatID == "" || qID == "" || replyFn == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	item := &PendingQuestion{
		ID:          qID,
		ChatID:      chatID,
		InitiatorID: initiatorID,
		Question:    question,
		Options:     append([]string(nil), options...),
		CardID:      cardID,
		Reply:       replyFn,
		CreatedAt:   time.Now(),
	}
	if m.timeout > 0 {
		item.timer = time.AfterFunc(m.timeout, func() {
			m.mu.Lock()
			removed := m.removeLocked(chatID, qID)
			m.mu.Unlock()
			if removed != nil {
				_ = removed.Reply("", true)
				if onTimeout != nil {
					onTimeout()
				}
			}
		})
	}
	m.pending[chatID] = append(m.pending[chatID], item)
}

// Remove cancels and removes a pending question (e.g., when a run finishes).
func (m *QuestionManager) Remove(chatID, qID string) {
	m.mu.Lock()
	removed := m.removeLocked(chatID, qID)
	m.mu.Unlock()
	if removed != nil && removed.timer != nil {
		removed.timer.Stop()
	}
}

func (m *QuestionManager) removeLocked(chatID, qID string) *PendingQuestion {
	list := m.pending[chatID]
	for i, item := range list {
		if item.ID == qID {
			list = append(list[:i], list[i+1:]...)
			if len(list) == 0 {
				delete(m.pending, chatID)
			} else {
				m.pending[chatID] = list
			}
			return item
		}
	}
	return nil
}

// ResolveByID resolves a pending question by ID when a user clicks an option or skip button.
func (m *QuestionManager) ResolveByID(chatID, qID, operatorID string, isAdmin bool, answer string, skipped bool) (found bool, authorized bool, item *PendingQuestion, err error) {
	m.mu.Lock()
	list := m.pending[chatID]
	var target *PendingQuestion
	for _, it := range list {
		if it.ID == qID {
			target = it
			break
		}
	}
	if target == nil {
		m.mu.Unlock()
		return false, false, nil, nil
	}
	if !isAdmin && (target.InitiatorID == "" || operatorID != target.InitiatorID) {
		m.mu.Unlock()
		return true, false, target, nil
	}
	m.removeLocked(chatID, qID)
	if target.timer != nil {
		target.timer.Stop()
	}
	m.mu.Unlock()

	err = target.Reply(answer, skipped)
	return true, true, target, err
}

// ResolveByCardText resolves the pending question whose CardID matches cardID
// (i.e. when the user explicitly quotes/replies to that question card), preventing
// unrelated messages in the chat from polluting the pending question.
func (m *QuestionManager) ResolveByCardText(chatID, cardID, operatorID string, isAdmin bool, text string) (matched bool, item *PendingQuestion, err error) {
	trimmed := strings.TrimSpace(text)
	cardID = strings.TrimSpace(cardID)
	if cardID == "" || trimmed == "" || strings.HasPrefix(trimmed, "/") {
		return false, nil, nil
	}

	m.mu.Lock()
	list := m.pending[chatID]
	var target *PendingQuestion
	for _, it := range list {
		if it.CardID != cardID {
			continue
		}
		if isAdmin || (it.InitiatorID != "" && operatorID == it.InitiatorID) {
			target = it
			break
		}
	}
	if target == nil {
		m.mu.Unlock()
		return false, nil, nil
	}
	m.removeLocked(chatID, target.ID)
	if target.timer != nil {
		target.timer.Stop()
	}
	m.mu.Unlock()

	err = target.Reply(trimmed, false)
	return true, target, err
}
