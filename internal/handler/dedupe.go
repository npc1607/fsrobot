package handler

import (
	"sync"
	"time"
)

// Deduplicator keeps track of processed message IDs to prevent duplicate responses.
type Deduplicator struct {
	mu    sync.Mutex
	items map[string]time.Time
	ttl   time.Duration
}

// NewDeduplicator creates a new Deduplicator with the given TTL.
func NewDeduplicator(ttl time.Duration) *Deduplicator {
	d := &Deduplicator{
		items: make(map[string]time.Time),
		ttl:   ttl,
	}
	go d.cleanupLoop()
	return d
}

// CheckAndSet returns true if the messageId has already been seen (duplicate).
// If not seen, it records the messageId and returns false.
func (d *Deduplicator) CheckAndSet(messageID string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()

	now := time.Now()
	if expireAt, exists := d.items[messageID]; exists {
		if now.Before(expireAt) {
			return true // duplicate
		}
	}

	d.items[messageID] = now.Add(d.ttl)
	return false
}

func (d *Deduplicator) cleanupLoop() {
	ticker := time.NewTicker(2 * time.Minute)
	for range ticker.C {
		d.mu.Lock()
		now := time.Now()
		for k, expireAt := range d.items {
			if now.After(expireAt) {
				delete(d.items, k)
			}
		}
		d.mu.Unlock()
	}
}
