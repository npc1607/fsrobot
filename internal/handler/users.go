package handler

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	lark "github.com/larksuite/oapi-sdk-go/v3"
	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"
)

// UserResolver resolves and caches Feishu user names from group member lists.
type UserResolver struct {
	client *lark.Client
	mu     sync.Mutex
	names  map[string]string    // open_id -> name
	chats  map[string]time.Time // chat_id -> last fetched
}

// NewUserResolver creates a new UserResolver.
func NewUserResolver(client *lark.Client) *UserResolver {
	return &UserResolver{
		client: client,
		names:  make(map[string]string),
		chats:  make(map[string]time.Time),
	}
}

// Remember stores a known open_id -> name mapping (e.g. from MentionEvent).
func (u *UserResolver) Remember(openID, name string) {
	if openID == "" || name == "" {
		return
	}
	u.mu.Lock()
	u.names[openID] = name
	u.mu.Unlock()
}

// FormatSender returns a human-readable sender label, refreshing group members if needed.
func (u *UserResolver) FormatSender(ctx context.Context, chatID, openID string) string {
	if openID == "" {
		return "未知用户"
	}

	u.mu.Lock()
	name := u.names[openID]
	lastFetch := u.chats[chatID]
	u.mu.Unlock()

	if name == "" && chatID != "" && u.client != nil && time.Since(lastFetch) > 10*time.Minute {
		u.refreshChatMembers(ctx, chatID)
		u.mu.Lock()
		name = u.names[openID]
		u.mu.Unlock()
	}

	if name != "" {
		return fmt.Sprintf("%s (open_id: %s)", name, openID)
	}
	return fmt.Sprintf("open_id: %s", openID)
}

func (u *UserResolver) refreshChatMembers(ctx context.Context, chatID string) {
	u.mu.Lock()
	u.chats[chatID] = time.Now()
	u.mu.Unlock()

	fetchCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	req := larkim.NewGetChatMembersReqBuilder().
		ChatId(chatID).
		MemberIdType("open_id").
		PageSize(100).
		Build()

	resp, err := u.client.Im.ChatMembers.Get(fetchCtx, req)
	if err != nil {
		log.Printf("[Users] Failed to fetch chat members for %s: %v", chatID, err)
		return
	}
	if !resp.Success() {
		log.Printf("[Users] Feishu GetChatMembers error code=%d msg=%s", resp.Code, resp.Msg)
		return
	}
	if resp.Data == nil {
		return
	}

	u.mu.Lock()
	for _, m := range resp.Data.Items {
		if m != nil && m.MemberId != nil && m.Name != nil && *m.MemberId != "" && *m.Name != "" {
			u.names[*m.MemberId] = *m.Name
		}
	}
	u.mu.Unlock()
}

// resolveMentions replaces Feishu @_user_N placeholders with real @Name (open_id: ...)
// while stripping @mention of the bot itself.
func resolveMentions(text string, mentions []*larkim.MentionEvent, botOpenID string, resolver *UserResolver) string {
	out := text
	for _, m := range mentions {
		if m == nil || m.Key == nil || *m.Key == "" {
			continue
		}
		key := *m.Key
		openID := ""
		if m.Id != nil && m.Id.OpenId != nil {
			openID = *m.Id.OpenId
		}
		name := ""
		if m.Name != nil {
			name = strings.TrimSpace(*m.Name)
		}

		if openID != "" && name != "" && resolver != nil {
			resolver.Remember(openID, name)
		}

		// Strip @bot itself so commands and prompts stay clean
		if botOpenID != "" && openID == botOpenID {
			out = strings.ReplaceAll(out, key, "")
			continue
		}

		var replacement string
		switch {
		case name != "" && openID != "":
			replacement = fmt.Sprintf("@%s (open_id: %s)", name, openID)
		case name != "":
			replacement = "@" + name
		case openID != "":
			replacement = fmt.Sprintf("@(open_id: %s)", openID)
		}
		out = strings.ReplaceAll(out, key, replacement)
	}

	// Clean up any leftover unmapped @_user_N placeholders
	out = mentionRegexp.ReplaceAllString(out, "")
	return strings.TrimSpace(out)
}
