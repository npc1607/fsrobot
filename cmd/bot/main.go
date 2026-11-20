package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	lark "github.com/larksuite/oapi-sdk-go/v3"
	larkcore "github.com/larksuite/oapi-sdk-go/v3/core"
	"github.com/larksuite/oapi-sdk-go/v3/event/dispatcher"
	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"
	larkws "github.com/larksuite/oapi-sdk-go/v3/ws"

	"fsrobot/internal/agent"
	"fsrobot/internal/config"
	"fsrobot/internal/handler"
	"fsrobot/internal/registry"
)

func main() {
	log.Println("Initializing Feishu Robot with Agent Registry...")

	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	if cfg.AppID == "" || cfg.AppSecret == "" {
		log.Fatal("Missing required Feishu credentials: FEISHU_APP_ID and FEISHU_APP_SECRET must be set (via config.json or environment variables)")
	}

	// 1. Initialize Agent Registry & register configured agents
	reg := registry.NewRegistry(cfg.DefaultAgent)
	defer reg.CloseAll()

	if err := registerAgents(reg, cfg); err != nil {
		log.Fatalf("Failed to register agents: %v", err)
	}

	registered := reg.List()
	if len(registered) == 0 {
		log.Fatal("No agents registered; please check your config.json or environment variables")
	}
	for _, m := range registered {
		log.Printf("Registered Agent: id=%q name=%q streaming=%v (%s)", m.ID, m.Name, m.Streaming, m.Description)
	}
	if def, ok := reg.Default(); ok {
		log.Printf("Default Agent: %s", def.Meta().ID)
	}

	// 2. Initialize Lark OpenAPI client & fetch bot OpenID
	larkClient := lark.NewClient(cfg.AppID, cfg.AppSecret)
	botOpenID := fetchBotOpenID(larkClient)
	if botOpenID != "" {
		log.Printf("Bot OpenID: %s (require_at=%v)", botOpenID, cfg.RequireAt)
	} else {
		log.Printf("Notice: could not fetch Bot OpenID (require_at=%v)", cfg.RequireAt)
	}

	// 3. Initialize message handler
	log.Printf("Bosses configured: %d (empty = deny high-risk actions)", len(cfg.Bosses))
	msgHandler := handler.NewMessageHandler(larkClient, reg, botOpenID, cfg.RequireAt, cfg.Bosses)

	// 4. Setup event dispatcher
	eventDispatcher := dispatcher.NewEventDispatcher("", "").
		OnP2MessageReceiveV1(msgHandler.HandleMessageReceive).
		OnP2CardActionTrigger(msgHandler.HandleCardAction).
		OnP2MessageReadV1(func(ctx context.Context, event *larkim.P2MessageReadV1) error {
			return nil
		}).
		OnP2MessageReactionCreatedV1(func(ctx context.Context, event *larkim.P2MessageReactionCreatedV1) error {
			return nil
		}).
		OnP2MessageReactionDeletedV1(func(ctx context.Context, event *larkim.P2MessageReactionDeletedV1) error {
			return nil
		})

	// 5. Setup WebSocket client (no public IP / domain required)
	wsClient := larkws.NewClient(
		cfg.AppID,
		cfg.AppSecret,
		larkws.WithEventHandler(eventDispatcher),
		larkws.WithLogLevel(larkcore.LogLevelInfo),
		larkws.WithAutoReconnect(true),
	)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log.Println("Starting Feishu Bot (WebSocket mode)...")
	errCh := make(chan error, 1)
	go func() {
		errCh <- wsClient.Start(ctx)
	}()

	select {
	case <-ctx.Done():
		stop()
		log.Println("Received shutdown signal, stopping Feishu Robot...")
	case err := <-errCh:
		if err != nil {
			log.Fatalf("WebSocket client exited with error: %v", err)
		}
	}

	log.Println("Feishu Robot has stopped cleanly.")
}

func registerAgents(reg *registry.Registry, cfg *config.Config) error {
	// If explicit agents are configured, register them
	if len(cfg.Agents) > 0 {
		for _, ac := range cfg.Agents {
			if !ac.IsEnabled() {
				continue
			}
			var (
				p   registry.AgentProvider
				err error
			)
			switch ac.Type {
			case "pi", "":
				p, err = agent.NewPiProvider(ac, cfg)
			case "agp", "gp":
				p, err = agent.NewAGPProvider(ac, cfg)
			default:
				log.Printf("Warning: unknown agent type %q for id=%q, skipping", ac.Type, ac.ID)
				continue
			}
			if err != nil {
				log.Printf("Warning: skip registering agent %q: %v", ac.ID, err)
				continue
			}
			if err := reg.Register(p); err != nil {
				return err
			}
		}
		return nil
	}

	// Shorthand fallback: register default pi (if available) and gp (if GP_ENDPOINT is set)
	if p, err := agent.NewPiProvider(config.AgentConfig{ID: "pi"}, cfg); err == nil {
		_ = reg.Register(p)
	} else {
		log.Printf("Notice: default 'pi' agent not registered (%v)", err)
	}

	if cfg.GPEndpoint != "" {
		if p, err := agent.NewAGPProvider(config.AgentConfig{
			ID:       "gp",
			Endpoint: cfg.GPEndpoint,
		}, cfg); err == nil {
			_ = reg.Register(p)
		} else {
			log.Printf("Notice: 'gp' agent not registered (%v)", err)
		}
	}

	return nil
}

func fetchBotOpenID(client *lark.Client) string {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := client.Get(ctx, "/open-apis/bot/v3/info", nil, larkcore.AccessTokenTypeTenant)
	if err != nil || resp == nil {
		return ""
	}
	var result struct {
		Code int `json:"code"`
		Bot  struct {
			OpenID string `json:"open_id"`
		} `json:"bot"`
	}
	if err := json.Unmarshal(resp.RawBody, &result); err != nil || result.Code != 0 {
		return ""
	}
	return result.Bot.OpenID
}
