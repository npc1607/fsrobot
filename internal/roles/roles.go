package roles

import (
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"gopkg.in/yaml.v3"

	"fsrobot/internal/registry"
)

const (
	defaultThinkingPlaceholder = "⏳ 正在思考并处理中..."
	greetingPlaceholder        = "{{GREETING}}"
	defaultTimezone            = "Asia/Shanghai"
	hotReloadCheckInterval     = time.Second
)

// Role defines a chat persona preset with its prompt, UI thinking indicator, and optional access policy.
type Role struct {
	Name         string                   `yaml:"name"`
	Aliases      []string                 `yaml:"aliases,omitempty"`
	Prompt       string                   `yaml:"prompt"`
	ThinkingText string                   `yaml:"thinking_text"`
	Policy       registry.ExecutionPolicy `yaml:"policy,omitempty"`
}

// IsComplete reports whether r has all required fields (name, prompt, thinking_text)
// populated so incomplete definitions are never exposed to end users.
func IsComplete(r Role) bool {
	return strings.TrimSpace(r.Name) != "" &&
		strings.TrimSpace(r.Prompt) != "" &&
		strings.TrimSpace(r.ThinkingText) != ""
}

// Provider abstracts role preset lookup, matching, and thinking status text generation.
type Provider interface {
	Default() Role
	Presets() []Role
	Resolve(nameOrAlias string) (Role, bool)
	MatchByPrompt(prompt string) (Role, bool)
	ResolvePromptOrDefault(rawPrompt string) Role
	FormatPrompt(rawPrompt string) string
	ThinkingText(rawPrompt string) string
}

type yamlRoleFile struct {
	Default  string `yaml:"default"`
	Timezone string `yaml:"timezone"`
	Roles    []Role `yaml:"roles"`
}

// Catalog is an in-memory Provider backed by registered Role definitions.
type Catalog struct {
	defaultName string
	location    *time.Location
	order       []string
	byName      map[string]Role
	byAlias     map[string]string
}

// NewCatalog creates a Catalog with the given default role name and initial presets.
// Incomplete roles are skipped so partial definitions are never exposed.
func NewCatalog(defaultName string, presets []Role) *Catalog {
	return newCatalogWithTZ(defaultName, defaultTimezone, presets)
}

func newCatalogWithTZ(defaultName, tzName string, presets []Role) *Catalog {
	c := &Catalog{
		defaultName: strings.TrimSpace(defaultName),
		location:    resolveLocation(tzName),
		byName:      make(map[string]Role, len(presets)),
		byAlias:     make(map[string]string),
	}
	for _, r := range presets {
		c.Register(r)
	}
	return c
}

func resolveLocation(tzName string) *time.Location {
	tz := strings.TrimSpace(tzName)
	if tz == "" {
		tz = defaultTimezone
	}
	if loc, err := time.LoadLocation(tz); err == nil {
		return loc
	}
	if tz == defaultTimezone {
		return time.FixedZone("CST", 8*3600)
	}
	log.Printf("[Roles] invalid timezone %q, falling back to %s", tzName, defaultTimezone)
	if loc, err := time.LoadLocation(defaultTimezone); err == nil {
		return loc
	}
	return time.FixedZone("CST", 8*3600)
}

// Register validates and adds a complete role preset and non-colliding aliases.
func (c *Catalog) Register(r Role) {
	r.Name = strings.TrimSpace(r.Name)
	r.Prompt = strings.TrimSpace(r.Prompt)
	r.ThinkingText = strings.TrimSpace(r.ThinkingText)
	if !IsComplete(r) {
		log.Printf("[Roles] skipping incomplete role definition (name=%q)", r.Name)
		return
	}

	nameKey := strings.ToLower(r.Name)
	if owner, exists := c.byAlias[nameKey]; exists && owner != r.Name {
		log.Printf("[Roles] role name %q conflicts with existing alias owned by %q; skipping", r.Name, owner)
		return
	}

	if _, exists := c.byName[r.Name]; !exists {
		c.order = append(c.order, r.Name)
	}

	var cleanAliases []string
	c.byAlias[nameKey] = r.Name
	for _, a := range r.Aliases {
		trimmed := strings.TrimSpace(a)
		key := strings.ToLower(trimmed)
		if key == "" || key == nameKey {
			continue
		}
		if owner, exists := c.byAlias[key]; exists && owner != r.Name {
			log.Printf("[Roles] alias %q for role %q conflicts with role %q; ignoring alias", trimmed, r.Name, owner)
			continue
		}
		c.byAlias[key] = r.Name
		cleanAliases = append(cleanAliases, trimmed)
	}
	r.Aliases = cleanAliases
	c.byName[r.Name] = r
}

// Default returns the configured default Role (falling back to the first registered role if any).
func (c *Catalog) Default() Role {
	if r, ok := c.byName[c.defaultName]; ok {
		return r
	}
	if len(c.order) > 0 {
		return c.byName[c.order[0]]
	}
	return Role{}
}

// Presets returns all registered roles in registration order.
func (c *Catalog) Presets() []Role {
	out := make([]Role, 0, len(c.order))
	for _, name := range c.order {
		out = append(out, c.byName[name])
	}
	return out
}

// Resolve looks up a Role by canonical name or case-insensitive alias.
func (c *Catalog) Resolve(nameOrAlias string) (Role, bool) {
	key := strings.ToLower(strings.TrimSpace(nameOrAlias))
	if key == "" {
		return Role{}, false
	}
	canonical, ok := c.byAlias[key]
	if !ok {
		return Role{}, false
	}
	r, ok := c.byName[canonical]
	return r, ok
}

// MatchByPrompt finds the registered Role whose prompt matches the given prompt text.
func (c *Catalog) MatchByPrompt(prompt string) (Role, bool) {
	trimmed := strings.TrimSpace(prompt)
	for _, name := range c.order {
		r := c.byName[name]
		if r.Prompt == prompt || r.Prompt == trimmed {
			return r, true
		}
	}
	return Role{}, false
}

// ResolvePromptOrDefault returns the matching Role for rawPrompt, or Default() when rawPrompt is empty.
// For custom ad-hoc prompts not matching any preset, it returns a synthetic Role carrying rawPrompt
// and inheriting the Default role's access policy.
func (c *Catalog) ResolvePromptOrDefault(rawPrompt string) Role {
	if strings.TrimSpace(rawPrompt) == "" {
		return c.Default()
	}
	if r, ok := c.Resolve(rawPrompt); ok {
		return r
	}
	if r, ok := c.MatchByPrompt(rawPrompt); ok {
		return r
	}
	def := c.Default()
	def.Prompt = rawPrompt
	return def
}

// TimeGreeting returns a Chinese time-of-day greeting for now.
func TimeGreeting(now time.Time) string {
	h := now.Hour()
	switch {
	case h >= 5 && h < 12:
		return "早上好"
	case h >= 12 && h < 14:
		return "中午好"
	case h >= 14 && h < 19:
		return "下午好"
	default:
		return "晚上好"
	}
}

func renderGreeting(s string, now time.Time) string {
	if !strings.Contains(s, greetingPlaceholder) {
		return s
	}
	return strings.ReplaceAll(s, greetingPlaceholder, TimeGreeting(now))
}

func (c *Catalog) now() time.Time {
	if c != nil && c.location != nil {
		return time.Now().In(c.location)
	}
	return time.Now().In(resolveLocation(defaultTimezone))
}

// FormatPrompt resolves rawPrompt (falling back to Default().Prompt when empty)
// and renders time-aware placeholders such as {{GREETING}}.
func (c *Catalog) FormatPrompt(rawPrompt string) string {
	p := c.ResolvePromptOrDefault(rawPrompt).Prompt
	return renderGreeting(p, c.now())
}

// ThinkingText returns the thinking placeholder for rawPrompt.
// When rawPrompt is empty, it uses the Default role's ThinkingText if available,
// falling back to the neutral default placeholder only when no ThinkingText is configured.
func (c *Catalog) ThinkingText(rawPrompt string) string {
	now := c.now()
	trimmed := strings.TrimSpace(rawPrompt)
	if trimmed == "" {
		if def := c.Default(); strings.TrimSpace(def.ThinkingText) != "" {
			return renderGreeting(def.ThinkingText, now)
		}
		return defaultThinkingPlaceholder
	}
	if r, ok := c.Resolve(rawPrompt); ok && strings.TrimSpace(r.ThinkingText) != "" {
		return renderGreeting(r.ThinkingText, now)
	}
	if r, ok := c.MatchByPrompt(rawPrompt); ok && strings.TrimSpace(r.ThinkingText) != "" {
		return renderGreeting(r.ThinkingText, now)
	}
	return fmt.Sprintf("🎭 [%s] 正在思考并处理中...", truncateRunes(strings.ReplaceAll(trimmed, "\n", " "), 20))
}

func truncateRunes(s string, maxRunes int) string {
	runes := []rune(s)
	if len(runes) <= maxRunes {
		return s
	}
	return string(runes[:maxRunes]) + "..."
}

// LoadCatalogFromYAML parses and validates a YAML role definition file.
// Incomplete role entries are skipped; returns an error if the file cannot be parsed
// or contains zero complete roles.
func LoadCatalogFromYAML(path string) (*Catalog, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cfg yamlRoleFile
	if err := yaml.Unmarshal(raw, &cfg); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	cat := newCatalogWithTZ(cfg.Default, cfg.Timezone, cfg.Roles)
	if len(cat.order) == 0 {
		return nil, fmt.Errorf("no complete roles found in %s", path)
	}
	return cat, nil
}

// HotReloadCatalog wraps a Catalog backed by a YAML file and hot-reloads on file change.
// If a reload fails or yields zero complete roles, the last known good Catalog stays active.
type HotReloadCatalog struct {
	mu          sync.RWMutex
	path        string
	lastModTime time.Time
	lastSize    int64
	lastCheck   time.Time
	current     *Catalog
}

// NewHotReloadCatalog creates a hot-reloading Provider from path, falling back to
// NewDefaultCatalog if path is empty or initially unreadable.
func NewHotReloadCatalog(path string) *HotReloadCatalog {
	h := &HotReloadCatalog{
		path:    strings.TrimSpace(path),
		current: NewDefaultCatalog(),
	}
	if h.path != "" {
		if info, err := os.Stat(h.path); err == nil {
			if cat, loadErr := LoadCatalogFromYAML(h.path); loadErr == nil {
				h.current = cat
				h.lastModTime = info.ModTime()
				h.lastSize = info.Size()
				log.Printf("[Roles] loaded %d roles from %s", len(cat.order), h.path)
			} else {
				log.Printf("[Roles] failed to load %s (%v); using built-in defaults", h.path, loadErr)
			}
		} else {
			log.Printf("[Roles] %s not found (%v); using built-in defaults", h.path, err)
		}
		h.lastCheck = time.Now()
	}
	return h
}

func (h *HotReloadCatalog) snapshot() *Catalog {
	h.mu.RLock()
	path := h.path
	needCheck := path != "" && time.Since(h.lastCheck) >= hotReloadCheckInterval
	cur := h.current
	h.mu.RUnlock()

	if !needCheck {
		return cur
	}

	h.mu.Lock()
	defer h.mu.Unlock()
	if time.Since(h.lastCheck) < hotReloadCheckInterval {
		return h.current
	}
	h.lastCheck = time.Now()

	info, err := os.Stat(h.path)
	if err != nil {
		return h.current
	}
	if info.ModTime().Equal(h.lastModTime) && info.Size() == h.lastSize {
		return h.current
	}

	cat, loadErr := LoadCatalogFromYAML(h.path)
	if loadErr != nil {
		log.Printf("[Roles] hot-reload %s failed (%v); keeping previous valid catalog", h.path, loadErr)
		h.lastModTime = info.ModTime()
		h.lastSize = info.Size()
		return h.current
	}
	h.current = cat
	h.lastModTime = info.ModTime()
	h.lastSize = info.Size()
	log.Printf("[Roles] hot-reloaded %d roles from %s", len(cat.order), h.path)
	return h.current
}

func (h *HotReloadCatalog) Default() Role                       { return h.snapshot().Default() }
func (h *HotReloadCatalog) Presets() []Role                     { return h.snapshot().Presets() }
func (h *HotReloadCatalog) Resolve(q string) (Role, bool)       { return h.snapshot().Resolve(q) }
func (h *HotReloadCatalog) MatchByPrompt(p string) (Role, bool) { return h.snapshot().MatchByPrompt(p) }
func (h *HotReloadCatalog) ResolvePromptOrDefault(p string) Role {
	return h.snapshot().ResolvePromptOrDefault(p)
}
func (h *HotReloadCatalog) FormatPrompt(p string) string { return h.snapshot().FormatPrompt(p) }
func (h *HotReloadCatalog) ThinkingText(p string) string { return h.snapshot().ThinkingText(p) }

// NewDefaultCatalog returns a Catalog pre-populated with built-in chat personas.
func NewDefaultCatalog() *Catalog {
	return NewCatalog("贱兮兮", []Role{
		{
			Name: "贱兮兮",
			Prompt: `你现在是一个“贱兮兮”的群聊 AI 助手，性格毒舌、爱耍贫嘴、阴阳怪气但非常护短且干活靠谱。

说话风格要求：
1. 先吐槽，再办事：面对用户的提问，先用欠揍、调侃、略带戏谑的语气吐槽两句（比如“哟，大忙人终于想起找我了？”“这 bug 写得挺有艺术感啊”），然后再利索地给出真正有用的答案。
2. 善用反问与热梗：常用“不会吧不会吧”“就这？”“尊嘟假嘟”“格局打开”等贱萌语气，偶尔配合 😏、💅、🤡 等表情增强画面感。
3. 爱凑热闹、会接梗：遇到群友闲聊或发牢骚时，精准补刀或拱火吃瓜，绝不冷场，可以尽情开玩笑调侃互怼。
4. 回复简短有力：别长篇大论说教，每句话都要带点“欠揍的机灵劲儿”。`,
			ThinkingText: "😏 本大爷正在翻白眼顺便帮你干活，急啥...",
		},
		{
			Name:         "阿衰",
			Prompt:       "你现在扮演《阿衰 online》主角“阿衰”，是怕踢中学初二学生，脑洞大、爱自嘲、狂热喜爱臭豆腐，常提大脸妹和金乘五老师，说话搞笑接地气，善用感叹号和拟声词。",
			ThinkingText: "🦨 阿衰正在啃着臭豆腐憋大招，稍等一下下...",
		},
		{
			Name:         "正经",
			Prompt:       "你是一个专业、严谨、简洁的技术与办公助手，直接高效回答问题，不使用调侃或网络热梗。",
			ThinkingText: "⏳ 正在分析请求并检索上下文，请稍候...",
		},
		{
			Name:    "夜之城",
			Aliases: []string{"2077", "斯坦", "stan"},
			Prompt: `你现在扮演《赛博朋克 2077》夜之城电台主持人“斯坦（Stan）”，用标志性的激情广播腔调和赛博朋克黑话与群友互动，同时保持干活靠谱。

说话风格要求：
1. 广播开场白气场：自带夜之城电台的热血与黑色幽默感，根据当前时段使用“{{GREETING}}啊，夜之城！”“昨晚的死亡大乐透又开奖了！”等经典播报腔，把群里的日常吐槽、Bug 排查、上线翻车比作海伍德街头的枪战、荒坂塔防火墙或赛博精神病发作。
2. 赛博黑话点缀：自然穿插“义体”“黑超梦”“中间人”“来生酒吧”“大人物”“NCPD”等词，保持画面感但不影响正常技术/办公沟通。
3. 先播报整活，再利索办事：先用斯坦的播音腔调侃播报两句，再干脆利落地给出准确有用的技术方案或执行结果。`,
			ThinkingText: "📻 斯坦正在连线夜之城信号塔，{{GREETING}}，马上开麦播报...",
		},
		{
			Name:    "海绵宝宝",
			Aliases: []string{"spongebob"},
			Prompt: `你现在扮演《海绵宝宝》主角“海绵宝宝（SpongeBob）”，住在比奇堡菠萝屋、在蟹堡王做厨师的乐观小海绵，性格元气满满、真诚热情、干活极其认真。

说话风格要求：
1. 标志性口头禅与元气感：常用“我准备好了！我准备好了！”“哈哈哈哈哈~”“哦，天哪！”“蟹老板一定会对这个赞不绝口的！”等台词，自带比奇堡阳光开朗的氛围。
2. 比奇堡日常比喻：把写代码、修 Bug、跑测试比作煎美味蟹堡、去水母田抓水母、和派大星一起冒险，或者防着痞老板偷秘方。
3. 热情但干活靠谱：先元气满满地打个招呼或接个梗，然后像做完美美味蟹堡一样把技术问题或任务踏实搞定。`,
			ThinkingText: "🧽 我准备好了！海绵宝宝正在蟹堡王煎锅旁思考中...",
		},
		{
			Name:    "咨询客服",
			Aliases: []string{"客服", "support"},
			Policy: registry.ExecutionPolicy{
				WorkDir:     "/Users/pcong/Project/services",
				AllowedDirs: []string{"/Users/pcong/Project/services/data-service", "/Users/pcong/Project/services/collect-dao"},
				Tools: []string{
					"read", "bash", "restore_observation", "ask_user_question",
				},
				BashMode:   "scoped",
				AllowWrite: boolPtr(false),
				Capabilities: []registry.CapabilityRule{
					{Kind: "cli", Name: "readonly-search", Allow: []string{"rg", "grep", "ls"}},
					{
						Kind:  "skill",
						Name:  "lark-whiteboard",
						Allow: []string{"lark-cli whiteboard", "/Users/pcong/.agents/skills/lark-whiteboard/"},
						Paths: []string{"/Users/pcong/.agents/skills/lark-whiteboard"},
					},
				},
				DeniedPaths: []string{".env*", "*.key", "*.pem", "~/.ssh", "~/.gp/gp.db", "config.toml"},
			},
			Prompt: `你是一位专业、耐心、亲切的技术支持与业务咨询客服，专门负责解答与 data-service 和 collect-dao 两个代码仓库相关的业务流、接口逻辑、状态流转、错误码与数据查询问题。

工作规范：
1. 仅在 data-service 和 collect-dao 目录内进行只读检索与核实，禁止修改任何代码或尝试访问其他项目、密钥、配置。
2. 回答时先用通俗易懂的语言给出明确结论，再附上对应的模块/文件依据和排查建议，语气礼貌专业。`,
			ThinkingText: "🎧 客服同学正在为您核查 data-service 与 collect-dao 相关资料，请稍候...",
		},
	})
}

func boolPtr(v bool) *bool { return &v }
