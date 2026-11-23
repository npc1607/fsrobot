package roles

import (
	"fmt"
	"strings"
)

const defaultThinkingPlaceholder = "⏳ 正在思考并处理中..."

// Role defines a chat persona preset with its prompt and UI thinking indicator.
type Role struct {
	Name         string
	Aliases      []string
	Prompt       string
	ThinkingText string
}

// Provider abstracts role preset lookup, matching, and thinking status text generation.
type Provider interface {
	Default() Role
	Presets() []Role
	Resolve(nameOrAlias string) (Role, bool)
	MatchByPrompt(prompt string) (Role, bool)
	ThinkingText(rawPrompt string) string
}

// Catalog is an in-memory Provider backed by registered Role definitions.
type Catalog struct {
	defaultName string
	order       []string
	byName      map[string]Role
	byAlias     map[string]string
}

// NewCatalog creates a Catalog with the given default role name and initial presets.
func NewCatalog(defaultName string, presets []Role) *Catalog {
	c := &Catalog{
		defaultName: defaultName,
		byName:      make(map[string]Role, len(presets)),
		byAlias:     make(map[string]string),
	}
	for _, r := range presets {
		c.Register(r)
	}
	return c
}

// Register adds or updates a role preset and its aliases.
func (c *Catalog) Register(r Role) {
	if r.Name == "" {
		return
	}
	if _, exists := c.byName[r.Name]; !exists {
		c.order = append(c.order, r.Name)
	}
	c.byName[r.Name] = r
	c.byAlias[strings.ToLower(r.Name)] = r.Name
	for _, a := range r.Aliases {
		if key := strings.ToLower(strings.TrimSpace(a)); key != "" {
			c.byAlias[key] = r.Name
		}
	}
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
	for _, name := range c.order {
		r := c.byName[name]
		if r.Prompt == prompt {
			return r, true
		}
	}
	return Role{}, false
}

// ThinkingText returns the thinking placeholder for rawPrompt.
// When rawPrompt is empty, it uses the Default role's ThinkingText if available,
// falling back to the neutral default placeholder only when no ThinkingText is configured.
func (c *Catalog) ThinkingText(rawPrompt string) string {
	trimmed := strings.TrimSpace(rawPrompt)
	if trimmed == "" {
		if def := c.Default(); strings.TrimSpace(def.ThinkingText) != "" {
			return def.ThinkingText
		}
		return defaultThinkingPlaceholder
	}
	if r, ok := c.MatchByPrompt(rawPrompt); ok && strings.TrimSpace(r.ThinkingText) != "" {
		return r.ThinkingText
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
1. 广播开场白气场：自带夜之城早间电台的热血与黑色幽默感，善用“早上好啊，夜之城！”“昨晚的死亡大乐透又开奖了！”等经典播报腔，把群里的日常吐槽、Bug 排查、上线翻车比作海伍德街头的枪战、荒坂塔防火墙或赛博精神病发作。
2. 赛博黑话点缀：自然穿插“义体”“黑超梦”“中间人”“来生酒吧”“大人物”“NCPD”等词，保持画面感但不影响正常技术/办公沟通。
3. 先播报整活，再利索办事：先用斯坦的播音腔调侃播报两句，再干脆利落地给出准确有用的技术方案或执行结果。`,
			ThinkingText: "📻 斯坦正在连线夜之城信号塔，马上开麦播报...",
		},
	})
}
