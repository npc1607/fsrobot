package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	lark "github.com/larksuite/oapi-sdk-go/v3"
	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"
)

const (
	imageCacheDir    = "/tmp/fsrobot-images"
	maxUploadImgSize = 10 * 1024 * 1024 // 10MB Feishu limit
)

var (
	mdImageRegexp    = regexp.MustCompile(`!\[([^\]]*)\]\(([^)\s]+)\)`)
	fileImgRegexp    = regexp.MustCompile(`((?:[a-zA-Z0-9._~/@-]+/)+[a-zA-Z0-9._-]+\.(?:png|jpe?g|gif|webp|bmp))`)
	braceImgRegexp   = regexp.MustCompile(`([a-zA-Z0-9._~/@/-]+)\.\{([^}]+)\}`)
)

type imageContent struct {
	ImageKey string `json:"image_key"`
}

type postElement struct {
	Tag      string `json:"tag"`
	Text     string `json:"text,omitempty"`
	Href     string `json:"href,omitempty"`
	ImageKey string `json:"image_key,omitempty"`
	UserName string `json:"user_name,omitempty"`
}

type postBody struct {
	Title   string          `json:"title"`
	Content [][]postElement `json:"content"`
}

func parseImageContent(raw string) (string, error) {
	var ic imageContent
	if err := json.Unmarshal([]byte(raw), &ic); err != nil {
		return "", err
	}
	if strings.TrimSpace(ic.ImageKey) == "" {
		return "", fmt.Errorf("empty image_key")
	}
	return ic.ImageKey, nil
}

// parsePostContent extracts plain text and image_keys from Feishu rich-text ("post") JSON.
func parsePostContent(raw string) (string, []string, error) {
	var direct postBody
	if err := json.Unmarshal([]byte(raw), &direct); err == nil && (direct.Title != "" || len(direct.Content) > 0) {
		return extractPostBody(direct)
	}

	// Sometimes Feishu wraps post content in locale keys like {"zh_cn": {...}}
	var localized map[string]postBody
	if err := json.Unmarshal([]byte(raw), &localized); err != nil {
		return "", nil, err
	}
	for _, body := range localized {
		if body.Title != "" || len(body.Content) > 0 {
			return extractPostBody(body)
		}
	}
	return "", nil, nil
}

func extractPostBody(b postBody) (string, []string, error) {
	var sb strings.Builder
	var imageKeys []string

	if strings.TrimSpace(b.Title) != "" {
		sb.WriteString(strings.TrimSpace(b.Title))
		sb.WriteString("\n")
	}

	for _, line := range b.Content {
		var lineBuf strings.Builder
		for _, el := range line {
			switch el.Tag {
			case "text":
				lineBuf.WriteString(el.Text)
			case "a":
				if el.Text != "" {
					lineBuf.WriteString(el.Text)
				}
				if el.Href != "" {
					lineBuf.WriteString(" (" + el.Href + ") ")
				}
			case "code_block", "code":
				lineBuf.WriteString(el.Text)
			case "img":
				if el.ImageKey != "" {
					imageKeys = append(imageKeys, el.ImageKey)
				}
			case "at":
				// Skip @mention tokens (same as text messages)
			}
		}
		if s := strings.TrimSpace(lineBuf.String()); s != "" {
			sb.WriteString(s)
			sb.WriteString("\n")
		}
	}

	text := strings.TrimSpace(mentionRegexp.ReplaceAllString(sb.String(), ""))
	return text, imageKeys, nil
}

func downloadFeishuImage(ctx context.Context, client *lark.Client, msgID, imageKey string) (string, error) {
	if err := os.MkdirAll(imageCacheDir, 0o755); err != nil {
		return "", fmt.Errorf("create image cache dir: %w", err)
	}

	req := larkim.NewGetMessageResourceReqBuilder().
		MessageId(msgID).
		FileKey(imageKey).
		Type("image").
		Build()

	resp, err := client.Im.MessageResource.Get(ctx, req)
	if err != nil {
		return "", fmt.Errorf("fetch image resource %s: %w", imageKey, err)
	}
	if !resp.Success() {
		return "", fmt.Errorf("feishu image resource error code=%d msg=%s", resp.Code, resp.Msg)
	}

	data, err := io.ReadAll(resp.File)
	if err != nil {
		return "", fmt.Errorf("read image stream: %w", err)
	}
	if len(data) == 0 {
		return "", fmt.Errorf("empty image data for %s", imageKey)
	}

	ext := detectImageExt(data, resp.FileName)
	safeKey := piSessionInvalidRegexpReplace(imageKey)
	destPath := filepath.Join(imageCacheDir, safeKey+ext)
	if err := os.WriteFile(destPath, data, 0o644); err != nil {
		return "", fmt.Errorf("write image file %s: %w", destPath, err)
	}
	return destPath, nil
}

func detectImageExt(data []byte, fileName string) string {
	mime := http.DetectContentType(data)
	switch mime {
	case "image/png":
		return ".png"
	case "image/jpeg":
		return ".jpg"
	case "image/gif":
		return ".gif"
	case "image/webp":
		return ".webp"
	}
	if ext := strings.ToLower(filepath.Ext(fileName)); ext != "" {
		return ext
	}
	return ".png"
}

func piSessionInvalidRegexpReplace(s string) string {
	var b strings.Builder
	for _, r := range s {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' || r == '-' {
			b.WriteRune(r)
		} else {
			b.WriteRune('_')
		}
	}
	if b.Len() == 0 {
		return "img"
	}
	return b.String()
}

// extractLocalImagePaths finds existing local image file paths (absolute or relative to workDir) referenced in markdown text.
func extractLocalImagePaths(text, workDir string) []string {
	seen := make(map[string]struct{})
	var paths []string

	addIfValid := func(raw string) {
		p := strings.TrimPrefix(strings.TrimSpace(raw), "file://")
		if p == "" {
			return
		}
		if !filepath.IsAbs(p) {
			if workDir == "" {
				return
			}
			p = filepath.Join(workDir, p)
		}
		clean := filepath.Clean(p)
		if _, dup := seen[clean]; dup {
			return
		}
		info, err := os.Stat(clean)
		if err != nil || info.IsDir() || info.Size() == 0 || info.Size() > maxUploadImgSize {
			return
		}
		ext := strings.ToLower(filepath.Ext(clean))
		switch ext {
		case ".png", ".jpg", ".jpeg", ".gif", ".webp", ".bmp":
			seen[clean] = struct{}{}
			paths = append(paths, clean)
		}
	}

	for _, m := range mdImageRegexp.FindAllStringSubmatch(text, -1) {
		if len(m) >= 3 {
			addIfValid(m[2])
		}
	}
	for _, m := range fileImgRegexp.FindAllStringSubmatch(text, -1) {
		if len(m) >= 2 {
			addIfValid(m[1])
		}
	}
	for _, m := range braceImgRegexp.FindAllStringSubmatch(text, -1) {
		if len(m) >= 3 {
			base := m[1]
			for _, ext := range strings.Split(m[2], ",") {
				ext = strings.TrimSpace(ext)
				if ext != "" {
					addIfValid(base + "." + ext)
				}
			}
		}
	}
	return paths
}

func uploadFeishuImage(ctx context.Context, client *lark.Client, localPath string) (string, error) {
	f, err := os.Open(localPath)
	if err != nil {
		return "", fmt.Errorf("open image %s: %w", localPath, err)
	}
	defer f.Close()

	req := larkim.NewCreateImageReqBuilder().
		Body(larkim.NewCreateImageReqBodyBuilder().
			ImageType("message").
			Image(f).
			Build()).
		Build()

	resp, err := client.Im.Image.Create(ctx, req)
	if err != nil {
		return "", fmt.Errorf("upload image %s: %w", localPath, err)
	}
	if !resp.Success() {
		return "", fmt.Errorf("feishu upload image error code=%d msg=%s", resp.Code, resp.Msg)
	}
	if resp.Data == nil || resp.Data.ImageKey == nil || *resp.Data.ImageKey == "" {
		return "", fmt.Errorf("feishu upload image returned empty image_key")
	}
	return *resp.Data.ImageKey, nil
}

// prepareCardImages detects local image paths in markdown, uploads them to Feishu,
// cleans broken local ![alt](path) tags, and returns the uploaded image_keys.
func prepareCardImages(ctx context.Context, client *lark.Client, workDir, markdown string) (string, []string) {
	if client == nil {
		return markdown, nil
	}
	localPaths := extractLocalImagePaths(markdown, workDir)
	if len(localPaths) == 0 {
		return markdown, nil
	}

	uploaded := make(map[string]string)
	var keys []string
	for _, p := range localPaths {
		key, err := uploadFeishuImage(ctx, client, p)
		if err != nil {
			log.Printf("[Media] Failed to upload image %s: %v", p, err)
			continue
		}
		uploaded[p] = key
		keys = append(keys, key)
	}

	if len(uploaded) == 0 {
		return markdown, nil
	}

	// Replace ![alt](localPath) with a clean caption so Feishu doesn't render broken links,
	// while the actual image is rendered via dedicated img card elements.
	cleaned := mdImageRegexp.ReplaceAllStringFunc(markdown, func(full string) string {
		m := mdImageRegexp.FindStringSubmatch(full)
		if len(m) < 3 {
			return full
		}
		p := strings.TrimPrefix(strings.TrimSpace(m[2]), "file://")
		if !filepath.IsAbs(p) && workDir != "" {
			p = filepath.Join(workDir, p)
		}
		p = filepath.Clean(p)
		if _, ok := uploaded[p]; ok {
			alt := strings.TrimSpace(m[1])
			if alt != "" {
				return fmt.Sprintf("🖼️ *%s*", alt)
			}
			return ""
		}
		return full
	})

	return strings.TrimSpace(cleaned), keys
}
