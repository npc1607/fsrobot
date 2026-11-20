package handler

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestParseImageContent(t *testing.T) {
	key, err := parseImageContent(`{"image_key":"img_v3_02a1"}`)
	if err != nil || key != "img_v3_02a1" {
		t.Fatalf("unexpected result: key=%q err=%v", key, err)
	}
}

func TestParsePostContent(t *testing.T) {
	raw := `{"title":"报错截图","content":[[{"tag":"at","user_id":"ou_123"},{"tag":"text","text":" 帮我看下这个"},{"tag":"img","image_key":"img_abc"}]]}`
	text, imgs, err := parsePostContent(raw)
	if err != nil {
		t.Fatalf("parsePostContent failed: %v", err)
	}
	if text != "报错截图\n帮我看下这个" {
		t.Fatalf("unexpected text: %q", text)
	}
	if len(imgs) != 1 || imgs[0] != "img_abc" {
		t.Fatalf("unexpected imgs: %+v", imgs)
	}
}

func TestExtractLocalImagePathsAndBuildCard(t *testing.T) {
	dir := t.TempDir()
	img1 := filepath.Join(dir, "chart.png")
	img2 := filepath.Join(dir, "plot.jpg")
	subDir := filepath.Join(dir, "docs", "diagrams")
	_ = os.MkdirAll(subDir, 0o755)
	relImg := filepath.Join(subDir, "arch.png")
	txtFile := filepath.Join(dir, "note.txt")

	_ = os.WriteFile(img1, []byte("fake-png-bytes"), 0o644)
	_ = os.WriteFile(img2, []byte("fake-jpg-bytes"), 0o644)
	_ = os.WriteFile(relImg, []byte("fake-arch-png"), 0o644)
	_ = os.WriteFile(txtFile, []byte("hello"), 0o644)

	md := "图表如下：\n![架构图](file://" + img1 + ")\n另外保存了 " + img2 + "\n相对路径：docs/diagrams/arch.{png,svg,excalidraw}\n忽略不存在的 /tmp/does_not_exist_12345.png 和非图片 " + txtFile
	paths := extractLocalImagePaths(md, dir)
	if len(paths) != 3 || paths[0] != img1 || paths[1] != img2 || paths[2] != relImg {
		t.Fatalf("unexpected extracted paths: %+v", paths)
	}

	cardStr, err := buildCardJSON("标题", "blue", "正文", []string{"img_v3_123"})
	if err != nil {
		t.Fatalf("buildCardJSON failed: %v", err)
	}
	if !strings.Contains(cardStr, `"tag":"img"`) || !strings.Contains(cardStr, `"img_key":"img_v3_123"`) {
		t.Fatalf("expected img element in card JSON: %s", cardStr)
	}
}
