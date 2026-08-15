package util

import (
	"io"
	"strings"
)

// WriteSSE 组装并写入一条 SSE 事件帧，id/event 可为空。
// 内部使用 strings.Builder 复用缓冲区，避免高频推送时的拼接开销。
func WriteSSE(w io.Writer, id, event, data string) error {
	var buf strings.Builder
	if id != "" {
		buf.WriteString("id: " + id + "\n")
	}
	if event != "" {
		buf.WriteString("event: " + event + "\n")
	}
	if data != "" {
		buf.WriteString("data: " + data + "\n")
	}
	buf.WriteString("\n")

	_, err := io.WriteString(w, buf.String())
	return err
}
