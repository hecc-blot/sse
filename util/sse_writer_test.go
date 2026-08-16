package util

import (
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestWriteSSE 验证 SSE 事件帧的组装格式，id/event/data 可缺省，末尾固定一个空行。
func TestWriteSSE(t *testing.T) {
	tests := []struct {
		name         string
		id, evt, data string
		want         string
	}{
		{"全空", "", "", "", "\n"},
		{"仅 id", "1", "", "", "id: 1\n\n"},
		{"仅 event", "", "ping", "", "event: ping\n\n"},
		{"仅 data", "", "", "hello", "data: hello\n\n"},
		{"完整帧", "1", "msg", "hello", "id: 1\nevent: msg\ndata: hello\n\n"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf strings.Builder
			err := WriteSSE(&buf, tt.id, tt.evt, tt.data)
			assert.NoError(t, err)
			assert.Equal(t, tt.want, buf.String())
		})
	}
}

// errorWriter 写入时返回错误，用于验证错误透传。
type errorWriter struct{}

func (errorWriter) Write([]byte) (int, error) {
	return 0, errors.New("write failed")
}

func TestWriteSSEPropagatesError(t *testing.T) {
	err := WriteSSE(errorWriter{}, "1", "msg", "hello")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "write failed")
}
