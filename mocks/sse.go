package mocks

import (
	"context"

	sse "github.com/hecc-blot/sse/contract"
)

// MockWriter 是 Writer 接口的 mock 实现，记录最后一次 Send 的帧内容。
type MockWriter struct {
	LastID    string
	SentID    string
	SentEvent string
	SentData  string
}

func (m *MockWriter) Send(id, event, data string) error {
	m.SentID = id
	m.SentEvent = event
	m.SentData = data
	return nil
}

func (m *MockWriter) LastEventID() string {
	return m.LastID
}

// MockSse 是 ISse 接口的 mock 实现，可通过 ServeFn 定制行为。
type MockSse struct {
	ServeFn func(ctx context.Context, w sse.Writer) error
}

func (m *MockSse) Serve(ctx context.Context, w sse.Writer) error {
	if m.ServeFn != nil {
		return m.ServeFn(ctx, w)
	}
	return nil
}
