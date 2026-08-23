package sse

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	sseContract "github.com/hecc-blot/sse/contract"
	"github.com/hecc-blot/ioc/mocks"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
)

// blockingSse 阻塞直到连接关闭，用于模拟长连接。
type blockingSse struct{}

func (b blockingSse) Serve(ctx context.Context, w sseContract.Writer) error {
	<-ctx.Done()
	return nil
}

// errorSse 立即返回错误，用于验证错误帧。
type errorSse struct{}

func (e errorSse) Serve(ctx context.Context, w sseContract.Writer) error {
	return errors.New("test error")
}

// newTestHandle 直接构造 SseHandle，便于设置小的连接数上限。
func newTestHandle(maxConns int) (*SseHandle, *gin.Engine) {
	gin.SetMode(gin.TestMode)
	engine := gin.New()
	handle := &SseHandle{
		engine:    engine,
		group:     &engine.RouterGroup,
		container: &mocks.MockContainer{},
		semaphore: make(chan struct{}, maxConns),
		stats:     &sseStats{},
		conns:     &connTable{conns: make(map[*sseWriter]struct{})},
	}
	return handle, engine
}

// waitActive 等待活跃连接数达到期望值。
func waitActive(handle *SseHandle, want int64) {
	deadline := time.Now().Add(2 * time.Second)
	for handle.Stats().Active < want && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
}

// waitInactive 等待活跃连接数降为 0。
func waitInactive(handle *SseHandle) {
	deadline := time.Now().Add(2 * time.Second)
	for handle.Stats().Active > 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
}

// openConnection 发起一个保持连接的 SSE 请求（阻塞读直到连接关闭）。
func openConnection(url string) {
	req, _ := http.NewRequest(http.MethodGet, url, nil)
	req.Header.Set("Accept", "text/event-stream")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
}

func TestSseStats(t *testing.T) {
	handle, engine := newTestHandle(1)
	handle.Get("/sse", &blockingSse{})
	server := httptest.NewServer(engine)
	defer server.Close()

	go openConnection(server.URL + "/sse")

	waitActive(handle, 1)
	assert.Equal(t, int64(1), handle.Stats().Active)
	assert.Equal(t, int64(1), handle.Stats().Total)

	handle.Shutdown()
	waitInactive(handle)
	assert.Equal(t, int64(0), handle.Stats().Active)
	assert.Equal(t, int64(1), handle.Stats().Closed)
}

func TestSseConnectionLimit(t *testing.T) {
	handle, engine := newTestHandle(1)
	handle.Get("/sse", &blockingSse{})
	server := httptest.NewServer(engine)
	defer server.Close()

	go openConnection(server.URL + "/sse")

	waitActive(handle, 1)

	req, _ := http.NewRequest(http.MethodGet, server.URL+"/sse", nil)
	req.Header.Set("Accept", "text/event-stream")
	resp, err := http.DefaultClient.Do(req)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
	resp.Body.Close()

	handle.Shutdown()
}

func TestSseShutdown(t *testing.T) {
	handle, engine := newTestHandle(1)
	handle.Get("/sse", &blockingSse{})
	server := httptest.NewServer(engine)
	defer server.Close()

	go openConnection(server.URL + "/sse")

	waitActive(handle, 1)

	handle.Shutdown()
	waitInactive(handle)
	assert.Equal(t, int64(0), handle.Stats().Active)
}

func TestSseErrorFrame(t *testing.T) {
	handle, engine := newTestHandle(1)
	handle.Get("/sse", &errorSse{})
	server := httptest.NewServer(engine)
	defer server.Close()

	req, _ := http.NewRequest(http.MethodGet, server.URL+"/sse", nil)
	req.Header.Set("Accept", "text/event-stream")
	resp, err := http.DefaultClient.Do(req)
	assert.NoError(t, err)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	assert.Contains(t, string(body), "event: error")
	assert.Contains(t, string(body), "test error")
}

func TestSseHeartbeat(t *testing.T) {
	// 缩短心跳间隔，避免测试等待 30s
	old := heartbeatInterval
	heartbeatInterval = 30 * time.Millisecond
	defer func() { heartbeatInterval = old }()

	handle, engine := newTestHandle(1)
	handle.Get("/sse", &blockingSse{})
	server := httptest.NewServer(engine)
	defer server.Close()

	req, _ := http.NewRequest(http.MethodGet, server.URL+"/sse", nil)
	req.Header.Set("Accept", "text/event-stream")
	resp, err := http.DefaultClient.Do(req)
	assert.NoError(t, err)
	defer resp.Body.Close()

	lineCh := make(chan string, 1)
	go func() {
		reader := bufio.NewReader(resp.Body)
		line, _ := reader.ReadString('\n')
		lineCh <- line
	}()

	select {
	case line := <-lineCh:
		assert.Contains(t, line, ": heartbeat")
	case <-time.After(2 * time.Second):
		t.Fatal("未在超时内收到心跳帧")
	}

	handle.Shutdown()
	waitInactive(handle)
}
