package service

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	mocks "github.com/hecc-blot/core/mocks/ioc"
	serverConfig "github.com/hecc-blot/framework/config/http"
	iCoreApi "github.com/hecc-blot/framework/contract/api"
	envEnum "github.com/hecc-blot/framework/enum/env"
	httpSvc "github.com/hecc-blot/framework/service/http"
	sseContract "github.com/hecc-blot/sse/contract"
)

// hookRecorder 只关心 OnShutdown 的假 handle：其余方法由嵌入的接口兜底 ——
// NewSseSvc 的构造流程只会调 OnShutdown，所以嵌入 nil 接口是安全的。
type hookRecorder struct {
	iCoreApi.IApiHandle
	hooks []func()
}

func (h *hookRecorder) OnShutdown(fn func()) { h.hooks = append(h.hooks, fn) }

// TestNewSseSvcRegistersShutdownHook：SSE 在**构造时**把自己托进框架停机流程，且只注册一次。
// 业务因此不必再写 Listen(sseHandle.Shutdown) —— 忘了那步的后果是客户端静默突然断连。
func TestNewSseSvcRegistersShutdownHook(t *testing.T) {
	rec := &hookRecorder{}

	handle := NewSseSvc(rec, &mocks.MockContainer{})

	assert.Len(t, rec.hooks, 1, "构造时应恰好注册一个关闭钩子")

	// 钩子就是 Shutdown：连接表为空时调用应安全无副作用（钩子须幂等）
	rec.hooks[0]()
	rec.hooks[0]()
	assert.Equal(t, int64(0), handle.Stats().Active)
}

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
// 底层引擎复用框架 ApiHandle，供 httptest.NewServer 挂载。
func newTestHandle(maxConns int) (*SseHandle, http.Handler) {
	apiHandle := httpSvc.NewApiSvc(
		&serverConfig.Config{Env: envEnum.TestMode},
		httpSvc.NewResponseSvc(),
		&mocks.MockContainer{},
	)
	handle := &SseHandle{
		handle:    apiHandle,
		container: &mocks.MockContainer{},
		semaphore: make(chan struct{}, maxConns),
		stats:     &sseStats{},
		conns:     &connTable{conns: make(map[*sseWriter]struct{})},
	}
	return handle, apiHandle.(*httpSvc.ApiHandle).Engine()
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

// TestStreamSurvivesWriteTimeout 是「SSE 连接被 WriteTimeout 切断」的回归用例。
//
// 机制：net/http 在读完请求头时**一次性**把连接的写 deadline 设为 now+WriteTimeout，
// 这是绝对时间点，Flush() 不会顺延 —— 于是长连接会准时在 WriteTimeout 那一刻静默断流
// （无 shutdown 帧、无 error 事件）。handler 内清掉该 deadline 后，流必须能跑过它。
//
// 这条以前只能手工 curl -N 验（CI 只跑单测），现在用真实 listener + 真实 WriteTimeout 钉住：
// 不修复时连接会在 writeTimeout 处被服务端关掉，客户端读到 EOF，最后一帧必然早于该时刻。
func TestStreamSurvivesWriteTimeout(t *testing.T) {
	oldInterval := heartbeatInterval
	heartbeatInterval = 200 * time.Millisecond
	defer func() { heartbeatInterval = oldInterval }()

	handle, engine := newTestHandle(10)
	handle.Get("/sse", &blockingSse{})
	defer handle.Shutdown()

	const writeTimeout = 800 * time.Millisecond
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	assert.NoError(t, err)
	srv := &http.Server{Handler: engine, WriteTimeout: writeTimeout}
	go func() { _ = srv.Serve(ln) }()
	defer srv.Close()

	req, err := http.NewRequest(http.MethodGet, "http://"+ln.Addr().String()+"/sse", nil)
	assert.NoError(t, err)
	req.Header.Set("Accept", "text/event-stream")
	resp, err := http.DefaultClient.Do(req)
	assert.NoError(t, err)
	defer resp.Body.Close()

	start := time.Now()
	deadline := start.Add(writeTimeout + 3*heartbeatInterval)
	reader := bufio.NewReader(resp.Body)

	var lastBeat time.Time
	for time.Now().Before(deadline) {
		line, err := reader.ReadString('\n')
		if err != nil {
			break // 服务端提前关闭连接（未修复时即在此处读到 EOF）
		}
		if strings.Contains(line, ": heartbeat") {
			lastBeat = time.Now()
		}
	}

	assert.False(t, lastBeat.IsZero(), "未收到任何心跳帧")
	assert.True(t, lastBeat.After(start.Add(writeTimeout)),
		"最后一帧 %v 早于 WriteTimeout(%v)：流被写 deadline 切断", lastBeat.Sub(start), writeTimeout)
}
