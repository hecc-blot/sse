package service

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	iCoreApi "github.com/hecc-blot/framework/contract/api"
	"github.com/hecc-blot/framework/contract/ioc"
	sseContract "github.com/hecc-blot/sse/contract"
	sseutil "github.com/hecc-blot/sse/util"
)

const (
	// defaultMaxConnections 默认最大并发 SSE 连接数。
	defaultMaxConnections = 1000
)

// heartbeatInterval 心跳间隔，用于检测客户端静默断开。
// 用 var 而非 const，便于测试中缩短间隔。
var heartbeatInterval = 30 * time.Second

// SseHandle SSE 服务处理器，复用框架 IApiHandle 注册路由，不感知具体 HTTP 内核。
type SseHandle struct {
	handle    iCoreApi.IApiHandle
	container ioc.IContainer
	semaphore chan struct{} // 连接信号量，限制并发连接数
	stats     *sseStats
	conns     *connTable
}

// sseStats SSE 连接统计，分组与根共享同一实例。
type sseStats struct {
	active atomic.Int64
	total  atomic.Int64
	closed atomic.Int64
}

// connTable 活跃连接表，分组与根共享，用于优雅关闭通知。
type connTable struct {
	mu    sync.Mutex
	conns map[*sseWriter]struct{}
}

// Middleware 注册 SSE 分组中间件，由框架 IApiHandle 完成依赖注入与挂载。
func (f *SseHandle) Middleware(middlewares ...iCoreApi.IMiddleware) sseContract.ISseHandle {
	f.handle.Middleware(middlewares...)
	return f
}

// Group 创建 SSE 路由分组，分组内的中间件仅作用于该分组。
func (f *SseHandle) Group(relativePath string, middlewares ...iCoreApi.IMiddleware) sseContract.ISseHandle {
	group := &SseHandle{
		handle:    f.handle.Group(relativePath),
		container: f.container,
		semaphore: f.semaphore,
		stats:     f.stats,
		conns:     f.conns,
	}
	group.Middleware(middlewares...)
	return group
}

// Stats 返回 SSE 连接统计指标。
func (f *SseHandle) Stats() sseContract.Stats {
	return sseContract.Stats{
		Active: f.stats.active.Load(),
		Total:  f.stats.total.Load(),
		Closed: f.stats.closed.Load(),
	}
}

// Shutdown 通知所有活跃连接优雅关闭：发送 shutdown 帧并取消连接。
func (f *SseHandle) Shutdown() {
	f.conns.mu.Lock()
	conns := make([]*sseWriter, 0, len(f.conns.conns))
	for w := range f.conns.conns {
		conns = append(conns, w)
	}
	f.conns.mu.Unlock()

	for _, w := range conns {
		w.shutdown()
	}
}

// Get 注册 SSE 路由（GET 方式，EventSource 标准用法）。
func (f *SseHandle) Get(apiPath string, sseInstance sseContract.ISse) {
	f.registerSse(apiPath, sseInstance, http.MethodGet)
}

// Post 注册 SSE 路由（POST 方式，适用于 fetch + ReadableStream 场景）。
func (f *SseHandle) Post(apiPath string, sseInstance sseContract.ISse) {
	f.registerSse(apiPath, sseInstance, http.MethodPost)
}

// registerSse 注册 SSE 路由。每个连接会创建独立的业务实例，避免并发共享写入。
func (f *SseHandle) registerSse(apiPath string, sseInstance sseContract.ISse, method string) {
	// 注入模板实例，仅用于反射获取具体类型
	f.container.Inject(sseInstance)
	sseType := reflect.TypeOf(sseInstance).Elem()

	handler := func(w http.ResponseWriter, r *http.Request) {
		// 1.4 http.Flusher 断言：不支持流式写入返回 500
		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "streaming unsupported", http.StatusInternalServerError)
			return
		}

		// 1.2 连接数上限：信号量已满返回 503
		select {
		case f.semaphore <- struct{}{}:
			defer func() { <-f.semaphore }()
		default:
			http.Error(w, "too many connections", http.StatusServiceUnavailable)
			return
		}

		// 2.2 连接统计：连接建立 +1，结束 -1
		f.stats.total.Add(1)
		f.stats.active.Add(1)
		defer func() {
			f.stats.active.Add(-1)
			f.stats.closed.Add(1)
		}()

		// 1.1 实例隔离：每个连接创建独立实例并注入依赖
		newInstance := reflect.New(sseType).Interface()
		f.container.Inject(newInstance)
		sse := newInstance.(sseContract.ISse)

		// 1.3 心跳：带取消能力的上下文，客户端断开或心跳写入失败时取消
		ctx, cancel := context.WithCancel(r.Context())
		defer cancel()

		// 设置 SSE 响应头
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		w.WriteHeader(http.StatusOK)
		flusher.Flush()

		// 1.7 构造写入抽象，暴露 Last-Event-Id 供业务做断线续传
		writer := &sseWriter{
			writer:      w,
			flusher:     flusher,
			lastEventID: r.Header.Get("Last-Event-Id"),
			ctx:         ctx,
			cancel:      cancel,
		}

		// 2.1 注册到活跃连接表，供优雅关闭通知
		f.conns.mu.Lock()
		f.conns.conns[writer] = struct{}{}
		f.conns.mu.Unlock()
		defer func() {
			f.conns.mu.Lock()
			delete(f.conns.conns, writer)
			f.conns.mu.Unlock()
		}()

		// 心跳 goroutine：定期发送 comment，写入失败时取消连接
		go writer.heartbeat()

		// 调用业务逻辑
		if err := sse.Serve(ctx, writer); err != nil {
			// 1.6 错误帧使用 JSON 格式，便于客户端区分错误类型
			writer.writeError(err)
		}
	}

	f.handle.Handle(method, apiPath, http.HandlerFunc(handler))
}

// NewSseSvc 创建 SSE 服务处理器，复用框架 IApiHandle 注册路由。
func NewSseSvc(handle iCoreApi.IApiHandle, container ioc.IContainer) sseContract.ISseHandle {
	if container == nil {
		panic("ioc: 注入容器不能为空")
	}
	return &SseHandle{
		handle:    handle,
		container: container,
		stats:     &sseStats{},
		conns:     &connTable{conns: make(map[*sseWriter]struct{})},
		semaphore: make(chan struct{}, defaultMaxConnections),
	}
}

// sseWriter 框架层 SSE 写入实现，封装 Flusher、并发锁、心跳与错误格式。
type sseWriter struct {
	writer      http.ResponseWriter
	flusher     http.Flusher
	lastEventID string
	ctx         context.Context
	cancel      context.CancelFunc
	mu          sync.Mutex
}

// Send 发送一条 SSE 事件，id/event 可为空。
func (w *sseWriter) Send(id, event, data string) error {
	return w.write(id, event, data)
}

// LastEventID 返回客户端重连时携带的 Last-Event-Id 请求头。
func (w *sseWriter) LastEventID() string {
	return w.lastEventID
}

// write 组装并写入一条 SSE 事件帧，复用 util.WriteSSE 组装帧。
func (w *sseWriter) write(id, event, data string) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	var buf bytes.Buffer
	if err := sseutil.WriteSSE(&buf, id, event, data); err != nil {
		return err
	}
	return w.writeRaw(buf.String())
}

// writeRaw 写入原始字节并刷新，写入失败时取消连接。调用方需持锁。
func (w *sseWriter) writeRaw(s string) error {
	if _, err := w.writer.Write([]byte(s)); err != nil {
		w.cancel()
		return err
	}
	w.flusher.Flush()
	return nil
}

// heartbeat 定期发送 SSE comment 保持连接，写入失败即退出（连接已断）。
func (w *sseWriter) heartbeat() {
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-w.ctx.Done():
			return
		case <-ticker.C:
			w.mu.Lock()
			err := w.writeRaw(": heartbeat\n\n")
			w.mu.Unlock()
			if err != nil {
				return
			}
		}
	}
}

// writeError 以 JSON 格式发送错误事件，客户端可据此区分错误类型。
func (w *sseWriter) writeError(err error) {
	payload, _ := json.Marshal(map[string]string{
		"code":    "500",
		"message": err.Error(),
	})
	_ = w.Send("", "error", string(payload))
}

// shutdown 发送 shutdown 帧并取消连接，通知客户端主动断开。
func (w *sseWriter) shutdown() {
	_ = w.Send("", "shutdown", `{"code":"shutdown","message":"server is shutting down"}`)
	w.cancel()
}
