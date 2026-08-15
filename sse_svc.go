package sse

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	iCoreApi "github.com/hecc-blot/hecc-blot-core/contract/api"
	iCoreSse "github.com/hecc-blot/hecc-blot-core/contract/sse"
	iCoreTrace "github.com/hecc-blot/hecc-blot-core/contract/trace"

	"github.com/hecc-blot/hecc-blot-core/contract/ioc"
	"github.com/hecc-blot/hecc-blot-core/util"
	"github.com/gin-gonic/gin"
)

const (
	// defaultMaxConnections 默认最大并发 SSE 连接数。
	defaultMaxConnections = 1000
	// heartbeatInterval 心跳间隔，用于检测客户端静默断开。
	heartbeatInterval = 30 * time.Second
)

// SseHandle SSE 服务处理器，与 API 共享 gin.Engine。
type SseHandle struct {
	engine    *gin.Engine
	group     *gin.RouterGroup
	container ioc.IContainer
	semaphore chan struct{} // 连接信号量，限制并发连接数
	traceSvc  iCoreTrace.ITrace
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

// Middleware 注册 SSE 分组中间件，Middleware() 方法自动完成依赖注入。
func (f *SseHandle) Middleware(middlewares ...iCoreApi.IMiddleware) iCoreSse.ISseHandle {
	for _, iMiddleware := range middlewares {
		f.container.Inject(iMiddleware)

		middlewareValue := iMiddleware.Middleware()
		if middlewareValue != nil && reflect.TypeOf(middlewareValue).Kind() == reflect.Func {
			f.group.Use(middlewareValue.(func(*gin.Context)))
		}
	}

	return f
}

// Group 创建 SSE 路由分组，分组内的中间件仅作用于该分组。
func (f *SseHandle) Group(relativePath string, middlewares ...iCoreApi.IMiddleware) iCoreSse.ISseHandle {
	group := &SseHandle{
		engine:    f.engine,
		group:     f.group.Group(relativePath),
		container: f.container,
		semaphore: f.semaphore,
		traceSvc:  f.traceSvc,
		stats:     f.stats,
		conns:     f.conns,
	}
	group.Middleware(middlewares...)
	return group
}

// Stats 返回 SSE 连接统计指标。
func (f *SseHandle) Stats() iCoreSse.Stats {
	return iCoreSse.Stats{
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
func (f *SseHandle) Get(apiPath string, sseInstance iCoreSse.ISse) {
	f.registerSse(apiPath, sseInstance, http.MethodGet)
}

// Post 注册 SSE 路由（POST 方式，适用于 fetch + ReadableStream 场景）。
func (f *SseHandle) Post(apiPath string, sseInstance iCoreSse.ISse) {
	f.registerSse(apiPath, sseInstance, http.MethodPost)
}

// registerSse 注册 SSE 路由。每个连接会创建独立的业务实例，避免并发共享写入。
func (f *SseHandle) registerSse(apiPath string, sseInstance iCoreSse.ISse, method string) {
	// 注入模板实例，仅用于反射获取具体类型
	f.container.Inject(sseInstance)
	sseType := reflect.TypeOf(sseInstance).Elem()

	handler := func(c *gin.Context) {
		// 1.4 http.Flusher 断言：不支持流式写入返回 500
		flusher, ok := c.Writer.(http.Flusher)
		if !ok {
			c.String(http.StatusInternalServerError, "streaming unsupported")
			return
		}

		// 1.2 连接数上限：信号量已满返回 503
		select {
		case f.semaphore <- struct{}{}:
			defer func() { <-f.semaphore }()
		default:
			c.String(http.StatusServiceUnavailable, "too many connections")
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
		sse := newInstance.(iCoreSse.ISse)

		// 5.1 链路追踪：为连接创建 span 并注入上下文
		ctx := c.Request.Context()
		if f.traceSvc != nil {
			var span iCoreTrace.Span
			ctx, span = f.traceSvc.Start(ctx, "sse.connection", "sse.path", apiPath)
			defer span.End()
		}

		// 1.3 心跳：带取消能力的上下文，客户端断开或心跳写入失败时取消
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		// 设置 SSE 响应头
		c.Writer.Header().Set("Content-Type", "text/event-stream")
		c.Writer.Header().Set("Cache-Control", "no-cache")
		c.Writer.Header().Set("Connection", "keep-alive")
		c.Writer.WriteHeader(http.StatusOK)
		flusher.Flush()

		// 1.7 构造写入抽象，暴露 Last-Event-Id 供业务做断线续传
		writer := &sseWriter{
			writer:      c.Writer,
			flusher:     flusher,
			lastEventID: c.GetHeader("Last-Event-Id"),
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

	switch method {
	case http.MethodGet:
		f.group.GET(apiPath, handler)
	case http.MethodPost:
		f.group.POST(apiPath, handler)
	default:
		panic(fmt.Sprintf("无效http请求类型，%s", method))
	}
}

// NewSseSvc 创建 SSE 服务处理器。traceSvc 可为 nil（不启用链路追踪）。
func NewSseSvc(engine *gin.Engine, container ioc.IContainer, traceSvc iCoreTrace.ITrace) iCoreSse.ISseHandle {
	if container == nil {
		panic("ioc: 注入容器不能为空")
	}
	return &SseHandle{
		engine:    engine,
		group:     &engine.RouterGroup,
		container: container,
		traceSvc:  traceSvc,
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
	if err := util.WriteSSE(&buf, id, event, data); err != nil {
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
