package sse

import (
	"context"
	"encoding/json"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"time"

	iCoreApi "github.com/hecc-blot/hecc-blot-core/contract/api"
	iCoreSse "github.com/hecc-blot/hecc-blot-core/contract/sse"

	"github.com/hecc-blot/hecc-blot-core/contract/ioc"
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
	container ioc.IContainer
	semaphore chan struct{} // 连接信号量，限制并发连接数
}

// Middleware 注册 SSE 全局中间件，Middleware() 方法自动完成依赖注入。
func (f *SseHandle) Middleware(middlewares ...iCoreApi.IMiddleware) iCoreSse.ISseHandle {
	for _, iMiddleware := range middlewares {
		f.container.Inject(iMiddleware)

		middlewareValue := iMiddleware.Middleware()
		if middlewareValue != nil && reflect.TypeOf(middlewareValue).Kind() == reflect.Func {
			f.engine.Use(middlewareValue.(func(*gin.Context)))
		}
	}

	return f
}

// Get 注册 SSE 路由。每个连接会创建独立的业务实例，避免并发共享写入。
func (f *SseHandle) Get(apiPath string, sseInstance iCoreSse.ISse) {
	// 注入模板实例，仅用于反射获取具体类型
	f.container.Inject(sseInstance)
	sseType := reflect.TypeOf(sseInstance).Elem()

	f.engine.GET(apiPath, func(c *gin.Context) {
		// 1.5 Accept 头校验：缺失 text/event-stream 返回 406
		if !strings.Contains(c.GetHeader("Accept"), "text/event-stream") {
			c.String(http.StatusNotAcceptable, "Accept: text/event-stream required")
			return
		}

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

		// 1.1 实例隔离：每个连接创建独立实例并注入依赖
		newInstance := reflect.New(sseType).Interface()
		f.container.Inject(newInstance)
		sse := newInstance.(iCoreSse.ISse)

		// 1.3 心跳：带取消能力的上下文，客户端断开或心跳写入失败时取消
		ctx, cancel := context.WithCancel(c.Request.Context())
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

		// 心跳 goroutine：定期发送 comment，写入失败时取消连接
		go writer.heartbeat()

		// 调用业务逻辑
		if err := sse.Serve(ctx, writer); err != nil {
			// 1.6 错误帧使用 JSON 格式，便于客户端区分错误类型
			writer.writeError(err)
		}
	})
}

// NewSseSvc 创建 SSE 服务处理器。
func NewSseSvc(engine *gin.Engine, container ioc.IContainer) iCoreSse.ISseHandle {
	if container == nil {
		panic("ioc: 注入容器不能为空")
	}
	return &SseHandle{
		engine:    engine,
		container: container,
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

// write 组装并写入一条 SSE 事件帧。
func (w *sseWriter) write(id, event, data string) error {
	w.mu.Lock()
	defer w.mu.Unlock()

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
