package sse

import "context"

// ISse SSE 服务接口，业务实现 Serve 编写推送逻辑。
//
// ctx 为带取消能力的上下文：客户端断开或心跳失败时会被取消，业务应在循环里 select ctx.Done() 退出。
// w 为框架提供的写入抽象，封装了 Flusher、心跳与并发安全，业务无需直接操作 gin.Context。
type ISse interface {
	Serve(ctx context.Context, w Writer) error
}

// Writer SSE 写入抽象，由框架实现并注入给业务。
type Writer interface {
	// Send 发送一条 SSE 事件，id/event 可为空，data 为消息体（业务自行格式化）。
	Send(id, event, data string) error
	// LastEventID 返回客户端重连时携带的 Last-Event-Id 请求头，用于断线续传。
	LastEventID() string
}
