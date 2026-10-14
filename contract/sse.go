package sse

import (
	"context"

	"github.com/hecc-blot/core/enum/errorcode"
)

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
	//
	// 帧进入该连接的有界队列后由框架异步写出，因此返回 nil 表示"已入队"、不代表"已送达"。
	// 队列满时**阻塞**（背压），不丢帧；连接已关时返回 ErrClosed。
	Send(id, event, data string) error
	// TrySend 非阻塞地发送一条 SSE 事件：队列满时**丢弃该帧**并返回 ErrBackpressure，
	// 由业务自行决定策略（降速、跳过本次、或断开该连接）。
	//
	// 与 Send 的关系同 core 的 util.Go 与 util.TryGo：要可靠就用 Send（等），
	// 要吞吐就用 TrySend（等不起就丢）。
	TrySend(id, event, data string) error
	// LastEventID 返回客户端重连时携带的 Last-Event-Id 请求头，用于断线续传。
	LastEventID() string
}

var (
	// ErrBackpressure 由 TrySend 返回：该连接待写队列已满，本次帧被丢弃。
	// 语义与 core 的 util.ErrLimitFull 同构（缓冲满，由调用方决定丢弃/降速），故用同一个码。
	ErrBackpressure = errorcode.New(errorcode.CodeRateLimited, "sse: send buffer is full, frame dropped")
	// ErrClosed 由 Send / TrySend 返回：连接已关闭（业务返回、客户端断开或服务停机），帧未入队。
	ErrClosed = errorcode.New(errorcode.CodeIllegalState, "sse: connection is already closed")
)
