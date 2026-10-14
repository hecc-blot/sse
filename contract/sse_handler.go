package sse

import iCoreApi "github.com/hecc-blot/framework/contract/api"

type ISseHandle interface {
	Get(apiPath string, sse ISse)
	Post(apiPath string, sse ISse)
	Middleware(middlewares ...iCoreApi.IMiddleware) ISseHandle
	Group(relativePath string, middlewares ...iCoreApi.IMiddleware) ISseHandle
	Stats() Stats
	// Shutdown 通知所有活跃连接优雅关闭（发送 shutdown 帧并取消连接）。
	Shutdown()
}

// Stats SSE 连接统计指标。分组共享同一份计数，因此是全局聚合值、无法按分组下钻。
type Stats struct {
	Active int64 // 当前活跃连接数
	Total  int64 // 历史连接总数
	Closed int64 // 已关闭连接数
	// Dropped 被丢弃的帧数：TrySend 因队列满丢掉的帧 + 心跳/错误/shutdown 等
	// 尽力投递的帧未能入队。持续增长说明有客户端消费跟不上。
	Dropped int64
}
