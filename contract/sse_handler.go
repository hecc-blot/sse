package sse

import iCoreApi "github.com/hecc-blot/core/contract/api"

type ISseHandle interface {
	Get(apiPath string, sse ISse)
	Post(apiPath string, sse ISse)
	Middleware(middlewares ...iCoreApi.IMiddleware) ISseHandle
	Group(relativePath string, middlewares ...iCoreApi.IMiddleware) ISseHandle
	Stats() Stats
	// Shutdown 通知所有活跃连接优雅关闭（发送 shutdown 帧并取消连接）。
	Shutdown()
}

// Stats SSE 连接统计指标。
type Stats struct {
	Active int64 // 当前活跃连接数
	Total  int64 // 历史连接总数
	Closed int64 // 已关闭连接数
}
