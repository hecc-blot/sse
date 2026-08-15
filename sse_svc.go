package sse

import (
	"reflect"

	iCoreApi "github.com/hecc-blot/hecc-blot-core/contract/api"
	iCoreSse "github.com/hecc-blot/hecc-blot-core/contract/sse"

	"github.com/hecc-blot/hecc-blot-core/contract/ioc"
	"github.com/gin-gonic/gin"
)

type SseHandle struct {
	engine    *gin.Engine
	container ioc.IContainer
}

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

func (f *SseHandle) Get(apiPath string, sseInstance iCoreSse.ISse) {
	f.container.Inject(sseInstance)

	f.engine.GET(apiPath, func(c *gin.Context) {
		c.Writer.Header().Set("Content-Type", "text/event-stream")
		c.Writer.Header().Set("Cache-Control", "no-cache")
		c.Writer.Header().Set("Connection", "keep-alive")
		c.Writer.Flush()

		if err := sseInstance.Serve(c); err != nil {
			c.SSEvent("error", err.Error())
		}
	})
}

func NewSseSvc(engine *gin.Engine, container ioc.IContainer) iCoreSse.ISseHandle {
	if container == nil {
		panic("ioc: 注入容器不能为空")
	}
	return &SseHandle{
		engine:    engine,
		container: container,
	}
}
