# hecc-blot-sse

Server-Sent Events 推送：与 API 共享端口，内置心跳 / 连接限流 / 错误帧 / 优雅关闭。

## 安装

```bash
go get github.com/hecc-blot/sse
```

## 接口定义

```go
// sse/contract/sse.go
type ISse interface {
    Serve(ctx context.Context, w Writer) error
}

// Writer SSE 写入抽象，由框架实现并注入给业务
type Writer interface {
    Send(id, event, data string) error
    LastEventID() string
}

// sse/contract/sse_handler.go
type ISseHandle interface {
    Get(apiPath string, sse ISse)
    Post(apiPath string, sse ISse)
    Middleware(middlewares ...iCoreApi.IMiddleware) ISseHandle
    Group(relativePath string, middlewares ...iCoreApi.IMiddleware) ISseHandle
    Stats() Stats
    Shutdown()
}
```

SSE 复用 `framework/contract/api` 中的 `IMiddleware` 接口，无需单独定义中间件接口。

框架在底层封装了 `http.Flusher` 断言、心跳保活、并发锁、连接数上限与错误帧格式化，业务只需通过 `Writer` 写入、通过 `ctx` 感知连接断开。策略性校验（如 `Accept` 头）建议通过中间件实现。

## 初始化

SSE 服务与 API 服务共享 Engine，创建时传入 API 的 Engine：

```go
import (
    httpSvc "github.com/hecc-blot/framework/service/http"
    sse "github.com/hecc-blot/sse/service"
)

// 创建 API 处理器
apiHandle := httpSvc.NewApiSvc(&config.Server, responseSvc, container)

// 创建 SSE 处理器，复用 API 的路由注册
sseHandle := sse.NewSseSvc(apiHandle, container)

// 启动服务（仅 API 调用 Listen，SSE 共享同一端口）
apiHandle.Listen(sseHandle.Shutdown)
```

## 定义 SSE 端点

实现 `ISse` 接口：

```go
type ExampleSse struct {
    LogSvc logContract.ILog `inject:""`
}

func (e ExampleSse) Serve(ctx context.Context, w sseContract.Writer) error {
    e.LogSvc.Info(ctx, "sse start")

    ticker := time.NewTicker(1 * time.Second)
    defer ticker.Stop()

    for {
        select {
        case <-ctx.Done():
            // 客户端断开或心跳写入失败
            return nil
        case <-ticker.C:
            msg := fmt.Sprintf("当前服务器时间：%s", time.Now().Format(time.RFC3339))
            if err := w.Send("", "", msg); err != nil {
                return err
            }
        }
    }
}
```

## 注册路由

```go
func registerSse(sseHandle sseContract.ISseHandle) {
    sseHandle.Middleware(&TokenMiddleware{})
    sseHandle.Get("example/sse", &ExampleSse{})
}
```

### 请求处理流程

```
请求 → [中间件链] → [Flusher 断言] → [连接数限流] → [设置SSE响应头] → [Serve()] → [持续推送事件]
                                                                              ↓
                                                                   客户端断开 / 心跳失败 / Serve 返回 error
                                                                              ↓
                                                               发送 error SSE 事件（JSON）→ 结束
```

### 框架自动处理的能力

- **实例隔离**：每个连接创建独立业务实例，避免并发共享
- **连接数上限**：超出上限返回 503
- **心跳保活**：每 30s 发送 SSE comment，写入失败自动取消连接
- **Flusher 断言**：Writer 不支持流式时返回 500
- **Last-Event-Id**：提取请求头，通过 `w.LastEventID()` 暴露给业务做断线续传
- **连接统计**：`Stats()` 返回活跃/总数/断开连接数
- **优雅关闭**：`Shutdown()` 通知所有活跃连接发送 shutdown 帧后断开，配合 `apiHandle.Listen(sseHandle.Shutdown)` 使用
- **链路追踪**：传入 trace 中间件后为连接创建 `sse.connection` span
- **帧写入工具**：`sse/util` 提供 `WriteSSE(w, id, event, data)` 辅助方法

## 完整示例

```go
func main() {
    // ... 初始化日志、数据库、缓存、IOC 注册 ...

    container := ioc.New()
    // ... container.Set(...) ...

    apiHandle := httpSvc.NewApiSvc(&config.Server, responseSvc, container)

    // 注册 API 路由
    apiHandle.Middleware(&TokenMiddleware{})
    apiHandle.Post("example/api", &ExampleApi{})

    // 注册 SSE 路由（复用 API 路由注册）
    sseHandle := sse.NewSseSvc(apiHandle, container)
    sseHandle.Middleware(&TokenMiddleware{})
    sseHandle.Get("example/sse", &ExampleSse{})

    // 启动服务
    apiHandle.Listen(sseHandle.Shutdown)
}
```

## 错误处理

Serve 返回 error 时，框架以 JSON 格式发送 SSE `error` 事件，不会中断流连接：

```
event: error
data: {"code":"500","message":"..."}
```

## 注意事项

1. **共享端口**：SSE 与 API 共用同一 Gin Engine，只需调用 `apiHandle.Listen()` 一次
2. **长连接**：SSE 连接一直保持直到客户端断开或心跳失败，Serve 方法监听 `ctx.Done()` 感知断开
3. **写入**：业务通过 `w.Send()` 写入，框架自动 Flush 并保证并发安全
4. **中间件复用**：SSE 中间件与 API 中间件共用 `IMiddleware` 接口
5. **依赖注入**：与 API 一样，SSE 实例通过 IOC 自动注入依赖

## 相关模块

| 模块 | 说明 |
|------|------|
| [framework](https://github.com/hecc-blot/framework) | 路由与中间件复用、`IMiddleware` 接口 |
| [trace](https://github.com/hecc-blot/trace) | `SseTraceMiddleware` 连接追踪 |
