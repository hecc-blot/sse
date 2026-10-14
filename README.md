# hecc-blot-sse

SSE（Server-Sent Events）推送模块：与 API 共享端口，框架封装连接管理、心跳、限流与优雅关闭。

## 安装

```bash
go get github.com/hecc-blot/sse
```

## 快速开始

```go
import (
	"context"
	"fmt"
	"time"

	logContract "github.com/hecc-blot/core/contract/log"
	ioc "github.com/hecc-blot/core/service/ioc"
	httpSvc "github.com/hecc-blot/framework/service/http"
	sseContract "github.com/hecc-blot/sse/contract"
	sse "github.com/hecc-blot/sse/service"
)

type TimeSse struct {
	LogSvc logContract.ILog `inject:""`
}

func (s *TimeSse) Serve(ctx context.Context, w sseContract.Writer) error {
	s.LogSvc.Info(ctx, "sse connected")
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done(): // 客户端断开或服务关闭
			return nil
		case <-ticker.C:
			if err := w.Send("", "", fmt.Sprintf("now: %s", time.Now().Format(time.RFC3339))); err != nil {
				return err
			}
		}
	}
}

func main() {
	// 1. HTTP 内核（SSE 复用它，不另起端口）
	container := ioc.New()
	container.Set(new(logContract.ILog), logSvc)
	// 只要服务里不注册 API 路由（Get/Post），就不必注入响应渲染 ——
	// SSE 只挂原生 handler，一个只跑 SSE 的进程因此不用构造 responseSvc。
	apiHandle := httpSvc.NewApiSvc(&config.Server, container)

	// 2. SSE 句柄
	sseHandle := sse.NewSseSvc(apiHandle, container)

	// 3. 注册端点
	sseHandle.Get("events/time", &TimeSse{})

	// 4. 启动（阻塞）。SSE 已在 NewSseSvc 里把自己注册进停机流程，无需手工传钩子
	apiHandle.Listen()
}
```

完整的应用组装顺序见[组织主页](https://github.com/hecc-blot)。

## 接口定义

```go
import (
	"context"

	iCoreApi "github.com/hecc-blot/framework/contract/api"
	sseContract "github.com/hecc-blot/sse/contract"
)

// 业务实现 Serve 编写推送逻辑
type ISse interface {
	// ctx 带取消能力：客户端断开或心跳失败时被取消，业务应在循环里 select ctx.Done() 退出
	Serve(ctx context.Context, w Writer) error
}

// 框架注入给业务的写入抽象
type Writer interface {
	Send(id, event, data string) error    // id/event 可为空；队满时阻塞（背压），不丢帧
	TrySend(id, event, data string) error // 队满时丢弃该帧并返回 ErrBackpressure，不阻塞
	LastEventID() string                  // 客户端重连时携带的 Last-Event-Id，用于断线续传
}

// 两个契约型 sentinel（带 core 码，可用 errors.Is 判断）
var (
	ErrBackpressure = errorcode.New(errorcode.CodeRateLimited, "sse: send buffer is full, frame dropped")
	ErrClosed       = errorcode.New(errorcode.CodeIllegalState, "sse: connection is already closed")
)

type ISseHandle interface {
	Get(apiPath string, sse ISse)
	Post(apiPath string, sse ISse)
	Middleware(middlewares ...iCoreApi.IMiddleware) ISseHandle
	Group(relativePath string, middlewares ...iCoreApi.IMiddleware) ISseHandle
	Stats() Stats
	Shutdown()
}

type Stats struct {
	Active  int64 // 当前活跃连接数
	Total   int64 // 历史连接总数
	Closed  int64 // 已关闭连接数
	Dropped int64 // 被丢弃的帧数（TrySend 背压丢弃 + 尽力投递的控制帧未能入队）
}
```

## 使用

### 定义 SSE 端点

```go
type TimeSse struct {
	LogSvc logContract.ILog `inject:""` // 注入字段在前
}

func (s *TimeSse) Serve(ctx context.Context, w sseContract.Writer) error {
	// 断线续传：客户端带上 Last-Event-Id 时可以从上次的位置继续
	last := w.LastEventID()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := w.Send("", "", "data"); err != nil {
				return err
			}
		}
	}
}
```

### 注册路由

```go
// 直接注册
sseHandle.Get("events/time", &TimeSse{})
sseHandle.Post("events/time", &TimeSse{}) // fetch + ReadableStream 用法
sseHandle.Handle(...)                     // ❌ 无此方法；需原生 handler 请用 apiHandle.Handle
```

> 一个 SSE 端点同时注册 `Get` 与 `Post`：`EventSource` 只能发 GET，前端用 `fetch` + `ReadableStream` 时走 POST。

### 分组与中间件

```go
// 分组：中间件只作用于本组
sseGroup := sseHandle.Group("admin", &demo.SseAcceptMiddleware{})
sseGroup.Get("events/order", &OrderSse{})
```

> ⚠️ **`sseHandle.Middleware(...)` 是全局中间件**。它内部直接挂到 HTTP 内核的**根 group** 上，会作用于**全部 API 路由**，而不只是 SSE 路由。只想作用于 SSE 就用 `Group()`。
>
> 示例里常见「同一个 Token 中间件同时挂在 `apiHandle` 和 `sseHandle`」，那是重复注册，没必要。

### 请求处理流程

```
GET /events/time
  ├─ 断言 http.Flusher（失败 → 500）
  ├─ 连接数限流（超过上限 → 503）
  ├─ Stats 计数 +1
  ├─ reflect.New 创建独立实例并 Inject 依赖
  ├─ POST 请求时校验 Accept 头
  ├─ 启动写 goroutine + 心跳 goroutine（每 30s 一条 SSE comment）
  ├─ 注册到连接表（供 Shutdown 通知）
  └─ sse.Serve(ctx, writer)
       └─ 业务 Send/TrySend → 有界队列 → 写 goroutine → socket
```

### 框架自动处理的能力

| 能力 | 说明 |
|------|------|
| Flusher 断言 | 写客户端不支持流式时返回 500 |
| 心跳 | 每 30s 发送一条 SSE comment，防中间层断连 |
| Accept 校验 | POST 请求校验 `Accept: text/event-stream` |
| 连接限流 | 并发连接上限 **1000**（硬编码，不可配置），超出返回 503 |
| 背压与丢帧 | 每连接一条有界写队列：`Send` 队满则阻塞（不丢帧），`TrySend` 队满则丢帧并报 `ErrBackpressure` |
| 单帧写超时 | 每帧写单独设 10s deadline，客户端不读时不会永久卡住写 goroutine |
| 优雅关闭 | `Shutdown()` 向所有活跃连接**尽力**发送 `shutdown` 帧并取消 |
| 错误帧 | `Serve` 返回 error 时发送一条 `event: error` 的 JSON 帧 |

### 错误处理

`Serve` 返回 error 时，框架会先写一条错误帧，**然后结束该连接**：

```json
event: error
data: {"code":"500","message":"..."}
```

> 注意：**连接会被关闭**，不是「继续推送」。错误帧的 `code` 目前恒为字符串 `"500"`。

### 背压与丢弃

每个连接的帧不直接写 socket，而是进入该连接自己的**有界队列**，由每连接一个的写 goroutine 依次写出。于是慢客户端只能把队列填满，卡不住心跳，也卡不住停机钩子。

| 调用 | 队满时 | 语义 |
|------|--------|------|
| `Send` | **阻塞**到有空位 | 背压：不丢帧，代价是拖慢业务循环 |
| `TrySend` | **丢弃该帧**并返回 `ErrBackpressure` | 吞吐优先：由业务决定降级策略 |

```go
for ev := range events {
	// 不能丢的帧：等（背压自然传导到上游）
	if err := w.Send("", "order", ev); err != nil {
		return err
	}
	// 可以丢的帧：丢不起就跳过，并自己计数
	if err := w.TrySend("", "metric", m); errors.Is(err, sseContract.ErrBackpressure) {
		dropped.Add(1)
	}
}
```

- **`Send` 返回 nil 只代表「已入队」，不代表「已送达」** —— 写出是异步的。连接是否还活着请看 `ctx`（客户端断开、心跳失败、写失败都会取消它）；发送侧的失败由 `ErrClosed` / `ErrBackpressure` 表达。
- **帧序恒定**：两种调用共用同一条队列与同一个写 goroutine，顺序即调用顺序。
- **内存有上界**：`队列深度(16) × 单帧字节 × 连接数(上限 1000)`。故业务应避免推送超大帧 —— 1KB 的帧满负载约 16MB，1MB 的帧就是 16GB。
- **丢帧可观测**：`Stats().Dropped` 统计所有被丢弃的帧（`TrySend` 丢的 + 心跳/错误/shutdown 等尽力投递却没挤进队列的）。它持续增长说明有客户端消费跟不上。
- **控制帧是尽力投递**：心跳、错误帧、`shutdown` 帧都走非阻塞入队，队满时会被丢掉。这不是妥协 —— 队满必然意味着对端 socket 缓冲也满了，此时它连数据帧都收不到。

### 优雅关闭

```go
apiHandle.Listen() // 无需传参：NewSseSvc 已在构造时注册好关闭钩子
```

`NewSseSvc` 会调用 `apiHandle.OnShutdown(sseHandle.Shutdown)`，把自己托进框架的停机流程：`Listen` 收到退出信号后**先执行关闭钩子**，向所有 SSE 连接**尽力**发送 `shutdown` 帧并取消（都走非阻塞入队，因此钩子不会被慢客户端挂住 —— 对它的意义见上「背压与丢弃」），再以 5s 超时关闭 HTTP 服务。

这个顺序是承重的：`http.Server.Shutdown` 只等活跃连接变 idle，而 SSE 长连接永远不会自己变 idle —— 没有钩子就会干等到超时，客户端只能突然断连。之所以改成自注册，是因为旧写法 `Listen(sseHandle.Shutdown)` 一旦忘了传就**静默**出现这个问题，且不报任何错。

## 语义与约定

- **每个连接一个实例**：框架对每个连接 `reflect.New` 出独立的 `ISse` 实例并重新注入依赖，因此**不要在注入字段上保存连接态数据**。
- **`NewSseSvc` 的 `container` 为 nil 会 panic**；`handle` 也不能为 nil。
- **`Writer.Send` / `TrySend` 并发安全**：帧经 channel 入队，可从多个 goroutine 调用；但跨 goroutine 的**相对顺序**不确定，需要严格有序就让同一个 goroutine 发。队满行为见上「背压与丢弃」。
- **`Send` 不再返回 I/O 错误**：写出由写 goroutine 异步完成，写失败只会取消 `ctx`、并让后续 `Send` / `TrySend` 返回 `ErrClosed`。判断连接死活请以 `ctx` 为准（框架不承诺 `Send` 返回 nil 就等于送达）。
- **写超时已被 handler 自行清除**：框架按 `write_timeout` 设下的写 deadline 是**绝对**时间点（`net/http` 在读完请求头时一次性设定，`Flush()` 不会顺延），长连接会准时在该时刻静默断流 —— handler 内已用 `http.NewResponseController(w).SetWriteDeadline(time.Time{})` 清掉它，因此**SSE 不受 `write_timeout` 限制**（回归用例 `TestStreamSurvivesWriteTimeout` 钉住这一点）。之后每帧写前会重设一个**单帧写超时（10s）**：它只保证一帧不会永久卡住写 goroutine，不限制连接总时长（客户端不读时该帧写超时、连接被取消）。
- **无释放语义**：`NewSseSvc` 不返回 cleanup，连接随进程退出回收。

## 测试与 mock

```go
import "github.com/hecc-blot/sse/mocks"

mockWriter := &mocks.MockWriter{} // 记录 Send 的内容，便于断言
mockSse := &mocks.MockSse{}       // ServeFn 定制行为
```

## 相关模块

| 模块 | 说明 |
|------|------|
| [core](https://github.com/hecc-blot/core) | `ioc.IContainer` 依赖注入、`log.ILog` 日志契约 |
| [framework](https://github.com/hecc-blot/framework) | HTTP 内核与 `IMiddleware`；SSE 复用同一端口 |
| [trace](https://github.com/hecc-blot/trace) | `NewSseMiddleware` 提供每个连接的 span |
| [metrics](https://github.com/hecc-blot/metrics) | 连接数等指标的来源（`Stats()`） |
