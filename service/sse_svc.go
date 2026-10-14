package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hecc-blot/core/contract/ioc"
	coreUtil "github.com/hecc-blot/core/util"
	iCoreApi "github.com/hecc-blot/framework/contract/api"
	sseContract "github.com/hecc-blot/sse/contract"
	sseutil "github.com/hecc-blot/sse/util"
)

const (
	// defaultMaxConnections 默认最大并发 SSE 连接数。
	defaultMaxConnections = 1000

	// defaultQueueDepth 单连接待写队列的默认容量（帧）。
	//
	// 队列把"写 socket"从业务 goroutine 挪到每连接一个的写 goroutine，因此慢客户端
	// 只能把队列填满、不能卡住心跳与 Shutdown。深度决定能吸收多少突发帧：
	// 太小会让正常突发也触发背压，太大则慢连接占用更多内存 ——
	// 内存上限约 = queueDepth × 单帧字节 × maxConnections，故默认取得偏小，
	// 并要求业务避免推送超大帧（见 README「背压与丢弃」）。
	defaultQueueDepth = 16

	// drainBudget 排空的时限。队列关闭后写 goroutine 会先把缓冲帧写完再退出，
	// 这个预算保证"慢客户端"不会把 handler 与连接槽位一起拖住。
	drainBudget = time.Second

	// closeGrace 关闭时等待写 goroutine 退出的时限。正常客户端在这一步是微秒级；
	// 只有写正卡在 socket 上时才会走到等待，故取得很短。
	closeGrace = 250 * time.Millisecond
)

// heartbeatInterval 心跳间隔，用于检测客户端静默断开。
// 用 var 而非 const，便于测试中缩短间隔；连接构造时快照进 sseWriter，之后不再全局读。
var heartbeatInterval = 30 * time.Second

// defaultFrameWriteTimeout 单帧写出的默认超时。
//
// 连接级写 deadline 已在 handler 里清掉（否则长连接会被框架的 write_timeout 掐断），
// 代价是客户端不读时 Write 会**永久**阻塞：写 goroutine 卡死 → close() 等不到它 →
// handler 无法返回（连接槽位与信号量泄漏）。所以这里给每次 Write 单独设一个 deadline ——
// 它不限制连接总时长，只限制"单帧写入在 N 秒内完成"，超时即判定客户端已死并取消连接。
var defaultFrameWriteTimeout = 10 * time.Second

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
	active  atomic.Int64
	total   atomic.Int64
	closed  atomic.Int64
	dropped atomic.Int64 // 被丢弃的帧数（TrySend 背压丢弃 + 尽力投递的控制帧未能入队）
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
		Active:  f.stats.active.Load(),
		Total:   f.stats.total.Load(),
		Closed:  f.stats.closed.Load(),
		Dropped: f.stats.dropped.Load(),
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

		// 清掉框架按 WriteTimeout 设下的写 deadline。
		//
		// net/http 在读完请求头时**一次性**把连接的写 deadline 设为 now+WriteTimeout，
		// 这是绝对时间点，Flush() 不会顺延 —— 于是长连接会准时在 WriteTimeout 那一刻
		// 静默断流（无 shutdown 帧、无 error 事件，业务也不知道自己写的帧没送达）。
		// SSE 靠心跳维持连接，语义上就不该有"连接级"写超时；设成零值即不设 deadline。
		//
		// 后续每一帧由 writeFrame 单独设一个"单帧写超时"（见 defaultFrameWriteTimeout），
		// 只约束单帧别永久卡住，不限制连接总时长。
		//
		// 用 ResponseController 而非类型断言：到这里 w 已经是 gin 的 responseWriter，
		// 经 Unwrap() 链能拿到底层 http.ResponseWriter；不支持时返回 error，忽略即可
		// （此时退回框架配置的行为，不会比现在更差）。
		_ = http.NewResponseController(w).SetWriteDeadline(time.Time{})

		// 设置 SSE 响应头
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		w.WriteHeader(http.StatusOK)
		flusher.Flush()

		// 1.7 构造写入抽象，暴露 Last-Event-Id 供业务做断线续传。
		// 该连接自己的写 goroutine 与心跳 goroutine 在构造时启动（见 sseWriter 注释）。
		writer := newSseWriter(w, ctx, cancel, r.Header.Get("Last-Event-Id"), f.stats, writerOpts{})

		// 2.1 注册到活跃连接表，供优雅关闭通知
		f.conns.mu.Lock()
		f.conns.conns[writer] = struct{}{}
		f.conns.mu.Unlock()
		defer func() {
			f.conns.mu.Lock()
			delete(f.conns.conns, writer)
			f.conns.mu.Unlock()
		}()

		// 2.2 close 必须在 handler 返回前完成：handler 返回之后 gin 会把 ResponseWriter
		// 放回 sync.Pool 复用，此时若写 goroutine 还在写，写进去的就不是"失败的写入"，
		// 而是别人的连接（静默串包，只有 -race 或线上才看得见）。
		//
		// defer 是 LIFO：本行比上面的连接表摘除注册得晚 ⇒ 退出时先摘连接表、再 close，
		// 于是停机钩子不会碰到正在收尾的连接。
		defer writer.close()

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
		panic("ioc: container must not be nil")
	}
	f := &SseHandle{
		handle:    handle,
		container: container,
		stats:     &sseStats{},
		conns:     &connTable{conns: make(map[*sseWriter]struct{})},
		semaphore: make(chan struct{}, defaultMaxConnections),
	}

	// 构造时自注册关闭钩子，把 Shutdown 托给框架的停机流程。
	//
	// 放在构造处而不是每个路由注册点：conns 表由分组共享，一次注册即可；
	// 若在每个 Get/Post 里注册，多路由就会注册多份（框架无法对钩子去重），
	// 停机时同一批连接会被反复通知。
	// 业务侧因此不必再写 Listen(sseHandle.Shutdown) —— 忘了那一步的后果是
	// 客户端静默突然断连、且不报任何错，属于"忘不掉才好"的约定。
	handle.OnShutdown(f.Shutdown)

	return f
}

// sseWriter 框架层 SSE 写入实现，封装 Flusher、心跳、错误格式与背压。
//
// 正确性由五条不变量撑着，改这里之前先读一遍：
//
//  1. **只有 flushLoop 写 socket**：业务帧、心跳帧、错误帧、shutdown 帧一律先入队。
//     单写者 ⇒ 同一连接的帧天然有序，也就不需要为"顺序"加锁。
//  2. **close(out) 只在 mu.Lock() 内发生，所有入队都在 mu.RLock() 内** ⇒ 二者互斥，
//     结构上不可能出现 send on closed channel。
//  3. **flushLoop 永不持 mu**（只动原子量与 cancel）⇒ 能腾出队位的那一方不会被 lock 挡住。
//     反过来：若 flushLoop 为了改状态去抢写锁，就会与"持着读锁、阻塞在满队列上"的生产者
//     互等 —— 而唯一能腾出队位的正是 flushLoop 自己。
//     另外，阻塞式 Send 是**持着读锁**在等的（这正是 close() 敢用写锁来等在途入队的原因），
//     所以它必须能被 ctx.Done 唤醒，否则会把 close() 的 mu.Lock() 一起拖住。
//  4. **done 由 flushLoop 的 defer 关闭** ⇒ 即便 flushLoop panic 展开，阻塞的生产者也能逃逸。
//  5. **close() 必须在 handler 返回前完成**：handler 返回后 gin 会把 ResponseWriter 放回
//     sync.Pool 复用，此时还在写就不是"写失败"，而是写进别人的连接。
type sseWriter struct {
	writer      http.ResponseWriter
	rc          *http.ResponseController // 只用于按帧设写 deadline
	lastEventID string
	ctx         context.Context
	cancel      context.CancelFunc
	stats       *sseStats

	mu     sync.RWMutex  // 只负责"close(out) 不与入队并发"，不覆盖 socket 写入
	closed atomic.Bool   // 已拒绝新帧；close() 与 flushLoop 都会置它
	out    chan []byte   // 待写帧（已编码），只由 close() 关闭
	done   chan struct{} // flushLoop 退出后关闭

	once     sync.Once
	hb       sync.WaitGroup // 心跳 goroutine，close 时等它退出
	draining atomic.Bool
	stopped  atomic.Bool // shutdown 帧只发一次

	// 构造时快照，之后不再读包级 var：包级 var 会被测试改写，在协程里读会构成数据竞争。
	frameTimeout time.Duration
	interval     time.Duration
}

// writerOpts 写队列参数。零值即生产默认值，测试可缩短。
type writerOpts struct {
	queueDepth   int           // 待写队列容量（帧）
	frameTimeout time.Duration // 单帧写超时
	heartbeat    time.Duration // 心跳间隔
}

// newSseWriter 构造连接的写入器，并启动写 goroutine 与心跳 goroutine。
func newSseWriter(w http.ResponseWriter, ctx context.Context, cancel context.CancelFunc, lastEventID string, stats *sseStats, opts writerOpts) *sseWriter {
	if opts.queueDepth <= 0 {
		opts.queueDepth = defaultQueueDepth
	}
	if opts.frameTimeout <= 0 {
		opts.frameTimeout = defaultFrameWriteTimeout
	}
	if opts.heartbeat <= 0 {
		opts.heartbeat = heartbeatInterval
	}
	sw := &sseWriter{
		writer:       w,
		rc:           http.NewResponseController(w),
		lastEventID:  lastEventID,
		ctx:          ctx,
		cancel:       cancel,
		stats:        stats,
		out:          make(chan []byte, opts.queueDepth),
		done:         make(chan struct{}),
		frameTimeout: opts.frameTimeout,
		interval:     opts.heartbeat,
	}

	// 用 core 的 util.Go 而非裸 go：这两个协程跑在 gin 的 handler 协程之外，
	// panic 没有 gin.Recovery 兜底，一次 panic 会打死整个进程。
	coreUtil.Go(ctx, sw.flushLoop)
	sw.hb.Add(1)
	coreUtil.Go(ctx, func() {
		defer sw.hb.Done()
		sw.heartbeat()
	})
	return sw
}

// Send 发送一条 SSE 事件，id/event 可为空。
//
// 帧进入该连接的有界队列后由写 goroutine 异步写出：返回 nil 只表示"已入队"、不代表"已送达"；
// 队列满时**阻塞**（背压）而不丢帧；连接已关时返回 ErrClosed。
func (w *sseWriter) Send(id, event, data string) error {
	return w.enqueue(buildFrame(id, event, data), true)
}

// TrySend 非阻塞发送一条 SSE 事件：队列满时**丢弃该帧**并返回 ErrBackpressure，
// 由业务自行决定策略（降速、跳过本次、或断开该连接）。
func (w *sseWriter) TrySend(id, event, data string) error {
	return w.enqueue(buildFrame(id, event, data), false)
}

// LastEventID 返回客户端重连时携带的 Last-Event-Id 请求头。
func (w *sseWriter) LastEventID() string {
	return w.lastEventID
}

// buildFrame 编码一条 SSE 事件帧，复用 util.WriteSSE，避免帧格式两处各写一遍。
func buildFrame(id, event, data string) []byte {
	var buf bytes.Buffer
	_ = sseutil.WriteSSE(&buf, id, event, data) // bytes.Buffer 的 Write 不返回错误
	return buf.Bytes()
}

// enqueue 把一帧投进待写队列。block=true 时队满则等待（背压，不丢帧）；
// block=false 时队满即丢弃该帧并返回 ErrBackpressure。
func (w *sseWriter) enqueue(frame []byte, block bool) error {
	if w.closed.Load() { // 快路径：关闭之后不必再抢锁
		return sseContract.ErrClosed
	}

	w.mu.RLock()
	defer w.mu.RUnlock()
	// 必须在锁内复查：close() 先置 closed 再抢写锁，所以拿到读锁时要么它还没开始关
	// （close(out) 被写锁挡在门外，不可能与本次入队并发），要么 closed 已为真。
	if w.closed.Load() {
		return sseContract.ErrClosed
	}

	// 写 goroutine 已退出 ⇒ 队列不会再被消费，投进去等于永久丢失，直接报关闭。
	select {
	case <-w.done:
		return sseContract.ErrClosed
	default:
	}

	if block {
		select {
		case w.out <- frame:
			return nil
		case <-w.done: // 写失败导致 flushLoop 提前退出，让阻塞的生产者逃逸
			return sseContract.ErrClosed
		case <-w.ctx.Done():
			// 连接已取消（客户端断开 / 停机 / 写失败）：继续等没有意义，而且
			// 这里还持着读锁 —— 不逃逸就会把 close() 的 mu.Lock() 一起拖住。
			return sseContract.ErrClosed
		}
	}
	select {
	case w.out <- frame:
		return nil
	default:
		w.stats.dropped.Add(1)
		return sseContract.ErrBackpressure
	}
}

// flushLoop 是唯一写 socket 的 goroutine：从队列取帧写出，直到 close(out)。
//
// close(out) 之后 range 会把缓冲帧取干净再退出 —— 这正好就是"排空"：Shutdown 是
// "先入队 shutdown 帧、再 cancel"，帧必然已在缓冲里，所以客户端仍能收到它。
// 排空受 drainBudget 约束（客户端活着但很慢时，每帧都可能写很久），且遇到写失败即停。
//
// 本函数**绝不持 mu**（不变量 3）。
func (w *sseWriter) flushLoop() {
	defer close(w.done)

	var drainDeadline time.Time
	for frame := range w.out {
		if w.draining.Load() {
			if drainDeadline.IsZero() {
				drainDeadline = time.Now().Add(drainBudget)
			} else if time.Now().After(drainDeadline) {
				return // 超预算：丢掉的是这个慢客户端本来就吃不下的积压
			}
		}
		if err := w.writeFrame(frame); err != nil {
			w.closed.Store(true) // 只动原子量，不抢 mu
			w.cancel()           // 连接已死：让业务 Serve 与心跳一并退出
			return               // 首错即止，不再逐帧重试
		}
	}
}

// writeFrame 写出一帧并 flush，返回 error 表示连接已不可用。
//
// 每次写前重设写 deadline 是**硬性要求**而非防御：deadline 是绝对且持久的，
// 上一帧留下的旧 deadline 会让下一帧的 Write 立刻失败（与框架 WriteTimeout 那个坑同因）。
func (w *sseWriter) writeFrame(frame []byte) error {
	// 用 ResponseController 而非类型断言：这里拿到的是 gin 的 responseWriter，
	// 经 Unwrap() 链能到底层 http.ResponseWriter；不支持该能力的实现（HTTP/2、
	// httptest.Recorder）会返回 error，忽略即可 —— 此时只剩 close() 里的打断兜底。
	_ = w.rc.SetWriteDeadline(time.Now().Add(w.frameTimeout))
	if _, err := w.writer.Write(frame); err != nil {
		return err
	}
	return flushErr(w.writer)
}

// flushErr 沿 Unwrap 链下探并优先取 FlushError。
//
// 不能用 flusher.Flush()，也不能用 http.ResponseController.Flush()：gin 的
// responseWriter 同时实现了 Flusher 与 Unwrap，而 ResponseController 的类型分支里
// Flusher 在前 —— 遇到它就直接 Flush() 并丢弃错误，永不下探到底层
// http.response.FlushError()。后果是"半截帧"被当成成功、还会接着重设 deadline 继续写，
// 客户端解析器静默错乱。
func flushErr(w http.ResponseWriter) error {
	for {
		switch t := w.(type) {
		case interface{ FlushError() error }: // 底层 http.response：能回报真实写错误
			return t.FlushError()
		case interface{ Unwrap() http.ResponseWriter }: // gin 等包装层：先下探
			w = t.Unwrap()
		case http.Flusher:
			t.Flush()
			return nil
		default:
			return nil // 无从上报错误的实现（如 httptest.ResponseRecorder）
		}
	}
}

// heartbeat 定期发送 SSE comment 保持连接。
//
// 队满只说明客户端吃得慢、不等于连接已死，所以丢帧只跳过本次心跳；
// 只有连接已关（ErrClosed）或 ctx 结束才退出。真正的死链检测在 flushLoop（写失败即取消）。
func (w *sseWriter) heartbeat() {
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()
	for {
		select {
		case <-w.ctx.Done():
			return
		case <-ticker.C:
			if err := w.enqueue(heartbeatFrame, false); errors.Is(err, sseContract.ErrClosed) {
				return
			}
		}
	}
}

// heartbeatFrame 心跳帧（SSE comment）。只读共享，Write 不得改写传入的切片。
var heartbeatFrame = []byte(": heartbeat\n\n")

// shutdownFrame 停机帧，客户端据此主动断开。
var shutdownFrame = buildFrame("", "shutdown", `{"code":"shutdown","message":"server is shutting down"}`)

// writeError 以 JSON 格式发送错误事件，客户端可据此区分错误类型。
//
// 尽力投递：这是最后一帧，为它把 handler 卡在满队列上不值得（队满时客户端本来也读不到）。
func (w *sseWriter) writeError(err error) {
	payload, _ := json.Marshal(map[string]string{
		"code":    "500",
		"message": err.Error(),
	})
	_ = w.enqueue(buildFrame("", "error", string(payload)), false)
}

// shutdown 发送 shutdown 帧并取消连接，通知客户端主动断开。重复调用无副作用（钩子须幂等）。
//
// 尽力投递：停机钩子跑在框架的关闭路径上，卡住的客户端不能把它挂住。队满时这一帧到不了
// 客户端也不是妥协 —— 队满必然意味着对端 socket 缓冲也已填满，此时它连数据帧都收不到。
func (w *sseWriter) shutdown() {
	if !w.stopped.CompareAndSwap(false, true) {
		return
	}
	_ = w.enqueue(shutdownFrame, false)
	w.cancel()
}

// close 停止写 goroutine 并等它退出。必须在 handler 返回前调用（不变量 5）。
func (w *sseWriter) close() {
	w.once.Do(func() {
		w.closed.Store(true) // 1) 先拒绝新帧（心跳、错误帧、停机钩子从这里开始失败）
		w.cancel()           // 2) 让业务 Serve 与心跳退出（幂等）
		w.hb.Wait()          // 3) 心跳不可能再入队

		w.mu.Lock() // 4) 等在途入队落位（阻塞在满队列上的那个由仍在 range 的 flushLoop 放行）
		w.draining.Store(true)
		close(w.out) // 5) 关门：range 取干缓冲帧后自然退出 = 排空
		w.mu.Unlock()

		select {
		case <-w.done:
			return
		case <-time.After(closeGrace):
		}

		// 写正卡在 socket 上（客户端不读、旧 deadline 还没到）：deadline 对**正在进行**的
		// I/O 同样生效，设成过去的时间点即让这次 Write 立刻失败返回。
		_ = w.rc.SetWriteDeadline(time.Now())
		select {
		case <-w.done:
		case <-time.After(closeGrace):
			// 连 deadline 都打断不了（ResponseWriter 不支持该能力）时不再干等：
			// 继续等只会把 handler 与连接槽位一起拖住，而这条连接已经不可救。
		}
	})
}
