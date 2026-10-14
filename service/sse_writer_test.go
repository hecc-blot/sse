package service

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	sseContract "github.com/hecc-blot/sse/contract"
)

// errStallWriteTimeout 假 writer 在写 deadline 到点时返回的错误（模拟真实连接的超时写失败）。
var errStallWriteTimeout = errors.New("stallWriter: write deadline exceeded")

// stallWriter 是可控的假 ResponseWriter，用来模拟"客户端不读、TCP 发送缓冲已满"的连接：
// Write 会一直阻塞，直到测试放行（unblock）或写 deadline 到点。
//
// 与 net/http 一致，SetWriteDeadline 对**正在进行**的写同样生效 —— close() 靠设一个过去的
// 时间点来打断卡住的写，这条语义必须被模拟出来，否则那段代码无从验证。
type stallWriter struct {
	release chan struct{} // 测试关闭它以放行所有写
	entered chan struct{} // 每次进入 Write 时非阻塞投递一个信号（容量 1，用于同步）

	mu      sync.Mutex
	dl      time.Time
	written []string
}

func newStallWriter() *stallWriter {
	return &stallWriter{
		release: make(chan struct{}),
		entered: make(chan struct{}, 1),
	}
}

func (s *stallWriter) Header() http.Header { return http.Header{} }
func (s *stallWriter) WriteHeader(int)     {}
func (s *stallWriter) Flush()              {}

func (s *stallWriter) SetWriteDeadline(t time.Time) error {
	s.mu.Lock()
	s.dl = t
	s.mu.Unlock()
	return nil
}

func (s *stallWriter) Write(p []byte) (int, error) {
	select {
	case s.entered <- struct{}{}:
	default:
	}

	// 阻塞直到被放行、或写 deadline 到点。deadline 每轮重取，因此 close() 事后把
	// deadline 设成过去的时间点，也能打断**正在进行**的这次写（net.Conn 的语义）。
	for {
		s.mu.Lock()
		dl := s.dl
		s.mu.Unlock()

		if !dl.IsZero() && !dl.After(time.Now()) {
			return 0, errStallWriteTimeout
		}

		wait := 5 * time.Millisecond
		if !dl.IsZero() {
			if until := time.Until(dl); until < wait {
				wait = until
			}
		}
		select {
		case <-s.release:
			s.mu.Lock()
			s.written = append(s.written, string(p))
			s.mu.Unlock()
			return len(p), nil
		case <-time.After(wait):
		}
	}
}

// unblock 放行所有写。重复调用会 panic（与 close(channel) 一致），测试里只调一次。
func (s *stallWriter) unblock() { close(s.release) }

// frames 返回已写出的帧副本。
func (s *stallWriter) frames() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.written...)
}

// waitForFrame 轮询等待出现包含 want 的帧。
func waitForFrame(t *testing.T, s *stallWriter, want string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, f := range s.frames() {
			if strings.Contains(f, want) {
				return
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("超时未等到包含 %q 的帧，已写出：%q", want, s.frames())
}

// newTestWriter 构造直连假 writer 的连接写入器：队列与写阻塞都精确可控。
// 心跳与单帧超时默认调得很长，避免干扰；用例按需覆盖。
func newTestWriter(t *testing.T, fake *stallWriter, opts writerOpts) (*sseWriter, *sseStats) {
	t.Helper()
	if opts.frameTimeout == 0 {
		opts.frameTimeout = time.Minute
	}
	if opts.heartbeat == 0 {
		opts.heartbeat = time.Hour
	}
	ctx, cancel := context.WithCancel(context.Background())
	stats := &sseStats{}
	sw := newSseWriter(fake, ctx, cancel, "", stats, opts)
	t.Cleanup(sw.close) // 每个用例都收口，避免写 goroutine 泄漏
	return sw, stats
}

// TestSendBlocksWhenQueueFull：Send 是背压语义 —— 队满时阻塞等待，绝不丢帧。
func TestSendBlocksWhenQueueFull(t *testing.T) {
	fake := newStallWriter()
	sw, _ := newTestWriter(t, fake, writerOpts{queueDepth: 2})

	assert.NoError(t, sw.Send("1", "", "a"))
	<-fake.entered // 写 goroutine 已取走第一帧并卡在 Write 上

	assert.NoError(t, sw.Send("2", "", "b"))
	assert.NoError(t, sw.Send("3", "", "c")) // 队列（容量 2）满

	sent := make(chan error, 1)
	go func() { sent <- sw.Send("4", "", "d") }()

	select {
	case err := <-sent:
		t.Fatalf("队满时 Send 应阻塞形成背压，却立刻返回：%v", err)
	case <-time.After(100 * time.Millisecond):
	}

	fake.unblock()
	select {
	case err := <-sent:
		assert.NoError(t, err, "队列腾出空位后 Send 应返回 nil")
	case <-time.After(2 * time.Second):
		t.Fatal("队列腾出空位后 Send 仍未返回")
	}

	sw.close()
	// 背压不丢帧：四帧按发送顺序全部写出。
	assert.Equal(t, []string{
		string(buildFrame("1", "", "a")),
		string(buildFrame("2", "", "b")),
		string(buildFrame("3", "", "c")),
		string(buildFrame("4", "", "d")),
	}, fake.frames())
}

// TestTrySendDropsWhenQueueFull：TrySend 是非阻塞语义 —— 队满即丢当前帧并报 ErrBackpressure，
// 且计入 Dropped 便于运维发现"有客户端消费跟不上"。
func TestTrySendDropsWhenQueueFull(t *testing.T) {
	fake := newStallWriter()
	sw, stats := newTestWriter(t, fake, writerOpts{queueDepth: 2})

	assert.NoError(t, sw.Send("1", "", "a"))
	<-fake.entered

	assert.NoError(t, sw.TrySend("2", "", "b"))
	assert.NoError(t, sw.TrySend("3", "", "c"))

	start := time.Now()
	err := sw.TrySend("4", "", "d")
	assert.ErrorIs(t, err, sseContract.ErrBackpressure)
	assert.Less(t, time.Since(start), 100*time.Millisecond, "TrySend 不得阻塞")
	assert.Equal(t, int64(1), stats.dropped.Load())

	fake.unblock()
	sw.close()

	// 被丢弃的第四帧不出现在客户端，其余三帧照常写出。
	assert.Equal(t, []string{
		string(buildFrame("1", "", "a")),
		string(buildFrame("2", "", "b")),
		string(buildFrame("3", "", "c")),
	}, fake.frames())
}

// TestFrameOrderPreserved：Send 与 TrySend 混用时帧序必须仍是发送序 ——
// 所有帧共用同一条队列、同一个写 goroutine，正是为了排除"同步写插队"导致的乱序。
func TestFrameOrderPreserved(t *testing.T) {
	fake := newStallWriter()
	sw, _ := newTestWriter(t, fake, writerOpts{queueDepth: 8})

	assert.NoError(t, sw.Send("1", "", "a"))
	<-fake.entered // 卡住第一帧，后面的帧才能稳定排在队列里

	assert.NoError(t, sw.TrySend("2", "", "b"))
	assert.NoError(t, sw.Send("3", "", "c"))
	assert.NoError(t, sw.TrySend("4", "", "d"))

	fake.unblock()
	sw.close()

	assert.Equal(t, []string{
		string(buildFrame("1", "", "a")),
		string(buildFrame("2", "", "b")),
		string(buildFrame("3", "", "c")),
		string(buildFrame("4", "", "d")),
	}, fake.frames())
}

// TestSendAfterCloseReturnsErrClosed：连接关掉之后入队一律报 ErrClosed（不是 panic、也不是丢进队列）。
func TestSendAfterCloseReturnsErrClosed(t *testing.T) {
	fake := newStallWriter()
	sw, _ := newTestWriter(t, fake, writerOpts{queueDepth: 2})
	assert.NoError(t, sw.Send("1", "", "a"))
	<-fake.entered

	sw.close()

	assert.ErrorIs(t, sw.Send("2", "", "b"), sseContract.ErrClosed)
	assert.ErrorIs(t, sw.TrySend("3", "", "c"), sseContract.ErrClosed)
}

// TestStuckWriteIsInterruptedByFrameTimeout：客户端不读时写会永久阻塞，靠"单帧写超时"
// 打断它并取消连接 —— 否则写 goroutine 永远卡在 Write 上，close() 也就永远等不到它。
func TestStuckWriteIsInterruptedByFrameTimeout(t *testing.T) {
	fake := newStallWriter()
	sw, _ := newTestWriter(t, fake, writerOpts{queueDepth: 2, frameTimeout: 50 * time.Millisecond})

	assert.NoError(t, sw.Send("1", "", "a"))
	<-fake.entered

	select {
	case <-sw.ctx.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("单帧写超时到点后应取消连接")
	}
	assert.ErrorIs(t, sw.Send("2", "", "b"), sseContract.ErrClosed)
}

// TestCloseIsBoundedAndStopsWriting：close() 必须在有界时间内返回（用写 deadline 打断
// 正在进行的写），且返回之后绝不能再写 —— handler 返回后 gin 会把 ResponseWriter 放回
// sync.Pool 复用，那时再写就是写进别人的连接。
func TestCloseIsBoundedAndStopsWriting(t *testing.T) {
	fake := newStallWriter()
	// 单帧超时故意设得很长：这里验的是 close() 自己的打断兜底，而不是等超时。
	sw, _ := newTestWriter(t, fake, writerOpts{queueDepth: 2, frameTimeout: time.Hour})

	assert.NoError(t, sw.Send("1", "", "a"))
	<-fake.entered

	start := time.Now()
	sw.close()
	assert.Less(t, time.Since(start), time.Second, "close 不得被卡住的写拖住")

	// 即使随后放行，也不该再有写发生。
	fake.unblock()
	time.Sleep(50 * time.Millisecond)
	assert.Empty(t, fake.frames(), "close 返回后不得再写 ResponseWriter")
}

// TestCloseNotBlockedByStalledSender：业务可以从自己的 goroutine 里发帧，那个 goroutine 可能
// 正**持着读锁**阻塞在满队列上。close() 靠先 cancel 让它逃逸 —— 否则会在 mu.Lock() 上被拖住，
// handler 也就迟迟返回不了。
func TestCloseNotBlockedByStalledSender(t *testing.T) {
	fake := newStallWriter()
	sw, _ := newTestWriter(t, fake, writerOpts{queueDepth: 1})

	assert.NoError(t, sw.Send("1", "", "a"))
	<-fake.entered
	assert.NoError(t, sw.Send("2", "", "b")) // 队满（容量 1）

	stuck := make(chan error, 1)
	go func() { stuck <- sw.Send("3", "", "c") }() // 业务侧 goroutine，阻塞在满队列上
	time.Sleep(50 * time.Millisecond)              // 等它确实进入阻塞

	start := time.Now()
	sw.close()
	assert.Less(t, time.Since(start), time.Second, "close 不得被阻塞中的 Send 拖住")

	assert.ErrorIs(t, <-stuck, sseContract.ErrClosed)
}

// TestShutdownDoesNotBlockOnStuckClient：Shutdown 只做"尽力入队 + cancel"，即使队满且写卡死
// 也必须立刻返回。它跑在框架的停机钩子上 —— 钩子挂死就等于优雅停机失效（这正是本次修复
// 的现存缺陷：旧实现里 Shutdown 会走同步 Send，被卡住的客户端一起拖住）。
func TestShutdownDoesNotBlockOnStuckClient(t *testing.T) {
	fake := newStallWriter()
	sw, _ := newTestWriter(t, fake, writerOpts{queueDepth: 2})

	assert.NoError(t, sw.Send("1", "", "a"))
	<-fake.entered
	assert.NoError(t, sw.Send("2", "", "b"))
	assert.NoError(t, sw.Send("3", "", "c")) // 队满

	start := time.Now()
	sw.shutdown()
	assert.Less(t, time.Since(start), 100*time.Millisecond, "Shutdown 不得被卡住的客户端拖住")

	select {
	case <-sw.ctx.Done():
	default:
		t.Fatal("shutdown 应取消连接")
	}

	sw.shutdown() // 幂等：重复调用不发帧、不 panic
}

// TestHeartbeatSurvivesBackpressure：队满只说明客户端慢，不等于连接已死 ——
// 心跳该跳过本次而不是把协程杀掉，客户端恢复后心跳必须继续。
func TestHeartbeatSurvivesBackpressure(t *testing.T) {
	fake := newStallWriter()
	sw, _ := newTestWriter(t, fake, writerOpts{queueDepth: 1, heartbeat: 20 * time.Millisecond})

	assert.NoError(t, sw.Send("1", "", "a"))
	<-fake.entered
	assert.NoError(t, sw.Send("2", "", "b")) // 队满（容量 1）

	// 这段时间里心跳反复撞上满队列，全部走"丢帧"分支。
	time.Sleep(100 * time.Millisecond)

	fake.unblock()
	waitForFrame(t, fake, ": heartbeat", 2*time.Second)
}
