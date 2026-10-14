package sse

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/hecc-blot/core/enum/errorcode"
)

// TestSentinelCarriesCoreCode 钉住 sentinel → core 码的对应关系（见 feature.md 的「判定规则」）。
func TestSentinelCarriesCoreCode(t *testing.T) {
	// 与 core 的 util.ErrLimitFull 同码：缓冲满，由调用方决定丢弃还是降速。
	code, ok := errorcode.From(ErrBackpressure)
	assert.True(t, ok, "ErrBackpressure 未携带 core 码")
	assert.Equal(t, errorcode.CodeRateLimited, code)

	code, ok = errorcode.From(ErrClosed)
	assert.True(t, ok, "ErrClosed 未携带 core 码")
	assert.Equal(t, errorcode.CodeIllegalState, code)
}

// TestSentinelRemainsIsSable：携带码不得破坏 errors.Is 的单例语义 ——
// 业务就是靠 errors.Is(err, ErrBackpressure) 决定"这帧丢了该怎么降级"的。
func TestSentinelRemainsIsSable(t *testing.T) {
	assert.True(t, errors.Is(fmt.Errorf("push event: %w", ErrBackpressure), ErrBackpressure))
	assert.True(t, errors.Is(fmt.Errorf("push event: %w", ErrClosed), ErrClosed))
	assert.False(t, errors.Is(ErrBackpressure, ErrClosed))
}
