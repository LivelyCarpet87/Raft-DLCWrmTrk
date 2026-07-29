package workers

import (
	"context"
	"sync"
	"time"
)

type ActivitySink struct {
	mu       sync.Mutex
	lastSeen time.Time
	ctx      context.Context
}

func NewActivitySink(ctx context.Context) *ActivitySink {
	return &ActivitySink{
		lastSeen: time.Time{},
		ctx:      ctx,
	}
}

func (s *ActivitySink) Write(p []byte) (int, error) {
	select {
	case <-s.ctx.Done():
		return 0, s.ctx.Err()
	default:
		s.mu.Lock()
		s.lastSeen = time.Now()
		s.mu.Unlock()
		return len(p), nil
	}
}

func (s *ActivitySink) LastSeen() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastSeen
}
