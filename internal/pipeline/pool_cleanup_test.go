package pipeline

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/warpstreamlabs/bento/internal/log"
	"github.com/warpstreamlabs/bento/internal/message"
)

func TestPoolJoinsEveryWorkerAfterCleanupError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		firstErr := errors.New("first worker cleanup failed")
		secondErr := errors.New("second worker cleanup failed")
		release := make(chan struct{})
		pool, err := NewPool(2, log.Noop())
		if err != nil {
			t.Fatal(err)
		}
		// Distinct worker fixtures make the join ordering deterministic without
		// changing the production shared-processor ownership contract.
		pool.workers[0] = NewProcessor(&gatedCleanupProcessor{err: firstErr})
		pool.workers[1] = NewProcessor(&gatedCleanupProcessor{release: release, err: secondErr})
		if err := pool.Consume(make(chan message.Transaction)); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		pool.TriggerCloseNow()
		synctest.Wait()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := pool.WaitForClose(ctx); !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("WaitForClose(first error, second held) = %v, want deadline exceeded", err)
		}
		close(release)
		for range 2 {
			err := pool.WaitForClose(context.Background())
			if !errors.Is(err, firstErr) || !errors.Is(err, secondErr) {
				t.Errorf("WaitForClose(released workers) = %v, want both cleanup causes", err)
			}
		}
	})
}

type gatedCleanupProcessor struct {
	release <-chan struct{}
	err     error
}

func (p *gatedCleanupProcessor) ProcessBatch(context.Context, message.Batch) ([]message.Batch, error) {
	return nil, nil
}

func (p *gatedCleanupProcessor) Close(ctx context.Context) error {
	if p.release != nil {
		select {
		case <-p.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return p.err
}
