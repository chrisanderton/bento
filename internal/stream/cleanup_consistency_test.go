package stream_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/warpstreamlabs/bento/internal/message"
	"github.com/warpstreamlabs/bento/internal/pipeline"
)

// Reuse the existing lifecycle fixtures, but start each layer before shutdown.
// The older coverage intentionally tested pre-start ownership only.
func TestStartedComponentRetainsCleanupAfterForcedStop(t *testing.T) {
	for _, kind := range []string{"processor", "pool", "buffer", "writer"} {
		t.Run(kind, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				release := make(chan struct{})
				var releaseOnce sync.Once
				resource := &lifecycleResource{release: release}
				layer := newLifecycleComponent(t, kind, resource)
				t.Cleanup(func() {
					releaseOnce.Do(func() { close(release) })
					layer.TriggerCloseNow()
					synctest.Wait() // Join held fixture workers, even when the runtime reports completion early.
					if err := layer.WaitForClose(context.Background()); err != nil {
						t.Errorf("cleanup WaitForClose(%s) = %v, want nil", kind, err)
					}
				})
				if err := layer.Consume(make(chan message.Transaction)); err != nil {
					t.Fatal(err)
				}
				synctest.Wait()
				layer.TriggerCloseNow()
				synctest.Wait()
				if resource.closes.Load() == 0 || resource.finished.Load() {
					t.Fatalf("forced Close(%s): calls=%d finished=%t, want entered and held", kind, resource.closes.Load(), resource.finished.Load())
				}
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				if err := layer.WaitForClose(ctx); !errors.Is(err, context.DeadlineExceeded) {
					t.Errorf("WaitForClose(%s, held cleanup) = %v, want deadline exceeded", kind, err)
				}
				joined := make(chan error, 1)
				go func() { joined <- layer.WaitForClose(context.Background()) }()
				synctest.Wait()
				returned := false
				select {
				case err := <-joined:
					returned = true
					t.Errorf("fresh WaitForClose(%s) = %v before child release, want pending", kind, err)
				default:
				}
				releaseOnce.Do(func() { close(release) })
				synctest.Wait()
				if !returned {
					if err := <-joined; err != nil {
						t.Errorf("fresh WaitForClose(%s, released) = %v, want nil", kind, err)
					}
				}
				if !resource.finished.Load() {
					t.Errorf("Close(%s) did not finish after fixture release", kind)
				}
			})
		})
	}
}

func TestStartedProcessorClosesRemainingChildrenAfterError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		closeErr := errors.New("first processor cleanup failed")
		first := &lifecycleResource{closeErr: closeErr}
		second := &lifecycleResource{}
		layer := pipeline.NewProcessor(first, second)
		messages := make(chan message.Transaction)
		if err := layer.Consume(messages); err != nil {
			t.Fatal(err)
		}
		close(messages)
		synctest.Wait()
		err := layer.WaitForClose(context.Background())
		if got := second.closes.Load(); got != 1 {
			t.Errorf("second processor Close calls = %d after first cleanup error, want 1", got)
		}
		if !errors.Is(err, closeErr) {
			t.Errorf("WaitForClose(after processor cleanup error) = %v, want original cleanup error", err)
		}
	})
}
