package stream_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/warpstreamlabs/bento/internal/component"
	"github.com/warpstreamlabs/bento/internal/component/buffer"
	"github.com/warpstreamlabs/bento/internal/component/output"
	"github.com/warpstreamlabs/bento/internal/log"
	"github.com/warpstreamlabs/bento/internal/message"
	"github.com/warpstreamlabs/bento/internal/pipeline"
)

func TestUnstartedComponentsCloseOnceWithoutProcessing(t *testing.T) {
	for _, kind := range []string{"buffer", "processor", "pool", "writer"} {
		t.Run(kind, func(t *testing.T) {
			closeErr := errors.New("close failed")
			resource := &lifecycleResource{closeErr: closeErr}
			layer := newLifecycleComponent(t, kind, resource)
			var callers sync.WaitGroup
			for range 8 {
				callers.Go(layer.TriggerCloseNow)
			}
			callers.Wait()
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			for range 2 {
				if err := layer.WaitForClose(ctx); !errors.Is(err, closeErr) {
					t.Errorf("WaitForClose(%s) = %v, want retained close error", kind, err)
				}
			}
			if got := resource.closes.Load(); got != 1 {
				t.Errorf("Close(%s) calls = %d, want 1", kind, got)
			}
			if got := resource.activity.Load(); got != 0 {
				t.Errorf("Close(%s) processing/connect calls = %d, want 0", kind, got)
			}
			if err := layer.Consume(make(chan message.Transaction)); !errors.Is(err, component.ErrAlreadyStarted) {
				t.Errorf("Consume(%s, after shutdown) = %v, want rejection", kind, err)
			}
		})
	}
}

func TestUnstartedComponentCloseCanBeJoinedAfterCancelledWait(t *testing.T) {
	for _, kind := range []string{"buffer", "processor", "pool", "writer"} {
		t.Run(kind, func(t *testing.T) {
			release := make(chan struct{})
			var releaseOnce sync.Once
			t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
			resource := &lifecycleResource{release: release, entered: make(chan struct{})}
			layer := newLifecycleComponent(t, kind, resource)
			layer.TriggerCloseNow()
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			select {
			case <-resource.entered:
			case <-ctx.Done():
				t.Fatal("Close did not enter resource cleanup")
			}
			cancelled, stop := context.WithCancel(t.Context())
			stop()
			if err := layer.WaitForClose(cancelled); !errors.Is(err, context.Canceled) {
				t.Errorf("WaitForClose(%s, cancelled while gated) = %v, want cancellation", kind, err)
			}
			if resource.finished.Load() {
				t.Error("gated Close finished before release")
			}
			releaseOnce.Do(func() { close(release) })
			if err := layer.WaitForClose(ctx); err != nil || !resource.finished.Load() {
				t.Errorf("WaitForClose(%s, after release) = %v, finished=%t; want joined cleanup", kind, err, resource.finished.Load())
			}
		})
	}
}

func TestComponentStartupAndShutdownHaveOneOwner(t *testing.T) {
	// A running pool retains its existing shared-processor shutdown semantics.
	// The individual resource-owning wrappers must not run both cleanup paths.
	for _, kind := range []string{"buffer", "processor", "writer"} {
		t.Run(kind, func(t *testing.T) {
			for range 50 {
				resource := &lifecycleResource{}
				layer := newLifecycleComponent(t, kind, resource)
				messages := make(chan message.Transaction)
				close(messages)
				var callers sync.WaitGroup
				callers.Go(func() {
					if err := layer.Consume(messages); err != nil && !errors.Is(err, component.ErrAlreadyStarted) {
						t.Errorf("Consume(%s, racing close) = %v, want start or closed rejection", kind, err)
					}
				})
				callers.Go(layer.TriggerCloseNow)
				callers.Wait()
				ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
				err := layer.WaitForClose(ctx)
				cancel()
				if err != nil || resource.closes.Load() != 1 {
					t.Fatalf("WaitForClose(%s, racing startup) = %v, closes=%d; want nil and 1", kind, err, resource.closes.Load())
				}
			}
		})
	}
}

func TestPoolStartupAndShutdownHaveOneOwner(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		for range 50 {
			resource := &lifecycleResource{}
			pool := newLifecycleComponent(t, "pool", resource)
			messages := make(chan message.Transaction)
			close(messages)
			var consumeErr error
			var callers sync.WaitGroup
			callers.Go(func() { consumeErr = pool.Consume(messages) })
			callers.Go(pool.TriggerCloseNow)
			callers.Wait()
			synctest.Wait()        // Includes worker cleanup even if the inherited pool join returned earlier.
			wantCloses := int64(2) // Running workers share processors in the existing implementation.
			if errors.Is(consumeErr, component.ErrAlreadyStarted) {
				wantCloses = 1 // Pre-start cleanup owns the shared processors directly.
			} else if consumeErr != nil {
				t.Fatalf("Pool.Consume(racing close) = %v, want start or closed rejection", consumeErr)
			}
			if err := pool.WaitForClose(t.Context()); err != nil || resource.closes.Load() != wantCloses {
				t.Errorf("Pool.WaitForClose(racing startup) = %v, closes=%d, want nil and %d", err, resource.closes.Load(), wantCloses)
			}
		}
	})
}

type lifecycleComponent interface {
	Consume(<-chan message.Transaction) error
	TriggerCloseNow()
	WaitForClose(context.Context) error
}

func newLifecycleComponent(t *testing.T, kind string, resource *lifecycleResource) lifecycleComponent {
	t.Helper()
	switch kind {
	case "buffer":
		return buffer.NewStream("probe", resource, component.NoopObservability())
	case "processor":
		return pipeline.NewProcessor(resource)
	case "pool":
		pool, err := pipeline.NewPool(2, log.Noop(), resource)
		if err != nil {
			t.Fatal(err)
		}
		return pool
	case "writer":
		writer, err := output.NewAsyncWriter("probe", 1, resource, component.NoopObservability())
		if err != nil {
			t.Fatal(err)
		}
		return writer
	default:
		t.Fatalf("unknown lifecycle fixture %q", kind)
		return nil
	}
}

type lifecycleResource struct {
	activity, closes atomic.Int64
	finished         atomic.Bool
	closeErr         error
	release          <-chan struct{}
	entered          chan struct{}
}

func (r *lifecycleResource) Close(context.Context) error {
	r.closes.Add(1)
	if r.entered != nil {
		close(r.entered)
	}
	if r.release != nil {
		<-r.release
	}
	r.finished.Store(true)
	return r.closeErr
}
func (r *lifecycleResource) Connect(context.Context) error {
	r.activity.Add(1)
	return component.ErrTypeClosed
}
func (r *lifecycleResource) Read(context.Context) (message.Batch, buffer.AckFunc, error) {
	r.activity.Add(1)
	return nil, nil, component.ErrTypeClosed
}
func (r *lifecycleResource) Write(context.Context, message.Batch, buffer.AckFunc) error {
	r.activity.Add(1)
	return nil
}
func (r *lifecycleResource) EndOfInput() {}
func (r *lifecycleResource) WriteBatch(context.Context, message.Batch) error {
	r.activity.Add(1)
	return nil
}
func (r *lifecycleResource) ProcessBatch(context.Context, message.Batch) ([]message.Batch, error) {
	r.activity.Add(1)
	return nil, nil
}
