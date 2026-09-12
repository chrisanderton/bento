package output

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/warpstreamlabs/bento/internal/component"
	"github.com/warpstreamlabs/bento/internal/message"
)

// A processor constructor can fail before the output receives Consume. Its
// already-created single-payload adapter must still release the underlying writer.
func TestNotBatchedCloseBeforeConsume(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		writer := &mockNBWriter{t: t}
		out, err := NewAsyncWriter("probe", 1, writer, component.NoopObservability())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			// Fixture cleanup is not credited to the adapter under test.
			out.TriggerCloseNow()
			if err := out.WaitForClose(context.Background()); err != nil {
				t.Errorf("fixture cleanup: %v", err)
			}
		})
		wrapped := OnlySinglePayloads(out)
		wrapped.TriggerCloseNow()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := wrapped.WaitForClose(ctx); err != nil {
			t.Errorf("unstarted adapter WaitForClose = %v, want completed cleanup", err)
		}
		writer.mut.Lock()
		closed := writer.closeCalled
		writer.mut.Unlock()
		if !closed {
			t.Error("unstarted adapter did not close its writer")
		}
	})
}

func TestNotBatchedPrestartCloseRetainsCleanup(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		closeErr := errors.New("writer cleanup failed")
		writer := &prestartCleanupWriter{release: release, closeErr: closeErr}
		out, err := NewAsyncWriter("probe", 1, writer, component.NoopObservability())
		if err != nil {
			t.Fatal(err)
		}
		wrapped := OnlySinglePayloads(out)
		for range 3 {
			wrapped.TriggerCloseNow()
		}
		synctest.Wait()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := wrapped.WaitForClose(ctx); !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("WaitForClose(held writer) = %v, want deadline exceeded", err)
		}
		close(release)
		for range 2 {
			if err := wrapped.WaitForClose(context.Background()); !errors.Is(err, closeErr) {
				t.Errorf("WaitForClose(released writer) = %v, want retained cleanup error", err)
			}
		}
		if writer.closes.Load() != 1 || writer.activity.Load() != 0 {
			t.Errorf("pre-start cleanup: closes=%d activity=%d, want 1 and 0", writer.closes.Load(), writer.activity.Load())
		}
		if err := wrapped.Consume(make(chan message.Transaction)); !errors.Is(err, component.ErrAlreadyStarted) {
			t.Errorf("Consume(after cleanup) = %v, want already started", err)
		}
	})
}

func TestNotBatchedFailedConsumeClosesChild(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		consumeErr := errors.New("writer startup failed")
		closeErr := errors.New("writer cleanup failed")
		writer := &prestartCleanupWriter{closeErr: closeErr}
		out, err := NewAsyncWriter("probe", 1, writer, component.NoopObservability())
		if err != nil {
			t.Fatal(err)
		}
		wrapped := OnlySinglePayloads(&rejectConsumeOutput{Streamed: out, err: consumeErr})
		if err := wrapped.Consume(make(chan message.Transaction)); !errors.Is(err, consumeErr) {
			t.Errorf("Consume(failed child) = %v, want startup cause", err)
		}
		wrapped.TriggerCloseNow()
		if err := wrapped.WaitForClose(context.Background()); !errors.Is(err, closeErr) {
			t.Errorf("WaitForClose(failed startup) = %v, want cleanup cause", err)
		}
		if writer.closes.Load() != 1 || writer.activity.Load() != 0 {
			t.Errorf("failed-start cleanup: closes=%d activity=%d, want 1 and 0", writer.closes.Load(), writer.activity.Load())
		}
	})
}

func TestNotBatchedStartupAndShutdownHaveOneOwner(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		for range 50 {
			writer := &prestartCleanupWriter{}
			out, err := NewAsyncWriter("probe", 1, writer, component.NoopObservability())
			if err != nil {
				t.Fatal(err)
			}
			wrapped := OnlySinglePayloads(out)
			messages := make(chan message.Transaction)
			close(messages)
			var callers sync.WaitGroup
			callers.Go(func() {
				if err := wrapped.Consume(messages); err != nil && !errors.Is(err, component.ErrAlreadyStarted) {
					t.Errorf("Consume(racing shutdown) = %v, want start or closed rejection", err)
				}
			})
			for range 3 {
				callers.Go(wrapped.TriggerCloseNow)
			}
			callers.Wait()
			if err := wrapped.WaitForClose(context.Background()); err != nil {
				t.Errorf("WaitForClose(racing startup) = %v, want nil", err)
			}
			// Count actual cleanup, independently of the running wait-completion
			// correction tracked in the separate output contribution.
			synctest.Wait()
			if got := writer.closes.Load(); got != 1 {
				t.Errorf("Close(racing startup) calls = %d, want 1", got)
			}
		}
	})
}

type prestartCleanupWriter struct {
	closes, activity atomic.Int64
	release          <-chan struct{}
	closeErr         error
}

func (w *prestartCleanupWriter) Connect(context.Context) error {
	w.activity.Add(1)
	return nil
}

func (w *prestartCleanupWriter) WriteBatch(context.Context, message.Batch) error {
	w.activity.Add(1)
	return nil
}

func (w *prestartCleanupWriter) Close(context.Context) error {
	w.closes.Add(1)
	if w.release != nil {
		<-w.release
	}
	return w.closeErr
}

type rejectConsumeOutput struct {
	Streamed
	err error
}

func (o *rejectConsumeOutput) Consume(<-chan message.Transaction) error {
	return o.err
}
