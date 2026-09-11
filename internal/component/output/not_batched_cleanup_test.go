package output

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/warpstreamlabs/bento/internal/component"
	"github.com/warpstreamlabs/bento/internal/message"
)

func TestNotBatchedForcedStopWaitsForChild(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		writer := &mockNBWriter{t: t, closeChan: make(chan error)}
		child, err := NewAsyncWriter("held", 1, writer, component.NoopObservability())
		if err != nil {
			t.Fatalf("NewAsyncWriter(held) = %v, want nil", err)
		}
		wrapped := OnlySinglePayloads(child)
		if err := wrapped.Consume(make(chan message.Transaction)); err != nil {
			t.Fatalf("Consume(held) = %v, want nil", err)
		}
		released := false
		t.Cleanup(func() {
			if !released {
				close(writer.closeChan)
			}
			wrapped.TriggerCloseNow()
			if err := child.WaitForClose(context.Background()); err != nil {
				t.Errorf("WaitForClose(released child) = %v, want nil", err)
			}
			if err := wrapped.WaitForClose(context.Background()); err != nil {
				t.Errorf("WaitForClose(released wrapper) = %v, want nil", err)
			}
		})
		wrapped.TriggerCloseNow()
		synctest.Wait()
		writer.mut.Lock()
		closeCalled := writer.closeCalled
		writer.mut.Unlock()
		if !closeCalled {
			t.Error("TriggerCloseNow(wrapper) did not reach child Close, want forced propagation")
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := wrapped.WaitForClose(ctx); !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("WaitForClose(held child) = %v, want DeadlineExceeded", err)
		}
		joined := make(chan error, 1)
		go func() { joined <- wrapped.WaitForClose(context.Background()) }()
		synctest.Wait()
		returned := false
		select {
		case err := <-joined:
			returned = true
			t.Errorf("WaitForClose(held child, no deadline) = %v, want pending", err)
		default:
		}
		close(writer.closeChan)
		released = true
		if !returned {
			if err := <-joined; err != nil {
				t.Errorf("WaitForClose(released child, no deadline) = %v, want nil", err)
			}
		}
	})
}
