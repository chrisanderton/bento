package service_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/warpstreamlabs/bento/public/service"
)

// This diagnostic uses the dependency's public wrapper, not a copy of its code.
// The child emits one message, then waits for another message or cancellation.
type oneMessageInput struct {
	read atomic.Bool
}

func (*oneMessageInput) Connect(context.Context) error { return nil }
func (*oneMessageInput) Close(context.Context) error   { return nil }
func (i *oneMessageInput) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	if !i.read.Swap(true) {
		return service.MessageBatch{service.NewMessage([]byte("bad event"))}, func(context.Context, error) error { return nil }, nil
	}
	<-ctx.Done()
	return nil, nil, ctx.Err()
}

// Stock failure depends on scheduling: a single passing sample is insufficient.
// Run this regression with -race -count=100. Repetition samples schedules; it
// does not prove that every possible schedule has been exercised.
func TestRetryReadCancellationDuringBackoff(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		input := service.AutoRetryNacksBatched(&oneMessageInput{})
		t.Cleanup(func() { _ = input.Close(context.Background()) })
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		// Initial read and the first two retries do not back off. Reject all
		// three so the next read is sleeping in the real retry delay.
		for range 3 {
			_, ack, err := input.ReadBatch(ctx)
			if err != nil {
				t.Fatalf("ReadBatch(before cancellation) = %v, want a message", err)
			}
			if err := ack(ctx, errors.New("transformation failed")); err != nil {
				t.Fatalf("Ack(rejected message) = %v, want nil", err)
			}
		}
		// Join the setup reads' cancellation notifications before holding the
		// retry lock across backoff. Mutex waits are not synctest-durable.
		synctest.Wait()
		done := make(chan error, 1)
		go func() {
			_, _, err := input.ReadBatch(ctx)
			done <- err
		}()
		synctest.Wait()
		cancel()
		synctest.Wait()
		select {
		case err := <-done:
			if !errors.Is(err, context.Canceled) {
				t.Errorf("ReadBatch(cancel during backoff) = %v, want context.Canceled", err)
			}
		default:
			t.Error("ReadBatch(cancel during backoff) remains blocked, want context.Canceled")
			// A separate native-reader cancellation releases the missed wake-up.
			// This is diagnostic cleanup, not a proposed runtime workaround.
			_ = input.Close(context.Background())
			if err := <-done; !errors.Is(err, context.Canceled) {
				t.Errorf("ReadBatch(after diagnostic cleanup) = %v, want context.Canceled", err)
			}
		}
	})
}
