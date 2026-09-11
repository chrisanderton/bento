package tracing

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"

	"github.com/warpstreamlabs/bento/internal/component/input"
	"github.com/warpstreamlabs/bento/internal/manager/mock"
	"github.com/warpstreamlabs/bento/internal/message"
)

func TestInputWaitJoinsForwarder(t *testing.T) {
	for _, tc := range []struct {
		name string
		wrap func(input.Streamed) input.Streamed
	}{
		{"flow", wrapWithFlowID},
		{"trace", func(i input.Streamed) input.Streamed {
			var count uint64
			return traceInput(&events{}, &count, i)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				source := &mock.Input{TChan: make(chan message.Transaction)}
				wrapped := tc.wrap(source)
				synctest.Wait() // Forwarder is parked reading its source.
				if err := wrapped.WaitForClose(t.Context()); err != nil {
					t.Fatal(err)
				}
				select {
				case _, open := <-wrapped.TransactionChan():
					if open {
						t.Error("WaitForClose returned with an open forwarding channel")
					}
				default:
					t.Error("WaitForClose returned before its forwarding worker stopped")
				}
				source.TriggerCloseNow()
				synctest.Wait()
			})
		})
	}
}

func TestInputWaitPreservesChildError(t *testing.T) {
	for _, tc := range []struct {
		name string
		wrap func(input.Streamed) input.Streamed
	}{
		{"flow", wrapWithFlowID},
		{"trace", func(i input.Streamed) input.Streamed {
			var count uint64
			return traceInput(&events{}, &count, i)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				want := errors.New("child cleanup")
				source := &failedCloseInput{Input: mock.Input{TChan: make(chan message.Transaction)}, err: want}
				wrapped := tc.wrap(source)
				for range 2 {
					if err := wrapped.WaitForClose(t.Context()); !errors.Is(err, want) {
						t.Errorf("WaitForClose = %v, want child error", err)
					}
				}
				source.TriggerCloseNow()
			})
		})
	}
}

func TestInputWaitCancelledThenFresh(t *testing.T) {
	for _, tc := range []struct {
		name string
		wrap func(input.Streamed) input.Streamed
	}{
		{"flow", wrapWithFlowID},
		{"trace", func(i input.Streamed) input.Streamed {
			var count uint64
			return traceInput(&events{}, &count, i)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				want := errors.New("child cleanup")
				done := make(chan struct{})
				source := &failedCloseInput{
					Input: mock.Input{TChan: make(chan message.Transaction)},
					done:  done, err: want,
				}
				wrapped := tc.wrap(source)
				ctx, cancel := context.WithCancel(t.Context())
				cancel()
				if err := wrapped.WaitForClose(ctx); !errors.Is(err, context.Canceled) {
					t.Errorf("WaitForClose(cancelled) = %v, want cancellation", err)
				}
				close(done)
				source.TriggerCloseNow()
				if err := wrapped.WaitForClose(t.Context()); !errors.Is(err, want) || errors.Is(err, context.Canceled) {
					t.Errorf("WaitForClose(fresh) = %v, want only child cleanup error", err)
				}
			})
		})
	}
}

type failedCloseInput struct {
	mock.Input
	done <-chan struct{}
	err  error
}

func (i *failedCloseInput) WaitForClose(ctx context.Context) error {
	if i.done != nil {
		select {
		case <-i.done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return i.err
}
