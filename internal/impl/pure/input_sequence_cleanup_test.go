package pure

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"

	"github.com/Jeffail/shutdown"

	"github.com/warpstreamlabs/bento/internal/manager/mock"
	"github.com/warpstreamlabs/bento/internal/message"
	"github.com/warpstreamlabs/bento/public/service"
)

func TestSequenceShutdownPreservesCleanupError(t *testing.T) {
	for _, tc := range []struct {
		name string
		stop func(*sequenceInput)
	}{
		{"graceful", (*sequenceInput).TriggerStopConsuming},
		{"hard", (*sequenceInput).TriggerCloseNow},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				want := errors.New("child cleanup")
				done := make(chan struct{})
				source := &sequenceCleanupInput{
					Input: mock.Input{TChan: make(chan message.Transaction)},
					done:  done, err: want,
				}
				p := &sequenceInput{
					target:       source,
					log:          service.MockResources().Logger(),
					transactions: make(chan message.Transaction),
					shutSig:      shutdown.NewSignaller(),
				}
				go p.loop()
				synctest.Wait()
				tc.stop(p)
				synctest.Wait() // The runtime owns a still-pending child cleanup.
				ctx, cancel := context.WithCancel(t.Context())
				cancel()
				if err := p.WaitForClose(ctx); !errors.Is(err, context.Canceled) {
					t.Errorf("WaitForClose(cancelled) = %v, want cancellation", err)
				}
				close(done)
				for range 2 {
					if err := p.WaitForClose(t.Context()); !errors.Is(err, want) || errors.Is(err, context.Canceled) {
						t.Errorf("WaitForClose(fresh) = %v, want only child cleanup error", err)
					}
				}
				if _, open := <-p.TransactionChan(); open {
					t.Error("WaitForClose(fresh) returned with an open forwarding channel")
				}
			})
		})
	}
}

type sequenceCleanupInput struct {
	mock.Input
	done <-chan struct{}
	err  error
}

func (i *sequenceCleanupInput) WaitForClose(ctx context.Context) error {
	select {
	case <-i.done:
		return i.err
	case <-ctx.Done():
		return ctx.Err()
	}
}
