package io

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"

	"github.com/warpstreamlabs/bento/internal/component/input"
	"github.com/warpstreamlabs/bento/internal/log"
	"github.com/warpstreamlabs/bento/internal/manager/mock"
	"github.com/warpstreamlabs/bento/internal/message"
)

func TestDynamicReplacementAllowsCompletedCleanupError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		want := errors.New("completed cleanup")
		done := make(chan struct{})
		close(done)
		source := &dynamicCleanupInput{
			Input: mock.Input{TChan: make(chan message.Transaction)},
			done:  done, err: want,
		}
		logger := &cleanupErrorLog{Modular: log.Noop()}
		p, err := newDynamicFanInInput(map[string]input.Streamed{"source": source}, logger, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		replacement := &mock.Input{TChan: make(chan message.Transaction)}
		if err := p.SetInput(t.Context(), "source", replacement); err != nil {
			t.Errorf("SetInput(after completed cleanup error) = %v, want nil", err)
		}
		if p.inputs["source"] != replacement {
			t.Error("SetInput(after completed cleanup error) did not install replacement")
		}
		if logger.calls != 1 || !errors.Is(logger.err, want) {
			t.Errorf("cleanup log = (%d, %v), want (1, %v)", logger.calls, logger.err, want)
		}
		p.TriggerCloseNow()
		if err := p.WaitForClose(t.Context()); err != nil {
			t.Errorf("WaitForClose(replacement) = %v, want nil", err)
		}
		// The failed candidate never adopts the caller-owned replacement.
		replacement.TriggerCloseNow()
	})
}

func TestDynamicReplacementRetainsInterruptedCleanup(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		done := make(chan struct{})
		source := &dynamicCleanupInput{
			Input: mock.Input{TChan: make(chan message.Transaction)},
			done:  done,
		}
		p, err := newDynamicFanInInput(map[string]input.Streamed{"source": source}, log.Noop(), nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		replacement := &mock.Input{TChan: make(chan message.Transaction)}
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		result := make(chan error, 1)
		go func() { result <- p.SetInput(ctx, "source", replacement) }()
		synctest.Wait() // Forwarding stopped, but source cleanup has not finished.
		if len(result) != 0 {
			t.Error("SetInput(unfinished cleanup) returned before completion or cancellation")
		}
		cancel()
		if err := <-result; !errors.Is(err, context.Canceled) {
			t.Errorf("SetInput(cancelled cleanup) = %v, want cancellation", err)
		}
		if p.inputs["source"] != source {
			t.Error("SetInput(cancelled cleanup) released the old input")
		}
		close(done)
		if err := p.SetInput(t.Context(), "source", replacement); err != nil {
			t.Errorf("SetInput(fresh wait after cleanup) = %v, want nil", err)
		}
		if p.inputs["source"] != replacement {
			t.Error("SetInput(fresh wait after cleanup) did not install replacement")
		}
		p.TriggerCloseNow()
		if err := p.WaitForClose(t.Context()); err != nil {
			t.Errorf("WaitForClose(replacement) = %v, want nil", err)
		}
	})
}

func TestDynamicShutdownPreservesCleanupError(t *testing.T) {
	for _, tc := range []struct {
		name string
		stop func(*dynamicFanInInput)
	}{
		{"graceful", (*dynamicFanInInput).TriggerStopConsuming},
		{"hard", (*dynamicFanInInput).TriggerCloseNow},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				want := errors.New("child cleanup")
				done := make(chan struct{})
				source := &dynamicCleanupInput{
					Input: mock.Input{TChan: make(chan message.Transaction)},
					done:  done, err: want,
				}
				p, err := newDynamicFanInInput(map[string]input.Streamed{"source": source}, log.Noop(), nil, nil)
				if err != nil {
					t.Fatal(err)
				}
				tc.stop(p)
				synctest.Wait()
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
			})
		})
	}
}

type dynamicCleanupInput struct {
	mock.Input
	done <-chan struct{}
	err  error
}

func (i *dynamicCleanupInput) WaitForClose(ctx context.Context) error {
	select {
	case <-i.done:
		return i.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

type cleanupErrorLog struct {
	log.Modular
	calls int
	err   error
}

func (l *cleanupErrorLog) Error(_ string, values ...any) {
	l.calls++
	for _, value := range values {
		if err, ok := value.(error); ok {
			l.err = err
		}
	}
}

func TestDynamicHardStopJoinsBlockedForwarder(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		source := &mock.Input{TChan: make(chan message.Transaction, 1)}
		source.TChan <- message.NewTransaction(message.QuickBatch([][]byte{[]byte("held")}), make(chan error, 1))
		p, err := newDynamicFanInInput(map[string]input.Streamed{"source": source}, log.Noop(), nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		synctest.Wait() // The worker is blocked forwarding to an unconsumed output.
		p.TriggerCloseNow()
		if err := p.WaitForClose(t.Context()); err != nil {
			t.Fatal(err)
		}
		if _, open := <-p.TransactionChan(); open {
			t.Error("forwarding channel remains open after close")
		}
	})
}
