package tracing

import (
	"context"
	"errors"
	"sync/atomic"

	"github.com/Jeffail/shutdown"

	"github.com/warpstreamlabs/bento/internal/component"
	"github.com/warpstreamlabs/bento/internal/component/input"
	"github.com/warpstreamlabs/bento/internal/message"
)

type tracedInput struct {
	e       *events
	ctr     *uint64
	wrapped input.Streamed
	tChan   chan message.Transaction
	shutSig *shutdown.Signaller
}

func traceInput(e *events, counter *uint64, i input.Streamed) input.Streamed {
	t := &tracedInput{
		e:       e,
		ctr:     counter,
		wrapped: i,
		tChan:   make(chan message.Transaction),
		shutSig: shutdown.NewSignaller(),
	}
	go t.loop()
	return t
}

func (t *tracedInput) UnwrapInput() input.Streamed {
	return t.wrapped
}

func (t *tracedInput) loop() {
	defer func() {
		close(t.tChan)
		t.shutSig.TriggerHasStopped()
	}()
	readChan := t.wrapped.TransactionChan()
	for {
		var tran message.Transaction
		var open bool
		select {
		case tran, open = <-readChan:
			if !open {
				return
			}
		case <-t.shutSig.HardStopChan():
			return
		}
		if t.e.IsEnabled() {
			_ = tran.Payload.Iter(func(i int, part *message.Part) error {
				_ = atomic.AddUint64(t.ctr, 1)
				t.e.Add(EventProduceOf(part))
				return nil
			})
		}
		select {
		case t.tChan <- tran:
		case <-t.shutSig.HardStopChan():
			// Stop flushing if we fully timed out
			return
		}
	}
}

func (t *tracedInput) TransactionChan() <-chan message.Transaction {
	return t.tChan
}

func (t *tracedInput) ConnectionStatus() component.ConnectionStatuses {
	return t.wrapped.ConnectionStatus()
}

func (t *tracedInput) TriggerStopConsuming() {
	t.wrapped.TriggerStopConsuming()
}

func (t *tracedInput) TriggerCloseNow() {
	t.wrapped.TriggerCloseNow()
	t.shutSig.TriggerHardStop()
}

func (t *tracedInput) WaitForClose(ctx context.Context) error {
	err := t.wrapped.WaitForClose(ctx)
	t.shutSig.TriggerHardStop()
	select {
	case <-t.shutSig.HasStoppedChan():
	case <-ctx.Done():
		return errors.Join(err, ctx.Err())
	}
	return err
}
