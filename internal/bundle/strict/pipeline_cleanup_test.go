package strict

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/warpstreamlabs/bento/internal/component"
	"github.com/warpstreamlabs/bento/internal/manager/mock"
	"github.com/warpstreamlabs/bento/internal/message"
	"github.com/warpstreamlabs/bento/internal/pipeline"
)

func TestFeedbackPrestartCloseCanBeJoinedAgain(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		want := errors.New("processor cleanup")
		child := &cleanupProcessor{release: make(chan struct{}), closeErr: want}
		p := newFeedbackProcessor(pipeline.NewProcessor(child), mock.NewManager())
		p.TriggerCloseNow()
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		if err := p.WaitForClose(ctx); !errors.Is(err, context.Canceled) {
			t.Errorf("cancelled WaitForClose = %v, want cancellation", err)
		}
		close(child.release)
		for range 2 {
			if err := p.WaitForClose(t.Context()); !errors.Is(err, want) {
				t.Errorf("fresh WaitForClose = %v, want child error", err)
			}
		}
		if got := child.closes.Load(); got != 1 {
			t.Errorf("child Close calls = %d, want 1", got)
		}
		if err := p.Consume(make(chan message.Transaction)); !errors.Is(err, component.ErrAlreadyStarted) {
			t.Errorf("Consume after close = %v, want ErrAlreadyStarted", err)
		}
	})
}

func TestFeedbackConcurrentStartAndClose(t *testing.T) {
	for range 50 {
		synctest.Test(t, func(t *testing.T) {
			child := &cleanupProcessor{}
			p := newFeedbackProcessor(pipeline.NewProcessor(child), mock.NewManager())
			var wg sync.WaitGroup
			wg.Add(2)
			go func() {
				defer wg.Done()
				if err := p.Consume(make(chan message.Transaction)); err != nil && !errors.Is(err, component.ErrAlreadyStarted) {
					t.Errorf("Consume racing close = %v", err)
				}
			}()
			go func() { defer wg.Done(); p.TriggerCloseNow() }()
			wg.Wait()
			if err := p.WaitForClose(t.Context()); err != nil {
				t.Fatal(err)
			}
			synctest.Wait()
			if got := child.closes.Load(); got != 1 {
				t.Errorf("child Close calls = %d, want 1", got)
			}
		})
	}
}

func TestFeedbackNormalDeliveryAndHardStop(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := newFeedbackProcessor(pipeline.NewProcessor(), mock.NewManager())
		in := make(chan message.Transaction)
		if err := p.Consume(in); err != nil {
			t.Fatal(err)
		}
		ack := make(chan error, 1)
		in <- message.NewTransaction(message.QuickBatch([][]byte{[]byte("hello")}), ack)
		tran := <-p.TransactionChan()
		if got := string(tran.Payload.Get(0).AsBytes()); got != "hello" {
			t.Errorf("delivered payload = %q, want hello", got)
		}
		if err := tran.Ack(t.Context(), nil); err != nil {
			t.Fatal(err)
		}
		if err := <-ack; err != nil {
			t.Fatal(err)
		}
		// The input remains open: hard stop must release both merge workers.
		p.TriggerCloseNow()
		if err := p.WaitForClose(t.Context()); err != nil {
			t.Fatal(err)
		}
	})
}

type cleanupProcessor struct {
	release  chan struct{}
	closeErr error
	closes   atomic.Int32
}

func (*cleanupProcessor) ProcessBatch(_ context.Context, b message.Batch) ([]message.Batch, error) {
	return []message.Batch{b}, nil
}
func (p *cleanupProcessor) Close(context.Context) error {
	p.closes.Add(1)
	if p.release != nil {
		<-p.release
	}
	return p.closeErr
}
