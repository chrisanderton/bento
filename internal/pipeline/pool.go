package pipeline

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/Jeffail/shutdown"

	"github.com/warpstreamlabs/bento/internal/component"
	"github.com/warpstreamlabs/bento/internal/component/processor"
	"github.com/warpstreamlabs/bento/internal/log"
	"github.com/warpstreamlabs/bento/internal/message"
)

// Pool is a pool of pipelines. Each pipeline reads from a shared transaction
// channel. Inputs remain coupled to their outputs as they propagate the
// response channel in the transaction.
type Pool struct {
	workers []processor.Pipeline

	log log.Modular

	messagesIn  <-chan message.Transaction
	messagesOut chan message.Transaction

	shutSig       *shutdown.Signaller
	lifecycleOnce sync.Once
	started       bool // Read after lifecycleOnce.Do has published the winning path.
	msgProcessors []processor.V1
	closeErr      error // Published before the stopped signal.
}

// NewPool creates a new processing pool.
func NewPool(threads int, log log.Modular, msgProcessors ...processor.V1) (*Pool, error) {
	if threads <= 0 {
		threads = runtime.NumCPU()
	}

	p := &Pool{
		msgProcessors: msgProcessors,
		workers:       make([]processor.Pipeline, threads),
		log:           log,
		messagesOut:   make(chan message.Transaction),
		shutSig:       shutdown.NewSignaller(),
	}

	for i := range p.workers {
		p.workers[i] = NewProcessor(msgProcessors...)
	}

	return p, nil
}

//------------------------------------------------------------------------------

// loop is the processing loop of this pipeline.
func (p *Pool) loop() {
	defer func() {
		// A hard stop cancels processing, not the join of worker-owned cleanup.
		var errs []error
		for _, c := range p.workers {
			if err := c.WaitForClose(context.Background()); err != nil {
				errs = append(errs, err)
			}
		}
		p.closeErr = errors.Join(errs...)

		close(p.messagesOut)
		p.shutSig.TriggerHasStopped()
	}()

	internalMessages := make(chan message.Transaction)
	remainingWorkers := int64(len(p.workers))

	var closeInternalOnce sync.Once

	for _, worker := range p.workers {
		if err := worker.Consume(p.messagesIn); err != nil {
			p.log.Error("Failed to start pipeline worker: %v\n", err)
			atomic.AddInt64(&remainingWorkers, -1)
			continue
		}
		go func(w processor.Pipeline) {
			defer func() {
				if v := atomic.AddInt64(&remainingWorkers, -1); v <= 0 {
					closeInternalOnce.Do(func() {
						close(internalMessages)
					})
				}
			}()
			for {
				var t message.Transaction
				var open bool
				select {
				case t, open = <-w.TransactionChan():
					if !open {
						return
					}
				case <-p.shutSig.HardStopChan():
					return
				}
				select {
				case internalMessages <- t:
				case <-p.shutSig.HardStopChan():
					return
				}
			}
		}(worker)
	}

	for atomic.LoadInt64(&remainingWorkers) > 0 {
		select {
		case t, open := <-internalMessages:
			if !open {
				return
			}
			select {
			case p.messagesOut <- t:
			case <-p.shutSig.HardStopChan():
				return
			}
		case <-p.shutSig.HardStopChan():
			return
		}
	}
}

//------------------------------------------------------------------------------

// Consume assigns a messages channel for the pipeline to read.
func (p *Pool) Consume(msgs <-chan message.Transaction) error {
	started := false
	p.lifecycleOnce.Do(func() {
		started = true
		p.started = true
		p.messagesIn = msgs
		go p.loop()
	})
	if !started {
		return component.ErrAlreadyStarted
	}
	return nil
}

// TransactionChan returns the channel used for consuming messages from this
// pipeline.
func (p *Pool) TransactionChan() <-chan message.Transaction {
	return p.messagesOut
}

// TriggerCloseNow signals that the component should close immediately,
// messages in flight will be dropped.
func (p *Pool) TriggerCloseNow() {
	p.lifecycleOnce.Do(func() {
		p.shutSig.TriggerHardStop()
		go func() {
			// Workers share these processors. Before startup there are no worker
			// loops to join, so close the shared resources once, not per worker.
			p.closeErr = closeProcessors(p.msgProcessors)
			close(p.messagesOut)
			p.shutSig.TriggerHasStopped()
		}()
	})
	if !p.started {
		return
	}
	for _, w := range p.workers {
		w.TriggerCloseNow()
	}
	p.shutSig.TriggerHardStop()
}

// WaitForClose blocks until the component has closed down or the context is
// cancelled. Closing occurs either when the input transaction channel is
// closed and messages are flushed (and acked), or when CloseNowAsync is
// called.
func (p *Pool) WaitForClose(ctx context.Context) error {
	select {
	case <-p.shutSig.HasStoppedChan():
	case <-ctx.Done():
		return ctx.Err()
	}
	return p.closeErr
}
