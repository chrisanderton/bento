package pipeline

import (
	"context"
	"errors"
	"sync"

	"github.com/Jeffail/shutdown"

	"github.com/warpstreamlabs/bento/internal/batch"
	"github.com/warpstreamlabs/bento/internal/component"
	"github.com/warpstreamlabs/bento/internal/component/processor"
	"github.com/warpstreamlabs/bento/internal/message"
)

// Processor is a pipeline that supports both Consumer and Producer interfaces.
// The processor will read from a source, perform some processing, and then
// either propagate a new message or drop it.
type Processor struct {
	msgProcessors []processor.V1

	messagesOut chan message.Transaction
	responsesIn chan error

	messagesIn <-chan message.Transaction

	shutSig       *shutdown.Signaller
	lifecycleOnce sync.Once
	closeErr      error // Published before the stopped signal.
}

// NewProcessor returns a new message processing pipeline.
func NewProcessor(msgProcessors ...processor.V1) *Processor {
	return &Processor{
		msgProcessors: msgProcessors,
		messagesOut:   make(chan message.Transaction),
		responsesIn:   make(chan error),
		shutSig:       shutdown.NewSignaller(),
	}
}

//------------------------------------------------------------------------------

// loop is the processing loop of this pipeline.
func (p *Processor) loop() {
	closeNowCtx, cnDone := p.shutSig.HardStopCtx(context.Background())
	defer cnDone()

	defer func() {
		p.closeErr = closeProcessors(p.msgProcessors)

		close(p.messagesOut)
		p.shutSig.TriggerHasStopped()
	}()

	var open bool
	for !p.shutSig.IsSoftStopSignalled() {
		var tran message.Transaction
		select {
		case tran, open = <-p.messagesIn:
			if !open {
				return
			}
		case <-p.shutSig.HardStopChan():
			return
		}

		sorter, sortBatch := message.NewSortGroup(tran.Payload)

		resultBatches, err := processor.ExecuteAll(closeNowCtx, p.msgProcessors, sortBatch)
		if len(resultBatches) == 0 || err != nil {
			if _ = tran.Ack(closeNowCtx, err); closeNowCtx.Err() != nil {
				return
			}
			continue
		}

		if len(resultBatches) == 1 {
			select {
			case p.messagesOut <- message.NewTransactionFunc(resultBatches[0], tran.Ack):
			case <-p.shutSig.HardStopChan():
				return
			}
			continue
		}

		var (
			errMut     sync.Mutex
			batchErr   *batch.Error
			generalErr error
			batchWG    sync.WaitGroup
		)

		for _, b := range resultBatches {
			var wgOnce sync.Once
			batchWG.Add(1)
			tmpBatch := b.ShallowCopy()

			select {
			case p.messagesOut <- message.NewTransactionFunc(tmpBatch, func(ctx context.Context, err error) error {
				if err != nil {
					errMut.Lock()
					defer errMut.Unlock()

					if batchErr == nil {
						batchErr = batch.NewError(sortBatch, err)
					}
					for _, m := range tmpBatch {
						if bIndex := sorter.GetIndex(m); bIndex >= 0 {
							batchErr.Failed(bIndex, err)
						} else {
							// We are unable to link this message with an origin
							// and therefore we must provide a general
							// batch-wide error instead.
							generalErr = err
						}
					}
				}

				wgOnce.Do(func() {
					batchWG.Done()
				})
				return nil
			}):
			case <-p.shutSig.HardStopChan():
				return
			}
		}

		batchWG.Wait()

		if generalErr != nil {
			_ = tran.Ack(closeNowCtx, generalErr)
		} else if batchErr != nil {
			_ = tran.Ack(closeNowCtx, batchErr)
		} else {
			_ = tran.Ack(closeNowCtx, nil)
		}
	}
}

//------------------------------------------------------------------------------

// Consume assigns a messages channel for the pipeline to read.
func (p *Processor) Consume(msgs <-chan message.Transaction) error {
	started := false
	p.lifecycleOnce.Do(func() {
		started = true
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
func (p *Processor) TransactionChan() <-chan message.Transaction {
	return p.messagesOut
}

// TriggerCloseNow signals that the processor pipeline should close immediately.
func (p *Processor) TriggerCloseNow() {
	p.shutSig.TriggerHardStop()
	p.lifecycleOnce.Do(func() {
		go func() {
			p.closeErr = closeProcessors(p.msgProcessors)
			close(p.messagesOut)
			p.shutSig.TriggerHasStopped()
		}()
	})
}

// closeProcessors joins resource cleanup independently of processing cancellation.
// Callers can bound WaitForClose without abandoning cleanup. A completed error
// must not prevent the remaining processors from closing.
func closeProcessors(processors []processor.V1) error {
	var errs []error
	for _, c := range processors {
		if err := c.Close(context.Background()); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// WaitForClose blocks until the component has closed down or the context is
// cancelled. Closing occurs either when the input transaction channel is closed
// and messages are flushed (and acked), or when CloseNowAsync is called.
func (p *Processor) WaitForClose(ctx context.Context) error {
	select {
	case <-p.shutSig.HasStoppedChan():
	case <-ctx.Done():
		return ctx.Err()
	}
	return p.closeErr
}
