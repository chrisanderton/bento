package stream_test

import (
	"context"
	"errors"
	"testing"

	"github.com/warpstreamlabs/bento/internal/component"
	"github.com/warpstreamlabs/bento/internal/component/input"
	"github.com/warpstreamlabs/bento/internal/component/output"
	"github.com/warpstreamlabs/bento/internal/component/processor"
	"github.com/warpstreamlabs/bento/internal/message"
)

func TestProcessorWrapperConstructionRollback(t *testing.T) {
	for _, kind := range []string{"input", "output"} {
		for _, failure := range []string{"factory", "consume"} {
			t.Run(kind+"/"+failure, func(t *testing.T) {
				buildErr := errors.New("construction failed")
				endpointErr := errors.New("endpoint cleanup failed")
				pipeErr := errors.New("pipeline cleanup failed")
				endpoint := &rollbackComponent{closeErr: endpointErr}
				pipe := &rollbackComponent{closeErr: pipeErr}
				factory := func() (processor.Pipeline, error) {
					if failure == "factory" {
						return nil, buildErr
					}
					return pipe, nil
				}
				var err error
				if kind == "input" {
					pipe.consumeErr = buildErr
					_, err = input.WrapWithPipeline(endpoint, factory)
				} else {
					endpoint.consumeErr = buildErr
					_, err = output.WrapWithPipeline(endpoint, factory)
				}
				if !errors.Is(err, buildErr) || !errors.Is(err, endpointErr) {
					t.Errorf("WrapWithPipeline(%s, %s) = %v, want construction and endpoint cleanup causes", kind, failure, err)
				}
				if endpoint.closes != 1 || endpoint.waits != 1 {
					t.Errorf("endpoint cleanup: closes=%d waits=%d, want 1 each", endpoint.closes, endpoint.waits)
				}
				if failure == "consume" {
					if !errors.Is(err, pipeErr) || pipe.closes != 1 || pipe.waits != 1 {
						t.Errorf("pipeline cleanup: error=%v closes=%d waits=%d, want pipeline cause and 1 each", err, pipe.closes, pipe.waits)
					}
				}
			})
		}
	}
}

type rollbackComponent struct {
	consumeErr, closeErr error
	closes, waits        int
}

func (c *rollbackComponent) Consume(<-chan message.Transaction) error       { return c.consumeErr }
func (c *rollbackComponent) TransactionChan() <-chan message.Transaction    { return nil }
func (c *rollbackComponent) ConnectionStatus() component.ConnectionStatuses { return nil }
func (c *rollbackComponent) TriggerStopConsuming()                          {}
func (c *rollbackComponent) TriggerCloseNow()                               { c.closes++ }
func (c *rollbackComponent) WaitForClose(context.Context) error {
	c.waits++
	return c.closeErr
}
