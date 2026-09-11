package service_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/warpstreamlabs/bento/public/service"
)

// Test each supported placement of an authored processor list. The second
// constructor fails after the first succeeds; Run/Stop must retain the first.
func TestProcessorConstructionFailureRetainsEarlierResources(t *testing.T) {
	for _, tc := range []struct{ name, yaml string }{
		{"pipeline", "input: {probe_input: {}}\npipeline: {processors: [{counted_cleanup: {}}, {failed_cleanup: {}}]}\noutput: {probe_output: {}}\n"},
		{"input", "input: {probe_input: {}, processors: [{counted_cleanup: {}}, {failed_cleanup: {}}]}\noutput: {probe_output: {}}\n"},
		{"output", "input: {probe_input: {}}\noutput: {probe_output: {}, processors: [{counted_cleanup: {}}, {failed_cleanup: {}}]}\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				env := service.NewEnvironment()
				input := &probeInput{release: make(chan struct{}), closed: make(chan struct{})}
				processor := &countedCleanupProcessor{}
				output := &countedCleanupOutput{}
				var processorBuilt, outputBuilt bool
				t.Cleanup(func() {
					// Release the input's read loop even if construction lost its owner.
					// This is fixture cleanup, not credited to Run/Stop.
					close(input.release)
					synctest.Wait()
				})
				if err := env.RegisterInput("probe_input", service.NewConfigSpec(), func(*service.ParsedConfig, *service.Resources) (service.Input, error) {
					return input, nil
				}); err != nil {
					t.Fatal(err)
				}
				if err := env.RegisterOutput("probe_output", service.NewConfigSpec(), func(*service.ParsedConfig, *service.Resources) (service.Output, int, error) {
					outputBuilt = true
					return output, 1, nil
				}); err != nil {
					t.Fatal(err)
				}
				if err := env.RegisterProcessor("counted_cleanup", service.NewConfigSpec(), func(*service.ParsedConfig, *service.Resources) (service.Processor, error) {
					processorBuilt = true
					return processor, nil
				}); err != nil {
					t.Fatal(err)
				}
				if err := env.RegisterProcessor("failed_cleanup", service.NewConfigSpec(), func(*service.ParsedConfig, *service.Resources) (service.Processor, error) {
					return nil, errors.New("second processor construction failed")
				}); err != nil {
					t.Fatal(err)
				}
				builder := env.NewStreamBuilder()
				builder.SetLogger(slog.New(slog.NewTextHandler(io.Discard, nil)))
				if err := builder.SetYAML("http: {enabled: false}\n" + tc.yaml); err != nil {
					t.Fatal(err)
				}
				stream, err := builder.Build()
				if err != nil {
					t.Fatal(err)
				}
				if err := stream.Run(context.Background()); err == nil {
					t.Fatal("Run(failed processor construction) = nil, want construction failure")
				}
				if err := stream.Stop(context.Background()); err != nil {
					t.Errorf("Stop(after failed construction) = %v, want joined cleanup", err)
				}
				synctest.Wait()
				if !processorBuilt {
					t.Fatal("first processor was not constructed; invalid proof")
				}
				if got := processor.closes.Load(); got != 1 {
					t.Errorf("first processor Close calls = %d after %s construction failure, want 1", got, tc.name)
				}
				if got := input.closeCalls.Load(); got != 1 {
					t.Errorf("input Close calls = %d after %s construction failure, want 1", got, tc.name)
				}
				if outputBuilt {
					if got := output.closes.Load(); got != 1 {
						t.Errorf("output Close calls = %d after %s construction failure, want 1", got, tc.name)
					}
				}
			})
		})
	}
}

type countedCleanupProcessor struct {
	noopProc
	closes atomic.Int64
}

func (p *countedCleanupProcessor) Close(context.Context) error {
	p.closes.Add(1)
	return nil
}

type countedCleanupOutput struct {
	probeOutput
	closes atomic.Int64
}

func (o *countedCleanupOutput) Close(context.Context) error {
	o.closes.Add(1)
	return nil
}
