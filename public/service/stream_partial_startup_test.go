package service_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/warpstreamlabs/bento/public/components/pure" // Register core components used by the public builder.
	"github.com/warpstreamlabs/bento/public/service"
)

// Exercise the real public construction/Stop boundary, not a replacement engine.
// The probe input owns no sockets or goroutines. Its release signal lets Bento's
// reader wrapper finish after the observation, even if Bento loses ownership.
func TestPartialPipelineConstructionClosesInput(t *testing.T) {
	for _, failOutput := range []bool{false, true} {
		name := "valid_output"
		if failOutput {
			name = "failed_output"
		}
		t.Run(name, func(t *testing.T) {
			environment := service.NewEnvironment()
			input := &probeInput{release: make(chan struct{}), closed: make(chan struct{})}
			var constructed atomic.Bool
			t.Cleanup(func() {
				close(input.release)
				if !constructed.Load() {
					return
				}
				select {
				case <-input.closed:
				case <-time.After(5 * time.Second):
					t.Error("probe cleanup: input did not finish after explicit fixture release")
				}
			})
			if err := environment.RegisterInput("probe_input", service.NewConfigSpec(), func(*service.ParsedConfig, *service.Resources) (service.Input, error) {
				constructed.Store(true)
				return input, nil
			}); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			startupErr := errors.New("output constructor failed")
			if err := environment.RegisterOutput("probe_output", service.NewConfigSpec(), func(*service.ParsedConfig, *service.Resources) (service.Output, int, error) {
				if failOutput {
					return nil, 1, startupErr
				}
				cancel() // The valid control can finish Run without a timing race.
				return &probeOutput{}, 1, nil
			}); err != nil {
				t.Fatal(err)
			}
			builder := environment.NewStreamBuilder()
			builder.SetLogger(slog.New(slog.NewTextHandler(io.Discard, nil)))
			if err := builder.SetYAML("http: {enabled: false}\ninput: {probe_input: {}}\noutput: {probe_output: {}}\n"); err != nil {
				t.Fatal(err)
			}
			stream, err := builder.Build()
			if err != nil {
				t.Fatal(err)
			}
			runErr := stream.Run(ctx)
			wantErr := error(context.Canceled)
			if failOutput {
				wantErr = startupErr
			}
			if !errors.Is(runErr, wantErr) {
				t.Errorf("Run(%s) = %v, want cause %v", name, runErr, wantErr)
			}
			join, cancelJoin := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancelJoin()
			if err := stream.Stop(join); err != nil {
				t.Errorf("Stop(%s) = %v, want nil", name, err)
			}
			if !constructed.Load() {
				t.Fatal("Run did not construct input; partial-construction proof is invalid")
			}
			if got := input.closeCalls.Load(); got != 1 {
				t.Errorf("Stop(%s): input Close calls = %d, want 1 before returning", name, got)
			}
		})
	}
}

type probeInput struct {
	release    chan struct{}
	closed     chan struct{}
	closeCalls atomic.Int64
}

func (p *probeInput) Connect(context.Context) error { return nil }
func (p *probeInput) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {
	select {
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	case <-p.release:
		return nil, nil, service.ErrEndOfInput
	}
}
func (p *probeInput) Close(context.Context) error {
	if p.closeCalls.Add(1) == 1 {
		close(p.closed)
	}
	return nil
}

type probeOutput struct{}

func (p *probeOutput) Connect(context.Context) error                 { return nil }
func (p *probeOutput) Write(context.Context, *service.Message) error { return nil }
func (p *probeOutput) Close(context.Context) error                   { return nil }
