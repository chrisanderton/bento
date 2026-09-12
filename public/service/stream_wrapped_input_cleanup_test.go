package service_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"testing/synctest"

	_ "github.com/warpstreamlabs/bento/public/components/io"
	_ "github.com/warpstreamlabs/bento/public/components/pure"
	"github.com/warpstreamlabs/bento/public/service"
)

// The source Close is deliberately held open. No broker or socket is used.
// Run must retain ownership until that Close completes, even with input processors.
func TestFailedConstructionJoinsWrappedInput(t *testing.T) {
	testWrappedInputCleanup(t, true)
}

func TestNormalShutdownJoinsWrappedInput(t *testing.T) {
	testWrappedInputCleanup(t, false)
}

func testWrappedInputCleanup(t *testing.T, failOutput bool) {
	t.Helper()
	testNativeInputCleanup(t, "input:\n  gated_input: {}\n  processors:\n    - mapping: 'root = this'\n", failOutput)
}

func TestNativeInputCleanup(t *testing.T) {
	for _, tc := range []struct{ name, config string }{
		{"broker", "input: {broker: {inputs: [{gated_input: {}}, {generate: {mapping: 'root = {}', count: 1}}]}}\n"},
		{"sequence", "input: {sequence: {inputs: [{gated_input: {}}]}}\n"},
		{"dynamic", "input: {dynamic: {inputs: {source: {gated_input: {}}}}}\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Run("failed_construction", func(t *testing.T) { testNativeInputCleanup(t, tc.config, true) })
			t.Run("normal_stop", func(t *testing.T) { testNativeInputCleanup(t, tc.config, false) })
		})
	}
}

func testNativeInputCleanup(t *testing.T, inputConfig string, failOutput bool) {
	t.Helper()
	synctest.Test(t, func(t *testing.T) {
		source := &gatedInput{entered: make(chan struct{}), release: make(chan struct{})}
		env := service.NewEnvironment()
		if err := env.RegisterInput("gated_input", service.NewConfigSpec(), func(*service.ParsedConfig, *service.Resources) (service.Input, error) {
			return source, nil
		}); err != nil {
			t.Fatal(err)
		}
		startupErr := errors.New("output construction failed")
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		if err := env.RegisterOutput("failed_output", service.NewConfigSpec(), func(*service.ParsedConfig, *service.Resources) (service.Output, int, error) {
			if failOutput {
				return nil, 1, startupErr
			}
			cancel() // The input is constructed before the successful output control cancels Run.
			return &probeOutput{}, 1, nil
		}); err != nil {
			t.Fatal(err)
		}
		builder := env.NewStreamBuilder()
		builder.SetLogger(slog.New(slog.NewTextHandler(io.Discard, nil)))
		if err := builder.SetYAML("http: {enabled: false}\n" + inputConfig + "output: {failed_output: {}}\n"); err != nil {
			t.Fatal(err)
		}
		stream, err := builder.Build()
		if err != nil {
			t.Fatal(err)
		}
		result := make(chan error, 1)
		go func() {
			err := stream.Run(ctx)
			if !failOutput {
				// Run cancellation only releases its caller; normal shutdown is
				// explicitly owned by Stop, as documented by the public API.
				err = errors.Join(err, stream.Stop(t.Context()))
			}
			result <- err
		}()
		<-source.entered
		synctest.Wait()
		returned := false
		select {
		case err := <-result:
			returned = true
			t.Errorf("Run/Stop(failOutput=%t) returned %v while input Close is still blocked", failOutput, err)
		default:
		}
		close(source.release) // Fixture release; never credited as runtime cleanup.
		synctest.Wait()
		if !returned {
			wantErr := error(context.Canceled)
			if failOutput {
				wantErr = startupErr
			}
			if err := <-result; !errors.Is(err, wantErr) {
				t.Errorf("Run(failOutput=%t) after joined cleanup = %v, want %v", failOutput, err, wantErr)
			}
		}
		if err := stream.Stop(t.Context()); err != nil {
			t.Errorf("Stop = %v", err)
		}
	})
}

type gatedInput struct{ entered, release chan struct{} }

func (*gatedInput) Connect(context.Context) error { return nil }
func (*gatedInput) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {
	<-ctx.Done()
	return nil, nil, ctx.Err()
}
func (i *gatedInput) Close(context.Context) error {
	close(i.entered)
	<-i.release
	return nil
}
