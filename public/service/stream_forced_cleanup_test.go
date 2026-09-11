package service_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"testing/synctest"
	"time"

	"github.com/warpstreamlabs/bento/public/service"
)

// Exercise the public registration wrapper, not a substitute Stop implementation.
func TestForcedStopRetainsOutputCleanup(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		output := &heldShutdownOutput{
			written: make(chan struct{}),
			closing: make(chan struct{}),
			release: make(chan struct{}),
			closed:  make(chan struct{}),
		}
		env := service.NewEnvironment()
		if err := env.RegisterOutput("held_shutdown", service.NewConfigSpec(), func(*service.ParsedConfig, *service.Resources) (service.Output, int, error) {
			return output, 1, nil
		}); err != nil {
			t.Fatalf("RegisterOutput(held_shutdown) = %v, want nil", err)
		}
		builder := env.NewStreamBuilder()
		builder.SetLogger(slog.New(slog.NewTextHandler(io.Discard, nil)))
		push, err := builder.AddProducerFunc()
		if err != nil {
			t.Fatalf("AddProducerFunc() = %v, want nil", err)
		}
		if err := builder.AddOutputYAML("held_shutdown: {}"); err != nil {
			t.Fatalf("AddOutputYAML(held_shutdown) = %v, want nil", err)
		}
		stream, err := builder.Build()
		if err != nil {
			t.Fatalf("Build(held shutdown) = %v, want nil", err)
		}
		runCtx, cancelRun := context.WithCancel(context.Background())
		defer cancelRun()
		runDone := make(chan error, 1)
		pushDone := make(chan error, 1)
		go func() { runDone <- stream.Run(runCtx) }()
		go func() { pushDone <- push(runCtx, service.NewMessage([]byte("held"))) }()
		released := false
		t.Cleanup(func() {
			if !released {
				close(output.release)
			}
			cancelRun()
			if err := stream.Stop(context.Background()); err != nil {
				t.Errorf("Stop(released output) = %v, want nil", err)
			}
			<-output.closed
			<-runDone  // Natural completion or caller cancellation is valid here.
			<-pushDone // Acknowledgement or caller cancellation is valid here.
		})
		<-output.written
		started := time.Now()
		stopCtx, cancelStop := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancelStop()
		err = stream.Stop(stopCtx)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("Stop(held output, 5s deadline) = %v, want DeadlineExceeded", err)
		}
		if elapsed := time.Since(started); elapsed != 5*time.Second {
			t.Errorf("Stop(held output) elapsed = %s, want 5s synthetic deadline", elapsed)
		}
		<-output.closing
		select {
		case <-output.closed:
			t.Error("Stop(held output) closed child, want child still held")
		default:
		}
		retryDone := make(chan struct{})
		var retryErr error // Published by closing retryDone.
		go func() {
			retryErr = stream.Stop(context.Background())
			close(retryDone)
		}()
		synctest.Wait()
		select {
		case <-retryDone:
			t.Errorf("Stop(saved handle, no deadline) = %v before child closed, want pending", retryErr)
		default:
		}
		close(output.release)
		released = true
		<-retryDone
		if retryErr != nil {
			t.Errorf("Stop(saved handle, released child) = %v, want nil", retryErr)
		}
		select {
		case <-output.closed:
		default:
			t.Error("Stop(saved handle, released child) returned before child closure")
		}
	})
}

type heldShutdownOutput struct {
	written chan struct{}
	closing chan struct{}
	release chan struct{}
	closed  chan struct{}
}

func (*heldShutdownOutput) Connect(context.Context) error { return nil }

func (o *heldShutdownOutput) Write(context.Context, *service.Message) error {
	close(o.written)
	return nil
}

func (o *heldShutdownOutput) Close(context.Context) error {
	close(o.closing)
	<-o.release // Test-owned resource; cancellation is not proof of release.
	close(o.closed)
	return nil
}
