package service_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/warpstreamlabs/bento/public/service"
)

func TestForcedStopRetainsProcessorPoolCleanup(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		processor := &heldShutdownProcessor{closing: make(chan struct{}), release: make(chan struct{})}
		env := service.NewEnvironment()
		if err := env.RegisterProcessor("held_pool_cleanup", service.NewConfigSpec(), func(*service.ParsedConfig, *service.Resources) (service.Processor, error) {
			return processor, nil
		}); err != nil {
			t.Fatal(err)
		}
		builder := env.NewStreamBuilder()
		builder.SetLogger(slog.New(slog.NewTextHandler(io.Discard, nil)))
		builder.SetThreads(2)
		push, err := builder.AddProducerFunc()
		if err != nil {
			t.Fatal(err)
		}
		if err := builder.AddProcessorYAML("held_pool_cleanup: {}"); err != nil {
			t.Fatal(err)
		}
		if err := builder.AddConsumerFunc(func(context.Context, *service.Message) error { return nil }); err != nil {
			t.Fatal(err)
		}
		stream, err := builder.Build()
		if err != nil {
			t.Fatal(err)
		}
		runCtx, cancelRun := context.WithCancel(context.Background())
		defer cancelRun()
		runDone := make(chan error, 1)
		go func() { runDone <- stream.Run(runCtx) }()
		var releaseOnce sync.Once
		t.Cleanup(func() {
			releaseOnce.Do(func() { close(processor.release) })
			cancelRun()
			synctest.Wait()
			if err := stream.Stop(context.Background()); err != nil {
				t.Errorf("Stop(released processor) = %v, want nil", err)
			}
			<-runDone
		})
		if err := push(runCtx, service.NewMessage([]byte("processed"))); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		started := time.Now()
		err = stream.Stop(ctx)
		<-processor.closing
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("Stop(held processor pool, 5s deadline) = %v at %s, want deadline exceeded", err, time.Since(started))
		}
		joined := make(chan error, 1)
		go func() { joined <- stream.Stop(context.Background()) }()
		synctest.Wait()
		returned := false
		select {
		case err := <-joined:
			returned = true
			t.Errorf("Stop(saved handle, held processor pool) = %v, want pending", err)
		default:
		}
		releaseOnce.Do(func() { close(processor.release) })
		synctest.Wait()
		if !returned {
			if err := <-joined; err != nil {
				t.Errorf("Stop(released processor pool) = %v, want nil", err)
			}
		}
	})
}

type heldShutdownProcessor struct {
	closing, release chan struct{}
	closeOnce        sync.Once
}

func (*heldShutdownProcessor) Process(_ context.Context, msg *service.Message) (service.MessageBatch, error) {
	return service.MessageBatch{msg}, nil
}

func (p *heldShutdownProcessor) Close(context.Context) error {
	p.closeOnce.Do(func() { close(p.closing) })
	<-p.release
	return nil
}
