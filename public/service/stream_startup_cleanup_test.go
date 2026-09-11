package service_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/warpstreamlabs/bento/public/service"
)

func TestStreamFailedStartupClosesResources(t *testing.T) {
	startupErr := errors.New("input constructor failed")
	stream, resource, attempts := newStartupCleanupStream(t, startupErr)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	if err := stream.Run(ctx); !errors.Is(err, startupErr) {
		t.Errorf("Run(failed input constructor) = %v, want original constructor error", err)
	} else if _, joined := err.(interface{ Unwrap() []error }); joined {
		t.Errorf("Run(failed input constructor, successful cleanup) error type = %T, want the unjoined constructor error", err)
	}
	if !resource.closed.Load() {
		t.Error("Run(failed input constructor) returned without closing its built cache resource")
	}
	if err := stream.Stop(ctx); err != nil {
		t.Errorf("Stop(after failed Run) = %v, want joined cleanup", err)
	}
	if err := stream.Run(ctx); err == nil {
		t.Error("Run(after failed startup cleanup) = nil, want rejection of closed stream reuse")
	}
	if got := attempts.Load(); got != 1 {
		t.Errorf("Run(repeated after failed construction) constructor attempts = %d, want 1", got)
	}
}

func TestStreamStopBeforeRunPreservesResources(t *testing.T) {
	stream, resource, _ := newStartupCleanupStream(t, nil)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := stream.Stop(ctx); err == nil {
		t.Error("Stop(before Run) = nil, want existing pre-start rejection")
	}
	if resource.closed.Load() {
		t.Error("Stop(before Run) closed resource needed by subsequent Run")
	}
	if err := stream.Run(ctx); err != nil {
		t.Errorf("Run(valid pipeline after rejected Stop) = %v, want nil", err)
	}
	if !resource.closed.Load() {
		t.Error("Run(valid finite pipeline) returned without closing its cache")
	}
	if err := stream.Stop(ctx); err != nil {
		t.Errorf("Stop(after normal completion) = %v, want nil", err)
	}
}

func TestStreamFailedStartupCleanupCanBeJoinedLater(t *testing.T) {
	startupErr := errors.New("input constructor failed")
	stream, resource, _ := newStartupCleanupStream(t, startupErr)
	gate, release := context.WithCancel(t.Context())
	defer release()
	resource.beforeClose = func(ctx context.Context) error {
		select {
		case <-gate.Done():
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := stream.Run(ctx); !errors.Is(err, startupErr) {
		t.Errorf("Run(cancelled cleanup) = %v, want original startup error preserved", err)
	} else if joined, ok := err.(interface{ Unwrap() []error }); !ok {
		t.Errorf("Run(cancelled cleanup) error type = %T, want both startup and cleanup errors", err)
	} else if causes := joined.Unwrap(); len(causes) != 2 || causes[1] == nil || errors.Is(causes[1], startupErr) {
		t.Errorf("Run(cancelled cleanup) causes = %v, want startup error plus a distinct cleanup failure", causes)
	}
	if resource.closed.Load() {
		t.Error("Run(cancelled cleanup) unexpectedly joined gated cache")
	}
	release()
	join, cancelJoin := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancelJoin()
	if err := stream.Stop(join); err != nil {
		t.Errorf("Stop(after releasing incomplete cleanup) = %v, want nil", err)
	}
	if !resource.closed.Load() {
		t.Error("Stop(after releasing incomplete cleanup) returned before cache closed")
	}
}

func newStartupCleanupStream(t *testing.T, startupErr error) (*service.Stream, *startupCleanupCache, *atomic.Int32) {
	t.Helper()
	environment := service.NewEnvironment()
	resource := &startupCleanupCache{}
	attempts := &atomic.Int32{}
	var constructed bool
	if err := environment.RegisterCache("startup_cleanup_probe", service.NewConfigSpec(), func(*service.ParsedConfig, *service.Resources) (service.Cache, error) {
		constructed = true
		return resource, nil
	}); err != nil {
		t.Fatal(err)
	}
	input := "generate: {count: 1, interval: '', mapping: 'root = {}'}"
	if startupErr != nil {
		if err := environment.RegisterInput("startup_failure_probe", service.NewConfigSpec(), func(*service.ParsedConfig, *service.Resources) (service.Input, error) {
			attempts.Add(1)
			return nil, startupErr
		}); err != nil {
			t.Fatal(err)
		}
		input = "startup_failure_probe: {}"
	}
	builder := environment.NewStreamBuilder()
	builder.SetLogger(slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err := builder.SetYAML("http: {enabled: false}\ncache_resources: [{label: probe, startup_cleanup_probe: {}}]\ninput: {" + input + "}\noutput: {drop: {}}\n"); err != nil {
		t.Fatal(err)
	}
	stream, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	if !constructed {
		t.Fatal("Build did not construct cache; cleanup probe is invalid")
	}
	return stream, resource, attempts
}

// This marker owns no real resources, even when testing the broken lifecycle.
type startupCleanupCache struct {
	closed      atomic.Bool
	beforeClose func(context.Context) error
}

func (c *startupCleanupCache) Get(context.Context, string) ([]byte, error) {
	return nil, service.ErrKeyNotFound
}
func (c *startupCleanupCache) Set(context.Context, string, []byte, *time.Duration) error {
	return nil
}
func (c *startupCleanupCache) Add(context.Context, string, []byte, *time.Duration) error {
	return nil
}
func (c *startupCleanupCache) Delete(context.Context, string) error { return nil }
func (c *startupCleanupCache) Close(ctx context.Context) error {
	if c.beforeClose != nil {
		if err := c.beforeClose(ctx); err != nil {
			return err
		}
	}
	c.closed.Store(true)
	return nil
}
