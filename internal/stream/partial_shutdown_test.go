package stream_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/warpstreamlabs/bento/internal/component"
	"github.com/warpstreamlabs/bento/internal/component/processor"
	"github.com/warpstreamlabs/bento/internal/component/testutil"
	"github.com/warpstreamlabs/bento/internal/manager"
	"github.com/warpstreamlabs/bento/internal/message"
	"github.com/warpstreamlabs/bento/internal/pipeline"

	_ "github.com/warpstreamlabs/bento/public/components/pure"
)

func TestConstructedLayersCloseBeforeConsume(t *testing.T) {
	conf, err := testutil.StreamFromYAML("buffer: {memory: {}}")
	if err != nil {
		t.Fatal(err)
	}
	mgr, err := manager.New(manager.NewResourceConfig())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		mgr.TriggerStopConsuming()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := mgr.WaitForClose(ctx); err != nil {
			t.Errorf("fixture manager cleanup = %v, want nil", err)
		}
	})
	memory, err := mgr.NewBuffer(conf.Buffer)
	if err != nil {
		t.Fatal(err)
	}
	pool, err := pipeline.NewPool(2, mgr.Logger())
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name  string
		layer processor.Pipeline
	}{
		{name: "memory_buffer", layer: memory},
		{name: "processor_pipeline", layer: pipeline.NewProcessor()},
		{name: "processor_pool", layer: pool},
	} {
		t.Run(tc.name, func(t *testing.T) {
			joined := false
			// This explicit fixture release is NOT credited as component cleanup.
			// It lets the old implementation terminate after the failed assertion.
			t.Cleanup(func() {
				if joined {
					return
				}
				messages := make(chan message.Transaction)
				close(messages)
				if err := tc.layer.Consume(messages); err != nil && !errors.Is(err, component.ErrAlreadyStarted) {
					t.Errorf("fixture Consume(closed channel) = %v, want nil", err)
				}
				join, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				if err := tc.layer.WaitForClose(join); err != nil {
					t.Errorf("fixture release WaitForClose = %v, want nil", err)
				}
			})
			tc.layer.TriggerCloseNow()
			ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
			defer cancel()
			if err := tc.layer.WaitForClose(ctx); err != nil {
				t.Errorf("WaitForClose(%s, before Consume) = %v, want completed shutdown", tc.name, err)
				return
			}
			joined = true
			if err := tc.layer.Consume(make(chan message.Transaction)); !errors.Is(err, component.ErrAlreadyStarted) {
				t.Errorf("Consume(%s, after close) = %v, want rejection", tc.name, err)
			}
		})
	}
}
