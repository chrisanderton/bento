package aws

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
)

func TestCloudWatchConcurrentCloseJoinsFinalFlush(t *testing.T) {
	client := &cleanupCloudWatchClient{entered: make(chan struct{}), release: make(chan struct{})}
	cw := cwmMock(client)
	cw.ctx, cw.cancel = context.WithCancel(t.Context())
	t.Cleanup(cw.cancel)
	var release sync.Once
	unblock := func() { release.Do(func() { close(client.release) }) }
	t.Cleanup(unblock)
	cw.NewCounterCtor("probe")().Incr(1)
	first := make(chan error, 1)
	go func() { first <- cw.Close(t.Context()) }()
	<-client.entered
	second := make(chan error, 1)
	go func() { second <- cw.Close(t.Context()) }()
	select {
	case err := <-second:
		t.Errorf("Second Close before final flush finished = %v, want joined first Close", err)
		unblock()
		if err := <-first; err != nil {
			t.Errorf("First Close = %v, want successful final flush", err)
		}
		return
	case <-time.After(50 * time.Millisecond):
	}
	unblock()
	if err := <-first; err != nil {
		t.Errorf("First Close = %v, want nil", err)
	}
	if err := <-second; err != nil {
		t.Errorf("Second Close = %v, want nil", err)
	}
	if err := cw.Close(t.Context()); err != nil {
		t.Errorf("Repeated Close = %v, want nil", err)
	}
	if got := client.calls.Load(); got != 1 {
		t.Errorf("Final uploads = %d, want exactly one", got)
	}
}

func TestCloudWatchCloseRetainsFinalFlushError(t *testing.T) {
	want := context.Canceled
	client := &cleanupCloudWatchClient{entered: make(chan struct{}), release: make(chan struct{}), err: want}
	close(client.release)
	cw := cwmMock(client)
	cw.ctx, cw.cancel = context.WithCancel(t.Context())
	t.Cleanup(cw.cancel)
	cw.NewCounterCtor("probe")().Incr(1)
	// CloudWatch logs ordinary upload errors but returns cancellation errors.
	// Preserve that existing distinction rather than changing exporter policy.
	cw.cancel()
	for range 2 {
		if err := cw.Close(t.Context()); !errors.Is(err, want) {
			t.Errorf("Close(failed final upload) = %v, want retained upload error", err)
		}
	}
}

type cleanupCloudWatchClient struct {
	entered chan struct{}
	release chan struct{}
	calls   atomic.Int32
	err     error
}

func (c *cleanupCloudWatchClient) PutMetricData(ctx context.Context, _ *cloudwatch.PutMetricDataInput, _ ...func(*cloudwatch.Options)) (*cloudwatch.PutMetricDataOutput, error) {
	if c.calls.Add(1) == 1 {
		close(c.entered)
	}
	select {
	case <-c.release:
		return nil, c.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
