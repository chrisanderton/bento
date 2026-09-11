package influxdb

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestInfluxCloseStopsPollingAndFlushesOnce(t *testing.T) {
	var writes atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, err := io.Copy(io.Discard, r.Body); err != nil {
			t.Errorf("Read metrics body: %v", err)
		}
		if r.URL.Path == "/write" {
			writes.Add(1)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(server.Close)
	i := fromYAML(t, "url: %s\ndb: probe\ninterval: 1h\nping_interval: 1h\n", server.URL)
	t.Cleanup(i.cancel) // Also clean the broken baseline during the red test.
	i.NewCounterCtor("probe")().Incr(1)
	var group sync.WaitGroup
	for range 2 {
		group.Go(func() {
			if err := i.Close(t.Context()); err != nil {
				t.Errorf("Concurrent Influx Close = %v, want nil", err)
			}
		})
	}
	group.Wait()
	if err := i.Close(t.Context()); err != nil {
		t.Errorf("Repeated Influx Close = %v, want nil", err)
	}
	if i.ctx.Err() == nil {
		t.Error("Close left the polling context active, want cancelled")
	}
	if got := writes.Load(); got != 1 {
		t.Errorf("Final writes = %d, want one", got)
	}
}

func TestInfluxCloseJoinsActivePoll(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	var first sync.Once
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/ping" {
			first.Do(func() { close(entered) })
			select {
			case <-release:
			case <-r.Context().Done():
			}
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(server.Close)
	i := fromYAML(t, "url: %s\ndb: probe\ninterval: 1h\nping_interval: 10ms\n", server.URL)
	t.Cleanup(i.cancel)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("Polling did not reach the test server")
	}
	done := make(chan error, 1)
	go func() { done <- i.Close(ctx) }()
	select {
	case err := <-done:
		t.Errorf("Close while poll blocked = %v, want joined poll", err)
		unblock()
		return
	case <-time.After(50 * time.Millisecond):
	}
	unblock()
	if err := <-done; err != nil {
		t.Errorf("Close after poll released = %v, want nil", err)
	}
}
