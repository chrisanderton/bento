package service_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	_ "github.com/warpstreamlabs/bento/public/components/io"
	_ "github.com/warpstreamlabs/bento/public/components/pure"
	"github.com/warpstreamlabs/bento/public/service"
)

// A malformed output proxy URL is accepted as a string by structural validation.
// The built-in input has already registered its HTTP route when output creation
// fails. Shutdown must disable that route rather than leave the input alive.
func TestPartialPipelineFailureClosesHTTPInput(t *testing.T) {
	for _, threads := range []int{0, 1, 2} {
		t.Run(fmt.Sprintf("processor_threads_%d", threads), func(t *testing.T) {
			testPartialHTTPInputCleanup(t, threads)
		})
	}
}

func testPartialHTTPInputCleanup(t *testing.T, threads int) {
	t.Helper()
	mux := &partialHTTPMux{disabled: make(chan struct{})}
	builder := service.NewStreamBuilder()
	builder.SetLogger(slog.New(slog.NewTextHandler(io.Discard, nil)))
	builder.SetHTTPMux(mux)
	configuration := `
http: {enabled: false}
input:
  http_server:
    path: /ingest
    ws_path: ""
output:
  http_client:
    url: http://127.0.0.1:1/unused
    proxy_url: "://invalid"
`
	if threads > 0 {
		configuration += fmt.Sprintf("buffer: {memory: {}}\npipeline:\n  threads: %d\n  processors:\n    - mapping: 'root = this'\n", threads)
	}
	if err := builder.SetYAML(configuration); err != nil {
		t.Fatalf("SetYAML(malformed proxy URL) = %v, want structural acceptance", err)
	}
	stream, err := builder.Build()
	if err != nil {
		t.Fatalf("Build(malformed proxy URL) = %v, want acceptance before output construction", err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := stream.Run(ctx); err == nil {
		t.Fatal("Run(malformed proxy URL) = nil, want constructor failure")
	}
	if err := stream.Stop(ctx); err != nil {
		t.Errorf("Stop(failed output construction) = %v, want nil", err)
	}
	mux.mu.Lock()
	registered := mux.handler != nil
	mux.mu.Unlock()
	if !registered {
		t.Fatal("http_server input did not register /ingest; partial-construction proof is invalid")
	}
	select {
	case <-mux.disabled:
	case <-ctx.Done():
		t.Fatal("Stop(failed output construction): input route was never disabled")
	}
	mux.mu.Lock()
	handler := mux.handler
	mux.mu.Unlock()
	recorder := httptest.NewRecorder()
	handler(recorder, httptest.NewRequest(http.MethodPost, "/ingest", nil))
	if got := recorder.Code; got != http.StatusServiceUnavailable {
		t.Errorf("closed input route status = %d, want 503", got)
	}
}

// Bento registers a replacement handler during HTTP input shutdown. No listener
// is created: this fixture observes the public mux shutdown contract.
type partialHTTPMux struct {
	mu       sync.Mutex
	handler  http.HandlerFunc
	disabled chan struct{}
	once     sync.Once
}

func (m *partialHTTPMux) HandleFunc(path string, handler func(http.ResponseWriter, *http.Request)) {
	if path != "/ingest" {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.handler != nil {
		m.once.Do(func() { close(m.disabled) })
	}
	m.handler = handler
}
