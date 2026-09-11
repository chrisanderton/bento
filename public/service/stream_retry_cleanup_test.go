package service_test

import (
	"errors"
	"io"
	"log/slog"
	"testing"
	"testing/synctest"

	_ "github.com/warpstreamlabs/bento/public/components/pure"
	"github.com/warpstreamlabs/bento/public/service"
)

func TestConstructionFailureReturnsWithRetryPipeline(t *testing.T) {
	for _, retry := range []bool{false, true} {
		name := "default"
		if retry {
			name = "retry"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				env := service.NewEnvironment()
				startupErr := errors.New("output construction failed")
				if err := env.RegisterOutput("failed_output", service.NewConfigSpec(), func(*service.ParsedConfig, *service.Resources) (service.Output, int, error) {
					return nil, 1, startupErr
				}); err != nil {
					t.Fatal(err)
				}
				builder := env.NewStreamBuilder()
				builder.SetLogger(slog.New(slog.NewTextHandler(io.Discard, nil)))
				config := "http: {enabled: false}\ninput: {generate: {mapping: 'root = {}', count: 1}}\npipeline:\n  processors:\n    - mapping: 'root = this'\noutput: {failed_output: {}}\n"
				if retry {
					config += "error_handling: {strategy: retry}\n"
				}
				if err := builder.SetYAML(config); err != nil {
					t.Fatal(err)
				}
				stream, err := builder.Build()
				if err != nil {
					t.Fatal(err)
				}
				if err := stream.Run(t.Context()); !errors.Is(err, startupErr) {
					t.Errorf("Run(retry=%t) = %v, want constructor error", retry, err)
				}
				if err := stream.Stop(t.Context()); err != nil {
					t.Errorf("Stop(retry=%t) = %v", retry, err)
				}
			})
		})
	}
}
