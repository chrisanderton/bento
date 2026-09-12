package stream_test

import (
	"context"
	"errors"
	"testing"

	"github.com/warpstreamlabs/bento/internal/bundle"
	"github.com/warpstreamlabs/bento/internal/component"
	"github.com/warpstreamlabs/bento/internal/component/buffer"
	"github.com/warpstreamlabs/bento/internal/component/input"
	"github.com/warpstreamlabs/bento/internal/component/output"
	"github.com/warpstreamlabs/bento/internal/component/processor"
	"github.com/warpstreamlabs/bento/internal/manager/mock"
	"github.com/warpstreamlabs/bento/internal/message"
	"github.com/warpstreamlabs/bento/internal/pipeline"
	"github.com/warpstreamlabs/bento/internal/stream"
)

func TestConstructionFailureClosesEveryOwnedLayer(t *testing.T) {
	for _, stage := range []string{"input", "buffer", "pipeline", "output", "buffer_consume", "pipeline_consume", "output_consume"} {
		t.Run(stage, func(t *testing.T) {
			startupErr := errors.New("construction failed")
			cleanupErr := errors.New("cleanup failed")
			mgr := &failingConstructionManager{
				NewManagement: mock.NewManager(), stage: stage,
				startupErr: startupErr, cleanupErr: cleanupErr,
			}
			conf := stream.Config{}
			conf.Buffer.Type = "probe"
			conf.Pipeline.Processors = []processor.Config{{}}
			result, err := stream.New(conf, mgr)
			if result != nil || !errors.Is(err, startupErr) {
				t.Errorf("New(failed %s) = (%v, %v), want nil and original startup cause", stage, result, err)
			}
			if len(mgr.constructed) > 0 && !errors.Is(err, cleanupErr) {
				t.Errorf("New(failed %s) = %v, want cleanup cause retained too", stage, err)
			}
			for _, layer := range mgr.constructed {
				if layer.closeCalls != 1 || layer.waitCalls != 1 || !layer.allSignalled {
					t.Errorf("New(failed %s), owned %s: close=%d wait=%d all-signalled=%t; want 1, 1, true", stage, layer.name, layer.closeCalls, layer.waitCalls, layer.allSignalled)
				}
			}
		})
	}
}

// Only construction seams are replaced. stream.New owns the actual order,
// failure handling and cleanup. Each layer returns a cleanup error so an early
// return from joining one layer cannot accidentally pass this test.
type failingConstructionManager struct {
	bundle.NewManagement
	stage                  string
	startupErr, cleanupErr error
	constructed            []*constructionLayer
}

func (m *failingConstructionManager) IntoPath(...string) bundle.NewManagement { return m }
func (m *failingConstructionManager) layer(name string) (*constructionLayer, error) {
	if m.stage == name {
		return nil, m.startupErr
	}
	layer := &constructionLayer{name: name, owner: m}
	m.constructed = append(m.constructed, layer)
	return layer, nil
}
func (m *failingConstructionManager) NewInput(input.Config) (input.Streamed, error) {
	return m.layer("input")
}
func (m *failingConstructionManager) NewBuffer(buffer.Config) (buffer.Streamed, error) {
	return m.layer("buffer")
}
func (m *failingConstructionManager) NewPipeline(pipeline.Config) (processor.Pipeline, error) {
	return m.layer("pipeline")
}
func (m *failingConstructionManager) NewOutput(output.Config, ...processor.PipelineConstructorFunc) (output.Streamed, error) {
	return m.layer("output")
}

type constructionLayer struct {
	name                  string
	owner                 *failingConstructionManager
	closeCalls, waitCalls int
	allSignalled          bool
}

func (l *constructionLayer) Consume(<-chan message.Transaction) error {
	if l.owner.stage == l.name+"_consume" {
		return l.owner.startupErr
	}
	return nil
}
func (l *constructionLayer) TransactionChan() <-chan message.Transaction    { return nil }
func (l *constructionLayer) ConnectionStatus() component.ConnectionStatuses { return nil }
func (l *constructionLayer) TriggerStopConsuming()                          {}
func (l *constructionLayer) TriggerCloseNow()                               { l.closeCalls++ }
func (l *constructionLayer) WaitForClose(context.Context) error {
	l.waitCalls++
	l.allSignalled = true
	for _, owned := range l.owner.constructed {
		if owned.closeCalls != 1 {
			l.allSignalled = false
		}
	}
	return l.owner.cleanupErr
}
