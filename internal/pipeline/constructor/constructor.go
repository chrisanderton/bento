package constructor

import (
	"context"
	"errors"
	"strconv"

	"github.com/warpstreamlabs/bento/internal/bundle"
	"github.com/warpstreamlabs/bento/internal/component/processor"
	"github.com/warpstreamlabs/bento/internal/pipeline"
)

// New creates an input type based on an input configuration.
func New(conf pipeline.Config, mgr bundle.NewManagement) (processor.Pipeline, error) {
	processors := make([]processor.V1, len(conf.Processors))
	for j, procConf := range conf.Processors {
		var err error
		pMgr := mgr.IntoPath("processors", strconv.Itoa(j))
		processors[j], err = pMgr.NewProcessor(procConf)
		if err != nil {
			// Reuse pre-start cleanup without executing any messages.
			partial := pipeline.NewProcessor(processors[:j]...)
			partial.TriggerCloseNow()
			return nil, errors.Join(err, partial.WaitForClose(context.Background()))
		}
	}
	if conf.Threads == 1 {
		return pipeline.NewProcessor(processors...), nil
	}
	return pipeline.NewPool(conf.Threads, mgr.Logger(), processors...)
}
