package pure

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"

	"github.com/warpstreamlabs/bento/internal/component/input"
	"github.com/warpstreamlabs/bento/internal/manager/mock"
	"github.com/warpstreamlabs/bento/internal/message"
)

func TestBrokerWaitPreservesAllChildErrors(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		firstErr, secondErr := errors.New("first cleanup"), errors.New("second cleanup")
		a := &cleanupInput{Input: mock.Input{TChan: make(chan message.Transaction)}, err: firstErr}
		b := &cleanupInput{Input: mock.Input{TChan: make(chan message.Transaction)}, err: secondErr}
		p, err := newFanInInputBroker([]input.Streamed{a, b})
		if err != nil {
			t.Fatal(err)
		}
		p.TriggerCloseNow()
		for range 2 {
			err := p.WaitForClose(t.Context())
			if !errors.Is(err, firstErr) || !errors.Is(err, secondErr) {
				t.Errorf("WaitForClose = %v, want both child errors", err)
			}
		}
		if a.waits != 2 || b.waits != 2 {
			t.Errorf("child wait counts = %d, %d, want 2, 2", a.waits, b.waits)
		}
	})
}

type cleanupInput struct {
	mock.Input
	err   error
	waits int
}

func (i *cleanupInput) WaitForClose(context.Context) error { i.waits++; return i.err }
