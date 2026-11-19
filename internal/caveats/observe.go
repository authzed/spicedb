package caveats

import (
	"context"

	"github.com/authzed/ctxkey"
)

// EvaluationEvent identifies work performed by the caveat evaluator. The hook
// is optional and intended for diagnostic runs, never latency measurements.
type EvaluationEvent struct{ Stage, Name string }

var evaluationObserverKey = ctxkey.New[func(EvaluationEvent)]()

func WithEvaluationObserver(ctx context.Context, observer func(EvaluationEvent)) context.Context {
	return evaluationObserverKey.Set(ctx, observer)
}

func observeEvaluation(ctx context.Context, stage, name string) {
	if observer, ok := evaluationObserverKey.Value(ctx); ok {
		observer(EvaluationEvent{Stage: stage, Name: name})
	}
}
