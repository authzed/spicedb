package caveats

import "context"

// EvaluationEvent identifies work performed by the caveat evaluator. The hook
// is optional and intended for diagnostic runs, never latency measurements.
type (
	EvaluationEvent       struct{ Stage, Name string }
	evaluationObserverKey struct{}
)

func WithEvaluationObserver(ctx context.Context, observer func(EvaluationEvent)) context.Context {
	return context.WithValue(ctx, evaluationObserverKey{}, observer)
}

func observeEvaluation(ctx context.Context, stage, name string) {
	if observer, ok := ctx.Value(evaluationObserverKey{}).(func(EvaluationEvent)); ok {
		observer(EvaluationEvent{Stage: stage, Name: name})
	}
}
