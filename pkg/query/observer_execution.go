package query

import (
	"fmt"
	"time"

	"github.com/authzed/spicedb/pkg/query/signals/signal"
)

// ExecutionObservation is one invocation's terminal event. Count is paths, not entities.
type ExecutionObservation struct {
	// Invocation identifies an invocation within the query context.
	Invocation uint64

	// BatchSize is the attempted-element upper bound for a batch invocation.
	BatchSize uint64

	// Key identifies the observed execution population.
	Key signal.ExecutionKey

	// Status describes how the observed invocation ended.
	Status signal.Terminal

	// Count counts raw paths produced before API deduplication or projection.
	Count uint64

	// Denied counts completed checks with no matching path.
	Denied uint64

	// Granted counts completed checks with an unconditional matching path.
	Granted uint64

	// Conditional counts completed checks whose result depends on a caveat.
	Conditional uint64

	// Started is the invocation's original start time.
	Started time.Time

	// Finished is the invocation's terminal time.
	Finished time.Time
}

// ExecutionObserver receives completion-aware invocation observations.
type ExecutionObserver interface {
	// ObserveExecution receives one invocation's terminal observation.
	ObserveExecution(ExecutionObservation)
}

// WithExecutionObserver adds a receiver for terminal invocation observations.
func WithExecutionObserver(o ExecutionObserver) ContextOption {
	return func(ctx *Context) {
		if ctx.executionState == nil {
			ctx.executionState = &executionObservationState{}
		}

		ctx.executionState.observers = append(ctx.executionState.observers, o)
	}
}

func (ctx *Context) executionStart(it Iterator, op Operation, filter, mode string) *ExecutionObservation {
	if ctx.executionState == nil || len(ctx.executionState.observers) == 0 {
		return nil
	}

	ctx.executionState.initialize.Do(func() {
		descriptors, err := ExecutionDescriptors(it)
		if err == nil {
			for node, d := range descriptors {
				ctx.executionState.descriptors.Store(node, d)
			}
		}
	})
	d, _ := ctx.executionState.descriptors.Load(it)
	facts, _ := d.(ExecutionDescriptor)
	strategy := facts.Strategy
	if strategy == "" {
		strategy = "default"
	}

	if ctx.BatchedArrows {
		strategy += "/batched"
	}

	return &ExecutionObservation{
		Invocation: ctx.executionState.sequence.Add(1),
		Key: signal.ExecutionKey{
			Identity:     facts.Identity,
			Operation:    SignalOperation(op),
			Filter:       filter,
			Mode:         mode,
			Strategy:     strategy,
			Role:         facts.Role,
			Dependencies: signal.EncodeFootprint(facts.Footprint),
		},
		Started: time.Now(),
	}
}

func (ctx *Context) executionFinish(e *ExecutionObservation, err error) {
	if e == nil {
		return
	}

	if err != nil {
		e.Status = signal.Errored
	}

	if ctx.Err() != nil {
		e.Status = signal.Canceled
	}

	e.Finished = time.Now()

	ctx.executionState.callbacks.Lock()
	defer ctx.executionState.callbacks.Unlock()

	for _, o := range ctx.executionState.observers {
		o.ObserveExecution(*e)
	}
}

func (ctx *Context) executionCheck(e *ExecutionObservation, p *Path, err error) {
	if e == nil {
		return
	}

	if err == nil {
		switch {
		case p == nil:
			e.Denied++
		case p.Caveat != nil:
			e.Conditional++
		default:
			e.Granted++
		}

		if p != nil {
			e.Count++
		}
	}

	ctx.executionFinish(e, err)
}

func (ctx *Context) executionSequenceWrap(e *ExecutionObservation, seq PathSeq) PathSeq {
	if e == nil {
		return seq
	}

	return func(yield func(*Path, error) bool) {
		event := *e
		event.Invocation = ctx.executionState.sequence.Add(1)
		var terminalErr error
		defer func() { ctx.executionFinish(&event, terminalErr) }()

		for p, err := range seq {
			if err != nil {
				terminalErr = err
				yield(p, err)
				return
			}

			if p != nil {
				event.Count++
			}

			if !yield(p, err) {
				event.Status = signal.ConsumerStopped
				return
			}
		}
	}
}

// ObserveImplCheck wraps a receiver root without re-entering its dispatch executor.
func (ctx *Context) ObserveImplCheck(it Iterator, resource Object, subject ObjectAndRelation) (*Path, error) {
	return ctx.observeImplCheck(it, resource, subject, "single")
}

func (ctx *Context) observeImplCheck(it Iterator, resource Object, subject ObjectAndRelation, mode string) (*Path, error) {
	e := ctx.executionStart(it, OperationCheck, CheckFilter(resource, subject), mode)
	p, err := it.CheckImpl(ctx, resource, subject)
	ctx.executionCheck(e, p, err)
	return p, err
}

// ObserveImplSubjects observes a receiver root's subject enumeration without re-entering dispatch.
func (ctx *Context) ObserveImplSubjects(it Iterator, resource Object, filter ObjectType) (PathSeq, error) {
	e := ctx.executionStart(it, OperationIterSubjects, filter.String(), "single")
	seq, err := it.IterSubjectsImpl(ctx, resource, filter)
	if err != nil {
		ctx.executionFinish(e, err)
		return nil, err
	}

	return ctx.executionSequenceWrap(e, seq), nil
}

// ObserveImplResources observes a receiver root's resource enumeration without re-entering dispatch.
func (ctx *Context) ObserveImplResources(it Iterator, subject ObjectAndRelation, filter ObjectType) (PathSeq, error) {
	e := ctx.executionStart(it, OperationIterResources, ResourceFilter(subject, filter), "single")
	seq, err := it.IterResourcesImpl(ctx, subject, filter)
	if err != nil {
		ctx.executionFinish(e, err)
		return nil, err
	}

	return ctx.executionSequenceWrap(e, seq), nil
}

// CheckFilter encodes resource and subject types and the subject relation for Check evidence.
func CheckFilter(resource Object, subject ObjectAndRelation) string {
	return fmt.Sprintf("%s|%s#%s", resource.ObjectType, subject.ObjectType, subject.Relation)
}

// ResourceFilter encodes subject and filter types and relations for resource-enumeration evidence.
func ResourceFilter(subject ObjectAndRelation, filter ObjectType) string {
	return subject.ObjectType + "#" + subject.Relation + "|" + filter.String()
}

func (ctx *Context) executionBatch(e *ExecutionObservation, paths []*Path, err error) {
	if e == nil {
		return
	}

	if err == nil {
		for _, p := range paths {
			switch {
			case p == nil:
				e.Denied++
			case p.Caveat != nil:
				e.Conditional++
				e.Count++
			default:
				e.Granted++
				e.Count++
			}
		}
	}

	ctx.executionFinish(e, err)
}

// SignalOperation maps supported execution operations without narrowing unchecked values.
func SignalOperation(op Operation) signal.Operation {
	switch op {
	case OperationCheck:
		return signal.Check
	case OperationIterSubjects:
		return signal.IterSubjects
	case OperationIterResources:
		return signal.IterResources
	default:
		return 0
	}
}

// ObserveImplCheckManyResources observes one attempted element inside a receiver batch.
func (ctx *Context) ObserveImplCheckManyResources(it Iterator, resource Object, subject ObjectAndRelation) (*Path, error) {
	return ctx.observeImplCheck(it, resource, subject, "many-resources")
}

// ObserveImplCheckManySubjects observes one attempted element inside a receiver batch.
func (ctx *Context) ObserveImplCheckManySubjects(it Iterator, resource Object, subject ObjectAndRelation) (*Path, error) {
	return ctx.observeImplCheck(it, resource, subject, "many-subjects")
}

// ObserveImplCheckResourceBatch observes a receiver's resource batch without
// crossing the same dispatch boundary again. The mode defaults to AllCheckResults.
func (ctx *Context) ObserveImplCheckResourceBatch(
	it Iterator, resources []Object, subject ObjectAndRelation, modes ...CheckResultMode,
) ([]*Path, error) {
	if len(resources) == 0 {
		return nil, nil
	}

	if !ctx.checkExecution.BatchChecks && ctx.checkBatchMode(modes) == AllCheckResults {
		paths := make([]*Path, len(resources))

		for i, resource := range resources {
			path, err := ctx.ObserveImplCheckManyResources(it, resource, subject)
			if err != nil {
				return nil, err
			}

			paths[i] = path
		}

		return paths, nil
	}

	mode := batchObservationMode("many-resources", ctx.checkBatchMode(modes))
	event := ctx.executionStart(it, OperationCheck, CheckFilter(resources[0], subject), mode)
	if event != nil {
		event.BatchSize = uint64(len(resources))
	}

	paths, err := CheckManyResourcesImpl(ctx, it, resources, subject, modes...)
	ctx.executionCheckBatch(event, paths, err, ctx.checkBatchMode(modes))
	return paths, err
}

// ObserveImplCheckSubjectBatch observes a receiver's subject batch without
// crossing the same dispatch boundary again. The mode defaults to AllCheckResults.
func (ctx *Context) ObserveImplCheckSubjectBatch(
	it Iterator, resource Object, subjects []ObjectAndRelation, modes ...CheckResultMode,
) ([]*Path, error) {
	if len(subjects) == 0 {
		return nil, nil
	}

	if !ctx.checkExecution.BatchChecks && ctx.checkBatchMode(modes) == AllCheckResults {
		paths := make([]*Path, len(subjects))

		for i, subject := range subjects {
			path, err := ctx.ObserveImplCheckManySubjects(it, resource, subject)
			if err != nil {
				return nil, err
			}

			paths[i] = path
		}

		return paths, nil
	}

	mode := batchObservationMode("many-subjects", ctx.checkBatchMode(modes))
	event := ctx.executionStart(it, OperationCheck, CheckFilter(resource, subjects[0]), mode)
	if event != nil {
		event.BatchSize = uint64(len(subjects))
	}

	paths, err := CheckManySubjectsImpl(ctx, it, resource, subjects, modes...)
	ctx.executionCheckBatch(event, paths, err, ctx.checkBatchMode(modes))
	return paths, err
}

// executionCheckBatch keeps an early grant from manufacturing denials for skipped
// targets. A separate mode and incomplete terminal prevent complete-only signals
// from treating this partial population as an exhaustive batch observation.
func (ctx *Context) executionCheckBatch(e *ExecutionObservation, paths []*Path, err error, mode CheckResultMode) {
	if e != nil && err == nil && mode == AllowSingleResult && hasUnconditionalGrant(paths) {
		e.Status = signal.ConsumerStopped

		for _, path := range paths {
			if path == nil {
				continue
			}

			e.Count++

			if path.Caveat == nil {
				e.Granted++
			} else {
				e.Conditional++
			}
		}

		ctx.executionFinish(e, err)
		return
	}

	ctx.executionBatch(e, paths, err)
}

func batchObservationMode(dimension string, mode CheckResultMode) string {
	if mode == AllowSingleResult {
		return dimension + "/any"
	}

	return dimension
}
