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
	return func(c *Context) { c.executionObservers = append(c.executionObservers, o) }
}

// WithExecutionClock sets the clock used to timestamp invocation observations.
func WithExecutionClock(now func() time.Time) ContextOption {
	return func(c *Context) { c.executionNow = now }
}

func (c *Context) executionTime() time.Time {
	if c.executionNow != nil {
		return c.executionNow()
	}

	return time.Now()
}

func (c *Context) executionStart(it Iterator, op Operation, filter, mode string) *ExecutionObservation {
	if len(c.executionObservers) == 0 {
		return nil
	}

	c.executionInit.Do(func() {
		descriptors, err := ExecutionDescriptors(it)
		if err == nil {
			for node, d := range descriptors {
				c.executionDescriptors.Store(node, d)
			}
		}
	})
	d, _ := c.executionDescriptors.Load(it)
	facts, _ := d.(ExecutionDescriptor)
	strategy := facts.Strategy
	if strategy == "" {
		strategy = "default"
	}

	if c.BatchedArrows {
		strategy += "/batched"
	}

	return &ExecutionObservation{
		Invocation: c.executionSequence.Add(1),
		Key: signal.ExecutionKey{
			Identity:     facts.Identity,
			Operation:    SignalOperation(op),
			Filter:       filter,
			Mode:         mode,
			Strategy:     strategy,
			Role:         facts.Role,
			Dependencies: signal.EncodeFootprint(facts.Footprint),
		},
		Started: c.executionTime(),
	}
}

func (c *Context) executionFinish(e *ExecutionObservation, err error) {
	if e == nil {
		return
	}

	if err != nil {
		e.Status = signal.Errored
	}

	if c.Err() != nil {
		e.Status = signal.Canceled
	}

	e.Finished = c.executionTime()

	for _, o := range c.executionObservers {
		o.ObserveExecution(*e)
	}
}

func (c *Context) executionCheck(e *ExecutionObservation, p *Path, err error) {
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

	c.executionFinish(e, err)
}

func (c *Context) executionSequenceWrap(e *ExecutionObservation, seq PathSeq) PathSeq {
	if e == nil {
		return seq
	}

	return func(yield func(*Path, error) bool) {
		event := *e
		event.Invocation = c.executionSequence.Add(1)
		var terminalErr error
		defer func() { c.executionFinish(&event, terminalErr) }()

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
func (c *Context) ObserveImplCheck(it Iterator, resource Object, subject ObjectAndRelation) (*Path, error) {
	return c.observeImplCheck(it, resource, subject, "single")
}

func (c *Context) observeImplCheck(it Iterator, resource Object, subject ObjectAndRelation, mode string) (*Path, error) {
	e := c.executionStart(it, OperationCheck, CheckFilter(resource, subject), mode)
	p, err := it.CheckImpl(c, resource, subject)
	c.executionCheck(e, p, err)
	return p, err
}

// ObserveImplSubjects observes a receiver root's subject enumeration without re-entering dispatch.
func (c *Context) ObserveImplSubjects(it Iterator, resource Object, filter ObjectType) (PathSeq, error) {
	e := c.executionStart(it, OperationIterSubjects, filter.String(), "single")
	seq, err := it.IterSubjectsImpl(c, resource, filter)
	if err != nil {
		c.executionFinish(e, err)
		return nil, err
	}

	return c.executionSequenceWrap(e, seq), nil
}

// ObserveImplResources observes a receiver root's resource enumeration without re-entering dispatch.
func (c *Context) ObserveImplResources(it Iterator, subject ObjectAndRelation, filter ObjectType) (PathSeq, error) {
	e := c.executionStart(it, OperationIterResources, ResourceFilter(subject, filter), "single")
	seq, err := it.IterResourcesImpl(c, subject, filter)
	if err != nil {
		c.executionFinish(e, err)
		return nil, err
	}

	return c.executionSequenceWrap(e, seq), nil
}

// CheckFilter encodes resource and subject types and the subject relation for Check evidence.
func CheckFilter(resource Object, subject ObjectAndRelation) string {
	return fmt.Sprintf("%s|%s#%s", resource.ObjectType, subject.ObjectType, subject.Relation)
}

// ResourceFilter encodes subject and filter types and relations for resource-enumeration evidence.
func ResourceFilter(subject ObjectAndRelation, filter ObjectType) string {
	return subject.ObjectType + "#" + subject.Relation + "|" + filter.String()
}

func (c *Context) executionBatch(e *ExecutionObservation, paths []*Path, err error) {
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

	c.executionFinish(e, err)
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
func (c *Context) ObserveImplCheckManyResources(it Iterator, resource Object, subject ObjectAndRelation) (*Path, error) {
	return c.observeImplCheck(it, resource, subject, "many-resources")
}

// ObserveImplCheckManySubjects observes one attempted element inside a receiver batch.
func (c *Context) ObserveImplCheckManySubjects(it Iterator, resource Object, subject ObjectAndRelation) (*Path, error) {
	return c.observeImplCheck(it, resource, subject, "many-subjects")
}
