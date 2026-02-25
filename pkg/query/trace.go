package query

import (
	"fmt"
	"io"
	"strings"
)

// TraceLogger is used for debugging iterator execution
type TraceLogger struct {
	traces []string
	depth  int
	stack  []Iterator // Stack of iterator pointers for proper indentation context
	writer io.Writer  // Optional writer to output traces in real-time
}

// NewTraceLogger creates a new trace logger
func NewTraceLogger() *TraceLogger {
	return &TraceLogger{
		traces: make([]string, 0),
		depth:  0,
		stack:  make([]Iterator, 0),
		writer: nil,
	}
}

// NewTraceLoggerWithWriter creates a new trace logger with an optional writer
// for real-time trace output
func NewTraceLoggerWithWriter(w io.Writer) *TraceLogger {
	return &TraceLogger{
		traces: make([]string, 0),
		depth:  0,
		stack:  make([]Iterator, 0),
		writer: w,
	}
}

// appendTrace appends a trace line to the traces slice and optionally writes it
// to the writer if one is configured
func (t *TraceLogger) appendTrace(line string) {
	t.traces = append(t.traces, line)
	if t.writer != nil {
		fmt.Fprintln(t.writer, line)
	}
}

// iteratorIDPrefix generates a formatted prefix string for an iterator with its ID
func iteratorIDPrefix(it Iterator) string {
	explain := it.Explain()
	iteratorName := explain.Name
	if iteratorName == "" {
		iteratorName = explain.Info
	}
	return fmt.Sprintf(" %s[%x]", iteratorName, it.CanonicalKey().Hash())
}

// EnterIterator logs entering an iterator and pushes it onto the stack
func (t *TraceLogger) EnterIterator(it Iterator, traceString string) {
	indent := strings.Repeat("  ", t.depth)
	idPrefix := iteratorIDPrefix(it)

	t.appendTrace(fmt.Sprintf("%s-> %s: %s",
		indent,
		idPrefix,
		traceString,
	))
	t.depth++
	t.stack = append(t.stack, it) // Push iterator pointer onto stack
}

func subjectRelationTraceString(subject ObjectAndRelation) string {
	var subjectRelation string
	if subject.Relation != "" {
		subjectRelation = "#" + subject.Relation
	}
	return subjectRelation
}

func checkTraceString(resources []Object, subject ObjectAndRelation) string {
	resourceStrs := make([]string, len(resources))
	for i, r := range resources {
		resourceStrs[i] = fmt.Sprintf("%s:%s", r.ObjectType, r.ObjectID)
	}
	return fmt.Sprintf("check(%s, %s:%s%s)", strings.Join(resourceStrs, ", "), subject.ObjectType, subject.ObjectID, subjectRelationTraceString(subject))
}

func iterResourcesTraceString(subject ObjectAndRelation, filter ObjectType) string {
	filterStr := ""
	if filter.Type != "" {
		filterStr = ", filter=" + filter.Type
		if filter.Subrelation != "" {
			filterStr += "#" + filter.Subrelation
		}
	}
	return fmt.Sprintf("iterResources(%s:%s%s%s)", subject.ObjectType, subject.ObjectID, subjectRelationTraceString(subject), filterStr)
}

func iterSubjectsTraceString(resource Object, filter ObjectType) string {
	filterStr := ""
	if filter.Type != "" {
		filterStr = ", filter=" + filter.Type
		if filter.Subrelation != "" {
			filterStr += "#" + filter.Subrelation
		}
	}
	return fmt.Sprintf("iterSubjects(%s:%s%s)", resource.ObjectType, resource.ObjectID, filterStr)
}

// ExitIterator logs exiting an iterator and pops it from the stack
func (t *TraceLogger) ExitIterator(it Iterator, paths []Path) {
	// Pop from stack to maintain stack consistency
	if len(t.stack) > 0 {
		t.stack = t.stack[:len(t.stack)-1]
	}
	t.depth--

	indent := strings.Repeat("  ", t.depth)
	idPrefix := iteratorIDPrefix(it)

	pathStrs := make([]string, len(paths))
	for i, p := range paths {
		caveatInfo := ""
		if p.Caveat != nil {
			if p.Caveat.GetCaveat() != nil {
				caveatInfo = fmt.Sprintf("[%s]", p.Caveat.GetCaveat().CaveatName)
			} else {
				caveatInfo = "[complex_caveat]"
			}
		}
		pathStrs[i] = fmt.Sprintf("%s:%s#%s@%s:%s%s",
			p.Resource.ObjectType, p.Resource.ObjectID, p.Relation,
			p.Subject.ObjectType, p.Subject.ObjectID, caveatInfo)
	}
	t.appendTrace(fmt.Sprintf("%s<- %s: returned %d paths: [%s]",
		indent, idPrefix, len(paths), strings.Join(pathStrs, ", ")))
}

// LogStep logs an intermediate step within an iterator, using the iterator pointer to find the correct indentation level
func (t *TraceLogger) LogStep(it Iterator, step string, data ...any) {
	// Find the iterator's position in the stack by comparing pointers
	indentLevel := 0
	found := false

	for i, stackIt := range t.stack {
		if stackIt == it {
			indentLevel = i + 1 // +1 because we want to be indented inside the iterator
			found = true
			break
		}
	}

	if !found {
		// Iterator not found in stack, use current depth
		indentLevel = t.depth
	}

	indent := strings.Repeat("  ", indentLevel)
	idPrefix := iteratorIDPrefix(it)
	message := fmt.Sprintf(step, data...)
	t.appendTrace(fmt.Sprintf("%s   %s: %s", indent, idPrefix, message))
}

// DumpTrace returns all traces as a string
func (t *TraceLogger) DumpTrace() string {
	return strings.Join(t.traces, "\n")
}
