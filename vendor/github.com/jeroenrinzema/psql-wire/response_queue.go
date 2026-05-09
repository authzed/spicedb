package wire

import (
	"context"
)

// ResponseEventKind represents the type of event in the ResponseQueue
type ResponseEventKind uint8

const (
	// ResponseParseComplete represents a ParseComplete ack
	ResponseParseComplete ResponseEventKind = iota + 1
	// ResponseBindComplete represents a BindComplete ack
	ResponseBindComplete
	// ResponseStmtDescribe represents a composite ParameterDescription + RowDescription
	// for a statement (from Describe Statement)
	ResponseStmtDescribe
	// ResponsePortalDescribe represents a RowDescription for a portal (from Describe Portal)
	ResponsePortalDescribe
	// ResponseExecute represents an Execute with its complete result set
	// (DataRows, CommandComplete)
	ResponseExecute
)

// ResponseEvent represents an event in the response stream
// Use the constructor functions (NewParseCompleteEvent, etc.) to create events
type ResponseEvent struct {
	Kind ResponseEventKind

	// For ResponseStmtDescribe: holds parameter OIDs and column definitions
	Parameters []uint32
	Columns    Columns

	// For ResponsePortalDescribe and ResponseExecute: format codes for result columns
	Formats []FormatCode

	// For ResponseExecute: tracks completion and results
	ResultChannel chan *QueuedDataWriter // channel to receive results
	Result        *QueuedDataWriter      // cached result once received
}

// NewParseCompleteEvent creates a ParseComplete response event
func NewParseCompleteEvent() *ResponseEvent {
	return &ResponseEvent{
		Kind: ResponseParseComplete,
	}
}

// NewBindCompleteEvent creates a BindComplete response event
func NewBindCompleteEvent() *ResponseEvent {
	return &ResponseEvent{
		Kind: ResponseBindComplete,
	}
}

// NewStmtDescribeEvent creates a statement Describe response event
func NewStmtDescribeEvent(parameters []uint32, columns Columns) *ResponseEvent {
	return &ResponseEvent{
		Kind:       ResponseStmtDescribe,
		Parameters: parameters,
		Columns:    columns,
	}
}

// NewPortalDescribeEvent creates a portal Describe response event
func NewPortalDescribeEvent(columns Columns, formats []FormatCode) *ResponseEvent {
	return &ResponseEvent{
		Kind:    ResponsePortalDescribe,
		Columns: columns,
		Formats: formats,
	}
}

// NewExecuteEvent creates an Execute response event
func NewExecuteEvent(resultChan chan *QueuedDataWriter, formats []FormatCode) *ResponseEvent {
	return &ResponseEvent{
		Kind:          ResponseExecute,
		ResultChannel: resultChan,
		Formats:       formats,
	}
}

// ResponseQueue maintains all events in arrival order for a cycle
type ResponseQueue struct {
	events []*ResponseEvent
}

// NewResponseQueue creates a new empty ResponseQueue
func NewResponseQueue() *ResponseQueue {
	return &ResponseQueue{
		events: make([]*ResponseEvent, 0),
	}
}

// Enqueue adds an event to the queue
func (q *ResponseQueue) Enqueue(event *ResponseEvent) {
	q.events = append(q.events, event)
}

// DrainSync drains all events, waiting for all results to be received
// It returns early if an error is encountered or the context is cancelled
// Only returns events up to but not including an error event
func (q *ResponseQueue) DrainSync(ctx context.Context) ([]*ResponseEvent, error) {
	processedEvents := make([]*ResponseEvent, 0, len(q.events))

	for _, event := range q.events {

		if event.Kind == ResponseExecute {
			if event.ResultChannel != nil {
				select {
				case res := <-event.ResultChannel:
					event.Result = res
					// Check if the result contains an error
					if res != nil && res.GetError() != nil {
						// Return events processed so far,not including the error event
						// Events after this one won't be sent on the wire
						return processedEvents, res.GetError()
					}
				case <-ctx.Done():
					// Context cancelled - return events processed up to this point
					// The current event doesn't have a result, but it's included
					// so the caller knows where processing stopped
					return processedEvents, ctx.Err()
				}
			}
		}

		processedEvents = append(processedEvents, event)
	}

	// All events processed successfully
	return processedEvents, nil
}

// DrainAll returns all events in arrival order and clears the queue
func (q *ResponseQueue) DrainAll() []*ResponseEvent {
	result := q.events
	q.events = make([]*ResponseEvent, 0)
	return result
}

// Clear resets the queue for a new cycle
func (q *ResponseQueue) Clear() {
	q.events = make([]*ResponseEvent, 0)
}

// Len returns the number of events in the queue
func (q *ResponseQueue) Len() int {
	return len(q.events)
}
