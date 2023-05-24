package graph

import (
	"context"
	"fmt"

	"github.com/authzed/spicedb/internal/datasets"
	"github.com/authzed/spicedb/internal/dispatch"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

// lookupSubjectsUnion defines a reducer for union operations, where all the results from each stream
// for each branch are unioned together, filtered, limited and then published.
type lookupSubjectsUnion struct {
	parentStream dispatch.LookupSubjectsStream
	collectors   map[int]*dispatch.CollectingDispatchStream[*v1.DispatchLookupSubjectsResponse]
	ci           cursorInformation
}

func newLookupSubjectsUnion(parentStream dispatch.LookupSubjectsStream, ci cursorInformation) *lookupSubjectsUnion {
	return &lookupSubjectsUnion{
		parentStream: parentStream,
		collectors:   map[int]*dispatch.CollectingDispatchStream[*v1.DispatchLookupSubjectsResponse]{},
		ci:           ci,
	}
}

func (lsu *lookupSubjectsUnion) ForIndex(ctx context.Context, setOperationIndex int) dispatch.LookupSubjectsStream {
	collector := dispatch.NewCollectingDispatchStream[*v1.DispatchLookupSubjectsResponse](ctx)
	lsu.collectors[setOperationIndex] = collector
	return collector
}

func (lsu *lookupSubjectsUnion) CompletedChildOperations() error {
	foundSubjects := datasets.NewSubjectSetByResourceID()
	metadata := emptyMetadata

	for index := 0; index < len(lsu.collectors); index++ {
		collector, ok := lsu.collectors[index]
		if !ok {
			return fmt.Errorf("missing collector for index %d", index)
		}

		for _, result := range collector.Results() {
			metadata = combineResponseMetadata(metadata, result.Metadata)
			if err := foundSubjects.UnionWith(result.FoundSubjectsByResourceId); err != nil {
				return fmt.Errorf("failed to UnionWith under lookupSubjectsUnion: %w", err)
			}
		}
	}

	if foundSubjects.IsEmpty() {
		return nil
	}

	// Since we've collected results from multiple branches, some which may be past the end of the overall limit,
	// do a cursor-based filtering here to ensure we only return the limit.
	resp, done, err := createFilteredAndLimitedResponse(lsu.ci, foundSubjects.AsMap(), metadata)
	defer done()
	if err != nil {
		return err
	}

	if resp == nil {
		return nil
	}

	return lsu.parentStream.Publish(resp)
}

// branchRunInformation is information passed to a RunUntilSpanned handler.
type branchRunInformation struct {
	ci cursorInformation
}

// dependentBranchReducerReloopLimit is the limit of results for each iteration of the dependent branch LookupSubject redispatches.
const dependentBranchReducerReloopLimit = 1000

// dependentBranchReducer is the implementation reducer for any rewrite operations whose branches depend upon one another
// (intersection and exclusion).
type dependentBranchReducer struct {
	// parentStream is the stream to which results will be published, after reduction.
	parentStream dispatch.LookupSubjectsStream

	// collectors are a map from branch index to the associated collector of stream results.
	collectors map[int]*dispatch.CollectingDispatchStream[*v1.DispatchLookupSubjectsResponse]

	// parentCi is the cursor information from the parent call.
	parentCi cursorInformation

	// combinationHandler is the function invoked to "combine" the results from different branches, such as performing
	// intersection or exclusion.
	combinationHandler func(fs datasets.SubjectSetByResourceID, other datasets.SubjectSetByResourceID) error

	// firstBranchCi is the *current* cursor for the first branch; this value is updated during iteration as the reducer is
	// re-run.
	firstBranchCi cursorInformation
}

// ForIndex returns the stream to which results should be published for the branch with the given index. Must not be called
// in parallel.
func (dbr *dependentBranchReducer) ForIndex(ctx context.Context, setOperationIndex int) dispatch.LookupSubjectsStream {
	collector := dispatch.NewCollectingDispatchStream[*v1.DispatchLookupSubjectsResponse](ctx)
	dbr.collectors[setOperationIndex] = collector
	return collector
}

// RunUntilSpanned runs the branch (with the given index) until all necessary results have been collected. For the first branch,
// this is just a direct invocation. For all other branches, the handler will be reinvoked until all results have been collected
// *or* the last subject ID found is >= the last subject ID found by the first branch, ensuring that all other branches have
// "spanned" the subjects of the first branch. This is necessary because an intersection or exclusion must operate over the same
// set of subject IDs.
func (dbr *dependentBranchReducer) RunUntilSpanned(ctx context.Context, index int, handler func(ctx context.Context, current branchRunInformation) error) error {
	// If invoking the run for the first branch, use the current first branch cursor.
	if index == 0 {
		return handler(ctx, branchRunInformation{ci: dbr.firstBranchCi.withClonedLimits()})
	}

	// Otherwise, run the branch until it has either exhausted all results OR the last result returned matches the last result previously
	// returned by the first branch. This is to ensure that the other branches encompass the entire "span" of results from the first branch,
	// which is necessary for intersection or exclusion (e.g. dependent branches).
	firstBranchTerminalSubjectID, err := finalSubjectIDForResults(dbr.firstBranchCi, dbr.collectors[0].Results())
	if err != nil {
		return err
	}

	// If there are no concrete subject IDs found, then simply invoke the handler with the first branch's cursor/limit to
	// return the wildcard; all other results will be superflouous.
	if firstBranchTerminalSubjectID == "" {
		return handler(ctx, branchRunInformation{ci: dbr.firstBranchCi})
	}

	// Otherwise, run the handler until its returned results is empty OR its cursor is >= the terminal subject ID.
	startingCursor := dbr.firstBranchCi.currentCursor
	previousResultCount := 0
	for {
		limits := newLimitTracker(dependentBranchReducerReloopLimit)
		ci, err := newCursorInformation(startingCursor, limits, lsDispatchVersion)
		if err != nil {
			return err
		}

		// Invoke the handler with a modified limits and a cursor starting at the previous call.
		if err := handler(ctx, branchRunInformation{
			ci: ci,
		}); err != nil {
			return err
		}

		// Check for any new results found. If none, then we're done.
		updatedResults := dbr.collectors[index].Results()
		if len(updatedResults) == previousResultCount {
			return nil
		}

		// Otherwise, grab the terminal subject ID to create the next cursor.
		previousResultCount = len(updatedResults)
		terminalSubjectID, err := finalSubjectIDForResults(dbr.parentCi, updatedResults)
		if err != nil {
			return nil
		}

		// If the cursor is now the wildcard, then we know that all concrete results have been consumed.
		if terminalSubjectID == tuple.PublicWildcard {
			return nil
		}

		// If the terminal subject in the results collector is now at or beyond that of the first branch, then
		// we've spanned the entire results set necessary to perform the intersection or exclusion.
		if firstBranchTerminalSubjectID != tuple.PublicWildcard && terminalSubjectID >= firstBranchTerminalSubjectID {
			return nil
		}

		startingCursor = updatedResults[len(updatedResults)-1].AfterResponseCursor
	}
}

// CompletedDependentChildOperations is invoked once all branches have been run to perform combination and publish any
// valid subject IDs. This also moves the first branch's cursor forward.
//
// Returns the number of results from the first branch, and/or any error. The number of results is used to determine whether
// the first branch has been exhausted.
func (dbr *dependentBranchReducer) CompletedDependentChildOperations() (int, error) {
	firstBranchCount := -1

	// Update the first branch cursor for moving forward. This ensures that each iteration of the first branch for
	// RunUntilSpanned is moving forward.
	firstBranchTerminalSubjectID, err := finalSubjectIDForResults(dbr.parentCi, dbr.collectors[0].Results())
	if err != nil {
		return firstBranchCount, err
	}

	existingFirstBranchCI := dbr.firstBranchCi
	if firstBranchTerminalSubjectID != "" {
		updatedCI, err := dbr.firstBranchCi.withOutgoingSection(firstBranchTerminalSubjectID)
		if err != nil {
			return -1, err
		}

		updatedCursor := updatedCI.responsePartialCursor()
		fbci, err := newCursorInformation(updatedCursor, dbr.firstBranchCi.limits, lsDispatchVersion)
		if err != nil {
			return firstBranchCount, err
		}

		dbr.firstBranchCi = fbci
	}

	// Run the combiner over the results.
	var foundSubjects datasets.SubjectSetByResourceID
	metadata := emptyMetadata

	for index := 0; index < len(dbr.collectors); index++ {
		collector, ok := dbr.collectors[index]
		if !ok {
			return firstBranchCount, fmt.Errorf("missing collector for index %d", index)
		}

		results := datasets.NewSubjectSetByResourceID()
		for _, result := range collector.Results() {
			metadata = combineResponseMetadata(metadata, result.Metadata)
			if err := results.UnionWith(result.FoundSubjectsByResourceId); err != nil {
				return firstBranchCount, fmt.Errorf("failed to UnionWith: %w", err)
			}
		}

		if index == 0 {
			foundSubjects = results
			firstBranchCount = results.ConcreteSubjectCount()
		} else {
			err := dbr.combinationHandler(foundSubjects, results)
			if err != nil {
				return firstBranchCount, err
			}

			if foundSubjects.IsEmpty() {
				return firstBranchCount, nil
			}
		}
	}

	// Apply the limits to the found results.
	resp, done, err := createFilteredAndLimitedResponse(existingFirstBranchCI, foundSubjects.AsMap(), metadata)
	defer done()
	if err != nil {
		return firstBranchCount, err
	}

	if resp == nil {
		return firstBranchCount, nil
	}

	return firstBranchCount, dbr.parentStream.Publish(resp)
}

func newLookupSubjectsIntersection(parentStream dispatch.LookupSubjectsStream, ci cursorInformation) *dependentBranchReducer {
	return &dependentBranchReducer{
		parentStream: parentStream,
		collectors:   map[int]*dispatch.CollectingDispatchStream[*v1.DispatchLookupSubjectsResponse]{},
		parentCi:     ci,
		combinationHandler: func(fs datasets.SubjectSetByResourceID, other datasets.SubjectSetByResourceID) error {
			return fs.IntersectionDifference(other)
		},
		firstBranchCi: ci,
	}
}

func newLookupSubjectsExclusion(parentStream dispatch.LookupSubjectsStream, ci cursorInformation) *dependentBranchReducer {
	return &dependentBranchReducer{
		parentStream: parentStream,
		collectors:   map[int]*dispatch.CollectingDispatchStream[*v1.DispatchLookupSubjectsResponse]{},
		parentCi:     ci,
		combinationHandler: func(fs datasets.SubjectSetByResourceID, other datasets.SubjectSetByResourceID) error {
			fs.SubtractAll(other)
			return nil
		},
		firstBranchCi: ci,
	}
}
