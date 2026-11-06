package arrowdirection_test

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/queryopttest"
	"github.com/authzed/spicedb/internal/querysignalstest"
	"github.com/authzed/spicedb/pkg/query"
	"github.com/authzed/spicedb/pkg/query/planhint"
	"github.com/authzed/spicedb/pkg/query/queryopt"
	"github.com/authzed/spicedb/pkg/query/queryopt/arrowdirection"
	"github.com/authzed/spicedb/pkg/query/queryopt/optimization"
	"github.com/authzed/spicedb/pkg/query/signals/historicalresultsize"
	"github.com/authzed/spicedb/pkg/query/signals/resultsize"
)

func TestObservedDirectionAndHistoricalFallback(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		owner := querysignalstest.Owner(t, resultsize.New(), historicalresultsize.New())
		v := querysignalstest.View(t, owner, "a", 1)

		// Distinct leaf identities let us assign independent fanout observations.
		co := queryopttest.Canonical(t, queryopttest.Arrow(
			queryopttest.Empty(),
			queryopttest.Fixed(t, "folder:f#viewer@user:a"),
		))
		left, right := co.Root.SubOutlines[0], co.Root.SubOutlines[1]

		// The right side is cheaper to enumerate: four samples per side provide
		// evidence at this revision and feed the historical derivative as well.
		querysignalstest.ResultSizes(t, v, left, query.OperationIterSubjects, time.Now(), 8, 8, 8, 8)
		querysignalstest.ResultSizes(t, v, right, query.OperationIterResources, time.Now(), 2, 2, 2, 2)
		p, err := queryopt.NewPipeline([]optimization.Optimizer{arrowdirection.New()})
		require.NoError(t, err)

		for _, rev := range []uint64{1, 2} {
			for _, tracing := range []bool{false, true} {
				view := querysignalstest.View(t, owner, "a", rev)
				var sink planhint.TraceSink

				if tracing {
					sink = discardSink{}
				}

				ctx := optimization.NewContext(optimization.RequestParams{}, view.ForPlan(), sink)
				ctx.Key = querysignalstest.ExecutionKey

				// Check the producer before ledger admission: dropping evidence at the ledger
				// alone would still allocate the references on every ordinary planning request.
				opt := arrowdirection.New()
				produced, err := opt.Run(ctx.ForPass(opt), co)
				require.NoError(t, err)
				require.Len(t, produced.Hints, 1)

				if !tracing {
					require.Nil(t, produced.Hints[0].Evidence)
				}

				out, err := p.Run(co, ctx)
				require.NoError(t, err)
				h := out.Hints[out.Root.ID][0]
				require.Equal(t, planhint.RightToLeft, h.Payload.(planhint.ArrowDirection).Direction)

				if tracing {
					require.Len(t, h.Evidence, 2)
					require.Equal(t, rev == 2, h.Evidence[0].Historical)
				} else {
					require.Nil(t, h.Evidence)
				}

				_, err = out.Compile()
				require.NoError(t, err)
				require.True(t, out.HintLedger.HasResponse(h.ID, "iterator-compiler"))
			}
		}
	})
}

type discardSink struct{}

// Record accepts trace dispositions without retaining them in this fixture.
func (discardSink) Record(planhint.Disposition) {}

func TestExecutionKeyBuilderIsRequired(t *testing.T) {
	co := queryopttest.Canonical(t, queryopttest.Arrow(queryopttest.Empty(), queryopttest.Empty()))
	ctx := optimization.NewContext(optimization.RequestParams{}, nil, nil)
	opt := arrowdirection.New()
	_, err := opt.Run(ctx.ForPass(opt), co)
	require.ErrorContains(t, err, "execution key builder")
}
