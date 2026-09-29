package checkbaseline

import (
	"context"
	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/dispatch"
	bm "github.com/authzed/spicedb/pkg/benchmarks"
	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/query"
	"github.com/authzed/spicedb/pkg/tuple"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"testing"
)

func TestEngineDecisions(t *testing.T) {
	for _, d := range GeneratedDatasets([]Scale{{Name: "small", Fanout: 3, Depth: 3, DirectRelationships: 10}}) {
		t.Run(d.ID, func(t *testing.T) {
			ds, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC)
			require.NoError(t, err)
			defer ds.Close()
			cases, err := d.Setup(t.Context(), ds)
			require.NoError(t, err)
			rev, err := ds.HeadRevision(t.Context())
			require.NoError(t, err)
			schema, err := bm.ReadSchema(t.Context(), ds, rev.Revision)
			require.NoError(t, err)
			engines, err := PrepareEngines(t.Context(), WrapDataLayer(datalayer.NewDataLayer(ds)), rev, schema, cases, DefaultPolicy())
			require.NoError(t, err)
			for _, e := range engines {
				require.Positive(t, e.Preparation().NSPerOp)
				require.Positive(t, e.Preparation().BytesPerOp)
				defer e.Close()
				for range 2 {
					for _, c := range cases {
						rec := NewRecorder()
						decision, err := e.Check(WithRecorder(t.Context(), rec), c)
						require.NoError(t, err, "%s %s", e.Name(), c.ID)
						require.Equal(t, c.Expected.Outcome, decision.Outcome, "%s %s", e.Name(), c.ID)
						require.NotEmpty(t, rec.Seal().Events)
					}
				}
			}
		})
	}
}

func TestCycleDepthAndCancellation(t *testing.T) {
	ds, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC)
	require.NoError(t, err)
	defer ds.Close()
	datasets := GeneratedDatasets([]Scale{{Name: "cycle", Fanout: 3, Depth: 3, DirectRelationships: 3}})
	var cases []Case
	for _, d := range datasets {
		if d.Family == "recursive" {
			cases, err = d.Setup(t.Context(), ds)
			require.NoError(t, err)
		}
	}
	_, err = common.WriteRelationships(t.Context(), ds, tuple.UpdateOperationCreate, tuple.MustParse("document:d2#parent@document:doc"))
	require.NoError(t, err)
	rev, err := ds.HeadRevision(t.Context())
	require.NoError(t, err)
	sch, err := bm.ReadSchema(t.Context(), ds, rev.Revision)
	require.NoError(t, err)
	engines, err := PrepareEngines(t.Context(), datalayer.NewDataLayer(ds), rev, sch, cases, DefaultPolicy())
	require.NoError(t, err)
	for _, e := range engines {
		defer e.Close()
		_, err := e.Check(t.Context(), cases[1])
		require.Error(t, err, e.Name())
		if e.Name() == "classic" {
			require.True(t, dispatch.IsMaxDepthExceeded(err))
		} else {
			require.ErrorAs(t, err, &query.MaxRecursionDepthError{})
		}
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		_, err = e.Check(ctx, cases[1])
		if e.Name() == "classic" {
			require.Equal(t, codes.Canceled, status.Code(err))
		} else {
			require.ErrorIs(t, err, context.Canceled)
		}
	}
}
