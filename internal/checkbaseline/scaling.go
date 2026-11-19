package checkbaseline

import (
	"context"
	"fmt"
	"sort"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/tuple"
)

// BackgroundDatasets grows the database while holding each checked subgraph
// constant. Noise occupies separate document resources, 100 relationships each.
func BackgroundDatasets(totals []int) []Dataset {
	bases := GeneratedDatasets([]Scale{{Name: "small", Fanout: 10, Depth: 3, DirectRelationships: 100}})
	out := make([]Dataset, 0, len(bases)*len(totals))
	for _, total := range totals {
		for _, base := range bases {
			d := base
			d.ID = fmt.Sprintf("sized/%s/background/%d", base.Family, total)
			d.Family = base.Family + "/background"
			d.Source = "background generator v1; fixed small subgraph; 100 relationships per background resource"
			d.Setup = func(ctx context.Context, ds datastore.Datastore) ([]Case, error) {
				cases, err := base.Setup(ctx, ds)
				if err != nil {
					return nil, err
				}
				rev, err := ds.HeadRevision(ctx)
				if err != nil {
					return nil, err
				}
				reader := datalayer.NewDataLayer(ds).SnapshotReader(rev.Revision, datalayer.SchemaHash(rev.SchemaHash))
				seq, err := reader.QueryRelationships(ctx, datastore.RelationshipsFilter{OptionalResourceType: "document"})
				if err != nil {
					return nil, err
				}
				current := 0
				for _, err := range seq {
					if err != nil {
						return nil, err
					}
					current++
				}
				seq, err = reader.QueryRelationships(ctx, datastore.RelationshipsFilter{OptionalResourceType: "group"})
				if err != nil {
					return nil, err
				}
				for _, err := range seq {
					if err != nil {
						return nil, err
					}
					current++
				}
				if current > total {
					return nil, fmt.Errorf("target %d smaller than hot subgraph %d", total, current)
				}
				batch := make([]tuple.Relationship, 0, 1000)
				flush := func() error {
					_, err := common.WriteRelationships(ctx, ds, tuple.UpdateOperationCreate, batch...)
					batch = batch[:0]
					return err
				}
				for i := 0; i < total-current; i++ {
					if err := ctx.Err(); err != nil {
						return nil, err
					}
					batch = append(batch, tuple.MustParse(fmt.Sprintf("document:background-%08d#viewer@user:background-%03d", i/100, i%100)))
					if len(batch) == cap(batch) {
						if err := flush(); err != nil {
							return nil, err
						}
					}
				}
				if len(batch) > 0 {
					if err := flush(); err != nil {
						return nil, err
					}
				}
				return cases, nil
			}
			out = append(out, d)
		}
	}
	return out
}

// ScalingDatasets keeps background size, dense relation size, fanout and depth
// as separate experiments. Original anchors retain their exact IDs and inputs.
func ScalingDatasets() []Dataset {
	out := BackgroundDatasets([]int{1000, 100000, 1000000})
	for _, d := range GeneratedDatasets(DefaultScales()) {
		if d.Scale.Name == "small" && (d.Family == "direct" || d.Family == "arrow" || d.Family == "recursive" || d.Family == "exclusion") {
			out = append(out, d)
		}
	}
	for _, n := range []int{100000, 1000000} {
		for _, d := range GeneratedDatasets([]Scale{{Name: fmt.Sprintf("dense%d", n), DirectRelationships: n - 1}}) {
			if d.Family == "direct" {
				out = append(out, d)
			}
		}
	}
	for _, n := range []int{5000, 10000} {
		for _, d := range GeneratedDatasets([]Scale{{Name: fmt.Sprintf("fanout%d", n), Fanout: n}}) {
			if d.Family == "arrow" || d.Family == "groups" || d.Family == "all" {
				out = append(out, d)
			}
		}
	}
	for _, n := range []uint8{64, 128} {
		for _, d := range GeneratedDatasets([]Scale{{Name: fmt.Sprintf("depth%d", n), Depth: int(n)}}) {
			if d.Family == "recursive" {
				setup := d.Setup
				d.Setup = func(ctx context.Context, ds datastore.Datastore) ([]Case, error) {
					cases, err := setup(ctx, ds)
					for i := range cases {
						cases[i].QPDepth = int(n) + 10
						cases[i].ClassicDepth = 8 * (uint32(n) + 10)
					}
					return cases, err
				}
				out = append(out, d)
			}
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}
