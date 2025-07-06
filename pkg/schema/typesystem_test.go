package schema

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/caveats"
	ns "github.com/authzed/spicedb/pkg/namespace"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

func TestTypeSystemConcurrency(t *testing.T) {
	emptyEnv := caveats.NewEnvironmentWithDefaultTypeSet()
	setup := &PredefinedElements{
		Definitions: []*core.NamespaceDefinition{
			ns.Namespace(
				"document",
				ns.MustRelation("viewer", nil,
					ns.AllowedRelationWithExpiration("user", "..."),
					ns.AllowedRelationWithCaveat("user", "...", ns.AllowedCaveat("definedcaveat")),
				),
			),
			ns.Namespace("user"),
			ns.Namespace("team",
				ns.MustRelation("member", nil),
			),
		},
		Caveats: []*core.CaveatDefinition{
			ns.MustCaveatDefinition(emptyEnv, "definedcaveat", "1 == 2"),
		},
	}

	var wg sync.WaitGroup
	ctx := t.Context()
	ts := NewTypeSystem(ResolverForPredefinedDefinitions(*setup))
	require := require.New(t)
	var mu sync.Mutex
	var errs []error

	for range 10 {
		wg.Add(1)
		go func() {
			for range 20 {
				for _, n := range []string{"document", "user", "team"} {
					_, err := ts.GetValidatedDefinition(ctx, n)
					if err != nil {
						mu.Lock()
						errs = append(errs, err)
						mu.Unlock()
					}
				}
			}
			wg.Done()
		}()
	}
	wg.Wait()

	require.Empty(errs, "expected no errors in concurrent GetValidatedDefinition calls")
}
