package graph

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/authzed/spicedb/internal/testfixtures"
	pb "github.com/authzed/spicedb/pkg/REDACTEDapi/api"
)

func init() {
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stdout})

	// Set this to Trace to dump log statements in tests.
	zerolog.SetGlobalLevel(zerolog.DebugLevel)
}

func TestSimple(t *testing.T) {
	type expected struct {
		relation string
		isMember bool
	}

	type userset struct {
		userset  *pb.ObjectAndRelation
		expected []expected
	}

	testCases := []struct {
		namespace string
		objectID  string
		usersets  []userset
	}{
		{"document", "masterplan", []userset{
			{testfixtures.ONR("user", "pm", Ellipsis), []expected{{"owner", true}, {"editor", true}, {"viewer", true}}},
			{testfixtures.ONR("user", "cfo", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", true}}},
			{testfixtures.ONR("user", "owner", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", true}}},
			{testfixtures.ONR("user", "legal", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", true}}},
			{testfixtures.ONR("user", "vp_product", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", true}}},
			{testfixtures.ONR("user", "eng_lead", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", true}}},
			{testfixtures.ONR("user", "auditor", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", true}}},
			{testfixtures.ONR("user", "villain", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", false}}},
		}},
		{"document", "healthplan", []userset{
			{testfixtures.ONR("user", "pm", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", false}}},
			{testfixtures.ONR("user", "cfo", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", true}}},
			{testfixtures.ONR("user", "owner", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", false}}},
			{testfixtures.ONR("user", "legal", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", false}}},
			{testfixtures.ONR("user", "vp_product", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", false}}},
			{testfixtures.ONR("user", "eng_lead", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", false}}},
			{testfixtures.ONR("user", "auditor", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", false}}},
			{testfixtures.ONR("user", "villain", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", false}}},
		}},
		{"folder", "company", []userset{
			{testfixtures.ONR("user", "pm", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", false}}},
			{testfixtures.ONR("user", "cfo", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", false}}},
			{testfixtures.ONR("user", "owner", Ellipsis), []expected{{"owner", true}, {"editor", true}, {"viewer", true}}},
			{testfixtures.ONR("user", "legal", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", true}}},
			{testfixtures.ONR("user", "vp_product", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", false}}},
			{testfixtures.ONR("user", "eng_lead", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", false}}},
			{testfixtures.ONR("user", "auditor", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", true}}},
			{testfixtures.ONR("user", "villain", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", false}}},
			{testfixtures.ONR("folder", "auditors", "viewer"), []expected{{"viewer", true}}},
		}},
		{"folder", "strategy", []userset{
			{testfixtures.ONR("user", "pm", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", false}}},
			{testfixtures.ONR("user", "cfo", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", false}}},
			{testfixtures.ONR("user", "owner", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", true}}},
			{testfixtures.ONR("user", "legal", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", true}}},
			{testfixtures.ONR("user", "vp_product", Ellipsis), []expected{{"owner", true}, {"editor", true}, {"viewer", true}}},
			{testfixtures.ONR("user", "eng_lead", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", false}}},
			{testfixtures.ONR("user", "auditor", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", true}}},
			{testfixtures.ONR("user", "villain", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", false}}},
			{testfixtures.ONR("folder", "company", Ellipsis), []expected{{"parent", true}}},
		}},
		{"folder", "isolated", []userset{
			{testfixtures.ONR("user", "pm", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", false}}},
			{testfixtures.ONR("user", "cfo", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", false}}},
			{testfixtures.ONR("user", "owner", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", false}}},
			{testfixtures.ONR("user", "legal", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", false}}},
			{testfixtures.ONR("user", "vp_product", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", false}}},
			{testfixtures.ONR("user", "eng_lead", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", false}}},
			{testfixtures.ONR("user", "auditor", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", false}}},
			{testfixtures.ONR("user", "villain", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", true}}},
		}},
	}

	for _, tc := range testCases {
		for _, userset := range tc.usersets {
			for _, expected := range userset.expected {
				name := fmt.Sprintf(
					"%s:%s#%s@%s:%s#%s=>%t",
					tc.namespace,
					tc.objectID,
					expected.relation,
					userset.userset.Namespace,
					userset.userset.ObjectId,
					userset.userset.Relation,
					expected.isMember,
				)

				t.Run(name, func(t *testing.T) {
					require := require.New(t)

					ds, revision := testfixtures.StandardDatastoreWithData(require)

					dispatch, err := NewLocalDispatcher(ds)
					require.NoError(err)

					checkResult := dispatch.Check(context.Background(), CheckRequest{
						Start:      testfixtures.ONR(tc.namespace, tc.objectID, expected.relation),
						Goal:       userset.userset,
						AtRevision: revision,
					})

					require.NoError(checkResult.Err)
					require.Equal(expected.isMember, checkResult.IsMember)

					// Check for goroutine leaks.
					defer goleak.VerifyNone(t)
				})
			}
		}
	}
}
