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
}

func onr(ns, oid, rel string) *pb.ObjectAndRelation {
	return &pb.ObjectAndRelation{
		Namespace: ns,
		ObjectId:  oid,
		Relation:  rel,
	}
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
			{onr("user", "pm", Ellipsis), []expected{{"owner", true}, {"editor", true}, {"viewer", true}}},
			{onr("user", "cfo", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", true}}},
			{onr("user", "owner", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", true}}},
			{onr("user", "legal", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", true}}},
			{onr("user", "vp_product", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", true}}},
			{onr("user", "eng_lead", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", true}}},
			{onr("user", "auditor", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", true}}},
			{onr("user", "villain", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", false}}},
		}},
		{"document", "healthplan", []userset{
			{onr("user", "pm", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", false}}},
			{onr("user", "cfo", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", true}}},
			{onr("user", "owner", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", false}}},
			{onr("user", "legal", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", false}}},
			{onr("user", "vp_product", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", false}}},
			{onr("user", "eng_lead", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", false}}},
			{onr("user", "auditor", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", false}}},
			{onr("user", "villain", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", false}}},
		}},
		{"folder", "company", []userset{
			{onr("user", "pm", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", false}}},
			{onr("user", "cfo", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", false}}},
			{onr("user", "owner", Ellipsis), []expected{{"owner", true}, {"editor", true}, {"viewer", true}}},
			{onr("user", "legal", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", true}}},
			{onr("user", "vp_product", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", false}}},
			{onr("user", "eng_lead", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", false}}},
			{onr("user", "auditor", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", true}}},
			{onr("user", "villain", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", false}}},
			{onr("folder", "auditors", "viewer"), []expected{{"viewer", true}}},
		}},
		{"folder", "strategy", []userset{
			{onr("user", "pm", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", false}}},
			{onr("user", "cfo", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", false}}},
			{onr("user", "owner", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", true}}},
			{onr("user", "legal", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", true}}},
			{onr("user", "vp_product", Ellipsis), []expected{{"owner", true}, {"editor", true}, {"viewer", true}}},
			{onr("user", "eng_lead", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", false}}},
			{onr("user", "auditor", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", true}}},
			{onr("user", "villain", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", false}}},
			{onr("folder", "company", Ellipsis), []expected{{"parent", true}}},
		}},
		{"folder", "isolated", []userset{
			{onr("user", "pm", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", false}}},
			{onr("user", "cfo", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", false}}},
			{onr("user", "owner", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", false}}},
			{onr("user", "legal", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", false}}},
			{onr("user", "vp_product", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", false}}},
			{onr("user", "eng_lead", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", false}}},
			{onr("user", "auditor", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", false}}},
			{onr("user", "villain", Ellipsis), []expected{{"owner", false}, {"editor", false}, {"viewer", true}}},
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
						Start:      onr(tc.namespace, tc.objectID, expected.relation),
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
