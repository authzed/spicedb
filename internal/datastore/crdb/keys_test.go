package crdb

import (
	"context"
	"net"
	"sort"
	"strings"
	"testing"

	"github.com/authzed/authzed-go/pkg/requestmeta"
	"github.com/dustin/go-humanize"
	"github.com/grpc-ecosystem/go-grpc-middleware/v2/testing/testpb"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/maps"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"

	"github.com/authzed/spicedb/internal/grpchelpers"
)

func TestOverlapKeyAddition(t *testing.T) {
	cases := []struct {
		name       string
		keyer      overlapKeyer
		namespaces []string
		expected   keySet
	}{
		{
			name:       "none",
			keyer:      noOverlapKeyer,
			namespaces: []string{"a", "a/b", "c", "a/b/c"},
			expected:   map[string]struct{}{},
		},
		{
			name:       "static",
			keyer:      appendStaticKey("test"),
			namespaces: []string{"a", "a/b", "c", "a/b/c"},
			expected:   map[string]struct{}{"test": {}},
		},
		{
			name:       "prefix with default",
			keyer:      prefixKeyer,
			namespaces: []string{"a", "a/b", "c", "a/b/c"},
			expected: map[string]struct{}{
				defaultOverlapKey: {},
				"a":               {},
			},
		},
		{
			name:       "prefix no default",
			keyer:      prefixKeyer,
			namespaces: []string{"a/b", "a/b/c"},
			expected: map[string]struct{}{
				"a": {},
			},
		},
	}
	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			set := newKeySet(context.Background())
			for _, n := range tt.namespaces {
				tt.keyer.addKey(set, n)
			}
			require.EqualValues(t, tt.expected, set)
		})
	}
}

type testServer struct {
	testpb.UnimplementedTestServiceServer
}

func (t testServer) Ping(ctx context.Context, _ *testpb.PingRequest) (*testpb.PingResponse, error) {
	keys := maps.Keys(overlapKeysFromContext(ctx))
	sort.Strings(keys)
	return &testpb.PingResponse{Value: strings.Join(keys, ",")}, nil
}

func TestOverlapKeysFromContext(t *testing.T) {
	overlapKey := string(requestmeta.RequestOverlapKey)
	tests := []struct {
		name     string
		headers  []map[string]string
		expected string
	}{
		{
			name:     "no overlap keys",
			expected: "",
		},
		{
			name: "an overlap key",
			headers: []map[string]string{{
				overlapKey: "test",
			}},
			expected: "test",
		},
		{
			name: "collapses duplicate overlap keys",
			headers: []map[string]string{{
				overlapKey: "test,test",
			}},
			expected: "test",
		},
		{
			name: "collapses duplicate overlap keys in different headers",
			headers: []map[string]string{{
				overlapKey: "test,test",
			}, {
				overlapKey: "test,test",
			}},
			expected: "test",
		},
		{
			name: "collects overlap keys from different headers, ignoring duplicates",
			headers: []map[string]string{{
				overlapKey: "test,test1",
			}, {
				overlapKey: "test,test2",
			}},
			expected: "test,test1,test2",
		},
		{
			name: "sanitizes space",
			headers: []map[string]string{{
				overlapKey: "   test,test1   , ",
			}, {
				overlapKey: "test,  test2   , ,  ",
			}},
			expected: "test,test1,test2",
		},
	}
	for _, tt := range tests {
		listener := bufconn.Listen(humanize.MiByte)
		s := grpc.NewServer()
		testpb.RegisterTestServiceServer(s, &testServer{})
		go func() {
			// Ignore any errors
			_ = s.Serve(listener)
		}()

		conn, err := grpchelpers.DialAndWait(
			context.Background(),
			"",
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return listener.Dial()
			}),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		require.NoError(t, err)

		t.Cleanup(func() {
			conn.Close()
			listener.Close()
			s.Stop()
		})
		client := testpb.NewTestServiceClient(conn)

		t.Run(tt.name, func(t *testing.T) {
			md := metadata.New(map[string]string{})
			for _, h := range tt.headers {
				part := metadata.New(h)
				md = metadata.Join(md, part)
			}
			ctx := metadata.NewOutgoingContext(context.Background(), md)
			resp, err := client.Ping(ctx, &testpb.PingRequest{})
			require.NoError(t, err)
			require.Equal(t, tt.expected, resp.Value)
		})
	}
}
