package caveats

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/authzed/spicedb/pkg/caveats/types"
	"github.com/authzed/spicedb/pkg/testutil"
)

func mustNewStruct(values map[string]any) *structpb.Struct {
	s, err := structpb.NewStruct(values)
	if err != nil {
		panic(err)
	}
	return s
}

func mustParse(str string) time.Time {
	p, err := time.Parse(time.RFC3339, str)
	if err != nil {
		panic(err)
	}
	return p
}

func TestConvertContextToStruct(t *testing.T) {
	tcs := []struct {
		name     string
		input    map[string]any
		expected *structpb.Struct
	}{
		{
			"converts time",
			map[string]any{
				"another_field": 1234,
				"some_time":     mustParse("2023-01-02T03:04:05Z"),
			},
			mustNewStruct(map[string]any{
				"another_field": 1234,
				"some_time":     "2023-01-02T03:04:05Z",
			}),
		},
		{
			"converts time in map",
			map[string]any{
				"some_key": map[string]any{
					"some_time": mustParse("2023-01-02T03:04:05Z"),
				},
			},
			mustNewStruct(map[string]any{
				"some_key": map[string]any{
					"some_time": "2023-01-02T03:04:05Z",
				},
			}),
		},
		{
			"converts time in list",
			map[string]any{
				"some_key": []any{
					1,
					true,
					mustParse("2023-01-02T03:04:05Z"),
					mustParse("2022-01-02T03:04:06Z"),
				},
			},
			mustNewStruct(map[string]any{
				"some_key": []any{
					1,
					true,
					"2023-01-02T03:04:05Z",
					"2022-01-02T03:04:06Z",
				},
			}),
		},
		{
			"converts duration",
			map[string]any{
				"another_field": 1234,
				"some_time":     3 * time.Second,
			},
			mustNewStruct(map[string]any{
				"another_field": 1234,
				"some_time":     "3s",
			}),
		},
		{
			"converts long duration",
			map[string]any{
				"another_field": 1234,
				"some_time":     2*time.Hour + 45*time.Minute,
			},
			mustNewStruct(map[string]any{
				"another_field": 1234,
				"some_time":     "2h45m0s",
			}),
		},
		{
			"converts ip address",
			map[string]any{
				"user_ip": types.MustParseIPAddress("192.168.1.100"),
			},
			mustNewStruct(map[string]any{
				"user_ip": "192.168.1.100",
			}),
		},
	}

	for _, tc := range tcs {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			result, err := ConvertContextToStruct(tc.input)
			require.NoError(t, err)
			testutil.RequireProtoEqual(t, tc.expected, result, "mismatch in converted context")
		})
	}
}
