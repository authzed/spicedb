package main

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestInvalidProfile(t *testing.T) {
	var out bytes.Buffer
	require.Equal(t, 2, run(context.Background(), []string{"-profile=invalid", "-output=unused"}, &out, &out))
}

func TestListScalingDatasets(t *testing.T) {
	var out bytes.Buffer
	require.Equal(t, 0, run(context.Background(), []string{"-catalog=scaling", "-list", "-dataset=^sized/direct/background/1000000$"}, &out, &out))
	require.JSONEq(t, `["sized/direct/background/1000000"]`, out.String())
}
