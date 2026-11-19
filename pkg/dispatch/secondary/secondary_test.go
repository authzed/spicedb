package secondary

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBasePolicyDefaults(t *testing.T) {
	var p BasePolicy
	primary := Participant{Name: PrimaryName, Primary: true}
	other := Participant{Name: "secondary"}

	require.True(t, p.Start(t.Context(), other))
	p.Done(other, nil)

	require.False(t, p.ReadyToClaim(t.Context(), primary, 0, false))
	require.True(t, p.ReadyToClaim(t.Context(), primary, 1, false), "claims at the first result")
	require.True(t, p.ReadyToClaim(t.Context(), primary, 0, true), "claims at the end of an empty stream")

	primaryErr := errors.New("primary failed")
	require.ErrorIs(t, p.NoWinnerError(map[string]error{PrimaryName: primaryErr, other.Name: errors.New("secondary failed")}), primaryErr)
	require.NoError(t, p.NoWinnerError(map[string]error{other.Name: errors.New("secondary failed")}), "only the primary's error is returned")
}
