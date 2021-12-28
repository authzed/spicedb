package testutil

import "testing"

func TestRequireEqualEmptyNil(t *testing.T) {
	RequireEqualEmptyNil(t, []int(nil), []int(nil))
	RequireEqualEmptyNil(t, []int(nil), []int{})
	RequireEqualEmptyNil(t, []int{}, []int(nil))
	RequireEqualEmptyNil(t, []int{}, []int{})
}
