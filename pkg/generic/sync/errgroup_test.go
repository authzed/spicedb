package sync

import (
	"testing"

	"github.com/stretchr/testify/require"
)

type specificError struct {
	error
}

type errorWithCountsInterface interface {
	error

	GetCount() int
}

type withCountsError struct {
	msg   string
	count int
}

func (err withCountsError) Error() string {
	return err.msg
}

func (err withCountsError) GetCount() int {
	return err.count
}

func TestTypeErrgroup(t *testing.T) {
	require := require.New(t)

	g := TypedGroup[specificError]{}

	g.Go(func() specificError {
		return specificError{}
	})

	err := g.Wait()

	require.ErrorAs(err, &specificError{})
}

func TestTypeErrgroupWithInterface(t *testing.T) {
	require := require.New(t)

	g := TypedGroup[errorWithCountsInterface]{}

	g.Go(func() errorWithCountsInterface {
		return withCountsError{"oops", 42}
	})

	err := g.Wait()
	require.Equal("oops", err.Error())
	require.Equal(42, err.GetCount())

	require.ErrorAs(err, &withCountsError{})
}

func TestNoError(t *testing.T) {
	require := require.New(t)

	g := TypedGroup[errorWithCountsInterface]{}

	g.Go(func() errorWithCountsInterface {
		return nil
	})

	err := g.Wait()

	require.NoError(err)
}
