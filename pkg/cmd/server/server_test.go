package server

import (
	"context"
	"testing"
	"time"

	"github.com/authzed/spicedb/internal/datastore/memdb"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestServerGracefulTermination(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	ctx, cancel := context.WithCancel(context.Background())
	ds, err := memdb.NewMemdbDatastore(0, 1*time.Second, 10*time.Second)
	require.NoError(t, err)
	c := ConfigWithOptions(&Config{}, WithPresharedKey("psk"), WithDatastore(ds))
	rs, err := c.Complete(ctx)
	require.NoError(t, err)

	ch := make(chan struct{}, 1)
	go func() {
		require.NoError(t, rs.Run(ctx))
		ch <- struct{}{}
	}()
	cancel()
	<-ch
}
