package datastore

import (
	"context"
	"testing"

	"github.com/moby/moby/client"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
)

// PausableEngineForTest is a RunningEngineForTest whose backing container can be
// frozen and thawed.
//
// Pausing is how tests simulate a datastore that has become unresponsive
// without dropping its connections: the container's processes stop running
// while its sockets stay open, so callers neither get a response nor a
// connection error.
//
// Engines that are not backed by a container (the in-memory engine) do not
// implement this interface, so tests must type assert and skip.
type PausableEngineForTest interface {
	RunningEngineForTest

	Pause(t testing.TB)
}

// pausableContainer provides Pause and Unpause for an engine backed by a single container.
type pausableContainer struct {
	container testcontainers.Container
}

// Pause freezes every process in the backing container. The container is unpaused during test cleanup.
func (p *pausableContainer) Pause(t testing.TB) {
	t.Helper()

	containerID := p.containerID(t)
	cli := dockerClientForTesting(t)

	// Registered before the pause itself so the container is thawed even if
	// the test fails in between.
	t.Cleanup(func() {
		t.Log("unpausing container", containerID)

		// Detached from the test's context, which testing cancels before it
		// runs cleanups.
		_, err := cli.ContainerUnpause(context.WithoutCancel(t.Context()), containerID, client.ContainerUnpauseOptions{})
		require.NoError(t, err, "failed to unpause container, every subsequent test will probably fail")
	})

	t.Log("pausing container", containerID)
	_, err := cli.ContainerPause(t.Context(), containerID, client.ContainerPauseOptions{})
	require.NoError(t, err)
}

func (p *pausableContainer) containerID(t testing.TB) string {
	t.Helper()

	require.NotNil(t, p.container, "engine has no container to pause")
	return p.container.GetContainerID()
}

// dockerClientForTesting returns a Docker client closed when the test finishes.
// Pausing is not part of the testcontainers container API, so it is issued
// against the daemon directly.
func dockerClientForTesting(t testing.TB) *testcontainers.DockerClient {
	t.Helper()

	cli, err := testcontainers.NewDockerClientWithOpts(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = cli.Close()
	})

	return cli
}
