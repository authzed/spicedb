package mysql

import (
	"context"
	"database/sql/driver"
	"errors"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func TestInstrumentConnectorRepeatedRegistration(t *testing.T) {
	registry := prometheus.NewRegistry()
	base := fakeDriverConnector{}

	firstConnector, firstCollectors, err := instrumentConnector(base, "0", registry)
	require.NoError(t, err)
	require.Len(t, firstCollectors, 2)

	secondConnector, secondCollectors, err := instrumentConnector(base, "0", registry)
	require.NoError(t, err)
	require.Len(t, secondCollectors, 2)

	for index := range firstCollectors {
		require.Same(t, firstCollectors[index], secondCollectors[index])
	}

	firstInstrumented, ok := firstConnector.(*instrumentedConnector)
	require.True(t, ok)
	secondInstrumented, ok := secondConnector.(*instrumentedConnector)
	require.True(t, ok)

	require.Same(t, firstInstrumented.connectCount, secondInstrumented.connectCount)
}

func TestInstrumentConnectorNilRegistererDefaultsAndReuses(t *testing.T) {
	originalRegisterer := prometheus.DefaultRegisterer
	originalGatherer := prometheus.DefaultGatherer
	registry := prometheus.NewRegistry()
	prometheus.DefaultRegisterer = registry
	prometheus.DefaultGatherer = registry
	t.Cleanup(func() {
		prometheus.DefaultRegisterer = originalRegisterer
		prometheus.DefaultGatherer = originalGatherer
	})

	_, firstCollectors, err := instrumentConnector(fakeDriverConnector{}, "1", nil)
	require.NoError(t, err)
	_, secondCollectors, err := instrumentConnector(fakeDriverConnector{}, "1", nil)
	require.NoError(t, err)

	for index := range firstCollectors {
		require.Same(t, firstCollectors[index], secondCollectors[index])
	}
}

type fakeDriverConnector struct{}

func (fakeDriverConnector) Connect(context.Context) (driver.Conn, error) {
	return nil, errors.New("not implemented")
}

func (fakeDriverConnector) Driver() driver.Driver {
	return fakeDriver{}
}

type fakeDriver struct{}

func (fakeDriver) Open(string) (driver.Conn, error) {
	return nil, errors.New("not implemented")
}
