package sharederrors

import (
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHelpLinks(t *testing.T) {
	c := http.Client{}
	t.Cleanup(c.CloseIdleConnections)

	for _, helpURL := range allErrorHelpUrls {
		t.Run(helpURL, func(t *testing.T) {
			require.EventuallyWithT(t, func(collect *assert.CollectT) {
				get, err := c.Get(helpURL)
				if !assert.NoError(collect, err) {
					return
				}
				if !assert.NoError(collect, err) {
					return
				}
				t.Cleanup(func() {
					get.Body.Close()
				})
				if !assert.Equal(collect, http.StatusOK, get.StatusCode) {
					body, err := io.ReadAll(get.Body)
					if err != nil {
						t.Log("could not read body: %w", err)
						return
					}
					t.Log("body: ", string(body))
				}
			}, 10*time.Second, 1*time.Second)
		})
	}
}
