package sharederrors

import (
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestHelpLinks(t *testing.T) {
	t.Parallel()

	c := http.Client{}
	t.Cleanup(c.CloseIdleConnections)

	for _, helpURL := range allErrorHelpUrls {
		t.Run(helpURL, func(t *testing.T) {
			t.Parallel()

			require.Eventually(t, func() bool {
				get, err := c.Get(helpURL)
				if err != nil {
					t.Log("got error", err)
					return false
				}
				_ = get.Body.Close()
				if get.StatusCode != http.StatusOK {
					t.Log("got status code", get.StatusCode)
					return false
				}
				return true
			}, 10*time.Second, 1*time.Second)
		})
	}
}
