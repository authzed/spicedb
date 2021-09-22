package e2e

import (
	"fmt"
	"io"
	"net"
	"time"
)

// WaitForServerReady polls the address every second until it responds
func WaitForServerReady(address string, out io.Writer) {
	ready := func() bool {
		timeout := time.Second
		conn, err := net.DialTimeout("tcp", address, timeout)
		if err != nil {
			fmt.Fprintln(out, err)
			return false
		}
		if conn != nil {
			conn.Close()
		}
		return true
	}

	for {
		if ready() {
			break
		}
		time.Sleep(1 * time.Second)
	}
}
