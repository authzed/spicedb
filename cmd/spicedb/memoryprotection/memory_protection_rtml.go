//go:build memoryprotection

package memoryprotection

import (
	"github.com/authzed/spicedb/internal/middleware/memoryprotection/rtml"
	cmdutil "github.com/authzed/spicedb/pkg/cmd/server"
)

// InitDefaultMemoryUsageProvider initializes the default memory usage provider.
// When "memoryprotection" tag is set at build time, this sets the go-rtml provider.
// If you see the following error, add "-ldflags=-checklinkname=0" at build time:
//
//	link: github.com/odigos-io/go-rtml: invalid reference to runtime.gcController
func InitDefaultMemoryUsageProvider() {
	cmdutil.DefaultMemoryUsageProvider = rtml.NewRealTimeMemoryUsageProvider()
}
