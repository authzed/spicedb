//go:build !memoryprotection

package memoryprotection

// InitDefaultMemoryUsageProvider initializes the default memory usage provider.
// When no "memoryprotection" tag is set at build time, this falls back to the no-op provider.
func InitDefaultMemoryUsageProvider() {}
