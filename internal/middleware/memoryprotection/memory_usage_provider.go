package memoryprotection

type MemoryUsageProvider interface {
	IsMemLimitReached() bool
}

var (
	_ MemoryUsageProvider = (*HarcodedMemoryUsageProvider)(nil)
	_ MemoryUsageProvider = (*NoopMemoryUsageProvider)(nil)
)

type HarcodedMemoryUsageProvider struct {
	AcceptAllRequests bool
}

func (n *HarcodedMemoryUsageProvider) IsMemLimitReached() bool {
	return !n.AcceptAllRequests
}

type NoopMemoryUsageProvider struct{}

func (n *NoopMemoryUsageProvider) IsMemLimitReached() bool {
	return false
}

func NewNoopMemoryUsageProvider() *NoopMemoryUsageProvider {
	return &NoopMemoryUsageProvider{}
}
