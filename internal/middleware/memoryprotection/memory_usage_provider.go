package memoryprotection

type MemoryUsageProvider interface {
	IsMemLimitReached() bool
}

var (
	_ MemoryUsageProvider = (*HarcodedMemoryLimitProvider)(nil)
	_ MemoryUsageProvider = (*NoopMemoryUsageProvider)(nil)
)

type HarcodedMemoryLimitProvider struct {
	AcceptAllRequests bool
}

func (n *HarcodedMemoryLimitProvider) IsMemLimitReached() bool {
	return !n.AcceptAllRequests
}

type NoopMemoryUsageProvider struct{}

func (n *NoopMemoryUsageProvider) IsMemLimitReached() bool {
	return false
}

func NewNoopMemoryUsageProvider() *NoopMemoryUsageProvider {
	return &NoopMemoryUsageProvider{}
}
