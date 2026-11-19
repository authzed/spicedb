package signals

import "time"

// Options configures signal storage limits and its clock.
type Options struct {
	// Now supplies the clock used for evidence publication and aging.
	Now func() time.Time

	// MaxBytes limits the total resident storage budget in bytes.
	MaxBytes int64

	// MaxEntryBytes limits the resident size of one evidence entry in bytes.
	MaxEntryBytes int64

	// MaxSchemas limits the number of resident schema versions.
	MaxSchemas int

	// MaxRevisions limits resident exact-revision buckets per schema.
	MaxRevisions int

	// MaxSchemaHandles limits owner handles attached to a stored schema.
	MaxSchemaHandles int

	// Enabled names select a learning profile; nil enables the whole registry.
	Enabled []string
}

func (o Options) defaults() Options {
	if o.Now == nil {
		o.Now = time.Now
	}

	if o.MaxBytes == 0 {
		o.MaxBytes = 16 << 20
	}

	if o.MaxEntryBytes == 0 {
		o.MaxEntryBytes = 256 << 10
	}

	if o.MaxSchemas == 0 {
		o.MaxSchemas = 4
	}

	if o.MaxSchemaHandles == 0 {
		o.MaxSchemaHandles = 64
	}

	if o.MaxRevisions == 0 {
		o.MaxRevisions = 64
	}

	return o
}
