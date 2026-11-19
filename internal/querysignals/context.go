package querysignals

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"slices"
	"sort"

	"github.com/authzed/spicedb/pkg/query/signals"
	"github.com/authzed/spicedb/pkg/query/signals/contextfield"
	"github.com/authzed/spicedb/pkg/query/signals/signal"
)

var fingerprintKey = func() []byte {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}

	return b
}()

// ContextOptions bounds and selects request-context fields to observe.
type ContextOptions struct {
	// Fields lists context field names to sample, including absent fields.
	Fields []string

	// MaxFields limits the number of fields sampled per request.
	MaxFields int

	// MaxBytes limits total encoded context bytes sampled per request.
	MaxBytes int
}

// ContextStats counts observed and skipped context fields.
type ContextStats struct {
	// Observed counts fields successfully published as observations.
	Observed int

	// Skipped counts fields rejected by limits, encoding, or publication errors.
	Skipped int
}

func canonical(v any) (any, error) {
	switch x := v.(type) {
	case nil:
		return []any{"null"}, nil
	case bool:
		return []any{"bool", x}, nil
	case string:
		return []any{"string", x}, nil
	case float64:
		return []any{"number", x}, nil
	case int:
		return []any{"integer", x}, nil
	case json.Number:
		return []any{"json-number", string(x)}, nil
	case []any:
		a := []any{"array"}

		for _, item := range x {
			value, err := canonical(item)
			if err != nil {
				return nil, err
			}

			a = append(a, value)
		}

		return a, nil
	case map[string]any:
		keys := make([]string, 0, len(x))

		for k := range x {
			keys = append(keys, k)
		}

		sort.Strings(keys)
		a := []any{"object"}

		for _, k := range keys {
			value, err := canonical(x[k])
			if err != nil {
				return nil, err
			}

			a = append(a, []any{k, value})
		}

		return a, nil
	default:
		return nil, fmt.Errorf("unsupported context type %T", v)
	}
}

func encoded(v any) ([]byte, error) {
	x, err := canonical(v)
	if err != nil {
		return nil, err
	}

	return json.Marshal(x)
}

func fingerprintBytes(b []byte) string {
	h := hmac.New(sha256.New, fingerprintKey)
	h.Write(b)
	return hex.EncodeToString(h.Sum(nil))
}

// Fingerprint returns a process-keyed fingerprint of a canonically encoded context value.
func Fingerprint(v any) (string, error) {
	b, err := encoded(v)
	if err != nil {
		return "", err
	}

	return fingerprintBytes(b), nil
}

// ObserveRequestContext samples selected context fields into the request's scoped signal view.
func ObserveRequestContext(view *signals.View, ctx map[string]any, opts ContextOptions) ContextStats {
	if !view.Enabled(contextfield.Name) {
		return ContextStats{}
	}

	if opts.MaxFields <= 0 {
		opts.MaxFields = 128
	}

	if opts.MaxBytes <= 0 {
		opts.MaxBytes = 64 << 10
	}

	fields := slices.Clone(opts.Fields)
	sort.Strings(fields)
	fields = slices.Compact(fields)
	result := ContextStats{}
	bytes := 0

	for i, field := range fields {
		if i >= opts.MaxFields {
			result.Skipped++
			continue
		}

		value, present := ctx[field]
		o := contextfield.Observation{Present: present}

		if present {
			b, err := encoded(value)
			if err != nil || bytes+len(b) > opts.MaxBytes {
				result.Skipped++
				continue
			}

			bytes += len(b)
			o.Fingerprint = fingerprintBytes(b)
		}

		if err := signal.Observe(view, contextfield.Input, field, o); err != nil {
			result.Skipped++
		} else {
			result.Observed++
		}
	}

	return result
}
