package pebbleutil

import (
	"bytes"
	"errors"
	"fmt"
	"iter"

	"github.com/cockroachdb/pebble"

	log "github.com/authzed/spicedb/internal/logging"
)

// ErrSkipEntry is a sentinel error that can be returned by conversion functions
// to indicate that the current entry should be skipped and iteration should continue.
var ErrSkipEntry = errors.New("skip entry")

// Range represents a range.
type Range struct {
	upper []byte
	lower []byte
}

// Lower returns the lower part of the range
func (pr Range) Lower() []byte {
	return pr.lower
}

// Upper returns the upper part of the range
func (pr Range) Upper() []byte {
	return pr.upper
}

// NextUpper returns the next lexicographical value to the upper boundary.
// This is useful when we want to reuse this for pebble iterators and want
// to ensure that both parts of the range are included in the iteration.
func (pr Range) NextUpper() []byte {
	return KeyUpperBound(pr.upper)
}

// Prefix represents a prefix for a key in Pebble.
type Prefix struct {
	value         []byte
	upperBoundary []byte
}

// ToRange creates a Range out of this Prefix based on a lower and upper boundary suffixes.
func (p Prefix) ToRange(lowerSuffix, upperSuffix string) (Range, error) {
	if lowerSuffix == "" || upperSuffix == "" {
		return Range{}, errors.New("lower and upper suffix must be provided")
	}

	lower := p.WithSuffix(lowerSuffix)
	upper := p.WithSuffix(upperSuffix)
	if bytes.Compare(lower.value, upper.value) > 0 {
		return Range{}, fmt.Errorf("lower prefix %s is greater than upper prefix %s", lower.value, upper.value)
	}

	return Range{lower: lower.value, upper: upper.value}, nil
}

// WithSuffix appends the provided suffix to the Prefix and returns a new Prefix.
func (p Prefix) WithSuffix(suffix string) Prefix {
	return p.WithByteSuffix([]byte(suffix))
}

// WithByteSuffix appends the provided byte suffix to the Prefix and returns a new Prefix.
func (p Prefix) WithByteSuffix(suffix []byte) Prefix {
	newPrefix := make([]byte, 0, len(p.value)+len(suffix))
	newPrefix = append(newPrefix, p.value...)
	newPrefix = append(newPrefix, suffix...)
	newUpperBoundary := KeyUpperBound(newPrefix)
	return Prefix{newPrefix, newUpperBoundary}
}

func (p Prefix) Prefix() []byte {
	return p.value
}

func (p Prefix) UpperBoundary() []byte {
	return p.upperBoundary
}

// StripPrefix removes this prefix from the provided value.
func (p Prefix) StripPrefix(value []byte) ([]byte, error) {
	idx := bytes.Index(value, p.value)
	if idx == -1 {
		return nil, fmt.Errorf("did not find prefix %s in %s", p.value, value)
	}

	return value[idx+len(p.value):], nil
}

// ConversionFunc is a function that converts a byte value coming out of a pebble query into a value of type K.
type ConversionFunc[K any] func([]byte) (K, error)

// IterForKey creates an iterator that iterates over all the keys defined by the Prefix in a pebble.Reader.
// The key will be stripped out of the Prefix, and the remainder of the key will be fed into the ConversionFunc to turn
// it into the type requested.
//
// Reusing an iterator will result in an error.
func IterForKey[K any](prefix Prefix, reader pebble.Reader, convFunc ConversionFunc[K]) iter.Seq2[K, error] {
	var closed bool
	var zeroK K
	var it *pebble.Iterator
	return func(yield func(K, error) bool) {
		var err error
		if it == nil {
			it, err = reader.NewIter(&pebble.IterOptions{
				LowerBound: prefix.value,
				UpperBound: prefix.upperBoundary,
			})
			// error in the creation of the iterator
			if err != nil {
				yield(zeroK, err)
				return
			}

			defer func() {
				closed = true
				itErr := it.Close()
				if itErr != nil {
					// we can't yield the error here, as the consumer may have stopped iterating, and that
					// would lead Go runtime to panic
					log.Err(itErr).Str("prefix", string(prefix.Prefix())).Msg("failed to close iterator in IterForKey")
					return
				}
			}()

			if !it.First() {
				return
			}
		}

		if closed {
			yield(zeroK, errors.New("iterator already closed"))
			return
		}

		for {
			if !it.Valid() {
				return
			}

			key := it.Key()

			// pebble may reuse the buffer, so we need to clone it
			strippedKey, stripErr := prefix.StripPrefix(bytes.Clone(key))
			if stripErr != nil {
				yield(zeroK, stripErr)
				return
			}

			convertedKey, kErr := convFunc(strippedKey)
			if kErr != nil {
				// If the conversion function returns ErrSkipEntry, skip this entry and continue
				if errors.Is(kErr, ErrSkipEntry) {
					if !it.Next() {
						return
					}
					continue
				}
				yield(zeroK, kErr)
				return
			}

			if !yield(convertedKey, nil) {
				return
			}

			if !it.Next() {
				return
			}
		}
	}
}

// IterForValue creates an iterator that iterates over all the values defined by the key defined by the Prefix in a pebble.Reader.
// The value will be stripped out of the Prefix, and the remainder of the value will be fed into the ConversionFunc to turn
// it into the type requested.
//
// Reusing an iterator will result in an error.
func IterForValue[K any](prefix Prefix, reader pebble.Reader, convFunc ConversionFunc[K]) iter.Seq2[K, error] {
	var zeroK K
	var hasValues bool
	var it *pebble.Iterator
	var closed bool
	return func(yield func(K, error) bool) {
		var err error
		if it == nil {
			it, err = reader.NewIter(&pebble.IterOptions{
				LowerBound: prefix.value,
				UpperBound: prefix.upperBoundary,
			})
			if err != nil {
				yield(zeroK, err)
				return
			}

			defer func() {
				itErr := it.Close()
				closed = true
				if itErr != nil {
					// we can't yield the error here, as the consumer may have stopped iterating, and that
					// would lead Go runtime to panic
					log.Err(itErr).Str("prefix", string(prefix.Prefix())).Msg("failed to close iterator in IterForValue")
					return
				}
			}()

			hasValues = it.First()
		}

		if closed {
			yield(zeroK, errors.New("iterator already closed"))
			return
		}

		if !hasValues {
			return
		}

		for {
			val, valErr := it.ValueAndErr()
			if valErr != nil {
				yield(zeroK, valErr)
				return
			}

			// pebble may reuse the buffer, so we need to clone it
			convertedVal, kErr := convFunc(bytes.Clone(val))
			if kErr != nil {
				yield(zeroK, kErr)
				return
			}

			if !yield(convertedVal, nil) {
				return
			}

			if !it.Next() {
				return
			}
		}
	}
}

func PrefixFor(p string) Prefix {
	return PrefixForByte([]byte(p))
}

func PrefixForByte(p []byte) Prefix {
	return Prefix{p, KeyUpperBound(p)}
}

// KeyUpperBound computes the next lexicographical value for the given key.
func KeyUpperBound(key []byte) []byte {
	end := make([]byte, len(key))
	copy(end, key)
	for i := len(end) - 1; i >= 0; i-- {
		end[i]++
		if end[i] != 0 {
			return end[:i+1]
		}
	}
	return nil
}
