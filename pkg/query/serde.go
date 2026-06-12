package query

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/authzed/spicedb/pkg/schema/v2"
)

// DeserializeContext carries everything an iterator might need to reconstitute
// itself from wire bytes that don't fully capture its in-memory state.
// Today the only required field is Schema, used by DatastoreIterator to resolve
// (definition, relation, type, subrelation, flags) back to a live *schema.BaseRelation.
// Future resolvers (e.g. caveat registries) plug in here without changing the
// IteratorSpec.Deserialize signature.
type DeserializeContext struct {
	Schema *schema.Schema
}

// Deserialize reads one Iterator subtree from r. It peels the type byte,
// looks up the matching IteratorSpec, reads the canonical key, then delegates
// the rest of the body to the spec's Deserialize, which reads its own fields
// and recurses for any sub-iterators directly from the same underlying reader.
//
// The wire format per node is:
//
//	[1B type][uvarint keyLen][key bytes][type-specific body bytes]
//
// No body-length prefix: this saves a per-node []byte and *bytes.Reader on
// decode. Forward compatibility for body shape comes via registering a new
// IteratorType byte (the registry is the schema). Additive changes still work
// when the new field is gated on a new flag bit and emits nothing when
// defaulted, so old plans are byte-for-byte unchanged.
func Deserialize(r io.Reader, dctx *DeserializeContext) (Iterator, error) {
	br := asByteReader(r)
	t, key, err := readHeader(br)
	if err != nil {
		return nil, err
	}
	spec, ok := iteratorRegistry[t]
	if !ok {
		return nil, fmt.Errorf("query: unknown iterator type %q", byte(t))
	}
	if spec.Deserialize == nil {
		return nil, fmt.Errorf("query: iterator type %q (%s) has no Deserialize registered", byte(t), spec.Name)
	}
	return spec.Deserialize(br, key, dctx)
}

// byteReader is the union of io.Reader and io.ByteReader that the binary
// uvarint helpers require.
type byteReader interface {
	io.Reader
	io.ByteReader
}

func asByteReader(r io.Reader) byteReader {
	if br, ok := r.(byteReader); ok {
		return br
	}
	return bufio.NewReader(r)
}

// ----- Header framing -----

// readHeader reads [type][keyLen+key] and returns the type and key. The
// type-specific body bytes are left on the reader for the spec's Deserialize
// to consume directly — no intermediate slice or *bytes.Reader is allocated.
func readHeader(r byteReader) (IteratorType, CanonicalKey, error) {
	var tb [1]byte
	if _, err := io.ReadFull(r, tb[:]); err != nil {
		return 0, "", fmt.Errorf("query: read type byte: %w", err)
	}
	key, err := readString(r)
	if err != nil {
		return 0, "", fmt.Errorf("query: read canonical key: %w", err)
	}
	return IteratorType(tb[0]), CanonicalKey(key), nil
}

// writeHeader emits [type][keyLen+key]. The type-specific body bytes are
// written by the caller directly to w.
func writeHeader(w io.Writer, t IteratorType, key CanonicalKey) error {
	if _, err := w.Write([]byte{byte(t)}); err != nil {
		return err
	}
	return writeString(w, string(key))
}

// SerializeWithHeader emits the header and then invokes writeBody to write the
// type-specific body straight into w — no intermediate buffer. (An earlier
// version built the body in a pooled bytes.Buffer so we could length-prefix
// it; we dropped the length prefix to let the decode side avoid a per-node
// allocation, which also lets the encode side skip the staging buffer.)
//
// Exported for external iterators registered via MustRegisterIterator that
// need to emit their own bodies with the same framing as the built-ins.
func SerializeWithHeader(w io.Writer, t IteratorType, key CanonicalKey, writeBody func(io.Writer) error) error {
	if err := writeHeader(w, t, key); err != nil {
		return err
	}
	return writeBody(w)
}

// ----- Primitives -----

func writeUvarint(w io.Writer, v uint64) error {
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buf[:], v)
	_, err := w.Write(buf[:n])
	return err
}

func readUvarint(r io.ByteReader) (uint64, error) {
	return binary.ReadUvarint(r)
}

func writeString(w io.Writer, s string) error {
	if err := writeUvarint(w, uint64(len(s))); err != nil {
		return err
	}
	if len(s) == 0 {
		return nil
	}
	_, err := io.WriteString(w, s)
	return err
}

func readString(r byteReader) (string, error) {
	n, err := readUvarint(r)
	if err != nil {
		return "", err
	}
	if n == 0 {
		return "", nil
	}
	if n > maxFieldLen {
		return "", fmt.Errorf("query: string length %d exceeds maximum %d", n, maxFieldLen)
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", err
	}
	return string(buf), nil
}

func writeBytes(w io.Writer, b []byte) error {
	if err := writeUvarint(w, uint64(len(b))); err != nil {
		return err
	}
	if len(b) == 0 {
		return nil
	}
	_, err := w.Write(b)
	return err
}

func readBytes(r byteReader) ([]byte, error) {
	n, err := readUvarint(r)
	if err != nil {
		return nil, err
	}
	if n == 0 {
		return nil, nil
	}
	if n > maxFieldLen {
		return nil, fmt.Errorf("query: bytes length %d exceeds maximum %d", n, maxFieldLen)
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}
	return buf, nil
}

// maxFieldLen guards against a corrupt/malicious wire payload trying to
// allocate gigabytes. 64MiB is well above any realistic plan fragment.
const maxFieldLen = 64 * 1024 * 1024

// ----- Proto helpers -----

type vtMarshaler interface {
	MarshalVT() ([]byte, error)
}

type vtUnmarshaler interface {
	UnmarshalVT([]byte) error
}

func writeProto(w io.Writer, m vtMarshaler) error {
	raw, err := m.MarshalVT()
	if err != nil {
		return err
	}
	return writeBytes(w, raw)
}

func readProto(r byteReader, m vtUnmarshaler) error {
	raw, err := readBytes(r)
	if err != nil {
		return err
	}
	if len(raw) == 0 {
		return errors.New("query: readProto got empty payload")
	}
	return m.UnmarshalVT(raw)
}

// ----- Sub-iterator recursion -----

// writeSubs emits a variable-arity list of sub-iterators: a uvarint count
// followed by each child serialized in turn. Fixed-arity parents should call
// sub.Serialize directly instead of going through this.
func writeSubs(w io.Writer, subs []Iterator) error {
	if err := writeUvarint(w, uint64(len(subs))); err != nil {
		return err
	}
	for i, sub := range subs {
		if err := sub.Serialize(w); err != nil {
			return fmt.Errorf("query: serialize sub %d: %w", i, err)
		}
	}
	return nil
}

// readSubs reads a uvarint count followed by that many sub-iterators.
func readSubs(r byteReader, dctx *DeserializeContext) ([]Iterator, error) {
	n, err := readUvarint(r)
	if err != nil {
		return nil, err
	}
	if n > maxSubCount {
		return nil, fmt.Errorf("query: sub-iterator count %d exceeds maximum %d", n, maxSubCount)
	}
	if n == 0 {
		return nil, nil
	}
	subs := make([]Iterator, n)
	for i := range subs {
		sub, err := Deserialize(r, dctx)
		if err != nil {
			return nil, fmt.Errorf("query: deserialize sub %d: %w", i, err)
		}
		subs[i] = sub
	}
	return subs, nil
}

// readNSubs reads exactly N sub-iterators (no count prefix) — for fixed-arity
// parents that hard-coded their child count into the body schema.
func readNSubs(r byteReader, n int, dctx *DeserializeContext) ([]Iterator, error) {
	subs := make([]Iterator, n)
	for i := range subs {
		sub, err := Deserialize(r, dctx)
		if err != nil {
			return nil, fmt.Errorf("query: deserialize sub %d: %w", i, err)
		}
		subs[i] = sub
	}
	return subs, nil
}

const maxSubCount = 1 << 20

// ----- Flag-bitset helpers -----
//
// Each iterator body begins with a uvarint flags bitset. Only bits set imply
// their corresponding field follows in the body. Empty strings, false bools,
// nil pointers, and zero-length slices are signaled by their bit being clear
// and emit nothing. New fields append at higher bit positions; old readers
// observing a high bit they don't recognize will (combined with the body
// length prefix at the header) simply read fewer bytes than the body contains
// and the remainder is discarded.

func setFlag(flags *uint64, bit uint, on bool) {
	if on {
		*flags |= 1 << bit
	}
}

func hasFlag(flags uint64, bit uint) bool {
	return flags&(1<<bit) != 0
}
