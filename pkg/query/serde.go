package query

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"

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
// looks up the matching IteratorSpec, reads the framed key + body, then
// delegates the body to the spec's Deserialize.
//
// The wire format per node is:
//
//	[1B type][uvarint keyLen][key bytes][uvarint bodyLen][body bytes]
//
// Bodies are length-prefixed so unknown trailing extensions can be skipped
// for forward compatibility; bodies for whole subtrees nest inside their
// parent's bodyLen wrapper.
func Deserialize(r io.Reader, dctx *DeserializeContext) (Iterator, error) {
	br := asByteReader(r)
	t, key, body, err := readHeader(br)
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
	return spec.Deserialize(body, key, dctx)
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

// readHeader reads [type][keyLen+key][bodyLen+body] and returns a bytes.Reader
// over the body slice.
func readHeader(r byteReader) (IteratorType, CanonicalKey, *bytes.Reader, error) {
	var tb [1]byte
	if _, err := io.ReadFull(r, tb[:]); err != nil {
		return 0, "", nil, fmt.Errorf("query: read type byte: %w", err)
	}
	key, err := readString(r)
	if err != nil {
		return 0, "", nil, fmt.Errorf("query: read canonical key: %w", err)
	}
	body, err := readBytes(r)
	if err != nil {
		return 0, "", nil, fmt.Errorf("query: read body: %w", err)
	}
	return IteratorType(tb[0]), CanonicalKey(key), bytes.NewReader(body), nil
}

// writeHeader emits [type][keyLen+key][bodyLen+body] in one shot.
func writeHeader(w io.Writer, t IteratorType, key CanonicalKey, body []byte) error {
	if _, err := w.Write([]byte{byte(t)}); err != nil {
		return err
	}
	if err := writeString(w, string(key)); err != nil {
		return err
	}
	return writeBytes(w, body)
}

// ----- Body buffer pool -----

var bodyBufPool = sync.Pool{
	New: func() any { return new(bytes.Buffer) },
}

func acquireBodyBuf() *bytes.Buffer {
	b := bodyBufPool.Get().(*bytes.Buffer)
	b.Reset()
	return b
}

func releaseBodyBuf(b *bytes.Buffer) {
	// Don't pool huge buffers — they'd hold memory indefinitely.
	if b.Cap() > 64*1024 {
		return
	}
	bodyBufPool.Put(b)
}

// serializeWithHeader is a convenience that builds a body in a pooled buffer,
// invokes writeBody to fill it, then emits the framed node to w.
func serializeWithHeader(w io.Writer, t IteratorType, key CanonicalKey, writeBody func(*bytes.Buffer) error) error {
	buf := acquireBodyBuf()
	defer releaseBodyBuf(buf)
	if err := writeBody(buf); err != nil {
		return err
	}
	return writeHeader(w, t, key, buf.Bytes())
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
