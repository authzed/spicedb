package postgres

import (
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/ccoveille/go-safecast"
	"github.com/jackc/pgx/v5/pgtype"
)

// RegisterTypes registers pgSnapshot and xid8 with a pgtype.ConnInfo.
func RegisterTypes(m *pgtype.Map) {
	m.RegisterType(&pgtype.Type{
		Name:  "snapshot",
		OID:   5038,
		Codec: SnapshotCodec{},
	})
	m.RegisterType(&pgtype.Type{
		Name:  "xid",
		OID:   5069,
		Codec: Uint64Codec{},
	})
	m.RegisterDefaultPgType(pgSnapshot{}, "snapshot")
	m.RegisterDefaultPgType(xid8{}, "xid")
	// needed for text query modes (exec, and simple), so that caveats are serialized as JSONB
	m.RegisterDefaultPgType(map[string]any{}, "jsonb")
}

type SnapshotCodec struct {
	pgtype.TextCodec
}

func (SnapshotCodec) DecodeValue(tm *pgtype.Map, oid uint32, format int16, src []byte) (interface{}, error) {
	if src == nil {
		return nil, nil
	}

	var target pgSnapshot
	scanPlan := tm.PlanScan(oid, format, &target)
	if scanPlan == nil {
		return nil, fmt.Errorf("PlanScan did not find a plan")
	}

	err := scanPlan.Scan(src, &target)
	if err != nil {
		return nil, err
	}

	return target, nil
}

type pgSnapshot struct {
	xmin, xmax uint64
	xipList    []uint64 // Must always be sorted
}

var (
	_ pgtype.TextScanner = &pgSnapshot{}
	_ pgtype.TextValuer  = &pgSnapshot{}
)

func (s *pgSnapshot) ScanText(v pgtype.Text) error {
	if !v.Valid {
		return fmt.Errorf("cannot scan NULL into pgSnapshot")
	}

	components := strings.SplitN(v.String, ":", 3)
	if len(components) != 3 {
		return fmt.Errorf("wrong number of snapshot components: %s", v.String)
	}

	var err error
	s.xmin, err = strconv.ParseUint(components[0], 10, 64)
	if err != nil {
		return fmt.Errorf("unable to parse xmin: %s", components[0])
	}

	s.xmax, err = strconv.ParseUint(components[1], 10, 64)
	if err != nil {
		return fmt.Errorf("unable to parse xmax: %s", components[1])
	}

	if components[2] != "" {
		xipStrings := strings.Split(components[2], ",")
		s.xipList = make([]uint64, len(xipStrings))
		for i, xipStr := range xipStrings {
			s.xipList[i], err = strconv.ParseUint(xipStr, 10, 64)
			if err != nil {
				return fmt.Errorf("unable to parse xip: %s", xipStr)
			}
		}

		// Do a defensive sort in case the server is feeling out of sorts.
		slices.Sort(s.xipList)
	} else {
		s.xipList = nil
	}

	return nil
}

func (s pgSnapshot) TextValue() (pgtype.Text, error) {
	return pgtype.Text{String: s.String(), Valid: true}, nil
}

// String uses the official postgres encoding for snapshots, which is described here:
// https://www.postgresql.org/docs/current/functions-info.html#FUNCTIONS-PG-SNAPSHOT-PARTS
func (s pgSnapshot) String() string {
	xipStrs := make([]string, len(s.xipList))
	for i, xip := range s.xipList {
		xipStrs[i] = strconv.FormatUint(xip, 10)
	}

	components := []string{
		strconv.FormatUint(s.xmin, 10),
		strconv.FormatUint(s.xmax, 10),
		strings.Join(xipStrs, ","),
	}

	return strings.Join(components, ":")
}

func (s pgSnapshot) Equal(rhs pgSnapshot) bool {
	return s.compare(rhs) == equal
}

func (s pgSnapshot) GreaterThan(rhs pgSnapshot) bool {
	return s.compare(rhs) == gt
}

func (s pgSnapshot) LessThan(rhs pgSnapshot) bool {
	return s.compare(rhs) == lt
}

type comparisonResult uint8

const (
	_ comparisonResult = iota
	equal
	lt
	gt
	concurrent
)

func (cr comparisonResult) String() string {
	switch cr {
	case equal:
		return "="
	case lt:
		return "<"
	case gt:
		return ">"
	case concurrent:
		return "~"
	default:
		return "?"
	}
}

// compare will return whether we can definitely assert that one snapshot was
// definitively created after, before, at the same time, or was executed
// concurrent with another transaction. We assess this based on whether a
// transaction has more, less, or conflicting information about the resolution
// of in-progress transactions. E.g. if one snapshot only sees txids 1 and 3 as
// visible but another transaction sees 1-3 as visible, that transaction is
// greater.
// example:
// 0:4:2   -> (1,3 visible)
// 0:4:2,3 -> (1 visible)
func (s pgSnapshot) compare(rhs pgSnapshot) comparisonResult {
	rhsHasMoreInfo := rhs.anyTXVisible(s.xmax, s.xipList)
	lhsHasMoreInfo := s.anyTXVisible(rhs.xmax, rhs.xipList)

	switch {
	case rhsHasMoreInfo && lhsHasMoreInfo:
		return concurrent
	case rhsHasMoreInfo:
		return lt
	case lhsHasMoreInfo:
		return gt
	default:
		return equal
	}
}

func (s pgSnapshot) anyTXVisible(first uint64, others []uint64) bool {
	if s.txVisible(first) {
		return true
	}
	for _, txid := range others {
		if s.txVisible(txid) {
			return true
		}
	}

	return false
}

// markComplete will create a new snapshot where the specified transaction will be marked as
// complete and visible. For example, if txid was present in the xip list of this snapshot
// it will be removed and the xmin and xmax will be adjusted accordingly.
func (s pgSnapshot) markComplete(txid uint64) pgSnapshot {
	if txid < s.xmin {
		// Nothing to do
		return s
	}

	xipListCopy := make([]uint64, len(s.xipList))
	copy(xipListCopy, s.xipList)

	newSnapshot := pgSnapshot{
		s.xmin,
		s.xmax,
		xipListCopy,
	}

	// Adjust the xmax and running tx if necessary
	if txid >= s.xmax {
		for newIP := s.xmax; newIP < txid+1; newIP++ {
			newSnapshot.xipList = append(newSnapshot.xipList, newIP)
		}
		newSnapshot.xmax = txid + 1
	}

	// Mark the tx complete if it's in the xipList
	// Note: we only find the first if it was erroneously duplicate
	pos, found := slices.BinarySearch(newSnapshot.xipList, txid)
	if found {
		newSnapshot.xipList = slices.Delete(newSnapshot.xipList, pos, pos+1)
	}

	// Adjust the xmin if necessary
	if len(newSnapshot.xipList) > 0 {
		newSnapshot.xmin = newSnapshot.xipList[0]
	} else {
		newSnapshot.xmin = newSnapshot.xmax
		newSnapshot.xipList = nil
	}

	return newSnapshot
}

// markInProgress will create a new snapshot where the specified transaction will be marked as
// in-progress and therefore invisible. For example, if the specified xmin falls between two
// values in the xip list, it will be inserted in order.
func (s pgSnapshot) markInProgress(txid uint64) pgSnapshot {
	if txid >= s.xmax {
		// Nothing to do
		return s
	}

	xipListCopy := make([]uint64, len(s.xipList))
	copy(xipListCopy, s.xipList)

	newSnapshot := pgSnapshot{
		s.xmin,
		s.xmax,
		xipListCopy,
	}

	// Adjust the xmax and running tx if necessary
	if txid < s.xmin {
		// Adjust the xmin and prepend the newly running tx
		newSnapshot.xmin = txid
		newSnapshot.xipList = append([]uint64{txid}, newSnapshot.xipList...)
	} else {
		// Add the newly in-progress xip to the list of in-progress transactions
		pos, found := slices.BinarySearch(newSnapshot.xipList, txid)
		if !found {
			newSnapshot.xipList = slices.Insert(newSnapshot.xipList, pos, txid)
		}
	}

	// Adjust the xmax if necessary
	var numToDrop int
	startingXipLen := len(newSnapshot.xipList)
	for numToDrop = 0; numToDrop < startingXipLen; numToDrop++ {
		// numToDrop should be nonnegative
		uintNumToDrop, _ := safecast.ToUint64(numToDrop)
		if newSnapshot.xipList[startingXipLen-1-numToDrop] != newSnapshot.xmax-uintNumToDrop-1 {
			break
		}
	}

	if numToDrop > 0 {
		newSnapshot.xmax = newSnapshot.xipList[startingXipLen-numToDrop]
		newSnapshot.xipList = newSnapshot.xipList[:startingXipLen-numToDrop]
		if len(newSnapshot.xipList) == 0 {
			newSnapshot.xipList = nil
		}
	}

	return newSnapshot
}

// txVisible will return whether the specified txid has a disposition (i.e. committed or rolled back)
// in the specified snapshot, and is therefore txVisible to transactions using this snapshot.
func (s pgSnapshot) txVisible(txid uint64) bool {
	switch {
	case txid < s.xmin:
		return true
	case txid >= s.xmax:
		return false
	default:
		_, txInProgress := slices.BinarySearch(s.xipList, txid)
		return !txInProgress
	}
}
