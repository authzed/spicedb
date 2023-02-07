// Adapted from https://github.com/jackc/pgtype/blob/f59f1408937ef0bed249f2bfbafb77222bb48f65/xid.go

package postgres

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/jackc/pgtype"
	"golang.org/x/exp/slices"
)

type pgSnapshot struct {
	xmin, xmax uint64
	xipList    []uint64 // Must always be sorted

	status pgtype.Status
}

func (s *pgSnapshot) DecodeText(ci *pgtype.ConnInfo, src []byte) error {
	s.status = pgtype.Undefined
	asText := string(src)
	components := strings.SplitN(asText, ":", 3)
	if len(components) != 3 {
		return fmt.Errorf("wrong number of snapshot components: %s", asText)
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
		slices.Sort(s.xipList)
	} else {
		s.xipList = nil
	}

	// Parsed successfully
	s.status = pgtype.Present
	return nil
}

// EncodeText should append the text format of self to buf. If self is the
// SQL value NULL then append nothing and return (nil, nil). The caller of
// EncodeText is responsible for writing the correct NULL value or the
// length of the data written.
func (s pgSnapshot) EncodeText(ci *pgtype.ConnInfo, buf []byte) ([]byte, error) {
	if s.status == pgtype.Null {
		return nil, nil
	}

	return append(buf, []byte(s.String())...), nil
}

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
	uncomparable comparisonResult = iota
	equal
	lt
	gt
	concurrent
)

func (s pgSnapshot) compare(rhs pgSnapshot) comparisonResult {
	if s.status != pgtype.Present || rhs.status != pgtype.Present {
		return uncomparable
	}

	var rhsHasMoreInfo bool
	for _, txid := range append(s.xipList, s.xmax) {
		if rhs.visible(txid) {
			rhsHasMoreInfo = true
			break
		}
	}

	var lhsHasMoreInfo bool
	for _, txid := range append(rhs.xipList, rhs.xmax) {
		if s.visible(txid) {
			lhsHasMoreInfo = true
			break
		}
	}

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

func (s pgSnapshot) markComplete(txid uint64) pgSnapshot {
	if txid < s.xmin || s.status != pgtype.Present {
		// Nothing to do
		return s
	}

	newSnapshot := pgSnapshot{
		s.xmin,
		s.xmax,
		s.xipList,
		pgtype.Present,
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

func (s pgSnapshot) markInProgress(txid uint64) pgSnapshot {
	if txid >= s.xmax {
		// Nothing to do
		return s
	}

	newSnapshot := pgSnapshot{
		s.xmin,
		s.xmax,
		s.xipList,
		pgtype.Present,
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
		if newSnapshot.xipList[startingXipLen-1-numToDrop] != newSnapshot.xmax-uint64(numToDrop)-1 {
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

func (s pgSnapshot) visible(txid uint64) bool {
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
