package postgres

import (
	"database/sql/driver"
	"encoding/binary"
	"fmt"
	"strconv"

	"github.com/jackc/pgio"
	"github.com/jackc/pgx/v5/pgtype"
)

// Adapted from https://github.com/jackc/pgx/blob/ca022267dbbfe7a8ba7070557352a5cd08f6cb37/pgtype/uint32.go
type Uint64Scanner interface {
	ScanUint64(v xid8) error
}

type Uint64Valuer interface {
	Uint64Value() (xid8, error)
}

// xid8 is the core type that is used to XID.
type xid8 struct {
	Uint64 uint64
	Valid  bool
}

func newXid8(u uint64) xid8 {
	return xid8{
		Uint64: u,
		Valid:  true,
	}
}

func (n *xid8) ScanUint64(v xid8) error {
	*n = v
	return nil
}

func (n xid8) Uint64Value() (xid8, error) {
	return n, nil
}

// TextValue is the implementation of pgx.TextValuer.
// Achieves the same as the codecs, but slightly more efficient in pgx because it skips reflection
func (n xid8) TextValue() (pgtype.Text, error) {
	return pgtype.Text{
		Valid:  n.Valid,
		String: strconv.FormatUint(n.Uint64, 10),
	}, nil
}

type Uint64Codec struct{}

func (Uint64Codec) FormatSupported(format int16) bool {
	return format == pgtype.TextFormatCode || format == pgtype.BinaryFormatCode
}

func (Uint64Codec) PreferredFormat() int16 {
	return pgtype.BinaryFormatCode
}

func (Uint64Codec) PlanEncode(_ *pgtype.Map, _ uint32, format int16, value any) pgtype.EncodePlan {
	switch format {
	case pgtype.BinaryFormatCode:
		switch value.(type) {
		case uint64:
			return encodePlanUint64CodecBinaryUint64{}
		case Uint64Valuer:
			return encodePlanUint64CodecBinaryUint64Valuer{}
		}
	case pgtype.TextFormatCode:
		switch value.(type) {
		case uint64:
			return encodePlanUint64CodecTextUint64{}
		}
	}

	return nil
}

type encodePlanUint64CodecBinaryUint64 struct{}

func (encodePlanUint64CodecBinaryUint64) Encode(value any, buf []byte) (newBuf []byte, err error) {
	v := value.(uint64)
	return pgio.AppendUint64(buf, v), nil
}

type encodePlanUint64CodecBinaryUint64Valuer struct{}

func (encodePlanUint64CodecBinaryUint64Valuer) Encode(value any, buf []byte) (newBuf []byte, err error) {
	v, err := value.(Uint64Valuer).Uint64Value()
	if err != nil {
		return nil, err
	}

	if !v.Valid {
		return nil, nil
	}

	return pgio.AppendUint64(buf, v.Uint64), nil
}

type encodePlanUint64CodecTextUint64 struct{}

func (encodePlanUint64CodecTextUint64) Encode(value any, buf []byte) (newBuf []byte, err error) {
	v := value.(uint64)
	return append(buf, strconv.FormatUint(v, 10)...), nil
}

func (Uint64Codec) PlanScan(_ *pgtype.Map, _ uint32, format int16, target any) pgtype.ScanPlan {
	switch format {
	case pgtype.BinaryFormatCode:
		switch target.(type) {
		case *uint64:
			return scanPlanBinaryUint64ToUint64{}
		case Uint64Scanner:
			return scanPlanBinaryUint64ToUint64Scanner{}
		}
	case pgtype.TextFormatCode:
		switch target.(type) {
		case *uint64:
			return scanPlanTextAnyToUint64{}
		case Uint64Scanner:
			return scanPlanTextAnyToUint64Scanner{}
		}
	}

	return nil
}

func (c Uint64Codec) DecodeDatabaseSQLValue(m *pgtype.Map, oid uint32, format int16, src []byte) (driver.Value, error) {
	if src == nil {
		return nil, nil
	}

	var n uint64
	err := codecScan(c, m, oid, format, src, &n)
	if err != nil {
		return nil, err
	}
	return n, nil
}

func (c Uint64Codec) DecodeValue(m *pgtype.Map, oid uint32, format int16, src []byte) (any, error) {
	if src == nil {
		return nil, nil
	}

	var n uint64
	err := codecScan(c, m, oid, format, src, &n)
	if err != nil {
		return nil, err
	}
	return n, nil
}

type scanPlanBinaryUint64ToUint64 struct{}

func (scanPlanBinaryUint64ToUint64) Scan(src []byte, dst any) error {
	if src == nil {
		return fmt.Errorf("cannot scan NULL into %T", dst)
	}

	if len(src) != 8 {
		return fmt.Errorf("invalid length for uint64: %v", len(src))
	}

	p := (dst).(*uint64)
	*p = binary.BigEndian.Uint64(src)

	return nil
}

type scanPlanBinaryUint64ToUint64Scanner struct{}

func (scanPlanBinaryUint64ToUint64Scanner) Scan(src []byte, dst any) error {
	s, ok := (dst).(Uint64Scanner)
	if !ok {
		return pgtype.ErrScanTargetTypeChanged
	}

	if src == nil {
		return s.ScanUint64(xid8{})
	}

	if len(src) != 8 {
		return fmt.Errorf("invalid length for uint64: %v", len(src))
	}

	n := binary.BigEndian.Uint64(src)

	return s.ScanUint64(xid8{Uint64: n, Valid: true})
}

type scanPlanTextAnyToUint64Scanner struct{}

func (scanPlanTextAnyToUint64Scanner) Scan(src []byte, dst any) error {
	s, ok := (dst).(Uint64Scanner)
	if !ok {
		return pgtype.ErrScanTargetTypeChanged
	}

	if src == nil {
		return s.ScanUint64(xid8{})
	}

	n, err := strconv.ParseUint(string(src), 10, 64)
	if err != nil {
		return err
	}

	return s.ScanUint64(xid8{Uint64: n, Valid: true})
}

func codecScan(codec pgtype.Codec, m *pgtype.Map, oid uint32, format int16, src []byte, dst any) error {
	scanPlan := codec.PlanScan(m, oid, format, dst)
	if scanPlan == nil {
		return fmt.Errorf("PlanScan did not find a plan")
	}
	return scanPlan.Scan(src, dst)
}

type scanPlanTextAnyToUint64 struct{}

func (scanPlanTextAnyToUint64) Scan(src []byte, dst any) error {
	if src == nil {
		return fmt.Errorf("cannot scan NULL into %T", dst)
	}

	p, ok := (dst).(*uint64)
	if !ok {
		return pgtype.ErrScanTargetTypeChanged
	}

	n, err := strconv.ParseUint(string(src), 10, 32)
	if err != nil {
		return err
	}

	*p = n
	return nil
}
