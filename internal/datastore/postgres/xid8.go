// Adapted from https://github.com/jackc/pgtype/blob/f59f1408937ef0bed249f2bfbafb77222bb48f65/xid.go

package postgres

import (
	"database/sql/driver"
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"

	"github.com/jackc/pgio"
	"github.com/jackc/pgtype"
)

type pguint64 struct {
	Uint   uint64
	Status pgtype.Status
}

var errUndefined = errors.New("cannot encode status undefined")

// xid8 represents the xid8 postgres type, which is a 64-bit transaction ID that is *NOT*
// subject to transaction wraparound limitations.
// https://www.postgresql.org/docs/current/datatype-oid.html
type xid8 pguint64

func (txid *xid8) Set(src interface{}) error {
	return (*pguint64)(txid).Set(src)
}

func (txid xid8) Get() interface{} {
	return (pguint64)(txid).Get()
}

func (txid *xid8) AssignTo(dst interface{}) error {
	return (*pguint64)(txid).AssignTo(dst)
}

func (txid *xid8) DecodeText(ci *pgtype.ConnInfo, src []byte) error {
	return (*pguint64)(txid).DecodeText(ci, src)
}

func (txid *xid8) DecodeBinary(ci *pgtype.ConnInfo, src []byte) error {
	return (*pguint64)(txid).DecodeBinary(ci, src)
}

func (txid xid8) EncodeText(ci *pgtype.ConnInfo, buf []byte) ([]byte, error) {
	return (pguint64)(txid).EncodeText(ci, buf)
}

func (txid xid8) EncodeBinary(ci *pgtype.ConnInfo, buf []byte) ([]byte, error) {
	return (pguint64)(txid).EncodeBinary(ci, buf)
}

func (txid *xid8) Scan(src interface{}) error {
	return (*pguint64)(txid).Scan(src)
}

func (txid xid8) Value() (driver.Value, error) {
	return (pguint64)(txid).Value()
}

func (txid xid8) String() string {
	switch txid.Status {
	case pgtype.Null:
		return "nil"
	case pgtype.Present:
		return strconv.FormatUint(txid.Uint, 10)
	default:
		return "undefined"
	}
}

func (pui *pguint64) Set(src interface{}) error {
	switch value := src.(type) {
	case int64:
		if value < 0 {
			return fmt.Errorf("%d is less than minimum value for pguint64", value)
		}
		*pui = pguint64{Uint: uint64(value), Status: pgtype.Present}
	case int32:
		if value < 0 {
			return fmt.Errorf("%d is less than minimum value for pguint64", value)
		}
		*pui = pguint64{Uint: uint64(value), Status: pgtype.Present}
	case uint32:
		*pui = pguint64{Uint: uint64(value), Status: pgtype.Present}
	case uint64:
		*pui = pguint64{Uint: value, Status: pgtype.Present}
	default:
		return fmt.Errorf("cannot convert %v to pguint64", value)
	}

	return nil
}

func (pui pguint64) Get() interface{} {
	switch pui.Status {
	case pgtype.Present:
		return pui.Uint
	case pgtype.Null:
		return nil
	default:
		return pui.Status
	}
}

func (pui *pguint64) AssignTo(dst interface{}) error {
	switch v := dst.(type) {
	case *uint64:
		if pui.Status == pgtype.Present {
			*v = pui.Uint
		} else {
			return fmt.Errorf("cannot assign %v into %T", pui, dst)
		}
	case **uint64:
		if pui.Status == pgtype.Present {
			n := pui.Uint
			*v = &n
		} else {
			*v = nil
		}
	}

	return nil
}

func (pui *pguint64) DecodeText(ci *pgtype.ConnInfo, src []byte) error {
	if src == nil {
		*pui = pguint64{Status: pgtype.Null}
		return nil
	}

	n, err := strconv.ParseUint(string(src), 10, 64)
	if err != nil {
		return err
	}

	*pui = pguint64{Uint: n, Status: pgtype.Present}
	return nil
}

func (pui *pguint64) DecodeBinary(ci *pgtype.ConnInfo, src []byte) error {
	if src == nil {
		*pui = pguint64{Status: pgtype.Null}
		return nil
	}

	if len(src) != 8 {
		return fmt.Errorf("invalid length: %v", len(src))
	}

	n := binary.BigEndian.Uint64(src)

	*pui = pguint64{Uint: n, Status: pgtype.Present}
	return nil
}

func (pui pguint64) EncodeText(ci *pgtype.ConnInfo, buf []byte) ([]byte, error) {
	switch pui.Status {
	case pgtype.Null:
		return nil, nil
	case pgtype.Undefined:
		return nil, errUndefined
	}

	return append(buf, strconv.FormatUint(pui.Uint, 10)...), nil
}

func (pui pguint64) EncodeBinary(ci *pgtype.ConnInfo, buf []byte) ([]byte, error) {
	switch pui.Status {
	case pgtype.Null:
		return nil, nil
	case pgtype.Undefined:
		return nil, errUndefined
	}

	return pgio.AppendUint64(buf, pui.Uint), nil
}

func (pui *pguint64) Scan(src interface{}) error {
	if src == nil {
		*pui = pguint64{Status: pgtype.Null}
		return nil
	}

	switch src := src.(type) {
	case uint32:
		*pui = pguint64{Uint: uint64(src), Status: pgtype.Present}
		return nil
	case int64:
		*pui = pguint64{Uint: uint64(src), Status: pgtype.Present}
		return nil
	case uint64:
		*pui = pguint64{Uint: src, Status: pgtype.Present}
		return nil
	case string:
		return pui.DecodeText(nil, []byte(src))
	case []byte:
		srcCopy := make([]byte, len(src))
		copy(srcCopy, src)
		return pui.DecodeText(nil, srcCopy)
	}

	return fmt.Errorf("cannot scan %T", src)
}

func (pui pguint64) Value() (driver.Value, error) {
	switch pui.Status {
	case pgtype.Present:
		return int64(pui.Uint), nil
	case pgtype.Null:
		return nil, nil
	default:
		return nil, errUndefined
	}
}
