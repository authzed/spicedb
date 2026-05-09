package wire

import (
	"errors"

	"github.com/jackc/pgx/v5/pgtype"
)

var ErrUnknownOid = errors.New("unknown oid")

func NewParameter(types *pgtype.Map, format FormatCode, value []byte) Parameter {
	return Parameter{
		types:  types,
		format: format,
		value:  value,
	}
}

type Parameter struct {
	types  *pgtype.Map
	format FormatCode
	value  []byte
}

func (p Parameter) Scan(oid uint32) (any, error) {
	typed, has := p.types.TypeForOID(oid)
	if !has {
		return nil, ErrUnknownOid
	}

	return typed.Codec.DecodeValue(p.types, oid, int16(p.format), p.value)
}

func (p Parameter) Format() FormatCode {
	return p.format
}

func (p Parameter) Value() []byte {
	return p.value
}
