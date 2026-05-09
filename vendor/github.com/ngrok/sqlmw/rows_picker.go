// Code generated using tool/rows_picker_gen.go DO NOT EDIT.
// Date: Dec 20 09:54:15

package sqlmw

import (
	"context"
	"database/sql/driver"
)

const (
	rowsNextResultSet = 1 << iota
	rowsColumnTypeDatabaseTypeName
	rowsColumnTypeLength
	rowsColumnTypeNullable
	rowsColumnTypePrecisionScale
	rowsColumnTypeScanType
)

var pickRows = make([]func(*wrappedRows) driver.Rows, 64)

func init() {

	// plain driver.Rows
	pickRows[0] = func(r *wrappedRows) driver.Rows {
		return r
	}

	// plain driver.Rows
	pickRows[1] = func(r *wrappedRows) driver.Rows {
		return struct {
			*wrappedRows
			wrappedRowsNextResultSet
		}{
			r,
			wrappedRowsNextResultSet{r.parent},
		}
	}

	// plain driver.Rows
	pickRows[2] = func(r *wrappedRows) driver.Rows {
		return struct {
			*wrappedRows
			wrappedRowsColumnTypeDatabaseTypeName
		}{
			r,
			wrappedRowsColumnTypeDatabaseTypeName{r.parent},
		}
	}

	// plain driver.Rows
	pickRows[3] = func(r *wrappedRows) driver.Rows {
		return struct {
			*wrappedRows
			wrappedRowsNextResultSet
			wrappedRowsColumnTypeDatabaseTypeName
		}{
			r,
			wrappedRowsNextResultSet{r.parent},
			wrappedRowsColumnTypeDatabaseTypeName{r.parent},
		}
	}

	// plain driver.Rows
	pickRows[4] = func(r *wrappedRows) driver.Rows {
		return struct {
			*wrappedRows
			wrappedRowsColumnTypeLength
		}{
			r,
			wrappedRowsColumnTypeLength{r.parent},
		}
	}

	// plain driver.Rows
	pickRows[5] = func(r *wrappedRows) driver.Rows {
		return struct {
			*wrappedRows
			wrappedRowsNextResultSet
			wrappedRowsColumnTypeLength
		}{
			r,
			wrappedRowsNextResultSet{r.parent},
			wrappedRowsColumnTypeLength{r.parent},
		}
	}

	// plain driver.Rows
	pickRows[6] = func(r *wrappedRows) driver.Rows {
		return struct {
			*wrappedRows
			wrappedRowsColumnTypeDatabaseTypeName
			wrappedRowsColumnTypeLength
		}{
			r,
			wrappedRowsColumnTypeDatabaseTypeName{r.parent},
			wrappedRowsColumnTypeLength{r.parent},
		}
	}

	// plain driver.Rows
	pickRows[7] = func(r *wrappedRows) driver.Rows {
		return struct {
			*wrappedRows
			wrappedRowsNextResultSet
			wrappedRowsColumnTypeDatabaseTypeName
			wrappedRowsColumnTypeLength
		}{
			r,
			wrappedRowsNextResultSet{r.parent},
			wrappedRowsColumnTypeDatabaseTypeName{r.parent},
			wrappedRowsColumnTypeLength{r.parent},
		}
	}

	// plain driver.Rows
	pickRows[8] = func(r *wrappedRows) driver.Rows {
		return struct {
			*wrappedRows
			wrappedRowsColumnTypeNullable
		}{
			r,
			wrappedRowsColumnTypeNullable{r.parent},
		}
	}

	// plain driver.Rows
	pickRows[9] = func(r *wrappedRows) driver.Rows {
		return struct {
			*wrappedRows
			wrappedRowsNextResultSet
			wrappedRowsColumnTypeNullable
		}{
			r,
			wrappedRowsNextResultSet{r.parent},
			wrappedRowsColumnTypeNullable{r.parent},
		}
	}

	// plain driver.Rows
	pickRows[10] = func(r *wrappedRows) driver.Rows {
		return struct {
			*wrappedRows
			wrappedRowsColumnTypeDatabaseTypeName
			wrappedRowsColumnTypeNullable
		}{
			r,
			wrappedRowsColumnTypeDatabaseTypeName{r.parent},
			wrappedRowsColumnTypeNullable{r.parent},
		}
	}

	// plain driver.Rows
	pickRows[11] = func(r *wrappedRows) driver.Rows {
		return struct {
			*wrappedRows
			wrappedRowsNextResultSet
			wrappedRowsColumnTypeDatabaseTypeName
			wrappedRowsColumnTypeNullable
		}{
			r,
			wrappedRowsNextResultSet{r.parent},
			wrappedRowsColumnTypeDatabaseTypeName{r.parent},
			wrappedRowsColumnTypeNullable{r.parent},
		}
	}

	// plain driver.Rows
	pickRows[12] = func(r *wrappedRows) driver.Rows {
		return struct {
			*wrappedRows
			wrappedRowsColumnTypeLength
			wrappedRowsColumnTypeNullable
		}{
			r,
			wrappedRowsColumnTypeLength{r.parent},
			wrappedRowsColumnTypeNullable{r.parent},
		}
	}

	// plain driver.Rows
	pickRows[13] = func(r *wrappedRows) driver.Rows {
		return struct {
			*wrappedRows
			wrappedRowsNextResultSet
			wrappedRowsColumnTypeLength
			wrappedRowsColumnTypeNullable
		}{
			r,
			wrappedRowsNextResultSet{r.parent},
			wrappedRowsColumnTypeLength{r.parent},
			wrappedRowsColumnTypeNullable{r.parent},
		}
	}

	// plain driver.Rows
	pickRows[14] = func(r *wrappedRows) driver.Rows {
		return struct {
			*wrappedRows
			wrappedRowsColumnTypeDatabaseTypeName
			wrappedRowsColumnTypeLength
			wrappedRowsColumnTypeNullable
		}{
			r,
			wrappedRowsColumnTypeDatabaseTypeName{r.parent},
			wrappedRowsColumnTypeLength{r.parent},
			wrappedRowsColumnTypeNullable{r.parent},
		}
	}

	// plain driver.Rows
	pickRows[15] = func(r *wrappedRows) driver.Rows {
		return struct {
			*wrappedRows
			wrappedRowsNextResultSet
			wrappedRowsColumnTypeDatabaseTypeName
			wrappedRowsColumnTypeLength
			wrappedRowsColumnTypeNullable
		}{
			r,
			wrappedRowsNextResultSet{r.parent},
			wrappedRowsColumnTypeDatabaseTypeName{r.parent},
			wrappedRowsColumnTypeLength{r.parent},
			wrappedRowsColumnTypeNullable{r.parent},
		}
	}

	// plain driver.Rows
	pickRows[16] = func(r *wrappedRows) driver.Rows {
		return struct {
			*wrappedRows
			wrappedRowsColumnTypePrecisionScale
		}{
			r,
			wrappedRowsColumnTypePrecisionScale{r.parent},
		}
	}

	// plain driver.Rows
	pickRows[17] = func(r *wrappedRows) driver.Rows {
		return struct {
			*wrappedRows
			wrappedRowsNextResultSet
			wrappedRowsColumnTypePrecisionScale
		}{
			r,
			wrappedRowsNextResultSet{r.parent},
			wrappedRowsColumnTypePrecisionScale{r.parent},
		}
	}

	// plain driver.Rows
	pickRows[18] = func(r *wrappedRows) driver.Rows {
		return struct {
			*wrappedRows
			wrappedRowsColumnTypeDatabaseTypeName
			wrappedRowsColumnTypePrecisionScale
		}{
			r,
			wrappedRowsColumnTypeDatabaseTypeName{r.parent},
			wrappedRowsColumnTypePrecisionScale{r.parent},
		}
	}

	// plain driver.Rows
	pickRows[19] = func(r *wrappedRows) driver.Rows {
		return struct {
			*wrappedRows
			wrappedRowsNextResultSet
			wrappedRowsColumnTypeDatabaseTypeName
			wrappedRowsColumnTypePrecisionScale
		}{
			r,
			wrappedRowsNextResultSet{r.parent},
			wrappedRowsColumnTypeDatabaseTypeName{r.parent},
			wrappedRowsColumnTypePrecisionScale{r.parent},
		}
	}

	// plain driver.Rows
	pickRows[20] = func(r *wrappedRows) driver.Rows {
		return struct {
			*wrappedRows
			wrappedRowsColumnTypeLength
			wrappedRowsColumnTypePrecisionScale
		}{
			r,
			wrappedRowsColumnTypeLength{r.parent},
			wrappedRowsColumnTypePrecisionScale{r.parent},
		}
	}

	// plain driver.Rows
	pickRows[21] = func(r *wrappedRows) driver.Rows {
		return struct {
			*wrappedRows
			wrappedRowsNextResultSet
			wrappedRowsColumnTypeLength
			wrappedRowsColumnTypePrecisionScale
		}{
			r,
			wrappedRowsNextResultSet{r.parent},
			wrappedRowsColumnTypeLength{r.parent},
			wrappedRowsColumnTypePrecisionScale{r.parent},
		}
	}

	// plain driver.Rows
	pickRows[22] = func(r *wrappedRows) driver.Rows {
		return struct {
			*wrappedRows
			wrappedRowsColumnTypeDatabaseTypeName
			wrappedRowsColumnTypeLength
			wrappedRowsColumnTypePrecisionScale
		}{
			r,
			wrappedRowsColumnTypeDatabaseTypeName{r.parent},
			wrappedRowsColumnTypeLength{r.parent},
			wrappedRowsColumnTypePrecisionScale{r.parent},
		}
	}

	// plain driver.Rows
	pickRows[23] = func(r *wrappedRows) driver.Rows {
		return struct {
			*wrappedRows
			wrappedRowsNextResultSet
			wrappedRowsColumnTypeDatabaseTypeName
			wrappedRowsColumnTypeLength
			wrappedRowsColumnTypePrecisionScale
		}{
			r,
			wrappedRowsNextResultSet{r.parent},
			wrappedRowsColumnTypeDatabaseTypeName{r.parent},
			wrappedRowsColumnTypeLength{r.parent},
			wrappedRowsColumnTypePrecisionScale{r.parent},
		}
	}

	// plain driver.Rows
	pickRows[24] = func(r *wrappedRows) driver.Rows {
		return struct {
			*wrappedRows
			wrappedRowsColumnTypeNullable
			wrappedRowsColumnTypePrecisionScale
		}{
			r,
			wrappedRowsColumnTypeNullable{r.parent},
			wrappedRowsColumnTypePrecisionScale{r.parent},
		}
	}

	// plain driver.Rows
	pickRows[25] = func(r *wrappedRows) driver.Rows {
		return struct {
			*wrappedRows
			wrappedRowsNextResultSet
			wrappedRowsColumnTypeNullable
			wrappedRowsColumnTypePrecisionScale
		}{
			r,
			wrappedRowsNextResultSet{r.parent},
			wrappedRowsColumnTypeNullable{r.parent},
			wrappedRowsColumnTypePrecisionScale{r.parent},
		}
	}

	// plain driver.Rows
	pickRows[26] = func(r *wrappedRows) driver.Rows {
		return struct {
			*wrappedRows
			wrappedRowsColumnTypeDatabaseTypeName
			wrappedRowsColumnTypeNullable
			wrappedRowsColumnTypePrecisionScale
		}{
			r,
			wrappedRowsColumnTypeDatabaseTypeName{r.parent},
			wrappedRowsColumnTypeNullable{r.parent},
			wrappedRowsColumnTypePrecisionScale{r.parent},
		}
	}

	// plain driver.Rows
	pickRows[27] = func(r *wrappedRows) driver.Rows {
		return struct {
			*wrappedRows
			wrappedRowsNextResultSet
			wrappedRowsColumnTypeDatabaseTypeName
			wrappedRowsColumnTypeNullable
			wrappedRowsColumnTypePrecisionScale
		}{
			r,
			wrappedRowsNextResultSet{r.parent},
			wrappedRowsColumnTypeDatabaseTypeName{r.parent},
			wrappedRowsColumnTypeNullable{r.parent},
			wrappedRowsColumnTypePrecisionScale{r.parent},
		}
	}

	// plain driver.Rows
	pickRows[28] = func(r *wrappedRows) driver.Rows {
		return struct {
			*wrappedRows
			wrappedRowsColumnTypeLength
			wrappedRowsColumnTypeNullable
			wrappedRowsColumnTypePrecisionScale
		}{
			r,
			wrappedRowsColumnTypeLength{r.parent},
			wrappedRowsColumnTypeNullable{r.parent},
			wrappedRowsColumnTypePrecisionScale{r.parent},
		}
	}

	// plain driver.Rows
	pickRows[29] = func(r *wrappedRows) driver.Rows {
		return struct {
			*wrappedRows
			wrappedRowsNextResultSet
			wrappedRowsColumnTypeLength
			wrappedRowsColumnTypeNullable
			wrappedRowsColumnTypePrecisionScale
		}{
			r,
			wrappedRowsNextResultSet{r.parent},
			wrappedRowsColumnTypeLength{r.parent},
			wrappedRowsColumnTypeNullable{r.parent},
			wrappedRowsColumnTypePrecisionScale{r.parent},
		}
	}

	// plain driver.Rows
	pickRows[30] = func(r *wrappedRows) driver.Rows {
		return struct {
			*wrappedRows
			wrappedRowsColumnTypeDatabaseTypeName
			wrappedRowsColumnTypeLength
			wrappedRowsColumnTypeNullable
			wrappedRowsColumnTypePrecisionScale
		}{
			r,
			wrappedRowsColumnTypeDatabaseTypeName{r.parent},
			wrappedRowsColumnTypeLength{r.parent},
			wrappedRowsColumnTypeNullable{r.parent},
			wrappedRowsColumnTypePrecisionScale{r.parent},
		}
	}

	// plain driver.Rows
	pickRows[31] = func(r *wrappedRows) driver.Rows {
		return struct {
			*wrappedRows
			wrappedRowsNextResultSet
			wrappedRowsColumnTypeDatabaseTypeName
			wrappedRowsColumnTypeLength
			wrappedRowsColumnTypeNullable
			wrappedRowsColumnTypePrecisionScale
		}{
			r,
			wrappedRowsNextResultSet{r.parent},
			wrappedRowsColumnTypeDatabaseTypeName{r.parent},
			wrappedRowsColumnTypeLength{r.parent},
			wrappedRowsColumnTypeNullable{r.parent},
			wrappedRowsColumnTypePrecisionScale{r.parent},
		}
	}

	// plain driver.Rows
	pickRows[32] = func(r *wrappedRows) driver.Rows {
		return struct {
			*wrappedRows
			wrappedRowsColumnTypeScanType
		}{
			r,
			wrappedRowsColumnTypeScanType{r.parent},
		}
	}

	// plain driver.Rows
	pickRows[33] = func(r *wrappedRows) driver.Rows {
		return struct {
			*wrappedRows
			wrappedRowsNextResultSet
			wrappedRowsColumnTypeScanType
		}{
			r,
			wrappedRowsNextResultSet{r.parent},
			wrappedRowsColumnTypeScanType{r.parent},
		}
	}

	// plain driver.Rows
	pickRows[34] = func(r *wrappedRows) driver.Rows {
		return struct {
			*wrappedRows
			wrappedRowsColumnTypeDatabaseTypeName
			wrappedRowsColumnTypeScanType
		}{
			r,
			wrappedRowsColumnTypeDatabaseTypeName{r.parent},
			wrappedRowsColumnTypeScanType{r.parent},
		}
	}

	// plain driver.Rows
	pickRows[35] = func(r *wrappedRows) driver.Rows {
		return struct {
			*wrappedRows
			wrappedRowsNextResultSet
			wrappedRowsColumnTypeDatabaseTypeName
			wrappedRowsColumnTypeScanType
		}{
			r,
			wrappedRowsNextResultSet{r.parent},
			wrappedRowsColumnTypeDatabaseTypeName{r.parent},
			wrappedRowsColumnTypeScanType{r.parent},
		}
	}

	// plain driver.Rows
	pickRows[36] = func(r *wrappedRows) driver.Rows {
		return struct {
			*wrappedRows
			wrappedRowsColumnTypeLength
			wrappedRowsColumnTypeScanType
		}{
			r,
			wrappedRowsColumnTypeLength{r.parent},
			wrappedRowsColumnTypeScanType{r.parent},
		}
	}

	// plain driver.Rows
	pickRows[37] = func(r *wrappedRows) driver.Rows {
		return struct {
			*wrappedRows
			wrappedRowsNextResultSet
			wrappedRowsColumnTypeLength
			wrappedRowsColumnTypeScanType
		}{
			r,
			wrappedRowsNextResultSet{r.parent},
			wrappedRowsColumnTypeLength{r.parent},
			wrappedRowsColumnTypeScanType{r.parent},
		}
	}

	// plain driver.Rows
	pickRows[38] = func(r *wrappedRows) driver.Rows {
		return struct {
			*wrappedRows
			wrappedRowsColumnTypeDatabaseTypeName
			wrappedRowsColumnTypeLength
			wrappedRowsColumnTypeScanType
		}{
			r,
			wrappedRowsColumnTypeDatabaseTypeName{r.parent},
			wrappedRowsColumnTypeLength{r.parent},
			wrappedRowsColumnTypeScanType{r.parent},
		}
	}

	// plain driver.Rows
	pickRows[39] = func(r *wrappedRows) driver.Rows {
		return struct {
			*wrappedRows
			wrappedRowsNextResultSet
			wrappedRowsColumnTypeDatabaseTypeName
			wrappedRowsColumnTypeLength
			wrappedRowsColumnTypeScanType
		}{
			r,
			wrappedRowsNextResultSet{r.parent},
			wrappedRowsColumnTypeDatabaseTypeName{r.parent},
			wrappedRowsColumnTypeLength{r.parent},
			wrappedRowsColumnTypeScanType{r.parent},
		}
	}

	// plain driver.Rows
	pickRows[40] = func(r *wrappedRows) driver.Rows {
		return struct {
			*wrappedRows
			wrappedRowsColumnTypeNullable
			wrappedRowsColumnTypeScanType
		}{
			r,
			wrappedRowsColumnTypeNullable{r.parent},
			wrappedRowsColumnTypeScanType{r.parent},
		}
	}

	// plain driver.Rows
	pickRows[41] = func(r *wrappedRows) driver.Rows {
		return struct {
			*wrappedRows
			wrappedRowsNextResultSet
			wrappedRowsColumnTypeNullable
			wrappedRowsColumnTypeScanType
		}{
			r,
			wrappedRowsNextResultSet{r.parent},
			wrappedRowsColumnTypeNullable{r.parent},
			wrappedRowsColumnTypeScanType{r.parent},
		}
	}

	// plain driver.Rows
	pickRows[42] = func(r *wrappedRows) driver.Rows {
		return struct {
			*wrappedRows
			wrappedRowsColumnTypeDatabaseTypeName
			wrappedRowsColumnTypeNullable
			wrappedRowsColumnTypeScanType
		}{
			r,
			wrappedRowsColumnTypeDatabaseTypeName{r.parent},
			wrappedRowsColumnTypeNullable{r.parent},
			wrappedRowsColumnTypeScanType{r.parent},
		}
	}

	// plain driver.Rows
	pickRows[43] = func(r *wrappedRows) driver.Rows {
		return struct {
			*wrappedRows
			wrappedRowsNextResultSet
			wrappedRowsColumnTypeDatabaseTypeName
			wrappedRowsColumnTypeNullable
			wrappedRowsColumnTypeScanType
		}{
			r,
			wrappedRowsNextResultSet{r.parent},
			wrappedRowsColumnTypeDatabaseTypeName{r.parent},
			wrappedRowsColumnTypeNullable{r.parent},
			wrappedRowsColumnTypeScanType{r.parent},
		}
	}

	// plain driver.Rows
	pickRows[44] = func(r *wrappedRows) driver.Rows {
		return struct {
			*wrappedRows
			wrappedRowsColumnTypeLength
			wrappedRowsColumnTypeNullable
			wrappedRowsColumnTypeScanType
		}{
			r,
			wrappedRowsColumnTypeLength{r.parent},
			wrappedRowsColumnTypeNullable{r.parent},
			wrappedRowsColumnTypeScanType{r.parent},
		}
	}

	// plain driver.Rows
	pickRows[45] = func(r *wrappedRows) driver.Rows {
		return struct {
			*wrappedRows
			wrappedRowsNextResultSet
			wrappedRowsColumnTypeLength
			wrappedRowsColumnTypeNullable
			wrappedRowsColumnTypeScanType
		}{
			r,
			wrappedRowsNextResultSet{r.parent},
			wrappedRowsColumnTypeLength{r.parent},
			wrappedRowsColumnTypeNullable{r.parent},
			wrappedRowsColumnTypeScanType{r.parent},
		}
	}

	// plain driver.Rows
	pickRows[46] = func(r *wrappedRows) driver.Rows {
		return struct {
			*wrappedRows
			wrappedRowsColumnTypeDatabaseTypeName
			wrappedRowsColumnTypeLength
			wrappedRowsColumnTypeNullable
			wrappedRowsColumnTypeScanType
		}{
			r,
			wrappedRowsColumnTypeDatabaseTypeName{r.parent},
			wrappedRowsColumnTypeLength{r.parent},
			wrappedRowsColumnTypeNullable{r.parent},
			wrappedRowsColumnTypeScanType{r.parent},
		}
	}

	// plain driver.Rows
	pickRows[47] = func(r *wrappedRows) driver.Rows {
		return struct {
			*wrappedRows
			wrappedRowsNextResultSet
			wrappedRowsColumnTypeDatabaseTypeName
			wrappedRowsColumnTypeLength
			wrappedRowsColumnTypeNullable
			wrappedRowsColumnTypeScanType
		}{
			r,
			wrappedRowsNextResultSet{r.parent},
			wrappedRowsColumnTypeDatabaseTypeName{r.parent},
			wrappedRowsColumnTypeLength{r.parent},
			wrappedRowsColumnTypeNullable{r.parent},
			wrappedRowsColumnTypeScanType{r.parent},
		}
	}

	// plain driver.Rows
	pickRows[48] = func(r *wrappedRows) driver.Rows {
		return struct {
			*wrappedRows
			wrappedRowsColumnTypePrecisionScale
			wrappedRowsColumnTypeScanType
		}{
			r,
			wrappedRowsColumnTypePrecisionScale{r.parent},
			wrappedRowsColumnTypeScanType{r.parent},
		}
	}

	// plain driver.Rows
	pickRows[49] = func(r *wrappedRows) driver.Rows {
		return struct {
			*wrappedRows
			wrappedRowsNextResultSet
			wrappedRowsColumnTypePrecisionScale
			wrappedRowsColumnTypeScanType
		}{
			r,
			wrappedRowsNextResultSet{r.parent},
			wrappedRowsColumnTypePrecisionScale{r.parent},
			wrappedRowsColumnTypeScanType{r.parent},
		}
	}

	// plain driver.Rows
	pickRows[50] = func(r *wrappedRows) driver.Rows {
		return struct {
			*wrappedRows
			wrappedRowsColumnTypeDatabaseTypeName
			wrappedRowsColumnTypePrecisionScale
			wrappedRowsColumnTypeScanType
		}{
			r,
			wrappedRowsColumnTypeDatabaseTypeName{r.parent},
			wrappedRowsColumnTypePrecisionScale{r.parent},
			wrappedRowsColumnTypeScanType{r.parent},
		}
	}

	// plain driver.Rows
	pickRows[51] = func(r *wrappedRows) driver.Rows {
		return struct {
			*wrappedRows
			wrappedRowsNextResultSet
			wrappedRowsColumnTypeDatabaseTypeName
			wrappedRowsColumnTypePrecisionScale
			wrappedRowsColumnTypeScanType
		}{
			r,
			wrappedRowsNextResultSet{r.parent},
			wrappedRowsColumnTypeDatabaseTypeName{r.parent},
			wrappedRowsColumnTypePrecisionScale{r.parent},
			wrappedRowsColumnTypeScanType{r.parent},
		}
	}

	// plain driver.Rows
	pickRows[52] = func(r *wrappedRows) driver.Rows {
		return struct {
			*wrappedRows
			wrappedRowsColumnTypeLength
			wrappedRowsColumnTypePrecisionScale
			wrappedRowsColumnTypeScanType
		}{
			r,
			wrappedRowsColumnTypeLength{r.parent},
			wrappedRowsColumnTypePrecisionScale{r.parent},
			wrappedRowsColumnTypeScanType{r.parent},
		}
	}

	// plain driver.Rows
	pickRows[53] = func(r *wrappedRows) driver.Rows {
		return struct {
			*wrappedRows
			wrappedRowsNextResultSet
			wrappedRowsColumnTypeLength
			wrappedRowsColumnTypePrecisionScale
			wrappedRowsColumnTypeScanType
		}{
			r,
			wrappedRowsNextResultSet{r.parent},
			wrappedRowsColumnTypeLength{r.parent},
			wrappedRowsColumnTypePrecisionScale{r.parent},
			wrappedRowsColumnTypeScanType{r.parent},
		}
	}

	// plain driver.Rows
	pickRows[54] = func(r *wrappedRows) driver.Rows {
		return struct {
			*wrappedRows
			wrappedRowsColumnTypeDatabaseTypeName
			wrappedRowsColumnTypeLength
			wrappedRowsColumnTypePrecisionScale
			wrappedRowsColumnTypeScanType
		}{
			r,
			wrappedRowsColumnTypeDatabaseTypeName{r.parent},
			wrappedRowsColumnTypeLength{r.parent},
			wrappedRowsColumnTypePrecisionScale{r.parent},
			wrappedRowsColumnTypeScanType{r.parent},
		}
	}

	// plain driver.Rows
	pickRows[55] = func(r *wrappedRows) driver.Rows {
		return struct {
			*wrappedRows
			wrappedRowsNextResultSet
			wrappedRowsColumnTypeDatabaseTypeName
			wrappedRowsColumnTypeLength
			wrappedRowsColumnTypePrecisionScale
			wrappedRowsColumnTypeScanType
		}{
			r,
			wrappedRowsNextResultSet{r.parent},
			wrappedRowsColumnTypeDatabaseTypeName{r.parent},
			wrappedRowsColumnTypeLength{r.parent},
			wrappedRowsColumnTypePrecisionScale{r.parent},
			wrappedRowsColumnTypeScanType{r.parent},
		}
	}

	// plain driver.Rows
	pickRows[56] = func(r *wrappedRows) driver.Rows {
		return struct {
			*wrappedRows
			wrappedRowsColumnTypeNullable
			wrappedRowsColumnTypePrecisionScale
			wrappedRowsColumnTypeScanType
		}{
			r,
			wrappedRowsColumnTypeNullable{r.parent},
			wrappedRowsColumnTypePrecisionScale{r.parent},
			wrappedRowsColumnTypeScanType{r.parent},
		}
	}

	// plain driver.Rows
	pickRows[57] = func(r *wrappedRows) driver.Rows {
		return struct {
			*wrappedRows
			wrappedRowsNextResultSet
			wrappedRowsColumnTypeNullable
			wrappedRowsColumnTypePrecisionScale
			wrappedRowsColumnTypeScanType
		}{
			r,
			wrappedRowsNextResultSet{r.parent},
			wrappedRowsColumnTypeNullable{r.parent},
			wrappedRowsColumnTypePrecisionScale{r.parent},
			wrappedRowsColumnTypeScanType{r.parent},
		}
	}

	// plain driver.Rows
	pickRows[58] = func(r *wrappedRows) driver.Rows {
		return struct {
			*wrappedRows
			wrappedRowsColumnTypeDatabaseTypeName
			wrappedRowsColumnTypeNullable
			wrappedRowsColumnTypePrecisionScale
			wrappedRowsColumnTypeScanType
		}{
			r,
			wrappedRowsColumnTypeDatabaseTypeName{r.parent},
			wrappedRowsColumnTypeNullable{r.parent},
			wrappedRowsColumnTypePrecisionScale{r.parent},
			wrappedRowsColumnTypeScanType{r.parent},
		}
	}

	// plain driver.Rows
	pickRows[59] = func(r *wrappedRows) driver.Rows {
		return struct {
			*wrappedRows
			wrappedRowsNextResultSet
			wrappedRowsColumnTypeDatabaseTypeName
			wrappedRowsColumnTypeNullable
			wrappedRowsColumnTypePrecisionScale
			wrappedRowsColumnTypeScanType
		}{
			r,
			wrappedRowsNextResultSet{r.parent},
			wrappedRowsColumnTypeDatabaseTypeName{r.parent},
			wrappedRowsColumnTypeNullable{r.parent},
			wrappedRowsColumnTypePrecisionScale{r.parent},
			wrappedRowsColumnTypeScanType{r.parent},
		}
	}

	// plain driver.Rows
	pickRows[60] = func(r *wrappedRows) driver.Rows {
		return struct {
			*wrappedRows
			wrappedRowsColumnTypeLength
			wrappedRowsColumnTypeNullable
			wrappedRowsColumnTypePrecisionScale
			wrappedRowsColumnTypeScanType
		}{
			r,
			wrappedRowsColumnTypeLength{r.parent},
			wrappedRowsColumnTypeNullable{r.parent},
			wrappedRowsColumnTypePrecisionScale{r.parent},
			wrappedRowsColumnTypeScanType{r.parent},
		}
	}

	// plain driver.Rows
	pickRows[61] = func(r *wrappedRows) driver.Rows {
		return struct {
			*wrappedRows
			wrappedRowsNextResultSet
			wrappedRowsColumnTypeLength
			wrappedRowsColumnTypeNullable
			wrappedRowsColumnTypePrecisionScale
			wrappedRowsColumnTypeScanType
		}{
			r,
			wrappedRowsNextResultSet{r.parent},
			wrappedRowsColumnTypeLength{r.parent},
			wrappedRowsColumnTypeNullable{r.parent},
			wrappedRowsColumnTypePrecisionScale{r.parent},
			wrappedRowsColumnTypeScanType{r.parent},
		}
	}

	// plain driver.Rows
	pickRows[62] = func(r *wrappedRows) driver.Rows {
		return struct {
			*wrappedRows
			wrappedRowsColumnTypeDatabaseTypeName
			wrappedRowsColumnTypeLength
			wrappedRowsColumnTypeNullable
			wrappedRowsColumnTypePrecisionScale
			wrappedRowsColumnTypeScanType
		}{
			r,
			wrappedRowsColumnTypeDatabaseTypeName{r.parent},
			wrappedRowsColumnTypeLength{r.parent},
			wrappedRowsColumnTypeNullable{r.parent},
			wrappedRowsColumnTypePrecisionScale{r.parent},
			wrappedRowsColumnTypeScanType{r.parent},
		}
	}

	// plain driver.Rows
	pickRows[63] = func(r *wrappedRows) driver.Rows {
		return struct {
			*wrappedRows
			wrappedRowsNextResultSet
			wrappedRowsColumnTypeDatabaseTypeName
			wrappedRowsColumnTypeLength
			wrappedRowsColumnTypeNullable
			wrappedRowsColumnTypePrecisionScale
			wrappedRowsColumnTypeScanType
		}{
			r,
			wrappedRowsNextResultSet{r.parent},
			wrappedRowsColumnTypeDatabaseTypeName{r.parent},
			wrappedRowsColumnTypeLength{r.parent},
			wrappedRowsColumnTypeNullable{r.parent},
			wrappedRowsColumnTypePrecisionScale{r.parent},
			wrappedRowsColumnTypeScanType{r.parent},
		}
	}
}

func wrapRows(ctx context.Context, intr Interceptor, r driver.Rows) driver.Rows {
	or := r
	for {
		ur, ok := or.(RowsUnwrapper)
		if !ok {
			break
		}
		or = ur.Unwrap()
	}

	id := 0

	if _, ok := or.(driver.RowsNextResultSet); ok {
		id += rowsNextResultSet
	}
	if _, ok := or.(driver.RowsColumnTypeDatabaseTypeName); ok {
		id += rowsColumnTypeDatabaseTypeName
	}
	if _, ok := or.(driver.RowsColumnTypeLength); ok {
		id += rowsColumnTypeLength
	}
	if _, ok := or.(driver.RowsColumnTypeNullable); ok {
		id += rowsColumnTypeNullable
	}
	if _, ok := or.(driver.RowsColumnTypePrecisionScale); ok {
		id += rowsColumnTypePrecisionScale
	}
	if _, ok := or.(driver.RowsColumnTypeScanType); ok {
		id += rowsColumnTypeScanType
	}
	wr := &wrappedRows{
		ctx:    ctx,
		intr:   intr,
		parent: r,
	}
	return pickRows[id](wr)
}
