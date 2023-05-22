package pool

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
)

// This file contains code taken verbatim from github.com/jackc/pgx
// The included Copyright and License apply to this file only.

//	Copyright (c) 2013-2021 Jack Christensen
//
//	MIT License
//
//	Permission is hereby granted, free of charge, to any person obtaining
//	a copy of this software and associated documentation files (the
//	"Software"), to deal in the Software without restriction, including
//	without limitation the rights to use, copy, modify, merge, publish,
//	distribute, sublicense, and/or sell copies of the Software, and to
//	permit persons to whom the Software is furnished to do so, subject to
//	the following conditions:
//
//	The above copyright notice and this permission notice shall be
//	included in all copies or substantial portions of the Software.
//
//	THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
//	EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
//	MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
//	NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
//	LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
//	OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
//	WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

// beginFuncExec is copied directly from pgx
// src: https://github.com/jackc/pgx/blob/f59e8bf5551f403e6b7ec0912097bce85ea21351/tx.go#L410-L425
func beginFuncExec(ctx context.Context, tx pgx.Tx, fn func(pgx.Tx) error) (err error) {
	defer func() {
		rollbackErr := tx.Rollback(ctx)
		if rollbackErr != nil && !errors.Is(rollbackErr, pgx.ErrTxClosed) {
			err = rollbackErr
		}
	}()

	fErr := fn(tx)
	if fErr != nil {
		_ = tx.Rollback(ctx) // ignore rollback error as there is already an error to return
		return fErr
	}

	return tx.Commit(ctx)
}
