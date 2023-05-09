// Copyright 2022-2023 Tigris Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tigris

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/tigrisdata/tigris-client-go/driver"
)

// ErrNotTransactional returned if not transactional call is called in
// a transactional context.
var ErrNotTransactional = fmt.Errorf("incorrect use of non-transactional operation in a transaction context")

type TxOptions struct {
	AutoRetry bool
}

// Tx is the interface for accessing APIs in a transactional way.
type Tx struct {
	db *Database
	tx driver.Tx
}

type key string

var txCtxKey key = "tx_ctx"

func setTxCtx(ctx context.Context, tx *Tx) context.Context {
	return context.WithValue(ctx, txCtxKey, tx)
}

func getTxCtx(ctx context.Context) *Tx {
	tx, _ := ctx.Value(txCtxKey).(*Tx)
	return tx
}

func getDB(ctx context.Context, crud driver.Database) driver.Database {
	if tx := getTxCtx(ctx); tx != nil {
		return tx.tx
	}

	return crud
}

// low level with no retries.
func (db *Database) tx(ctx context.Context, fn func(ctx context.Context) error) error {
	if tx := getTxCtx(ctx); tx != nil {
		return fn(ctx)
	}

	dtx, err := db.driver.UseDatabase(db.name).BeginTx(ctx)
	if err != nil {
		return err
	}

	defer func() { _ = dtx.Rollback(ctx) }()

	tx := &Tx{db, dtx}

	if err = fn(setTxCtx(ctx, tx)); err != nil {
		return err
	}

	return dtx.Commit(ctx)
}

// Tx executes given set of operations in a transaction
//
// All operation in the "fn" closure is executed atomically in a transaction.
// If the closure returns no error the changes are applied to the database,
// when error is returned then changes just discarded,
// database stays intact.
func (db *Database) Tx(ctx context.Context, fn func(ctx context.Context) error, options ...TxOptions) error {
	for {
		err := db.tx(ctx, fn)
		if err == nil {
			return nil
		}

		var te *driver.Error

		if errors.As(err, &te) && te.RetryDelay() > 0 && len(options) > 0 && options[0].AutoRetry {
			time.Sleep(te.RetryDelay())

			continue
		}

		return err
	}
}
