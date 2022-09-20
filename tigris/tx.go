package tigris

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/tigrisdata/tigris-client-go/driver"
)

var (
	// ErrNotTransactional returned if not transactional call is called in
	// a transactional context
	ErrNotTransactional = fmt.Errorf("incorrect use of non-transactional operation in a transaction context")
)

type TxOptions struct {
	AutoRetry bool
}

// Tx is the interface for accessing APIs in a transactional way
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

// low level with no retries
func (db *Database) tx(ctx context.Context, fn func(ctx context.Context) error) error {
	dtx, err := db.driver.BeginTx(ctx, db.name)
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
