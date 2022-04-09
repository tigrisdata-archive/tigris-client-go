package client

import (
	"context"
	"fmt"

	"github.com/tigrisdata/tigrisdb-client-go/driver"
)

type database struct {
	name   string
	driver driver.Driver
}

func newDatabase(name string, driver driver.Driver) Database {
	return &database{
		name:   name,
		driver: driver,
	}
}

func (d *database) Run(
	ctx context.Context,
	fn TransactionFunc,
) (interface{}, error) {
	tx, err := d.driver.BeginTx(ctx, d.name, nil)
	if err != nil {
		return nil, fmt.Errorf("Client: Run: error beginning transaction: %w", err)
	}

	res, err := fn(ctx, tx)
	if err != nil {
		if rollbackErr := tx.Rollback(ctx); rollbackErr != nil {
			return nil, fmt.Errorf(
				"error trying to rollback transaction: %v, original error: %w",
				rollbackErr, err)
		}
		return nil, fmt.Errorf("error running transaction: %w", err)
	}

	return res, nil
}

func (d *database) ApplySchemasFromDirectory(path string) error {
	panic("not implemented")
}
