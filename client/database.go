package client

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"

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

func (d *database) ApplySchemasFromDirectory(ctx context.Context, path string) error {
	files, err := ioutil.ReadDir(path)
	if err != nil {
		return fmt.Errorf(
			"error reading schemas directory: %s, err: %w", path, err)
	}

	type schema struct {
		name  string
		bytes []byte
	}

	type collectionName struct {
		Name string `json:"name"`
	}

	schemas := map[string]schema{}
	for _, f := range files {
		fBytes, err := ioutil.ReadFile(f.Name())
		if err != nil {
			return fmt.Errorf(
				"error reading schema from file: %s, err: %w",
				f.Name(), err)
		}

		name := collectionName{}
		if err := json.Unmarshal(fBytes, &name); err != nil {
			return fmt.Errorf(
				"error parsing JSON for schema in file: %s, err: %w",
				f.Name(), err)
		}
		if name.Name == "" {
			return fmt.Errorf(
				"did not find collection name in schema file: %s", f.Name())
		}

		schemas[f.Name()] = schema{
			name:  name.Name,
			bytes: fBytes,
		}
	}

	_, err = d.Run(ctx, func(ctx context.Context, tx driver.Tx) (interface{}, error) {
		for _, schema := range schemas {
			err := tx.CreateOrUpdateCollection(
				ctx, schema.name, driver.Schema(schema.bytes))
			if err != nil {
				return nil, fmt.Errorf(
					"error applying schema for collection: %s, err: %w", schema.name, err)
			}
		}

		return nil, nil
	})
	return err
}
