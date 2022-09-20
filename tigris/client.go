package tigris

import (
	"context"

	"github.com/tigrisdata/tigris-client-go/config"
	"github.com/tigrisdata/tigris-client-go/driver"
	"github.com/tigrisdata/tigris-client-go/schema"
)

// Client responsible for connecting to the server and opening a database
type Client struct {
	driver driver.Driver
	config *config.Database
}

// NewClient creates a connection to the Tigris server
func NewClient(ctx context.Context, cfg *config.Database) (*Client, error) {
	d, err := driver.NewDriver(ctx, &cfg.Driver)
	if err != nil {
		return nil, err
	}

	return &Client{driver: d, config: cfg}, nil
}

// Close terminates client connections and release resources
func (c *Client) Close() error {
	return c.driver.Close()
}

// OpenDatabase initializes Database from given collection models.
// It creates Database if necessary.
// Creates and migrates schemas of the collections which constitutes the Database
func (c *Client) OpenDatabase(ctx context.Context, dbName string, model schema.Model, models ...schema.Model) (*Database, error) {
	if getTxCtx(ctx) != nil {
		return nil, ErrNotTransactional
	}

	return openDatabaseFromModels(ctx, c.driver, c.config, dbName, model, models...)
}

// DropDatabase deletes the database and all collections in it
func (c *Client) DropDatabase(ctx context.Context, dbName string) error {
	if getTxCtx(ctx) != nil {
		return ErrNotTransactional
	}

	return c.driver.DropDatabase(ctx, dbName)
}
