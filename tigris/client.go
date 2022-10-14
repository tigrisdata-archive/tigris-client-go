// Copyright 2022 Tigris Data, Inc.
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
	"fmt"

	"github.com/tigrisdata/tigris-client-go/config"
	"github.com/tigrisdata/tigris-client-go/driver"
	"github.com/tigrisdata/tigris-client-go/schema"
)

// Client responsible for connecting to the server and opening a database.
type Client struct {
	driver driver.Driver
	config *config.Client
}

// NewClient creates a connection to the Tigris server.
func NewClient(ctx context.Context, cfg ...*config.Client) (*Client, error) {
	var pCfg config.Client

	if len(cfg) > 0 {
		if len(cfg) != 1 {
			return nil, fmt.Errorf("only one config structure allowed")
		}
		pCfg = *cfg[0]
	}

	d, err := driver.NewDriver(ctx, &pCfg.Driver)
	if err != nil {
		return nil, err
	}

	return &Client{driver: d, config: &pCfg}, nil
}

// Close terminates client connections and release resources.
func (c *Client) Close() error {
	return c.driver.Close()
}

// OpenDatabase initializes Database from given collection models.
// It creates Database if necessary.
// Creates and migrates schemas of the collections which constitutes the Database.
func (c *Client) OpenDatabase(ctx context.Context, dbName string, models ...schema.Model) (*Database, error) {
	if getTxCtx(ctx) != nil {
		return nil, ErrNotTransactional
	}

	return openDatabaseFromModels(ctx, c.driver, c.config, dbName, models...)
}

// DropDatabase deletes the database and all collections in it.
func (c *Client) DropDatabase(ctx context.Context, dbName string) error {
	if getTxCtx(ctx) != nil {
		return ErrNotTransactional
	}

	return c.driver.DropDatabase(ctx, dbName)
}
