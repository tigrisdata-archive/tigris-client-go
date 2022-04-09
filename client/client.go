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

package client

import (
	"context"
	"fmt"

	"github.com/tigrisdata/tigrisdb-client-go/driver"
)

// Client is the interface for a TigrisDB client.
type Client interface {
	// Database returns a Database interface for the provided
	// database name.
	//
	// TODO: Return bool/error if the database exists or not
	//       or should the client just get the error when they
	//       try to use it? Latter is more performant.
	Database(name string) Database

	// CreateDatabaseIfNotExist creates a database if it doesn't
	// already exist.
	CreateDatabaseIfNotExist(
		ctx context.Context,
		db string,
		options ...*driver.DatabaseOptions,
	) error
}

type client struct {
	driver driver.Driver
}

func NewClient(
	ctx context.Context,
	url string,
	config *driver.Config,
) (Client, error) {
	d, err := driver.NewDriver(ctx, url, config)
	if err != nil {
		return nil, err
	}
	return &client{
		driver: d,
	}, nil
}

func (c *client) Database(name string) Database {
	return newDatabase(name, c.driver)
}

func (c *client) CreateDatabaseIfNotExist(
	ctx context.Context,
	name string,
	opts ...*driver.DatabaseOptions,
) error {
	// TODO: Is this run transactionally?
	if err := c.driver.CreateDatabase(ctx, name, opts...); err != nil {
		return fmt.Errorf("error creating database: %w", err)
	}
	return nil
}
