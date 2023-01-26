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
	"crypto/tls"
	"errors"
	"fmt"
	"os"

	"github.com/tigrisdata/tigris-client-go/config"
	"github.com/tigrisdata/tigris-client-go/driver"
	"github.com/tigrisdata/tigris-client-go/schema"
)

const DefaultBranch = "main"

type Config struct {
	TLS          *tls.Config `json:"tls,omitempty"`
	ClientID     string      `json:"client_id,omitempty"`
	ClientSecret string      `json:"client_secret,omitempty"`
	Token        string      `json:"token,omitempty"`
	URL          string      `json:"url,omitempty"`
	Protocol     string      `json:"protocol,omitempty"`
	Project      string      `json:"project,omitempty"`
	Branch       string      `json:"branch,omitempty"`
	// MustExist if set skips implicit database creation
	MustExist bool
}

// Client responsible for connecting to the server and opening a database.
type Client struct {
	driver driver.Driver
	config *Config
}

func driverConfig(cfg *Config) *config.Driver {
	return &config.Driver{
		TLS:          cfg.TLS,
		URL:          cfg.URL,
		ClientID:     cfg.ClientID,
		ClientSecret: cfg.ClientSecret,
		Branch:       cfg.Branch,
		Token:        cfg.Token,
		Protocol:     cfg.Protocol,
	}
}

// NewClient creates a connection to the Tigris server.
func NewClient(ctx context.Context, cfg ...*Config) (*Client, error) {
	var pCfg Config

	if len(cfg) > 0 {
		if len(cfg) != 1 {
			return nil, fmt.Errorf("only one config structure allowed")
		}
		pCfg = *cfg[0]
	}

	if pCfg.Project == "" {
		pCfg.Project = os.Getenv(driver.EnvProject)
		if pCfg.Project == "" {
			return nil, errors.New("failed to configure tigris project")
		}
	}

	if pCfg.Branch == "" {
		pCfg.Branch = os.Getenv(driver.EnvDBBranch)
		if pCfg.Branch == "" {
			// setting default branch
			pCfg.Branch = DefaultBranch
		}
	}

	d, err := driver.NewDriver(ctx, driverConfig(&pCfg))
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
func (c *Client) OpenDatabase(ctx context.Context, models ...schema.Model) (*Database, error) {
	if getTxCtx(ctx) != nil {
		return nil, ErrNotTransactional
	}

	return OpenDatabaseFromModels(ctx, c.driver, c.config.Project, models...)
}

// GetDatabase gets the Database for this project.
func (c *Client) GetDatabase() *Database {
	return newDatabase(c.config.Project, c.driver)
}

// InitializeBranch will create a database branch provided in config, if not existing already.
func (c *Client) InitializeBranch(ctx context.Context) (*driver.CreateBranchResponse, error) {
	db := c.GetDatabase()
	return db.CreateBranch(ctx, c.config.Branch)
}
