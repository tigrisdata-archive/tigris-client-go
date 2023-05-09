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

// Package search provides an interface for accessing Tigris data-platform
// search APIs.
package search

import (
	"context"
	"crypto/tls"
	"fmt"

	"github.com/tigrisdata/tigris-client-go/config"
	"github.com/tigrisdata/tigris-client-go/driver"
	"github.com/tigrisdata/tigris-client-go/schema"
)

// Search is the interface for interacting with a Tigris Search
// Due to the limitations of Golang generics instantiations of the indexs
// should be done using GetIndex[Model](ctx, db) top level function instead of
// method of this interface.
// Similarly to get access to index APIs in a transaction
// top level GetTxIndex(ctx, tx) function should be used
// instead of method of Tx interface.
type Search struct {
	name   string
	search driver.SearchClient
}

func NewSearch(name string, search driver.SearchClient) *Search {
	return &Search{
		name:   name,
		search: search,
	}
}

// CreateIndexes creates indexes in the Search using provided index models
// This method is only needed if indexes need to be created dynamically,
// all static indexes are created by OpenSearch.
func (s *Search) CreateIndexes(ctx context.Context, model schema.Model, models ...schema.Model) error {
	schemas, err := schema.FromCollectionModels(0, schema.Search, model, models...)
	if err != nil {
		return fmt.Errorf("error parsing model schema: %w", err)
	}

	return s.createIndexesFromSchemas(ctx, schemas)
}

func (s *Search) createIndexesFromSchemas(ctx context.Context, schemas map[string]*schema.Schema) error {
	for _, v := range schemas {
		sch, err := schema.Build(v)
		if err != nil {
			return err
		}

		if err = s.search.CreateOrUpdateIndex(ctx, v.Name, sch); err != nil {
			return err
		}
	}

	return nil
}

// DeleteIndex drops the index(es).
func (s *Search) DeleteIndex(ctx context.Context, model schema.Model, models ...schema.Model) error {
	name := schema.ModelName(model)
	if err := getSearch(ctx, s.search).DeleteIndex(ctx, name); err != nil {
		return err
	}

	for _, m := range models {
		name = schema.ModelName(m)
		if err := getSearch(ctx, s.search).DeleteIndex(ctx, name); err != nil {
			return err
		}
	}

	return nil
}

// GetIndex returns index object corresponding to index model T.
func GetIndex[T schema.Model](s *Search) *Index[T] {
	var m T
	name := schema.ModelName(&m)
	return getNamedIndex[T](s, name)
}

func getNamedIndex[T schema.Model](s *Search, name string) *Index[T] {
	return &Index[T]{Name: name, SearchClient: s.search}
}

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

func Open(ctx context.Context, cfg *Config, models ...schema.Model) (*Search, error) {
	d, err := driver.NewDriver(ctx, driverConfig(cfg))
	if err != nil {
		return nil, err
	}

	return openSearch(ctx, d, cfg.Project, models...)
}

func MustOpen(ctx context.Context, cfg *Config, models ...schema.Model) *Search {
	s, err := Open(ctx, cfg, models...)
	if err != nil {
		panic(err)
	}

	return s
}

func openSearch(ctx context.Context, d driver.Driver,
	project string, models ...schema.Model,
) (*Search, error) {
	s := NewSearch(project, d.UseSearch(project))

	if len(models) > 0 {
		err := s.CreateIndexes(ctx, models[0], models[1:]...)
		if err != nil {
			return nil, err
		}
	}

	return s, nil
}
