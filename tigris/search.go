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

// Package tigris provides an interface for accessing Tigris data-platform.
// This is the main client package you are looking for.
package tigris

import (
	"context"
	"fmt"
	"github.com/tigrisdata/tigris-client-go/search"

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
	branch string
	search driver.SearchClient
}

func newSearch(name string, branch string, search driver.SearchClient) *Search {
	return &Search{
		name:   name,
		branch: branch,
		search: search,
	}
}

// CreateIndexes creates indexes in the Search using provided index models
// This method is only needed if indexs need to be created dynamically,
// all static indexs are created by OpenSearch.
func (s *Search) CreateIndexes(ctx context.Context, model schema.Model, models ...schema.Model) error {
	schemas, err := schema.FromCollectionModels(schema.Search, model, models...)
	if err != nil {
		return fmt.Errorf("error parsing model schema: %w", err)
	}

	return s.createIndexesFromSchemas(ctx, schemas)
}

func (s *Search) createIndexesFromSchemas(ctx context.Context, schemas map[string]*schema.Schema) error {
	for _, v := range schemas {

		if v.SearchSource != nil && v.SearchSource.Type == schema.SearchSourceTigris {
			v.SearchSource.Branch = s.branch
		}

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

func openSearch(ctx context.Context, d driver.Driver,
	project string, branch string, models ...schema.Model,
) (*Search, error) {
	s := newSearch(project, branch, d.UseSearch(project))

	if len(models) > 0 {
		err := s.CreateIndexes(ctx, models[0], models[1:]...)
		if err != nil {
			return nil, err
		}
	}

	return s, nil
}

// OpenSearch initializes search client from given index models.
// Creates and migrates schemas of the indexes.
// This is identical to calling:
//
//	client := tigris.NewClient(...)
//	client.OpenSearch(...)
func OpenSearch(ctx context.Context, cfg *Config, models ...schema.Model,
) (*Search, error) {
	if getTxCtx(ctx) != nil {
		return nil, ErrNotTransactional
	}

	d, err := driver.NewDriver(ctx, driverConfig(cfg))
	if err != nil {
		return nil, err
	}

	return openSearch(ctx, d, cfg.Project, cfg.Branch, models...)
}

// GetIndex returns index object corresponding to index model T.
func GetIndex[T schema.Model](db *Search) *search.Index[T] {
	var m T
	name := schema.ModelName(&m)
	return getNamedIndex[T](db, name)
}

func getNamedIndex[T schema.Model](s *Search, name string) *search.Index[T] {
	return &search.Index[T]{Name: name, SearchClient: s.search}
}

// TestOpenSearch allows to provide mocked driver in tests.
func TestOpenSearch(ctx context.Context, d driver.Driver,
	project string, models ...schema.Model,
) (*Search, error) {
	return openSearch(ctx, d, project, "", models...)
}
