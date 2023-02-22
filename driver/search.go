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

package driver

import (
	"context"
	api "github.com/tigrisdata/tigris-client-go/api/server/v1"
)

type (
	IndexDoc            = api.IndexDoc
	IndexInfo           = api.IndexInfo
	SearchIndexResponse = *api.SearchIndexResponse
	IndexSource         = api.IndexSource

	DocStatus = api.DocStatus
)

type SearchClient interface {
	CreateOrUpdateIndex(ctx context.Context, name string, schema Schema) error
	GetIndex(ctx context.Context, name string) (*IndexInfo, error)
	DeleteIndex(ctx context.Context, name string) error
	ListIndexes(ctx context.Context, filter *IndexSource) ([]*IndexInfo, error)
	Get(ctx context.Context, name string, ids []string) ([]*IndexDoc, error)
	CreateByID(ctx context.Context, name string, id string, doc Document) error
	Create(ctx context.Context, name string, docs []Document) ([]*DocStatus, error)
	CreateOrReplace(ctx context.Context, name string, docs []Document) ([]*DocStatus, error)
	Update(ctx context.Context, name string, docs []Document) ([]*DocStatus, error)
	Delete(ctx context.Context, name string, ids []string) ([]*DocStatus, error)
	DeleteByQuery(ctx context.Context, name string, filter Filter) (int32, error)
	Search(ctx context.Context, name string, req *SearchRequest) (SearchIndexResultIterator, error)
}
