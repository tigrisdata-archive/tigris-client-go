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
	"errors"
	"io"

	api "github.com/tigrisdata/tigris-client-go/api/server/v1"
)

type (
	SearchHit           = api.SearchHit
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
	Get(ctx context.Context, name string, ids []string) ([]*SearchHit, error)
	CreateByID(ctx context.Context, name string, id string, doc Document) error
	Create(ctx context.Context, name string, docs []Document) ([]*DocStatus, error)
	CreateOrReplace(ctx context.Context, name string, docs []Document) ([]*DocStatus, error)
	Update(ctx context.Context, name string, docs []Document) ([]*DocStatus, error)
	Delete(ctx context.Context, name string, ids []string) ([]*DocStatus, error)
	DeleteByQuery(ctx context.Context, name string, filter Filter) (int32, error)
	Search(ctx context.Context, name string, req *SearchRequest) (SearchIndexResultIterator, error)
}

type SearchIndexResultIterator interface {
	Next(r *SearchIndexResponse) bool
	Err() error
	Close()
}

type searchIndexReader interface {
	read() (SearchIndexResponse, error)
	close() error
}

type searchIndexResultIterator struct {
	searchIndexReader
	eof bool
	err error
}

func (i *searchIndexResultIterator) Next(r *SearchIndexResponse) bool {
	if i.eof {
		return false
	}

	resp, err := i.read()
	if errors.Is(err, io.EOF) {
		i.eof = true
		_ = i.close()
		return false
	}

	if err != nil {
		i.eof = true
		i.err = err
		_ = i.close()

		return false
	}

	*r = resp

	return true
}

func (i *searchIndexResultIterator) Err() error {
	return i.err
}

func (i *searchIndexResultIterator) Close() {
	if i.eof {
		return
	}

	_ = i.close()
	i.eof = true
}
