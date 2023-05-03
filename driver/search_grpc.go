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

//go:build tigris_grpc || (!tigris_grpc && !tigris_http)

package driver

import (
	"context"
	"unsafe"

	jsoniter "github.com/json-iterator/go"
	api "github.com/tigrisdata/tigris-client-go/api/server/v1"
)

type grpcSearch struct {
	Project string

	search api.SearchClient
}

func NewGRPCSearchClient(project string, client api.SearchClient) SearchClient {
	return &grpcSearch{Project: project, search: client}
}

func (c *grpcSearch) CreateOrUpdateIndex(ctx context.Context, name string, schema Schema) error {
	_, err := c.search.CreateOrUpdateIndex(ctx, &api.CreateOrUpdateIndexRequest{
		Project: c.Project,
		Name:    name,
		Schema:  schema,
	})

	return GRPCError(err)
}

func (c *grpcSearch) GetIndex(ctx context.Context, name string) (*IndexInfo, error) {
	resp, err := c.search.GetIndex(ctx, &api.GetIndexRequest{
		Project: c.Project,
		Name:    name,
	})
	if err != nil {
		return nil, GRPCError(err)
	}

	return &IndexInfo{Name: resp.Index.Name, Schema: resp.Index.Schema}, nil
}

func (c *grpcSearch) DeleteIndex(ctx context.Context, name string) error {
	_, err := c.search.DeleteIndex(ctx, &api.DeleteIndexRequest{
		Project: c.Project,
		Name:    name,
	})

	return GRPCError(err)
}

func (c *grpcSearch) ListIndexes(ctx context.Context, filter *IndexSource) ([]*IndexInfo, error) {
	if filter == nil {
		filter = &IndexSource{}
	}

	resp, err := c.search.ListIndexes(ctx, &api.ListIndexesRequest{
		Project: c.Project,
		Filter:  filter,
	})
	if err != nil {
		return nil, GRPCError(err)
	}

	return resp.GetIndexes(), nil
}

func (c *grpcSearch) Get(ctx context.Context, name string, ids []string) ([]*SearchHit, error) {
	resp, err := c.search.Get(ctx, &api.GetDocumentRequest{
		Project: c.Project,
		Index:   name,
		Ids:     ids,
	})
	if err != nil {
		return nil, GRPCError(err)
	}

	return resp.GetDocuments(), nil
}

func (c *grpcSearch) CreateByID(ctx context.Context, name string, id string, doc Document) error {
	_, err := c.search.CreateById(ctx, &api.CreateByIdRequest{
		Project:  c.Project,
		Index:    name,
		Id:       id,
		Document: doc,
	})

	return GRPCError(err)
}

func (c *grpcSearch) Create(ctx context.Context, name string, docs []Document) ([]*DocStatus, error) {
	resp, err := c.search.Create(ctx, &api.CreateDocumentRequest{
		Project:   c.Project,
		Index:     name,
		Documents: *(*[][]byte)(unsafe.Pointer(&docs)),
	})
	if err != nil {
		return nil, GRPCError(err)
	}

	return resp.Status, nil
}

func (c *grpcSearch) CreateOrReplace(ctx context.Context, name string, docs []Document) ([]*DocStatus, error) {
	resp, err := c.search.CreateOrReplace(ctx, &api.CreateOrReplaceDocumentRequest{
		Project:   c.Project,
		Index:     name,
		Documents: *(*[][]byte)(unsafe.Pointer(&docs)),
	})
	if err != nil {
		return nil, GRPCError(err)
	}

	return resp.Status, nil
}

func (c *grpcSearch) Update(ctx context.Context, name string, docs []Document) ([]*DocStatus, error) {
	resp, err := c.search.Update(ctx, &api.UpdateDocumentRequest{
		Project:   c.Project,
		Index:     name,
		Documents: *(*[][]byte)(unsafe.Pointer(&docs)),
	})
	if err != nil {
		return nil, GRPCError(err)
	}

	return resp.Status, nil
}

func (c *grpcSearch) Delete(ctx context.Context, name string, ids []string) ([]*DocStatus, error) {
	resp, err := c.search.Delete(ctx, &api.DeleteDocumentRequest{
		Project: c.Project,
		Index:   name,
		Ids:     ids,
	})
	if err != nil {
		return nil, GRPCError(err)
	}

	return resp.Status, nil
}

func (c *grpcSearch) DeleteByQuery(ctx context.Context, name string, filter Filter) (int32, error) {
	resp, err := c.search.DeleteByQuery(ctx, &api.DeleteByQueryRequest{
		Project: c.Project,
		Index:   name,
		Filter:  filter,
	})
	if err != nil {
		return 0, GRPCError(err)
	}

	return resp.Count, nil
}

func (c *grpcSearch) Search(ctx context.Context, name string, req *SearchRequest) (SearchIndexResultIterator, error) {
	var coll *api.Collation

	if req.Collation != nil {
		coll = &api.Collation{Case: req.Collation.Case}
	}

	var b []byte
	var err error

	if req.Sort != nil {
		if b, err = jsoniter.Marshal(req.Sort); err != nil {
			return nil, err
		}
	}

	var cancel context.CancelFunc
	ctx, cancel = context.WithCancel(ctx)

	resp, err := c.search.Search(ctx, &api.SearchIndexRequest{
		Project:       c.Project,
		Index:         name,
		Q:             req.Q,
		Filter:        req.Filter,
		Facet:         req.Facet,
		Sort:          b,
		SearchFields:  req.SearchFields,
		IncludeFields: req.IncludeFields,
		ExcludeFields: req.ExcludeFields,
		PageSize:      req.PageSize,
		Page:          req.Page,
		Collation:     coll,
		Vector:        req.Vector,
	})
	if err != nil {
		cancel()
		return nil, GRPCError(err)
	}

	return &searchIndexResultIterator{
		searchIndexReader: &searchIndexStreamReader{
			stream: resp, cancel: cancel,
		},
	}, nil
}

type searchIndexStreamReader struct {
	stream api.Search_SearchClient
	cancel context.CancelFunc
}

func (g *searchIndexStreamReader) read() (SearchIndexResponse, error) {
	resp, err := g.stream.Recv()
	if err != nil {
		return nil, GRPCError(err)
	}

	return resp, nil
}

func (g *searchIndexStreamReader) close() error {
	g.cancel()

	return nil
}
