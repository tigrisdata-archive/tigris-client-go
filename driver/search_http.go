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

//nolint:bodyclose
package driver

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"

	apiHTTP "github.com/tigrisdata/tigris-client-go/api/client/v1/api"
	api "github.com/tigrisdata/tigris-client-go/api/server/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type httpSearch struct {
	Project string

	api *apiHTTP.ClientWithResponses
}

func NewHTTPSearchClient(project string, client *apiHTTP.ClientWithResponses) SearchClient {
	return &httpSearch{Project: project, api: client}
}

func (c *httpSearch) CreateOrUpdateIndex(ctx context.Context, name string, schema json.RawMessage) error {
	resp, err := c.api.SearchCreateOrUpdateIndex(ctx, c.Project, name,
		apiHTTP.SearchCreateOrUpdateIndexJSONRequestBody{
			Schema: schema,
		})
	if err = HTTPError(err, resp); err != nil {
		return err
	}

	var i apiHTTP.SearchCreateOrUpdateIndexResponse

	if err := respDecode(resp.Body, &i); err != nil {
		return err
	}

	return nil
}

func (c *httpSearch) GetIndex(ctx context.Context, name string) (*IndexInfo, error) {
	resp, err := c.api.SearchGetIndex(ctx, c.Project, name)
	if err = HTTPError(err, resp); err != nil {
		return nil, err
	}

	var i apiHTTP.GetIndexResponse

	if err := respDecode(resp.Body, &i); err != nil {
		return nil, err
	}

	if i.Index == nil {
		return nil, os.ErrNotExist
	}

	return &IndexInfo{Name: PtrToString(i.Index.Name), Schema: i.Index.Schema}, nil
}

func (c *httpSearch) DeleteIndex(ctx context.Context, name string) error {
	resp, err := c.api.SearchDeleteIndex(ctx, c.Project, name, apiHTTP.SearchDeleteIndexJSONRequestBody{})
	if err := HTTPError(err, resp); err != nil {
		return err
	}

	return err
}

func (c *httpSearch) ListIndexes(ctx context.Context, filter *IndexSource) ([]*IndexInfo, error) {
	if filter == nil {
		filter = &IndexSource{}
	}

	resp, err := c.api.SearchListIndexes(ctx, c.Project, &apiHTTP.SearchListIndexesParams{
		FilterType:       &filter.Type,
		FilterCollection: &filter.Collection,
		FilterBranch:     &filter.Branch,
	})
	if err = HTTPError(err, resp); err != nil {
		return nil, err
	}

	var i apiHTTP.ListIndexesResponse

	if err = respDecode(resp.Body, &i); err != nil {
		return nil, err
	}

	var indexes []*IndexInfo

	if i.Indexes != nil {
		for _, v := range *i.Indexes {
			indexes = append(indexes, &IndexInfo{Name: PtrToString(v.Name), Schema: v.Schema})
		}
	}

	return indexes, nil
}

func (c *httpSearch) Get(ctx context.Context, name string, ids []string) ([]*IndexDoc, error) {
	resp, err := c.api.SearchGet(ctx, c.Project, name, &apiHTTP.SearchGetParams{
		Ids: &ids,
	})
	if err = HTTPError(err, resp); err != nil {
		return nil, err
	}

	var i apiHTTP.GetDocumentResponse

	if err = respDecode(resp.Body, &i); err != nil {
		return nil, err
	}

	var docs []*IndexDoc

	if i.Documents != nil {
		for _, v := range *i.Documents {
			doc := &IndexDoc{Doc: v.Doc}

			if v.Metadata != nil {
				if v.Metadata.CreatedAt != nil {
					doc.Metadata.CreatedAt = timestamppb.New(*v.Metadata.CreatedAt)
				}
				if v.Metadata.UpdatedAt != nil {
					doc.Metadata.UpdatedAt = timestamppb.New(*v.Metadata.UpdatedAt)
				}
			}

			docs = append(docs, doc)
		}
	}

	return docs, nil
}

func respStatuses(resp *http.Response, err error) ([]*DocStatus, error) {
	if err = HTTPError(err, resp); err != nil {
		return nil, err
	}

	var res struct {
		Status []*DocStatus
	}

	if err = respDecode(resp.Body, &res); err != nil {
		return nil, err
	}

	return res.Status, nil
}

func (c *httpSearch) CreateByID(ctx context.Context, name string, id string, doc json.RawMessage) error {
	resp, err := c.api.SearchCreateById(ctx, c.Project, name, id, apiHTTP.SearchCreateByIdJSONRequestBody{
		Document: doc,
	})

	return HTTPError(err, resp)
}

func (c *httpSearch) Create(ctx context.Context, name string, docs []json.RawMessage) ([]*DocStatus, error) {
	resp, err := c.api.SearchCreate(ctx, c.Project, name, apiHTTP.SearchCreateJSONRequestBody{
		Documents: &docs,
	})

	return respStatuses(resp, err)
}

func (c *httpSearch) CreateOrReplace(ctx context.Context, name string, docs []json.RawMessage) ([]*DocStatus, error) {
	resp, err := c.api.SearchCreateOrReplace(ctx, c.Project, name, apiHTTP.SearchCreateOrReplaceJSONRequestBody{
		Documents: &docs,
	})

	return respStatuses(resp, err)
}

func (c *httpSearch) Update(ctx context.Context, name string, docs []json.RawMessage) ([]*DocStatus, error) {
	resp, err := c.api.SearchUpdate(ctx, c.Project, name, apiHTTP.SearchUpdateJSONRequestBody{
		Documents: &docs,
	})

	return respStatuses(resp, err)
}

func (c *httpSearch) Delete(ctx context.Context, name string, ids []string) ([]*DocStatus, error) {
	resp, err := c.api.SearchDelete(ctx, c.Project, name, apiHTTP.SearchDeleteJSONRequestBody{
		Ids: &ids,
	})

	return respStatuses(resp, err)
}

func (c *httpSearch) DeleteByQuery(ctx context.Context, name string, filter json.RawMessage) (int32, error) {
	resp, err := c.api.SearchDeleteByQuery(ctx, c.Project, name, apiHTTP.SearchDeleteByQueryJSONRequestBody{
		Filter: filter,
	})
	if err = HTTPError(err, resp); err != nil {
		return 0, err
	}

	var i apiHTTP.DeleteByQueryResponse

	if err = respDecode(resp.Body, &i); err != nil {
		return 0, err
	}

	return PtrToInt32(i.Count), nil
}

func (c *httpSearch) Search(ctx context.Context, name string, req *SearchRequest) (SearchIndexResultIterator, error) {
	var coll *apiHTTP.Collation
	if req.Collation != nil {
		coll = &apiHTTP.Collation{Case: &req.Collation.Case}
	}

	if req.SearchFields == nil {
		req.SearchFields = []string{}
	}

	if req.IncludeFields == nil {
		req.IncludeFields = []string{}
	}

	if req.ExcludeFields == nil {
		req.ExcludeFields = []string{}
	}

	resp, err := c.api.SearchSearch(ctx, c.Project, name, apiHTTP.SearchSearchJSONRequestBody{
		Project:       &c.Project,
		Index:         &name,
		Q:             &req.Q,
		Filter:        json.RawMessage(req.Filter),
		Facet:         json.RawMessage(req.Facet),
		Sort:          json.RawMessage(req.Sort),
		SearchFields:  &req.SearchFields,
		IncludeFields: &req.IncludeFields,
		ExcludeFields: &req.ExcludeFields,
		PageSize:      &req.PageSize,
		Page:          &req.Page,
		Collation:     coll,
	})

	err = HTTPError(err, resp)

	e := &searchIndexResultIterator{err: err, eof: err != nil}

	if err == nil {
		e.searchIndexReader = &httpSearchIndexReader{stream: json.NewDecoder(resp.Body), closer: resp.Body}
	}

	return e, nil
}

type httpSearchIndexReader struct {
	closer io.Closer
	stream *json.Decoder
}

func (g *httpSearchIndexReader) read() (SearchIndexResponse, error) {
	var res struct {
		Result struct {
			Hits []*struct {
				Doc      json.RawMessage `json:"doc"`
				Metadata api.DocMetadata
			} `json:"hits"`
			Facets map[string]*api.SearchFacet `json:"facets"`
			Meta   *api.SearchMetadata         `json:"meta"`
		} `json:"result"`
		Error *api.ErrorDetails `json:"error"`
	}

	if err := g.stream.Decode(&res); err != nil {
		return nil, HTTPError(err, nil)
	}

	if res.Error != nil {
		return nil, &Error{TigrisError: api.FromErrorDetails(res.Error)}
	}

	resp := &api.SearchIndexResponse{
		Facets: res.Result.Facets,
		Meta:   res.Result.Meta,
	}

	for _, v := range res.Result.Hits {
		doc := &IndexDoc{Doc: v.Doc}

		if v.Metadata.CreatedAt != nil {
			doc.Metadata.CreatedAt = timestamppb.New(*v.Metadata.CreatedAt)
		}
		if v.Metadata.UpdatedAt != nil {
			doc.Metadata.UpdatedAt = timestamppb.New(*v.Metadata.UpdatedAt)
		}

		resp.Hits = append(resp.Hits, doc)
	}

	return resp, nil
}

func (g *httpSearchIndexReader) close() error {
	return g.closer.Close()
}
