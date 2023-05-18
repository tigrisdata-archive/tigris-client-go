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

//go:build tigris_http

//nolint:bodyclose
package driver

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"unsafe"

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

func (c *httpSearch) CreateOrUpdateIndex(ctx context.Context, name string, schema Schema) error {
	resp, err := c.api.SearchCreateOrUpdateIndex(ctx, c.Project, name,
		apiHTTP.SearchCreateOrUpdateIndexJSONRequestBody{
			Schema: json.RawMessage(schema),
		})
	if err = HTTPError(err, resp); err != nil {
		return err
	}

	var i apiHTTP.SearchCreateOrUpdateIndexResponse

	return respDecode(resp.Body, &i)
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

func (c *httpSearch) Get(ctx context.Context, name string, ids []string) ([]*SearchHit, error) {
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
	if i.Documents == nil {
		return nil, nil
	}

	var docs []*SearchHit
	if i.Documents != nil {
		for _, v := range *i.Documents {
			doc := &SearchHit{Data: v.Data}

			if v.Metadata != nil {
				doc.Metadata = &api.SearchHitMeta{}
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

func (c *httpSearch) CreateByID(ctx context.Context, name string, id string, doc Document) error {
	resp, err := c.api.SearchCreateById(ctx, c.Project, name, id, apiHTTP.SearchCreateByIdJSONRequestBody{
		Document: json.RawMessage(doc),
	})

	return HTTPError(err, resp)
}

func (c *httpSearch) Create(ctx context.Context, name string, docs []Document) ([]*DocStatus, error) {
	resp, err := c.api.SearchCreate(ctx, c.Project, name, apiHTTP.SearchCreateJSONRequestBody{
		Documents: (*[]json.RawMessage)(unsafe.Pointer(&docs)),
	})

	return respStatuses(resp, err)
}

func (c *httpSearch) CreateOrReplace(ctx context.Context, name string, docs []Document) ([]*DocStatus, error) {
	resp, err := c.api.SearchCreateOrReplace(ctx, c.Project, name, apiHTTP.SearchCreateOrReplaceJSONRequestBody{
		Documents: (*[]json.RawMessage)(unsafe.Pointer(&docs)),
	})

	return respStatuses(resp, err)
}

func (c *httpSearch) Update(ctx context.Context, name string, docs []Document) ([]*DocStatus, error) {
	resp, err := c.api.SearchUpdate(ctx, c.Project, name, apiHTTP.SearchUpdateJSONRequestBody{
		Documents: (*[]json.RawMessage)(unsafe.Pointer(&docs)),
	})

	return respStatuses(resp, err)
}

func (c *httpSearch) Delete(ctx context.Context, name string, ids []string) ([]*DocStatus, error) {
	resp, err := c.api.SearchDelete(ctx, c.Project, name, apiHTTP.SearchDeleteJSONRequestBody{
		Ids: &ids,
	})

	return respStatuses(resp, err)
}

func (c *httpSearch) DeleteByQuery(ctx context.Context, name string, filter Filter) (int32, error) {
	resp, err := c.api.SearchDeleteByQuery(ctx, c.Project, name, apiHTTP.SearchDeleteByQueryJSONRequestBody{
		Filter: json.RawMessage(filter),
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
		Sort:          (*[]json.RawMessage)(&req.Sort),
		SearchFields:  &req.SearchFields,
		IncludeFields: &req.IncludeFields,
		ExcludeFields: &req.ExcludeFields,
		PageSize:      &req.PageSize,
		Page:          &req.Page,
		Collation:     coll,
		Vector:        json.RawMessage(req.Vector),
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

type searchHTTPResult struct {
	Result struct {
		Hits   []*apiHTTP.SearchHit            `json:"hits"`
		Facets map[string]*apiHTTP.SearchFacet `json:"facets"`
		Meta   *apiHTTP.SearchMetadata         `json:"meta"`
	} `json:"result"`
	Error *api.ErrorDetails `json:"error"`
}

func (g *httpSearchIndexReader) read() (SearchIndexResponse, error) {
	var res searchHTTPResult

	if err := g.stream.Decode(&res); err != nil {
		return nil, HTTPError(err, nil)
	}

	if res.Error != nil {
		return nil, &Error{TigrisError: api.FromErrorDetails(res.Error)}
	}

	return &api.SearchIndexResponse{
		Hits:   fromHTTPHits(res.Result.Hits),
		Facets: fromHTTPFacets(res.Result.Facets),
		Meta:   fromHTTPSearchMeta(res.Result.Meta),
	}, nil
}

func fromHTTPHits(incoming []*apiHTTP.SearchHit) []*SearchHit {
	var hits []*SearchHit
	for _, h := range incoming {
		if h.Data != nil {
			hit := &SearchHit{Data: h.Data}

			if h.Metadata != nil {
				hit.Metadata = &api.SearchHitMeta{}
				if h.Metadata.CreatedAt != nil {
					hit.Metadata.CreatedAt = timestamppb.New(*h.Metadata.CreatedAt)
				}
				if h.Metadata.UpdatedAt != nil {
					hit.Metadata.UpdatedAt = timestamppb.New(*h.Metadata.UpdatedAt)
				}
			}

			hits = append(hits, hit)
		}
	}

	return hits
}

func fromHTTPFacets(incoming map[string]*apiHTTP.SearchFacet) map[string]*api.SearchFacet {
	if incoming == nil {
		return nil
	}

	facets := make(map[string]*api.SearchFacet)
	for k, f := range incoming {
		facetCopy := &api.SearchFacet{}
		if f.Stats != nil {
			statsCopy := &api.FacetStats{
				Avg: f.Stats.Avg,
				Sum: f.Stats.Sum,
				Min: f.Stats.Min,
				Max: f.Stats.Max,
			}
			if f.Stats.Count != nil {
				statsCopy.Count = *f.Stats.Count
			}

			facetCopy.Stats = statsCopy
		}
		if f.Counts != nil {
			var countsCopy []*api.FacetCount
			for _, c := range *f.Counts {
				copyCount := &api.FacetCount{}
				if c.Count != nil {
					copyCount.Count = *c.Count
				}
				if c.Value != nil {
					copyCount.Value = *c.Value
				}

				countsCopy = append(countsCopy, copyCount)
			}
			facetCopy.Counts = countsCopy
		}
		facets[k] = facetCopy
	}

	return facets
}

func fromHTTPSearchMeta(incoming *apiHTTP.SearchMetadata) *api.SearchMetadata {
	if incoming == nil {
		return nil
	}

	meta := &api.SearchMetadata{}
	if incoming.MatchedFields != nil {
		meta.MatchedFields = *incoming.MatchedFields
	}
	if incoming.Page != nil {
		meta.Page = &api.Page{}
		if incoming.Page.Current != nil {
			meta.Page.Current = *incoming.Page.Current
		}
		if incoming.Page.Size != nil {
			meta.Page.Size = *incoming.Page.Size
		}
	}
	if incoming.Found != nil {
		meta.Found = *incoming.Found
	}
	if incoming.TotalPages != nil {
		meta.TotalPages = *incoming.TotalPages
	}

	return meta
}

func (g *httpSearchIndexReader) close() error {
	return g.closer.Close()
}
