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
	"encoding/json"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	api "github.com/tigrisdata/tigris-client-go/api/server/v1"
	"github.com/tigrisdata/tigris-client-go/config"
	mock "github.com/tigrisdata/tigris-client-go/mock/api"
	"github.com/tigrisdata/tigris-client-go/test"
)

func testSearchBasic(t *testing.T, c Driver, mc *mock.MockSearchServer) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	search := c.UseSearch("p1")

	t.Run("search", func(t *testing.T) {
		dv := 1.234
		searchResp := api.SearchIndexResponse{
			Hits: []*IndexDoc{
				{Doc: []byte(`{"doc1":"value1"}`)},
			},
			Facets: map[string]*api.SearchFacet{
				"one": {
					Counts: []*api.FacetCount{
						{
							Count: 10,
							Value: "value1",
						},
					},
					Stats: &api.FacetStats{
						Avg:   &dv,
						Max:   &dv,
						Min:   &dv,
						Sum:   &dv,
						Count: 5,
					},
				},
			},
			Meta: &api.SearchMetadata{
				Found:      10,
				TotalPages: 2,
				Page: &api.Page{
					Current: 2,
					Size:    20,
				},
			},
		}

		mc.EXPECT().Search(
			pm(&api.SearchIndexRequest{
				Project:       "p1",
				Index:         "c1",
				Q:             "search text",
				SearchFields:  []string{"field_1"},
				Facet:         []byte(`{"field_1":{"size":10},"field_2":{"size":10}}`),
				IncludeFields: nil,
				ExcludeFields: nil,
				Sort:          []byte(`{"sort_1":"desc"}`),
				Filter:        []byte(`{"filter_1":"desc"}`),
				PageSize:      12,
				Page:          3,
			}), gomock.Any()).DoAndReturn(func(r *api.SearchIndexRequest, srv api.Search_SearchServer) error {
			err := srv.Send(&searchResp)
			require.NoError(t, err)

			return &api.TigrisError{Code: api.Code_DATA_LOSS, Message: "error_stream"}
		})

		sit, err := search.Search(ctx, "c1", &SearchRequest{
			Q:            "search text",
			SearchFields: []string{"field_1"},
			Facet:        Facet(`{"field_1":{"size":10},"field_2":{"size":10}}`),
			Sort:         SortOrder(`{"sort_1":"desc"}`),
			Filter:       Filter(`{"filter_1":"desc"}`),
			PageSize:     12,
			Page:         3,
		})
		require.NoError(t, err)

		var r SearchIndexResponse

		assert.True(t, sit.Next(&r))
		require.NoError(t, sit.Err())

		res, err := json.Marshal(r)
		require.NoError(t, err)

		exp, err := json.Marshal(&searchResp)
		require.NoError(t, err)

		require.JSONEq(t, string(exp), string(res))

		require.False(t, sit.Next(&r))
		require.Equal(t, &Error{&api.TigrisError{Code: api.Code_DATA_LOSS, Message: "error_stream"}}, sit.Err())
	})

	t.Run("create_or_update_index", func(t *testing.T) {
		mc.EXPECT().CreateOrUpdateIndex(gomock.Any(),
			pm(&api.CreateOrUpdateIndexRequest{
				Project: "p1",
				Name:    "idx1",
				Schema:  []byte(`{"field_1":"$desc"}`),
			})).Return(&api.CreateOrUpdateIndexResponse{Message: "created"}, nil)

		err := search.CreateOrUpdateIndex(ctx, "idx1", []byte(`{"field_1":"$desc"}`))
		require.NoError(t, err)
	})

	t.Run("delete_index", func(t *testing.T) {
		mc.EXPECT().DeleteIndex(gomock.Any(),
			pm(&api.DeleteIndexRequest{
				Project: "p1",
				Name:    "idx1",
			})).Return(&api.DeleteIndexResponse{Message: "deleted"}, nil)

		err := search.DeleteIndex(ctx, "idx1")
		require.NoError(t, err)
	})

	t.Run("get_index", func(t *testing.T) {
		mc.EXPECT().GetIndex(gomock.Any(),
			pm(&api.GetIndexRequest{
				Project: "p1",
				Name:    "idx1",
			})).Return(&api.GetIndexResponse{Index: &api.IndexInfo{Name: "idx1", Schema: []byte(`{"schema_idx1":"value1"}`)}}, nil)

		idx, err := search.GetIndex(ctx, "idx1")
		require.NoError(t, err)
		require.Equal(t, &IndexInfo{Name: "idx1", Schema: []byte(`{"schema_idx1":"value1"}`)}, idx)
	})

	t.Run("list_indexes", func(t *testing.T) {
		// one result
		mc.EXPECT().ListIndexes(gomock.Any(),
			pm(&api.ListIndexesRequest{
				Project: "p1",
				Filter:  &IndexSource{},
			})).Return(&api.ListIndexesResponse{Indexes: []*api.IndexInfo{{Name: "idx1", Schema: []byte(`{"schema_idx1":"value1"}`)}}}, nil)

		idxes, err := search.ListIndexes(ctx, nil)
		require.NoError(t, err)
		require.Equal(t, []*IndexInfo{{Name: "idx1", Schema: []byte(`{"schema_idx1":"value1"}`)}}, idxes)

		// two results
		mc.EXPECT().ListIndexes(gomock.Any(),
			pm(&api.ListIndexesRequest{
				Project: "p1",
				Filter:  &IndexSource{},
			})).Return(&api.ListIndexesResponse{Indexes: []*api.IndexInfo{
			{Name: "idx1", Schema: []byte(`{"schema_idx1":"value1"}`)},
			{Name: "idx2", Schema: []byte(`{"schema_idx2":"value2"}`)},
		}}, nil)

		idxes, err = search.ListIndexes(ctx, nil)
		require.NoError(t, err)
		require.Equal(t, []*IndexInfo{
			{Name: "idx1", Schema: []byte(`{"schema_idx1":"value1"}`)},
			{Name: "idx2", Schema: []byte(`{"schema_idx2":"value2"}`)},
		}, idxes)

		// empty result
		mc.EXPECT().ListIndexes(gomock.Any(),
			pm(&api.ListIndexesRequest{
				Project: "p1",
				Filter:  &IndexSource{},
			})).Return(&api.ListIndexesResponse{}, nil)

		idxes, err = search.ListIndexes(ctx, &IndexSource{})
		require.NoError(t, err)
		require.Equal(t, []*IndexInfo(nil), idxes)
	})

	t.Run("get", func(t *testing.T) {
		// empty result
		mc.EXPECT().Get(gomock.Any(),
			pm(&api.GetDocumentRequest{
				Project: "p1",
				Index:   "idx1",
				Ids:     []string{"id1"},
			})).Return(&api.GetDocumentResponse{Documents: []*IndexDoc{}}, nil)

		docs, err := search.Get(ctx, "idx1", []string{"id1"})
		require.NoError(t, err)
		require.Equal(t, []*IndexDoc(nil), docs)

		// one result
		mc.EXPECT().Get(gomock.Any(),
			pm(&api.GetDocumentRequest{
				Project: "p1",
				Index:   "idx1",
				Ids:     []string{"id1", "id2"},
			})).Return(&api.GetDocumentResponse{Documents: []*IndexDoc{{Doc: []byte(`{"doc1":"value1"}`)}}}, nil)

		docs, err = search.Get(ctx, "idx1", []string{"id1", "id2"})
		require.NoError(t, err)
		require.Equal(t, []*IndexDoc{{Doc: []byte(`{"doc1":"value1"}`)}}, docs)

		// two result
		mc.EXPECT().Get(gomock.Any(),
			pm(&api.GetDocumentRequest{
				Project: "p1",
				Index:   "idx1",
				Ids:     []string{"id1", "id2"},
			})).Return(&api.GetDocumentResponse{Documents: []*IndexDoc{{Doc: []byte(`{"doc1":"value1"}`)}, {Doc: []byte(`{"doc2":"value2"}`)}}}, nil)

		docs, err = search.Get(ctx, "idx1", []string{"id1", "id2"})
		require.NoError(t, err)
		require.Equal(t, []*IndexDoc{{Doc: []byte(`{"doc1":"value1"}`)}, {Doc: []byte(`{"doc2":"value2"}`)}}, docs)
	})

	t.Run("create_by_id", func(t *testing.T) {
		mc.EXPECT().CreateById(gomock.Any(),
			pm(&api.CreateByIdRequest{
				Project:  "p1",
				Index:    "idx1",
				Id:       "id1",
				Document: []byte(`{"doc1":"value1"}`),
			})).Return(&api.CreateByIdResponse{Id: "id1"}, nil)

		err := search.CreateByID(ctx, "idx1", "id1", []byte(`{"doc1":"value1"}`))
		require.NoError(t, err)
	})

	t.Run("create", func(t *testing.T) {
		mc.EXPECT().Create(gomock.Any(),
			pm(&api.CreateDocumentRequest{
				Project:   "p1",
				Index:     "idx1",
				Documents: [][]byte{[]byte(`{"doc1":"value1"}`), []byte(`{"doc2":"value2"}`)},
			})).Return(&api.CreateDocumentResponse{Status: []*api.DocStatus{{Id: "id1"}, {Id: "id2", Error: &api.Error{Code: 123, Message: "error1"}}}}, nil)

		status, err := search.Create(ctx, "idx1", []json.RawMessage{json.RawMessage(`{"doc1":"value1"}`), json.RawMessage(`{"doc2":"value2"}`)})
		require.NoError(t, err)
		require.Equal(t, []*DocStatus{{Id: "id1"}, {Id: "id2", Error: &api.Error{Code: 123, Message: "error1"}}}, status)
	})

	t.Run("create_or_replace", func(t *testing.T) {
		mc.EXPECT().CreateOrReplace(gomock.Any(),
			pm(&api.CreateOrReplaceDocumentRequest{
				Project:   "p1",
				Index:     "idx1",
				Documents: [][]byte{[]byte(`{"doc1":"value1"}`), []byte(`{"doc2":"value2"}`)},
			})).Return(&api.CreateOrReplaceDocumentResponse{Status: []*api.DocStatus{{Id: "id1"}, {Id: "id2", Error: &api.Error{Code: 123, Message: "error1"}}}}, nil)

		status, err := search.CreateOrReplace(ctx, "idx1", []json.RawMessage{json.RawMessage(`{"doc1":"value1"}`), json.RawMessage(`{"doc2":"value2"}`)})
		require.NoError(t, err)
		require.Equal(t, []*DocStatus{{Id: "id1"}, {Id: "id2", Error: &api.Error{Code: 123, Message: "error1"}}}, status)
	})

	t.Run("update", func(t *testing.T) {
		mc.EXPECT().Update(gomock.Any(),
			pm(&api.UpdateDocumentRequest{
				Project:   "p1",
				Index:     "idx1",
				Documents: [][]byte{[]byte(`{"doc1":"value1"}`), []byte(`{"doc2":"value2"}`)},
			})).Return(&api.UpdateDocumentResponse{Status: []*api.DocStatus{{Id: "id1"}, {Id: "id2", Error: &api.Error{Code: 123, Message: "error1"}}}}, nil)

		status, err := search.Update(ctx, "idx1", []json.RawMessage{json.RawMessage(`{"doc1":"value1"}`), json.RawMessage(`{"doc2":"value2"}`)})
		require.NoError(t, err)
		require.Equal(t, []*DocStatus{{Id: "id1"}, {Id: "id2", Error: &api.Error{Code: 123, Message: "error1"}}}, status)
	})

	t.Run("delete", func(t *testing.T) {
		mc.EXPECT().Delete(gomock.Any(),
			pm(&api.DeleteDocumentRequest{
				Project: "p1",
				Index:   "idx1",
				Ids:     []string{"id1", "id2"},
			})).Return(&api.DeleteDocumentResponse{Status: []*api.DocStatus{{Id: "id1"}, {Id: "id2", Error: &api.Error{Code: 123, Message: "error1"}}}}, nil)

		status, err := search.Delete(ctx, "idx1", []string{"id1", "id2"})
		require.NoError(t, err)
		require.Equal(t, []*DocStatus{{Id: "id1"}, {Id: "id2", Error: &api.Error{Code: 123, Message: "error1"}}}, status)
	})

	t.Run("delete_by_query", func(t *testing.T) {
		mc.EXPECT().DeleteByQuery(gomock.Any(),
			pm(&api.DeleteByQueryRequest{
				Project: "p1",
				Index:   "idx1",
				Filter:  []byte(`{"filter1":"value1"}`),
			})).Return(&api.DeleteByQueryResponse{Count: 1}, nil)

		count, err := search.DeleteByQuery(ctx, "idx1", json.RawMessage(`{"filter1":"value1"}`))
		require.NoError(t, err)
		require.Equal(t, int32(1), count)
	})
}

func setupSearchGRPCTests(t *testing.T, config *config.Driver) (Driver, *grpcDriver, *test.MockServers, func()) {
	return setupGRPCTests(t, config)
}

func setupSearchHTTPTests(t *testing.T, config *config.Driver) (Driver, *httpDriver, *test.MockServers, func()) {
	return setupHTTPTests(t, config)
}

func TestSearchGRPCDriver(t *testing.T) {
	client, _, mockServer, cancel := setupSearchGRPCTests(t, &config.Driver{})
	defer cancel()
	testSearchBasic(t, client, mockServer.Search)
}

func TestSearchHTTPDriver(t *testing.T) {
	client, _, mockServer, cancel := setupSearchHTTPTests(t, &config.Driver{})
	defer cancel()
	testSearchBasic(t, client, mockServer.Search)
}
