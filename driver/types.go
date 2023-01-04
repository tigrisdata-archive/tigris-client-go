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
	"encoding/json"
	"time"

	api "github.com/tigrisdata/tigris-client-go/api/server/v1"
)

const (
	GRPC  = "GRPC"
	HTTP  = "HTTP"
	HTTPS = "HTTPS"

	EnvClientID     = "TIGRIS_CLIENT_ID"
	EnvClientSecret = "TIGRIS_CLIENT_SECRET" //nolint:golint,gosec
	EnvToken        = "TIGRIS_TOKEN"         //nolint:golint,gosec
	EnvProtocol     = "TIGRIS_PROTOCOL"
	EnvURL          = "TIGRIS_URL"
	EnvProject      = "TIGRIS_PROJECT"

	Version   = "v1.0.0"
	UserAgent = "tigris-client-go/" + Version

	tokenRequestTimeout = 15 * time.Second
)

var (
	DefaultProtocol = GRPC
	DefaultURL      = "api.preview.tigrisdata.cloud"

	// TokenURLOverride Only used in tests to point auth to proper HTTP port in GRPC tests.
	TokenURLOverride string
)

type (
	Document   json.RawMessage
	Message    json.RawMessage
	Filter     json.RawMessage
	Projection json.RawMessage
	Update     json.RawMessage
	Schema     json.RawMessage
	Facet      json.RawMessage
	SortOrder  json.RawMessage
)

type (
	InsertOptions  api.InsertRequestOptions
	ReplaceOptions api.ReplaceRequestOptions
	UpdateOptions  api.UpdateRequestOptions
	DeleteOptions  api.DeleteRequestOptions
	ReadOptions    api.ReadRequestOptions
	WriteOptions   api.WriteOptions
	Collation      api.Collation
)

type (
	CreateCollectionOptions struct {
		OnlyCreate bool
	}

	CollectionOptions api.CollectionOptions
	TxOptions         api.TransactionOptions

	DescribeProjectOptions struct {
		SchemaFormat string
		Branch       string
	}

	DeleteProjectOptions struct {
		Branch string
	}
	CreateProjectOptions struct {
		Branch string
	}

	DescribeCollectionOptions struct {
		SchemaFormat string
	}
)

type (
	InsertResponse  api.InsertResponse
	ReplaceResponse api.ReplaceResponse
	UpdateResponse  api.UpdateResponse
	DeleteResponse  api.DeleteResponse
)

type (
	DescribeDatabaseResponse   api.DescribeDatabaseResponse
	ListProjectsResponse       api.ListProjectsResponse
	CreateProjectResponse      api.CreateProjectResponse
	DeleteProjectResponse      api.DeleteProjectResponse
	DescribeCollectionResponse api.DescribeCollectionResponse
)

type (
	InfoResponse   api.GetInfoResponse
	HealthResponse api.HealthCheckResponse
)

type SearchRequest struct {
	Q             string
	SearchFields  []string
	Filter        Filter
	Facet         Facet
	Sort          SortOrder
	IncludeFields []string
	ExcludeFields []string
	Page          int32
	PageSize      int32
}
type SearchResponse *api.SearchResponse

type Error struct {
	*api.TigrisError
}

// As converts driver.Error the error which implements AsTigrisError interface.
func (e *Error) As(i any) bool {
	if x, ok := i.(interface{ AsTigrisError(*Error) bool }); ok && x.AsTigrisError(e) {
		return true
	}
	return false
}

type (
	AppKey        api.AppKey
	TokenResponse api.GetAccessTokenResponse
	Namespace     api.NamespaceInfo

	QuotaLimits api.QuotaLimitsResponse
	QuotaUsage  api.QuotaUsageResponse
)
