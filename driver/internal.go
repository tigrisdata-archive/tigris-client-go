// Copyright 2022 Tigris Data, Inc.
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
	"net/http"
	"strings"

	"github.com/tigrisdata/tigris-client-go/config"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"
)

type driverWithOptions interface {
	beginTxWithOptions(ctx context.Context, db string, options *TxOptions) (txWithOptions, error)
	describeDatabaseWithOptions(ctx context.Context, options *DescribeDatabaseOptions) (*DescribeDatabaseResponse, error)

	Info(ctx context.Context) (*InfoResponse, error)
	Health(ctx context.Context) (*HealthResponse, error)
	UseDatabase() Database
	Close() error
}

type CRUDWithOptions interface {
	insertWithOptions(ctx context.Context, collection string, docs []Document, options *InsertOptions) (
		*InsertResponse, error)
	replaceWithOptions(ctx context.Context, collection string, docs []Document, options *ReplaceOptions) (
		*ReplaceResponse, error)
	readWithOptions(ctx context.Context, collection string, filter Filter, fields Projection, options *ReadOptions) (
		Iterator, error)
	search(ctx context.Context, collection string, req *SearchRequest) (SearchResultIterator, error)
	updateWithOptions(ctx context.Context, collection string, filter Filter, fields Update, options *UpdateOptions) (
		*UpdateResponse, error)
	deleteWithOptions(ctx context.Context, collection string, filter Filter, options *DeleteOptions) (
		*DeleteResponse, error)
	createOrUpdateCollectionWithOptions(ctx context.Context, collection string, schema Schema,
		options *CreateCollectionOptions) error
	dropCollectionWithOptions(ctx context.Context, collection string, options *CollectionOptions) error
	listCollectionsWithOptions(ctx context.Context, options *CollectionOptions) ([]string, error)
	describeCollectionWithOptions(ctx context.Context, collection string, options *DescribeCollectionOptions) (*DescribeCollectionResponse, error)
}

type txWithOptions interface {
	CRUDWithOptions
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
}

// func configAuth(config *config.Driver) (*clientcredentials.Config, context.Context) {.
func configAuth(cfg *config.Driver) (oauth2.TokenSource, *http.Client, string) {
	tr := &http.Transport{
		TLSClientConfig: cfg.TLS,
	}

	tokenURL := cfg.URL + "/v1/auth/token"
	tokenURL = strings.TrimPrefix(tokenURL, "dns:")

	if !strings.Contains(tokenURL, "://") {
		tokenURL = "https://" + tokenURL
	}

	if TokenURLOverride != "" {
		tokenURL = TokenURLOverride
	}

	ctxClient := context.WithValue(context.Background(),
		oauth2.HTTPClient, &http.Client{Transport: tr, Timeout: tokenRequestTimeout})

	var (
		ts     oauth2.TokenSource
		client *http.Client
	)

	// use access-token authentication if it's set,
	// use client_id, client_secret otherwise
	if cfg.Token != "" {
		t := &oauth2.Token{AccessToken: cfg.Token}
		ocfg1 := &oauth2.Config{Endpoint: oauth2.Endpoint{TokenURL: tokenURL}}
		client = ocfg1.Client(ctxClient, t)
		ts = ocfg1.TokenSource(ctxClient, t)
	} else if cfg.ClientID != "" || cfg.ClientSecret != "" {
		oCfg := &clientcredentials.Config{
			TokenURL: tokenURL, ClientID: cfg.ClientID, ClientSecret: cfg.ClientSecret,
			AuthStyle: oauth2.AuthStyleInParams,
		}
		client = oCfg.Client(ctxClient)
		ts = oCfg.TokenSource(ctxClient)
	}

	// token source is configured for GRPC token retrieval.
	// client is configured for HTTP token retrieval
	return ts, client, tokenURL
}

func PtrToString(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func PtrToInt64(b *int64) int64 {
	if b == nil {
		return 0
	}
	return *b
}
