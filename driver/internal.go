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
	"os"
	"strings"

	"github.com/tigrisdata/tigris-client-go/config"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"
)

type driverWithOptions interface {
	createDatabaseWithOptions(ctx context.Context, db string, options *DatabaseOptions) error
	dropDatabaseWithOptions(ctx context.Context, db string, options *DatabaseOptions) error
	beginTxWithOptions(ctx context.Context, db string, options *TxOptions) (txWithOptions, error)

	Info(ctx context.Context) (*InfoResponse, error)
	UseDatabase(name string) Database
	ListDatabases(ctx context.Context) ([]string, error)
	DescribeDatabase(ctx context.Context, db string) (*DescribeDatabaseResponse, error)
	Close() error
}

type CRUDWithOptions interface {
	insertWithOptions(ctx context.Context, collection string, docs []Document, options *InsertOptions) (*InsertResponse, error)
	replaceWithOptions(ctx context.Context, collection string, docs []Document, options *ReplaceOptions) (*ReplaceResponse, error)
	readWithOptions(ctx context.Context, collection string, filter Filter, fields Projection, options *ReadOptions) (Iterator, error)
	search(ctx context.Context, collection string, req *SearchRequest) (SearchResultIterator, error)
	updateWithOptions(ctx context.Context, collection string, filter Filter, fields Update, options *UpdateOptions) (*UpdateResponse, error)
	deleteWithOptions(ctx context.Context, collection string, filter Filter, options *DeleteOptions) (*DeleteResponse, error)
	createOrUpdateCollectionWithOptions(ctx context.Context, collection string, schema Schema, options *CollectionOptions) error
	dropCollectionWithOptions(ctx context.Context, collection string, options *CollectionOptions) error
	listCollectionsWithOptions(ctx context.Context, options *CollectionOptions) ([]string, error)
	describeCollectionWithOptions(ctx context.Context, collection string, options *CollectionOptions) (*DescribeCollectionResponse, error)
	publishWithOptions(ctx context.Context, collection string, msgs []Message, options *PublishOptions) (*PublishResponse, error)
	subscribeWithOptions(ctx context.Context, collection string, filter Filter, options *SubscribeOptions) (Iterator, error)
}

type txWithOptions interface {
	CRUDWithOptions
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
}

//func configAuth(config *config.Driver) (*clientcredentials.Config, context.Context) {
func configAuth(config *config.Driver) (oauth2.TokenSource, *http.Client, string) {
	clientId := config.ClientId
	if os.Getenv(ApplicationID) != "" {
		clientId = os.Getenv(ApplicationID)
	}

	clientSecret := config.ClientSecret
	if os.Getenv(ApplicationSecret) != "" {
		clientSecret = os.Getenv(ApplicationSecret)
	}

	token := config.Token
	if os.Getenv(Token) != "" {
		token = os.Getenv(Token)
	}

	tr := &http.Transport{
		TLSClientConfig: config.TLS,
	}

	tokenURL := config.URL + "/v1/auth/token"
	tokenURL = strings.TrimPrefix(tokenURL, "dns:")
	if !strings.Contains(tokenURL, "://") {
		tokenURL = "https://" + tokenURL
	}

	if TokenURLOverride != "" {
		tokenURL = TokenURLOverride
	}

	ctxClient := context.WithValue(context.Background(), oauth2.HTTPClient, &http.Client{Transport: tr, Timeout: tokenRequestTimeout})

	var ts oauth2.TokenSource
	var client *http.Client

	// use access-token authentication if it's set,
	// use client_id, client_secret otherwise
	if token != "" {
		t := &oauth2.Token{AccessToken: token}
		ocfg1 := &oauth2.Config{Endpoint: oauth2.Endpoint{TokenURL: tokenURL}}
		client = ocfg1.Client(ctxClient, t)
		ts = ocfg1.TokenSource(ctxClient, t)
	} else if clientId != "" || clientSecret != "" {
		oCfg := &clientcredentials.Config{TokenURL: tokenURL, ClientID: clientId, ClientSecret: clientSecret, AuthStyle: oauth2.AuthStyleInParams}
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

func PtrToBytes(b *[]byte) []byte {
	if b == nil {
		return nil
	}
	return *b
}

func PtrToBool(b *bool) bool {
	if b == nil {
		return false
	}
	return *b
}

func PtrToInt64(b *int64) int64 {
	if b == nil {
		return 0
	}
	return *b
}
