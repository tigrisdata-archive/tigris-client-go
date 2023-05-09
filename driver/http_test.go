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

package driver

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	api "github.com/tigrisdata/tigris-client-go/api/server/v1"
	"github.com/tigrisdata/tigris-client-go/config"
	mock "github.com/tigrisdata/tigris-client-go/mock/api"
	"github.com/tigrisdata/tigris-client-go/test"
	metadata2 "google.golang.org/grpc/metadata"
)

func SetupO11yHTTPTests(t *testing.T, config *config.Driver) (Driver, Observability, *test.MockServers, func()) {
	return setupHTTPTests(t, config)
}

func SetupMgmtHTTPTests(t *testing.T, config *config.Driver) (Driver, Management, *test.MockServers, func()) {
	return setupHTTPTests(t, config)
}

func SetupHTTPTests(t *testing.T, config *config.Driver) (Driver, *mock.MockTigrisServer, func()) {
	// not user by http so set it to empty
	setMetadata = func(ctx context.Context, txCtx *api.TransactionCtx, md metadata2.MD) {}

	d, _, s, f := setupHTTPTests(t, config)
	return d, s.API, f
}

func setupHTTPTests(t *testing.T, config *config.Driver) (Driver, *httpDriver, *test.MockServers, func()) {
	mockServers, cancel := test.SetupTests(t, 2)
	url := test.HTTPURL(2)
	config.URL = url
	config.TLS = test.SetupTLS(t)
	client, _, _, err := newHTTPClient(context.Background(), config)
	require.NoError(t, err)

	// FIXME: implement proper wait for HTTP server to start
	time.Sleep(10 * time.Millisecond)

	return &driver{driverWithOptions: client, cfg: config}, client.(*httpDriver), mockServers, func() { cancel(); _ = client.Close() }
}

func TestHTTPHeaders(t *testing.T) {
	c, mc, cancel := SetupHTTPTests(t, &config.Driver{SkipSchemaValidation: true})
	defer cancel()

	testSchemaSignOffHeader(t, mc, c)
	testSchemaVersion(t, mc, c)
}

func TestHTTPError(t *testing.T) {
	client, mockServer, cancel := SetupHTTPTests(t, &config.Driver{})
	defer cancel()
	testErrors(t, client, mockServer)
	t.Run("read_stream_error", func(t *testing.T) {
		testReadStreamError(t, client, mockServer)
	})
	t.Run("search_stream_error", func(t *testing.T) {
		testSearchStreamError(t, client, mockServer)
	})
	t.Run("branch_crud_errors", func(t *testing.T) {
		testBranchCrudErrors(t, client, mockServer)
	})
}

func TestHTTPDriver(t *testing.T) {
	client, mockServer, cancel := SetupHTTPTests(t, &config.Driver{})
	defer cancel()
	testDriverBasic(t, client, mockServer)
	testResponseMetadata(t, client, mockServer)
}

func TestTxHTTPDriver(t *testing.T) {
	client, mockServer, cancel := SetupHTTPTests(t, &config.Driver{})
	defer cancel()
	testTxBasic(t, client, mockServer)
}

func TestTxHTTPDriverNegative(t *testing.T) {
	client, mockServer, cancel := SetupHTTPTests(t, &config.Driver{})
	defer cancel()
	testTxBasicNegative(t, client, mockServer)
}

func TestNewDriverHTTP(t *testing.T) {
	_, cancel := test.SetupTests(t, 4)
	defer cancel()

	DefaultProtocol = HTTP
	cfg := config.Driver{URL: test.HTTPURL(4)}
	client, err := NewDriver(context.Background(), &cfg)
	require.NoError(t, err)

	_ = client.Close()
}
