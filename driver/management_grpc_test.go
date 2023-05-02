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
	"crypto/tls"
	"crypto/x509"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/tigris-client-go/config"
	"github.com/tigrisdata/tigris-client-go/test"
)

func TestGRPCDriverToken(t *testing.T) {
	t.Run("config", func(t *testing.T) {
		// this is needed because in the tests GRPC and HTTP servers listen on different ports
		// HTTP - 33333, GRPC 33334
		TokenURLOverride = testTokenURLOverride

		client, _, mockServers, cancel := SetupMgmtGRPCTests(t, &config.Driver{
			URL:   test.GRPCURL(0),
			Token: "token_grpc_config_1",
		})
		defer cancel()

		testDriverToken(t, client, mockServers.API, mockServers.Auth, "token_grpc_config_1", false)
	})

	t.Run("env", func(t *testing.T) {
		TokenURLOverride = testTokenURLOverride

		t.Setenv(EnvToken, "token_grpc_env_1")

		cfg, err := initConfig(&config.Driver{URL: test.GRPCURL(0)})
		require.NoError(t, err)

		client, _, mockServers, cancel := SetupMgmtGRPCTests(t, cfg)
		defer cancel()

		testDriverToken(t, client, mockServers.API, mockServers.Auth, "token_grpc_env_1", false)
	})
}

func TestGRPCAuthDriver(t *testing.T) {
	drv, managementClient, mockServers, cancel := SetupMgmtGRPCTests(t, &config.Driver{})
	defer cancel()
	testDriverAuth(t, drv, managementClient, mockServers.API, mockServers.Auth, mockServers.Mgmt)
	testDriverAuthNegative(t, drv, managementClient, mockServers.API, mockServers.Mgmt)
}

func TestGRPCDriverCredentials(t *testing.T) {
	t.Run("config", func(t *testing.T) {
		TokenURLOverride = testTokenURLOverride
		client, _, mockServers, cancel := SetupMgmtGRPCTests(t, &config.Driver{
			URL:          test.GRPCURL(0),
			ClientID:     "client_id_test",
			ClientSecret: "client_secret_test",
		})
		defer cancel()

		testDriverToken(t, client, mockServers.API, mockServers.Auth, "token_config_123", true)
		// token expiry timeout is 11 seconds. oauth2 timeout delta is 10 seconds.
		time.Sleep(1 * time.Second)
		// check that new token requested after expiry
		testDriverToken(t, client, mockServers.API, mockServers.Auth, "token_config_123", true)
	})

	t.Run("env", func(t *testing.T) {
		TokenURLOverride = testTokenURLOverride

		t.Setenv(EnvClientID, "client_id_test")
		t.Setenv(EnvClientSecret, "client_secret_test")

		cfg, err := initConfig(&config.Driver{URL: test.GRPCURL(0)})
		require.NoError(t, err)

		client, _, mockServers, cancel := SetupMgmtGRPCTests(t, cfg)
		defer cancel()

		testDriverToken(t, client, mockServers.API, mockServers.Auth, "token_config_123", true)
	})
}

func TestGRPCGetInfo(t *testing.T) {
	client, _, mockServers, cancel := SetupMgmtGRPCTests(t, &config.Driver{
		URL: test.GRPCURL(0),
	})
	defer cancel()

	testGetInfo(t, client, mockServers.O11y)
}

func TestNewAuthGRPC(t *testing.T) {
	_, cancel := test.SetupTests(t, 4)
	defer cancel()

	DefaultProtocol = GRPC

	certPool := x509.NewCertPool()
	require.True(t, certPool.AppendCertsFromPEM([]byte(test.CaCert)))

	cfg := config.Driver{
		URL: test.GRPCURL(4),
		TLS: &tls.Config{RootCAs: certPool, ServerName: "localhost", MinVersion: tls.VersionTLS12},
	}
	client, err := NewManagement(context.Background(), &cfg)
	require.NoError(t, err)

	_ = client.Close()
	DefaultProtocol = "SOMETHING"
	_, err = NewManagement(context.Background(), nil)
	require.Error(t, err)

	DefaultProtocol = GRPC
}
