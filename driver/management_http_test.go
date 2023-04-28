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
	"github.com/tigrisdata/tigris-client-go/config"
	"github.com/tigrisdata/tigris-client-go/test"
)

func TestHTTPAuthDriver(t *testing.T) {
	TokenURLOverride = ""
	drv, managementClient, mockServers, cancel := SetupMgmtHTTPTests(t, &config.Driver{})
	defer cancel()
	testDriverAuth(t, drv, managementClient, mockServers.API, mockServers.Auth, mockServers.Mgmt)
}

func TestHTTPGetInfo(t *testing.T) {
	client, _, mockServers, cancel := SetupMgmtHTTPTests(t, &config.Driver{
		URL: test.HTTPURL(2),
	})
	defer cancel()

	testGetInfo(t, client, mockServers.O11y)
}

func TestHTTPDriverToken(t *testing.T) {
	t.Run("config", func(t *testing.T) {
		TokenURLOverride = ""
		client, _, mockServers, cancel := SetupMgmtHTTPTests(t, &config.Driver{
			URL:   test.HTTPURL(2),
			Token: "token_http_config_1",
		})
		defer cancel()

		testDriverToken(t, client, mockServers.API, mockServers.Auth, "token_http_config_1", false)
	})

	t.Run("env", func(t *testing.T) {
		TokenURLOverride = ""

		t.Setenv(EnvToken, "token_http_env_1")

		cfg, err := initConfig(&config.Driver{URL: test.HTTPURL(2)})
		require.NoError(t, err)

		client, _, mockServers, cancel := SetupMgmtHTTPTests(t, cfg)
		defer cancel()

		testDriverToken(t, client, mockServers.API, mockServers.Auth, "token_http_env_1", false)
	})
}

func TestHTTPDriverCredentials(t *testing.T) {
	t.Run("config", func(t *testing.T) {
		TokenURLOverride = ""
		client, _, mockServers, cancel := SetupMgmtHTTPTests(t, &config.Driver{
			URL:          test.HTTPURL(2),
			ClientID:     "client_id_test",
			ClientSecret: "client_secret_test",
		})
		defer cancel()

		testDriverToken(t, client, mockServers.API, mockServers.Auth, "token_config_123", true)
		time.Sleep(1 * time.Second)
		// check that new token requested after expiry
		testDriverToken(t, client, mockServers.API, mockServers.Auth, "token_config_123", true)
	})

	t.Run("env", func(t *testing.T) {
		TokenURLOverride = ""
		t.Setenv(EnvClientID, "client_id_test")
		t.Setenv(EnvClientSecret, "client_secret_test")

		cfg, err := initConfig(&config.Driver{URL: test.HTTPURL(2)})
		require.NoError(t, err)

		client, _, mockServers, cancel := SetupMgmtHTTPTests(t, cfg)
		defer cancel()

		testDriverToken(t, client, mockServers.API, mockServers.Auth, "token_config_123", true)
	})
}

func TestNewAuthHTTP(t *testing.T) {
	_, cancel := test.SetupTests(t, 4)
	defer cancel()

	DefaultProtocol = HTTP
	cfg := config.Driver{URL: test.HTTPURL(4)}
	client, err := NewManagement(context.Background(), &cfg)
	require.NoError(t, err)

	_ = client.Close()
}
