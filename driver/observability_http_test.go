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

	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/tigris-client-go/config"
	"github.com/tigrisdata/tigris-client-go/test"
)

func TestHTTPObservabilityDriver(t *testing.T) {
	_, client, mockServers, cancel := SetupO11yHTTPTests(t, &config.Driver{})
	defer cancel()
	testDriverObservability(t, client, mockServers.O11y)
	testDriverObservabilityNegative(t, client, mockServers.O11y)
}

func TestNewO11yHTTP(t *testing.T) {
	_, cancel := test.SetupTests(t, 4)
	defer cancel()

	DefaultProtocol = HTTP
	cfg := config.Driver{URL: test.HTTPURL(4)}
	client, err := NewObservability(context.Background(), &cfg)
	require.NoError(t, err)

	_ = client.Close()
}
