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

	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/tigris-client-go/config"
	"github.com/tigrisdata/tigris-client-go/test"
)

func TestGRPCObservabilityDriver(t *testing.T) {
	_, client, mockServers, cancel := SetupO11yGRPCTests(t, &config.Driver{})
	defer cancel()
	testDriverObservability(t, client, mockServers.O11y)
	testDriverObservabilityNegative(t, client, mockServers.O11y)
}

func TestNewO11yGRPC(t *testing.T) {
	_, cancel := test.SetupTests(t, 4)
	defer cancel()

	DefaultProtocol = GRPC

	certPool := x509.NewCertPool()
	require.True(t, certPool.AppendCertsFromPEM([]byte(test.CaCert)))

	cfg := config.Driver{
		URL: test.GRPCURL(4),
		TLS: &tls.Config{RootCAs: certPool, ServerName: "localhost", MinVersion: tls.VersionTLS12},
	}
	client, err := NewObservability(context.Background(), &cfg)
	require.NoError(t, err)

	_ = client.Close()
	DefaultProtocol = "SOMETHING"
	_, err = NewObservability(context.Background(), nil)
	require.Error(t, err)

	DefaultProtocol = GRPC
}
