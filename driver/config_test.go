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
	"crypto/tls"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/tigris-client-go/config"
)

func TestDriverConfig(t *testing.T) {
	cTLS := &tls.Config{MinVersion: tls.VersionTLS12}

	cases := []struct {
		name string
		url  string
		cfg  *config.Driver
		err  error
	}{
		{name: "host", url: "host1", cfg: &config.Driver{URL: "host1", Protocol: DefaultProtocol}},
		{name: "host_with_port", url: "host1:333", cfg: &config.Driver{URL: "host1:333", Protocol: DefaultProtocol}},
		{name: "localhost", url: "localhost", cfg: &config.Driver{URL: "localhost", Protocol: DefaultProtocol}},
		{name: "localhost_with_port", url: "localhost:555", cfg: &config.Driver{URL: "localhost:555", Protocol: DefaultProtocol}},
		{name: "ip", url: "127.0.0.1", cfg: &config.Driver{URL: "127.0.0.1", Protocol: DefaultProtocol}},
		{name: "ip_with_port", url: "127.0.0.1:777", cfg: &config.Driver{URL: "127.0.0.1:777", Protocol: DefaultProtocol}},
		{name: "https_ipv6", url: "https://[::1]", cfg: &config.Driver{URL: "[::1]", Protocol: HTTP, TLS: cTLS}},
		{name: "https_ipv6_with_port", url: "https://[::1]:777", cfg: &config.Driver{URL: "[::1]:777", Protocol: HTTP, TLS: cTLS}},
		{name: "ipv6", url: "::1", cfg: &config.Driver{URL: "::1", Protocol: DefaultProtocol}},
		{name: "ipv6_with_port", url: "[::1]:777", cfg: &config.Driver{URL: "[::1]:777", Protocol: DefaultProtocol}},
		{name: "http_host", url: "http://host1", cfg: &config.Driver{URL: "host1", Protocol: HTTP}},
		{name: "http_host_with_port", url: "http://host1:333", cfg: &config.Driver{URL: "host1:333", Protocol: HTTP}},
		{name: "http_localhost", url: "http://localhost", cfg: &config.Driver{URL: "localhost", Protocol: HTTP}},
		{name: "http_localhost_with_port", url: "http://localhost:555", cfg: &config.Driver{URL: "localhost:555", Protocol: HTTP}},
		{name: "http_ip", url: "http://127.0.0.1", cfg: &config.Driver{URL: "127.0.0.1", Protocol: HTTP}},
		{name: "http_ip_with_port", url: "http://127.0.0.1:777", cfg: &config.Driver{URL: "127.0.0.1:777", Protocol: HTTP}},
		{name: "https_host", url: "https://host1", cfg: &config.Driver{URL: "host1", Protocol: HTTP, TLS: cTLS}},
		{name: "https_host_with_port", url: "https://host1:333", cfg: &config.Driver{URL: "host1:333", Protocol: HTTP, TLS: cTLS}},
		{name: "https_localhost", url: "https://localhost", cfg: &config.Driver{URL: "localhost", Protocol: HTTP, TLS: cTLS}},
		{name: "https_localhost_with_port", url: "https://localhost:555", cfg: &config.Driver{URL: "localhost:555", Protocol: HTTP, TLS: cTLS}},
		{name: "https_ip", url: "https://127.0.0.1", cfg: &config.Driver{URL: "127.0.0.1", Protocol: HTTP, TLS: cTLS}},
		{name: "https_ip_with_port", url: "https://127.0.0.1:777", cfg: &config.Driver{URL: "127.0.0.1:777", Protocol: HTTP, TLS: cTLS}},
		{name: "grpc_host", url: "grpc://host1", cfg: &config.Driver{URL: "host1", Protocol: GRPC}},
		{name: "grpc_host_with_port", url: "grpc://host1:333", cfg: &config.Driver{URL: "host1:333", Protocol: GRPC}},
		{name: "grpc_localhost", url: "grpc://localhost", cfg: &config.Driver{URL: "localhost", Protocol: GRPC}},
		{name: "grpc_localhost_with_port", url: "grpc://localhost:555", cfg: &config.Driver{URL: "localhost:555", Protocol: GRPC}},
		{name: "grpc_ip", url: "grpc://127.0.0.1", cfg: &config.Driver{URL: "127.0.0.1", Protocol: GRPC}},
		{name: "grpc_ip_with_port", url: "grpc://127.0.0.1:777", cfg: &config.Driver{URL: "127.0.0.1:777", Protocol: GRPC}},
		{name: "https_host_query", url: "https://host1?client_id=usr1&client_secret=pwd1", cfg: &config.Driver{URL: "host1", Protocol: HTTP, TLS: cTLS, ClientID: "usr1", ClientSecret: "pwd1"}},
		{name: "https_host_with_port_query", url: "https://host1:333?client_id=usr1&client_secret=pwd1", cfg: &config.Driver{URL: "host1:333", Protocol: HTTP, TLS: cTLS, ClientID: "usr1", ClientSecret: "pwd1"}},
		{name: "https_localhost_query", url: "https://localhost?client_id=usr1&client_secret=pwd1", cfg: &config.Driver{URL: "localhost", Protocol: HTTP, TLS: cTLS, ClientID: "usr1", ClientSecret: "pwd1"}},
		{name: "https_localhost_with_port_query", url: "https://localhost:555?client_id=usr1&client_secret=pwd1", cfg: &config.Driver{URL: "localhost:555", Protocol: HTTP, TLS: cTLS, ClientID: "usr1", ClientSecret: "pwd1"}},
		{name: "https_ip_query", url: "https://127.0.0.1?client_id=usr1&client_secret=pwd1", cfg: &config.Driver{URL: "127.0.0.1", Protocol: HTTP, TLS: cTLS, ClientID: "usr1", ClientSecret: "pwd1"}},
		{name: "https_ip_with_port_query", url: "https://127.0.0.1:777?client_id=usr1&client_secret=pwd1", cfg: &config.Driver{URL: "127.0.0.1:777", Protocol: HTTP, TLS: cTLS, ClientID: "usr1", ClientSecret: "pwd1"}},
		{name: "https_host_userinfo", url: "https://usr1:pwd1@host1", cfg: &config.Driver{URL: "host1", Protocol: HTTP, TLS: cTLS, ClientID: "usr1", ClientSecret: "pwd1"}},
		{name: "https_host_with_port_userinfo", url: "https://usr1:pwd1@host1:333", cfg: &config.Driver{URL: "host1:333", Protocol: HTTP, TLS: cTLS, ClientID: "usr1", ClientSecret: "pwd1"}},
		{name: "https_localhost_userinfo", url: "https://usr1:pwd1@localhost", cfg: &config.Driver{URL: "localhost", Protocol: HTTP, TLS: cTLS, ClientID: "usr1", ClientSecret: "pwd1"}},
		{name: "https_localhost_with_port_userinfo", url: "https://usr1:pwd1@localhost:555", cfg: &config.Driver{URL: "localhost:555", Protocol: HTTP, TLS: cTLS, ClientID: "usr1", ClientSecret: "pwd1"}},
		{name: "https_ip_userinfo", url: "https://usr1:pwd1@127.0.0.1", cfg: &config.Driver{URL: "127.0.0.1", Protocol: HTTP, TLS: cTLS, ClientID: "usr1", ClientSecret: "pwd1"}},
		{name: "https_ip_with_port_userinfo", url: "https://usr1:pwd1@127.0.0.1:777", cfg: &config.Driver{URL: "127.0.0.1:777", Protocol: HTTP, TLS: cTLS, ClientID: "usr1", ClientSecret: "pwd1"}},
		{name: "ip_with_port_userinfo", url: "usr1:pwd1@127.0.0.1:777", cfg: &config.Driver{URL: "127.0.0.1:777", Protocol: DefaultProtocol, TLS: cTLS, ClientID: "usr1", ClientSecret: "pwd1"}},
		{name: "dns_host", url: "dns://host1", cfg: &config.Driver{URL: "host1", Protocol: GRPC}},
		{name: "dns_host_with_port", url: "dns://host1:333", cfg: &config.Driver{URL: "host1:333", Protocol: GRPC}},
		{name: "dns_localhost", url: "dns://localhost", cfg: &config.Driver{URL: "localhost", Protocol: GRPC}},
		{name: "dns_localhost_with_port", url: "dns://localhost:555", cfg: &config.Driver{URL: "localhost:555", Protocol: GRPC}},
		{name: "dns_ip", url: "dns://127.0.0.1", cfg: &config.Driver{URL: "127.0.0.1", Protocol: GRPC}},
		{name: "dns_ip_with_port", url: "dns://127.0.0.1:777", cfg: &config.Driver{URL: "127.0.0.1:777", Protocol: GRPC}},

		{name: "https_host_token", url: "https://host1?token=tkn1", cfg: &config.Driver{URL: "host1", Protocol: HTTP, TLS: cTLS, Token: "tkn1"}},
		{name: "https_host_with_port_token", url: "https://host1:333?token=tkn1", cfg: &config.Driver{URL: "host1:333", Protocol: HTTP, TLS: cTLS, Token: "tkn1"}},
		{name: "https_localhost_token", url: "https://localhost?token=tkn1", cfg: &config.Driver{URL: "localhost", Protocol: HTTP, TLS: cTLS, Token: "tkn1"}},
		{name: "https_localhost_with_port_token", url: "https://localhost:555?token=tkn1", cfg: &config.Driver{URL: "localhost:555", Protocol: HTTP, TLS: cTLS, Token: "tkn1"}},
		{name: "https_ip_token", url: "https://127.0.0.1?token=tkn1", cfg: &config.Driver{URL: "127.0.0.1", Protocol: HTTP, TLS: cTLS, Token: "tkn1"}},
		{name: "https_ip_with_port_token", url: "https://127.0.0.1:777?token=tkn1", cfg: &config.Driver{URL: "127.0.0.1:777", Protocol: HTTP, TLS: cTLS, Token: "tkn1"}},
		{name: "unix_socket", url: "/var/lib/tigris/unix.sock", cfg: &config.Driver{URL: "/var/lib/tigris/unix.sock", Protocol: DefaultProtocol}},
		{name: "unix_socket", url: "/var/lib/tigris/unix.sock", cfg: &config.Driver{URL: "/var/lib/tigris/unix.sock", Protocol: DefaultProtocol}},
		{name: "unix_socket_scheme", url: "unix://localhost:/var/lib/tigris/unix.sock", cfg: &config.Driver{URL: "/var/lib/tigris/unix.sock", Protocol: DefaultProtocol}},
		{name: "unix_socket_relative", url: "./tigris/unix.sock", cfg: &config.Driver{URL: "./tigris/unix.sock", Protocol: DefaultProtocol}},
	}

	for _, v := range cases {
		t.Run(v.name, func(t *testing.T) {
			cfg := config.Driver{URL: v.url}
			rcfg, err := initConfig(&cfg)
			assert.Equal(t, v.err, err)
			assert.Equal(t, v.cfg, rcfg)
		})
	}
}

func TestDriverConfigPrecedence(t *testing.T) {
	cTLS := &tls.Config{MinVersion: tls.VersionTLS12}

	cfg := config.Driver{}

	t.Setenv(EnvToken, "")
	t.Setenv(EnvClientID, "")
	t.Setenv(EnvClientSecret, "")
	t.Setenv(EnvURL, "")
	t.Setenv(EnvProtocol, "")

	res, err := initConfig(&cfg)
	assert.NoError(t, err)

	// default
	assert.Equal(t, config.Driver{URL: "api.preview.tigrisdata.cloud", Protocol: DefaultProtocol}, *res)

	t.Setenv(EnvURI, "host2:234")

	res, err = initConfig(&cfg)
	assert.NoError(t, err)
	assert.Equal(t, config.Driver{
		URL:      "host2:234",
		Protocol: DefaultProtocol,
	}, *res)

	// env have precedence over default
	t.Setenv(EnvToken, "token1")
	t.Setenv(EnvClientID, "client1")
	t.Setenv(EnvClientSecret, "client_secret1")
	t.Setenv(EnvURL, "host1:234")
	t.Setenv(EnvProtocol, "HTTP")

	res, err = initConfig(&cfg)
	assert.NoError(t, err)
	assert.Equal(t, config.Driver{
		URL:          "host1:234",
		ClientID:     "client1",
		ClientSecret: "client_secret1",
		Token:        "token1",
		Protocol:     HTTP,
		TLS:          cTLS,
	}, *res)

	// config have precedence over env
	cfg.URL = "url.config"
	cfg.ClientID = "client_id2"
	cfg.ClientSecret = "client_secret2"
	cfg.Protocol = "GRPC"
	cfg.Token = "token2"
	res, err = initConfig(&cfg)
	assert.NoError(t, err)

	assert.Equal(t, config.Driver{
		URL:          "url.config",
		ClientID:     "client_id2",
		ClientSecret: "client_secret2",
		Token:        "token2",
		Protocol:     GRPC,
		TLS:          cTLS,
	}, *res)

	// URL params have precedence over config
	cfg.URL = "https://ii.ii?client_id=id3&client_secret=secret3&token=token3"
	res, err = initConfig(&cfg)
	assert.NoError(t, err)

	assert.Equal(t, config.Driver{
		URL:          "ii.ii",
		ClientID:     "id3",
		ClientSecret: "secret3",
		Token:        "token3",
		Protocol:     HTTP,
		TLS:          cTLS,
	}, *res)
}

func TestDriverConfigNegative(t *testing.T) {
	cfg := config.Driver{}
	cfg.URL = "::+invlaid//"
	_, err := initConfig(&cfg)
	assert.Error(t, err)

	cfg.URL = ""
	t.Setenv(EnvProtocol, "INVALID")
	_, err = initConfig(&cfg)
	assert.Error(t, err)
}

func TestDriverConfigProto(t *testing.T) {
	cases := []struct {
		proto string
		exp   string
		err   error
	}{
		{proto: "GRPC", exp: GRPC},
		{proto: "HTTP", exp: HTTP},
		{proto: "http", exp: HTTP},
		{proto: "grpc", exp: GRPC},
		{proto: "HtTp", exp: HTTP},
		{proto: "GrPc", exp: GRPC},
		{proto: "INVALID", exp: "INVALID", err: fmt.Errorf("unsupported protocol")},
		{proto: "HTTPS", exp: HTTP},
	}

	for _, v := range cases {
		t.Run(v.proto, func(t *testing.T) {
			cfg := config.Driver{Protocol: v.proto}
			_, err := initProto("", &cfg)
			assert.Equal(t, v.err, err)
			assert.Equal(t, v.exp, cfg.Protocol)
		})
	}
}

func TestLocalURL(t *testing.T) {
	// Test cases with local URLs that should return true
	localURLs := []string{
		"localhost:8080",
		"127.0.0.1:8000",
		"http://localhost:3000",
		"http://127.0.0.1:5000",
		"[::1]:8080",
		"http://[::1]:8000",
	}

	for _, url := range localURLs {
		require.True(t, localURL(url))
	}

	// Test cases with non-local URLs that should return false
	nonLocalURLs := []string{
		"example.com",
		"http://example.com",
		"www.google.com",
		"http://www.google.com",
		"127.0.0.1.5",
		"localhost123",
	}

	for _, url := range nonLocalURLs {
		require.False(t, localURL(url))
	}
}
