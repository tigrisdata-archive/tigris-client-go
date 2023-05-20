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

// Package test contains testing helpers
package test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/fullstorydev/grpchan/inprocgrpc"
	"github.com/go-chi/chi/v5"
	"github.com/golang/mock/gomock"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/stretchr/testify/require"
	api "github.com/tigrisdata/tigris-client-go/api/server/v1"
	mock "github.com/tigrisdata/tigris-client-go/mock/api"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

var (
	apiPathPrefix = "/v1"

	HTTPRoutes = []string{
		apiPathPrefix + ("/projects"),
		apiPathPrefix + ("/projects/*"),
		apiPathPrefix + ("/collections/*"),
		apiPathPrefix + ("/documents/*"),
		apiPathPrefix + ("/management/*"),
		apiPathPrefix + ("/observability/*"),
		apiPathPrefix + ("/auth/*"),
		apiPathPrefix + ("/apps/*"),
	}
)

func GRPCURL(shift int) string {
	return fmt.Sprintf("localhost:%d", 33334+shift)
}

func HTTPURL(shift int) string {
	return fmt.Sprintf("localhost:%d", 33333+shift)
}

// FIXME: the tests which are calling URL function in "tigris" package
// FIXME: cannot be run from IDE.

var URL func(shift int) string

func SetupTLS(t *testing.T) *tls.Config {
	t.Helper()

	certPool := x509.NewCertPool()
	require.True(t, certPool.AppendCertsFromPEM([]byte(CaCert)))

	return &tls.Config{RootCAs: certPool, ServerName: "localhost", MinVersion: tls.VersionTLS12}
}

// copy from the Tigris' server/middleware.CustomMatcher.
func customMatcher(key string) (string, bool) {
	if strings.HasPrefix(key, api.HeaderPrefix) {
		return key, true
	}
	return runtime.DefaultHeaderMatcher(key)
}

type MockServers struct {
	API    *mock.MockTigrisServer
	Mgmt   *mock.MockManagementServer
	Auth   *mock.MockAuthServer
	O11y   *mock.MockObservabilityServer
	Search *mock.MockSearchServer
}

func SetupTests(t *testing.T, portShift int) (*MockServers, func()) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	c := gomock.NewController(t)

	ms := &MockServers{
		API:    mock.NewMockTigrisServer(c),
		Auth:   mock.NewMockAuthServer(c),
		Mgmt:   mock.NewMockManagementServer(c),
		O11y:   mock.NewMockObservabilityServer(c),
		Search: mock.NewMockSearchServer(c),
	}

	inproc := &inprocgrpc.Channel{}
	client := api.NewTigrisClient(inproc)
	authClient := api.NewAuthClient(inproc)
	userClient := api.NewManagementClient(inproc)
	o11yClient := api.NewObservabilityClient(inproc)
	searchClient := api.NewSearchClient(inproc)

	mux := runtime.NewServeMux(
		runtime.WithMarshalerOption(runtime.MIMEWildcard, &api.CustomMarshaler{}),
		runtime.WithIncomingHeaderMatcher(customMatcher))

	err := api.RegisterTigrisHandlerClient(ctx, mux, client)
	require.NoError(t, err)
	err = api.RegisterAuthHandlerClient(ctx, mux, authClient)
	require.NoError(t, err)
	err = api.RegisterManagementHandlerClient(ctx, mux, userClient)
	require.NoError(t, err)
	err = api.RegisterObservabilityHandlerClient(ctx, mux, o11yClient)
	require.NoError(t, err)
	err = api.RegisterSearchHandlerClient(ctx, mux, searchClient)
	require.NoError(t, err)

	api.RegisterTigrisServer(inproc, ms.API)
	api.RegisterAuthServer(inproc, ms.Auth)
	api.RegisterManagementServer(inproc, ms.Mgmt)
	api.RegisterObservabilityServer(inproc, ms.O11y)
	api.RegisterSearchServer(inproc, ms.Search)

	cert, err := tls.X509KeyPair([]byte(ServerCert), []byte(ServerKey))
	require.NoError(t, err)

	tlsConfig := &tls.Config{
		ServerName:   "localhost",
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}

	s := grpc.NewServer(grpc.Creds(credentials.NewTLS(tlsConfig)),
		grpc.MaxRecvMsgSize(16777216),
		grpc.MaxSendMsgSize(16777216),
	)

	api.RegisterTigrisServer(s, ms.API)
	api.RegisterAuthServer(s, ms.Auth)
	api.RegisterManagementServer(s, ms.Mgmt)
	api.RegisterObservabilityServer(s, ms.O11y)
	api.RegisterSearchServer(s, ms.Search)

	r := chi.NewRouter()

	for _, v := range HTTPRoutes {
		r.HandleFunc(v, func(w http.ResponseWriter, r *http.Request) {
			mux.ServeHTTP(w, r)
		})
	}

	var wg sync.WaitGroup

	l, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", 33334+portShift))
	require.NoError(t, err)

	wg.Add(1)

	go func() {
		defer wg.Done()

		_ = s.Serve(l)
	}()

	server := &http.Server{Addr: fmt.Sprintf("localhost:%d", 33333+portShift), TLSConfig: tlsConfig, Handler: r, ReadHeaderTimeout: 5 * time.Second}

	wg.Add(1)

	go func() {
		defer wg.Done()

		_ = server.ListenAndServeTLS("", "")
	}()

	return ms, func() {
		err = server.Shutdown(context.Background())
		require.NoError(t, err)
		s.Stop()
		wg.Wait()
	}
}

var ServerKey = `
-----BEGIN PRIVATE KEY-----
MIICdgIBADANBgkqhkiG9w0BAQEFAASCAmAwggJcAgEAAoGBALmad2p/jWwu6Wbc
ehfuOggJy+Ic/QGmFMMELplDwEFzPedlwU+OZsDsyqwK9jKltRlwCIBdI5+XtyN/
pOOuShmfYzkXHCx1XdyxFX7YmlPubALvgzw/9YIt/L4uVvH3h3o0k/7El9jSc3pT
3NPO+nIhbbRFlwVfJAOf9kye5dpvAgMBAAECgYA/GQxP4F0r0ib3GS1IxWxlHy95
B3HcBaI5SkqtQCM0HQGGkUlOypKUM+wS4Qch4MPYigXZ3dAmiWVxZAuie7Yku50X
IlNYTpSh4BKyDggr0BCL92I8xfQkHbuxp8ZmZgUBVuD+6W58WvTTh8vUF8u7XVVO
2mvdFES7G5Nv4SbqwQJBANpdCptrKkELurhjwBSf3p2bC9QgWwSjQT6eKuzulgHL
YWDZpbwpj7gdT9npd9CqXFCkwvvJ+XsTLpLFMB0/850CQQDZl+/MvL6HRMyF1Rw9
p/vipRKXl5g+hy0mhjNABzg+Z7iKiC8uVQhfuS6m0MCa8r/D6IPnyC4tB709jxtZ
xKZ7AkAoZkBZIsmNgTsJdEMMTculAxN8KoRMZlvi1uaAMWAFcvhQL9RO7K2PVbT5
Tw2AyJQNw33jkambkJ/0PZE6SCOtAkApid7GZ/W7Xv/oQKGuh4YHY1nkRJVUwnt1
EkNwYrBzAVvyXkMbhjIeC/0C7XEHY3YGUTn1InrmL8cJnGstPORHAkEAz+LcAUA6
3FP5O2vJmEIh9cC08WWmfTXM6y3t0tBrRq4bp7Xl2xEuzq4NO3UlY4J+1T8AG4iR
wxs1YNDMMc+6+w==
-----END PRIVATE KEY-----
`

var ServerCert = `
-----BEGIN CERTIFICATE-----
MIICtDCCAh2gAwIBAgIUKkjf/qcFf3NjowTHfRTVtiM8t0kwDQYJKoZIhvcNAQEL
BQAwgYcxCzAJBgNVBAYTAlVTMQswCQYDVQQIDAJDQTESMBAGA1UEBwwJUGFsbyBB
bHRvMRAwDgYDVQQKDAdUZXN0T3JnMRIwEAYDVQQLDAlFZHVjYXRpb24xEjAQBgNV
BAMMCWxvY2FsaG9zdDEdMBsGCSqGSIb3DQEJARYOcm9vdEBsb2NhbGhvc3QwHhcN
MjIwMzExMjA1NTQ1WhcNMzIwMzA4MjA1NTQ1WjB4MQswCQYDVQQGEwJVUzELMAkG
A1UECAwCQ0ExCzAJBgNVBAcMAk1WMQswCQYDVQQKDAJQQzEPMA0GA1UECwwGTGFw
dG9wMRIwEAYDVQQDDAlsb2NhbGhvc3QxHTAbBgkqhkiG9w0BCQEWDnJvb3RAbG9j
YWxob3N0MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQC5mndqf41sLulm3HoX
7joICcviHP0BphTDBC6ZQ8BBcz3nZcFPjmbA7MqsCvYypbUZcAiAXSOfl7cjf6Tj
rkoZn2M5FxwsdV3csRV+2JpT7mwC74M8P/WCLfy+Llbx94d6NJP+xJfY0nN6U9zT
zvpyIW20RZcFXyQDn/ZMnuXabwIDAQABoyswKTAnBgNVHREEIDAegglsb2NhbGhv
c3SCCyoubG9jYWxob3N0hwR/AAABMA0GCSqGSIb3DQEBCwUAA4GBAK/pK2D6QU8V
mpje8CP4jhfhDk3GSATNxWJu6oPrk+fRERRXMSO5gjq4+P9ZjztbOJ8r0BvnLWUh
XCJcAUG578CGZU9iiwL8lhpfvT9HFPLR6YCFDqqExjxi3uMZ7/DT/a/LB0c6pMUk
pKxnN2NqLuAiGQB7Bekk0rVTxMxcKTJb
-----END CERTIFICATE-----
`

var CaCert = `
-----BEGIN CERTIFICATE-----
MIIDAjCCAmugAwIBAgIUDl7q+G0GxV4JYWlPZQJrruwlb4AwDQYJKoZIhvcNAQEL
BQAwgYcxCzAJBgNVBAYTAlVTMQswCQYDVQQIDAJDQTESMBAGA1UEBwwJUGFsbyBB
bHRvMRAwDgYDVQQKDAdUZXN0T3JnMRIwEAYDVQQLDAlFZHVjYXRpb24xEjAQBgNV
BAMMCWxvY2FsaG9zdDEdMBsGCSqGSIb3DQEJARYOcm9vdEBsb2NhbGhvc3QwHhcN
MjIwMzExMjA1NTQ1WhcNMzIwMzA4MjA1NTQ1WjCBhzELMAkGA1UEBhMCVVMxCzAJ
BgNVBAgMAkNBMRIwEAYDVQQHDAlQYWxvIEFsdG8xEDAOBgNVBAoMB1Rlc3RPcmcx
EjAQBgNVBAsMCUVkdWNhdGlvbjESMBAGA1UEAwwJbG9jYWxob3N0MR0wGwYJKoZI
hvcNAQkBFg5yb290QGxvY2FsaG9zdDCBnzANBgkqhkiG9w0BAQEFAAOBjQAwgYkC
gYEA4PoLX/qB+r4bxcDCuazk7eSseGy6Amxg0Wn+muUAcQCC39sv3XK3DbOn/WYO
5K7D2XccIlKP7pBBeBnmPkdczCWiXWgBXPyWJJGNUjEbAHaykoNnDnDvoL83T+Qo
rECdw70RWk7hZVtak/kx+X/iRi82iHeKfpJ4skmKrHYXAWECAwEAAaNpMGcwHQYD
VR0OBBYEFEwr6009qnQENHnTMcbfRF4r2vrcMB8GA1UdIwQYMBaAFEwr6009qnQE
NHnTMcbfRF4r2vrcMA8GA1UdEwEB/wQFMAMBAf8wFAYDVR0RBA0wC4IJbG9jYWxo
b3N0MA0GCSqGSIb3DQEBCwUAA4GBABbCdSWWKEL+LIGvav2LpHpmfgI2GDWP9spS
uFm22fXbhhxaqzVSrfL49paBWC+DKZ3BvkyynY/6a/tpYI4b5T8HP/4NgYrQ/QeT
FiQWUL55eZ7qYvwq9LRBUk5QeSjgPi1tAVJz9c7VC2e+p3hXVwEusibCNSP9y2/S
aYWrWBUT
-----END CERTIFICATE-----
`

/*
#Above certificates and keys generated using the following script:

#!/bin/bash

HOSTNAME=localhost
EMAIL=root@localhost

rm -f -- *.pem

# 1. Generate CA's private key and self-signed certificate
openssl req -x509 -newkey rsa:1024 -days 3650 -nodes -keyout ca-key.pem -out ca-cert.pem \
	-subj "/C=US/ST=CA/L=Palo Alto/O=TestOrg/OU=Dev/CN=$HOSTNAME/emailAddress=$EMAIL" \
	-addext "subjectAltName = DNS:$HOSTNAME"

echo "----------------------------"
echo "CA's self-signed certificate"
echo "----------------------------"
openssl x509 -in ca-cert.pem -noout -text
echo "----------------------------"

# 2. Generate web server's private key and certificate signing request (CSR)
openssl req -newkey rsa:1024 -nodes -keyout server-key.pem -out server-req.pem \
	-subj "/C=US/ST=CA/L=MV/O=PC/OU=Laptop/CN=$HOSTNAME/emailAddress=$EMAIL" \
	-addext "subjectAltName = DNS:$HOSTNAME"

# 3. Use CA's private key to sign web server's CSR and get back the signed certificate
echo "subjectAltName=DNS:localhost,DNS:*.localhost,IP:127.0.0.1" >/tmp/server-ext.cnf
openssl x509 -req -in server-req.pem -days 3650 -CA ca-cert.pem -CAkey ca-key.pem -CAcreateserial -out server-cert.pem -extfile /tmp/server-ext.cnf
#openssl x509 -req -in server-req.pem -days 60 -CA ca-cert.pem -CAkey ca-key.pem -CAcreateserial -out server-cert.pem -extfile server-ext.cnf

echo "----------------------------"
echo "Server's signed certificate"
echo "----------------------------"
openssl x509 -in server-cert.pem -noout -text
echo "----------------------------"
*/
