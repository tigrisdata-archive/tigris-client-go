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
	"testing"

	"github.com/tigrisdata/tigris-client-go/config"
)

func TestGRPCObservabilityDriver(t *testing.T) {
	_, client, mockServers, cancel := SetupO11yGRPCTests(t, &config.Driver{})
	defer cancel()
	testDriverObservability(t, client, mockServers.O11y)
	testDriverObservabilityNegative(t, client, mockServers.O11y)
}