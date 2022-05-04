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

package config

import "crypto/tls"

type Driver struct {
	TLS   *tls.Config `json:"tls,omitempty"`
	Token string      `json:",omitempty"`
	URL   string      `json:"url,omitempty"`
}

// Database contains database and connection config
type Database struct {
	Driver
	// MustExist if set skips implicit database creation
	MustExist bool
}
