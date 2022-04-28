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

package projection

import (
	"encoding/json"

	"github.com/tigrisdata/tigris-client-go/driver"
)

type Projection map[string]bool

func Builder() Projection {
	return Projection{}
}

func (pr Projection) Include(field string) Projection {
	pr[field] = true
	return pr
}

func (pr Projection) Exclude(field string) Projection {
	pr[field] = false
	return pr
}

func (pr Projection) Build() (driver.Projection, error) {
	if pr == nil || len(pr) == 0 {
		return nil, nil
	}
	b, err := json.Marshal(pr)
	return b, err
}

func Include(field string) Projection {
	pr := map[string]bool{}
	pr[field] = true
	return pr
}

func Exclude(field string) Projection {
	pr := map[string]bool{}
	pr[field] = false
	return pr
}
