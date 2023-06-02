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

package fields

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFieldsBasic(t *testing.T) {
	cases := []struct {
		name   string
		fields *Read
		exp    string
		err    error
	}{
		{"nil", nil, `{}`, nil},
		{"include_1", Include("a"), `{"a":true}`, nil},
		{"exclude_1", Exclude("a"), `{"a":false}`, nil},
		{"include_exclude", Include("a").Exclude("b"), `{"a":true,"b":false}`, nil},
		{"exclude_include", Exclude("a").Include("b"), `{"a":false,"b":true}`, nil},
		{"exclude_include_nested", Exclude("a.b.c").Include("b.c.d"), `{"a.b.c":false,"b.c.d":true}`, nil},
	}

	for _, v := range cases {
		t.Run(v.name, func(t *testing.T) {
			b, err := v.fields.Build()
			assert.Equal(t, v.err, err)
			assert.JSONEq(t, v.exp, string(b.Built()))
		})
	}

	b, err := ReadBuilder().Build()
	assert.NoError(t, err)
	assert.Equal(t, All, b)
}
