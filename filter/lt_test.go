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

package filter

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestLt(t *testing.T) {
	cases := []struct {
		name string
		expr Expr
		exp  string
	}{
		{"int0", LtInt("f", 0), `{"f":{"$lt":0}}`},
		{"intNeg", LtInt32("f", -1), `{"f":{"$lt":-1}}`},
		{"int", LtInt("f", 12345), `{"f":{"$lt":12345}}`},
		{"int32", LtInt32("f", 12345), `{"f":{"$lt":12345}}`},
		{"int64", LtInt64("f", 123456789012), `{"f":{"$lt":123456789012}}`},
		{"float32", LtFloat32("f", 12345.67), `{"f":{"$lt":12345.67}}`},
		{"float64", LtFloat64("f", 123456789012.34), `{"f":{"$lt":123456789012.34}}`},
		{"string", LtString("f", "1234"), `{"f":{"$lt":"1234"}}`},
		{"string0", LtString("f", ""), `{"f":{"$lt":""}}`},
		{"bytes", LtBytes("f", []byte("123")), `{"f":{"$lt":"MTIz"}}`},
		{"time", LtTime("f", time.Time{}), `{"f":{"$lt":"0001-01-01T00:00:00Z"}}`},
		{
			"uuid", LtUUID("f", uuid.MustParse("11111111-00b6-4eb5-a64d-351be56afe36")),
			`{"f":{"$lt":"11111111-00b6-4eb5-a64d-351be56afe36"}}`,
		},
	}

	for _, v := range cases {
		t.Run(v.name, func(t *testing.T) {
			act, err := v.expr.Build()
			require.NoError(t, err)
			require.Equal(t, v.exp, string(act))
		})
	}
}
