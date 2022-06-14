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

package fields

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestSet(t *testing.T) {
	cases := []struct {
		name string
		upd  *Update
		exp  string
	}{
		{"int", SetInt("f", 123), `{"$set":{"f":123}}`},
		{"int32", SetInt32("f", 12345), `{"$set":{"f":12345}}`},
		{"int64", SetInt64("f", 123456789012), `{"$set":{"f":123456789012}}`},
		{"float32", SetFloat32("f", 12345.67), `{"$set":{"f":12345.67}}`},
		{"float64", SetFloat64("f", 123456789012.34), `{"$set":{"f":123456789012.34}}`},
		{"string", SetString("f", "1234"), `{"$set":{"f":"1234"}}`},
		{"bytes", SetBytes("f", []byte("123")), `{"$set":{"f":"MTIz"}}`},
		{"time", SetTime("f", time.Time{}), `{"$set":{"f":"0001-01-01T00:00:00Z"}}`},
		{"uuid", SetUUID("f", uuid.MustParse("11111111-00b6-4eb5-a64d-351be56afe36")), `{"$set":{"f":"11111111-00b6-4eb5-a64d-351be56afe36"}}`},
	}

	for _, v := range cases {
		t.Run(v.name, func(t *testing.T) {
			act, err := v.upd.Build()
			require.NoError(t, err)
			require.Equal(t, v.exp, string(act.Built()))
		})
	}
}

func TestSetScoped(t *testing.T) {
	u := UpdateBuilder()
	cases := []struct {
		name string
		upd  *Update
		exp  string
	}{
		{"int", u.SetInt("f", 123), `{"$set":{"f":123}}`},
		{"int32", u.SetInt32("f", 12345), `{"$set":{"f":12345}}`},
		{"int64", u.SetInt64("f", 123456789012), `{"$set":{"f":123456789012}}`},
		{"float32", u.SetFloat32("f", 12345.67), `{"$set":{"f":12345.67}}`},
		{"float64", u.SetFloat64("f", 123456789012.34), `{"$set":{"f":123456789012.34}}`},
		{"string", u.SetString("f", "1234"), `{"$set":{"f":"1234"}}`},
		{"bytes", u.SetBytes("f", []byte("123")), `{"$set":{"f":"MTIz"}}`},
		{"time", u.SetTime("f", time.Time{}), `{"$set":{"f":"0001-01-01T00:00:00Z"}}`},
		{"uuid", u.SetUUID("f", uuid.MustParse("11111111-00b6-4eb5-a64d-351be56afe36")), `{"$set":{"f":"11111111-00b6-4eb5-a64d-351be56afe36"}}`},
	}

	for _, v := range cases {
		t.Run(v.name, func(t *testing.T) {
			act, err := v.upd.Build()
			require.NoError(t, err)
			require.Equal(t, v.exp, string(act.Built()))
		})
	}
}
