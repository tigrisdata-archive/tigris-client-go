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

package filter

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPrefixFilterBuilderBasic(t *testing.T) {
	cases := []struct {
		name string
		fb   Expr
		exp  string
	}{
		{"eq", Eq("a", 1), `{"a":{"$eq":1}}`},
		/*
			{"ne", Ne("a", 1), `{"a":{"$ne":1}}`},
			{"gt", Gt("a", 1), `{"a":{"$gt":1}}`},
			{"gte", Gte("a", 1), `{"a":{"$gte":1}}`},
			{"lt", Lt("a", 1), `{"a":{"$lt":1}}`},
			{"lte", Lte("a", 1), `{"a":{"$lte":1}}`},
			{"not(eq)", Not(Eq("a", 1)), `{"$not":{"a":{"$eq":1}}}`},
			{"not(not(eq))", Not(Not(Eq("a", 1))), `{"$not":{"$not":{"a":{"$eq":1}}}}`},
		*/
		{"and(eq)", And(Eq("a", 1)), `{"$and":[{"a":{"$eq":1}}]}`},
		{"and(eq, eq)", And(Eq("a", 1), Eq("b", 2)), `{"$and":[{"a":{"$eq":1}},{"b":{"$eq":2}}]}`},
		{"or(eq)", Or(Eq("a", 1)), `{"$or":[{"a":{"$eq":1}}]}`},
		{"or(eq, eq)", Or(Eq("a", 1), Eq("b", 2)), `{"$or":[{"a":{"$eq":1}},{"b":{"$eq":2}}]}`},
		{"or(and(eq), eq)", Or(And(Eq("a", 1)), Eq("b", 2)), `{"$or":[{"$and":[{"a":{"$eq":1}}]},{"b":{"$eq":2}}]}`},
		{"and(or(eq), eq)", And(Or(Eq("a", 1)), Eq("b", 2)), `{"$and":[{"$or":[{"a":{"$eq":1}}]},{"b":{"$eq":2}}]}`},
		{"and(or(eq, and(eq, eq)), eq)", And(Or(Eq("a", 1), And(Eq("c", 3), Eq("d", 4))), Eq("b", 2)), `{"$and":[{"$or":[{"a":{"$eq":1}},{"$and":[{"c":{"$eq":3}},{"d":{"$eq":4}}]}]},{"b":{"$eq":2}}]}`},
	}

	for _, v := range cases {
		t.Run(v.name, func(t *testing.T) {
			b, err := v.fb.Build()
			assert.NoError(t, err)
			assert.Equal(t, v.exp, string(b))
		})
	}
}
