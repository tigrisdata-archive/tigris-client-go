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

package search_test

import (
	"context"
	"fmt"

	"github.com/tigrisdata/tigris-client-go/search"
)

func ExampleMustOpen() {
	ctx := context.TODO()

	type Coll1 struct {
		Field1 string
	}

	s := search.MustOpen(ctx, &search.Config{Project: "db1"}, &Coll1{})

	idx := search.GetIndex[Coll1](s)

	if _, err := idx.Create(ctx, &Coll1{"aaa"}); err != nil {
		panic(err)
	}
}

func ExampleSearch() {
	ctx := context.TODO()

	type Review struct {
		ID       string     `json:"id"`
		Text     string     `json:"text"`
		VecField [3]float64 `json:"vec_field" tigris:"vector"`
	}

	s := search.MustOpen(ctx, &search.Config{URL: "localhost:8081", Project: "db1"}, &Review{})

	idx := search.GetIndex[Review](s)

	_, err := idx.Create(ctx,
		&Review{
			ID:       "1",
			Text:     "This is a great product. I would recommend it to anyone.",
			VecField: [3]float64{1.2, 2.3, 4.5},
		},
		&Review{
			ID:       "2",
			Text:     "This is a bad product. I would not recommend it to anyone.",
			VecField: [3]float64{6.7, 8.2, 9.2},
		},
	)
	if err != nil {
		panic(err)
	}

	it, err := idx.Search(ctx, &search.Request{
		Vector: search.VectorType{
			"vec_field": {6.1, 7.2, 6.3},
		},
	})
	if err != nil {
		panic(err)
	}
	defer it.Close()

	var res search.Result[Review]
	for it.Next(&res) {
		fmt.Printf("%+v\n", res.Hits[0].Document)
		fmt.Printf("%+v\n", res.Hits[1].Document)
	}

	if it.Err() != nil {
		panic(it.Err())
	}

	// &{ID:2 Text:This is a bad product. I would not recommend it to anyone. VecField:[6.7 8.2 9.2]}
	// &{ID:1 Text:This is a great product. I would recommend it to anyone. VecField:[1.2 2.3 4.5]}
}
