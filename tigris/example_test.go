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

package tigris

import (
	"context"
	"errors"
	"fmt"

	"github.com/tigrisdata/tigris-client-go/code"
	"github.com/tigrisdata/tigris-client-go/filter"
)

func ExampleDatabase_Tx() {
	ctx := context.TODO()

	type Coll1 struct {
		Key1 string `tigris:"primary_key"`
	}

	db, err := OpenDatabase(ctx, &Config{}, "db1", &Coll1{})
	if err != nil {
		panic(err)
	}

	err = db.Tx(ctx, func(ctx context.Context) error {
		c := GetCollection[Coll1](db)

		if _, err := c.Insert(ctx, &Coll1{"aaa"}); err != nil {
			panic(err)
		}

		if _, err := c.Delete(ctx, filter.Eq("Key1", "bbb")); err != nil {
			panic(err)
		}

		return nil
	})
	if err != nil {
		panic(err)
	}
}

func ExampleOpenDatabase() {
	ctx := context.TODO()

	type Coll1 struct {
		Key1 string `tigris:"primary_key"`
	}

	// Connects to the Tigris server. Creates or opens database "db1".
	// Creates or migrate &Coll1{}. Returns a "db" object, which provides
	// access to the collections of the database, Coll1 in this example.
	db, err := OpenDatabase(ctx, &Config{}, "db1", &Coll1{})
	if err != nil {
		panic(err)
	}

	c := GetCollection[Coll1](db)

	if _, err := c.Insert(ctx, &Coll1{"aaa"}); err != nil {
		panic(err)
	}
}

func ExampleIterator() {
	ctx := context.TODO()

	type Coll1 struct {
		Key1 string `tigris:"primary_key"`
	}

	db, err := OpenDatabase(ctx, &Config{}, "db1", &Coll1{})
	if err != nil {
		panic(err)
	}

	c := GetCollection[Coll1](db)

	it, err := c.ReadAll(ctx)
	if err != nil {
		panic(err)
	}

	defer it.Close()

	var doc Coll1
	for it.Next(&doc) {
		fmt.Printf("%+v\n", doc)
	}

	if err := it.Err(); err != nil {
		panic(err)
	}
}

func ExampleError() {
	ctx := context.TODO()

	type Coll1 struct {
		Key1 string `tigris:"primary_key"`
	}

	db, err := OpenDatabase(ctx, &Config{}, "db1", &Coll1{})
	if err != nil {
		panic(err)
	}

	coll := GetCollection[Coll1](db)

	// Insert document into collection
	_, err = coll.Insert(ctx, &Coll1{"aaa"})
	if err != nil {
		panic(err)
	}

	// Insert of the same object causes duplicate key error
	_, err = coll.Insert(ctx, &Coll1{"aaa"})

	// Unwrap tigris.Error and check the code
	var ep *Error
	if errors.As(err, &ep) {
		if ep.Code == code.AlreadyExists {
			// handle duplicate key
		}
	}
}
