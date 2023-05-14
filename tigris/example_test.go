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

//revive:disable:unhandled-error,unexported-naming
package tigris_test

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/google/uuid"
	"github.com/tigrisdata/tigris-client-go/code"
	"github.com/tigrisdata/tigris-client-go/filter"
	"github.com/tigrisdata/tigris-client-go/search"
	"github.com/tigrisdata/tigris-client-go/tigris"
)

func ExampleDatabase_Tx() {
	ctx := context.TODO()

	type Coll1 struct {
		Key1 string `tigris:"primary_key"`
	}

	db, err := tigris.OpenDatabase(ctx, &tigris.Config{Project: "db1"}, &Coll1{})
	if err != nil {
		panic(err)
	}

	err = db.Tx(ctx, func(ctx context.Context) error {
		c := tigris.GetCollection[Coll1](db)

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
	db, err := tigris.OpenDatabase(ctx, &tigris.Config{Project: "db1"}, &Coll1{})
	if err != nil {
		panic(err)
	}

	c := tigris.GetCollection[Coll1](db)

	if _, err := c.Insert(ctx, &Coll1{"aaa"}); err != nil {
		panic(err)
	}
}

func ExampleIterator() {
	ctx := context.TODO()

	type Coll1 struct {
		Key1 string `tigris:"primary_key"`
	}

	db, err := tigris.OpenDatabase(ctx, &tigris.Config{Project: "db1"}, &Coll1{})
	if err != nil {
		panic(err)
	}

	c := tigris.GetCollection[Coll1](db)

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

	db, err := tigris.OpenDatabase(ctx, &tigris.Config{Project: "db1"}, &Coll1{})
	if err != nil {
		panic(err)
	}

	coll := tigris.GetCollection[Coll1](db)

	// Insert document into collection
	_, err = coll.Insert(ctx, &Coll1{"aaa"})
	if err != nil {
		panic(err)
	}

	// Insert of the same object causes duplicate key error
	_, err = coll.Insert(ctx, &Coll1{"aaa"})

	// Unwrap tigris.Error and check the code
	var ep *tigris.Error
	if errors.As(err, &ep) {
		if ep.Code == code.AlreadyExists {
			// handle duplicate key
			return
		}
	}
}

func ExampleClient_GetSearch() {
	ctx := context.TODO()

	type Coll1 struct {
		Key1 string
	}

	c, err := tigris.NewClient(ctx, &tigris.Config{Project: "db1"})
	if err != nil {
		panic(err)
	}

	s := c.GetSearch()

	if err := s.CreateIndexes(ctx, &Coll1{}); err != nil {
		panic(err)
	}

	idx := search.GetIndex[Coll1](s)

	if _, err = idx.Create(ctx, &Coll1{"aaa"}); err != nil {
		panic(err)
	}
}

func ExampleProjection_Read() {
	ctx := context.TODO()

	type NestedColl1 struct {
		Key2 string
	}

	type Coll1 struct {
		Key1   string
		Nested NestedColl1
	}

	db := tigris.MustOpenDatabase(ctx, &tigris.Config{Project: "db1"}, &Coll1{})

	coll := tigris.GetCollection[Coll1](db)

	_, err := coll.Insert(ctx, &Coll1{Key1: "k1", Nested: NestedColl1{Key2: "k2"}})
	if err != nil {
		panic(err)
	}

	proj := tigris.GetProjection[Coll1, NestedColl1](db, "Nested")

	it, err := proj.Read(ctx, filter.All)
	if err != nil {
		panic(err)
	}

	var Nested NestedColl1
	for it.Next(&Nested) {
		fmt.Printf("%+v\n", Nested)
	}

	// Projection with only top level key included
	type Proj2 struct {
		Key1 string
	}

	proj2 := tigris.GetProjection[Coll1, Proj2](db)

	it2, err := proj2.Read(ctx, filter.All)
	if err != nil {
		panic(err)
	}

	var p2 Proj2
	for it2.Next(&p2) {
		fmt.Printf("%+v\n", p2)
	}
}

func ExampleIterator_Iterate() {
	ctx := context.TODO()

	type Coll1 struct {
		Key1 string
	}

	db := tigris.MustOpenDatabase(ctx, &tigris.Config{Project: "db1"}, &Coll1{})

	coll := tigris.GetCollection[Coll1](db)

	it, err := coll.Read(ctx, filter.All)
	if err != nil {
		panic(err)
	}

	err = it.Iterate(func(doc *Coll1) error {
		fmt.Fprintf(os.Stderr, "%+v\n", doc)
		return nil
	})
	if err != nil {
		panic(err)
	}
}

func ExampleIterator_Array() {
	ctx := context.TODO()

	type Coll1 struct {
		Key1 string
	}

	db := tigris.MustOpenDatabase(ctx, &tigris.Config{Project: "db1"}, &Coll1{})

	coll := tigris.GetCollection[Coll1](db)

	it, err := coll.Read(ctx, filter.All)
	if err != nil {
		panic(err)
	}

	arr, err := it.Array()
	if err != nil {
		panic(err)
	}

	for k, v := range arr {
		fmt.Fprintf(os.Stderr, "doc %v = %+v\n", k, v)
	}
}

func ExampleGetJoin() {
	ctx := context.TODO()

	type User struct {
		ID   uuid.UUID
		Name string
	}

	type Order struct {
		ID     uuid.UUID
		UserID uuid.UUID
		Price  float64
	}

	db := tigris.MustOpenDatabase(ctx, &tigris.Config{Project: "db1"}, &User{}, &Order{})

	join := tigris.GetJoin[User, Order](db, "ID", "UserID")

	it, err := join.Read(ctx, filter.All) // return all users
	if err != nil {
		panic(err)
	}

	var (
		user   User
		orders []*Order
	)

	// iterate the users with corresponding orders
	for it.Next(&user, &orders) {
		fmt.Printf("User: %s", user.Name)

		for _, o := range orders {
			fmt.Printf("Order: %f", o.Price)
		}
	}
}
