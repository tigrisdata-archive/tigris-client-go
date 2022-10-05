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

package driver

import (
	"context"
	"errors"
	"fmt"

	api "github.com/tigrisdata/tigris-client-go/api/server/v1"
	"github.com/tigrisdata/tigris-client-go/config"
)

func ExampleDriver() {
	ctx := context.TODO()

	c, _ := NewDriver(ctx, &config.Driver{URL: "localhost"})

	_ = c.CreateDatabase(ctx, "db1", &DatabaseOptions{})

	db := c.UseDatabase("db1")

	_ = db.CreateOrUpdateCollection(ctx, "coll1",
		Schema(`{ "properties": { "F1": { "type": "string" }, "F2": { "type": "string" } }, "primary_key": ["F1"] }`))

	_, _ = db.Insert(ctx, "c1", []Document{Document(`{"F1":"V1"}`)})

	it, _ := db.Read(ctx, "c1", Filter(`{"F1":"V1"}`), Projection(`{}`))

	var doc Document
	for it.Next(&doc) {
		fmt.Printf("doc: %v\n", doc)
	}

	_ = it.Err()

	_, _ = db.Delete(ctx, "c1", Filter(`{"F1":"V1"}`))

	tx, _ := c.BeginTx(ctx, "db1", nil)
	defer func() { _ = tx.Rollback(ctx) }()

	_, _ = tx.Insert(ctx, "c1", []Document{Document(`{"F1":"V1"}`)})

	it, _ = tx.Read(ctx, "c1", Filter(`{"F1":"V1"}`), Projection("{}"))

	for it.Next(&doc) {
		fmt.Printf("doc: %v\n", doc)
	}

	var e Error

	err := it.Err()
	if errors.As(err, &e) && e.Code == api.Code_ALREADY_EXISTS {
		// handle already exists error
	}

	_, _ = tx.Update(ctx, "c1", Filter(`{"F1":"V1"}`),
		Update(`{"$set" : { "F2" : "V2"}}`), &UpdateOptions{})

	_, _ = tx.Delete(ctx, "c1", Filter(`{"F1":"V1"}`), &DeleteOptions{})

	_ = tx.Commit(ctx)

	_ = c.DropDatabase(ctx, "db1", &DatabaseOptions{})
}
