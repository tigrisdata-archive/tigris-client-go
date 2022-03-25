package driver

import (
	"context"
	"fmt"
)

func ExampleDriver() {
	ctx := context.TODO()

	c, _ := NewDriver(ctx, "localhost", &Config{})

	_ = c.CreateDatabase(ctx, "db1", &DatabaseOptions{})

	_ = c.CreateCollection(ctx, "db1", "coll1",
		Schema(`{ "properties": { "F1": { "type": "string" }, "F2": { "type": "string" } }, "primary_key": ["F1"] }`), &CollectionOptions{})

	_, _ = c.Insert(ctx, "db1", "c1", []Document{Document(`{"F1":"V1"}`)}, &InsertOptions{})

	it, _ := c.Read(ctx, "db1", "c1", Filter(`{"F1":"V1"}`), &ReadOptions{})

	var doc Document
	for it.Next(&doc) {
		fmt.Printf("doc: %v\n", doc)
	}

	_ = it.Err()

	_, _ = c.Delete(ctx, "db1", "c1", Filter(`{"F1":"V1"}`), &DeleteOptions{})

	tx, _ := c.BeginTx(ctx, "db1", nil)
	defer func() { _ = tx.Rollback(ctx) }()

	_, _ = tx.Insert(ctx, "c1", []Document{Document(`{"F1":"V1"}`)}, &InsertOptions{})

	it, _ = tx.Read(ctx, "c1", Filter(`{"F1":"V1"}`), &ReadOptions{})

	for it.Next(&doc) {
		fmt.Printf("doc: %v\n", doc)
	}

	_ = it.Err()

	_, _ = tx.Update(ctx, "c1", Filter(`{"F1":"V1"}`),
		Fields(`{"$set" : { "F2" : "V2"}}`), &UpdateOptions{})

	_, _ = tx.Delete(ctx, "c1", Filter(`{"F1":"V1"}`), &DeleteOptions{})

	_ = tx.Commit(ctx)

	_ = c.DropDatabase(ctx, "db1", &DatabaseOptions{})
}
