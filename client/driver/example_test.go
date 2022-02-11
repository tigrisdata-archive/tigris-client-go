package driver

import (
	"context"
	"fmt"
)

func ExampleDriver() {
	ctx := context.TODO()

	c, _ := NewDriver(ctx, "localhost", Config{})

	_, _ = c.Insert(ctx, "db1", "c1", []Document{Document(`{"F1":"V1"}`)}, nil)

	it, _ := c.Read(ctx, "db1", "c1", Filter(`{"F1":"V1"}`), nil)

	for it.More() {
		doc, _ := it.Next()
		fmt.Printf("doc: %v\n", doc)
	}

	_, _ = c.Delete(ctx, "db1", "c1", Filter(`{"F1":"V1"}`), nil)

	tx, _ := c.BeginTx(ctx, "db1", nil)
	defer func() { _ = tx.Rollback(ctx) }()

	_, _ = tx.Insert(ctx, "c1", []Document{Document(`{"F1":"V1"}`)}, nil)

	it, _ = tx.Read(ctx, "c1", Filter(`{"F1":"V1"}`), nil)

	for it.More() {
		doc, _ := it.Next()
		fmt.Printf("doc: %v\n", doc)
	}

	_, _ = tx.Delete(ctx, "c1", Filter(`{"F1":"V1"}`), nil)

	_ = tx.Commit(ctx)
}
