# TigrisDB Golang client

[![Go Report](https://goreportcard.com/badge/github.com/tigrisdata/tigrisdb-client-go)](https://goreportcard.com/report/github.com/tigrisdata/tigrisdb-client-go)
[![Build Status](https://github.com/tigrisdata/tigrisdb-client-go/workflows/go-test/badge.svg)]()
[![Build Status](https://github.com/tigrisdata/tigrisdb-client-go/workflows/go-lint/badge.svg)]()
[![codecov](https://codecov.io/gh/tigrisdata/tigrisdb-client-go/branch/main/graph/badge.svg)](https://codecov.io/gh/tigrisdata/tigrisdb-client-go)

# Install

```sh
go get github.com/tigrisdata/tigrisdb-client-go@latest
```

# Example

```golang
package example

import (
    "context"
    "fmt"

    "github.com/tigrisdata/tigrisdb-client-go/driver"
)

func example() error {
    ctx := context.TODO()

    token := "{auth token here}" // optional

    drv, err := driver.NewDriver(ctx, "localhost:8081", &driver.Config{Token: token})
    if err != nil {
        return err
    }

    err = drv.CreateOrUpdateCollection(ctx, "db1", "coll1",
        driver.Schema(`{
            "name" : "coll1",
            "properties": {
               "Key1": { "type": "string" },
               "Field1": { "type": "integer" }
            },
            "primary_key": ["Key1"] }`))
    if err != nil {
        return err
    }

    _, err = drv.Insert(ctx, "db1", "coll1", []driver.Document{
            driver.Document(`{"Key1": "vK1", "Field1": 1}`) })
    if err != nil {
        return err
    }

    // Nil Filter value {} means full table scan,
    it, err := drv.Read(ctx, "db1", "coll1", driver.Filter(`{"Key1" : "vK1"}`), nil)
    if err != nil {
        return err
    }

    var doc driver.Document
    for it.Next(&doc) {
        fmt.Printf("%s\n", string(doc))
    }

    if err := it.Err(); err != nil {
        return err
    }

    return nil
}
```

# License

This software is licensed under the [Apache 2.0](LICENSE).

## Development

1. sh scripts/install_build_deps.sh
2. sh scripts/install_test_deps.sh
3. make test
