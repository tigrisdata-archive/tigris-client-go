# TigrisDB Golang client

[![Go Report](https://goreportcard.com/badge/github.com/tigrisdata/tigrisdb-client-go)](https://goreportcard.com/report/github.com/tigrisdata/tigrisdb-client-go)
[![Build Status](https://github.com/tigrisdata/tigrisdb-client-go/workflows/go-test/badge.svg)]()
[![Build Status](https://github.com/tigrisdata/tigrisdb-client-go/workflows/go-lint/badge.svg)]()

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

    token := "{auth token here}"

    drv, err := driver.NewDriver(context.Background(),
        "{namespace}.{env}.tigrisdata.cloud",
        &driver.Config{Token: token})
    if err != nil {
        return err
    }

    err = drv.CreateOrUpdateCollection(ctx, "db1", "coll1",
        driver.Schema(`{ "properties": {
                    "Key1": { "type": "string" },
                    "Field1": { "type": "int" }
                    },
                    "primary_key": ["Key1"] }`),
        &driver.CollectionOptions{})
    if err != nil {
        return err
    }

    _, err = drv.Insert(ctx, "db1", "coll1",
        []driver.Document{
            driver.Document(`{"Key1": "vK1", "Field1": 1}`)
        },
        &driver.InsertOptions{})
    if err != nil {
        return err
    }

    // Filter value {} means full table scan
    it, err := drv.Read(ctx, "db1", "coll1",
            driver.Filter(`{"Key1" : "vK1"}`))
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
