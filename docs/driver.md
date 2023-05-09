# Driver API example

```golang
package main

import (
    "context"
    "fmt"

    "github.com/tigrisdata/tigris-client-go/driver"
	"github.com/tigrisdata/tigris-client-go/config"
)

func main() {
    ctx := context.TODO()

	// Config is optional for local development 
	cfg := &config.Driver{
		URL: "localhost:8081",
	}

    drv, err := driver.NewDriver(ctx, cfg)
    if err != nil {
		panic(err)
    }

    err = drv.CreateOrUpdateCollection(ctx, "db1", "coll1",
        driver.Schema(`{
            "title" : "coll1",
            "properties": {
               "Key1": { "type": "string" },
               "Field1": { "type": "integer" }
            },
            "primary_key": ["Key1"] }`))
    if err != nil {
		panic(err)
    }

    _, err = drv.Insert(ctx, "db1", "coll1", []driver.Document{
            driver.Document(`{"Key1": "vK1", "Field1": 1}`) })
    if err != nil {
		panic(err)
    }

    // Nil Filter value {} means full table scan.
    it, err := drv.Read(ctx, "db1", "coll1", driver.Filter(`{"Key1" : "vK1"}`), nil)
    if err != nil {
		panic(err)
    }

    var doc driver.Document
    for it.Next(&doc) {
        fmt.Printf("%s\n", string(doc))
    }

    if err := it.Err(); err != nil {
		panic(err)
    }

    return
}
```
