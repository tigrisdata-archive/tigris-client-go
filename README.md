# TigrisDB Golang client

[![Build Status](https://github.com/tigrisdata/tigrisdb-client-go/workflows/go-test/badge.svg)]()
[![Build Status](https://github.com/tigrisdata/tigrisdb-client-go/workflows/go-lint/badge.svg)]()

# Install

```sh
go get github.com/tigrisdata/tigrisdb-client-go@latest
```

# Example

```golang
    package example

    import "github.com/tigrisdata/tigrisdb-client-go/driver"

    token := "{auth token here}"
	
    drv, err := driver.NewDriver(context.Background(),
		"{namespace}.{env}.tigrisdata.cloud", &driver.Config{Token: token})
    if err != nil {
        return err
    }

    err := c.CreateCollection(ctx, "db1", "coll1",
        Schema(`{ "properties": {
                    "Key1": { "type": "string" },
                    "Field1": { "type": "int" }
                    },
                    "primary_key": ["Key1"] }`
		        ), &CollectionOptions{})
    if err != nil {
		return err
    }

    _, err := drv.Insert(ctx, "db1", "coll1",
		[]driver.Document(driver.Document(`{"Key1": "vK1", "Field1": 1}`)))
    if err != nil {
		return err
    }

	// Filter value {} means full table scan
    it, err := drv.Read(ctx, "db1", "coll1", driver.Filter("{}"))
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
```

# License
This software is licensed under the [Apache 2.0](LICENSE).
