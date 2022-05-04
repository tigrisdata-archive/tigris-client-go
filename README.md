# Tigris Golang client

[![Go Report](https://goreportcard.com/badge/github.com/tigrisdata/tigris-client-go)](https://goreportcard.com/report/github.com/tigrisdata/tigris-client-go)
[![Build Status](https://github.com/tigrisdata/tigris-client-go/workflows/go-test/badge.svg)]()
[![Build Status](https://github.com/tigrisdata/tigris-client-go/workflows/go-lint/badge.svg)]()
[![codecov](https://codecov.io/gh/tigrisdata/tigris-client-go/branch/main/graph/badge.svg)](https://codecov.io/gh/tigrisdata/tigris-client-go)
[![Go Reference](https://pkg.go.dev/badge/github.com/tigrisdata/tigris-client-go.svg)](https://pkg.go.dev/github.com/tigrisdata/tigris-client-go)

# Installation

```sh
go get github.com/tigrisdata/tigris-client-go@latest
```

# Getting started

For fully functional getting-started application, please check [Tigris Starter Go application](https://github.com/tigrisdata/tigris-starter-go),
or install the [CLI](https://github.com/tigrisdata/tigris-cli) and scaffold you project by running:
```shell
tigris scaffold go Company1 Project1 DbName1 Coll1 Coll2
```

# Client API example

```golang
package main

import (
	"context"
	"fmt"

	"github.com/tigrisdata/tigris-client-go/config"
	"github.com/tigrisdata/tigris-client-go/filter"
	"github.com/tigrisdata/tigris-client-go/tigris"
	"github.com/tigrisdata/tigris-client-go/update"
)

type User struct {
	Id      int `tigris:"primary_key"`
	Name    string
	Balance int
}

func main() {
	ctx := context.TODO()

	// Config database and connection. Optional for local development.
	cfg := &config.Database{Driver: config.Driver{URL: "localhost:8081"}}

	db, err := tigris.OpenDatabase(ctx, cfg, "OrderDB", &User{})

	users := tigris.GetCollection[User](db)

	_, err = users.Insert(ctx, &User{Id: 1, Name: "Jane", Balance: 100},
		&User{Id: 2, Name: "John", Balance: 100})
	if err != nil {
		panic(err)
	}

	_, err = users.Update(ctx, filter.Eq("Id", "1"), update.Set("Balance", 200))
	if err != nil {
		panic(err)
	}

	it, err := users.Read(ctx, filter.Or(filter.Eq("Id", 1), filter.Eq("Id", 2)))
	if err != nil {
		panic(err)
	}

	var user User
	for it.Next(&user) {
		fmt.Printf("%+v\n", user)
	}

	if it.Err() != nil {
		panic(it.Err())
	}

	if _, err := users.Delete(ctx, filter.Eq("Id", "1")); err != nil {
		panic(err)
	}

	// When the closure returns no error, the changes from all operations
	// executed in it will be applied to the database.
	// Changes will be discarded when the closure returns an error.
	// In this example if Balance is lower than 100 transaction will be rolled back
	err = db.Tx(ctx, func(ctx context.Context, tx *tigris.Tx) error {
		c := tigris.GetTxCollection[User](tx)
		
		u, err := c.ReadOne(ctx, filter.Eq("Id", 1))
		if err != nil {
			return err
		}

		if u.Balance < 100 {
			return fmt.Errorf("low balance")
		}
		
		// ...
		// Same API as in non-transactional case can be used here
		return nil
	})
	if err != nil {
		panic(err)
	}
}
```

# Integration API

[Low level integration API](docs/driver.md)

# Development

```sh
sh scripts/install_build_deps.sh
sh scripts/install_test_deps.sh
make test
```

# License

This software is licensed under the [Apache 2.0](LICENSE).

