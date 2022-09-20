# Tigris Go Client Library

[![Go Report](https://goreportcard.com/badge/github.com/tigrisdata/tigris-client-go)](https://goreportcard.com/report/github.com/tigrisdata/tigris-client-go)
[![Build Status](https://github.com/tigrisdata/tigris-client-go/workflows/go-test/badge.svg)]()
[![Build Status](https://github.com/tigrisdata/tigris-client-go/workflows/go-lint/badge.svg)]()
[![codecov](https://codecov.io/gh/tigrisdata/tigris-client-go/branch/main/graph/badge.svg)](https://codecov.io/gh/tigrisdata/tigris-client-go)
[![Go Reference](https://pkg.go.dev/badge/github.com/tigrisdata/tigris-client-go.svg)](https://pkg.go.dev/github.com/tigrisdata/tigris-client-go)

Tigris provides an easy-to-use and intuitive interface for Go. Setting up 
the database is instantaneous, as well - no need for tedious configuration. 
You define the data model as part of the application code, which then drives 
the database infrastructure without you having to configure and provision 
database resources.

# Documentation
- [Quickstart](https://docs.tigrisdata.com/quickstart/with-go)
- [Client Library](https://docs.tigrisdata.com/client-libraries/go)
- [Data Modeling Using Go](https://docs.tigrisdata.com/datamodels/using-go)

# Installation

```sh
go get -u github.com/tigrisdata/tigris-client-go@latest
```

# Usage

```go
package main

import (
	"context"
	"time"

	"github.com/tigrisdata/tigris-client-go/config"
	"github.com/tigrisdata/tigris-client-go/fields"
	"github.com/tigrisdata/tigris-client-go/filter"
	"github.com/tigrisdata/tigris-client-go/tigris"
)

type User struct {
	Id      int `tigris:"primary_key"`
	Name    string
	Balance float64
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	
	// Connect to the Tigris server
	client, err := tigris.NewClient(ctx, &config.Database{Driver: config.Driver{URL: "localhost:8081"}})
	if err != nil {
		panic(err)
	}
	defer client.Close()

	// Create the database and collection if they don't exist,
	// otherwise update the schema of the collection if it already exists
	db, err := client.OpenDatabase(ctx, "hello_db", &User{})
	if err != nil {
		panic(err)
	}

	// Get the collection object, all the CRUD operations on the collection will be performed
	// through this collection object
	users := tigris.GetCollection[User](db)

	// Insert or replace user
	users.InsertOrReplace(ctx,
		&User{Id: 1, Name: "Jania McGrory", Balance: 6045.7},
		&User{Id: 2, Name: "Bunny Instone", Balance: 2948.87})

	// Read
	var user *User
	user, err = users.ReadOne(ctx, filter.Eq("Id", 1)) // find user with Id 1
	if err != nil {
		panic(err)
	}

	// Update - update user's name
	users.Update(ctx, filter.Eq("Id", 1), fields.Set("Name", "Jania McGrover"))

	// Delete - delete users with Id 1 or 2
	users.Delete(ctx, filter.Or(filter.Eq("Id", 1), filter.Eq("Id", 2)))
}
```

# Developing

The following scripts will set up and install the dependencies needed for 
building and testing the client.

```shell
sh scripts/install_build_deps.sh
sh scripts/install_test_deps.sh
```

Once the dependencies have been installed, the tests can be run simply by

```shell
make test
```

# License

This software is licensed under the [Apache 2.0](LICENSE).
