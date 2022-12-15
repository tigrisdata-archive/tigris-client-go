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

package timeseries_test

import (
	"context"
	"fmt"
	"time"

	"github.com/tigrisdata/tigris-client-go/filter"
	"github.com/tigrisdata/tigris-client-go/tigris"
	"github.com/tigrisdata/tigris-client-go/timeseries"
)

func Example() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	type LogEvent struct {
		timeseries.Model

		Level   string `json:"level"`
		Message string `json:"message"`
	}

	cfg := &tigris.Config{Project: "db1", URL: "localhost:8081"}

	// Open database and evolve schemas
	db, err := timeseries.OpenDatabase(ctx, cfg, &LogEvent{})
	if err != nil {
		panic(err)
	}

	// Get collection object
	coll := timeseries.GetCollection[LogEvent](db)
	defer coll.Flush(ctx)

	// Append events to the collection
	err = coll.Append(ctx,
		&LogEvent{Message: "one", Level: "info"},
		&LogEvent{Message: "two", Level: "error"},
		&LogEvent{Message: "three", Level: "debug"},
	)
	if err != nil {
		panic(err)
	}

	// Force flushing (for the sake of the example, otherwise flushing happens every timeseries.BufSize events)
	err = coll.Flush(ctx)
	if err != nil {
		panic(err)
	}

	// Lookup events in the time range
	it, err := coll.FindInRange(ctx,
		time.Now().Add(-10*time.Second), // from ten seconds ago
		time.Now(),                      // till now
		filter.Eq("level", "error"),     // return only error level events
	)
	if err != nil {
		panic(err)
	}
	defer it.Close()

	var ev LogEvent

	for it.Next(&ev) {
		fmt.Printf("event: %+v\n", ev)
	}
}
