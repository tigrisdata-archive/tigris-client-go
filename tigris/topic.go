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

package tigris

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/tigrisdata/tigris-client-go/driver"
	"github.com/tigrisdata/tigris-client-go/filter"
	"github.com/tigrisdata/tigris-client-go/schema"
)

// Topic provides an interface for publishing and subscribing for the messages.
type Topic[T schema.Model] struct {
	name string
	db   driver.Database
}

// Drop drops the topic.
func (c *Topic[T]) Drop(ctx context.Context) error {
	return getDB(ctx, c.db).DropCollection(ctx, c.name)
}

// Publish publishes messages to the topic.
func (c *Topic[T]) Publish(ctx context.Context, docs ...*T) (*PublishResponse, error) {
	var err error

	bdocs := make([]driver.Message, len(docs))
	for k, v := range docs {
		if bdocs[k], err = json.Marshal(v); err != nil {
			return nil, err
		}
	}

	md, err := getDB(ctx, c.db).Publish(ctx, c.name, bdocs)
	if err != nil {
		return nil, err
	}

	if md == nil {
		return &PublishResponse{}, nil
	}

	if len(md.Keys) > 0 && len(md.Keys) != len(docs) {
		return nil, fmt.Errorf("broken response. number of published messages is not the same as number of provided messages")
	}

	for k, v := range md.Keys {
		if err := populateModelMetadata(docs[k], md.Metadata, v); err != nil {
			return nil, err
		}
	}

	return &PublishResponse{Keys: md.Keys}, nil
}

// Subscribe returns published messages from the topic.
func (c *Topic[T]) Subscribe(ctx context.Context, filter filter.Filter) (*Iterator[T], error) {
	f, err := filter.Build()
	if err != nil {
		return nil, err
	}

	it, err := getDB(ctx, c.db).Subscribe(ctx, c.name, f)
	if err != nil {
		return nil, err
	}

	return &Iterator[T]{Iterator: it}, nil
}
