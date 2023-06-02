// Copyright 2022-2023 Tigris Data, Inc.
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

package timeseries

import (
	"context"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/tigrisdata/tigris-client-go/filter"
	"github.com/tigrisdata/tigris-client-go/schema"
	"github.com/tigrisdata/tigris-client-go/tigris"
)

const timestamp = "timestamp"

// BufSize is the size of the event buffer.
var BufSize = 16

// Model is embeddable model with primary key for time-series collection.
type Model struct {
	Timestamp time.Time `json:"timestamp" tigris:"primaryKey:1,autoGenerate"`
	ID        uuid.UUID `json:"id"        tigris:"primaryKey:2,autoGenerate"`
}

// Collection is buffered time-series collection.
type Collection[T schema.Model] struct {
	*tigris.Collection[T]
	buffer []*T

	sync.Mutex
}

// OpenDatabase initializes Database from given time-series collection models.
func OpenDatabase(ctx context.Context, cfg *tigris.Config, models ...schema.Model,
) (*tigris.Database, error) {
	return tigris.OpenDatabase(ctx, cfg, models...)
}

// GetCollection object which implements time-series API to append and find events.
func GetCollection[T schema.Model](db *tigris.Database) *Collection[T] {
	coll := tigris.GetCollection[T](db)
	return &Collection[T]{Collection: coll}
}

// Append writes events to the collections.
// It buffers the writes and flushes events once the buffer is full.
func (c *Collection[T]) Append(ctx context.Context, events ...*T) error {
	i := 0

	c.Lock()

	for i < len(events) {
		for ; len(c.buffer) < BufSize && i < len(events); i++ {
			c.buffer = append(c.buffer, events[i])
		}

		if len(c.buffer) >= BufSize {
			buf := c.buffer
			c.buffer = make([]*T, 0, BufSize)

			c.Unlock()

			if _, err := c.InsertOrReplace(ctx, buf...); err != nil {
				return err
			}

			c.Lock()
		}
	}

	c.Unlock()

	return nil
}

// Flush forces flushing buffered events to the Tigris.
func (c *Collection[T]) Flush(ctx context.Context) error {
	c.Lock()

	buf := c.buffer
	c.buffer = make([]*T, 0, BufSize)

	c.Unlock()

	if _, err := c.InsertOrReplace(ctx, buf...); err != nil {
		return err
	}

	return nil
}

// FindAfter returns events created at or after given time parameter.
// Documents can be further filtered by additional filter.
func (c *Collection[T]) FindAfter(ctx context.Context, ts time.Time, addFilter ...filter.Filter) (*tigris.Iterator[T], error) {
	if len(addFilter) > 0 {
		it, err := c.Read(ctx, filter.And(filter.Gte(timestamp, ts), addFilter[0]))
		return it, err
	}

	return c.Read(ctx, filter.Gte(timestamp, ts))
}

// FindBefore returns events created before given time parameter.
// Documents can be further filtered by additional filter.
func (c *Collection[T]) FindBefore(ctx context.Context, ts time.Time, addFilter ...filter.Filter) (*tigris.Iterator[T], error) {
	if len(addFilter) > 0 {
		it, err := c.Read(ctx, filter.And(filter.Lt(timestamp, ts), addFilter[0]))
		return it, err
	}

	return c.Read(ctx, filter.Lt(timestamp, ts))
}

// FindInRange returns events in the time range [startTS, endTS).
// Start time is included in the range, end time is excluded.
// Documents can be further filtered by additional filter.
func (c *Collection[T]) FindInRange(ctx context.Context, startTS time.Time, endTS time.Time, addFilter ...filter.Filter) (*tigris.Iterator[T], error) {
	if len(addFilter) > 0 {
		it, err := c.Read(ctx, filter.And(filter.Gte(timestamp, startTS), filter.Lt(timestamp, endTS), addFilter[0]))
		return it, err
	}

	return c.Read(ctx, filter.And(filter.Gte(timestamp, startTS), filter.Lt(timestamp, endTS)))
}
