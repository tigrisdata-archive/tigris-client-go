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

package tigris

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/buger/jsonparser"
	jsoniter "github.com/json-iterator/go"
	"github.com/tigrisdata/tigris-client-go/driver"
	"github.com/tigrisdata/tigris-client-go/fields"
	"github.com/tigrisdata/tigris-client-go/filter"
	"github.com/tigrisdata/tigris-client-go/schema"
)

type JoinType int

const (
	OuterJoin JoinType = iota
	InnerJoin
)

type JoinOptions struct {
	Type        JoinType
	ArrayUnroll bool
}

type Join[P schema.Model, C schema.Model] struct {
	options     *JoinOptions
	db          driver.Database
	parentName  string
	childName   string
	parentField string
	childField  string

	_ JoinResult[P, C]
}

func GetJoin[P schema.Model, C schema.Model](db *Database, parentField string, childField string, options ...*JoinOptions) *Join[P, C] {
	var (
		p P
		c C
	)

	j := &Join[P, C]{
		parentName: schema.ModelName(&p),
		childName:  schema.ModelName(&c),
		db:         db.driver.UseDatabase(db.name), parentField: parentField, childField: childField,
	}

	if len(options) > 0 && options[0] != nil {
		j.options = options[0]
	}

	return j
}

func parseJSONVal(tp jsonparser.ValueType, pv []byte) (any, error) {
	var (
		vv  any
		err error
	)

	switch tp {
	case jsonparser.String:
		vv, err = jsonparser.ParseString(pv)
	case jsonparser.Number:
		vv, err = jsonparser.ParseInt(pv)
		if err != nil {
			vv, err = jsonparser.ParseFloat(pv)
		}
	case jsonparser.Object, jsonparser.Array:
		vv = json.RawMessage(pv)
	case jsonparser.Boolean:
		vv, err = jsonparser.ParseBoolean(pv)
	case jsonparser.Null:
		vv = nil
	case jsonparser.Unknown, jsonparser.NotExist:
		panic("unexpected json value in join result")
	}

	return vv, err
}

type joinRead[P schema.Model, C schema.Model] struct {
	in         []filter.Expr
	res        map[string][]*JoinResult[P, C]
	sortedRes  []*JoinResult[P, C]
	childField string
}

func (r *joinRead[P, C]) stageLeftResult(tp jsonparser.ValueType, leftField []byte, rec *JoinResult[P, C]) error {
	vv, err := parseJSONVal(tp, leftField)
	if err != nil {
		return err
	}

	if r.res[string(leftField)] == nil {
		if vv == nil {
			vv = (*string)(nil) // untyped nil is not marshalled as null in the filter
		}

		// seeing value of leftField for the first time, stage for select from the right table
		r.in = append(r.in, filter.Eq(r.childField, vv))
	}

	r.res[string(leftField)] = append(r.res[string(leftField)], rec)

	return nil
}

func (c *Join[P, C]) readLeft(ctx context.Context, flt filter.Filter, flds []*fields.Read, options *ReadOptions,
) (*joinRead[P, C], error) {
	p, err := getFields(flds...)
	if err != nil {
		return nil, err
	}

	f, err := flt.Build()
	if err != nil {
		return nil, err
	}

	opt, err := setReadOptions(options)
	if err != nil {
		return nil, err
	}

	it, err := getDB(ctx, c.db).Read(ctx, c.parentName, f, p, opt)
	if err != nil {
		return nil, err
	}

	pk := strings.Split(c.parentField, ".")

	var (
		pd driver.Document
		r  = joinRead[P, C]{childField: c.childField, res: map[string][]*JoinResult[P, C]{}}
	)

	for it.Next(&pd) {
		pv, tp, _, err := jsonparser.Get(pd, pk...)
		if err != nil {
			return nil, err
		}

		var pp P

		if err = jsoniter.Unmarshal(pd, &pp); err != nil {
			return nil, err
		}

		pr := &JoinResult[P, C]{Parent: &pp}
		r.sortedRes = append(r.sortedRes, pr) // preserve the order of left query result

		if tp == jsonparser.Array && c.options != nil && c.options.ArrayUnroll {
			_, err = jsonparser.ArrayEach(pv, func(val []byte, tp jsonparser.ValueType, _ int, err error) {
				_ = r.stageLeftResult(tp, val, pr)
			})
		} else {
			err = r.stageLeftResult(tp, pv, pr)
		}

		if err != nil {
			return nil, err
		}
	}

	return &r, nil
}

func (c *Join[P, C]) readRight(ctx context.Context, r *joinRead[P, C]) error {
	f, err := filter.Or(r.in...).Build()
	if err != nil {
		return err
	}

	itc, err := getDB(ctx, c.db).Read(ctx, c.childName, f, driver.Projection(`{}`))
	if err != nil {
		return err
	}

	ck := strings.Split(c.childField, ".")

	var cd driver.Document

	for itc.Next(&cd) {
		cv, _, _, err := jsonparser.Get(cd, ck...)
		if err != nil {
			return err
		}

		jr, ok := r.res[string(cv)]
		if !ok {
			return fmt.Errorf("unexpected join-child row result. parent field: %v, child field %v, value: %v",
				c.parentField, c.childField, string(cv))
		}

		var cc C

		if err := jsoniter.Unmarshal(cd, &cc); err != nil {
			return err
		}

		// join with all left records which expects this value of join field
		for _, v := range jr {
			v.Child = append(v.Child, &cc)
		}
	}

	return nil
}

func (c *Join[P, C]) fetchJoinResult(ctx context.Context, flt filter.Filter, flds []*fields.Read,
	options *ReadOptions,
) (*JoinIterator[P, C], error) {
	r, err := c.readLeft(ctx, flt, flds, options)
	if err != nil {
		return nil, err
	}

	if len(r.in) > 0 {
		if err = c.readRight(ctx, r); err != nil {
			return nil, err
		}
	}

	return &JoinIterator[P, C]{err: nil, sortedRes: r.sortedRes, options: c.options}, nil
}

// Read returns documents which satisfies the filter.
// Only field from the give fields are populated in the documents. By default, all fields are populated.
func (c *Join[P, C]) Read(ctx context.Context, flt filter.Filter, flds ...*fields.Read) (*JoinIterator[P, C], error) {
	return c.fetchJoinResult(ctx, flt, flds, nil)
}

// ReadWithOptions returns specific fields of the documents according to the filter.
// It allows further configure returned documents by providing options:
//
//	Limit - only returned first "Limit" documents from the result
//	Skip - start returning documents after skipping "Skip" documents from the result
func (c *Join[P, C]) ReadWithOptions(ctx context.Context, flt filter.Filter, flds *fields.Read, options *ReadOptions,
) (*JoinIterator[P, C], error) {
	if options == nil {
		return nil, fmt.Errorf("API expecting options but received null")
	}

	return c.fetchJoinResult(ctx, flt, []*fields.Read{flds}, options)
}

// ReadOne reads one document from the collection satisfying the filter.
func (c *Join[P, C]) ReadOne(ctx context.Context, flt filter.Filter, flds ...*fields.Read) (*JoinResult[P, C], error) {
	it, err := c.Read(ctx, flt, flds...)
	if err != nil {
		return nil, err
	}

	defer it.Close()

	var (
		p    P
		chld []*C
	)

	if !it.Next(&p, &chld) {
		if it.Err() != nil {
			return nil, it.Err()
		}

		return nil, ErrNotFound
	}

	return &JoinResult[P, C]{Parent: &p, Child: chld}, nil
}

// ReadAll returns JoinIterator which iterates over all the documents
// in the collection.
func (c *Join[P, C]) ReadAll(ctx context.Context, flds ...*fields.Read) (*JoinIterator[P, C], error) {
	return c.fetchJoinResult(ctx, filter.All, flds, nil)
}
