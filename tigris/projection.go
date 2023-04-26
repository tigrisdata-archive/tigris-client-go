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
	"reflect"
	"strings"

	"github.com/buger/jsonparser"
	api "github.com/tigrisdata/tigris-client-go/api/server/v1"
	"github.com/tigrisdata/tigris-client-go/driver"
	"github.com/tigrisdata/tigris-client-go/fields"
	"github.com/tigrisdata/tigris-client-go/filter"
	"github.com/tigrisdata/tigris-client-go/schema"
)

type Projection[T schema.Model, P schema.Model] struct {
	coll       *Collection[T]
	projection driver.Projection
	err        error
	keyPath    []string
}

func findSubType(t reflect.Type, keyPath []string) (reflect.Type, error) {
	for _, v := range keyPath {
		found := false
		for i := 0; i < t.NumField(); i++ {
			field := t.Field(i)

			fName := fieldName(field)

			tp := field.Type
			if tp.Kind() == reflect.Ptr {
				tp = tp.Elem()
			}

			if tp.Kind() == reflect.Slice || tp.Kind() == reflect.Array {
				tp = tp.Elem()
				if tp.Kind() == reflect.Ptr {
					tp = tp.Elem()
				}
			}

			if tp.Kind() == reflect.Struct {
				if fName == v {
					t = tp
					found = true
					break
				}
			}
		}
		if !found {
			return nil, fmt.Errorf("no subobject found for key path %+v", keyPath)
		}
	}

	return t, nil
}

// GetProjection returns collection object corresponding to collection model T.
func GetProjection[T schema.Model, P schema.Model](db *Database, keyPath ...string) *Projection[T, P] {
	var (
		m T
		p P
	)

	name := schema.ModelName(&m)
	prefix := strings.Join(keyPath, ".")
	pt := &Projection[T, P]{coll: getNamedCollection[T](db, name)}

	t, err := findSubType(reflect.TypeOf(m), keyPath)
	if err != nil {
		pt.err = fmt.Errorf("no subobject found for key path %+v", keyPath)
		return pt
	}

	pt.keyPath = keyPath

	// TODO: Cache marshaled projections
	include := fields.ReadBuilder()
	_, pt.err = flattenType(prefix, t, reflect.TypeOf(p), include)
	if pt.err != nil {
		return pt
	}

	_, pt.err = include.Build()
	if pt.err != nil {
		return pt
	}

	pt.projection = include.Built()

	// All the fields under the prefix is included
	if string(pt.projection) == `{}` && len(prefix) > 0 {
		include = fields.ReadBuilder()
		include.Include(prefix)
		_, pt.err = include.Build()
		pt.projection = include.Built()
	}

	return pt
}

// Read returns documents which satisfies the filter.
// Only field from the give fields are populated in the documents. By default, all fields are populated.
func (p *Projection[T, P]) Read(ctx context.Context, filter filter.Filter) (*Iterator[P], error) {
	if p.err != nil {
		return nil, p.err
	}

	f, err := filter.Build()
	if err != nil {
		return nil, err
	}

	it, err := getDB(ctx, p.coll.db).Read(ctx, p.coll.name, f, p.projection)
	if err != nil {
		return nil, err
	}

	return &Iterator[P]{Iterator: it, unmarshaler: p.projectionUnmarshal}, nil
}

// ReadAll reads all the documents from the collection.
func (p *Projection[T, P]) ReadAll(ctx context.Context) (*Iterator[P], error) {
	if p.err != nil {
		return nil, p.err
	}

	it, err := getDB(ctx, p.coll.db).Read(ctx, p.coll.name, driver.Filter(`{}`), p.projection)
	if err != nil {
		return nil, err
	}

	return &Iterator[P]{Iterator: it, unmarshaler: p.projectionUnmarshal}, nil
}

// ReadWithOptions returns specific fields of the documents according to the filter.
// It allows further configure returned documents by providing options:
//
//	Limit - only returned first "Limit" documents from the result
//	Skip - start returning documents after skipping "Skip" documents from the result
func (p *Projection[T, P]) ReadWithOptions(ctx context.Context, filter filter.Filter, options *ReadOptions) (*Iterator[P], error) {
	if p.err != nil {
		return nil, p.err
	}

	f, err := filter.Build()
	if err != nil {
		return nil, err
	}
	if options == nil {
		return nil, fmt.Errorf("API expecting options but received null")
	}

	it, err := getDB(ctx, p.coll.db).Read(ctx, p.coll.name, f, p.projection, &driver.ReadOptions{
		Limit:     options.Limit,
		Skip:      options.Skip,
		Offset:    options.Offset,
		Collation: (*api.Collation)(options.Collation),
	})
	if err != nil {
		return nil, err
	}

	return &Iterator[P]{Iterator: it, unmarshaler: p.projectionUnmarshal}, nil
}

func fieldName(field reflect.StructField) string {
	fName := strings.Split(field.Tag.Get("json"), ",")[0]

	// Obey JSON skip tag
	// and skip unexported fields
	if strings.Compare(fName, "-") == 0 || !field.IsExported() {
		return ""
	}

	if strings.Trim(field.Tag.Get("tigris"), " ") == "-" {
		return ""
	}

	// Use json name if it's defined
	if fName == "" && !field.Anonymous {
		fName = field.Name
	}

	return fName
}

// ReadOne returns first documents which satisfies the filter.
func (p *Projection[T, P]) ReadOne(ctx context.Context, filter filter.Filter) (*P, error) {
	var doc P

	it, err := p.Read(ctx, filter)
	if err != nil {
		return nil, err
	}

	defer it.Close()

	if !it.Next(&doc) {
		if it.Err() != nil {
			return nil, it.Err()
		}

		return nil, ErrNotFound
	}

	return &doc, nil
}

func flattenType(prefix string, d reflect.Type, p reflect.Type, fields *fields.Read) (bool, error) {
	if prefix != "" {
		prefix += "."
	}

	docFields := make(map[string]reflect.Type)
	for i := 0; i < d.NumField(); i++ {
		fn := fieldName(d.Field(i))
		if fn == "" {
			continue
		}

		dp := d.Field(i).Type
		if dp.Kind() == reflect.Ptr {
			dp = dp.Elem()
		}

		docFields[fn] = dp
	}

	if p.Kind() == reflect.Slice || p.Kind() == reflect.Array {
		p = p.Elem()
		if p.Kind() == reflect.Ptr {
			p = p.Elem()
		}
	}

	incomplete := make(map[string]bool)
	for i := 0; i < p.NumField(); i++ {
		field := p.Field(i)

		fName := fieldName(field)
		if fName == "" {
			continue
		}

		pft := field.Type
		if pft.Kind() == reflect.Ptr {
			pft = pft.Elem()
		}

		dft, ok := docFields[fName]
		if !ok {
			return false, fmt.Errorf("field present in projection and doesn't exist in the document: %s", prefix+fName)
		}

		if dft.Kind() == reflect.Ptr {
			dft = dft.Elem()
		}

		if pft.Kind() != dft.Kind() {
			return false, fmt.Errorf("projection type mismatch. doc=%s, projection=%s", dft.Kind(), pft.Kind())
		}

		if pft.Kind() == reflect.Slice || pft.Kind() == reflect.Array {
			pft = pft.Elem()
			dft = dft.Elem()
			if pft.Kind() == reflect.Ptr {
				pft = pft.Elem()
			}
			if dft.Kind() == reflect.Ptr {
				dft = dft.Elem()
			}
		}

		if pft.Kind() == reflect.Struct && !schema.SimpleType(pft) {
			i, err := flattenType(prefix+fName, dft, pft, fields)
			if err != nil {
				return false, err
			}
			if i {
				incomplete[prefix+fName] = true
			}
		}

		delete(docFields, fName)
	}

	if len(docFields) > 0 || len(incomplete) > 0 {
		for i := 0; i < p.NumField(); i++ {
			fName := fieldName(p.Field(i))

			if !incomplete[prefix+fName] {
				fields.Include(prefix + fName)
			}
		}

		return true, nil
	}

	return false, nil
}

func (p *Projection[T, P]) projectionUnmarshal(b []byte, v *P) error {
	if len(p.keyPath) > 0 {
		sub, _, _, err := jsonparser.Get(b, p.keyPath...)
		if err != nil {
			return err
		}

		return json.Unmarshal(sub, v)
	}

	return json.Unmarshal(b, v)
}
