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

// Package schema provides methods to build JSON schema of the collections
// from user defined models.
//
// Collection model is declared using Go struct tags. The "tigris" tag contains field properties.
// The following properties are recognised:
//  * primary_key - declares primary key of the collection
//
// Composite primary keys defined using optional tag indexes.
//
// Special tag value "-" can be specified to skip field persistence.
// Unexported fields are not persisted too.
package schema

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
	"unsafe"

	"github.com/tigrisdata/tigris-client-go/driver"
)

// Tags supported by Tigris
const (
	tagName = "tigris"

	tagPrimaryKey = "primary_key"
	tagSkip       = "-"
)

// PrimitiveFieldType represents types supported by non-composite fields
type PrimitiveFieldType interface {
	string |
		int | int32 | int64 |
		float32 | float64 |
		[]byte | *time.Time
}

// Supported data types
// See translateType for Golang to JSON schema translation rules
const (
	typeInteger = "integer"
	typeString  = "string"
	typeBoolean = "boolean"
	typeNumber  = "number"
	typeArray   = "array"
	typeObject  = "object"
)

// Supported subtypes
const (
	formatInt32    = "int32"
	formatByte     = "byte"
	formatDateTime = "date-time"
)

// Field represents JSON schema object
type Field struct {
	Name   string   `json:"title,omitempty"`
	Type   string   `json:"type,omitempty"`
	Format string   `json:"format,omitempty"`
	Tags   []string `json:"tags,omitempty"`
	Desc   string   `json:"description,omitempty"`
	Fields []Field  `json:"properties,omitempty"`
	Items  *Field   `json:"items,omitempty"`
}

// Schema is top level JSON schema object
type Schema struct {
	Name       string   `json:"title,omitempty"`
	Desc       string   `json:"description,omitempty"`
	Fields     []Field  `json:"properties,omitempty"`
	PrimaryKey []string `json:"primary_key,omitempty"`
}

// ModelName returns name of the model
func ModelName(s interface{}) string {
	t := reflect.TypeOf(s)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	return t.Name()
}

// TODO: Handle UUID, Datetime, Set types
// Uint is translated to int64 to avoid integer overflow
// Uint64 cannot be represented by the JSON schema types
func translateType(t reflect.Type) (string, string, error) {
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	switch t.Kind() {
	case reflect.Struct:
		if t.PkgPath() == "time" && t.Name() == "Time" {
			return typeString, formatDateTime, nil
		}
		return typeObject, "", nil
	case reflect.Array:
		if t.Elem().Kind() == reflect.Uint8 {
			return typeString, formatByte, nil
		}
		return typeArray, "", nil
	case reflect.Slice:
		if t.Elem().Kind() == reflect.Uint8 {
			return typeString, formatByte, nil
		}
		return typeArray, "", nil
	case reflect.Int32:
		return typeInteger, formatInt32, nil
	case reflect.Int64:
		return typeInteger, "", nil
	case reflect.String:
		return typeString, "", nil
	case reflect.Float32, reflect.Float64:
		return typeNumber, "", nil
	case reflect.Bool:
		return typeBoolean, "", nil
	case reflect.Map:
		return typeObject, "", nil
	case reflect.Int:
		var a int
		if unsafe.Sizeof(a) == 4 {
			return typeInteger, formatInt32, nil
		}
		return typeInteger, "", nil
	}

	return "", "", fmt.Errorf("unsupported type: '%s'", t.Name())
}

func isPrimaryKeyType(tp string) bool {
	return tp != typeArray && tp != typeNumber && tp != typeObject && tp != typeBoolean
}

// Build converts structured schema to driver schema
func Build(sch *Schema) (driver.Schema, error) {
	return json.Marshal(sch)
}

// Build converts structured schema to driver schema
func (sch *Schema) Build() (driver.Schema, error) {
	return Build(sch)
}

// parsePrimaryKeyIndex parses primary key tag
// The tag is expected to be in the form:
//   primary_key:{index}
// where {index} is primary key index part order in
// the composite key. Index starts from 1.
func parsePrimaryKeyIndex(tag string) (int, error) {
	pks := strings.Split(tag, ":")
	i := 1
	if len(pks) > 1 {
		if len(pks) > 2 {
			return 0, fmt.Errorf("only one colon allowed in the tag")
		}
		var err error
		i, err = strconv.Atoi(pks[1])
		if err != nil {
			return 0, fmt.Errorf("error parsing primary key index %s", err)
		}
		if i == 0 {
			return 0, fmt.Errorf("primary key index starts from 1")
		}
	}

	return i, nil
}

// parseTag parses "tigris" tag and returns recognised tags.
// It also returns encountered "primary_key" tagged field in "pk" map,
// which maps field name to primary key index part
func parseTag(name string, tag string, _ *[]string, pk map[string]int) (bool, error) {
	if tag == "" {
		return false, nil
	}

	tagList := strings.Split(tag, ",")

	for _, tag := range tagList {
		if strings.Compare(tag, tagSkip) == 0 {
			return true, nil
		}
	}

	for _, tag := range tagList {
		if strings.HasPrefix(tag, tagPrimaryKey) {
			if pk == nil {
				return false, fmt.Errorf("cannot define primary key on database level")
			}
			i, err := parsePrimaryKeyIndex(tag)
			if err != nil {
				return false, err
			}
			pk[name] = i
			/*
				} else if strings.Compare(tag, tagPII) == 0 {
					*tags = append(*tags, tagPII)
				} else if strings.Compare(tag, tagEncrypted) == 0 {
					*tags = append(*tags, tagEncrypted)
			*/
		} else {
			return false, fmt.Errorf("unknown tigris tag: %s", tag)
		}
	}

	return false, nil
}

// traverseFields recursively parses the model structure and build the schema structure out of it
func traverseFields(prefix string, t reflect.Type, pk map[string]int, nFields *int) ([]Field, error) {
	var fields []Field

	if prefix != "" {
		prefix += "."
	}

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)

		name := strings.Split(field.Tag.Get("json"), ",")[0]

		// Obey JSON skip tag
		if strings.Compare(name, tagSkip) == 0 {
			continue
		}

		// Use json name if it's defined
		if name == "" {
			name = field.Name
		}

		var f Field

		f.Name = name

		skip, err := parseTag(prefix+f.Name, field.Tag.Get(tagName), &f.Tags, pk)
		if err != nil {
			return nil, err
		}

		if skip {
			continue
		}

		f.Type, f.Format, err = translateType(field.Type)
		if err != nil {
			return nil, err
		}

		if _, ok := pk[prefix+f.Name]; ok && !isPrimaryKeyType(f.Type) {
			return nil, fmt.Errorf("type is not supported for the key: %s", field.Type.Name())

		}

		tp := field.Type
		if field.Type.Kind() == reflect.Ptr {
			tp = field.Type.Elem()
		}

		if tp.Kind() == reflect.Struct && f.Format != formatDateTime {
			f.Fields, err = traverseFields(prefix+f.Name, tp, pk, nFields)
			if err != nil {
				return nil, err
			}
		} else if f.Type == typeArray {
			if field.Type.Elem().Kind() == reflect.Struct {
				fields, err := traverseFields(prefix+f.Name, tp.Elem(), pk, nFields)
				if err != nil {
					return nil, err
				}
				f.Items = &Field{
					Name:   tp.Elem().Name(),
					Type:   typeObject,
					Fields: fields,
				}
			} else {
				tp, fm, err := translateType(tp.Elem())
				if err != nil {
					return nil, err
				}
				f.Items = &Field{
					Type:   tp,
					Format: fm,
				}
			}
		}

		fields = append(fields, f)
		*nFields++
	}

	return fields, nil
}

func fromCollectionModel(model interface{}) (*Schema, error) {
	t := reflect.TypeOf(model)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return nil, fmt.Errorf("model should be of struct type, not %s", t.Name())
	}

	var err error
	var sch Schema
	var nFields int
	pk := make(map[string]int)

	sch.Name = ModelName(model)

	sch.Fields, err = traverseFields("", t, pk, &nFields)
	if err != nil {
		return nil, err
	}

	sch.PrimaryKey = make([]string, len(pk))
	for k, v := range pk {
		if v > nFields {
			return nil, fmt.Errorf("maximum primary key index is %v", nFields)
		}
		if v > len(pk) {
			return nil, fmt.Errorf("gap in the primary key index")
		}
		if sch.PrimaryKey[v-1] != "" {
			return nil, fmt.Errorf("duplicate primary key index %d set for %s and %s", v, k, sch.PrimaryKey[v-1])
		}
		sch.PrimaryKey[v-1] = k
	}

	if len(pk) == 0 {
		return nil, fmt.Errorf("no primary key defined in schema")
	}

	return &sch, nil
}

// FromCollectionModels converts provided model(s) to the schema structure
func FromCollectionModels(model interface{}, models ...interface{}) ([]*Schema, error) {
	//models parameter added to require at least one schema to migrate
	var schemas []*Schema

	schema, err := fromCollectionModel(model)
	if err != nil {
		return nil, err
	}
	schemas = append(schemas, schema)

	for _, m := range models {
		schema, err := fromCollectionModel(m)
		if err != nil {
			return nil, err
		}
		schemas = append(schemas, schema)
	}

	return schemas, nil
}

// FromDatabaseModel converts provided database model to collections schema structures
func FromDatabaseModel(dbModel interface{}) (string, []*Schema, error) {
	t := reflect.TypeOf(dbModel)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return "", nil, fmt.Errorf("database model should be of struct type containing collection models types as fields")
	}

	var schemas []*Schema

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)

		// Use json name if it's defined
		name := strings.Split(field.Tag.Get("json"), ",")[0]
		if name == "" {
			// Use field name and if it's not defined type name
			name = field.Name
		}

		var tags []string
		skip, err := parseTag(name, field.Tag.Get(tagName), &tags, nil)
		if err != nil {
			return "", nil, err
		}

		if skip {
			continue
		}

		tt := field.Type
		if field.Type.Kind() == reflect.Array || field.Type.Kind() == reflect.Slice {
			tt = field.Type.Elem()
		}

		sch, err := fromCollectionModel(reflect.New(tt).Elem().Interface())
		if err != nil {
			return "", nil, err
		}

		if name != "" {
			sch.Name = name
		}

		schemas = append(schemas, sch)
	}

	return t.Name(), schemas, nil
}
