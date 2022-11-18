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
//   - primary_key - declares primary key of the collection
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
	"unicode"
	"unsafe"

	"github.com/gertd/go-pluralize"
	"github.com/google/uuid"
	"github.com/iancoleman/strcase"
	"github.com/tigrisdata/tigris-client-go/driver"
)

// Tags supported by Tigris.
const (
	tagName = "tigris"

	tagPrimaryKey   = "primaryKey"
	tagAutoGenerate = "autoGenerate"
	tagSkip         = "-"

	id = "ID"
)

// Collection types.
const (
	Documents = "documents" // Regular collection containing documents
)

// Model represents types supported as collection models.
type Model interface{}

// PrimitiveFieldType represents types supported by non-composite fields.
type PrimitiveFieldType interface {
	string |
		int | int32 | int64 |
		float32 | float64 |
		[]byte | time.Time | uuid.UUID
}

var plural *pluralize.Client

func init() {
	plural = pluralize.NewClient()
}

// Supported data types
// See translateType for Golang to JSON schema translation rules.
const (
	typeInteger = "integer"
	typeString  = "string"
	typeBoolean = "boolean"
	typeNumber  = "number"
	typeArray   = "array"
	typeObject  = "object"
)

// Supported subtypes.
const (
	formatInt32    = "int32"
	formatByte     = "byte"
	formatDateTime = "date-time"
	formatUUID     = "uuid"
)

// Field represents JSON schema object.
type Field struct {
	Type   string            `json:"type,omitempty"`
	Format string            `json:"format,omitempty"`
	Tags   []string          `json:"tags,omitempty"`
	Desc   string            `json:"description,omitempty"`
	Fields map[string]*Field `json:"properties,omitempty"`
	Items  *Field            `json:"items,omitempty"`

	AutoGenerate bool `json:"autoGenerate,omitempty"`
}

// Schema is top level JSON schema object.
type Schema struct {
	Name       string            `json:"title,omitempty"`
	Desc       string            `json:"description,omitempty"`
	Fields     map[string]*Field `json:"properties,omitempty"`
	PrimaryKey []string          `json:"primary_key,omitempty"`

	CollectionType string `json:"collection_type,omitempty"`
}

// DatabaseModelName returns name of the database derived from the given database model.
func DatabaseModelName(s interface{}) string {
	t := reflect.TypeOf(s)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	return t.Name()
}

// ModelName returns name of the collection derived from the given collection model type.
// The name is snake case pluralized.
// If the original name ends with digit then it's not pluralized.
func ModelName(s interface{}) string {
	t := reflect.TypeOf(s)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	name := strcase.ToSnake(t.Name())
	if unicode.IsDigit(rune(name[len(name)-1])) {
		return name
	}

	return plural.Plural(name)
}

func translateType(t reflect.Type) (string, string, error) {
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	//nolint:golint,exhaustive
	switch t.Kind() {
	case reflect.Struct:
		if t.PkgPath() == "time" && t.Name() == "Time" {
			return typeString, formatDateTime, nil
		}

		return typeObject, "", nil
	case reflect.Array:
		if t.Elem().Kind() == reflect.Uint8 {
			if t.PkgPath() == "github.com/google/uuid" && t.Name() == "UUID" {
				return typeString, formatUUID, nil
			}
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

	return "", "", fmt.Errorf("unsupported type: name='%s' kind='%s'", t.Name(), t.Kind())
}

func isPrimaryKeyType(tp string) bool {
	return tp != typeArray && tp != typeNumber && tp != typeObject && tp != typeBoolean
}

// Build converts structured schema to driver schema.
func Build(sch *Schema) (driver.Schema, error) {
	return json.Marshal(sch)
}

// Build converts structured schema to driver schema.
func (sch *Schema) Build() (driver.Schema, error) {
	return Build(sch)
}

// parsePrimaryKeyIndex parses primary key tag
// The tag is expected to be in the form:
//
//	primary_key:{index}
//
// where {index} is primary key index part order in
// the composite key. Index starts from 1.
func parsePrimaryKeyIndex(tag string) (int, error) {
	i, pks := 1, strings.Split(tag, ":")
	if len(pks) > 1 {
		if len(pks) > 2 {
			return 0, fmt.Errorf("only one colon allowed in the tag")
		}

		var err error

		i, err = strconv.Atoi(pks[1])
		if err != nil {
			return 0, fmt.Errorf("error parsing primary key index %w", err)
		}

		if i == 0 {
			return 0, fmt.Errorf("primary key index starts from 1")
		}
	}

	return i, nil
}

func isTag(name string, tag string) bool {
	return strings.Compare(name, tag) == 0 || strings.Compare(name, strcase.ToSnake(tag)) == 0
}

// parseTag parses "tigris" tag and returns recognised tags.
// It also returns encountered "primary_key" tagged field in "pk" map,
// which maps field name to primary key index part.
func parseTag(name string, tag string, field *Field, pk map[string]int) (bool, error) {
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
		switch {
		case strings.HasPrefix(tag, tagPrimaryKey) ||
			strings.HasPrefix(tag, strcase.ToSnake(tagPrimaryKey)):
			if pk == nil {
				return false, nil
			}

			i, err := parsePrimaryKeyIndex(tag)
			if err != nil {
				return false, err
			}

			pk[name] = i
		case isTag(tag, tagAutoGenerate):
			field.AutoGenerate = true
		default:
			return false, fmt.Errorf("unknown tigris tag: %s", tag)
		}
	}

	return false, nil
}

// traverseFields recursively parses the model structure and build the schema structure out of it.
func traverseFields(prefix string, t reflect.Type, fields map[string]*Field, pk map[string]int, nFields *int) error {
	if prefix != "" {
		prefix += "."
	}

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)

		fName := strings.Split(field.Tag.Get("json"), ",")[0]

		// Obey JSON skip tag
		// and skip unexported fields
		if strings.Compare(fName, tagSkip) == 0 || !field.IsExported() {
			continue
		}

		// Use json name if it's defined
		if fName == "" && !field.Anonymous {
			fName = field.Name
		}

		// detect primary key tags in anonymous struct fields
		var lpk map[string]int
		if field.Anonymous {
			lpk = pk
		}

		var f Field

		skip, err := parseTag(prefix+fName, field.Tag.Get(tagName), &f, pk)
		if err != nil {
			return err
		}

		if skip {
			continue
		}

		f.Type, f.Format, err = translateType(field.Type)
		if err != nil {
			return err
		}

		if _, ok := pk[prefix+fName]; ok && !isPrimaryKeyType(f.Type) {
			return fmt.Errorf("type is not supported for the key: %s", field.Type.Name())
		}

		if f.AutoGenerate && !isPrimaryKeyType(f.Type) {
			return fmt.Errorf("type cannot be autogenerated: %s", field.Type.Name())
		}

		tp := field.Type
		if field.Type.Kind() == reflect.Ptr {
			tp = field.Type.Elem()
		}

		if tp.Kind() == reflect.Struct && f.Format != formatDateTime {
			if field.Anonymous {
				if err = traverseFields(prefix+fName, tp, fields, lpk, nFields); err != nil {
					return err
				}

				continue
			}

			f.Fields = make(map[string]*Field)
			if err = traverseFields(prefix+fName, tp, f.Fields, lpk, nFields); err != nil {
				return err
			}
		} else if f.Type == typeArray {
			if field.Type.Elem().Kind() == reflect.Struct {
				f.Items = &Field{
					// Name:   tp.Elem().Name(),
					Type:   typeObject,
					Fields: make(map[string]*Field),
				}
				if err := traverseFields(prefix+fName, tp.Elem(), f.Items.Fields, lpk, nFields); err != nil {
					return err
				}
			} else {
				// FIXME: Support multidimensional arrays
				tp, fm, err := translateType(tp.Elem())
				if err != nil {
					return err
				}
				f.Items = &Field{
					Type:   tp,
					Format: fm,
				}
			}
		}

		fields[fName] = &f
		*nFields++
	}

	return nil
}

func fromCollectionModel(model interface{}, typ string) (*Schema, error) {
	t := reflect.TypeOf(model)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	if t.Kind() != reflect.Struct {
		return nil, fmt.Errorf("model should be of struct type, not %s", t.Name())
	}

	sch := Schema{Name: ModelName(model), Fields: make(map[string]*Field)}
	pk := make(map[string]int)

	var nFields int

	if err := traverseFields("", t, sch.Fields, pk, &nFields); err != nil {
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

	if nFields == 0 {
		return nil, fmt.Errorf("no data fields in the collection schema")
	}

	// No explicit primary key defined and no tigris.Model embedded
	if len(pk) == 0 && typ == Documents {
		var (
			p  *Field
			ok bool
		)
		name := id
		// Check for existing and not annotated Id and ID fields
		// add `ID uuid.UUID` if none found
		if p, ok = sch.Fields["Id"]; ok {
			name = "Id"
		} else if p, ok = sch.Fields[id]; !ok {
			sch.Fields[id] = &Field{Type: typeString, Format: formatUUID, AutoGenerate: true}
			p = sch.Fields[id]
		}

		if !isPrimaryKeyType(p.Type) {
			return nil, fmt.Errorf("type is not supported for the key: %s", p.Type)
		}

		p.AutoGenerate = true

		sch.PrimaryKey = append(sch.PrimaryKey, name)
	}

	sch.CollectionType = typ

	return &sch, nil
}

// FromCollectionModels converts provided model(s) to the schema structure.
func FromCollectionModels(tp string, model Model, models ...Model) (map[string]*Schema, error) {
	schemas := make(map[string]*Schema)

	schema, err := fromCollectionModel(model, tp)
	if err != nil {
		return nil, err
	}

	schemas[schema.Name] = schema

	for _, m := range models {
		schema, err := fromCollectionModel(m, tp)
		if err != nil {
			return nil, err
		}

		schemas[schema.Name] = schema
	}

	return schemas, nil
}

// FromDatabaseModel converts provided database model to collections schema structures.
func FromDatabaseModel(dbModel interface{}) (string, map[string]*Schema, error) {
	t := reflect.TypeOf(dbModel)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	if t.Kind() != reflect.Struct {
		return "", nil, fmt.Errorf("database model should be of struct type containing collection models types as fields")
	}

	schemas := make(map[string]*Schema)

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)

		// Use json name if it's defined
		name := strings.Split(field.Tag.Get("json"), ",")[0]
		if name == "" {
			// Use field name and if it's not defined type name
			name = field.Name
		}

		skip, err := parseTag(name, field.Tag.Get(tagName), nil, nil)
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

		sch, err := fromCollectionModel(reflect.New(tt).Elem().Interface(), Documents)
		if err != nil {
			return "", nil, err
		}

		if name != "" {
			sch.Name = name
		}

		schemas[name] = sch
	}

	return t.Name(), schemas, nil
}
