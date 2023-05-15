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
	"sort"
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

// Tags supported by the Tigris.
const (
	tagName = "tigris"

	tagPrimaryKey   = "primaryKey"
	tagAutoGenerate = "autoGenerate"
	tagSkip         = "-"
	tagRequired     = "required"
	tagDefault      = "default"
	tagMaxLength    = "maxLength"
	tagUpdatedAt    = "updatedAt"
	tagCreatedAt    = "createdAt"
	tagIndex        = "index"
	tagSearchIndex  = "searchIndex"
	tagSort         = "sort"
	tagFacet        = "facet"
	tagVector       = "vector"

	tagVersion = "version"

	id = "ID"
)

// Collection types.
const (
	Documents = "documents" // Regular collection containing documents
	Search    = "search"    // Search index
)

// Model represents types supported as collection models.
type Model any

// PrimitiveFieldType represents types supported by non-composite fields.
type PrimitiveFieldType interface {
	string |
		int | int32 | int64 |
		float32 | float64 |
		[]byte | time.Time | uuid.UUID |
		bool
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
	formatVector   = "vector"
)

// Field represents JSON schema object.
type Field struct {
	Type   string            `json:"type,omitempty"`
	Format string            `json:"format,omitempty"`
	Tags   []string          `json:"tags,omitempty"`
	Desc   string            `json:"description,omitempty"`
	Fields map[string]*Field `json:"properties,omitempty"`
	Items  *Field            `json:"items,omitempty"`

	Default      any  `json:"default,omitempty"`
	AutoGenerate bool `json:"autoGenerate,omitempty"`
	MaxLength    int  `json:"maxLength,omitempty"`
	UpdatedAt    bool `json:"updatedAt,omitempty"`
	CreatedAt    bool `json:"createdAt,omitempty"`
	Index        bool `json:"index,omitempty"`
	SearchIndex  bool `json:"searchIndex,omitempty"`
	Sort         bool `json:"sort,omitempty"`
	Facet        bool `json:"facet,omitempty"`
	MaxItems     int  `json:"maxItems,omitempty"`
	Dimensions   int  `json:"dimensions,omitempty"`

	Required []string `json:"required,omitempty"`

	// RequiredTag And IndexTag is used during schema building only
	RequiredTag bool `json:"-"`

	AdditionalProperties bool `json:"additionalProperties,omitempty"`
}

// Schema is top level JSON schema object.
type Schema struct {
	Version int `json:"version,omitempty"`

	Name string `json:"title,omitempty"`
	Desc string `json:"description,omitempty"`

	Fields     map[string]*Field `json:"properties,omitempty"`
	PrimaryKey []string          `json:"primary_key,omitempty"`
	Required   []string          `json:"required,omitempty"`
	Index      []string          `json:"index,omitempty"`

	CollectionType string        `json:"collection_type,omitempty"`
	SearchSource   *SearchSource `json:"source,omitempty"`
}

type SearchSource struct {
	Type string `json:"type"`
}

// DatabaseModelName returns name of the database derived from the given database model.
func DatabaseModelName(s any) string {
	t := reflect.TypeOf(s)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	return t.Name()
}

// ModelName returns name of the collection derived from the given collection model type.
// The name is snake case pluralized.
// If the original name ends with digit then it's not pluralized.
func ModelName(s any) string {
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

func translateType(t reflect.Type, f *Field) error {
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	//nolint:exhaustive
	switch t.Kind() {
	case reflect.Struct:
		f.Type = typeObject

		if t.PkgPath() == "time" && t.Name() == "Time" {
			f.Type = typeString
			f.Format = formatDateTime
		}
	case reflect.Array:
		f.Type = typeArray
		f.MaxItems = t.Len()

		if t.Elem().Kind() == reflect.Uint8 {
			f.Type = typeString
			f.Format = formatByte
			f.MaxItems = 0 // it's irrelevant for strings

			if t.PkgPath() == "github.com/google/uuid" && t.Name() == "UUID" {
				f.Format = formatUUID
			}
		}
	case reflect.Slice:
		f.Type = typeArray

		if t.Elem().Kind() == reflect.Uint8 {
			f.Type = typeString
			f.Format = formatByte
		}
	case reflect.Int32:
		f.Type = typeInteger
		f.Format = formatInt32
	case reflect.Int64:
		f.Type = typeInteger
	case reflect.String:
		f.Type = typeString
	case reflect.Float32, reflect.Float64:
		f.Type = typeNumber
	case reflect.Bool:
		f.Type = typeBoolean
	case reflect.Map:
		f.Type = typeObject
		f.AdditionalProperties = true
	case reflect.Int:
		f.Type = typeInteger

		var a int
		if unsafe.Sizeof(a) == 4 {
			f.Format = formatInt32
		}
	default:
		return fmt.Errorf("unsupported type: name='%s' kind='%s'", t.Name(), t.Kind())
	}

	return nil
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

func setRequired(fields map[string]*Field) []string {
	var req []string

	for k, v := range fields {
		if v.RequiredTag {
			req = append(req, k)
		}
	}

	sort.Strings(req)

	return req
}

// SimpleType returns true if type marshals to primitive JSON types: string, bool, number.
func SimpleType(t reflect.Type) bool {
	return t.PkgPath() == "time" && t.Name() == "Time" ||
		t.PkgPath() == "github.com/google/uuid" && t.Name() == "UUID"
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
		var err error

		if err = translateType(field.Type, &f); err != nil {
			return err
		}

		skip, err := parseTag(prefix+fName, field.Tag.Get(tagName), &f, pk)
		if err != nil {
			return err
		}

		if skip {
			continue
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

				f.Required = setRequired(f.Fields)
				continue
			}

			f.Fields = make(map[string]*Field)
			if err = traverseFields(prefix+fName, tp, f.Fields, lpk, nFields); err != nil {
				return err
			}

			f.Required = setRequired(f.Fields)
		} else if f.Type == typeArray {
			if tp.Elem().Kind() == reflect.Ptr {
				tp = tp.Elem()
			}

			if tp.Elem().Kind() == reflect.Struct {
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
				f.Items = &Field{}
				if err = translateType(tp.Elem(), f.Items); err != nil {
					return err
				}
			}
		}

		fields[fName] = &f
		*nFields++
	}

	return nil
}

func fromCollectionModel(schemaVersion int, model any, typ string) (*Schema, error) {
	t := reflect.TypeOf(model)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	if t.Kind() != reflect.Struct {
		return nil, fmt.Errorf("model should be of struct type, not %s", t.Name())
	}

	sch := Schema{Version: schemaVersion, Name: ModelName(model), Fields: make(map[string]*Field)}
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
		} else if p, ok = sch.Fields["id"]; ok {
			name = "id"
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

	sch.Required = setRequired(sch.Fields)
	if typ == Search {
		sch.SearchSource = &SearchSource{Type: "external"}
	} else {
		sch.CollectionType = typ
	}

	return &sch, nil
}

// FromCollectionModels converts provided model(s) to the schema structure.
func FromCollectionModels(schemaVersion int, tp string, model Model, models ...Model) (map[string]*Schema, error) {
	schemas := make(map[string]*Schema)

	schema, err := fromCollectionModel(schemaVersion, model, tp)
	if err != nil {
		return nil, err
	}

	schemas[schema.Name] = schema

	for _, m := range models {
		schema, err = fromCollectionModel(schemaVersion, m, tp)
		if err != nil {
			return nil, err
		}

		schemas[schema.Name] = schema
	}

	return schemas, nil
}

// FromDatabaseModel converts provided database model to collections schema structures.
func FromDatabaseModel(dbModel any) (string, map[string]*Schema, error) {
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

		// skip if tag is equal to "-"
		if strings.Trim(field.Tag.Get(tagName), " ") == "-" {
			continue
		}

		var version int

		tags, err := tokenizeTag(field.Tag.Get(tagName))
		if err != nil {
			return "", nil, err
		}

		for tag, val := range tags {
			if tag == tagVersion {
				version, err = strconv.Atoi(val)
				if err != nil {
					return "", nil, err
				}
			}
		}

		tt := field.Type
		if field.Type.Kind() == reflect.Array || field.Type.Kind() == reflect.Slice {
			tt = field.Type.Elem()
		}

		sch, err := fromCollectionModel(version, reflect.New(tt).Elem().Interface(), Documents)
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
