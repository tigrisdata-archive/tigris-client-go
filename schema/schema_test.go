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

package schema

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCollectionSchema(t *testing.T) {
	// invalid
	type empty struct{}

	// valid. implicit primary key
	type noPK struct {
		Key1 string `json:"key_1"`
	}

	type unknownTag struct {
		Key1 string `tigris:"some"`
	}

	type unsupportedType struct {
		Key1        string `json:"key_1" tigris:"primary_key"`
		Unsupported chan struct{}
	}

	// valid. one part primary key with implicit index 1
	type pk struct {
		Key1 string `json:"key_1" tigris:"primary_key"`
	}

	// invalid. index starts from 1
	type pk0 struct {
		Key1 string `json:"key_1" tigris:"primary_key:0"`
	}

	// valid. optional index specified
	type pk1 struct {
		Key1 string `json:"key_1" tigris:"primary_key:1"`
	}

	// valid. composite primary key
	type pk2 struct {
		Key2 string `json:"key_2" tigris:"primary_key:2"`
		Key1 string `json:"key_1" tigris:"primary_key:1"`
	}

	type pkDup struct {
		Key2 string `json:"key_2" tigris:"primary_key:1"`
		Key1 string `json:"key_1" tigris:"primary_key:1"`
	}

	// invalid. primary key index greater than number of fields
	type pkOutOfBound struct {
		Key3 string `json:"key_3" tigris:"primary_key:3"`
		Key1 string `json:"key_1" tigris:"primary_key:1"`
	}

	// invalid. gap in primary key indexes
	type pkGap struct {
		Key3  string `json:"key_3" tigris:"primary_key:3"`
		Key1  string `json:"key_1" tigris:"primary_key:1"`
		Data1 string
	}

	type pkInvalidType struct {
		Key2 bool `json:"key_2" tigris:"primary_key:1"`
	}

	type pkInvalidTag struct {
		Key2 bool `json:"key_2" tigris:"primary_key:1:a"`
	}

	type pk3 struct {
		Key2 string `json:"key_2" tigris:"primary_key:2"`
		Key1 string `json:"key_1" tigris:"primary_key:1"`
		Key3 string `json:"key_3" tigris:"primary_key:3"`
	}

	type subSubStructPK struct {
		Data1  int    `tigris:"-"`
		Field1 string `json:"ss_field_1" tigris:"primary_key:1"`
	}

	type subStructPK struct {
		Field1 string `json:"field_1"`
		Nested subSubStructPK
	}

	type subSubStruct struct {
		Data1  int    `tigris:"-"`
		Field1 string `json:"ss_field_1"`
	}

	type subStruct struct {
		Field1 string `json:"field_1"`
		Nested subSubStruct
	}

	type TestModel struct {
		TestField int `json:"Test_Field" tigris:"autoGenerate"`
	}

	type testModelSub struct {
		TestModel           // nested embedded testModel
		Tm        TestModel // named field included as is
	}

	type embModel struct {
		TestModel           // treated as a metadata model
		Field1    string    `json:"ss_field_1"`
		Tm        TestModel // name field included as is

		Sub testModelSub
	}

	type TestModel1 struct {
		TestField int `json:"ID" tigris:"primary_key:1,autoGenerate"`
	}

	// user can extend implicit primary key
	type embModelPK struct {
		TestModel1
		Field1 string `json:"ss_field_1" tigris:"primary_key:2"`
	}

	type autoGen struct {
		Field1 string    `tigris:"primaryKey,autoGenerate"`
		Field2 int       `tigris:"autoGenerate"`
		Field3 int64     `tigris:"autoGenerate"`
		Field4 uuid.UUID `tigris:"autoGenerate"`
		Field5 time.Time `tigris:"autoGenerate"`
	}

	type autoGenNeg struct {
		Field1 bool `json:"ss_field_1" tigris:"autoGenerate"`
	}

	// field with name ID is automatically considered as primary key
	type autoID struct {
		ID time.Time
	}

	type autoID1 struct {
		ID int64 `json:"id"`
	}

	// use annotated and not implied ID
	type autoIDOverride struct {
		RealID time.Time `tigris:"primaryKey,autoGenerate"`
		ID     time.Time
	}

	type autoIDBadType struct {
		ID []bool
	}

	type allTypes struct {
		Tm      time.Time
		TmPtr   *time.Time
		UUID    uuid.UUID
		UUIDPtr *uuid.UUID
		Int32   int32 `json:"int_32"`

		Int64 int64 `json:"int_64"`
		Int   int   `json:"int_1"`

		Bytes  []byte  `json:"bytes_1"`
		BytesA [4]byte `json:"bytes_2"`

		Float32 float32 `json:"float_32"`
		Float64 float64 `json:"float_64"`

		Bool bool `json:"bool_1"`

		String string `json:"string_1" tigris:"primary_key:1"`

		Data1  subStructPK          `json:"data_1"`
		Slice1 []string             `json:"slice_1"`
		Arr1   [3]string            `json:"arr_1"`
		Map    map[string]string    `json:"map_1"`
		Slice2 []subStruct          `json:"slice_2"`
		Map2   map[string]subStruct `json:"map_2"`
		Data2  subStructPK          `json:"data_2" tigris:"-"` // should be skipped

		Bool123 bool `json:"bool_123"`

		DataSkipped int `json:"-"`

		PtrStruct *subSubStruct
		//		DataEnc   int64 `tigris:"encrypted"`
		//		DataPII   int64 `tigris:"pii"`

		// unexported fields should not be in the schema
		//nolint:unused
		skipUnexported int
	}

	cases := []struct {
		input  interface{}
		output *Schema
		err    error
	}{
		{empty{}, nil, fmt.Errorf("no data fields in the collection schema")},
		{unknownTag{}, nil, fmt.Errorf("%w: %s", ErrUnknownTag, "some")},
		{unsupportedType{}, nil, fmt.Errorf("unsupported type: name='' kind='chan'")},
		{pk0{}, nil, fmt.Errorf("primary key index starts from 1")},
		{pkOutOfBound{}, nil, fmt.Errorf("maximum primary key index is 2")},
		{pkGap{}, nil, fmt.Errorf("gap in the primary key index")},
		{pkInvalidType{}, nil, fmt.Errorf("type is not supported for the key: bool")},
		{pkInvalidTag{}, nil, fmt.Errorf("%w: %s", ErrPrimaryKeyIdx, "strconv.Atoi: parsing \"1:a\": invalid syntax")},
		{input: noPK{}, output: &Schema{Name: "no_pks", Fields: map[string]*Field{
			"ID":    {Type: typeString, Format: formatUUID, AutoGenerate: true},
			"key_1": {Type: typeString},
		}, PrimaryKey: []string{"ID"}}},
		{input: pk{}, output: &Schema{Name: "pks", Fields: map[string]*Field{
			"key_1": {Type: typeString},
		}, PrimaryKey: []string{"key_1"}}},
		{pk1{}, &Schema{Name: "pk_1", Fields: map[string]*Field{
			"key_1": {Type: typeString},
		}, PrimaryKey: []string{"key_1"}}, nil},
		{pk2{}, &Schema{
			Name: "pk_2", Fields: map[string]*Field{
				"key_2": {Type: typeString}, "key_1": {Type: typeString},
			},
			PrimaryKey: []string{"key_1", "key_2"},
		}, nil},
		{pk3{}, &Schema{
			Name: "pk_3", Fields: map[string]*Field{
				"key_2": {Type: typeString}, "key_1": {Type: typeString}, "key_3": {Type: typeString},
			},
			PrimaryKey: []string{"key_1", "key_2", "key_3"},
		}, nil},
		{allTypes{}, &Schema{Name: "all_types", Fields: map[string]*Field{
			"Tm":      {Type: typeString, Format: formatDateTime},
			"TmPtr":   {Type: typeString, Format: formatDateTime},
			"UUID":    {Type: typeString, Format: formatUUID},
			"UUIDPtr": {Type: typeString, Format: formatUUID},

			"int_32": {Type: typeInteger, Format: formatInt32},

			"int_64": {Type: typeInteger},
			//			{Name: "uint_64", Type: typeInteger},
			"int_1": {Type: typeInteger},

			"bytes_1": {Type: typeString, Format: formatByte},
			"bytes_2": {Type: typeString, Format: formatByte},

			"float_32": {Type: typeNumber},
			"float_64": {Type: typeNumber},

			"bool_1": {Type: typeBoolean},

			"string_1": {Type: typeString},

			"data_1": {
				Type: "object",
				Fields: map[string]*Field{
					"field_1": {Type: typeString},
					"Nested": {
						Type: "object",
						Fields: map[string]*Field{
							"ss_field_1": {Type: typeString},
						},
					},
				},
			},

			"slice_1": {Type: typeArray, Items: &Field{Type: typeString}},
			"arr_1":   {Type: typeArray, Items: &Field{Type: typeString}},
			"map_1":   {Type: typeObject},

			"slice_2": {
				Type: typeArray,
				Items: &Field{
					Type: "object",
					Fields: map[string]*Field{
						"field_1": {Type: typeString},
						"Nested": {
							Type: "object",
							Fields: map[string]*Field{
								"ss_field_1": {Type: typeString},
							},
						},
					},
				},
			},
			"map_2": {Type: typeObject},

			// use original name if JSON tag name is not defined
			"bool_123": {Type: typeBoolean},
			"PtrStruct": {Type: typeObject, Fields: map[string]*Field{
				"ss_field_1": {
					Type: "string",
				},
			}},
			//	{Name: "DataEnc", Type: typeInteger, Tags: []string{"encrypted"}},
			//	{Name: "DataPII", Type: typeInteger, Tags: []string{"pii"}},
		}, PrimaryKey: []string{"string_1"}}, nil},
		{embModel{}, &Schema{
			Name: "emb_models", Fields: map[string]*Field{
				"ID": {Type: typeString, Format: formatUUID, AutoGenerate: true},
				"Sub": {
					Type: "object",
					Fields: map[string]*Field{
						"Test_Field": {Type: typeInteger, AutoGenerate: true},
						"Tm": {
							Type: "object",
							Fields: map[string]*Field{
								"Test_Field": {Type: typeInteger, AutoGenerate: true},
							},
						},
					},
				},
				"Test_Field": {Type: typeInteger, AutoGenerate: true},
				"ss_field_1": {Type: typeString},
				"Tm": {
					Type: "object",
					Fields: map[string]*Field{
						"Test_Field": {Type: typeInteger, AutoGenerate: true},
					},
				},
			},
			PrimaryKey: []string{"ID"},
		}, nil},
		{embModelPK{}, &Schema{
			Name: "emb_model_pks", Fields: map[string]*Field{
				"ID":         {Type: typeInteger, AutoGenerate: true},
				"ss_field_1": {Type: typeString},
			},
			PrimaryKey: []string{"ID", "ss_field_1"},
		}, nil},
		{autoGen{}, &Schema{
			Name: "auto_gens", Fields: map[string]*Field{
				"Field1": {Type: typeString, AutoGenerate: true},
				"Field2": {Type: typeInteger, AutoGenerate: true},
				"Field3": {Type: typeInteger, AutoGenerate: true},
				"Field4": {Type: typeString, Format: formatUUID, AutoGenerate: true},
				"Field5": {Type: typeString, Format: formatDateTime, AutoGenerate: true},
			},
			PrimaryKey: []string{"Field1"},
		}, nil},
		{autoGenNeg{}, nil, fmt.Errorf("type cannot be autogenerated: bool")},
		{autoID{}, &Schema{
			Name: "auto_ids", Fields: map[string]*Field{
				"ID": {Type: typeString, Format: formatDateTime, AutoGenerate: true},
			},
			PrimaryKey: []string{"ID"},
		}, nil},
		{autoID1{}, &Schema{
			Name: "auto_id_1", Fields: map[string]*Field{
				"id": {Type: typeInteger, AutoGenerate: true},
			},
			PrimaryKey: []string{"id"},
		}, nil},
		{autoIDOverride{}, &Schema{
			Name: "auto_id_overrides", Fields: map[string]*Field{
				"RealID": {Type: typeString, Format: formatDateTime, AutoGenerate: true},
				"ID":     {Type: typeString, Format: formatDateTime},
			},
			PrimaryKey: []string{"RealID"},
		}, nil},
		{autoIDBadType{}, nil, fmt.Errorf("type is not supported for the key: array")},
	}

	for _, c := range cases {
		t.Run(reflect.TypeOf(c.input).Name(), func(t *testing.T) {
			schema, err := fromCollectionModel(c.input, Documents)
			assert.Equal(t, c.err, err)
			if schema != nil {
				assert.Equal(t, Documents, schema.CollectionType)
				c.output.CollectionType = Documents
			}
			assert.Equal(t, c.output, schema)
		})
	}

	t.Run("build", func(t *testing.T) {
		s, err := fromCollectionModel(allTypes{}, Documents)
		require.NoError(t, err)

		assert.Equal(t, Documents, s.CollectionType)
		s.CollectionType = ""

		b, err := s.Build()
		require.NoError(t, err)

		require.Equal(t, `{"title":"all_types","properties":{"PtrStruct":{"type":"object","properties":{"ss_field_1":{"type":"string"}}},"Tm":{"type":"string","format":"date-time"},"TmPtr":{"type":"string","format":"date-time"},"UUID":{"type":"string","format":"uuid"},"UUIDPtr":{"type":"string","format":"uuid"},"arr_1":{"type":"array","items":{"type":"string"}},"bool_1":{"type":"boolean"},"bool_123":{"type":"boolean"},"bytes_1":{"type":"string","format":"byte"},"bytes_2":{"type":"string","format":"byte"},"data_1":{"type":"object","properties":{"Nested":{"type":"object","properties":{"ss_field_1":{"type":"string"}}},"field_1":{"type":"string"}}},"float_32":{"type":"number"},"float_64":{"type":"number"},"int_1":{"type":"integer"},"int_32":{"type":"integer","format":"int32"},"int_64":{"type":"integer"},"map_1":{"type":"object"},"map_2":{"type":"object"},"slice_1":{"type":"array","items":{"type":"string"}},"slice_2":{"type":"array","items":{"type":"object","properties":{"Nested":{"type":"object","properties":{"ss_field_1":{"type":"string"}}},"field_1":{"type":"string"}}}},"string_1":{"type":"string"}},"primary_key":["string_1"]}`, string(b))
	})

	t.Run("multiple_models", func(t *testing.T) {
		s, err := FromCollectionModels(Documents, pk{}, pk1{})
		require.NoError(t, err)

		assert.Equal(t, map[string]*Schema{
			"pks":  {Name: "pks", Fields: map[string]*Field{"key_1": {Type: typeString}}, PrimaryKey: []string{"key_1"}, CollectionType: Documents},
			"pk_1": {Name: "pk_1", Fields: map[string]*Field{"key_1": {Type: typeString}}, PrimaryKey: []string{"key_1"}, CollectionType: Documents},
		}, s)
	})

	t.Run("duplicate_pk_index", func(t *testing.T) {
		_, err := FromCollectionModels(Documents, pkDup{})
		if err.Error() == "duplicate primary key index 1 set for key_1 and key_2" {
			require.Equal(t, err, fmt.Errorf("duplicate primary key index 1 set for key_1 and key_2"))
		} else {
			require.Equal(t, err, fmt.Errorf("duplicate primary key index 1 set for key_2 and key_1"))
		}
	})
}

func TestDefaults(t *testing.T) {
	type struct1 struct {
		Field1 string

		FieldNestedIndexAndReq int `json:"field_nested_index_and_req" tigris:"required,index"`
		FieldNestedIndex2      int `json:"field_nested_index2" tigris:"index"`
		FieldNestedRequired    int `json:"field_nested_required" tigris:"required"`
		FieldNestedRequired2   int `json:"field_nested_required2" tigris:"required"`
	}

	type TestDefaults struct {
		FieldIndexAndReq  int      `json:"field_index_and_req" tigris:"required,index"`
		FieldIndex2       int      `json:"field_index2" tigris:"index"`
		FieldRequired     int      `json:"field_required" tigris:"required"`
		FieldRequired2    int      `json:"field_required2" tigris:"required"`
		FieldMaxLength    string   `json:"field_max_length" tigris:"maxLength:123"`
		FieldDefaultBool  bool     `json:"def_bool" tigris:"default:true"`
		FieldDefaultInt   int      `json:"def_int" tigris:"default:789"`
		FieldDefaultFloat float64  `json:"def_float" tigris:"default:456.34"`
		FieldDefaultStr   string   `json:"def_str" tigris:"default:str1"`
		FieldDefaultStr1  string   `json:"def_str1" tigris:"default:'st\\'r2'"`
		FieldDefaultArr   []string `json:"def_arr_str" tigris:"default:'[\"one\", \"two\"]'"`
		FieldDefaultObj   struct1  `json:"def_obj_str" tigris:"default:'{\"Field1\":\"aaa\"}'"`

		FieldDefaultTime time.Time `json:"def_time" tigris:"default:now(),updatedAt"`
		FieldDefaultUUID uuid.UUID `json:"def_uuid" tigris:"default:uuid()"`
	}

	schema, err := fromCollectionModel(TestDefaults{}, Documents)
	assert.Equal(t, nil, err)

	b, err := schema.Build()
	require.NoError(t, err)
	exp := `{
	"title":"test_defaults",
	"properties":{
		"ID":{"type":"string","format":"uuid","autoGenerate":true},
		"def_bool":{"type":"boolean","default":true},
		"def_int":{"type":"integer","default":789},
		"def_float":{"type":"number","default":456.34},
		"def_str":{"type":"string","default":"str1"},
		"def_str1":{"type":"string","default":"st'r2"},
		"def_arr_str":{"type":"array","default":["one", "two"], "items": {"type":"string"}},
		"def_obj_str":{
			"type":"object",
			"default":{"Field1":"aaa"},
			"properties": {
				"Field1" : { "type":"string" },
				"field_nested_required":{"type":"integer"},
				"field_nested_required2":{"type":"integer"},
				"field_nested_index2":{"type":"integer", "index": true},
				"field_nested_index_and_req":{"type":"integer", "index": true}
			},
			"required":["field_nested_index_and_req","field_nested_required","field_nested_required2"]
		},
		"def_time":{"type":"string","format":"date-time","default":"now()","updatedAt":true},
		"def_uuid":{"type":"string","format":"uuid","default":"uuid()"},
		"field_max_length":{"type":"string","maxLength":123},
		"field_required":{"type":"integer"},
		"field_required2":{"type":"integer"},
		"field_index2":{"type":"integer", "index": true},
		"field_index_and_req":{"type":"integer", "index": true}
	},
	"primary_key":["ID"],
	"required":["field_index_and_req","field_required","field_required2"],
	"collection_type":"documents"
}`

	require.JSONEq(t, exp, string(b))
}

func TestDefaultsNegative(t *testing.T) {
	cases := []struct {
		name  string
		tp    string
		input string
		err   error
	}{
		{"int", "integer", "vbn", fmt.Errorf("%w: %s", ErrInvalidDefaultTag, "strconv.ParseInt: parsing \"vbn\": invalid syntax")},
		{"float", "number", "vbn", fmt.Errorf("%w: %s", ErrInvalidDefaultTag, "strconv.ParseFloat: parsing \"vbn\": invalid syntax")},
		{"bool", "boolean", "vbn", fmt.Errorf("%w: %s", ErrInvalidDefaultTag, "invalid bool value: vbn")},
		{"array", "array", "vbn", fmt.Errorf("%w: %s", ErrInvalidDefaultTag, "invalid character 'v' looking for beginning of value")},
		{"object", "object", "vbn", fmt.Errorf("%w: %s", ErrInvalidDefaultTag, "invalid character 'v' looking for beginning of value")},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := parseDefaultTag(&Field{Type: c.tp}, c.input)
			assert.Equal(t, c.err, err)
		})
	}
}

func TestDatabaseSchema(t *testing.T) {
	type Coll1 struct {
		Key1 int64 `tigris:"primary_key"`
	}

	type Coll2 struct {
		Key2 int64 `tigris:"primary_key"`
	}

	type DB1 struct {
		c1 Coll1
		c2 *Coll2
		c3 []Coll2
		C4 []*Coll2 `json:"coll_4"`
	}

	_ = DB1{c1: Coll1{}, c2: &Coll2{}, c3: []Coll2{}, C4: []*Coll2{}}

	type DB3 struct {
		Coll1
		Coll2 `tigris:"-"`
	}

	type DB4 struct {
		int64
	}

	_ = DB4{1}

	coll1 := Schema{Name: "Coll1", Fields: map[string]*Field{"Key1": {Type: "integer"}}, PrimaryKey: []string{"Key1"}, CollectionType: Documents}
	c1 := Schema{Name: "c1", Fields: map[string]*Field{"Key1": {Type: "integer"}}, PrimaryKey: []string{"Key1"}, CollectionType: Documents}
	c2 := Schema{Name: "c2", Fields: map[string]*Field{"Key2": {Type: "integer"}}, PrimaryKey: []string{"Key2"}, CollectionType: Documents}
	c3 := Schema{Name: "c3", Fields: map[string]*Field{"Key2": {Type: "integer"}}, PrimaryKey: []string{"Key2"}, CollectionType: Documents}
	c4 := Schema{Name: "coll_4", Fields: map[string]*Field{"Key2": {Type: "integer"}}, PrimaryKey: []string{"Key2"}, CollectionType: Documents}

	var i int64

	cases := []struct {
		input  interface{}
		name   string
		output map[string]*Schema
		err    error
	}{
		{DB1{}, "DB1", map[string]*Schema{"c1": &c1, "c2": &c2, "c3": &c3, "coll_4": &c4}, nil},
		{&DB3{}, "DB3", map[string]*Schema{"Coll1": &coll1}, nil},
		{DB4{}, "", nil, fmt.Errorf("model should be of struct type, not int64")},
		{i, "", nil, fmt.Errorf("database model should be of struct type containing collection models types as fields")},
	}
	for _, c := range cases {
		t.Run(reflect.TypeOf(c.input).Name(), func(t *testing.T) {
			name, schema, err := FromDatabaseModel(c.input)
			assert.Equal(t, c.name, name)
			assert.Equal(t, c.err, err)
			assert.Equal(t, c.output, schema)
		})
	}
}
