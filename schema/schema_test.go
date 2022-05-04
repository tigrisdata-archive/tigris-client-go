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

package schema

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCollectionSchema(t *testing.T) {
	// invalid
	type empty struct {
	}

	// invalid
	type noPK struct {
		Key1 string `json:"key_1"`
	}

	type unknownTag struct {
		Key1 string `tigris:"some"`
	}

	// valid. implicit one part primary key with index 1
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

	// invalid. promary key index greater then number of fields
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

	type allTypes struct {
		tm    time.Time
		tmPtr *time.Time
		Int32 int32 `json:"int_32"`

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
		Data2  subStructPK          `json:"data_2" tigris:"primary_key:3,-"` // should be skipped

		Bool123 bool `json:"bool_123"`

		DataSkipped int `json:"-"`

		ptrStruct *subSubStruct
		//		DataEnc   int64 `tigris:"encrypted"`
		//		DataPII   int64 `tigris:"pii"`
	}

	// Fix linting errors
	_ = allTypes{ptrStruct: &subSubStruct{}, tm: time.Time{}, tmPtr: &time.Time{}}

	cases := []struct {
		input  interface{}
		output *Schema
		err    error
	}{
		{empty{}, nil, fmt.Errorf("no primary key defined in schema")},
		{noPK{}, nil, fmt.Errorf("no primary key defined in schema")},
		{unknownTag{}, nil, fmt.Errorf("unknown tigris tag: some")},
		{pk0{}, nil, fmt.Errorf("primary key index starts from 1")},
		{pkOutOfBound{}, nil, fmt.Errorf("maximum primary key index is 2")},
		{pkGap{}, nil, fmt.Errorf("gap in the primary key index")},
		{pkInvalidType{}, nil, fmt.Errorf("type is not supported for the key: bool")},
		{pkInvalidTag{}, nil, fmt.Errorf("only one colon allowed in the tag")},
		{input: pk{}, output: &Schema{Name: "pks", Fields: map[string]Field{
			"key_1": {Type: typeString}}, PrimaryKey: []string{"key_1"}}},
		{pk1{}, &Schema{Name: "pk_1", Fields: map[string]Field{
			"key_1": {Type: typeString}}, PrimaryKey: []string{"key_1"}}, nil},
		{pk2{}, &Schema{Name: "pk_2", Fields: map[string]Field{
			"key_2": {Type: typeString}, "key_1": {Type: typeString}},
			PrimaryKey: []string{"key_1", "key_2"}}, nil},
		{pk3{}, &Schema{Name: "pk_3", Fields: map[string]Field{
			"key_2": {Type: typeString}, "key_1": {Type: typeString}, "key_3": {Type: typeString}},
			PrimaryKey: []string{"key_1", "key_2", "key_3"}}, nil},
		{allTypes{}, &Schema{Name: "all_types", Fields: map[string]Field{
			"tm":    {Type: typeString, Format: formatDateTime},
			"tmPtr": {Type: typeString, Format: formatDateTime},

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
				Fields: map[string]Field{
					"field_1": {Type: typeString},
					"Nested": {
						Type: "object",
						Fields: map[string]Field{
							"ss_field_1": {Type: typeString},
						},
					},
				},
			},

			"slice_1": {Type: typeArray, Items: &Field{Type: typeString}},
			"arr_1":   {Type: typeArray, Items: &Field{Type: typeString}},
			"map_1":   {Type: typeObject},

			"slice_2": {Type: typeArray,
				Items: &Field{
					Type: "object",
					Fields: map[string]Field{
						"field_1": {Type: typeString},
						"Nested": {
							Type: "object",
							Fields: map[string]Field{
								"ss_field_1": {Type: typeString},
							},
						},
					},
				},
			},
			"map_2": {Type: typeObject},

			// use original name if JSON tag name is not defined
			"bool_123": {Type: typeBoolean},
			"ptrStruct": {Type: typeObject, Fields: map[string]Field{
				"ss_field_1": {
					Type: "string",
				}}},
			//	{Name: "DataEnc", Type: typeInteger, Tags: []string{"encrypted"}},
			//	{Name: "DataPII", Type: typeInteger, Tags: []string{"pii"}},
		}, PrimaryKey: []string{"string_1"}}, nil},
	}

	for _, c := range cases {
		t.Run(reflect.TypeOf(c.input).Name(), func(t *testing.T) {
			schema, err := FromCollectionModel(c.input)
			assert.Equal(t, c.err, err)
			assert.Equal(t, c.output, schema)
		})
	}

	t.Run("build", func(t *testing.T) {
		s, err := FromCollectionModel(allTypes{})
		require.NoError(t, err)

		b, err := s.Build()
		require.NoError(t, err)

		require.Equal(t, `{"title":"all_types","properties":{"arr_1":{"type":"array","items":{"type":"string"}},"bool_1":{"type":"boolean"},"bool_123":{"type":"boolean"},"bytes_1":{"type":"string","format":"byte"},"bytes_2":{"type":"string","format":"byte"},"data_1":{"type":"object","properties":{"Nested":{"type":"object","properties":{"ss_field_1":{"type":"string"}}},"field_1":{"type":"string"}}},"float_32":{"type":"number"},"float_64":{"type":"number"},"int_1":{"type":"integer"},"int_32":{"type":"integer","format":"int32"},"int_64":{"type":"integer"},"map_1":{"type":"object"},"map_2":{"type":"object"},"ptrStruct":{"type":"object","properties":{"ss_field_1":{"type":"string"}}},"slice_1":{"type":"array","items":{"type":"string"}},"slice_2":{"type":"array","items":{"type":"object","properties":{"Nested":{"type":"object","properties":{"ss_field_1":{"type":"string"}}},"field_1":{"type":"string"}}}},"string_1":{"type":"string"},"tm":{"type":"string","format":"date-time"},"tmPtr":{"type":"string","format":"date-time"}},"primary_key":["string_1"]}`, string(b))
	})

	t.Run("multiple_models", func(t *testing.T) {
		s, err := FromCollectionModels(pk{}, pk1{})
		require.NoError(t, err)

		assert.Equal(t, map[string]*Schema{"pks": {Name: "pks", Fields: map[string]Field{"key_1": {Type: typeString}}, PrimaryKey: []string{"key_1"}},
			"pk_1": {Name: "pk_1", Fields: map[string]Field{"key_1": {Type: typeString}}, PrimaryKey: []string{"key_1"}}}, s)
	})

	t.Run("duplicate_pk_index", func(t *testing.T) {
		_, err := FromCollectionModels(pkDup{})
		if err.Error() == "duplicate primary key index 1 set for key_1 and key_2" {
			require.Equal(t, err, fmt.Errorf("duplicate primary key index 1 set for key_1 and key_2"))
		} else {
			require.Equal(t, err, fmt.Errorf("duplicate primary key index 1 set for key_2 and key_1"))
		}
	})
}

func TestDatabaseSchema(t *testing.T) {
	type Coll1 struct {
		Key1 int64 `tigris:"primary_key"`
	}

	type Coll2 struct {
		Key2 int64 `tigris:"primary_key"`
	}

	type Db1 struct {
		c1 Coll1
		c2 *Coll2
		c3 []Coll2
		C4 []*Coll2 `json:"coll_4"`
	}

	_ = Db1{c1: Coll1{}, c2: &Coll2{}, c3: []Coll2{}, C4: []*Coll2{}}

	type Db3 struct {
		Coll1
		Coll2 `tigris:"-"`
	}

	type Db4 struct {
		int64
	}

	_ = Db4{1}

	coll1 := Schema{Name: "Coll1", Fields: map[string]Field{"Key1": {Type: "integer"}}, PrimaryKey: []string{"Key1"}}
	c1 := Schema{Name: "c1", Fields: map[string]Field{"Key1": {Type: "integer"}}, PrimaryKey: []string{"Key1"}}
	c2 := Schema{Name: "c2", Fields: map[string]Field{"Key2": {Type: "integer"}}, PrimaryKey: []string{"Key2"}}
	c3 := Schema{Name: "c3", Fields: map[string]Field{"Key2": {Type: "integer"}}, PrimaryKey: []string{"Key2"}}
	c4 := Schema{Name: "coll_4", Fields: map[string]Field{"Key2": {Type: "integer"}}, PrimaryKey: []string{"Key2"}}

	var i int64

	cases := []struct {
		input  interface{}
		name   string
		output map[string]*Schema
		err    error
	}{
		{Db1{}, "Db1", map[string]*Schema{"c1": &c1, "c2": &c2, "c3": &c3, "coll_4": &c4}, nil},
		{&Db3{}, "Db3", map[string]*Schema{"Coll1": &coll1}, nil},
		{Db4{}, "", nil, fmt.Errorf("model should be of struct type, not int64")},
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
