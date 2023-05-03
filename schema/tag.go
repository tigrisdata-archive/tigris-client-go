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
	"strconv"
	"strings"

	"github.com/iancoleman/strcase"
	jsoniter "github.com/json-iterator/go"
)

var (
	ErrInvalidKeyName        = fmt.Errorf("ivalid tag key. only a-zA-z_ allowed")
	ErrInvalidCharAfterValue = fmt.Errorf("unexpected character after value")
	ErrMissingClosingQuote   = fmt.Errorf("value missing closing quote")

	ErrUnknownTag                 = fmt.Errorf("unknown tigris tag")
	ErrPrimaryKeyIdx              = fmt.Errorf("invalid primary key index")
	ErrPrimaryKeyIdxStartsFromOne = fmt.Errorf("primary key index starts from 1")

	ErrInvalidMaxLength  = fmt.Errorf("invalid maxLength value")
	ErrInvalidDefaultTag = fmt.Errorf("invalid default value")

	ErrInvalidBoolTagValue = fmt.Errorf("boolean tag allows true/false value only")

	keyValueSeparator = ':'
	tagsSeparator     = ','
)

func tokenizeTag(tag string) (map[string]string, error) {
	m := make(map[string]string)

	const (
		spaceBeforeKey = iota
		stateKey
		spaceAfterKey
		spaceBeforeValue
		value
		quotedValue
		spaceAfterValue
	)

	state := spaceBeforeKey

	var (
		escaped  bool
		key, val string
		quote    int32
	)

	for i := 0; i < len(tag); {
		s := int32(tag[i])

		switch state {
		case spaceBeforeKey:
			if s != ' ' {
				state = stateKey
				continue
			}
		case stateKey:
			switch {
			case s == ' ' || s == keyValueSeparator || s == tagsSeparator:
				state = spaceAfterKey
				continue
			case (s < 'a' || s > 'z') && (s < 'A' || s > 'Z') && s != '_':
				return nil, ErrInvalidKeyName
			default:
				key += string(s)
			}
		case spaceAfterKey:
			if s != ' ' {
				if s == tagsSeparator {
					m[key] = "true"
					key = ""
					state = spaceBeforeKey
				} else if s == keyValueSeparator {
					state = spaceBeforeValue
				}
			}
		case spaceBeforeValue:
			if s != ' ' {
				state = value
				continue
			}
		case value:
			switch {
			case len(val) == 0 && (s == '\'' || s == '"'):
				quote = s
				state = quotedValue
			case s == tagsSeparator || s == ' ':
				state = spaceAfterValue
				continue
			default:
				val += string(s)
			}
		case quotedValue:
			switch {
			case s == '\\':
				if escaped {
					val += string('\\')
					val += string('\\')
				}
				escaped = !escaped
			case s == quote:
				if !escaped {
					state = spaceAfterValue
				} else {
					val += string(s)
					escaped = !escaped
				}
			default:
				if escaped {
					escaped = false
					val += string('\\')
				}
				val += string(s)
			}
		case spaceAfterValue:
			if s != ' ' {
				if s != tagsSeparator {
					return nil, ErrInvalidCharAfterValue
				}

				m[key] = val
				key, val = "", ""
				state = spaceBeforeKey
			}
		}

		i++
	}

	if len(key) > 0 {
		if state == quotedValue {
			return nil, ErrMissingClosingQuote
		}

		if len(val) == 0 {
			val = "true"
		}

		m[key] = val
	}

	return m, nil
}

func tagError(err error, orig string) error {
	return fmt.Errorf("%w: %s", err, orig)
}

func parseDefaultTag(f *Field, val string) error {
	// We only parse base JSON types and let server validate actual Tigris default values
	switch f.Type.First() {
	case typeString:
		f.Default = val
	case typeObject:
		var o map[string]any

		if err := jsoniter.Unmarshal([]byte(val), &o); err != nil {
			return tagError(ErrInvalidDefaultTag, err.Error())
		}

		f.Default = o
	case typeArray:
		var a []any
		if err := jsoniter.Unmarshal([]byte(val), &a); err != nil {
			return tagError(ErrInvalidDefaultTag, err.Error())
		}

		f.Default = a
	case typeBoolean:
		if val != "true" && val != "false" {
			return fmt.Errorf("%w: %s: %s", ErrInvalidDefaultTag, "invalid bool value", val)
		}

		f.Default = val == "true"
	case typeNumber:
		i, err := strconv.ParseFloat(val, 64)
		if err != nil {
			return tagError(ErrInvalidDefaultTag, err.Error())
		}

		f.Default = i
	case typeInteger:
		i, err := strconv.ParseInt(val, 10, 64)
		if err != nil {
			return tagError(ErrInvalidDefaultTag, err.Error())
		}

		f.Default = i
	}

	return nil
}

// parseTag parses "tigris" tag and returns recognised tags.
// It also returns encountered "primary_key" tagged field in "pk" map,
// which maps field name to primary key index part.
func parseTag(name string, tag string, field *Field, pk map[string]int) (bool, error) {
	if tag == "" {
		return false, nil
	}

	if strings.Trim(tag, " ") == tagSkip {
		return true, nil
	}

	tags, err := tokenizeTag(tag)
	if err != nil {
		return false, err
	}

	for tag, val := range tags {
		switch tag {
		case tagPrimaryKey, strcase.ToSnake(tagPrimaryKey):
			if pk == nil {
				return false, nil
			}

			// The tag is expected to be in the form:
			//
			//	primary_key:{index}
			//
			// where {index} is primary key index part order in
			// the composite key. Index starts from 1.

			i := 1
			if val != "true" {
				i, err = strconv.Atoi(val)
				if err != nil {
					return false, tagError(ErrPrimaryKeyIdx, err.Error())
				}

				if i == 0 {
					return false, ErrPrimaryKeyIdxStartsFromOne
				}
			}

			pk[name] = i
		case tagRequired:
			if val != "true" {
				return false, fmt.Errorf("%w: required field", ErrInvalidBoolTagValue)
			}

			field.RequiredTag = true
		case tagIndex:
			if val != "true" {
				return false, fmt.Errorf("%w: index field", ErrInvalidBoolTagValue)
			}

			field.Index = true
		case tagSearchIndex:
			if val != "true" {
				return false, fmt.Errorf("%w: search index field", ErrInvalidBoolTagValue)
			}

			field.SearchIndex = true
		case tagSort:
			if val != "true" {
				return false, fmt.Errorf("%w: search index sort field", ErrInvalidBoolTagValue)
			}

			field.Sort = true
		case tagFacet:
			if val != "true" {
				return false, fmt.Errorf("%w: search index facet field", ErrInvalidBoolTagValue)
			}

			field.Facet = true
		case tagDefault:
			if err = parseDefaultTag(field, val); err != nil {
				return false, tagError(ErrInvalidDefaultTag, err.Error())
			}
		case tagMaxLength, strcase.ToSnake(tagMaxLength):
			i, err := strconv.Atoi(val)
			if err != nil {
				return false, tagError(ErrInvalidMaxLength, err.Error())
			}

			field.MaxLength = i
		case tagAutoGenerate, strcase.ToSnake(tagAutoGenerate):
			field.AutoGenerate = true
		case tagUpdatedAt, strcase.ToSnake(tagUpdatedAt):
			if val != "true" {
				return false, fmt.Errorf("%w: updatedAt field", ErrInvalidBoolTagValue)
			}

			field.UpdatedAt = true
		case tagCreatedAt, strcase.ToSnake(tagCreatedAt):
			if val != "true" {
				return false, fmt.Errorf("%w: createdAt field", ErrInvalidBoolTagValue)
			}

			field.CreatedAt = true
		case tagVector, strcase.ToSnake(tagVector):
			if val != "true" {
				return false, fmt.Errorf("%w: vector field", ErrInvalidBoolTagValue)
			}

			if field.Type.First() != typeArray {
				return false, fmt.Errorf("only array of type float64 can be annotated with vector tag")
			}

			field.Dimensions = field.MaxItems
			field.MaxItems = 0

			field.Format = formatVector
		default:
			return false, fmt.Errorf("%w: %s", ErrUnknownTag, tag)
		}
	}

	return false, nil
}
