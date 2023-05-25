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
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTokenizeTag(t *testing.T) {
	cases := []struct {
		name     string
		input    string
		expected map[string]string
		err      error
	}{
		{
			name:  "single",
			input: "one_tag",
			expected: map[string]string{
				"one_tag": "true",
			},
		},
		{
			name:  "two_simple",
			input: "one_tag,two_tag",
			expected: map[string]string{
				"one_tag": "true",
				"two_tag": "true",
			},
		},
		{
			name:  "single_value",
			input: "one_tag:1.2",
			expected: map[string]string{
				"one_tag": "1.2",
			},
		},
		{
			name:  "two_with_value",
			input: "one_tag:1.2,two_tag:str1",
			expected: map[string]string{
				"one_tag": "1.2",
				"two_tag": "str1",
			},
		},
		{
			name:  "single_with_spaces",
			input: " one_tag ",
			expected: map[string]string{
				"one_tag": "true",
			},
		},
		{
			name:  "two_with_value_with_spaces",
			input: "  one_tag  :  1.2   ",
			expected: map[string]string{
				"one_tag": "1.2",
			},
		},
		{
			name:  "two_with_spaces",
			input: " one_tag , two_tag",
			expected: map[string]string{
				"one_tag": "true",
				"two_tag": "true",
			},
		},
		{
			name:  "two_with_value_with_spaces",
			input: " one_tag : 1.2 , two_tag:str1 ",
			expected: map[string]string{
				"one_tag": "1.2",
				"two_tag": "str1",
			},
		},
		{
			name:  "quoted_value",
			input: " one_tag:'1.2'",
			expected: map[string]string{
				"one_tag": "1.2",
			},
		},
		{
			name:  "quoted_with_special_chars",
			input: "one_tag:'one:,two',two_tag:'three=:,'",
			expected: map[string]string{
				"one_tag": "one:,two",
				"two_tag": "three=:,",
			},
		},
		{
			name:  "quoted_with_quote",
			input: "one_tag:'one\\'two'",
			expected: map[string]string{
				"one_tag": "one'two",
			},
		},
		{
			// slash put in value as is,
			// unless it followed by a quote
			name:  "quoted_with_escaped_slash",
			input: "one_tag:'one\\'t\\\\w\\o'",
			expected: map[string]string{
				"one_tag": "one't\\\\w\\o",
			},
		},
		{
			name:  "qouble_quotes_in_value",
			input: "one_tag:'one \"two\" three'",
			expected: map[string]string{
				"one_tag": "one \"two\" three",
			},
		},
		{
			name:  "invalid_key_name",
			input: "one_*tag",
			err:   ErrInvalidKeyName,
		},
		{
			name:  "no_closing_quote",
			input: "one:'two",
			err:   ErrMissingClosingQuote,
		},
		{
			name:  "invalid_char_after_value",
			input: "one:'two'asdf",
			err:   ErrInvalidCharAfterValue,
		},
		{
			name:  "invalid_key_name_1",
			input: "'",
			err:   ErrInvalidKeyName,
		},
		{
			name:  "trailing_comma",
			input: "one_tag:'one',",
			expected: map[string]string{
				"one_tag": "one",
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			res, err := tokenizeTag(c.input)
			assert.Equal(t, c.err, err)
			assert.Equal(t, c.expected, res)
		})
	}
}

func TestParseTag(t *testing.T) {
	cases := []struct {
		name string
		tag  string
		err  error
		res  bool
	}{
		{
			"invalid_required",
			"required:asdf",
			ErrInvalidBoolTagValue,
			false,
		},
		{
			"invalid_index",
			"index:asdf",
			ErrInvalidBoolTagValue,
			false,
		},
		{
			"invalid_default",
			"default:asdf",
			ErrInvalidDefaultTag,
			false,
		},
		{
			"invalid_max_length",
			"maxLength:asdf",
			ErrInvalidMaxLength,
			false,
		},
		{
			"invalid_updated_at",
			"updatedAt:asdf",
			ErrInvalidBoolTagValue,
			false,
		},
		{
			"invalid_created_at",
			"createdAt:asdf",
			ErrInvalidBoolTagValue,
			false,
		},
		{
			"invalid_key_name",
			"creat%#:asdf",
			ErrInvalidKeyName,
			false,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			field := Field{Type: NewMultiType(typeBoolean)}
			res, err := parseTag(c.name, c.tag, &field, nil)
			assert.True(t, errors.Is(err, c.err))
			assert.Equal(t, c.res, res)
		})
	}
}
