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

package driver

import (
	"fmt"
	"testing"
	"unsafe"

	"github.com/golang/mock/gomock"
	jsoniter "github.com/json-iterator/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// This is shared utility function for tests

type JSONMatcher struct {
	T        *testing.T
	Expected []byte
}

func (matcher *JSONMatcher) Matches(actual any) bool {
	var s string
	switch t := actual.(type) {
	case Schema:
		s = string(t)
	case Projection:
		s = string(t)
	}

	return assert.JSONEq(matcher.T, string(matcher.Expected), s)
}

func (matcher *JSONMatcher) String() string {
	return fmt.Sprintf("JSONMatcher: %v", string(matcher.Expected))
}

func (*JSONMatcher) Got(actual any) string {
	ptr := unsafe.Pointer(&actual)
	return fmt.Sprintf("JSONMatcher: %v", string(*(*[]byte)(ptr)))
}

func JM(t *testing.T, expected string) gomock.Matcher {
	j := &JSONMatcher{T: t, Expected: []byte(expected)}
	return gomock.GotFormatterAdapter(j, j)
}

type JSONArrMatcher struct {
	T        *testing.T
	Expected []string
}

func (matcher *JSONArrMatcher) Matches(actual any) bool {
	act, err := jsoniter.Marshal(actual)
	require.NoError(matcher.T, err)
	exp, err := jsoniter.Marshal(actual)
	require.NoError(matcher.T, err)

	assert.JSONEq(matcher.T, string(exp), string(act))

	return true
}

func (matcher *JSONArrMatcher) String() string {
	return fmt.Sprintf("JSONMatcher: %+v", matcher.Expected)
}

func (*JSONArrMatcher) Got(actual any) string {
	return fmt.Sprintf("JSONArrMatcher: %+v", actual)
}

// JAM = JSON Array Matcher.
func JAM(t *testing.T, expected []string) gomock.Matcher {
	j := &JSONArrMatcher{T: t, Expected: expected}
	return gomock.GotFormatterAdapter(j, j)
}

func ToDocument(t *testing.T, doc any) Document {
	b, err := jsoniter.Marshal(doc)
	require.NoError(t, err)

	return b
}

type ProtoMatcher struct {
	Message proto.Message
}

func (matcher *ProtoMatcher) Matches(actual any) bool {
	message, ok := actual.(proto.Message)
	if !ok {
		return false
	}

	return proto.Equal(message, matcher.Message)
}

func (matcher *ProtoMatcher) String() string {
	return fmt.Sprintf("ProtoMatcher: %v", matcher.Message)
}

func PM(m proto.Message) gomock.Matcher {
	return &ProtoMatcher{Message: m}
}
