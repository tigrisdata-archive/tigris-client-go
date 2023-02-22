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
	"encoding/json"
	"fmt"
	"testing"
	"unsafe"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// This is shared utility function for tests

type JSONMatcher struct {
	T        *testing.T
	Expected []byte
}

func (matcher *JSONMatcher) Matches(actual interface{}) bool {
	return assert.JSONEq(matcher.T, string(matcher.Expected), string(actual.(Schema)))
}

func (matcher *JSONMatcher) String() string {
	return fmt.Sprintf("JSONMatcher: %v", string(matcher.Expected))
}

func (matcher *JSONMatcher) Got(actual interface{}) string {
	ptr := unsafe.Pointer(&actual)
	return fmt.Sprintf("JSONMatcher: %v", string(*(*[]byte)(ptr)))
}

func JM(t *testing.T, expected string) gomock.Matcher {
	j := &JSONMatcher{T: t, Expected: []byte(expected)}
	return gomock.GotFormatterAdapter(j, j)
}

func ToDocument(t *testing.T, doc interface{}) Document {
	b, err := json.Marshal(doc)
	require.NoError(t, err)

	return b
}

type ProtoMatcher struct {
	Message proto.Message
}

func (matcher *ProtoMatcher) Matches(actual interface{}) bool {
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
