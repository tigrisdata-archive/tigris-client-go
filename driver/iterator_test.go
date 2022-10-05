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

package driver

import (
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockStreamReader struct {
	docs        []Document
	err         error
	cur         int
	closeCalled int
}

func (m *mockStreamReader) read() (Document, error) {
	if m.err != nil {
		return nil, m.err
	}

	if m.cur >= len(m.docs) {
		return nil, io.EOF
	}

	d := m.docs[m.cur]
	m.cur++

	return d, nil
}

func (m *mockStreamReader) close() error {
	m.closeCalled++

	return nil
}

func TestIterator(t *testing.T) {
	cases := []struct {
		name     string
		docs     []Document
		err      error
		expCount int
		expError error
	}{
		{"empty set", nil, nil, 0, nil},
		{"one doc", []Document{Document("one1")}, nil, 1, nil},
		{"multi doc", []Document{Document("one1"), Document("two2")}, nil, 2, nil},
		{"eor error", nil, io.EOF, 0, nil},
		{"other error", nil, fmt.Errorf("some error"), 0, fmt.Errorf("some error")},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			mci := &mockStreamReader{docs: c.docs, err: c.err}
			it := readIterator{streamReader: mci}
			var d Document
			var i int
			for it.Next(&d) {
				assert.NoError(t, it.Err())
				assert.Equal(t, c.docs[i], d)
				i++
				assert.Equal(t, 0, mci.closeCalled)
			}
			assert.Equal(t, 1, mci.closeCalled)
			assert.False(t, it.Next(&d))
			assert.Equal(t, c.expError, it.Err())
			assert.Equal(t, c.expCount, i)
			assert.Equal(t, 1, mci.closeCalled)
			it.Close()
			assert.Equal(t, 1, mci.closeCalled)
			assert.False(t, it.Next(&d))
			assert.Equal(t, c.expError, it.Err())
		})
	}

	t.Run("premature close", func(t *testing.T) {
		mci := &mockStreamReader{docs: []Document{Document("one1"), Document("two2")}, err: nil}
		it := readIterator{streamReader: mci}
		var d Document
		require.True(t, it.Next(&d))
		assert.Equal(t, 0, mci.closeCalled)
		it.Close()
		assert.Equal(t, 1, mci.closeCalled)
		assert.False(t, it.Next(&d))
		assert.NoError(t, it.Err())
		assert.Equal(t, 1, mci.closeCalled)
		it.Close()
		assert.Equal(t, 1, mci.closeCalled)
	})
}
