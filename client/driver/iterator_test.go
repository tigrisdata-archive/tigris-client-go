package driver

import (
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
)

type mockStreamReader struct {
	docs []Document
	err  error
	cur  int
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
			it := readIterator{streamReader: &mockStreamReader{docs: c.docs, err: c.err}}
			var d Document
			var i int
			for it.Next(&d) {
				assert.NoError(t, it.Err())
				assert.Equal(t, c.docs[i], d)
				i++
			}
			assert.False(t, it.Next(&d))
			assert.Equal(t, c.expError, it.Err())
			assert.Equal(t, c.expCount, i)
		})
	}
}
