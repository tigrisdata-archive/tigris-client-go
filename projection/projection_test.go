package projection

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tigrisdata/tigris-client-go/driver"
)

func TestProjectionBasic(t *testing.T) {

	cases := []struct {
		name   string
		fields Projection
		exp    string
		err    error
	}{
		{"nil", nil, ``, nil},
		{"include_1", Include("a"), `{"a":true}`, nil},
		{"exclude_1", Exclude("a"), `{"a":false}`, nil},
		{"include_exclude", Include("a").Exclude("b"), `{"a":true,"b":false}`, nil},
		{"exclude_include", Exclude("a").Include("b"), `{"a":false,"b":true}`, nil},
		{"exclude_include_nested", Exclude("a.b.c").Include("b.c.d"), `{"a.b.c":false,"b.c.d":true}`, nil},
	}

	for _, v := range cases {
		t.Run(v.name, func(t *testing.T) {
			b, err := v.fields.Build()
			assert.Equal(t, v.err, err)
			assert.Equal(t, v.exp, string(b))
		})
	}

	b, err := Builder().Build()
	assert.NoError(t, err)
	assert.Equal(t, driver.Projection(nil), b)
}
