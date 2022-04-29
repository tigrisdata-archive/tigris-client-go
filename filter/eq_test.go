package filter

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestEq(t *testing.T) {
	cases := []struct {
		name string
		expr Expr
		exp  string
	}{
		{"int", EqInt("f", 12345), `{"f":{"$eq":12345}}`},
		{"int32", EqInt32("f", 12345), `{"f":{"$eq":12345}}`},
		{"int64", EqInt64("f", 123456789012), `{"f":{"$eq":123456789012}}`},
		{"float32", EqFloat32("f", 12345.67), `{"f":{"$eq":12345.67}}`},
		{"float64", EqFloat64("f", 123456789012.34), `{"f":{"$eq":123456789012.34}}`},
		{"string", EqString("f", "1234"), `{"f":{"$eq":"1234"}}`},
		{"bytes", EqBytes("f", []byte("123")), `{"f":{"$eq":"MTIz"}}`},
		{"time", EqTime("f", &time.Time{}), `{"f":{"$eq":"0001-01-01T00:00:00Z"}}`},
	}

	for _, v := range cases {
		t.Run(v.name, func(t *testing.T) {
			act, err := v.expr.Build()
			require.NoError(t, err)
			require.Equal(t, v.exp, string(act))
		})
	}
}
