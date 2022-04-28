package filter

import "time"

// EqInt64 composes 'equal' operation from int64 value.
// Result is equivalent to: field == value
func EqInt64(field string, value int64) Expr {
	return Expr{field: comparison{Eq: value}}
}

// EqInt32 composes 'equal' operation from int32 value.
// Result is equivalent to: field == value
func EqInt32(field string, value int32) Expr {
	return Expr{field: comparison{Eq: value}}
}

// EqInt composes 'equal' operation from int value.
// Result is equivalent to: field == value
func EqInt(field string, value int) Expr {
	return Expr{field: comparison{Eq: value}}
}

// EqString composes 'equal' operation from string value.
// Result is equivalent to: field == value
func EqString(field string, value string) Expr {
	return Expr{field: comparison{Eq: value}}
}

// EqBytes composes 'equal' operation from []byte value.
// Result is equivalent to: field == value
func EqBytes(field string, value []byte) Expr {
	return Expr{field: comparison{Eq: value}}
}

// EqFloat32 composes 'equal' operation from float32 value.
// Result is equivalent to: field == value
func EqFloat32(field string, value float32) Expr {
	return Expr{field: comparison{Eq: value}}
}

// EqFloat64 composes 'equal' operation from float64 value.
// Result is equivalent to: field == value
func EqFloat64(field string, value float64) Expr {
	return Expr{field: comparison{Eq: value}}
}

// EqTime composes 'equal' operation. from time.Time value.
// Result is equivalent to: field == value
func EqTime(field string, value *time.Time) Expr {
	return Expr{field: comparison{Eq: value}}
}
