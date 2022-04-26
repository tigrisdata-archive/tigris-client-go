// Package filter provides methods to build logical filters.
// Filters are used to read or update only documents,
// which satisfies the filter.
package filter

import (
	"encoding/json"

	"github.com/tigrisdata/tigris-client-go/driver"
)

type Operand map[string]comparison
type value interface{}

type comparison struct {
	Gt  value `json:"$gt,omitempty"`
	Gte value `json:"$gte,omitempty"`
	Lt  value `json:"$lt,omitempty"`
	Lte value `json:"$lte,omitempty"`
	Ne  value `json:"$ne,omitempty"`
	Eq  value `json:"$eq,omitempty"`
}

const (
	and = "$and"
	or  = "$or"
	//	not = "$not"
)

type expr map[string]interface{}

// And composes 'and' operation.
// Result is equivalent to: (ops[0] && ... && ops[len(ops-1])
func And(ops ...expr) expr {
	return expr{and: ops}
}

// Or composes 'or' operation.
// Result is equivalent to: (ops[0] || ... || ops[len(ops-1])
func Or(ops ...expr) expr {
	return expr{or: ops}
}

/*
// Not composes 'not' operation.
// Result is equivalent to: !(op)
func Not(op expr) expr {
	return expr{not: op}
}
*/

// Eq composes 'equal' operation.
// Result is equivalent to: field == value
func Eq(field string, value interface{}) expr {
	return expr{field: comparison{Eq: value}}
}

/*
// Ne composes 'not equal' operation.
// Result is equivalent to: field != value
func Ne(field string, value interface{}) expr {
	return expr{field: comparison{Ne: value}}
}

// Gt composes 'greater than' operation.
// Result is equivalent to: field > value
func Gt(field string, value interface{}) expr {
	return expr{field: comparison{Gt: value}}
}

// Gte composes 'greater than or equal' operation.
// Result is equivalent to: field >= value
func Gte(field string, value interface{}) expr {
	return expr{field: comparison{Gte: value}}
}

// Lt composes 'less than' operation.
// Result is equivalent to: field < value
func Lt(field string, value interface{}) expr {
	return expr{field: comparison{Lt: value}}
}

// Lte composes 'less than or equal' operation.
// Result is equivalent to: field <= value
func Lte(field string, value interface{}) expr {
	return expr{field: comparison{Lte: value}}
}
*/

// Build materializes the filter
func (prev expr) Build() (driver.Filter, error) {
	b, err := json.Marshal(prev)
	return b, err
}
