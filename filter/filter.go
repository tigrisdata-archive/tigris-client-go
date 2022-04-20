package filter

import (
	"encoding/json"
	"fmt"

	"github.com/tigrisdata/tigrisdb-client-go/driver"
)

type Operand map[string]interface{}

type op string

const (
	unknown op = ""

	and op = "$and"
	or  op = "$or"
	not op = "$not"

	gt  op = "$gt"
	gte op = "$gte"
	lt  op = "$lt"
	lte op = "$lte"
	ne  op = "$ne"
	eq  op = "$eq"
)

type Expr struct {
	AndF []*Expr `json:"$and,omitempty"`
	OrF  []*Expr `json:"$or,omitempty"`
	NotF *Expr   `json:"$not,omitempty"`

	GtF  Operand `json:"$gt,omitempty"`
	GteF Operand `json:"$gte,omitempty"`
	LtF  Operand `json:"$lt,omitempty"`
	LteF Operand `json:"$lte,omitempty"`
	NeF  Operand `json:"$ne,omitempty"`
	EqF  Operand `json:"$eq,omitempty"`

	Operand `json:",inline,omitempty"`

	parent *Expr
	prev   *Expr
	kind   op
	err    error
}

func NewFilterBuilder() *Expr {
	return &Expr{}
}

func (prev *Expr) Err() error {
	return prev.err
}

func (prev *Expr) And(operands ...*Expr) *Expr {
	if prev.err != nil {
		return prev
	}
	switch prev.kind {
	case eq, ne, lt, gt, lte, gte:
		e := &Expr{AndF: []*Expr{prev}, kind: and, parent: prev.parent}
		e.AndF = append(e.AndF, operands...)
		return e
	case and:
		if len(operands) == 0 && prev.AndF == nil {
			prev.err = fmt.Errorf("ineffective 'and'")
			return prev
		}
		prev.AndF = append(prev.AndF, operands...)
		return prev
	case or:
		if len(operands) == 0 && prev.OrF == nil {
			prev.err = fmt.Errorf("ineffective 'or'")
			return prev
		}
		e := &Expr{kind: and, parent: prev.parent}
		if prev.OrF != nil {
			e.AndF = append(e.AndF, prev)
		}
		e.AndF = append(e.AndF, operands...)
		return e
	case not:
		if prev.NotF == nil {
			if len(operands) == 0 {
				prev.err = fmt.Errorf("ineffective 'not'")
				return prev
			}
			prev.NotF = &Expr{OrF: operands, kind: and}
			return prev
		}
		e := &Expr{AndF: []*Expr{prev}, kind: and}
		e.AndF = append(e.AndF, operands...)
		return e
	case unknown:
		if len(operands) == 0 {
			prev.err = fmt.Errorf("ineffective 'and'")
			return prev
		}
	}
	return &Expr{AndF: operands, kind: and}
}

func (prev *Expr) Or(operands ...*Expr) *Expr {
	if prev.err != nil {
		return prev
	}
	ops := &Expr{OrF: operands, kind: or}
	switch prev.kind {
	case eq, ne, lt, gt, lte, gte:
		e := &Expr{OrF: []*Expr{prev}, kind: or}
		if len(operands) != 0 {
			e.OrF = append(e.OrF, ops)
		}
		if prev.parent != nil {
			prev.parent.OrF = append(prev.parent.OrF, e.OrF...)
			return prev.parent
		}
		return e
	case or:
		if prev.OrF == nil {
			prev.err = fmt.Errorf("ineffective 'or'")
			return prev
		}
		if prev.parent != nil {
			prev.parent.OrF = append(prev.parent.OrF, prev.OrF...)
			return prev.parent
		}
		prev.OrF = append(prev.OrF, operands...)
		return prev
	case and:
		if len(operands) == 0 && prev.AndF == nil {
			prev.err = fmt.Errorf("ineffective 'and'")
			return prev
		}
		if prev.parent != nil {
			prev.parent.OrF = append(prev.parent.OrF, prev)
			ops.parent = prev.parent
		} else {
			e := &Expr{OrF: []*Expr{prev}, kind: or}
			ops.parent = e
		}
		return ops
	case not:
		if prev.NotF == nil {
			if len(operands) == 0 {
				prev.err = fmt.Errorf("ineffective 'not'")
				return prev
			}
			prev.NotF = &Expr{OrF: operands, kind: or}
			if prev.parent != nil {
				prev.parent.OrF = append(prev.parent.OrF, prev.NotF)
				return prev.parent
			}
			return prev
		}
		e := &Expr{OrF: []*Expr{prev}, kind: or}
		if prev.prev != nil {
			e.prev = prev.prev
		}
		if len(operands) != 0 {
			e.OrF = append(e.OrF, ops)
		}
		return e
	case unknown:
		if len(operands) == 0 {
			prev.err = fmt.Errorf("ineffective 'or'")
			return prev
		}
		return &Expr{OrF: operands, kind: or}
	default:
		prev.err = fmt.Errorf("unknown prev operation")
		return prev
	}
}

func (prev *Expr) Not(expr ...*Expr) *Expr {
	if prev.err != nil {
		return prev
	}
	var err error
	var e *Expr
	if len(expr) > 1 {
		err = fmt.Errorf("'not' accepts no more than 1 parameter")
	} else if len(expr) > 0 {
		e = expr[0]
	}
	e = &Expr{NotF: e, kind: not, err: err, prev: prev, parent: prev.parent}
	if err != nil {
		return e
	}
	switch prev.kind {
	case eq, ne, gt, gte, lt, lte:
		if e.NotF == nil {
			return e
		}
		return &Expr{AndF: []*Expr{prev, e}, kind: and, parent: prev.parent}
	case and:
		if e.NotF == nil {
			return e
		}
		prev.AndF = append(prev.AndF, e)
	case or:
		if e.NotF == nil {
			return e
		}
		prev.OrF = append(prev.OrF, e)
	case not:
		if prev.NotF != nil {
			if e.NotF != nil {
				return &Expr{AndF: []*Expr{prev, e}, kind: and, parent: prev.parent}
			}
			return e
		}
		if e.NotF == nil {
			return prev.prev
		}
		return prev.prev.Not(e)
	default:
		return e
	}
	return prev
}

func (prev *Expr) Eq(field string, value interface{}) *Expr {
	return prev.Op(eq, field, value)
}

func (prev *Expr) Ne(field string, value interface{}) *Expr {
	return prev.Op(ne, field, value)
}

func (prev *Expr) Gt(field string, value interface{}) *Expr {
	return prev.Op(gt, field, value)
}

func (prev *Expr) Gte(field string, value interface{}) *Expr {
	return prev.Op(gte, field, value)
}

func (prev *Expr) Lt(field string, value interface{}) *Expr {
	return prev.Op(lt, field, value)
}

func (prev *Expr) Lte(field string, value interface{}) *Expr {
	return prev.Op(lte, field, value)
}

func newOp(kind op, field string, value interface{}) *Expr {
	var e *Expr
	op := Operand{field: value}
	switch kind {
	case eq:
		e = &Expr{EqF: op, kind: eq}
	case ne:
		e = &Expr{NeF: op, kind: ne}
	case gt:
		e = &Expr{GtF: op, kind: gt}
	case gte:
		e = &Expr{GteF: op, kind: gte}
	case lt:
		e = &Expr{LtF: op, kind: lt}
	case lte:
		e = &Expr{LteF: op, kind: lte}
	}
	return e
}

func (prev *Expr) Op(kind op, field string, value interface{}) *Expr {
	if prev.err != nil {
		return prev
	}
	e := newOp(kind, field, value)
	switch prev.kind {
	case eq, ne, gt, gte, lt, lte:
		return &Expr{AndF: []*Expr{prev, e}, kind: and, parent: prev.parent}
	case and:
		prev.AndF = append(prev.AndF, e)
		return prev
	case or:
		e.parent = prev.parent
		if prev.parent == nil {
			e.parent = prev
		}
		return e
	case not:
		if prev.NotF != nil {
			return &Expr{AndF: []*Expr{prev, e}, kind: and, prev: e.prev, parent: prev.parent}
		}
		return prev.prev.Not(e)
	}
	return e
}

func And(operands ...*Expr) *Expr {
	e := &Expr{}
	if len(operands) == 0 {
		e.err = fmt.Errorf("ineffective 'and'")
	}
	return e.And(operands...)
}

func Or(operands ...*Expr) *Expr {
	e := &Expr{}
	if len(operands) == 0 {
		e.err = fmt.Errorf("ineffective 'or'")
	}
	return e.Or(operands...)
}

func Not(operands ...*Expr) *Expr {
	return (&Expr{}).Not(operands...)
}

func Eq(field string, value interface{}) *Expr {
	return (&Expr{}).Eq(field, value)
}

func Ne(field string, value interface{}) *Expr {
	return (&Expr{}).Ne(field, value)
}

func Gt(field string, value interface{}) *Expr {
	return (&Expr{}).Gt(field, value)
}

func Gte(field string, value interface{}) *Expr {
	return (&Expr{}).Gte(field, value)
}

func Lt(field string, value interface{}) *Expr {
	return (&Expr{}).Lt(field, value)
}

func Lte(field string, value interface{}) *Expr {
	return (&Expr{}).Lte(field, value)
}

func (prev *Expr) Build() (driver.Filter, error) {
	if prev.err != nil {
		return nil, prev.err
	}

	switch prev.kind {
	case not:
		if prev.NotF == nil {
			prev.err = fmt.Errorf("ineffective 'not'")
			return nil, prev.err
		}
	case and:
		if prev.AndF == nil {
			prev.err = fmt.Errorf("ineffective 'and'")
			return nil, prev.err
		}
	case or:
		if prev.OrF == nil {
			prev.err = fmt.Errorf("ineffective 'or'")
			return nil, prev.err
		}
	}
	expr := prev
	if prev.parent != nil {
		prev.parent.OrF = append(prev.parent.OrF, prev)
		expr = prev.parent
	}
	b, err := json.Marshal(expr)
	return b, err
}
