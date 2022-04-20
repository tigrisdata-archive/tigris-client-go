package filter

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func fb() *Expr {
	return NewFilterBuilder()
}

func TestPrefixFilterBuilderBasic(t *testing.T) {
	cases := []struct {
		name string
		fb   *Expr
		exp  string
		err  error
	}{
		{"eq", Eq("a", 1), `{"$eq":{"a":1}}`, nil},
		{"ne", Ne("a", 1), `{"$ne":{"a":1}}`, nil},
		{"gt", Gt("a", 1), `{"$gt":{"a":1}}`, nil},
		{"gte", Gte("a", 1), `{"$gte":{"a":1}}`, nil},
		{"lt", Lt("a", 1), `{"$lt":{"a":1}}`, nil},
		{"lte", Lte("a", 1), `{"$lte":{"a":1}}`, nil},
		{"not(eq)", Not(Eq("a", 1)), `{"$not":{"$eq":{"a":1}}}`, nil},
		{"not(not(eq))", Not(Not(Eq("a", 1))), `{"$not":{"$not":{"$eq":{"a":1}}}}`, nil},
		{"and(eq)", And(Eq("a", 1)), `{"$and":[{"$eq":{"a":1}}]}`, nil},
		{"and(eq, eq)", And(Eq("a", 1), Eq("b", 2)), `{"$and":[{"$eq":{"a":1}},{"$eq":{"b":2}}]}`, nil},
		{"or(eq)", Or(Eq("a", 1)), `{"$or":[{"$eq":{"a":1}}]}`, nil},
		{"or(eq, eq)", Or(Eq("a", 1), Eq("b", 2)), `{"$or":[{"$eq":{"a":1}},{"$eq":{"b":2}}]}`, nil},
		{"or(and(eq), eq)", Or(And(Eq("a", 1)), Eq("b", 2)), `{"$or":[{"$and":[{"$eq":{"a":1}}]},{"$eq":{"b":2}}]}`, nil},
		{"and(or(eq), eq)", And(Or(Eq("a", 1)), Eq("b", 2)), `{"$and":[{"$or":[{"$eq":{"a":1}}]},{"$eq":{"b":2}}]}`, nil},
		{"and(or(eq, and(eq, eq)), eq)", And(Or(Eq("a", 1), And(Eq("c", 3), Eq("d", 4))), Eq("b", 2)), `{"$and":[{"$or":[{"$eq":{"a":1}},{"$and":[{"$eq":{"c":3}},{"$eq":{"d":4}}]}]},{"$eq":{"b":2}}]}`, nil},
	}

	for _, v := range cases {
		t.Run(v.name, func(t *testing.T) {
			b, err := v.fb.Build()
			assert.Equal(t, v.err, err)
			assert.Equal(t, v.err, v.fb.Err())
			assert.Equal(t, v.exp, string(b))
		})
	}
}

func TestInfixFilterBuilderBasic(t *testing.T) {
	cases := []struct {
		name string
		fb   *Expr
		exp  string
		err  error
	}{
		{"not.and.not", Not().And().Not(), ``, fmt.Errorf("ineffective 'not'")},
		{"not(eq,eq)", Not(fb(), fb()), ``, fmt.Errorf("'not' accepts no more than 1 parameter")},
		{"not.and.eq", Not().And().Eq("a", 1), ``, fmt.Errorf("ineffective 'not'")},
		{"and.and.eq", fb().And().And().Eq("a", 1), ``, fmt.Errorf("ineffective 'and'")},
		{"or.or.eq", Or().Or().Eq("a", 1), ``, fmt.Errorf("ineffective 'or'")},
		{"or.or(eq)", Or().Or(Eq("a", 1)), ``, fmt.Errorf("ineffective 'or'")},
		{"or(eq).or.eq", Or(Eq("a", 1)).Or().Eq("b", 2), `{"$or":[{"$eq":{"a":1}},{"$eq":{"b":2}}]}`, nil},
		{"or(eq).eq", Or(Eq("a", 1)).Or().Eq("b", 2), `{"$or":[{"$eq":{"a":1}},{"$eq":{"b":2}}]}`, nil},
		{"and(eq).and.eq", And(Eq("a", 1)).And().Eq("b", 2), `{"$and":[{"$eq":{"a":1}},{"$eq":{"b":2}}]}`, nil},
		{"and.or.eq", And().Or().Eq("a", 1), ``, fmt.Errorf("ineffective 'and'")},
		{"or.and.eq", Or().And().Eq("a", 1), ``, fmt.Errorf("ineffective 'or'")},
		{"not.and.and", Not().And().And(), ``, fmt.Errorf("ineffective 'not'")},
		{"not.and.or", Not().And().Or(), ``, fmt.Errorf("ineffective 'not'")},
		{"not.and.not", Not().And().Not(), ``, fmt.Errorf("ineffective 'not'")},
		{"eq", Eq("a", 1), `{"$eq":{"a":1}}`, nil},
		{"ne", fb().Ne("a", 1), `{"$ne":{"a":1}}`, nil},
		{"gt", fb().Gt("a", 1), `{"$gt":{"a":1}}`, nil},
		{"lt", fb().Lt("a", 1), `{"$lt":{"a":1}}`, nil},
		{"gte", fb().Gte("a", 1), `{"$gte":{"a":1}}`, nil},
		{"lte", fb().Lte("a", 1), `{"$lte":{"a":1}}`, nil},
		{"eq", Eq("a", 1), `{"$eq":{"a":1}}`, nil},
		{"ne", Ne("a", 1), `{"$ne":{"a":1}}`, nil},
		{"gt", Gt("a", 1), `{"$gt":{"a":1}}`, nil},
		{"lt", Lt("a", 1), `{"$lt":{"a":1}}`, nil},
		{"gte", Gte("a", 1), `{"$gte":{"a":1}}`, nil},
		{"lte", Lte("a", 1), `{"$lte":{"a":1}}`, nil},
		{"eq.or.eq", Eq("a", 1).Or().Eq("b", 1), `{"$or":[{"$eq":{"a":1}},{"$eq":{"b":1}}]}`, nil},
		{"ne.or.ne", Ne("a", 1).Or().Ne("b", 1), `{"$or":[{"$ne":{"a":1}},{"$ne":{"b":1}}]}`, nil},
		{"gt.or.gt", Gt("a", 1).Or().Gt("b", 1), `{"$or":[{"$gt":{"a":1}},{"$gt":{"b":1}}]}`, nil},
		{"lt.or.lt", Lt("a", 1).Or().Lt("b", 1), `{"$or":[{"$lt":{"a":1}},{"$lt":{"b":1}}]}`, nil},
		{"gte.or.gte", Gte("a", 1).Or().Gte("b", 1), `{"$or":[{"$gte":{"a":1}},{"$gte":{"b":1}}]}`, nil},
		{"lte.or.lte", Lte("a", 1).Or().Lte("b", 1), `{"$or":[{"$lte":{"a":1}},{"$lte":{"b":1}}]}`, nil},
		{"eq.and.eq", Eq("a", 1).And().Eq("b", 2), `{"$and":[{"$eq":{"a":1}},{"$eq":{"b":2}}]}`, nil},
		{"ne.and.ne", Ne("a", 1).And().Ne("b", 2), `{"$and":[{"$ne":{"a":1}},{"$ne":{"b":2}}]}`, nil},
		{"gt.and.gt", Gt("a", 1).And().Gt("b", 2), `{"$and":[{"$gt":{"a":1}},{"$gt":{"b":2}}]}`, nil},
		{"lt.and.lt", Lt("a", 1).And().Lt("b", 2), `{"$and":[{"$lt":{"a":1}},{"$lt":{"b":2}}]}`, nil},
		{"gte.and.gte", Gte("a", 1).And().Gte("a", 1), `{"$and":[{"$gte":{"a":1}},{"$gte":{"a":1}}]}`, nil},
		{"lte.and.lte", Lte("a", 1).And().Lte("a", 1), `{"$and":[{"$lte":{"a":1}},{"$lte":{"a":1}}]}`, nil},
		{"not.eq", Not().Eq("a", 1), `{"$not":{"$eq":{"a":1}}}`, nil},
		{"not(eq)", Not(Eq("a", 1)), `{"$not":{"$eq":{"a":1}}}`, nil},
		{"not(eq).eq", Not(Eq("a", 1)).Eq("b", 2), `{"$and":[{"$not":{"$eq":{"a":1}}},{"$eq":{"b":2}}]}`, nil},
		{"not.not.eq", Not().Not().Eq("a", 1), `{"$eq":{"a":1}}`, nil},
		{"not.not.eq.eq", Not().Not().Eq("a", 1).Eq("b", 1), `{"$and":[{"$eq":{"a":1}},{"$eq":{"b":1}}]}`, nil},
		{"eq.eq", Eq("a", 1).Eq("b", 1), `{"$and":[{"$eq":{"a":1}},{"$eq":{"b":1}}]}`, nil},
		{"eq.not.eq", Eq("a", 1).Not().Eq("b", 1), `{"$and":[{"$eq":{"a":1}},{"$not":{"$eq":{"b":1}}}]}`, nil},
		{"eq.and.eq", Eq("a", 1).And().Eq("b", 1), `{"$and":[{"$eq":{"a":1}},{"$eq":{"b":1}}]}`, nil},
		{"eq.and.not.eq", Eq("a", 1).And().Eq("b", 1), `{"$and":[{"$eq":{"a":1}},{"$eq":{"b":1}}]}`, nil},
		{"eq.and.eq.and.eq", Eq("a", 1).And().Eq("b", 2).And().Eq("b", 3), `{"$and":[{"$eq":{"a":1}},{"$eq":{"b":2}},{"$eq":{"b":3}}]}`, nil},
		{"not.eq.and.eq", Not().Eq("a", 1).And().Eq("b", 1), `{"$and":[{"$not":{"$eq":{"a":1}}},{"$eq":{"b":1}}]}`, nil},
		{"not(eq).and.eq", Not(Eq("a", 1)).And().Eq("b", 1), `{"$and":[{"$not":{"$eq":{"a":1}}},{"$eq":{"b":1}}]}`, nil},
		{"eq.or.eq", Eq("a", 1).Or().Eq("b", 1), `{"$or":[{"$eq":{"a":1}},{"$eq":{"b":1}}]}`, nil},
		{"eq.or.not.eq", Eq("a", 1).Or().Not().Eq("b", 1), `{"$or":[{"$eq":{"a":1}},{"$not":{"$eq":{"b":1}}}]}`, nil},
		{"eq.or.eq.or.eq", Eq("a", 1).Or().Eq("b", 2).Or().Eq("b", 3), `{"$or":[{"$eq":{"a":1}},{"$eq":{"b":2}},{"$eq":{"b":3}}]}`, nil},
		{"not.eq.or.eq", Not().Eq("a", 1).Or().Eq("b", 1), `{"$or":[{"$not":{"$eq":{"a":1}}},{"$eq":{"b":1}}]}`, nil},
		{"not(eq).or.eq", Not(Eq("a", 1)).Or().Eq("b", 1), `{"$or":[{"$not":{"$eq":{"a":1}}},{"$eq":{"b":1}}]}`, nil},
		{"and(eq,eq)", And(Eq("a", 1), Eq("b", 1)), `{"$and":[{"$eq":{"a":1}},{"$eq":{"b":1}}]}`, nil},
		{"or(eq,eq)", Or(Eq("a", 1), Eq("b", 1)), `{"$or":[{"$eq":{"a":1}},{"$eq":{"b":1}}]}`, nil},
		{"eq.and", Eq("a", 1).And(), `{"$and":[{"$eq":{"a":1}}]}`, nil},
		{"eq.and.not", Eq("a", 1).And().Not(), ``, fmt.Errorf("ineffective 'not'")},
		{"eq.or.not", Eq("a", 1).Or().Not(), ``, fmt.Errorf("ineffective 'not'")},
		{"or", Or(), ``, fmt.Errorf("ineffective 'or'")},
		{"and", And(), ``, fmt.Errorf("ineffective 'and'")},
		{"not", Not(), ``, fmt.Errorf("ineffective 'not'")},
		{"not.not", Not().Not(), `{}`, nil},
		{"not.not.not", Not().Not().Not(), ``, fmt.Errorf("ineffective 'not'")},
		{"and.not.not", And().Not().Not(), ``, fmt.Errorf("ineffective 'and'")},
		{"or.not.not", Or().Not().Not(), ``, fmt.Errorf("ineffective 'or'")},
		{"not.not(eq)", Not().Not(Eq("a", 1)), `{"$not":{"$not":{"$eq":{"a":1}}}}`, nil},
		{"not.not.eq.or.not.not.eq", Not().Not().Eq("a", 1).Or().Not().Not().Eq("b", 2), `{"$or":[{"$eq":{"a":1}},{"$eq":{"b":2}}]}`, nil},
		{"not.not.eq.or.not.not.eq.or.not.not.eq", Not().Not().Eq("a", 1).Or().Not().Not().Eq("b", 2).Or().Not().Not().Eq("c", 3), `{"$or":[{"$eq":{"a":1}},{"$eq":{"b":2}},{"$eq":{"c":3}}]}`, nil},
		{"not.not.eq.and.not.not.eq", Not().Not().Eq("a", 1).And().Not().Not().Eq("a", 1), `{"$and":[{"$eq":{"a":1}},{"$eq":{"a":1}}]}`, nil},
		{"not.not.eq.and.not.not.eq.and.not.not.eq", Not().Not().Eq("a", 1).And().Not().Not().Eq("b", 2).And().Not().Not().Eq("c", 3), `{"$and":[{"$eq":{"a":1}},{"$eq":{"b":2}},{"$eq":{"c":3}}]}`, nil},
		{"not(eq).not", Not(Eq("a", 1)).Not(), ``, fmt.Errorf("ineffective 'not'")},
		{"not(eq).not(eq)", Not(Eq("a", 1)).Not(Eq("b", 1)), `{"$and":[{"$not":{"$eq":{"a":1}}},{"$not":{"$eq":{"b":1}}}]}`, nil},
		{"not(eq).not.eq", Not(Eq("a", 1)).Not().Eq("b", 1), `{"$and":[{"$not":{"$eq":{"a":1}}},{"$not":{"$eq":{"b":1}}}]}`, nil},
		{"not.eq.not.eq", Not().Eq("a", 1).Not().Eq("b", 1), `{"$and":[{"$not":{"$eq":{"a":1}}},{"$not":{"$eq":{"b":1}}}]}`, nil},
		{"not(eq).and.not(eq)", Not(Eq("a", 1)).And().Not(Eq("b", 1)), `{"$and":[{"$not":{"$eq":{"a":1}}},{"$not":{"$eq":{"b":1}}}]}`, nil},
		{"not(eq).or.not(eq)", Not(Eq("a", 1)).Or().Not(Eq("b", 1)), `{"$or":[{"$not":{"$eq":{"a":1}}},{"$not":{"$eq":{"b":1}}}]}`, nil},
		{"and.not(eq).not().and()", And().Not(Eq("a", 1)).Not().And(), ``, fmt.Errorf("ineffective 'and'")},
		{"not(eq).not().and()", Not(Eq("a", 1)).Not().And(), ``, fmt.Errorf("ineffective 'not'")},
		//FIXME: trailing and() and or() without operands should end up with error on build
		{"not.not(eq).and()", Not().Not(Eq("a", 1)).And(), `{"$and":[{"$not":{"$not":{"$eq":{"a":1}}}}]}`, nil},
		{"not(eq).not.or", Not(Eq("a", 1)).Not().Or(), ``, fmt.Errorf("ineffective 'not'")},
		{"not.not(eq).or", Not().Not(Eq("a", 1)).Or(), `{"$or":[{"$not":{"$not":{"$eq":{"a":1}}}}]}`, nil},
		{"eq.eq.or.eq.eq.or.eq.eq",
			Eq("a", 1).Eq("b", 2).
				Or().
				Eq("c", 3).Eq("d", 4).
				Or().
				Eq("e", 5).Eq("f", 6),
			`{"$or":[{"$and":[{"$eq":{"a":1}},{"$eq":{"b":2}}]},{"$and":[{"$eq":{"c":3}},{"$eq":{"d":4}}]},{"$and":[{"$eq":{"e":5}},{"$eq":{"f":6}}]}]}`, nil,
		},
		{"and(eq,eq).or().and(eq,eq).or().and(eq,eq)",
			And(
				Eq("a", 1), Eq("b", 2),
			).Or().And(
				Eq("c", 3), Eq("d", 4),
			).Or().And(
				Eq("e", 5), Eq("f", 6),
			),
			`{"$or":[{"$and":[{"$eq":{"a":1}},{"$eq":{"b":2}}]},{"$and":[{"$eq":{"c":3}},{"$eq":{"d":4}}]},{"$and":[{"$eq":{"e":5}},{"$eq":{"f":6}}]}]}`, nil,
		},
		{"and(eq).or(eq).and(eq).or(eq).and(eq)",
			And(Eq("a", 1)).
				Or(Eq("b", 2)).
				And(Eq("c", 3)).
				Or(Eq("d", 4)).
				And(Eq("e", 5)),
			`{"$or":[{"$and":[{"$eq":{"a":1}}]},{"$and":[{"$or":[{"$eq":{"b":2}}]},{"$eq":{"c":3}}]},{"$and":[{"$or":[{"$eq":{"d":4}}]},{"$eq":{"e":5}}]}]}`, nil,
		},
		{"or(eq).or(eq)",
			Or(Eq("u", 3)).Or(Eq("o", 3)),
			`{"$or":[{"$eq":{"u":3}},{"$eq":{"o":3}}]}`, nil,
		},
	}

	for _, v := range cases {
		t.Run(v.name, func(t *testing.T) {
			b, err := v.fb.Build()
			assert.Equal(t, v.err, err)
			assert.Equal(t, v.err, v.fb.Err())
			assert.Equal(t, v.exp, string(b))
		})
	}
}
