package projection

import (
	"encoding/json"

	"github.com/tigrisdata/tigrisdb-client-go/driver"
)

type Projection map[string]bool

func Builder() Projection {
	return Projection{}
}

func (pr Projection) Include(field string) Projection {
	pr[field] = true
	return pr
}

func (pr Projection) Exclude(field string) Projection {
	pr[field] = false
	return pr
}

func (pr Projection) Build() (driver.Projection, error) {
	if pr == nil || len(pr) == 0 {
		return nil, nil
	}
	b, err := json.Marshal(pr)
	return b, err
}

func Include(field string) Projection {
	pr := map[string]bool{}
	pr[field] = true
	return pr
}

func Exclude(field string) Projection {
	pr := map[string]bool{}
	pr[field] = false
	return pr
}
