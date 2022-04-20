package update

import (
	"encoding/json"
	"fmt"

	"github.com/tigrisdata/tigrisdb-client-go/driver"
)

type Update struct {
	SetF   map[string]interface{} `json:"$set,omitempty"`
	UnsetF map[string]interface{} `json:"$unset,omitempty"`
}

func Builder() *Update {
	return &Update{SetF: map[string]interface{}{}, UnsetF: map[string]interface{}{}}
}

func (u *Update) Build() (driver.Update, error) {
	if len(u.SetF) == 0 && len(u.UnsetF) == 0 {
		return nil, fmt.Errorf("empty update")
	}
	b, err := json.Marshal(u)
	return b, err
}

func (u *Update) Set(field string, value interface{}) *Update {
	u.SetF[field] = value
	return u
}

func (u *Update) Unset(field string) *Update {
	u.UnsetF[field] = nil
	return u
}

func Set(field string, value interface{}) *Update {
	u := &Update{SetF: map[string]interface{}{}, UnsetF: map[string]interface{}{}}
	u.SetF[field] = value
	return u
}

func Unset(field string) *Update {
	u := &Update{SetF: map[string]interface{}{}, UnsetF: map[string]interface{}{}}
	u.UnsetF[field] = nil
	return u
}
