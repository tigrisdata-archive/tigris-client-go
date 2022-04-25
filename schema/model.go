package schema

import (
	"time"
)

type Model interface{}

type model struct {
	CreatedAt time.Time `json:"-"`
	UpdatedAt time.Time `json:"-"`

	Name string `json:"-"`

	Schema *Schema `json:"-"`
}

func (m *model) GetCreatedAt() time.Time {
	return m.CreatedAt
}

func (m *model) GetUpdatedAt() time.Time {
	return m.UpdatedAt
}
