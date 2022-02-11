package client

import (
	"context"
	"github.com/tigrisdata/tigrisdb-client-go/client/driver"
)

type Client interface {
}

type client struct {
	driver driver.Driver
}

func NewClient(ctx context.Context, url string, config driver.Config) (Client, error) {
	d, err := driver.NewDriver(ctx, url, config)
	if err != nil {
		return nil, err
	}
	return &client{d}, nil
}
