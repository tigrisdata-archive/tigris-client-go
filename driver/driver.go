// Copyright 2022-2023 Tigris Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package driver provides access to the low level Tigris API.
// It abstracts underlying transport protocol.
package driver

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net/url"
	"os"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/tigrisdata/tigris-client-go/config"
)

// Driver implements Tigris API.
type Driver interface {
	// Info returns server information
	Info(ctx context.Context) (*InfoResponse, error)
	// Health returns server health
	Health(ctx context.Context) (*HealthResponse, error)

	// UseDatabase returns and interface for collections and documents management
	// of the database
	UseDatabase(project string) Database
	UseSearch(project string) SearchClient

	// CreateProject creates the project
	CreateProject(ctx context.Context, project string, options ...*CreateProjectOptions) (*CreateProjectResponse, error)

	// DescribeDatabase returns project description metadata
	DescribeDatabase(ctx context.Context, project string, options ...*DescribeProjectOptions) (*DescribeDatabaseResponse, error)

	// ListProjects returns all projects
	ListProjects(ctx context.Context) ([]string, error)

	// DeleteProject deletes the project
	DeleteProject(ctx context.Context, project string, options ...*DeleteProjectOptions) (*DeleteProjectResponse, error)

	// Close releases resources of the driver
	Close() error

	CreateAppKey(ctx context.Context, project string, name string, description string) (*AppKey, error)
	DeleteAppKey(ctx context.Context, project string, id string) error
	UpdateAppKey(ctx context.Context, project string, id string, name string, description string) (*AppKey, error)
	ListAppKeys(ctx context.Context, project string) ([]*AppKey, error)
	RotateAppKeySecret(ctx context.Context, project string, id string) (*AppKey, error)
}

// Tx object is used to atomically modify documents.
// This object is returned by BeginTx.
type Tx interface {
	// Commit all the modification of the transaction
	Commit(ctx context.Context) error

	// Rollback discard all the modification made by the transaction
	Rollback(ctx context.Context) error

	Database
}

// Database is the interface that encapsulates the CRUD portions of the transaction API.
type Database interface {
	// BeginTx starts new transaction
	BeginTx(ctx context.Context, options ...*TxOptions) (Tx, error)
	// Insert array of documents into specified database and collection.
	Insert(ctx context.Context, collection string, docs []Document,
		options ...*InsertOptions) (*InsertResponse, error)

	// Replace array of documents into specified database and collection
	// Creates document if it doesn't exist.
	Replace(ctx context.Context, collection string, docs []Document,
		options ...*ReplaceOptions) (*ReplaceResponse, error)

	// Read documents from the collection matching the specified filter.
	Read(ctx context.Context, collection string, filter Filter, fields Projection,
		options ...*ReadOptions) (Iterator, error)

	// Search for documents in the collection matching specified query
	Search(ctx context.Context, collection string, request *SearchRequest) (SearchResultIterator, error)

	// Update documents in the collection matching the specified filter.
	Update(ctx context.Context, collection string, filter Filter, fields Update,
		options ...*UpdateOptions) (*UpdateResponse, error)

	// Delete documents from the collection matching specified filter.
	Delete(ctx context.Context, collection string, filter Filter, options ...*DeleteOptions) (*DeleteResponse, error)

	// CreateOrUpdateCollection either creates a collection or update the collection with the new schema
	// There are three categories of data types supported:
	//   Primitive: Strings, Numbers, Binary Data, Booleans, UUIDs, DateTime
	//   Complex: Arrays
	//   Objects: A container data type defined by the user that stores fields of primitive types,
	//   complex types as well as other Objects
	//
	//  The data types are derived from the types defined in the JSON schema specification
	//  with extensions that enable support for richer semantics.
	//  As an example, the string is defined like this,
	//   {
	//     "name": {
	//       "type": "string"
	//     }
	//   }
	// More detailed information here: https://docs.tigrisdata.com/datamodels/types
	CreateOrUpdateCollection(ctx context.Context, collection string, schema Schema,
		options ...*CreateCollectionOptions) error

	// DropCollection deletes the collection and all documents it contains.
	DropCollection(ctx context.Context, collection string, options ...*CollectionOptions) error

	// DropAllCollections deletes all the collections and all documents it contains.
	DropAllCollections(ctx context.Context, options ...*CollectionOptions) error

	// ListCollections lists collections in the database.
	ListCollections(ctx context.Context, options ...*CollectionOptions) ([]string, error)

	// DescribeCollection returns metadata of the collection in the database
	DescribeCollection(ctx context.Context, collection string, options ...*DescribeCollectionOptions) (
		*DescribeCollectionResponse, error)

	// CreateBranch creates a branch of this database
	CreateBranch(ctx context.Context, name string) (*CreateBranchResponse, error)

	// DeleteBranch deletes a branch of this database, throws an error if "main" branch is being deleted
	DeleteBranch(ctx context.Context, name string) (*DeleteBranchResponse, error)
}

type driver struct {
	driverWithOptions
	closeWg *sync.WaitGroup
	closeCh chan struct{}
	cfg     *config.Driver
}

func (c *driver) CreateProject(ctx context.Context, project string, options ...*CreateProjectOptions) (*CreateProjectResponse, error) {
	opts, err := validateOptionsParam(options, &CreateProjectOptions{})
	if err != nil {
		return nil, err
	}

	return c.createProjectWithOptions(ctx, project, opts.(*CreateProjectOptions))
}

func (c *driver) DescribeDatabase(ctx context.Context, project string, options ...*DescribeProjectOptions) (*DescribeDatabaseResponse, error) {
	opts, err := validateOptionsParam(options, &DescribeProjectOptions{})
	if err != nil {
		return nil, err
	}

	return c.describeProjectWithOptions(ctx, project, opts.(*DescribeProjectOptions))
}

func (c *driver) DeleteProject(ctx context.Context, project string, options ...*DeleteProjectOptions) (*DeleteProjectResponse, error) {
	opts, err := validateOptionsParam(options, &DeleteProjectOptions{})
	if err != nil {
		return nil, err
	}

	return c.deleteProjectWithOptions(ctx, project, opts.(*DeleteProjectOptions))
}

type driverCRUDTx struct {
	*driverCRUD
	txWithOptions
}

type driverCRUD struct {
	CRUDWithOptions
}

func (c *driverCRUD) BeginTx(ctx context.Context, options ...*TxOptions) (Tx, error) {
	opts, err := validateOptionsParam(options, &TxOptions{})
	if err != nil {
		return nil, err
	}

	tx, err := c.beginTxWithOptions(ctx, opts.(*TxOptions))
	if err != nil {
		return nil, err
	}

	return &driverCRUDTx{driverCRUD: &driverCRUD{tx}, txWithOptions: tx}, nil
}

func (c *driverCRUD) Insert(ctx context.Context, collection string, docs []Document, options ...*InsertOptions) (
	*InsertResponse, error,
) {
	opts, err := validateOptionsParam(options, &InsertOptions{})
	if err != nil {
		return nil, err
	}

	fmt.Fprintf(os.Stderr, "driver insert %s", spew.Sprint(docs))

	return c.insertWithOptions(ctx, collection, docs, opts.(*InsertOptions))
}

func (c *driverCRUD) Replace(ctx context.Context, collection string, docs []Document, options ...*ReplaceOptions) (
	*ReplaceResponse, error,
) {
	opts, err := validateOptionsParam(options, &ReplaceOptions{})
	if err != nil {
		return nil, err
	}

	fmt.Fprintf(os.Stderr, "driver replace %s", spew.Sprint(docs))

	return c.replaceWithOptions(ctx, collection, docs, opts.(*ReplaceOptions))
}

func (c *driverCRUD) Update(ctx context.Context, collection string, filter Filter, fields Update,
	options ...*UpdateOptions,
) (*UpdateResponse, error) {
	opts, err := validateOptionsParam(options, &UpdateOptions{})
	if err != nil {
		return nil, err
	}

	fmt.Fprintf(os.Stderr, "driver update %s", spew.Sprint(filter))
	fmt.Fprintf(os.Stderr, "driver update %s", spew.Sprint(fields))

	return c.updateWithOptions(ctx, collection, filter, fields, opts.(*UpdateOptions))
}

func (c *driverCRUD) Delete(ctx context.Context, collection string, filter Filter, options ...*DeleteOptions) (
	*DeleteResponse, error,
) {
	opts, err := validateOptionsParam(options, &DeleteOptions{})
	if err != nil {
		return nil, err
	}

	return c.deleteWithOptions(ctx, collection, filter, opts.(*DeleteOptions))
}

func (c *driverCRUD) Read(ctx context.Context, collection string, filter Filter, fields Projection,
	options ...*ReadOptions,
) (Iterator, error) {
	opts, err := validateOptionsParam(options, &ReadOptions{})
	if err != nil {
		return nil, err
	}

	return c.readWithOptions(ctx, collection, filter, fields, opts.(*ReadOptions))
}

func (c *driverCRUD) Search(ctx context.Context, collection string, request *SearchRequest) (
	SearchResultIterator, error,
) {
	if request == nil {
		return nil, fmt.Errorf("API does accept nil Search Request")
	}

	return c.search(ctx, collection, request)
}

func (c *driverCRUD) CreateOrUpdateCollection(ctx context.Context, collection string, schema Schema,
	options ...*CreateCollectionOptions,
) error {
	opts, err := validateOptionsParam(options, &CreateCollectionOptions{})
	if err != nil {
		return err
	}

	return c.createOrUpdateCollectionWithOptions(ctx, collection, schema, opts.(*CreateCollectionOptions))
}

func (c *driverCRUD) DropCollection(ctx context.Context, collection string, options ...*CollectionOptions) error {
	opts, err := validateOptionsParam(options, &CollectionOptions{})
	if err != nil {
		return err
	}

	return c.dropCollectionWithOptions(ctx, collection, opts.(*CollectionOptions))
}

func (c *driverCRUD) DropAllCollections(ctx context.Context, _ ...*CollectionOptions) error {
	collections, err := c.ListCollections(ctx)
	if err != nil {
		return errors.New("failed to drop all collections")
	}
	for _, collection := range collections {
		err = c.DropCollection(ctx, collection)
		if err != nil {
			return errors.New("failed to drop all collections")
		}
	}
	return nil
}

func (c *driverCRUD) ListCollections(ctx context.Context, options ...*CollectionOptions) ([]string, error) {
	opts, err := validateOptionsParam(options, &CollectionOptions{})
	if err != nil {
		return nil, err
	}
	return c.listCollectionsWithOptions(ctx, opts.(*CollectionOptions))
}

func (c *driverCRUD) DescribeCollection(ctx context.Context, collection string, options ...*DescribeCollectionOptions) (
	*DescribeCollectionResponse, error,
) {
	opts, err := validateOptionsParam(options, &DescribeCollectionOptions{})
	if err != nil {
		return nil, err
	}

	return c.describeCollectionWithOptions(ctx, collection, opts.(*DescribeCollectionOptions))
}

func (c *driverCRUD) CreateBranch(ctx context.Context, name string) (*CreateBranchResponse, error) {
	if len(name) == 0 {
		return nil, fmt.Errorf("branch name is required")
	}
	return c.createBranch(ctx, name)
}

func (c *driverCRUD) DeleteBranch(ctx context.Context, name string) (*DeleteBranchResponse, error) {
	if len(name) == 0 {
		return nil, fmt.Errorf("branch name is required")
	}
	return c.deleteBranch(ctx, name)
}

func validateOptionsParam(options interface{}, out interface{}) (interface{}, error) {
	v := reflect.ValueOf(options)

	if (v.Kind() != reflect.Array && v.Kind() != reflect.Slice) || v.Len() > 1 {
		return nil, fmt.Errorf("API accepts no more then one options parameter")
	}

	if v.Len() < 1 || v.Index(0).IsNil() {
		return out, nil
	}

	return v.Index(0).Interface(), nil
}

func initProto(scheme string, cfg *config.Driver) (bool, error) {
	proto := os.Getenv(EnvProtocol)

	if cfg.Protocol != "" {
		proto = cfg.Protocol
	}

	if scheme != "" {
		proto = scheme
	}

	if proto == "" {
		proto = DefaultProtocol
	}

	s := false
	proto = strings.ToUpper(proto)
	switch proto {
	case HTTP, HTTPS:
		if proto == HTTPS {
			s = true
		}
		cfg.Protocol = HTTP
	case "DNS", GRPC:
		cfg.Protocol = GRPC
	default:
		return false, fmt.Errorf("unsupported protocol")
	}

	return s, nil
}

func initSecrets(u *url.URL, cfg *config.Driver) {
	// Initialize from environment if not set explicitly in the config
	if cfg.ClientID == "" {
		cfg.ClientID = os.Getenv(EnvClientID)
	}

	if cfg.ClientSecret == "" {
		cfg.ClientSecret = os.Getenv(EnvClientSecret)
	}

	if cfg.Token == "" {
		cfg.Token = os.Getenv(EnvToken)
	}

	// In URL config takes precedence over environment and config struct
	// http://{usr}:{pwd}@host...
	if usr := u.User.Username(); usr != "" {
		cfg.ClientID = usr
	}

	if pwd, _ := u.User.Password(); pwd != "" {
		cfg.ClientSecret = pwd
	}

	// http://host/path?client_id={usr}&client_secret={pwd}
	if usr := u.Query().Get("client_id"); usr != "" {
		cfg.ClientID = usr
	}

	if pwd := u.Query().Get("client_secret"); pwd != "" {
		cfg.ClientSecret = pwd
	}

	if tkn := u.Query().Get("token"); tkn != "" {
		cfg.Token = tkn
	}
}

func initConfig(lCfg *config.Driver) (*config.Driver, error) {
	cfg := config.Driver{}
	if lCfg != nil {
		cfg = *lCfg
	}

	if cfg.URL == "" {
		cfg.URL = os.Getenv(EnvURL)
	}
	if cfg.URL == "" {
		cfg.URL = DefaultURL
	}

	URL := cfg.URL
	noScheme := !strings.Contains(URL, "://")
	if noScheme {
		URL = strings.ToLower(DefaultProtocol) + "://" + URL
	}

	u, err := url.Parse(URL)
	if err != nil {
		return nil, err
	}

	if noScheme {
		u.Scheme = ""
	}

	var sec bool
	if sec, err = initProto(u.Scheme, &cfg); err != nil {
		return nil, err
	}

	initSecrets(u, &cfg)

	// Retain only host:port for connection
	cfg.URL = u.Host

	if cfg.TLS == nil && (cfg.ClientID != "" || cfg.ClientSecret != "" || cfg.Token != "" || u.Scheme == "https" || sec) {
		cfg.TLS = &tls.Config{MinVersion: tls.VersionTLS12}
	}

	return &cfg, nil
}

// NewDriver connect to the Tigris instance at the specified URL.
// URL should be in the form: {hostname}:{port}.
func NewDriver(ctx context.Context, cfg *config.Driver) (Driver, error) {
	var (
		drv driverWithOptions
		err error
	)

	cfg, err = initConfig(cfg)
	if err != nil {
		return nil, err
	}

	switch cfg.Protocol {
	case GRPC:
		drv, err = newGRPCClient(ctx, cfg)
	case HTTP:
		drv, err = newHTTPClient(ctx, cfg)
	default:
		err = fmt.Errorf("unsupported protocol")
	}

	if err != nil {
		return nil, err
	}

	wg, ch := startHealthPingLoop(cfg.PingInterval, drv)
	return &driver{driverWithOptions: drv, closeCh: ch, closeWg: wg, cfg: cfg}, nil
}

func startHealthPingLoop(cfgInterval time.Duration, drv driverWithOptions) (*sync.WaitGroup, chan struct{}) {
	var wg sync.WaitGroup
	ch := make(chan struct{})

	wg.Add(1)
	go func() {
		defer wg.Done()

		interval := cfgInterval
		if interval == 0 {
			interval = time.Duration(5) * time.Minute
		}

		t := time.NewTicker(interval)
		defer t.Stop()

		for {
			select {
			case <-t.C:
			case <-ch:
				return
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_, _ = drv.Health(ctx)
			cancel()
		}
	}()
	return &wg, ch
}

func (c *driver) Close() error {
	close(c.closeCh)
	c.closeWg.Wait()

	return c.driverWithOptions.Close()
}
