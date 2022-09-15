VERSION=$(shell git describe --tags --always)
GO_SRC=$(shell find . -name "*.go" -not -name "*_test.go")
API_DIR=api
V=v1
GEN_DIR=${API_DIR}/server/${V}
PROTO_DIR=${API_DIR}/proto/server/${V}

BUILD_PARAM=-tags=release -ldflags "-X 'github.com/tigrisdata/tigris-client-go/main.Version=$(VERSION)'" $(shell printenv BUILD_PARAM)
TEST_PARAM=-cover -race -tags=test $(shell printenv TEST_PARAM)

all: generate ${GO_SRC}
	#go build ${BUILD_PARAM} .

${PROTO_DIR}/%.proto:
	git submodule update --init --recursive --rebase

# separated from above to avoid error of mixing implicit and normal rules
${PROTO_DIR}/openapi.yaml:
	git submodule update --init --recursive --rebase

upgrade_api:
	git submodule update --remote --recursive --rebase

# generate GRPC client/server, openapi spec, http server
${GEN_DIR}/%.pb.go ${GEN_DIR}/%.pb.gw.go: ${PROTO_DIR}/%.proto
	make -C api/proto generate GEN_DIR=../../${GEN_DIR} API_DIR=..

# generate Go HTTP client from openapi spec
${API_DIR}/client/${V}/api/http.go: ${PROTO_DIR}/openapi.yaml
	mkdir -p ${API_DIR}/client/${V}/api
	/bin/bash scripts/fix_openapi.sh ${PROTO_DIR}/openapi.yaml /tmp/openapi.yaml
	oapi-codegen --old-config-style -package api -generate "client, types, spec" \
		-o ${API_DIR}/client/${V}/api/http.go \
		/tmp/openapi.yaml


generate: ${GEN_DIR}/api.pb.go ${GEN_DIR}/health.pb.go ${GEN_DIR}/admin.pb.go ${GEN_DIR}/user.pb.go ${GEN_DIR}/auth.pb.go ${GEN_DIR}/observability.pb.go ${API_DIR}/client/${V}/api/http.go

mock/api/grpc.go mock/driver.go: ${GEN_DIR}/api.pb.go ${GEN_DIR}/user.pb.go ${GEN_DIR}/auth.pb.go
	mkdir -p mock/api
	mockgen -package mock -destination mock/driver.go github.com/tigrisdata/tigris-client-go/driver \
		Driver,Tx,Database,Iterator,SearchResultIterator,EventIterator
	mockgen -package api -destination mock/api/grpc.go github.com/tigrisdata/tigris-client-go/api/server/v1 TigrisServer,AuthServer,UserServer

mock: mock/api/grpc.go mock/driver.go

lint:
	yq --exit-status 'tag == "!!map" or tag== "!!seq"' .github/workflows/*.yaml
	shellcheck scripts/*
	golangci-lint run

go.sum: go.mod generate mock
	go mod download

test: go.sum generate mock
	go test $(TEST_PARAM) ./...
