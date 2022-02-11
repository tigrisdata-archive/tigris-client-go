VERSION=1.0.0
GIT_HASH=$(shell [ ! -d .git ] || git rev-parse --short HEAD)
GO_SRC=$(shell find . -name "*.go" -not -name "*_test.go")
API_DIR=api
V=v1
GEN_DIR=${API_DIR}/server/${V}

BUILD_PARAM=-tags=release -ldflags "-X 'main.Version=$(VERSION)' -X 'main.BuildHash=$(GIT_HASH)'" $(shell printenv BUILD_PARAM)
TEST_PARAM=-cover -race -tags=test $(shell printenv TEST_PARAM)

all: generate ${GO_SRC}
	#go build ${BUILD_PARAM} .

${GEN_DIR}/%.proto ${GEN_DIR}/%_openapi.yaml:
	git submodule update --init --recursive --rebase

upgrade_api:
	git submodule update --remote --recursive --rebase

# generate GRPC client/server, openapi spec, http server
${GEN_DIR}/%.pb.go: ${GEN_DIR}/%.proto
	protoc -Iapi --go_out=${API_DIR} --go_opt=paths=source_relative \
		--go-grpc_out=${API_DIR} --go-grpc_opt=require_unimplemented_servers=false,paths=source_relative \
		--grpc-gateway_out=${API_DIR} --grpc-gateway_opt=paths=source_relative,allow_delete_body=true \
		$<

# generate Go HTTP client from openapi spec
${API_DIR}/client/${V}/%/http.go: ${GEN_DIR}/%_openapi.yaml
	/bin/bash scripts/fix_openapi.sh ${GEN_DIR}/$(*F)_openapi.yaml /tmp/$(*F)_openapi.yaml
	mkdir -p ${API_DIR}/client/${V}/$(*F)
	oapi-codegen -package api -generate "client, types, spec" \
		-o ${API_DIR}/client/${V}/$(*F)/http.go \
		/tmp/$(*F)_openapi.yaml

generate: ${GEN_DIR}/user.pb.go ${GEN_DIR}/health.pb.go ${API_DIR}/client/${V}/user/http.go

mock: generate
	mkdir -p mock
	mockgen -source=api/server/v1/user_grpc.pb.go -package=mock >mock/user_grpc.go

lint:
	shellcheck scripts/*
	# golangci-lint run #FIXME: doesn't work with go1.18beta1

go.sum: go.mod mock generate
	go mod download

test: go.sum generate mock lint
	go test $(TEST_PARAM) ./...

clean:
	rm -f api/server/${V}/*.pb.go \
		api/server/${V}/*.pb.gw.go \
		api/client/${V}/*/http.go
	rm -rf mock
	find api -type d -empty -delete
