VERSION=$(git describe --tags)
GO_SRC=$(shell find . -name "*.go" -not -name "*_test.go")
API_DIR=api
V=v1
GEN_DIR=${API_DIR}/server/${V}

BUILD_PARAM=-tags=release -ldflags "-X 'main.Version=$(VERSION)'" $(shell printenv BUILD_PARAM)
TEST_PARAM=-cover -race -tags=test $(shell printenv TEST_PARAM)

all: ${GO_SRC}
	#go build ${BUILD_PARAM} .

mock:
	mkdir -p mock
	mockgen -source=api/server/v1/user_grpc.pb.go -package=mock >mock/user_grpc.go

lint:
	yq --exit-status 'tag == "!!map" or tag== "!!seq"' .github/workflows/*.yaml
	shellcheck scripts/*
	golangci-lint run

go.sum: go.mod mock
	go mod download

test: go.sum mock lint
	go test $(TEST_PARAM) ./...

release:
	sh scripts/make-release.sh alpha

%-release:
	sh scripts/make-release.sh $(*F)

clean:
	rm -rf mock
