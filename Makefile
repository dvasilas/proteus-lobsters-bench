REPO := 127.0.0.1:5000/lobsters-bench
TAG := $(shell git log -1 --pretty=%H | cut -c1-8)
IMG := ${REPO}:${TAG}
BIN_DIR  := ${CURDIR}/bin
PKGS     := $(or $(PKG),$(shell env GO111MODULE=on go list ./...))
TESTPKGS := $(shell env GO111MODULE=on go list -f \
			'{{ if or .TestGoFiles .XTestGoFiles }}{{ .ImportPath }}{{ end }}' \
			$(PKGS))

$(PROTOC_CMD):
ifeq ($(UNAME), Darwin)
	https://github.com/protocolbuffers/protobuf/releases/download/v3.6.1/protoc-3.6.1-osx-x86_64.zip
	unzip /tmp/protoc.zip -d "$(HOME)/protoc"
endif
ifeq ($(UNAME), Linux)
	curl -L https://github.com/google/protobuf/releases/download/v3.6.1/protoc-3.6.1-linux-x86_64.zip -o /tmp/protoc.zip
	unzip /tmp/protoc.zip -d "$(HOME)/protoc"
endif

export GO111MODULE=on

.PHONY: bench
## bench: build the benchmark
bench:
	@echo "Building..."
	@go build -o ${BIN_DIR}/benchmark cmd/benchmark/main.go

.PHONY: fmt
## fmt: runs gofmt on all source files
fmt: ; $(info $(M) running gofmt…)
	@go fmt $(PKGS)

.PHONY: test
## test: run tests
test: fmt ; $(info $(M) running $(NAME:%=% )tests…)
	@go test $(TESTPKGS)

.PHONY: image-build
## image-build:
image-build:
	docker build -t lobsters-bench .

.PHONY: image-push-registry
## image-push-registry: Pushes the image to the local registry
image-push-registry:
	docker tag lobsters-bench ${IMG}
	docker push ${IMG}

.PHONY: help
## help: Prints this help message
help:
	@echo "Usage: \n"
	@sed -n 's/^##//p' ${MAKEFILE_LIST} | column -t -s ':' |  sed -e 's/^/ /'
