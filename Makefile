.DEFAULT_GOAL := all
.PHONY: integration end_to_end

CBCOPY=cbcopy
COPYHELPER=cbcopy_helper

GINKGO_FLAGS := -r --keep-going --no-color
GIT_VERSION := $(shell git describe --tags | perl -pe 's/(.*)-([0-9]*)-(g[0-9a-f]*)/\1+dev.\2.\3/')
VERSION_STR="-X github.com/cloudberrydb/cbcopy/utils.Version=$(GIT_VERSION)"

SUBDIRS_HAS_UNIT= meta/builtin/ testutils/ utils/
GINKGO=$(shell go env GOPATH)/bin/ginkgo
GOIMPORTS=$(shell go env GOPATH)/bin/goimports

all: build

depend:
	$(GO_ENV) go mod download

$(GINKGO):
	$(GO_ENV) go install github.com/onsi/ginkgo/v2/ginkgo

$(GOIMPORTS):
	$(GO_ENV) go install golang.org/x/tools/cmd/goimports

format: $(GOIMPORTS)
	$(GOIMPORTS) -w $(shell find . -type f -name '*.go' -not -path "./vendor/*")

unit: $(GINKGO)
	@echo "Running unit tests..."
	$(GINKGO) $(GINKGO_FLAGS) $(SUBDIRS_HAS_UNIT)

integration: $(GINKGO)
	@echo "Running integration tests..."
	$(GINKGO) $(GINKGO_FLAGS) integration

test: unit integration

end_to_end: $(GINKGO) install
	@echo "Running end to end tests..."
	$(GINKGO) $(GINKGO_FLAGS) end_to_end

build:
	$(GO_ENV) go build -tags '$(CBCOPY)' $(GOFLAGS) -o $(CBCOPY) -ldflags $(VERSION_STR)
	$(GO_ENV) go build -tags '$(COPYHELPER)' $(GOFLAGS) -o $(COPYHELPER) -ldflags $(VERSION_STR)

install: build
	cp $(CBCOPY) $(GPHOME)/bin
	cp $(COPYHELPER) $(GPHOME)/bin

clean:
	rm -f $(CBCOPY)
	rm -f $(COPYHELPER)
