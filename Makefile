CMDDIR = ./cmd
BINDIR = ./bin
PKGDIR = ./pkg
LOG_COURIER_CMDDIR = $(CMDDIR)/log-courier
LOG_COURIER_BIN = $(BINDIR)/log-courier
ENSURE_SERVICE_USER_CMDDIR = $(CMDDIR)/ensureServiceUser
ENSURE_SERVICE_USER_BIN = $(BINDIR)/ensureServiceUser
COVER_COMMONFLAGS = -coverpkg "./..."
COVDATA_DIR = $(PWD)/covdatafiles

.PHONY: all clean test test-coverage lint fmt coverage-report

all:
	mkdir -p $(BINDIR)
	CGO_ENABLED=0 go build $(COVER_BUILDFLAGS) -o $(LOG_COURIER_BIN) $(LOG_COURIER_CMDDIR)/.
	CGO_ENABLED=0 go build -o $(ENSURE_SERVICE_USER_BIN) $(ENSURE_SERVICE_USER_CMDDIR)/.

clean:
	rm -rf $(BINDIR)
	rm -f cover.out integration-coverage.out
	rm -rf $(COVDATA_DIR)

test: all
	mkdir -p $(COVDATA_DIR)
	COVDATA_DIR=$(COVDATA_DIR) go test $(COVER_TESTFLAGS) -v ./...

test-coverage: COVER_BUILDFLAGS=$(COVER_COMMONFLAGS)
test-coverage: COVER_TESTFLAGS=$(COVER_COMMONFLAGS) -coverprofile cover.out
test-coverage: test
	go tool covdata textfmt -i $(COVDATA_DIR) -o integration-coverage.out

lint:
	golangci-lint run

fmt:
	go fmt ./...

coverage-report: test-coverage
	go tool cover -html=cover.out -o unit-coverage.html
	go tool cover -html=integration-coverage.out -o integration-coverage.html
