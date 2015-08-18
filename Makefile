GO=go
#GO=/code/go15/bin/go
REDISSERV=redis-server
REDISCLI=redis-cli
INTSOCK=/tmp/redis-test.sock
VER=0.3.1.4

all: clean test build-dev build

clean:
	rm -f ./build/*

test:
	$(GO) test ./...

test-dev:
	$(GO) test -v -tags 'dev' ./...

test-integration:
	$(REDISSERV) $(PWD)/example/redis.conf
	sleep 1
	$(REDISCLI) -s $(INTSOCK) flushall
	$(GO) test -tags 'integration' ./...
	kill -TERM $$(cat /tmp/redis-test-instance-for-rmux.pid)

test-integration-dev:
	$(REDISSERV) $(PWD)/example/redis.conf
	sleep 1
	$(REDISCLI) -s $(INTSOCK) flushall
	$(GO) test -tags 'integration dev' ./...
	kill -TERM $$(cat /tmp/redis-test-instance-for-rmux.pid)

fmt:
	$(GO) fmt ./...

mkbuild:
	mkdir -p ./build

build: mkbuild
	$(GO) build -o build/rmux ./main

build-all: mkbuild build
	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 $(GO) build -o build/rmux.amd64.$(VER) ./main
	GOOS=linux GOARCH=386 CGO_ENABLED=0 $(GO) build -o build/rmux.386.$(VER) ./main
	GOOS=darwin GOARCH=amd64 CGO_ENABLED=0 $(GO) build -o build/rmux.osx.$(VER) ./main

build-dev: mkbuild
	$(GO) build -tags 'dev' -o build/rmux-dev ./main

build-all-dev: mkbuild build-dev
	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 $(GO) build -tags 'dev' -o build/rmux-linux-amd64-dev ./main
	GOOS=linux GOARCH=386 CGO_ENABLED=0 $(GO) build -tags 'dev' -o build/rmux-linux-386-dev ./main

run-example: build
	./build/rmux -config=./example/config.json -graphite=localhost:8125 -timing

run-example-dev: build-dev
	./build/rmux-dev -config=./example/config.json

run-profile: build
	./build/rmux -config=./example/config.json -cpuProfile=./build/profile.prof

run-example-mux: build
	./build/rmux -config=./example/config-mux.json

.PHONY: clean test test-dev mkbuild build build-all build-dev build-all-dev fmt run-example run-example-dev run-profile test-integration test-integration-dev
