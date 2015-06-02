GO=go
#GO=/code/go15/bin/go

all: clean test build-dev build

clean:
	rm -f ./build/*

test:
	$(GO) test ./...

test-dev:
	$(GO) test -tags 'dev' ./...

fmt:
	$(GO) fmt ./...

mkbuild:
	mkdir -p ./build

build: mkbuild
	$(GO) build -o build/rmux ./main

build-all: mkbuild build
	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 $(GO) build -o build/rmux-linux-amd64 ./main
	GOOS=linux GOARCH=386 CGO_ENABLED=0 $(GO) build -o build/rmux-linux-386 ./main

build-dev: mkbuild
	$(GO) build -tags 'dev' -o build/rmux-dev ./main

build-all-dev: mkbuild build-dev
	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 $(GO) build -tags 'dev' -o build/rmux-linux-amd64-dev ./main
	GOOS=linux GOARCH=386 CGO_ENABLED=0 $(GO) build -tags 'dev' -o build/rmux-linux-386-dev ./main

run-example: build
	./build/rmux -config=./example/config.json

run-example-dev: build-dev
	./build/rmux-dev -config=./example/config.json

run-profile: build
	./build/rmux -config=./example/config.json -cpuProfile=./build/profile.prof

.PHONY: clean test test-dev mkbuild build build-all build-dev build-all-dev fmt run-example run-example-dev run-profile
