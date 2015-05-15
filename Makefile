all: test build-dev build

test:
	go test ./... -tags 'dev'

mkbuild:
	mkdir -p ./build

build: mkbuild
	go build -o build/rmux ./main

build-all: mkbuild build
	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o build/rmux-linux-amd64 ./main
	GOOS=linux GOARCH=386 CGO_ENABLED=0 go build -o build/rmux-linux-386 ./main

build-dev: mkbuild
	go build -tags 'dev' -o build/rmux-dev ./main

build-all-dev: mkbuild build-dev
	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -tags 'dev' -o build/rmux-linux-amd64-dev ./main
	GOOS=linux GOARCH=386 CGO_ENABLED=0 go build -tags 'dev' -o build/rmux-linux-386-dev ./main
