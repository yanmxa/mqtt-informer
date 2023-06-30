.PHONY: build
build:
	go build -o bin/informer cmd/informer/main.go
	go build -o bin/provider cmd/provider/main.go

