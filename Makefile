.PHONY: build
build:
	go build -o bin/sender cmd/sender/main.go
	go build -o bin/receiver cmd/receiver/main.go

