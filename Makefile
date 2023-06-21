.PHONY: build
build:
	go build -o bin/manager cmd/manager/main.go
	go build -o bin/agent cmd/agent/main.go

