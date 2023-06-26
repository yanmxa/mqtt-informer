.PHONY: build
build:
	go build -o bin/source cmd/source/main.go
	go build -o bin/controlplane cmd/controlplane/main.go

