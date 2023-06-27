.PHONY: build
build:
	go build -o bin/syncer cmd/syncer/main.go
	go build -o bin/controlplane cmd/controlplane/main.go

