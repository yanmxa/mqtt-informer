.PHONY: build
build:
	go build -o bin/informer cmd/informer/main.go
	go build -o bin/provider cmd/provider/main.go
	go build -o bin/reflector cmd/reflector/main.go

	go build -o bin/agent cmd/agent/main.go
	go build -o bin/manager cmd/manager/main.go

