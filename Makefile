# 常用开发命令。本地跑请在 WSL 里用 /usr/local/go/bin/go（Windows Go 锁不了 WSL 文件）。

.PHONY: build vet test race cover

build:
	go build ./...

vet:
	go vet ./...

test:
	go test ./...

race:
	go test -race ./...

cover:
	go test -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out | tail -1
