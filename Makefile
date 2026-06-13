GOBIN ?= $(HOME)/go/bin
GO    ?= $(HOME)/go/pkg/mod/golang.org/toolchain@v0.0.1-go1.25.0.linux-amd64/bin/go
BUF   ?= $(GOBIN)/buf

.PHONY: generate build test lint clean

generate:
	$(BUF) generate

build: generate
	$(GO) build ./...

test:
	$(GO) test -timeout 60s ./...

buf-breaking:
	$(BUF) breaking --against '.git#branch=master,subdir=proto'

clean:
	rm -f gen/*.go
