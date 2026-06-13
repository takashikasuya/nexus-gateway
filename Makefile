GOBIN ?= $(HOME)/go/bin
GO    ?= go
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
