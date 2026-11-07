.PHONY: fmt vet test example mermaid spec

fmt:
	gofmt -w .

vet:
	go vet ./...

test:
	go test -race ./...

example:
	go run ./examples/codingagent

mermaid:
	go run ./examples/codingagent -mermaid > docs/coding-agent.mmd

spec:
	go run ./examples/codingagent -spec > docs/coding-agent.json
