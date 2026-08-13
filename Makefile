.PHONY: build test test-race vet lint cluster chaos client clean

build:
	go build -o bin/quorumd ./cmd/quorumd
	go build -o bin/quorumctl ./cmd/quorumctl

test:
	go test ./...

test-race:
	go test -race ./...

vet:
	go vet ./...

lint:
	golangci-lint run ./...

# Runs a local N-node cluster (N defaults to 5). Each node's data dir
# and ports are under ./data/nodeN. Placeholder until multi-node wiring
# (Phase 2+) lands; Phase 1 only supports a single node.
cluster: build
	./scripts/cluster.sh

# Runs the chaos-testing harness (Phase 5).
chaos: build
	go run ./cmd/chaos

client: build
	./bin/quorumctl $(ARGS)

clean:
	rm -rf bin data
