.PHONY: build test test-race vet lint chaos client clean

build:
	go build -o bin/quorumd ./cmd/quorumd
	go build -o bin/quorumctl ./cmd/quorumctl
	go build -o bin/chaos ./cmd/chaos

test:
	go test ./...

test-race:
	go test -race ./...

vet:
	go vet ./...

lint:
	golangci-lint run ./...

# Runs a chaos scenario against real quorumd processes. Pass the
# scenario (and any flags) via ARGS, e.g.:
#   make chaos ARGS="leader-crash"
#   make chaos ARGS="sustained -duration 5m"
# See `./bin/chaos` with no args for the full scenario/flag list.
chaos: build
	./bin/chaos $(ARGS)

client: build
	./bin/quorumctl $(ARGS)

clean:
	rm -rf bin data
