GO ?= go

.PHONY: build test race chaos bench e2e proto vet fmt cluster clean

build:
	$(GO) build -o bin/ ./cmd/...

test:
	$(GO) test ./...

race:
	$(GO) test -race -count=1 ./...

# longer nemesis runs, bump the knobs to hunt for rare interleavings
chaos:
	KV_CHAOS_RUNS=8 KV_CHAOS_DURATION=10s $(GO) test -race -count=1 -timeout 30m -run 'TestChaos' ./internal/testcluster

bench: build
	./bin/kvbench -endpoints=127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003 -mix=mixed

e2e:
	$(GO) test -count=1 -v ./test/e2e

proto:
	protoc -I api/proto --go_out=api/kvpb --go_opt=paths=source_relative \
		--go-grpc_out=api/kvpb --go-grpc_opt=paths=source_relative api/proto/kv.proto

vet:
	$(GO) vet ./...

fmt:
	gofmt -l -w .

# three local nodes in the foreground of three shells is tedious, use compose
cluster:
	docker compose -f deploy/docker-compose.yml up --build

clean:
	rm -rf bin
