gen:
	protoc --proto_path=script/proto --go_out=script/rpc_server/worker --go_opt=paths=source_relative --go-grpc_out=script/rpc_server/worker --go-grpc_opt=paths=source_relative script/proto/worker.proto
	protoc --proto_path=script/proto --go_out=script/rpc_server/controller --go_opt=paths=source_relative --go-grpc_out=script/rpc_server/controller --go-grpc_opt=paths=source_relative script/proto/controller.proto

init:
	go mod init github.com/kregonia/brander_mixer
	go mod tidy

tidy:
	go mod tidy

build-controller:
	go build -o bin/controller cmd/controller/main.go

build-worker:
	go build -o bin/worker cmd/worker/main.go

build-reader:
	go build -o bin/disk_status_reader cmd/disk_status_reader/main.go

build-benchmark:
	go build -o bin/benchmark cmd/benchmark/main.go

build: build-controller build-worker build-reader build-benchmark

clean:
	rm -rf bin/

all: clean gen tidy build

run-controller:
	./bin/controller -port 50051

run-worker:
	./bin/worker -t localhost:50051

run-benchmark:
	./bin/benchmark -i $(VIDEO) -workers $(WORKERS) -codec $(CODEC) -preset $(PRESET) -crf $(CRF)

# Default benchmark variables
VIDEO ?= input.mp4
WORKERS ?= 0
CODEC ?= libx264
PRESET ?= fast
CRF ?= 23

.PHONY: gen init tidy build-controller build-worker build-reader build-benchmark build clean all run-controller run-worker run-benchmark
