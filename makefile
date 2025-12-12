gen:
	protoc --proto_path=script/proto --go_out=script/rpc_server/worker --go_opt=paths=source_relative --go-grpc_out=script/rpc_server/worker --go-grpc_opt=paths=source_relative script/proto/worker.proto 
	protoc --proto_path=script/proto --go_out=script/rpc_server/controller --go_opt=paths=source_relative --go-grpc_out=script/rpc_server/controller --go-grpc_opt=paths=source_relative script/proto/controller.proto 
init:
	go mod init github.com/kregonia/brander_mixer
	go mod tidy