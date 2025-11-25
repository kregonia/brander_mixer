package server

import (
	"log"
	"net"

	worker_service "github.com/kregonia/brander_mixer/script/rpc_server/worker"
	"google.golang.org/grpc"
)

type WorkerServer struct {
	worker_service.Worker2ControllerServer
}

func WorkerServering(port string) {
	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	// 创建gRPC服务器实例
	s := grpc.NewServer()
	// 注册我们的服务实现到gRPC服务器
	worker_service.RegisterWorker2ControllerServer(s, &WorkerServer{})
	log.Println("gRPC server listening on port " + port)

	// 阻塞等待，直到进程被杀死或调用 `Stop`
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
