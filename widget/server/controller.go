package server

import (
	"log"
	"net"

	controller_service "github.com/kregonia/brander_mixer/script/rpc_server/controller"
	"google.golang.org/grpc"
)

type server struct {
	controller_service.BranderWorkerStatusServer
}

func ControllerServering(port string) {
	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	// 创建gRPC服务器实例
	s := grpc.NewServer()
	// 注册我们的服务实现到gRPC服务器
	controller_service.RegisterBranderWorkerStatusServer(s, &server{})
	log.Println("gRPC server listening on port " + port)

	// 阻塞等待，直到进程被杀死或调用 `Stop`
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
