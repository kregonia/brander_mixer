package server

import (
	"context"
	"log"
	"net"
	"sync"

	controller_service "github.com/kregonia/brander_mixer/script/rpc_server/controller"
	"google.golang.org/grpc"
)

type ControllerServer struct {
	controller_service.BranderWorkerStatusServer
	M sync.Map
}

var (
	ControllerServerInstance = &ControllerServer{M: sync.Map{}}
)

func ControllerServering(port string) {
	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	// 创建gRPC服务器实例
	s := grpc.NewServer()
	// 注册我们的服务实现到gRPC服务器
	controller_service.RegisterBranderWorkerStatusServer(s, ControllerServerInstance)
	log.Println("gRPC server listening on port " + port)

	// 阻塞等待，直到进程被杀死或调用 `Stop`
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

func (s *ControllerServer) Hearting(context.Context, *controller_service.HeartingRequest) (*controller_service.HeartingResponse, error) {
	// todo: 维护status状态集
	return &controller_service.HeartingResponse{}, nil
}
