package server

import (
	"context"
	"fmt"
	"log"
	"net"

	worker_2_controller_service "github.com/kregonia/brander_mixer/script/rpc_server/worker"
	"github.com/kregonia/brander_mixer/widget/holder"
	"google.golang.org/grpc"
)

type ControllerServer struct {
	worker_2_controller_service.Worker2ControllerServer
	HD holder.StatusHolder
}

var (
	ControllerServerInstance = &ControllerServer{HD: *holder.NewStatusHolder()}
)

func ControllerServering(port string) {
	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	// 创建gRPC服务器实例
	s := grpc.NewServer()
	// 注册我们的服务实现到gRPC服务器
	worker_2_controller_service.RegisterWorker2ControllerServer(s, ControllerServerInstance)
	log.Println("gRPC server listening on port " + port)

	// 阻塞等待，直到进程被杀死或调用 `Stop`
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

func (s *ControllerServer) Hearting(ctx context.Context, req *worker_2_controller_service.HeartingRequest) (*worker_2_controller_service.HeartingResponse, error) {
	// todo: 维护status状态集
	fmt.Println("accept the hearting")
	s.HD.AppendStatusByKey(req.Ip, req.GetStatus())
	return &worker_2_controller_service.HeartingResponse{}, nil
}

func (s *ControllerServer) RegistWorker(ctx context.Context, in *worker_2_controller_service.RegistRequest) (*worker_2_controller_service.RegistResponse, error) {
	fmt.Println(in.WorkerId)
	return &worker_2_controller_service.RegistResponse{Success: true}, nil
}
