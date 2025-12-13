package connection

import (
	"context"
	"log"

	worker_2_controller_service "github.com/kregonia/brander_mixer/script/rpc_server/worker"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type ControllerClient struct {
	conn   *grpc.ClientConn
	client worker_2_controller_service.Worker2ControllerClient
}

func InitWorkerConnection(target string) *ControllerClient {
	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	client := worker_2_controller_service.NewWorker2ControllerClient(conn)
	return &ControllerClient{
		conn:   conn,
		client: client,
	}
}
func (cc *ControllerClient) Close() {
	if cc.conn != nil {
		cc.conn.Close()
	}
}

func (cc *ControllerClient) GetClient() worker_2_controller_service.Worker2ControllerClient {
	return cc.client
}

func (cc *ControllerClient) GetConn() *grpc.ClientConn {
	return cc.conn
}

func (cc *ControllerClient) RegistWorker2Controller(ctx context.Context, workerID string, password string) *worker_2_controller_service.RegistResponse {
	response, err := cc.client.RegistWorker(ctx, &worker_2_controller_service.RegistRequest{WorkerId: workerID, Password: password})
	if err != nil {
		log.Fatalf("could not get worker status: %v", err)
	}
	return response
}
