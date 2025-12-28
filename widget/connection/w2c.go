package connection

import (
	"context"
	"log"
	"time"

	logger "github.com/kregonia/brander_mixer/log"
	worker_2_controller_service "github.com/kregonia/brander_mixer/script/rpc_server/worker"
	"github.com/kregonia/brander_mixer/widget/parameter"
	"github.com/kregonia/brander_mixer/widget/status"
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

func (cc *ControllerClient) RegistWorker2Controller(ctx context.Context, workerID string, password string) bool {
	tryTimes := 0
	for {
		tryTimes++
		if tryTimes > 5 {
			logger.Errorf("failed to regist worker to controller after %d times\n", tryTimes-1)
			break
		}
		response, err := cc.client.RegistWorker(
			ctx,
			&worker_2_controller_service.RegistRequest{
				Ip: workerID,
				// todo: WokerInfo 待补充
				Info: &worker_2_controller_service.WorkerInfo{},
			},
		)
		if err != nil {
			logger.Errorf("could not get worker status: %v\n", err)
		}
		if response.GetSuccess() {
			return true
		}
	}
	return false
}

func (cc *ControllerClient) SendHearting(ctx context.Context, ip string) {
	ticker := time.NewTicker(time.Second * time.Duration(parameter.DefaultIntervalSeconds))
	for range ticker.C {
		status, err := status.GetWorkerStatus()
		if err != nil {
			logger.Errorf("[w2c SendHearting] get worker status failed,err:%v\n", err)
			continue
		}
		res, err := cc.client.Hearting(ctx, &worker_2_controller_service.HeartingRequest{Ip: ip, Status: status})
		if err != nil {
			logger.Errorf("[w2c SendHearting] send hearting failed,err:%v\n", err)
		}
		if res == nil {
			logger.Errorf("[w2c SendHearting] send hearting failed,res is nil\n")
		}
	}
}
