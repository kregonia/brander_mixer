package connection

import (
	"context"
	"log"
	"sync"

	brander_service "github.com/kregonia/brander_mixer/script/rpc_server/brander"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type WorkerClients struct {
	conn        []*grpc.ClientConn
	clinet      []brander_service.BranderWorkerStatusClient
	clientCount int
	sync.RWMutex
}

func InitControllerConnection(targets []string) *WorkerClients {
	cc := WorkerClients{
		conn:   make([]*grpc.ClientConn, 0, len(targets)),
		clinet: make([]brander_service.BranderWorkerStatusClient, 0, len(targets)),
	}
	cc.Lock()
	defer cc.Unlock()
	for _, target := range targets {
		conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Fatalf("did not connect: %v", err)
		}
		cc.conn = append(cc.conn, conn)
		// 创建客户端存根
		c := brander_service.NewBranderWorkerStatusClient(conn)
		cc.clinet = append(cc.clinet, c)
	}
	cc.clientCount = len(cc.clinet)
	return &cc
}

func (wc *WorkerClients) Close() {
	wc.Lock()
	defer wc.Unlock()
	if wc.conn != nil {
		for _, conn := range wc.conn {
			conn.Close()
		}
	}
}

func (wc *WorkerClients) GetClientsByIndex(i int) brander_service.BranderWorkerStatusClient {
	wc.RLock()
	defer wc.RUnlock()
	return wc.clinet[i]
}

func (wc *WorkerClients) GetConnsByIndex(i int) *grpc.ClientConn {
	wc.RLock()
	defer wc.RUnlock()
	return wc.conn[i]
}

func (wc *WorkerClients) GetWorkerStatus(ctx context.Context) []*brander_service.MachineStatusResponse {
	statuses := make([]*brander_service.MachineStatusResponse, 0, wc.clientCount)
	for i := 0; i < wc.clientCount; i++ {
		client := wc.GetClientsByIndex(i)
		resp, err := client.Hearting(ctx, &brander_service.HeartingRequest{})
		if err != nil {
			log.Printf("Error getting worker status from client %d: %v", i, err)
			continue
		}
		statuses = append(statuses, resp)
	}
	return statuses
}
