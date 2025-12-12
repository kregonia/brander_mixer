package connection

import (
	"log"
	"sync"

	controller_service "github.com/kregonia/brander_mixer/script/rpc_server/controller"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type WorkerClients struct {
	conns       []*grpc.ClientConn
	clients     []controller_service.BranderWorkerStatusClient
	clientCount int
	sync.RWMutex
}

func InitControllerConnection(targets []string) *WorkerClients {
	cc := WorkerClients{
		conns:   make([]*grpc.ClientConn, 0, len(targets)),
		clients: make([]controller_service.BranderWorkerStatusClient, 0, len(targets)),
	}
	cc.Lock()
	defer cc.Unlock()
	for _, target := range targets {
		conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Fatalf("did not connect: %v", err)
		}
		cc.conns = append(cc.conns, conn)
		// 创建客户端存根
		c := controller_service.NewBranderWorkerStatusClient(conn)
		cc.clients = append(cc.clients, c)
	}
	cc.clientCount = len(cc.clients)
	return &cc
}

func (wc *WorkerClients) Close() {
	wc.Lock()
	defer wc.Unlock()
	if wc.conns != nil {
		for _, conn := range wc.conns {
			conn.Close()
		}
	}
}

func (wc *WorkerClients) GetClientsByIndex(i int) controller_service.BranderWorkerStatusClient {
	wc.RLock()
	defer wc.RUnlock()
	return wc.clients[i]
}

func (wc *WorkerClients) GetConnsByIndex(i int) *grpc.ClientConn {
	wc.RLock()
	defer wc.RUnlock()
	return wc.conns[i]
}
