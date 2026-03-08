package connection

import (
	"context"
	"fmt"
	"sync"
	"time"

	logger "github.com/kregonia/brander_mixer/log"
	c2w "github.com/kregonia/brander_mixer/script/rpc_server/controller"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// ---------------------------------------------------------------------------
// WorkerClient 管理到单个 worker 的 gRPC 连接
//
// 封装 Controller2WorkerClient 接口，提供连接生命周期管理和
// 便捷的 RPC 调用方法。
// ---------------------------------------------------------------------------

type WorkerClient struct {
	mu       sync.RWMutex
	workerID string
	addr     string // worker 的 gRPC 地址（host:port）
	conn     *grpc.ClientConn
	client   c2w.Controller2WorkerClient
	lastPing time.Time // 上次 ping 成功时间
	rttMs    int64     // 上次 ping 的 RTT（毫秒）
}

// NewWorkerClient 创建到指定 worker 的 gRPC 客户端连接
func NewWorkerClient(workerID, addr string) (*WorkerClient, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to worker %s at %s: %w", workerID, addr, err)
	}

	client := c2w.NewController2WorkerClient(conn)

	logger.Noticef("[c2w] connected to worker %s at %s", workerID, addr)

	return &WorkerClient{
		workerID: workerID,
		addr:     addr,
		conn:     conn,
		client:   client,
	}, nil
}

// Close 关闭连接
func (wc *WorkerClient) Close() error {
	wc.mu.Lock()
	defer wc.mu.Unlock()
	if wc.conn != nil {
		logger.Noticef("[c2w] closing connection to worker %s at %s", wc.workerID, wc.addr)
		return wc.conn.Close()
	}
	return nil
}

// WorkerID 返回 worker 标识
func (wc *WorkerClient) WorkerID() string {
	return wc.workerID
}

// Addr 返回 worker 的 gRPC 地址
func (wc *WorkerClient) Addr() string {
	return wc.addr
}

// LastRTT 返回上次 Ping 的 RTT（毫秒），0 表示从未 ping 过
func (wc *WorkerClient) LastRTT() int64 {
	wc.mu.RLock()
	defer wc.mu.RUnlock()
	return wc.rttMs
}

// ---------------------------------------------------------------------------
// DispatchTask 向 worker 下发任务分片
// ---------------------------------------------------------------------------

func (wc *WorkerClient) DispatchTask(ctx context.Context, chunk *c2w.TaskChunk) (*c2w.DispatchTaskResponse, error) {
	resp, err := wc.client.DispatchTask(ctx, &c2w.DispatchTaskRequest{
		Chunk: chunk,
	})
	if err != nil {
		logger.Errorf("[c2w] DispatchTask to worker %s failed: %v", wc.workerID, err)
		return nil, err
	}
	if resp.GetAccepted() {
		logger.Noticef("[c2w] DispatchTask accepted by worker %s: taskID=%s", wc.workerID, chunk.GetTaskId())
	} else {
		logger.Warnf("[c2w] DispatchTask rejected by worker %s: taskID=%s reason=%s",
			wc.workerID, chunk.GetTaskId(), resp.GetReason())
	}
	return resp, nil
}

// ---------------------------------------------------------------------------
// CancelTask 取消 worker 上正在执行的任务
// ---------------------------------------------------------------------------

func (wc *WorkerClient) CancelTask(ctx context.Context, taskID, reason string) (*c2w.CancelTaskResponse, error) {
	resp, err := wc.client.CancelTask(ctx, &c2w.CancelTaskRequest{
		TaskId: taskID,
		Reason: reason,
	})
	if err != nil {
		logger.Errorf("[c2w] CancelTask on worker %s failed: taskID=%s err=%v", wc.workerID, taskID, err)
		return nil, err
	}
	return resp, nil
}

// ---------------------------------------------------------------------------
// Ping 探活 + 延迟测量
//
// 发送带时间戳的 ping 请求，计算 RTT 并缓存结果。
// ---------------------------------------------------------------------------

func (wc *WorkerClient) Ping(ctx context.Context) (*c2w.PingResponse, error) {
	sendTime := time.Now()
	sendMs := sendTime.UnixMilli()

	resp, err := wc.client.Ping(ctx, &c2w.PingRequest{
		Timestamp: sendMs,
	})
	if err != nil {
		logger.Errorf("[c2w] Ping worker %s failed: %v", wc.workerID, err)
		return nil, err
	}

	rtt := time.Since(sendTime).Milliseconds()

	wc.mu.Lock()
	wc.lastPing = time.Now()
	wc.rttMs = rtt
	wc.mu.Unlock()

	logger.Noticef("[c2w] Ping worker %s: RTT=%dms, tasks=%d, cpu=%.1f%%, mem=%.1f%%",
		wc.workerID, rtt, resp.GetCurrentTaskCount(),
		resp.GetCpuUsagePercent(), resp.GetMemoryUsagePercent())

	return resp, nil
}

// ---------------------------------------------------------------------------
// ListTasks 查询 worker 上当前所有任务
// ---------------------------------------------------------------------------

func (wc *WorkerClient) ListTasks(ctx context.Context) (*c2w.ListTasksResponse, error) {
	resp, err := wc.client.ListTasks(ctx, &c2w.ListTasksRequest{})
	if err != nil {
		logger.Errorf("[c2w] ListTasks on worker %s failed: %v", wc.workerID, err)
		return nil, err
	}
	return resp, nil
}

// ---------------------------------------------------------------------------
// ReportProgress 向 worker 发送进度查询/确认
// ---------------------------------------------------------------------------

func (wc *WorkerClient) ReportProgress(ctx context.Context, report *c2w.TaskProgressReport) (*c2w.TaskProgressResponse, error) {
	resp, err := wc.client.ReportProgress(ctx, report)
	if err != nil {
		logger.Errorf("[c2w] ReportProgress on worker %s failed: %v", wc.workerID, err)
		return nil, err
	}
	return resp, nil
}

// ===========================================================================
// WorkerClientPool 管理到所有 worker 的 gRPC 连接
//
// 线程安全，根据 workerID 索引。通常在 controller 端全局持有一个实例，
// 当 worker 注册时通过 AddWorker 建立连接，worker 离线时通过
// RemoveWorker 关闭并移除连接。
// ===========================================================================

type WorkerClientPool struct {
	mu      sync.RWMutex
	clients map[string]*WorkerClient // key = workerID
}

// NewWorkerClientPool 创建空的连接池
func NewWorkerClientPool() *WorkerClientPool {
	return &WorkerClientPool{
		clients: make(map[string]*WorkerClient),
	}
}

// AddWorker 建立到 worker 的 gRPC 连接并加入池中
//
// 如果该 workerID 已存在连接，先关闭旧连接再创建新连接。
func (p *WorkerClientPool) AddWorker(workerID, grpcAddr string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// 关闭已有连接（worker 可能重启换了端口）
	if old, exists := p.clients[workerID]; exists {
		logger.Noticef("[WorkerClientPool] replacing existing connection to worker %s", workerID)
		_ = old.Close()
	}

	wc, err := NewWorkerClient(workerID, grpcAddr)
	if err != nil {
		return err
	}

	p.clients[workerID] = wc
	logger.Noticef("[WorkerClientPool] worker %s added (addr=%s), pool size=%d", workerID, grpcAddr, len(p.clients))
	return nil
}

// RemoveWorker 关闭并移除指定 worker 的连接
func (p *WorkerClientPool) RemoveWorker(workerID string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if wc, exists := p.clients[workerID]; exists {
		_ = wc.Close()
		delete(p.clients, workerID)
		logger.Noticef("[WorkerClientPool] worker %s removed, pool size=%d", workerID, len(p.clients))
	}
}

// Get 获取指定 worker 的客户端（不存在返回 nil, false）
func (p *WorkerClientPool) Get(workerID string) (*WorkerClient, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	wc, ok := p.clients[workerID]
	return wc, ok
}

// Size 返回连接池中的 worker 数量
func (p *WorkerClientPool) Size() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.clients)
}

// AllWorkerIDs 返回所有已连接的 workerID 列表
func (p *WorkerClientPool) AllWorkerIDs() []string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	ids := make([]string, 0, len(p.clients))
	for id := range p.clients {
		ids = append(ids, id)
	}
	return ids
}

// CloseAll 关闭所有连接
func (p *WorkerClientPool) CloseAll() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for id, wc := range p.clients {
		_ = wc.Close()
		delete(p.clients, id)
	}
	logger.Noticef("[WorkerClientPool] all connections closed")
}

// ---------------------------------------------------------------------------
// 便捷方法：通过 pool 直接对指定 worker 调用 RPC
// ---------------------------------------------------------------------------

// DispatchTask 向指定 worker 下发任务
func (p *WorkerClientPool) DispatchTask(ctx context.Context, workerID string, chunk *c2w.TaskChunk) (*c2w.DispatchTaskResponse, error) {
	wc, ok := p.Get(workerID)
	if !ok {
		return nil, fmt.Errorf("worker %s not found in connection pool", workerID)
	}
	return wc.DispatchTask(ctx, chunk)
}

// CancelTask 取消指定 worker 上的任务
func (p *WorkerClientPool) CancelTask(ctx context.Context, workerID, taskID, reason string) (*c2w.CancelTaskResponse, error) {
	wc, ok := p.Get(workerID)
	if !ok {
		return nil, fmt.Errorf("worker %s not found in connection pool", workerID)
	}
	return wc.CancelTask(ctx, taskID, reason)
}

// Ping 探活指定 worker
func (p *WorkerClientPool) Ping(ctx context.Context, workerID string) (*c2w.PingResponse, error) {
	wc, ok := p.Get(workerID)
	if !ok {
		return nil, fmt.Errorf("worker %s not found in connection pool", workerID)
	}
	return wc.Ping(ctx)
}

// ListTasks 查询指定 worker 上的任务
func (p *WorkerClientPool) ListTasks(ctx context.Context, workerID string) (*c2w.ListTasksResponse, error) {
	wc, ok := p.Get(workerID)
	if !ok {
		return nil, fmt.Errorf("worker %s not found in connection pool", workerID)
	}
	return wc.ListTasks(ctx)
}

// PingAll 对所有 worker 执行 Ping，返回成功/失败的结果
func (p *WorkerClientPool) PingAll(ctx context.Context) (successes map[string]*c2w.PingResponse, failures map[string]error) {
	ids := p.AllWorkerIDs()
	successes = make(map[string]*c2w.PingResponse, len(ids))
	failures = make(map[string]error)

	var mu sync.Mutex
	var wg sync.WaitGroup

	for _, id := range ids {
		wg.Add(1)
		go func(workerID string) {
			defer wg.Done()
			resp, err := p.Ping(ctx, workerID)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				failures[workerID] = err
			} else {
				successes[workerID] = resp
			}
		}(id)
	}

	wg.Wait()
	return successes, failures
}
