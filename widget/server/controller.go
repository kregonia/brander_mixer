package server

import (
	"context"
	"log"
	"net"

	logger "github.com/kregonia/brander_mixer/log"
	worker_2_controller_service "github.com/kregonia/brander_mixer/script/rpc_server/worker"
	"github.com/kregonia/brander_mixer/widget/connection"
	"github.com/kregonia/brander_mixer/widget/holder"
	"github.com/kregonia/brander_mixer/widget/nodetable"
	"github.com/kregonia/brander_mixer/widget/scheduler"
	"google.golang.org/grpc"
)

// ---------------------------------------------------------------------------
// Controller gRPC Server
// ---------------------------------------------------------------------------

type ControllerServer struct {
	worker_2_controller_service.UnimplementedWorker2ControllerServer
	HD         holder.StatusHolder
	NodeTable  *nodetable.NodeTable
	ClientPool *connection.WorkerClientPool
}

var (
	GlobalNodeTable  = nodetable.NewNodeTable()
	GlobalScheduler  = scheduler.NewScheduler(nil)
	GlobalClientPool = connection.NewWorkerClientPool()
	_                = func() int {
		GlobalScheduler.SetNodeTable(GlobalNodeTable)
		return 0
	}()
	ControllerServerInstance = &ControllerServer{
		HD:         *holder.NewStatusHolder(),
		NodeTable:  GlobalNodeTable,
		ClientPool: GlobalClientPool,
	}
	RegisterInstance = holder.NewWorkerAliveSecrets()
)

func ControllerServering(port string) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 启动增强版自愈巡检：心跳超时检测 + 滑动窗口可靠性趋势预测
	GlobalNodeTable.StartHealingLoopWithPrediction(ctx,
		// onNodeOffline: 节点离线后的任务迁移回调
		func(workerID string, tasks []string) {
			logger.Warnf("[TaskMigration] node %s offline, reassigning %d tasks: %v", workerID, len(tasks), tasks)
			results, errs := GlobalScheduler.ReassignTasks(workerID, tasks)
			for taskID, result := range results {
				logger.Noticef("[TaskMigration] task %s reassigned to node %s (score=%.4f)", taskID, result.WorkerID, result.Score)
			}
			for taskID, err := range errs {
				logger.Errorf("[TaskMigration] task %s reassignment failed: %v", taskID, err)
			}
			// 移除离线 worker 的 c2w 连接
			GlobalClientPool.RemoveWorker(workerID)
		},
		// onDeclining: 节点可靠性下降时的 speculative execution 回调
		func(workerID string, tasks []string, probability float64) {
			logger.Warnf("[SpeculativeExec] node %s declining (offlineProb=%.2f), tasks=%v", workerID, probability, tasks)
			specResults, specErrs := GlobalScheduler.SpeculativeExecute(workerID, tasks, probability)
			for _, spec := range specResults {
				logger.Noticef("[SpeculativeExec] task %s: speculative copy %s dispatched to worker %s (score=%.4f)",
					spec.TaskID, spec.ReplicaTaskID, spec.ReplicaWorkerID, spec.ReplicaScore)
				// TODO: 通过 GlobalClientPool 向 ReplicaWorkerID 实际下发 speculative 任务副本
			}
			for _, err := range specErrs {
				logger.Warnf("[SpeculativeExec] speculative scheduling error: %v", err)
			}
		},
	)

	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	s := grpc.NewServer()
	worker_2_controller_service.RegisterWorker2ControllerServer(s, ControllerServerInstance)
	log.Printf("✅ controller gRPC server listening on port %s", port)
	log.Printf("✅ healing loop started with prediction (heartbeat timeout=%v, offline timeout=%v, window=%d)",
		nodetable.HeartbeatTimeout, nodetable.OfflineTimeout, nodetable.ReliabilityWindowSize)
	log.Printf("✅ scheduler initialized (weights: α=%.2f β=%.2f γ=%.2f)",
		scheduler.DefaultWeights.Alpha, scheduler.DefaultWeights.Beta, scheduler.DefaultWeights.Gamma)
	log.Printf("✅ worker client pool initialized")

	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

// ---------------------------------------------------------------------------
// RegistWorker — 修复原来的逻辑 bug
//
// 原代码：先 SetSecret → 再 GetSecret → exist==true → 返回 success=false
// 这导致永远注册失败。
//
// 修复后逻辑：
//  1. 检查是否已注册（已有 secret）→ 如果已注册返回已有 secret
//  2. 未注册 → SetSecret → 获取 → 返回
//  3. 注册成功后建立 controller → worker 的 gRPC 反向连接
// ---------------------------------------------------------------------------

func (s *ControllerServer) RegistWorker(ctx context.Context, in *worker_2_controller_service.RegistRequest) (*worker_2_controller_service.RegistResponse, error) {
	workerID := in.GetIp()
	workerGrpcAddr := in.GetWorkerGrpcAddr()
	logger.Noticef("[RegistWorker] registration request from worker=%s, grpc_addr=%s", workerID, workerGrpcAddr)

	// 检查是否已经注册过
	if existingSecret, exists := RegisterInstance.GetSecret(workerID); exists {
		logger.Noticef("[RegistWorker] worker=%s already registered, returning existing secret", workerID)
		// 更新节点信息（设备可能重启但 IP 没变）
		s.NodeTable.Register(workerID, in.GetInfo(), workerGrpcAddr)
		// 重建 c2w 连接（worker 重启后端口可能变了）
		if workerGrpcAddr != "" {
			if err := s.ClientPool.AddWorker(workerID, workerGrpcAddr); err != nil {
				logger.Errorf("[RegistWorker] failed to establish c2w connection to worker=%s at %s: %v", workerID, workerGrpcAddr, err)
			} else {
				logger.Noticef("[RegistWorker] c2w connection re-established to worker=%s at %s", workerID, workerGrpcAddr)
			}
		}
		return &worker_2_controller_service.RegistResponse{
			Success: true,
			Secret:  existingSecret,
		}, nil
	}

	// 首次注册：生成 secret
	RegisterInstance.SetSecret(workerID)
	secret, ok := RegisterInstance.GetSecret(workerID)
	if !ok {
		logger.Errorf("[RegistWorker] failed to generate secret for worker=%s", workerID)
		return &worker_2_controller_service.RegistResponse{
			Success: false,
			Secret:  "",
		}, nil
	}

	// 写入节点表（包括 gRPC 地址）
	s.NodeTable.Register(workerID, in.GetInfo(), workerGrpcAddr)

	// 建立 controller → worker 的 gRPC 反向连接
	if workerGrpcAddr != "" {
		if err := s.ClientPool.AddWorker(workerID, workerGrpcAddr); err != nil {
			logger.Errorf("[RegistWorker] failed to establish c2w connection to worker=%s at %s: %v", workerID, workerGrpcAddr, err)
			// 不阻止注册成功，c2w 连接可以后续重建
		} else {
			logger.Noticef("[RegistWorker] c2w connection established to worker=%s at %s", workerID, workerGrpcAddr)
		}
	} else {
		logger.Warnf("[RegistWorker] worker=%s did not provide grpc_addr, c2w connection skipped", workerID)
	}

	logger.Noticef("[RegistWorker] worker=%s registered successfully", workerID)
	return &worker_2_controller_service.RegistResponse{
		Success: true,
		Secret:  secret,
	}, nil
}

// ---------------------------------------------------------------------------
// Hearting — 增加 NodeTable 更新
// ---------------------------------------------------------------------------

func (s *ControllerServer) Hearting(ctx context.Context, req *worker_2_controller_service.HeartingRequest) (*worker_2_controller_service.HeartingResponse, error) {
	workerID := req.GetIp()

	// 校验 secret
	if !RegisterInstance.CompareSecret(workerID, req.GetSecret()) {
		logger.Warnf("[Hearting] secret mismatch for worker=%s, rejecting heartbeat", workerID)
		return &worker_2_controller_service.HeartingResponse{Success: false}, nil
	}

	// 更新节点表（心跳时间 + 最新状态）
	s.NodeTable.UpdateHeartbeat(workerID, req.GetStatus())

	// 更新原有的 StatusHolder（保持落盘逻辑不变）
	go s.HD.AppendStatusByKey(workerID, req.GetStatus())

	return &worker_2_controller_service.HeartingResponse{Success: true}, nil
}

// ---------------------------------------------------------------------------
// ReportTaskStatus — 任务状态上报（stub，为后续任务调度做准备）
// ---------------------------------------------------------------------------

func (s *ControllerServer) ReportTaskStatus(ctx context.Context, req *worker_2_controller_service.TaskReportRequest) (*worker_2_controller_service.TaskReportResponse, error) {
	workerID := req.GetTaskPortId()
	logger.Noticef("[ReportTaskStatus] received task report from worker=%s, %d task(s)", workerID, len(req.GetDetails()))

	// TODO Phase B: 将任务完成状态反馈给 scheduler，触发后续任务分配
	// 目前仅记录日志，返回空的任务分配列表
	return &worker_2_controller_service.TaskReportResponse{
		Received: true,
		Tasks:    []*worker_2_controller_service.TaskDistribution{},
	}, nil
}
