package server

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	logger "github.com/kregonia/brander_mixer/log"
	c2w "github.com/kregonia/brander_mixer/script/rpc_server/controller"
	"github.com/kregonia/brander_mixer/widget/executor"
	"google.golang.org/grpc"

	"net"

	"github.com/shirou/gopsutil/v4/cpu"
	"github.com/shirou/gopsutil/v4/mem"
)

// ---------------------------------------------------------------------------
// WorkerServer 实现 Controller2Worker gRPC 服务端
//
// 运行在 worker 节点上，接收来自 controller 的任务下发、取消、
// ping 探活、进度查询等请求，内部委托给 Executor 执行。
// ---------------------------------------------------------------------------

type WorkerServer struct {
	c2w.UnimplementedController2WorkerServer

	mu       sync.RWMutex
	workerID string
	exec     *executor.Executor
	grpcAddr string // 本 server 的监听地址（host:port）
}

// NewWorkerServer 创建 WorkerServer 实例
//
// workerID: 本节点的唯一标识（hostname@IP）
// exec:     本地 Executor 实例（可以为 nil，将在启动时延迟创建）
func NewWorkerServer(workerID string, exec *executor.Executor) *WorkerServer {
	return &WorkerServer{
		workerID: workerID,
		exec:     exec,
	}
}

// SetExecutor 设置或替换底层 Executor（用于延迟初始化场景）
func (ws *WorkerServer) SetExecutor(exec *executor.Executor) {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	ws.exec = exec
}

// GrpcAddr 返回 worker gRPC server 的监听地址
func (ws *WorkerServer) GrpcAddr() string {
	ws.mu.RLock()
	defer ws.mu.RUnlock()
	return ws.grpcAddr
}

// ---------------------------------------------------------------------------
// WorkerServering 启动 Worker 侧 Controller2Worker gRPC 服务
//
// port: 监听端口号（如 "50052"）。传 "0" 由系统分配空闲端口。
// 返回实际监听地址（host:port）以便注册时上报给 controller。
// ---------------------------------------------------------------------------

func WorkerServering(ws *WorkerServer, port string) (string, error) {
	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		return "", fmt.Errorf("failed to listen on port %s: %w", port, err)
	}

	// 获取实际监听地址（在 port=0 时尤其重要）
	actualAddr := lis.Addr().String()
	ws.mu.Lock()
	ws.grpcAddr = actualAddr
	ws.mu.Unlock()

	s := grpc.NewServer()
	c2w.RegisterController2WorkerServer(s, ws)

	logger.Noticef("[WorkerServer] Controller2Worker gRPC server listening on %s", actualAddr)

	// 非阻塞启动：在 goroutine 中 serve
	go func() {
		if err := s.Serve(lis); err != nil {
			logger.Errorf("[WorkerServer] gRPC serve error: %v", err)
		}
	}()

	return actualAddr, nil
}

// ---------------------------------------------------------------------------
// DispatchTask — controller 下发任务分片
//
// 将 proto TaskChunk 转化为 executor.ExecTask 并提交给本地 Executor。
// ---------------------------------------------------------------------------

func (ws *WorkerServer) DispatchTask(ctx context.Context, req *c2w.DispatchTaskRequest) (*c2w.DispatchTaskResponse, error) {
	ws.mu.RLock()
	exec := ws.exec
	ws.mu.RUnlock()

	if exec == nil {
		logger.Warnf("[WorkerServer] DispatchTask rejected: executor not initialized")
		return &c2w.DispatchTaskResponse{
			Accepted: false,
			Reason:   "executor not initialized on this worker",
			WorkerId: ws.workerID,
		}, nil
	}

	chunk := req.GetChunk()
	if chunk == nil {
		return &c2w.DispatchTaskResponse{
			Accepted: false,
			Reason:   "empty task chunk",
			WorkerId: ws.workerID,
		}, nil
	}

	// 构造 FFmpeg 参数列表
	ffmpegArgs := buildFFmpegArgs(chunk)

	task := executor.ExecTask{
		TaskID:       chunk.GetTaskId(),
		ParentTaskID: chunk.GetParentTaskId(),
		ChunkIndex:   int(chunk.GetChunkIndex()),
		TotalChunks:  int(chunk.GetTotalChunks()),
		InputFile:    chunk.GetInputUrl(),
		OutputFile:   chunk.GetOutputUrl(),
		FFmpegArgs:   ffmpegArgs,
		TimeoutMs:    chunk.GetTimeoutMs(),
		Priority:     int(chunk.GetPriority()),
	}

	if err := exec.Submit(task); err != nil {
		logger.Warnf("[WorkerServer] DispatchTask rejected: taskID=%s err=%v", chunk.GetTaskId(), err)
		return &c2w.DispatchTaskResponse{
			Accepted: false,
			Reason:   err.Error(),
			WorkerId: ws.workerID,
		}, nil
	}

	logger.Noticef("[WorkerServer] DispatchTask accepted: taskID=%s chunk=%d/%d",
		chunk.GetTaskId(), chunk.GetChunkIndex(), chunk.GetTotalChunks())

	return &c2w.DispatchTaskResponse{
		Accepted: true,
		WorkerId: ws.workerID,
	}, nil
}

// ---------------------------------------------------------------------------
// CancelTask — controller 取消正在执行的任务
// ---------------------------------------------------------------------------

func (ws *WorkerServer) CancelTask(ctx context.Context, req *c2w.CancelTaskRequest) (*c2w.CancelTaskResponse, error) {
	ws.mu.RLock()
	exec := ws.exec
	ws.mu.RUnlock()

	if exec == nil {
		return &c2w.CancelTaskResponse{
			Success: false,
			Message: "executor not initialized",
		}, nil
	}

	taskID := req.GetTaskId()
	reason := req.GetReason()
	if reason == "" {
		reason = "cancelled by controller"
	}

	err := exec.Cancel(taskID, reason)
	if err != nil {
		logger.Warnf("[WorkerServer] CancelTask failed: taskID=%s err=%v", taskID, err)
		return &c2w.CancelTaskResponse{
			Success: false,
			Message: err.Error(),
		}, nil
	}

	logger.Noticef("[WorkerServer] CancelTask success: taskID=%s reason=%s", taskID, reason)
	return &c2w.CancelTaskResponse{
		Success: true,
		Message: fmt.Sprintf("task %s cancelled", taskID),
	}, nil
}

// ---------------------------------------------------------------------------
// Ping — controller 主动探活 + 延迟测量
//
// 返回 worker 当前 CPU / Memory 使用率、任务数以及响应时间戳，
// 使 controller 可以计算 RTT。
// ---------------------------------------------------------------------------

func (ws *WorkerServer) Ping(ctx context.Context, req *c2w.PingRequest) (*c2w.PingResponse, error) {
	now := time.Now().UnixMilli()

	// 收集 CPU 使用率（非阻塞，使用最近一次采样）
	cpuPercent := 0.0
	if percents, err := cpu.Percent(0, false); err == nil && len(percents) > 0 {
		cpuPercent = percents[0]
	}

	// 收集内存使用率
	memPercent := 0.0
	if memInfo, err := mem.VirtualMemory(); err == nil {
		memPercent = memInfo.UsedPercent
	}

	// 当前任务数
	taskCount := int32(0)
	ws.mu.RLock()
	exec := ws.exec
	ws.mu.RUnlock()
	if exec != nil {
		taskCount = int32(exec.RunningCount() + exec.PendingCount())
	}

	return &c2w.PingResponse{
		RequestTimestamp:   req.GetTimestamp(),
		ResponseTimestamp:  now,
		WorkerId:           ws.workerID,
		CurrentTaskCount:   taskCount,
		CpuUsagePercent:    cpuPercent,
		MemoryUsagePercent: memPercent,
	}, nil
}

// ---------------------------------------------------------------------------
// ReportProgress — worker 上报任务进度给 controller
//
// 注意：虽然 proto 中此 RPC 的方向语义是"worker → controller 上报"，
// 但在 gRPC 实现中它仍然是 controller 调用 worker 端的接口。
// controller 可以通过 should_cancel 字段告知 worker 取消该任务。
// ---------------------------------------------------------------------------

func (ws *WorkerServer) ReportProgress(ctx context.Context, req *c2w.TaskProgressReport) (*c2w.TaskProgressResponse, error) {
	// 此 RPC 在当前架构中主要由 controller 轮询调用，
	// worker 端只是把 executor 中对应任务的最新进度返回。
	// 实际的进度数据已经通过 ListTasks/GetProgress 获取，
	// 这里简单确认收到即可。

	logger.Noticef("[WorkerServer] ReportProgress received: taskID=%s progress=%.1f%% status=%v",
		req.GetTaskId(), req.GetProgressPercent(), req.GetStatus())

	return &c2w.TaskProgressResponse{
		Received:     true,
		ShouldCancel: false,
	}, nil
}

// ---------------------------------------------------------------------------
// ListTasks — 查询 worker 上当前所有任务
// ---------------------------------------------------------------------------

func (ws *WorkerServer) ListTasks(ctx context.Context, req *c2w.ListTasksRequest) (*c2w.ListTasksResponse, error) {
	ws.mu.RLock()
	exec := ws.exec
	ws.mu.RUnlock()

	if exec == nil {
		return &c2w.ListTasksResponse{
			WorkerId: ws.workerID,
			Tasks:    []*c2w.TaskInfo{},
		}, nil
	}

	progresses := exec.ListTasks()
	tasks := make([]*c2w.TaskInfo, 0, len(progresses))

	for _, p := range progresses {
		tasks = append(tasks, &c2w.TaskInfo{
			TaskId:          p.TaskID,
			TaskType:        "video_transcode", // 当前只有视频转码任务类型
			Status:          mapTaskState(p.State),
			ProgressPercent: p.ProgressPercent,
			StartTime:       p.StartTime.UnixMilli(),
			ElapsedMs:       p.ElapsedMs,
		})
	}

	return &c2w.ListTasksResponse{
		WorkerId: ws.workerID,
		Tasks:    tasks,
	}, nil
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

// buildFFmpegArgs 从 proto TaskChunk 构造 FFmpeg 参数列表
//
// 生成 seek-before-input 模式的参数：
//
//	-ss <start> -t <duration> -i <input> -c:v <codec> -preset <preset> -crf <crf> <extra> <output>
func buildFFmpegArgs(chunk *c2w.TaskChunk) []string {
	var args []string

	// seek-before-input 模式（更快的 seek）
	if chunk.GetStartOffset() > 0 {
		startSec := float64(chunk.GetStartOffset()) / 1000.0 // 假设 ms 单位
		args = append(args, "-ss", fmt.Sprintf("%.3f", startSec))
	}

	// 时长限制
	if chunk.GetLength() > 0 {
		durSec := float64(chunk.GetLength()) / 1000.0
		args = append(args, "-t", fmt.Sprintf("%.3f", durSec))
	}

	// 输入文件
	args = append(args, "-i", chunk.GetInputUrl())

	// 视频编码参数
	codec := chunk.GetCodec()
	if codec != "" {
		args = append(args, "-c:v", codec)
	}

	preset := chunk.GetPreset()
	if preset != "" {
		args = append(args, "-preset", preset)
	}

	crf := chunk.GetCrf()
	if crf > 0 {
		args = append(args, "-crf", fmt.Sprintf("%d", crf))
	}

	// 音频直接 copy（分片时不重新编码音频）
	args = append(args, "-c:a", "copy")

	// 额外参数（JSON 格式字符串，解析为空格分隔的参数）
	if extra := chunk.GetExtraArgs(); extra != "" {
		extra = strings.TrimSpace(extra)
		if extra != "" {
			parts := strings.Fields(extra)
			args = append(args, parts...)
		}
	}

	// 覆盖已有输出文件
	args = append(args, "-y")

	// 输出文件
	args = append(args, chunk.GetOutputUrl())

	return args
}

// mapTaskState 将 executor.TaskState 映射为 proto TaskStatus 枚举
func mapTaskState(state executor.TaskState) c2w.TaskStatus {
	switch state {
	case executor.TaskStatePending:
		return c2w.TaskStatus_TASK_STATUS_PENDING
	case executor.TaskStateRunning:
		return c2w.TaskStatus_TASK_STATUS_RUNNING
	case executor.TaskStateCompleted:
		return c2w.TaskStatus_TASK_STATUS_COMPLETED
	case executor.TaskStateFailed:
		return c2w.TaskStatus_TASK_STATUS_FAILED
	case executor.TaskStateCancelled:
		return c2w.TaskStatus_TASK_STATUS_CANCELLED
	case executor.TaskStateTimeout:
		return c2w.TaskStatus_TASK_STATUS_TIMEOUT
	default:
		return c2w.TaskStatus_TASK_STATUS_UNKNOWN
	}
}
