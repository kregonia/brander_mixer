package taskmanager

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	logger "github.com/kregonia/brander_mixer/log"
	c2w_proto "github.com/kregonia/brander_mixer/script/rpc_server/controller"
	"github.com/kregonia/brander_mixer/widget/breaker"
	"github.com/kregonia/brander_mixer/widget/connection"
	"github.com/kregonia/brander_mixer/widget/mixer"
	"github.com/kregonia/brander_mixer/widget/nodetable"
	"github.com/kregonia/brander_mixer/widget/scheduler"
)

// ---------------------------------------------------------------------------
// GlobalTaskState 全局任务生命周期状态
// ---------------------------------------------------------------------------

type GlobalTaskState int

const (
	GlobalTaskPending    GlobalTaskState = iota // 已提交，等待调度
	GlobalTaskSplitting                         // 正在执行视频探测 + 分片
	GlobalTaskDispatched                        // 已下发到各 worker
	GlobalTaskRunning                           // 至少一个分片在执行中
	GlobalTaskMerging                           // 所有分片完成，正在合并
	GlobalTaskCompleted                         // 全部完成
	GlobalTaskFailed                            // 失败（不可恢复）
	GlobalTaskCancelled                         // 已取消
)

func (s GlobalTaskState) String() string {
	switch s {
	case GlobalTaskPending:
		return "pending"
	case GlobalTaskSplitting:
		return "splitting"
	case GlobalTaskDispatched:
		return "dispatched"
	case GlobalTaskRunning:
		return "running"
	case GlobalTaskMerging:
		return "merging"
	case GlobalTaskCompleted:
		return "completed"
	case GlobalTaskFailed:
		return "failed"
	case GlobalTaskCancelled:
		return "cancelled"
	}
	return "unknown"
}

// IsTerminal 返回是否处于终态
func (s GlobalTaskState) IsTerminal() bool {
	return s == GlobalTaskCompleted || s == GlobalTaskFailed || s == GlobalTaskCancelled
}

// ---------------------------------------------------------------------------
// VideoTaskConfig 视频任务配置
// ---------------------------------------------------------------------------

type VideoTaskConfig struct {
	Codec     string // 目标编码器 (如 "libx264", "libsvtav1")
	Preset    string // 编码预设 (如 "fast", "medium")
	CRF       int    // 质量参数 (0-51)
	ExtraArgs string // 额外 FFmpeg 参数（空格分隔）

	// 输出路径（最终合并后的文件路径）
	OutputPath string

	// 分片输出目录（临时目录，用于存放各分片的输出文件）
	// 为空时自动在 OutputPath 同目录下创建 .chunks/ 子目录
	ChunkOutputDir string

	// 合并模式
	MergeMode mixer.MergeMode

	// 任务超时（整体超时，0 = 不限）
	TimeoutMs int64

	// 最低可靠性要求 [0, 1]
	MinReliability float64
}

// DefaultVideoTaskConfig 默认视频任务配置
func DefaultVideoTaskConfig() VideoTaskConfig {
	return VideoTaskConfig{
		Codec:          "libx264",
		Preset:         "fast",
		CRF:            23,
		MergeMode:      mixer.MergeModeConcat,
		MinReliability: 0.3,
	}
}

// ---------------------------------------------------------------------------
// ChunkRecord 分片级别的状态记录
// ---------------------------------------------------------------------------

type ChunkRecord struct {
	ChunkIndex  int
	SubTaskID   string
	WorkerID    string
	InputFile   string
	OutputFile  string
	StartOffset int64
	Length      int64
	State       GlobalTaskState
	Progress    float64 // [0, 100]
	ErrorMsg    string
	DispatchAt  time.Time
	CompleteAt  time.Time
}

// ---------------------------------------------------------------------------
// TaskRecord 全局任务级别的状态记录
// ---------------------------------------------------------------------------

type TaskRecord struct {
	mu sync.RWMutex

	TaskID    string
	InputFile string
	Config    VideoTaskConfig
	State     GlobalTaskState

	// 视频探测信息
	VideoInfo *breaker.VideoInfo

	// 分片记录
	Chunks []*ChunkRecord

	// 时间线
	CreateAt   time.Time
	SplitAt    time.Time
	DispatchAt time.Time
	MergeAt    time.Time
	CompleteAt time.Time

	// 错误信息
	ErrorMsg string
}

// Progress 返回整体进度 [0, 100]
func (tr *TaskRecord) Progress() float64 {
	tr.mu.RLock()
	defer tr.mu.RUnlock()

	if len(tr.Chunks) == 0 {
		if tr.State == GlobalTaskCompleted {
			return 100
		}
		return 0
	}

	total := 0.0
	for _, c := range tr.Chunks {
		total += c.Progress
	}
	return total / float64(len(tr.Chunks))
}

// CompletedChunks 返回已完成的分片数
func (tr *TaskRecord) CompletedChunks() int {
	tr.mu.RLock()
	defer tr.mu.RUnlock()
	count := 0
	for _, c := range tr.Chunks {
		if c.State == GlobalTaskCompleted {
			count++
		}
	}
	return count
}

// ---------------------------------------------------------------------------
// TaskManager 全局任务管理器
//
// 编排完整的视频任务管线：
//   1. Probe    — 通过 ffprobe 获取视频信息
//   2. Split    — 根据节点能力权重进行 GOP 对齐分片
//   3. Schedule — 调度器为每个分片选择最优节点
//   4. Dispatch — 通过 c2w 连接池将 TaskChunk 下发到各 worker
//   5. Track    — 追踪全局任务状态（pending/running/completed/failed）
//   6. Merge    — 所有分片完成后调用 mixer 合并最终输出
// ---------------------------------------------------------------------------

type TaskManager struct {
	mu    sync.RWMutex
	tasks map[string]*TaskRecord // key = taskID

	sched      *scheduler.Scheduler
	nodeTable  *nodetable.NodeTable
	clientPool *connection.WorkerClientPool

	ctx    context.Context
	cancel context.CancelFunc
}

// NewTaskManager 创建任务管理器
func NewTaskManager(
	ctx context.Context,
	sched *scheduler.Scheduler,
	nt *nodetable.NodeTable,
	pool *connection.WorkerClientPool,
) *TaskManager {
	tmCtx, tmCancel := context.WithCancel(ctx)
	tm := &TaskManager{
		tasks:      make(map[string]*TaskRecord),
		sched:      sched,
		nodeTable:  nt,
		clientPool: pool,
		ctx:        tmCtx,
		cancel:     tmCancel,
	}
	logger.Noticef("[TaskManager] initialized")
	return tm
}

// Shutdown 停止任务管理器
func (tm *TaskManager) Shutdown() {
	tm.cancel()
	logger.Noticef("[TaskManager] shutdown")
}

// ---------------------------------------------------------------------------
// SubmitVideoTask 提交一个视频转码任务
//
// 全流程：probe → split → schedule → dispatch → track → merge
// 该方法是异步的，会立即返回 taskID，后续可通过 GetTask 查询状态。
// ---------------------------------------------------------------------------

func (tm *TaskManager) SubmitVideoTask(taskID, inputFile string, cfg VideoTaskConfig) (string, error) {
	if taskID == "" {
		taskID = fmt.Sprintf("task_%d", time.Now().UnixNano())
	}

	if cfg.OutputPath == "" {
		ext := filepath.Ext(inputFile)
		base := inputFile[:len(inputFile)-len(ext)]
		cfg.OutputPath = base + "_output" + ext
	}

	if cfg.ChunkOutputDir == "" {
		cfg.ChunkOutputDir = filepath.Join(filepath.Dir(cfg.OutputPath), ".chunks_"+taskID)
	}

	record := &TaskRecord{
		TaskID:    taskID,
		InputFile: inputFile,
		Config:    cfg,
		State:     GlobalTaskPending,
		CreateAt:  time.Now(),
	}

	tm.mu.Lock()
	if _, exists := tm.tasks[taskID]; exists {
		tm.mu.Unlock()
		return "", fmt.Errorf("task %s already exists", taskID)
	}
	tm.tasks[taskID] = record
	tm.mu.Unlock()

	logger.Noticef("[TaskManager] task %s submitted: input=%s output=%s codec=%s preset=%s crf=%d",
		taskID, inputFile, cfg.OutputPath, cfg.Codec, cfg.Preset, cfg.CRF)

	// 异步执行管线
	go tm.executePipeline(record)

	return taskID, nil
}

// ---------------------------------------------------------------------------
// executePipeline 执行完整管线
// ---------------------------------------------------------------------------

func (tm *TaskManager) executePipeline(record *TaskRecord) {
	taskID := record.TaskID

	// ---------------------------------------------------------------
	// Step 1: Probe
	// ---------------------------------------------------------------
	record.mu.Lock()
	record.State = GlobalTaskSplitting
	record.SplitAt = time.Now()
	record.mu.Unlock()

	logger.Noticef("[TaskManager] [%s] probing video: %s", taskID, record.InputFile)

	videoInfo, err := breaker.Probe(record.InputFile)
	if err != nil {
		tm.failTask(record, fmt.Sprintf("probe failed: %v", err))
		return
	}
	record.mu.Lock()
	record.VideoInfo = videoInfo
	record.mu.Unlock()

	logger.Noticef("[TaskManager] [%s] probe result: duration=%.2fs resolution=%dx%d fps=%.2f codec=%s gop=%d",
		taskID, videoInfo.Duration, videoInfo.Width, videoInfo.Height,
		videoInfo.FPS, videoInfo.VideoCodec, videoInfo.GOPSize)

	// ---------------------------------------------------------------
	// Step 2: Get online nodes and compute capability weights
	// ---------------------------------------------------------------
	onlineNodes := tm.nodeTable.OnlineNodes()
	if len(onlineNodes) == 0 {
		tm.failTask(record, "no online nodes available")
		return
	}

	logger.Noticef("[TaskManager] [%s] %d online nodes available", taskID, len(onlineNodes))

	// Use scheduler.SplitTask to split by node capability proportions
	totalFrames := videoInfo.TotalFrames
	if totalFrames <= 0 {
		// Estimate from duration and fps
		totalFrames = int64(videoInfo.Duration * videoInfo.FPS)
	}
	if totalFrames <= 0 {
		totalFrames = int64(videoInfo.Duration * 30) // fallback: 30fps
	}

	taskReq := scheduler.TaskRequirement{
		TaskID:         taskID,
		TaskType:       "video_transcode",
		MinCPU:         0.01,
		MinMemory:      0.01,
		MinDisk:        0.01,
		MinReliability: record.Config.MinReliability,
		Weight:         1.0,
	}

	splits, err := tm.sched.SplitTask(taskID, totalFrames, taskReq)
	if err != nil {
		tm.failTask(record, fmt.Sprintf("split task failed: %v", err))
		return
	}

	if len(splits) == 0 {
		tm.failTask(record, "split resulted in 0 chunks")
		return
	}

	// ---------------------------------------------------------------
	// Step 3: Convert scheduler splits into breaker-compatible weights
	//         and use breaker.SplitByWeights for GOP-aligned splitting
	// ---------------------------------------------------------------
	weights := make([]float64, len(splits))
	for i, sp := range splits {
		weights[i] = sp.Weight
	}

	splitCfg := breaker.SplitConfig{
		ChunkCount:    len(splits),
		Codec:         record.Config.Codec,
		Preset:        record.Config.Preset,
		CRF:           record.Config.CRF,
		OutputFmt:     "mp4",
		OutputPattern: filepath.Join(record.Config.ChunkOutputDir, "chunk_%03d.mp4"),
	}

	chunks := breaker.SplitByWeights(videoInfo, weights, splitCfg)
	if len(chunks) == 0 {
		tm.failTask(record, "breaker split returned 0 chunks")
		return
	}

	logger.Noticef("[TaskManager] [%s] split into %d chunks", taskID, len(chunks))

	// Ensure chunk output directory exists
	if err := os.MkdirAll(record.Config.ChunkOutputDir, 0o755); err != nil {
		tm.failTask(record, fmt.Sprintf("failed to create chunk output dir: %v", err))
		return
	}

	// ---------------------------------------------------------------
	// Step 4: Build chunk records and dispatch to workers
	// ---------------------------------------------------------------
	record.mu.Lock()
	record.Chunks = make([]*ChunkRecord, len(chunks))
	record.State = GlobalTaskDispatched
	record.DispatchAt = time.Now()
	record.mu.Unlock()

	dispatchErrors := 0

	for i, chunk := range chunks {
		// Determine which worker this chunk goes to
		workerID := ""
		if i < len(splits) {
			workerID = splits[i].WorkerID
		}
		if workerID == "" {
			// Fallback: use scheduler to pick
			result, schedErr := tm.sched.SelectNode(taskReq)
			if schedErr != nil {
				logger.Errorf("[TaskManager] [%s] chunk %d: no worker available: %v", taskID, i, schedErr)
				dispatchErrors++
				continue
			}
			workerID = result.WorkerID
		}

		subTaskID := fmt.Sprintf("%s_chunk_%d", taskID, i)
		outputFile := fmt.Sprintf(splitCfg.OutputPattern, i)

		chunkRecord := &ChunkRecord{
			ChunkIndex:  i,
			SubTaskID:   subTaskID,
			WorkerID:    workerID,
			InputFile:   record.InputFile,
			OutputFile:  outputFile,
			StartOffset: int64(chunk.StartTime * 1000), // seconds to ms
			Length:      int64(chunk.Duration * 1000),
			State:       GlobalTaskPending,
			DispatchAt:  time.Now(),
		}

		record.mu.Lock()
		record.Chunks[i] = chunkRecord
		record.mu.Unlock()

		// Build proto TaskChunk
		protoChunk := &c2w_proto.TaskChunk{
			TaskId:       subTaskID,
			ParentTaskId: taskID,
			ChunkIndex:   int32(i),
			TotalChunks:  int32(len(chunks)),
			TaskType:     "video_transcode",
			InputUrl:     record.InputFile,
			OutputUrl:    outputFile,
			StartOffset:  int64(chunk.StartTime * 1000),
			Length:       int64(chunk.Duration * 1000),
			Codec:        record.Config.Codec,
			Preset:       record.Config.Preset,
			Crf:          int32(record.Config.CRF),
			ExtraArgs:    record.Config.ExtraArgs,
			Priority:     0,
			TimeoutMs:    record.Config.TimeoutMs,
		}

		// Dispatch via c2w connection pool
		resp, err := tm.clientPool.DispatchTask(tm.ctx, workerID, protoChunk)
		if err != nil {
			logger.Errorf("[TaskManager] [%s] dispatch chunk %d to worker %s failed: %v",
				taskID, i, workerID, err)
			chunkRecord.State = GlobalTaskFailed
			chunkRecord.ErrorMsg = err.Error()
			dispatchErrors++
			continue
		}

		if !resp.GetAccepted() {
			logger.Warnf("[TaskManager] [%s] chunk %d rejected by worker %s: %s",
				taskID, i, workerID, resp.GetReason())
			chunkRecord.State = GlobalTaskFailed
			chunkRecord.ErrorMsg = "rejected: " + resp.GetReason()
			dispatchErrors++
			continue
		}

		chunkRecord.State = GlobalTaskRunning
		logger.Noticef("[TaskManager] [%s] chunk %d dispatched to worker %s",
			taskID, i, workerID)
	}

	if dispatchErrors == len(chunks) {
		tm.failTask(record, "all chunks failed to dispatch")
		return
	}

	// ---------------------------------------------------------------
	// Step 5: Track progress — poll workers until all chunks complete
	// ---------------------------------------------------------------
	record.mu.Lock()
	record.State = GlobalTaskRunning
	record.mu.Unlock()

	logger.Noticef("[TaskManager] [%s] tracking %d chunks (%d dispatch errors)",
		taskID, len(chunks)-dispatchErrors, dispatchErrors)

	tm.trackUntilCompletion(record)

	// Check if task was cancelled while tracking
	if record.State == GlobalTaskCancelled || record.State == GlobalTaskFailed {
		return
	}

	// ---------------------------------------------------------------
	// Step 6: Merge all completed chunks
	// ---------------------------------------------------------------
	record.mu.Lock()
	record.State = GlobalTaskMerging
	record.MergeAt = time.Now()
	record.mu.Unlock()

	logger.Noticef("[TaskManager] [%s] all chunks completed, starting merge...", taskID)

	chunkFiles := make([]mixer.ChunkFile, 0, len(record.Chunks))
	record.mu.RLock()
	for _, cr := range record.Chunks {
		if cr != nil && cr.State == GlobalTaskCompleted {
			var fileSize int64
			if fi, statErr := os.Stat(cr.OutputFile); statErr == nil {
				fileSize = fi.Size()
			}
			chunkFiles = append(chunkFiles, mixer.ChunkFile{
				Index:    cr.ChunkIndex,
				FilePath: cr.OutputFile,
				Size:     fileSize,
				Duration: float64(cr.Length) / 1000.0, // ms → seconds
			})
		}
	}
	record.mu.RUnlock()

	if len(chunkFiles) == 0 {
		tm.failTask(record, "no completed chunks to merge")
		return
	}

	mergeCfg := mixer.MergeConfig{
		OutputFile:    record.Config.OutputPath,
		Mode:          record.Config.MergeMode,
		CleanupChunks: true,
		AudioCodec:    "copy",
	}

	mergeResult := mixer.Merge(chunkFiles, mergeCfg)
	if mergeResult.Error != nil {
		tm.failTask(record, fmt.Sprintf("merge failed: %v", mergeResult.Error))
		return
	}

	// ---------------------------------------------------------------
	// Step 7: Mark completed
	// ---------------------------------------------------------------
	record.mu.Lock()
	record.State = GlobalTaskCompleted
	record.CompleteAt = time.Now()
	record.mu.Unlock()

	totalTime := record.CompleteAt.Sub(record.CreateAt)

	logger.Noticef("[TaskManager] [%s] ✅ COMPLETED: output=%s size=%d merge_time=%v total_time=%v",
		taskID, mergeResult.OutputFile, mergeResult.OutputSize,
		mergeResult.Duration, totalTime)
}

// ---------------------------------------------------------------------------
// trackUntilCompletion 轮询 worker 直到所有分片完成/失败
// ---------------------------------------------------------------------------

func (tm *TaskManager) trackUntilCompletion(record *TaskRecord) {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-tm.ctx.Done():
			logger.Warnf("[TaskManager] [%s] context cancelled during tracking", record.TaskID)
			record.mu.Lock()
			record.State = GlobalTaskCancelled
			record.mu.Unlock()
			return
		case <-ticker.C:
			allDone := true
			anyFailed := false
			failedCount := 0

			record.mu.RLock()
			chunks := record.Chunks
			record.mu.RUnlock()

			for _, cr := range chunks {
				if cr == nil {
					continue
				}
				if cr.State.IsTerminal() {
					if cr.State == GlobalTaskFailed {
						failedCount++
					}
					continue
				}

				// Query the worker for this chunk's status
				allDone = false
				resp, err := tm.clientPool.ListTasks(tm.ctx, cr.WorkerID)
				if err != nil {
					logger.Warnf("[TaskManager] [%s] failed to query worker %s: %v",
						record.TaskID, cr.WorkerID, err)
					continue
				}

				// Find our chunk in the worker's task list
				for _, taskInfo := range resp.GetTasks() {
					if taskInfo.GetTaskId() == cr.SubTaskID {
						cr.Progress = taskInfo.GetProgressPercent()
						switch taskInfo.GetStatus() {
						case c2w_proto.TaskStatus_TASK_STATUS_COMPLETED:
							cr.State = GlobalTaskCompleted
							cr.CompleteAt = time.Now()
							cr.Progress = 100
							logger.Noticef("[TaskManager] [%s] chunk %d completed on worker %s",
								record.TaskID, cr.ChunkIndex, cr.WorkerID)
						case c2w_proto.TaskStatus_TASK_STATUS_FAILED:
							cr.State = GlobalTaskFailed
							cr.CompleteAt = time.Now()
							anyFailed = true
							failedCount++
							logger.Errorf("[TaskManager] [%s] chunk %d failed on worker %s",
								record.TaskID, cr.ChunkIndex, cr.WorkerID)
						case c2w_proto.TaskStatus_TASK_STATUS_CANCELLED:
							cr.State = GlobalTaskCancelled
							cr.CompleteAt = time.Now()
							failedCount++
						case c2w_proto.TaskStatus_TASK_STATUS_TIMEOUT:
							cr.State = GlobalTaskFailed
							cr.CompleteAt = time.Now()
							cr.ErrorMsg = "timeout"
							failedCount++
						}
						break
					}
				}
			}

			// All chunks are in a terminal state
			if allDone {
				if failedCount == len(chunks) {
					tm.failTask(record, "all chunks failed")
					return
				}
				if anyFailed {
					logger.Warnf("[TaskManager] [%s] %d/%d chunks failed, proceeding with partial merge",
						record.TaskID, failedCount, len(chunks))
				}
				return // proceed to merge
			}
		}
	}
}

// ---------------------------------------------------------------------------
// failTask 将任务标记为失败
// ---------------------------------------------------------------------------

func (tm *TaskManager) failTask(record *TaskRecord, reason string) {
	record.mu.Lock()
	record.State = GlobalTaskFailed
	record.ErrorMsg = reason
	record.CompleteAt = time.Now()
	record.mu.Unlock()

	logger.Errorf("[TaskManager] [%s] ❌ FAILED: %s", record.TaskID, reason)
}

// ---------------------------------------------------------------------------
// CancelTask 取消指定任务
// ---------------------------------------------------------------------------

func (tm *TaskManager) CancelTask(taskID, reason string) error {
	tm.mu.RLock()
	record, exists := tm.tasks[taskID]
	tm.mu.RUnlock()

	if !exists {
		return fmt.Errorf("task %s not found", taskID)
	}

	record.mu.Lock()
	if record.State.IsTerminal() {
		record.mu.Unlock()
		return fmt.Errorf("task %s already in terminal state: %s", taskID, record.State)
	}
	record.State = GlobalTaskCancelled
	record.ErrorMsg = "cancelled: " + reason
	record.CompleteAt = time.Now()
	chunks := record.Chunks
	record.mu.Unlock()

	// Cancel all running chunks on workers
	for _, cr := range chunks {
		if cr == nil || cr.State.IsTerminal() {
			continue
		}
		cr.State = GlobalTaskCancelled
		if _, err := tm.clientPool.CancelTask(tm.ctx, cr.WorkerID, cr.SubTaskID, reason); err != nil {
			logger.Warnf("[TaskManager] [%s] failed to cancel chunk %d on worker %s: %v",
				taskID, cr.ChunkIndex, cr.WorkerID, err)
		}
	}

	logger.Noticef("[TaskManager] [%s] cancelled: %s", taskID, reason)
	return nil
}

// ---------------------------------------------------------------------------
// GetTask 获取任务记录
// ---------------------------------------------------------------------------

func (tm *TaskManager) GetTask(taskID string) (*TaskRecord, bool) {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	rec, ok := tm.tasks[taskID]
	return rec, ok
}

// ---------------------------------------------------------------------------
// AllTasks 返回所有任务记录
// ---------------------------------------------------------------------------

func (tm *TaskManager) AllTasks() map[string]*TaskRecord {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	cp := make(map[string]*TaskRecord, len(tm.tasks))
	for k, v := range tm.tasks {
		cp[k] = v
	}
	return cp
}

// ---------------------------------------------------------------------------
// Stats 返回任务统计摘要
// ---------------------------------------------------------------------------

type TaskManagerStats struct {
	Total     int
	Pending   int
	Running   int
	Completed int
	Failed    int
	Cancelled int
}

func (tm *TaskManager) Stats() TaskManagerStats {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	var stats TaskManagerStats
	stats.Total = len(tm.tasks)

	for _, rec := range tm.tasks {
		rec.mu.RLock()
		switch rec.State {
		case GlobalTaskPending, GlobalTaskSplitting:
			stats.Pending++
		case GlobalTaskDispatched, GlobalTaskRunning, GlobalTaskMerging:
			stats.Running++
		case GlobalTaskCompleted:
			stats.Completed++
		case GlobalTaskFailed:
			stats.Failed++
		case GlobalTaskCancelled:
			stats.Cancelled++
		}
		rec.mu.RUnlock()
	}

	return stats
}

// ---------------------------------------------------------------------------
// ReassignChunk 在节点离线时重新调度指定分片
//
// 由 controller 的自愈回调调用，将离线节点上的分片重新分配到在线节点。
// ---------------------------------------------------------------------------

func (tm *TaskManager) ReassignChunk(taskID string, chunkIndex int) error {
	tm.mu.RLock()
	record, exists := tm.tasks[taskID]
	tm.mu.RUnlock()

	if !exists {
		return fmt.Errorf("task %s not found", taskID)
	}

	record.mu.Lock()
	if record.State.IsTerminal() {
		record.mu.Unlock()
		return fmt.Errorf("task %s is in terminal state: %s", taskID, record.State)
	}
	if chunkIndex < 0 || chunkIndex >= len(record.Chunks) {
		record.mu.Unlock()
		return fmt.Errorf("chunk index %d out of range for task %s", chunkIndex, taskID)
	}
	cr := record.Chunks[chunkIndex]
	record.mu.Unlock()

	if cr.State == GlobalTaskCompleted {
		return nil // already done, no need to reassign
	}

	// Find a new node
	taskReq := scheduler.TaskRequirement{
		TaskID:         cr.SubTaskID,
		TaskType:       "video_transcode",
		MinCPU:         0.01,
		MinMemory:      0.01,
		MinDisk:        0.01,
		MinReliability: record.Config.MinReliability,
		Weight:         1.0,
	}

	result, err := tm.sched.SelectNode(taskReq)
	if err != nil {
		return fmt.Errorf("no available node for reassignment: %w", err)
	}

	oldWorker := cr.WorkerID
	cr.WorkerID = result.WorkerID
	cr.State = GlobalTaskPending
	cr.ErrorMsg = ""

	// Re-dispatch
	protoChunk := &c2w_proto.TaskChunk{
		TaskId:       cr.SubTaskID,
		ParentTaskId: taskID,
		ChunkIndex:   int32(cr.ChunkIndex),
		TotalChunks:  int32(len(record.Chunks)),
		TaskType:     "video_transcode",
		InputUrl:     cr.InputFile,
		OutputUrl:    cr.OutputFile,
		StartOffset:  cr.StartOffset,
		Length:       cr.Length,
		Codec:        record.Config.Codec,
		Preset:       record.Config.Preset,
		Crf:          int32(record.Config.CRF),
		ExtraArgs:    record.Config.ExtraArgs,
		TimeoutMs:    record.Config.TimeoutMs,
	}

	resp, err := tm.clientPool.DispatchTask(tm.ctx, result.WorkerID, protoChunk)
	if err != nil {
		cr.State = GlobalTaskFailed
		cr.ErrorMsg = fmt.Sprintf("reassignment dispatch failed: %v", err)
		return err
	}

	if !resp.GetAccepted() {
		cr.State = GlobalTaskFailed
		cr.ErrorMsg = "reassignment rejected: " + resp.GetReason()
		return fmt.Errorf("worker %s rejected reassignment: %s", result.WorkerID, resp.GetReason())
	}

	cr.State = GlobalTaskRunning
	cr.DispatchAt = time.Now()

	logger.Noticef("[TaskManager] [%s] chunk %d reassigned: %s -> %s",
		taskID, chunkIndex, oldWorker, result.WorkerID)

	return nil
}
