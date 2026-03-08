package executor

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"

	logger "github.com/kregonia/brander_mixer/log"
)

// ---------------------------------------------------------------------------
// TaskState 任务执行状态
// ---------------------------------------------------------------------------

type TaskState int

const (
	TaskStatePending   TaskState = iota // 已接收，等待执行
	TaskStateRunning                    // 正在执行
	TaskStateCompleted                  // 执行完成
	TaskStateFailed                     // 执行失败
	TaskStateCancelled                  // 已取消
	TaskStateTimeout                    // 超时
)

func (s TaskState) String() string {
	switch s {
	case TaskStatePending:
		return "pending"
	case TaskStateRunning:
		return "running"
	case TaskStateCompleted:
		return "completed"
	case TaskStateFailed:
		return "failed"
	case TaskStateCancelled:
		return "cancelled"
	case TaskStateTimeout:
		return "timeout"
	}
	return "unknown"
}

// IsTerminal 返回该状态是否为终态（不可再变更）
func (s TaskState) IsTerminal() bool {
	return s == TaskStateCompleted || s == TaskStateFailed ||
		s == TaskStateCancelled || s == TaskStateTimeout
}

// ---------------------------------------------------------------------------
// ExecTask 一个待执行的 FFmpeg 任务描述
// ---------------------------------------------------------------------------

type ExecTask struct {
	TaskID       string   // 全局唯一任务 ID
	ParentTaskID string   // 父任务 ID（拆分前的原始任务）
	ChunkIndex   int      // 分片索引
	TotalChunks  int      // 总分片数
	InputFile    string   // 输入文件路径或 URL
	OutputFile   string   // 输出文件路径
	FFmpegArgs   []string // FFmpeg 参数列表（不含 "ffmpeg" 本身）
	TimeoutMs    int64    // 超时毫秒数，0 = 不限
	Priority     int      // 优先级（越大越高）
}

// ---------------------------------------------------------------------------
// TaskProgress 任务进度快照
// ---------------------------------------------------------------------------

type TaskProgress struct {
	TaskID          string
	State           TaskState
	ProgressPercent float64   // [0, 100]
	StartTime       time.Time // 开始执行的时刻
	ElapsedMs       int64     // 已耗时（毫秒）
	ErrorMessage    string    // 失败时的错误信息
	OutputFileSize  int64     // 输出文件当前大小（bytes）
}

// ---------------------------------------------------------------------------
// taskEntry 内部运行时记录
// ---------------------------------------------------------------------------

type taskEntry struct {
	mu       sync.RWMutex
	task     ExecTask
	state    TaskState
	start    time.Time
	end      time.Time
	cancel   context.CancelFunc
	cmd      *exec.Cmd
	errMsg   string
	progress float64
}

func (te *taskEntry) snapshot() TaskProgress {
	te.mu.RLock()
	defer te.mu.RUnlock()

	elapsed := int64(0)
	if !te.start.IsZero() {
		if te.end.IsZero() {
			elapsed = time.Since(te.start).Milliseconds()
		} else {
			elapsed = te.end.Sub(te.start).Milliseconds()
		}
	}

	var outputSize int64
	if te.task.OutputFile != "" {
		if fi, err := os.Stat(te.task.OutputFile); err == nil {
			outputSize = fi.Size()
		}
	}

	return TaskProgress{
		TaskID:          te.task.TaskID,
		State:           te.state,
		ProgressPercent: te.progress,
		StartTime:       te.start,
		ElapsedMs:       elapsed,
		ErrorMessage:    te.errMsg,
		OutputFileSize:  outputSize,
	}
}

// ---------------------------------------------------------------------------
// ProgressCallback 进度回调函数类型
//
// 在任务状态变更或定期进度更新时调用。
// 实现方可据此向 controller 上报进度。
// ---------------------------------------------------------------------------

type ProgressCallback func(progress TaskProgress)

// ---------------------------------------------------------------------------
// ExecutorConfig 执行器配置
// ---------------------------------------------------------------------------

type ExecutorConfig struct {
	// 最大并发任务数（0 = 不限制，默认值为 CPU 核心数）
	MaxConcurrency int

	// 进度轮询间隔（检查输出文件大小估算进度）
	ProgressInterval time.Duration

	// 进度回调
	OnProgress ProgressCallback

	// FFmpeg 路径（为空则从 PATH 中搜索）
	FFmpegPath string
}

// DefaultConfig 返回默认配置
func DefaultConfig() ExecutorConfig {
	return ExecutorConfig{
		MaxConcurrency:   4,
		ProgressInterval: 3 * time.Second,
	}
}

// ---------------------------------------------------------------------------
// Executor 任务执行器
//
// 负责在 worker 节点上管理多个 FFmpeg 任务的并发执行、
// 进度跟踪、取消支持和结果汇报。
// ---------------------------------------------------------------------------

type Executor struct {
	mu     sync.RWMutex
	tasks  map[string]*taskEntry // key = taskID
	cfg    ExecutorConfig
	sem    chan struct{} // 并发控制信号量
	ctx    context.Context
	cancel context.CancelFunc

	ffmpegPath string
}

// NewExecutor 创建一个新的任务执行器
func NewExecutor(ctx context.Context, cfg ExecutorConfig) (*Executor, error) {
	if cfg.MaxConcurrency <= 0 {
		cfg.MaxConcurrency = 4
	}
	if cfg.ProgressInterval <= 0 {
		cfg.ProgressInterval = 3 * time.Second
	}

	ffmpegPath := cfg.FFmpegPath
	if ffmpegPath == "" {
		var err error
		ffmpegPath, err = exec.LookPath("ffmpeg")
		if err != nil {
			return nil, fmt.Errorf("ffmpeg not found in PATH: %w", err)
		}
	}

	execCtx, execCancel := context.WithCancel(ctx)

	e := &Executor{
		tasks:      make(map[string]*taskEntry),
		cfg:        cfg,
		sem:        make(chan struct{}, cfg.MaxConcurrency),
		ctx:        execCtx,
		cancel:     execCancel,
		ffmpegPath: ffmpegPath,
	}

	logger.Noticef("[Executor] initialized: maxConcurrency=%d, progressInterval=%v, ffmpeg=%s",
		cfg.MaxConcurrency, cfg.ProgressInterval, ffmpegPath)

	return e, nil
}

// ---------------------------------------------------------------------------
// Submit 提交一个任务到执行器
//
// 任务被放入待执行队列，执行器会在有空闲 slot 时自动开始执行。
// 如果 taskID 重复，会返回错误。
// ---------------------------------------------------------------------------

func (e *Executor) Submit(task ExecTask) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if _, exists := e.tasks[task.TaskID]; exists {
		return fmt.Errorf("task %s already exists", task.TaskID)
	}

	entry := &taskEntry{
		task:  task,
		state: TaskStatePending,
	}
	e.tasks[task.TaskID] = entry

	logger.Noticef("[Executor] task %s submitted (chunk %d/%d, input=%s, output=%s)",
		task.TaskID, task.ChunkIndex, task.TotalChunks, task.InputFile, task.OutputFile)

	// 在后台启动执行
	go e.execute(entry)

	return nil
}

// ---------------------------------------------------------------------------
// Cancel 取消指定任务
// ---------------------------------------------------------------------------

func (e *Executor) Cancel(taskID string, reason string) error {
	e.mu.RLock()
	entry, exists := e.tasks[taskID]
	e.mu.RUnlock()

	if !exists {
		return fmt.Errorf("task %s not found", taskID)
	}

	entry.mu.Lock()
	defer entry.mu.Unlock()

	if entry.state.IsTerminal() {
		return fmt.Errorf("task %s already in terminal state: %s", taskID, entry.state)
	}

	entry.state = TaskStateCancelled
	entry.errMsg = "cancelled: " + reason
	entry.end = time.Now()

	if entry.cancel != nil {
		entry.cancel()
	}

	logger.Noticef("[Executor] task %s cancelled: %s", taskID, reason)
	e.notifyProgress(entry.snapshot())

	return nil
}

// ---------------------------------------------------------------------------
// CancelAll 取消所有非终态任务
// ---------------------------------------------------------------------------

func (e *Executor) CancelAll(reason string) int {
	e.mu.RLock()
	var toCancel []string
	for id, entry := range e.tasks {
		entry.mu.RLock()
		if !entry.state.IsTerminal() {
			toCancel = append(toCancel, id)
		}
		entry.mu.RUnlock()
	}
	e.mu.RUnlock()

	cancelled := 0
	for _, id := range toCancel {
		if err := e.Cancel(id, reason); err == nil {
			cancelled++
		}
	}

	logger.Noticef("[Executor] cancelled %d tasks: %s", cancelled, reason)
	return cancelled
}

// ---------------------------------------------------------------------------
// GetProgress 获取指定任务的进度
// ---------------------------------------------------------------------------

func (e *Executor) GetProgress(taskID string) (TaskProgress, error) {
	e.mu.RLock()
	entry, exists := e.tasks[taskID]
	e.mu.RUnlock()

	if !exists {
		return TaskProgress{}, fmt.Errorf("task %s not found", taskID)
	}

	return entry.snapshot(), nil
}

// ---------------------------------------------------------------------------
// ListTasks 获取所有任务的进度快照
// ---------------------------------------------------------------------------

func (e *Executor) ListTasks() []TaskProgress {
	e.mu.RLock()
	defer e.mu.RUnlock()

	results := make([]TaskProgress, 0, len(e.tasks))
	for _, entry := range e.tasks {
		results = append(results, entry.snapshot())
	}
	return results
}

// ---------------------------------------------------------------------------
// RunningCount 返回当前正在运行的任务数
// ---------------------------------------------------------------------------

func (e *Executor) RunningCount() int {
	e.mu.RLock()
	defer e.mu.RUnlock()

	count := 0
	for _, entry := range e.tasks {
		entry.mu.RLock()
		if entry.state == TaskStateRunning {
			count++
		}
		entry.mu.RUnlock()
	}
	return count
}

// ---------------------------------------------------------------------------
// PendingCount 返回当前等待执行的任务数
// ---------------------------------------------------------------------------

func (e *Executor) PendingCount() int {
	e.mu.RLock()
	defer e.mu.RUnlock()

	count := 0
	for _, entry := range e.tasks {
		entry.mu.RLock()
		if entry.state == TaskStatePending {
			count++
		}
		entry.mu.RUnlock()
	}
	return count
}

// ---------------------------------------------------------------------------
// TotalCount 返回执行器中所有任务总数
// ---------------------------------------------------------------------------

func (e *Executor) TotalCount() int {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return len(e.tasks)
}

// ---------------------------------------------------------------------------
// Shutdown 优雅关闭执行器
//
// 取消所有正在运行的任务，等待它们退出。
// ---------------------------------------------------------------------------

func (e *Executor) Shutdown() {
	logger.Noticef("[Executor] shutting down, cancelling all tasks...")
	e.cancel()
	e.CancelAll("executor shutdown")
}

// ---------------------------------------------------------------------------
// CleanupCompleted 清理已完成（终态）任务的记录，释放内存
//
// 返回清理的任务数量。
// ---------------------------------------------------------------------------

func (e *Executor) CleanupCompleted() int {
	e.mu.Lock()
	defer e.mu.Unlock()

	cleaned := 0
	for id, entry := range e.tasks {
		entry.mu.RLock()
		terminal := entry.state.IsTerminal()
		entry.mu.RUnlock()

		if terminal {
			delete(e.tasks, id)
			cleaned++
		}
	}

	if cleaned > 0 {
		logger.Noticef("[Executor] cleaned up %d completed task records", cleaned)
	}
	return cleaned
}

// ---------------------------------------------------------------------------
// execute 内部：实际执行一个任务
// ---------------------------------------------------------------------------

func (e *Executor) execute(entry *taskEntry) {
	// 等待并发 slot
	select {
	case e.sem <- struct{}{}:
		// 获得 slot
	case <-e.ctx.Done():
		entry.mu.Lock()
		entry.state = TaskStateCancelled
		entry.errMsg = "executor context cancelled while waiting for slot"
		entry.end = time.Now()
		entry.mu.Unlock()
		e.notifyProgress(entry.snapshot())
		return
	}
	defer func() { <-e.sem }()

	// 再次检查是否已被取消
	entry.mu.RLock()
	if entry.state.IsTerminal() {
		entry.mu.RUnlock()
		return
	}
	entry.mu.RUnlock()

	// 构建 context（支持超时和取消）
	taskCtx, taskCancel := context.WithCancel(e.ctx)
	if entry.task.TimeoutMs > 0 {
		var cancel2 context.CancelFunc
		taskCtx, cancel2 = context.WithTimeout(taskCtx, time.Duration(entry.task.TimeoutMs)*time.Millisecond)
		origCancel := taskCancel
		taskCancel = func() {
			cancel2()
			origCancel()
		}
	}
	defer taskCancel()

	// 标记运行状态
	entry.mu.Lock()
	entry.state = TaskStateRunning
	entry.start = time.Now()
	entry.cancel = taskCancel
	entry.mu.Unlock()

	logger.Noticef("[Executor] task %s started (chunk %d/%d)",
		entry.task.TaskID, entry.task.ChunkIndex, entry.task.TotalChunks)
	e.notifyProgress(entry.snapshot())

	// 确保输出文件目录存在
	if entry.task.OutputFile != "" {
		outDir := dirOf(entry.task.OutputFile)
		if outDir != "" && outDir != "." {
			_ = os.MkdirAll(outDir, 0755)
		}
	}

	// 构建 FFmpeg 命令
	args := entry.task.FFmpegArgs
	if len(args) == 0 {
		// 如果没有显式指定 args，构建一个默认的 copy 命令
		args = []string{
			"-y",
			"-i", entry.task.InputFile,
			"-c", "copy",
			entry.task.OutputFile,
		}
	}

	cmd := exec.CommandContext(taskCtx, e.ffmpegPath, args...)
	cmd.Stdout = nil // FFmpeg 主要输出到 stderr
	cmd.Stderr = nil // 生产环境不捕获 stderr 到内存，避免爆内存

	entry.mu.Lock()
	entry.cmd = cmd
	entry.mu.Unlock()

	cmdLine := e.ffmpegPath + " " + strings.Join(args, " ")
	logger.Noticef("[Executor] task %s executing: %s", entry.task.TaskID, cmdLine)

	// 启动进度监控
	progressDone := make(chan struct{})
	go e.monitorProgress(entry, progressDone)

	// 执行
	err := cmd.Run()

	// 关闭进度监控
	close(progressDone)

	// 判断结果
	entry.mu.Lock()
	entry.end = time.Now()
	entry.cmd = nil

	if entry.state == TaskStateCancelled {
		// 已经被外部取消了，保持 cancelled 状态
		entry.mu.Unlock()
	} else if taskCtx.Err() == context.DeadlineExceeded {
		entry.state = TaskStateTimeout
		entry.errMsg = fmt.Sprintf("task timed out after %dms", entry.task.TimeoutMs)
		entry.mu.Unlock()
		logger.Warnf("[Executor] task %s TIMEOUT after %dms", entry.task.TaskID, entry.task.TimeoutMs)
	} else if taskCtx.Err() == context.Canceled {
		if entry.state != TaskStateCancelled {
			entry.state = TaskStateCancelled
			entry.errMsg = "executor context cancelled"
		}
		entry.mu.Unlock()
	} else if err != nil {
		entry.state = TaskStateFailed
		entry.errMsg = err.Error()
		entry.mu.Unlock()
		logger.Errorf("[Executor] task %s FAILED: %v", entry.task.TaskID, err)
	} else {
		entry.state = TaskStateCompleted
		entry.progress = 100.0
		entry.mu.Unlock()

		elapsed := entry.end.Sub(entry.start)
		logger.Noticef("[Executor] task %s COMPLETED in %v (chunk %d/%d)",
			entry.task.TaskID, elapsed, entry.task.ChunkIndex, entry.task.TotalChunks)
	}

	e.notifyProgress(entry.snapshot())
}

// ---------------------------------------------------------------------------
// monitorProgress 进度监控协程
//
// 通过定期检查输出文件大小来估算进度（粗略但实用）。
// 更精确的做法是解析 FFmpeg 的 stderr 输出中的 time= 字段，
// 但那需要捕获 stderr 流，这里先用文件大小作为基础实现。
// ---------------------------------------------------------------------------

func (e *Executor) monitorProgress(entry *taskEntry, done <-chan struct{}) {
	ticker := time.NewTicker(e.cfg.ProgressInterval)
	defer ticker.Stop()

	// 尝试获取输入文件大小来估算进度比
	var inputSize int64
	if fi, err := os.Stat(entry.task.InputFile); err == nil {
		inputSize = fi.Size()
	}

	for {
		select {
		case <-done:
			return
		case <-e.ctx.Done():
			return
		case <-ticker.C:
			entry.mu.RLock()
			if entry.state.IsTerminal() {
				entry.mu.RUnlock()
				return
			}
			outputFile := entry.task.OutputFile
			entry.mu.RUnlock()

			// 基于输出文件大小估算进度
			if outputFile != "" && inputSize > 0 {
				if fi, err := os.Stat(outputFile); err == nil {
					// 粗略估算：输出大小 / 输入大小 * 100
					// 由于编码参数不同，输出可能大于或小于输入，
					// 所以这里用 min(ratio*100, 99) 封顶
					ratio := float64(fi.Size()) / float64(inputSize) * 100.0
					if ratio > 99.0 {
						ratio = 99.0
					}
					if ratio < 0 {
						ratio = 0
					}

					entry.mu.Lock()
					entry.progress = ratio
					entry.mu.Unlock()
				}
			}

			e.notifyProgress(entry.snapshot())
		}
	}
}

// ---------------------------------------------------------------------------
// notifyProgress 发送进度回调
// ---------------------------------------------------------------------------

func (e *Executor) notifyProgress(progress TaskProgress) {
	if e.cfg.OnProgress != nil {
		// 异步调用回调，避免阻塞执行器
		go func(p TaskProgress) {
			defer func() {
				if r := recover(); r != nil {
					logger.Errorf("[Executor] progress callback panicked: %v", r)
				}
			}()
			e.cfg.OnProgress(p)
		}(progress)
	}
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

// dirOf 返回文件路径的目录部分
func dirOf(filePath string) string {
	idx := strings.LastIndexAny(filePath, "/\\")
	if idx < 0 {
		return "."
	}
	return filePath[:idx]
}

// ---------------------------------------------------------------------------
// SubmitBatch 批量提交任务
//
// 返回第一个提交失败的错误（后续任务仍尝试提交）。
// ---------------------------------------------------------------------------

func (e *Executor) SubmitBatch(tasks []ExecTask) (submitted int, firstErr error) {
	for _, t := range tasks {
		if err := e.Submit(t); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			logger.Warnf("[Executor] batch submit: task %s failed: %v", t.TaskID, err)
			continue
		}
		submitted++
	}

	logger.Noticef("[Executor] batch submitted: %d/%d tasks", submitted, len(tasks))
	return submitted, firstErr
}

// ---------------------------------------------------------------------------
// WaitForAll 阻塞等待所有任务进入终态
//
// 返回所有任务的最终进度快照。
// pollInterval 控制检查频率，为 0 时使用默认值 500ms。
// ---------------------------------------------------------------------------

func (e *Executor) WaitForAll(pollInterval time.Duration) []TaskProgress {
	if pollInterval <= 0 {
		pollInterval = 500 * time.Millisecond
	}

	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-e.ctx.Done():
			return e.ListTasks()
		case <-ticker.C:
			allDone := true
			e.mu.RLock()
			for _, entry := range e.tasks {
				entry.mu.RLock()
				if !entry.state.IsTerminal() {
					allDone = false
				}
				entry.mu.RUnlock()
				if !allDone {
					break
				}
			}
			e.mu.RUnlock()

			if allDone {
				return e.ListTasks()
			}
		}
	}
}

// ---------------------------------------------------------------------------
// WaitForTask 阻塞等待指定任务完成
//
// 返回该任务的最终进度快照。
// ---------------------------------------------------------------------------

func (e *Executor) WaitForTask(taskID string, pollInterval time.Duration) (TaskProgress, error) {
	if pollInterval <= 0 {
		pollInterval = 500 * time.Millisecond
	}

	e.mu.RLock()
	entry, exists := e.tasks[taskID]
	e.mu.RUnlock()

	if !exists {
		return TaskProgress{}, fmt.Errorf("task %s not found", taskID)
	}

	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-e.ctx.Done():
			return entry.snapshot(), fmt.Errorf("executor context cancelled")
		case <-ticker.C:
			entry.mu.RLock()
			done := entry.state.IsTerminal()
			entry.mu.RUnlock()

			if done {
				return entry.snapshot(), nil
			}
		}
	}
}

// ---------------------------------------------------------------------------
// Stats 执行器统计摘要
// ---------------------------------------------------------------------------

type Stats struct {
	Total     int
	Pending   int
	Running   int
	Completed int
	Failed    int
	Cancelled int
	Timeout   int
}

func (e *Executor) Stats() Stats {
	e.mu.RLock()
	defer e.mu.RUnlock()

	var s Stats
	s.Total = len(e.tasks)

	for _, entry := range e.tasks {
		entry.mu.RLock()
		switch entry.state {
		case TaskStatePending:
			s.Pending++
		case TaskStateRunning:
			s.Running++
		case TaskStateCompleted:
			s.Completed++
		case TaskStateFailed:
			s.Failed++
		case TaskStateCancelled:
			s.Cancelled++
		case TaskStateTimeout:
			s.Timeout++
		}
		entry.mu.RUnlock()
	}

	return s
}
