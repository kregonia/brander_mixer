package nodetable

import (
	"context"
	"math"
	"sync"
	"time"

	logger "github.com/kregonia/brander_mixer/log"
	worker_2_controller_service "github.com/kregonia/brander_mixer/script/rpc_server/worker"
)

// ---------------------------------------------------------------------------
// 滑动窗口可靠性预测参数
// ---------------------------------------------------------------------------

const (
	// ReliabilityWindowSize 滑动窗口大小（记录最近 N 次心跳事件）
	ReliabilityWindowSize = 30

	// ReliabilityTrendThreshold 可靠性下降趋势阈值
	// 当最近窗口的可靠性比历史可靠性低超过此值时，认为趋势明显下降
	ReliabilityTrendThreshold = 0.15

	// RecentWindowRatio 用于计算"近期"可靠性的窗口比例
	// 取最近 1/3 窗口的数据来和整体窗口对比
	RecentWindowRatio = 3
)

// HeartbeatEvent 心跳事件记录（hit=true 表示收到心跳，false 表示超时未收到）
type HeartbeatEvent struct {
	Timestamp time.Time
	Hit       bool // true=收到心跳, false=心跳超时
}

// ---------------------------------------------------------------------------
// NodeState 节点在线状态枚举
// ---------------------------------------------------------------------------

type NodeState int

const (
	NodeStateOnline   NodeState = iota // 正常在线
	NodeStateUnstable                  // 心跳不稳定（连续丢失 1~2 次）
	NodeStateOffline                   // 已离线
)

func (s NodeState) String() string {
	switch s {
	case NodeStateOnline:
		return "online"
	case NodeStateUnstable:
		return "unstable"
	case NodeStateOffline:
		return "offline"
	}
	return "unknown"
}

// ---------------------------------------------------------------------------
// NodeRecord 单个节点在 controller 侧的完整记录
// ---------------------------------------------------------------------------

type NodeRecord struct {
	// 基础信息（注册时上报，一般不变）
	Info *worker_2_controller_service.WorkerInfo

	// Worker 侧 Controller2Worker gRPC 服务地址（注册时上报）
	GrpcAddr string

	// 最新一次心跳携带的运行时状态
	LatestStatus *worker_2_controller_service.Status

	// 心跳时间线
	LastHeartbeat time.Time // 最近一次收到心跳的时刻
	RegisteredAt  time.Time // 注册时刻

	// 可靠性统计
	HeartbeatHitCount  int64 // 累计收到心跳次数
	HeartbeatMissCount int64 // 累计心跳超时次数
	FailCount          int64 // 累计失败/离线次数

	// 滑动窗口心跳事件（用于可靠性趋势预测）
	HeartbeatWindow []HeartbeatEvent

	// 当前在线状态
	State NodeState

	// 当前分配的任务 ID 列表
	AssignedTasks []string
}

// Reliability 返回 [0,1] 的可靠性分数（全局历史）
func (nr *NodeRecord) Reliability() float64 {
	total := nr.HeartbeatHitCount + nr.HeartbeatMissCount
	if total == 0 {
		return 1.0 // 刚注册，默认可靠
	}
	return float64(nr.HeartbeatHitCount) / float64(total)
}

// ReliabilityWindow 返回基于滑动窗口的可靠性分数 [0,1]
//
// 只看最近 ReliabilityWindowSize 次心跳事件，比全局 Reliability() 更灵敏。
// 如果窗口内没有数据则回退到全局 Reliability()。
func (nr *NodeRecord) ReliabilityWindow() float64 {
	if len(nr.HeartbeatWindow) == 0 {
		return nr.Reliability()
	}
	hits := 0
	for _, ev := range nr.HeartbeatWindow {
		if ev.Hit {
			hits++
		}
	}
	return float64(hits) / float64(len(nr.HeartbeatWindow))
}

// ReliabilityTrend 返回可靠性趋势值
//
// 正值 → 可靠性上升（恢复中）
// 零值 → 稳定
// 负值 → 可靠性下降（有离线风险）
//
// 算法：将滑动窗口分为 "近期" 和 "整体"，返回 recent - overall
func (nr *NodeRecord) ReliabilityTrend() float64 {
	n := len(nr.HeartbeatWindow)
	if n < 4 {
		// 数据太少，无法判断趋势
		return 0
	}

	// 整体窗口可靠性
	overallHits := 0
	for _, ev := range nr.HeartbeatWindow {
		if ev.Hit {
			overallHits++
		}
	}
	overallRate := float64(overallHits) / float64(n)

	// 近期可靠性（取最近 1/RecentWindowRatio 的数据）
	recentSize := n / RecentWindowRatio
	if recentSize < 2 {
		recentSize = 2
	}
	recentStart := n - recentSize
	recentHits := 0
	for _, ev := range nr.HeartbeatWindow[recentStart:] {
		if ev.Hit {
			recentHits++
		}
	}
	recentRate := float64(recentHits) / float64(recentSize)

	return recentRate - overallRate
}

// IsDeclining 判断节点可靠性是否呈明显下降趋势
//
// 当 ReliabilityTrend < -ReliabilityTrendThreshold 时返回 true
// 可用于触发提前任务迁移（speculative execution）
func (nr *NodeRecord) IsDeclining() bool {
	return nr.ReliabilityTrend() < -ReliabilityTrendThreshold
}

// appendHeartbeatEvent 追加一条心跳事件到滑动窗口（内部调用，调用方需持有锁）
func (nr *NodeRecord) appendHeartbeatEvent(hit bool) {
	ev := HeartbeatEvent{
		Timestamp: time.Now(),
		Hit:       hit,
	}
	nr.HeartbeatWindow = append(nr.HeartbeatWindow, ev)
	// 保持窗口大小不超过上限
	if len(nr.HeartbeatWindow) > ReliabilityWindowSize {
		nr.HeartbeatWindow = nr.HeartbeatWindow[len(nr.HeartbeatWindow)-ReliabilityWindowSize:]
	}
}

// PredictOfflineProbability 基于滑动窗口估算节点在未来 N 次心跳内离线的概率
//
// 使用简单的指数加权：越近期的 miss 权重越高。
// 返回 [0,1]，越大表示越有可能离线。
func (nr *NodeRecord) PredictOfflineProbability() float64 {
	n := len(nr.HeartbeatWindow)
	if n == 0 {
		return 0
	}

	// 指数加权：最近的事件权重最高
	// weight_i = exp(decay * (i - n + 1)), i = 0..n-1
	const decay = 0.15
	weightedMiss := 0.0
	totalWeight := 0.0
	for i, ev := range nr.HeartbeatWindow {
		w := math.Exp(decay * float64(i-n+1))
		totalWeight += w
		if !ev.Hit {
			weightedMiss += w
		}
	}

	if totalWeight == 0 {
		return 0
	}
	return weightedMiss / totalWeight
}

// Uptime 返回自注册以来的运行时间
func (nr *NodeRecord) Uptime() time.Duration {
	return time.Since(nr.RegisteredAt)
}

// ---------------------------------------------------------------------------
// NodeTable 全局节点表（线程安全）
// ---------------------------------------------------------------------------

type NodeTable struct {
	mu    sync.RWMutex
	nodes map[string]*NodeRecord // key = workerID (ip)
}

func NewNodeTable() *NodeTable {
	return &NodeTable{
		nodes: make(map[string]*NodeRecord),
	}
}

func (nt *NodeTable) Register(workerID string, info *worker_2_controller_service.WorkerInfo, grpcAddr string) {
	nt.mu.Lock()
	defer nt.mu.Unlock()
	now := time.Now()
	nt.nodes[workerID] = &NodeRecord{
		Info:          info,
		GrpcAddr:      grpcAddr,
		LastHeartbeat: now,
		RegisteredAt:  now,
		State:         NodeStateOnline,
	}
}

func (nt *NodeTable) UpdateHeartbeat(workerID string, status *worker_2_controller_service.Status) {
	nt.mu.Lock()
	defer nt.mu.Unlock()
	rec, ok := nt.nodes[workerID]
	if !ok {
		return
	}
	rec.LastHeartbeat = time.Now()
	rec.LatestStatus = status
	rec.HeartbeatHitCount++
	rec.appendHeartbeatEvent(true) // 记录心跳命中事件到滑动窗口
	if rec.State != NodeStateOnline {
		rec.State = NodeStateOnline
		logger.Noticef("[NodeTable] node %s back online (window reliability=%.2f)", workerID, rec.ReliabilityWindow())
	}
}

func (nt *NodeTable) Get(workerID string) (*NodeRecord, bool) {
	nt.mu.RLock()
	defer nt.mu.RUnlock()
	rec, ok := nt.nodes[workerID]
	return rec, ok
}

func (nt *NodeTable) Remove(workerID string) {
	nt.mu.Lock()
	defer nt.mu.Unlock()
	delete(nt.nodes, workerID)
}

// OnlineNodes 返回当前所有在线节点 ID 列表
func (nt *NodeTable) OnlineNodes() []string {
	nt.mu.RLock()
	defer nt.mu.RUnlock()
	var ids []string
	for id, rec := range nt.nodes {
		if rec.State == NodeStateOnline || rec.State == NodeStateUnstable {
			ids = append(ids, id)
		}
	}
	return ids
}

// AllNodes 返回所有节点 ID 和记录（用于调度器遍历）
func (nt *NodeTable) AllNodes() map[string]*NodeRecord {
	nt.mu.RLock()
	defer nt.mu.RUnlock()
	cp := make(map[string]*NodeRecord, len(nt.nodes))
	for k, v := range nt.nodes {
		cp[k] = v
	}
	return cp
}

// AssignTask 给指定节点追加一个任务 ID
func (nt *NodeTable) AssignTask(workerID, taskID string) {
	nt.mu.Lock()
	defer nt.mu.Unlock()
	if rec, ok := nt.nodes[workerID]; ok {
		rec.AssignedTasks = append(rec.AssignedTasks, taskID)
	}
}

// RemoveTask 从指定节点移除一个任务 ID
func (nt *NodeTable) RemoveTask(workerID, taskID string) {
	nt.mu.Lock()
	defer nt.mu.Unlock()
	rec, ok := nt.nodes[workerID]
	if !ok {
		return
	}
	for i, id := range rec.AssignedTasks {
		if id == taskID {
			rec.AssignedTasks = append(rec.AssignedTasks[:i], rec.AssignedTasks[i+1:]...)
			return
		}
	}
}

// ---------------------------------------------------------------------------
// 心跳超时巡检（自愈机制基础）
// ---------------------------------------------------------------------------

const (
	HeartbeatTimeout  = 15 * time.Second // 超过此时间未收到心跳 → unstable
	OfflineTimeout    = 30 * time.Second // 超过此时间未收到心跳 → offline
	HealCheckInterval = 5 * time.Second  // 巡检间隔
)

// StartHealingLoop 启动后台巡检协程，检测心跳超时并触发自愈逻辑
func (nt *NodeTable) StartHealingLoop(ctx context.Context, onNodeOffline func(workerID string, tasks []string)) {
	go func() {
		ticker := time.NewTicker(HealCheckInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				nt.checkHeartbeats(onNodeOffline)
			}
		}
	}()
}

// OnNodeDeclining 节点可靠性下降时的回调签名
// workerID: 可靠性下降的节点 ID
// tasks: 该节点上的任务列表
// probability: 预测离线概率 [0,1]
type OnNodeDeclining func(workerID string, tasks []string, probability float64)

// StartHealingLoopWithPrediction 启动增强版后台巡检协程
//
// 除了原有的心跳超时检测 + offline 回调外，还增加了基于滑动窗口的
// 可靠性趋势预测。当检测到节点可靠性明显下降时，提前触发 onDeclining 回调，
// 使调度器可以实施 speculative execution（渐进式任务迁移）。
func (nt *NodeTable) StartHealingLoopWithPrediction(
	ctx context.Context,
	onNodeOffline func(workerID string, tasks []string),
	onDeclining OnNodeDeclining,
) {
	go func() {
		ticker := time.NewTicker(HealCheckInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				nt.checkHeartbeats(onNodeOffline)
				nt.checkReliabilityTrend(onDeclining)
			}
		}
	}()
}

func (nt *NodeTable) checkHeartbeats(onNodeOffline func(workerID string, tasks []string)) {
	nt.mu.Lock()
	defer nt.mu.Unlock()
	now := time.Now()
	for id, rec := range nt.nodes {
		if rec.State == NodeStateOffline {
			continue // 已经处理过
		}
		elapsed := now.Sub(rec.LastHeartbeat)
		if elapsed > OfflineTimeout {
			// 记录 miss 事件到滑动窗口（估算丢失次数）
			missCount := int(elapsed / (5 * time.Second))
			for i := 0; i < missCount; i++ {
				rec.appendHeartbeatEvent(false)
			}
			// 标记离线
			rec.State = NodeStateOffline
			rec.FailCount++
			rec.HeartbeatMissCount += int64(missCount)
			logger.Warnf("[Healing] node %s OFFLINE (no heartbeat for %v), failCount=%d, windowReliability=%.2f, offlineProb=%.2f",
				id, elapsed, rec.FailCount, rec.ReliabilityWindow(), rec.PredictOfflineProbability())
			// 拷贝任务列表，异步触发迁移回调
			if len(rec.AssignedTasks) > 0 && onNodeOffline != nil {
				tasks := make([]string, len(rec.AssignedTasks))
				copy(tasks, rec.AssignedTasks)
				go onNodeOffline(id, tasks)
			}
		} else if elapsed > HeartbeatTimeout {
			if rec.State == NodeStateOnline {
				rec.State = NodeStateUnstable
				rec.HeartbeatMissCount++
				rec.appendHeartbeatEvent(false) // 记录 miss 事件到滑动窗口
				logger.Warnf("[Healing] node %s UNSTABLE (no heartbeat for %v), windowReliability=%.2f, trend=%.3f",
					id, elapsed, rec.ReliabilityWindow(), rec.ReliabilityTrend())
			}
		}
	}
}

// checkReliabilityTrend 检查所有在线/不稳定节点的可靠性趋势
//
// 当检测到可靠性明显下降趋势时，提前触发 onDeclining 回调，
// 不必等到节点真正 offline 才做任务迁移。
func (nt *NodeTable) checkReliabilityTrend(onDeclining OnNodeDeclining) {
	if onDeclining == nil {
		return
	}

	nt.mu.RLock()
	defer nt.mu.RUnlock()

	for id, rec := range nt.nodes {
		if rec.State == NodeStateOffline {
			continue
		}
		if len(rec.AssignedTasks) == 0 {
			continue // 没有任务的节点不需要关心
		}

		if rec.IsDeclining() {
			prob := rec.PredictOfflineProbability()
			logger.Warnf("[Healing] node %s reliability DECLINING: trend=%.3f, windowReliability=%.2f, offlineProb=%.2f, tasks=%d",
				id, rec.ReliabilityTrend(), rec.ReliabilityWindow(), prob, len(rec.AssignedTasks))

			tasks := make([]string, len(rec.AssignedTasks))
			copy(tasks, rec.AssignedTasks)
			go onDeclining(id, tasks, prob)
		}
	}
}
