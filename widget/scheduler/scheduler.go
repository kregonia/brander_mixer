package scheduler

import (
	"fmt"
	"math"
	"sort"
	"sync"

	logger "github.com/kregonia/brander_mixer/log"
	"github.com/kregonia/brander_mixer/widget/nodetable"
	"github.com/kregonia/brander_mixer/widget/profile"
)

// ---------------------------------------------------------------------------
// Speculative Execution 参数
// ---------------------------------------------------------------------------

const (
	// SpeculativeProbThreshold 当节点离线概率超过此值时，触发 speculative 副本
	SpeculativeProbThreshold = 0.35

	// SpeculativeMaxReplicas 每个任务最多创建的 speculative 副本数
	SpeculativeMaxReplicas = 1
)

// SpeculativeResult 一次 speculative execution 调度的结果
type SpeculativeResult struct {
	OriginalWorkerID string  // 原任务所在的 worker
	TaskID           string  // 原任务 ID
	ReplicaTaskID    string  // speculative 副本任务 ID
	ReplicaWorkerID  string  // 副本被分配到的 worker
	ReplicaScore     float64 // 副本节点得分
	ReplicaProfile   *profile.NodeProfile
	OfflineProb      float64 // 原 worker 的预测离线概率
}

// ---------------------------------------------------------------------------
// 调度权重配置（多目标优化）
//
//	score = α * latencyScore + β * loadScore + γ * reliabilityScore
//
// 所有 score 范围 [0, 1]，越高越好。调度器选择 score 最高的节点。
// ---------------------------------------------------------------------------

type SchedulerWeights struct {
	Alpha float64 // 延迟/响应（基于 CPU + 网络空闲）
	Beta  float64 // 负载均衡（基于 CPU/Memory 剩余 + 当前任务数）
	Gamma float64 // 可靠性（基于心跳命中率）
}

// DefaultWeights 默认权重
var DefaultWeights = SchedulerWeights{
	Alpha: 0.3,
	Beta:  0.4,
	Gamma: 0.3,
}

// ---------------------------------------------------------------------------
// TaskRequirement 任务对资源的需求描述
// ---------------------------------------------------------------------------

type TaskRequirement struct {
	TaskID   string
	TaskType string // "video_transcode", "image_process", "generic" 等

	// 预估资源需求（归一化到 [0,1]，与 NodeProfile.CapabilityVector 同维度对齐）
	MinCPU         float64 // 最低 CPU 能力（归一化值）
	MinMemory      float64 // 最低内存（归一化值）
	MinDisk        float64 // 最低磁盘（归一化值）
	MinReliability float64 // 最低可靠性要求

	// 任务预估耗时权重（越高说明任务越重，应分给更强的节点）
	Weight float64
}

// ---------------------------------------------------------------------------
// ScheduleResult 调度结果
// ---------------------------------------------------------------------------

type ScheduleResult struct {
	WorkerID string
	Score    float64
	Profile  *profile.NodeProfile
}

// ---------------------------------------------------------------------------
// Scheduler 智能调度器
// ---------------------------------------------------------------------------

type Scheduler struct {
	mu        sync.RWMutex
	weights   SchedulerWeights
	nodeTable *nodetable.NodeTable
}

// NewScheduler 创建调度器
func NewScheduler(w *SchedulerWeights) *Scheduler {
	if w == nil {
		w = &DefaultWeights
	}
	return &Scheduler{weights: *w}
}

// SetNodeTable 设置调度器使用的节点表（避免循环依赖，在 controller 启动时注入）
func (s *Scheduler) SetNodeTable(nt *nodetable.NodeTable) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nodeTable = nt
}

// SetWeights 动态更新权重
func (s *Scheduler) SetWeights(w SchedulerWeights) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.weights = w
}

// ---------------------------------------------------------------------------
// SelectNode 为指定任务选择最优节点
//
// 核心算法：
//  1. 遍历 NodeTable 中所有在线节点
//  2. 为每个节点构建 NodeProfile
//  3. 过滤不满足最低要求的节点
//  4. 使用加权多目标评分函数打分
//  5. 返回得分最高的节点
// ---------------------------------------------------------------------------

func (s *Scheduler) SelectNode(req TaskRequirement) (*ScheduleResult, error) {
	s.mu.RLock()
	weights := s.weights
	nt := s.nodeTable
	s.mu.RUnlock()

	if nt == nil {
		return nil, fmt.Errorf("scheduler has no NodeTable set")
	}

	allNodes := nt.AllNodes()

	if len(allNodes) == 0 {
		return nil, fmt.Errorf("no available nodes in NodeTable")
	}

	type candidate struct {
		workerID string
		score    float64
		profile  *profile.NodeProfile
	}

	var candidates []candidate

	for workerID, rec := range allNodes {
		// 只考虑在线或不稳定（但还没完全离线）的节点
		if rec.State == nodetable.NodeStateOffline {
			continue
		}

		// 构建能力画像
		p := profile.BuildProfile(workerID, rec.Info, rec.LatestStatus, rec.Reliability())

		// 获取归一化能力向量 [cpu, memory, disk, network_idle, reliability]
		vec := p.CapabilityVector()

		// ---------- 过滤：不满足最低要求的节点 ----------
		if vec[0] < req.MinCPU {
			logger.Noticef("[Scheduler] node %s filtered: cpu %.3f < min %.3f", workerID, vec[0], req.MinCPU)
			continue
		}
		if vec[1] < req.MinMemory {
			logger.Noticef("[Scheduler] node %s filtered: memory %.3f < min %.3f", workerID, vec[1], req.MinMemory)
			continue
		}
		if vec[2] < req.MinDisk {
			logger.Noticef("[Scheduler] node %s filtered: disk %.3f < min %.3f", workerID, vec[2], req.MinDisk)
			continue
		}
		if vec[4] < req.MinReliability {
			logger.Noticef("[Scheduler] node %s filtered: reliability %.3f < min %.3f", workerID, vec[4], req.MinReliability)
			continue
		}

		// ---------- 打分：多目标加权 ----------

		// latencyScore: CPU 能力 + 网络空闲（越高越快）
		latencyScore := 0.6*vec[0] + 0.4*vec[3]

		// loadScore: 综合剩余资源 + 当前任务数惩罚
		taskPenalty := 1.0 / (1.0 + float64(p.TaskCount)*0.2) // 任务越多分越低
		loadScore := (0.4*vec[0] + 0.3*vec[1] + 0.15*vec[2] + 0.15*vec[3]) * taskPenalty

		// reliabilityScore: 直接取可靠性
		reliabilityScore := vec[4]

		// 综合分
		score := weights.Alpha*latencyScore +
			weights.Beta*loadScore +
			weights.Gamma*reliabilityScore

		// 如果任务很重（weight > 1），给高能力节点额外加分
		if req.Weight > 1.0 {
			cpuBonus := vec[0] * 0.1 * math.Log2(req.Weight)
			score += cpuBonus
		}

		candidates = append(candidates, candidate{
			workerID: workerID,
			score:    score,
			profile:  p,
		})
	}

	if len(candidates) == 0 {
		return nil, fmt.Errorf("no nodes satisfy task requirements (taskID=%s)", req.TaskID)
	}

	// 按 score 降序排序
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].score > candidates[j].score
	})

	best := candidates[0]
	logger.Noticef("[Scheduler] selected node %s (score=%.4f) for task %s (from %d candidates)",
		best.workerID, best.score, req.TaskID, len(candidates))

	return &ScheduleResult{
		WorkerID: best.workerID,
		Score:    best.score,
		Profile:  best.profile,
	}, nil
}

// ---------------------------------------------------------------------------
// SelectNodes 为一组子任务批量选择节点（任务拆解后的分发）
//
// 返回 taskID → ScheduleResult 的映射
// 如果某个子任务找不到合适节点，会在 errors map 中记录
// ---------------------------------------------------------------------------

func (s *Scheduler) SelectNodes(tasks []TaskRequirement) (map[string]*ScheduleResult, map[string]error) {
	results := make(map[string]*ScheduleResult, len(tasks))
	errs := make(map[string]error)

	for _, task := range tasks {
		result, err := s.SelectNode(task)
		if err != nil {
			errs[task.TaskID] = err
			logger.Warnf("[Scheduler] failed to schedule task %s: %v", task.TaskID, err)
			continue
		}
		results[task.TaskID] = result
	}

	logger.Noticef("[Scheduler] batch schedule: %d tasks, %d assigned, %d failed",
		len(tasks), len(results), len(errs))

	return results, errs
}

// ---------------------------------------------------------------------------
// ReassignTasks 自愈：将离线节点的任务重新调度到在线节点
//
// 这个函数会被 NodeTable 的 StartHealingLoop 在检测到节点离线时调用
// ---------------------------------------------------------------------------

func (s *Scheduler) ReassignTasks(offlineWorkerID string, taskIDs []string) (map[string]*ScheduleResult, map[string]error) {
	logger.Warnf("[Scheduler] reassigning %d tasks from offline node %s", len(taskIDs), offlineWorkerID)

	s.mu.RLock()
	nt := s.nodeTable
	s.mu.RUnlock()

	tasks := make([]TaskRequirement, 0, len(taskIDs))
	for _, taskID := range taskIDs {
		// 重新分配时使用宽松的最低要求和普通权重
		tasks = append(tasks, TaskRequirement{
			TaskID:         taskID,
			TaskType:       "reassigned",
			MinCPU:         0.01,
			MinMemory:      0.01,
			MinDisk:        0.01,
			MinReliability: 0.3, // 至少 30% 可靠性
			Weight:         1.0,
		})
	}

	results, errs := s.SelectNodes(tasks)

	// 更新 NodeTable 中的任务分配
	if nt != nil {
		for taskID, result := range results {
			nt.AssignTask(result.WorkerID, taskID)
			logger.Noticef("[Scheduler] task %s migrated: %s -> %s", taskID, offlineWorkerID, result.WorkerID)
		}
	}

	return results, errs
}

// ---------------------------------------------------------------------------
// SpeculativeExecute 渐进式任务迁移（Speculative Execution）
//
// 当某个节点的可靠性趋势明显下降（但还未真正 offline）时，
// 在另一个健康节点上启动同一任务的副本。任一副本先完成后可取消另一个。
//
// decliningWorkerID: 可靠性下降的节点
// taskIDs:           该节点上需要保护的任务 ID 列表
// offlineProb:       预测的离线概率 [0,1]
//
// 返回: 每个任务的 speculative 调度结果（仅概率超过阈值的任务才产生副本）
// ---------------------------------------------------------------------------

func (s *Scheduler) SpeculativeExecute(
	decliningWorkerID string,
	taskIDs []string,
	offlineProb float64,
) ([]SpeculativeResult, []error) {

	logger.Warnf("[Scheduler/Speculative] node %s declining (offlineProb=%.2f), evaluating %d tasks for speculative copies",
		decliningWorkerID, offlineProb, len(taskIDs))

	// 如果离线概率低于阈值，不需要创建副本
	if offlineProb < SpeculativeProbThreshold {
		logger.Noticef("[Scheduler/Speculative] offlineProb %.2f < threshold %.2f, skipping speculative execution",
			offlineProb, SpeculativeProbThreshold)
		return nil, nil
	}

	s.mu.RLock()
	nt := s.nodeTable
	s.mu.RUnlock()

	if nt == nil {
		return nil, []error{fmt.Errorf("scheduler has no NodeTable set")}
	}

	var results []SpeculativeResult
	var errs []error

	for _, taskID := range taskIDs {
		replicaTaskID := fmt.Sprintf("%s_spec", taskID)

		// 为 speculative 副本寻找一个健康的替代节点
		// 排除正在下降的原始节点
		req := TaskRequirement{
			TaskID:         replicaTaskID,
			TaskType:       "speculative",
			MinCPU:         0.01,
			MinMemory:      0.01,
			MinDisk:        0.01,
			MinReliability: 0.5, // speculative 副本需要更高可靠性
			Weight:         1.0,
		}

		result, err := s.selectNodeExcluding(req, decliningWorkerID)
		if err != nil {
			logger.Warnf("[Scheduler/Speculative] cannot find replica node for task %s: %v", taskID, err)
			errs = append(errs, fmt.Errorf("task %s: %w", taskID, err))
			continue
		}

		// 在 NodeTable 中记录 speculative 任务
		nt.AssignTask(result.WorkerID, replicaTaskID)

		specResult := SpeculativeResult{
			OriginalWorkerID: decliningWorkerID,
			TaskID:           taskID,
			ReplicaTaskID:    replicaTaskID,
			ReplicaWorkerID:  result.WorkerID,
			ReplicaScore:     result.Score,
			ReplicaProfile:   result.Profile,
			OfflineProb:      offlineProb,
		}
		results = append(results, specResult)

		logger.Noticef("[Scheduler/Speculative] task %s: speculative copy %s -> worker %s (score=%.4f)",
			taskID, replicaTaskID, result.WorkerID, result.Score)
	}

	logger.Noticef("[Scheduler/Speculative] speculative execution result: %d replicas created, %d failed",
		len(results), len(errs))

	return results, errs
}

// selectNodeExcluding 与 SelectNode 相同的逻辑，但排除指定的 workerID
func (s *Scheduler) selectNodeExcluding(req TaskRequirement, excludeWorkerID string) (*ScheduleResult, error) {
	s.mu.RLock()
	weights := s.weights
	nt := s.nodeTable
	s.mu.RUnlock()

	if nt == nil {
		return nil, fmt.Errorf("scheduler has no NodeTable set")
	}

	allNodes := nt.AllNodes()

	type candidate struct {
		workerID string
		score    float64
		profile  *profile.NodeProfile
	}

	var candidates []candidate

	for workerID, rec := range allNodes {
		if rec.State == nodetable.NodeStateOffline {
			continue
		}
		if workerID == excludeWorkerID {
			continue // 排除指定节点
		}

		p := profile.BuildProfile(workerID, rec.Info, rec.LatestStatus, rec.Reliability())
		vec := p.CapabilityVector()

		if vec[0] < req.MinCPU || vec[1] < req.MinMemory || vec[2] < req.MinDisk || vec[4] < req.MinReliability {
			continue
		}

		latencyScore := 0.6*vec[0] + 0.4*vec[3]
		taskPenalty := 1.0 / (1.0 + float64(p.TaskCount)*0.2)
		loadScore := (0.4*vec[0] + 0.3*vec[1] + 0.15*vec[2] + 0.15*vec[3]) * taskPenalty
		reliabilityScore := vec[4]

		score := weights.Alpha*latencyScore +
			weights.Beta*loadScore +
			weights.Gamma*reliabilityScore

		// Speculative 副本更偏重可靠性
		score += 0.1 * reliabilityScore

		candidates = append(candidates, candidate{
			workerID: workerID,
			score:    score,
			profile:  p,
		})
	}

	if len(candidates) == 0 {
		return nil, fmt.Errorf("no available nodes (excluding %s) for task %s", excludeWorkerID, req.TaskID)
	}

	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].score > candidates[j].score
	})

	best := candidates[0]
	return &ScheduleResult{
		WorkerID: best.workerID,
		Score:    best.score,
		Profile:  best.profile,
	}, nil
}

// CancelSpeculativeTask 在原任务或副本完成后，取消另一个
//
// 调用方应在任务完成回调中调用此方法清理 NodeTable 中的 speculative 任务记录。
func (s *Scheduler) CancelSpeculativeTask(spec SpeculativeResult) {
	s.mu.RLock()
	nt := s.nodeTable
	s.mu.RUnlock()

	if nt == nil {
		return
	}

	// 从副本节点移除 speculative 任务
	nt.RemoveTask(spec.ReplicaWorkerID, spec.ReplicaTaskID)
	logger.Noticef("[Scheduler/Speculative] cancelled speculative task %s on worker %s (original task %s)",
		spec.ReplicaTaskID, spec.ReplicaWorkerID, spec.TaskID)
}

// ---------------------------------------------------------------------------
// TaskSplitStrategy 任务拆分策略
//
// 根据当前在线节点的能力画像，决定如何将一个大任务拆分成多个子任务
// ---------------------------------------------------------------------------

type TaskSplit struct {
	SubTaskID string
	WorkerID  string
	Weight    float64 // 该子任务占总任务的比重
	Offset    int64   // 例如视频任务中的起始帧/字节偏移
	Length    int64   // 子任务的长度（帧数/字节数）
}

// SplitTask 根据在线节点能力比例拆分任务
//
// totalUnits: 任务总量（比如总帧数、总字节数）
// req: 基础任务需求模板
//
// 返回: 每个节点应分配的子任务描述
func (s *Scheduler) SplitTask(taskID string, totalUnits int64, req TaskRequirement) ([]TaskSplit, error) {
	s.mu.RLock()
	nt := s.nodeTable
	s.mu.RUnlock()

	if nt == nil {
		return nil, fmt.Errorf("scheduler has no NodeTable set")
	}

	allNodes := nt.AllNodes()

	// 只选在线节点
	type nodeWeight struct {
		workerID string
		capacity float64
	}

	var nodes []nodeWeight
	totalCapacity := 0.0

	for workerID, rec := range allNodes {
		if rec.State == nodetable.NodeStateOffline {
			continue
		}

		p := profile.BuildProfile(workerID, rec.Info, rec.LatestStatus, rec.Reliability())
		vec := p.CapabilityVector()

		// 过滤不满足最低要求的
		if vec[0] < req.MinCPU || vec[1] < req.MinMemory || vec[4] < req.MinReliability {
			continue
		}

		// 能力值 = CPU 能力 * 可靠性 * 任务数惩罚
		capacity := p.CPUCapacity() * p.Reliability * (1.0 / (1.0 + float64(p.TaskCount)*0.3))
		if capacity <= 0 {
			capacity = 0.01 // 至少给一点点
		}

		nodes = append(nodes, nodeWeight{
			workerID: workerID,
			capacity: capacity,
		})
		totalCapacity += capacity
	}

	if len(nodes) == 0 {
		return nil, fmt.Errorf("no available nodes for task splitting (taskID=%s)", taskID)
	}

	// 按能力降序排列
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].capacity > nodes[j].capacity
	})

	// 按能力比例分配
	splits := make([]TaskSplit, 0, len(nodes))
	var offset int64
	remaining := totalUnits

	for i, n := range nodes {
		var units int64
		if i == len(nodes)-1 {
			// 最后一个节点拿走剩余全部（避免精度丢失）
			units = remaining
		} else {
			ratio := n.capacity / totalCapacity
			units = int64(math.Round(float64(totalUnits) * ratio))
			if units > remaining {
				units = remaining
			}
		}

		if units <= 0 {
			continue
		}

		subTaskID := fmt.Sprintf("%s_sub_%d", taskID, i)
		splits = append(splits, TaskSplit{
			SubTaskID: subTaskID,
			WorkerID:  n.workerID,
			Weight:    n.capacity / totalCapacity,
			Offset:    offset,
			Length:    units,
		})

		// 更新 NodeTable 中的任务分配
		nt.AssignTask(n.workerID, subTaskID)

		offset += units
		remaining -= units
	}

	logger.Noticef("[Scheduler] task %s split into %d sub-tasks across %d nodes (total=%d units)",
		taskID, len(splits), len(nodes), totalUnits)

	for _, sp := range splits {
		logger.Noticef("[Scheduler]   %s -> worker=%s, offset=%d, length=%d, weight=%.2f%%",
			sp.SubTaskID, sp.WorkerID, sp.Offset, sp.Length, sp.Weight*100)
	}

	return splits, nil
}
