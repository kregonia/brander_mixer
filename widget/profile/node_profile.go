package profile

import (
	worker_2_controller_service "github.com/kregonia/brander_mixer/script/rpc_server/worker"
)

// ---------------------------------------------------------------------------
// NodeProfile 节点能力画像
//
// 从 WorkerInfo（静态，注册时上报）和 Status（动态，心跳上报）中提取出
// 一个统一的能力向量，供调度器打分使用。
//
// 能力向量：
//   CPU     — 逻辑核心数 × (1 - 平均使用率)  → 剩余 CPU 算力
//   Memory  — 总内存 × (1 - 使用率)           → 剩余内存 (bytes)
//   Disk    — 总磁盘 × (1 - 使用率)           → 剩余磁盘 (bytes)
//   Network — 当前网络带宽使用 (sent+recv)     → 越小越好
//   GPU     — 预留字段（后续扩展 VPU/NPU）
//
// 另外附加：
//   Reliability — 由 NodeTable 中的 HeartbeatHit/Miss 计算，[0,1]
//   TaskCount   — 当前正在执行的任务数
// ---------------------------------------------------------------------------

// NodeProfile 节点能力画像结构体
type NodeProfile struct {
	// 标识
	WorkerID string
	Hostname string
	OS       string
	Arch     string

	// 静态能力（注册时确定，一般不变）
	CPUModelName     string
	CPULogicalCores  int32
	CPUPhysicalCores int32
	MemoryTotalBytes uint64
	DiskTotalBytes   uint64

	// 动态负载（每次心跳更新）
	CPUAvailableRatio    float64 // [0,1] 剩余 CPU 比例
	MemoryAvailableRatio float64 // [0,1] 剩余内存比例
	DiskAvailableRatio   float64 // [0,1] 剩余磁盘比例
	NetworkLoadBytes     int64   // 当前周期内网络收发字节总和

	// 应用层
	TaskCount int32 // 当前正在执行的任务数

	// 可靠性（由外部 NodeTable 注入）
	Reliability float64 // [0,1]
	GPUScore    float64 // 预留：GPU/VPU/NPU 能力分，默认 0
}

// ---------------------------------------------------------------------------
// BuildProfile 从 WorkerInfo + Status + 外部可靠性分 构建 NodeProfile
// ---------------------------------------------------------------------------

func BuildProfile(
	workerID string,
	info *worker_2_controller_service.WorkerInfo,
	status *worker_2_controller_service.Status,
	reliability float64,
) *NodeProfile {
	p := &NodeProfile{
		WorkerID:    workerID,
		Reliability: reliability,
	}

	// ---------- 静态信息 ----------
	if info != nil {
		p.Hostname = info.GetHostname()
		p.OS = info.GetOs()
		p.Arch = info.GetArch()
		p.CPUModelName = info.GetCpuModelName()
		p.CPULogicalCores = info.GetCpuLogicalCores()
		p.CPUPhysicalCores = info.GetCpuPhysicalCores()
	}

	// ---------- 动态状态 ----------
	if status == nil {
		// 没有心跳数据，给保守默认值
		p.CPUAvailableRatio = 0
		p.MemoryAvailableRatio = 0
		p.DiskAvailableRatio = 0
		return p
	}

	// CPU
	if cpuInfo := status.GetCpu(); cpuInfo != nil {
		avgUsage := averageFloat64(cpuInfo.GetCpuUsagePercents())
		p.CPUAvailableRatio = clamp01(1.0 - avgUsage/100.0)
		if p.CPULogicalCores == 0 {
			p.CPULogicalCores = cpuInfo.GetCpuLogicalCores()
		}
	}

	// Memory
	if memInfo := status.GetMemory(); memInfo != nil {
		p.MemoryTotalBytes = memInfo.GetMemoryTotal()
		p.MemoryAvailableRatio = clamp01(1.0 - memInfo.GetMemoryUsagePercent()/100.0)
	}

	// Disk
	if diskInfo := status.GetDisk(); diskInfo != nil {
		p.DiskTotalBytes = diskInfo.GetDiskTotal()
		p.DiskAvailableRatio = clamp01(1.0 - diskInfo.GetDiskUsagePercent()/100.0)
	}

	// Network
	if netInfo := status.GetNetwork(); netInfo != nil {
		p.NetworkLoadBytes = int64(netInfo.GetNetworkSentBytes()) + int64(netInfo.GetNetworkReceivedBytes())
	}

	// TaskCount
	p.TaskCount = status.GetTaskCount()

	return p
}

// ---------------------------------------------------------------------------
// CPUCapacity 综合 CPU 能力分 = 逻辑核心数 × 剩余比例
// ---------------------------------------------------------------------------

func (p *NodeProfile) CPUCapacity() float64 {
	return float64(p.CPULogicalCores) * p.CPUAvailableRatio
}

// ---------------------------------------------------------------------------
// MemoryCapacityBytes 剩余可用内存 (bytes)
// ---------------------------------------------------------------------------

func (p *NodeProfile) MemoryCapacityBytes() float64 {
	return float64(p.MemoryTotalBytes) * p.MemoryAvailableRatio
}

// ---------------------------------------------------------------------------
// DiskCapacityBytes 剩余可用磁盘 (bytes)
// ---------------------------------------------------------------------------

func (p *NodeProfile) DiskCapacityBytes() float64 {
	return float64(p.DiskTotalBytes) * p.DiskAvailableRatio
}

// ---------------------------------------------------------------------------
// CapabilityVector 返回归一化的能力向量（用于多目标优化调度）
//
// 返回 5 维向量：[cpu, memory, disk, network_idle, reliability]
// 所有维度范围 [0, 1]，越大越好
// ---------------------------------------------------------------------------

func (p *NodeProfile) CapabilityVector() [5]float64 {
	// CPU: 用逻辑核心数归一化（假设最大 128 核）
	cpuNorm := clamp01(float64(p.CPULogicalCores) * p.CPUAvailableRatio / 128.0)

	// Memory: 用 256GB 归一化
	memNorm := clamp01(float64(p.MemoryTotalBytes) * p.MemoryAvailableRatio / (256.0 * 1024 * 1024 * 1024))

	// Disk: 用 4TB 归一化
	diskNorm := clamp01(float64(p.DiskTotalBytes) * p.DiskAvailableRatio / (4.0 * 1024 * 1024 * 1024 * 1024))

	// Network idle: 网络负载越低越好，用 1GB/s 归一化后取反
	netNorm := clamp01(1.0 - float64(p.NetworkLoadBytes)/(1024.0*1024*1024))

	return [5]float64{
		cpuNorm,
		memNorm,
		diskNorm,
		netNorm,
		p.Reliability,
	}
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func averageFloat64(vals []float64) float64 {
	if len(vals) == 0 {
		return 0
	}
	sum := 0.0
	for _, v := range vals {
		sum += v
	}
	return sum / float64(len(vals))
}

func clamp01(v float64) float64 {
	if v < 0 {
		return 0
	}
	if v > 1 {
		return 1
	}
	return v
}
