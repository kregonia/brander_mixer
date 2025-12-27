package status

import (
	"runtime"
	"time"

	worker_2_controller_service "github.com/kregonia/brander_mixer/script/rpc_server/worker"
	"github.com/kregonia/brander_mixer/widget/parameter"
	"github.com/shirou/gopsutil/v4/cpu"
	"github.com/shirou/gopsutil/v4/disk"
	"github.com/shirou/gopsutil/v4/mem"
)

var (
	preDiskStat                 map[string]disk.IOCountersStat
	diskCapacityRefreshTimeLeft = 0 // 每隔 10 分钟刷新一次磁盘容量数据
)

// GetWorkerStatus 收集当前机器的基础运行信息（尽量在 Linux 上工作）
func GetWorkerStatus() (*worker_2_controller_service.Status, error) {
	logicCores, err := cpu.Info()
	if err != nil {
		return nil, err
	}
	physicalCores := logicCores[0].Cores
	cpuUsages, err := cpu.Percent(time.Second, true)
	if err != nil {
		return nil, err
	}

	memInfo, err := mem.VirtualMemory()
	if err != nil {
		return nil, err
	}
	memUsage := memInfo.UsedPercent
	memTotal := memInfo.Total

	usage, err := disk.Usage("/")
	if err != nil {
		return nil, err
	}
	readBytesPer5Sec := uint64(0)
	writeBytesPer5Sec := uint64(0)
	if diskCapacityRefreshTimeLeft <= 0 {
		diskCapacityRefreshTimeLeft = parameter.DefaultDiskCapacityRefreshTimeLeft
		diskStat, err := disk.IOCounters()
		if err != nil {
			return nil, err
		}
		if preDiskStat != nil {
			preDiskStat = diskStat
		} else {
			if readCount, exist := diskStat["readCount"]; exist {
				if preReadCount, exist := preDiskStat["readCount"]; exist {
					readBytesPer5Sec = readCount.ReadBytes - preReadCount.ReadBytes
				}
			}
			if writeCount, exist := diskStat["writeCount"]; exist {
				if preWriteCount, exist := preDiskStat["writeCount"]; exist {
					writeBytesPer5Sec = writeCount.WriteBytes - preWriteCount.WriteBytes
				}
			}
			preDiskStat = diskStat
		}
	} else {
		diskCapacityRefreshTimeLeft--
	}

	// 任务数：使用当前 goroutine 数作为近似值
	taskNum := runtime.NumGoroutine()

	return &worker_2_controller_service.Status{
		CpuLogicalCores:       int32(len(logicCores)),
		CpuUsagePercents:      cpuUsages,
		SuperThreadingEnabled: len(logicCores) > int(physicalCores),
		MemoryUsagePercent:    memUsage,
		MemoryTotal:           memTotal,
		TaskCount:             int32(taskNum),
		DiskUsagePercent:      usage.UsedPercent,
		DiskTotal:             usage.Total,
		DiskReadBytes:         int32(readBytesPer5Sec),
		DiskWriteBytes:        int32(writeBytesPer5Sec),
	}, nil
}
