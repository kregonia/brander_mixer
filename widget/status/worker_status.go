package status

import (
	"strings"
	"time"

	worker_2_controller_service "github.com/kregonia/brander_mixer/script/rpc_server/worker"
	"github.com/kregonia/brander_mixer/widget/parameter"
	"github.com/shirou/gopsutil/v4/cpu"
	"github.com/shirou/gopsutil/v4/disk"
	"github.com/shirou/gopsutil/v4/mem"
	"github.com/shirou/gopsutil/v4/net"
)

var (
	preDiskStat                 map[string]disk.IOCountersStat
	preNetworkStat              []net.IOCountersStat
	diskCapacityRefreshTimeLeft = 0 // 每隔 10 分钟刷新一次磁盘容量数据
)

// GetWorkerStatus 收集当前机器的基础运行信息（尽量在 Linux 上工作）
func GetWorkerStatus() (*worker_2_controller_service.Status, error) {
	// CPU 信息
	logicCores, err := cpu.Info()
	if err != nil {
		return nil, err
	}
	physicalCores := logicCores[0].Cores
	cpuUsages, err := cpu.Percent(time.Second, true)
	if err != nil {
		return nil, err
	}
	// Memory 信息
	memInfo, err := mem.VirtualMemory()
	if err != nil {
		return nil, err
	}
	memUsage := memInfo.UsedPercent
	memTotal := memInfo.Total
	// Disk 信息
	usage, err := disk.Usage("/")
	if err != nil {
		return nil, err
	}
	readBytesPer5Sec, writeBytesPer5Sec, err := calculateDiskIOPerInterval()
	if err != nil {
		return nil, err
	}
	// Disk 列表信息
	diskInfo := &worker_2_controller_service.DiskInfo{
		DiskUsagePercent: usage.UsedPercent,
		DiskTotal:        usage.Total,
		DiskReadBytes:    readBytesPer5Sec,
		DiskWriteBytes:   writeBytesPer5Sec,
	}
	// Network 信息
	// todo: 网络信息待补充

	return &worker_2_controller_service.Status{
		Cpu: &worker_2_controller_service.CpuInfo{
			CpuLogicalCores:       int32(len(logicCores)),
			CpuUsagePercents:      cpuUsages,
			SuperThreadingEnabled: len(logicCores) > int(physicalCores),
		},
		Memory: &worker_2_controller_service.MemoryInfo{
			MemoryUsagePercent: memUsage,
			MemoryTotal:        memTotal,
		},
		Disk: diskInfo,
		// todo: 网络信息待补充
		Network: &worker_2_controller_service.NetworkInfo{},
		// todo: 任务数待补充
		TaskCount: 0,
	}, nil
}

func isExternalInterface(iface net.InterfaceStat) bool {
	// 必须有 MAC
	if iface.HardwareAddr == "" {
		return false
	}

	// 名称过滤（虚拟接口）
	name := iface.Name
	virtualPrefixes := []string{
		"lo", "docker", "veth", "br-", "cni", "flannel", "tun",
	}

	for _, p := range virtualPrefixes {
		if strings.HasPrefix(name, p) {
			return false
		}
	}

	// 必须有 IP 地址
	for _, addr := range iface.Addrs {
		if strings.Contains(addr.Addr, ".") || strings.Contains(addr.Addr, ":") {
			return true
		}
	}

	return false
}

// func getExternalInterfaces() ([]net.InterfaceStat, error) {
// 	ifaces, err := net.Interfaces()
// 	if err != nil {
// 		return nil, err
// 	}

// }

func calculateDiskIOPerInterval() (int32, int32, error) {
	readBytesPer5Sec := uint64(0)
	writeBytesPer5Sec := uint64(0)
	if diskCapacityRefreshTimeLeft <= 0 {
		diskCapacityRefreshTimeLeft = parameter.DefaultDiskCapacityRefreshTimeLeft
		diskStat, err := disk.IOCounters()
		if err != nil {
			return 0, 0, err
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
	return int32(readBytesPer5Sec) / int32(parameter.DefaultIntervalSeconds),
		int32(writeBytesPer5Sec) / int32(parameter.DefaultIntervalSeconds),
		nil
}
