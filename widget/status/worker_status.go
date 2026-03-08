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
	preNetworkStat              map[string]net.IOCountersStat // key = interface name
	diskCapacityRefreshTimeLeft = 0                           // 每隔 10 分钟刷新一次磁盘容量数据
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
	netIfaceName, sentBytesPerInterval, recvBytesPerInterval := calculateNetworkIOPerInterval()

	networkInfo := &worker_2_controller_service.NetworkInfo{
		InterfaceName:        netIfaceName,
		NetworkSentBytes:     sentBytesPerInterval,
		NetworkReceivedBytes: recvBytesPerInterval,
	}

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
		Disk:    diskInfo,
		Network: networkInfo,
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

// getExternalInterfaceNames 返回所有物理（非虚拟/回环）网络接口名称列表
func getExternalInterfaceNames() []string {
	ifaces, err := net.Interfaces()
	if err != nil {
		return nil
	}
	var names []string
	for _, iface := range ifaces {
		if isExternalInterface(iface) {
			names = append(names, iface.Name)
		}
	}
	return names
}

// calculateNetworkIOPerInterval 计算每个心跳周期内的网络收发字节数
//
// 返回: (接口名, 每周期发送字节数, 每周期接收字节数)
// 多个物理接口的流量会累加到一起，接口名取第一个物理接口的名称。
func calculateNetworkIOPerInterval() (string, int32, int32) {
	// 获取所有网络接口的 IO 计数器
	counters, err := net.IOCounters(true) // pernic=true，按接口分别返回
	if err != nil {
		return "", 0, 0
	}

	// 筛选物理接口名称
	externalNames := getExternalInterfaceNames()
	externalSet := make(map[string]bool, len(externalNames))
	for _, n := range externalNames {
		externalSet[n] = true
	}

	// 构建当前快照：只保留物理接口
	currentStats := make(map[string]net.IOCountersStat, len(counters))
	for _, c := range counters {
		if externalSet[c.Name] {
			currentStats[c.Name] = c
		}
	}

	// 如果没有找到物理接口，回退到所有非 lo 接口
	if len(currentStats) == 0 {
		for _, c := range counters {
			if !strings.HasPrefix(c.Name, "lo") {
				currentStats[c.Name] = c
			}
		}
	}

	var totalSentDelta, totalRecvDelta uint64
	primaryIface := ""

	if preNetworkStat != nil {
		for name, cur := range currentStats {
			if primaryIface == "" {
				primaryIface = name
			}
			if prev, ok := preNetworkStat[name]; ok {
				// 计算差值（处理计数器回绕：如果当前值小于上次，说明回绕了，跳过）
				if cur.BytesSent >= prev.BytesSent {
					totalSentDelta += cur.BytesSent - prev.BytesSent
				}
				if cur.BytesRecv >= prev.BytesRecv {
					totalRecvDelta += cur.BytesRecv - prev.BytesRecv
				}
			}
		}
	} else {
		// 第一次采集，还没有上一次的数据，只记录快照
		for name := range currentStats {
			if primaryIface == "" {
				primaryIface = name
			}
		}
	}

	// 保存当前快照供下次计算差值
	preNetworkStat = currentStats

	// 转换为每秒速率（除以心跳间隔秒数），然后返回整个周期的字节数
	intervalSec := int64(parameter.DefaultIntervalSeconds)
	if intervalSec <= 0 {
		intervalSec = 5
	}

	sentPerSec := int32(totalSentDelta / uint64(intervalSec))
	recvPerSec := int32(totalRecvDelta / uint64(intervalSec))

	return primaryIface, sentPerSec, recvPerSec
}

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
