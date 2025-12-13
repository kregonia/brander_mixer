package status

import (
	"bufio"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"

	worker_2_controller_service "github.com/kregonia/brander_mixer/script/rpc_server/worker"
)

// GetWorkerStatus 收集当前机器的基础运行信息（尽量在 Linux 上工作）
func GetWorkerStatus() worker_2_controller_service.Status {
	cores := runtime.NumCPU()

	// CPU 频率（MHz）: 优先尝试 sysfs，再尝试 /proc/cpuinfo
	freq := readCPUFreq()

	// 近似 CPU 使用率（百分比）: 读取 /proc/stat 两次取差
	cpuUsage := readCPUUsage()

	// 内存: 从 /proc/meminfo 读取（返回 bytes）
	memTotal, memAvail := readMemInfo()
	memUsage := float32(0.0)
	if memTotal > 0 {
		used := memTotal - memAvail
		memUsage = used / memTotal * 100.0
	}

	// 任务数：使用当前 goroutine 数作为近似值
	taskNum := runtime.NumGoroutine()

	return worker_2_controller_service.Status{
		CpuUsage:     cpuUsage,
		CpuCores:     int32(cores),
		CpuFrequency: freq,
		MemoryUsage:  memUsage,
		MemoryTotal:  memTotal,
		TaskCount:    int32(taskNum),
	}
}

// readCPUFreq 尝试读取 CPU 频率（MHz），若失败返回 0
func readCPUFreq() float32 {
	// 尝试 sysfs: cpuinfo_max_freq (单位 kHz)
	p := "/sys/devices/system/cpu/cpu0/cpufreq/cpuinfo_max_freq"
	if b, err := os.ReadFile(p); err == nil {
		s := strings.TrimSpace(string(b))
		if v, err := strconv.ParseFloat(s, 32); err == nil && v > 0 {
			return float32(v) / 1000.0 / 1000.0 // kHz -> MHz
		}
	}

	// 回退到 /proc/cpuinfo 查找 "cpu MHz"
	if b, err := os.ReadFile("/proc/cpuinfo"); err == nil {
		lines := strings.SplitSeq(string(b), "\n")
		for ln := range lines {
			if strings.HasPrefix(strings.ToLower(strings.TrimSpace(ln)), "cpu mhz") {
				parts := strings.Split(ln, ":")
				if len(parts) >= 2 {
					if v, err := strconv.ParseFloat(strings.TrimSpace(parts[1]), 32); err == nil {
						return float32(v)
					}
				}
			}
		}
	}
	return 0
}

// readCPUUsage 通过两次读取 /proc/stat 计算短时 CPU 使用率（百分比）
func readCPUUsage() float32 {
	idle0, total0, err := readCPUSTat()
	if err != nil {
		return 0
	}
	time.Sleep(120 * time.Millisecond)
	idle1, total1, err := readCPUSTat()
	if err != nil || total1 <= total0 {
		return 0
	}
	idleTicks := float32(idle1 - idle0)
	totalTicks := float32(total1 - total0)
	if totalTicks == 0 {
		return 0
	}
	usage := (1.0 - idleTicks/totalTicks) * 100.0
	if usage < 0 {
		usage = 0
	}
	if usage > 100 {
		usage = 100
	}
	return usage
}

// readCPUSTat 解析 /proc/stat 的第一行 cpu 字段，返回 idle 与 total（ticks）
func readCPUSTat() (idle, total uint64, err error) {
	f, e := os.Open("/proc/stat")
	if e != nil {
		return 0, 0, e
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	if scanner.Scan() {
		line := scanner.Text()
		fields := strings.Fields(line)
		if len(fields) < 5 {
			return 0, 0, nil
		}
		// fields[0] == "cpu"
		var vals []uint64
		for _, s := range fields[1:] {
			v, err := strconv.ParseUint(s, 10, 64)
			if err != nil {
				vals = append(vals, 0)
			} else {
				vals = append(vals, v)
			}
		}
		var tot uint64
		for _, v := range vals {
			tot += v
		}
		// idle is at position 4 (index 3) in vals for user,nice,system,idle,...
		var id uint64
		if len(vals) >= 4 {
			id = vals[3]
		}
		return id, tot, nil
	}
	if err := scanner.Err(); err != nil {
		return 0, 0, err
	}
	return 0, 0, nil
}

// readMemInfo 读取 /proc/meminfo，返回 (totalBytes, availableBytes)
func readMemInfo() (float32, float32) {
	b, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return 0, 0
	}
	var totalKB, availKB float32
	lines := strings.Split(string(b), "\n")
	for _, ln := range lines {
		if strings.HasPrefix(ln, "MemTotal:") {
			parts := strings.Fields(ln)
			if len(parts) >= 2 {
				if v, err := strconv.ParseFloat(parts[1], 32); err == nil {
					totalKB = float32(v)
				}
			}
		} else if strings.HasPrefix(ln, "MemAvailable:") {
			parts := strings.Fields(ln)
			if len(parts) >= 2 {
				if v, err := strconv.ParseFloat(parts[1], 32); err == nil {
					availKB = float32(v)
				}
			}
		}
	}
	return totalKB / 1024, availKB / 1024
}
