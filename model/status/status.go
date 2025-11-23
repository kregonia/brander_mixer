package status

import "fmt"

type WorkerStatus struct {
	CPUUsage float64 `json:"cpu_usage"`
	CPUCores int32   `json:"cpu_cores"`
	CPUFreq  float64 `json:"cpu_freq"`
	MemUsage float64 `json:"mem_usage"`
	MemTotal float64 `json:"mem_total"`
	TaskNum  int32   `json:"task_num"`
}

func (w WorkerStatus) String() string {
	return fmt.Sprintf("CPU Usage: %.2f%%\nCPU Cores: %d\nCPU Freq: %.2f GHz\nMem Usage: %.2f%%\nMem Total: %.2f MB\nTask Num: %d",
		w.CPUUsage, w.CPUCores, w.CPUFreq, w.MemUsage, w.MemTotal, w.TaskNum)
}
