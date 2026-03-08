package profile

import (
	"math"
	"testing"

	worker "github.com/kregonia/brander_mixer/script/rpc_server/worker"
)

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func makeWorkerInfo(hostname string, logicalCores, physicalCores int32, memTotal uint64) *worker.WorkerInfo {
	return &worker.WorkerInfo{
		Hostname:         hostname,
		Os:               "linux",
		Arch:             "amd64",
		CpuLogicalCores:  logicalCores,
		CpuPhysicalCores: physicalCores,
		CpuModelName:     "test-cpu",
		MemoryTotal:      memTotal,
	}
}

func makeStatus(cpuUsagePerCore float64, cores int, memUsagePct float64, memTotal uint64, diskUsagePct float64, diskTotal uint64, netSent, netRecv int32, taskCount int32) *worker.Status {
	cpuPercents := make([]float64, cores)
	for i := range cpuPercents {
		cpuPercents[i] = cpuUsagePerCore
	}
	return &worker.Status{
		Cpu: &worker.CpuInfo{
			CpuLogicalCores:  int32(cores),
			CpuUsagePercents: cpuPercents,
		},
		Memory: &worker.MemoryInfo{
			MemoryUsagePercent: memUsagePct,
			MemoryTotal:        memTotal,
		},
		Disk: &worker.DiskInfo{
			DiskUsagePercent: diskUsagePct,
			DiskTotal:        diskTotal,
		},
		Network: &worker.NetworkInfo{
			NetworkSentBytes:     netSent,
			NetworkReceivedBytes: netRecv,
		},
		TaskCount: taskCount,
	}
}

func floatNear(a, b, epsilon float64) bool {
	return math.Abs(a-b) < epsilon
}

// ---------------------------------------------------------------------------
// Test: BuildProfile with nil info and nil status
// ---------------------------------------------------------------------------

func TestBuildProfile_NilInfoNilStatus(t *testing.T) {
	p := BuildProfile("w1", nil, nil, 0.95)
	if p == nil {
		t.Fatal("BuildProfile returned nil")
	}
	if p.WorkerID != "w1" {
		t.Errorf("expected WorkerID 'w1', got %q", p.WorkerID)
	}
	if p.Reliability != 0.95 {
		t.Errorf("expected Reliability 0.95, got %.4f", p.Reliability)
	}
	// With nil info, host fields should be zero-value
	if p.Hostname != "" {
		t.Errorf("expected empty Hostname, got %q", p.Hostname)
	}
	if p.CPULogicalCores != 0 {
		t.Errorf("expected 0 CPULogicalCores, got %d", p.CPULogicalCores)
	}
	// With nil status, available ratios should be 0
	if p.CPUAvailableRatio != 0 {
		t.Errorf("expected CPUAvailableRatio 0, got %.4f", p.CPUAvailableRatio)
	}
	if p.MemoryAvailableRatio != 0 {
		t.Errorf("expected MemoryAvailableRatio 0, got %.4f", p.MemoryAvailableRatio)
	}
	if p.DiskAvailableRatio != 0 {
		t.Errorf("expected DiskAvailableRatio 0, got %.4f", p.DiskAvailableRatio)
	}
}

// ---------------------------------------------------------------------------
// Test: BuildProfile with valid info and nil status
// ---------------------------------------------------------------------------

func TestBuildProfile_ValidInfoNilStatus(t *testing.T) {
	info := makeWorkerInfo("host-a", 16, 8, 32*1024*1024*1024)
	p := BuildProfile("w1", info, nil, 1.0)

	if p.Hostname != "host-a" {
		t.Errorf("expected Hostname 'host-a', got %q", p.Hostname)
	}
	if p.OS != "linux" {
		t.Errorf("expected OS 'linux', got %q", p.OS)
	}
	if p.Arch != "amd64" {
		t.Errorf("expected Arch 'amd64', got %q", p.Arch)
	}
	if p.CPULogicalCores != 16 {
		t.Errorf("expected 16 logical cores, got %d", p.CPULogicalCores)
	}
	if p.CPUPhysicalCores != 8 {
		t.Errorf("expected 8 physical cores, got %d", p.CPUPhysicalCores)
	}
	if p.CPUModelName != "test-cpu" {
		t.Errorf("expected CPUModelName 'test-cpu', got %q", p.CPUModelName)
	}
	// Status is nil, so dynamic fields should be 0
	if p.CPUAvailableRatio != 0 {
		t.Errorf("expected CPUAvailableRatio 0, got %.4f", p.CPUAvailableRatio)
	}
}

// ---------------------------------------------------------------------------
// Test: BuildProfile with valid info and status
// ---------------------------------------------------------------------------

func TestBuildProfile_FullData(t *testing.T) {
	info := makeWorkerInfo("host-b", 8, 4, 16*1024*1024*1024)
	status := makeStatus(
		30.0,               // CPU usage 30% per core
		8,                  // 8 cores
		40.0,               // memory usage 40%
		16*1024*1024*1024,  // 16 GB total
		50.0,               // disk usage 50%
		500*1024*1024*1024, // 500 GB total
		1000,               // 1000 bytes sent
		2000,               // 2000 bytes recv
		3,                  // 3 running tasks
	)
	p := BuildProfile("w2", info, status, 0.85)

	// CPU: avg usage = 30%, available = 70%
	if !floatNear(p.CPUAvailableRatio, 0.7, 0.01) {
		t.Errorf("expected CPUAvailableRatio ~0.70, got %.4f", p.CPUAvailableRatio)
	}

	// Memory: usage 40%, available = 60%
	if !floatNear(p.MemoryAvailableRatio, 0.6, 0.01) {
		t.Errorf("expected MemoryAvailableRatio ~0.60, got %.4f", p.MemoryAvailableRatio)
	}

	// Disk: usage 50%, available = 50%
	if !floatNear(p.DiskAvailableRatio, 0.5, 0.01) {
		t.Errorf("expected DiskAvailableRatio ~0.50, got %.4f", p.DiskAvailableRatio)
	}

	// Memory total
	if p.MemoryTotalBytes != 16*1024*1024*1024 {
		t.Errorf("expected MemoryTotalBytes 16 GB, got %d", p.MemoryTotalBytes)
	}

	// Disk total
	if p.DiskTotalBytes != 500*1024*1024*1024 {
		t.Errorf("expected DiskTotalBytes 500 GB, got %d", p.DiskTotalBytes)
	}

	// Network load
	if p.NetworkLoadBytes != 3000 {
		t.Errorf("expected NetworkLoadBytes 3000, got %d", p.NetworkLoadBytes)
	}

	// Task count
	if p.TaskCount != 3 {
		t.Errorf("expected TaskCount 3, got %d", p.TaskCount)
	}

	// Reliability
	if p.Reliability != 0.85 {
		t.Errorf("expected Reliability 0.85, got %.4f", p.Reliability)
	}
}

// ---------------------------------------------------------------------------
// Test: BuildProfile CPU cores fallback from status when info has 0
// ---------------------------------------------------------------------------

func TestBuildProfile_CPUCoresFallback(t *testing.T) {
	info := &worker.WorkerInfo{
		Hostname:        "fallback-host",
		CpuLogicalCores: 0, // not provided at registration
	}
	status := makeStatus(20.0, 4, 30.0, 8<<30, 40.0, 200<<30, 0, 0, 0)

	p := BuildProfile("w-fb", info, status, 1.0)

	// Should fall back to status.Cpu.CpuLogicalCores
	if p.CPULogicalCores != 4 {
		t.Errorf("expected CPULogicalCores fallback to 4, got %d", p.CPULogicalCores)
	}
}

// ---------------------------------------------------------------------------
// Test: CPUCapacity
// ---------------------------------------------------------------------------

func TestCPUCapacity(t *testing.T) {
	p := &NodeProfile{
		CPULogicalCores:   16,
		CPUAvailableRatio: 0.75, // 75% available
	}
	expected := 16.0 * 0.75 // 12.0
	got := p.CPUCapacity()
	if !floatNear(got, expected, 0.001) {
		t.Errorf("expected CPUCapacity %.2f, got %.4f", expected, got)
	}
}

func TestCPUCapacity_Zero(t *testing.T) {
	p := &NodeProfile{
		CPULogicalCores:   0,
		CPUAvailableRatio: 0.5,
	}
	if p.CPUCapacity() != 0 {
		t.Errorf("expected CPUCapacity 0, got %.4f", p.CPUCapacity())
	}
}

func TestCPUCapacity_FullLoad(t *testing.T) {
	p := &NodeProfile{
		CPULogicalCores:   8,
		CPUAvailableRatio: 0.0, // fully loaded
	}
	if p.CPUCapacity() != 0 {
		t.Errorf("expected CPUCapacity 0 when fully loaded, got %.4f", p.CPUCapacity())
	}
}

// ---------------------------------------------------------------------------
// Test: MemoryCapacityBytes
// ---------------------------------------------------------------------------

func TestMemoryCapacityBytes(t *testing.T) {
	p := &NodeProfile{
		MemoryTotalBytes:     32 * 1024 * 1024 * 1024, // 32 GB
		MemoryAvailableRatio: 0.6,                     // 60% available
	}
	expected := 32.0 * 1024 * 1024 * 1024 * 0.6
	got := p.MemoryCapacityBytes()
	if !floatNear(got, expected, 1024) {
		t.Errorf("expected MemoryCapacityBytes ~%.0f, got %.0f", expected, got)
	}
}

// ---------------------------------------------------------------------------
// Test: DiskCapacityBytes
// ---------------------------------------------------------------------------

func TestDiskCapacityBytes(t *testing.T) {
	p := &NodeProfile{
		DiskTotalBytes:     1024 * 1024 * 1024 * 1024, // 1 TB
		DiskAvailableRatio: 0.8,                       // 80% available
	}
	expected := 1024.0 * 1024 * 1024 * 1024 * 0.8
	got := p.DiskCapacityBytes()
	if !floatNear(got, expected, 1024*1024) {
		t.Errorf("expected DiskCapacityBytes ~%.0f, got %.0f", expected, got)
	}
}

// ---------------------------------------------------------------------------
// Test: CapabilityVector basic normalization
// ---------------------------------------------------------------------------

func TestCapabilityVector_Basic(t *testing.T) {
	p := &NodeProfile{
		CPULogicalCores:      8,
		CPUAvailableRatio:    0.5,                       // 4 effective cores
		MemoryTotalBytes:     16 * 1024 * 1024 * 1024,   // 16 GB
		MemoryAvailableRatio: 0.5,                       // 8 GB available
		DiskTotalBytes:       1024 * 1024 * 1024 * 1024, // 1 TB
		DiskAvailableRatio:   0.5,                       // 500 GB available
		NetworkLoadBytes:     0,                         // no network load
		Reliability:          0.9,
	}

	vec := p.CapabilityVector()

	// CPU: 8 * 0.5 / 128 = 4/128 = 0.03125
	expectedCPU := 4.0 / 128.0
	if !floatNear(vec[0], expectedCPU, 0.001) {
		t.Errorf("cpu norm: expected ~%.4f, got %.4f", expectedCPU, vec[0])
	}

	// Memory: 16GB * 0.5 / 256GB = 8/256 = 0.03125
	expectedMem := (16.0 * 1024 * 1024 * 1024 * 0.5) / (256.0 * 1024 * 1024 * 1024)
	if !floatNear(vec[1], expectedMem, 0.001) {
		t.Errorf("mem norm: expected ~%.4f, got %.4f", expectedMem, vec[1])
	}

	// Disk: 1TB * 0.5 / 4TB = 0.125
	expectedDisk := (1024.0 * 1024 * 1024 * 1024 * 0.5) / (4.0 * 1024 * 1024 * 1024 * 1024)
	if !floatNear(vec[2], expectedDisk, 0.001) {
		t.Errorf("disk norm: expected ~%.4f, got %.4f", expectedDisk, vec[2])
	}

	// Network idle: 1.0 - 0/(1GB) = 1.0
	if !floatNear(vec[3], 1.0, 0.001) {
		t.Errorf("net idle norm: expected ~1.0, got %.4f", vec[3])
	}

	// Reliability: direct passthrough
	if !floatNear(vec[4], 0.9, 0.001) {
		t.Errorf("reliability: expected 0.9, got %.4f", vec[4])
	}
}

// ---------------------------------------------------------------------------
// Test: CapabilityVector all zeros
// ---------------------------------------------------------------------------

func TestCapabilityVector_AllZeros(t *testing.T) {
	p := &NodeProfile{
		CPULogicalCores:      0,
		CPUAvailableRatio:    0,
		MemoryTotalBytes:     0,
		MemoryAvailableRatio: 0,
		DiskTotalBytes:       0,
		DiskAvailableRatio:   0,
		NetworkLoadBytes:     0,
		Reliability:          0,
	}

	vec := p.CapabilityVector()

	if vec[0] != 0 || vec[1] != 0 || vec[2] != 0 || vec[4] != 0 {
		t.Errorf("expected [0,0,0,_,0] for zero profile, got %v", vec)
	}
	// Network idle with 0 load should be 1.0
	if !floatNear(vec[3], 1.0, 0.001) {
		t.Errorf("net idle with 0 load should be 1.0, got %.4f", vec[3])
	}
}

// ---------------------------------------------------------------------------
// Test: CapabilityVector clamps to [0, 1]
// ---------------------------------------------------------------------------

func TestCapabilityVector_Clamping(t *testing.T) {
	p := &NodeProfile{
		CPULogicalCores:      256, // way above 128 norm → should clamp
		CPUAvailableRatio:    1.0,
		MemoryTotalBytes:     512 * 1024 * 1024 * 1024, // 512 GB > 256 GB norm
		MemoryAvailableRatio: 1.0,
		DiskTotalBytes:       8 * 1024 * 1024 * 1024 * 1024, // 8 TB > 4 TB norm
		DiskAvailableRatio:   1.0,
		NetworkLoadBytes:     0,
		Reliability:          1.0,
	}

	vec := p.CapabilityVector()

	for i, v := range vec {
		if v < 0 || v > 1 {
			t.Errorf("vec[%d] = %.4f is out of [0, 1] range", i, v)
		}
	}
	// CPU: 256 * 1.0 / 128 = 2.0 → clamped to 1.0
	if vec[0] != 1.0 {
		t.Errorf("cpu norm should clamp to 1.0, got %.4f", vec[0])
	}
	// Memory: 512GB / 256GB = 2.0 → clamped to 1.0
	if vec[1] != 1.0 {
		t.Errorf("mem norm should clamp to 1.0, got %.4f", vec[1])
	}
	// Disk: 8TB / 4TB = 2.0 → clamped to 1.0
	if vec[2] != 1.0 {
		t.Errorf("disk norm should clamp to 1.0, got %.4f", vec[2])
	}
}

// ---------------------------------------------------------------------------
// Test: CapabilityVector high network load → low idle score
// ---------------------------------------------------------------------------

func TestCapabilityVector_HighNetworkLoad(t *testing.T) {
	p := &NodeProfile{
		CPULogicalCores:      8,
		CPUAvailableRatio:    0.5,
		MemoryTotalBytes:     16 * 1024 * 1024 * 1024,
		MemoryAvailableRatio: 0.5,
		DiskTotalBytes:       512 * 1024 * 1024 * 1024,
		DiskAvailableRatio:   0.5,
		NetworkLoadBytes:     512 * 1024 * 1024, // 512 MB/interval → 50% of 1 GB/s
		Reliability:          1.0,
	}

	vec := p.CapabilityVector()

	// net idle = 1.0 - 512MB / 1GB = 0.5
	if !floatNear(vec[3], 0.5, 0.01) {
		t.Errorf("expected net idle ~0.5, got %.4f", vec[3])
	}
}

func TestCapabilityVector_MaxNetworkLoad(t *testing.T) {
	p := &NodeProfile{
		NetworkLoadBytes: 2 * 1024 * 1024 * 1024, // 2 GB → exceeds 1 GB norm
	}

	vec := p.CapabilityVector()

	// net idle = 1.0 - 2.0 = -1.0 → clamped to 0.0
	if vec[3] != 0.0 {
		t.Errorf("expected net idle 0.0 when load exceeds norm, got %.4f", vec[3])
	}
}

// ---------------------------------------------------------------------------
// Test: BuildProfile end-to-end + CapabilityVector
// ---------------------------------------------------------------------------

func TestBuildProfile_CapabilityVector_Integration(t *testing.T) {
	info := makeWorkerInfo("int-host", 16, 8, 32*1024*1024*1024)
	status := makeStatus(
		25.0,                  // CPU 25% usage per core
		16,                    // 16 cores
		50.0,                  // memory 50% used
		32*1024*1024*1024,     // 32 GB
		30.0,                  // disk 30% used
		2*1024*1024*1024*1024, // 2 TB
		100*1024*1024,         // 100 MB sent
		50*1024*1024,          // 50 MB recv
		2,
	)

	p := BuildProfile("w-int", info, status, 0.95)

	vec := p.CapabilityVector()

	// All values should be in [0, 1]
	for i, v := range vec {
		if v < 0 || v > 1 {
			t.Errorf("vec[%d] = %.4f out of [0, 1]", i, v)
		}
	}

	// CPU: 16 cores * 0.75 available / 128 = 12/128 = 0.09375
	expectedCPU := 16.0 * 0.75 / 128.0
	if !floatNear(vec[0], expectedCPU, 0.01) {
		t.Errorf("cpu norm: expected ~%.4f, got %.4f", expectedCPU, vec[0])
	}

	// Memory: 32GB * 0.5 / 256GB = 0.0625
	expectedMem := 32.0 * 0.5 / 256.0
	if !floatNear(vec[1], expectedMem, 0.01) {
		t.Errorf("mem norm: expected ~%.4f, got %.4f", expectedMem, vec[1])
	}

	// Disk: 2TB * 0.7 / 4TB = 0.35
	expectedDisk := 2.0 * 0.7 / 4.0
	if !floatNear(vec[2], expectedDisk, 0.01) {
		t.Errorf("disk norm: expected ~%.4f, got %.4f", expectedDisk, vec[2])
	}

	// Network idle: 1 - 150MB / 1GB = 1 - 0.1465 ≈ 0.8535
	netLoadGB := float64(150*1024*1024) / float64(1024*1024*1024)
	expectedNet := 1.0 - netLoadGB
	if !floatNear(vec[3], expectedNet, 0.01) {
		t.Errorf("net idle: expected ~%.4f, got %.4f", expectedNet, vec[3])
	}

	// Reliability: 0.95
	if !floatNear(vec[4], 0.95, 0.001) {
		t.Errorf("reliability: expected 0.95, got %.4f", vec[4])
	}
}

// ---------------------------------------------------------------------------
// Test: clamp01 helper
// ---------------------------------------------------------------------------

func TestClamp01(t *testing.T) {
	tests := []struct {
		input    float64
		expected float64
	}{
		{-1.0, 0.0},
		{-0.001, 0.0},
		{0.0, 0.0},
		{0.5, 0.5},
		{1.0, 1.0},
		{1.001, 1.0},
		{99.0, 1.0},
	}
	for _, tt := range tests {
		got := clamp01(tt.input)
		if got != tt.expected {
			t.Errorf("clamp01(%f) = %f, want %f", tt.input, got, tt.expected)
		}
	}
}

// ---------------------------------------------------------------------------
// Test: averageFloat64 helper
// ---------------------------------------------------------------------------

func TestAverageFloat64(t *testing.T) {
	tests := []struct {
		name     string
		input    []float64
		expected float64
	}{
		{"empty", []float64{}, 0},
		{"single", []float64{42.0}, 42.0},
		{"multiple", []float64{10, 20, 30}, 20.0},
		{"uniform", []float64{5, 5, 5, 5}, 5.0},
		{"mixed", []float64{0, 100}, 50.0},
	}
	for _, tt := range tests {
		got := averageFloat64(tt.input)
		if !floatNear(got, tt.expected, 0.001) {
			t.Errorf("averageFloat64 %s: expected %.4f, got %.4f", tt.name, tt.expected, got)
		}
	}
}

// ---------------------------------------------------------------------------
// Test: GPUScore defaults to 0
// ---------------------------------------------------------------------------

func TestGPUScore_Default(t *testing.T) {
	p := BuildProfile("w1", nil, nil, 1.0)
	if p.GPUScore != 0 {
		t.Errorf("expected GPUScore 0 by default, got %.4f", p.GPUScore)
	}
}

// ---------------------------------------------------------------------------
// Test: Status with partial nil sub-messages
// ---------------------------------------------------------------------------

func TestBuildProfile_PartialStatus(t *testing.T) {
	info := makeWorkerInfo("partial-host", 4, 2, 8<<30)

	// Status with only CPU info, everything else nil
	status := &worker.Status{
		Cpu: &worker.CpuInfo{
			CpuLogicalCores:  4,
			CpuUsagePercents: []float64{20.0, 40.0, 60.0, 80.0}, // avg = 50%
		},
		// Memory, Disk, Network are nil
		TaskCount: 1,
	}

	p := BuildProfile("w-partial", info, status, 0.8)

	// CPU available should be 1 - 0.5 = 0.5
	if !floatNear(p.CPUAvailableRatio, 0.5, 0.01) {
		t.Errorf("expected CPUAvailableRatio ~0.50, got %.4f", p.CPUAvailableRatio)
	}

	// Memory should be 0 (nil sub-message)
	if p.MemoryAvailableRatio != 0 {
		t.Errorf("expected MemoryAvailableRatio 0 with nil memory, got %.4f", p.MemoryAvailableRatio)
	}

	// Disk should be 0
	if p.DiskAvailableRatio != 0 {
		t.Errorf("expected DiskAvailableRatio 0 with nil disk, got %.4f", p.DiskAvailableRatio)
	}

	// Network load should be 0
	if p.NetworkLoadBytes != 0 {
		t.Errorf("expected NetworkLoadBytes 0 with nil network, got %d", p.NetworkLoadBytes)
	}

	// Task count should still be read
	if p.TaskCount != 1 {
		t.Errorf("expected TaskCount 1, got %d", p.TaskCount)
	}
}

// ---------------------------------------------------------------------------
// Test: Status with empty CPU usage percents
// ---------------------------------------------------------------------------

func TestBuildProfile_EmptyCPUPercents(t *testing.T) {
	info := makeWorkerInfo("empty-cpu", 8, 4, 16<<30)
	status := &worker.Status{
		Cpu: &worker.CpuInfo{
			CpuLogicalCores:  8,
			CpuUsagePercents: []float64{}, // no per-core data
		},
	}

	p := BuildProfile("w-nocpu", info, status, 1.0)

	// averageFloat64 of empty slice returns 0, so available = 1 - 0 = 1.0
	if !floatNear(p.CPUAvailableRatio, 1.0, 0.001) {
		t.Errorf("expected CPUAvailableRatio 1.0 with empty percents, got %.4f", p.CPUAvailableRatio)
	}
}
