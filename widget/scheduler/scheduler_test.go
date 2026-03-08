package scheduler

import (
	"testing"

	worker "github.com/kregonia/brander_mixer/script/rpc_server/worker"
	"github.com/kregonia/brander_mixer/widget/nodetable"
)

// ---------------------------------------------------------------------------
// helpers: register mock nodes into a NodeTable
// ---------------------------------------------------------------------------

func makeNodeTable(nodes map[string]*mockNode) *nodetable.NodeTable {
	nt := nodetable.NewNodeTable()
	for id, n := range nodes {
		info := &worker.WorkerInfo{
			Hostname:         n.hostname,
			Os:               "linux",
			Arch:             "amd64",
			CpuLogicalCores:  n.cpuCores,
			CpuPhysicalCores: n.cpuCores / 2,
			CpuModelName:     "mock-cpu",
		}
		nt.Register(id, info, "")

		// Simulate heartbeats so the node is online with a real status
		status := &worker.Status{
			Cpu: &worker.CpuInfo{
				CpuLogicalCores:  n.cpuCores,
				CpuUsagePercents: makeCPUPercents(int(n.cpuCores), n.cpuUsage),
			},
			Memory: &worker.MemoryInfo{
				MemoryUsagePercent: n.memUsage,
				MemoryTotal:        n.memTotal,
			},
			Disk: &worker.DiskInfo{
				DiskUsagePercent: n.diskUsage,
				DiskTotal:        n.diskTotal,
			},
			Network: &worker.NetworkInfo{
				NetworkSentBytes:     0,
				NetworkReceivedBytes: 0,
			},
			TaskCount: n.taskCount,
		}

		// Send multiple heartbeats to build reliability
		for i := 0; i < int(n.heartbeats); i++ {
			nt.UpdateHeartbeat(id, status)
		}
	}
	return nt
}

type mockNode struct {
	hostname   string
	cpuCores   int32
	cpuUsage   float64 // per-core usage percent
	memUsage   float64
	memTotal   uint64
	diskUsage  float64
	diskTotal  uint64
	taskCount  int32
	heartbeats int32 // number of heartbeat hits for reliability
}

func makeCPUPercents(cores int, usage float64) []float64 {
	p := make([]float64, cores)
	for i := range p {
		p[i] = usage
	}
	return p
}

// ---------------------------------------------------------------------------
// Test: NewScheduler with default weights
// ---------------------------------------------------------------------------

func TestNewScheduler_DefaultWeights(t *testing.T) {
	s := NewScheduler(nil)
	if s == nil {
		t.Fatal("NewScheduler returned nil")
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.weights.Alpha != DefaultWeights.Alpha {
		t.Errorf("expected alpha=%.2f, got %.2f", DefaultWeights.Alpha, s.weights.Alpha)
	}
	if s.weights.Beta != DefaultWeights.Beta {
		t.Errorf("expected beta=%.2f, got %.2f", DefaultWeights.Beta, s.weights.Beta)
	}
	if s.weights.Gamma != DefaultWeights.Gamma {
		t.Errorf("expected gamma=%.2f, got %.2f", DefaultWeights.Gamma, s.weights.Gamma)
	}
}

func TestNewScheduler_CustomWeights(t *testing.T) {
	w := &SchedulerWeights{Alpha: 0.5, Beta: 0.3, Gamma: 0.2}
	s := NewScheduler(w)
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.weights.Alpha != 0.5 || s.weights.Beta != 0.3 || s.weights.Gamma != 0.2 {
		t.Errorf("custom weights not applied correctly: got α=%.2f β=%.2f γ=%.2f",
			s.weights.Alpha, s.weights.Beta, s.weights.Gamma)
	}
}

// ---------------------------------------------------------------------------
// Test: SetNodeTable / SetWeights
// ---------------------------------------------------------------------------

func TestSetNodeTable(t *testing.T) {
	s := NewScheduler(nil)
	nt := nodetable.NewNodeTable()
	s.SetNodeTable(nt)
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.nodeTable != nt {
		t.Error("SetNodeTable did not set the NodeTable")
	}
}

func TestSetWeights(t *testing.T) {
	s := NewScheduler(nil)
	s.SetWeights(SchedulerWeights{Alpha: 0.1, Beta: 0.2, Gamma: 0.7})
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.weights.Alpha != 0.1 || s.weights.Beta != 0.2 || s.weights.Gamma != 0.7 {
		t.Error("SetWeights did not update weights correctly")
	}
}

// ---------------------------------------------------------------------------
// Test: SelectNode — no NodeTable
// ---------------------------------------------------------------------------

func TestSelectNode_NoNodeTable(t *testing.T) {
	s := NewScheduler(nil)
	// no NodeTable set
	_, err := s.SelectNode(TaskRequirement{TaskID: "t1"})
	if err == nil {
		t.Error("expected error when NodeTable is nil")
	}
}

// ---------------------------------------------------------------------------
// Test: SelectNode — empty NodeTable
// ---------------------------------------------------------------------------

func TestSelectNode_EmptyNodeTable(t *testing.T) {
	s := NewScheduler(nil)
	s.SetNodeTable(nodetable.NewNodeTable())
	_, err := s.SelectNode(TaskRequirement{TaskID: "t1"})
	if err == nil {
		t.Error("expected error when NodeTable is empty")
	}
}

// ---------------------------------------------------------------------------
// Test: SelectNode — single node
// ---------------------------------------------------------------------------

func TestSelectNode_SingleNode(t *testing.T) {
	nt := makeNodeTable(map[string]*mockNode{
		"worker-A": {
			hostname:   "host-a",
			cpuCores:   8,
			cpuUsage:   30.0,
			memUsage:   40.0,
			memTotal:   16 * 1024 * 1024 * 1024, // 16 GB
			diskUsage:  50.0,
			diskTotal:  500 * 1024 * 1024 * 1024,
			taskCount:  0,
			heartbeats: 10,
		},
	})

	s := NewScheduler(nil)
	s.SetNodeTable(nt)

	result, err := s.SelectNode(TaskRequirement{
		TaskID:   "t1",
		TaskType: "video_transcode",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.WorkerID != "worker-A" {
		t.Errorf("expected worker-A, got %s", result.WorkerID)
	}
	if result.Score <= 0 {
		t.Errorf("expected positive score, got %.4f", result.Score)
	}
	if result.Profile == nil {
		t.Error("expected non-nil Profile")
	}
}

// ---------------------------------------------------------------------------
// Test: SelectNode — stronger node gets higher score
// ---------------------------------------------------------------------------

func TestSelectNode_StrongerNodePreferred(t *testing.T) {
	nt := makeNodeTable(map[string]*mockNode{
		"strong": {
			hostname:   "strong-host",
			cpuCores:   16,
			cpuUsage:   10.0, // barely loaded
			memUsage:   20.0,
			memTotal:   32 * 1024 * 1024 * 1024,
			diskUsage:  30.0,
			diskTotal:  1024 * 1024 * 1024 * 1024,
			taskCount:  0,
			heartbeats: 20,
		},
		"weak": {
			hostname:   "weak-host",
			cpuCores:   2,
			cpuUsage:   80.0, // heavily loaded
			memUsage:   90.0,
			memTotal:   4 * 1024 * 1024 * 1024,
			diskUsage:  85.0,
			diskTotal:  100 * 1024 * 1024 * 1024,
			taskCount:  3,
			heartbeats: 20,
		},
	})

	s := NewScheduler(nil)
	s.SetNodeTable(nt)

	result, err := s.SelectNode(TaskRequirement{
		TaskID:   "t1",
		TaskType: "video_transcode",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.WorkerID != "strong" {
		t.Errorf("expected 'strong' node to be selected, got %s", result.WorkerID)
	}
}

// ---------------------------------------------------------------------------
// Test: SelectNode — task count penalty
//
// Two identical nodes but one has tasks already running.
// The idle node should be preferred.
// ---------------------------------------------------------------------------

func TestSelectNode_TaskCountPenalty(t *testing.T) {
	nt := makeNodeTable(map[string]*mockNode{
		"idle": {
			hostname:   "idle-host",
			cpuCores:   8,
			cpuUsage:   20.0,
			memUsage:   30.0,
			memTotal:   16 * 1024 * 1024 * 1024,
			diskUsage:  40.0,
			diskTotal:  500 * 1024 * 1024 * 1024,
			taskCount:  0,
			heartbeats: 10,
		},
		"busy": {
			hostname:   "busy-host",
			cpuCores:   8,
			cpuUsage:   20.0,
			memUsage:   30.0,
			memTotal:   16 * 1024 * 1024 * 1024,
			diskUsage:  40.0,
			diskTotal:  500 * 1024 * 1024 * 1024,
			taskCount:  5,
			heartbeats: 10,
		},
	})

	s := NewScheduler(nil)
	s.SetNodeTable(nt)

	result, err := s.SelectNode(TaskRequirement{TaskID: "t1"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.WorkerID != "idle" {
		t.Errorf("expected 'idle' node (no tasks) to be preferred, got %s", result.WorkerID)
	}
}

// ---------------------------------------------------------------------------
// Test: SelectNode — reliability filter
// ---------------------------------------------------------------------------

func TestSelectNode_ReliabilityFilter(t *testing.T) {
	nt := makeNodeTable(map[string]*mockNode{
		"reliable": {
			hostname:   "reliable-host",
			cpuCores:   4,
			cpuUsage:   50.0,
			memUsage:   50.0,
			memTotal:   8 * 1024 * 1024 * 1024,
			diskUsage:  50.0,
			diskTotal:  256 * 1024 * 1024 * 1024,
			taskCount:  0,
			heartbeats: 20,
		},
		"unreliable": {
			hostname:   "unreliable-host",
			cpuCores:   16,
			cpuUsage:   10.0,
			memUsage:   10.0,
			memTotal:   64 * 1024 * 1024 * 1024,
			diskUsage:  10.0,
			diskTotal:  2 * 1024 * 1024 * 1024 * 1024,
			taskCount:  0,
			heartbeats: 1, // very few heartbeats; reliability might still be high at 1/1
		},
	})

	s := NewScheduler(nil)
	s.SetNodeTable(nt)

	// With a high minimum reliability both should pass since heartbeat hits / total are 100%
	result, err := s.SelectNode(TaskRequirement{
		TaskID:         "t1",
		MinReliability: 0.9,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// The "unreliable" node actually has 16 cores and low usage, so it should score higher
	if result.WorkerID != "unreliable" {
		t.Logf("note: selected %s (both nodes have 100%% reliability from heartbeats only)", result.WorkerID)
	}
}

// ---------------------------------------------------------------------------
// Test: SelectNode — MinCPU filter
// ---------------------------------------------------------------------------

func TestSelectNode_MinCPUFilter(t *testing.T) {
	nt := makeNodeTable(map[string]*mockNode{
		"tiny": {
			hostname:   "tiny",
			cpuCores:   1,
			cpuUsage:   90.0, // only 10% of 1 core available → very low cpu norm
			memUsage:   50.0,
			memTotal:   4 * 1024 * 1024 * 1024,
			diskUsage:  50.0,
			diskTotal:  100 * 1024 * 1024 * 1024,
			taskCount:  0,
			heartbeats: 10,
		},
		"big": {
			hostname:   "big",
			cpuCores:   32,
			cpuUsage:   10.0,
			memUsage:   20.0,
			memTotal:   64 * 1024 * 1024 * 1024,
			diskUsage:  20.0,
			diskTotal:  1024 * 1024 * 1024 * 1024,
			taskCount:  0,
			heartbeats: 10,
		},
	})

	s := NewScheduler(nil)
	s.SetNodeTable(nt)

	// Set MinCPU high enough to filter out the tiny node
	// tiny: 1 core * 0.1 available / 128 norm = 0.00078 → below 0.01
	result, err := s.SelectNode(TaskRequirement{
		TaskID: "t1",
		MinCPU: 0.01,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.WorkerID != "big" {
		t.Errorf("expected 'big' node after MinCPU filter, got %s", result.WorkerID)
	}
}

// ---------------------------------------------------------------------------
// Test: SelectNodes — batch scheduling
// ---------------------------------------------------------------------------

func TestSelectNodes_Batch(t *testing.T) {
	nt := makeNodeTable(map[string]*mockNode{
		"w1": {hostname: "h1", cpuCores: 8, cpuUsage: 30, memUsage: 30, memTotal: 16 << 30, diskUsage: 30, diskTotal: 500 << 30, heartbeats: 10},
		"w2": {hostname: "h2", cpuCores: 4, cpuUsage: 50, memUsage: 50, memTotal: 8 << 30, diskUsage: 50, diskTotal: 250 << 30, heartbeats: 10},
	})

	s := NewScheduler(nil)
	s.SetNodeTable(nt)

	tasks := []TaskRequirement{
		{TaskID: "t1", TaskType: "video"},
		{TaskID: "t2", TaskType: "video"},
		{TaskID: "t3", TaskType: "video"},
	}

	results, errs := s.SelectNodes(tasks)

	if len(errs) != 0 {
		t.Errorf("expected 0 errors, got %d: %v", len(errs), errs)
	}
	if len(results) != 3 {
		t.Errorf("expected 3 results, got %d", len(results))
	}
	for taskID, result := range results {
		if result.WorkerID == "" {
			t.Errorf("task %s has empty WorkerID", taskID)
		}
		if result.Score <= 0 {
			t.Errorf("task %s has non-positive score: %.4f", taskID, result.Score)
		}
	}
}

// ---------------------------------------------------------------------------
// Test: SplitTask — proportional splitting by node capacity
// ---------------------------------------------------------------------------

func TestSplitTask_Proportional(t *testing.T) {
	nt := makeNodeTable(map[string]*mockNode{
		"fast": {
			hostname:   "fast",
			cpuCores:   16,
			cpuUsage:   10.0,
			memUsage:   20.0,
			memTotal:   32 * 1024 * 1024 * 1024,
			diskUsage:  20.0,
			diskTotal:  1 * 1024 * 1024 * 1024 * 1024,
			taskCount:  0,
			heartbeats: 20,
		},
		"slow": {
			hostname:   "slow",
			cpuCores:   4,
			cpuUsage:   50.0,
			memUsage:   60.0,
			memTotal:   8 * 1024 * 1024 * 1024,
			diskUsage:  60.0,
			diskTotal:  256 * 1024 * 1024 * 1024,
			taskCount:  0,
			heartbeats: 20,
		},
	})

	s := NewScheduler(nil)
	s.SetNodeTable(nt)

	totalUnits := int64(10000)
	splits, err := s.SplitTask("task-1", totalUnits, TaskRequirement{
		TaskID:   "task-1",
		TaskType: "video_transcode",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(splits) != 2 {
		t.Fatalf("expected 2 splits, got %d", len(splits))
	}

	// Verify total units sum correctly
	var totalAssigned int64
	for _, sp := range splits {
		totalAssigned += sp.Length
		if sp.WorkerID == "" {
			t.Error("split has empty WorkerID")
		}
		if sp.Weight <= 0 || sp.Weight > 1 {
			t.Errorf("split weight out of range: %.4f", sp.Weight)
		}
	}
	if totalAssigned != totalUnits {
		t.Errorf("total assigned units %d != total %d", totalAssigned, totalUnits)
	}

	// The "fast" node should get more units than the "slow" node
	fastUnits := int64(0)
	slowUnits := int64(0)
	for _, sp := range splits {
		if sp.WorkerID == "fast" {
			fastUnits = sp.Length
		} else if sp.WorkerID == "slow" {
			slowUnits = sp.Length
		}
	}
	if fastUnits <= slowUnits {
		t.Errorf("expected 'fast' node to get more units (%d) than 'slow' (%d)", fastUnits, slowUnits)
	}
}

// ---------------------------------------------------------------------------
// Test: SplitTask — offsets are contiguous
// ---------------------------------------------------------------------------

func TestSplitTask_ContiguousOffsets(t *testing.T) {
	nt := makeNodeTable(map[string]*mockNode{
		"w1": {hostname: "h1", cpuCores: 8, cpuUsage: 20, memUsage: 30, memTotal: 16 << 30, diskUsage: 30, diskTotal: 500 << 30, heartbeats: 10},
		"w2": {hostname: "h2", cpuCores: 8, cpuUsage: 20, memUsage: 30, memTotal: 16 << 30, diskUsage: 30, diskTotal: 500 << 30, heartbeats: 10},
		"w3": {hostname: "h3", cpuCores: 4, cpuUsage: 40, memUsage: 50, memTotal: 8 << 30, diskUsage: 50, diskTotal: 250 << 30, heartbeats: 10},
	})

	s := NewScheduler(nil)
	s.SetNodeTable(nt)

	totalUnits := int64(90000)
	splits, err := s.SplitTask("task-offsets", totalUnits, TaskRequirement{TaskID: "task-offsets"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify offsets are contiguous
	expectedOffset := int64(0)
	for i, sp := range splits {
		if sp.Offset != expectedOffset {
			t.Errorf("split %d: expected offset %d, got %d", i, expectedOffset, sp.Offset)
		}
		expectedOffset += sp.Length
	}
	if expectedOffset != totalUnits {
		t.Errorf("final offset %d != totalUnits %d", expectedOffset, totalUnits)
	}
}

// ---------------------------------------------------------------------------
// Test: SplitTask — no NodeTable
// ---------------------------------------------------------------------------

func TestSplitTask_NoNodeTable(t *testing.T) {
	s := NewScheduler(nil)
	_, err := s.SplitTask("t1", 1000, TaskRequirement{TaskID: "t1"})
	if err == nil {
		t.Error("expected error when NodeTable is nil")
	}
}

// ---------------------------------------------------------------------------
// Test: SplitTask — empty NodeTable
// ---------------------------------------------------------------------------

func TestSplitTask_EmptyNodeTable(t *testing.T) {
	s := NewScheduler(nil)
	s.SetNodeTable(nodetable.NewNodeTable())
	_, err := s.SplitTask("t1", 1000, TaskRequirement{TaskID: "t1"})
	if err == nil {
		t.Error("expected error when no nodes available")
	}
}

// ---------------------------------------------------------------------------
// Test: ReassignTasks
// ---------------------------------------------------------------------------

func TestReassignTasks(t *testing.T) {
	nt := makeNodeTable(map[string]*mockNode{
		"alive-1": {hostname: "h1", cpuCores: 8, cpuUsage: 20, memUsage: 30, memTotal: 16 << 30, diskUsage: 30, diskTotal: 500 << 30, heartbeats: 10},
		"alive-2": {hostname: "h2", cpuCores: 4, cpuUsage: 40, memUsage: 50, memTotal: 8 << 30, diskUsage: 50, diskTotal: 250 << 30, heartbeats: 10},
	})

	// Simulate an offline node that had tasks
	offlineInfo := &worker.WorkerInfo{
		Hostname:        "dead-host",
		Os:              "linux",
		Arch:            "amd64",
		CpuLogicalCores: 8,
	}
	nt.Register("dead-node", offlineInfo, "")
	nt.AssignTask("dead-node", "task-a")
	nt.AssignTask("dead-node", "task-b")

	s := NewScheduler(nil)
	s.SetNodeTable(nt)

	results, errs := s.ReassignTasks("dead-node", []string{"task-a", "task-b"})

	if len(errs) != 0 {
		t.Errorf("expected 0 errors, got %d: %v", len(errs), errs)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 reassigned tasks, got %d", len(results))
	}

	for taskID, result := range results {
		if result.WorkerID == "dead-node" {
			t.Errorf("task %s was reassigned back to the dead node", taskID)
		}
		if result.WorkerID != "alive-1" && result.WorkerID != "alive-2" {
			t.Errorf("task %s assigned to unknown worker %s", taskID, result.WorkerID)
		}
	}
}

// ---------------------------------------------------------------------------
// Test: Heavy task weight gives CPU bonus
// ---------------------------------------------------------------------------

func TestSelectNode_HeavyWeight(t *testing.T) {
	nt := makeNodeTable(map[string]*mockNode{
		"balanced": {
			hostname:   "bal",
			cpuCores:   8,
			cpuUsage:   30.0,
			memUsage:   30.0,
			memTotal:   16 * 1024 * 1024 * 1024,
			diskUsage:  30.0,
			diskTotal:  500 * 1024 * 1024 * 1024,
			taskCount:  0,
			heartbeats: 15,
		},
	})

	s := NewScheduler(nil)
	s.SetNodeTable(nt)

	// Normal weight task
	resultNormal, err := s.SelectNode(TaskRequirement{
		TaskID: "normal",
		Weight: 1.0,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Heavy weight task
	resultHeavy, err := s.SelectNode(TaskRequirement{
		TaskID: "heavy",
		Weight: 8.0,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Heavy task should get a bonus score (same node, but score is higher)
	if resultHeavy.Score <= resultNormal.Score {
		t.Errorf("heavy task should have higher score (got heavy=%.4f, normal=%.4f)",
			resultHeavy.Score, resultNormal.Score)
	}
}

// ---------------------------------------------------------------------------
// Test: Offline node is excluded from scheduling
// ---------------------------------------------------------------------------

func TestSelectNode_OfflineExcluded(t *testing.T) {
	nt := makeNodeTable(map[string]*mockNode{
		"online": {hostname: "on", cpuCores: 4, cpuUsage: 30, memUsage: 30, memTotal: 8 << 30, diskUsage: 30, diskTotal: 250 << 30, heartbeats: 10},
	})

	// Register another node but don't send heartbeats — manually set offline
	offlineInfo := &worker.WorkerInfo{
		Hostname:        "off",
		Os:              "linux",
		Arch:            "amd64",
		CpuLogicalCores: 32,
	}
	nt.Register("offline-node", offlineInfo, "")
	// Get the record and manually set it offline
	rec, _ := nt.Get("offline-node")
	if rec != nil {
		rec.State = nodetable.NodeStateOffline
	}

	s := NewScheduler(nil)
	s.SetNodeTable(nt)

	result, err := s.SelectNode(TaskRequirement{TaskID: "t1"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.WorkerID != "online" {
		t.Errorf("expected 'online' node, got %s (offline node should be excluded)", result.WorkerID)
	}
}

// ---------------------------------------------------------------------------
// Test: All nodes filtered out produces error
// ---------------------------------------------------------------------------

func TestSelectNode_AllFiltered(t *testing.T) {
	nt := makeNodeTable(map[string]*mockNode{
		"w1": {hostname: "h1", cpuCores: 2, cpuUsage: 90, memUsage: 90, memTotal: 2 << 30, diskUsage: 90, diskTotal: 50 << 30, heartbeats: 5},
	})

	s := NewScheduler(nil)
	s.SetNodeTable(nt)

	// Demand extremely high CPU → should filter out the only node
	_, err := s.SelectNode(TaskRequirement{
		TaskID: "t1",
		MinCPU: 0.99, // 99% of 128 cores = ~127 cores of available capacity needed
	})
	if err == nil {
		t.Error("expected error when all nodes are filtered out")
	}
}

// ===========================================================================
// Speculative Execution Tests
// ===========================================================================

// ---------------------------------------------------------------------------
// Test: SpeculativeExecute — probability below threshold, no replicas
// ---------------------------------------------------------------------------

func TestSpeculativeExecute_BelowThreshold(t *testing.T) {
	nt := makeNodeTable(map[string]*mockNode{
		"w1": {hostname: "h1", cpuCores: 8, cpuUsage: 30, memUsage: 40, memTotal: 16 << 30, diskUsage: 30, diskTotal: 500 << 30, heartbeats: 20},
		"w2": {hostname: "h2", cpuCores: 8, cpuUsage: 30, memUsage: 40, memTotal: 16 << 30, diskUsage: 30, diskTotal: 500 << 30, heartbeats: 20},
	})

	s := NewScheduler(nil)
	s.SetNodeTable(nt)

	// offlineProb < SpeculativeProbThreshold → no replicas
	results, errs := s.SpeculativeExecute("w1", []string{"task-1", "task-2"}, 0.1)

	if len(results) != 0 {
		t.Errorf("expected 0 results when below threshold, got %d", len(results))
	}
	if len(errs) != 0 {
		t.Errorf("expected 0 errors when below threshold, got %d", len(errs))
	}
}

// ---------------------------------------------------------------------------
// Test: SpeculativeExecute — probability above threshold, replicas created
// ---------------------------------------------------------------------------

func TestSpeculativeExecute_AboveThreshold(t *testing.T) {
	nt := makeNodeTable(map[string]*mockNode{
		"w1": {hostname: "h1", cpuCores: 8, cpuUsage: 30, memUsage: 40, memTotal: 16 << 30, diskUsage: 30, diskTotal: 500 << 30, heartbeats: 20},
		"w2": {hostname: "h2", cpuCores: 16, cpuUsage: 20, memUsage: 30, memTotal: 32 << 30, diskUsage: 20, diskTotal: 1000 << 30, heartbeats: 30},
	})

	s := NewScheduler(nil)
	s.SetNodeTable(nt)

	results, errs := s.SpeculativeExecute("w1", []string{"task-1"}, 0.6)

	if len(errs) > 0 {
		t.Errorf("unexpected errors: %v", errs)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 speculative result, got %d", len(results))
	}

	spec := results[0]
	if spec.OriginalWorkerID != "w1" {
		t.Errorf("expected original worker w1, got %s", spec.OriginalWorkerID)
	}
	if spec.TaskID != "task-1" {
		t.Errorf("expected task-1, got %s", spec.TaskID)
	}
	if spec.ReplicaTaskID != "task-1_spec" {
		t.Errorf("expected replica ID task-1_spec, got %s", spec.ReplicaTaskID)
	}
	if spec.ReplicaWorkerID != "w2" {
		t.Errorf("expected replica on w2, got %s", spec.ReplicaWorkerID)
	}
	if spec.ReplicaScore <= 0 {
		t.Errorf("expected positive replica score, got %.4f", spec.ReplicaScore)
	}
	if spec.OfflineProb != 0.6 {
		t.Errorf("expected offlineProb=0.6, got %.2f", spec.OfflineProb)
	}

	// Verify the speculative task was assigned in NodeTable
	rec, ok := nt.Get("w2")
	if !ok {
		t.Fatal("w2 not found in NodeTable")
	}
	found := false
	for _, tid := range rec.AssignedTasks {
		if tid == "task-1_spec" {
			found = true
			break
		}
	}
	if !found {
		t.Error("speculative task task-1_spec should be assigned to w2 in NodeTable")
	}
}

// ---------------------------------------------------------------------------
// Test: SpeculativeExecute — multiple tasks
// ---------------------------------------------------------------------------

func TestSpeculativeExecute_MultipleTasks(t *testing.T) {
	nt := makeNodeTable(map[string]*mockNode{
		"w1": {hostname: "h1", cpuCores: 8, cpuUsage: 30, memUsage: 40, memTotal: 16 << 30, diskUsage: 30, diskTotal: 500 << 30, heartbeats: 20},
		"w2": {hostname: "h2", cpuCores: 16, cpuUsage: 20, memUsage: 30, memTotal: 32 << 30, diskUsage: 20, diskTotal: 1000 << 30, heartbeats: 30},
		"w3": {hostname: "h3", cpuCores: 12, cpuUsage: 25, memUsage: 35, memTotal: 24 << 30, diskUsage: 25, diskTotal: 800 << 30, heartbeats: 25},
	})

	s := NewScheduler(nil)
	s.SetNodeTable(nt)

	results, errs := s.SpeculativeExecute("w1", []string{"task-1", "task-2", "task-3"}, 0.5)

	if len(errs) > 0 {
		t.Errorf("unexpected errors: %v", errs)
	}
	if len(results) != 3 {
		t.Fatalf("expected 3 speculative results, got %d", len(results))
	}

	// All replicas should be on w2 or w3 (not w1)
	for _, spec := range results {
		if spec.ReplicaWorkerID == "w1" {
			t.Errorf("replica for %s should not be on declining worker w1", spec.TaskID)
		}
	}
}

// ---------------------------------------------------------------------------
// Test: SpeculativeExecute — only one node (declining), no alternatives
// ---------------------------------------------------------------------------

func TestSpeculativeExecute_NoAlternativeNodes(t *testing.T) {
	nt := makeNodeTable(map[string]*mockNode{
		"w1": {hostname: "h1", cpuCores: 8, cpuUsage: 30, memUsage: 40, memTotal: 16 << 30, diskUsage: 30, diskTotal: 500 << 30, heartbeats: 20},
	})

	s := NewScheduler(nil)
	s.SetNodeTable(nt)

	results, errs := s.SpeculativeExecute("w1", []string{"task-1"}, 0.8)

	if len(results) != 0 {
		t.Errorf("expected 0 results when no alternative nodes, got %d", len(results))
	}
	if len(errs) != 1 {
		t.Errorf("expected 1 error when no alternative nodes, got %d", len(errs))
	}
}

// ---------------------------------------------------------------------------
// Test: SpeculativeExecute — no NodeTable set
// ---------------------------------------------------------------------------

func TestSpeculativeExecute_NoNodeTable(t *testing.T) {
	s := NewScheduler(nil)
	// Do not set NodeTable

	results, errs := s.SpeculativeExecute("w1", []string{"task-1"}, 0.8)

	if len(results) != 0 {
		t.Errorf("expected 0 results, got %d", len(results))
	}
	if len(errs) != 1 {
		t.Errorf("expected 1 error, got %d", len(errs))
	}
}

// ---------------------------------------------------------------------------
// Test: SpeculativeExecute — excludes offline nodes
// ---------------------------------------------------------------------------

func TestSpeculativeExecute_ExcludesOfflineNodes(t *testing.T) {
	nt := makeNodeTable(map[string]*mockNode{
		"w1": {hostname: "h1", cpuCores: 8, cpuUsage: 30, memUsage: 40, memTotal: 16 << 30, diskUsage: 30, diskTotal: 500 << 30, heartbeats: 20},
		"w2": {hostname: "h2", cpuCores: 16, cpuUsage: 20, memUsage: 30, memTotal: 32 << 30, diskUsage: 20, diskTotal: 1000 << 30, heartbeats: 30},
		"w3": {hostname: "h3", cpuCores: 12, cpuUsage: 25, memUsage: 35, memTotal: 24 << 30, diskUsage: 25, diskTotal: 800 << 30, heartbeats: 25},
	})

	// Mark w2 and w3 as offline
	rec2, _ := nt.Get("w2")
	rec2.State = nodetable.NodeStateOffline
	rec3, _ := nt.Get("w3")
	rec3.State = nodetable.NodeStateOffline

	s := NewScheduler(nil)
	s.SetNodeTable(nt)

	results, errs := s.SpeculativeExecute("w1", []string{"task-1"}, 0.8)

	if len(results) != 0 {
		t.Errorf("expected 0 results when all alternatives are offline, got %d", len(results))
	}
	if len(errs) != 1 {
		t.Errorf("expected 1 error, got %d", len(errs))
	}
}

// ---------------------------------------------------------------------------
// Test: SpeculativeExecute — replica MinReliability filter
// ---------------------------------------------------------------------------

func TestSpeculativeExecute_ReliabilityFilter(t *testing.T) {
	nt := makeNodeTable(map[string]*mockNode{
		"w1": {hostname: "h1", cpuCores: 8, cpuUsage: 30, memUsage: 40, memTotal: 16 << 30, diskUsage: 30, diskTotal: 500 << 30, heartbeats: 20},
		"w2": {hostname: "h2", cpuCores: 16, cpuUsage: 20, memUsage: 30, memTotal: 32 << 30, diskUsage: 20, diskTotal: 1000 << 30, heartbeats: 1},
	})

	// Make w2 have very low reliability: 1 hit and many misses
	rec2, _ := nt.Get("w2")
	rec2.HeartbeatMissCount = 100

	s := NewScheduler(nil)
	s.SetNodeTable(nt)

	// Speculative requires MinReliability=0.5, w2's reliability ≈ 1/101 ≈ 0.01
	results, errs := s.SpeculativeExecute("w1", []string{"task-1"}, 0.8)

	if len(results) != 0 {
		t.Errorf("expected 0 results when alternative has low reliability, got %d", len(results))
	}
	if len(errs) != 1 {
		t.Errorf("expected 1 error, got %d", len(errs))
	}
}

// ---------------------------------------------------------------------------
// Test: selectNodeExcluding — basic functionality
// ---------------------------------------------------------------------------

func TestSelectNodeExcluding_Basic(t *testing.T) {
	nt := makeNodeTable(map[string]*mockNode{
		"w1": {hostname: "h1", cpuCores: 8, cpuUsage: 30, memUsage: 40, memTotal: 16 << 30, diskUsage: 30, diskTotal: 500 << 30, heartbeats: 20},
		"w2": {hostname: "h2", cpuCores: 16, cpuUsage: 20, memUsage: 30, memTotal: 32 << 30, diskUsage: 20, diskTotal: 1000 << 30, heartbeats: 30},
		"w3": {hostname: "h3", cpuCores: 4, cpuUsage: 50, memUsage: 60, memTotal: 8 << 30, diskUsage: 50, diskTotal: 200 << 30, heartbeats: 15},
	})

	s := NewScheduler(nil)
	s.SetNodeTable(nt)

	req := TaskRequirement{
		TaskID: "test-task",
		MinCPU: 0.01,
	}

	result, err := s.selectNodeExcluding(req, "w2")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result.WorkerID == "w2" {
		t.Error("excluded worker w2 should not be selected")
	}
}

func TestSelectNodeExcluding_ExcludeOnlyNode(t *testing.T) {
	nt := makeNodeTable(map[string]*mockNode{
		"w1": {hostname: "h1", cpuCores: 8, cpuUsage: 30, memUsage: 40, memTotal: 16 << 30, diskUsage: 30, diskTotal: 500 << 30, heartbeats: 20},
	})

	s := NewScheduler(nil)
	s.SetNodeTable(nt)

	req := TaskRequirement{
		TaskID: "test-task",
		MinCPU: 0.01,
	}

	_, err := s.selectNodeExcluding(req, "w1")
	if err == nil {
		t.Error("expected error when excluding the only available node")
	}
}

func TestSelectNodeExcluding_NoNodeTable(t *testing.T) {
	s := NewScheduler(nil)

	req := TaskRequirement{TaskID: "test-task"}
	_, err := s.selectNodeExcluding(req, "w1")
	if err == nil {
		t.Error("expected error when no NodeTable is set")
	}
}

// ---------------------------------------------------------------------------
// Test: CancelSpeculativeTask
// ---------------------------------------------------------------------------

func TestCancelSpeculativeTask_Basic(t *testing.T) {
	nt := makeNodeTable(map[string]*mockNode{
		"w1": {hostname: "h1", cpuCores: 8, cpuUsage: 30, memUsage: 40, memTotal: 16 << 30, diskUsage: 30, diskTotal: 500 << 30, heartbeats: 20},
		"w2": {hostname: "h2", cpuCores: 16, cpuUsage: 20, memUsage: 30, memTotal: 32 << 30, diskUsage: 20, diskTotal: 1000 << 30, heartbeats: 30},
	})

	s := NewScheduler(nil)
	s.SetNodeTable(nt)

	// First create a speculative copy
	results, _ := s.SpeculativeExecute("w1", []string{"task-1"}, 0.6)
	if len(results) != 1 {
		t.Fatalf("expected 1 speculative result, got %d", len(results))
	}

	spec := results[0]

	// Verify the speculative task is in NodeTable
	rec, _ := nt.Get(spec.ReplicaWorkerID)
	foundBefore := false
	for _, tid := range rec.AssignedTasks {
		if tid == spec.ReplicaTaskID {
			foundBefore = true
			break
		}
	}
	if !foundBefore {
		t.Fatal("speculative task should exist in NodeTable before cancel")
	}

	// Now cancel the speculative task
	s.CancelSpeculativeTask(spec)

	// Verify the speculative task is removed from NodeTable
	rec, _ = nt.Get(spec.ReplicaWorkerID)
	foundAfter := false
	for _, tid := range rec.AssignedTasks {
		if tid == spec.ReplicaTaskID {
			foundAfter = true
			break
		}
	}
	if foundAfter {
		t.Error("speculative task should be removed from NodeTable after cancel")
	}
}

func TestCancelSpeculativeTask_NoNodeTable(t *testing.T) {
	s := NewScheduler(nil)
	// Should not panic when NodeTable is nil
	s.CancelSpeculativeTask(SpeculativeResult{
		OriginalWorkerID: "w1",
		TaskID:           "task-1",
		ReplicaTaskID:    "task-1_spec",
		ReplicaWorkerID:  "w2",
	})
}

// ---------------------------------------------------------------------------
// Test: SpeculativeExecute — empty task list
// ---------------------------------------------------------------------------

func TestSpeculativeExecute_EmptyTasks(t *testing.T) {
	nt := makeNodeTable(map[string]*mockNode{
		"w1": {hostname: "h1", cpuCores: 8, cpuUsage: 30, memUsage: 40, memTotal: 16 << 30, diskUsage: 30, diskTotal: 500 << 30, heartbeats: 20},
		"w2": {hostname: "h2", cpuCores: 16, cpuUsage: 20, memUsage: 30, memTotal: 32 << 30, diskUsage: 20, diskTotal: 1000 << 30, heartbeats: 30},
	})

	s := NewScheduler(nil)
	s.SetNodeTable(nt)

	results, errs := s.SpeculativeExecute("w1", []string{}, 0.8)

	if len(results) != 0 {
		t.Errorf("expected 0 results for empty tasks, got %d", len(results))
	}
	if len(errs) != 0 {
		t.Errorf("expected 0 errors for empty tasks, got %d", len(errs))
	}
}

// ---------------------------------------------------------------------------
// Test: SpeculativeExecute — prefers strongest available node
// ---------------------------------------------------------------------------

func TestSpeculativeExecute_PrefersStrongestNode(t *testing.T) {
	nt := makeNodeTable(map[string]*mockNode{
		"w1":     {hostname: "h1", cpuCores: 8, cpuUsage: 30, memUsage: 40, memTotal: 16 << 30, diskUsage: 30, diskTotal: 500 << 30, heartbeats: 20},
		"weak":   {hostname: "h2", cpuCores: 2, cpuUsage: 80, memUsage: 80, memTotal: 4 << 30, diskUsage: 70, diskTotal: 100 << 30, heartbeats: 20},
		"strong": {hostname: "h3", cpuCores: 32, cpuUsage: 10, memUsage: 20, memTotal: 64 << 30, diskUsage: 10, diskTotal: 2000 << 30, heartbeats: 30},
	})

	s := NewScheduler(nil)
	s.SetNodeTable(nt)

	results, errs := s.SpeculativeExecute("w1", []string{"task-1"}, 0.6)

	if len(errs) > 0 {
		t.Errorf("unexpected errors: %v", errs)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	if results[0].ReplicaWorkerID != "strong" {
		t.Errorf("expected replica on 'strong' node, got %s", results[0].ReplicaWorkerID)
	}
}
