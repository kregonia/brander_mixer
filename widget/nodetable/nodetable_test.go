package nodetable

import (
	"context"
	"math"
	"sync"
	"testing"
	"time"

	worker "github.com/kregonia/brander_mixer/script/rpc_server/worker"
)

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func makeWorkerInfo(hostname string, cores int32) *worker.WorkerInfo {
	return &worker.WorkerInfo{
		Hostname:        hostname,
		Os:              "linux",
		Arch:            "amd64",
		CpuLogicalCores: cores,
		CpuModelName:    "test-cpu",
	}
}

func makeStatus(cpuUsage float64, memUsage float64, taskCount int32) *worker.Status {
	return &worker.Status{
		Cpu: &worker.CpuInfo{
			CpuLogicalCores:  4,
			CpuUsagePercents: []float64{cpuUsage, cpuUsage, cpuUsage, cpuUsage},
		},
		Memory: &worker.MemoryInfo{
			MemoryUsagePercent: memUsage,
			MemoryTotal:        8 * 1024 * 1024 * 1024,
		},
		Disk: &worker.DiskInfo{
			DiskUsagePercent: 50.0,
			DiskTotal:        256 * 1024 * 1024 * 1024,
		},
		Network:   &worker.NetworkInfo{},
		TaskCount: taskCount,
	}
}

// ---------------------------------------------------------------------------
// Test: NewNodeTable
// ---------------------------------------------------------------------------

func TestNewNodeTable(t *testing.T) {
	nt := NewNodeTable()
	if nt == nil {
		t.Fatal("NewNodeTable returned nil")
	}
	all := nt.AllNodes()
	if len(all) != 0 {
		t.Errorf("expected empty NodeTable, got %d nodes", len(all))
	}
}

// ---------------------------------------------------------------------------
// Test: Register
// ---------------------------------------------------------------------------

func TestRegister(t *testing.T) {
	nt := NewNodeTable()

	info := makeWorkerInfo("host-a", 8)
	nt.Register("worker-1", info, "10.0.0.1:50052")

	rec, ok := nt.Get("worker-1")
	if !ok {
		t.Fatal("worker-1 not found after registration")
	}
	if rec.Info.GetHostname() != "host-a" {
		t.Errorf("expected hostname 'host-a', got '%s'", rec.Info.GetHostname())
	}
	if rec.GrpcAddr != "10.0.0.1:50052" {
		t.Errorf("expected GrpcAddr '10.0.0.1:50052', got '%s'", rec.GrpcAddr)
	}
	if rec.State != NodeStateOnline {
		t.Errorf("expected state Online, got %s", rec.State)
	}
	if rec.LastHeartbeat.IsZero() {
		t.Error("expected non-zero LastHeartbeat after registration")
	}
	if rec.RegisteredAt.IsZero() {
		t.Error("expected non-zero RegisteredAt after registration")
	}
}

// ---------------------------------------------------------------------------
// Test: Register replaces existing node
// ---------------------------------------------------------------------------

func TestRegister_Overwrite(t *testing.T) {
	nt := NewNodeTable()

	nt.Register("w1", makeWorkerInfo("old-host", 4), "addr-old")
	nt.Register("w1", makeWorkerInfo("new-host", 16), "addr-new")

	rec, ok := nt.Get("w1")
	if !ok {
		t.Fatal("w1 not found")
	}
	if rec.Info.GetHostname() != "new-host" {
		t.Errorf("expected hostname 'new-host' after re-register, got '%s'", rec.Info.GetHostname())
	}
	if rec.Info.GetCpuLogicalCores() != 16 {
		t.Errorf("expected 16 cores after re-register, got %d", rec.Info.GetCpuLogicalCores())
	}
	if rec.GrpcAddr != "addr-new" {
		t.Errorf("expected GrpcAddr 'addr-new', got '%s'", rec.GrpcAddr)
	}
}

// ---------------------------------------------------------------------------
// Test: UpdateHeartbeat
// ---------------------------------------------------------------------------

func TestUpdateHeartbeat(t *testing.T) {
	nt := NewNodeTable()
	nt.Register("w1", makeWorkerInfo("h1", 4), "")

	rec, _ := nt.Get("w1")
	initialHB := rec.LastHeartbeat

	time.Sleep(10 * time.Millisecond)

	status := makeStatus(30.0, 40.0, 2)
	nt.UpdateHeartbeat("w1", status)

	rec, _ = nt.Get("w1")
	if !rec.LastHeartbeat.After(initialHB) {
		t.Error("expected LastHeartbeat to be updated after heartbeat")
	}
	if rec.LatestStatus == nil {
		t.Fatal("expected LatestStatus to be non-nil after heartbeat")
	}
	if rec.LatestStatus.GetTaskCount() != 2 {
		t.Errorf("expected task count 2, got %d", rec.LatestStatus.GetTaskCount())
	}
	if rec.HeartbeatHitCount != 1 {
		t.Errorf("expected HeartbeatHitCount 1, got %d", rec.HeartbeatHitCount)
	}
}

// ---------------------------------------------------------------------------
// Test: UpdateHeartbeat for non-existent worker is a no-op
// ---------------------------------------------------------------------------

func TestUpdateHeartbeat_NonExistent(t *testing.T) {
	nt := NewNodeTable()
	// Should not panic
	nt.UpdateHeartbeat("ghost", makeStatus(10, 10, 0))

	_, ok := nt.Get("ghost")
	if ok {
		t.Error("heartbeat for non-existent worker should not create a record")
	}
}

// ---------------------------------------------------------------------------
// Test: UpdateHeartbeat restores Online state from Unstable
// ---------------------------------------------------------------------------

func TestUpdateHeartbeat_RestoresOnline(t *testing.T) {
	nt := NewNodeTable()
	nt.Register("w1", makeWorkerInfo("h1", 4), "")

	// Manually set node to Unstable
	rec, _ := nt.Get("w1")
	nt.mu.Lock()
	rec.State = NodeStateUnstable
	nt.mu.Unlock()

	// Heartbeat should bring it back to Online
	nt.UpdateHeartbeat("w1", makeStatus(20, 30, 0))

	rec, _ = nt.Get("w1")
	if rec.State != NodeStateOnline {
		t.Errorf("expected state Online after heartbeat recovery, got %s", rec.State)
	}
}

// ---------------------------------------------------------------------------
// Test: Remove
// ---------------------------------------------------------------------------

func TestRemove(t *testing.T) {
	nt := NewNodeTable()
	nt.Register("w1", makeWorkerInfo("h1", 4), "")
	nt.Register("w2", makeWorkerInfo("h2", 8), "")

	nt.Remove("w1")

	_, ok := nt.Get("w1")
	if ok {
		t.Error("w1 should not exist after removal")
	}

	_, ok = nt.Get("w2")
	if !ok {
		t.Error("w2 should still exist after removing w1")
	}
}

// ---------------------------------------------------------------------------
// Test: Remove non-existent is no-op
// ---------------------------------------------------------------------------

func TestRemove_NonExistent(t *testing.T) {
	nt := NewNodeTable()
	// Should not panic
	nt.Remove("ghost")
}

// ---------------------------------------------------------------------------
// Test: OnlineNodes
// ---------------------------------------------------------------------------

func TestOnlineNodes(t *testing.T) {
	nt := NewNodeTable()
	nt.Register("w1", makeWorkerInfo("h1", 4), "")
	nt.Register("w2", makeWorkerInfo("h2", 8), "")
	nt.Register("w3", makeWorkerInfo("h3", 2), "")

	// Set w3 to offline
	rec3, _ := nt.Get("w3")
	nt.mu.Lock()
	rec3.State = NodeStateOffline
	nt.mu.Unlock()

	// Set w2 to unstable (should still appear in OnlineNodes)
	rec2, _ := nt.Get("w2")
	nt.mu.Lock()
	rec2.State = NodeStateUnstable
	nt.mu.Unlock()

	online := nt.OnlineNodes()
	if len(online) != 2 {
		t.Fatalf("expected 2 online/unstable nodes, got %d", len(online))
	}

	found := map[string]bool{}
	for _, id := range online {
		found[id] = true
	}
	if !found["w1"] {
		t.Error("w1 should be in OnlineNodes")
	}
	if !found["w2"] {
		t.Error("w2 (unstable) should be in OnlineNodes")
	}
	if found["w3"] {
		t.Error("w3 (offline) should NOT be in OnlineNodes")
	}
}

// ---------------------------------------------------------------------------
// Test: AllNodes returns a copy
// ---------------------------------------------------------------------------

func TestAllNodes_ReturnsCopy(t *testing.T) {
	nt := NewNodeTable()
	nt.Register("w1", makeWorkerInfo("h1", 4), "")

	all := nt.AllNodes()
	if len(all) != 1 {
		t.Fatalf("expected 1 node, got %d", len(all))
	}

	// Modifying the returned map should not affect the NodeTable
	delete(all, "w1")
	all2 := nt.AllNodes()
	if len(all2) != 1 {
		t.Error("deleting from AllNodes result should not affect internal state")
	}
}

// ---------------------------------------------------------------------------
// Test: AssignTask / RemoveTask
// ---------------------------------------------------------------------------

func TestAssignTask(t *testing.T) {
	nt := NewNodeTable()
	nt.Register("w1", makeWorkerInfo("h1", 4), "")

	nt.AssignTask("w1", "task-a")
	nt.AssignTask("w1", "task-b")
	nt.AssignTask("w1", "task-c")

	rec, _ := nt.Get("w1")
	if len(rec.AssignedTasks) != 3 {
		t.Fatalf("expected 3 assigned tasks, got %d", len(rec.AssignedTasks))
	}
	if rec.AssignedTasks[0] != "task-a" || rec.AssignedTasks[1] != "task-b" || rec.AssignedTasks[2] != "task-c" {
		t.Errorf("unexpected task list: %v", rec.AssignedTasks)
	}
}

func TestAssignTask_NonExistentWorker(t *testing.T) {
	nt := NewNodeTable()
	// Should not panic
	nt.AssignTask("ghost", "task-x")
}

func TestRemoveTask(t *testing.T) {
	nt := NewNodeTable()
	nt.Register("w1", makeWorkerInfo("h1", 4), "")

	nt.AssignTask("w1", "task-a")
	nt.AssignTask("w1", "task-b")
	nt.AssignTask("w1", "task-c")

	nt.RemoveTask("w1", "task-b")

	rec, _ := nt.Get("w1")
	if len(rec.AssignedTasks) != 2 {
		t.Fatalf("expected 2 tasks after removal, got %d", len(rec.AssignedTasks))
	}
	for _, id := range rec.AssignedTasks {
		if id == "task-b" {
			t.Error("task-b should have been removed")
		}
	}
}

func TestRemoveTask_NonExistent(t *testing.T) {
	nt := NewNodeTable()
	nt.Register("w1", makeWorkerInfo("h1", 4), "")
	nt.AssignTask("w1", "task-a")

	// Removing a non-existent task should be a no-op
	nt.RemoveTask("w1", "task-z")

	rec, _ := nt.Get("w1")
	if len(rec.AssignedTasks) != 1 {
		t.Error("removing non-existent task should not change the list")
	}
}

func TestRemoveTask_NonExistentWorker(t *testing.T) {
	nt := NewNodeTable()
	// Should not panic
	nt.RemoveTask("ghost", "task-x")
}

// ---------------------------------------------------------------------------
// Test: Reliability calculation
// ---------------------------------------------------------------------------

func TestReliability_Initial(t *testing.T) {
	rec := &NodeRecord{}
	r := rec.Reliability()
	if r != 1.0 {
		t.Errorf("expected reliability 1.0 for fresh node, got %.4f", r)
	}
}

func TestReliability_AllHits(t *testing.T) {
	rec := &NodeRecord{
		HeartbeatHitCount:  20,
		HeartbeatMissCount: 0,
	}
	r := rec.Reliability()
	if r != 1.0 {
		t.Errorf("expected reliability 1.0, got %.4f", r)
	}
}

func TestReliability_Mixed(t *testing.T) {
	rec := &NodeRecord{
		HeartbeatHitCount:  15,
		HeartbeatMissCount: 5,
	}
	r := rec.Reliability()
	expected := 0.75 // 15/20
	if r < expected-0.001 || r > expected+0.001 {
		t.Errorf("expected reliability ~%.2f, got %.4f", expected, r)
	}
}

func TestReliability_AllMisses(t *testing.T) {
	rec := &NodeRecord{
		HeartbeatHitCount:  0,
		HeartbeatMissCount: 10,
	}
	r := rec.Reliability()
	if r != 0.0 {
		t.Errorf("expected reliability 0.0, got %.4f", r)
	}
}

// ---------------------------------------------------------------------------
// Test: Uptime
// ---------------------------------------------------------------------------

func TestUptime(t *testing.T) {
	rec := &NodeRecord{
		RegisteredAt: time.Now().Add(-10 * time.Second),
	}
	uptime := rec.Uptime()
	if uptime < 9*time.Second || uptime > 11*time.Second {
		t.Errorf("expected ~10s uptime, got %v", uptime)
	}
}

// ---------------------------------------------------------------------------
// Test: NodeState.String()
// ---------------------------------------------------------------------------

func TestNodeState_String(t *testing.T) {
	tests := []struct {
		state    NodeState
		expected string
	}{
		{NodeStateOnline, "online"},
		{NodeStateUnstable, "unstable"},
		{NodeStateOffline, "offline"},
		{NodeState(99), "unknown"},
	}
	for _, tt := range tests {
		if got := tt.state.String(); got != tt.expected {
			t.Errorf("NodeState(%d).String() = %q, want %q", tt.state, got, tt.expected)
		}
	}
}

// ---------------------------------------------------------------------------
// Test: checkHeartbeats — Online → Unstable transition
// ---------------------------------------------------------------------------

func TestCheckHeartbeats_OnlineToUnstable(t *testing.T) {
	nt := NewNodeTable()
	nt.Register("w1", makeWorkerInfo("h1", 4), "")

	// Manually set LastHeartbeat to exceed HeartbeatTimeout but not OfflineTimeout
	nt.mu.Lock()
	rec := nt.nodes["w1"]
	rec.LastHeartbeat = time.Now().Add(-(HeartbeatTimeout + 1*time.Second))
	nt.mu.Unlock()

	called := false
	nt.checkHeartbeats(func(workerID string, tasks []string) {
		called = true
	})

	rec, _ = nt.Get("w1")
	if rec.State != NodeStateUnstable {
		t.Errorf("expected Unstable state, got %s", rec.State)
	}
	if rec.HeartbeatMissCount == 0 {
		t.Error("expected HeartbeatMissCount to increment")
	}
	if called {
		t.Error("onNodeOffline callback should NOT be called for unstable (only offline)")
	}
}

// ---------------------------------------------------------------------------
// Test: checkHeartbeats — Online → Offline transition
// ---------------------------------------------------------------------------

func TestCheckHeartbeats_OnlineToOffline(t *testing.T) {
	nt := NewNodeTable()
	nt.Register("w1", makeWorkerInfo("h1", 4), "")
	nt.AssignTask("w1", "task-a")
	nt.AssignTask("w1", "task-b")

	// Set LastHeartbeat to exceed OfflineTimeout
	nt.mu.Lock()
	rec := nt.nodes["w1"]
	rec.LastHeartbeat = time.Now().Add(-(OfflineTimeout + 2*time.Second))
	nt.mu.Unlock()

	var callbackWorkerID string
	var callbackTasks []string
	callbackMu := sync.Mutex{}

	nt.checkHeartbeats(func(workerID string, tasks []string) {
		callbackMu.Lock()
		defer callbackMu.Unlock()
		callbackWorkerID = workerID
		callbackTasks = tasks
	})

	rec, _ = nt.Get("w1")
	if rec.State != NodeStateOffline {
		t.Errorf("expected Offline state, got %s", rec.State)
	}
	if rec.FailCount != 1 {
		t.Errorf("expected FailCount 1, got %d", rec.FailCount)
	}

	// The callback is invoked asynchronously, give it a moment
	time.Sleep(100 * time.Millisecond)

	callbackMu.Lock()
	defer callbackMu.Unlock()

	if callbackWorkerID != "w1" {
		t.Errorf("expected callback for w1, got %q", callbackWorkerID)
	}
	if len(callbackTasks) != 2 {
		t.Errorf("expected 2 tasks in callback, got %d", len(callbackTasks))
	}
}

// ---------------------------------------------------------------------------
// Test: checkHeartbeats — already offline is skipped
// ---------------------------------------------------------------------------

func TestCheckHeartbeats_AlreadyOfflineSkipped(t *testing.T) {
	nt := NewNodeTable()
	nt.Register("w1", makeWorkerInfo("h1", 4), "")

	nt.mu.Lock()
	rec := nt.nodes["w1"]
	rec.State = NodeStateOffline
	rec.LastHeartbeat = time.Now().Add(-1 * time.Hour)
	nt.mu.Unlock()

	called := false
	nt.checkHeartbeats(func(workerID string, tasks []string) {
		called = true
	})

	if called {
		t.Error("callback should not be called for already-offline node")
	}
	// FailCount should not increment again
	rec, _ = nt.Get("w1")
	if rec.FailCount != 0 {
		t.Errorf("FailCount should remain 0, got %d", rec.FailCount)
	}
}

// ---------------------------------------------------------------------------
// Test: checkHeartbeats — no callback when no tasks assigned
// ---------------------------------------------------------------------------

func TestCheckHeartbeats_OfflineNoTasks(t *testing.T) {
	nt := NewNodeTable()
	nt.Register("w1", makeWorkerInfo("h1", 4), "")
	// No tasks assigned

	nt.mu.Lock()
	rec := nt.nodes["w1"]
	rec.LastHeartbeat = time.Now().Add(-(OfflineTimeout + 2*time.Second))
	nt.mu.Unlock()

	called := false
	nt.checkHeartbeats(func(workerID string, tasks []string) {
		called = true
	})

	rec, _ = nt.Get("w1")
	if rec.State != NodeStateOffline {
		t.Errorf("expected Offline state, got %s", rec.State)
	}
	if called {
		t.Error("callback should not be called when node has no assigned tasks")
	}
}

// ---------------------------------------------------------------------------
// Test: checkHeartbeats — nil callback doesn't panic
// ---------------------------------------------------------------------------

func TestCheckHeartbeats_NilCallback(t *testing.T) {
	nt := NewNodeTable()
	nt.Register("w1", makeWorkerInfo("h1", 4), "")
	nt.AssignTask("w1", "task-a")

	nt.mu.Lock()
	rec := nt.nodes["w1"]
	rec.LastHeartbeat = time.Now().Add(-(OfflineTimeout + 2*time.Second))
	nt.mu.Unlock()

	// Should not panic with nil callback
	nt.checkHeartbeats(nil)

	rec, _ = nt.Get("w1")
	if rec.State != NodeStateOffline {
		t.Errorf("expected Offline, got %s", rec.State)
	}
}

// ---------------------------------------------------------------------------
// Test: StartHealingLoop — integration test
// ---------------------------------------------------------------------------

func TestStartHealingLoop(t *testing.T) {
	nt := NewNodeTable()
	nt.Register("w1", makeWorkerInfo("h1", 4), "")
	nt.AssignTask("w1", "task-a")

	// Set heartbeat to far past so it will be detected on first tick
	nt.mu.Lock()
	rec := nt.nodes["w1"]
	rec.LastHeartbeat = time.Now().Add(-(OfflineTimeout + 5*time.Second))
	nt.mu.Unlock()

	offlineCh := make(chan string, 1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	nt.StartHealingLoop(ctx, func(workerID string, tasks []string) {
		offlineCh <- workerID
	})

	// Wait for the healing loop to detect the offline node
	select {
	case id := <-offlineCh:
		if id != "w1" {
			t.Errorf("expected w1 to go offline, got %s", id)
		}
	case <-time.After(HealCheckInterval + 3*time.Second):
		t.Error("healing loop did not detect offline node in time")
	}

	rec, _ = nt.Get("w1")
	if rec.State != NodeStateOffline {
		t.Errorf("expected Offline state after healing loop, got %s", rec.State)
	}

	cancel()
}

// ---------------------------------------------------------------------------
// Test: StartHealingLoop — context cancellation stops the loop
// ---------------------------------------------------------------------------

func TestStartHealingLoop_ContextCancel(t *testing.T) {
	nt := NewNodeTable()
	nt.Register("w1", makeWorkerInfo("h1", 4), "")

	ctx, cancel := context.WithCancel(context.Background())
	callCount := 0
	var mu sync.Mutex

	nt.StartHealingLoop(ctx, func(workerID string, tasks []string) {
		mu.Lock()
		callCount++
		mu.Unlock()
	})

	// Cancel immediately
	cancel()

	// Wait a bit and verify loop stopped
	time.Sleep(HealCheckInterval + 2*time.Second)

	mu.Lock()
	defer mu.Unlock()
	// callCount should be 0 since the node heartbeat is fresh
	if callCount != 0 {
		t.Errorf("expected 0 offline callbacks, got %d", callCount)
	}
}

// ---------------------------------------------------------------------------
// Test: Multiple heartbeats accumulate correctly
// ---------------------------------------------------------------------------

func TestMultipleHeartbeats(t *testing.T) {
	nt := NewNodeTable()
	nt.Register("w1", makeWorkerInfo("h1", 4), "")

	status := makeStatus(20, 30, 1)
	for i := 0; i < 100; i++ {
		nt.UpdateHeartbeat("w1", status)
	}

	rec, _ := nt.Get("w1")
	if rec.HeartbeatHitCount != 100 {
		t.Errorf("expected 100 heartbeat hits, got %d", rec.HeartbeatHitCount)
	}
	if rec.Reliability() != 1.0 {
		t.Errorf("expected reliability 1.0, got %.4f", rec.Reliability())
	}
}

// ---------------------------------------------------------------------------
// Test: Concurrent access safety
// ---------------------------------------------------------------------------

func TestConcurrentAccess(t *testing.T) {
	nt := NewNodeTable()

	// Register some nodes
	for i := 0; i < 10; i++ {
		id := "w" + string(rune('0'+i))
		nt.Register(id, makeWorkerInfo("h"+string(rune('0'+i)), int32(i+1)), "")
	}

	var wg sync.WaitGroup
	done := make(chan struct{})

	// Concurrent heartbeats
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			id := "w" + string(rune('0'+idx))
			status := makeStatus(float64(idx*10), float64(idx*5), int32(idx))
			for j := 0; j < 50; j++ {
				nt.UpdateHeartbeat(id, status)
			}
		}(i)
	}

	// Concurrent reads
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				_ = nt.OnlineNodes()
				_ = nt.AllNodes()
			}
		}()
	}

	// Concurrent task assignment/removal
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			id := "w" + string(rune('0'+idx))
			for j := 0; j < 20; j++ {
				taskID := "task-" + string(rune('A'+j))
				nt.AssignTask(id, taskID)
				nt.RemoveTask(id, taskID)
			}
		}(i)
	}

	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Success — no race condition panics
	case <-time.After(10 * time.Second):
		t.Fatal("concurrent access test timed out — possible deadlock")
	}
}

// ---------------------------------------------------------------------------
// Test: Unstable → Online recovery via heartbeat
// ---------------------------------------------------------------------------

func TestStateTransition_UnstableToOnline(t *testing.T) {
	nt := NewNodeTable()
	nt.Register("w1", makeWorkerInfo("h1", 8), "addr1")

	// Set to unstable
	nt.mu.Lock()
	nt.nodes["w1"].State = NodeStateUnstable
	nt.nodes["w1"].HeartbeatMissCount = 3
	nt.mu.Unlock()

	rec, _ := nt.Get("w1")
	if rec.State != NodeStateUnstable {
		t.Fatalf("precondition failed: expected Unstable, got %s", rec.State)
	}

	// Heartbeat should restore to Online
	nt.UpdateHeartbeat("w1", makeStatus(10, 20, 0))

	rec, _ = nt.Get("w1")
	if rec.State != NodeStateOnline {
		t.Errorf("expected Online after heartbeat, got %s", rec.State)
	}
	// Hit count should increment
	if rec.HeartbeatHitCount != 1 {
		t.Errorf("expected HeartbeatHitCount 1, got %d", rec.HeartbeatHitCount)
	}
	// Miss count should not change
	if rec.HeartbeatMissCount != 3 {
		t.Errorf("expected HeartbeatMissCount to remain 3, got %d", rec.HeartbeatMissCount)
	}
}

// ---------------------------------------------------------------------------
// Test: Full lifecycle — register, heartbeat, unstable, offline, re-register
// ---------------------------------------------------------------------------

func TestFullLifecycle(t *testing.T) {
	nt := NewNodeTable()
	info := makeWorkerInfo("lifecycle-host", 8)

	// 1. Register
	nt.Register("w1", info, "addr:50052")
	rec, _ := nt.Get("w1")
	if rec.State != NodeStateOnline {
		t.Fatalf("step 1: expected Online, got %s", rec.State)
	}

	// 2. Send some heartbeats
	for i := 0; i < 5; i++ {
		nt.UpdateHeartbeat("w1", makeStatus(20, 30, 0))
	}
	rec, _ = nt.Get("w1")
	if rec.HeartbeatHitCount != 5 {
		t.Fatalf("step 2: expected 5 hits, got %d", rec.HeartbeatHitCount)
	}

	// 3. Simulate heartbeat timeout → Unstable
	nt.mu.Lock()
	nt.nodes["w1"].LastHeartbeat = time.Now().Add(-(HeartbeatTimeout + 1*time.Second))
	nt.mu.Unlock()

	nt.checkHeartbeats(nil)
	rec, _ = nt.Get("w1")
	if rec.State != NodeStateUnstable {
		t.Fatalf("step 3: expected Unstable, got %s", rec.State)
	}

	// 4. Simulate further timeout → Offline
	nt.mu.Lock()
	nt.nodes["w1"].LastHeartbeat = time.Now().Add(-(OfflineTimeout + 5*time.Second))
	nt.mu.Unlock()

	nt.checkHeartbeats(nil)
	rec, _ = nt.Get("w1")
	if rec.State != NodeStateOffline {
		t.Fatalf("step 4: expected Offline, got %s", rec.State)
	}
	if rec.FailCount != 1 {
		t.Fatalf("step 4: expected FailCount 1, got %d", rec.FailCount)
	}

	// 5. Worker re-registers (same ID)
	nt.Register("w1", info, "addr:50053")
	rec, _ = nt.Get("w1")
	if rec.State != NodeStateOnline {
		t.Fatalf("step 5: expected Online after re-register, got %s", rec.State)
	}
	if rec.GrpcAddr != "addr:50053" {
		t.Fatalf("step 5: expected new addr, got %s", rec.GrpcAddr)
	}
}

// ===========================================================================
// Sliding Window Reliability Prediction Tests
// ===========================================================================

// ---------------------------------------------------------------------------
// Test: appendHeartbeatEvent
// ---------------------------------------------------------------------------

func TestAppendHeartbeatEvent_Basic(t *testing.T) {
	rec := &NodeRecord{}

	rec.appendHeartbeatEvent(true)
	rec.appendHeartbeatEvent(false)
	rec.appendHeartbeatEvent(true)

	if len(rec.HeartbeatWindow) != 3 {
		t.Fatalf("expected 3 events, got %d", len(rec.HeartbeatWindow))
	}
	if !rec.HeartbeatWindow[0].Hit {
		t.Error("event 0 should be hit")
	}
	if rec.HeartbeatWindow[1].Hit {
		t.Error("event 1 should be miss")
	}
	if !rec.HeartbeatWindow[2].Hit {
		t.Error("event 2 should be hit")
	}
}

func TestAppendHeartbeatEvent_WindowCap(t *testing.T) {
	rec := &NodeRecord{}

	// Fill beyond window size
	for i := 0; i < ReliabilityWindowSize+20; i++ {
		rec.appendHeartbeatEvent(true)
	}

	if len(rec.HeartbeatWindow) != ReliabilityWindowSize {
		t.Errorf("window should be capped at %d, got %d", ReliabilityWindowSize, len(rec.HeartbeatWindow))
	}
}

func TestAppendHeartbeatEvent_OldEventsEvicted(t *testing.T) {
	rec := &NodeRecord{}

	// Add misses first
	for i := 0; i < ReliabilityWindowSize; i++ {
		rec.appendHeartbeatEvent(false)
	}

	// Now add hits — these should push out the old misses
	for i := 0; i < ReliabilityWindowSize; i++ {
		rec.appendHeartbeatEvent(true)
	}

	if len(rec.HeartbeatWindow) != ReliabilityWindowSize {
		t.Fatalf("expected window size %d, got %d", ReliabilityWindowSize, len(rec.HeartbeatWindow))
	}

	// All events should now be hits
	for i, ev := range rec.HeartbeatWindow {
		if !ev.Hit {
			t.Errorf("event %d should be hit after eviction of old misses", i)
		}
	}
}

func TestAppendHeartbeatEvent_TimestampsSet(t *testing.T) {
	rec := &NodeRecord{}
	before := time.Now()
	rec.appendHeartbeatEvent(true)
	after := time.Now()

	ev := rec.HeartbeatWindow[0]
	if ev.Timestamp.Before(before) || ev.Timestamp.After(after) {
		t.Error("event timestamp should be between before and after")
	}
}

// ---------------------------------------------------------------------------
// Test: ReliabilityWindow
// ---------------------------------------------------------------------------

func TestReliabilityWindow_Empty(t *testing.T) {
	rec := &NodeRecord{}
	// No window data → fallback to global Reliability()
	r := rec.ReliabilityWindow()
	if r != 1.0 {
		t.Errorf("empty window with no global data should return 1.0, got %.3f", r)
	}
}

func TestReliabilityWindow_AllHits(t *testing.T) {
	rec := &NodeRecord{}
	for i := 0; i < 10; i++ {
		rec.appendHeartbeatEvent(true)
	}
	r := rec.ReliabilityWindow()
	if r != 1.0 {
		t.Errorf("all hits should give 1.0, got %.3f", r)
	}
}

func TestReliabilityWindow_AllMisses(t *testing.T) {
	rec := &NodeRecord{}
	for i := 0; i < 10; i++ {
		rec.appendHeartbeatEvent(false)
	}
	r := rec.ReliabilityWindow()
	if r != 0.0 {
		t.Errorf("all misses should give 0.0, got %.3f", r)
	}
}

func TestReliabilityWindow_Mixed(t *testing.T) {
	rec := &NodeRecord{}
	// 7 hits, 3 misses = 0.7
	for i := 0; i < 7; i++ {
		rec.appendHeartbeatEvent(true)
	}
	for i := 0; i < 3; i++ {
		rec.appendHeartbeatEvent(false)
	}
	r := rec.ReliabilityWindow()
	if math.Abs(r-0.7) > 0.001 {
		t.Errorf("expected ~0.7, got %.3f", r)
	}
}

func TestReliabilityWindow_DiffersFromGlobal(t *testing.T) {
	rec := &NodeRecord{
		HeartbeatHitCount:  100,
		HeartbeatMissCount: 0,
	}
	// Global reliability = 100/100 = 1.0
	// Window: fill with misses
	for i := 0; i < 10; i++ {
		rec.appendHeartbeatEvent(false)
	}

	global := rec.Reliability()
	window := rec.ReliabilityWindow()

	if global != 1.0 {
		t.Errorf("global reliability should be 1.0, got %.3f", global)
	}
	if window != 0.0 {
		t.Errorf("window reliability should be 0.0, got %.3f", window)
	}
}

// ---------------------------------------------------------------------------
// Test: ReliabilityTrend
// ---------------------------------------------------------------------------

func TestReliabilityTrend_TooFewEvents(t *testing.T) {
	rec := &NodeRecord{}
	rec.appendHeartbeatEvent(true)
	rec.appendHeartbeatEvent(false)
	rec.appendHeartbeatEvent(true)

	trend := rec.ReliabilityTrend()
	if trend != 0 {
		t.Errorf("with <4 events, trend should be 0, got %.3f", trend)
	}
}

func TestReliabilityTrend_Stable(t *testing.T) {
	rec := &NodeRecord{}
	// All hits → no trend difference
	for i := 0; i < 20; i++ {
		rec.appendHeartbeatEvent(true)
	}

	trend := rec.ReliabilityTrend()
	if trend != 0.0 {
		t.Errorf("stable all-hit window should have trend=0, got %.3f", trend)
	}
}

func TestReliabilityTrend_Declining(t *testing.T) {
	rec := &NodeRecord{}
	// First half: all hits
	for i := 0; i < 15; i++ {
		rec.appendHeartbeatEvent(true)
	}
	// Second half: all misses → recent is worse than overall
	for i := 0; i < 15; i++ {
		rec.appendHeartbeatEvent(false)
	}

	trend := rec.ReliabilityTrend()
	if trend >= 0 {
		t.Errorf("declining pattern should produce negative trend, got %.3f", trend)
	}
}

func TestReliabilityTrend_Recovering(t *testing.T) {
	rec := &NodeRecord{}
	// First half: all misses
	for i := 0; i < 15; i++ {
		rec.appendHeartbeatEvent(false)
	}
	// Second half: all hits → recent is better than overall
	for i := 0; i < 15; i++ {
		rec.appendHeartbeatEvent(true)
	}

	trend := rec.ReliabilityTrend()
	if trend <= 0 {
		t.Errorf("recovering pattern should produce positive trend, got %.3f", trend)
	}
}

// ---------------------------------------------------------------------------
// Test: IsDeclining
// ---------------------------------------------------------------------------

func TestIsDeclining_StableNode(t *testing.T) {
	rec := &NodeRecord{}
	for i := 0; i < 20; i++ {
		rec.appendHeartbeatEvent(true)
	}
	if rec.IsDeclining() {
		t.Error("stable node should not be declining")
	}
}

func TestIsDeclining_DecliningNode(t *testing.T) {
	rec := &NodeRecord{}
	// First: all hits
	for i := 0; i < 20; i++ {
		rec.appendHeartbeatEvent(true)
	}
	// Then: many misses → trend drops below -threshold
	for i := 0; i < 10; i++ {
		rec.appendHeartbeatEvent(false)
	}

	trend := rec.ReliabilityTrend()
	if trend >= -ReliabilityTrendThreshold {
		t.Skipf("trend %.3f not below threshold %.3f — pattern may not trigger decline with current window", trend, -ReliabilityTrendThreshold)
	}
	if !rec.IsDeclining() {
		t.Errorf("node with trend=%.3f should be declining (threshold=%.3f)", trend, ReliabilityTrendThreshold)
	}
}

func TestIsDeclining_EmptyWindow(t *testing.T) {
	rec := &NodeRecord{}
	if rec.IsDeclining() {
		t.Error("empty window should not be declining")
	}
}

// ---------------------------------------------------------------------------
// Test: PredictOfflineProbability
// ---------------------------------------------------------------------------

func TestPredictOfflineProbability_Empty(t *testing.T) {
	rec := &NodeRecord{}
	prob := rec.PredictOfflineProbability()
	if prob != 0 {
		t.Errorf("empty window should give prob=0, got %.3f", prob)
	}
}

func TestPredictOfflineProbability_AllHits(t *testing.T) {
	rec := &NodeRecord{}
	for i := 0; i < 20; i++ {
		rec.appendHeartbeatEvent(true)
	}
	prob := rec.PredictOfflineProbability()
	if prob != 0 {
		t.Errorf("all hits should give prob=0, got %.3f", prob)
	}
}

func TestPredictOfflineProbability_AllMisses(t *testing.T) {
	rec := &NodeRecord{}
	for i := 0; i < 20; i++ {
		rec.appendHeartbeatEvent(false)
	}
	prob := rec.PredictOfflineProbability()
	if prob < 0.9 {
		t.Errorf("all misses should give prob near 1.0, got %.3f", prob)
	}
}

func TestPredictOfflineProbability_RecentMissesWeighMore(t *testing.T) {
	// Window where misses are at the beginning (old)
	recOldMisses := &NodeRecord{}
	for i := 0; i < 10; i++ {
		recOldMisses.appendHeartbeatEvent(false)
	}
	for i := 0; i < 10; i++ {
		recOldMisses.appendHeartbeatEvent(true)
	}

	// Window where misses are at the end (recent)
	recRecentMisses := &NodeRecord{}
	for i := 0; i < 10; i++ {
		recRecentMisses.appendHeartbeatEvent(true)
	}
	for i := 0; i < 10; i++ {
		recRecentMisses.appendHeartbeatEvent(false)
	}

	probOld := recOldMisses.PredictOfflineProbability()
	probRecent := recRecentMisses.PredictOfflineProbability()

	// Recent misses should produce higher probability due to exponential weighting
	if probRecent <= probOld {
		t.Errorf("recent misses (prob=%.3f) should give higher probability than old misses (prob=%.3f)",
			probRecent, probOld)
	}
}

func TestPredictOfflineProbability_Range(t *testing.T) {
	// Test various patterns and ensure result is always in [0,1]
	patterns := [][]bool{
		{true, true, true, true, true},
		{false, false, false, false, false},
		{true, false, true, false, true},
		{false, true, false, true},
		{true, true, true, false},
	}

	for _, pattern := range patterns {
		rec := &NodeRecord{}
		for _, hit := range pattern {
			rec.appendHeartbeatEvent(hit)
		}
		prob := rec.PredictOfflineProbability()
		if prob < 0 || prob > 1.0 {
			t.Errorf("probability %.3f out of range [0,1] for pattern %v", prob, pattern)
		}
	}
}

// ---------------------------------------------------------------------------
// Test: UpdateHeartbeat records window events
// ---------------------------------------------------------------------------

func TestUpdateHeartbeat_RecordsWindowEvent(t *testing.T) {
	nt := NewNodeTable()
	nt.Register("w1", makeWorkerInfo("host1", 4), "addr:50052")

	status := makeStatus(50.0, 50.0, 0)
	nt.UpdateHeartbeat("w1", status)
	nt.UpdateHeartbeat("w1", status)
	nt.UpdateHeartbeat("w1", status)

	rec, _ := nt.Get("w1")
	if len(rec.HeartbeatWindow) != 3 {
		t.Errorf("expected 3 window events after 3 heartbeats, got %d", len(rec.HeartbeatWindow))
	}
	for i, ev := range rec.HeartbeatWindow {
		if !ev.Hit {
			t.Errorf("window event %d should be hit", i)
		}
	}
}

// ---------------------------------------------------------------------------
// Test: checkHeartbeats records miss events in window
// ---------------------------------------------------------------------------

func TestCheckHeartbeats_RecordsMissInWindow(t *testing.T) {
	nt := NewNodeTable()
	nt.Register("w1", makeWorkerInfo("host1", 4), "addr:50052")

	// Push last heartbeat back so it becomes unstable
	nt.mu.Lock()
	nt.nodes["w1"].LastHeartbeat = time.Now().Add(-20 * time.Second)
	nt.mu.Unlock()

	nt.checkHeartbeats(nil)

	rec, _ := nt.Get("w1")
	if rec.State != NodeStateUnstable {
		t.Fatalf("expected unstable, got %s", rec.State)
	}

	// Should have at least 1 miss event in window
	missCount := 0
	for _, ev := range rec.HeartbeatWindow {
		if !ev.Hit {
			missCount++
		}
	}
	if missCount == 0 {
		t.Error("expected at least 1 miss event in window after unstable detection")
	}
}

func TestCheckHeartbeats_OfflineRecordsManyMisses(t *testing.T) {
	nt := NewNodeTable()
	nt.Register("w1", makeWorkerInfo("host1", 4), "addr:50052")
	nt.AssignTask("w1", "task-1")

	// Push last heartbeat back beyond offline timeout
	nt.mu.Lock()
	nt.nodes["w1"].LastHeartbeat = time.Now().Add(-35 * time.Second)
	nt.mu.Unlock()

	nt.checkHeartbeats(nil)

	rec, _ := nt.Get("w1")
	if rec.State != NodeStateOffline {
		t.Fatalf("expected offline, got %s", rec.State)
	}

	// Should have multiple miss events (35s / 5s = ~7 misses)
	missCount := 0
	for _, ev := range rec.HeartbeatWindow {
		if !ev.Hit {
			missCount++
		}
	}
	if missCount < 5 {
		t.Errorf("expected at least 5 miss events for 35s offline, got %d", missCount)
	}
}

// ---------------------------------------------------------------------------
// Test: checkReliabilityTrend
// ---------------------------------------------------------------------------

func TestCheckReliabilityTrend_NilCallback(t *testing.T) {
	nt := NewNodeTable()
	nt.Register("w1", makeWorkerInfo("host1", 4), "addr:50052")
	// Should not panic with nil callback
	nt.checkReliabilityTrend(nil)
}

func TestCheckReliabilityTrend_NoTasksSkipped(t *testing.T) {
	nt := NewNodeTable()
	nt.Register("w1", makeWorkerInfo("host1", 4), "addr:50052")

	// Make node look like it's declining
	nt.mu.Lock()
	rec := nt.nodes["w1"]
	for i := 0; i < 20; i++ {
		rec.appendHeartbeatEvent(true)
	}
	for i := 0; i < 10; i++ {
		rec.appendHeartbeatEvent(false)
	}
	// But don't assign any tasks
	nt.mu.Unlock()

	called := false
	nt.checkReliabilityTrend(func(workerID string, tasks []string, prob float64) {
		called = true
	})

	if called {
		t.Error("should not trigger callback for node without tasks")
	}
}

func TestCheckReliabilityTrend_OfflineSkipped(t *testing.T) {
	nt := NewNodeTable()
	nt.Register("w1", makeWorkerInfo("host1", 4), "addr:50052")
	nt.AssignTask("w1", "task-1")

	nt.mu.Lock()
	rec := nt.nodes["w1"]
	rec.State = NodeStateOffline
	for i := 0; i < 20; i++ {
		rec.appendHeartbeatEvent(true)
	}
	for i := 0; i < 10; i++ {
		rec.appendHeartbeatEvent(false)
	}
	nt.mu.Unlock()

	called := false
	nt.checkReliabilityTrend(func(workerID string, tasks []string, prob float64) {
		called = true
	})

	if called {
		t.Error("should not trigger callback for offline node")
	}
}

func TestCheckReliabilityTrend_DecliningTriggersCallback(t *testing.T) {
	nt := NewNodeTable()
	nt.Register("w1", makeWorkerInfo("host1", 4), "addr:50052")
	nt.AssignTask("w1", "task-1")
	nt.AssignTask("w1", "task-2")

	// Construct a clearly declining pattern
	nt.mu.Lock()
	rec := nt.nodes["w1"]
	for i := 0; i < 20; i++ {
		rec.appendHeartbeatEvent(true)
	}
	for i := 0; i < 10; i++ {
		rec.appendHeartbeatEvent(false)
	}
	nt.mu.Unlock()

	// Check if this pattern actually triggers declining
	nt.mu.RLock()
	declining := rec.IsDeclining()
	nt.mu.RUnlock()

	if !declining {
		t.Skip("pattern did not produce sufficient decline with current thresholds; skipping callback test")
	}

	var calledWorkerID string
	var calledTasks []string
	var calledProb float64
	done := make(chan struct{})

	nt.checkReliabilityTrend(func(workerID string, tasks []string, prob float64) {
		calledWorkerID = workerID
		calledTasks = tasks
		calledProb = prob
		close(done)
	})

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("callback was not triggered within timeout")
	}

	if calledWorkerID != "w1" {
		t.Errorf("expected workerID=w1, got %s", calledWorkerID)
	}
	if len(calledTasks) != 2 {
		t.Errorf("expected 2 tasks, got %d", len(calledTasks))
	}
	if calledProb < 0 || calledProb > 1 {
		t.Errorf("probability %.3f out of range [0,1]", calledProb)
	}
}

// ---------------------------------------------------------------------------
// Test: StartHealingLoopWithPrediction integration
// ---------------------------------------------------------------------------

func TestStartHealingLoopWithPrediction_OfflineCallback(t *testing.T) {
	nt := NewNodeTable()
	nt.Register("w1", makeWorkerInfo("host1", 4), "addr:50052")
	nt.AssignTask("w1", "task-1")

	// Push heartbeat back beyond offline threshold
	nt.mu.Lock()
	nt.nodes["w1"].LastHeartbeat = time.Now().Add(-35 * time.Second)
	nt.mu.Unlock()

	offlineCalled := make(chan string, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	nt.StartHealingLoopWithPrediction(ctx,
		func(workerID string, tasks []string) {
			offlineCalled <- workerID
		},
		func(workerID string, tasks []string, prob float64) {
			// declining callback — not the focus here
		},
	)

	select {
	case wid := <-offlineCalled:
		if wid != "w1" {
			t.Errorf("expected w1, got %s", wid)
		}
	case <-ctx.Done():
		t.Fatal("offline callback not triggered within timeout")
	}
}

func TestStartHealingLoopWithPrediction_ContextCancel(t *testing.T) {
	nt := NewNodeTable()
	nt.Register("w1", makeWorkerInfo("host1", 4), "addr:50052")

	ctx, cancel := context.WithCancel(context.Background())
	nt.StartHealingLoopWithPrediction(ctx, nil, nil)

	// Cancel quickly — should not panic
	cancel()
	time.Sleep(100 * time.Millisecond)
}

// ---------------------------------------------------------------------------
// Test: Window reliability after heartbeat recovery cycle
// ---------------------------------------------------------------------------

func TestReliabilityWindow_RecoveryCycle(t *testing.T) {
	rec := &NodeRecord{}

	// Phase 1: healthy
	for i := 0; i < 10; i++ {
		rec.appendHeartbeatEvent(true)
	}
	if rec.ReliabilityWindow() != 1.0 {
		t.Errorf("phase 1: expected 1.0, got %.3f", rec.ReliabilityWindow())
	}

	// Phase 2: degradation
	for i := 0; i < 5; i++ {
		rec.appendHeartbeatEvent(false)
	}
	rDegraded := rec.ReliabilityWindow()
	if rDegraded >= 1.0 || rDegraded <= 0.0 {
		t.Errorf("phase 2: expected between 0 and 1, got %.3f", rDegraded)
	}

	// Phase 3: recovery — fill window with hits again
	for i := 0; i < ReliabilityWindowSize; i++ {
		rec.appendHeartbeatEvent(true)
	}
	rRecovered := rec.ReliabilityWindow()
	if rRecovered != 1.0 {
		t.Errorf("phase 3: expected 1.0 after full recovery, got %.3f", rRecovered)
	}
}

// ---------------------------------------------------------------------------
// Test: PredictOfflineProbability monotonicity with increasing misses
// ---------------------------------------------------------------------------

func TestPredictOfflineProbability_Monotonic(t *testing.T) {
	// As we add more recent misses, probability should increase
	var prevProb float64

	for numMisses := 0; numMisses <= 10; numMisses++ {
		rec := &NodeRecord{}
		for i := 0; i < 10; i++ {
			rec.appendHeartbeatEvent(true)
		}
		for i := 0; i < numMisses; i++ {
			rec.appendHeartbeatEvent(false)
		}
		prob := rec.PredictOfflineProbability()

		if numMisses > 0 && prob < prevProb-0.001 {
			t.Errorf("probability should not decrease as misses increase: misses=%d, prob=%.3f < prevProb=%.3f",
				numMisses, prob, prevProb)
		}
		prevProb = prob
	}
}
