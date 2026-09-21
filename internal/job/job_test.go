package job

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func waitState(t *testing.T, owner, id string, terminal bool) Snapshot {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		snap, err := Status(owner, id)
		if err != nil {
			t.Fatalf("status %s: %v", id, err)
		}
		if terminal {
			switch snap.State {
			case "succeeded", "failed", "canceled", "timed_out", "blocked":
				return snap
			}
		} else if snap.State == "running" {
			return snap
		}
		time.Sleep(5 * time.Millisecond)
	}
	snap, _ := Status(owner, id)
	t.Fatalf("timed out waiting for job %s terminal=%v; last=%+v", id, terminal, snap)
	return Snapshot{}
}

func testConfig(t *testing.T, maxActive, maxQueued, perOwner, maxCPU int) Config {
	t.Helper()
	return Config{
		DataDir:           t.TempDir(),
		MaxActive:         maxActive,
		MaxQueued:         maxQueued,
		MaxQueuedPerOwner: perOwner,
		MaxCPUHeavy:       maxCPU,
		ResultMaxBytes:    4096,
		DefaultTimeout:    3 * time.Second,
		MaxTimeout:        5 * time.Second,
		Retention:         time.Hour,
	}
}

func TestCPUHeavyCapAndDiskBackedResult(t *testing.T) {
	Init(testConfig(t, 3, 12, 8, 1))
	defer Shutdown()

	cpu1, err := Submit("a", SubmitArgs{Command: "sleep 0.20; echo cpu1", Class: ClassCPUHeavy})
	if err != nil {
		t.Fatal(err)
	}
	cpu2, err := Submit("b", SubmitArgs{Command: "sleep 0.05; echo cpu2", Class: ClassCPUHeavy})
	if err != nil {
		t.Fatal(err)
	}
	normal, err := Submit("c", SubmitArgs{Command: "sleep 0.05; echo normal", Class: ClassNormal})
	if err != nil {
		t.Fatal(err)
	}

	waitState(t, "a", cpu1.JobID, false)

	deadline := time.Now().Add(150 * time.Millisecond)
	for time.Now().Before(deadline) {
		_, cap := List("a")
		if cap.CPUHeavy > 1 {
			t.Fatalf("cpu-heavy cap violated: %+v", cap)
		}
		time.Sleep(5 * time.Millisecond)
	}

	for owner, id := range map[string]string{"a": cpu1.JobID, "b": cpu2.JobID, "c": normal.JobID} {
		got := waitState(t, owner, id, true)
		if got.State != "succeeded" {
			t.Fatalf("job %s state=%s err=%s", id, got.State, got.Error)
		}
	}

	res, err := ResultFor("b", cpu2.JobID, 4096)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(res.Output, "cpu2") {
		t.Fatalf("missing disk-backed output: %+v", res)
	}
}

func TestPerOwnerQueueBoundWithoutGlobalStarvation(t *testing.T) {
	Init(testConfig(t, 1, 8, 2, 1))
	defer Shutdown()

	blocker, err := Submit("a", SubmitArgs{Command: "sleep 0.30"})
	if err != nil {
		t.Fatal(err)
	}
	waitState(t, "a", blocker.JobID, false)

	if _, err := Submit("a", SubmitArgs{Command: "echo a2"}); err != nil {
		t.Fatal(err)
	}
	if _, err := Submit("a", SubmitArgs{Command: "echo a3"}); err != nil {
		t.Fatal(err)
	}
	if _, err := Submit("a", SubmitArgs{Command: "echo rejected"}); err == nil {
		t.Fatal("expected per-owner queue rejection")
	}
	if _, err := Submit("b", SubmitArgs{Command: "echo b1"}); err != nil {
		t.Fatalf("another owner should still be admitted: %v", err)
	}
}

func TestRoundRobinAcrossOwners(t *testing.T) {
	Init(testConfig(t, 1, 10, 8, 1))
	defer Shutdown()

	a1, err := Submit("a", SubmitArgs{Command: "sleep 0.20; echo a1"})
	if err != nil {
		t.Fatal(err)
	}
	waitState(t, "a", a1.JobID, false)

	a2, err := Submit("a", SubmitArgs{Command: "sleep 0.05; echo a2"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Submit("a", SubmitArgs{Command: "echo a3"}); err != nil {
		t.Fatal(err)
	}
	b1, err := Submit("b", SubmitArgs{Command: "echo b1"})
	if err != nil {
		t.Fatal(err)
	}

	bDone := waitState(t, "b", b1.JobID, true)
	a2Done := waitState(t, "a", a2.JobID, true)
	if bDone.State != "succeeded" || a2Done.State != "succeeded" {
		t.Fatalf("unexpected states b=%s a2=%s", bDone.State, a2Done.State)
	}
	bt, err := time.Parse(time.RFC3339Nano, bDone.StartedAt)
	if err != nil {
		t.Fatal(err)
	}
	at, err := time.Parse(time.RFC3339Nano, a2Done.StartedAt)
	if err != nil {
		t.Fatal(err)
	}
	if !bt.Before(at) {
		t.Fatalf("round-robin fairness failed: b started %s, a2 started %s", bt, at)
	}
}

func TestSupersessionCancelsStaleRunningWork(t *testing.T) {
	Init(testConfig(t, 1, 8, 8, 1))
	defer Shutdown()

	old, err := Submit("a", SubmitArgs{
		Command:      "sleep 2; echo stale",
		SupersedeKey: "build",
	})
	if err != nil {
		t.Fatal(err)
	}
	waitState(t, "a", old.JobID, false)

	fresh, err := Submit("a", SubmitArgs{
		Command:      "echo fresh",
		SupersedeKey: "build",
	})
	if err != nil {
		t.Fatal(err)
	}

	oldDone := waitState(t, "a", old.JobID, true)
	freshDone := waitState(t, "a", fresh.JobID, true)
	if oldDone.State != "canceled" {
		t.Fatalf("stale job state=%s err=%s", oldDone.State, oldDone.Error)
	}
	if freshDone.State != "succeeded" {
		t.Fatalf("fresh job state=%s err=%s", freshDone.State, freshDone.Error)
	}

	res, err := ResultFor("a", fresh.JobID, 4096)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(res.Output, "fresh") {
		t.Fatalf("fresh output missing: %+v", res)
	}
}

func TestFiftyLogicalProducersStayPhysicallyBounded(t *testing.T) {
	Init(testConfig(t, 4, 64, 2, 2))
	defer Shutdown()

	type ownedJob struct {
		owner string
		id    string
	}
	jobs := make([]ownedJob, 0, 50)
	for i := 0; i < 50; i++ {
		owner := "owner-" + time.Unix(int64(i), 0).UTC().Format("150405")
		class := ClassNormal
		if i%3 == 0 {
			class = ClassCPUHeavy
		}
		snap, err := Submit(owner, SubmitArgs{
			Command: "sleep 0.08; echo done",
			Class:   class,
		})
		if err != nil {
			t.Fatalf("submit %d: %v", i, err)
		}
		jobs = append(jobs, ownedJob{owner: owner, id: snap.JobID})
	}

	peakActive := 0
	peakCPU := 0
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		_, cap := List(jobs[0].owner)
		if cap.Active > peakActive {
			peakActive = cap.Active
		}
		if cap.CPUHeavy > peakCPU {
			peakCPU = cap.CPUHeavy
		}
		if cap.Active > 4 {
			t.Fatalf("physical active cap escaped: %+v", cap)
		}
		if cap.CPUHeavy > 2 {
			t.Fatalf("cpu-heavy cap escaped: %+v", cap)
		}

		complete := 0
		for _, j := range jobs {
			snap, err := Status(j.owner, j.id)
			if err != nil {
				t.Fatal(err)
			}
			switch snap.State {
			case "succeeded":
				complete++
			case "failed", "canceled", "timed_out":
				t.Fatalf("job %s unexpectedly ended %s: %s", j.id, snap.State, snap.Error)
			}
		}
		if complete == len(jobs) {
			if peakActive < 2 {
				t.Fatalf("scheduler never exercised useful concurrency; peak=%d", peakActive)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("50-producer workload did not drain; peak active=%d cpu=%d", peakActive, peakCPU)
}

func TestAdaptivePressurePolicy(t *testing.T) {
	base := Pressure{
		EffectiveCPU:         3,
		MemoryTotalBytes:     8 << 30,
		MemoryAvailableBytes: 5 << 30,
	}
	if got := targetForPressure(6, base); got != 5 {
		t.Fatalf("low-pressure target=%d, want 5", got)
	}

	cpuHot := base
	cpuHot.CPUSomeAvg10 = 75
	if got := targetForPressure(6, cpuHot); got != 1 {
		t.Fatalf("high-CPU target=%d, want 1", got)
	}

	memTight := base
	memTight.MemoryAvailableBytes = 1 << 30
	if got := targetForPressure(6, memTight); got != 2 {
		t.Fatalf("memory-tight target=%d, want 2", got)
	}

	ioHot := base
	ioHot.IOFullAvg10 = 25
	if got := targetForPressure(6, ioHot); got != 3 {
		t.Fatalf("I/O-pressure target=%d, want 3", got)
	}

	bgOK := base
	if !classAllowedByPressure(ClassBackground, bgOK) {
		t.Fatal("background should be allowed under low pressure")
	}
	bgHot := base
	bgHot.IOFullAvg10 = 15
	if classAllowedByPressure(ClassBackground, bgHot) {
		t.Fatal("background should yield under I/O pressure")
	}
}

func TestIdempotencyDeduplicatesAndRejectsDrift(t *testing.T) {
	Init(testConfig(t, 2, 8, 8, 1))
	defer Shutdown()

	first, err := Submit("a", SubmitArgs{
		Command:        "sleep 0.10; echo once",
		IdempotencyKey: "op-1",
		LockKeys:       []string{"repo:x"},
	})
	if err != nil {
		t.Fatal(err)
	}
	second, err := Submit("a", SubmitArgs{
		Command:        "sleep 0.10; echo once",
		IdempotencyKey: "op-1",
		LockKeys:       []string{"repo:x"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if first.JobID != second.JobID {
		t.Fatalf("idempotent replay created duplicate jobs: %s vs %s", first.JobID, second.JobID)
	}

	if _, err := Submit("a", SubmitArgs{
		Command:        "echo changed",
		IdempotencyKey: "op-1",
		LockKeys:       []string{"repo:x"},
	}); err == nil {
		t.Fatal("expected changed payload under same idempotency key to be rejected")
	}

	done := waitState(t, "a", first.JobID, true)
	if done.State != "succeeded" {
		t.Fatalf("idempotent job state=%s err=%s", done.State, done.Error)
	}
}

func TestDependenciesSerializeAndPropagateFailure(t *testing.T) {
	Init(testConfig(t, 3, 12, 12, 2))
	defer Shutdown()

	dep, err := Submit("a", SubmitArgs{Command: "sleep 0.12; echo dep"})
	if err != nil {
		t.Fatal(err)
	}
	child, err := Submit("a", SubmitArgs{
		Command:   "echo child",
		DependsOn: []string{dep.JobID},
	})
	if err != nil {
		t.Fatal(err)
	}

	depDone := waitState(t, "a", dep.JobID, true)
	childDone := waitState(t, "a", child.JobID, true)
	if depDone.State != "succeeded" || childDone.State != "succeeded" {
		t.Fatalf("dependency chain states dep=%s child=%s", depDone.State, childDone.State)
	}
	depFinish, err := time.Parse(time.RFC3339Nano, depDone.FinishedAt)
	if err != nil {
		t.Fatal(err)
	}
	childStart, err := time.Parse(time.RFC3339Nano, childDone.StartedAt)
	if err != nil {
		t.Fatal(err)
	}
	if childStart.Before(depFinish) {
		t.Fatalf("child started before dependency completed: child=%s dep=%s", childStart, depFinish)
	}

	bad, err := Submit("a", SubmitArgs{Command: "exit 7"})
	if err != nil {
		t.Fatal(err)
	}
	blocked, err := Submit("a", SubmitArgs{
		Command:   "echo should-not-run",
		DependsOn: []string{bad.JobID},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := waitState(t, "a", bad.JobID, true); got.State != "failed" {
		t.Fatalf("bad dependency state=%s", got.State)
	}
	blockedDone := waitState(t, "a", blocked.JobID, true)
	if blockedDone.State != "blocked" || !strings.Contains(blockedDone.Error, bad.JobID) {
		t.Fatalf("dependent should block with dependency identity: %+v", blockedDone)
	}
}

func TestLockKeysPreventConflictingOverlap(t *testing.T) {
	Init(testConfig(t, 2, 8, 8, 2))
	defer Shutdown()

	a, err := Submit("a", SubmitArgs{
		Command:  "sleep 0.18; echo a",
		LockKeys: []string{"repo:shared"},
	})
	if err != nil {
		t.Fatal(err)
	}
	b, err := Submit("b", SubmitArgs{
		Command:  "sleep 0.18; echo b",
		LockKeys: []string{"repo:shared"},
	})
	if err != nil {
		t.Fatal(err)
	}

	aDone := waitState(t, "a", a.JobID, true)
	bDone := waitState(t, "b", b.JobID, true)
	if aDone.State != "succeeded" || bDone.State != "succeeded" {
		t.Fatalf("lock jobs states a=%s b=%s", aDone.State, bDone.State)
	}

	as, _ := time.Parse(time.RFC3339Nano, aDone.StartedAt)
	af, _ := time.Parse(time.RFC3339Nano, aDone.FinishedAt)
	bs, _ := time.Parse(time.RFC3339Nano, bDone.StartedAt)
	bf, _ := time.Parse(time.RFC3339Nano, bDone.FinishedAt)
	overlap := as.Before(bf) && bs.Before(af)
	if overlap {
		t.Fatalf("conflicting lock jobs overlapped: a=%s..%s b=%s..%s", as, af, bs, bf)
	}
}

func TestGraphDependenciesAndReplay(t *testing.T) {
	Init(testConfig(t, 3, 12, 12, 2))
	defer Shutdown()

	graph, err := SubmitGraph("a", GraphSubmitArgs{
		GraphKey: "build-graph",
		Nodes: []GraphNodeArgs{
			{NodeID: "inspect-a", Command: "sleep 0.12; echo a"},
			{NodeID: "inspect-b", Command: "sleep 0.12; echo b"},
			{NodeID: "join", Command: "echo joined", DependsOn: []string{"inspect-a", "inspect-b"}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(graph.Nodes) != 3 {
		t.Fatalf("graph nodes=%d, want 3", len(graph.Nodes))
	}

	ids := map[string]string{}
	for _, node := range graph.Nodes {
		ids[node.NodeID] = node.Job.JobID
	}
	aDone := waitState(t, "a", ids["inspect-a"], true)
	bDone := waitState(t, "a", ids["inspect-b"], true)
	joinDone := waitState(t, "a", ids["join"], true)
	if aDone.State != "succeeded" || bDone.State != "succeeded" || joinDone.State != "succeeded" {
		t.Fatalf("graph states a=%s b=%s join=%s", aDone.State, bDone.State, joinDone.State)
	}

	joinStart, _ := time.Parse(time.RFC3339Nano, joinDone.StartedAt)
	aFinish, _ := time.Parse(time.RFC3339Nano, aDone.FinishedAt)
	bFinish, _ := time.Parse(time.RFC3339Nano, bDone.FinishedAt)
	if joinStart.Before(aFinish) || joinStart.Before(bFinish) {
		t.Fatalf("join started before dependencies finished: join=%s a=%s b=%s", joinStart, aFinish, bFinish)
	}

	replay, err := SubmitGraph("a", GraphSubmitArgs{
		GraphKey: "build-graph",
		Nodes: []GraphNodeArgs{
			{NodeID: "join", Command: "echo joined", DependsOn: []string{"inspect-b", "inspect-a"}},
			{NodeID: "inspect-b", Command: "sleep 0.12; echo b"},
			{NodeID: "inspect-a", Command: "sleep 0.12; echo a"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	replayIDs := map[string]string{}
	for _, node := range replay.Nodes {
		replayIDs[node.NodeID] = node.Job.JobID
	}
	for nodeID, id := range ids {
		if replayIDs[nodeID] != id {
			t.Fatalf("graph replay duplicated node %s: %s vs %s", nodeID, id, replayIDs[nodeID])
		}
	}

	if _, err := SubmitGraph("a", GraphSubmitArgs{
		GraphKey: "build-graph",
		Nodes: []GraphNodeArgs{
			{NodeID: "inspect-a", Command: "echo CHANGED"},
			{NodeID: "inspect-b", Command: "sleep 0.12; echo b"},
			{NodeID: "join", Command: "echo joined", DependsOn: []string{"inspect-a", "inspect-b"}},
		},
	}); err == nil {
		t.Fatal("expected changed graph under same graph key to be rejected")
	}
}

func TestGraphCycleRejectsAtomically(t *testing.T) {
	Init(testConfig(t, 2, 8, 8, 1))
	defer Shutdown()

	if _, err := SubmitGraph("a", GraphSubmitArgs{
		GraphKey: "cycle",
		Nodes: []GraphNodeArgs{
			{NodeID: "a", Command: "echo a", DependsOn: []string{"b"}},
			{NodeID: "b", Command: "echo b", DependsOn: []string{"a"}},
		},
	}); err == nil {
		t.Fatal("expected cycle rejection")
	}

	jobs, cap := List("a")
	if len(jobs) != 0 || cap.Queued != 0 || cap.Active != 0 {
		t.Fatalf("cycle rejection left partial work: jobs=%d cap=%+v", len(jobs), cap)
	}
}

func TestGraphAdmissionIsAllOrNothing(t *testing.T) {
	Init(testConfig(t, 1, 3, 2, 1))
	defer Shutdown()

	blocker, err := Submit("a", SubmitArgs{Command: "sleep 0.30"})
	if err != nil {
		t.Fatal(err)
	}
	waitState(t, "a", blocker.JobID, false)

	if _, err := SubmitGraph("a", GraphSubmitArgs{
		GraphKey: "too-large",
		Nodes: []GraphNodeArgs{
			{NodeID: "x", Command: "echo x"},
			{NodeID: "y", Command: "echo y"},
			{NodeID: "z", Command: "echo z"},
		},
	}); err == nil {
		t.Fatal("expected owner queue admission rejection")
	}

	jobs, _ := List("a")
	if len(jobs) != 1 || jobs[0].JobID != blocker.JobID {
		t.Fatalf("graph admission was partial; jobs=%+v", jobs)
	}
}

func TestDetachedChildCannotOutliveSuccessfulJob(t *testing.T) {
	if !jobCgroupIsolationAvailable() {
		t.Skip("cgroup v2 job isolation unavailable")
	}
	Init(testConfig(t, 2, 8, 8, 1))
	defer Shutdown()

	job, err := Submit("a", SubmitArgs{Command: "setsid sh -c 'sleep 30' >/dev/null 2>&1 & echo $!"})
	if err != nil {
		t.Fatal(err)
	}
	done := waitState(t, "a", job.JobID, true)
	if done.State != "succeeded" {
		t.Fatalf("job state=%s err=%s", done.State, done.Error)
	}
	res, err := ResultFor("a", job.JobID, 4096)
	if err != nil {
		t.Fatal(err)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(res.Output))
	if err != nil {
		t.Fatalf("parse detached pid from %q: %v", res.Output, err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(fmt.Sprintf("/proc/%d", pid)); os.IsNotExist(err) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("detached child %d survived completed job", pid)
}

func TestOwnerSetPrunesAfterQueuesDrain(t *testing.T) {
	Init(testConfig(t, 4, 128, 4, 2))
	defer Shutdown()

	type owned struct{ owner, id string }
	jobs := make([]owned, 0, 40)
	for i := 0; i < 40; i++ {
		owner := fmt.Sprintf("owner-prune-%02d", i)
		snap, err := Submit(owner, SubmitArgs{Command: "echo done"})
		if err != nil {
			t.Fatal(err)
		}
		jobs = append(jobs, owned{owner: owner, id: snap.JobID})
	}
	for _, j := range jobs {
		if got := waitState(t, j.owner, j.id, true); got.State != "succeeded" {
			t.Fatalf("job %s ended %s", j.id, got.State)
		}
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		s := current()
		s.mu.Lock()
		owners := len(s.owners)
		queues := len(s.queues)
		s.mu.Unlock()
		if owners == 0 && queues == 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	s := current()
	s.mu.Lock()
	defer s.mu.Unlock()
	t.Fatalf("historical owners retained after drain: owners=%d queues=%d", len(s.owners), len(s.queues))
}

func TestSupersessionUsesNormalizedKey(t *testing.T) {
	Init(testConfig(t, 1, 8, 8, 1))
	defer Shutdown()

	old, err := Submit("a", SubmitArgs{Command: "sleep 2", SupersedeKey: "build"})
	if err != nil {
		t.Fatal(err)
	}
	waitState(t, "a", old.JobID, false)
	fresh, err := Submit("a", SubmitArgs{Command: "echo fresh", SupersedeKey: "  build  "})
	if err != nil {
		t.Fatal(err)
	}
	if got := waitState(t, "a", old.JobID, true); got.State != "canceled" {
		t.Fatalf("normalized supersession did not cancel old job: %+v", got)
	}
	if got := waitState(t, "a", fresh.JobID, true); got.State != "succeeded" {
		t.Fatalf("fresh job state=%s err=%s", got.State, got.Error)
	}
}

func TestTighterMemoryEnvelopeUsesCgroupBudget(t *testing.T) {
	const gib = uint64(1 << 30)
	total, available := tighterMemoryEnvelope(8*gib, 6*gib, 2*gib, 3*gib)
	if total != 3*gib || available != 1*gib {
		t.Fatalf("cgroup-tight envelope total=%d available=%d", total, available)
	}
	total, available = tighterMemoryEnvelope(8*gib, 1*gib, 1*gib, 5*gib)
	if total != 8*gib || available != 1*gib {
		t.Fatalf("global-tight envelope total=%d available=%d", total, available)
	}
}

func TestCgroupMemoryBudgetPrefersHighBeforeMax(t *testing.T) {
	const gib = uint64(1 << 30)
	dir := t.TempDir()
	if err := os.WriteFile(dir+"/memory.current", []byte(strconv.FormatUint(2*gib, 10)), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dir+"/memory.high", []byte(strconv.FormatUint(3*gib, 10)), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dir+"/memory.max", []byte(strconv.FormatUint(5*gib, 10)), 0o600); err != nil {
		t.Fatal(err)
	}
	current, budget := readCgroupMemoryBudget(dir)
	if current != 2*gib || budget != 3*gib {
		t.Fatalf("current=%d budget=%d", current, budget)
	}
}

func TestOrphanOutputSweepPreservesKnownJobs(t *testing.T) {
	cfg := testConfig(t, 2, 8, 8, 1)
	Init(cfg)
	defer Shutdown()

	known, err := Submit("a", SubmitArgs{Command: "echo known"})
	if err != nil {
		t.Fatal(err)
	}
	if got := waitState(t, "a", known.JobID, true); got.State != "succeeded" {
		t.Fatalf("known job state=%s err=%s", got.State, got.Error)
	}

	s := current()
	s.mu.Lock()
	knownPath := s.jobs[known.JobID].OutputPath
	dataDir := s.cfg.DataDir
	s.mu.Unlock()
	old := time.Now().Add(-2 * time.Hour)
	if err := os.Chtimes(knownPath, old, old); err != nil {
		t.Fatal(err)
	}
	orphanPath := filepath.Join(dataDir, "jobs", "orphan.log")
	if err := os.WriteFile(orphanPath, []byte("orphan"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(orphanPath, old, old); err != nil {
		t.Fatal(err)
	}

	s.sweepOrphanOutputs(time.Now().Add(-time.Hour))
	if _, err := os.Stat(orphanPath); !os.IsNotExist(err) {
		t.Fatalf("orphan output not swept: %v", err)
	}
	if _, err := os.Stat(knownPath); err != nil {
		t.Fatalf("known job output was swept: %v", err)
	}
}

func TestGCKeepsLiveDependenciesAndWholeGraphGeneration(t *testing.T) {
	Init(testConfig(t, 2, 16, 16, 1))
	defer Shutdown()

	graph, err := SubmitGraph("a", GraphSubmitArgs{
		GraphKey: "gc-protected-graph",
		Nodes: []GraphNodeArgs{
			{NodeID: "root", Command: "echo root"},
			{NodeID: "child", Command: "sleep 0.30; echo child", DependsOn: []string{"root"}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	ids := map[string]string{}
	for _, node := range graph.Nodes {
		ids[node.NodeID] = node.Job.JobID
	}
	rootDone := waitState(t, "a", ids["root"], true)
	if rootDone.State != "succeeded" {
		t.Fatalf("root state=%s", rootDone.State)
	}
	waitState(t, "a", ids["child"], false)

	s := current()
	old := time.Now().Add(-2 * time.Hour)
	cutoff := time.Now().Add(-time.Hour)
	s.mu.Lock()
	root := s.jobs[ids["root"]]
	root.mu.Lock()
	root.FinishedAt = old
	root.mu.Unlock()
	protected := s.protectedJobsLocked(cutoff)
	_, rootProtected := protected[ids["root"]]
	_, graphPresent := s.graphs[graphMapKey("a", "gc-protected-graph")]
	s.mu.Unlock()
	if !rootProtected || !graphPresent {
		t.Fatalf("live graph lost protection: rootProtected=%v graphPresent=%v", rootProtected, graphPresent)
	}

	childDone := waitState(t, "a", ids["child"], true)
	if childDone.State != "succeeded" {
		t.Fatalf("child state=%s err=%s", childDone.State, childDone.Error)
	}
	s.mu.Lock()
	for _, id := range ids {
		j := s.jobs[id]
		j.mu.Lock()
		j.FinishedAt = old
		j.mu.Unlock()
	}
	protected = s.protectedJobsLocked(cutoff)
	_, graphPresent = s.graphs[graphMapKey("a", "gc-protected-graph")]
	_, rootProtected = protected[ids["root"]]
	s.mu.Unlock()
	if graphPresent || rootProtected {
		t.Fatalf("expired terminal graph still retained: graphPresent=%v rootProtected=%v", graphPresent, rootProtected)
	}
}

func TestGCProtectsStandaloneDependencyOfRunningJob(t *testing.T) {
	Init(testConfig(t, 2, 16, 16, 1))
	defer Shutdown()

	dep, err := Submit("a", SubmitArgs{Command: "echo dep"})
	if err != nil {
		t.Fatal(err)
	}
	if got := waitState(t, "a", dep.JobID, true); got.State != "succeeded" {
		t.Fatalf("dep state=%s", got.State)
	}
	child, err := Submit("a", SubmitArgs{Command: "sleep 0.30", DependsOn: []string{dep.JobID}})
	if err != nil {
		t.Fatal(err)
	}
	waitState(t, "a", child.JobID, false)

	s := current()
	s.mu.Lock()
	d := s.jobs[dep.JobID]
	d.mu.Lock()
	d.FinishedAt = time.Now().Add(-2 * time.Hour)
	d.mu.Unlock()
	protected := s.protectedJobsLocked(time.Now().Add(-time.Hour))
	_, ok := protected[dep.JobID]
	s.mu.Unlock()
	if !ok {
		t.Fatal("running dependent did not protect its dependency from GC")
	}
}

func waitOutputPID(t *testing.T, owner, jobID string) int {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		res, err := ResultFor(owner, jobID, 4096)
		if err != nil {
			t.Fatal(err)
		}
		if raw := strings.TrimSpace(res.Output); raw != "" {
			pid, err := strconv.Atoi(strings.Fields(raw)[0])
			if err != nil {
				t.Fatalf("parse child pid from %q: %v", raw, err)
			}
			return pid
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("timed out waiting for detached child pid output")
	return 0
}

func waitProcessGone(t *testing.T, pid int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(fmt.Sprintf("/proc/%d", pid)); os.IsNotExist(err) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("process %d survived job containment cleanup", pid)
}

func TestCancelKillsDetachedDescendants(t *testing.T) {
	if !jobCgroupIsolationAvailable() {
		t.Skip("cgroup v2 job isolation unavailable")
	}
	Init(testConfig(t, 1, 8, 8, 1))
	defer Shutdown()

	snap, err := Submit("a", SubmitArgs{Command: "setsid sh -c 'sleep 30' >/dev/null 2>&1 & echo $!; sleep 30"})
	if err != nil {
		t.Fatal(err)
	}
	waitState(t, "a", snap.JobID, false)
	pid := waitOutputPID(t, "a", snap.JobID)
	if _, err := Cancel("a", snap.JobID); err != nil {
		t.Fatal(err)
	}
	done := waitState(t, "a", snap.JobID, true)
	if done.State != "canceled" {
		t.Fatalf("canceled job state=%s err=%s", done.State, done.Error)
	}
	waitProcessGone(t, pid)
}

func TestTimeoutKillsDetachedDescendants(t *testing.T) {
	if !jobCgroupIsolationAvailable() {
		t.Skip("cgroup v2 job isolation unavailable")
	}
	cfg := testConfig(t, 1, 8, 8, 1)
	cfg.DefaultTimeout = 150 * time.Millisecond
	Init(cfg)
	defer Shutdown()

	snap, err := Submit("a", SubmitArgs{Command: "setsid sh -c 'sleep 30' >/dev/null 2>&1 & echo $!; sleep 30", Timeout: 150 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	waitState(t, "a", snap.JobID, false)
	pid := waitOutputPID(t, "a", snap.JobID)
	done := waitState(t, "a", snap.JobID, true)
	if done.State != "timed_out" {
		t.Fatalf("timed out job state=%s err=%s", done.State, done.Error)
	}
	waitProcessGone(t, pid)
}

func TestRejectedReplacementPreservesRunningIncumbent(t *testing.T) {
	Init(testConfig(t, 1, 1, 1, 1))
	defer Shutdown()

	incumbent, err := Submit("a", SubmitArgs{Command: "sleep 0.25; echo incumbent", SupersedeKey: "build"})
	if err != nil {
		t.Fatal(err)
	}
	waitState(t, "a", incumbent.JobID, false)
	filler, err := Submit("b", SubmitArgs{Command: "echo filler"})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := Submit("a", SubmitArgs{Command: "echo replacement", SupersedeKey: "build"}); err == nil {
		t.Fatal("expected replacement admission rejection")
	}
	inc, err := Status("a", incumbent.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if inc.CancelRequested {
		t.Fatalf("rejected replacement canceled incumbent: %+v", inc)
	}
	if got := waitState(t, "a", incumbent.JobID, true); got.State != "succeeded" {
		t.Fatalf("incumbent state=%s err=%s", got.State, got.Error)
	}
	if got := waitState(t, "b", filler.JobID, true); got.State != "succeeded" {
		t.Fatalf("filler state=%s err=%s", got.State, got.Error)
	}
}

func TestQueuedSupersessionFreesAdmissionCapacity(t *testing.T) {
	Init(testConfig(t, 1, 1, 1, 1))
	defer Shutdown()

	blocker, err := Submit("b", SubmitArgs{Command: "sleep 0.25"})
	if err != nil {
		t.Fatal(err)
	}
	waitState(t, "b", blocker.JobID, false)
	stale, err := Submit("a", SubmitArgs{Command: "echo stale", SupersedeKey: "build"})
	if err != nil {
		t.Fatal(err)
	}

	fresh, err := Submit("a", SubmitArgs{Command: "echo fresh", SupersedeKey: "build"})
	if err != nil {
		t.Fatalf("replacement should consume superseded queue capacity: %v", err)
	}
	staleDone := waitState(t, "a", stale.JobID, true)
	if staleDone.State != "canceled" {
		t.Fatalf("stale state=%s", staleDone.State)
	}
	if got := waitState(t, "a", fresh.JobID, true); got.State != "succeeded" {
		t.Fatalf("fresh state=%s err=%s", got.State, got.Error)
	}
}

func TestJobCannotSupersedeOwnDependency(t *testing.T) {
	Init(testConfig(t, 1, 4, 4, 1))
	defer Shutdown()

	dep, err := Submit("a", SubmitArgs{Command: "sleep 0.20; echo dep", SupersedeKey: "build"})
	if err != nil {
		t.Fatal(err)
	}
	waitState(t, "a", dep.JobID, false)
	if _, err := Submit("a", SubmitArgs{Command: "echo child", SupersedeKey: "build", DependsOn: []string{dep.JobID}}); err == nil {
		t.Fatal("expected supersede-own-dependency rejection")
	}
	status, err := Status("a", dep.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if status.CancelRequested {
		t.Fatalf("rejected dependent canceled dependency: %+v", status)
	}
	if got := waitState(t, "a", dep.JobID, true); got.State != "succeeded" {
		t.Fatalf("dependency state=%s err=%s", got.State, got.Error)
	}
}

func TestGraphSupersessionFreesAdmissionCapacity(t *testing.T) {
	Init(testConfig(t, 1, 2, 2, 1))
	defer Shutdown()

	blocker, err := Submit("b", SubmitArgs{Command: "sleep 0.30"})
	if err != nil {
		t.Fatal(err)
	}
	waitState(t, "b", blocker.JobID, false)
	stale1, err := Submit("a", SubmitArgs{Command: "echo stale1", SupersedeKey: "graph-build"})
	if err != nil {
		t.Fatal(err)
	}
	stale2, err := Submit("a", SubmitArgs{Command: "echo stale2", SupersedeKey: "graph-build"})
	if err != nil {
		t.Fatal(err)
	}

	graph, err := SubmitGraph("a", GraphSubmitArgs{GraphKey: "replacement-graph", Nodes: []GraphNodeArgs{
		{NodeID: "left", Command: "echo left", SupersedeKey: "graph-build"},
		{NodeID: "right", Command: "echo right", SupersedeKey: "graph-build"},
	}})
	if err != nil {
		t.Fatalf("replacement graph should consume superseded queue capacity: %v", err)
	}
	if len(graph.Nodes) != 2 {
		t.Fatalf("graph nodes=%d", len(graph.Nodes))
	}
	for _, stale := range []Snapshot{stale1, stale2} {
		if got := waitState(t, "a", stale.JobID, true); got.State != "canceled" {
			t.Fatalf("stale graph predecessor %s state=%s", stale.JobID, got.State)
		}
	}
	for _, node := range graph.Nodes {
		if got := waitState(t, "a", node.Job.JobID, true); got.State != "succeeded" {
			t.Fatalf("replacement graph node %s state=%s err=%s", node.NodeID, got.State, got.Error)
		}
	}
}

func TestRejectedGraphPreservesRunningIncumbent(t *testing.T) {
	Init(testConfig(t, 1, 1, 1, 1))
	defer Shutdown()

	incumbent, err := Submit("a", SubmitArgs{Command: "sleep 0.25; echo incumbent", SupersedeKey: "graph-build"})
	if err != nil {
		t.Fatal(err)
	}
	waitState(t, "a", incumbent.JobID, false)
	filler, err := Submit("b", SubmitArgs{Command: "echo filler"})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := SubmitGraph("a", GraphSubmitArgs{Nodes: []GraphNodeArgs{{NodeID: "replacement", Command: "echo replacement", SupersedeKey: "graph-build"}}}); err == nil {
		t.Fatal("expected graph admission rejection")
	}
	status, err := Status("a", incumbent.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if status.CancelRequested {
		t.Fatalf("rejected graph canceled incumbent: %+v", status)
	}
	if got := waitState(t, "a", incumbent.JobID, true); got.State != "succeeded" {
		t.Fatalf("incumbent state=%s err=%s", got.State, got.Error)
	}
	if got := waitState(t, "b", filler.JobID, true); got.State != "succeeded" {
		t.Fatalf("filler state=%s err=%s", got.State, got.Error)
	}
}

func TestPrepareCgroupIsolationPreservesActiveSibling(t *testing.T) {
	if !jobCgroupIsolationAvailable() {
		t.Skip("cgroup v2 job isolation unavailable")
	}
	base, ok := currentCgroupV2Base()
	if !ok {
		t.Skip("no cgroup v2 base")
	}

	cmd := exec.Command("sleep", "30")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	pid := cmd.Process.Pid
	start, ok := processStartTicks(pid)
	if !ok {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		t.Fatal("could not read sibling process start time")
	}
	sibling := filepath.Join(base, fmt.Sprintf("%s%d-%s", workerCgroupPrefix, pid, start))
	if err := os.Mkdir(sibling, 0o700); err != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		t.Fatal(err)
	}
	g := &jobCgroup{path: sibling}
	defer func() {
		_ = g.terminateAndRemove()
		_ = cmd.Wait()
	}()
	if err := os.WriteFile(filepath.Join(sibling, "cgroup.procs"), []byte(strconv.Itoa(pid)), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := prepareJobCgroupIsolation(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(sibling); err != nil {
		t.Fatalf("active sibling namespace was reaped: %v", err)
	}
	if err := cmd.Process.Signal(os.Signal(syscall.Signal(0))); err != nil {
		t.Fatalf("active sibling process was killed: %v", err)
	}
}

func TestPrepareCgroupIsolationReapsDeadSibling(t *testing.T) {
	if !jobCgroupIsolationAvailable() {
		t.Skip("cgroup v2 job isolation unavailable")
	}
	base, ok := currentCgroupV2Base()
	if !ok {
		t.Skip("no cgroup v2 base")
	}
	stale := filepath.Join(base, workerCgroupPrefix+"99999999-1")
	_ = os.Remove(stale)
	if err := os.Mkdir(stale, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := prepareJobCgroupIsolation(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(stale); !os.IsNotExist(err) {
		t.Fatalf("dead sibling namespace not reaped: %v", err)
	}
}

func TestResultForWaitReachesTerminal(t *testing.T) {
	Init(testConfig(t, 1, 8, 8, 1))
	defer Shutdown()

	snap, err := Submit("wait-owner", SubmitArgs{Command: "sleep 0.2; printf done"})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	start := time.Now()
	res, err := ResultForWait("wait-owner", snap.JobID, 4096, 2*time.Second)
	if err != nil {
		t.Fatalf("result wait: %v", err)
	}
	if res.Snapshot.State != "succeeded" {
		t.Fatalf("expected succeeded, got %+v", res.Snapshot)
	}
	if res.Output != "done" {
		t.Fatalf("expected output done, got %q", res.Output)
	}
	if elapsed := time.Since(start); elapsed < 150*time.Millisecond {
		t.Fatalf("result returned before job could complete: %v", elapsed)
	}
}

func TestResultForWaitReturnsAtBound(t *testing.T) {
	Init(testConfig(t, 1, 8, 8, 1))
	defer Shutdown()

	snap, err := Submit("wait-owner", SubmitArgs{Command: "sleep 1"})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	start := time.Now()
	res, err := ResultForWait("wait-owner", snap.JobID, 4096, 100*time.Millisecond)
	if err != nil {
		t.Fatalf("result wait: %v", err)
	}
	elapsed := time.Since(start)
	if isTerminalState(res.Snapshot.State) {
		t.Fatalf("expected non-terminal snapshot at wait bound, got %+v", res.Snapshot)
	}
	if elapsed < 80*time.Millisecond || elapsed > 500*time.Millisecond {
		t.Fatalf("bounded wait duration out of range: %v", elapsed)
	}
	if _, err := Cancel("wait-owner", snap.JobID); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	_ = waitState(t, "wait-owner", snap.JobID, true)
}
