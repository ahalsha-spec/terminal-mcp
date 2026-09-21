package job

import (
	"strings"
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
			case "succeeded", "failed", "canceled", "timed_out":
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
