package mcpserver

import (
	"strings"
	"testing"
	"time"

	"github.com/fzxbl/terminal-mcp/internal/config"
	"github.com/fzxbl/terminal-mcp/internal/job"
)

func initMCPObservationStore(t *testing.T, maxActive int) {
	t.Helper()
	config.Load("")
	oldBlock := config.Get().MaxBlockSeconds
	oldBatch := config.Get().JobMaxBatch
	oldResult := config.Get().JobResultMaxBytes
	config.Get().MaxBlockSeconds = 3
	config.Get().JobMaxBatch = 24
	config.Get().JobResultMaxBytes = 65536
	t.Cleanup(func() {
		config.Get().MaxBlockSeconds = oldBlock
		config.Get().JobMaxBatch = oldBatch
		config.Get().JobResultMaxBytes = oldResult
	})
	job.Init(job.Config{
		DataDir: t.TempDir(), MaxActive: maxActive, MaxQueued: 32, MaxQueuedPerOwner: 32,
		MaxCPUHeavy: maxActive, ResultMaxBytes: 65536, DefaultTimeout: 3 * time.Second,
		MaxTimeout: 5 * time.Second, Retention: time.Hour, DisableAdaptive: true,
	})
	t.Cleanup(job.Shutdown)
}

func snapshotsOverlap(a, b job.Snapshot) bool {
	as, err1 := time.Parse(time.RFC3339Nano, a.StartedAt)
	af, err2 := time.Parse(time.RFC3339Nano, a.FinishedAt)
	bs, err3 := time.Parse(time.RFC3339Nano, b.StartedAt)
	bf, err4 := time.Parse(time.RFC3339Nano, b.FinishedAt)
	if err1 != nil || err2 != nil || err3 != nil || err4 != nil {
		return false
	}
	return as.Before(bf) && bs.Before(af)
}

func TestBatchAttachedDefaultReturnsConcurrentTerminalResults(t *testing.T) {
	initMCPObservationStore(t, 3)
	out, err := submitBatchAndMaybeWait("owner-a", jobBatchInput{
		WaitMs: 2000, MaxBytesPerJob: 4096,
		Items: []jobBatchItemInput{
			{Command: "sleep 0.30; printf A"},
			{Command: "sleep 0.30; printf B"},
			{Command: "sleep 0.30; printf C"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if out.Accepted != 3 || out.TerminalCount != 3 || out.PendingCount != 0 || !out.AllTerminal || out.FollowupRequired {
		t.Fatalf("attached batch did not close in one call: %+v", out)
	}
	for i, want := range []string{"A", "B", "C"} {
		if out.Results[i].Job == nil || out.Results[i].Job.State != "succeeded" || !out.Results[i].Job.Observed || !strings.Contains(out.Results[i].Output, want) {
			t.Fatalf("result %d missing terminal observation/output: %+v", i, out.Results[i])
		}
	}
	if !snapshotsOverlap(*out.Results[0].Job, *out.Results[1].Job) || !snapshotsOverlap(*out.Results[1].Job, *out.Results[2].Job) {
		t.Fatalf("independent batch jobs did not overlap physically: %+v", out.Results)
	}
}

func TestBatchMixedDurationReportsTerminalAndPendingHonestly(t *testing.T) {
	initMCPObservationStore(t, 3)
	out, err := submitBatchAndMaybeWait("owner-a", jobBatchInput{
		WaitMs: 300, MaxBytesPerJob: 4096,
		Items: []jobBatchItemInput{
			{Command: "printf ok"},
			{Command: "printf bad; exit 7"},
			{Command: "sleep 1; printf late"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if out.TerminalCount != 2 || out.FailedCount != 1 || out.PendingCount != 1 || out.AllTerminal || !out.FollowupRequired || len(out.PendingJobIDs) != 1 {
		t.Fatalf("mixed batch aggregate wrong: %+v", out)
	}
	if !strings.Contains(out.Results[0].Output, "ok") || !strings.Contains(out.Results[1].Output, "bad") {
		t.Fatalf("terminal outputs missing: %+v", out.Results)
	}
	if out.Results[2].Job == nil || out.Results[2].Job.Observed {
		t.Fatalf("pending job falsely observed: %+v", out.Results[2])
	}
}

func TestBatchAsyncRemainsAdmissionOnly(t *testing.T) {
	initMCPObservationStore(t, 2)
	start := time.Now()
	out, err := submitBatchAndMaybeWait("owner-a", jobBatchInput{
		Async: true,
		Items: []jobBatchItemInput{{Command: "sleep 0.4; printf A"}, {Command: "sleep 0.4; printf B"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if time.Since(start) > 200*time.Millisecond {
		t.Fatalf("async batch blocked: %s", time.Since(start))
	}
	if out.Accepted != 2 || out.PendingCount != 2 || !out.FollowupRequired {
		t.Fatalf("async batch did not return admission state: %+v", out)
	}
}

func TestGraphAttachedDefaultPreservesDependenciesAndParallelism(t *testing.T) {
	initMCPObservationStore(t, 3)
	out, err := submitGraphAndMaybeWait("owner-a", jobGraphInput{
		GraphKey: "attached-graph", WaitMs: 2500, MaxBytesPerJob: 4096,
		Nodes: []jobGraphNodeInput{
			{NodeID: "A", Command: "sleep 0.25; printf A"},
			{NodeID: "B", Command: "sleep 0.25; printf B"},
			{NodeID: "C", Command: "printf C", DependsOn: []string{"A", "B"}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if out.TerminalCount != 3 || out.PendingCount != 0 || !out.AllTerminal || out.FollowupRequired {
		t.Fatalf("attached graph did not close: %+v", out)
	}
	byID := map[string]jobGraphNodeOutput{}
	for _, n := range out.Nodes {
		byID[n.NodeID] = n
		if n.Job.State != "succeeded" || !n.Job.Observed || n.Output == "" {
			t.Fatalf("node not returned terminal+observed: %+v", n)
		}
	}
	if !snapshotsOverlap(byID["A"].Job, byID["B"].Job) {
		t.Fatalf("independent graph roots did not overlap: A=%+v B=%+v", byID["A"].Job, byID["B"].Job)
	}
	aDone, _ := time.Parse(time.RFC3339Nano, byID["A"].Job.FinishedAt)
	bDone, _ := time.Parse(time.RFC3339Nano, byID["B"].Job.FinishedAt)
	cStart, _ := time.Parse(time.RFC3339Nano, byID["C"].Job.StartedAt)
	if cStart.Before(aDone) || cStart.Before(bDone) {
		t.Fatalf("dependent C started before roots completed: A=%s B=%s C=%s", aDone, bDone, cStart)
	}
}

func waitTerminalStatusOnly(t *testing.T, owner, id string) job.Snapshot {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		snap, err := job.Status(owner, id)
		if err != nil {
			t.Fatal(err)
		}
		if terminalJobState(snap.State) {
			return snap
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("job %s did not finish", id)
	return job.Snapshot{}
}

func TestDrainDiscoversUnreadTerminalDebtAndExplicitRereadIsSafe(t *testing.T) {
	initMCPObservationStore(t, 2)
	submitted, err := submitBatchAndMaybeWait("owner-a", jobBatchInput{
		Async: true,
		Items: []jobBatchItemInput{{Command: "printf recover-me", IdempotencyKey: "recover-me"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	id := submitted.Results[0].Job.JobID
	terminal := waitTerminalStatusOnly(t, "owner-a", id)
	if terminal.Observed || job.UnobservedTerminalCount("owner-a") != 1 {
		t.Fatalf("async terminal debt not represented: %+v count=%d", terminal, job.UnobservedTerminalCount("owner-a"))
	}

	drained, err := drainJobs("owner-a", jobDrainInput{WaitMs: 1000, MaxBytesPerJob: 4096})
	if err != nil {
		t.Fatal(err)
	}
	if drained.Selected != 1 || drained.TerminalCount != 1 || drained.PendingCount != 0 || drained.RemainingUnobservedTerminalCount != 0 || !strings.Contains(drained.Results[0].Output, "recover-me") {
		t.Fatalf("drain did not close observation debt: %+v", drained)
	}

	reread, err := drainJobs("owner-a", jobDrainInput{JobIDs: []string{id}, WaitMs: 1, MaxBytesPerJob: 4096})
	if err != nil {
		t.Fatal(err)
	}
	if reread.TerminalCount != 1 || !strings.Contains(reread.Results[0].Output, "recover-me") {
		t.Fatalf("explicit retry/reread lost evidence: %+v", reread)
	}
	if _, err := drainJobs("owner-b", jobDrainInput{JobIDs: []string{id}}); err == nil {
		t.Fatal("other owner drained owner-a job")
	}
}

func TestAggregateOutputBudgetBoundsBatch(t *testing.T) {
	initMCPObservationStore(t, 2)
	config.Get().JobResultMaxBytes = 1024
	out, err := submitBatchAndMaybeWait("owner-a", jobBatchInput{
		WaitMs: 1500, MaxBytesPerJob: 4096,
		Items: []jobBatchItemInput{
			{Command: "head -c 4000 /dev/zero | tr '\\0' A"},
			{Command: "head -c 4000 /dev/zero | tr '\\0' B"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !out.AllTerminal || len(out.Results[0].Output) > 512 || len(out.Results[1].Output) > 512 || !out.Results[0].Truncated || !out.Results[1].Truncated {
		t.Fatalf("aggregate output budget not enforced: len0=%d len1=%d out=%+v", len(out.Results[0].Output), len(out.Results[1].Output), out)
	}
}
