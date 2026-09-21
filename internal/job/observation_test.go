package job

import (
	"strings"
	"testing"
	"time"
)

func initObservationTestStore(t *testing.T) {
	t.Helper()
	Init(Config{
		DataDir: t.TempDir(), MaxActive: 3, MaxQueued: 16, MaxQueuedPerOwner: 16,
		MaxCPUHeavy: 2, ResultMaxBytes: 4096, DefaultTimeout: 2 * time.Second,
		MaxTimeout: 5 * time.Second, Retention: time.Hour, DisableAdaptive: true,
	})
	t.Cleanup(Shutdown)
}

func waitTerminalWithoutResult(t *testing.T, owner, id string) Snapshot {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		snap, err := Status(owner, id)
		if err != nil {
			t.Fatal(err)
		}
		if isTerminalState(snap.State) {
			return snap
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("job %s did not become terminal", id)
	return Snapshot{}
}

func TestTerminalObservationDebtOnlyClosesOnResult(t *testing.T) {
	initObservationTestStore(t)
	snap, err := Submit("owner-a", SubmitArgs{Command: "printf debt-proof"})
	if err != nil {
		t.Fatal(err)
	}
	terminal := waitTerminalWithoutResult(t, "owner-a", snap.JobID)
	if terminal.Observed {
		t.Fatalf("status must not observe terminal result: %+v", terminal)
	}
	if got := UnobservedTerminalCount("owner-a"); got != 1 {
		t.Fatalf("unobserved terminal count=%d want 1", got)
	}
	if got := UnobservedTerminalCount("owner-b"); got != 0 {
		t.Fatalf("other owner saw debt=%d", got)
	}

	res, err := ResultFor("owner-a", snap.JobID, 4096)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Snapshot.Observed || res.Snapshot.ObservedAt == "" {
		t.Fatalf("terminal result did not become observed: %+v", res.Snapshot)
	}
	if !strings.Contains(res.Output, "debt-proof") {
		t.Fatalf("missing result output: %q", res.Output)
	}
	if got := UnobservedTerminalCount("owner-a"); got != 0 {
		t.Fatalf("debt remained after result read: %d", got)
	}

	again, err := ResultFor("owner-a", snap.JobID, 4096)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(again.Output, "debt-proof") {
		t.Fatalf("observation destroyed output: %q", again.Output)
	}
}

func TestPendingResultDoesNotBecomeObserved(t *testing.T) {
	initObservationTestStore(t)
	snap, err := Submit("owner-a", SubmitArgs{Command: "sleep 0.35; printf later"})
	if err != nil {
		t.Fatal(err)
	}
	res, err := ResultForWait("owner-a", snap.JobID, 4096, 25*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if isTerminalState(res.Snapshot.State) {
		t.Fatalf("test did not exercise pending path: %+v", res.Snapshot)
	}
	if res.Snapshot.Observed {
		t.Fatalf("pending result was falsely marked observed: %+v", res.Snapshot)
	}
	terminal := waitTerminalWithoutResult(t, "owner-a", snap.JobID)
	if terminal.Observed {
		t.Fatalf("terminal state became observed without result read: %+v", terminal)
	}
	if got := UnobservedTerminalCount("owner-a"); got != 1 {
		t.Fatalf("terminal debt count=%d want 1", got)
	}
}
