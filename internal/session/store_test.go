package session

import (
	"testing"
	"time"

	"github.com/fzxbl/terminal-mcp/internal/config"
)

func TestDeliveredOffset(t *testing.T) {
	s := &Session{}
	s.setDelivered(10)
	if s.delivered() != 10 {
		t.Fatalf("delivered=%d", s.delivered())
	}
}

func TestNewSessionIDUnique(t *testing.T) {
	a, b := newSessionID(), newSessionID()
	if a == "" || a == b {
		t.Fatalf("ids should be non-empty and unique: %q %q", a, b)
	}
}

func TestEncodeDecodeSessionID(t *testing.T) {
	SetSelfAddr("10.0.0.1:8900")
	id := newSessionID()
	tok, uuid := decodeSessionID(id)
	if tok != "10.0.0.1:8900" {
		t.Fatalf("self address = %q", tok)
	}
	if uuid == "" || uuid == id {
		t.Fatalf("uuid part not extracted: %q", uuid)
	}
}

func TestDecodeOwnerlessID(t *testing.T) {
	tok, uuid := decodeSessionID("bare-uuid-no-sep")
	if tok != "" || uuid != "bare-uuid-no-sep" {
		t.Fatalf("ownerless decode got (%q,%q)", tok, uuid)
	}
}

func setLastUsedForTest(s *Session, when time.Time) {
	s.stateMu.Lock()
	s.lastUsed = when
	s.stateMu.Unlock()
}

func TestCapacityPressureReclaimsOldestEligibleIdleSession(t *testing.T) {
	config.Load("")
	oldStore := theStore
	oldGrace := config.Get().PressureReapIdleSeconds
	defer func() {
		if theStore != nil {
			theStore.closeAll()
		}
		theStore = oldStore
		config.Get().PressureReapIdleSeconds = oldGrace
	}()

	InitStore(2)
	config.Get().PressureReapIdleSeconds = 1
	a := openLocalReady(t)
	// Age only the first real prompt-idle PTY past the pressure grace. Its PTY
	// output timestamp, not a forged lastUsed value, is now part of eligibility.
	time.Sleep(1100 * time.Millisecond)
	b := openLocalReady(t)

	c := openLocalReady(t)
	if theStore.get(a) != nil {
		t.Fatalf("oldest idle session %s was not reclaimed under capacity pressure", a)
	}
	if theStore.get(b) == nil || theStore.get(c) == nil {
		t.Fatalf("newer idle and newly admitted sessions must remain")
	}
}

func TestCapacityPressureProtectsHeldRunningAndRecentSessions(t *testing.T) {
	config.Load("")
	oldStore := theStore
	oldGrace := config.Get().PressureReapIdleSeconds
	defer func() {
		if theStore != nil {
			theStore.closeAll()
		}
		theStore = oldStore
		config.Get().PressureReapIdleSeconds = oldGrace
	}()

	InitStore(3)
	config.Get().PressureReapIdleSeconds = 30
	held := openLocalReady(t)
	running := openLocalReady(t)
	recent := openLocalReady(t)

	theStore.get(held).setHold(true)
	setLastUsedForTest(theStore.get(held), time.Now().Add(-2*time.Minute))
	env := Send(running, "sleep 5", 50)
	if env.State != "running" {
		t.Fatalf("expected running session, got state=%q error=%q", env.State, env.Error)
	}
	setLastUsedForTest(theStore.get(running), time.Now().Add(-2*time.Minute))
	setLastUsedForTest(theStore.get(recent), time.Now())

	if _, err := OpenLocalForTest(); err == nil {
		t.Fatalf("pressure admission must fail when all existing sessions are held, running, or within grace")
	}
	_ = Control(running, "ctrl-c")
}

func waitForIdleForTest(t *testing.T, id string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if env := Status(id); env.State == "idle" {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("session %s did not become idle", id)
}

func TestCapacityPressureRespectsRecentPTYOutput(t *testing.T) {
	config.Load("")
	oldStore := theStore
	oldGrace := config.Get().PressureReapIdleSeconds
	defer func() {
		if theStore != nil {
			theStore.closeAll()
		}
		theStore = oldStore
		config.Get().PressureReapIdleSeconds = oldGrace
	}()

	InitStore(1)
	config.Get().PressureReapIdleSeconds = 30
	id := openLocalReady(t)
	sess := theStore.get(id)
	setLastUsedForTest(sess, time.Now().Add(-2*time.Minute))
	// Simulate a long-running command completing after the last MCP touch: PTY
	// output/prompt becomes recent while lastUsed remains old.
	sess.getProc().Write("echo RECENT_PTY_OUTPUT\n")
	waitForIdleForTest(t, id)
	if _, err := OpenLocalForTest(); err == nil {
		t.Fatal("recent PTY output must protect an idle session from pressure reclaim")
	}
	if theStore.get(id) == nil {
		t.Fatal("session with recent PTY output was reclaimed")
	}
}

func TestIdleGCPreservesRunningHeldAndRecentOutput(t *testing.T) {
	t.Run("running", func(t *testing.T) {
		config.Load("")
		oldStore := theStore
		defer func() {
			if theStore != nil {
				theStore.closeAll()
			}
			theStore = oldStore
		}()
		InitStore(1)
		id := openLocalReady(t)
		env := Send(id, "sleep 5", 50)
		if env.State != "running" {
			t.Fatalf("expected running session, got state=%q error=%q", env.State, env.Error)
		}
		setLastUsedForTest(theStore.get(id), time.Now().Add(-time.Hour))
		theStore.gcIdle(0)
		if theStore.get(id) == nil {
			t.Fatal("idle GC killed a running session")
		}
		_ = Control(id, "ctrl-c")
	})

	t.Run("held", func(t *testing.T) {
		config.Load("")
		oldStore := theStore
		defer func() {
			if theStore != nil {
				theStore.closeAll()
			}
			theStore = oldStore
		}()
		InitStore(1)
		id := openLocalReady(t)
		sess := theStore.get(id)
		sess.setHold(true)
		setLastUsedForTest(sess, time.Now().Add(-time.Hour))
		theStore.gcIdle(0)
		if theStore.get(id) == nil {
			t.Fatal("idle GC killed a human-held session")
		}
	})

	t.Run("recent_output", func(t *testing.T) {
		config.Load("")
		oldStore := theStore
		defer func() {
			if theStore != nil {
				theStore.closeAll()
			}
			theStore = oldStore
		}()
		InitStore(1)
		id := openLocalReady(t)
		sess := theStore.get(id)
		setLastUsedForTest(sess, time.Now().Add(-time.Hour))
		sess.getProc().Write("echo GC_RECENT_OUTPUT\n")
		waitForIdleForTest(t, id)
		theStore.gcIdle(30 * time.Second)
		if theStore.get(id) == nil {
			t.Fatal("idle GC ignored recent PTY output")
		}
	})

	t.Run("stale_prompt_idle", func(t *testing.T) {
		config.Load("")
		oldStore := theStore
		defer func() {
			if theStore != nil {
				theStore.closeAll()
			}
			theStore = oldStore
		}()
		InitStore(1)
		id := openLocalReady(t)
		theStore.gcIdle(0)
		if theStore.get(id) != nil {
			t.Fatal("idle GC retained an eligible prompt-idle session")
		}
	})
}
