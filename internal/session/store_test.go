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
	config.Get().PressureReapIdleSeconds = 30
	a := openLocalReady(t)
	b := openLocalReady(t)
	setLastUsedForTest(theStore.get(a), time.Now().Add(-2*time.Minute))
	setLastUsedForTest(theStore.get(b), time.Now().Add(-45*time.Second))

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
