package job

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
)

const (
	ClassNormal     = "normal"
	ClassCPUHeavy   = "cpu_heavy"
	ClassIOWait     = "io_wait"
	ClassBackground = "background"
)

type Config struct {
	DataDir           string
	MaxActive         int
	MaxQueued         int
	MaxQueuedPerOwner int
	MaxCPUHeavy       int
	ResultMaxBytes    int64
	DefaultTimeout    time.Duration
	MaxTimeout        time.Duration
	Retention         time.Duration
	DisableAdaptive   bool
}

type SubmitArgs struct {
	Command        string
	Cwd            string
	Class          string
	SupersedeKey   string
	IdempotencyKey string
	DependsOn      []string
	LockKeys       []string
	Timeout        time.Duration
}

type Snapshot struct {
	JobID           string   `json:"job_id"`
	State           string   `json:"state"`
	Class           string   `json:"class"`
	Command         string   `json:"command,omitempty"`
	Cwd             string   `json:"cwd,omitempty"`
	SupersedeKey    string   `json:"supersede_key,omitempty"`
	IdempotencyKey  string   `json:"idempotency_key,omitempty"`
	DependsOn       []string `json:"depends_on,omitempty"`
	LockKeys        []string `json:"lock_keys,omitempty"`
	CreatedAt       string   `json:"created_at"`
	StartedAt       string   `json:"started_at,omitempty"`
	FinishedAt      string   `json:"finished_at,omitempty"`
	ExitCode        *int     `json:"exit_code,omitempty"`
	Error           string   `json:"error,omitempty"`
	OutputBytes     int64    `json:"output_bytes,omitempty"`
	CancelRequested bool     `json:"cancel_requested,omitempty"`
}

type Result struct {
	Snapshot  Snapshot `json:"job"`
	Output    string   `json:"output,omitempty"`
	Truncated bool     `json:"truncated,omitempty"`
}

type Capacity struct {
	Active                int     `json:"active"`
	MaxActive             int     `json:"max_active"`
	Queued                int     `json:"queued"`
	MaxQueued             int     `json:"max_queued"`
	CPUHeavy              int     `json:"cpu_heavy_active"`
	MaxCPUHeavy           int     `json:"max_cpu_heavy"`
	AdaptiveMaxActive     int     `json:"adaptive_max_active"`
	Adaptive              bool    `json:"adaptive"`
	EffectiveCPU          int     `json:"effective_cpu"`
	CPUSomeAvg10          float64 `json:"cpu_some_avg10"`
	MemorySomeAvg10       float64 `json:"memory_some_avg10"`
	IOFullAvg10           float64 `json:"io_full_avg10"`
	MemoryAvailableMB     uint64  `json:"memory_available_mb"`
	MemoryCgroupCurrentMB uint64  `json:"memory_cgroup_current_mb,omitempty"`
	MemoryCgroupBudgetMB  uint64  `json:"memory_cgroup_budget_mb,omitempty"`
	CgroupIsolation       bool    `json:"cgroup_isolation"`
	CgroupIsolationError  string  `json:"cgroup_isolation_error,omitempty"`
}

type Job struct {
	mu              sync.Mutex
	ID              string
	Owner           string
	Command         string
	Cwd             string
	Class           string
	SupersedeKey    string
	IdempotencyKey  string
	DependsOn       []string
	LockKeys        []string
	State           string
	CreatedAt       time.Time
	StartedAt       time.Time
	FinishedAt      time.Time
	ExitCode        *int
	Error           string
	OutputPath      string
	OutputBytes     int64
	Timeout         time.Duration
	CancelRequested bool
	cancel          context.CancelFunc
	proc            *os.Process
}

func (j *Job) snapshot() Snapshot {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.snapshotLocked()
}

func (j *Job) snapshotLocked() Snapshot {
	s := Snapshot{
		JobID: j.ID, State: j.State, Class: j.Class, Command: j.Command, Cwd: j.Cwd,
		SupersedeKey: j.SupersedeKey, IdempotencyKey: j.IdempotencyKey,
		DependsOn: append([]string(nil), j.DependsOn...), LockKeys: append([]string(nil), j.LockKeys...),
		CreatedAt: j.CreatedAt.UTC().Format(time.RFC3339Nano),
		ExitCode:  j.ExitCode, Error: j.Error, OutputBytes: j.OutputBytes,
		CancelRequested: j.CancelRequested,
	}
	if !j.StartedAt.IsZero() {
		s.StartedAt = j.StartedAt.UTC().Format(time.RFC3339Nano)
	}
	if !j.FinishedAt.IsZero() {
		s.FinishedAt = j.FinishedAt.UTC().Format(time.RFC3339Nano)
	}
	return s
}

type store struct {
	mu             sync.Mutex
	cond           *sync.Cond
	cfg            Config
	cgroupSetupErr error
	jobs           map[string]*Job
	queues         map[string][]*Job
	owners         []string
	idempotency    map[string]string
	locks          map[string]string
	graphs         map[string]*graphRecord
	active         int
	activeCPUHeavy int
	adaptiveMax    int
	pressure       Pressure
	lastOwner      string
	closed         bool
	stop           chan struct{}
}

var (
	globalMu sync.Mutex
	theStore *store
)

func normalizeConfig(c Config) Config {
	if c.DataDir == "" {
		c.DataDir = "./data"
	}
	if c.MaxActive <= 0 {
		c.MaxActive = 4
	}
	if c.MaxQueued <= 0 {
		c.MaxQueued = 128
	}
	if c.MaxQueuedPerOwner <= 0 {
		c.MaxQueuedPerOwner = 24
	}
	if c.MaxQueuedPerOwner > c.MaxQueued {
		c.MaxQueuedPerOwner = c.MaxQueued
	}
	if c.MaxCPUHeavy <= 0 {
		c.MaxCPUHeavy = 2
	}
	if c.MaxCPUHeavy > c.MaxActive {
		c.MaxCPUHeavy = c.MaxActive
	}
	if c.ResultMaxBytes <= 0 {
		c.ResultMaxBytes = 64 << 10
	}
	if c.DefaultTimeout <= 0 {
		c.DefaultTimeout = 15 * time.Minute
	}
	if c.MaxTimeout <= 0 {
		c.MaxTimeout = time.Hour
	}
	if c.DefaultTimeout > c.MaxTimeout {
		c.DefaultTimeout = c.MaxTimeout
	}
	if c.Retention <= 0 {
		c.Retention = time.Hour
	}
	return c
}

func Init(c Config) {
	c = normalizeConfig(c)
	cgroupSetupErr := prepareJobCgroupIsolation()
	s := &store{
		cfg:            c,
		cgroupSetupErr: cgroupSetupErr,
		jobs:           map[string]*Job{},
		queues:         map[string][]*Job{},
		idempotency:    map[string]string{},
		locks:          map[string]string{},
		graphs:         map[string]*graphRecord{},
		stop:           make(chan struct{}),
	}
	s.cond = sync.NewCond(&s.mu)
	if c.DisableAdaptive {
		s.adaptiveMax = c.MaxActive
	} else {
		s.pressure = readPressure()
		s.adaptiveMax = targetForPressure(c.MaxActive, s.pressure)
	}

	globalMu.Lock()
	old := theStore
	theStore = s
	globalMu.Unlock()
	if old != nil {
		old.shutdown()
	}

	_ = os.MkdirAll(filepath.Join(c.DataDir, "jobs"), 0o700)
	s.sweepOrphanOutputs(time.Now().Add(-c.Retention))
	go s.dispatch()
	go s.gcLoop()
	if !c.DisableAdaptive {
		go s.pressureLoop()
	}
}

func current() *store {
	globalMu.Lock()
	defer globalMu.Unlock()
	return theStore
}

func Shutdown() {
	globalMu.Lock()
	s := theStore
	theStore = nil
	globalMu.Unlock()
	if s != nil {
		s.shutdown()
	}
}

func normalizeStringSet(in []string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(in))
	for _, raw := range in {
		v := strings.TrimSpace(raw)
		if v == "" {
			continue
		}
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	sort.Strings(out)
	return out
}

func equalStringSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func idempotencyMapKey(owner, key string) string {
	return owner + string(rune(31)) + key
}

func isTerminalState(state string) bool {
	switch state {
	case "succeeded", "failed", "canceled", "timed_out", "blocked":
		return true
	default:
		return false
	}
}

func normalizeClass(v string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "", ClassNormal:
		return ClassNormal, nil
	case ClassCPUHeavy:
		return ClassCPUHeavy, nil
	case ClassIOWait:
		return ClassIOWait, nil
	case ClassBackground:
		return ClassBackground, nil
	default:
		return "", fmt.Errorf("invalid job class %q", v)
	}
}

type supersessionPlan struct {
	queued  []*Job
	running []*Job
}

func (s *store) planSupersessionLocked(owner string, keys map[string]struct{}) supersessionPlan {
	var plan supersessionPlan
	if len(keys) == 0 {
		return plan
	}
	for _, existing := range s.jobs {
		if existing.Owner != owner {
			continue
		}
		if _, ok := keys[existing.SupersedeKey]; !ok || existing.SupersedeKey == "" {
			continue
		}
		existing.mu.Lock()
		state := existing.State
		existing.mu.Unlock()
		switch state {
		case "queued":
			plan.queued = append(plan.queued, existing)
		case "running":
			plan.running = append(plan.running, existing)
		}
	}
	return plan
}

func (p supersessionPlan) containsJobID(id string) bool {
	for _, j := range p.queued {
		if j.ID == id {
			return true
		}
	}
	for _, j := range p.running {
		if j.ID == id {
			return true
		}
	}
	return false
}

func (s *store) applySupersessionLocked(plan supersessionPlan) []context.CancelFunc {
	var cancels []context.CancelFunc
	for _, existing := range plan.queued {
		existing.mu.Lock()
		if existing.State == "queued" {
			existing.State = "canceled"
			existing.CancelRequested = true
			existing.FinishedAt = time.Now()
			code := -1
			existing.ExitCode = &code
			existing.Error = "canceled"
			s.removeQueuedLocked(existing)
		}
		existing.mu.Unlock()
	}
	for _, existing := range plan.running {
		existing.mu.Lock()
		if existing.State == "running" {
			existing.CancelRequested = true
			if existing.cancel != nil {
				cancels = append(cancels, existing.cancel)
			}
		}
		existing.mu.Unlock()
	}
	return cancels
}

func Submit(owner string, in SubmitArgs) (Snapshot, error) {
	s := current()
	if s == nil {
		return Snapshot{}, errors.New("job scheduler not initialized")
	}
	if owner == "" {
		return Snapshot{}, errors.New("missing owner")
	}
	cmd := strings.TrimSpace(in.Command)
	if cmd == "" {
		return Snapshot{}, errors.New("command cannot be empty")
	}
	class, err := normalizeClass(in.Class)
	if err != nil {
		return Snapshot{}, err
	}
	idempotencyKey := strings.TrimSpace(in.IdempotencyKey)
	supersedeKey := strings.TrimSpace(in.SupersedeKey)
	cwd := strings.TrimSpace(in.Cwd)
	dependsOn := normalizeStringSet(in.DependsOn)
	lockKeys := normalizeStringSet(in.LockKeys)

	timeout := in.Timeout
	if timeout <= 0 {
		timeout = s.cfg.DefaultTimeout
	}
	if timeout > s.cfg.MaxTimeout {
		timeout = s.cfg.MaxTimeout
	}

	var cancels []context.CancelFunc

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return Snapshot{}, errors.New("job scheduler is shutting down")
	}

	if idempotencyKey != "" {
		mapKey := idempotencyMapKey(owner, idempotencyKey)
		if existingID := s.idempotency[mapKey]; existingID != "" {
			existing := s.jobs[existingID]
			if existing != nil {
				existing.mu.Lock()
				same := existing.Command == cmd &&
					existing.Cwd == cwd &&
					existing.Class == class &&
					existing.SupersedeKey == supersedeKey &&
					equalStringSlices(existing.DependsOn, dependsOn) &&
					equalStringSlices(existing.LockKeys, lockKeys) &&
					existing.Timeout == timeout
				snap := existing.snapshotLocked()
				existing.mu.Unlock()
				s.mu.Unlock()
				if !same {
					return Snapshot{}, fmt.Errorf("idempotency key %q reused with different job definition", idempotencyKey)
				}
				return snap, nil
			}
			delete(s.idempotency, mapKey)
		}
	}

	for _, depID := range dependsOn {
		dep := s.jobs[depID]
		if dep == nil || dep.Owner != owner {
			s.mu.Unlock()
			return Snapshot{}, fmt.Errorf("dependency not found: %s", depID)
		}
	}

	keys := map[string]struct{}{}
	if supersedeKey != "" {
		keys[supersedeKey] = struct{}{}
	}
	plan := s.planSupersessionLocked(owner, keys)
	for _, depID := range dependsOn {
		if plan.containsJobID(depID) {
			s.mu.Unlock()
			return Snapshot{}, fmt.Errorf("job cannot supersede its dependency: %s", depID)
		}
	}

	queued := s.queuedCountLocked() - len(plan.queued)
	if queued >= s.cfg.MaxQueued {
		s.mu.Unlock()
		return Snapshot{}, fmt.Errorf("job queue full after supersession plan: %d/%d", queued, s.cfg.MaxQueued)
	}
	ownerQueued := len(s.queues[owner]) - len(plan.queued)
	if ownerQueued >= s.cfg.MaxQueuedPerOwner {
		s.mu.Unlock()
		return Snapshot{}, fmt.Errorf("owner job queue full after supersession plan: %d/%d", ownerQueued, s.cfg.MaxQueuedPerOwner)
	}

	cancels = s.applySupersessionLocked(plan)

	id := uuid.NewString()
	j := &Job{
		ID: id, Owner: owner, Command: cmd, Cwd: cwd,
		Class: class, SupersedeKey: supersedeKey,
		IdempotencyKey: idempotencyKey, DependsOn: dependsOn, LockKeys: lockKeys,
		State: "queued", CreatedAt: time.Now(), Timeout: timeout,
		OutputPath: filepath.Join(s.cfg.DataDir, "jobs", id+".log"),
	}
	s.jobs[id] = j
	if idempotencyKey != "" {
		s.idempotency[idempotencyMapKey(owner, idempotencyKey)] = id
	}
	if _, exists := s.queues[owner]; !exists {
		s.owners = append(s.owners, owner)
	}
	s.queues[owner] = append(s.queues[owner], j)
	s.cond.Broadcast()
	s.mu.Unlock()

	for _, cancel := range cancels {
		cancel()
	}
	return j.snapshot(), nil
}

func (s *store) queuedCountLocked() int {
	n := 0
	for _, q := range s.queues {
		n += len(q)
	}
	return n
}

func (s *store) removeQueuedLocked(target *Job) {
	q := s.queues[target.Owner]
	for i, j := range q {
		if j == target {
			s.queues[target.Owner] = append(q[:i], q[i+1:]...)
			return
		}
	}
}

func (s *store) dependencyStateLocked(j *Job) (ready bool, blockedReason string) {
	for _, depID := range j.DependsOn {
		dep := s.jobs[depID]
		if dep == nil {
			return false, "dependency disappeared: " + depID
		}
		dep.mu.Lock()
		state := dep.State
		dep.mu.Unlock()
		if state == "succeeded" {
			continue
		}
		if isTerminalState(state) {
			return false, fmt.Sprintf("dependency %s ended %s", depID, state)
		}
		return false, ""
	}
	return true, ""
}

func (s *store) locksAvailableLocked(j *Job) bool {
	for _, key := range j.LockKeys {
		if holder := s.locks[key]; holder != "" && holder != j.ID {
			return false
		}
	}
	return true
}

func (s *store) acquireLocksLocked(j *Job) {
	for _, key := range j.LockKeys {
		s.locks[key] = j.ID
	}
}

func (s *store) releaseLocksLocked(j *Job) {
	for _, key := range j.LockKeys {
		if s.locks[key] == j.ID {
			delete(s.locks, key)
		}
	}
}

func (s *store) compactOwnersLocked() {
	if len(s.owners) == 0 {
		return
	}
	kept := s.owners[:0]
	for _, owner := range s.owners {
		if len(s.queues[owner]) == 0 {
			delete(s.queues, owner)
			continue
		}
		kept = append(kept, owner)
	}
	s.owners = kept
}

func (s *store) nextRunnableLocked() *Job {
	s.compactOwnersLocked()
	limit := s.adaptiveMax
	if limit <= 0 || limit > s.cfg.MaxActive {
		limit = s.cfg.MaxActive
	}
	if s.active >= limit || len(s.owners) == 0 {
		return nil
	}

	start := 0
	if s.lastOwner != "" {
		for i, owner := range s.owners {
			if owner == s.lastOwner {
				start = (i + 1) % len(s.owners)
				break
			}
		}
	}

	for step := 0; step < len(s.owners); step++ {
		idx := (start + step) % len(s.owners)
		owner := s.owners[idx]
		q := s.queues[owner]
		for pos := 0; pos < len(q); pos++ {
			j := q[pos]
			j.mu.Lock()
			state, class := j.State, j.Class
			j.mu.Unlock()
			if state != "queued" {
				q = append(q[:pos], q[pos+1:]...)
				s.queues[owner] = q
				pos--
				continue
			}
			ready, blockedReason := s.dependencyStateLocked(j)
			if blockedReason != "" {
				j.mu.Lock()
				j.State = "blocked"
				j.Error = blockedReason
				j.FinishedAt = time.Now()
				code := -1
				j.ExitCode = &code
				j.mu.Unlock()
				q = append(q[:pos], q[pos+1:]...)
				s.queues[owner] = q
				pos--
				continue
			}
			if !ready {
				continue
			}
			if class == ClassCPUHeavy && s.activeCPUHeavy >= s.cfg.MaxCPUHeavy {
				continue
			}
			if !s.cfg.DisableAdaptive && !classAllowedByPressure(class, s.pressure) {
				continue
			}
			if !s.locksAvailableLocked(j) {
				continue
			}
			s.queues[owner] = append(q[:pos], q[pos+1:]...)
			s.acquireLocksLocked(j)
			s.lastOwner = owner
			s.active++
			if class == ClassCPUHeavy {
				s.activeCPUHeavy++
			}
			j.mu.Lock()
			j.State = "running"
			j.StartedAt = time.Now()
			j.mu.Unlock()
			return j
		}
	}
	return nil
}

func (s *store) dispatch() {
	for {
		s.mu.Lock()
		for !s.closed {
			j := s.nextRunnableLocked()
			if j != nil {
				s.mu.Unlock()
				go s.run(j)
				goto next
			}
			s.cond.Wait()
		}
		s.mu.Unlock()
		return
	next:
	}
}

func (s *store) run(j *Job) {
	_ = os.MkdirAll(filepath.Dir(j.OutputPath), 0o700)
	f, err := os.OpenFile(j.OutputPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		s.finish(j, -1, fmt.Errorf("open job output: %w", err), false, false)
		return
	}
	defer f.Close()

	ctx, cancel := context.WithTimeout(context.Background(), j.Timeout)
	defer cancel()

	j.mu.Lock()
	j.cancel = cancel
	if j.CancelRequested {
		j.mu.Unlock()
		cancel()
		s.finish(j, -1, context.Canceled, true, false)
		return
	}
	j.mu.Unlock()

	if s.cgroupSetupErr != nil {
		s.finish(j, -1, fmt.Errorf("job cgroup isolation setup: %w", s.cgroupSetupErr), false, false)
		return
	}
	cg, err := createJobCgroup(j.ID)
	if err != nil {
		s.finish(j, -1, err, false, false)
		return
	}
	cleanupCgroup := func(runErr error) error {
		if cg == nil {
			return runErr
		}
		if err := cg.terminateAndRemove(); err != nil {
			wrapped := fmt.Errorf("job cgroup cleanup: %w", err)
			if runErr == nil {
				return wrapped
			}
			return errors.Join(runErr, wrapped)
		}
		return runErr
	}
	finishSetupFailure := func(runErr error) {
		cleanupOnlyErr := cleanupCgroup(nil)
		if cleanupOnlyErr != nil {
			runErr = errors.Join(runErr, cleanupOnlyErr)
		}
		j.mu.Lock()
		cancelRequested := j.CancelRequested
		j.mu.Unlock()
		timedOut := errors.Is(ctx.Err(), context.DeadlineExceeded)
		if cleanupOnlyErr != nil {
			cancelRequested = false
			timedOut = false
		}
		s.finish(j, -1, runErr, cancelRequested, timedOut)
	}

	gateR, gateW, err := os.Pipe()
	if err != nil {
		finishSetupFailure(fmt.Errorf("create job start gate: %w", err))
		return
	}

	cmd := exec.CommandContext(ctx, "/bin/bash", "-c", `IFS= read -r _ <&3 || exit 125; exec 3<&-; exec /bin/bash -lc "$1"`, "mu-job-gate", j.Command)
	cmd.ExtraFiles = []*os.File{gateR}
	if j.Cwd != "" {
		cmd.Dir = j.Cwd
	}
	cmd.Stdout = f
	cmd.Stderr = f
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.WaitDelay = 2 * time.Second
	cmd.Cancel = func() error {
		if cmd.Process == nil {
			return nil
		}
		err := syscall.Kill(-cmd.Process.Pid, syscall.SIGTERM)
		if err == syscall.ESRCH {
			return os.ErrProcessDone
		}
		return err
	}

	if err := cmd.Start(); err != nil {
		_ = gateR.Close()
		_ = gateW.Close()
		finishSetupFailure(err)
		return
	}
	_ = gateR.Close()

	j.mu.Lock()
	j.proc = cmd.Process
	j.mu.Unlock()

	if cg != nil {
		if err := cg.attach(cmd.Process.Pid); err != nil {
			_ = gateW.Close()
			_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
			_ = cmd.Wait()
			finishSetupFailure(fmt.Errorf("attach job process to cgroup: %w", err))
			return
		}
	}
	if _, err := io.WriteString(gateW, "go\n"); err != nil {
		_ = gateW.Close()
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		_ = cmd.Wait()
		finishSetupFailure(fmt.Errorf("release job start gate: %w", err))
		return
	}
	_ = gateW.Close()

	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			timer := time.NewTimer(2 * time.Second)
			defer timer.Stop()
			select {
			case <-done:
				return
			case <-timer.C:
			}
			if cg != nil {
				_ = cg.killAll()
			} else if cmd.Process != nil {
				_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
			}
		case <-done:
		}
	}()

	err = cmd.Wait()
	close(done)
	_ = f.Sync()
	st, _ := f.Stat()
	var size int64
	if st != nil {
		size = st.Size()
	}

	exitCode := 0
	if err != nil {
		exitCode = -1
		var ee *exec.ExitError
		if errors.As(err, &ee) {
			exitCode = ee.ExitCode()
		}
	}

	j.mu.Lock()
	cancelRequested := j.CancelRequested
	j.OutputBytes = size
	j.mu.Unlock()

	timedOut := errors.Is(ctx.Err(), context.DeadlineExceeded)
	cleanupErr := cleanupCgroup(nil)
	if cleanupErr != nil {
		err = errors.Join(err, cleanupErr)
		exitCode = -1
		cancelRequested = false
		timedOut = false
	}
	s.finish(j, exitCode, err, cancelRequested, timedOut)
}

func (s *store) finish(j *Job, exitCode int, runErr error, canceled, timedOut bool) {
	j.mu.Lock()
	if st, err := os.Stat(j.OutputPath); err == nil {
		j.OutputBytes = st.Size()
	}
	j.FinishedAt = time.Now()
	j.cancel = nil
	j.proc = nil
	j.ExitCode = &exitCode
	switch {
	case canceled:
		j.State, j.Error = "canceled", "canceled"
	case timedOut:
		j.State, j.Error = "timed_out", "execution timeout"
	case runErr == nil:
		j.State, j.Error = "succeeded", ""
	default:
		j.State, j.Error = "failed", runErr.Error()
	}
	class := j.Class
	j.mu.Unlock()

	s.mu.Lock()
	s.releaseLocksLocked(j)
	if s.active > 0 {
		s.active--
	}
	if class == ClassCPUHeavy && s.activeCPUHeavy > 0 {
		s.activeCPUHeavy--
	}
	s.cond.Broadcast()
	s.mu.Unlock()
}

func Cancel(owner, id string) (Snapshot, error) {
	s := current()
	if s == nil {
		return Snapshot{}, errors.New("job scheduler not initialized")
	}

	var cancel context.CancelFunc
	s.mu.Lock()
	j := s.jobs[id]
	if j == nil || j.Owner != owner {
		s.mu.Unlock()
		return Snapshot{}, errors.New("job not found")
	}
	j.mu.Lock()
	switch j.State {
	case "queued":
		j.State = "canceled"
		j.CancelRequested = true
		j.FinishedAt = time.Now()
		code := -1
		j.ExitCode = &code
		s.removeQueuedLocked(j)
	case "running":
		j.CancelRequested = true
		cancel = j.cancel
	}
	snap := j.snapshotLocked()
	j.mu.Unlock()
	s.cond.Broadcast()
	s.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	return snap, nil
}

func Status(owner, id string) (Snapshot, error) {
	s := current()
	if s == nil {
		return Snapshot{}, errors.New("job scheduler not initialized")
	}
	s.mu.Lock()
	j := s.jobs[id]
	s.mu.Unlock()
	if j == nil || j.Owner != owner {
		return Snapshot{}, errors.New("job not found")
	}
	return j.snapshot(), nil
}

func List(owner string) ([]Snapshot, Capacity) {
	s := current()
	if s == nil {
		return nil, Capacity{}
	}
	s.mu.Lock()
	var jobs []*Job
	for _, j := range s.jobs {
		if j.Owner == owner {
			jobs = append(jobs, j)
		}
	}
	cgroupIsolation := s.cgroupSetupErr == nil && jobCgroupIsolationAvailable()
	cgroupIsolationError := ""
	if s.cgroupSetupErr != nil {
		cgroupIsolationError = s.cgroupSetupErr.Error()
	}
	capacity := Capacity{
		Active: s.active, MaxActive: s.cfg.MaxActive,
		Queued: s.queuedCountLocked(), MaxQueued: s.cfg.MaxQueued,
		CPUHeavy: s.activeCPUHeavy, MaxCPUHeavy: s.cfg.MaxCPUHeavy,
		AdaptiveMaxActive: s.adaptiveMax, Adaptive: !s.cfg.DisableAdaptive,
		EffectiveCPU: s.pressure.EffectiveCPU,
		CPUSomeAvg10: s.pressure.CPUSomeAvg10, MemorySomeAvg10: s.pressure.MemorySomeAvg10,
		IOFullAvg10: s.pressure.IOFullAvg10, MemoryAvailableMB: s.pressure.MemoryAvailableBytes >> 20,
		MemoryCgroupCurrentMB: s.pressure.MemoryCgroupCurrentBytes >> 20,
		MemoryCgroupBudgetMB:  s.pressure.MemoryCgroupBudgetBytes >> 20,
		CgroupIsolation:       cgroupIsolation,
		CgroupIsolationError:  cgroupIsolationError,
	}
	s.mu.Unlock()

	out := make([]Snapshot, 0, len(jobs))
	for _, j := range jobs {
		out = append(out, j.snapshot())
	}
	sort.Slice(out, func(i, k int) bool { return out[i].CreatedAt > out[k].CreatedAt })
	return out, capacity
}

func ResultFor(owner, id string, maxBytes int64) (Result, error) {
	s := current()
	if s == nil {
		return Result{}, errors.New("job scheduler not initialized")
	}
	s.mu.Lock()
	j := s.jobs[id]
	limit := s.cfg.ResultMaxBytes
	s.mu.Unlock()
	if j == nil || j.Owner != owner {
		return Result{}, errors.New("job not found")
	}
	if maxBytes > 0 && maxBytes < limit {
		limit = maxBytes
	}

	snap := j.snapshot()
	f, err := os.Open(j.OutputPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return Result{Snapshot: snap}, nil
		}
		return Result{}, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return Result{}, err
	}
	size := st.Size()
	start := int64(0)
	truncated := false
	if size > limit {
		start = size - limit
		truncated = true
	}
	if _, err := f.Seek(start, io.SeekStart); err != nil {
		return Result{}, err
	}
	b, err := io.ReadAll(io.LimitReader(f, limit))
	if err != nil {
		return Result{}, err
	}
	return Result{Snapshot: snap, Output: string(b), Truncated: truncated}, nil
}

func (s *store) sweepOrphanOutputs(cutoff time.Time) {
	jobsDir := filepath.Join(s.cfg.DataDir, "jobs")
	live := make(map[string]struct{})
	s.mu.Lock()
	for _, j := range s.jobs {
		j.mu.Lock()
		path := j.OutputPath
		j.mu.Unlock()
		if path != "" {
			live[path] = struct{}{}
		}
	}
	s.mu.Unlock()

	entries, err := os.ReadDir(jobsDir)
	if err != nil {
		return
	}
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".log" {
			continue
		}
		path := filepath.Join(jobsDir, entry.Name())
		if _, ok := live[path]; ok {
			continue
		}
		info, err := entry.Info()
		if err != nil || !info.ModTime().Before(cutoff) {
			continue
		}
		_ = os.Remove(path)
	}
}

func (s *store) protectedJobsLocked(cutoff time.Time) map[string]struct{} {
	protected := make(map[string]struct{})

	// A non-terminal job still needs its dependency records to decide whether it
	// may run or must block. Never GC those dependencies out from under it.
	for _, j := range s.jobs {
		j.mu.Lock()
		terminal := isTerminalState(j.State)
		deps := append([]string(nil), j.DependsOn...)
		j.mu.Unlock()
		if terminal {
			continue
		}
		for _, depID := range deps {
			protected[depID] = struct{}{}
		}
	}

	// A keyed graph is an idempotent generation: retain every node record until
	// the whole graph is terminal and the newest terminal node has aged past the
	// retention window. This prevents partial replay of a still-live generation.
	for key, rec := range s.graphs {
		allPresent := true
		allTerminal := true
		latestFinished := time.Time{}
		for _, jobID := range rec.JobIDs {
			j := s.jobs[jobID]
			if j == nil {
				allPresent = false
				break
			}
			j.mu.Lock()
			terminal := isTerminalState(j.State)
			finished := j.FinishedAt
			j.mu.Unlock()
			if !terminal {
				allTerminal = false
			}
			if finished.After(latestFinished) {
				latestFinished = finished
			}
		}
		if !allPresent {
			// Legacy/partial state cannot satisfy exact graph replay; drop the key
			// rather than retaining an incomplete generation indefinitely.
			delete(s.graphs, key)
			continue
		}
		if allTerminal && !latestFinished.IsZero() && latestFinished.Before(cutoff) {
			delete(s.graphs, key)
			continue
		}
		for _, jobID := range rec.JobIDs {
			protected[jobID] = struct{}{}
		}
	}
	return protected
}

func (s *store) gcLoop() {
	t := time.NewTicker(time.Minute)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			cutoff := time.Now().Add(-s.cfg.Retention)
			var removePaths []string
			s.mu.Lock()
			protected := s.protectedJobsLocked(cutoff)
			for id, j := range s.jobs {
				j.mu.Lock()
				terminal := isTerminalState(j.State)
				expired := terminal && !j.FinishedAt.IsZero() && j.FinishedAt.Before(cutoff)
				path := j.OutputPath
				owner := j.Owner
				idempotencyKey := j.IdempotencyKey
				j.mu.Unlock()
				if expired {
					if _, keep := protected[id]; keep {
						continue
					}
					if idempotencyKey != "" {
						mapKey := idempotencyMapKey(owner, idempotencyKey)
						if s.idempotency[mapKey] == id {
							delete(s.idempotency, mapKey)
						}
					}
					delete(s.jobs, id)
					removePaths = append(removePaths, path)
				}
			}
			s.mu.Unlock()
			for _, p := range removePaths {
				_ = os.Remove(p)
			}
			s.sweepOrphanOutputs(cutoff)
		case <-s.stop:
			return
		}
	}
}

func (s *store) shutdown() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	close(s.stop)
	var cancels []context.CancelFunc
	for _, j := range s.jobs {
		j.mu.Lock()
		if j.State == "running" {
			j.CancelRequested = true
			if j.cancel != nil {
				cancels = append(cancels, j.cancel)
			}
		} else if j.State == "queued" {
			j.State = "canceled"
			j.CancelRequested = true
			j.FinishedAt = time.Now()
		}
		j.mu.Unlock()
	}
	s.cond.Broadcast()
	s.mu.Unlock()
	for _, cancel := range cancels {
		cancel()
	}
}
