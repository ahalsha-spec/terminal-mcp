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
	Active            int     `json:"active"`
	MaxActive         int     `json:"max_active"`
	Queued            int     `json:"queued"`
	MaxQueued         int     `json:"max_queued"`
	CPUHeavy          int     `json:"cpu_heavy_active"`
	MaxCPUHeavy       int     `json:"max_cpu_heavy"`
	AdaptiveMaxActive int     `json:"adaptive_max_active"`
	Adaptive          bool    `json:"adaptive"`
	EffectiveCPU      int     `json:"effective_cpu"`
	CPUSomeAvg10      float64 `json:"cpu_some_avg10"`
	MemorySomeAvg10   float64 `json:"memory_some_avg10"`
	IOFullAvg10       float64 `json:"io_full_avg10"`
	MemoryAvailableMB uint64  `json:"memory_available_mb"`
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
	s := &store{
		cfg:         c,
		jobs:        map[string]*Job{},
		queues:      map[string][]*Job{},
		idempotency: map[string]string{},
		locks:       map[string]string{},
		graphs:      map[string]*graphRecord{},
		stop:        make(chan struct{}),
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

	if in.SupersedeKey != "" {
		for _, existing := range s.jobs {
			if existing.Owner != owner || existing.SupersedeKey != in.SupersedeKey {
				continue
			}
			existing.mu.Lock()
			switch existing.State {
			case "queued":
				existing.State = "canceled"
				existing.CancelRequested = true
				existing.FinishedAt = time.Now()
				s.removeQueuedLocked(existing)
			case "running":
				existing.CancelRequested = true
				if existing.cancel != nil {
					cancels = append(cancels, existing.cancel)
				}
			}
			existing.mu.Unlock()
		}
	}

	queued := s.queuedCountLocked()
	if queued >= s.cfg.MaxQueued {
		s.mu.Unlock()
		for _, cancel := range cancels {
			cancel()
		}
		return Snapshot{}, fmt.Errorf("job queue full: %d/%d", queued, s.cfg.MaxQueued)
	}
	if len(s.queues[owner]) >= s.cfg.MaxQueuedPerOwner {
		s.mu.Unlock()
		for _, cancel := range cancels {
			cancel()
		}
		return Snapshot{}, fmt.Errorf("owner job queue full: %d/%d", len(s.queues[owner]), s.cfg.MaxQueuedPerOwner)
	}

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

func (s *store) nextRunnableLocked() *Job {
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

	cmd := exec.CommandContext(ctx, "/bin/bash", "-lc", j.Command)
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
		s.finish(j, -1, err, false, false)
		return
	}
	j.mu.Lock()
	j.proc = cmd.Process
	j.mu.Unlock()

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
			if cmd.Process != nil {
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
	capacity := Capacity{
		Active: s.active, MaxActive: s.cfg.MaxActive,
		Queued: s.queuedCountLocked(), MaxQueued: s.cfg.MaxQueued,
		CPUHeavy: s.activeCPUHeavy, MaxCPUHeavy: s.cfg.MaxCPUHeavy,
		AdaptiveMaxActive: s.adaptiveMax, Adaptive: !s.cfg.DisableAdaptive,
		EffectiveCPU: s.pressure.EffectiveCPU,
		CPUSomeAvg10: s.pressure.CPUSomeAvg10, MemorySomeAvg10: s.pressure.MemorySomeAvg10,
		IOFullAvg10: s.pressure.IOFullAvg10, MemoryAvailableMB: s.pressure.MemoryAvailableBytes >> 20,
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

func (s *store) gcLoop() {
	t := time.NewTicker(time.Minute)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			cutoff := time.Now().Add(-s.cfg.Retention)
			var removePaths []string
			s.mu.Lock()
			for id, j := range s.jobs {
				j.mu.Lock()
				terminal := isTerminalState(j.State)
				expired := terminal && !j.FinishedAt.IsZero() && j.FinishedAt.Before(cutoff)
				path := j.OutputPath
				owner := j.Owner
				idempotencyKey := j.IdempotencyKey
				j.mu.Unlock()
				if expired {
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
			for key, rec := range s.graphs {
				if rec.CreatedAt.After(cutoff) {
					continue
				}
				allGone := true
				for _, jobID := range rec.JobIDs {
					if _, exists := s.jobs[jobID]; exists {
						allGone = false
						break
					}
				}
				if allGone {
					delete(s.graphs, key)
				}
			}
			s.mu.Unlock()
			for _, p := range removePaths {
				_ = os.Remove(p)
			}
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
