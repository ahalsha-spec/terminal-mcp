package job

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	workerCgroupPrefix = "mu-worker-"
	jobCgroupPrefix    = "job-"
)

type jobCgroup struct {
	path string
}

var (
	workerNameOnce sync.Once
	workerName     string
	workerNameErr  error

	cgroupAvailabilityOnce sync.Once
	cgroupAvailable        bool
)

func currentCgroupV2Base() (string, bool) {
	rel := selfCgroupPath()
	if rel == "" {
		return "", false
	}
	base := filepath.Join("/sys/fs/cgroup", rel)
	if _, err := os.Stat(filepath.Join(base, "cgroup.controllers")); err != nil {
		return "", false
	}
	return base, true
}

func processStartTicks(pid int) (string, bool) {
	b, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return "", false
	}
	// /proc/<pid>/stat field 2 (comm) is parenthesized and may contain spaces.
	// After its final ')', fields[0] is field 3 (state), so starttime (field 22)
	// is fields[19].
	end := strings.LastIndexByte(string(b), ')')
	if end < 0 || end+1 >= len(b) {
		return "", false
	}
	fields := strings.Fields(string(b[end+1:]))
	if len(fields) <= 19 {
		return "", false
	}
	if _, err := strconv.ParseUint(fields[19], 10, 64); err != nil {
		return "", false
	}
	return fields[19], true
}

func currentWorkerCgroupName() (string, error) {
	workerNameOnce.Do(func() {
		start, ok := processStartTicks(os.Getpid())
		if !ok {
			workerNameErr = errors.New("cannot read current process start time for cgroup namespace")
			return
		}
		workerName = fmt.Sprintf("%s%d-%s", workerCgroupPrefix, os.Getpid(), start)
	})
	return workerName, workerNameErr
}

func workerNamespaceOwnerAlive(name string) bool {
	rest := strings.TrimPrefix(name, workerCgroupPrefix)
	parts := strings.SplitN(rest, "-", 2)
	if len(parts) != 2 {
		return false
	}
	pid, err := strconv.Atoi(parts[0])
	if err != nil || pid <= 0 {
		return false
	}
	start, ok := processStartTicks(pid)
	return ok && start == parts[1]
}

func ensureWorkerCgroup() (string, error) {
	base, ok := currentCgroupV2Base()
	if !ok {
		return "", nil
	}
	name, err := currentWorkerCgroupName()
	if err != nil {
		return "", err
	}
	path := filepath.Join(base, name)
	if err := os.Mkdir(path, 0o700); err != nil && !errors.Is(err, os.ErrExist) {
		return "", fmt.Errorf("create worker cgroup: %w", err)
	}
	if _, err := os.Stat(filepath.Join(path, "cgroup.kill")); err != nil {
		return "", fmt.Errorf("worker cgroup lacks cgroup.kill: %w", err)
	}
	return path, nil
}

func prepareJobCgroupIsolation() error {
	base, ok := currentCgroupV2Base()
	if !ok {
		return nil // portable process-group fallback
	}
	currentName, err := currentWorkerCgroupName()
	if err != nil {
		return err
	}
	entries, err := os.ReadDir(base)
	if err != nil {
		return fmt.Errorf("read cgroup parent: %w", err)
	}
	for _, entry := range entries {
		if !entry.IsDir() || !strings.HasPrefix(entry.Name(), workerCgroupPrefix) {
			continue
		}
		path := filepath.Join(base, entry.Name())
		if entry.Name() != currentName && workerNamespaceOwnerAlive(entry.Name()) {
			continue // active sibling Workstation/test process: never touch it
		}
		g := &jobCgroup{path: path}
		if err := g.terminateAndRemove(); err != nil {
			return fmt.Errorf("reap stale worker cgroup %s: %w", entry.Name(), err)
		}
	}
	_, err = ensureWorkerCgroup()
	return err
}

func createJobCgroup(jobID string) (*jobCgroup, error) {
	worker, err := ensureWorkerCgroup()
	if err != nil {
		return nil, err
	}
	if worker == "" {
		return nil, nil
	}
	path := filepath.Join(worker, jobCgroupPrefix+jobID)
	if err := os.Mkdir(path, 0o700); err != nil {
		return nil, fmt.Errorf("create job cgroup: %w", err)
	}
	if _, err := os.Stat(filepath.Join(path, "cgroup.kill")); err != nil {
		_ = os.Remove(path)
		return nil, fmt.Errorf("job cgroup lacks cgroup.kill: %w", err)
	}
	return &jobCgroup{path: path}, nil
}

func (g *jobCgroup) attach(pid int) error {
	if g == nil {
		return nil
	}
	return os.WriteFile(filepath.Join(g.path, "cgroup.procs"), []byte(strconv.Itoa(pid)), 0o600)
}

func (g *jobCgroup) killAll() error {
	if g == nil {
		return nil
	}
	err := os.WriteFile(filepath.Join(g.path, "cgroup.kill"), []byte("1"), 0o600)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

func (g *jobCgroup) populated() (bool, error) {
	if g == nil {
		return false, nil
	}
	b, err := os.ReadFile(filepath.Join(g.path, "cgroup.events"))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		return false, err
	}
	for _, line := range strings.Split(string(b), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 2 && fields[0] == "populated" {
			return fields[1] != "0", nil
		}
	}
	return false, nil
}

func removeCgroupTree(root string) error {
	var dirs []string
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() {
			dirs = append(dirs, path)
		}
		return nil
	})
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	sort.Slice(dirs, func(i, j int) bool {
		return strings.Count(dirs[i], string(os.PathSeparator)) > strings.Count(dirs[j], string(os.PathSeparator))
	})
	var lastErr error
	for _, dir := range dirs {
		if err := os.Remove(dir); err != nil && !errors.Is(err, os.ErrNotExist) {
			lastErr = err
		}
	}
	return lastErr
}

func (g *jobCgroup) terminateAndRemove() error {
	if g == nil {
		return nil
	}
	var firstErr error
	if err := g.killAll(); err != nil {
		firstErr = fmt.Errorf("kill job cgroup: %w", err)
	}
	deadline := time.Now().Add(750 * time.Millisecond)
	for {
		populated, err := g.populated()
		if err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("read job cgroup state: %w", err)
			}
			break
		}
		if !populated || time.Now().After(deadline) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	var removeErr error
	for i := 0; i < 25; i++ {
		removeErr = removeCgroupTree(g.path)
		if removeErr == nil || errors.Is(removeErr, os.ErrNotExist) {
			return firstErr
		}
		if !errors.Is(removeErr, syscall.EBUSY) && !errors.Is(removeErr, syscall.ENOTEMPTY) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	wrapped := fmt.Errorf("remove job cgroup: %w", removeErr)
	if firstErr == nil {
		return wrapped
	}
	return errors.Join(firstErr, wrapped)
}

func jobCgroupIsolationAvailable() bool {
	cgroupAvailabilityOnce.Do(func() {
		worker, err := ensureWorkerCgroup()
		if err != nil || worker == "" {
			return
		}
		probe := filepath.Join(worker, fmt.Sprintf("%sprobe-%d", jobCgroupPrefix, time.Now().UnixNano()))
		if err := os.Mkdir(probe, 0o700); err != nil {
			return
		}
		_, statErr := os.Stat(filepath.Join(probe, "cgroup.kill"))
		_ = os.Remove(probe)
		cgroupAvailable = statErr == nil
	})
	return cgroupAvailable
}
