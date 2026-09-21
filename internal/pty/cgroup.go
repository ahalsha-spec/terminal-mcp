package pty

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const ptyWorkerCgroupPrefix = "mu-pty-worker-"

var (
	ptyWorkerNameOnce    sync.Once
	ptyWorkerName        string
	ptyWorkerNameErr     error
	ptyCgroupPrepareOnce sync.Once
	ptyCgroupPrepareErr  error
	ptySessionSeq        atomic.Uint64
)

type sessionCgroup struct{ path string }

func selfCgroupPath() string {
	b, err := os.ReadFile("/proc/self/cgroup")
	if err != nil {
		return ""
	}
	for _, line := range strings.Split(string(b), "\n") {
		parts := strings.SplitN(line, ":", 3)
		if len(parts) == 3 && parts[0] == "0" && parts[1] == "" {
			return strings.TrimPrefix(parts[2], "/")
		}
	}
	return ""
}

func processStartTicks(pid int) (string, bool) {
	b, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return "", false
	}
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

func currentPTYWorkerName() (string, error) {
	ptyWorkerNameOnce.Do(func() {
		start, ok := processStartTicks(os.Getpid())
		if !ok {
			ptyWorkerNameErr = errors.New("cannot read current process start time for PTY cgroup namespace")
			return
		}
		ptyWorkerName = fmt.Sprintf("%s%d-%s", ptyWorkerCgroupPrefix, os.Getpid(), start)
	})
	return ptyWorkerName, ptyWorkerNameErr
}

func ptyWorkerOwnerAlive(name string) bool {
	rest := strings.TrimPrefix(name, ptyWorkerCgroupPrefix)
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

func currentCgroupBase() (string, bool) {
	rel := selfCgroupPath()
	if rel == "" {
		return "", false
	}
	base := filepath.Join("/sys/fs/cgroup", rel)
	if _, err := os.Stat(filepath.Join(base, "cgroup.controllers")); err != nil {
		return "", false
	}
	if _, err := os.Stat(filepath.Join(base, "cgroup.kill")); err != nil {
		return "", false
	}
	return base, true
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

func terminateAndRemoveCgroup(path string) error {
	if path == "" {
		return nil
	}
	var firstErr error
	if err := os.WriteFile(filepath.Join(path, "cgroup.kill"), []byte("1"), 0o600); err != nil && !errors.Is(err, os.ErrNotExist) {
		firstErr = fmt.Errorf("kill PTY cgroup: %w", err)
	}
	deadline := time.Now().Add(750 * time.Millisecond)
	for time.Now().Before(deadline) {
		b, err := os.ReadFile(filepath.Join(path, "cgroup.events"))
		if errors.Is(err, os.ErrNotExist) {
			break
		}
		if err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("read PTY cgroup state: %w", err)
			}
			break
		}
		if strings.Contains(string(b), "populated 0") {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	var removeErr error
	for i := 0; i < 25; i++ {
		removeErr = removeCgroupTree(path)
		if removeErr == nil || errors.Is(removeErr, os.ErrNotExist) {
			return firstErr
		}
		if !errors.Is(removeErr, syscall.EBUSY) && !errors.Is(removeErr, syscall.ENOTEMPTY) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	wrapped := fmt.Errorf("remove PTY cgroup: %w", removeErr)
	if firstErr == nil {
		return wrapped
	}
	return errors.Join(firstErr, wrapped)
}

func preparePTYCgroupIsolation() error {
	ptyCgroupPrepareOnce.Do(func() {
		base, ok := currentCgroupBase()
		if !ok {
			return
		}
		currentName, err := currentPTYWorkerName()
		if err != nil {
			ptyCgroupPrepareErr = err
			return
		}
		entries, err := os.ReadDir(base)
		if err != nil {
			ptyCgroupPrepareErr = fmt.Errorf("read PTY cgroup parent: %w", err)
			return
		}
		for _, entry := range entries {
			if !entry.IsDir() || !strings.HasPrefix(entry.Name(), ptyWorkerCgroupPrefix) {
				continue
			}
			if entry.Name() != currentName && ptyWorkerOwnerAlive(entry.Name()) {
				continue
			}
			if err := terminateAndRemoveCgroup(filepath.Join(base, entry.Name())); err != nil {
				ptyCgroupPrepareErr = fmt.Errorf("reap stale PTY worker cgroup %s: %w", entry.Name(), err)
				return
			}
		}
	})
	return ptyCgroupPrepareErr
}

func ensurePTYWorkerCgroup() (string, error) {
	base, ok := currentCgroupBase()
	if !ok {
		return "", nil
	}
	if err := preparePTYCgroupIsolation(); err != nil {
		return "", err
	}
	name, err := currentPTYWorkerName()
	if err != nil {
		return "", err
	}
	path := filepath.Join(base, name)
	if err := os.Mkdir(path, 0o700); err != nil && !errors.Is(err, os.ErrExist) {
		return "", fmt.Errorf("create PTY worker cgroup: %w", err)
	}
	if _, err := os.Stat(filepath.Join(path, "cgroup.kill")); err != nil {
		return "", fmt.Errorf("PTY worker cgroup lacks cgroup.kill: %w", err)
	}
	return path, nil
}

func createSessionCgroup() (*sessionCgroup, error) {
	worker, err := ensurePTYWorkerCgroup()
	if err != nil {
		return nil, err
	}
	if worker == "" {
		return nil, nil
	}
	path := filepath.Join(worker, fmt.Sprintf("session-%d", ptySessionSeq.Add(1)))
	if err := os.Mkdir(path, 0o700); err != nil {
		return nil, fmt.Errorf("create PTY session cgroup: %w", err)
	}
	if _, err := os.Stat(filepath.Join(path, "cgroup.kill")); err != nil {
		_ = os.Remove(path)
		return nil, fmt.Errorf("PTY session cgroup lacks cgroup.kill: %w", err)
	}
	return &sessionCgroup{path: path}, nil
}

func (g *sessionCgroup) attach(pid int) error {
	if g == nil {
		return nil
	}
	return os.WriteFile(filepath.Join(g.path, "cgroup.procs"), []byte(strconv.Itoa(pid)), 0o600)
}

func (g *sessionCgroup) close() error {
	if g == nil {
		return nil
	}
	err := terminateAndRemoveCgroup(g.path)
	if worker, werr := ensurePTYWorkerCgroup(); werr == nil && worker != "" {
		_ = os.Remove(worker)
	}
	return err
}
