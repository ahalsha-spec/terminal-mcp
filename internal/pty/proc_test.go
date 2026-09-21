package pty

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestProcEchoAndSnapshot(t *testing.T) {
	p, err := NewProcSession(filepath.Join(t.TempDir(), "s.raw"), 1<<20, "bash", "--norc", "--noprofile")
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	p.Write("echo hi\n")
	time.Sleep(300 * time.Millisecond)
	if !strings.Contains(p.Since(0), "hi") {
		t.Fatalf("buffer missing output: %q", p.Since(0))
	}
	if p.Len() == 0 {
		t.Fatal("len 0")
	}
	if time.Since(p.LastByteTime()) > time.Second {
		t.Fatal("lastByteTime not updated")
	}
}

func TestProcCloseKillsChildTree(t *testing.T) {
	p, err := NewProcSession(filepath.Join(t.TempDir(), "s.raw"), 1<<20, "bash", "--norc", "--noprofile")
	if err != nil {
		t.Fatal(err)
	}
	pgid := p.cmd.Process.Pid // Setpgid 令 pgid==pid
	p.Write("sleep 300\n")    // 前台孙进程，应随整组一起被杀
	time.Sleep(300 * time.Millisecond)
	// 关闭前进程组应存在
	if err := syscall.Kill(-pgid, syscall.Signal(0)); err != nil {
		t.Fatalf("process group not alive before close: %v", err)
	}
	p.Close()
	// 关闭后进程组应整体消失（ESRCH）
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if err := syscall.Kill(-pgid, syscall.Signal(0)); err == syscall.ESRCH {
			return // 整组已回收，无遗留
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("process group still alive after close: leaked child process")
}

func TestProcFlushInput(t *testing.T) {
	p, err := NewProcSession(filepath.Join(t.TempDir(), "s.raw"), 1<<20, "bash", "--norc", "--noprofile")
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	p.Write("echo \"unterminated")
	time.Sleep(100 * time.Millisecond)
	if err := p.FlushInput(); err != nil {
		t.Fatalf("flushInput: %v", err)
	}
	p.KillLine()
	p.Write("\n")
	off := p.Len()
	p.Write("echo clean\n")
	time.Sleep(300 * time.Millisecond)
	if !strings.Contains(p.Since(off), "clean") {
		t.Fatalf("session not clean after flush: %q", p.Since(off))
	}
}

func TestProcTranscriptWriteThrough(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "t.raw")
	p, err := NewProcSession(logPath, 1<<20, "bash", "--norc", "--noprofile")
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	p.Write("echo hi\n")
	time.Sleep(300 * time.Millisecond)
	b, _ := os.ReadFile(logPath)
	if !strings.Contains(string(b), "hi") {
		t.Fatalf("transcript missing output: %q", string(b))
	}
}

func TestProcCloseIsIdempotent(t *testing.T) {
	p, err := NewProcSession(filepath.Join(t.TempDir(), "close-twice.raw"), 1<<20, "bash", "--norc", "--noprofile")
	if err != nil {
		t.Fatal(err)
	}
	p.Close()
	p.Close()
}

func TestProcCloseKillsDetachedSetsidDescendant(t *testing.T) {
	p, err := NewProcSession(filepath.Join(t.TempDir(), "setsid.raw"), 1<<20, "bash", "--norc", "--noprofile")
	if err != nil {
		t.Fatal(err)
	}
	pidFile := filepath.Join(t.TempDir(), "setsid.pid")
	p.Write("setsid sh -c 'echo $$ > " + pidFile + "; exec sleep 300' </dev/null >/dev/null 2>&1 &\n")
	var pid int
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		b, rerr := os.ReadFile(pidFile)
		if rerr == nil {
			if _, err := fmt.Sscanf(strings.TrimSpace(string(b)), "%d", &pid); err == nil && pid > 0 {
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	if pid <= 0 {
		p.Close()
		t.Fatal("detached setsid child never reported pid")
	}
	p.Close()
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		b, rerr := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
		if os.IsNotExist(rerr) {
			return
		}
		if rerr == nil {
			end := strings.LastIndexByte(string(b), ')')
			if end >= 0 {
				fields := strings.Fields(string(b[end+1:]))
				if len(fields) > 0 && fields[0] == "Z" {
					return
				}
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	_ = syscall.Kill(pid, syscall.SIGKILL)
	t.Fatalf("detached setsid child %d survived ProcSession.Close", pid)
}
