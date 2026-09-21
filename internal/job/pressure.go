package job

import (
	"bufio"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"
)

type Pressure struct {
	SampledAt            time.Time
	EffectiveCPU         int
	CPUSomeAvg10         float64
	MemorySomeAvg10      float64
	IOFullAvg10          float64
	MemoryAvailableBytes uint64
	MemoryTotalBytes     uint64
}

func (p Pressure) MemoryAvailableRatio() float64 {
	if p.MemoryTotalBytes == 0 {
		return 1
	}
	return float64(p.MemoryAvailableBytes) / float64(p.MemoryTotalBytes)
}

func targetForPressure(hard int, p Pressure) int {
	if hard <= 0 {
		hard = 1
	}
	target := hard
	effective := p.EffectiveCPU
	if effective <= 0 {
		effective = runtime.GOMAXPROCS(0)
	}
	if effective > 0 && target > effective+2 {
		target = effective + 2
	}

	ratio := p.MemoryAvailableRatio()
	if ratio < 0.10 {
		target = 1
	} else if ratio < 0.20 && target > 2 {
		target = 2
	}

	if p.CPUSomeAvg10 >= 70 {
		cpuTarget := effective / 2
		if cpuTarget < 1 {
			cpuTarget = 1
		}
		if target > cpuTarget {
			target = cpuTarget
		}
	} else if p.CPUSomeAvg10 >= 40 {
		cpuTarget := effective
		if cpuTarget < 2 {
			cpuTarget = 2
		}
		if target > cpuTarget {
			target = cpuTarget
		}
	}

	if p.IOFullAvg10 >= 40 {
		ioTarget := effective
		if ioTarget < 1 {
			ioTarget = 1
		}
		if target > ioTarget {
			target = ioTarget
		}
	} else if p.IOFullAvg10 >= 20 {
		ioTarget := effective
		if ioTarget < 2 {
			ioTarget = 2
		}
		if target > ioTarget {
			target = ioTarget
		}
	}

	if target < 1 {
		target = 1
	}
	return target
}

func classAllowedByPressure(class string, p Pressure) bool {
	switch class {
	case ClassCPUHeavy:
		return p.CPUSomeAvg10 < 65 && p.MemoryAvailableRatio() >= 0.12
	case ClassIOWait:
		return p.IOFullAvg10 < 50 && p.MemoryAvailableRatio() >= 0.10
	case ClassBackground:
		return p.CPUSomeAvg10 < 20 &&
			p.MemorySomeAvg10 < 2 &&
			p.IOFullAvg10 < 10 &&
			p.MemoryAvailableRatio() >= 0.25
	default:
		return p.MemoryAvailableRatio() >= 0.08
	}
}

func readPressure() Pressure {
	p := Pressure{
		SampledAt:    time.Now(),
		EffectiveCPU: runtime.GOMAXPROCS(0),
	}
	cg := selfCgroupPath()
	if cg != "" {
		if v := effectiveCPUFromMax(filepath.Join("/sys/fs/cgroup", cg, "cpu.max")); v > 0 {
			p.EffectiveCPU = v
		}
		p.CPUSomeAvg10 = readPSI(filepath.Join("/sys/fs/cgroup", cg, "cpu.pressure"), "some")
		p.MemorySomeAvg10 = readPSI(filepath.Join("/sys/fs/cgroup", cg, "memory.pressure"), "some")
		p.IOFullAvg10 = readPSI(filepath.Join("/sys/fs/cgroup", cg, "io.pressure"), "full")
	} else {
		p.CPUSomeAvg10 = readPSI("/proc/pressure/cpu", "some")
		p.MemorySomeAvg10 = readPSI("/proc/pressure/memory", "some")
		p.IOFullAvg10 = readPSI("/proc/pressure/io", "full")
	}
	p.MemoryTotalBytes, p.MemoryAvailableBytes = readMemInfo()
	return p
}

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

func effectiveCPUFromMax(path string) int {
	b, err := os.ReadFile(path)
	if err != nil {
		return 0
	}
	fields := strings.Fields(string(b))
	if len(fields) < 2 || fields[0] == "max" {
		return 0
	}
	quota, err1 := strconv.ParseFloat(fields[0], 64)
	period, err2 := strconv.ParseFloat(fields[1], 64)
	if err1 != nil || err2 != nil || quota <= 0 || period <= 0 {
		return 0
	}
	n := int(math.Ceil(quota / period))
	if n < 1 {
		n = 1
	}
	return n
}

func readPSI(path, kind string) float64 {
	f, err := os.Open(path)
	if err != nil {
		return 0
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) == 0 || fields[0] != kind {
			continue
		}
		for _, field := range fields[1:] {
			if !strings.HasPrefix(field, "avg10=") {
				continue
			}
			v, err := strconv.ParseFloat(strings.TrimPrefix(field, "avg10="), 64)
			if err == nil {
				return v
			}
		}
	}
	return 0
}

func readMemInfo() (total, available uint64) {
	f, err := os.Open("/proc/meminfo")
	if err != nil {
		return 0, 0
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) < 2 {
			continue
		}
		v, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			continue
		}
		switch strings.TrimSuffix(fields[0], ":") {
		case "MemTotal":
			total = v * 1024
		case "MemAvailable":
			available = v * 1024
		}
	}
	return total, available
}

func (s *store) pressureLoop() {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			sample := readPressure()
			target := targetForPressure(s.cfg.MaxActive, sample)
			s.mu.Lock()
			s.pressure = sample
			s.adaptiveMax = target
			s.cond.Broadcast()
			s.mu.Unlock()
		case <-s.stop:
			return
		}
	}
}
