package observe

import (
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

// NodeTarget 一台被观测主机(其 node-exporter 地址)
type NodeTarget struct {
	Name string `yaml:"name" json:"name"`
	Addr string `yaml:"addr" json:"addr"` // host 或 host:port, 缺省端口 9100
}

// NodeSample 单机瞬时资源占用
type NodeSample struct {
	Name  string  `json:"name"`
	CPU   float64 `json:"cpu_pct"` // 与上次采样间的使用率(1 - Δidle/Δtotal)
	Mem   float64 `json:"mem_pct"`
	Load1 float64 `json:"load1"`
}

// NodeSnapshot 一轮全体主机采样
type NodeSnapshot struct {
	T     time.Time    `json:"t"`
	Nodes []NodeSample `json:"nodes"`
}

type cpuTimes struct{ idle, total float64 }

// NodePoller 周期抓取各机 node-exporter /metrics, 供报告输出「各机资源占用」。
// 只解析 CPU/内存/load 三个家族, 不引依赖。
type NodePoller struct {
	targets  []NodeTarget
	interval time.Duration
	client   *http.Client
	mu       sync.Mutex
	snaps    []NodeSnapshot
	stop     chan struct{}
	prev     map[string]cpuTimes
	hasPrev  bool
}

func NewNodePoller(targets []NodeTarget, interval time.Duration) *NodePoller {
	return &NodePoller{
		targets:  targets,
		interval: interval,
		client:   &http.Client{Timeout: 3 * time.Second},
		stop:     make(chan struct{}),
		prev:     make(map[string]cpuTimes),
	}
}

func (p *NodePoller) Start() {
	go func() {
		t := time.NewTicker(p.interval)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				p.poll()
			case <-p.stop:
				return
			}
		}
	}()
}

func (p *NodePoller) Stop() []NodeSnapshot {
	close(p.stop)
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.snaps
}

func (p *NodePoller) poll() {
	snap := NodeSnapshot{T: time.Now()}
	for _, tgt := range p.targets {
		if s, ok := p.sample(tgt); ok {
			snap.Nodes = append(snap.Nodes, s)
		}
	}
	p.mu.Lock()
	p.snaps = append(p.snaps, snap)
	p.mu.Unlock()
}

func (p *NodePoller) sample(tgt NodeTarget) (NodeSample, bool) {
	addr := tgt.Addr
	if !strings.Contains(addr, ":") {
		addr += ":9100"
	}
	resp, err := p.client.Get("http://" + addr + "/metrics")
	if err != nil {
		return NodeSample{}, false
	}
	defer resp.Body.Close()
	buf := make([]byte, 0, 256<<10)
	tmp := make([]byte, 32<<10)
	for {
		n, err := resp.Body.Read(tmp)
		buf = append(buf, tmp[:n]...)
		if err != nil {
			break
		}
	}
	var cpu map[string]float64
	var memTotal, memAvail, load1 float64
	var hasMem, hasLoad bool
	for _, line := range strings.Split(string(buf), "\n") {
		switch {
		case strings.HasPrefix(line, "node_cpu_seconds_total{"):
			mode := between(line, `mode="`, `"`)
			if v, ok := lastNumber(line); ok {
				if cpu == nil {
					cpu = make(map[string]float64)
				}
				cpu[mode] += v
			}
		case strings.HasPrefix(line, "node_memory_MemTotal_bytes"):
			memTotal, _ = lastNumber(line)
			hasMem = true
		case strings.HasPrefix(line, "node_memory_MemAvailable_bytes"):
			memAvail, _ = lastNumber(line)
		case strings.HasPrefix(line, "node_load1 "):
			load1, _ = lastNumber(line)
			hasLoad = true
		}
	}
	s := NodeSample{Name: tgt.Name, Load1: load1}
	if hasMem && memTotal > 0 {
		s.Mem = (1 - memAvail/memTotal) * 100
	}
	// CPU 使用率需要与上次采样的差分; 首轮只有基线, 从第二轮起出数
	if cpu != nil {
		var idle, total float64
		for m, v := range cpu {
			total += v
			if m == "idle" {
				idle = v
			}
		}
		key := tgt.Addr
		if p.hasPrev {
			if pt, ok := p.prev[key]; ok {
				dTotal, dIdle := total-pt.total, idle-pt.idle
				if dTotal > 0 {
					s.CPU = clamp(100 - dIdle/dTotal*100)
				}
			}
		}
		p.prev[key] = cpuTimes{idle: idle, total: total}
		p.hasPrev = true
	} else {
		return NodeSample{}, false
	}
	if !hasLoad && !hasMem {
		return NodeSample{}, false
	}
	return s, true
}

func between(s, a, b string) string {
	i := strings.Index(s, a)
	if i < 0 {
		return ""
	}
	rest := s[i+len(a):]
	j := strings.Index(rest, b)
	if j < 0 {
		return ""
	}
	return rest[:j]
}

func lastNumber(line string) (float64, bool) {
	i := strings.LastIndex(line, " ")
	if i < 0 {
		return 0, false
	}
	v, err := strconv.ParseFloat(strings.TrimSpace(line[i+1:]), 64)
	return v, err == nil
}

func clamp(v float64) float64 {
	if v < 0 {
		return 0
	}
	if v > 100 {
		return 100
	}
	return v
}

// 合并多轮采样输出各机摘要(报告用)
type NodeSummary struct {
	Name    string  `json:"name"`
	CPUAvg  float64 `json:"cpu_avg"`
	CPUMax  float64 `json:"cpu_max"`
	MemAvg  float64 `json:"mem_avg"`
	MemMax  float64 `json:"mem_max"`
	LoadMax float64 `json:"load_max"`
	Samples int     `json:"samples"`
}

// Summarize 把快照序列折叠成每台主机的平均/峰值。样本数 <2 时 CPU 列无意义, 调用方自行判断。
func Summarize(snaps []NodeSnapshot) []NodeSummary {
	type acc struct {
		cpuSum, cpuMax, memSum, memMax, loadMax float64
		n                                       int
	}
	order := []string{}
	m := map[string]*acc{}
	for _, sn := range snaps {
		for _, s := range sn.Nodes {
			a, ok := m[s.Name]
			if !ok {
				a = &acc{}
				m[s.Name] = a
				order = append(order, s.Name)
			}
			a.cpuSum += s.CPU
			a.memSum += s.Mem
			if s.CPU > a.cpuMax {
				a.cpuMax = s.CPU
			}
			if s.Mem > a.memMax {
				a.memMax = s.Mem
			}
			if s.Load1 > a.loadMax {
				a.loadMax = s.Load1
			}
			a.n++
		}
	}
	out := make([]NodeSummary, 0, len(order))
	for _, name := range order {
		a := m[name]
		out = append(out, NodeSummary{
			Name: name, CPUAvg: a.cpuSum / float64(a.n), CPUMax: a.cpuMax,
			MemAvg: a.memSum / float64(a.n), MemMax: a.memMax,
			LoadMax: a.loadMax, Samples: a.n,
		})
	}
	return out
}
