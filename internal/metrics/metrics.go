// Package metrics 压测指标: 分片延迟直方图 + 设备侧步骤统计 + JSON 快照。
package metrics

import (
	"sync"
	"sync/atomic"

	"github.com/HdrHistogram/hdrhistogram-go"
)

// Hist 分片直方图: Record 无锁(仅原子游标), 适合每秒十几万次记录。
// 以纳秒记录, 量程 1ns ~ 130s, 有效数字 2 位(5% 分辨率足够容量结论)。
type Hist struct {
	shards [8]*histShard
}

type histShard struct {
	mu sync.Mutex
	h  *hdrhistogram.Histogram
}

func NewHist() *Hist {
	h := &Hist{}
	for i := range h.shards {
		h.shards[i] = &histShard{h: hdrhistogram.New(1, 130_000_000_000, 2)}
	}
	return h
}

var histCursor atomic.Uint64

func (h *Hist) Record(nanos int64) {
	s := h.shards[histCursor.Add(1)%uint64(len(h.shards))]
	s.mu.Lock()
	_ = s.h.RecordValue(nanos)
	s.mu.Unlock()
}

// Snapshot 合并出百分位快照(单位 ns)。空直方图返回零值。
func (h *Hist) Snapshot() HistSnapshot {
	merged := hdrhistogram.New(1, 130_000_000_000, 2)
	for _, s := range h.shards {
		s.mu.Lock()
		merged.Merge(s.h)
		s.mu.Unlock()
	}
	if merged.TotalCount() == 0 {
		return HistSnapshot{}
	}
	return HistSnapshot{
		Count: merged.TotalCount(),
		P50:   merged.ValueAtQuantile(50),
		P95:   merged.ValueAtQuantile(95),
		P99:   merged.ValueAtQuantile(99),
		Max:   merged.Max(),
		Mean:  int64(merged.Mean()),
	}
}

// HistSnapshot 百分位快照, 单位 ns
type HistSnapshot struct {
	Count int64 `json:"count"`
	P50   int64 `json:"p50_ns"`
	P95   int64 `json:"p95_ns"`
	P99   int64 `json:"p99_ns"`
	Max   int64 `json:"max_ns"`
	Mean  int64 `json:"mean_ns"`
}

// StepStats 单个负载步的设备侧统计(全部原子, 调度/读回调直接写)
type StepStats struct {
	Index       int
	Type        string // connect | soak | load
	RateTarget  int64
	ConnsTarget int64 // connect 步: 累计目标连接数(全局)
	DurationS   int64

	Sent       atomic.Int64 // 本步发出的业务消息(含基线)
	Baseline   atomic.Int64 // 其中基线周期消息
	Acked      atomic.Int64 // 收到 PUBACK
	PubTimeout atomic.Int64 // 发布写失败
	EchoRecv   atomic.Int64 // 收到回显
	EchoDup    atomic.Int64 // 乱序/重复回显

	AckRTT *Hist // publish → PUBACK
	RTT    *Hist // publish → 回显收到(全环)
}

func NewStep(index int, typ string, rate, durS, conns int64) *StepStats {
	return &StepStats{Index: index, Type: typ, RateTarget: rate, DurationS: durS, ConnsTarget: conns,
		AckRTT: NewHist(), RTT: NewHist()}
}

// StepSnapshot JSON 序列化形态
type StepSnapshot struct {
	Index       int    `json:"index"`
	Type        string `json:"type"`
	RateTarget  int64  `json:"rate_target,omitempty"`
	ConnsTarget int64  `json:"conns_target,omitempty"`
	DurationS   int64  `json:"duration_s,omitempty"`

	Sent       int64 `json:"sent"`
	Baseline   int64 `json:"baseline"`
	Acked      int64 `json:"acked"`
	PubTimeout int64 `json:"pub_error"`
	EchoRecv   int64 `json:"echo_recv"`
	EchoDup    int64 `json:"echo_dup"`

	AckRTT HistSnapshot `json:"ack_rtt"`
	RTT    HistSnapshot `json:"rtt"`
}

// Snapshot 输出可序列化形态
func (s *StepStats) Snapshot() StepSnapshot {
	return StepSnapshot{
		Index: s.Index, Type: s.Type, RateTarget: s.RateTarget, ConnsTarget: s.ConnsTarget,
		DurationS: s.DurationS,
		Sent:      s.Sent.Load(), Baseline: s.Baseline.Load(), Acked: s.Acked.Load(),
		PubTimeout: s.PubTimeout.Load(), EchoRecv: s.EchoRecv.Load(), EchoDup: s.EchoDup.Load(),
		AckRTT: s.AckRTT.Snapshot(), RTT: s.RTT.Snapshot(),
	}
}
