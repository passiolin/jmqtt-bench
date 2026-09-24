// Package report 聚合 device/backend JSON 产物为 Markdown 报告。
// 核心是三集合差做丢失归因:
//
//	设备已发 − backend 已消费  = 上行腿丢失(含 backend gap 计数印证)
//	backend 已回显 − 设备已收  = 下行腿丢失
package report

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"jmqtt-bench/internal/observe"
)

type StepSnapshot struct {
	Index       int    `json:"index"`
	Type        string `json:"type"`
	RateTarget  int64  `json:"rate_target"`
	ConnsTarget int64  `json:"conns_target"`
	DurationS   int64  `json:"duration_s"`
	Sent        int64  `json:"sent"`
	Baseline    int64  `json:"baseline"`
	Acked       int64  `json:"acked"`
	PubError    int64  `json:"pub_error"`
	EchoRecv    int64  `json:"echo_recv"`
	EchoDup     int64  `json:"echo_dup"`
	AckRTT      Hist   `json:"ack_rtt"`
	RTT         Hist   `json:"rtt"`
}

type Hist struct {
	Count int64 `json:"count"`
	P50   int64 `json:"p50_ns"`
	P95   int64 `json:"p95_ns"`
	P99   int64 `json:"p99_ns"`
	Max   int64 `json:"max_ns"`
	Mean  int64 `json:"mean_ns"`
}

type runFile struct {
	RunID         string                 `json:"run_id"`
	Role          string                 `json:"role"`
	InstanceIndex int                    `json:"instance_index"`
	Shard         []int                  `json:"shard"`
	T0            int64                  `json:"t0"`
	EndedAt       time.Time              `json:"ended_at"`
	Aborted       bool                   `json:"aborted"`
	Totals        map[string]int64       `json:"totals"`
	Steps         []StepSnapshot         `json:"steps"`
	NodeStats     []observe.NodeSnapshot `json:"node_stats"`
}

type backendStep struct {
	Index     int    `json:"index"`
	Type      string `json:"type"`
	Consumed  int64  `json:"consumed"`
	EchoOK    int64  `json:"echo_ok"`
	EchoErr   int64  `json:"echo_err"`
	Gaps      int64  `json:"uplink_gap"`
	Dup       int64  `json:"dup"`
	Bad       int64  `json:"bad_records"`
	UpLatency Hist   `json:"uplink_latency"`
}

type backendFile struct {
	RunID         string        `json:"run_id"`
	Role          string        `json:"role"`
	InstanceIndex int           `json:"instance_index"`
	T0            int64         `json:"t0"`
	Steps         []backendStep `json:"steps"`
}

// Generate 读 outDir 下所有产物, 输出 Markdown 报告
func Generate(outDir string) (string, error) {
	devFiles, _ := filepath.Glob(filepath.Join(outDir, "device-*.json"))
	beFiles, _ := filepath.Glob(filepath.Join(outDir, "backend-*.json"))
	if len(devFiles) == 0 {
		return "", fmt.Errorf("no device-*.json in %s", outDir)
	}

	var devs []runFile
	for _, f := range devFiles {
		var r runFile
		if err := readJSON(f, &r); err != nil {
			return "", fmt.Errorf("read %s: %w", f, err)
		}
		devs = append(devs, r)
	}
	sort.Slice(devs, func(i, j int) bool { return devs[i].InstanceIndex < devs[j].InstanceIndex })

	var backends []backendFile
	for _, f := range beFiles {
		var b backendFile
		if err := readJSON(f, &b); err != nil {
			return "", fmt.Errorf("read %s: %w", f, err)
		}
		backends = append(backends, b)
	}
	sort.Slice(backends, func(i, j int) bool { return backends[i].InstanceIndex < backends[j].InstanceIndex })

	nSteps := len(devs[0].Steps)
	var b strings.Builder
	w := func(format string, args ...any) { fmt.Fprintf(&b, format+"\n", args...) }

	w("# mqtt-bench 报告: %s", devs[0].RunID)
	w("")
	w("- 实例: %d device (分片合计 %d 设备), %d backend", len(devs), totalDevices(devs), len(backends))
	w("- T0: %s", time.Unix(devs[0].T0, 0).Format("2006-01-02 15:04:05"))
	aborted := false
	var totConn, totFail, totReconn int64
	for _, d := range devs {
		if d.Aborted {
			aborted = true
		}
		totConn += d.Totals["connect_ok"]
		totFail += d.Totals["connect_failed"]
		totReconn += d.Totals["reconnects"]
	}
	w("- 连接: ok=%d fail=%d reconnects=%d", totConn, totFail, totReconn)
	if aborted {
		w("- **有实例被安全阀中止**, 结果为部分结果")
	}
	w("")

	w("## 逐步结果")
	w("")
	w("指令 TPS 目标 = 连接数/60(每设备每 60s 一条指令, 心跳 PING 同频, 不占消息口径)")
	w("")
	w("| 步 | 类型 | 目标连接 | 指令TPS目标 | 实测TPS | 发送 | 确认 | 上行丢失 | 上行重复 | 回显收到 | 回显丢失 | ACK RTT P50/P95/P99 (ms) | 全环 RTT P50/P95/P99 (ms) |")
	w("|---|---|---|---|---|---|---|---|---|---|---|---|---|")
	var gSent, gAcked, gEcho, gGap, gDup, gEchoRecv int64
	for i := 0; i < nSteps; i++ {
		st := devs[0].Steps[i]
		var sent, acked, echoRecv, echoDup, baseline int64
		for _, d := range devs {
			if i < len(d.Steps) {
				sent += d.Steps[i].Sent
				acked += d.Steps[i].Acked
				echoRecv += d.Steps[i].EchoRecv
				echoDup += d.Steps[i].EchoDup
				baseline += d.Steps[i].Baseline
			}
		}
		var consumed, echoOK, gaps, dup int64
		for _, be := range backends {
			if i < len(be.Steps) {
				consumed += be.Steps[i].Consumed
				echoOK += be.Steps[i].EchoOK
				gaps += be.Steps[i].Gaps
				dup += be.Steps[i].Dup
			}
		}
		var durSec int64
		if st.Type == "soak" || st.Type == "load" {
			durSec = st.DurationS
		} else if st.Type == "connect" {
			durSec = st.DurationS
			if durSec < 1 {
				durSec = 1
			}
		}
		// connect/soak 步的负载就是基线本身; load 步剔除基线看叠加速率
		numerator := sent
		if st.Type == "load" {
			numerator = sent - baseline
		}
		var actual float64
		if durSec > 0 {
			actual = float64(numerator) / float64(durSec)
		}
		echoLost := echoOK - echoRecv
		if echoLost < 0 {
			echoLost = 0 // 跨步边界在途回显
		}
		rtt, artt := st.RTT, st.AckRTT
		gSent, gAcked, gEcho, gGap, gDup, gEchoRecv = gSent+sent, gAcked+acked, gEcho+echoOK, gGap+gaps, gDup+dup, gEchoRecv+echoRecv
		connsCol := "-"
		if st.ConnsTarget > 0 {
			connsCol = strconv.FormatInt(st.ConnsTarget, 10)
		}
		tpsTarget := "-"
		if st.ConnsTarget > 0 {
			tpsTarget = fmt.Sprintf("%.0f", float64(st.ConnsTarget)/60)
		} else if st.RateTarget > 0 {
			tpsTarget = rateStr(st.RateTarget)
		}
		w("| %d | %s | %s | %s | %.0f | %d | %d | %d | %d | %d | %d | %s | %s |",
			i, st.Type, connsCol, tpsTarget, actual, sent, acked, gaps, dup, echoRecv, echoLost,
			msTriple(artt), msTriple(rtt))
	}
	w("")

	w("## 全局闭环守恒")
	w("")
	var beConsumed, beEcho, beGaps, beDup int64
	for _, be := range backends {
		for _, s := range be.Steps {
			beConsumed += s.Consumed
			beEcho += s.EchoOK
			beGaps += s.Gaps
			beDup += s.Dup
		}
	}
	w("- 设备累计发送: %d, 收到 PUBACK: %d (%.4f%%)", gSent, gAcked, pct(gAcked, gSent))
	w("- backend 累计消费: %d (上行腿 gap 计 %d, 重复 %d)", beConsumed, beGaps, beDup)
	w("- backend 累计回显: %d, 设备收到回显: %d (下行腿丢失 %d, 设备侧重复 %d)",
		beEcho, gEchoRecv, max64(0, beEcho-gEchoRecv), 0)
	w("")

	// 各机资源占用(压测期间对 node-exporter 的采样聚合, 主机以 hostname 显示)
	for _, d := range devs {
		if len(d.NodeStats) > 0 {
			w("## 各机资源占用(压测全程)")
			w("")
			w("| 主机 | CPU 平均 %% | CPU 峰值 %% | 内存平均 %% | 内存峰值 %% | load1 峰值 | 采样数 |")
			w("|---|---|---|---|---|---|---|")
			for _, n := range observe.Summarize(d.NodeStats) {
				cpuNote := ""
				if n.Samples < 2 {
					cpuNote = " (样本不足)"
				}
				w("| %s | %.1f%s | %.1f | %.1f | %.1f | %.2f | %d |",
					n.Name, n.CPUAvg, cpuNote, n.CPUMax, n.MemAvg, n.MemMax, n.LoadMax, n.Samples)
			}
			w("")
			break
		}
	}

	w("## 产物")
	w("")
	for _, d := range devs {
		w("- device-%d.json shard=%v", d.InstanceIndex, d.Shard)
	}
	for _, be := range backends {
		w("- backend-%d.json", be.InstanceIndex)
	}
	return b.String(), nil
}

func rateStr(r int64) string {
	if r <= 0 {
		return "-"
	}
	return fmt.Sprintf("%d/s", r)
}

func msTriple(h Hist) string {
	if h.Count == 0 {
		return "-"
	}
	return fmt.Sprintf("%.1f/%.1f/%.1f", float64(h.P50)/1e6, float64(h.P95)/1e6, float64(h.P99)/1e6)
}

func pct(a, b int64) float64 {
	if b == 0 {
		return 0
	}
	return float64(a) / float64(b) * 100
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func totalDevices(devs []runFile) int64 {
	var n int64
	for _, d := range devs {
		if len(d.Shard) == 2 {
			n += int64(d.Shard[1] - d.Shard[0])
		}
	}
	return n
}

func readJSON(path string, v any) error {
	bb, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(bb, v)
}
