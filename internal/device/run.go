// Package device 模拟终端: 海量 MQTT 连接 + 错峰接入 + 60s 基线(心跳+业务消息)
// + 回显接收与 RTT 测量。
//
// 加压维度是「连接数」而非消息速率: 场景由 connect 步(增量抬升累计连接数)与
// soak 步(在该连接数下保温观察)组成, 每台设备的行为恒定 —— 每 60s 一条
// PINGREQ(keepalive=120 时恰为 60s)加一条基线业务消息, 指令 TPS = 连接数/60。
// load 步(叠加固定聚合速率)保留作兼容, 仅用于专项微基准。
//
// 在途统计口径: inflight = 累计发出 − 累计确认(均为单调量)。
// 断连时未确认的发布永远等不到 PUBACK, 表现为 inflight 抬升 —— 这正是
// 真实发生的投递破损, 应该被看见而不是被冲销。
package device

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"jmqtt-bench/internal/metrics"
	"jmqtt-bench/internal/mqttc"
	"jmqtt-bench/internal/observe"
	"jmqtt-bench/internal/proto"
	"jmqtt-bench/internal/scenario"
)

type dev struct {
	gidx      int
	did       string
	upTopic   string
	downTopic string
	cli       atomic.Pointer[mqttc.Client]
	connected atomic.Bool
	seq       atomic.Uint32
	echoLast  atomic.Uint32
	nextBase  atomic.Int64 // 下次基线消息 unixNano; 0=未设置
	srcIP     net.IP
}

// Runner 一个 device 实例
type Runner struct {
	cfg    *scenario.Config
	plans  []scenario.StepPlan
	lo, hi int
	devs   []*dev
	srcIPs []net.IP

	totals struct {
		connAttempted atomic.Int64
		connOK        atomic.Int64
		connFailed    atomic.Int64
		reconnects    atomic.Int64
		skipDisc      atomic.Int64 // 调度时设备已断连而跳过
	}
	connected atomic.Int64 // 当前已接入(成功 CONNACK+SUBACK)的设备数

	steps  []*metrics.StepStats
	cur    atomic.Int32
	curPtr atomic.Pointer[metrics.StepStats]

	brokerObs *observe.Poller
	nodeObs   *observe.NodePoller
	outPath   string
	abortFlag atomic.Bool
}

func Run(cfg *scenario.Config) error {
	r := &Runner{cfg: cfg}
	r.lo, r.hi = cfg.Shard()
	r.plans = cfg.Timeline(resolveT0(cfg))
	last := r.plans[len(r.plans)-1]
	if time.Now().After(last.EndAt) {
		return fmt.Errorf("T0 时间线已过期(终点 %s), 拒绝空跑 —— 请用更大的 --t0 重新对齐",
			last.EndAt.Format(time.RFC3339))
	}
	log.Printf("[device] run=%s shard=[%d,%d) connect_rate=%d/s t0=%s steps=%d",
		cfg.RunID, r.lo, r.hi, cfg.Devices.ConnectRate,
		r.plans[0].StartAt.Format(time.RFC3339), len(cfg.Steps))

	// 源 IP 池只保留本机真实存在的地址: 多台压测机共用一份场景配置时,
	// 别的机器的 IP 在本机 bind 会直接 "cannot assign requested address"
	localIPs := map[string]bool{}
	if addrs, err := net.InterfaceAddrs(); err == nil {
		for _, a := range addrs {
			if ipnet, ok := a.(*net.IPNet); ok {
				localIPs[ipnet.IP.String()] = true
			}
		}
	}
	for _, ip := range cfg.SourceIPs {
		p := net.ParseIP(ip)
		if p == nil || !localIPs[ip] {
			continue
		}
		r.srcIPs = append(r.srcIPs, p)
	}
	if len(r.srcIPs) == 0 {
		r.srcIPs = append(r.srcIPs, nil)
	}
	log.Printf("[device] 本机源 IP 池: %d 个", len(r.srcIPs))

	for i := r.lo; i < r.hi; i++ {
		did := proto.DID(i)
		r.devs = append(r.devs, &dev{
			gidx:      i,
			did:       did,
			upTopic:   fmt.Sprintf("%s/%s/event/up", cfg.TopicPrefix, did),
			downTopic: fmt.Sprintf("%s/%s/service/down", cfg.TopicPrefix, did),
			srcIP:     r.srcIPs[i%len(r.srcIPs)],
		})
	}

	for i, p := range r.plans {
		var rate int64
		if p.Type == "load" {
			rate = p.Rate
		}
		var conns int64
		if p.Type == "connect" {
			conns = p.Target
		}
		r.steps = append(r.steps, metrics.NewStep(i, p.Type, rate, p.Duration, conns))
	}
	r.curPtr.Store(r.steps[0])

	if cfg.ObserveBroker != "" {
		r.brokerObs = observe.New(cfg.ObserveBroker, time.Duration(cfg.ObserveIntervalS)*time.Second)
		r.brokerObs.Start()
	}
	if len(cfg.ObserveNodes) > 0 {
		r.nodeObs = observe.NewNodePoller(cfg.ObserveNodes, time.Duration(cfg.ObserveIntervalS)*time.Second)
		r.nodeObs.Start()
	}

	_ = os.MkdirAll(cfg.OutDir, 0o755)
	r.outPath = filepath.Join(cfg.OutDir, fmt.Sprintf("device-%d.json", cfg.Devices.InstanceIndex))

	// 等待 T0: 全体实例墙钟对齐后, 按时间线依次执行 connect/soak/load
	if d := time.Until(r.plans[0].StartAt); d > 0 {
		log.Printf("[device] 等待 T0 对齐 %s (%s)", r.plans[0].StartAt.Format("15:04:05"), d.Round(time.Second))
		time.Sleep(d)
	}
	r.runPhase()
	r.dump()
	log.Printf("[device] done, results -> %s", r.outPath)
	return nil
}

func resolveT0(c *scenario.Config) int64 {
	if c.T0 != 0 {
		return c.T0
	}
	return time.Now().Add(5 * time.Second).Unix()
}

// cleanSession 缺省 true(保持历史行为); 显式 false 走持久会话, 把 Redis 压进链路
func (r *Runner) cleanSession() bool {
	return r.cfg.CleanSession == nil || *r.cfg.CleanSession
}

// baselineEnabled 缺省 true; false = 纯连接模式(只在线+keepalive 心跳, 无业务消息)
func (r *Runner) baselineEnabled() bool {
	return r.cfg.Baseline == nil || *r.cfg.Baseline
}

// connectDev 连接 + 订阅下行主题。retry=true 表示重连路径。
func (r *Runner) connectDev(d *dev, retry bool) bool {
	opts := mqttc.Options{
		Addr:         r.cfg.Broker,
		ClientID:     d.did,
		Username:     r.cfg.Auth.Username,
		Password:     r.cfg.Auth.Password,
		KeepAlive:    r.cfg.KeepAlive,
		SourceIP:     d.srcIP,
		CleanSession: r.cleanSession(),
	}
	h := mqttc.Handlers{
		OnPuback:  func(pid uint16, el time.Duration) { r.onPuback(d, el) },
		OnMessage: func(m *mqttc.Message) { r.onEcho(d, m) },
		OnClose:   func(err error) { r.onDown(d, err) },
	}
	cli, err := mqttc.Connect(opts, h)
	if err != nil {
		if !retry {
			// 拒连是容量数据不是噪音: 只重试一次, 避免重试风暴掩盖拐点
			time.Sleep(2 * time.Second)
			cli, err = mqttc.Connect(opts, h)
		}
		if err != nil {
			r.totals.connFailed.Add(1)
			if r.totals.connFailed.Load()%100 == 1 {
				log.Printf("[device] connect fail (累计 %d): %v", r.totals.connFailed.Load(), err)
			}
			return false
		}
	}
	if _, err := cli.Subscribe(d.downTopic, r.cfg.QoS, 10*time.Second); err != nil {
		if r.totals.connFailed.Load()%100 == 0 {
			log.Printf("[device] subscribe fail (sample): %v", err)
		}
		cli.Close()
		r.totals.connFailed.Add(1)
		return false
	}
	if retry {
		r.totals.reconnects.Add(1)
	} else {
		r.totals.connOK.Add(1)
	}
	d.cli.Store(cli)
	d.connected.Store(true)
	return true
}

func (r *Runner) onDown(d *dev, err error) {
	if !d.connected.CompareAndSwap(true, false) {
		return
	}
	d.cli.Store(nil)
	if r.abortFlag.Load() {
		return
	}
	go func() {
		time.Sleep(2 * time.Second)
		for !r.abortFlag.Load() && !d.connected.Load() {
			if r.connectDev(d, true) {
				j := rand.Int63n(int64(r.cfg.BaselinePeriodS) * int64(time.Second))
				d.nextBase.Store(time.Now().Add(time.Duration(j)).UnixNano())
				return
			}
			time.Sleep(5 * time.Second)
		}
	}()
}

func (r *Runner) onPuback(d *dev, el time.Duration) {
	if s := r.curPtr.Load(); s != nil {
		s.Acked.Add(1)
		s.AckRTT.Record(int64(el))
	}
}

// onEcho 收到下行回显: 全环 RTT + 乱序/重复识别。
// 同设备的下行经「按设备分区 → broker 按分区 worker 消费」全程 FIFO,
// 单调性成立: seq <= last 视为重复, 跳号即下行腿丢失。
func (r *Runner) onEcho(d *dev, m *mqttc.Message) {
	s := r.curPtr.Load()
	if s == nil {
		return
	}
	seqV, ok := scanUintAfter(m.Payload, `"seq":`)
	if !ok {
		return
	}
	seq := uint32(seqV)
	last := d.echoLast.Load()
	if seq <= last {
		s.EchoDup.Add(1)
		return
	}
	d.echoLast.Store(seq)
	s.EchoRecv.Add(1)
	if t0v, ok := scanUintAfter(m.Payload, `"t0":`); ok && t0v > 0 {
		s.RTT.Record(time.Now().UnixNano() - int64(t0v))
	}
}

func scanUintAfter(b []byte, key string) (uint64, bool) {
	i := bytes.Index(b, []byte(key))
	if i < 0 {
		return 0, false
	}
	start := i + len(key)
	end := start
	for end < len(b) && b[end] >= '0' && b[end] <= '9' {
		end++
	}
	if end == start {
		return 0, false
	}
	v, err := strconv.ParseUint(string(b[start:end]), 10, 64)
	return v, err == nil
}

// runPhase 时间线主循环: connect 步增量错峰接入, soak 步保温(基线自然发生),
// load 步按预算叠加速率(兼容场景)。
func (r *Runner) runPhase() {
	// 步进协程: 按墙钟切换当前步
	go func() {
		lastConns := int64(0)
		for i := range r.plans {
			p := r.plans[i]
			if d := time.Until(p.StartAt); d > 0 {
				time.Sleep(d)
			}
			if p.Type == "connect" {
				lastConns = p.Target
			}
			switch p.Type {
			case "connect":
				log.Printf("[device] 步骤 %d: 接入至 %d 连接", i, p.Target)
			case "soak":
				log.Printf("[device] 步骤 %d: 保温 %ds (连接 %d, 指令 TPS≈%.0f + 回显同频, 心跳 PING 同频)",
					i, p.Duration, lastConns, float64(lastConns)/60)
			case "load":
				log.Printf("[device] 步骤 %d: rate=%d/s dur=%ds", i, p.Rate, p.Duration)
			}
			r.cur.Store(int32(i))
			r.curPtr.Store(r.steps[i])
		}
	}()

	// 连接驱动: 错峰 + 自适应节拍。
	//   - 初始速率 = connect_rate × 50%, 上限 connect_rate(按每实例计)
	//   - 任一接入失败 → 速率 ×0.6(下限 30/s), 让 broker 喘口气
	//   - 连续 5s 无失败 → 速率 ×1.25 缓慢回升
	//   - 失败设备回到队列尾部无限重试(不弃号); 接入速率自动收敛到 broker 能力
	// 节拍用 20ms 批量预算器: 逐台 Sleep(interval) 会被 sleep 粒度与 spawn
	// 开销拖慢(实测 2ms 间隔只能打出 ~390/s)。
	go func() {
		var wg sync.WaitGroup
		sem := make(chan struct{}, 1024)
		var failWindow atomic.Int64
		var rate atomic.Int64
		maxRate := float64(r.cfg.Devices.ConnectRate)
		init := maxRate * 0.5
		if init < 30 {
			init = 30
		}
		rate.Store(int64(init))
		// 速率调节器: 5s 一调
		go func() {
			t := time.NewTicker(5 * time.Second)
			defer t.Stop()
			for {
				select {
				case <-t.C:
					f := failWindow.Swap(0)
					cur := rate.Load()
					var nxt int64
					if f > 0 {
						nxt = int64(float64(cur) * 0.6)
						if nxt < 30 {
							nxt = 30
						}
					} else {
						nxt = int64(float64(cur) * 1.25)
						if float64(nxt) > maxRate {
							nxt = int64(maxRate)
						}
					}
					if nxt != cur {
						rate.Store(nxt)
					}
				default:
					if r.abortFlag.Load() {
						return
					}
					time.Sleep(50 * time.Millisecond)
				}
			}
		}()

		var pendMu sync.Mutex
		pending := []int64{}
		seeded := int64(0)
		budget := 0.0
		lastProgress := time.Now()
		for _, p := range r.plans {
			if p.Type != "connect" {
				continue
			}
			if d := time.Until(p.StartAt); d > 0 {
				time.Sleep(d)
			}
			myTarget := int64(float64(p.Target) * float64(len(r.devs)) / float64(r.cfg.Devices.Total))
			for i := seeded; i < myTarget; i++ {
				pending = append(pending, i)
			}
			seeded = myTarget
			t := time.NewTicker(20 * time.Millisecond)
			for len(pending) > 0 && !r.abortFlag.Load() {
				select {
				case <-t.C:
					budget += float64(rate.Load()) * 0.02
					// 出队在锁内, 拿 sem 与 spawn 在锁外 —— pacer 持锁等 sem、
					// 失败 goroutine 等锁回队, 会造成循环等待(实测已踩)
					pendMu.Lock()
					var batch []int64
					for budget >= 1 && len(pending) > 0 {
						budget--
						idx := pending[0]
						pending = pending[1:]
						if idx < int64(len(r.devs)) {
							batch = append(batch, idx)
						}
					}
					pendMu.Unlock()
					for _, idx := range batch {
						d := r.devs[idx]
						if d.connected.Load() {
							continue // 已被重连路径连上
						}
						wg.Add(1)
						sem <- struct{}{}
						go func(d *dev, idx int64) {
							defer wg.Done()
							defer func() { <-sem }()
							if r.connectDev(d, false) {
								r.connected.Add(1)
								j := rand.Int63n(int64(r.cfg.BaselinePeriodS) * int64(time.Second))
								d.nextBase.Store(time.Now().Add(time.Duration(j)).UnixNano())
							} else {
								failWindow.Add(1)
								pendMu.Lock()
								pending = append(pending, idx)
								pendMu.Unlock()
							}
						}(d, idx)
					}
					if time.Since(lastProgress) > 5*time.Second {
						lastProgress = time.Now()
						pendMu.Lock()
						pq := len(pending)
						pendMu.Unlock()
						log.Printf("[device] 接入进度: 已连=%d 待连=%d 速率=%d/s",
							r.connected.Load(), pq, rate.Load())
					}
				default:
					time.Sleep(2 * time.Millisecond)
				}
			}
			t.Stop()
			wg.Wait()
			if f := failWindow.Load(); f > 0 {
				failWindow.Store(0)
			}
		}
		// 全部 connect 步走完, 排干残余队列(重试到连上或 abort)
		t := time.NewTicker(20 * time.Millisecond)
		for len(pending) > 0 && !r.abortFlag.Load() {
			select {
			case <-t.C:
				budget += float64(rate.Load()) * 0.02
				pendMu.Lock()
				var batch []int64
				for budget >= 1 && len(pending) > 0 {
					budget--
					idx := pending[0]
					pending = pending[1:]
					batch = append(batch, idx)
				}
				pendMu.Unlock()
				for _, idx := range batch {
					d := r.devs[idx]
					if d.connected.Load() {
						continue
					}
					wg.Add(1)
					sem <- struct{}{}
					go func(d *dev, idx int64) {
						defer wg.Done()
						defer func() { <-sem }()
						if r.connectDev(d, true) {
							r.connected.Add(1)
						} else {
							failWindow.Add(1)
							pendMu.Lock()
							pending = append(pending, idx)
							pendMu.Unlock()
						}
					}(d, idx)
				}
			default:
				time.Sleep(2 * time.Millisecond)
			}
		}
		t.Stop()
		wg.Wait()
		log.Printf("[device] 连接驱动完成: 已连=%d", r.connected.Load())
	}()

	tick := 10 * time.Millisecond
	var cursor atomic.Uint64
	stop := make(chan struct{})
	// 速率预算池(仅 load 步使用): 0 号 worker 注入, 各 worker CAS 认领
	var budget int64
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			t := time.NewTicker(tick)
			defer t.Stop()
			for {
				select {
				case <-t.C:
					if r.abortFlag.Load() {
						return
					}
					stepIdx := int(r.cur.Load())
					p := r.plans[stepIdx]
					s := r.steps[stepIdx]
					now := time.Now()
					if p.Type == "load" && !now.Before(p.StartAt) && !now.After(p.EndAt) {
						if w == 0 {
							add := p.Rate * int64(tick/time.Millisecond) / 1000
							atomic.AddInt64(&budget, add)
						}
					}
					r.drainBaseline(s)
					if p.Type == "load" {
						n := uint64(len(r.devs))
						for {
							cur := atomic.LoadInt64(&budget)
							if cur <= 0 || r.abortFlag.Load() {
								break
							}
							if atomic.CompareAndSwapInt64(&budget, cur, cur-1) {
								d := r.devs[cursor.Add(1)%n]
								r.publish(d, s, false)
							}
						}
					}
				case <-stop:
					return
				}
			}
		}(w)
	}

	// 监控/安全阀: 10s 一报
	done := make(chan struct{})
	go func() {
		defer close(done)
		t := time.NewTicker(10 * time.Second)
		defer t.Stop()
		var lastAcked int64
		var stallSince time.Time
		for {
			select {
			case <-t.C:
				sent, acked := r.sumSentAcked()
				infl := sent - acked
				log.Printf("[device] step=%d conns=%d inflight=%d sent=%d acked=%d skipDisc=%d reconnects=%d",
					r.cur.Load(), r.connected.Load(), infl, sent, acked,
					r.totals.skipDisc.Load(), r.totals.reconnects.Load())
				if infl > r.cfg.AbortInflightGlobal && acked == lastAcked {
					if stallSince.IsZero() {
						stallSince = time.Now()
					} else if time.Since(stallSince) > 60*time.Second {
						log.Printf("[device] 安全阀触发: inflight=%d 确认停滞>60s, 中止本实例", infl)
						r.abortFlag.Store(true)
					}
				} else {
					stallSince = time.Time{}
				}
				lastAcked = acked
			case <-stop:
				return
			}
		}
	}()

	// 等时间线走完
	last := r.plans[len(r.plans)-1]
	if d := time.Until(last.EndAt); d > 0 {
		time.Sleep(d)
	}
	time.Sleep(2 * time.Second) // 尾部在途回包
	r.abortFlag.Store(true)     // worker 与重连协程退出
	close(stop)
	wg.Wait()
	<-done
}

// drainBaseline 每 tick 轮转扫描 设备数/100 台, 发出到期的 60s 基线消息, 一秒扫完一轮。
// 只有已连接且到期的设备才发(未接入的设备 nextBase=0)。
// baseline=false 的纯连接场景直接跳过(keepalive PING 由 mqttc 心跳协程自理)。
func (r *Runner) drainBaseline(s *metrics.StepStats) {
	if !r.baselineEnabled() {
		return
	}
	n := uint64(len(r.devs))
	window := n / 100
	if window < 1 {
		window = 1
	}
	now := time.Now().UnixNano()
	period := int64(r.cfg.BaselinePeriodS) * int64(time.Second)
	start := baseCursor.Add(window) % n
	for i := uint64(0); i < window; i++ {
		d := r.devs[(start+i)%n]
		nb := d.nextBase.Load()
		if nb == 0 || nb > now || !d.connected.Load() {
			continue
		}
		d.nextBase.Store(now + period)
		r.publish(d, s, true)
	}
}

var baseCursor atomic.Uint64

func (r *Runner) publish(d *dev, s *metrics.StepStats, baseline bool) {
	if !d.connected.Load() {
		r.totals.skipDisc.Add(1)
		return
	}
	cli := d.cli.Load()
	if cli == nil {
		r.totals.skipDisc.Add(1)
		return
	}
	seq := d.seq.Add(1)
	e := proto.Envelope{Seq: seq, T0: time.Now().UnixNano(), DID: d.did}
	bp := pbufPool.Get().(*[]byte)
	buf := proto.AppendMarshal((*bp)[:0], &e, r.cfg.PayloadSize)
	_, err := cli.PublishAsync(d.upTopic, buf)
	*bp = buf[:0]
	pbufPool.Put(bp)
	if err != nil {
		s.PubTimeout.Add(1)
		return
	}
	s.Sent.Add(1)
	if baseline {
		s.Baseline.Add(1)
	}
}

var pbufPool = sync.Pool{New: func() any { b := make([]byte, 0, 512); return &b }}

func (r *Runner) sumSentAcked() (int64, int64) {
	var sent, acked int64
	for _, st := range r.steps {
		sent += st.Sent.Load()
		acked += st.Acked.Load()
	}
	return sent, acked
}

type deviceReport struct {
	RunID         string                 `json:"run_id"`
	Role          string                 `json:"role"`
	InstanceIndex int                    `json:"instance_index"`
	Shard         [2]int                 `json:"shard"`
	T0            int64                  `json:"t0"`
	EndedAt       time.Time              `json:"ended_at"`
	Aborted       bool                   `json:"aborted"`
	Totals        map[string]int64       `json:"totals"`
	Steps         []metrics.StepSnapshot `json:"steps"`
	Broker        []observe.Snapshot     `json:"broker_snapshots,omitempty"`
	NodeStats     []observe.NodeSnapshot `json:"node_stats,omitempty"`
}

func (r *Runner) dump() {
	steps := make([]metrics.StepSnapshot, 0, len(r.steps))
	for _, s := range r.steps {
		steps = append(steps, s.Snapshot())
	}
	sent, acked := r.sumSentAcked()
	rep := deviceReport{
		RunID:         r.cfg.RunID,
		Role:          "device",
		InstanceIndex: r.cfg.Devices.InstanceIndex,
		Shard:         [2]int{r.lo, r.hi},
		T0:            r.plans[0].StartAt.Unix(),
		EndedAt:       time.Now(),
		Aborted:       r.abortFlag.Load() && time.Now().Before(r.plans[len(r.plans)-1].EndAt),
		Totals: map[string]int64{
			"connect_attempted":     r.totals.connAttempted.Load(),
			"connect_ok":            r.totals.connOK.Load(),
			"connect_failed":        r.totals.connFailed.Load(),
			"reconnects":            r.totals.reconnects.Load(),
			"pub_skip_disconnected": r.totals.skipDisc.Load(),
			"published":             sent,
			"acked":                 acked,
		},
		Steps: steps,
	}
	if r.brokerObs != nil {
		rep.Broker = r.brokerObs.Stop()
	}
	if r.nodeObs != nil {
		rep.NodeStats = r.nodeObs.Stop()
	}
	b, err := json.MarshalIndent(rep, "", " ")
	if err == nil {
		_ = os.WriteFile(r.outPath, b, 0o644)
	}
}
