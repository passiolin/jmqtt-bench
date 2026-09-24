// Package backend 模拟后台: 消费 Kafka 上行 → 记录时延/丢失/重复 → 逐条回显到 downlink。
// 与 broker 的契约(见 broker ClusterRecords):
//   - 上行 value = JSON 信封 {username,topic,timestamp,qos,payload,node,clientid},
//     key = clientId(routes key: device), header jmqtt-kind=routed-publish
//   - 下行 value = JSON {"topic","qos","payload"}, key 仅用于分区(取 deviceId)
package backend

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"jmqtt-bench/internal/metrics"
	"jmqtt-bench/internal/proto"
	"jmqtt-bench/internal/scenario"
)

type stepStats struct {
	sent      int64 // 上行信封 topic 对应的分片设备数(仅信息)
	Consumed  atomic.Int64
	EchoOK    atomic.Int64
	EchoErr   atomic.Int64
	Gaps      atomic.Int64 // seq 跳号 = 上行腿丢失
	Dup       atomic.Int64 // seq 回退/重复 = 端到端重复
	Bad       atomic.Int64 // 解码失败
	UpLatency *metrics.Hist
}

func newStep() *stepStats { return &stepStats{UpLatency: metrics.NewHist()} }

type stepSnapshot struct {
	Index     int                  `json:"index"`
	Type      string               `json:"type"`
	Consumed  int64                `json:"consumed"`
	EchoOK    int64                `json:"echo_ok"`
	EchoErr   int64                `json:"echo_err"`
	Gaps      int64                `json:"uplink_gap"`
	Dup       int64                `json:"dup"`
	Bad       int64                `json:"bad_records"`
	UpLatency metrics.HistSnapshot `json:"uplink_latency"`
}

type Runner struct {
	cfg    *scenario.Config
	plans  []scenario.StepPlan
	lo, hi int

	steps  []*stepStats
	cur    atomic.Int32
	curPtr atomic.Pointer[stepStats]

	outPath string
}

func Run(cfg *scenario.Config) error {
	r := &Runner{cfg: cfg}
	r.lo, r.hi = cfg.Shard()
	r.plans = scenario.TimelineOf(cfg)
	for range r.plans {
		r.steps = append(r.steps, newStep())
	}
	r.curPtr.Store(r.steps[0])

	_ = os.MkdirAll(cfg.OutDir, 0o755)
	r.outPath = filepath.Join(cfg.OutDir, fmt.Sprintf("backend-%d.json", cfg.Devices.InstanceIndex))

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Kafka.Bootstrap),
		kgo.ConsumeTopics(cfg.Kafka.Uplink),
		kgo.ConsumerGroup(cfg.Kafka.Group),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), // 消费组首启从头读, 避免漏掉连接风暴期间的上行
		kgo.FetchMaxBytes(8<<20),
		kgo.MaxConcurrentFetches(16),
	)
	if err != nil {
		return err
	}

	producer, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Kafka.Bootstrap),
		kgo.RequiredAcks(kgo.LeaderAck()),
		kgo.DisableIdempotentWrite(), // broker 侧同样 acks=1; 幂等写要求 acks=all
		kgo.ProducerBatchMaxBytes(1<<20),
		kgo.MaxBufferedRecords(200_000),
	)
	if err != nil {
		return err
	}
	defer producer.Close()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	log.Printf("[backend] run=%s group=%s uplink=%s downlink=%s t0=%s",
		cfg.RunID, cfg.Kafka.Group, cfg.Kafka.Uplink, cfg.Kafka.Downlink,
		r.plans[0].StartAt.Format(time.RFC3339))

	// 步进协程
	go func() {
		for i := range r.plans {
			p := r.plans[i]
			if d := time.Until(p.StartAt); d > 0 {
				time.Sleep(d)
			}
			r.cur.Store(int32(i))
			r.curPtr.Store(r.steps[i])
		}
	}()

	// 消费 → 按分区分派(同分区固定 worker ⇒ 同设备保序)
	const workers = 16
	queues := make([]chan *kgo.Record, workers)
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		queues[w] = make(chan *kgo.Record, 65536)
		wg.Add(1)
		go func(q chan *kgo.Record) {
			defer wg.Done()
			lastSeq := make(map[string]uint32, 1<<20)
			buf := make([]byte, 0, 512)
			for rec := range q {
				r.process(lastSeq, &buf, producer, rec)
			}
		}(queues[w])
	}

	// poll 循环: 单线程 poll, 记录按分区分派; worker 队列满时阻塞以形成背压
	pollDone := make(chan struct{})
	go func() {
		defer close(pollDone)
		for {
			fetches := cl.PollRecords(ctx, 10000)
			if fetches.IsClientClosed() {
				return
			}
			fetches.EachError(func(topic string, part int32, err error) {
				log.Printf("[backend] fetch error %s-%d: %v", topic, part, err)
			})
			fetches.EachRecord(func(rec *kgo.Record) {
				queues[int(rec.Partition)%workers] <- rec
			})
		}
	}()

	// 监控打印(由主流程 close(done) 结束, 这里绝不 close —— 双重 close 会 panic)
	done := make(chan struct{})
	go func() {
		t := time.NewTicker(10 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				s := r.curPtr.Load()
				log.Printf("[backend] step=%d consumed=%d echo=%d gaps=%d dup=%d bad=%d",
					r.cur.Load(), s.Consumed.Load(), s.EchoOK.Load(), s.Gaps.Load(), s.Dup.Load(), s.Bad.Load())
			case <-ctx.Done():
				return
			case <-done:
				return
			}
		}
	}()

	// 时间线结束后多等 30s 收尾流, 或被信号打断
	end := r.plans[len(r.plans)-1].EndAt.Add(30 * time.Second)
	log.Printf("[backend] 时间线终点 %s, 收尾截止 %s", r.plans[len(r.plans)-1].EndAt.Format("15:04:05"), end.Format("15:04:05"))
	timer := time.NewTimer(time.Until(end))
	defer timer.Stop()
	select {
	case <-timer.C:
		log.Printf("[backend] 时间线走完, 收尾")
	case <-ctx.Done():
		log.Printf("[backend] 收到退出信号, 立即落盘")
	}

	// 先停 poll(关闭 client 会令 PollRecords 返回), 再排干 worker
	cl.Close()
	<-pollDone
	for _, q := range queues {
		close(q)
	}
	wg.Wait()
	close(done)
	r.dump()
	log.Printf("[backend] done, results -> %s", r.outPath)
	return nil
}

func (r *Runner) process(lastSeq map[string]uint32, buf *[]byte, producer *kgo.Client, rec *kgo.Record) {
	// 积压过滤: topic 保留期内可能残留上一轮的记录, 只统计本轮时间线内的
	if rec.Timestamp.Before(r.plans[0].StartAt.Add(-2 * time.Second)) {
		return
	}
	var env struct {
		Topic    string `json:"topic"`
		Payload  string `json:"payload"`
		ClientID string `json:"clientid"`
	}
	if err := json.Unmarshal(rec.Value, &env); err != nil || env.Payload == "" {
		r.curPtr.Load().Bad.Add(1)
		return
	}
	did, seq, t0, ok := proto.ParseHeader([]byte(env.Payload))
	if !ok {
		r.curPtr.Load().Bad.Add(1)
		return
	}
	s := r.curPtr.Load()
	s.Consumed.Add(1)
	now := time.Now().UnixNano()
	if t0 > 0 {
		s.UpLatency.Record(now - t0)
	}
	// 同设备经 分区键=clientId → 同分区 → 同 worker, 严格保序: 跳号=丢, 回退=重
	if last, seen := lastSeq[did]; seen {
		if seq <= last {
			s.Dup.Add(1)
			// 重复消息不回显: 设备侧 echoLast 单调, 回显也会被当作重复,
			// 但这里直接不回显更干净
			return
		}
		s.Gaps.Add(int64(seq) - int64(last) - 1)
	}
	lastSeq[did] = seq

	// 逐条回显: 业务语义 = 后台对每条上行下发一条指令。
	// echo=false 时为纯上行模式, 只测消费不回显。
	if r.cfg.Echo != nil && !*r.cfg.Echo {
		return
	}
	downTopic := fmt.Sprintf("%s/%s/service/down", r.cfg.TopicPrefix, did)
	e := proto.Envelope{Seq: seq, T0: t0, T1: now, DID: did}
	*buf = proto.EchoJSON((*buf)[:0], env.Topic, downTopic, &e, r.cfg.PayloadSize)
	val := make([]byte, len(*buf))
	copy(val, *buf)
	producer.Produce(context.Background(), &kgo.Record{
		Topic: r.cfg.Kafka.Downlink,
		Key:   []byte(did),
		Value: val,
	}, func(_ *kgo.Record, err error) {
		if err != nil {
			s.EchoErr.Add(1)
			return
		}
		s.EchoOK.Add(1)
	})
}

func (r *Runner) dump() {
	steps := make([]stepSnapshot, 0, len(r.steps))
	for i, s := range r.steps {
		steps = append(steps, stepSnapshot{
			Index: i, Type: r.plans[i].Type,
			Consumed: s.Consumed.Load(), EchoOK: s.EchoOK.Load(), EchoErr: s.EchoErr.Load(),
			Gaps: s.Gaps.Load(), Dup: s.Dup.Load(), Bad: s.Bad.Load(),
			UpLatency: s.UpLatency.Snapshot(),
		})
	}
	rep := map[string]any{
		"run_id":         r.cfg.RunID,
		"role":           "backend",
		"instance_index": r.cfg.Devices.InstanceIndex,
		"shard":          []int{r.lo, r.hi},
		"t0":             r.plans[0].StartAt.Unix(),
		"ended_at":       time.Now(),
		"steps":          steps,
	}
	b, err := json.MarshalIndent(rep, "", " ")
	if err == nil {
		_ = os.WriteFile(r.outPath, b, 0o644)
	}
}
