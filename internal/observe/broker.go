// Package observe 压测期间定时抓取 broker 指标, 与负载时间线对齐存档。
// 容量拐点必须能与 broker 侧计数器(背压/丢弃/在途)互相印证。
package observe

import (
	"encoding/json"
	"log"
	"net/http"
	"sync"
	"time"
)

type Snapshot struct {
	T    time.Time       `json:"t"`
	Info json.RawMessage `json:"info"`
}

type Poller struct {
	url       string
	interval  time.Duration
	client    *http.Client
	mu        sync.Mutex
	snapshots []Snapshot
	stop      chan struct{}
}

func New(url string, interval time.Duration) *Poller {
	return &Poller{
		url:      url,
		interval: interval,
		client:   &http.Client{Timeout: 3 * time.Second},
		stop:     make(chan struct{}),
	}
}

func (p *Poller) Start() {
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

func (p *Poller) Stop() []Snapshot {
	close(p.stop)
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.snapshots
}

func (p *Poller) poll() {
	resp, err := p.client.Get(p.url)
	if err != nil {
		return // broker 忙时抓取失败是常态, 不刷屏
	}
	defer resp.Body.Close()
	var info json.RawMessage
	if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
		return
	}
	p.mu.Lock()
	p.snapshots = append(p.snapshots, Snapshot{T: time.Now(), Info: info})
	p.mu.Unlock()
	n := len(p.snapshots)
	if n%6 == 0 {
		log.Printf("[observe] broker snapshots=%d", n)
	}
}
