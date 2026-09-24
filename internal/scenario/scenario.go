// Package scenario 场景模型: YAML 配置 + 时间线。
//
// 所有实例(可多进程分片)以同一段墙钟时间线 lockstep 执行:
// t0 = 各实例 --t0 传入的 unix 秒。步骤依次为:
// connect(错峰接入, 限连接速率) → 若干 load 步(目标聚合速率 + 持续时长)。
// 每台设备在任何时刻都保有「每 60s 一条 PINGREQ + 一条基线业务消息」的底线行为,
// load 步的速率是叠加在其上的额外流量。
package scenario

import (
	"fmt"
	"os"
	"time"

	"jmqtt-bench/internal/observe"

	"gopkg.in/yaml.v3"
)

type Step struct {
	Type     string `yaml:"type" json:"type"`                             // connect | soak | load
	Target   int64  `yaml:"target,omitempty" json:"target,omitempty"`     // connect: 目标连接数(累计, 全局)
	Rate     int64  `yaml:"rate,omitempty" json:"rate,omitempty"`         // load: 聚合速率 msg/s
	Duration int64  `yaml:"duration,omitempty" json:"duration,omitempty"` // soak/load: 秒; connect: 到位后保温秒(可选)
}

type Config struct {
	RunID  string `yaml:"run_id" json:"run_id"`
	Broker string `yaml:"broker" json:"broker"` // host:port
	Auth   struct {
		Username string `yaml:"username" json:"username"`
		Password string `yaml:"password" json:"password"`
	} `yaml:"auth" json:"auth"`
	Kafka struct {
		Bootstrap string `yaml:"bootstrap" json:"bootstrap"`
		Uplink    string `yaml:"uplink" json:"uplink"`
		Downlink  string `yaml:"downlink" json:"downlink"`
		Group     string `yaml:"group" json:"group"`
	} `yaml:"kafka" json:"kafka"`

	TopicPrefix string `yaml:"topic_prefix" json:"topic_prefix"`
	PayloadSize int    `yaml:"payload_size" json:"payload_size"`
	QoS         byte   `yaml:"qos" json:"qos"`
	KeepAlive   uint16 `yaml:"keepalive" json:"keepalive"`
	// BaselinePeriodS 基线周期: 每 N 秒每设备一条业务消息(心跳 PINGREQ 同周期)
	BaselinePeriodS int `yaml:"baseline_period_s" json:"baseline_period_s"`
	// Echo backend 是否逐条回显。false = 纯上行模式, 单独测 broker 上行天花板
	// (排除下行回显管道的反压干扰)。nil/缺省 = 回显
	Echo *bool `yaml:"echo" json:"echo"`
	// CleanSession 设备会话模式。false = 持久会话(broker 落 Redis, 接入即触发
	// 会话/订阅持久化), true = 每次新建(broker 几乎不碰 Redis)。缺省 true。
	CleanSession *bool `yaml:"clean_session" json:"clean_session"`
	// Baseline 是否发送基线业务消息(每 60s 一条)。false = 纯连接模式:
	// 只保持在线与 keepalive 心跳, 无业务流量。缺省 true。
	Baseline *bool `yaml:"baseline" json:"baseline"`

	Devices struct {
		Total         int `yaml:"total" json:"total"`
		InstanceIndex int `yaml:"instance_index" json:"instance_index"`
		InstanceCount int `yaml:"instance_count" json:"instance_count"`
		// ConnectRate 错峰接入速率 (conn/s)。几十万连接绝不允许一次性发起
		ConnectRate int `yaml:"connect_rate" json:"connect_rate"`
	} `yaml:"devices" json:"devices"`

	SourceIPs []string `yaml:"source_ips" json:"source_ips"` // 源 IP 池(突破单 IP 端口上限)

	Steps []Step `yaml:"steps" json:"steps"`

	// T0 全体实例对齐的墙钟起点(unix 秒)。0 = 本实例自行取 now+5(单实例模式)
	T0 int64 `yaml:"t0" json:"t0"`

	// ObserveBroker broker 指标端点(留空不观测)
	ObserveBroker    string `yaml:"observe_broker" json:"observe_broker"`
	ObserveIntervalS int    `yaml:"observe_interval_s" json:"observe_interval_s"`
	// ObserveNodes 各机资源观测(node-exporter), 压测期间采样并写入报告
	ObserveNodes []observe.NodeTarget `yaml:"observe_nodes" json:"observe_nodes"`

	// PubAckTimeoutS 单条 QoS1 发布等待 PUBACK 上限(超时计数, 不重发)
	PubAckTimeoutS int `yaml:"pub_ack_timeout_s" json:"pub_ack_timeout_s"`
	// AbortInflightGlobal 全局在途(PUBACK 未归)超过该值并持续 60s → 本实例中止
	AbortInflightGlobal int64 `yaml:"abort_inflight_global" json:"abort_inflight_global"`

	OutDir string `yaml:"out_dir" json:"out_dir"`
}

// Load 从 YAML 文件读配置
func Load(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	c := &Config{}
	if err := yaml.Unmarshal(b, c); err != nil {
		return nil, err
	}
	c.defaults()
	if err := c.validate(); err != nil {
		return nil, err
	}
	return c, nil
}

func (c *Config) defaults() {
	if c.TopicPrefix == "" {
		c.TopicPrefix = "bench"
	}
	if c.PayloadSize == 0 {
		c.PayloadSize = 256
	}
	if c.QoS == 0 {
		c.QoS = 1
	}
	if c.KeepAlive == 0 {
		c.KeepAlive = 120 // PINGREQ 间隔 = keepalive/2 = 60s, 与基线周期对齐
	}
	if c.BaselinePeriodS == 0 {
		c.BaselinePeriodS = 60
	}
	if c.Devices.InstanceCount == 0 {
		c.Devices.InstanceCount = 1
	}
	if c.Devices.ConnectRate == 0 {
		c.Devices.ConnectRate = 1000
	}
	if c.PubAckTimeoutS == 0 {
		c.PubAckTimeoutS = 10
	}
	if c.AbortInflightGlobal == 0 {
		c.AbortInflightGlobal = 200_000
	}
	if c.ObserveIntervalS == 0 {
		c.ObserveIntervalS = 10
	}
	if c.OutDir == "" {
		c.OutDir = "out"
	}
}

func (c *Config) validate() error {
	if c.Broker == "" {
		return fmt.Errorf("broker 必填")
	}
	if c.Devices.Total <= 0 {
		return fmt.Errorf("devices.total 必须 > 0")
	}
	if c.Devices.InstanceIndex >= c.Devices.InstanceCount {
		return fmt.Errorf("instance_index 越界")
	}
	if len(c.Steps) == 0 {
		return fmt.Errorf("steps 不能为空")
	}
	for i, s := range c.Steps {
		switch s.Type {
		case "connect":
			if s.Target <= 0 {
				return fmt.Errorf("step %d: connect 需要 target", i)
			}
			if i > 0 && c.Steps[i-1].Type == "connect" && s.Target <= c.Steps[i-1].Target {
				return fmt.Errorf("step %d: connect 目标连接数必须递增", i)
			}
		case "soak":
			if s.Duration <= 0 {
				return fmt.Errorf("step %d: soak 需要 duration", i)
			}
		case "load":
			if s.Rate <= 0 || s.Duration <= 0 {
				return fmt.Errorf("step %d: load 需要 rate/duration", i)
			}
		default:
			return fmt.Errorf("step %d: 未知类型 %q", i, s.Type)
		}
	}
	return nil
}

// Shard 本实例负责的设备全局下标范围 [lo, hi)
func (c *Config) Shard() (lo, hi int) {
	per := c.Devices.Total / c.Devices.InstanceCount
	lo = c.Devices.InstanceIndex * per
	hi = lo + per
	if c.Devices.InstanceIndex == c.Devices.InstanceCount-1 {
		hi = c.Devices.Total // 尾差并入最后一个实例
	}
	return
}

// TotalLoadSec 全部 load 步总时长
func (c *Config) TotalLoadSec() int64 {
	var n int64
	for _, s := range c.Steps {
		if s.Type == "load" {
			n += s.Duration
		}
	}
	return n
}

// StepPlan 带绝对墙钟的执行计划
type StepPlan struct {
	Step
	StartAt time.Time
	EndAt   time.Time
}

// ResolveT0 配置未显式给 T0 时取 now+5s(单实例模式)
func ResolveT0(c *Config) int64 {
	if c.T0 != 0 {
		return c.T0
	}
	return time.Now().Add(5 * time.Second).Unix()
}

// TimelineOf 便捷组合: 解析 T0 并展开时间线
func TimelineOf(c *Config) []StepPlan {
	return c.Timeline(ResolveT0(c))
}

// Timeline 以 t0(unix 秒)展开时间线。
//   - connect 步: 本实例的增量连接数 ÷ 本实例连接速率 = 接入时长, 外加可选保温(Duration)
//   - soak / load 步: 按 Duration 首尾相接
func (c *Config) Timeline(t0 int64) []StepPlan {
	base := time.Unix(t0, 0)
	lo, hi := c.Shard()
	mine := int64(hi - lo)
	plans := make([]StepPlan, 0, len(c.Steps))
	cursor := base
	prevTarget := int64(0)
	for _, s := range c.Steps {
		dur := s.Duration
		if s.Type == "connect" {
			// 本实例本步的增量 = (全局目标 × 本实例份额) − 上一步累计
			myTarget := int64(float64(s.Target) * float64(mine) / float64(c.Devices.Total))
			incr := myTarget - prevTarget
			prevTarget = myTarget
			rate := int64(c.Devices.ConnectRate) // connect_rate 按每实例计
			if rate > 0 && incr > 0 {
				dur = (incr+rate-1)/rate + 2 // +2s: 批量节拍器与握手的余量
			}
			if incr == 0 {
				dur = 0 // 该实例此步无增量, 时间线上直接穿过
			}
			if s.Duration > 0 {
				dur += s.Duration
			}
			if dur < 0 {
				dur = 0
			}
		}
		p := StepPlan{Step: s, StartAt: cursor, EndAt: cursor.Add(time.Duration(dur) * time.Second)}
		plans = append(plans, p)
		cursor = p.EndAt
	}
	return plans
}
