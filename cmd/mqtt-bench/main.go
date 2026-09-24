package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"jmqtt-bench/internal/backend"
	"jmqtt-bench/internal/device"
	"jmqtt-bench/internal/report"
	"jmqtt-bench/internal/scenario"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "device":
		err = cmdDevice(os.Args[2:])
	case "backend":
		err = cmdBackend(os.Args[2:])
	case "report":
		err = cmdReport(os.Args[2:])
	case "ladder":
		err = cmdLadder(os.Args[2:])
	default:
		usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintf(os.Stderr, `mqtt-bench — jmqtt-broker 全链路容量压测

用法:
  mqtt-bench device  -config bench.yaml [-instance-index N]   模拟终端(海量 MQTT 连接)
  mqtt-bench backend -config bench.yaml [-instance-index N]   模拟后台(Kafka 收发)
  mqtt-bench report  -dir out/ [-out report.md]               聚合产物出报告
  mqtt-bench ladder  -start 1000 -factor 2 -steps 6 -dur 180  生成阶梯场景 YAML
`)
}

func loadCfg(args []string) (*scenario.Config, error) {
	fs := flag.NewFlagSet("config", flag.ContinueOnError)
	cfgPath := fs.String("config", "", "场景 YAML 路径")
	idx := fs.Int("instance-index", -1, "覆盖 instance_index")
	out := fs.String("out-dir", "", "覆盖 out_dir")
	t0 := fs.Int64("t0", 0, "覆盖 T0 (unix 秒, 多实例对齐)")
	if err := fs.Parse(args); err != nil {
		return nil, err
	}
	if *cfgPath == "" {
		return nil, fmt.Errorf("-config 必填")
	}
	cfg, err := scenario.Load(*cfgPath)
	if err != nil {
		return nil, err
	}
	if *idx >= 0 {
		cfg.Devices.InstanceIndex = *idx
	}
	if *out != "" {
		cfg.OutDir = *out
	}
	if *t0 != 0 {
		cfg.T0 = *t0
	}
	return cfg, nil
}

func cmdDevice(args []string) error {
	cfg, err := loadCfg(args)
	if err != nil {
		return err
	}
	return device.Run(cfg)
}

func cmdBackend(args []string) error {
	cfg, err := loadCfg(args)
	if err != nil {
		return err
	}
	return backend.Run(cfg)
}

func cmdReport(args []string) error {
	fs := flag.NewFlagSet("report", flag.ContinueOnError)
	dir := fs.String("dir", "out", "产物目录")
	out := fs.String("out", "", "输出 markdown 路径(缺省打印 stdout)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	md, err := report.Generate(*dir)
	if err != nil {
		return err
	}
	if *out == "" {
		fmt.Print(md)
		return nil
	}
	return os.WriteFile(*out, []byte(md), 0o644)
}

// cmdLadder 生成等比阶梯场景 YAML
func cmdLadder(args []string) error {
	fs := flag.NewFlagSet("ladder", flag.ContinueOnError)
	start := fs.Int64("start", 1000, "起始聚合速率 msg/s")
	factor := fs.Float64("factor", 2, "每步速率倍数")
	steps := fs.Int("steps", 6, "load 步数")
	dur := fs.Int64("dur", 180, "每步持续秒")
	conns := fs.Int("conns", 10000, "设备总数")
	connectRate := fs.Int("connect-rate", 2000, "错峰连接速率 conn/s")
	broker := fs.String("broker", "10.10.10.104:1883", "MQTT broker")
	kafka := fs.String("kafka", "10.10.10.128:9092", "Kafka bootstrap")
	prefix := fs.String("prefix", "bench", "topic 前缀")
	observe := fs.String("observe", "http://10.10.10.104:8922/open/api/jmqtt/info", "broker 指标端点(空串关闭)")
	out := fs.String("out", "", "写文件而非 stdout")
	runID := fs.String("run", "ladder", "run_id")
	if err := fs.Parse(args); err != nil {
		return err
	}

	var sb strings.Builder
	w := func(format string, args ...any) { fmt.Fprintf(&sb, format+"\n", args...) }
	w(`run_id: "%s-%s"`, *runID, time.Now().Format("0102-1504"))
	w("broker: %s", *broker)
	w("auth: {username: jmqtt, password: jmqtt}")
	w("kafka: {bootstrap: %s, uplink: jmqtt-uplink, downlink: jmqtt-downlink, group: bench-backend}", *kafka)
	w("topic_prefix: %s", *prefix)
	w("payload_size: 256")
	w("qos: 1")
	w("keepalive: 120")
	w("baseline_period_s: 60")
	w("devices: {total: %d, instance_index: 0, instance_count: 1, connect_rate: %d}", *conns, *connectRate)
	w("source_ips: []")
	w("t0: 0")
	w("observe_broker: %q", *observe)
	w("observe_interval_s: 10")
	w("pub_ack_timeout_s: 10")
	w("abort_inflight_global: 200000")
	w("out_dir: out")
	w("steps:")
	w("  - {type: connect, target: %d}", *conns)
	rate := *start
	for i := 0; i < *steps; i++ {
		w("  - {type: load, rate: %d, duration: %d}", rate, *dur)
		rate = int64(float64(rate) * *factor)
	}
	if *out == "" {
		fmt.Print(sb.String())
		return nil
	}
	return os.WriteFile(*out, []byte(sb.String()), 0o644)
}
