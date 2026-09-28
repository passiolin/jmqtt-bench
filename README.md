# jmqtt-bench

jmqtt-broker 全链路容量压测工具。模拟终端经 MQTT 收发消息、模拟后台经 Kafka 收发消息,
把「设备 → broker → Kafka 上行 → 后台消费 → Kafka 下行 → broker → 设备」整条闭环
跑起来并测准三件事:**吞吐上限、端到端时延、消息在哪条腿上丢**。

Go 实现,单个二进制 `mqtt-bench`,三个子命令对应三个角色:

```
cmd/mqtt-bench
├── device    模拟终端: 海量 MQTT 连接 + 错峰接入 + 60s 基线 + 阶梯开环加压 + 回显接收
├── backend   模拟后台: Kafka 消费上行 → 记录时延/丢失/重复 → 逐条回显到 downlink
├── report    聚合各实例 JSON 产物 → Markdown 报告
└── ladder    生成等比阶梯场景 YAML
```

## 测量模型

每条业务消息携带信封 `{"seq":n,"t0":<发送纳秒>,"did":<设备>}`,走完全程:

```
device ──PUBLISH(event/up, QoS1)──> broker ──Kafka uplink──> backend(记 t1)
   ^                                                        │ 按 seq 认领: 跳号=丢, 回退=重
   └────────PUBLISH(service/down, 回显 seq+t0+t1) <─────────┘ produce 到 downlink topic
   │
   └─ 收到回显记 RTT=t2-t0; 同设备下行全程 FIFO, 单调性成立
```

丢失归因靠三个集合做差,不靠猜:

| 差值 | 含义 |
|---|---|
| 已发送 − backend 已消费 | 上行腿丢失(broker → Kafka) |
| backend 已回显 − 设备已收到 | 下行腿丢失(Kafka → broker → 设备) |
| seq 回退/重复 | QoS1 重发产生的端到端重复 |

三种延迟分开记:**ACK RTT**(设备→broker 的 PUBACK,即投递腿)、
**上行腿时延**(t0→backend 消费)、**全环 RTT**(t0→回显收回)。

## 关键设计约束

- **开环加压**: 设备按目标速率发布、不等 PUBACK。broker 跟不上时 PUBACK 延迟、
  在途积压、broker 背压计数爬升 —— 这些正是要捕获的容量信号,闭环限速会把拐点藏起来。
- **错峰启动**: 连接阶段按 `connect_rate` 限速分批接入,基线定时器带随机相位,
  绝不一次性发起几十万连接。
- **60s 基线**: 每设备每 60s 一条 PINGREQ(keepalive=120 时 PING 间隔恰为 60s)
  加一条基线业务消息,load 步的速率叠加在其上 —— 对应真实设备「空闲也在报」的行为。
- **墙钟 lockstep**: 多实例分片时用同一 `--t0`(unix 秒)对齐时间线,不需要中心协调者。
  T0 已过期的实例拒绝启动(防静默空跑)。
- **自研极简 MQTT 3.1.1 客户端**(`internal/mqttc`): 只实现 bench 需要的报文集,
  与 broker 的 netty-codec-mqtt 编解码理解相互独立 —— 两边同源实现一起错、测试照样过
  的风险不存在。每连接 1 读 goroutine,写侧互斥,QoS1 异步发布 + PUBACK 回调。
- **源 IP 池**: 单 IP 对单一目标只有约 6.4 万端口可用,`source_ips` 配多个源地址,
  连接时 `LocalAddr` 轮询绑定。
- **安全阀**: 全局在途(发送−确认)超 `abort_inflight_global` 且确认停滞 60s,
  本实例中止并落盘部分结果。

## Kafka 契约(与 broker 侧对齐)

见 broker `cluster/kafka/ClusterRecords.java`:

- **上行**(broker → Kafka): value 为 JSON 信封
  `{"username","topic","timestamp","qos","payload","node","clientid"}`,
  payload 是字符串化的原始消息体; header `jmqtt-kind=routed-publish`;
  key 按路由 `key: device`(=clientId)分区,同设备分区内严格有序。
- **下行**(后台 → Kafka): value 为 JSON `{"topic","qos","payload"}`,
  qos 缺省 1; record 的 key 仅用于分区(取 deviceId);broker 每节点独立 group
  消费、只做本地投递、绝不回写。

broker 侧参考配置(压测用,`application-local.yml`):

```yaml
jmqtt:
  broker:
    kafka:
      enabled: true
      bootstrap-servers: <KAFKA_HOST>:9092
      broadcast-enabled: false
      routes:
        - filters: ["bench/+/event/up"]
          topic: jmqtt-uplink
          key: device
      downlink:
        - topic: jmqtt-downlink
```

## 使用

```bash
# 构建
go build -o bin/mqtt-bench ./cmd/mqtt-bench

# 生成阶梯场景(写入 bench.yaml 后按需手改)
mqtt-bench ladder -start 1000 -factor 2 -steps 6 -dur 180 -conns 10000 -connect-rate 2000 -out bench.yaml

# 起压测(顺序重要: backend 先入消费组, device 后接)
mqtt-bench backend -config bench.yaml --t0 $(($(date +%s) + 60)) &
mqtt-bench device  -config bench.yaml --t0 $(($(date +%s) + 60)) &

# 多实例分片(几十万连接时按 clientId 段拆多个进程/多台机器)
mqtt-bench device -config bench.yaml --instance-index 0 &   # instance_count 在 yaml 里
mqtt-bench device -config bench.yaml --instance-index 1 &

# 聚合出报告
mqtt-bench report -dir out/ -out report.md
```

产物: `out/device-<i>.json`(各步发送/确认/回显/RTT 直方图 + broker 指标快照)、
`out/backend-<i>.json`(消费/回显/gap/重复/上行时延)。report 做三集合差输出逐步表格。

## 场景配置速查

| 字段 | 说明 | 缺省 |
|---|---|---|
| `broker` | MQTT 地址 host:port | 必填 |
| `kafka.{bootstrap,uplink,downlink,group}` | Kafka 三个 topic 与消费组 | 必填 |
| `devices.{total,instance_index,instance_count}` | 设备总数与本实例分片 | 必填 |
| `devices.connect_rate` | 错峰连接速率 conn/s | 1000 |
| `source_ips` | 源 IP 池(单 IP 端口上限的解法) | 不绑定 |
| `payload_size` | 业务消息字节数(含信封) | 256 |
| `baseline_period_s` | 基线周期(心跳+一条业务消息) | 60 |
| `steps` | `connect`(target) + `load`(rate,duration) 序列 | 必填 |
| `t0` | 多实例对齐的墙钟起点; 0 = 自取 now+5s | 0 |
| `abort_inflight_global` | 安全阀在途阈值 | 200000 |

## 压测环境参考(<PVE_HOST>, PVE 32C/256G)

| VM | 名称 | 规格 | 地址 | 角色 |
|---|---|---|---|---|
| 105 | jmqtt-broker | 8C/16G/60G NVMe | <BROKER_HOST> | 被测 broker |
| 106 | bench-device | 8C/16G/40G NVMe, 7 个源 IP | <DEVICE_HOST_1>-.111 | 模拟终端 |
| 107 | bench-backend | 4C/8G/40G NVMe | <BACKEND_HOST> | 模拟后台 |
| 108 | bench-kafka | 4C/8G/60G NVMe | <KAFKA_HOST> | Kafka 4.1.2 KRaft 单节点 |

模板: PVE VMID 101(Ubuntu 26.04, 已预置公钥),全量克隆到 `nvme` 存储。
**克隆后必须禁用 `apt-daily-upgrade.timer`** —— unattended-upgrades 会在压测中途
重启服务(会导致压测中途所有设备重连)。

压测结果与结论见 broker 仓库 [passiolin/jmqtt-broker](https://github.com/passiolin/jmqtt-broker) 的压测章节。

## 许可

[Apache License 2.0](LICENSE)
