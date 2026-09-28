# 监控配置

## 文件

| 文件 | 说明 |
|---|---|
| prometheus.yml | Prometheus 主配置(抓取 + 告警规则引用) |
| prometheus-alert-rules.yml | 告警规则(critical + warning + infra) |
| grafana-dashboard.json | Grafana 面板(broker 概览 + 协议报文 + 集群总线 + 主机资源 + Kafka 消费组) |

## 部署(.129 bench-monitor)

```bash
# 1) Prometheus 告警规则
scp prometheus-alert-rules.yml root@<MONITOR_HOST>:/etc/prometheus/alert_rules.yml

# 2) Prometheus 主配置(如需更新)
scp prometheus.yml root@<MONITOR_HOST>:/etc/prometheus/prometheus.yml

# 3) Grafana 面板
scp grafana-dashboard.json root@<MONITOR_HOST>:/opt/grafana/grafana-v11/dashboards/jmqtt-bench.json

# 4) 重启 Prometheus(Grafana 自动扫描面板目录)
ssh root@<MONITOR_HOST> 'systemctl restart prometheus'
```

## 告警规则说明

| 级别 | 告警 | 条件 | 持续 | 含义 |
|---|---|---|---|---|
| critical | BrokerDown | up == 0 | 2m | broker 进程不可达 |
| critical | MessagesDropped | rate > 0 | 1m | broker 正在丢弃下行消息 |
| critical | ClusterBusDropped | rate > 0 | 2m | 集群总线丢弃消息 |
| critical | ConnectionStormRejected | rate > 50/s | 2m | 接入风暴被拒绝 |
| critical | BusOutboxNearFull | outbox/outboxCap > 80% | 2m | 集群总线出站队列接近满 |
| warning | BrokerHeapHigh | heap/max > 85% | 5m | JVM 堆使用率高 |
| warning | AbnormalDisconnects | rate > 100/s | 2m | 异常断连速率高 |
| warning | SendQueueBackpressure | enqueued > 10000 | 3m | 发送队列积压 |
| warning | DownlinkConsumerLag | lag > 50000 | 5m | Kafka 下行消费积压 |
| warning | HostCPUHigh | CPU > 90% | 5m | 主机 CPU 高 |
| warning | HostMemoryHigh | mem > 90% | 5m | 主机内存高 |

## 阈值调优

告警阈值基于 30 万连接 + 5k TPS 压测基线调整。实际业务量较小时按比例缩小;
接入风暴场景把 `ConnectionStormRejected` 阈值临时调高。
