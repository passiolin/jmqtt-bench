package backend

// 数据面 JSON 契约的编解码, 与 broker 侧 ClusterRecords 对齐(零 Kafka header):
//   - 上行(broker → Kafka): {"username","topic","timestamp","qos","payload"(base64),"node","clientid"}
//   - 下行(后台 → broker): {"topic","qos","payload"(base64)}; key 仅用于分区

import (
	"encoding/base64"
	"encoding/json"
	"strconv"

	"github.com/twmb/franz-go/pkg/kgo"
)

// uplinkMessage 上行 JSON 中 bench 需要的字段。
type uplinkMessage struct {
	Topic   string `json:"topic"`
	Payload []byte `json:"payload"` // JSON 中 base64 → []byte
}

// decodeUplink 解析 broker 产出的上行 JSON; 非法或缺字段返回 false。
func decodeUplink(rec *kgo.Record) (*uplinkMessage, bool) {
	var m uplinkMessage
	if err := json.Unmarshal(rec.Value, &m); err != nil {
		return nil, false
	}
	if m.Topic == "" || len(m.Payload) == 0 {
		return nil, false
	}
	return &m, true
}

// downlinkRecord 构造下行 JSON 记录: {"topic","qos","payload"(base64)}, 零 header;
// key 仅用于分区(取设备标识, 保序)。
func downlinkRecord(kafkaTopic, mqttTopic, did string, payload []byte) *kgo.Record {
	return &kgo.Record{
		Topic: kafkaTopic,
		Key:   []byte(did),
		Value: downlinkJSON(mqttTopic, 1, payload),
	}
}

// downlinkJSON 手工编码 {"topic","qos","payload"}: 字段受控, 避免 encoding/json 反射开销。
func downlinkJSON(topic string, qos int, payload []byte) []byte {
	b := make([]byte, 0, len(topic)+len(payload)*4/3+48)
	b = append(b, `{"topic":"`...)
	b = appendJSONString(b, topic)
	b = append(b, `","qos":`...)
	b = strconv.AppendInt(b, int64(qos), 10)
	b = append(b, `,"payload":"`...)
	b = append(b, base64.StdEncoding.EncodeToString(payload)...)
	b = append(b, `"}`...)
	return b
}

// appendJSONString 把 s 作为 JSON 字符串值写入(处理 " 和 \)。
func appendJSONString(dst []byte, s string) []byte {
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '"':
			dst = append(dst, '\\', '"')
		case '\\':
			dst = append(dst, '\\', '\\')
		default:
			dst = append(dst, s[i])
		}
	}
	return dst
}
