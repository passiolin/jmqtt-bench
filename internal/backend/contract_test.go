package backend

// 数据面 JSON 契约测试 —— 与 broker 侧 KafkaClusterMessageFrameTest 对齐:
//   - 上行: {"username","topic","timestamp","qos","payload"(base64),"node","clientid"}
//   - 下行: {"topic","qos","payload"(base64)}, 零 header

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

func TestDecodeUplinkJSON(t *testing.T) {
	payload := []byte(`{"seq":7,"t0":42,"did":"d1"}`)
	body, err := json.Marshal(map[string]any{
		"username":  "u",
		"topic":     "bench/d1/event/up",
		"timestamp": 123,
		"qos":       1,
		"payload":   payload, // json.Marshal 将 []byte 编码为 base64
		"node":      "n1",
		"clientid":  "d1",
	})
	if err != nil {
		t.Fatal(err)
	}

	m, ok := decodeUplink(&kgo.Record{Value: body})
	if !ok {
		t.Fatal("合法 JSON 应解码成功")
	}
	if m.Topic != "bench/d1/event/up" {
		t.Fatalf("topic = %q", m.Topic)
	}
	if !bytes.Equal(m.Payload, payload) {
		t.Fatalf("payload = %q, want %q", m.Payload, payload)
	}

	if _, ok := decodeUplink(&kgo.Record{Value: []byte("not-json")}); ok {
		t.Fatal("非法 JSON 应失败")
	}
	if _, ok := decodeUplink(&kgo.Record{Value: []byte(`{"qos":1}`)}); ok {
		t.Fatal("缺 topic/payload 应失败")
	}
}

func TestDownlinkRecordJSON(t *testing.T) {
	payload := []byte{0x00, 0x22, 0xFF} // 二进制也必须无损
	rec := downlinkRecord("backend-commands", "d1/dev/service/down", "d1", payload)

	if rec.Topic != "backend-commands" {
		t.Fatalf("record topic = %q", rec.Topic)
	}
	if string(rec.Key) != "d1" {
		t.Fatalf("key = %q, want d1(仅用于分区)", rec.Key)
	}
	if len(rec.Headers) != 0 {
		t.Fatalf("headers = %v, want 零 header", rec.Headers)
	}

	var got struct {
		Topic   string `json:"topic"`
		QoS     int    `json:"qos"`
		Payload []byte `json:"payload"`
	}
	if err := json.Unmarshal(rec.Value, &got); err != nil {
		t.Fatalf("value 不是合法 JSON: %v", err)
	}
	if got.Topic != "d1/dev/service/down" {
		t.Fatalf("topic = %q", got.Topic)
	}
	if got.QoS != 1 {
		t.Fatalf("qos = %d, want 1", got.QoS)
	}
	if !bytes.Equal(got.Payload, payload) {
		t.Fatal("payload 必须经 base64 无损往返")
	}
}
