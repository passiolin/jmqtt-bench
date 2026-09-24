// Package proto 定义闭环测量信封: 设备→broker→Kafka→backend→下行→设备,
// 同一个 seq 走完全程。手工序列化 —— 每条消息一次 JSON 编码, 在几十万
// msg/s 下 encoding/json 的反射开销会先吃光压测机。
package proto

import (
	"bytes"
	"strconv"
)

// Envelope 设备业务消息信封。T1 由 backend 回显时填充。
type Envelope struct {
	Seq uint32 `json:"seq"`
	T0  int64  `json:"t0"`           // 设备发送时刻 (unixNano)
	T1  int64  `json:"t1,omitempty"` // backend 消费并回显时刻
	DID string `json:"did"`          // 设备标识 (= clientId)
}

// AppendMarshal 手工编码 Envelope JSON, 并用 pad 字段补齐到 size 字节。
// size<=len 时按实际长度返回。设备侧每条消息都会走这里。
func AppendMarshal(dst []byte, e *Envelope, size int) []byte {
	dst = append(dst, `{"seq":`...)
	dst = strconv.AppendUint(dst, uint64(e.Seq), 10)
	dst = append(dst, `,"t0":`...)
	dst = strconv.AppendInt(dst, e.T0, 10)
	if e.T1 != 0 {
		dst = append(dst, `,"t1":`...)
		dst = strconv.AppendInt(dst, e.T1, 10)
	}
	dst = append(dst, `,"did":"`...)
	dst = append(dst, e.DID...)
	dst = append(dst, '"')
	used := len(dst) + 1 // 计上收尾 '}'
	if size > used {
		dst = append(dst, `,"pad":"`...)
		for len(dst) < size-2 { // 为收尾引号与 '}' 留位
			dst = append(dst, 'x')
		}
		dst = append(dst, '"')
	}
	dst = append(dst, '}')
	return dst
}

// DID 设备标识: d0000001 风格, 全局唯一(实例分片后仍单调)
func DID(globalIdx int) string {
	return "d" + strconv.FormatInt(int64(globalIdx+1), 10)
}

// ParseHeader 从信封 JSON 中提取 did/seq/t0(受控字段, 手工扫描避免反射开销)
func ParseHeader(b []byte) (did string, seq uint32, t0 int64, ok bool) {
	did, ok = scanStringAfter(b, `"did":"`)
	if !ok {
		return "", 0, 0, false
	}
	s, ok := scanNumAfter(b, `"seq":`)
	if !ok {
		return did, 0, 0, false
	}
	t0, _ = scanNumAfter(b, `"t0":`)
	return did, uint32(s), t0, true
}

func scanStringAfter(b []byte, key string) (string, bool) {
	i := bytes.Index(b, []byte(key))
	if i < 0 {
		return "", false
	}
	start := i + len(key)
	end := start
	for end < len(b) && b[end] != '"' {
		end++
	}
	if end == start || end >= len(b) {
		return "", false
	}
	return string(b[start:end]), true
}

func scanNumAfter(b []byte, key string) (int64, bool) {
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
	v, err := strconv.ParseInt(string(b[start:end]), 10, 64)
	return v, err == nil
}

// EchoJSON 构造 backend 的下行回显 record value:
// {"topic":"...","qos":1,"payload":"<envelope json>"}
// 契约见 broker ClusterRecords.DownlinkEnvelope —— value 是 JSON 信封,
// topic 在信封里, record key 仅用于分区(取 deviceId)。
func EchoJSON(buf []byte, upTopic, downTopic string, e *Envelope, size int) []byte {
	inner := AppendMarshal(nil, e, size)
	buf = append(buf, `{"topic":"`...)
	buf = append(buf, downTopic...)
	buf = append(buf, `","qos":1,"payload":"`...)
	buf = appendJSONString(buf, inner)
	buf = append(buf, '"', '}')
	return buf
}

// appendJSONString 把 b 作为 JSON 字符串值写入(处理转义)。信封内容
// 全部是受控字符(字母数字冒号点斜杠引号), 这里只需处理 " 和 \。
func appendJSONString(dst, b []byte) []byte {
	for _, c := range b {
		switch c {
		case '"':
			dst = append(dst, '\\', '"')
		case '\\':
			dst = append(dst, '\\', '\\')
		default:
			dst = append(dst, c)
		}
	}
	return dst
}
