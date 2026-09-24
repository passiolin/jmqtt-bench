// Package mqttc 实现压测所需的极简 MQTT 3.1.1 客户端。
//
// 只实现 bench 需要的报文集: CONNECT/CONNACK、SUBSCRIBE/SUBACK、
// PUBLISH/PUBACK、PINGREQ/PINGRESP、DISCONNECT。与 broker 侧的
// netty-codec-mqtt 编解码理解相互独立 —— 两边对线格式的理解若同时有错,
// 测试也能通过;用独立实现是 e2e 可信的前提。
package mqttc

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// 报文类型(固定头高 4 位)
const (
	pktConnect     = 1
	pktConnack     = 2
	pktPublish     = 3
	pktPuback      = 4
	pktSubscribe   = 8
	pktSuback      = 9
	pktPingreq     = 12
	pktPingresp    = 13
	pktDisconnect  = 14
	flagConnack    = 0x00
	flagPuback     = 0x00
	flagPingresp   = 0x00
	flagDisconnect = 0x00
)

var errShortPacket = errors.New("mqttc: short packet")

// encodeFixedHeader 写固定头: 首字节 + 变长剩余长度(最多 4 字节)
func encodeFixedHeader(buf []byte, first byte, remLen int) []byte {
	buf = append(buf, first)
	for {
		b := byte(remLen % 128)
		remLen /= 128
		if remLen > 0 {
			b |= 0x80
		}
		buf = append(buf, b)
		if remLen == 0 {
			return buf
		}
	}
}

func writeMQTTString(buf []byte, s string) []byte {
	var l [2]byte
	binary.BigEndian.PutUint16(l[:], uint16(len(s)))
	buf = append(buf, l[:]...)
	return append(buf, s...)
}

// encodeConnect 构造 CONNECT(MQTT 3.1.1, level 4)
func encodeConnect(clientId, username, password string, keepalive uint16, cleanSession bool) []byte {
	var payload []byte
	payload = append(payload, 0, 4, 'M', 'Q', 'T', 'T', 4) // 协议名 + level 4
	// 连接标志位 (spec 3.1.2.3): bit1=cleanSession bit2=willFlag bit3-4=willQos
	// bit5=willRetain bit6=password bit7=username
	flags := byte(0)
	if cleanSession {
		flags |= 1 << 1
	}
	if username != "" {
		flags |= 1 << 7
	}
	if password != "" {
		flags |= 1 << 6
	}
	payload = append(payload, flags)
	var ka [2]byte
	binary.BigEndian.PutUint16(ka[:], keepalive)
	payload = append(payload, ka[:]...)
	payload = writeMQTTString(payload, clientId)
	if username != "" {
		payload = writeMQTTString(payload, username)
	}
	if password != "" {
		payload = writeMQTTString(payload, password)
	}
	return append(encodeFixedHeader(nil, pktConnect<<4, len(payload)), payload...)
}

// Connack 是 CONNACK 解析结果
type Connack struct {
	SessionPresent bool
	ReturnCode     byte // 0 = 接受
}

// decodeConnack 解析 CONNACK(4 字节固定体)
func decodeConnack(body []byte) (*Connack, error) {
	if len(body) < 2 {
		return nil, errShortPacket
	}
	return &Connack{SessionPresent: body[0]&1 == 1, ReturnCode: body[1]}, nil
}

// encodeSubscribe 构造一条过滤器的 SUBSCRIBE(packetId 由调用方分配)
func encodeSubscribe(packetId uint16, filter string, qos byte) []byte {
	var body []byte
	var pid [2]byte
	binary.BigEndian.PutUint16(pid[:], packetId)
	body = append(body, pid[:]...)
	body = writeMQTTString(body, filter)
	body = append(body, qos) // 单条过滤器, 无通配/retain 标志位需求
	return append(encodeFixedHeader(nil, pktSubscribe<<4|0b0010, len(body)), body...)
}

// Suback 是 SUBACK 解析结果
type Suback struct {
	PacketId   uint16
	ReturnCode byte // 0x00/0x01/0x02 = 授予的 QoS; 0x80 = 失败
}

func decodeSuback(body []byte) (*Suback, error) {
	if len(body) < 3 {
		return nil, errShortPacket
	}
	return &Suback{PacketId: binary.BigEndian.Uint16(body), ReturnCode: body[2]}, nil
}

// Message 是一条 PUBLISH(双向通用)
type Message struct {
	Topic    string
	Payload  []byte
	PacketId uint16 // QoS>0 时有效
	QoS      byte
	Dup      bool
	Retain   bool
}

// encodePublish 构造 PUBLISH。qos=1 时携带 packetId。
func encodePublish(m *Message) []byte {
	var first byte = pktPublish << 4
	if m.Dup {
		first |= 0b1000
	}
	first |= m.QoS << 1
	if m.Retain {
		first |= 1
	}
	var body []byte
	body = writeMQTTString(body, m.Topic)
	if m.QoS > 0 {
		var pid [2]byte
		binary.BigEndian.PutUint16(pid[:], m.PacketId)
		body = append(body, pid[:]...)
	}
	body = append(body, m.Payload...)
	return append(encodeFixedHeader(nil, byte(first), len(body)), body...)
}

func decodePublish(first byte, body []byte) (*Message, error) {
	if len(body) < 2 {
		return nil, errShortPacket
	}
	tl := int(binary.BigEndian.Uint16(body))
	if len(body) < 2+tl {
		return nil, errShortPacket
	}
	m := &Message{
		Topic:  string(body[2 : 2+tl]),
		QoS:    (first >> 1) & 0b11,
		Dup:    first&0b1000 != 0,
		Retain: first&1 != 0,
	}
	rest := body[2+tl:]
	if m.QoS > 0 {
		if len(rest) < 2 {
			return nil, errShortPacket
		}
		m.PacketId = binary.BigEndian.Uint16(rest)
		rest = rest[2:]
	}
	m.Payload = append([]byte(nil), rest...)
	return m, nil
}

// encodePuback 构造 PUBACK
func encodePuback(packetId uint16) []byte {
	var body [2]byte
	binary.BigEndian.PutUint16(body[:], packetId)
	return append(encodeFixedHeader(nil, pktPuback<<4|flagPuback, 2), body[:]...)
}

// readPacket 从 r 读一个完整报文。返回类型、剩余体。
func readPacket(r io.Reader, maxRemaining int) (byte, []byte, error) {
	var fh [1]byte
	if _, err := io.ReadFull(r, fh[:]); err != nil {
		return 0, nil, err
	}
	first := fh[0]
	remLen, mul, n := 0, 1, 0
	for {
		if n == 4 {
			return 0, nil, fmt.Errorf("mqttc: malformed remaining length")
		}
		if _, err := io.ReadFull(r, fh[:]); err != nil {
			return 0, nil, err
		}
		remLen += int(fh[0]&0x7f) * mul
		mul *= 128
		n++
		if fh[0]&0x80 == 0 {
			break
		}
	}
	if remLen > maxRemaining {
		return 0, nil, fmt.Errorf("mqttc: packet too large (%d)", remLen)
	}
	body := make([]byte, remLen)
	if _, err := io.ReadFull(r, body); err != nil {
		return 0, nil, err
	}
	return first, body, nil
}
