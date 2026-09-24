package mqttc

import (
	"encoding/json"
	"net"
	"testing"
	"time"

	"jmqtt-bench/internal/proto"
)

// mockBroker 最小 TCP broker: CONNACK / SUBACK / PUBACK / 转发一条下行
type mockBroker struct {
	ln       net.Listener
	onPub    func(t string, payload []byte)
	sendDown func(conn net.Conn)
}

func startMock(t *testing.T, b *mockBroker) string {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	b.ln = ln
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go handleMock(conn, b)
		}
	}()
	t.Cleanup(func() { ln.Close() })
	return ln.Addr().String()
}

func handleMock(conn net.Conn, b *mockBroker) {
	defer conn.Close()
	// 期望 CONNECT
	first, body, err := readPacket(conn, 1<<20)
	if err != nil || first>>4 != pktConnect {
		return
	}
	_ = body
	conn.Write([]byte{pktConnack << 4, 2, 0, 0})
	if b.sendDown != nil {
		go b.sendDown(conn)
	}
	for {
		first, body, err := readPacket(conn, 1<<20)
		if err != nil {
			return
		}
		switch first >> 4 {
		case pktSubscribe:
			// pid(2) + filter(2+n) + qos(1) → SUBACK: pid + 1 个授予码
			pid := []byte{body[0], body[1]}
			conn.Write(append([]byte{pktSuback << 4, 3}, append(pid, 1)...))
		case pktPublish:
			m, err := decodePublish(first, body)
			if err != nil {
				return
			}
			if b.onPub != nil {
				b.onPub(m.Topic, m.Payload)
			}
			if m.QoS > 0 {
				var ack [4]byte
				ack[0] = pktPuback << 4
				ack[1] = 2
				ack[2] = byte(m.PacketId >> 8)
				ack[3] = byte(m.PacketId)
				conn.Write(ack[:])
			}
		case pktPingreq:
			conn.Write([]byte{pktPingresp << 4, 0})
		case pktDisconnect:
			return
		}
	}
}

func TestConnectSubscribePublish(t *testing.T) {
	gotPub := make(chan Message, 4)
	mb := &mockBroker{
		onPub: func(topic string, payload []byte) {
			gotPub <- Message{Topic: topic, Payload: payload}
		},
	}
	addr := startMock(t, mb)

	connacked := make(chan struct{}, 1)
	c, err := Connect(Options{
		Addr: addr, ClientID: "tester", Username: "u", Password: "p", KeepAlive: 120,
	}, Handlers{
		OnConnack: func(ca *Connack) { connacked <- struct{}{} },
		OnPuback:  func(pid uint16, el time.Duration) {},
	})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer c.Close()
	select {
	case <-connacked:
	default:
	}

	if _, err := c.Subscribe("bench/d1/service/down", 1, 3*time.Second); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	done := make(chan error, 1)
	go func() { done <- c.Publish("bench/d1/event/up", []byte(`{"seq":1,"t0":123,"did":"d1"}`), 3*time.Second) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("publish: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("publish timeout")
	}

	select {
	case m := <-gotPub:
		if m.Topic != "bench/d1/event/up" {
			t.Fatalf("topic = %q", m.Topic)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("broker 未收到 PUBLISH")
	}
}

func TestDownlinkDelivery(t *testing.T) {
	mb := &mockBroker{
		sendDown: func(conn net.Conn) {
			// 主动下发一条 QoS1 PUBLISH
			pkt := encodePublish(&Message{Topic: "bench/d1/service/down",
				Payload: []byte(`{"seq":7,"t0":42,"did":"d1"}`), QoS: 1, PacketId: 99})
			conn.Write(pkt)
		},
	}
	addr := startMock(t, mb)

	got := make(chan Message, 4)
	c, err := Connect(Options{Addr: addr, ClientID: "tester", KeepAlive: 120}, Handlers{
		OnMessage: func(m *Message) { got <- *m },
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	select {
	case m := <-got:
		if m.Topic != "bench/d1/service/down" || m.QoS != 1 {
			t.Fatalf("unexpected downlink: %+v", m)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("未收到下行 PUBLISH")
	}
}

func TestEnvelopeRoundtrip(t *testing.T) {
	// 信封序列化 → ParseHeader 往返
	e := proto.Envelope{Seq: 12345, T0: 1727000000123456789, DID: "d0000042"}
	b := proto.AppendMarshal(nil, &e, 256)
	if len(b) != 256 {
		t.Fatalf("size = %d, want 256", len(b))
	}
	did, seq, t0, ok := proto.ParseHeader(b)
	if !ok || did != "d0000042" || seq != 12345 || t0 != e.T0 {
		t.Fatalf("roundtrip mismatch: %v %d %d %v", did, seq, t0, ok)
	}
	// EchoJSON: payload 作为 JSON 字符串正确转义, topic 在信封里
	echo := proto.EchoJSON(nil, "bench/d/event/up", "bench/d/service/down",
		&proto.Envelope{Seq: 1, T0: 2, T1: 3, DID: "d"}, 256)
	var down struct {
		Topic   string `json:"topic"`
		QoS     int    `json:"qos"`
		Payload string `json:"payload"`
	}
	if err := json.Unmarshal(echo, &down); err != nil {
		t.Fatalf("echo json: %v", err)
	}
	if down.Topic != "bench/d/service/down" || down.QoS != 1 {
		t.Fatalf("echo envelope: %+v", down)
	}
	did2, seq2, t02, ok2 := proto.ParseHeader([]byte(down.Payload))
	if !ok2 || did2 != "d" || seq2 != 1 || t02 != 2 {
		t.Fatalf("echo payload roundtrip: %v %d %d %v", did2, seq2, t02, ok2)
	}
}
