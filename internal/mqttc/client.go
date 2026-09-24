package mqttc

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"
)

// Handlers 是客户端的回调集, 全部在 reader goroutine 上调用 —— 必须轻,
// 重活交给调用方自己的队列。OnMessage 返回后客户端才会代发 PUBACK(QoS1)。
type Handlers struct {
	OnConnack func(c *Connack)
	OnSuback  func(s *Suback)
	OnMessage func(m *Message)
	// OnPuback 在对应 PUBLISH 的 PUBACK 到达时调用; elapsed 为发送→确认耗时
	OnPuback   func(pid uint16, elapsed time.Duration)
	OnPingresp func()
	// OnClose 在连接断开时调用(含主动 Close 之外的一切断开)
	OnClose func(err error)
}

// Options 连接参数
type Options struct {
	Addr      string // broker 地址 host:port
	ClientID  string
	Username  string
	Password  string
	KeepAlive uint16 // 秒; PINGREQ 间隔取其一半
	// CleanSession=false 建持久会话(broker 侧落 Redis), true 每次新建
	CleanSession bool
	// SourceIP 非空时绑定该源地址(源 IP 池轮询的落点)
	SourceIP net.IP
	// MaxPacketSize 限制入站报文剩余长度, 默认 1MB
	MaxPacketSize int
	DialTimeout   time.Duration
	WriteTimeout  time.Duration
}

// Client 是单条 MQTT 连接。非并发安全: 同一时刻至多一个 Publish 在途以外,
// 并发 Publish 靠内部互斥串行化写入;读侧由独立 goroutine 处理。
type Client struct {
	opts Options
	h    Handlers

	conn     net.Conn
	writeMu  sync.Mutex
	nextPID  uint16
	inflight map[uint16]*inflightEntry

	closed    chan struct{}
	closeOnce sync.Once
}

// inflightEntry 一条已发出待确认的 QoS1 消息
type inflightEntry struct {
	ch     chan struct{}
	sentAt time.Time
}

var ErrClosed = errors.New("mqttc: client closed")

// Connect 拨号 + CONNECT/CONNACK + 启动读循环。
func Connect(opts Options, h Handlers) (*Client, error) {
	if opts.KeepAlive == 0 {
		opts.KeepAlive = 60
	}
	if opts.DialTimeout == 0 {
		opts.DialTimeout = 10 * time.Second
	}
	if opts.WriteTimeout == 0 {
		opts.WriteTimeout = 10 * time.Second
	}
	if opts.MaxPacketSize == 0 {
		opts.MaxPacketSize = 1 << 20
	}
	dialer := &net.Dialer{Timeout: opts.DialTimeout}
	if opts.SourceIP != nil {
		dialer.LocalAddr = &net.TCPAddr{IP: opts.SourceIP}
	}
	conn, err := dialer.Dial("tcp4", opts.Addr)
	if err != nil {
		return nil, err
	}
	c := &Client{
		opts:     opts,
		h:        h,
		conn:     conn,
		nextPID:  1, // packetId 合法域 1..65535
		inflight: make(map[uint16]*inflightEntry),
		closed:   make(chan struct{}),
	}
	if err := c.handshake(); err != nil {
		conn.Close()
		return nil, err
	}
	go c.readLoop()
	go c.pingLoop()
	return c, nil
}

func (c *Client) handshake() error {
	_ = c.conn.SetDeadline(time.Now().Add(c.opts.DialTimeout))
	if _, err := c.conn.Write(encodeConnect(c.opts.ClientID, c.opts.Username, c.opts.Password, c.opts.KeepAlive, c.opts.CleanSession)); err != nil {
		return err
	}
	first, body, err := readPacket(c.conn, c.opts.MaxPacketSize)
	if err != nil {
		return err
	}
	if first>>4 != pktConnack {
		return fmt.Errorf("mqttc: expected CONNACK, got type %d", first>>4)
	}
	ca, err := decodeConnack(body)
	if err != nil {
		return err
	}
	if ca.ReturnCode != 0 {
		return fmt.Errorf("mqttc: CONNECT refused, code=0x%02x", ca.ReturnCode)
	}
	_ = c.conn.SetDeadline(time.Time{})
	if c.h.OnConnack != nil {
		c.h.OnConnack(ca)
	}
	return nil
}

// PublishAsync 发一条 QoS1 PUBLISH, 不等待 PUBACK 即返回。
// 确认经 OnPuback 回调交付 —— 开环加压的正确形态。
func (c *Client) PublishAsync(topic string, payload []byte) (uint16, error) {
	pid, wait, err := c.beginInflight()
	if err != nil {
		return 0, err
	}
	if _, err := c.write(encodePublish(&Message{Topic: topic, Payload: payload, QoS: 1, PacketId: pid})); err != nil {
		c.endInflight(pid)
		return 0, err
	}
	_ = wait // 无人等待: PUBACK 经 endInflight 关闭该通道即回收
	return pid, nil
}

// Subscribe 发 SUBSCRIBE 并等待 SUBACK。
func (c *Client) Subscribe(filter string, qos byte, timeout time.Duration) (*Suback, error) {
	pid, wait, err := c.beginInflight()
	if err != nil {
		return nil, err
	}
	if _, err := c.write(encodeSubscribe(pid, filter, qos)); err != nil {
		c.endInflight(pid)
		return nil, err
	}
	select {
	case <-wait:
	case <-time.After(timeout):
		c.endInflight(pid)
		return nil, fmt.Errorf("mqttc: SUBACK timeout")
	case <-c.closed:
		c.endInflight(pid)
		return nil, ErrClosed
	}
	return nil, nil // SUBACK 结果经 OnSuback 回调交付; 这里只保证完成
}

// Publish 发一条 QoS1 PUBLISH 并等待 PUBACK。payload 会被内部复制。
func (c *Client) Publish(topic string, payload []byte, timeout time.Duration) error {
	pid, wait, err := c.beginInflight()
	if err != nil {
		return err
	}
	if _, err := c.write(encodePublish(&Message{Topic: topic, Payload: payload, QoS: 1, PacketId: pid})); err != nil {
		c.endInflight(pid)
		return err
	}
	select {
	case <-wait:
		return nil
	case <-time.After(timeout):
		c.endInflight(pid)
		return fmt.Errorf("mqttc: PUBACK timeout")
	case <-c.closed:
		c.endInflight(pid)
		return ErrClosed
	}
}

// beginInflight 分配 packetId 并注册等待通道。
// packetId 合法域是 1..65535, 0 会被 broker 解码器拒绝。
func (c *Client) beginInflight() (uint16, chan struct{}, error) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	pid := c.nextPID
	if pid == 0 || pid == 0xffff {
		pid = 1
		c.nextPID = 2
	} else {
		c.nextPID = pid + 1
	}
	if _, busy := c.inflight[pid]; busy {
		return 0, nil, fmt.Errorf("mqttc: packet id %d busy", pid)
	}
	ch := make(chan struct{})
	c.inflight[pid] = &inflightEntry{ch: ch, sentAt: time.Now()}
	return pid, ch, nil
}

func (c *Client) endInflight(pid uint16) {
	c.writeMu.Lock()
	if e, ok := c.inflight[pid]; ok {
		delete(c.inflight, pid)
		close(e.ch)
	}
	c.writeMu.Unlock()
}

func (c *Client) write(p []byte) (int, error) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if c.opts.WriteTimeout > 0 {
		_ = c.conn.SetWriteDeadline(time.Now().Add(c.opts.WriteTimeout))
	}
	return c.conn.Write(p)
}

func (c *Client) readLoop() {
	r := bufio.NewReaderSize(c.conn, 64<<10)
	for {
		first, body, err := readPacket(r, c.opts.MaxPacketSize)
		if err != nil {
			c.close(err)
			return
		}
		switch first >> 4 {
		case pktPublish:
			m, err := decodePublish(first, body)
			if err != nil {
				c.close(err)
				return
			}
			if c.h.OnMessage != nil {
				c.h.OnMessage(m)
			}
			if m.QoS > 0 && m.PacketId != 0 {
				if _, err := c.write(encodePuback(m.PacketId)); err != nil {
					c.close(err)
					return
				}
			}
		case pktPuback:
			var pid uint16
			if len(body) >= 2 {
				pid = uint16(body[0])<<8 | uint16(body[1])
			}
			var elapsed time.Duration
			c.writeMu.Lock()
			if e, ok := c.inflight[pid]; ok {
				elapsed = time.Since(e.sentAt)
			}
			c.writeMu.Unlock()
			c.endInflight(pid)
			if c.h.OnPuback != nil {
				c.h.OnPuback(pid, elapsed)
			}
		case pktSuback:
			if s, err := decodeSuback(body); err == nil {
				c.endInflight(s.PacketId)
				if c.h.OnSuback != nil {
					c.h.OnSuback(s)
				}
			}
		case pktPingresp:
			if c.h.OnPingresp != nil {
				c.h.OnPingresp()
			}
		default:
			// 未知类型: 压测场景直接断开重连, 不做容错解析
			c.close(fmt.Errorf("mqttc: unexpected packet type %d", first>>4))
			return
		}
	}
}

func (c *Client) pingLoop() {
	iv := time.Duration(c.opts.KeepAlive) * time.Second / 2
	if iv < 5*time.Second {
		iv = 5 * time.Second
	}
	t := time.NewTicker(iv)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			if _, err := c.write([]byte{pktPingreq << 4, 0}); err != nil {
				c.close(err)
				return
			}
		case <-c.closed:
			return
		}
	}
}

// Close 主动关闭连接(不触发 OnClose 的错误路径语义, 但仍会触发 OnClose)
func (c *Client) Close() {
	c.close(nil)
}

func (c *Client) close(err error) {
	c.closeOnce.Do(func() {
		close(c.closed)
		// 清空全部在途等待, 避免调用方悬挂
		c.writeMu.Lock()
		for pid, e := range c.inflight {
			delete(c.inflight, pid)
			close(e.ch)
		}
		c.writeMu.Unlock()
		c.conn.Close()
		if c.h.OnClose != nil {
			c.h.OnClose(err)
		}
	})
}
