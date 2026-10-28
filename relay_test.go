package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
)

const testRoom = "Room_0123456789"

func newTestRelay(t *testing.T, mutate func(*Config)) (*Relay, string) {
	t.Helper()
	cfg := DefaultConfig()
	if mutate != nil {
		mutate(&cfg)
	}
	r := NewRelay(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	srv := httptest.NewServer(r)
	t.Cleanup(srv.Close)
	return r, "ws" + strings.TrimPrefix(srv.URL, "http")
}

func dialRaw(base, path string, hdr http.Header) (*websocket.Conn, *http.Response, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return websocket.Dial(ctx, base+path, &websocket.DialOptions{HTTPHeader: hdr})
}

func dial(t *testing.T, base, room, role string) *websocket.Conn {
	t.Helper()
	return dialFrom(t, base, room, role, "")
}

func dialFrom(t *testing.T, base, room, role, xff string) *websocket.Conn {
	t.Helper()
	hdr := http.Header{}
	if xff != "" {
		hdr.Set("X-Forwarded-For", xff)
	}
	c, _, err := dialRaw(base, "/r/"+room+"?role="+role, hdr)
	if err != nil {
		t.Fatalf("dial %s: %v", role, err)
	}
	c.SetReadLimit(64 << 20)
	t.Cleanup(func() { c.CloseNow() })
	return c
}

func read(t *testing.T, c *websocket.Conn) (websocket.MessageType, []byte) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	typ, data, err := c.Read(ctx)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	return typ, data
}

func expectText(t *testing.T, c *websocket.Conn, want string) {
	t.Helper()
	typ, data := read(t, c)
	if typ != websocket.MessageText || string(data) != want {
		t.Fatalf("got %v %q, want text %q", typ, data, want)
	}
}

func expectBinary(t *testing.T, c *websocket.Conn, want []byte) {
	t.Helper()
	typ, data := read(t, c)
	if typ != websocket.MessageBinary || !bytes.Equal(data, want) {
		t.Fatalf("got %v %x, want binary %x", typ, data, want)
	}
}

func expectClose(t *testing.T, c *websocket.Conn, code websocket.StatusCode) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, data, err := c.Read(ctx)
	if err == nil {
		t.Fatalf("got frame %q, want close %d", data, code)
	}
	if got := websocket.CloseStatus(err); got != code {
		t.Fatalf("close status = %d (%v), want %d", got, err, code)
	}
}

func send(t *testing.T, c *websocket.Conn, typ websocket.MessageType, data []byte) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := c.Write(ctx, typ, data); err != nil {
		t.Fatalf("write: %v", err)
	}
}

func env(peer uint32, payload string) []byte {
	out := make([]byte, envelopeHeaderLen+len(payload))
	binary.BigEndian.PutUint32(out, peer)
	copy(out[envelopeHeaderLen:], payload)
	return out
}

func TestRouting(t *testing.T) {
	_, base := newTestRelay(t, nil)
	host := dial(t, base, testRoom, "host")
	g1 := dial(t, base, testRoom, "guest")
	expectText(t, host, `{"t":"peer-joined","peer":1}`)
	g2 := dial(t, base, testRoom, "guest")
	expectText(t, host, `{"t":"peer-joined","peer":2}`)

	// guest 填什么 peerId 都会被改写成自己的编号。
	send(t, g1, websocket.MessageBinary, env(999, "from-g1"))
	expectBinary(t, host, env(1, "from-g1"))
	send(t, g2, websocket.MessageBinary, env(0, "from-g2"))
	expectBinary(t, host, env(2, "from-g2"))

	// peerId 0 广播，帧原样转发。
	send(t, host, websocket.MessageBinary, env(0, "all"))
	expectBinary(t, g1, env(0, "all"))
	expectBinary(t, g2, env(0, "all"))

	// 非 0 只发给对应 guest；g1 下一帧应是之后的广播，而不是发给 g2 的那帧。
	send(t, host, websocket.MessageBinary, env(2, "only-g2"))
	send(t, host, websocket.MessageBinary, env(7, "nobody"))
	send(t, host, websocket.MessageBinary, env(0, "again"))
	expectBinary(t, g2, env(2, "only-g2"))
	expectBinary(t, g2, env(0, "again"))
	expectBinary(t, g1, env(0, "again"))
}

func TestHTTPRejections(t *testing.T) {
	_, base := newTestRelay(t, nil)
	cases := map[string]string{
		"短房间号":     "/r/short?role=host",
		"非法字符":     "/r/Room.0123456789?role=host",
		"百分号编码":    "/r/Room%5F0123456789?role=host",
		"缺少 role":  "/r/" + testRoom,
		"未知 role":  "/r/" + testRoom + "?role=admin",
		"role 重复":  "/r/" + testRoom + "?role=guest&role=host",
		"多余路径":     "/r/" + testRoom + "/x?role=host",
		"不在 /r/ 下": "/x/" + testRoom + "?role=host",
	}
	for name, path := range cases {
		t.Run(name, func(t *testing.T) {
			_, resp, err := dialRaw(base, path, nil)
			if err == nil || resp == nil || resp.StatusCode != http.StatusNotFound {
				t.Fatalf("want 404, got resp=%v err=%v", resp, err)
			}
		})
	}

	resp, err := http.Get("http" + strings.TrimPrefix(base, "ws") + "/r/" + testRoom + "?role=host")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUpgradeRequired {
		t.Fatalf("plain GET status = %d, want 426", resp.StatusCode)
	}
}

func TestDuplicateHostAndMissingRoom(t *testing.T) {
	_, base := newTestRelay(t, nil)
	guest := dial(t, base, testRoom, "guest")
	expectClose(t, guest, closeNoSuchRoom)

	host := dial(t, base, testRoom, "host")
	second := dial(t, base, testRoom, "host")
	expectClose(t, second, closeHostConflict)

	// 被拒的第二个 host 不能拆掉原房间。
	g := dial(t, base, testRoom, "guest")
	expectText(t, host, `{"t":"peer-joined","peer":1}`)
	send(t, g, websocket.MessageBinary, env(0, "still-alive"))
	expectBinary(t, host, env(1, "still-alive"))
}

func TestHostLeaveClosesRoomAndRecreateReissuesIDs(t *testing.T) {
	_, base := newTestRelay(t, nil)
	host := dial(t, base, testRoom, "host")
	g := dial(t, base, testRoom, "guest")
	expectText(t, host, `{"t":"peer-joined","peer":1}`)
	dial(t, base, testRoom, "guest")
	expectText(t, host, `{"t":"peer-joined","peer":2}`)

	host.CloseNow()
	expectText(t, g, `{"t":"room-closed"}`)
	expectClose(t, g, closeRoomClosed)

	host2 := dial(t, base, testRoom, "host")
	dial(t, base, testRoom, "guest")
	expectText(t, host2, `{"t":"peer-joined","peer":1}`)
}

func TestGuestLeaveNotifiesHost(t *testing.T) {
	_, base := newTestRelay(t, nil)
	host := dial(t, base, testRoom, "host")
	g := dial(t, base, testRoom, "guest")
	expectText(t, host, `{"t":"peer-joined","peer":1}`)
	g.Close(websocket.StatusNormalClosure, "")
	expectText(t, host, `{"t":"peer-left","peer":1}`)
}

// 旧房间的 guest 晚于房间重建才断开时，不得把新房间里同编号的 guest 当成自己移除。
func TestStaleGuestDoesNotAffectRecreatedRoom(t *testing.T) {
	_, base := newTestRelay(t, nil)
	host := dial(t, base, testRoom, "host")
	old := dial(t, base, testRoom, "guest")
	expectText(t, host, `{"t":"peer-joined","peer":1}`)

	host.CloseNow()
	host2 := dial(t, base, testRoom, "host")
	fresh := dial(t, base, testRoom, "guest")
	expectText(t, host2, `{"t":"peer-joined","peer":1}`)

	expectText(t, old, `{"t":"room-closed"}`)
	expectClose(t, old, closeRoomClosed)
	old.CloseNow()

	// 给错误的 peer-left 留出产生的时间（读超时会关闭连接，不能用读超时判断静默）；
	// 若真有，host2 收到的下一帧就不会是 fresh 的数据。
	time.Sleep(200 * time.Millisecond)
	send(t, fresh, websocket.MessageBinary, env(0, "from-fresh"))
	expectBinary(t, host2, env(1, "from-fresh"))
	send(t, host2, websocket.MessageBinary, env(1, "to-fresh"))
	expectBinary(t, fresh, env(1, "to-fresh"))
}

func TestIgnoresTextAndHeaderlessFrames(t *testing.T) {
	_, base := newTestRelay(t, nil)
	host := dial(t, base, testRoom, "host")
	g := dial(t, base, testRoom, "guest")
	expectText(t, host, `{"t":"peer-joined","peer":1}`)

	send(t, g, websocket.MessageText, []byte("hello"))
	send(t, g, websocket.MessageBinary, []byte{1, 2, 3})
	send(t, host, websocket.MessageText, []byte("hello"))
	send(t, host, websocket.MessageBinary, []byte{0, 0})
	send(t, g, websocket.MessageBinary, env(0, "real"))
	expectBinary(t, host, env(1, "real"))
	send(t, host, websocket.MessageBinary, env(0, "real"))
	expectBinary(t, g, env(0, "real"))
}

func TestHostSourceRestriction(t *testing.T) {
	_, base := newTestRelay(t, func(c *Config) {
		c.HostPrefixes = []netip.Prefix{netip.MustParsePrefix("100.64.0.0/10")}
	})
	path := "/r/" + testRoom + "?role=host"
	for name, xff := range map[string]string{
		"无转发头（回环地址）":    "",
		"公网地址":          "1.2.3.4",
		"伪造的 tailnet 头": "100.64.0.1, 1.2.3.4",
	} {
		t.Run(name, func(t *testing.T) {
			hdr := http.Header{}
			if xff != "" {
				hdr.Set("X-Forwarded-For", xff)
			}
			_, resp, err := dialRaw(base, path, hdr)
			if err == nil || resp == nil || resp.StatusCode != http.StatusForbidden {
				t.Fatalf("want 403, got resp=%v err=%v", resp, err)
			}
		})
	}

	host := dialFrom(t, base, testRoom, "host", "100.64.0.1")
	// guest 不受来源限制。
	g := dialFrom(t, base, testRoom, "guest", "1.2.3.4")
	expectText(t, host, `{"t":"peer-joined","peer":1}`)
	send(t, g, websocket.MessageBinary, env(0, "ok"))
	expectBinary(t, host, env(1, "ok"))
}

func TestCapacityLimits(t *testing.T) {
	t.Run("房间满员返回 4029", func(t *testing.T) {
		_, base := newTestRelay(t, func(c *Config) { c.MaxGuestsPerRoom = 1 })
		host := dial(t, base, testRoom, "host")
		dial(t, base, testRoom, "guest")
		expectText(t, host, `{"t":"peer-joined","peer":1}`)
		expectClose(t, dial(t, base, testRoom, "guest"), closeRoomFull)
	})
	t.Run("单 IP 连接数超限返回 429", func(t *testing.T) {
		_, base := newTestRelay(t, func(c *Config) { c.MaxConnsPerIP = 1 })
		dialFrom(t, base, testRoom, "host", "100.64.0.1")
		dialFrom(t, base, testRoom, "guest", "1.2.3.4")
		_, resp, err := dialRaw(base, "/r/"+testRoom+"?role=guest", http.Header{"X-Forwarded-For": {"1.2.3.4"}})
		if err == nil || resp == nil || resp.StatusCode != http.StatusTooManyRequests {
			t.Fatalf("want 429, got resp=%v err=%v", resp, err)
		}
	})
	t.Run("房间总数超限返回 503", func(t *testing.T) {
		_, base := newTestRelay(t, func(c *Config) { c.MaxRooms = 1 })
		dial(t, base, testRoom, "host")
		_, resp, err := dialRaw(base, "/r/Other_0123456789?role=host", nil)
		if err == nil || resp == nil || resp.StatusCode != http.StatusServiceUnavailable {
			t.Fatalf("want 503, got resp=%v err=%v", resp, err)
		}
	})
}

func TestHeartbeatDropsSilentPeer(t *testing.T) {
	_, base := newTestRelay(t, func(c *Config) {
		c.PingInterval = 50 * time.Millisecond
		c.PongTimeout = 100 * time.Millisecond
	})
	host := dial(t, base, testRoom, "host")
	// 不读的连接不会回 pong。
	dial(t, base, testRoom, "guest")
	expectText(t, host, `{"t":"peer-joined","peer":1}`)
	expectText(t, host, `{"t":"peer-left","peer":1}`)
}

// writeThrottledConn 限制写出速度，模拟上行很慢的对端：一个大帧要很久才能完整送到 relay。
type writeThrottledConn struct {
	net.Conn
	chunk int
	pause time.Duration
}

func (c writeThrottledConn) Write(p []byte) (int, error) {
	written := 0
	for len(p) > 0 {
		n := min(len(p), c.chunk)
		m, err := c.Conn.Write(p[:n])
		written += m
		if err != nil {
			return written, err
		}
		p = p[n:]
		time.Sleep(c.pause)
	}
	return written, nil
}

// 上行慢的 host 发大快照时，回给 relay 的 pong 排在已写进内核缓冲的几 MB 数据后面，可能几十秒才到。
// 这里的 host 从不读，所以根本不回 pong，只靠持续上传数据：只要还在收到它的数据，relay 就不能把它当死连接断开。
func TestHeartbeatKeepsPeerThatIsStillSending(t *testing.T) {
	_, base := newTestRelay(t, func(c *Config) {
		c.PingInterval = 100 * time.Millisecond
		c.PongTimeout = 300 * time.Millisecond
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	transport := &http.Transport{DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
		conn, err := (&net.Dialer{}).DialContext(ctx, network, addr)
		if err != nil {
			return nil, err
		}
		// 约 200 KiB/s。
		return writeThrottledConn{Conn: conn, chunk: 4 << 10, pause: 20 * time.Millisecond}, nil
	}}
	host, _, err := websocket.Dial(ctx, base+"/r/"+testRoom+"?role=host", &websocket.DialOptions{
		HTTPClient: &http.Client{Transport: transport},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer host.CloseNow()
	guest := dial(t, base, testRoom, "guest")

	// guest 一直在读，会自动回 pong；host 不读，peer-joined 留在它的接收缓冲里。
	// 消息和最终错误走同一个通道，保证 room-closed 不会被随后的关闭错误抢先。
	type result struct {
		data []byte
		err  error
	}
	results := make(chan result, 16)
	go func() {
		for {
			_, data, err := guest.Read(context.Background())
			results <- result{data, err}
			if err != nil {
				return
			}
		}
	}()
	next := func(what string) []byte {
		t.Helper()
		select {
		case r := <-results:
			if r.err != nil {
				t.Fatalf("%s: guest lost the room: %v", what, r.err)
			}
			return r.data
		case <-ctx.Done():
			t.Fatalf("%s: timed out", what)
			return nil
		}
	}

	// 单个 400 KiB 的帧要约 2 秒才能送完，远超 PingInterval+PongTimeout：中途没有完整消息，只有字节在流动。
	// relay 每读 32 KiB（约 160 ms）刷新一次活动时间，低于 PongTimeout。
	big := env(0, strings.Repeat("z", 400<<10))
	if err := host.Write(ctx, websocket.MessageBinary, big); err != nil {
		t.Fatalf("host dropped mid-frame: %v", err)
	}
	if got := next("big frame"); !bytes.Equal(got, big) {
		t.Fatalf("big frame: got len=%d, want len=%d", len(got), len(big))
	}
	// 再隔一段发几条小消息，同样跨过多个心跳周期。
	for i := range 10 {
		want := env(0, strconv.Itoa(i))
		if err := host.Write(ctx, websocket.MessageBinary, want); err != nil {
			t.Fatalf("small frame %d: %v", i, err)
		}
		if got := next("small frame"); !bytes.Equal(got, want) {
			t.Fatalf("small frame %d: got %x, want %x", i, got, want)
		}
		time.Sleep(80 * time.Millisecond)
	}

	// host 停止发送后，既无 pong 也无数据，应被断开，guest 先收到 room-closed。
	if got := next("after host went silent"); string(got) != `{"t":"room-closed"}` {
		t.Fatalf("got %q, want room-closed", got)
	}
}

// throttledConn 限制读取速度，模拟下行很慢但仍在收数据的对端。
type throttledConn struct {
	net.Conn
	chunk int
	pause time.Duration
}

func (c throttledConn) Read(p []byte) (int, error) {
	if len(p) > c.chunk {
		p = p[:c.chunk]
	}
	time.Sleep(c.pause)
	return c.Conn.Read(p)
}

// 慢速下行上发一个大帧要好几秒。relay 的 ping 和对 guest 主动 ping 的自动 pong 都要在帧之间插队，
// 这种仍在正常收数据的连接不能被任何一方当成死连接断开。
func TestHeartbeatSurvivesSlowLargeWrite(t *testing.T) {
	if testing.Short() {
		t.Skip("慢速传输测试需要约 10 秒")
	}
	_, base := newTestRelay(t, func(c *Config) {
		// pong 排在已进入内核缓冲的数据之后才被对端读到，超时得留足排空缓冲的时间，取默认值。
		// 不分片时，库的控制帧抢不到写锁、5 秒内就会失败并断开连接，与这里的超时无关。
		c.PingInterval = 100 * time.Millisecond
	})
	host := dial(t, base, testRoom, "host")

	transport := &http.Transport{DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
		conn, err := (&net.Dialer{}).DialContext(ctx, network, addr)
		if err != nil {
			return nil, err
		}
		// 约 1 MiB/s。
		return throttledConn{Conn: conn, chunk: 32 << 10, pause: 30 * time.Millisecond}, nil
	}}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	guest, _, err := websocket.Dial(ctx, base+"/r/"+testRoom+"?role=guest", &websocket.DialOptions{
		HTTPClient: &http.Client{Transport: transport},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer guest.CloseNow()
	guest.SetReadLimit(64 << 20)
	expectText(t, host, `{"t":"peer-joined","peer":1}`)

	big := env(0, strings.Repeat("y", 10<<20))
	send(t, host, websocket.MessageBinary, big)
	// 真实客户端每 15 秒主动 ping 一次；这里在传输期间密集 ping，pong 同样要排在缓冲数据之后，超时留足。
	pingErr := make(chan error, 1)
	go func() {
		for ctx.Err() == nil {
			pctx, pcancel := context.WithTimeout(ctx, 20*time.Second)
			err := guest.Ping(pctx)
			pcancel()
			if err != nil {
				pingErr <- err
				return
			}
			time.Sleep(200 * time.Millisecond)
		}
	}()
	typ, data, err := guest.Read(ctx)
	if err != nil {
		t.Fatalf("slow guest dropped mid-transfer: %v", err)
	}
	if typ != websocket.MessageBinary || !bytes.Equal(data, big) {
		t.Fatalf("got %v len=%d, want the full frame", typ, len(data))
	}
	select {
	case err := <-pingErr:
		t.Fatalf("guest ping failed during transfer: %v", err)
	default:
	}
}

func TestQueueBoundsTinyFrames(t *testing.T) {
	const limit = 64 << 10
	q := newSendQueue()
	accepted := 0
	for q.push(websocket.MessageBinary, make([]byte, envelopeHeaderLen), limit) {
		accepted++
		if accepted > limit {
			t.Fatal("queue never filled")
		}
	}
	if max := limit / queueEntryOverhead; accepted > max {
		t.Fatalf("accepted %d tiny frames, want at most %d", accepted, max)
	}

	// 按底层数组容量计费：长度很小但容量很大的缓冲区要按容量算。
	q = newSendQueue()
	if q.push(websocket.MessageBinary, make([]byte, envelopeHeaderLen, limit), limit) {
		t.Fatal("a buffer whose capacity alone exceeds the limit was accepted")
	}
}

// Shutdown 与新连接并发时不能 panic，返回后不能还有未登记的连接在握手。
func TestShutdownRacesAdmissions(t *testing.T) {
	r, base := newTestRelay(t, nil)
	host := dial(t, base, testRoom, "host")
	// host 要一直读，才能回应关闭握手，Shutdown 才不必等满超时。
	go func() {
		for {
			if _, _, err := host.Read(context.Background()); err != nil {
				return
			}
		}
	}()

	var wg sync.WaitGroup
	for i := range 40 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			hdr := http.Header{"X-Forwarded-For": {netip.AddrFrom4([4]byte{10, 0, byte(i >> 8), byte(i)}).String()}}
			if c, _, err := dialRaw(base, "/r/"+testRoom+"?role=guest", hdr); err == nil {
				c.CloseNow()
			}
		}()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	r.Shutdown(ctx)
	if ctx.Err() != nil {
		t.Fatal("shutdown timed out")
	}
	wg.Wait()
}

func TestSlowReaderIsDisconnected(t *testing.T) {
	_, base := newTestRelay(t, func(c *Config) { c.MaxQueueBytes = 256 << 10 })
	host := dial(t, base, testRoom, "host")
	dial(t, base, testRoom, "guest") // 从不读取
	expectText(t, host, `{"t":"peer-joined","peer":1}`)

	left := make(chan string, 1)
	go func() {
		_, data, err := host.Read(context.Background())
		if err == nil {
			left <- string(data)
		}
	}()
	chunk := env(0, strings.Repeat("x", 64<<10))
	deadline := time.After(5 * time.Second)
	for {
		select {
		case msg := <-left:
			if msg != `{"t":"peer-left","peer":1}` {
				t.Fatalf("got %q", msg)
			}
			return
		case <-deadline:
			t.Fatal("slow guest was never disconnected")
		default:
		}
		send(t, host, websocket.MessageBinary, chunk)
	}
}

func TestShutdownClosesRooms(t *testing.T) {
	r, base := newTestRelay(t, nil)
	host := dial(t, base, testRoom, "host")
	g := dial(t, base, testRoom, "guest")
	expectText(t, host, `{"t":"peer-joined","peer":1}`)

	done := make(chan struct{})
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		r.Shutdown(ctx)
		close(done)
	}()
	expectText(t, g, `{"t":"room-closed"}`)
	expectClose(t, g, closeRoomClosed)
	expectClose(t, host, websocket.StatusGoingAway)
	<-done

	_, resp, err := dialRaw(base, "/r/"+testRoom+"?role=host", nil)
	if err == nil || resp == nil || resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("after shutdown want 503, got resp=%v err=%v", resp, err)
	}
}
