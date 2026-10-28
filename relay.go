package main

import (
	"context"
	"encoding/binary"
	"log/slog"
	"net/http"
	"net/netip"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
)

// 与 omp collab 客户端约定的关闭码，含义见 oh-my-pi 的 relay-client.ts。
const (
	closeRoomClosed   websocket.StatusCode = 4001 // host 离开，房间被回收；guest 收到后会重连等待 host 重建
	closeNoSuchRoom   websocket.StatusCode = 4004 // guest 加入了不存在的房间
	closeHostConflict websocket.StatusCode = 4009 // 房间已有 host
	closeRoomFull     websocket.StatusCode = 4029 // 房间人数已满（客户端视为致命错误，不重连）
)

// 信封头：[4 字节大端 peerId][密文]。relay 只读写这 4 字节，密文原样转发。
const envelopeHeaderLen = 4

var roomPathRE = regexp.MustCompile(`^/r/([A-Za-z0-9_-]{10,64})$`)

// Config 是 relay 的运行参数。
type Config struct {
	// HostPrefixes 限定允许以 host 身份开房间的来源地址；为空表示不限制。
	HostPrefixes []netip.Prefix
	// TrustForwardedFor 为 true 时，来自回环地址的请求取 X-Forwarded-For 最后一跳作为客户端 IP（前面是 Caddy）。
	TrustForwardedFor bool

	MaxRooms         int   // 同时存在的房间上限
	MaxGuestsPerRoom int   // 每个房间的 guest 上限
	MaxConnsPerIP    int   // 每个来源 IP 的并发连接上限
	MaxFrameBytes    int64 // 单帧上限
	MaxQueueBytes    int   // 每个连接待发送数据占用的内存上限（按缓冲区容量加条目开销计），超过说明对端读得太慢，直接断开

	PingInterval time.Duration // 服务端心跳间隔
	PongTimeout  time.Duration // 等待 pong 的时间，超时视为死连接
	WriteTimeout time.Duration // 单帧写超时
}

// DefaultConfig 返回默认参数。单帧上限与参考实现（Bun 默认值）一致，保证官方客户端行为不受影响；
// 队列按缓冲区容量计费，一个满尺寸帧的底层数组可能比内容大不少，所以队列上限取单帧的两倍。
func DefaultConfig() Config {
	return Config{
		TrustForwardedFor: true,
		MaxRooms:          64,
		MaxGuestsPerRoom:  32,
		MaxConnsPerIP:     32,
		MaxFrameBytes:     16 << 20,
		MaxQueueBytes:     32 << 20,
		PingInterval:      15 * time.Second,
		PongTimeout:       20 * time.Second,
		WriteTimeout:      30 * time.Second,
	}
}

type role uint8

const (
	roleHost role = iota + 1
	roleGuest
)

func (r role) String() string {
	if r == roleHost {
		return "host"
	}
	return "guest"
}

type room struct {
	id         string
	host       *peer
	guests     map[uint32]*peer
	nextPeerID uint32
}

type peer struct {
	conn *websocket.Conn
	role role
	id   uint32 // host 恒为 0，guest 从 1 开始分配
	ip   netip.Addr
	room *room
	out  *sendQueue
}

// Relay 实现 omp collab 的转发协议，是一个 http.Handler。
type Relay struct {
	cfg Config
	log *slog.Logger

	mu        sync.Mutex
	rooms     map[string]*room
	connsByIP map[netip.Addr]int
	closing   bool

	active sync.WaitGroup // 所有已升级的连接
}

func NewRelay(cfg Config, log *slog.Logger) *Relay {
	return &Relay{
		cfg:       cfg,
		log:       log,
		rooms:     make(map[string]*room),
		connsByIP: make(map[netip.Addr]int),
	}
}

func (r *Relay) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if req.URL.Path == "/healthz" {
		r.mu.Lock()
		rooms := len(r.rooms)
		r.mu.Unlock()
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = w.Write([]byte("ok rooms=" + strconv.Itoa(rooms) + "\n"))
		return
	}

	// 用转义后的路径匹配，和参考实现一样不接受百分号编码的房间 ID。
	m := roomPathRE.FindStringSubmatch(req.URL.EscapedPath())
	roles := req.URL.Query()["role"]
	// role 必须恰好出现一次：避免前置代理和 relay 对 ?role=guest&role=host 各取一个造成越权。
	if m == nil || len(roles) != 1 || (roles[0] != "host" && roles[0] != "guest") {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	roomID := m[1]
	rl := roleGuest
	if roles[0] == "host" {
		rl = roleHost
	}
	if !headerHasToken(req.Header, "Upgrade", "websocket") {
		http.Error(w, "websocket upgrade required", http.StatusUpgradeRequired)
		return
	}

	ip := r.clientIP(req)
	if rl == roleHost && !r.hostAllowed(ip) {
		r.log.Warn("拒绝非授权来源开房", "ip", ip, "room", shortID(roomID))
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	if status := r.admit(ip, roomID, rl); status != 0 {
		r.log.Warn("拒绝连接", "ip", ip, "role", rl, "room", shortID(roomID), "status", status)
		http.Error(w, http.StatusText(status), status)
		return
	}
	defer r.release(ip)

	conn, err := websocket.Accept(w, req, &websocket.AcceptOptions{
		// 访客会从 my.omp.sh 等任意网页连进来，不校验 Origin；内容是端到端加密的。
		InsecureSkipVerify: true,
		CompressionMode:    websocket.CompressionDisabled,
	})
	if err != nil {
		return
	}
	conn.SetReadLimit(r.cfg.MaxFrameBytes)

	p := &peer{conn: conn, role: rl, ip: ip, out: newSendQueue()}
	if code, reason := r.join(p, roomID); code != 0 {
		_ = conn.Close(code, reason)
		return
	}
	r.serve(req.Context(), p)
}

// admit 在升级前做容量检查，占用一个连接名额并登记为活动连接；返回非 0 表示拒绝时的 HTTP 状态码。
// 登记和 closing 检查在同一把锁里完成，Shutdown 设置 closing 之后不会再有新的 Add。
func (r *Relay) admit(ip netip.Addr, roomID string, rl role) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closing {
		return http.StatusServiceUnavailable
	}
	if r.connsByIP[ip] >= r.cfg.MaxConnsPerIP {
		return http.StatusTooManyRequests
	}
	if rl == roleHost && r.rooms[roomID] == nil && len(r.rooms) >= r.cfg.MaxRooms {
		return http.StatusServiceUnavailable
	}
	r.connsByIP[ip]++
	r.active.Add(1)
	return 0
}

func (r *Relay) release(ip netip.Addr) {
	r.mu.Lock()
	if r.connsByIP[ip]--; r.connsByIP[ip] <= 0 {
		delete(r.connsByIP, ip)
	}
	r.mu.Unlock()
	r.active.Done()
}

// join 对应参考实现的 open 回调：host 建房，guest 入房并通知 host。
func (r *Relay) join(p *peer, roomID string) (websocket.StatusCode, string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closing {
		return websocket.StatusGoingAway, "relay shutting down"
	}
	rm := r.rooms[roomID]
	if p.role == roleHost {
		if rm != nil {
			return closeHostConflict, "a host is already connected for this room"
		}
		if len(r.rooms) >= r.cfg.MaxRooms {
			return websocket.StatusTryAgainLater, "too many rooms"
		}
		rm = &room{id: roomID, host: p, guests: make(map[uint32]*peer), nextPeerID: 1}
		r.rooms[roomID] = rm
		p.room = rm
		r.log.Info("房间创建", "room", shortID(roomID), "ip", p.ip)
		return 0, ""
	}
	if rm == nil {
		return closeNoSuchRoom, "no such room"
	}
	if len(rm.guests) >= r.cfg.MaxGuestsPerRoom {
		return closeRoomFull, "room is full"
	}
	p.id = rm.nextPeerID
	rm.nextPeerID++
	p.room = rm
	rm.guests[p.id] = p
	// 先于该 guest 的任何数据帧入队，保证 host 先看到 peer-joined。
	r.sendLocked(rm.host, websocket.MessageText, []byte(`{"t":"peer-joined","peer":`+strconv.FormatUint(uint64(p.id), 10)+`}`))
	r.log.Info("访客加入", "room", shortID(roomID), "peer", p.id, "ip", p.ip, "guests", len(rm.guests))
	return 0, ""
}

// serve 跑写协程和心跳，当前协程负责读；读循环结束即视为断开。
func (r *Relay) serve(ctx context.Context, p *peer) {
	ctx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); r.writeLoop(ctx, p) }()
	go func() { defer wg.Done(); r.heartbeat(ctx, p) }()

	r.readLoop(ctx, p)

	r.leave(p)
	p.out.shutdown()
	cancel()
	p.conn.CloseNow()
	wg.Wait()
}

func (r *Relay) readLoop(ctx context.Context, p *peer) {
	for {
		typ, data, err := p.conn.Read(ctx)
		if err != nil {
			return
		}
		// 客户端从不发文本帧；不足 4 字节的二进制帧没有信封头，一并丢弃。
		if typ != websocket.MessageBinary || len(data) < envelopeHeaderLen {
			continue
		}
		r.forward(p, data)
	}
}

func (r *Relay) forward(p *peer, data []byte) {
	r.mu.Lock()
	defer r.mu.Unlock()
	rm := p.room
	if p.role == roleHost {
		if rm.host != p {
			return
		}
		target := binary.BigEndian.Uint32(data[:envelopeHeaderLen])
		if target == 0 {
			for _, g := range rm.guests {
				r.sendLocked(g, websocket.MessageBinary, data)
			}
			return
		}
		if g := rm.guests[target]; g != nil {
			r.sendLocked(g, websocket.MessageBinary, data)
		}
		return
	}
	// 房间已回收或本 guest 已被移出：丢弃。
	if rm.guests[p.id] != p {
		return
	}
	binary.BigEndian.PutUint32(data[:envelopeHeaderLen], p.id)
	r.sendLocked(rm.host, websocket.MessageBinary, data)
}

// leave 对应参考实现的 close 回调。比较的是房间指针而不是房间 ID，
// 避免旧房间的 guest 断开时误删同名新房间里编号相同的 guest。
func (r *Relay) leave(p *peer) {
	r.mu.Lock()
	defer r.mu.Unlock()
	rm := p.room
	if rm == nil {
		return
	}
	if p.role == roleHost {
		if rm.host != p {
			return
		}
		if r.rooms[rm.id] == rm {
			delete(r.rooms, rm.id)
		}
		r.closeGuestsLocked(rm)
		r.log.Info("房间关闭", "room", shortID(rm.id))
		return
	}
	if rm.guests[p.id] != p {
		return
	}
	delete(rm.guests, p.id)
	r.sendLocked(rm.host, websocket.MessageText, []byte(`{"t":"peer-left","peer":`+strconv.FormatUint(uint64(p.id), 10)+`}`))
	r.log.Info("访客离开", "room", shortID(rm.id), "peer", p.id, "guests", len(rm.guests))
}

func (r *Relay) closeGuestsLocked(rm *room) {
	for id, g := range rm.guests {
		r.sendLocked(g, websocket.MessageText, []byte(`{"t":"room-closed"}`))
		g.out.pushClose(closeRoomClosed, "room closed")
		delete(rm.guests, id)
	}
}

// sendLocked 入队一帧；对端积压超过上限时直接断开它，不在内存里无限堆积。
func (r *Relay) sendLocked(p *peer, typ websocket.MessageType, data []byte) {
	if p.out.push(typ, data, r.cfg.MaxQueueBytes) {
		return
	}
	if p.out.markOverflow() {
		r.log.Warn("对端读取过慢，断开", "role", p.role, "peer", p.id, "ip", p.ip, "room", shortID(p.room.id))
		go p.conn.CloseNow()
	}
}

func (r *Relay) writeLoop(ctx context.Context, p *peer) {
	for {
		msg, ok := p.out.pop(ctx)
		if !ok {
			return
		}
		if msg.close {
			_ = p.conn.Close(msg.code, msg.reason)
			return
		}
		wctx, cancel := context.WithTimeout(ctx, r.cfg.WriteTimeout)
		err := writeMessage(wctx, p.conn, msg.typ, msg.data)
		cancel()
		if err != nil {
			p.conn.CloseNow()
			return
		}
	}
}

func (r *Relay) heartbeat(ctx context.Context, p *peer) {
	t := time.NewTicker(r.cfg.PingInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		pctx, cancel := context.WithTimeout(ctx, r.cfg.PongTimeout)
		err := p.conn.Ping(pctx)
		cancel()
		if err != nil {
			if ctx.Err() == nil {
				r.log.Info("心跳超时，断开", "role", p.role, "peer", p.id, "ip", p.ip)
			}
			p.conn.CloseNow()
			return
		}
	}
}

// writeChunk 是大消息分片的大小。
const writeChunk = 32 << 10

// writeMessage 把大消息拆成多个分片帧发送。库的写锁按帧持有，控制帧（ping、自动回复的 pong）
// 只能在帧之间插进来，且最多等 5 秒；整条大消息一次写完的话，慢速下行上发一个快照就可能超过 5 秒，
// 双方的心跳都会失败，把正常的连接当成死连接断开。
func writeMessage(ctx context.Context, conn *websocket.Conn, typ websocket.MessageType, data []byte) error {
	if len(data) <= writeChunk {
		return conn.Write(ctx, typ, data)
	}
	w, err := conn.Writer(ctx, typ)
	if err != nil {
		return err
	}
	for off := 0; off < len(data); off += writeChunk {
		if _, err := w.Write(data[off:min(off+writeChunk, len(data))]); err != nil {
			return err
		}
	}
	return w.Close()
}

// Shutdown 通知所有房间关闭并等待连接退出：guest 收到 room-closed 与 4001 后会重连，host 收到 1001 后会重建房间。
func (r *Relay) Shutdown(ctx context.Context) {
	r.mu.Lock()
	r.closing = true
	for id, rm := range r.rooms {
		r.closeGuestsLocked(rm)
		rm.host.out.pushClose(websocket.StatusGoingAway, "relay shutting down")
		delete(r.rooms, id)
	}
	r.mu.Unlock()

	done := make(chan struct{})
	go func() { r.active.Wait(); close(done) }()
	select {
	case <-done:
	case <-ctx.Done():
	}
}

func (r *Relay) clientIP(req *http.Request) netip.Addr {
	ap, err := netip.ParseAddrPort(req.RemoteAddr)
	if err != nil {
		return netip.Addr{}
	}
	addr := ap.Addr().Unmap()
	if !r.cfg.TrustForwardedFor || !addr.IsLoopback() {
		return addr
	}
	values := req.Header.Values("X-Forwarded-For")
	if len(values) == 0 {
		return addr
	}
	// 只取最后一跳：那是紧挨着 relay 的代理（Caddy）亲眼看到的来源，客户端伪造的值只会排在前面。
	hops := strings.Split(values[len(values)-1], ",")
	if fwd, err := netip.ParseAddr(strings.TrimSpace(hops[len(hops)-1])); err == nil {
		return fwd.Unmap()
	}
	return addr
}

func (r *Relay) hostAllowed(ip netip.Addr) bool {
	if len(r.cfg.HostPrefixes) == 0 {
		return true
	}
	for _, pfx := range r.cfg.HostPrefixes {
		if pfx.Contains(ip) {
			return true
		}
	}
	return false
}

func headerHasToken(h http.Header, name, token string) bool {
	for _, v := range h.Values(name) {
		for _, part := range strings.Split(v, ",") {
			if strings.EqualFold(strings.TrimSpace(part), token) {
				return true
			}
		}
	}
	return false
}

// shortID 日志里只记房间 ID 前缀，便于对照又不完整暴露。
func shortID(id string) string {
	if len(id) > 6 {
		return id[:6]
	}
	return id
}
