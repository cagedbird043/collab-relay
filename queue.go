package main

import (
	"context"
	"sync"

	"github.com/coder/websocket"
)

type outMsg struct {
	typ  websocket.MessageType
	data []byte

	close  bool
	code   websocket.StatusCode
	reason string
}

// sendQueue 是每个连接的有界发送队列：入队不阻塞，按占用内存计量，由单独的写协程按序发出。
type sendQueue struct {
	mu       sync.Mutex
	items    []outMsg
	bytes    int
	sealed   bool // 已排入关闭帧、已溢出或已停止：之后的入队一律丢弃
	overflow bool
	stopped  bool
	wake     chan struct{}
}

func newSendQueue() *sendQueue {
	return &sendQueue{wake: make(chan struct{}, 1)}
}

// queueEntryOverhead 是每条排队消息自身的估算开销（outMsg 结构体与切片头）。
// 计入它，才能防止大量极小帧在字节数远未到上限时就堆出数十万个条目。
const queueEntryOverhead = 128

// push 入队一帧；返回 false 表示超出上限，调用方应断开该连接。
// 按缓冲区容量而不是长度计费：读出来的帧底层数组往往比内容大，排队期间整块都被占着。
func (q *sendQueue) push(typ websocket.MessageType, data []byte, limit int) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.sealed {
		return true
	}
	cost := cap(data) + queueEntryOverhead
	if q.bytes+cost > limit {
		return false
	}
	q.items = append(q.items, outMsg{typ: typ, data: data})
	q.bytes += cost
	q.signal()
	return true
}

// pushClose 在已排队的帧之后发送关闭帧。
func (q *sendQueue) pushClose(code websocket.StatusCode, reason string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.sealed {
		return
	}
	q.items = append(q.items, outMsg{close: true, code: code, reason: reason})
	q.sealed = true
	q.signal()
}

// markOverflow 丢弃积压并封闭队列；只有第一次调用返回 true。
func (q *sendQueue) markOverflow() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.overflow {
		return false
	}
	q.overflow = true
	q.sealed = true
	q.items = nil
	q.bytes = 0
	return true
}

func (q *sendQueue) shutdown() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.stopped = true
	q.sealed = true
	q.items = nil
	q.bytes = 0
	q.signal()
}

// pop 阻塞直到取到一帧；队列停止或 ctx 结束时返回 false。
func (q *sendQueue) pop(ctx context.Context) (outMsg, bool) {
	for {
		q.mu.Lock()
		if len(q.items) > 0 && !q.stopped {
			msg := q.items[0]
			q.items[0] = outMsg{}
			q.items = q.items[1:]
			if !msg.close {
				q.bytes -= cap(msg.data) + queueEntryOverhead
			}
			q.mu.Unlock()
			return msg, true
		}
		stopped := q.stopped
		q.mu.Unlock()
		if stopped {
			return outMsg{}, false
		}
		select {
		case <-q.wake:
		case <-ctx.Done():
			return outMsg{}, false
		}
	}
}

func (q *sendQueue) signal() {
	select {
	case q.wake <- struct{}{}:
	default:
	}
}
