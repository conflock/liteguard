package internal

import (
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type ReceiverConfig struct {
	ListenAddr string
	DBPath     string
}

type Receiver struct {
	cfg            ReceiverConfig
	walPath        string
	listener       net.Listener
	lastSequence   atomic.Uint64
	lastHeartbeat  atomic.Int64
	stop           chan struct{}
	mu             sync.Mutex
}

func NewReceiver(cfg ReceiverConfig) *Receiver {
	return &Receiver{
		cfg:     cfg,
		walPath: cfg.DBPath + "-wal",
		stop:    make(chan struct{}),
	}
}

func (r *Receiver) Start() error {
	ln, err := net.Listen("tcp", r.cfg.ListenAddr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", r.cfg.ListenAddr, err)
	}
	r.listener = ln

	go r.acceptLoop()

	log.Printf("[receiver] listening on %s, WAL target: %s", r.cfg.ListenAddr, r.walPath)
	return nil
}

func (r *Receiver) Stop() {
	close(r.stop)
	if r.listener != nil {
		r.listener.Close()
	}
	log.Println("[receiver] stopped")
}

func (r *Receiver) acceptLoop() {
	for {
		conn, err := r.listener.Accept()
		if err != nil {
			select {
			case <-r.stop:
				return
			default:
				log.Printf("[receiver] accept error: %v", err)
				time.Sleep(time.Second)
				continue
			}
		}
		log.Printf("[receiver] primary connected from %s", conn.RemoteAddr())
		go r.handleConnection(conn)
	}
}

func (r *Receiver) handleConnection(conn net.Conn) {
	defer conn.Close()

	for {
		select {
		case <-r.stop:
			return
		default:
		}

		conn.SetReadDeadline(time.Now().Add(10 * time.Second))
		msg, err := Decode(conn)
		if err != nil {
			if err != io.EOF {
				log.Printf("[receiver] decode error: %v", err)
			}
			return
		}

		switch msg.Type {
		case MsgTypeWALFrame:
			if err := r.applyWAL(msg); err != nil {
				log.Printf("[receiver] apply WAL seq=%d failed: %v", msg.Sequence, err)
				continue
			}
			r.lastSequence.Store(msg.Sequence)
			r.lastHeartbeat.Store(time.Now().UnixMilli())

			ack := NewAck(msg.Sequence)
			conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
			if err := ack.Encode(conn); err != nil {
				log.Printf("[receiver] send ACK failed: %v", err)
				return
			}

		case MsgTypeHeartbeat:
			r.lastHeartbeat.Store(time.Now().UnixMilli())
			r.lastSequence.Store(msg.Sequence)
		}
	}
}

// applyWAL appends WAL frame data to the local WAL file.
func (r *Receiver) applyWAL(msg *Message) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	f, err := os.OpenFile(r.walPath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("open WAL: %w", err)
	}
	defer f.Close()

	if _, err := f.Write(msg.Payload); err != nil {
		return fmt.Errorf("write WAL: %w", err)
	}

	if err := f.Sync(); err != nil {
		return fmt.Errorf("sync WAL: %w", err)
	}

	return nil
}

func (r *Receiver) LastSequence() uint64 {
	return r.lastSequence.Load()
}

// SecondsSinceHeartbeat returns how many seconds have passed since last primary contact.
func (r *Receiver) SecondsSinceHeartbeat() float64 {
	last := r.lastHeartbeat.Load()
	if last == 0 {
		return -1
	}
	return float64(time.Now().UnixMilli()-last) / 1000.0
}
