package internal

import (
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

type SenderConfig struct {
	DBPath            string
	Replicas          []string
	HeartbeatInterval time.Duration
	WALPollInterval   time.Duration
}

type Sender struct {
	cfg       SenderConfig
	walPath   string
	sequence  atomic.Uint64
	replicas  []*replicaConn
	mu        sync.RWMutex
	stop      chan struct{}
	lastSize  int64
}

type replicaConn struct {
	addr     string
	conn     net.Conn
	mu       sync.Mutex
	lastAck  atomic.Uint64
	healthy  atomic.Bool
}

func NewSender(cfg SenderConfig) *Sender {
	if cfg.HeartbeatInterval == 0 {
		cfg.HeartbeatInterval = 2 * time.Second
	}
	if cfg.WALPollInterval == 0 {
		cfg.WALPollInterval = 100 * time.Millisecond
	}

	s := &Sender{
		cfg:     cfg,
		walPath: cfg.DBPath + "-wal",
		stop:    make(chan struct{}),
	}

	for _, addr := range cfg.Replicas {
		s.replicas = append(s.replicas, &replicaConn{addr: addr})
	}

	return s
}

func (s *Sender) Start() error {
	walDir := filepath.Dir(s.walPath)
	if _, err := os.Stat(walDir); err != nil {
		return fmt.Errorf("WAL directory not accessible: %w", err)
	}

	for _, r := range s.replicas {
		go s.connectLoop(r)
	}

	go s.walPollLoop()
	go s.heartbeatLoop()

	log.Printf("[sender] started – watching %s, streaming to %d replica(s)", s.walPath, len(s.replicas))
	return nil
}

func (s *Sender) Stop() {
	close(s.stop)
	for _, r := range s.replicas {
		r.mu.Lock()
		if r.conn != nil {
			r.conn.Close()
		}
		r.mu.Unlock()
	}
	log.Println("[sender] stopped")
}

func (s *Sender) connectLoop(r *replicaConn) {
	for {
		select {
		case <-s.stop:
			return
		default:
		}

		conn, err := net.DialTimeout("tcp", r.addr, 5*time.Second)
		if err != nil {
			log.Printf("[sender] connect to %s failed: %v", r.addr, err)
			r.healthy.Store(false)
			time.Sleep(3 * time.Second)
			continue
		}

		r.mu.Lock()
		r.conn = conn
		r.mu.Unlock()
		r.healthy.Store(true)
		log.Printf("[sender] connected to replica %s", r.addr)

		s.listenAcks(r)

		r.healthy.Store(false)
		log.Printf("[sender] disconnected from %s, reconnecting...", r.addr)
	}
}

// listenAcks reads ACK messages from replica until error.
func (s *Sender) listenAcks(r *replicaConn) {
	for {
		select {
		case <-s.stop:
			return
		default:
		}

		r.mu.Lock()
		c := r.conn
		r.mu.Unlock()
		if c == nil {
			return
		}

		c.SetReadDeadline(time.Now().Add(10 * time.Second))
		msg, err := Decode(c)
		if err != nil {
			return
		}
		if msg.Type == MsgTypeAck {
			r.lastAck.Store(msg.Sequence)
		}
	}
}

func (s *Sender) walPollLoop() {
	for {
		select {
		case <-s.stop:
			return
		case <-time.After(s.cfg.WALPollInterval):
			s.checkWAL()
		}
	}
}

func (s *Sender) checkWAL() {
	info, err := os.Stat(s.walPath)
	if err != nil {
		return
	}

	currentSize := info.Size()
	if currentSize <= s.lastSize {
		return
	}

	f, err := os.Open(s.walPath)
	if err != nil {
		log.Printf("[sender] open WAL: %v", err)
		return
	}
	defer f.Close()

	// Seek to where we left off
	if s.lastSize > 0 {
		if _, err := f.Seek(s.lastSize, io.SeekStart); err != nil {
			log.Printf("[sender] seek WAL: %v", err)
			return
		}
	}

	newData := make([]byte, currentSize-s.lastSize)
	n, err := io.ReadFull(f, newData)
	if err != nil {
		log.Printf("[sender] read WAL: %v", err)
		return
	}

	seq := s.sequence.Add(1)
	msg := NewWALFrame(seq, newData[:n])

	s.broadcast(msg)
	s.lastSize = currentSize
}

func (s *Sender) broadcast(msg *Message) {
	for _, r := range s.replicas {
		if !r.healthy.Load() {
			continue
		}
		go func(r *replicaConn) {
			r.mu.Lock()
			defer r.mu.Unlock()
			if r.conn == nil {
				return
			}
			r.conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
			if err := msg.Encode(r.conn); err != nil {
				log.Printf("[sender] send to %s failed: %v", r.addr, err)
				r.conn.Close()
				r.conn = nil
				r.healthy.Store(false)
			}
		}(r)
	}
}

func (s *Sender) heartbeatLoop() {
	ticker := time.NewTicker(s.cfg.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.stop:
			return
		case <-ticker.C:
			seq := s.sequence.Load()
			msg := NewHeartbeat(seq)
			s.broadcast(msg)
		}
	}
}

func (s *Sender) HealthyReplicaCount() int {
	count := 0
	for _, r := range s.replicas {
		if r.healthy.Load() {
			count++
		}
	}
	return count
}
