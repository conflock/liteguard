package internal

import (
	"database/sql"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	_ "modernc.org/sqlite"
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
	db             *sql.DB
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

	// Open a persistent SQLite connection for checkpointing received WAL frames.
	dsn := fmt.Sprintf("file:%s?_pragma=busy_timeout(5000)&_pragma=journal_mode(wal)", r.cfg.DBPath)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		ln.Close()
		return fmt.Errorf("open receiver db: %w", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	if err := db.Ping(); err != nil {
		db.Close()
		ln.Close()
		return fmt.Errorf("receiver db ping: %w", err)
	}
	r.db = db

	go r.acceptLoop()

	log.Printf("[receiver] listening on %s, WAL target: %s", r.cfg.ListenAddr, r.walPath)
	return nil
}

func (r *Receiver) Stop() {
	close(r.stop)
	if r.listener != nil {
		r.listener.Close()
	}
	if r.db != nil {
		r.db.Close()
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

// applyWAL overwrites the local WAL file with the full WAL snapshot
// received from the primary, then checkpoints it into the main DB.
// The sender streams the complete WAL content (not deltas) so that
// salt values and frame checksums stay consistent with the database.
func (r *Receiver) applyWAL(msg *Message) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if err := os.WriteFile(r.walPath, msg.Payload, 0644); err != nil {
		return fmt.Errorf("write WAL: %w", err)
	}

	// Checkpoint into main DB so data is immediately readable.
	if _, err := r.db.Exec("PRAGMA wal_checkpoint(PASSIVE)"); err != nil {
		log.Printf("[receiver] checkpoint warning: %v", err)
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
