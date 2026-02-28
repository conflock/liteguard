package internal

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	_ "modernc.org/sqlite"
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
	walGuard  *sql.DB
	walTx     *sql.Tx
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

	// Mirror the approach used by Litestream:
	// 1. Open connection with wal_autocheckpoint=0
	// 2. Ensure WAL mode
	// 3. Create a lock table if needed
	// 4. Start a long-running read transaction that reads from the table
	//    to acquire a SHARED lock on the database pages, preventing any
	//    other connection from checkpointing the WAL.
	dsn := fmt.Sprintf("file:%s?_pragma=busy_timeout(5000)&_pragma=wal_autocheckpoint(0)", s.cfg.DBPath)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return fmt.Errorf("open WAL guard: %w", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	var mode string
	if err := db.QueryRow("PRAGMA journal_mode=WAL;").Scan(&mode); err != nil {
		db.Close()
		return fmt.Errorf("set WAL mode: %w", err)
	}
	if mode != "wal" {
		db.Close()
		return fmt.Errorf("WAL mode not enabled, got %q", mode)
	}

	if _, err := db.Exec("CREATE TABLE IF NOT EXISTS _liteguard_lock (id INTEGER PRIMARY KEY)"); err != nil {
		db.Close()
		return fmt.Errorf("create lock table: %w", err)
	}
	if _, err := db.Exec("INSERT OR IGNORE INTO _liteguard_lock (id) VALUES (1)"); err != nil {
		db.Close()
		return fmt.Errorf("seed lock table: %w", err)
	}

	ctx := context.Background()
	tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		db.Close()
		return fmt.Errorf("begin WAL guard tx: %w", err)
	}
	if _, err := tx.ExecContext(ctx, "SELECT COUNT(1) FROM _liteguard_lock"); err != nil {
		tx.Rollback()
		db.Close()
		return fmt.Errorf("WAL guard read lock: %w", err)
	}

	s.walGuard = db
	s.walTx = tx
	log.Printf("[sender] WAL guard active (read tx held) on %s", s.cfg.DBPath)

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
	if s.walTx != nil {
		s.walTx.Rollback()
	}
	if s.walGuard != nil {
		s.walGuard.Close()
	}
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
	if currentSize == s.lastSize {
		return
	}

	// Send the complete WAL file so that the receiver always has a
	// consistent copy with correct salt values and frame checksums.
	data, err := os.ReadFile(s.walPath)
	if err != nil {
		log.Printf("[sender] read WAL: %v", err)
		return
	}

	seq := s.sequence.Add(1)
	msg := NewWALFrame(seq, data)

	s.broadcast(msg)
	s.lastSize = currentSize
	log.Printf("[sender] WAL frame seq=%d (%d bytes) sent to replicas", seq, len(data))
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
