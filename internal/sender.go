package internal

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"fmt"
	"io"
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
	HealthAddr        string // TCP address for health check (e.g. ":4201")
}

type Sender struct {
	cfg            SenderConfig
	walPath        string
	sequence       atomic.Uint64
	replicas       []*replicaConn
	mu             sync.RWMutex
	stop           chan struct{}
	lastSize       int64
	lastModTime    time.Time
	walGuard       *sql.DB
	walTx          *sql.Tx
	healthListener net.Listener
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

	if s.cfg.HealthAddr != "" {
		hl, err := net.Listen("tcp", s.cfg.HealthAddr)
		if err != nil {
			log.Printf("[sender] WARNING: health listener on %s failed: %v", s.cfg.HealthAddr, err)
		} else {
			s.healthListener = hl
			go s.healthLoop()
			log.Printf("[sender] health listener active on %s", s.cfg.HealthAddr)
		}
	}

	log.Printf("[sender] started – watching %s, streaming to %d replica(s)", s.walPath, len(s.replicas))
	return nil
}

func (s *Sender) Stop() {
	close(s.stop)
	if s.healthListener != nil {
		s.healthListener.Close()
	}
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

// healthLoop accepts TCP connections on the health port and immediately
// closes them. The sole purpose is to let recovery scripts detect a
// running primary via "nc -z <addr> 4201".
func (s *Sender) healthLoop() {
	for {
		conn, err := s.healthListener.Accept()
		if err != nil {
			select {
			case <-s.stop:
				return
			default:
				continue
			}
		}
		conn.Close()
	}
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

		log.Printf("[sender] connected to replica %s, waiting for sync request...", r.addr)

		if err := s.handleInitialSync(conn, r); err != nil {
			log.Printf("[sender] initial sync with %s failed: %v", r.addr, err)
			conn.Close()
			r.healthy.Store(false)
			time.Sleep(3 * time.Second)
			continue
		}

		r.mu.Lock()
		r.conn = conn
		r.mu.Unlock()
		r.healthy.Store(true)
		log.Printf("[sender] replica %s synced and ready for WAL stream", r.addr)

		s.listenAcks(r)

		r.healthy.Store(false)
		log.Printf("[sender] disconnected from %s, reconnecting...", r.addr)
	}
}

// handleInitialSync waits for a SyncRequest from the replica, compares DB
// hashes, and sends a full snapshot if needed. Returns nil when the replica
// is ready for normal WAL streaming.
func (s *Sender) handleInitialSync(conn net.Conn, r *replicaConn) error {
	conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	msg, err := Decode(conn)
	if err != nil {
		if err == io.EOF {
			// Old replica without sync support -- skip handshake
			log.Printf("[sender] replica %s uses legacy protocol (no sync request), proceeding", r.addr)
			return nil
		}
		return fmt.Errorf("read sync request: %w", err)
	}

	if msg.Type != MsgTypeSyncRequest {
		// Old replica sending ACKs or other messages -- skip handshake
		log.Printf("[sender] replica %s sent type=%d instead of SyncRequest, proceeding", r.addr, msg.Type)
		return nil
	}

	replicaHash := msg.Payload
	log.Printf("[sender] sync request from %s (hash=%x)", r.addr, replicaHash)

	localHash, err := s.computeDBHash()
	if err != nil {
		return fmt.Errorf("compute local DB hash: %w", err)
	}

	if bytes.Equal(localHash, replicaHash) {
		log.Printf("[sender] replica %s is in sync, no snapshot needed", r.addr)
		resp := NewSyncResponse(0, nil)
		conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
		if err := resp.Encode(conn); err != nil {
			return fmt.Errorf("send empty sync response: %w", err)
		}
	} else {
		log.Printf("[sender] replica %s is out of sync (local=%x), sending snapshot...", r.addr, localHash)
		snapshot, err := s.createSnapshot()
		if err != nil {
			return fmt.Errorf("create snapshot: %w", err)
		}
		resp := NewSyncResponse(0, snapshot)
		conn.SetWriteDeadline(time.Now().Add(60 * time.Second))
		if err := resp.Encode(conn); err != nil {
			return fmt.Errorf("send snapshot (%d bytes): %w", len(snapshot), err)
		}
		log.Printf("[sender] snapshot sent to %s (%d bytes)", r.addr, len(snapshot))
	}

	conn.SetReadDeadline(time.Now().Add(30 * time.Second))
	ack, err := Decode(conn)
	if err != nil {
		return fmt.Errorf("read sync ack: %w", err)
	}
	if ack.Type != MsgTypeSyncAck {
		return fmt.Errorf("expected SyncAck, got type=%d", ack.Type)
	}

	log.Printf("[sender] sync ack received from %s", r.addr)
	return nil
}

// computeDBHash calculates SHA256 of the main database file.
// Temporarily releases the WAL guard to checkpoint pending WAL frames
// into the main DB so the hash reflects the current state.
func (s *Sender) computeDBHash() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Temporarily release WAL guard so checkpoint can proceed
	if s.walTx != nil {
		s.walTx.Rollback()
		s.walTx = nil
	}

	// Checkpoint WAL into main DB
	ckDSN := fmt.Sprintf("file:%s?_pragma=busy_timeout(5000)&_pragma=journal_mode(wal)", s.cfg.DBPath)
	ckDB, err := sql.Open("sqlite", ckDSN)
	if err == nil {
		ckDB.SetMaxOpenConns(1)
		ckDB.Exec("PRAGMA wal_checkpoint(PASSIVE)")
		ckDB.Close()
	}

	// Hash the main DB file
	f, err := os.Open(s.cfg.DBPath)
	if err != nil {
		s.reacquireWALGuard()
		return nil, fmt.Errorf("open DB for hashing: %w", err)
	}
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		f.Close()
		s.reacquireWALGuard()
		return nil, fmt.Errorf("hash DB: %w", err)
	}
	f.Close()

	// Re-acquire WAL guard
	s.reacquireWALGuard()

	return h.Sum(nil), nil
}

// createSnapshot checkpoints and reads the full DB file for transfer.
func (s *Sender) createSnapshot() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.walTx != nil {
		s.walTx.Rollback()
		s.walTx = nil
	}

	ckDSN := fmt.Sprintf("file:%s?_pragma=busy_timeout(5000)&_pragma=journal_mode(wal)", s.cfg.DBPath)
	ckDB, err := sql.Open("sqlite", ckDSN)
	if err == nil {
		ckDB.SetMaxOpenConns(1)
		ckDB.Exec("PRAGMA wal_checkpoint(PASSIVE)")
		ckDB.Close()
	}

	data, err := os.ReadFile(s.cfg.DBPath)
	if err != nil {
		s.reacquireWALGuard()
		return nil, fmt.Errorf("read DB: %w", err)
	}

	s.reacquireWALGuard()
	return data, nil
}

// reacquireWALGuard re-establishes the read transaction that prevents
// external WAL checkpointing.
func (s *Sender) reacquireWALGuard() {
	if s.walGuard == nil {
		return
	}
	ctx := context.Background()
	tx, err := s.walGuard.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		log.Printf("[sender] WARNING: failed to re-acquire WAL guard: %v", err)
		return
	}
	if _, err := tx.ExecContext(ctx, "SELECT COUNT(1) FROM _liteguard_lock"); err != nil {
		tx.Rollback()
		log.Printf("[sender] WARNING: WAL guard read lock failed: %v", err)
		return
	}
	s.walTx = tx
	log.Printf("[sender] WAL guard re-acquired")
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
	modTime := info.ModTime()

	if currentSize == s.lastSize && modTime.Equal(s.lastModTime) {
		return
	}

	if currentSize == 0 {
		s.lastSize = 0
		s.lastModTime = modTime
		return
	}

	data, err := os.ReadFile(s.walPath)
	if err != nil {
		log.Printf("[sender] read WAL: %v", err)
		return
	}

	if len(data) == 0 {
		s.lastSize = 0
		s.lastModTime = modTime
		return
	}

	seq := s.sequence.Add(1)
	msg := NewWALFrame(seq, data)

	s.broadcast(msg)
	s.lastSize = currentSize
	s.lastModTime = modTime
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
