package internal

import (
	"crypto/sha256"
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
	stopped        atomic.Bool
	mu             sync.Mutex
}

func NewReceiver(cfg ReceiverConfig) *Receiver {
	r := &Receiver{
		cfg:     cfg,
		walPath: cfg.DBPath + "-wal",
		stop:    make(chan struct{}),
	}
	// Seed heartbeat so failover can trigger even if no primary ever connects
	r.lastHeartbeat.Store(time.Now().UnixMilli())
	return r
}

func (r *Receiver) Start() error {
	r.stop = make(chan struct{})
	r.stopped.Store(false)

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
	if r.stopped.Swap(true) {
		return // already stopped
	}
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

	if err := r.performInitialSync(conn); err != nil {
		log.Printf("[receiver] initial sync failed: %v", err)
		return
	}

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
			log.Printf("[receiver] applied WAL seq=%d (%d bytes)", msg.Sequence, len(msg.Payload))
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

			ack := NewAck(msg.Sequence)
			conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
			if err := ack.Encode(conn); err != nil {
				log.Printf("[receiver] send heartbeat ACK failed: %v", err)
				return
			}
		}
	}
}

// performInitialSync sends a SyncRequest with the local DB hash and
// applies the snapshot if the primary determines we are out of sync.
func (r *Receiver) performInitialSync(conn net.Conn) error {
	hash, err := r.computeLocalDBHash()
	if err != nil {
		log.Printf("[receiver] could not hash local DB, requesting full sync: %v", err)
		hash = make([]byte, 32) // all-zero hash forces full sync
	}

	log.Printf("[receiver] sending sync request (local hash=%x)", hash)
	req := NewSyncRequest(0, hash)
	conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	if err := req.Encode(conn); err != nil {
		return fmt.Errorf("send sync request: %w", err)
	}

	// Wait for sync response (may be large -- up to 512 MB)
	conn.SetReadDeadline(time.Now().Add(120 * time.Second))
	resp, err := Decode(conn)
	if err != nil {
		return fmt.Errorf("read sync response: %w", err)
	}
	if resp.Type != MsgTypeSyncResponse {
		return fmt.Errorf("expected SyncResponse, got type=%d", resp.Type)
	}

	if resp.PayloadLen > 0 && len(resp.Payload) > 0 {
		log.Printf("[receiver] received snapshot (%d bytes), applying...", len(resp.Payload))
		if err := r.applySnapshot(resp.Payload); err != nil {
			return fmt.Errorf("apply snapshot: %w", err)
		}
		log.Printf("[receiver] snapshot applied successfully")
	} else {
		log.Printf("[receiver] already in sync, no snapshot needed")
	}

	ack := NewSyncAck(0)
	conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	if err := ack.Encode(conn); err != nil {
		return fmt.Errorf("send sync ack: %w", err)
	}

	return nil
}

// computeLocalDBHash calculates SHA256 of the local database file.
func (r *Receiver) computeLocalDBHash() ([]byte, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Checkpoint any pending WAL into the main DB first
	dsn := fmt.Sprintf("file:%s?_pragma=busy_timeout(5000)&_pragma=journal_mode(wal)", r.cfg.DBPath)
	ckDB, err := sql.Open("sqlite", dsn)
	if err == nil {
		ckDB.SetMaxOpenConns(1)
		ckDB.Exec("PRAGMA wal_checkpoint(PASSIVE)")
		ckDB.Close()
	}

	f, err := os.Open(r.cfg.DBPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return nil, err
	}
	return h.Sum(nil), nil
}

// applySnapshot replaces the local DB with the snapshot from the primary.
func (r *Receiver) applySnapshot(data []byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Remove WAL and SHM files so SQLite starts clean
	os.Remove(r.walPath)
	os.Remove(r.cfg.DBPath + "-shm")

	if err := os.WriteFile(r.cfg.DBPath, data, 0644); err != nil {
		return fmt.Errorf("write snapshot to %s: %w", r.cfg.DBPath, err)
	}

	log.Printf("[receiver] snapshot written: %d bytes to %s", len(data), r.cfg.DBPath)
	return nil
}

// applyWAL overwrites the local WAL file with the full WAL snapshot
// received from the primary, then checkpoints it into the main DB.
// The sender streams the complete WAL content (not deltas) so that
// salt values and frame checksums stay consistent with the database.
//
// We open a fresh SQLite connection for each checkpoint because the
// persistent r.db connection caches the WAL index in shared memory;
// after we overwrite the WAL file externally that cached index is
// stale and PASSIVE checkpoint would silently skip all frames.
func (r *Receiver) applyWAL(msg *Message) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if err := os.WriteFile(r.walPath, msg.Payload, 0644); err != nil {
		return fmt.Errorf("write WAL: %w", err)
	}

	dsn := fmt.Sprintf("file:%s?_pragma=busy_timeout(5000)&_pragma=journal_mode(wal)", r.cfg.DBPath)
	ckDB, err := sql.Open("sqlite", dsn)
	if err != nil {
		return fmt.Errorf("open checkpoint db: %w", err)
	}
	defer ckDB.Close()

	ckDB.SetMaxOpenConns(1)
	if _, err := ckDB.Exec("PRAGMA wal_checkpoint(PASSIVE)"); err != nil {
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
