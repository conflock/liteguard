package internal

import (
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type FailoverConfig struct {
	Timeout       time.Duration
	CheckInterval time.Duration
	DBPath        string
	OnPromote     string   // optional shell command to run on promotion
	OwnAddr       string   // this replica's listen address (for election ranking)
	PeerAddrs     []string // addresses of other replicas for leader election
}

type Role int32

const (
	RoleReplica Role = 0
	RolePrimary Role = 1
)

func (r Role) String() string {
	if r == RolePrimary {
		return "primary"
	}
	return "replica"
}

type Failover struct {
	cfg      FailoverConfig
	receiver *Receiver
	role     atomic.Int32
	promoted atomic.Bool
	stop     chan struct{}
	mu       sync.Mutex
}

func NewFailover(cfg FailoverConfig, receiver *Receiver) *Failover {
	if cfg.Timeout == 0 {
		cfg.Timeout = 10 * time.Second
	}
	if cfg.CheckInterval == 0 {
		cfg.CheckInterval = 2 * time.Second
	}

	f := &Failover{
		cfg:      cfg,
		receiver: receiver,
		stop:     make(chan struct{}),
	}
	f.role.Store(int32(RoleReplica))
	return f
}

func (f *Failover) Start() {
	go f.monitorLoop()
	log.Printf("[failover] monitoring primary – timeout: %v, own=%s, peers=%v",
		f.cfg.Timeout, f.cfg.OwnAddr, f.cfg.PeerAddrs)
}

func (f *Failover) Stop() {
	close(f.stop)
	log.Println("[failover] stopped")
}

func (f *Failover) CurrentRole() Role {
	return Role(f.role.Load())
}

func (f *Failover) monitorLoop() {
	ticker := time.NewTicker(f.cfg.CheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-f.stop:
			return
		case <-ticker.C:
			f.check()
		}
	}
}

func (f *Failover) check() {
	if f.promoted.Load() {
		return
	}

	gap := f.receiver.SecondsSinceHeartbeat()
	if gap > f.cfg.Timeout.Seconds() {
		f.tryPromote()
	}
}

// tryPromote implements deterministic leader election.
// The replica with the lowest address among all reachable replicas
// (including itself) wins. No randomness, no race conditions.
func (f *Failover) tryPromote() {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.promoted.Load() {
		return
	}

	// Collect all reachable replicas (peers that are still listening)
	reachable := []string{}
	if f.cfg.OwnAddr != "" {
		reachable = append(reachable, f.cfg.OwnAddr)
	}
	for _, addr := range f.cfg.PeerAddrs {
		if addr == "" {
			continue
		}
		conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
		if err != nil {
			// Peer not reachable (offline or already promoted) -- skip
			log.Printf("[failover] peer %s not reachable, skipping", addr)
			continue
		}
		conn.Close()
		reachable = append(reachable, addr)
	}

	if len(reachable) == 0 {
		log.Printf("[failover] no peers reachable and no own addr, promoting")
		f.doPromote()
		return
	}

	sort.Strings(reachable)
	winner := reachable[0]

	if winner == f.cfg.OwnAddr {
		log.Printf("[failover] I have the lowest address (%s) among %v – promoting", winner, reachable)
		f.doPromote()
	} else {
		log.Printf("[failover] peer %s has lower address (mine=%s) – waiting", winner, f.cfg.OwnAddr)
	}
}

func (f *Failover) doPromote() {
	f.receiver.Stop()

	f.role.Store(int32(RolePrimary))
	f.promoted.Store(true)

	markerPath := f.cfg.DBPath + ".primary"
	if err := os.WriteFile(markerPath, []byte(fmt.Sprintf("promoted_at=%d\n", time.Now().Unix())), 0644); err != nil {
		log.Printf("[failover] write marker file: %v", err)
	}

	log.Println("[failover] promoted to PRIMARY")

	if f.cfg.OnPromote != "" {
		go f.runPromoteHook()
	}
}

func (f *Failover) runPromoteHook() {
	log.Printf("[failover] running promote hook: %s", f.cfg.OnPromote)
	cmd := exec.Command("sh", "-c", f.cfg.OnPromote)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		log.Printf("[failover] promote hook error: %v", err)
	}
}
