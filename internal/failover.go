package internal

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"sync"
	"sync/atomic"
	"time"
)

type FailoverConfig struct {
	Timeout       time.Duration
	CheckInterval time.Duration
	DBPath        string
	OnPromote     string // optional shell command to run on promotion
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
	log.Printf("[failover] monitoring primary – timeout: %v", f.cfg.Timeout)
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

	// No heartbeat received yet – primary might not have connected
	if gap < 0 {
		return
	}

	if gap > f.cfg.Timeout.Seconds() {
		log.Printf("[failover] primary timeout (%.1fs > %.1fs) – promoting to primary", gap, f.cfg.Timeout.Seconds())
		f.promote()
	}
}

func (f *Failover) promote() {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.promoted.Load() {
		return
	}

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
