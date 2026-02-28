package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/conflock/liteguard/internal"
)

var version = "dev"

func main() {
	mode := flag.String("mode", "", "Mode: 'primary' or 'replica'")
	dbPath := flag.String("db", "", "Path to SQLite database file")
	replicas := flag.String("replicas", "", "Comma-separated replica hostnames (primary mode, e.g. replica-a:4200,replica-b:4200)")
	listenAddr := flag.String("listen", ":4200", "Listen address for incoming WAL stream (replica mode)")
	heartbeat := flag.Duration("heartbeat", 2*time.Second, "Heartbeat interval (primary mode)")
	timeout := flag.Duration("timeout", 10*time.Second, "Primary timeout before failover (replica mode)")
	onPromote := flag.String("on-promote", "", "Shell command to run on promotion to primary (replica mode)")
	peers := flag.String("peers", "", "Comma-separated peer replica addresses for leader election (replica mode)")
	showVersion := flag.Bool("version", false, "Show version")
	flag.Parse()

	if *showVersion {
		fmt.Printf("Liteguard %s\n", version)
		os.Exit(0)
	}

	if *mode == "" || *dbPath == "" {
		fmt.Fprintln(os.Stderr, "Usage: liteguard -mode <primary|replica> -db <path>")
		fmt.Fprintln(os.Stderr, "")
		fmt.Fprintln(os.Stderr, "Primary mode:")
		fmt.Fprintln(os.Stderr, "  liteguard -mode primary -db /data/app.db -replicas replica-a:4200,replica-b:4200")
		fmt.Fprintln(os.Stderr, "")
		fmt.Fprintln(os.Stderr, "Replica mode:")
		fmt.Fprintln(os.Stderr, "  liteguard -mode replica -db /data/app.db -listen :4200 -timeout 10s")
		os.Exit(1)
	}

	if _, err := os.Stat(*dbPath); err != nil {
		log.Fatalf("Database not found: %s", *dbPath)
	}

	log.Printf("Liteguard %s starting in %s mode", version, *mode)
	log.Printf("database: %s", *dbPath)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)

	switch *mode {
	case "primary":
		runPrimary(*dbPath, *replicas, *heartbeat, sig)
	case "replica":
		runReplica(*dbPath, *listenAddr, *timeout, *onPromote, *peers, sig)
	default:
		log.Fatalf("Unknown mode: %s (use 'primary' or 'replica')", *mode)
	}
}

// probeForExistingPrimary checks if ANY replica address is reachable.
// A reachable replica means it is listening for a primary connection,
// which means either:
//   - Another primary is already streaming to it, OR
//   - It is waiting for a primary (our old role)
//
// In BOTH cases a server that was offline must NOT start as primary
// because it has stale data. It must demote to replica first and
// sync from the current primary.
func probeForExistingPrimary(addrs []string) string {
	for _, addr := range addrs {
		conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
		if err != nil {
			continue
		}
		conn.Close()
		return addr
	}
	return ""
}

func runPrimary(dbPath, replicaList string, heartbeat time.Duration, sig chan os.Signal) {
	if replicaList == "" {
		log.Fatal("Primary mode requires -replicas flag")
	}

	addrs := strings.Split(replicaList, ",")
	for i := range addrs {
		addrs[i] = strings.TrimSpace(addrs[i])
	}

	// CRITICAL: Before starting as primary, ALWAYS check if any replica
	// is reachable. If a replica is listening, another primary may already
	// be streaming to it, or we were offline and have stale data.
	// In either case we MUST NOT start as primary -- we exit with code 10
	// so the recovery script can demote us to replica.
	// This check runs regardless of whether a .primary marker exists.
	active := probeForExistingPrimary(addrs)
	if active != "" {
		log.Printf("CRITICAL: Replica %s is reachable. Another primary may be active.", active)
		log.Printf("Refusing to start as primary to prevent data loss.")
		log.Printf("Exiting with code 10 so the recovery script can demote to replica.")
		os.Exit(10)
	}
	log.Printf("[primary] no active replicas found, safe to start as primary")

	sender := internal.NewSender(internal.SenderConfig{
		DBPath:            dbPath,
		Replicas:          addrs,
		HeartbeatInterval: heartbeat,
	})

	if err := sender.Start(); err != nil {
		log.Fatalf("Sender start failed: %v", err)
	}

	<-sig
	log.Println("Shutting down...")
	sender.Stop()
}

func runReplica(dbPath, listenAddr string, timeout time.Duration, onPromote string, peerList string, sig chan os.Signal) {
	receiver := internal.NewReceiver(internal.ReceiverConfig{
		ListenAddr: listenAddr,
		DBPath:     dbPath,
	})

	if err := receiver.Start(); err != nil {
		log.Fatalf("Receiver start failed: %v", err)
	}

	var peerAddrs []string
	if peerList != "" {
		for _, p := range strings.Split(peerList, ",") {
			p = strings.TrimSpace(p)
			if p != "" {
				peerAddrs = append(peerAddrs, p)
			}
		}
	}

	failover := internal.NewFailover(internal.FailoverConfig{
		Timeout:   timeout,
		DBPath:    dbPath,
		OnPromote: onPromote,
		PeerAddrs: peerAddrs,
	}, receiver)
	failover.Start()

	<-sig
	log.Println("Shutting down...")
	failover.Stop()
	receiver.Stop()
}
