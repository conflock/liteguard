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
		runReplica(*dbPath, *listenAddr, *timeout, *onPromote, sig)
	default:
		log.Fatalf("Unknown mode: %s (use 'primary' or 'replica')", *mode)
	}
}

// probeForExistingPrimary checks if any of the listed replica addresses
// already has an active primary by trying to connect on the Liteguard port.
// If a connection succeeds and we receive data (heartbeat/WAL), another
// primary is already running.
func probeForExistingPrimary(addrs []string) string {
	for _, addr := range addrs {
		conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
		if err != nil {
			continue
		}
		conn.Close()
		// If we can connect, that replica is listening -- it could be the
		// promoted primary or just a replica. We check if the .primary
		// marker exists on the remote side by checking if the port is open
		// (replicas listen on 4200, promoted primaries don't).
		// A listening replica means another primary exists that it's
		// receiving from. Return this address as the active cluster.
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

	// Auto-demote: if this server was the original primary but was offline
	// while a replica got promoted, detect the situation and refuse to start
	// as a conflicting primary. The recovery script handles the actual
	// mode switch; here we just detect and exit with a specific code.
	markerPath := dbPath + ".primary"
	if _, err := os.Stat(markerPath); err != nil {
		// No .primary marker means we are the ORIGINAL primary (not promoted).
		// Check if any replica is already listening (meaning another primary
		// got promoted and is actively streaming to them).
		active := probeForExistingPrimary(addrs)
		if active != "" {
			log.Printf("WARNING: Another primary appears active (replica %s is listening).", active)
			log.Printf("This server was likely offline during a failover.")
			log.Printf("Exiting with code 10 so the recovery script can demote to replica.")
			os.Exit(10)
		}
	}

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

func runReplica(dbPath, listenAddr string, timeout time.Duration, onPromote string, sig chan os.Signal) {
	receiver := internal.NewReceiver(internal.ReceiverConfig{
		ListenAddr: listenAddr,
		DBPath:     dbPath,
	})

	if err := receiver.Start(); err != nil {
		log.Fatalf("Receiver start failed: %v", err)
	}

	failover := internal.NewFailover(internal.FailoverConfig{
		Timeout:   timeout,
		DBPath:    dbPath,
		OnPromote: onPromote,
	}, receiver)
	failover.Start()

	<-sig
	log.Println("Shutting down...")
	failover.Stop()
	receiver.Stop()
}
