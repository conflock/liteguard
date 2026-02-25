# Liteguard

**SQLite WAL Streaming & Automatic Failover for [Uncloud](https://uncloud.run)**

Liteguard is a lightweight sidecar that keeps SQLite replicas in sync by streaming WAL (Write-Ahead Log) changes over TCP. When the primary goes down, a replica automatically promotes itself and takes over — no manual intervention, no shared storage, no external coordination service.

---

## Background & Motivation

SQLite is the ideal database for many self-hosted applications: zero configuration, single-file storage, and rock-solid reliability. But in a distributed environment like Uncloud, where services can run across multiple machines, a single SQLite file becomes a single point of failure.

Existing solutions each have trade-offs:

| Solution | Approach | Limitation |
|---|---|---|
| **[Litestream](https://litestream.io)** | Streams WAL to S3-compatible storage | Requires external object storage; no automatic failover |
| **[LiteFS](https://fly.io/docs/litefs/)** | FUSE-based filesystem replication | Tied to Fly.io infrastructure; FUSE dependency |
| **PostgreSQL + Patroni** | Full RDBMS with HA | Heavy; requires etcd; not SQLite-compatible |

**Liteguard** takes direct inspiration from Litestream's WAL-based approach but replaces the S3 target with direct TCP streaming to replica nodes. This keeps everything self-hosted — no cloud storage dependency — and adds automatic failover that Litestream intentionally leaves out.

### Design Principles

- **No external dependencies** — no S3, no etcd, no FUSE. Just TCP between nodes.
- **SQLite stays SQLite** — your application reads/writes a normal SQLite file. Liteguard runs alongside as a sidecar and only touches the WAL file.
- **Passive replicas** — replicas receive WAL frames but are not queried live. They are cold standbys for failover only.
- **Automatic promotion** — if the primary disappears (heartbeat timeout), a replica promotes itself and signals the application via a configurable hook.
- **Tiny footprint** — single static Go binary, ~3.5 MB, no runtime dependencies.

---

## Architecture

```
           WireGuard encrypted tunnel (Uncloud network)
  ┌─────────────────────────────────────────────────────────┐
  │                                                         │
  │        TCP :4200                    TCP :4200            │
  │  [Primary Node] ──────────────> [Replica A] ──────> ... │
  │       |                              |                  │
  │   App writes to                 WAL frames are          │
  │   SQLite normally               appended locally        │
  │       |                              |                  │
  │   Liteguard polls               On primary timeout:     │
  │   the WAL file and              replica promotes to     │
  │   streams new frames            primary, runs hook      │
  │                                                         │
  └─────────────────────────────────────────────────────────┘
```

All traffic between nodes travels through Uncloud's WireGuard mesh network, so the WAL stream is encrypted in transit without any additional configuration.

### How it works

1. **Primary mode**: Liteguard watches the SQLite WAL file for new data (polling at 100ms intervals). When new WAL frames appear, they are packaged into binary messages and sent to all configured replicas over persistent TCP connections. Heartbeats are sent every 2 seconds to signal liveness.

2. **Replica mode**: Liteguard listens for incoming TCP connections from the primary. Received WAL frames are appended to the local WAL file and acknowledged. If no heartbeat arrives within the configured timeout (default: 10 seconds), the replica assumes the primary is down and promotes itself.

3. **Failover**: On promotion, the replica stops listening, writes a `.primary` marker file, and optionally executes a shell command (`-on-promote`). This hook can be used to update DNS, restart the application pointing to the local database, or notify a load balancer.

### Wire Protocol

All messages use a simple binary format (21-byte header + variable payload):

| Field | Size | Description |
|---|---|---|
| Type | 1 byte | `0x01` = WAL Frame, `0x02` = Heartbeat, `0x03` = ACK |
| Sequence | 8 bytes | Monotonically increasing counter |
| Timestamp | 8 bytes | Unix milliseconds |
| Payload Length | 4 bytes | Length of following payload (0 for heartbeat/ACK) |
| Payload | variable | Raw WAL frame data |

---

## Installation

### From source

```bash
git clone https://github.com/conflock/liteguard.git
cd liteguard
go build -o liteguard ./cmd/liteguard
```

### Docker

```bash
docker build -t liteguard .
```

The resulting image is based on Alpine and approximately 8 MB.

---

## Usage

### Primary Node

The primary watches a SQLite database and streams WAL changes to one or more replicas:

```bash
liteguard \
  -mode primary \
  -db /data/myapp.db \
  -replicas replica-a:4200,replica-b:4200 \
  -heartbeat 2s
```

### Replica Node

The replica listens for incoming WAL streams and monitors primary health:

```bash
liteguard \
  -mode replica \
  -db /data/myapp.db \
  -listen :4200 \
  -timeout 10s \
  -on-promote "systemctl restart myapp"
```

### All Flags

| Flag | Default | Description |
|---|---|---|
| `-mode` | *(required)* | `primary` or `replica` |
| `-db` | *(required)* | Path to the SQLite database file |
| `-replicas` | | Comma-separated replica hostnames or addresses (primary mode) |
| `-listen` | `:4200` | Listen address for WAL stream (replica mode) |
| `-heartbeat` | `2s` | Heartbeat interval (primary mode) |
| `-timeout` | `10s` | Seconds without heartbeat before failover (replica mode) |
| `-on-promote` | | Shell command executed on promotion (replica mode) |
| `-version` | | Print version and exit |

---

## Deployment with Uncloud

Liteguard is designed to run as a sidecar alongside any SQLite-based service in Uncloud. A typical setup:

```yaml
# docker-compose.yml (Uncloud service)
services:
  myapp:
    image: myapp:latest
    volumes:
      - db-data:/data

  liteguard:
    image: liteguard:latest
    command: >
      -mode primary
      -db /data/myapp.db
      -replicas replica-host:4200
    volumes:
      - db-data:/data

volumes:
  db-data:
```

On each replica node, Liteguard runs in replica mode pointing to the same database path. The application on replica nodes is either idle or read-only until a promotion event occurs.

---

## Failover Behavior

When a replica detects that the primary has not sent a heartbeat within the `-timeout` window:

1. The TCP listener is closed (no more incoming connections accepted)
2. The current role is changed from `replica` to `primary`
3. A marker file is written: `<db-path>.primary` (contains promotion timestamp)
4. The `-on-promote` command is executed (if configured)

The promotion is a one-way operation. Once promoted, the node stays primary until manually reconfigured. This prevents split-brain scenarios from automatic demotion.

---

## Security

Liteguard is designed for use within Uncloud's internal network, where all inter-node communication is encrypted by [WireGuard](https://www.wireguard.com/).

- **Transport encryption**: Handled by WireGuard at the network layer. Liteguard does not add its own TLS — this avoids redundant overhead and certificate management complexity.
- **No secrets in configuration**: Replica addresses use DNS hostnames (resolved by Uncloud internally), not IP addresses. No tokens, keys, or credentials are required.
- **Network isolation**: Liteguard only listens on the internal WireGuard interface (default port `4200`). It is never exposed to the public internet.

If you deploy Liteguard outside of a WireGuard-protected environment, you should wrap the TCP connection with an encrypted tunnel (e.g. `stunnel`, `spiped`, or a VPN).

---

## Limitations & Future Work

- **No live read replicas** — replicas are cold standbys, not queryable. This is intentional to keep the design simple and avoid consistency issues.
- **No automatic re-sync** — if a replica falls too far behind (e.g., was offline for hours), it needs a fresh copy of the database. A full-sync protocol is planned.
- **Single-primary only** — there is no multi-master support. Only one node writes at a time.
- **No application-level encryption** — Liteguard does not encrypt the TCP stream itself. **This is by design for Uncloud**, where all inter-node communication already runs through encrypted [WireGuard](https://www.wireguard.com/) tunnels — adding TLS on top would be redundant overhead. If you use Liteguard outside of a WireGuard-protected network, wrap the connection with a VPN or TLS tunnel (e.g. `stunnel`, `spiped`).

---

## Acknowledgements

Liteguard is inspired by [Litestream](https://litestream.io) by Ben Johnson. Litestream proved that WAL-based replication for SQLite is both practical and reliable. Liteguard builds on this idea by replacing S3 storage with direct node-to-node streaming and adding automatic failover — making it suitable for self-hosted infrastructure like Uncloud where external cloud dependencies are undesirable.

---

## Author

Julian Flockton

---

## License

Apache License 2.0 — see [LICENSE](LICENSE) for details.
