# Kafka Streaming Cart Pipeline

Event-driven pipeline for e-commerce cart events: **Kafka → Faust stream processor → ClickHouse**, with **deduplication**, **late-event handling**, and **Prometheus / Grafana** observability.

## Architecture
<img width="8192" height="2342" alt="image" src="https://github.com/user-attachments/assets/796bc7e4-ccfa-46a2-8e13-950d361a5955" />

What you actually need on your machine:

| Requirement | Notes |
|-------------|--------|
| **Docker** | [Docker Desktop](https://www.docker.com/products/docker-desktop/) (macOS/Windows) or Docker Engine (Linux). This project runs entirely in containers; you do not need a local Python install for `make demo`. |
| **`docker compose`** | Included with current Docker Desktop. Run `docker compose version` — it should print `v2.x`. Older standalone `docker-compose` (v1, hyphen) also works if it supports Compose file format 3. |
| **`make`** | Used only to run short commands from the `Makefile`. On macOS it is usually already available; if not: `xcode-select --install`. On Linux: `sudo apt install make` (Debian/Ubuntu) or equivalent. |
| **Free ports** | `3000`, `6066`, `9090`, `9092`, `8123`, `9002` — nothing else should listen on these ports. |
| **RAM** | About **6–8 GB** free for Docker (all services running). |
| **First run** | Often **3–5 minutes** while images download from Docker Hub and ClickHouse migrations run. Requires internet access during `docker compose build`. |

Check before you start:

```bash
docker compose version
make --version
```
## Quick start

```bash
git clone git@github.com:marinashtrassheim/kafka-streaming-pipeline.git
cd kafka-streaming-pipeline
make demo
```
(HTTPS clone: `git clone https://github.com/marinashtrassheim/kafka-streaming-pipeline.git`)

### Watch the dashboard while the demo runs

After containers start, open Grafana in your browser (keep this tab open during `make demo`):

**http://localhost:3000/d/cart-pipeline/cart-event-pipeline**

| | |
|---|---|
| Login | `admin` / `admin` |
| Navigation | **Dashboards → Kafka Pipeline → Cart Event Pipeline** (if you land on the home page) |
| Time range | **Last 15 minutes** |
| Refresh | **10s** (top right) |

The dashboard is provisioned when Grafana starts. Panels update as the consumer processes events. When the terminal shows `Running scenario producer (~2 minutes)...`, you should see counters and graphs move (saved events, duplicates, late events, lag, latency).

`make demo` will:

1. Build and start all services (`docker compose up -d --build`)
2. Wait until the Faust consumer is ready
3. Print the Grafana dashboard URL
4. Run the **2-minute scenario producer** (18 scripted events)
5. Print expected metric totals

## Scenario highlights

The producer sends a fixed script (~120 s):

- Cart updates and **checkout** path
- **Deduplication** (same `event_id` twice → one save, one duplicate)
- **Late event** within 5 min tolerance → saved, flagged `is_late`
- **Late event** beyond tolerance → rejected to `dead_letter`
- **Several users** send cart events within the same calendar minute; ClickHouse **materialized views** roll those rows up into per-minute metrics (for example event counts per minute) for analytics

## Services

| Service | URL / port | Role |
|---------|------------|------|
| Grafana | http://localhost:3000 | Dashboards |
| Prometheus | http://localhost:9090 | Metrics storage (backend for Grafana) |
| Consumer metrics | http://localhost:6066/metrics | Faust worker Prometheus scrape endpoint |
| ClickHouse HTTP | http://localhost:8123 | SQL over HTTP |
| Kafka | localhost:9092 | Broker (internal: `kafka:9092`) |

## Make targets

```bash
make help        # list commands
make demo        # full demo (recommended)
make up          # start stack only
make producer    # run scenario producer only
make logs        # follow consumer logs
make ps          # container status
make down        # stop containers
make clean       # stop and remove volumes (fresh Kafka/Faust state)
```

## Verify data in ClickHouse (optional)

```bash
docker compose exec clickhouse clickhouse-client --query \
  "SELECT event_id, user_id, event_type, is_late FROM cart_events_raw ORDER BY processed_at DESC LIMIT 10"
```

## Troubleshooting

| Symptom | What to do |
|---------|------------|
| `make demo` times out on consumer | `make logs` — wait for `Worker ready`; ensure port `6066` is free |
| Grafana panels empty | Consumer must be Up: `make ps`; rerun `make producer` |
| All events are duplicates, Saved = 0 | Old Faust changelog state; run `make clean && make demo` |
| Port already in use | Stop other stacks using the same ports (see Prerequisites) |

