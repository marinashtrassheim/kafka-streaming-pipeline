# Kafka Streaming Cart Pipeline

Event-driven pipeline for e-commerce cart events: **Kafka → Faust stream processor → ClickHouse**, with **deduplication**, **late-event handling**, and **Prometheus / Grafana** observability.

## Architecture
<img width="8192" height="2342" alt="image" src="https://github.com/user-attachments/assets/796bc7e4-ccfa-46a2-8e13-950d361a5955" />

## Prerequisites

- **Docker** and **Docker Compose** v2
- **Make**
- Free ports: `3000`, `6066`, `9090`, `9092`, `8123`, `9002`
- ~6–8 GB RAM for all containers
- First run may take **3–5 minutes** (image pull + ClickHouse migrations)

## Quick start (one command)

```bash
git clone <repository-url>
cd kafka-streaming-pipeline
make demo
```

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
- Multi-user burst for minute-level aggregation in ClickHouse MVs

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

