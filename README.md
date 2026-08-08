# Real-Time Data Pipeline Platform

A modular, real-time ingestion pipeline. The first connector streams live
crypto trades from Binance, but the architecture is built so that adding a
new source — news, retail POS, weather, sports scores, sensor data — means
writing one small connector file, not redesigning the pipeline.

## Architecture

```
Binance WebSocket
      |
      v
[connector] --publishes--> [Kafka topic: raw.crypto.trades] --consumed by--> [normalizer]
                                (Redpanda locally)                                |
                                                                                   v
                                                                    [Postgres: raw_events + crypto_trades]
                                                                                   |
                                                                                   v
                                                                            [FastAPI: /trades/{symbol}]
```

- **connectors/** — one file per data source. Each implements `BaseConnector`
  and only knows how to talk to its source. It emits a common `RawEvent`
  envelope, nothing more.
- **processor/normalizer.py** — consumes raw events, writes an untouched copy
  to `raw_events` (so you can replay/reprocess later), then parses each
  source into clean, typed tables via a small per-source parser function.
- **storage/schema.sql** — Postgres schema, auto-applied on first startup.
- **api/main.py** — FastAPI service exposing the normalized data.

## Prerequisites

- Docker Desktop installed and running (https://www.docker.com/products/docker-desktop/)
- That's it — everything else runs inside containers.

## Running it

1. From the project root, start everything:

   ```bash
   docker compose up --build
   ```

   First run will take a couple of minutes (downloading images, installing
   Python deps). You'll see logs from all 5 services interleaved.

2. Once you see `Producer connected to redpanda:29092` and
   `Normalizer consuming from topic 'raw.crypto.trades'`, data is flowing.

3. Check it's working:

   - Redpanda Console (see messages flowing through Kafka):
     http://localhost:8080
   - API docs (interactive, try it in the browser):
     http://localhost:8000/docs
   - Latest BTC trade:
     http://localhost:8000/trades/BTCUSDT/latest
   - Last 50 ETH trades:
     http://localhost:8000/trades/ETHUSDT?limit=50

4. To stop everything:

   ```bash
   docker compose down
   ```

   Add `-v` (`docker compose down -v`) if you also want to wipe the stored
   data and start fresh next time.

## Inspecting the data directly

```bash
docker exec -it crypto-postgres psql -U pipeline -d crypto_pipeline
```

Then in psql:
```sql
SELECT COUNT(*) FROM raw_events;
SELECT symbol, price, trade_time FROM crypto_trades ORDER BY trade_time DESC LIMIT 10;
```

## Adding a new data source (proving the "modular" design)

1. Create `connectors/your_source.py`, subclassing `BaseConnector`.
   Implement `stream()` to yield raw dicts from your source.
2. Create `parse_your_source()` in `processor/normalizer.py`, register it
   in `PARSERS`.
3. Add the matching table to `storage/schema.sql`.
4. Add a new service block to `docker-compose.yml` (copy the `producer`
   block, point `command` at your new connector).

No changes needed to Kafka setup, the normalizer's consumption loop, or the
API's structure — that's the point of the connector interface.

## Roadmap / what's next

- [ ] Second connector (e.g. news RSS or GTFS transit) to prove modularity
      end-to-end, not just in the abstract
- [ ] Basic tests (parser functions are pure functions — easy to unit test)
- [ ] Deploy to GCP: Pub/Sub instead of Redpanda, Cloud SQL or BigQuery,
      Cloud Run for normalizer/API
- [ ] Dead-letter handling for malformed events instead of just logging+drop
- [ ] Schema registry / versioning if a source's payload shape changes
