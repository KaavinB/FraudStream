# Executed tests and checks

Verification date: 2026-10-11 local / 2026-10-12 UTC. Apple Silicon macOS, Docker Desktop 29.8.0, Python 3.12.15, Go 1.26.8 (in the `golang:1.26.8-bookworm` container), Node 22.

| Check | Command | Executed result |
|---|---|---|
| Go formatting | `gofmt -l internal services` | Clean |
| Go static analysis | `go vet ./...` | Pass |
| Go unit tests | `go test -race -count=1 ./...` | 6 tests pass under the race detector |
| Go module integrity | `go mod tidy -diff` | `go.sum` committed and current |
| Python tests | `make test-python` | 24 passed |
| Python lint / types | `make lint` | Ruff pass; mypy pass (21 source files) |
| Frontend | `cd frontend && npm ci && npm run build` | TypeScript and Vite build pass (bundle >500 kB warning) |
| Model training | `make train` | Three models trained incl. XGBoost 3.4.1; MLflow run logged (11 metrics, 5 params) |
| Container build + startup | `make up` | All images build; all services healthy |
| Smoke | `make smoke` | 20 transactions → 20 durable decisions → 20 outbox messages published |
| Integration + crash recovery | `make integration` | Passed: duplicate submission, strict-lateness quarantine and all 8 crash points (`reliability-20261012T040419Z.json`) |
| Migration | `make migrate` on live volume, re-run, and fresh-volume init | Applies, idempotent, fresh init runs 001 → 002 |
| Pipeline load | `make benchmark`; `benchmarks.pipeline --mode drain` | 0 errors across all trials |
| Real Redis features | `python -m benchmarks.features` | Executed against Redis 7.4.6 |
| Dashboard in a browser | headless Chrome via puppeteer-core, all six tabs | 0 console errors, 0 failed API calls |

## Crash points

`scripts/integration.py` starts a dedicated worker or alert-publisher container with `CRASH_AT=<stage>` and `CRASH_TRANSACTION_ID=<id>`, submits that transaction, waits for exit code 86 to prove the crash point ran, restarts the normal service and asserts exactly one decision, at most one logical alert, a drained outbox and a consumer-observed alert.

| Crash point | Recovery (s) | Duplicate decisions | Missing decisions |
|---|---:|---:|---:|
| before_features | 6.9 | 0 | 0 |
| after_feature_intent | 6.3 | 0 | 0 |
| after_features | 6.6 | 0 | 0 |
| before_inference | 6.8 | 0 | 0 |
| after_inference | 6.6 | 0 | 0 |
| after_persistence | 6.1 | 0 | 0 |
| before_offset_commit | 6.2 | 0 | 0 |
| during_alert_publication | 6.1 | 0 | 0 |

The first run (`reliability-20261012T032540Z.json`) failed at `before_offset_commit` because crashed consumers held their partition for the 45 s default session timeout; see the session-timeout fix in the README.

## Regression evidence for fixes

- **Quarantine after offset reuse:** after recreating `transactions.raw`, a late event at offset 22 collided with an older quarantine row and was dropped (`reliability-20261012T040104Z-smoke-only.json`). After migration 002, a transactional check inserted two different payloads at the same topic/partition/offset (both stored) and a redelivery of the first (deduplicated).
- **Batched offset commits:** drain-mode A/B against a rebuilt per-record-commit worker (`pipeline-drain-20261012T035001Z-baseline-per-record-commit.json` vs `pipeline-drain-20261012T034801Z.json`); integration suite re-run afterwards.
- **Decision lookup plan:** `EXPLAIN ANALYZE` of the generic plan showed `Rows Removed by Filter: 9037` for one ID before the query split.
- **Dashboard proxy:** ingestion was recreated while another container held its previous IP (172.19.0.8 → 172.19.0.10); the dashboard kept serving API calls.

## Coverage notes

Python tests cover rolling-window boundaries, equal timestamps, prior-history-only features, duplicate materialization, conflicting IDs, zero-lateness quarantine, seeded Python/Lua/client-baseline replay parity (Lua run through fakeredis), separate labels, deterministic generation, delayed-label masks, threshold ties, metrics, controlled drift, model loading, batch equivalence and schema rejection. Go unit tests cover event validation/serialization, ingestion API behaviour with a mocked publisher, label rejection, bounded retries and cancellation. Storage, Kafka and Redis integration paths are covered by the container integration suite rather than Go unit tests.
