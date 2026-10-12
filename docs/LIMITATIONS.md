# Verification boundaries and remaining limitations

The full stack has been built and run end to end in Docker on one Apple Silicon laptop, with real Kafka, Redis, PostgreSQL, Python inference, Go services and the dashboard. The checks listed in [TESTING.md](TESTING.md) passed. This document lists what is still **not** demonstrated or not implemented, so no claim outruns the evidence.

## Deployment scope

- Single Kafka broker and one PostgreSQL/Redis instance, all on one host. `acks=all` therefore means one replica. Nothing here establishes high availability, multi-host behaviour or cross-fault-domain durability.
- All throughput and latency numbers come from one laptop where the load generator, all containers and the Docker VM share CPUs. They are relative measurements for this configuration, not capacity planning figures.
- No least-privilege database roles, TLS/SASL or per-tenant quotas. No payment-network integration or financial side effect exists.

## Throughput and scaling

- The global PostgreSQL advisory lock (feature sequencer) serializes feature preparation across all workers. Measured: 4 partitions with 1, 2 or 4 workers all stay at ~290–300 decisions/sec. Removing this bottleneck needs entity-consistent ownership or a stream-join/state engine; it is the main architectural limit.
- Zero event-time lateness per customer and merchant: regressions are quarantined, not buffered or corrected. Cross-customer merchant ordering is not guaranteed across partitions; one partition and ordered replay is the deterministic path. The 4-partition scaling runs used equal timestamps per trial and produced no new quarantines, which does not prove ordering safety for realistic timelines.
- Closed-loop benchmarks poll every in-flight decision every 20 ms, so at high client concurrency the polling load itself competes with the pipeline. The drain mode removes that effect but resolves completion only to 0.1 s. Open-loop fixed-arrival-rate load (k6/Locust style) and CPU profiling are not implemented.
- Partitions were raised live with `kafka-topics --alter` for the scaling test; a running consumer took one trial to discover them (recorded warm-up outlier). The topic was then recreated with one partition.

## Recovery and reliability

- Eight injected process-crash points pass. **Not tested:** Redis, PostgreSQL or inference becoming unavailable under load, broker restarts, network partitions, adverse rebalance timing, or combinations of failures. Zero-loss claims apply only to the enumerated scenarios.
- Crash recovery takes ~6 s, dominated by the 6 s consumer session timeout (the broker minimum by default). Lower values require broker configuration; static group membership could avoid the wait for planned restarts.
- Full Redis reset recovery replays the whole journal into a new generation without a resumable checkpoint; a large journal can exceed a per-attempt timeout. Loss of individual entity keys while generation metadata survives is undetected. `noeviction` and no external key mutation are assumptions. Redis Cluster is unsupported, idle entity state persists and memory grows with entity cardinality; a 50,000-event active-window cap fails closed.
- The outbox can publish a physical Kafka alert more than once after a publisher crash; logical alert IDs stay unique. This is at-least-once delivery with idempotent durable outcomes, not end-to-end exactly-once.
- Retry backoff is bounded exponential without jitter.
- Migrations run automatically only on a fresh PostgreSQL volume; existing volumes need `make migrate`. There is no migration-version table.

## Machine learning

- Data is an original Handbook-inspired synthetic simulation with rule-based labels, not the canonical upstream dataset or real bank data. 160 held-out fraud cases make the metrics noisy; no confidence intervals are reported.
- MLflow logs parameters, metrics and artifacts to a local `mlflow.db`. Named model registration and serving from a registry are not implemented; the service loads versioned local joblib artifacts.
- Runtime input-drift and delayed-label quality collectors are not connected. Tested PSI/KS and delayed-label helpers exist; the dashboard shows unavailable states rather than invented values.

## Engineering hygiene

- Python transitive dependencies are not locked (only direct pins); `go.sum` and `package-lock.json` exist.
- The GitHub Actions workflow has not run on GitHub. Its Go, Python, frontend and integration steps were run locally with the same commands.
- Go unit tests cover schema validation, the ingestion API with mocked publishers and retry/cancellation. Storage and Kafka code are exercised by the integration suite, not Go unit tests; no Go coverage figure is reported.
