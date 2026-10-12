# Benchmark methodology

Do not quote a metric without its experiment artifact. [Generated results](BENCHMARK_RESULTS.md) summarize every executed run.

| Experiment | Command | Boundary / required system |
|---|---|---|
| Ingestion + durable decisions (closed loop) | `make benchmark` | Real Go/Kafka/Redis/ML/PostgreSQL, clients 1/10/25/50/100, three repetitions, per-request decision polling |
| Pipeline drain throughput | `python -m benchmarks.pipeline --mode drain` | Same stack; unpolled submission, completion from one aggregate counter |
| CPU model + FastAPI | `make benchmark-local` | Actual trained model; Python process and in-process HTTP test client, no TCP |
| Redis baseline vs Lua | `.venv/bin/python -m benchmarks.features` | Real Redis; same events/history, one caller, entity counts 10/100/1000 |
| Recovery | `make integration` | Real containers, eight controlled crash points, uniqueness and pending-outbox assertions |
| Scaling | `sh scripts/scale.sh` | Worker configurations 1/2/4; explicitly configure topic partitions beforehand |
| Feature parity | `make test-python` | Actual Lua through fakeredis/Lua compared with independent Python oracle, not real Redis performance |
| Drift/labels | `make benchmark-local` and `ml.evaluation.delayed` | Controlled numeric shift and explicit seven-day delayed-label availability |

### Throughput and latency

Pipeline trials use a unique transaction set. Completed unique decisions/sec is distinct completed IDs divided by wall time from load start through final observed decision, including drain. HTTP acceptance is recorded separately; an HTTP 202 followed by scoring timeout remains an accepted request, not a completed outcome. Closed-loop clients wait for decisions, so acceptance throughput is not an isolated open-loop HTTP capacity measurement. Each closed-loop client polls its decision every 20 ms, so at high concurrency that polling competes with the pipeline for the same host. Drain mode submits without polling and watches `completed_unique_decisions` from `/api/v1/analytics/overview` every 0.1 s; its throughput is the pipeline's, but it has no per-transaction latency. Pure Kafka publishing benchmarks and k6/Locust open-loop load remain unimplemented.

Primary client latency is time from receipt of successful HTTP acknowledgment to the first successful GET exposing a committed decision. It includes polling/network overhead and can be zero/small if processing completes before acknowledgment. The worker's Prometheus `end_to_end` metric measures acceptance timestamp to post-COMMIT return. SQL `precommit_ms` measures acceptance to decision INSERT and **excludes COMMIT**; it is only a diagnostic lower bound and must not be used as a durable-completion resume metric.

Performance workloads retain source amounts/entities but assign unique IDs and one current event timestamp per trial to prevent concurrency-induced strict-watermark violations. This is explicitly a synthetic transport/processing workload, not a model-quality test or realistic activity timeline. Ordered historical demo replay uses original timestamps and waits per event. Varying concurrency changes arrival order among equal timestamps; scale comparisons must use the same timestamp policy and report failures/quarantines.

Latency raw samples use monotonic nanosecond clocks locally. Samples are summarized with NumPy linear percentiles. p99 is suppressed below 1,000 samples; p50/p95/mean and sample counts are always recorded. Three repetitions are the pipeline default. Component runs expose variance between separate runs rather than publishing only the fastest result. No cross-machine claim is inferred. Batch rates measure compute only, excluding queue delay.

### Features and reliability

Redis lookup/update timings are measured separately using identical inputs and isolated namespaces for a deliberately simple client-side baseline and server-side Lua. The baseline is correct for the single-thread benchmark and is not a production-concurrent alternative. Whole-instance Redis memory is distinguished from workload-attributable memory. A feature cap limits very dense windows; aggregate memory still grows with entity cardinality.

Feature freshness means acceptance-to-materialization, including queueing and feature-journal work. Kafka-to-materialization is a separate stage metric. Neither is historical event age during accelerated replay. The Python feature-oracle benchmark is explicitly **not Redis freshness or Redis latency**.

The fault harness explicitly targets created test containers and transaction IDs. It waits for exit code 86 to prove the crash point ran, restarts the service, verifies one decision, at most one logical alert, published outbox rows and observed alerts. Physical Kafka publication duplicates are permitted and not counted as unique logical alerts. Zero-loss claims apply only to an enumerated tested set, never arbitrary failure combinations. Recovery time is measured from the observed crash to the observed decision and is dominated by the consumer session timeout. Temporary infrastructure outages and resumable Redis full-state rebuild tests remain outstanding.

### Metadata and evidence

Artifacts include UTC date, experiment ID, source-tree hash, Git commit (when the checkout is a repository), dataset/model versions, seed, sample count, concurrency, software/hardware and raw samples. Compose fixes container memory limits; local CPU benchmarks record thread limits but are not CPU-isolated. Host RAM inspection is denied, so it is null. Kafka/worker counts are operator-declared in pipeline metadata and require verification against the running cluster; they are not automatically discovered.

Controlled drift uses PSI with quantile bins, a half-count smoothing prior and a heuristic 0.2 threshold, with KS statistics also reported. It requires 200 samples and enough variation. The synthetic Gaussian shift is not fraud behavior, not a calibrated false-alarm guarantee, and not confirmed model degradation. Runtime rolling drift/label metrics are not implemented. Artifacts can carry a `label` field describing the experiment (for example an A/B baseline); measurements are never edited.
