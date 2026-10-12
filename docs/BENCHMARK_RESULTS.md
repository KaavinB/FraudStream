# Executed benchmark report

Raw artifacts include individual measurements and conditions. Blocked runs are not successful benchmarks.

## features-20261012T035436Z

Status: executed; artifact: `benchmarks/results/features-20261012T035436Z.json`.

| Implementation | Entities | Events/sec | Lookup p95 (ms) | Update p95 (ms) |
|---|---:|---:|---:|---:|
| baseline_client | 10 | 190.3 | 3.10 | 4.82 |
| lua_server | 10 | 800.4 | 0.91 | 1.29 |
| baseline_client | 100 | 225.5 | 1.64 | 3.73 |
| lua_server | 100 | 841.5 | 0.63 | 1.23 |
| baseline_client | 1000 | 230.4 | 1.52 | 3.70 |
| lua_server | 1000 | 920.7 | 0.58 | 1.20 |

## local-20261012T022309Z

Status: executed; artifact: `benchmarks/results/local-20261012T022309Z.json`.

CPU model compute p95: 1.984 ms; in-process FastAPI p95: 3.398 ms.

These measure actual software on synthetic transactions. They exclude Kafka, Redis, PostgreSQL, and TCP.

## local-20261012T022710Z

Status: executed; artifact: `benchmarks/results/local-20261012T022710Z.json`.

CPU model compute p95: 0.580 ms; in-process FastAPI p95: 1.949 ms.

These measure actual software on synthetic transactions. They exclude Kafka, Redis, PostgreSQL, and TCP.

## local-20261012T023152Z

Status: executed; artifact: `benchmarks/results/local-20261012T023152Z.json`.

CPU model compute p95: 0.337 ms; in-process FastAPI p95: 1.123 ms.

These measure actual software on synthetic transactions. They exclude Kafka, Redis, PostgreSQL, and TCP.

## pipeline-20261012T033820Z

Status: executed; artifact: `benchmarks/results/pipeline-20261012T033820Z.json`.

**Closed loop before the batched-commit and decision-lookup fixes.**

Mode: closed. Original amounts/entities, unique IDs, same wall-clock event timestamp per trial; closed loop includes decision polling

| Clients | Decisions/sec | Post-ack p95 (ms) | Errors |
|---:|---:|---:|---:|
| 1 | 36.7 | 26 | 0 |
| 1 | 36.9 | 26 | 0 |
| 1 | 36.8 | 26 | 0 |
| 10 | 98.1 | 130 | 0 |
| 10 | 95.6 | 123 | 0 |
| 10 | 91.0 | 136 | 0 |
| 25 | 99.9 | 287 | 0 |
| 25 | 98.7 | 290 | 0 |
| 25 | 95.4 | 321 | 0 |
| 50 | 83.4 | 852 | 0 |
| 50 | 83.0 | 898 | 0 |
| 50 | 79.9 | 874 | 0 |
| 100 | 63.2 | 2829 | 0 |
| 100 | 61.9 | 2868 | 0 |
| 100 | 60.0 | 2943 | 0 |

## pipeline-20261012T034226Z

Status: executed; artifact: `benchmarks/results/pipeline-20261012T034226Z.json`.

**Exploratory single repetition after batched offset commits.**

Mode: closed. Original amounts/entities, unique IDs, same wall-clock event timestamp per trial; closed loop includes decision polling

| Clients | Decisions/sec | Post-ack p95 (ms) | Errors |
|---:|---:|---:|---:|
| 1 | 36.9 | 26 | 0 |
| 25 | 155.7 | 195 | 0 |
| 100 | 70.4 | 2491 | 0 |

## pipeline-20261012T034420Z

Status: executed; artifact: `benchmarks/results/pipeline-20261012T034420Z.json`.

**Exploratory single repetition, same build as the previous run (shows run-to-run variance).**

Mode: closed. Original amounts/entities, unique IDs, same wall-clock event timestamp per trial; closed loop includes decision polling

| Clients | Decisions/sec | Post-ack p95 (ms) | Errors |
|---:|---:|---:|---:|
| 1 | 36.8 | 26 | 0 |
| 25 | 105.1 | 316 | 0 |
| 100 | 73.4 | 2796 | 0 |

## pipeline-20261012T034619Z

Status: executed; artifact: `benchmarks/results/pipeline-20261012T034619Z.json`.

**Batched commits plus primary-key decision lookup, 3 repetitions.**

Mode: closed. Original amounts/entities, unique IDs, same wall-clock event timestamp per trial; closed loop includes decision polling

| Clients | Decisions/sec | Post-ack p95 (ms) | Errors |
|---:|---:|---:|---:|
| 10 | 183.8 | 70 | 0 |
| 10 | 140.2 | 90 | 0 |
| 10 | 165.0 | 77 | 0 |
| 25 | 115.0 | 314 | 0 |
| 25 | 114.7 | 300 | 0 |
| 25 | 134.6 | 251 | 0 |
| 50 | 88.1 | 832 | 0 |
| 50 | 90.4 | 785 | 0 |
| 50 | 87.9 | 840 | 0 |
| 100 | 72.3 | 3278 | 0 |
| 100 | 73.4 | 2764 | 0 |
| 100 | 74.3 | 3060 | 0 |

## pipeline-20261012T040721Z

Status: executed; artifact: `benchmarks/results/pipeline-20261012T040721Z.json`.

**Final closed loop on the finished code (all fixes). Client polls each decision every 20ms, so polling load grows with concurrency.**

Mode: closed. Original amounts/entities, unique IDs, same wall-clock event timestamp per trial; closed loop includes per-request decision polling every 20ms

| Clients | Decisions/sec | Post-ack p95 (ms) | Errors |
|---:|---:|---:|---:|
| 1 | 36.1 | 27 | 0 |
| 1 | 36.0 | 27 | 0 |
| 1 | 36.6 | 27 | 0 |
| 10 | 172.0 | 81 | 0 |
| 10 | 184.8 | 71 | 0 |
| 10 | 188.3 | 63 | 0 |
| 25 | 144.4 | 224 | 0 |
| 25 | 132.4 | 257 | 0 |
| 25 | 145.2 | 223 | 0 |
| 50 | 94.4 | 878 | 0 |
| 50 | 96.7 | 767 | 0 |
| 50 | 87.0 | 850 | 0 |
| 100 | 66.4 | 3206 | 0 |
| 100 | 63.5 | 3242 | 0 |
| 100 | 60.6 | 3043 | 0 |

## pipeline-drain-20261012T034801Z

Status: executed; artifact: `benchmarks/results/pipeline-drain-20261012T034801Z.json`.

**Batched per-partition offset commit (1 partition, 1 worker). Compare with the per-record-commit baseline.**

Mode: drain. Original amounts/entities, unique IDs, same wall-clock event timestamp per trial; drain: unpolled submission, completion from aggregate decision counter (0.1s resolution)

| Clients | Decisions/sec | Post-ack p95 (ms) | Errors |
|---:|---:|---:|---:|
| 10 | 290.2 | — | 0 |
| 10 | 241.3 | — | 0 |
| 10 | 234.9 | — | 0 |
| 50 | 224.0 | — | 0 |
| 50 | 226.3 | — | 0 |
| 50 | 231.6 | — | 0 |
| 100 | 236.4 | — | 0 |
| 100 | 221.9 | — | 0 |
| 100 | 216.5 | — | 0 |

## pipeline-drain-20261012T035001Z

Status: executed; artifact: `benchmarks/results/pipeline-drain-20261012T035001Z-baseline-per-record-commit.json`.

**A/B baseline: previous worker build that synchronously committed the Kafka offset after every record (1 partition, 1 worker).**

Mode: drain. Original amounts/entities, unique IDs, same wall-clock event timestamp per trial; drain: unpolled submission, completion from aggregate decision counter (0.1s resolution)

| Clients | Decisions/sec | Post-ack p95 (ms) | Errors |
|---:|---:|---:|---:|
| 50 | 107.2 | — | 0 |
| 50 | 104.1 | — | 0 |
| 50 | 106.2 | — | 0 |

## pipeline-drain-20261012T035548Z

Status: executed; artifact: `benchmarks/results/pipeline-drain-20261012T035548Z-4partitions-1workers.json`.

**Scaling: 4 partitions, 1 processor. Trial 0 is a warm-up outlier: the running consumer had not yet discovered partitions added live with kafka-topics --alter.**

Mode: drain. Original amounts/entities, unique IDs, same wall-clock event timestamp per trial; drain: unpolled submission, completion from aggregate decision counter (0.1s resolution)

| Clients | Decisions/sec | Post-ack p95 (ms) | Errors |
|---:|---:|---:|---:|
| 50 | 11.4 | — | 0 |
| 50 | 305.5 | — | 0 |
| 50 | 292.3 | — | 0 |

## pipeline-drain-20261012T035916Z

Status: executed; artifact: `benchmarks/results/pipeline-drain-20261012T035916Z-4partitions-2workers.json`.

**Scaling: 4 partitions, 2 processors.**

Mode: drain. Original amounts/entities, unique IDs, same wall-clock event timestamp per trial; drain: unpolled submission, completion from aggregate decision counter (0.1s resolution)

| Clients | Decisions/sec | Post-ack p95 (ms) | Errors |
|---:|---:|---:|---:|
| 50 | 294.8 | — | 0 |
| 50 | 307.3 | — | 0 |
| 50 | 262.9 | — | 0 |

## pipeline-drain-20261012T035957Z

Status: executed; artifact: `benchmarks/results/pipeline-drain-20261012T035957Z-4partitions-4workers.json`.

**Scaling: 4 partitions, 4 processors. Flat throughput shows the global feature sequencer is the bottleneck.**

Mode: drain. Original amounts/entities, unique IDs, same wall-clock event timestamp per trial; drain: unpolled submission, completion from aggregate decision counter (0.1s resolution)

| Clients | Decisions/sec | Post-ack p95 (ms) | Errors |
|---:|---:|---:|---:|
| 50 | 298.0 | — | 0 |
| 50 | 289.1 | — | 0 |
| 50 | 285.6 | — | 0 |

## reliability-20261012T032540Z

Status: failed; artifact: `benchmarks/results/reliability-20261012T032540Z.json`.

**Before the consumer session-timeout fix: crashed members held partitions for the 45s franz-go default.**

| Crash point | Recovery (s) | Duplicate decisions | Missing decisions |
|---|---:|---:|---:|
| before_features | 45.3 | 0 | 0 |
| after_feature_intent | 45.4 | 0 | 0 |
| after_features | 45.4 | 0 | 0 |
| before_inference | 45.4 | 0 | 0 |
| after_inference | 45.3 | 0 | 0 |
| after_persistence | 9.3 | 0 | 0 |

Blocking evidence: configured crash point did not execute: before_offset_commit

## reliability-20261012T033443Z

Status: passed; artifact: `benchmarks/results/reliability-20261012T033443Z.json`.

| Crash point | Recovery (s) | Duplicate decisions | Missing decisions |
|---|---:|---:|---:|
| before_features | 21.3 | 0 | 0 |
| after_feature_intent | 6.7 | 0 | 0 |
| after_features | 6.8 | 0 | 0 |
| before_inference | 6.8 | 0 | 0 |
| after_inference | 6.2 | 0 | 0 |
| after_persistence | 6.6 | 0 | 0 |
| before_offset_commit | 8.7 | 0 | 0 |
| during_alert_publication | 9.4 | 0 | 0 |

## reliability-20261012T035114Z

Status: passed; artifact: `benchmarks/results/reliability-20261012T035114Z.json`.

| Crash point | Recovery (s) | Duplicate decisions | Missing decisions |
|---|---:|---:|---:|
| before_features | 6.4 | 0 | 0 |
| after_feature_intent | 6.1 | 0 | 0 |
| after_features | 6.3 | 0 | 0 |
| before_inference | 7.3 | 0 | 0 |
| after_inference | 6.6 | 0 | 0 |
| after_persistence | 6.1 | 0 | 0 |
| before_offset_commit | 6.7 | 0 | 0 |
| during_alert_publication | 6.1 | 0 | 0 |

## reliability-20261012T040104Z

Status: failed; artifact: `benchmarks/results/reliability-20261012T040104Z-smoke-only.json`.

**Exposed quarantine loss after Kafka offset reuse (topic recreated); fixed by migration 002.**

Blocking evidence: late event was not durably quarantined

## reliability-20261012T040419Z

Status: passed; artifact: `benchmarks/results/reliability-20261012T040419Z.json`.

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

