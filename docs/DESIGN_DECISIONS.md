# Design decisions

| Decision | Reason | Cost / next experiment |
|---|---|---|
| Kafka with KRaft | Durable asynchronous log, consumer groups, explicit offsets and replay | Single local broker has no replica fault tolerance; acks=all here means one broker |
| Go transport, Python model | Separate resource/runtime concerns; CPU structured-data models integrate over HTTP | Serialization and HTTP overhead must be measured on the full pipeline |
| At-least-once + SQL idempotency | External Redis/SQL effects cannot share a Kafka transaction | Retries and rebalances can redeliver; no end-to-end exactly-once claim |
| Durable feature-intent journal | Preserve the scoring vector across crashes before/after Redis writes | Extra database round trips and journal storage |
| Global feature sequencer | Shared merchant/customer histories need a defined order across partitions | A serialized bottleneck; sharded sequencing requires another design, not a flag |
| Zero-lateness quarantine | Honest first implementation without retroactive or partial-history scoring | Multi-partition merchant regressions and realistic late arrivals can be rejected |
| Transactional alert outbox | Decision and alert intent commit together | Physical Kafka duplicates remain possible; logical alerts deduplicate |
| Redis sorted sets + Lua | Bounded time windows, atomic snapshot/update primitives | Work is linear in retained window size; exact distinct counts are not constant-time |
| Three temporal validation stages | Fit on available old labels, calibrate later, tune/select later still | Seven-day delay removes 36,359 unavailable-label rows from fitting/validation |
| CPU XGBoost candidate + two baselines | Structured features and local ARM hardware | HistGradientBoosting won validation AP over XGBoost and logistic regression; XGBoost is kept as a compared candidate |
| No neural network / Kubernetes | Neither has a measured benefit for this workload yet | These technologies are not represented as accomplishments |
| Same-origin dashboard proxy | No permissive browser CORS or hardcoded token | Developer enters local key; build verified, browser/runtime not verified |

Thresholds are selected from natural class prevalence, after Platt calibration on a disjoint chronological validation segment. Model selection maximizes validation average precision. The frozen operating point is the lowest attainable threshold satisfying an empirical 1% validation FPR budget, with tied scores treated together. Test FPR is measured independently and differs from the budget.

Original engineering includes the event schema, prior-history contract and independent implementations, durable-intent/outbox protocol, replay/fault tools, temporal label availability logic and benchmark boundary definitions. Kafka durability, Redis command execution, PostgreSQL constraints, estimator training and HTTP routing are provided by their respective libraries/frameworks.
