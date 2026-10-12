# Event schema versioning

`transaction.v1.json` is the client contract. The Go API also requires cent precision and validates timestamp support (year >=2000, no more than five minutes in the future). RFC3339 timestamps need a timezone. Schema-validation JSON alone does not substitute for those Go application checks.

The Kafka envelope wraps an event with server `accepted_at` and a generated `correlation_id`. Event time, server acceptance, feature preparation/materialization, model scoring and SQL persistence timestamps are distinct. A 202 response means configured Kafka acknowledgment, not a final decision. Ambiguous producer timeout should be retried with the same identifiers.

`transaction_id` is the logical idempotency key. `source_event_id` is independently unique in SQL. An identical resubmission reuses the persisted feature vector/outcome. Reusing an ID for a different payload is quarantined by the worker; acceptance itself is asynchronous and is not a promise that later application validation succeeds. Individual fields are not used as metric labels.

V1 permits USD PAYMENT only. `device_id` is optional and unused by `prior-v1`. Ground-truth fraud labels and scenarios are forbidden in client requests and absent from inference vectors. Labels join later through stable IDs and explicit availability timestamps.

Backward-incompatible changes require a new schema version and explicit consumer/producer support before rollout. Current code rejects unsupported versions; no automatic upcaster or schema registry is claimed. Evolve the feature version separately from transport versions, retrain/calibrate and run parity tests before deploying a changed feature order.
