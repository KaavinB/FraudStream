# Local development threat model

Trust boundaries are client→ingestion, application→Kafka/Redis/PostgreSQL, worker→inference, and browser→authenticated analytics. The only data is synthetic payments; no payment credentials, PII, actual financial action or external payment-network integration exists.

Ingestion requires an environment API key (minimum 16 characters), constant-time comparison, bounded JSON/body/header sizes, strict fields, supported schema version, finite cent amounts, global rate limiting and in-flight limits. Historical timestamps are allowed for replay; excessive future timestamps are rejected. A single global fixed-window limit is not a multi-tenant quota system.

Compose ports bind loopback. Internal Kafka/Redis/HTTP are plaintext and unauthenticated on the private development network. PostgreSQL's password is supplied from the ignored `.env`. Setup generates secrets without printing them; the example file contains clearly labeled development values. The local app role remains the bootstrap role and is not a tested least-privilege deployment.

The dashboard uses a same-origin proxy, keeps the entered API token only in page memory and never substitutes runtime numbers. Nginx sets CSP, content-type protection and referrer policy. React escapes display values. Prometheus and inference endpoints are trusted-network services, not public APIs. No wildcard CORS is enabled.

Database statements parameterize application inputs. Fault-test SQL interpolation only uses generated internal UUID identifiers/stage constants, never arbitrary transaction payloads. Logs should not include API keys or payment details; failed validation exposes a generic JSON error or bounded schema reason. Quarantine data contains the raw synthetic message and requires the same trusted local access as decisions.

Joblib artifacts are executable deserialization inputs: only load trusted artifacts created by the training workflow, never arbitrary uploads. Dependencies are directly pinned and the frontend has an executed npm lockfile; transitive Python/Go locks and vulnerability review remain incomplete. There is no PCI-DSS compliance or production banking-readiness claim.
