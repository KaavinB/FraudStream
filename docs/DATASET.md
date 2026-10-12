# Dataset provenance and selection

The project compares the [Fraud Detection Handbook simulator](https://fraud-detection-handbook.github.io/fraud-detection-handbook/Chapter_3_GettingStarted/SimulatedDataset.html) and [PaySim](https://www.kaggle.com/datasets/ealaxi/paysim1). Both are **synthetic**, not live bank records.

| Option | Useful properties | Access and limitations |
|---|---|---|
| Handbook | Customer/terminal IDs, timestamped activity, behavioral profiles and time-dependent fraud; local reproducible simulation | Upstream implementation uses [GPLv3](https://github.com/Fraud-Detection-Handbook/fraud-detection-handbook/blob/main/LICENSE). Copying its implementation requires honoring that license. Simulation rules are simplified. |
| PaySim | Mobile-money entities, transaction types, fraud labels and hourly simulation steps | Kaggle distribution should not be assumed anonymously accessible. Dataset license/redistribution terms were not independently established from the rendered Kaggle page; no PaySim files are downloaded or redistributed. [Original simulator](https://github.com/EdgarLopezPhD/PaySim) requires its own Java-based workflow. |

**Selected local path:** an original, explicitly Handbook-inspired simulator, rather than redistributed upstream code/data. It uses local NumPy CPU generation with no paid service or credentials. This is not the canonical Handbook dataset and its quality metrics must not be compared as if it were. The original implementation and generated synthetic records are offered under this repository's MIT license; upstream GPL source is not vendored.

`python -m ml.data.generate` generates one day at a time. Customer amount/rate/location profiles determine legitimate activity; terminals near customer locations provide merchant choices. Rules inject high-amount, transient terminal-compromise and customer amount-multiplication scenarios. Customer/terminal identities and daily compromise choices are synthetic. No unavailable demographic, merchant-category, device or geolocation feature is invented in the serving vector. Currency USD and PAYMENT are schema conventions, not evidence about source currency.

## Executed seed-42 dataset

- 156,014 transactions; 738 labeled fraud (0.473034% prevalence).
- 1,200 customers; 600 terminals mapped to merchant IDs.
- 60 simulated days, 2025-01-01 00:07:59 UTC through 2025-03-01 23:53:25 UTC.
- Dataset version: `handbook-inspired-v1-af87b7f48a1a`.
- Full event SHA-256: `af87b7f48a1a86b41e29c0674fb1eb58d8a6a8b43a6bf86b965390dd410fd0a4`.
- `events.jsonl` contains no ground-truth field. `labels.jsonl` joins by stable transaction ID and declares `available_at = event time + 7 days`.
- No missing input fields are generated. Empty historical state maps to documented zeros/sentinel recency; production malformed records are rejected rather than imputed silently.

Raw data is kept under `ml/data/raw`; processed features are a disk-backed NumPy array under `ml/data/processed`. Generation bounds memory by daily output, feature calculation by retained entity windows. Training materializes split arrays, so it still needs memory proportional to the selected training set. Configurable customer/day sizes allow larger tests; memory use is not guaranteed constant during model fitting.

Fraud labels indicate a simulator rule, not adjudicated criminal activity. Simple amount rules can be easy to learn; terminal compromise can be largely unpredictable without labels. Prevalence, entity activity and label-delay assumptions do not establish real-world generalization. Models omit entity IDs to avoid memorizing synthetic customer identities. Labels are never behavioral features. No Kaggle credential is required.
