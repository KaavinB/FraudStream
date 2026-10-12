# Executed machine-learning evaluation

Source: `ml/artifacts/model-info.json`; raw frozen held-out labels/scores: `ml/artifacts/heldout-predictions.npz`; per-candidate timing samples: `ml/artifacts/inference-samples.json`.

The seed-42 dataset contains 156,014 synthetic events. The chronological cutoffs are 2025-02-05 23:59:14.6 UTC (fit) and 2025-02-17 23:56:19.8 UTC (selection). Seven-day label availability purges 36,359 rows from fitting/validation. Final counts: training 75,428; available validation 12,856; held-out test 31,371, including 160 fraud events. The validation period is split again by timestamp into separate calibration and threshold/model-selection segments. No tied timestamp crosses that segment boundary. Test labels are evaluated as of 2025-03-08 23:53:25 UTC.

| Model | Test AP | Test ROC-AUC | Recall | Precision | Actual test FPR | Brier |
|---|---:|---:|---:|---:|---:|---:|
| Logistic regression | 0.334048 | 0.785614 | 49.375% | 23.0321% | 0.845856% | 0.00416062 |
| Histogram gradient boosting | 0.472011 | 0.816418 | 50.625% | 22.8814% | 0.874692% | 0.00306603 |
| XGBoost 3.4.1 | 0.419644 | 0.796501 | 52.500% | 19.1781% | 1.134216% | 0.00353254 |

Histogram gradient boosting was selected by **validation AP** (0.489810 vs XGBoost 0.404731 and logistic regression 0.276599), before test predictions. All three models use class imbalance handling during training and Platt calibration on later naturally imbalanced validation samples. Threshold optimization targets no more than 1% empirical FPR on the later validation segment. The frozen test threshold's actual FPR is reported above; it is not forced to 1%.

For the selected model, the held-out confusion matrix and full calibration/precision–recall data are in the report. Automatically generated plots are in `benchmarks/results/model-comparison.png`. The model-info endpoint and dashboard expose these actual results, including skipped candidates.

`make train` trains all three candidates and logs parameters, metrics and artifacts to MLflow (local `mlflow.db`; run `99b7eb97d6d44ae2ab96091fec254236`). XGBoost exceeded the FPR budget on the test set (1.13% vs a 1% validation target), illustrating that a validation-frozen threshold does not guarantee test FPR. Local joblib artifacts are versioned by model name, dataset fingerprint and feature version; that is not equivalent to a validated MLflow model-registry deployment.

## Reproduce

```sh
make setup
make train
make test-python
make benchmark-local
```

`make train-local` permits a missing XGBoost install and skips MLflow; it is a limited fallback.

Decision policy is configured at model-build time with `--target-fpr` (default 0.01) and `--block-threshold` (default 0.9). The review threshold is validation-selected; BLOCK is never set below REVIEW. Effective thresholds are frozen inside the served artifact and exposed by `/model/info`. A changed policy requires rebuilding and evaluating its operating point.

For label availability analysis:

```sh
.venv/bin/python -m ml.evaluation.delayed --as-of 2025-02-25T00:00:00+00:00
```

Inference measurements distinguish estimator/calibration compute, FastAPI TestClient overhead and actual worker HTTP transport. All three are measured: the worker's `inference_http` stage averages ~1.3–1.4 ms per transaction in the Docker pipeline. Batch benchmarks exclude queueing and do not imply batching helps a single-event path. All predictions are from the trained model, not a scoring stub. Synthetic fraud quality does not establish bank fraud detection performance.
