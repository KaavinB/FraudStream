import os
import time
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated

import joblib
import numpy as np
from fastapi import FastAPI, HTTPException, Response
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest
from pydantic import BaseModel, ConfigDict, Field, field_validator

from ml.features.reference import FEATURE_NAMES, FEATURE_VERSION
from ml.training.train import predict

LATENCY = Histogram(
    "fraudstream_inference_seconds",
    "Model prediction compute duration",
    buckets=(0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1),
)
SCORES = Histogram(
    "fraudstream_risk_score", "Risk score distribution", buckets=tuple(np.linspace(0, 1, 11))
)
DECISIONS = Counter("fraudstream_inference_decisions", "Simulated decision bands", ["decision"])


class PredictionRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    transaction_id: Annotated[str, Field(pattern=r"^[A-Za-z0-9_-]{1,96}$")]
    feature_version: str = FEATURE_VERSION
    features: Annotated[
        list[float], Field(min_length=len(FEATURE_NAMES), max_length=len(FEATURE_NAMES))
    ]

    @field_validator("features")
    @classmethod
    def finite(cls, values: list[float]) -> list[float]:
        if not np.isfinite(values).all():
            raise ValueError("features must be finite")
        return values


class PredictionResponse(BaseModel):
    transaction_id: str
    risk_score: float
    risk_band: str
    model_version: str
    scoring_timestamp: str
    inference_latency_ms: float


@asynccontextmanager
async def lifespan(app: FastAPI):
    path = Path(os.environ.get("MODEL_PATH", "ml/artifacts/model.joblib"))
    # Artifacts are trusted local build products, never user-uploaded pickles.
    bundle = joblib.load(path)
    if (
        bundle["info"]["feature_names"] != FEATURE_NAMES
        or bundle["info"]["feature_version"] != FEATURE_VERSION
    ):
        raise RuntimeError("model feature schema mismatch")
    app.state.bundle = bundle
    yield
    app.state.bundle = None


app = FastAPI(title="FraudStream inference", lifespan=lifespan)


def score(requests: list[PredictionRequest]) -> list[PredictionResponse]:
    if getattr(app.state, "bundle", None) is None:
        raise HTTPException(503, "model unavailable")
    if any(r.feature_version != FEATURE_VERSION for r in requests):
        raise HTTPException(422, "feature version mismatch")
    began = time.perf_counter()
    bundle = app.state.bundle
    scores = predict(bundle, np.asarray([r.features for r in requests]))
    elapsed = time.perf_counter() - began
    LATENCY.observe(elapsed)
    info = bundle["info"]
    results = []
    for request, value in zip(requests, scores, strict=True):
        band = (
            "BLOCK"
            if value >= info["block_threshold"]
            else "REVIEW"
            if value >= info["review_threshold"]
            else "ALLOW"
        )
        SCORES.observe(float(value))
        DECISIONS.labels(band).inc()
        results.append(
            PredictionResponse(
                transaction_id=request.transaction_id,
                risk_score=float(value),
                risk_band=band,
                model_version=info["model_version"],
                scoring_timestamp=datetime.now(UTC).isoformat(),
                inference_latency_ms=elapsed * 1000,
            )
        )
    return results


@app.post("/predict", response_model=PredictionResponse)
def single(request: PredictionRequest):
    return score([request])[0]


@app.post("/predict/batch", response_model=list[PredictionResponse])
def batch(requests: Annotated[list[PredictionRequest], Field(min_length=1, max_length=256)]):
    return score(requests)


@app.get("/health")
def health():
    return {"status": "alive"}


@app.get("/ready")
def ready():
    if getattr(app.state, "bundle", None) is None:
        raise HTTPException(503, "model unavailable")
    return {"status": "ready"}


@app.get("/model/info")
def info():
    ready()
    return app.state.bundle["info"]


@app.get("/metrics")
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
