from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from services.inference.app.main import app


@pytest.fixture
def client():
    if not Path("ml/artifacts/model.joblib").exists():
        pytest.skip("train model first")
    with TestClient(app) as value:
        yield value


def request():
    return {
        "transaction_id": "test",
        "feature_version": "prior-v1",
        "features": [10, 12, 2] + [0] * 12,
    }


def test_actual_loaded_model(client):
    assert client.get("/ready").status_code == 200
    response = client.post("/predict", json=request())
    assert response.status_code == 200
    score = response.json()
    assert 0 <= score["risk_score"] <= 1
    assert score["risk_band"] in {"ALLOW", "REVIEW", "BLOCK"}
    assert score["model_version"] == client.get("/model/info").json()["model_version"]
    batch = client.post("/predict/batch", json=[request(), request()])
    assert batch.status_code == 200
    assert batch.json()[0]["risk_score"] == pytest.approx(score["risk_score"])
    assert "fraudstream_inference_seconds" in client.get("/metrics").text


def test_reject_labels_and_invalid_feature_schema(client):
    assert client.post("/predict", json={**request(), "fraud": 1}).status_code == 422
    assert client.post("/predict", json={**request(), "features": [1]}).status_code == 422
    assert (
        client.post("/predict", json={**request(), "feature_version": "future"}).status_code == 422
    )
    assert client.post("/predict/batch", json=[]).status_code == 422


def test_model_load_failure(monkeypatch):
    monkeypatch.setenv("MODEL_PATH", "/nonexistent/fraudstream.joblib")
    with pytest.raises(FileNotFoundError), TestClient(app):
        pass
