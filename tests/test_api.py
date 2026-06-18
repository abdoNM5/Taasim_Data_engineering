import json
import os

from fastapi.testclient import TestClient

from src.api.core.config import AppConfig
from src.api.main import create_app

os.environ["APP_ENV"] = "test"

client = TestClient(create_app(AppConfig(environment="test")))


def test_read_root() -> None:
    response = client.get("/")
    assert response.status_code == 200
    assert response.json()["message"].startswith("Welcome to TaaSim API")


def test_invalid_forecast_request() -> None:
    response = client.post(
        "/api/demand/forecast",
        headers={"Content-Type": "application/json"},
        data=json.dumps({"zone_id": 1, "datetime": "invalid"}),
    )
    assert response.status_code == 400


def test_trip_status_not_found() -> None:
    response = client.get("/trip_status/not-exists")
    assert response.status_code == 404
