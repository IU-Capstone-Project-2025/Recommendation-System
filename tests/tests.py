import pytest
import requests

from src.config import TEST_BACKEND_LOGIN, TEST_BACKEND_PASSWORD

def test_backend_healthy():
    response = requests.get("http://backend:8000/api/healthchecker")
    assert response.status_code == 200
    assert response.json()["message"] == "Healthy"

def test_backend_auth():
    response = requests.post("http://backend:8000/api/signin", json={"username": TEST_BACKEND_LOGIN, "password": TEST_BACKEND_PASSWORD})
    assert response.status_code == 200

def test_airflow_healthy():
    response = requests.get("http://airflow:8080/health")
    assert response.status_code == 200


def test_postgres():
    from src.scripts.pg_connect import PgConnectionBuilder

    connection = PgConnectionBuilder.pg_conn()

    with connection.connection as conn:
        assert conn

