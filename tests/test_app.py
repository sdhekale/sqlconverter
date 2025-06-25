import os
import sys
import json

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from app import app


def test_transpile_success():
    with app.test_client() as client:
        response = client.post('/transpile', json={'sql_query': 'SELECT 1'})
        assert response.status_code == 200
        data = response.get_json()
        assert 'transpiled_sql' in data


def test_transpile_missing_sql_query():
    with app.test_client() as client:
        response = client.post('/transpile', json={})
        assert response.status_code == 400
        data = response.get_json()
        assert 'error' in data

