import pytest
from api import get_app, generate_jobdatabase_engine
import json

CREATE_API = '/api/jobs'
RUN_API = '/api/jobs'
STORAGE_ROOT = 'storage/data'

@pytest.fixture
def api():
    app, api = get_app()

    yield app.test_client()

def test_rollaback(api):
    pass