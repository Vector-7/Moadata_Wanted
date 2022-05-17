import pytest
from api import get_app, generate_jobdatabase_engine
import json

API = '/api/job'
CREATE_API = '/api/job/create'

example_job = {
    'job_name': 'Job1',
    'task_list': {
        'R1': ['R2', 'W1'],
        'R2': ['W1', 'W2'],
        'W1': ['W2'],
        'W2': [],
    },
    'property': {
        'R1': {'task_name': 'read', 'filename': 'a.csv', 'sep': ','},
        'R2': {'task_name': 'read', 'filename': '1.csv', 'sep': ','},
        'W1': {'task_name': 'write', 'filename': 'a.csv', 'sep': ','},
        'W2': {'task_name': 'write', 'filename': 'b.csv', 'sep': ','},
    }
}


@pytest.fixture
def api():
    app, api = get_app()

    # 데이터 3개 올리기
    for i in range(3):
        res = app.test_client().post(
            CREATE_API, data=json.dumps(example_job),
            content_type='application/json'
        )
        assert res.status_code == 201

    yield app.test_client()

    # 테스트 종료 후에 실행되는 코드
    # Job.json에 있는 내용들을 전부 지운다.
    generate_jobdatabase_engine().reset()


def test_remove_success(api):
    assert api.delete(f'{API}/1').status_code == 204
    assert api.delete(f'{API}/2').status_code == 204
    assert api.delete(f'{API}/3').status_code == 204
    # 2번 job이 데이터아 남아있으면 안된다.
    assert api.get(f'{API}/1').status_code == 404
    assert api.get(f'{API}/2').status_code == 404
    assert api.get(f'{API}/3').status_code == 404


def test_not_found(api):
    assert api.delete(f'{API}/9999').status_code == 404
