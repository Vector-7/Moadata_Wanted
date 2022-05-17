import json
import pytest

from api import get_app, generate_jobdatabase_engine

CREATE_API = '/api/job/create'
API = '/api/job'

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

    # 데이터 하나 올리기
    res = app.test_client().post(
        CREATE_API, data=json.dumps(example_job),
        content_type='application/json'
    )
    assert res.status_code == 201

    job_id = res.get_json()['job_id']

    yield app.test_client()

    # 테스트 종료 후에 실행되는 코드
    # Job.json에 있는 내용들을 전부 지운다.
    generate_jobdatabase_engine().reset()


def test_success_modified(api):
    modified_data = example_job.copy()
    modified_data['property']['R1']['filename'] = 'z.csv'
    assert api.patch(f'{API}/1', data=json.dumps(modified_data),
                     content_type='application/json').status_code == 201


def test_not_found(api):
    modified_data = example_job.copy()
    assert api.patch(f'{API}/9999', data=json.dumps(modified_data),
                     content_type='application/json').status_code == 404


def test_modified_data_validate_failed(api):
    modified_data = example_job.copy()
    modified_data['property']['ZA'] = {'task_name': 'aaaa'}
    assert api.patch(f'{API}/1', data=json.dumps(modified_data),
                     content_type='application/json').status_code == 400
