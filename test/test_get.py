import pytest
from api import get_app, generate_jobdatabase_engine
import json

API = '/api/job'
CREATE_API = '/api/job/create'

# 예시
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
# 예시의 Job_ID
job_id = 0


@pytest.fixture
def api():
    global job_id
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


def test_success_to_get(api):
    # job_id을 추가하면 예상 답안이 된다.
    answer = example_job.copy()
    answer['job_id'] = job_id

    res = api.get(f'{API}/{job_id}')

    assert res.status_code == 200
    assert res.get_json() == answer


def test_failed_to_get(api):
    """
    없는 Job_ID 면 찾기 불가
    """
    assert api.get(f'{API}/99999').status_code == 404


def test_binary_search(api):
    """
    추가 테스트
    검색 알고리즘으로 이분탐색을 선택했으므로
    이분 탐색의 정확성에 대해 테스트
    데이터 SIZE-1개를 추가하여 SIZE개를 만든 다음 테스트
    """

    SIZE = 30
    for i in range(SIZE - 1):
        res = api.post(
            CREATE_API, data=json.dumps(example_job),
            content_type='application/json'
        )
        assert res.status_code == 201

    # 1부터 SIZE까지 탐색에 성공해야 한다
    for i in range(1, SIZE + 1):
        assert api.get(f'{API}/{i}').status_code == 200