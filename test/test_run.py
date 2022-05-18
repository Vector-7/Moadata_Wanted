import json
from typing import List, Tuple, Dict, Any

import pytest
from api import get_app, generate_jobdatabase_engine
import pandas as pd

CREATE_API = '/api/jobs'
RUN_API = '/api/jobs'
STORAGE_ROOT = 'storage/data'

@pytest.fixture
def api():
    app, api = get_app()
    yield app.test_client()

    # 테스트 종류 후 실행되는 코드
    # Job.json에 있는 내용들을 전부 지운다
    generate_jobdatabase_engine().reset()


def save_files(files: List[Tuple[str, pd.DataFrame]]):
    for i in range(len(files)):
        name, data = files[i]
        data.to_csv(f'{STORAGE_ROOT}/{name}', index=False, index_label=False)


def upload_job(job: Dict[str, Any], api):
    res = api.post(CREATE_API, data=json.dumps(job),
                   content_type='application/json')
    assert res.status_code == 201


def test_on_way_root(api):
    """
    READ -> DROP -> WRITE 순으로
    이때 결과값이 반드시 일치하게 나와야 한다
    """

    # Data Setting
    input_file = 'b.scv'
    output_file = 'c.csv'

    input_data = pd.DataFrame({
        'id': [1, 2, 3],
        'title': ['title1', 'title2', 'title3']
    })
    save_files([(input_file, input_data)])
    deleted_column = 'title'

    # Job Setting
    job = {
        'job_name': 'Job1',
        'task_list': {
            'R': ['D'],
            'D': ['W'],
            'W': [],
        },
        'property': {
            'R': {'task_name': 'read', 'filename': input_file, 'sep': ','},
            'D': {'task_name': 'drop', 'column_name': deleted_column},
            'W': {'task_name': 'write', 'filename': output_file, 'sep': ','}
        }
    }
    upload_job(job, api)

    # run
    res = api.get(f'{RUN_API}/1/run')
    assert res.status_code == 200

    output = pd.read_csv(f'{STORAGE_ROOT}/{output_file}')
    answer = input_data.copy().drop([deleted_column], axis=1)
    assert output.equals(answer) is True


def test_multiple_edges(api):
    """
    Task가 복잡하게 얽혀있는 경우
    """
    file_b = ('b.csv', pd.DataFrame({'b': ['b1', 'b2', 'b3']}))
    file_c = ('c.csv', pd.DataFrame({'c': ['c1', 'c2', 'c3']}))
    file_d = ('d.csv', pd.DataFrame({'d': ['d1', 'd2', 'd3']}))
    file_e = ('e.csv', pd.DataFrame({'e': ['e1', 'e2', 'e3']}))

    # 해당 csv파일에는 아래와 같은 컬럼이 존재해야 한다.
    writed_w1 = 'w1.csv', ['b', 'c']  # b, c가 있어야 함
    writed_w2 = 'w2.csv', ['b', 'c', 'd', 'e']  # b, c, d, e 다 있어야 함

    job = {
        'job_name': 'Job1',
        'task_list': {
            'R1': ['R2', 'W1'],
            'R2': ['W1', 'R3'],
            'R3': ['W2'],
            'R4': ['R3'],
            'W1': ['R4', 'R3'],
            'W2': [],
        },
        'property': {
            'R1': {'task_name': 'read', 'filename': file_b[0], 'sep': ','},
            'R2': {'task_name': 'read', 'filename': file_c[0], 'sep': ','},
            'R3': {'task_name': 'read', 'filename': file_d[0], 'sep': ','},
            'R4': {'task_name': 'read', 'filename': file_e[0], 'sep': ','},
            'W1': {'task_name': 'write', 'filename': writed_w1[0], 'sep': ','},
            'W2': {'task_name': 'write', 'filename': writed_w2[0], 'sep': ','},
        }
    }
    save_files([file_b, file_c, file_d, file_e])
    upload_job(job, api)

    res = api.get(f'{RUN_API}/1/run')
    assert res.status_code == 200

    # 파일 부검
    # w1
    output = pd.read_csv(f'{STORAGE_ROOT}/{writed_w1[0]}')
    assert set(output.columns.values.tolist()) == set(writed_w1[1])
    # w2
    output = pd.read_csv(f'{STORAGE_ROOT}/{writed_w2[0]}')
    assert set(output.columns.values.tolist()) == set(writed_w2[1])


def test_with_remove_column(api):
    """
    실전: 사용자 데이터 정리

    아래의 세 개의 사용자 관련 정보를 입력받아

    사용자 프로파일(이름, 주소, 생일)과
    SNS 정보(이름, 주소, SNS종류, SNS 로그인 이메일)
    총 두 개로 저장한다.
    """

    file_user = ('user.csv', pd.DataFrame({
        'name': ['my name1', 'my name2', 'my name3'],
    }))
    file_profile = ('profile.csv', pd.DataFrame({
        'name': ['my name1', 'my name2', 'my name3'],
        'address': ['my address1', 'my address2', 'my address3'],
        'birthday': ['08/12', '', '09/13']
    }))
    file_sns = ('sns.csv', pd.DataFrame({
        'name': ['my name1', 'my name2', 'my name3'],
        'sns': ['facebook', 'instagram', ''],
        'sns-email': ['email1@gmail.com', 'email@gmail.com', ''],
        'password': ['password1', 'password2', ''],
    }))

    # 해당 csv파일에는 아래와 같은 컬럼이 존재해야 한다.
    writed_profile = 'result_profile.csv', {'name', 'address', 'birthday'}
    writed_sns = 'result_sns.csv', {'name', 'birthday', 'sns', 'sns-email'}

    job = {
        'job_name': 'Merge User Data',
        'task_list': {
            'read-user': ['read-profile', 'read-sns'],
            'read-profile': ['write-profile'],
            'read-sns': ['drop-password'],
            'write-profile': ['drop-address'],
            'drop-address': ['write-sns-profile'],
            'drop-password': ['write-sns-profile'],
            'write-sns-profile': []
        },
        'property': {
            'read-user': {'task_name': 'read', 'filename': file_user[0], 'sep': ','},
            'read-profile': {'task_name': 'read', 'filename': file_profile[0], 'sep': ','},
            'read-sns': {'task_name': 'read', 'filename': file_sns[0], 'sep': ','},
            'drop-password': {'task_name': 'drop', 'column_name': 'password'},
            'drop-address': {'task_name': 'drop', 'column_name': 'address'},
            'write-profile': {'task_name': 'write', 'filename': writed_profile[0], 'sep': ','},
            'write-sns-profile': {'task_name': 'write', 'filename': writed_sns[0], 'sep': ','},
        }
    }

    # 저장
    save_files([file_user, file_profile, file_sns])
    upload_job(job, api)

    # 실행
    res = api.get(f'{RUN_API}/1/run')
    assert res.status_code == 200

    # 부검
    output = pd.read_csv(f'{STORAGE_ROOT}/{writed_profile[0]}')
    assert set(output.columns.values.tolist()) == set(writed_profile[1])

    output = pd.read_csv(f'{STORAGE_ROOT}/{writed_sns[0]}')
    assert set(output.columns.values.tolist()) == set(writed_sns[1])