from libs.resource_access import lock_while_using_file
from utils.algorithms import topological_sort
from utils.job_database.default_algorithm import search_job_by_job_id
from utils.job_database.io import JobDatabaseRead, JobDatabaseWrite
from threading import Lock
from typing import Dict, Any, Optional
import json
import collections
import os
import pandas as pd

from libs.validator import ValidatorChain
from utils.job_database.task import task_read, task_write, task_drop_column
from utils.validator_chains import get_job_validator_chain


class JobDatabaseEngine:
    """
    Job.json을 관리하는 일종의 데이터베이스 엔진
    """

    """
    파일 접근은 한번에 하나의 클라이언트가 들어간다.
    파이썬에서는 동시에 접근할 경우 Error가 발생하기 때문에
    파일 접근을 시도할 때 먼저 Lock을 걸어둔다.
    """
    mutex: Lock

    """
    Job 데이터 상태가 유효한지를 파악하기 위한 Validator
    """
    validator: ValidatorChain

    def __new__(cls):
        """
        많은 트래픽으로 인한 Instance 남발을 줄이기 위해
        Singletone Pattern을 적용하여 하나의 인스턴스만 실행한다.
        """
        if not hasattr(cls, 'jobdatabase_instance'):
            cls.jobdatabase_instance = \
                super(JobDatabaseEngine, cls).__new__(cls)
        return cls.jobdatabase_instance

    def __init__(self):
        self.mutex = Lock()
        self.validator = get_job_validator_chain()

    def reset(self):
        """
        Job.json 초기화, storage 초기화
        테스트 할 때만 사용
        """
        with JobDatabaseWrite() as w:
            json.dump({'jobs': []}, w, indent=4)

        # 모든 csv 파일을 삭제하고
        BASE_DIR = 'storage/data'
        for f in os.scandir(BASE_DIR):
            os.remove(f.path)

        # a.csv 초기화
        df = pd.DataFrame({
            'col0': ['data00', 'data01'],
            'col1': ['data10', 'data11']
        })
        df.to_csv(f'{BASE_DIR}/a.csv', index=False)

    def save(self, job: Dict[str, Any]) \
        -> int:
        """
        새로운 Job을 json에 저장

        :param job: 추가하고자 하는 데이터

        :return: 새로 생성된 Job ID

        :exception ValieError: 추가하려는 데이터가 잘못된 경우
        """

        @lock_while_using_file(self.mutex)
        def __save() -> int:
            """
            실제 파일에 저장
            Mutex Decorator를 적용하기 위해 파일 접근하는 함수를 따로 구현함

            :return: 생성된 Job의 고유 아이디
            """
            with JobDatabaseRead() as r:
                # json에 있는 Data Load
                storage = json.load(r)
                # job id 생성
                # 맨 마지막 job의 id에 1를 추가하는 방식
                new_job_id = 1 if len(storage['jobs']) == 0 else \
                    storage['jobs'][-1]['job_id'] + 1
            # job_id를 job에 추가 및 storage에 추가
            job['job_id'] = new_job_id
            storage['jobs'].append(job)
            # 파일에 작성
            with JobDatabaseWrite() as w:
                json.dump(storage, w, indent=4)
            # job_id 리턴
            return new_job_id

        # Validate 판정
        is_valid, err = self.validator(job)
        if err:
            raise err
        try:
            job_id = __save()
        except Exception as e:
            raise e
        else:
            return job_id

    def update(self, job_id: int, updated_data: Dict[str, Any]) \
            -> bool:
        """
        해당 JOB_ID 에 대한 정보를 변경한다.

        :param job_id: 변경하고자 하는 데이터의 고유 ID
        :param updated_data: 변경 내용
        :return: (성공 여부, 에러 내용(없으면  None))
        """

        @lock_while_using_file(self.mutex)
        def __update():
            with JobDatabaseRead() as r:
                all_data = json.load(r)
                storage = all_data['jobs']
            # search data
            is_exists, idx = search_job_by_job_id(storage, job_id)
            if not is_exists:
                raise ValueError('Data Not Found')
            # validate data
            is_valid, err = self.validator(updated_data)
            if not is_valid:
                return False
            # update
            updated_data['job_id'] = job_id
            all_data['jobs'][idx] = updated_data
            # save
            with JobDatabaseWrite() as w:
                json.dump(all_data, w, indent=4)
            return True

        return __update()

    def get_item(self, job_id: int) -> Dict[str, Any]:
        """
        job_id에 대한 Job Data 얻기

        :param job_id: 찾고자 하는 Job의 ID

        :return:  job id에 데한 정보

        :exception ValueError: 찾고자 하는 데이터가 없음
        :exception Exception: 주로 job.json파일이 없어서 발생하는 에러
        """

        @lock_while_using_file(self.mutex)
        def __get_item() -> Optional[Dict[str, Any]]:
            """
            파일에 직접 접근하여 데이터 구하기
            :return: Job_ID에 대한 정보, 못찾으면 None Return
            """
            with JobDatabaseRead() as r:
                storage = json.load(r)['jobs']
                # idx -> job_id의 데이터가 위치해 있는 인덱스 값
                is_exists, idx = search_job_by_job_id(storage, job_id)
            return storage[idx] if is_exists else None

        try:
            # 파일에 접근해서 데이터 찾기
            res = __get_item()
        except Exception as e:
            # 파일 상의 에러 발생
            raise e
        else:
            if not res:
                raise ValueError(f'Failed to find id: {job_id}')
            else:
                return res

    def remove(self, job_id: int) \
            -> bool:
        """
        job_id 데이터 삭제

        :param job_id: 삭제 할 데이터의 ID
        :return: 
        """

        @lock_while_using_file(self.mutex)
        def __remove() -> bool:
            # Json에서 데이터 가져오기
            with JobDatabaseRead() as r:
                all_data = json.load(r)
                storage = all_data['jobs']
            # 삭제할 데이터 검색
            is_exists, idx = search_job_by_job_id(storage, job_id)
            if not is_exists:
                return False
            # 데이터 삭제 및 파일 갱신
            del storage[idx]
            all_data['jobs'] = storage
            with JobDatabaseWrite() as w:
                json.dump(all_data, w, indent=4)
            return True

        try:
            success = __remove()
        except Exception as e:
            raise e
        return True if success else False

    def run(self, job_id: int):
        """
        해당 Task를 실행한다.
        :param job_id: 실행할 Task_ID 데이터
        """

        @lock_while_using_file(self.mutex)
        def __run(queue, buffer, graph, properties):
            while queue:
                task = queue.popleft()
                task_type = properties[task]['task_name']
                if task_type == 'read':
                    task_read(graph, buffer, task,
                              properties[task]['filename'], properties[task]['sep'])
                elif task_type == 'write':
                    task_write(graph, buffer, task,
                               properties[task]['filename'], properties[task]['sep'])
                elif task_type == 'drop':
                    task_drop_column(graph, buffer,
                                     task, properties[task]['column_name'])

        # 데이터 가져오기
        try:
            data = self.get_item(job_id)
        except ValueError as e:
            raise e
        graph, properties = data['task_list'], data['property']
        # DataFrame Buffer 생성하기
        data_frame_buffer = {k: collections.deque() for k in properties.keys()}

        # 실행 순서 만들기
        run_queue = collections.deque(topological_sort(graph))
        __run(run_queue, data_frame_buffer, graph, properties)
