from threading import Lock
from typing import Dict, Any, Optional
import json
import collections
import os
import pandas as pd

from libs.validator import ValidatorChain
from libs.resource_access import lock_while_using_file
from utils.job_database.io import JobDatabaseRead, JobDatabaseWrite
from utils.job_database.task import task_read, task_write, task_drop_column
from utils.validator_chains import get_job_validator_chain
from utils.algorithms import topological_sort
from utils.job_database.default_algorithm import search_job_by_job_id


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

    def __read_from_database(self) -> Dict[str, Any]:
        """
        Json으로부터 데이터 불러오기
        """
        raw_storage = None
        with JobDatabaseRead() as r:
            raw_storage = json.load(r)
        return raw_storage

    def __write_to_database(self, data: Dict[str, Any]):
        """
        Json File에 갱신하기
        """
        with JobDatabaseWrite() as w:
            json.dump(data, w, indent=4)

    def __init__(self):
        self.mutex = Lock()
        self.validator = get_job_validator_chain()

    def reset(self):
        """
        Job.json 초기화, storage 초기화
        테스트 할 때만 사용
        """
        self.__write_to_database({'jobs': []})

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
            storage = self.__read_from_database()
            # job id 생성
            # 맨 마지막 job의 id에 1를 추가하는 방식
            new_job_id = 1 if len(storage['jobs']) == 0 else \
                storage['jobs'][-1]['job_id'] + 1
            # job_id를 job에 추가 및 storage에 추가
            job['job_id'] = new_job_id
            storage['jobs'].append(job)
            # 파일에 작성
            self.__write_to_database(storage)
            # job_id 리턴
            return new_job_id

        # Validate 판정
        is_valid, err = self.validator(job)
        if err:
            raise err
        elif not is_valid:
            raise ValueError("Validate Failed")
        # 에러 발생 시 바로 보냄
        return __save()

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
            all_data = self.__read_from_database()
            storage = all_data['jobs']
            # search data
            is_exists, idx = search_job_by_job_id(storage, job_id)
            if not is_exists:
                raise ValueError('Data Not Found')
            # validate data
            is_valid, err = self.validator(updated_data)
            if not is_valid:
                return False
            if err:
                raise err
            # update
            updated_data['job_id'] = job_id
            all_data['jobs'][idx] = updated_data
            # save
            self.__write_to_database(all_data)
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
            storage = self.__read_from_database()['jobs']
            # idx -> job_id의 데이터가 위치해 있는 인덱스 값
            is_exists, idx = search_job_by_job_id(storage, job_id)
            return storage[idx] if is_exists else None

        # 파일에 접근해서 데이터 찾기
        # 에러 발생은 View에서처리
        res = __get_item()
        if not res:
            raise ValueError(f'Failed to find id: {job_id}')
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
            all_data = self.__read_from_database()
            storage = all_data['jobs']
            # 삭제할 데이터 검색
            is_exists, idx = search_job_by_job_id(storage, job_id)
            if not is_exists:
                return False
            # 데이터 삭제 및 파일 갱신
            del storage[idx]
            all_data['jobs'] = storage
            self.__write_to_database(all_data)
            return True

        # 에러는 view에서 처리
        success = __remove()
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
