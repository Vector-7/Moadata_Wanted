from abc import ABCMeta, abstractmethod
import pandas as pd
from typing import Deque, Dict, List, Any, List
import collections

from utils.algorithms.topological_sort import topological_sort

BASE_DIR = 'storage/data'

def merge_dataframes(left_frame: pd.DataFrame, right_frame: pd.DataFrame):
    """
    두 개의 데이터 프레임을 병합하는 단일 함수

    겹치는 Column이 없으면 그냥 이어붙이고
    하나라도 있는 경우 해당 Column을 중심으로 병합한다.
    """
    left_cols, right_cols = \
        set(left_frame.columns.values), set(right_frame.columns.values)
    
    # 겹치는 colum 확인
    common_cols = left_cols & right_cols
    if common_cols:
        # 동일한 column이 존재하는 경우 동일 column대로 병합
        return pd.merge(left_frame, right_frame, how='outer', on=list(common_cols)), list(common_cols)
    # 없는 경우 그냥 column을 합친다.
    return pd.concat([left_frame, right_frame], axis=1), []

class TaskSpace(metaclass=ABCMeta):
    """
    Task 실행 단위 클래스

    :param task_name: Task 이름
    :param tasklog_stack: 수행한 작업(병합, 기타 작업 등..)의 내용을 보관(Rollback에 사용)
    :param dataframe_buffer: 이전 Task의 결과 dataframe을 모아서 Task실행 때 한꺼번에 병합하는 데 사용한다.
    """
    task_name: str
    tasklog_stack: List[Dict[str, Any]]
    dataframe_buffer: Deque[pd.DataFrame]
    
    def __init__(self, task_name: str):
        self.task_name = task_name
        self.tasklog_stack = []
        self.dataframe_buffer = collections.deque()

    def __str__(self):
        return self.task_name

    def merge_dataframes_in_buffer(self):
        """
        dataframe_buffer에 들어있는 모든 dataframe을 병합한다.
        """
        merged_dataframe = pd.DataFrame()
        while self.dataframe_buffer:
            merged_dataframe, _ = merge_dataframes(
                merged_dataframe, self.dataframe_buffer.pop())
        return merged_dataframe

    def input_dataframe(self, dataframe: pd.DataFrame):
        """
        dataframe_buffer에 dataframe을 push할 때 사용
        """
        self.dataframe_buffer.appendleft(pd.DataFrame.copy(dataframe))
    
    @abstractmethod
    def run(self):
        """
        단일 Task 실행
        """
        pass

    @abstractmethod
    def rollback(self):
        """
        단일 Task 복구
        """
        pass

class TaskReadSpace(TaskSpace):
    """
    Read Task

    :params filename: 읽기 대상의 filename
    :params sep: 읽기 대상의 구분자
    """
    filename: str
    sep: str
    def __init__(self, task_name: str, filename: str, sep: str):
        super().__init__(task_name)
        self.filename = filename
        self.sep = sep

    def run(self):
        dataframe = self.merge_dataframes_in_buffer()
        try:
            new_data = pd.read_csv(f'{BASE_DIR}/{self.filename}', sep=self.sep)
            dataframe, _ = merge_dataframes(dataframe, new_data)
        except Exception:
            pass
        return dataframe

    def rollback(self):
        raise NotImplemented()

class TaskWriteSpace(TaskSpace):
    """
    Read Task

    :params filename: 읽기 대상의 filename
    :params sep: 읽기 대상의 구분자
    """
    filename: str
    sep: str
    def __init__(self, task_name: str, filename: str, sep: str):
        super().__init__(task_name)
        self.filename = filename
        self.sep = sep

    def run(self):
        dataframe = self.merge_dataframes_in_buffer()
        dataframe.to_csv(f'{BASE_DIR}/{self.filename}', 
            sep=self.sep, index=False, index_label=False)
        return dataframe

    def rollback(self):
        raise NotImplemented()

class TaskDropColumnSpace(TaskSpace):
    """
    Column drop Task

    :params column_name: 삭제 대상의 Column
    """
    column_name: str
    def __init__(self, task_name: str, column_name: str):
        super().__init__(task_name)
        self.column_name = column_name

    def run(self):
        dataframe = self.merge_dataframes_in_buffer()
        try:
            dataframe = dataframe.drop([self.column_name], axis=1)
        except Exception:
            pass
        return dataframe

    def rollback(self):
        raise NotImplemented()

class TaskWorker:
    """
    Job Data에 있는 Task Data를 활용해
    모든 Task를 실행

    :params task_dictionary: task_name으로 TaskSpace를 찾는다.
    :params graph: task_list
    """
    task_dictionary: Dict[str, TaskSpace]
    graph: Dict[str, str]

    def __init__(self, job_data: Dict[str, Any]):
        """
        그래프 및 데이터 세팅
        """
        self.task_dictionary = dict()

        # 데이터 가져오기
        self.graph, properties = \
            job_data['task_list'], job_data['property']

        # TaskSpace 세팅
        for task_name, v in properties.items():
            task_type = v['task_name']
            task_space = None
            if task_type == 'read':
                task_space = TaskReadSpace(task_name, v['filename'], v['sep'])
            elif task_type == 'write':
                task_space = TaskWriteSpace(task_name, v['filename'], v['sep'])
            elif task_type == 'drop':
                task_space = TaskDropColumnSpace(task_name, v['column_name'])

            self.task_dictionary[task_name] = task_space

    def __call__(self):
        """
        Task 실행
        """
        task_queue: Deque[str] = collections.deque(topological_sort(self.graph))
        # Run
        while task_queue:
            task_name = task_queue.popleft()
            result_dataframe = self.task_dictionary[task_name].run()
            # 다른 TaskSpace에 결과 데이터 뿌리기
            for next_task_name in self.graph[task_name]:
                self.task_dictionary[next_task_name].input_dataframe(result_dataframe)