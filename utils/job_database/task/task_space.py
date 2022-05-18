from abc import ABCMeta, abstractmethod
import pandas as pd
import collections
from typing import List, Dict, Deque, Tuple, Any

from utils.job_database.task.task_algorithms import merge_dataframes
BASE_DIR = 'storage/data'

class TaskSpace(metaclass=ABCMeta):
    """
    Task 실행 단위 클래스

    :param task_name: Task 이름
    :param tasklog_stack: 수행한 작업(병합, 기타 작업 등..)의 내용을 보관(Rollback에 사용)
    :param dataframe_buffer: 이전 Task의 결과 dataframe을 모아서 Task실행 때 한꺼번에 병합하는 데 사용한다.
    """
    task_name: str
    tasklog_stack: List[Tuple[str, Any]]
    dataframe_buffer: Deque[Tuple[str, str, pd.DataFrame]]
    
    def __init__(self, task_name: str):
        self.task_name = task_name
        self.tasklog_stack = []
        self.dataframe_buffer = collections.deque()

    def __str__(self):
        return self.task_name

    def __write_log(self, task_type: str, prev_task_name: str, log: Any):
        self.tasklog_stack.append((task_type, prev_task_name, log))

    def merge_dataframes_in_buffer(self):
        """
        dataframe_buffer에 들어있는 모든 dataframe을 병합한다.
        """
        merged_dataframe = pd.DataFrame()
        while self.dataframe_buffer:
            prev_name, prev_buffer = self.dataframe_buffer.pop()
            merged_dataframe, common_columns = merge_dataframes(merged_dataframe, prev_buffer)
        return merged_dataframe

    def input_dataframe(self, task_name: str, dataframe: pd.DataFrame):
        """
        dataframe_buffer에 dataframe을 push할 때 사용
        """
        self.dataframe_buffer.appendleft(
            (task_name, pd.DataFrame.copy(dataframe))
        )
    
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