from abc import ABCMeta, abstractmethod
import pandas as pd
import collections
from typing import List, Deque, Tuple
import os

from utils.job_database.task.task_algorithms import merge_dataframes
from utils.job_database.task.task_log import TaskDropColumnLog, TaskLog, TaskMergeLog, TaskReadLog, TaskWriteLog
BASE_DIR = 'storage/data'

class TaskSpace(metaclass=ABCMeta):
    """
    Task 실행 단위

    :param task_name: Task 이름
    :param tasklog_stack: 수행한 작업(병합, 기타 작업 등..)의 내용을 보관(Rollback에 사용)
    :param dataframe_buffer: 이전 Task의 결과 dataframe을 모아서 Task실행 때 한꺼번에 병합하는 데 사용한다.
    """
    task_name: str
    tasklog_stack: List[TaskLog]
    dataframe_buffer: Deque[Tuple[str, pd.DataFrame]]
    
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
            prev_name, prev_dataframe = self.dataframe_buffer.pop()
            merged_dataframe, common_columns = merge_dataframes(merged_dataframe, prev_dataframe)

            # Write Log
            self.tasklog_stack.append(TaskMergeLog(common_columns, prev_name))

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
            dataframe, common_columns = merge_dataframes(dataframe, new_data)
            # Log Task
            self.tasklog_stack.append(TaskReadLog(self.filename, self.sep, common_columns))
        except Exception:
            pass
        return dataframe

    def rollback(self):
        raise NotImplemented()

class TaskWriteSpace(TaskSpace):
    """
    Write Task

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
        target_root = f'{BASE_DIR}/{self.filename}'

        if os.path.exists(target_root):
            # 존재하는 경우
            # TODO 에러 발생
            pass
            
        dataframe.to_csv(f'{BASE_DIR}/{self.filename}', 
            sep=self.sep, index=False, index_label=False)
        self.tasklog_stack.append(TaskWriteLog(self.filename, self.sep))
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
            # 삭제될 Column의 데이터 추출
            removed_column_data = list(dataframe[self.column_name].values)
            dataframe = dataframe.drop([self.column_name], axis=1)
            self.tasklog_stack.append(TaskDropColumnLog(self.column_name, removed_column_data))
        except Exception:
            pass
        return dataframe

    def rollback(self):
        raise NotImplemented()