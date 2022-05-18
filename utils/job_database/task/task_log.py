from abc import abstractmethod, ABCMeta
from typing import Iterable, List
import pandas as pd

class TaskLog(metaclass=ABCMeta):
    
    def recovery(self, dataframe: pd.DataFrame):
        """
        복구
        """
        print(f"{self} recovery")

class TaskMergeLog(TaskLog):
    common_columns: List[str]
    prev_task: str
    def __init__(self, common_columns, prev_task):
        self.common_columns = common_columns
        self.prev_task = prev_task

    def recovery(self, dataframe: pd.DataFrame):
        pass

    def __str__(self):
        return f"Merge with {self.prev_task} - {self.common_columns}"

class TaskReadLog(TaskLog):
    filename: str
    sep: str
    common_columns: List[str]
    def __init__(self, filename, sep, common_columns):
        self.filename = filename
        self.sep = sep
        self.common_columns = common_columns
    
    def recovery(self, dataframe: pd.DataFrame):
        pass

    def __str__(self):
        return f"Read from {self.filename} - {self.common_columns}"

class TaskWriteLog(TaskLog):
    filename: str
    sep: str
    def __init__(self, filename, sep):
        self.filename = filename
        self.sep = sep
    
    def recovery(self, dataframe: pd.DataFrame):
        pass

    def __str__(self):
        return f"Write from {self.filename} - {self.sep}"

class TaskDropColumnLog(TaskLog):
    removed_column: str
    column_data: List[str]
    def __init__(self, removed_column, column_data):
        self.removed_column = removed_column
        self.column_data = column_data

    def recovery(self, dataframe: pd.DataFrame):
        pass

    def __str__(self):
        return f"Remove Column : {self.removed_column} - {self.column_data}"