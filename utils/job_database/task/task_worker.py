import collections
from typing import Any, Deque, Dict
from utils.algorithms.topological_sort import topological_sort

from utils.job_database.task.task_space import TaskDropColumnSpace, TaskReadSpace, TaskSpace, TaskWriteSpace


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
                self.task_dictionary[next_task_name]    \
                    .input_dataframe(task_name, result_dataframe)
        

        for k, v, in self.task_dictionary.items():
            print(k)
            for s in v.tasklog_stack:
                print(s)
            print()
        print()
