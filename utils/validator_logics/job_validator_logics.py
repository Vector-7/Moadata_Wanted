from typing import Dict, List
from utils.algorithms import topological_sort


def validate_job_list(graph: Dict[str, List[str]])  \
        -> bool:
    """
    job_list 의 유효성을 판단하는 함수
    :param graph:
    :return:
    """
    topological_sort(graph)
    return True


def validate_job_properties(job_names: List[str],
                            properties: Dict[str, Dict[str, str]])  \
        -> bool:
    """
    job property의 유효성을 판단하는 함수
    :param job_names:
    :param properties:
    :return:
    """

    def __check_property(p: Dict[str, str], needs: Dict[str, str])  \
            -> bool:

        # task_name이 반드시 property 안에 들어가야 한다.
        if 'task_name' not in p:
            return False
        # task_name은 read/write/drop 중 하나여야만 한다
        if p['task_name'] not in set(needs.keys()):
            return False
        # task_name 갖고오기
        task_name = p['task_name']

        """
        read/write: filename, sep이 있어야 한다
        drop: column_name이 있어야 한다.
        """

        if (needs[task_name] | {'task_name'}) != set(p.keys()):
            return False
        return True

    needs = {
        'read': {'filename', 'sep'},
        'write': {'filename', 'sep'},
        'drop': {'column_name'},
    }

    # jobs_names의 내용과 properties key의 데이터가 정확히 일치해야 한다
    if set(job_names) != set(properties):
        raise ValueError("jobs and properties does not equal")

    for p in properties.values():
        if not __check_property(p, needs):
            return False
    return True
