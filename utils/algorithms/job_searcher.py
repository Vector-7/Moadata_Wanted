from typing import List, Dict, Any, Optional


def search_job_by_binary_search(storage: List[Dict[str, Any]], job_id: int) \
        -> (bool, Optional[int]):
    """
    이분 탐색을 이용한 데이터 찾기
    :param storage:
    :param job_id:
    :return:
    """

    left, right = 0, len(storage) - 1

    while left <= right:
        m = (left + right) >> 1

        v = storage[m]['job_id']

        if v == job_id:
            return True, m
        elif v < job_id:
            left = m + 1
        else:
            right = m - 1

    return False, None
