from typing import List, Dict, Any

from utils.algorithms.job_searcher import search_job_by_binary_search


def search_job_by_job_id(storage: List[Dict[str, Any]], job_id: int):
    return search_job_by_binary_search(storage, job_id)