from typing import Dict, List
import collections


def __check_double_edge(g):
    """
    출발지에서 목적지로 가는 간선이 두개 이상인 경우 찾기
    두개 이상이면 Error 호출
    """
    for u in g:
        # u: 출발지
        v_counter = collections.Counter(g[u])
        if len(v_counter) == 0:
            # 목적지가 없는 경우: 끝부분인 경우
            continue
        v, n = v_counter.most_common(1)[0]
        if n > 1:
            # 출발지에서 목적지로 가는 간선 갯수가 2개 이상이면 안된다.
            raise ValueError(f"{u} 에서 {v}로 가는 프로세스가 두개 이상이면 안됩니다.")


def __check_only_one_destination(g):
    """
    최종 목적지가 하나인 지 파악하기
    이걸로 동시에 그래프 갯수도 판단할 수 있으며
    그래프가 2개 이상이면 무조건 에러가 발생한다.
    """
    end_cnt = 0
    for u in g:
        if len(g[u]) == 0:
            end_cnt += 1
    if end_cnt > 1:
        raise ValueError("최종 목적지가 두개 이상이면 안됩니다.")


def __get_parents_size(g) -> Dict[str, int]:
    """
    해당 정점으로부터 부모 정점의 갯수 구하기
    """
    parents_size = {k: 0 for k in g.keys()}
    for u in g:
        for v in g[u]:
            parents_size[v] += 1
    return parents_size


def __topological_sort(g, p) -> List[str]:
    """
    위상 정렬
    동시에 사이클도 판단한다.
    """
    q = collections.deque()
    n = len(list(g.keys()))

    for k in g:
        if p[k] == 0:
            q.appendleft(k)

    sorted_data = []
    while q:
        u = q.pop()
        sorted_data.append(u)

        for v in g[u]:
            p[v] -= 1
            if p[v] == 0:
                q.appendleft(v)
    if len(sorted_data) < n:
        raise ValueError("순환 사이클 감지")
    return sorted_data


def topological_sort(g: Dict[str, List[str]]) -> List[str]:
    """
    위상 정렬 함수
    """
    # 중첩되는 간선 파악하기
    __check_double_edge(g)
    # 최종 목적지가 하나인 지 파악하기
    __check_only_one_destination(g)
    # 해당 정점에 대한 부모 정점 갯수 구하기
    parents_size = __get_parents_size(g)
    # 위상 정렬 수행(동시에 사이클까지 잡는다.)
    return __topological_sort(g, parents_size)
