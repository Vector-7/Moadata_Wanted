import pandas as pd
from typing import Deque, Dict, List, Any

BASE_DIR = 'storage/data'


def __merge_data_frames(left_frame: pd.DataFrame, right_frame: pd.DataFrame)    \
        -> pd.DataFrame:
    """
    두 개의 데이터 프레임을 병합하는 단일 함수
    :param left_frame:  왼쪽 Data Frame
    :param right_frame: 오른쪽 Data Frame
    :return: left + right
    """
    left_cols, right_cols = \
        set(left_frame.columns.values), set(right_frame.columns.values)
    # 겹치는 colum
    common_cols = left_cols & right_cols
    if common_cols:
        # 동일한 column이 존재하는 경우 동일 column대로 병합
        return pd.merge(left_frame, right_frame, how='outer', on=list(common_cols))
    # 없는 경우 그냥 column을 합친다.
    return pd.concat([left_frame, right_frame], axis=1)


def __merge_buffer(buffer: Dict[str, Deque[pd.DataFrame]], task_name: str) \
        -> pd.DataFrame:
    """
    buffer map에서 task_name 위치에 있는 buffer의 모든 DataFrame을 하나로 병합한다.
    :param buffer: buffer map
    :param task_name: task name
    :return: buffer[task_name]에 있는 모든 DataFrame의 합
    """
    data_frame = pd.DataFrame()
    while buffer[task_name]:
        data_frame = __merge_data_frames(buffer[task_name].pop(), data_frame)
    return data_frame


def __send_data_to_next(graph, buffer, dataframe, start_task)   \
        -> None:
    """
    작업이 완료된 DataFrame을 다음 목적지 task에다 전달
    :param graph: task graph
    :param buffer: task buffer
    :param dataframe: 다음 목적지로 보낼 dataframe
    :param start_task:
    """
    for next_task in graph[start_task]:
        # 여러 방향으로 보낼 수 있기 때문에 copy()를 사용한다.
        buffer[next_task].appendleft(pd.DataFrame.copy(dataframe))


def task_read(graph: Dict[str, List[str]],
              buffer: Dict[str, Deque[Any]],
              task_name: str,
              filename: str,
              sep: str) -> None:
    """
    특정 파일로부터 데이터를 불러온다
    :param graph: task graph
    :param buffer: task buffer
    :param task_name: task name
    :param filename: 불러 올 파일 이름
    :param sep: 구분자
    """
    
    # buffer에 남아있는 Data Frame과 전부 병합
    data_frame = __merge_buffer(buffer, task_name)
    try:
        new_data = pd.read_csv(f'{BASE_DIR}/{filename}', sep=sep)
        # 파일에서 읽어온 data frame과 현재 data frame을 병합한다.
        data_frame = __merge_data_frames(data_frame, new_data)
    except Exception:
        # 파일이 없는 경우 그냥 Pass한다
        pass

    # 다음 목적지로 Data Frame 전송
    __send_data_to_next(graph, buffer, data_frame, task_name)


def task_write(graph: Dict[str, List[str]],
               buffer: Dict[str, Deque[Any]],
               task_name: str,
               filename: str,
               sep: str):
    """
    파일에 DataFrame을 작성한다.
    :param graph: task graph
    :param buffer: task buffer
    :param task_name: task name
    :param filename: 생성 또는 새로 갱신할 파일 이름
    :param sep: 구분자
    """
    data_frame = __merge_buffer(buffer, task_name)
    data_frame.to_csv(f'{BASE_DIR}/{filename}', sep=sep, index=False, index_label=False)
    __send_data_to_next(graph, buffer, data_frame, task_name)


def task_drop_column(graph: Dict[str, List[str]],
                     buffer: Dict[str, Deque[Any]],
                     task_name: str,
                     column_name: str):
    """
    DataFrame에 Column을 하나 제거한다.
    :param graph: task graph
    :param buffer: task buffer
    :param task_name: task name
    :param column_name: 제거할 Column 이름
    """
    data_frame = __merge_buffer(buffer, task_name)
    try:
        data_frame = data_frame.drop([column_name], axis=1)
    except Exception:
        # 해당 컬럼이 없으면 그냥 지나감
        pass

    __send_data_to_next(graph, buffer, data_frame, task_name)
