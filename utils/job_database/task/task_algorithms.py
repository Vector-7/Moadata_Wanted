
import pandas as pd


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
        return pd.merge(left_frame, right_frame, how='outer', 
            on=list(common_cols)), list(common_cols)
    # 없는 경우 그냥 column을 합친다.
    return pd.concat([left_frame, right_frame], axis=1), []
