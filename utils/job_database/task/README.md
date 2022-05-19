# Task

Task를 수행하기 위해 추가로 구현된 Task관련 Class들 입니다.

## TaskSpace

Task를 수행하기 위해 사용되는 단일 Task 클래스 입니다. 추상 클래스로 선언되어 있습니다.
### TaskReadSpace
csv파일을 읽어 Dataframe을 병합합니다.

### TaskWriteSpace
갖고 있는 Dataframe을 csv파일에 저장합니다.

### TaskDropColumnSpace
현재 가지고 있는 Dataframe에서 Column을 삭제합니다.

## TaskWorker
TaskSpace를 모아서 한꺼번에 처리하는 클래스 입니다.


## 데이터 병합 원리
* 현재 Task내에 처리된 데이터는 다음 Task에서도 처리를 할 수 있게 데이터를 다음 Task 위치로 이동합니다. 이때, 현재 Task에서 두개 이상의 Task로 넘어갈 수 있기 때문에 깊은 복사가 아닌 앝은 복사를 사용합니다.
    ```python
        def input_dataframe(self, task_name: str, dataframe: pd.DataFrame):
        """
        dataframe_buffer에 dataframe을 push할 때 사용
        """
        self.dataframe_buffer.appendleft(
            (task_name, pd.DataFrame.copy(dataframe))
        )
    ```

* ```Task1 -> Task2 -> Task3``` 처럼 한뱡항으로 설정되어 있을 경우, 다음 Task에는 아무런 데이터가 없기 때문에 계속 갱신하면서 처리를 하면 되지만.
```(Task1) -> (Task2, Task3) -> Task4``` 처럼 ```Task2```와 ```Task3```의 데이터들이 동시에 ```Task4```로 모이는 경우가 있습니다. 이때 ```Task2```와 ```Task3```의
데이터 두개를 병합을 해서 ```Task4```에서 처리를 하게 됩니다. 병합을 할 때, 두 가지의 경우의 수가 존재하게 됩니다.
    * 동일한 Column이 없는 경우: 단순히 column을 붙입니다.
        ```python
        return pd.concat([left_frame, right_frame], axis=1)
        ```
    * 동일한 Column이 있는 경우: 이 Column들을 중심으로 병합을 합니다.
        ```python
        return pd.merge(left_frame, right_frame, how='outer', on=list(common_cols))
        ```

* 따라서 Task를 수행하기 전에 이전 Task의 결과를 모아야 하는데, 이때 Python 내장 자료구조(```from collections import deque```)인 Deque를 사용합니다. 예를 들어 ```(Task1, Task2, Task3) -> Task4``` 라는 Job이 있다고 가정할 때, ```Task1 ~ 3```는 테스트를 수행한 결과 DataFrame을 ```Task4```의 Data Queue에 ```push```를 한 다음, ```Task4```의 차례가 되면, 자신의 Data Queue에 저장되어 있는 이전의 DataFrmae을 전부 꺼내 병합을 한 다음, Task를 수행하게 됩니다. 이러한 Data Queue를 해당 프로젝트에서는 buffer라고 부릅니다.
    ```python
    def merge_dataframes_in_buffer(self):
        """
        dataframe_buffer에 들어있는 모든 dataframe을 병합한다.
        """
        merged_dataframe = pd.DataFrame()
        while self.dataframe_buffer:
            prev_name, prev_buffer = self.dataframe_buffer.pop()
            merged_dataframe, common_columns = merge_dataframes(merged_dataframe, prev_buffer)
        return merged_dataframe
    ```