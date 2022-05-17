# JobDatabase

Job에 대한 모든 정보를 다루는 Module의 모음

## JobDatabase
작성중

## Tasks
### 데이터 병합
* 현재 Task내에 처리된 데이터는 다음 Task에서도 처리를 할 수 있게 데이터를 다음 Task 위치로 이동합니다. 이때, 현재 Task에서 두개 이상의 Task로 넘어갈 수 있기 때문에 깊은 복사가 아닌 앝은 복사를 사용합니다.
    ```python
    def __send_data_to_next(graph, buffer, dataframe, start_task):
        for next_task in graph[start_task]:
            # 여러 방향으로 보낼 수 있기 때문에 copy()를 사용한다.
            buffer[next_task].appendleft(pd.DataFrame.copy(dataframe))
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
    def __merge_buffer(buffer: Dict[str, Deque[pd.DataFrame]], task_name: str) -> pd.DataFrame:
        data_frame = pd.DataFrame()
        while buffer[task_name]:
            data_frame = __merge_data_frames(buffer[task_name].pop(), data_frame)
        return data_frame
    ```

### 에러 처리
* 각 Task를 실행할 때 생기는 오류(파일을 찾을수 없음, 없는 Column 제거 등...)는 별다른 경고 없이 통과하게 구현되었습니다.
    * Read를 할 때 해당 파일을 찾을 수 없으면 비어있는 DataFrame을 리턴합니다.
    * 제거할 Column이 없는 경우, DataFrame에 변화를 주지 않습니다.
* 오류에 대한 처리를 구현하게 된다면 진행중인 Task를 맨 처음으로 돌리는 Rollback 기능도 같이 구현할 예정입니다.

### Commands
#### read
* 특정 파일로부터 데이터를 불러옵니다.
#### write
* 현재 갖고 있는 ```DataFrame```을 특정 파일에 생성하거나, 덮어씁니다.
#### drop
* 현재 갖고 있는 ```DataFrame```에서 특정 컬럼을 제거합니다.