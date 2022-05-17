# JobDatabase

Job에 대한 모든 정보를 다루는 Module의 모음

## JobDatabase

### Instance 생성 관련

[구현된 API](/views/job.py) 에서는 매 요청 마다 ```JobDatabase()``` 를 호출합니다. 즉, 요청할 때마다 하나의 인스턴스를 생성하게 되는 데, 만약 수천 개의 요청이 들어오게 되면 1000개의 Instance를 생성하게 되므로 과도한 메모리 낭비로 이어지게 됩니다.

그렇기 때문에 해당 Class는 아무리 많은 생성자 호출을 해도 새로 생성되는 것이 아닌 기존에 생성된 Instance를 꺼내는 방색으로 구현되었습니다. 이때 사용된 Design Pattern은 Singletone Pattern이 됩니다.

```python
    def __new__(cls):
        """
        많은 트래픽으로 인한 Instance 남발을 줄이기 위해
        Singletone Pattern을 적용하여 하나의 인스턴스만 실행한다.
        """
        if not hasattr(cls, 'jobdatabase_instance'):
            cls.jobdatabase_instance = \
                super(JobDatabaseEngine, cls).__new__(cls)
        return cls.jobdatabase_instance
```

### 파일 접근 관련

보통 파일 접근은 하나의 프로세스, 또는 하나의 쓰레드만 접근 할 수 있습니다. 동시에 접근할 수 없으며, DJango의 경우 File DB인 SQLite를 여러 Request가 동시에 접근하면 Permission Error가 발생합니다.

따라서 파일을 차례대로 접근하게 하기 위해 ```threading.Lock```을 추가했으며 File을 접근하는 함수에 Lock을 걸어놓은 ```Decorator Function```을 자체 구현하여 사용하고 있습니다.

* 변수
```python
class JobDatabaseEngine:
    """
    Job.json을 관리하는 일종의 데이터베이스 엔진
    """

    """
    파일 접근은 한번에 하나의 클라이언트가 들어간다.
    파이썬에서는 동시에 접근할 경우 Error가 발생하기 때문에
    파일 접근을 시도할 때 먼저 Lock을 걸어둔다.
    """
    mutex: Lock
```
* [파일 제어 Decorator Function](/libs/resource_access#lock_while_using_file)
* 사용 예제
    ```python
        @lock_while_using_file(self.mutex)
        def __get_item() -> Optional[Dict[str, Any]]:
            """
            파일에 직접 접근하여 데이터 구하기
            :return: Job_ID에 대한 정보, 못찾으면 None Return
            """
            with JobDatabaseRead() as r:
                storage = json.load(r)['jobs']
                # idx -> job_id의 데이터가 위치해 있는 인덱스 값
                is_exists, idx = search_job_by_job_id(storage, job_id)
            return storage[idx] if is_exists else None
    ```


### Functions
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