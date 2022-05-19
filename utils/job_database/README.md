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