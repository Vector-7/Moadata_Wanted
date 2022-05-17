# libs.io

외부 리소스(파일 등...)를 다루는 Module

## io
### RawFileIO
* 분류: Class _(Abstract)_
* 파일 Read/Write관련 Class, 기능은 open()과 동일하나 RawFileRead,
RawFileWrite구현을 위해 구현된 추상클래스


### RawFileRead
* 분류: Class
* 상위 클래스: RawFileIO
* text File을 ```with``` 문을 이용해 읽기 위해 구현된 클래스

#### Example
    ```python
    with RawFileRead('file.txt') as r:
        json_data = json.load(r)
    ```

### RawFileWrite
* 분류: Class
* 상위 클래스: RawFileIO
* text File을 ```with``` 문을 이용해 쓰기 위해 구현된 클래스

#### Example

    ```python
    with RawFileWrite('new.txt') as w:
        w.write("hello world\n")
    ```

## io_locker

### lock_while_using_file
* 분류: **decorator** function
* 보통 파일은 단 하나의 Thread만 접근할 수 있다. 파이썬의 경우 파일로 DB를 생성하는 SQLite
을 여러 Thread가 접근 할 경우 ```Exception```이 발생한다. 이를 초기에 막기 위해 사용되는 데코레이터 함수이다.
* 하지만 남용할 경우 어플리케이션의 성능에 영향을 미칠 수 있으므로 가능한 적게 사용할 수 있도록 노력하자.
* Parameter

  |Variable|Type|Comment|
  |---|---|---|
  |locker|```threading.Lock```|파일에 직접 접근하는 함수 전체를 잠글 대 사용되는 변수|

* Output
  
  |Type|Comment|
  |---|---|
  |```NoneType```|리턴값 없음|

* Exception
  
  |Type|Comment|
  |---|---|
  |```Exception```|데코레이터가 적용된 함수가 호출하는 모든 ```Exception```을 그대로 호출한다.

#### Example

  ```python
  from threading import Lock
  
  locker = Lock()
  
  @lock_while_using_file(locker)
  def foo(*args, **kwargs):
    # do something
  ```