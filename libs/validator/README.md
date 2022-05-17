# libs.validator

Data Format의 유효성을 판단하기 위한 모듈.

## validator

### Validator
* 분류: Class _(Abstract)_
* 상위 클래스: _없음_
* 추상 클래스로서 상속을 받은 다음, call 함수를 커스텀 하여 Validate Logic을 구현할 수 있다.

#### Functions
```__call__```
* 분류: **Magic** _abstract_ Function
* Parameter

    |Variable|Type|Comment|
    |---|---|---|
    |```*args```, ```**kwargs**```|```Any```|검증받을 데이터가 들어간다. 어느 타입이어도 상관없다.|

* Returns

    |Type|Comment|
    |---|---|
    |```bool```|입력 Data가 검증에 통과하면 ```True```, 그렇지 않으면 ```False```|
    |```Exception (Optional)```|검증에 실패했을 경우, ```bool``` 대신 사용할 수 있다. 단 return으로 출력되어야 하며 ```raise``` 문으로 호출해서는 안된다.|

#### Example

```python
class MyValidator(Validator):
    def __call__(self, data):
        return data > 10
  
v = MyValidator()
success, err = v(20)
print(success)  # True
```

### AutomaticValidator
* 분류: Class
* 상위 클래스: Validator
* Validator가 직접 상속을 받고 call함수를 구현해야 했다면 AutomaticValidator는
* 상속 없이 데이터 검증에 사용되는 함수만 추가하면 데이터가 들어올 때, 추가된 함수로
데이터 검증을 진행한다.

#### Variable

|Variable|Type|Comment|
|---|---|---|
|```validate_logic```|```Function(Any) -> bool```|데이터 검증에 사용되는 함수 이다. ```return```값이 ```bool```인 함수를 사용해야 한다.|

#### functions

```__init__```

* 분류: **Magic** Function
* Paremeter

  |Variable|Type|Comment|
  |---|---|---|
  |```validate_logic```|```Function(Any) -> bool```|Variable과 동일한 설명|



```__call__```

* 분류: **Magic** Function
* Parameter

    |Variable|Type|Comment|
    |---|---|---|
    |```*args```, ```**kwargs**```|```Any```|검증받을 데이터가 들어간다. 어느 타입이어도 상관없다.|

* Returns

    |Type|Comment|
    |---|---|
    |```bool```|입력 Data가 검증에 통과하면 ```True```, 그렇지 않으면 ```False```|
    |```Exception (Optional)```|검증에 실패했을 경우, ```bool``` 대신 사용할 수 있다.|

#### Example

```python
def over10(o):
    return o >= 10

v = AutomaticValidator(over10)
success, err = v(20)
print(success)  # True
print(err)      # None
```

## validator_chain

### ValidatorChain
* 분류: Class
* 상위 클래스: _없음_
* ValidatorChain은 하나의 데이터에 여러 종류의 데이터 검증이 필요할 경우, 여러 개의 Validator를
ValidatorChain에 전부 집어 넣고 ```__call__``` 함수 하나로 모든 Validate를 수행할 수 있다.

#### Variables

|Variable|Type|Comment|
|---|---|---|
|```validators```|```List[(AutomaticValidator, Callable(Optional)]```|Validator List. 오른쪽 Function은 Validator가 실행되기 전에 Validator에 맞는 데이터 타입으로 변환시키는 전처리 함수가 들어간다.|


#### functions

```add_validator```
* 분류: Function
* validator를 추가하는 함수
* Parameter

    |Variable|Type|Comment|
    |---|---|---|
    |```validator```|```AutomaticValidator```|데이터를 검수할 AutomaticValidator|
    |```pre_processor```|```Function(Any) -> Iterable[Any]```|데이터를 검수하기 전에 해당 Validator에 맞는 데이터 타입으로 변환하는 커스텀 함수다. 이때 리턴값은 List, Tuple 등이 되어야 한다. |

* Returns
  * 없음

```__call__```

* 분류: **Magic** Function
* Validator 검증을 시도하는 함수
* Parameters

    |Variable|Type|Comment|
    |---|---|---|
    |```data```|```Any```|검수 대상인 데이터가 들어간다.|

* Returns

    |Type|Comment|
    |---|---|
    |```bool```|입력 Data가 검증에 통과하면 ```True```, 그렇지 않으면 ```False```|
    |```Exception (Optional)```|검증에 실패했을 경우, ```bool``` 대신 사용할 수 있다.|

#### Example

```python
def down1000(o):
    return o < 1000
def over100(o):
    return o >= 100


data = {'v1': 100, 'v2': 300}
v = ValidatorChain()
v.add_validator(
    AutomaticValidator(validate_logic=down1000),
    lambda o: (o['v1'],)
)
v.add_validator(
    AutomaticValidator(validate_logic=over100),
    lambda o: (o['v2'],)
)

success, err = v(data)
print(success)  # True
print(err)      # None

```
