from abc import ABCMeta, abstractmethod
from typing import Callable, Optional


class Validator(metaclass=ABCMeta):
    """
    자체 Validator Interface
    """

    @abstractmethod
    def __call__(self, *args, **kwargs) \
            -> (bool, Optional[Exception]):
        """
        Call 함수를 사용해서 data 유효성을 판별한다.

        :return True if data is valid
        :return False if data is not valid and return Error Exception
        """
        pass


class AutomaticValidator(Validator):
    """
    Validator로부터 상속받는 클래스로 Validator Class에서 Validator를 만드려면
    Validator를 직접 상속받고 __call__ 함수를 오버라이딩 해야 하지만
    AutomaticValidator는 따로 상속 없이 Validator Logic함수만 추가하면 된다.
    """

    """
    validate 로직 함수, 리턴 값이 bool인 함수를 권장한다.
    """
    validate_logic: Callable

    def __init__(self, validate_logic: Callable):
        """
        validate 함수 추가
        """
        if not isinstance(validate_logic, Callable):
            raise TypeError("logic must be Callable Function")
        self.validate_logic = validate_logic

    def __call__(self, *args, **kwargs) \
            -> (bool, Optional[Exception]):
        """
        :param *args or **kwargs: validate데이터

        :return: (True, None) if data is valid, then Exception is None
        :return: (False, Exception) if data is not valid, then Exception is returned
        """
        try:
            # Validate 판별
            is_valid = self.validate_logic(*args, **kwargs)
        except Exception as e:
            # Validate에서 Excrption이 호출되면 False 처리
            return False, e

        if isinstance(is_valid, bool) and not is_valid:
            return False, ValueError("Validate Failed")
        return True, None
