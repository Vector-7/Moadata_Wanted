from typing import Any, List, Callable, Tuple, Optional
from libs.validator.validator import AutomaticValidator


class ValidatorChain:
    """
    여러개의 Validator를 한번에 처리하는 클래스
    """

    """
    Validators
    요소는 Tuple type으로 저장되는데 첫번 째는 Validator, 두번째는 함수가 된다.
    이 함수들은 검수 대상의 데이터를 Validator가 원하는 데이터 타입으로 가공하는데 사용되는
    전처리 함수가 된다.
    """
    validators: List[Tuple[AutomaticValidator, Optional[Callable]]]

    def __init__(self):
        self.validators = []

    def add_validator(self,
                      validator: AutomaticValidator,
                      pre_procssor: Optional[Callable] = None) \
            -> None:
        """
        Validator/전처리 함수 추가, 전처리 함수를 사용하지 않으면 None으로 비워도 된다.
        :param validator:       Validate Class
        :param pre_procssor:    pre-processor function, None if not used function
        """
        self.validators.append((validator, pre_procssor))

    def __call__(self, data: Any) \
            -> (bool, Optional[Exception]):
        """
        Validator를 한번에 작동시킨다.
        :param data: 검수 대사 데이터
        :return: (success, 실패시 Exception)
        """
        for validator, pre_processor in self.validators:
            # 데이터 전처ㅣㄹ
            pre_processed_data = \
                [data] if not pre_processor else pre_processor(data)
            is_valid, err = False, None
            try:
                # Validator 수행
                is_valid, err = validator(*pre_processed_data)
            except Exception as e:
                is_valid, e = False, e

            if not is_valid:
                return False, err
        return True, None
