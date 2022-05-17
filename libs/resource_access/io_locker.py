from threading import Lock


def lock_while_using_file(locker: Lock):
    """
    파일 접근을 하나의 인스턴스(또는 쓰레드)만 접근할 수 있게 제한하는 데코레이터 함수
    동일한 Lock Instance가 있어야 효과를 볼 수 있다.
    """
    def __lock_while_using_file(func):
        def __wrapper(*args, **kwargs):
            locker.acquire()
            try:
                # 본 함수 실행
                output = func(*args, **kwargs)
            except Exception as e:
                """
                에러가 발생할 경우 Lock을 풀어줘야
                다름 쓰레드가 접근을 할 수 있다.
                """
                locker.release()
                raise e
            else:
                locker.release()
            return output

        return __wrapper

    return __lock_while_using_file
