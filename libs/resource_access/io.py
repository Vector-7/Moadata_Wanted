import _io
from abc import ABCMeta


class RawFileIO(metaclass=ABCMeta):
    """
    텍스트 파일 위주를 다루는 함수
    주로 with문과 함께 쓰인다.
    """
    file_root: str
    fd: _io.TextIOWrapper

    def __init__(self, file_root: str):
        self.file_root = file_root
        self.fd = None

    def __enter__(self, mode: str):
        self.fd = open(self.file_root, mode=mode)
        return self.fd

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.fd.close()
        self.fd = None


class RawFileRead(RawFileIO):

    def __enter__(self, mode: str = None):
        return super().__enter__('rt')


class RawFileWrite(RawFileIO):

    def __enter__(self, mode: str = None):
        return super().__enter__('wt')