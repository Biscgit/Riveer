from abc import ABCMeta, abstractmethod

from core.io import AbstractIO

__all__ = ["AbstractSink"]


class AbstractSink(AbstractIO, metaclass=ABCMeta):

    @abstractmethod
    def process(self, data: list[dict]) -> None:
        """This method is called as a celery task to process the data."""
        pass

    @property
    def type(self) -> str:
        return "output"
