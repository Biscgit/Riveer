from abc import ABCMeta, abstractmethod

import typing

from core.io import AbstractIO

if typing.TYPE_CHECKING:
    from consumers.sourcetask import SourceTask

__all__ = ["AbstractSource"]


class AbstractSource(AbstractIO, metaclass=ABCMeta):
    @property
    def type(self) -> str:
        return "input"

    @staticmethod
    @abstractmethod
    def task_schema() -> dict:
        """This method holds the schema for this source's tasks."""
        pass

    @abstractmethod
    def create_tasks(self) -> list["SourceTask"]:
        """This method is called to create a task for the source."""
        pass

    @abstractmethod
    def task(self, *args) -> list[dict]:
        """This method holds the task schema that is executed"""
        pass
