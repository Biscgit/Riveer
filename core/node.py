from abc import ABCMeta, abstractmethod
import typing

from celery import current_app as celery_app

type Self = type["Self"]
type Data = list | dict

if typing.TYPE_CHECKING:
    from core.task import CronTask


class BaseNode(metaclass=ABCMeta):
    """This class acts as the base building block for the connected nodes."""

    def __init__(self, config):
        """Registers the function as a celery task."""
        self._config = config
        self.function = celery_app.task(
            self.function,
            name=f"{self.node_type()}-{self.name}-node-process",
            bind=True,
        )

    @classmethod
    def id(cls) -> str:
        """Returns the id of the class created from the source."""
        return cls.__name__.lower()

    @classmethod
    def node_type(cls) -> str:
        """To be implemented in the different ABC::Nodes."""
        return cls.__mro__[1].__name__.lower()

    @property
    def name(self) -> str:
        """Returns the name of the created instance."""
        return self._config["configuration"]["name"]

    @classmethod
    @abstractmethod
    def from_configuration(cls: "Self", config: dict) -> "Self":
        """Return an instance for a node using the provided configuration."""

    @abstractmethod
    def connect(self) -> None:
        """This method is called to connect and check the connection to the source."""

    @abstractmethod
    def shutdown(self) -> None:
        """This method is called for cleaning up on shutdown."""

    @abstractmethod
    def function(self, data: "Data", *args) -> "Data":
        """This function is executed on triggering the instance by another task.
        In the case of it being a source, this is triggered by the set schedules."""


class PipeWriter(BaseNode, metaclass=ABCMeta):
    """Instances that should be allowed to write to a pipe."""

    @abstractmethod
    def get_periodic_tasks(self) -> typing.Generator["CronTask"]:
        """Loads the functions with input from the config and returns them if any."""


class PipeReader(BaseNode, metaclass=ABCMeta):
    """Instances that should be allowed to read from a pipe."""


class Spring(PipeWriter, metaclass=ABCMeta):
    """Node element that acts as an input to the system."""


class Flow(PipeWriter, PipeReader, metaclass=ABCMeta):
    """Node element that acts as a transformer in the system."""

    def connect(self) -> None:
        return None

    def shutdown(self) -> None:
        return None

    def get_periodic_tasks(self) -> typing.Generator["CronTask"]:
        raise StopIteration()


class Delta(PipeReader, metaclass=ABCMeta):
    """Node element that acts as an output of the system."""
