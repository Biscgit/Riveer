from abc import ABCMeta, abstractmethod
import typing

from celery import current_app as celery_app
from voluptuous import Schema, Any

from core.task import TaskWrapper

if typing.TYPE_CHECKING:
    from core.cron import CronTask

    type Self = type["Self"]
    type Data = list | dict


class BaseNode(metaclass=ABCMeta):
    """This class acts as the base building block for the connected nodes."""

    def __init__(self, config: dict, use_wrapper: bool = True):
        """Registers the function as a celery task."""
        config_schema = self.config_schema().extend({"configuration": Any(dict)})
        self._config = config_schema(config)

        if use_wrapper:
            self.function = TaskWrapper(self.function, self.output_ids)

        _func = celery_app.task(
            self.function,
            name=f"{self.node_type()}-{self.name}-node-process",
            bind=True,
        )
        self.function = _func

    @property
    def output_ids(self) -> list[str]:
        """Returns the ids of the nodes that should be triggered by this node."""
        return []

    @classmethod
    def id(cls) -> str:
        """Returns the id of the class created from the source."""
        return cls.__name__.lower()

    @classmethod
    def node_type(cls) -> str:
        """Returns the name of the parent class, which should be of node type."""
        return cls.__mro__[1].__name__.lower()

    @property
    def name(self) -> str:
        """Returns the name of the created instance."""
        return self._config["configuration"]["name"]

    @classmethod
    def from_configuration(cls: "Self", config: dict) -> "Self":
        """Return an instance for a node using the provided configuration."""
        return cls(config)

    @staticmethod
    @abstractmethod
    def config_schema() -> "Schema":
        """Returns the schema against which the configuration is validated."""

    @abstractmethod
    def connect(self) -> None:
        """This method is called to connect and check the connection to the source."""

    def shutdown(self) -> None:
        """This method is called for cleaning up on shutdown."""
        return None

    @abstractmethod
    def function(self, data: "Data", *args) -> "Data":  # pylint: disable=E0202
        """This function is executed on triggering the instance by another task.
        In the case of it being a source, this is triggered by the set schedules."""


class GraphWriter(BaseNode, metaclass=ABCMeta):
    """Instances that should be allowed to write to the graph."""

    @abstractmethod
    def get_periodic_tasks(self) -> typing.Generator["CronTask"]:
        """Loads the functions with input from the config and returns them if any."""


class GraphReader(BaseNode, metaclass=ABCMeta):
    """Instances that should be allowed to read from the graph."""


class Spring(GraphWriter, metaclass=ABCMeta):
    """Node element that acts as an input to the graph."""

    def __init__(self, config: dict):
        super().__init__(config, use_wrapper=False)

    @property
    def output_ids(self) -> list[str]:
        return list(set(t["name"] for t in self._config["tasks"]))


class Flow(GraphWriter, GraphReader, metaclass=ABCMeta):
    """Node element that acts as a transformer in the graph."""

    def connect(self) -> None:
        return None

    def shutdown(self) -> None:
        return None

    def get_periodic_tasks(self) -> typing.Iterable["CronTask"]:
        return []

    @property
    def output_ids(self) -> list[str]:
        return self._config["processing"]["outputs"]


class Delta(GraphReader, metaclass=ABCMeta):
    """Node element that acts as an output of the graph."""
