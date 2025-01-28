from abc import ABCMeta, abstractmethod

__all__ = ["AbstractIO"]


class AbstractIO(metaclass=ABCMeta):
    @classmethod
    @abstractmethod
    def from_configuration(cls, config: dict) -> "AbstractIO":
        """This method returns a source object from the passed configuration."""
        pass

    @property
    @abstractmethod
    def name(self) -> str:
        """This method returns the name of a created source."""
        pass

    @classmethod
    def id(cls) -> str:
        """Returns the name of the implemented class used for addressing this module."""
        return cls.__name__.lower()

    @property
    @abstractmethod
    def type(self) -> str:
        """Implement in ABC: Name of the ABC's type"""
        pass

    @staticmethod
    @abstractmethod
    def connection_schema() -> dict:
        """This method holds the schema for this source's connection."""
        pass

    @abstractmethod
    def connect(self) -> None:
        """This method is called when initiating the connection to a source."""
        pass

    def check_connection(self) -> bool:
        """This method is called to check if the established connection is okay."""
        return True

    @abstractmethod
    def shutdown(self) -> None:
        """This method is called to shut down the source on program exit."""
        pass
