import importlib
import inspect
import pkgutil

from core.node import Spring, Flow, Delta, BaseNode

type NodeClass = type[BaseNode]


class Modules:
    _input_config_map: dict[str, type["Spring"]] = {}
    _transform_config_map: dict[str, type["Flow"]] = {}
    _output_config_map: dict[str, type["Delta"]] = {}

    @classmethod
    def initialize(cls):
        """Loads the extensions and processes them."""
        for node_cls in cls._get_extension_cls():
            if issubclass(node_cls, Spring):
                cls._add_node_cls(node_cls, cls._input_config_map)
            elif issubclass(node_cls, Flow):
                cls._add_node_cls(node_cls, cls._transform_config_map)
            elif issubclass(node_cls, Delta):
                cls._add_node_cls(node_cls, cls._output_config_map)
            else:
                raise ValueError("Invalid class provided")

    @classmethod
    def get_node_cls(cls, pipe_type: str, pipe_id: str) -> NodeClass:
        """Returns the corresponding node class to the type and class id"""
        try:
            if pipe_type == "spring":
                return cls._input_config_map[pipe_id]
            if pipe_type == "flow":
                return cls._transform_config_map[pipe_id]
            if pipe_type == "delta":
                return cls._output_config_map[pipe_id]

            raise ValueError(f"Node of type `{pipe_type}` is invalid.")

        except KeyError as e:
            raise ValueError(f"Node of name `{pipe_id}` is unknown.") from e

    @staticmethod
    def _add_node_cls(node_cls: NodeClass, mapping: dict):
        """Adds a new extension class to one of the dicts."""
        name = node_cls.id()

        if name in mapping.keys():
            raise ValueError("Extension with same id already exists!")

        mapping[name] = node_cls

    @staticmethod
    def _get_extension_cls():
        """Loads all extensions from the `extensions` folder."""

        package_module = importlib.import_module("extensions")

        for _, module_name, is_pkg in pkgutil.walk_packages(
            package_module.__path__, package_module.__name__ + "."
        ):
            if is_pkg:
                continue

            module = importlib.import_module(module_name)

            for _, cls in inspect.getmembers(module, inspect.isclass):
                if issubclass(cls, BaseNode) and len(cls.__abstractmethods__) == 0:
                    yield cls
