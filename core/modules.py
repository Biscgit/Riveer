
import importlib
import pkgutil
import inspect

from core.node import Spring, Flow, Delta, BaseNode

type NodeClass = type[BaseNode]


class Modules:
    _input_config_map: dict[str, type["Spring"]] = {}
    _transform_config_map: dict[str, type["Flow"]] = {}
    _output_config_map: dict[str, type["Delta"]] = {}

    @staticmethod
    def initialize():
        """Loads the extensions and processes them."""
        for cls in Modules._get_extension_cls():
            if issubclass(cls, Spring):
                Modules._add_node_cls(cls, Modules._input_config_map)
            elif issubclass(cls, Flow):
                Modules._add_node_cls(cls, Modules._transform_config_map)
            elif issubclass(cls, Delta):
                Modules._add_node_cls(cls, Modules._output_config_map)
            else:
                raise ValueError("Invalid class provided")

    @staticmethod
    def get_node_cls(pipe_type: str, pipe_id: str) -> NodeClass:
        try:
            if pipe_type == "spring":
                return Modules._input_config_map[pipe_id]
            elif pipe_type == "flow":
                return Modules._transform_config_map[pipe_id]
            elif pipe_type == "delta":
                return Modules._output_config_map[pipe_id]
            else:
                raise ValueError(f"Node of type `{pipe_type}` is invalid.")

        except KeyError:
            raise ValueError(f"Node of name `{pipe_id}` is unknown.")

    @staticmethod
    def _add_node_cls(cls: NodeClass, mapping: dict):
        """Adds a new extension class to one of the dicts."""
        name = cls.id()

        if name in mapping.keys():
            raise ValueError("Extension with same id already exists!")

        mapping[name] = cls

    @staticmethod
    def _get_extension_cls():
        """Loads all extensions from the `extensions` folder."""

        package_module = importlib.import_module("extensions")

        for _, module_name, is_pkg in pkgutil.walk_packages(package_module.__path__, package_module.__name__ + "."):
            if is_pkg:
                continue

            module = importlib.import_module(module_name)

            for _, cls in inspect.getmembers(module, inspect.isclass):
                if issubclass(cls, BaseNode) and len(cls.__abstractmethods__) == 0:
                    yield cls
