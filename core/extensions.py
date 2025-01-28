import inspect

from core.io import AbstractIO

import importlib
import pkgutil

__all__ = ["get_ext_classes"]


def get_ext_classes():
    """Loads all extensions from the `extensions` folder."""

    package_module = importlib.import_module("extensions")

    for _, module_name, is_pkg in pkgutil.walk_packages(package_module.__path__, package_module.__name__ + "."):

        if not is_pkg:
            module = importlib.import_module(module_name)

            for _, cls in inspect.getmembers(module, inspect.isclass):
                if issubclass(cls, AbstractIO):
                    if len(cls.__abstractmethods__) == 0:
                        yield cls
