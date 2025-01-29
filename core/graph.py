import logging
import typing

if typing.TYPE_CHECKING:
    from core.node import BaseNode

type Data = list | dict


class NodeGraph:
    """This holds the names of each Node that can be triggered."""
    _pipe_name_mapping: dict[str, "BaseNode"] = {}

    @staticmethod
    def send_result(data: Data, readers: list[str]) -> None:
        """Send a data object to many consumers."""
        print(f"GOT RESULT to {readers}")
        for reader in readers:
            node = NodeGraph._pipe_name_mapping[reader]
            node.function.delay(data)

    @staticmethod
    def register_node(name: str, node: "BaseNode"):
        if name in NodeGraph._pipe_name_mapping.keys():
            raise ValueError(f"Node of name `{name}` already exists.")

        NodeGraph._pipe_name_mapping[name] = node

    @staticmethod
    def iter_over_nodes(*filter_cls: type["BaseNode"]) -> typing.Generator[tuple[str, "BaseNode"]]:
        for name, node in NodeGraph._pipe_name_mapping.items():
            if not filter_cls or isinstance(node, filter_cls):
                yield (name, node)
